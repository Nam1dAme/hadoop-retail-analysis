"""Versioned purchase-behavior models; source user/item IDs never enter the network."""
import os
os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')
import random
import numpy as np
import torch
from torch import nn

FEATURES = ('category', 'behavior', 'hour', 'gap', 'numeric', 'length')


def seed_all(seed=42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False


class Logistic(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(14, 1)

    def forward(self, batch):
        return self.linear(batch['numeric']).squeeze(-1)


class BehaviorGRU(nn.Module):
    def __init__(self):
        super().__init__()
        self.category = nn.Embedding(16385, 16, padding_idx=0)
        self.behavior = nn.Embedding(5, 8, padding_idx=0)
        self.hour = nn.Embedding(25, 4, padding_idx=0)
        self.project = nn.Sequential(nn.Linear(29, 64), nn.ReLU())
        self.gru = nn.GRU(64, 64, batch_first=True)
        self.head = nn.Sequential(nn.Linear(78, 32), nn.ReLU(), nn.Dropout(.1), nn.Linear(32, 1))

    def forward(self, batch):
        sequence = torch.cat((self.category(batch['category']), self.behavior(batch['behavior']),
                              self.hour(batch['hour']), batch['gap'].unsqueeze(-1)), dim=-1)
        hidden, _ = self.gru(self.project(sequence))
        # The unidirectional hidden at the last real event is independent of later PADs.
        last = hidden[torch.arange(len(hidden), device=hidden.device), batch['length'] - 1]
        return self.head(torch.cat((last, batch['numeric']), dim=-1)).squeeze(-1)


def make_model(algorithm):
    if algorithm == 'LOGISTIC':
        return Logistic()
    if algorithm == 'GRU':
        return BehaviorGRU()
    raise ValueError('Unsupported trainable algorithm')


def tensors(batch, device, algorithm='GRU'):
    keys = FEATURES if algorithm == 'GRU' else ('numeric',)
    result = {}
    for key in (*keys, 'label'):
        if key not in batch:
            continue
        dtype = torch.long if key in ('category', 'behavior', 'hour', 'length') else torch.float32
        value = torch.from_numpy(batch[key]).to(dtype=dtype)
        if device.type == 'cuda':
            value = value.pin_memory().to(device, non_blocking=True)
        result[key] = value
    return result

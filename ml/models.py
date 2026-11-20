"""Versioned purchase-behavior models; source user/item IDs never enter the network."""
import os
os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')
import random
import numpy as np
import torch
from torch import nn

FEATURES = ('category', 'behavior', 'hour', 'gap', 'numeric', 'length')
NUMERIC_DIM = 20
GRU_LAYERS = 2
GRU_HIDDEN = 128
INPUT_DTYPES = {'category': torch.int32, 'behavior': torch.int32, 'hour': torch.int32, 'length': torch.int64}
# Raw (de-standardized) numeric dimensions for the RULE baseline: 48h cart/buy/fav and log1p(uncarted-bought items).
RULE_DIMS = (10, 9, 11, 18)


def rule_group_codes(numeric, mean, std):
    """Map standardized numeric rows to 16 groups (cart,buy,fav,other as 8/4/2/1 bits)."""
    raw = np.asarray(numeric, dtype=np.float64) * np.asarray(std, dtype=np.float64) + np.asarray(mean, dtype=np.float64)
    # log1p(count) is 0 for count=0 and >=log(2) otherwise; a midpoint threshold absorbs float32 round-trip error.
    signals = raw[:, RULE_DIMS] > 0.5 * np.log(2.0)
    return (signals[:, 0].astype(np.int64) * 8 + signals[:, 1].astype(np.int64) * 4 +
            signals[:, 2].astype(np.int64) * 2 + signals[:, 3].astype(np.int64))


def rule_scores(numeric, rule, mean=None, std=None):
    """Look up the TRAIN group rate for each sample; rule is {'table': {'0000': rate, ...}}."""
    table = rule['table'] if isinstance(rule, dict) and 'table' in rule else rule
    if mean is None or std is None:
        mean, std = rule['mean'], rule['std']
    values = np.asarray([table[f'{code:04b}'] for code in range(16)], dtype=np.float64)
    return values[rule_group_codes(numeric, mean, std)]


def seed_all(seed=42, deterministic=False):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(deterministic)
    torch.backends.cudnn.benchmark = not deterministic
    torch.backends.cudnn.deterministic = deterministic
    torch.backends.cuda.matmul.allow_tf32 = not deterministic
    torch.backends.cudnn.allow_tf32 = not deterministic


class Logistic(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(NUMERIC_DIM, 1)

    def forward(self, batch):
        return self.linear(batch['numeric']).squeeze(-1)


class BehaviorGRU(nn.Module):
    def __init__(self, hidden_size=GRU_HIDDEN, num_layers=GRU_LAYERS, dropout=0.1):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.category = nn.Embedding(16385, 16, padding_idx=0)
        self.behavior = nn.Embedding(5, 8, padding_idx=0)
        self.hour = nn.Embedding(25, 4, padding_idx=0)
        self.project = nn.Sequential(nn.Linear(29, hidden_size), nn.LayerNorm(hidden_size), nn.ReLU())
        self.gru = nn.GRU(hidden_size, hidden_size, num_layers=num_layers, batch_first=True,
                          dropout=dropout if num_layers > 1 else 0.0)
        self.head = nn.Sequential(nn.Linear(hidden_size + NUMERIC_DIM, 64), nn.ReLU(), nn.Dropout(dropout), nn.Linear(64, 1))

    def forward(self, batch):
        lengths = batch['length'].cpu()
        max_len = int(lengths.max())
        sequence = torch.cat((self.category(batch['category'][:, :max_len]),
                              self.behavior(batch['behavior'][:, :max_len]),
                              self.hour(batch['hour'][:, :max_len]),
                              batch['gap'][:, :max_len].unsqueeze(-1)), dim=-1)
        projected = self.project(sequence)
        packed = nn.utils.rnn.pack_padded_sequence(
            projected, lengths, batch_first=True, enforce_sorted=False
        )
        # Packed GRU stops at each sample's true length, so final hidden state ignores all PADs.
        _, hidden = self.gru(packed)
        last = hidden[-1]
        return self.head(torch.cat((last, batch['numeric']), dim=-1)).squeeze(-1)


def make_model(algorithm):
    if algorithm == 'LOGISTIC':
        return Logistic()
    if algorithm == 'GRU':
        return BehaviorGRU()
    raise ValueError('Unsupported trainable algorithm')


def tensor_columns(batch, algorithm):
    keys = FEATURES if algorithm == 'GRU' else ('numeric',)
    for key in (*keys, 'label'):
        if key in batch:
            yield key, batch[key], INPUT_DTYPES.get(key, torch.float32)


def tensors(batch, device, algorithm='GRU'):
    result = {}
    for key, array, dtype in tensor_columns(batch, algorithm):
        value = torch.from_numpy(array).to(dtype=dtype)
        # pack_padded_sequence uses CPU lengths. Keep the packed model off a GPU-to-CPU sync.
        if device.type == 'cuda' and key != 'length':
            value = value.pin_memory().to(device, non_blocking=True)
        result[key] = value
    return result

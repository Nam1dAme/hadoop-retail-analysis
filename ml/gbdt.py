"""Pure-numpy histogram gradient boosting for the 20-dimensional numeric features.

The binned matrix is kept feature-major (NUMERIC_DIM, n_rows) so every split
histogram reads a contiguous uint8 row.  No third-party learner is required.
"""
import hashlib
import json
import os
from pathlib import Path

import numpy as np

from orchestration.common import ManifestError, encode, sha
from .models import NUMERIC_DIM

SCHEMA_VERSION = 'gbdt-v1'
ALGORITHM = 'GBDT'
MAX_BINS = 64
DEFAULT_PARAMS = dict(max_bins=MAX_BINS, sample_limit=200_000, max_depth=4,
                      min_child_samples=200, learning_rate=0.1, l2=1.0,
                      max_trees=300, patience=20)


def sigmoid(logits):
    """Numerically stable elementwise sigmoid, always float64."""
    z = np.asarray(logits, dtype=np.float64)
    result = np.empty(z.shape, dtype=np.float64)
    positive = z >= 0
    result[positive] = 1. / (1. + np.exp(-z[positive]))
    exp_negative = np.exp(z[~positive])
    result[~positive] = exp_negative / (1. + exp_negative)
    return result


def _check_params(params):
    params = dict(params)
    if not 2 <= int(params['max_bins']) <= MAX_BINS:
        raise ValueError('max_bins must be between 2 and 64')
    if int(params['sample_limit']) < 2 or int(params['max_depth']) < 1:
        raise ValueError('Invalid sample_limit/max_depth')
    if int(params['min_child_samples']) < 1 or int(params['max_trees']) < 1 or int(params['patience']) < 1:
        raise ValueError('Invalid min_child_samples/max_trees/patience')
    if not 0 < float(params['learning_rate']) <= 1 or float(params['l2']) < 0:
        raise ValueError('Invalid learning_rate/l2')
    return dict(max_bins=int(params['max_bins']), sample_limit=int(params['sample_limit']),
                max_depth=int(params['max_depth']), min_child_samples=int(params['min_child_samples']),
                learning_rate=float(params['learning_rate']), l2=float(params['l2']),
                max_trees=int(params['max_trees']), patience=int(params['patience']))


def _check_numeric(x):
    array = np.asarray(x)
    if array.ndim != 2 or array.shape[1] != NUMERIC_DIM:
        raise ValueError(f'GBDT requires ({NUMERIC_DIM},) numeric features')
    if not np.isfinite(array).all():
        raise ValueError('Non-finite numeric feature')
    return array


def _row_count(source):
    if isinstance(source, tuple):
        return len(_check_numeric(source[0]))
    rows = int(source.manifest['rows'])
    if rows < 1:
        raise ValueError('Empty TRAIN split')
    return rows


def sample_train_rows(train, limit=DEFAULT_PARAMS['sample_limit'], seed=42, batch_size=32768):
    """Deterministic fixed-seed TRAIN sample (reservoir for shards, choice for arrays)."""
    if limit < 2:
        raise ValueError('sample_limit must be at least 2')
    rng = np.random.default_rng(seed)
    if isinstance(train, tuple):
        x, y = train
        x = _check_numeric(x)
        y = np.asarray(y, dtype=np.int8)
        if len(x) != len(y):
            raise ValueError('TRAIN feature/label count differs')
        size = min(limit, len(x))
        if size == len(x):
            return x.copy(), y.copy()
        indices = rng.choice(len(x), size=size, replace=False)
        return np.asarray(x[indices]), np.asarray(y[indices])
    rows = _row_count(train)
    size = min(limit, rows)
    sample = np.empty((size, NUMERIC_DIM), dtype=np.float32)
    sample_labels = np.empty(size, dtype=np.int8)
    filled = 0
    offset = 0
    for batch in train.batches(batch_size, shuffle=False, columns=('numeric', 'label')):
        values = batch['numeric']
        labels = np.asarray(batch['label'], dtype=np.int8)
        take = min(size - filled, len(values))
        if take:
            sample[filled:filled + take] = values[:take]
            sample_labels[filled:filled + take] = labels[:take]
            filled += take
        positions = np.arange(take, len(values))
        if len(positions):
            candidates = rng.integers(0, offset + positions + 1, size=len(positions))
            hit = candidates < size
            sample[candidates[hit]] = values[positions[hit]]
            sample_labels[candidates[hit]] = labels[positions[hit]]
        offset += len(values)
    if offset != rows or filled != size:
        raise ValueError('Incomplete TRAIN sample pass')
    return sample[:filled], sample_labels[:filled]


def compute_bin_edges(sample, max_bins=DEFAULT_PARAMS['max_bins']):
    """TRAIN-only equal-frequency edges; at most max_bins bins per feature."""
    sample = _check_numeric(sample)
    if not 2 <= int(max_bins) <= MAX_BINS:
        raise ValueError('max_bins must be between 2 and 64')
    quantiles = np.linspace(0., 1., int(max_bins) + 1)
    edges = []
    for feature in range(NUMERIC_DIM):
        values = np.unique(np.quantile(sample[:, feature].astype(np.float64), quantiles))
        edges.append(values.tolist())
    return edges


def bin_counts(edges):
    """Two distinct TRAIN values need two bins; a single value stays constant."""
    return [1 if len(values) < 2 else 2 if len(values) == 2 else len(values) - 1 for values in edges]


def binarize(x, edges):
    """Return a feature-major uint8 bin matrix for float numeric rows."""
    x = _check_numeric(x)
    counts = bin_counts(edges)
    result = np.empty((NUMERIC_DIM, len(x)), dtype=np.uint8)
    for feature in range(NUMERIC_DIM):
        boundaries = np.asarray(edges[feature], dtype=np.float64)
        if len(boundaries) > 2:
            result[feature] = np.searchsorted(boundaries[1:-1], x[:, feature], side='right')
        elif len(boundaries) == 2:
            # Binary TRAIN feature: split at the lower value so both levels remain separable.
            result[feature] = (x[:, feature] > boundaries[0]).astype(np.uint8)
        else:
            result[feature] = 0
        if counts[feature] > MAX_BINS:
            raise ValueError('Too many bins for uint8 storage')
    return result


def _binned_train(train, edges, batch_size=32768):
    rows = _row_count(train)
    codes = np.empty((NUMERIC_DIM, rows), dtype=np.uint8)
    labels = np.empty(rows, dtype=np.int8)
    if isinstance(train, tuple):
        x, y = train
        _check_numeric(x)
        codes[:] = binarize(x, edges)
        labels[:] = np.asarray(y, dtype=np.int8)
        return codes, labels
    offset = 0
    for batch in train.batches(batch_size, shuffle=False, columns=('numeric', 'label')):
        size = len(batch['label'])
        codes[:, offset:offset + size] = binarize(batch['numeric'], edges)
        labels[offset:offset + size] = batch['label']
        offset += size
    if offset != rows:
        raise ValueError('Incomplete TRAIN binning pass')
    return codes, labels


def _validation_arrays(validation, batch_size=32768):
    if isinstance(validation, tuple):
        x, y = validation
        x = _check_numeric(x)
        y = np.asarray(y, dtype=np.int8)
        if len(x) != len(y):
            raise ValueError('VALIDATION feature/label count differs')
        return x, y
    rows = int(validation.manifest['rows'])
    features = np.empty((rows, NUMERIC_DIM), dtype=np.float32)
    labels = np.empty(rows, dtype=np.int8)
    offset = 0
    for batch in validation.batches(batch_size, shuffle=False, columns=('numeric', 'label')):
        size = len(batch['label'])
        features[offset:offset + size] = batch['numeric']
        labels[offset:offset + size] = batch['label']
        offset += size
    if offset != rows:
        raise ValueError('Incomplete VALIDATION pass')
    return features, labels


def _logloss_from_logits(logits, labels):
    probability = np.clip(sigmoid(logits), 1e-12, 1. - 1e-12)
    y = labels.astype(np.float64)
    return float(np.mean(-y * np.log(probability) - (1. - y) * np.log1p(-probability)))


def _build_tree(codes, gradient, hessian, bin_counts_, params):
    """Level-wise exact histogram tree; root is depth 0, leaves stop at max_depth."""
    nodes = []
    queue = [(np.arange(codes.shape[1]), 0)]
    n_features = codes.shape[0]
    while queue:
        indices, depth = queue.pop(0)
        count = len(indices)
        position = len(nodes)
        nodes.append(None)
        gradients = gradient[indices]
        hessians = hessian[indices]
        total_g = float(gradients.sum())
        total_h = float(hessians.sum())
        if depth >= params['max_depth'] or count < 2 * params['min_child_samples']:
            nodes[position] = dict(feature=-1, bin_threshold=0, left=-1, right=-1,
                                   value=float(-total_g / (total_h + params['l2'])))
            continue
        best_gain = 0.
        best_feature = -1
        best_threshold = -1
        for feature in range(n_features):
            bins = codes[feature][indices]
            width = bin_counts_[feature]
            counts = np.bincount(bins, minlength=width)
            left_count = np.cumsum(counts)[:-1]
            valid = (left_count >= params['min_child_samples']) & \
                    (count - left_count >= params['min_child_samples'])
            if not valid.any():
                continue
            left_g = np.cumsum(np.bincount(bins, weights=gradients, minlength=width))[:-1]
            left_h = np.cumsum(np.bincount(bins, weights=hessians, minlength=width))[:-1]
            right_g = total_g - left_g
            right_h = total_h - left_h
            gains = .5 * (left_g ** 2 / (left_h + params['l2']) +
                          right_g ** 2 / (right_h + params['l2']) -
                          total_g ** 2 / (total_h + params['l2']))
            gains = np.where(np.isfinite(gains), gains, -np.inf)
            gains[~valid] = -np.inf
            threshold = int(np.argmax(gains))  # smallest threshold wins exact ties
            if gains[threshold] > best_gain + 1e-12:
                best_gain = float(gains[threshold])
                best_feature = feature
                best_threshold = threshold
        if best_feature < 0:
            nodes[position] = dict(feature=-1, bin_threshold=0, left=-1, right=-1,
                                   value=float(-total_g / (total_h + params['l2'])))
            continue
        masks = codes[best_feature][indices] <= best_threshold
        child_base = len(nodes) + len(queue)
        nodes[position] = dict(feature=best_feature, bin_threshold=best_threshold,
                               left=child_base, right=child_base + 1, value=0.)
        queue.append((indices[masks], depth + 1))
        queue.append((indices[~masks], depth + 1))
    return dict(nodes=nodes)


def _tree_values(tree, codes):
    nodes = tree['nodes']
    features = np.fromiter((node['feature'] for node in nodes), dtype=np.int16, count=len(nodes))
    thresholds = np.fromiter((node['bin_threshold'] for node in nodes), dtype=np.int16, count=len(nodes))
    left = np.fromiter((node['left'] for node in nodes), dtype=np.int32, count=len(nodes))
    right = np.fromiter((node['right'] for node in nodes), dtype=np.int32, count=len(nodes))
    values = np.fromiter((node['value'] for node in nodes), dtype=np.float64, count=len(nodes))
    current = np.zeros(codes.shape[1], dtype=np.int32)
    while True:
        active = np.flatnonzero(features[current] >= 0)
        if not len(active):
            break
        current_nodes = current[active]
        go_right = codes[features[current_nodes], active] > thresholds[current_nodes]
        current[active] = np.where(go_right, right[current_nodes], left[current_nodes])
    return values[current]


def train_binned(codes, labels, validation_codes, validation_labels, edges, params, seed=42):
    """Boost on pre-binned TRAIN; only validation_logloss drives early stopping."""
    params = _check_params(params)
    counts = bin_counts(edges)
    if len(edges) != NUMERIC_DIM or codes.shape[0] != NUMERIC_DIM or validation_codes.shape[0] != NUMERIC_DIM:
        raise ValueError('GBDT feature width mismatch')
    y = labels.astype(np.float64)
    positive = float(y.sum())
    if not 0 < positive < len(y):
        raise ValueError('TRAIN must contain both classes')
    base_score = float(np.log(positive / (len(y) - positive)))
    logits = np.full(len(y), base_score, dtype=np.float64)
    validation_logits = np.full(len(validation_labels), base_score, dtype=np.float64)
    best_loss = _logloss_from_logits(validation_logits, validation_labels)
    best_iteration = 0
    best_trees = []
    stale = 0
    trees = []
    history = []
    for iteration in range(1, params['max_trees'] + 1):
        probability = sigmoid(logits)
        gradient = probability - y
        hessian = np.maximum(probability * (1. - probability), 1e-6)
        tree = _build_tree(codes, gradient, hessian, counts, params)
        update = params['learning_rate'] * _tree_values(tree, codes)
        validation_update = params['learning_rate'] * _tree_values(tree, validation_codes)
        logits += update
        validation_logits += validation_update
        trees.append(tree)
        validation_loss = _logloss_from_logits(validation_logits, validation_labels)
        improved = validation_loss < best_loss - 1e-6
        if improved:
            best_loss = validation_loss
            best_iteration = iteration
            best_trees = list(trees)
            stale = 0
        else:
            stale += 1
        history.append(dict(tree=iteration, train_logloss=_logloss_from_logits(logits, y),
                            validation_logloss=validation_loss, best_validation_logloss=best_loss,
                            improved=bool(improved)))
        if stale >= params['patience']:
            break
    return dict(schema_version=SCHEMA_VERSION, algorithm=ALGORITHM, seed=int(seed),
                feature_count=NUMERIC_DIM, base_score=base_score, bin_edges=edges,
                bin_counts=counts, params=params, trees=best_trees,
                best_iteration=best_iteration, validation_logloss=best_loss, history=history)


def fit(train, validation, seed=42, **params):
    """Fit from ShardDataset splits or (numeric_matrix, labels) tuples."""
    params = _check_params({**DEFAULT_PARAMS, **params})
    sample, _ = sample_train_rows(train, params['sample_limit'], seed)
    edges = compute_bin_edges(sample, params['max_bins'])
    codes, labels = _binned_train(train, edges)
    validation_x, validation_y = _validation_arrays(validation)
    return train_binned(codes, labels, binarize(validation_x, edges), validation_y, edges, params, seed)


def predict_logits(model, x):
    codes = binarize(x, model['bin_edges'])
    logits = np.full(codes.shape[1], float(model['base_score']), dtype=np.float64)
    for tree in model['trees']:
        logits += float(model['params']['learning_rate']) * _tree_values(tree, codes)
    return logits


def predict_proba(model, x):
    """Sigmoid probability in [0, 1], float64."""
    return sigmoid(predict_logits(model, x))


def save(model, path):
    """Write canonical JSON (no trailing newline); return its sha256."""
    payload = encode(model).encode('utf-8')
    digest = hashlib.sha256(payload).hexdigest()
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix('.tmp')
    with temporary.open('wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, destination)
    if sha(destination) != digest:
        raise ManifestError('GBDT checkpoint hash changed during write')
    return digest


def load_model(path, digest=None, directory=None):
    """Load a JSON checkpoint with the same containment/digest rules as state_dicts."""
    checkpoint = Path(path).resolve(strict=True)
    if directory is not None and not checkpoint.is_relative_to(Path(directory).resolve()):
        raise ManifestError('Untrusted GBDT checkpoint path')
    if digest is not None and sha(checkpoint) != digest:
        raise ManifestError('GBDT checkpoint changed')
    model = json.loads(checkpoint.read_text())
    if (model.get('schema_version') != SCHEMA_VERSION or model.get('algorithm') != ALGORITHM or
            len(model.get('bin_edges', [])) != NUMERIC_DIM or not isinstance(model.get('trees'), list)):
        raise ManifestError('Invalid GBDT checkpoint')
    return model

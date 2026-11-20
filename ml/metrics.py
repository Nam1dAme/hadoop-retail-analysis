"""Exact metrics with bounded external sorting, including ties across run boundaries."""
import heapq
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo
import numpy as np

SCORE_DTYPE = np.dtype([('score', '<f8'), ('label', 'i1'), ('cohort', 'u1'), ('sample_key', 'S64'), ('as_of', '<i8')])
IN_MEMORY_LIMIT = 2_000_000
TOP_K = ((.01, '1pct'), (.05, '5pct'), (.10, '10pct'))
_DAY_ZONE = ZoneInfo('Asia/Shanghai')


def _fast_ap_auc(p, y, positive, negative):
    order = np.argsort(-p, kind='stable')
    p_sorted = p[order]
    y_sorted = y[order].astype(np.int64)
    boundaries = np.r_[0, np.flatnonzero(p_sorted[1:] != p_sorted[:-1]) + 1]
    counts = np.diff(np.r_[boundaries, len(p_sorted)])
    pos = np.add.reduceat(y_sorted, boundaries)
    neg = counts - pos
    tp = np.cumsum(pos)
    fp = np.cumsum(neg)
    ap = float(np.sum((pos / positive) * (tp / (tp + fp)))) if positive else 0.
    auc_pairs = float(np.sum(pos * (negative - fp + .5 * neg)))
    return ap, auc_pairs


def descending_groups(scores, labels, directory, chunk_size=65536):
    """Sort fixed-size runs on disk, then merge score groups without Python row lists."""
    with tempfile.TemporaryDirectory(dir=directory, prefix='metric-sort-') as temp:
        runs = []
        for start in range(0, len(scores), chunk_size):
            p = np.asarray(scores[start:start + chunk_size])
            y = np.asarray(labels[start:start + chunk_size])
            order = np.argsort(-p, kind='stable')
            p, y = p[order], y[order]
            boundaries = np.r_[0, np.flatnonzero(p[1:] != p[:-1]) + 1]
            counts = np.diff(np.r_[boundaries, len(p)])
            grouped = np.empty(len(boundaries), dtype=[('p', '<f8'), ('positive', '<i8'), ('count', '<i8')])
            grouped['p'] = p[boundaries]
            grouped['positive'] = np.add.reduceat(y.astype(np.int64), boundaries)
            grouped['count'] = counts
            path = Path(temp) / f'{len(runs):05}.npy'
            np.save(path, grouped, allow_pickle=False)
            runs.append(np.load(path, mmap_mode='r', allow_pickle=False))
        heap = [(-float(r['p'][0]), i, 0) for i, r in enumerate(runs) if len(r)]
        heapq.heapify(heap)
        while heap:
            score = -heap[0][0]
            positive = count = 0
            while heap and -heap[0][0] == score:
                _, i, index = heapq.heappop(heap)
                row = runs[i][index]
                positive += int(row['positive'])
                count += int(row['count'])
                index += 1
                if index < len(runs[i]):
                    heapq.heappush(heap, (-float(runs[i]['p'][index]), i, index))
            yield score, positive, count - positive


def _ranked_rows(scores, labels, keys, directory, chunk_size):
    """Yield (score, label, sample_key) by descending score then ascending sample_key."""
    p, y = np.asarray(scores), np.asarray(labels)
    n = len(p)
    if n <= IN_MEMORY_LIMIT:
        values = np.arange(n) if keys is None else np.asarray(keys)
        order = np.lexsort((values, -p))
        for index in order:
            yield float(p[index]), int(y[index]), values[index]
        return
    with tempfile.TemporaryDirectory(dir=directory, prefix='topk-sort-') as temp:
        runs = []
        for start in range(0, n, chunk_size):
            chunk_p, chunk_y = np.asarray(p[start:start + chunk_size]), np.asarray(y[start:start + chunk_size])
            if keys is None:
                chunk_keys = np.char.mod('%016d', np.arange(start, start + len(chunk_p))).astype('S64')
            else:
                chunk_keys = np.asarray(keys[start:start + chunk_size]).astype('S64')
            order = np.lexsort((chunk_keys, -chunk_p))
            rows = np.empty(len(chunk_p), dtype=[('p', '<f8'), ('y', 'i1'), ('key', 'S64')])
            rows['p'], rows['y'], rows['key'] = chunk_p[order], chunk_y[order], chunk_keys[order]
            path = Path(temp) / f'{len(runs):05}.npy'
            np.save(path, rows, allow_pickle=False)
            runs.append(np.load(path, mmap_mode='r', allow_pickle=False))
        heap = [(-float(r['p'][0]), bytes(r['key'][0]), i, 0) for i, r in enumerate(runs) if len(r)]
        heapq.heapify(heap)
        while heap:
            _, key, i, index = heapq.heappop(heap)
            row = runs[i][index]
            yield float(row['p']), int(row['y']), key
            index += 1
            if index < len(runs[i]):
                heapq.heappush(heap, (-float(runs[i]['p'][index]), bytes(runs[i]['key'][index]), i, index))


def _top_k(scores, labels, keys, positive, directory, chunk_size):
    """Top-k precision/recall/lift with ceil(k*n) rows, ties broken by sample_key."""
    n = len(scores)
    result = {}
    if n == 0:
        for _, suffix in TOP_K:
            for name in ('precision', 'recall', 'lift'):
                result[f'{name}_at_{suffix}'] = None
        return result
    targets = {}
    for fraction, suffix in TOP_K:
        targets.setdefault(max(1, int(np.ceil(fraction * n))), []).append(suffix)
    ranked = _ranked_rows(scores, labels, keys, directory, chunk_size)
    tp = 0
    try:
        for index, (_, label, _) in enumerate(ranked, start=1):
            tp += int(label)
            if index in targets:
                precision = tp / index
                for suffix in targets[index]:
                    result[f'precision_at_{suffix}'] = precision
                    result[f'recall_at_{suffix}'] = tp / positive if positive else None
                    result[f'lift_at_{suffix}'] = precision / (positive / n) if positive else None
            if index >= max(targets):
                break
    finally:
        ranked.close()
    return result


def metrics(scores, labels, threshold=.5, directory=None, chunk_size=65536, sample_keys=None):
    p, y = np.asarray(scores), np.asarray(labels)
    if len(p) != len(y) or not np.isfinite(p).all() or np.any((p < 0) | (p > 1)) or not np.isin(y, [0, 1]).all():
        raise ValueError('Invalid observed predictions')
    if not 0 <= threshold <= 1:
        raise ValueError('Invalid threshold')
    n, positive = len(p), int(np.count_nonzero(y))
    negative = n - positive
    if n == 0:
        return dict(sample_count=0, positive_count=0, reason='EMPTY_COHORT', ap=None, roc_auc=None,
                    logloss=None, brier=None, precision=None, recall=None, f1=None, calibration=[],
                    **_top_k(p, y, sample_keys, positive, directory, chunk_size))
    if n <= IN_MEMORY_LIMIT:
        ap, auc_pairs = _fast_ap_auc(p, y, positive, negative)
    else:
        tp = fp = 0
        ap = auc_pairs = 0.
        for _, pos, neg in descending_groups(p, y, directory, chunk_size):
            tp += pos
            fp += neg
            if positive:
                ap += pos / positive * tp / (tp + fp)
            auc_pairs += pos * (negative - fp + .5 * neg)
    selected = p >= threshold
    tp = int(np.count_nonzero(selected & (y == 1)))
    fp = int(np.count_nonzero(selected & (y == 0)))
    fn, tn = positive - tp, negative - fp
    precision = tp / (tp + fp) if tp + fp else 0.
    recall = tp / positive if positive else None
    f1 = None if recall is None else 2 * precision * recall / (precision + recall) if precision + recall else 0.
    clipped = np.clip(p, 1e-7, 1 - 1e-7)
    buckets = np.minimum((p * 10).astype(np.int8), 9)
    calibration = []
    for bucket in range(10):
        mask = buckets == bucket
        count = int(mask.sum())
        calibration.append(dict(start=bucket / 10, end=(bucket + 1) / 10, count=count,
                                mean_score=float(p[mask].mean()) if count else None,
                                observed_rate=float(y[mask].mean()) if count else None))
    return dict(sample_count=n, positive_count=positive, reason=None if positive and negative else 'SINGLE_CLASS_COHORT',
                ap=ap if positive and negative else None,
                roc_auc=auc_pairs / (positive * negative) if positive and negative else None,
                logloss=float(np.mean(-y * np.log(clipped) - (1 - y) * np.log1p(-clipped))),
                brier=float(np.mean((p - y) ** 2)), precision=precision, recall=recall, f1=f1,
                tp=tp, fp=fp, fn=fn, tn=tn, threshold=threshold, calibration=calibration,
                **_top_k(p, y, sample_keys, positive, directory, chunk_size))


def choose_threshold(scores, labels):
    best = (-1., 0.)
    positive = int(np.count_nonzero(labels))
    for step in range(1, 200):
        threshold = step / 200
        mask = scores >= threshold
        tp = int(np.count_nonzero(mask & (labels == 1)))
        denominator = int(mask.sum()) + positive
        f1 = 2 * tp / denominator if denominator else 0.
        best = max(best, (f1, threshold))
    return best[1]


def _day_group(anchor):
    moment = datetime.fromtimestamp(int(anchor), timezone.utc).astimezone(_DAY_ZONE)
    return 'DAY:' + moment.strftime('%Y-%m-%d')


def evaluate_file(path, threshold=.5):
    data = np.load(path, mmap_mode='r', allow_pickle=False)
    if 'as_of' not in data.dtype.names:
        raise ValueError('Score file is missing as_of')
    result = {}
    for name, value in [('ALL', None), ('SEEN_TRAIN', 1), ('UNSEEN_TRAIN', 0)]:
        part = data if value is None else data[data['cohort'] == value]
        result[name] = metrics(part['score'], part['label'], threshold, Path(path).parent,
                               sample_keys=part['sample_key'])
    days = {}
    for anchor in np.unique(data['as_of']):
        days.setdefault(_day_group(anchor), []).append(data['as_of'] == anchor)
    for name, masks in days.items():
        part = data[masks[0] if len(masks) == 1 else np.logical_or.reduce(masks)]
        result[name] = metrics(part['score'], part['label'], threshold, Path(path).parent,
                               sample_keys=part['sample_key'])
    return result

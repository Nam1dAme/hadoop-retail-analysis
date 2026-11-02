"""Exact metrics with bounded external sorting, including ties across run boundaries."""
import heapq
from pathlib import Path
import tempfile
import numpy as np

SCORE_DTYPE = np.dtype([('score', '<f8'), ('label', 'i1'), ('cohort', 'u1'), ('sample_key', 'S64')])


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


def metrics(scores, labels, threshold=.5, directory=None, chunk_size=65536):
    p, y = np.asarray(scores), np.asarray(labels)
    if len(p) != len(y) or not np.isfinite(p).all() or np.any((p < 0) | (p > 1)) or not np.isin(y, [0, 1]).all():
        raise ValueError('Invalid observed predictions')
    if not 0 <= threshold <= 1:
        raise ValueError('Invalid threshold')
    n, positive = len(p), int(np.count_nonzero(y))
    negative = n - positive
    if n == 0:
        return dict(sample_count=0, positive_count=0, reason='EMPTY_COHORT', ap=None, roc_auc=None,
                    logloss=None, brier=None, precision=None, recall=None, f1=None, calibration=[])
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
                tp=tp, fp=fp, fn=fn, tn=tn, threshold=threshold, calibration=calibration)


def choose_threshold(scores, labels):
    best = (-1., 0.)
    positive = int(np.count_nonzero(labels))
    for step in range(1, 100):
        threshold = step / 100
        mask = scores >= threshold
        tp = int(np.count_nonzero(mask & (labels == 1)))
        denominator = int(mask.sum()) + positive
        f1 = 2 * tp / denominator if denominator else 0.
        best = max(best, (f1, threshold))
    return best[1]


def evaluate_file(path, threshold=.5):
    data = np.load(path, mmap_mode='r', allow_pickle=False)
    result = {}
    for name, value in [('ALL', None), ('SEEN_TRAIN', 1), ('UNSEEN_TRAIN', 0)]:
        part = data if value is None else data[data['cohort'] == value]
        result[name] = metrics(part['score'], part['label'], threshold, Path(path).parent)
    return result

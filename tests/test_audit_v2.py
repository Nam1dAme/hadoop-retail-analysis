"""Independent bitmap audit for the 8-anchor / two-TEST-day v2 split."""
import pytest

from orchestration import audit
from orchestration.common import ManifestError


class Storage:
    def __init__(self, lines):
        self.lines = lines

    def dump(self, paths):
        return iter(self.lines)


def mask(days):
    return sum(1 << day for day in days)


USERS = {'outputs': [{'path': '/stage/user_masks/part-0', 'key_class': 'text'}]}


def bitmap(user, active_days, buy_days):
    active = mask(active_days)
    return f'{user}\t{active}\t{active}\t{mask(buy_days)}'


def test_anchors_and_splits_are_v2():
    assert audit.ANCHORS == (2, 3, 4, 5, 6, 7, 8, 9)
    assert audit.SPLITS == ('TRAIN', 'TRAIN', 'TRAIN', 'TRAIN', 'VALIDATION', 'TEST', 'TEST', 'FORECAST')


def test_populations_audit_two_test_days_and_nine_bit_label():
    lines = [
        bitmap('1', range(9), {2, 6, 7, 8}),  # buys on 12-02 and 12-03 (bit 8)
        bitmap('2', {6, 7, 8}, set()),        # active only in the last three days
    ]
    pop = audit.populations(Storage(lines), USERS)
    samples = pop['samples']
    assert samples['TEST'] == dict(rows=4, positive_count=2, negative_count=2, seen=2, unseen=2)
    assert samples['FORECAST'] == dict(rows=2, positive_count=0, negative_count=0, seen=1, unseen=1)
    assert samples['VALIDATION']['rows'] == 1
    assert sum(samples['TRAIN'][key] for key in ('rows',)) == 4
    assert pop['users'] == 2 and pop['active'][7] == 2 and pop['active'][8] == 2

    no_day8_buy = [
        bitmap('1', range(9), {2, 6, 7}),
        bitmap('2', {6, 7, 8}, set()),
    ]
    changed = audit.populations(Storage(no_day8_buy), USERS)['samples']
    assert changed['TEST']['positive_count'] == 1
    assert changed['TEST']['negative_count'] == 3
    assert changed['FORECAST']['rows'] == 2


def test_populations_rejects_bitmaps_outside_nine_days():
    with pytest.raises(ManifestError):
        audit.populations(Storage([f'1\t512\t511\t{mask({8})}']), USERS)

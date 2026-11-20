from datetime import date

import pytest

from orchestration.common import ROOT, ConfigError, dataset_window, load


def test_dataset_window_is_derived_from_single_config_source():
    window = dataset_window(load(ROOT / 'config/development.toml'))
    assert window['start'] == date(2017, 11, 25)
    assert window['end'] == date(2017, 12, 3)  # 配置中的 end_date 是包含的最后一天
    assert window['exclusive_end'] == date(2017, 12, 4)
    assert window['days'] == 9
    assert window['dates'][0] == window['start']
    assert window['dates'][-1] == window['end']
    assert len(window['dates']) == 9


def test_dataset_window_rejects_windows_outside_fixed_split():
    config = dict(dataset=dict(start_date='2017-11-25', end_date='2017-12-04'))
    with pytest.raises(ConfigError):
        dataset_window(config)

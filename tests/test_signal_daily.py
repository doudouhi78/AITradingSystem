from __future__ import annotations

import json

import pandas as pd

from scripts.signal_daily import (
    PROMOTION_CRITERIA,
    default_strategy_configs_payload,
    initialize_strategy_configs_file,
    is_month_end,
    ensure_strategy_registry_metadata,
)



def test_is_month_end_true_for_last_trading_day() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-03-27',
        '2026-03-30',
        '2026-03-31',
        '2026-04-01',
    ])
    assert is_month_end('2026-03-31', trading_dates)



def test_is_month_end_false_before_last_trading_day() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-03-27',
        '2026-03-30',
        '2026-03-31',
        '2026-04-01',
    ])
    assert not is_month_end('2026-03-30', trading_dates)



def test_is_month_end_handles_short_month() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-02-25',
        '2026-02-26',
        '2026-02-27',
        '2026-03-02',
    ])
    assert is_month_end('2026-02-27', trading_dates)



def test_initialize_strategy_configs_file_creates_five_entries(tmp_path) -> None:
    config_path = tmp_path / 'strategy_configs.json'
    payload = initialize_strategy_configs_file(config_path)
    assert len(payload) == 5
    assert payload[0]['strategy_id'] == 'strat_breakout_v1'
    assert payload[0]['is_active'] is True
    persisted = json.loads(config_path.read_text(encoding='utf-8'))
    assert persisted == default_strategy_configs_payload()



def test_ensure_strategy_registry_metadata_adds_forward_fields(tmp_path) -> None:
    registry_path = tmp_path / 'strategy_registry.json'
    registry_path.write_text(json.dumps([
        {
            'strategy_id': 'strat_breakout_v1',
            'strategy_name': 'Breakout',
            'status': 'observation',
        }
    ], ensure_ascii=False, indent=2), encoding='utf-8')
    payload = ensure_strategy_registry_metadata(registry_path)
    assert payload[0]['days_in_forward_sim'] == 0
    assert payload[0]['forward_sharpe'] is None
    assert payload[0]['promotion_criteria'] == PROMOTION_CRITERIA

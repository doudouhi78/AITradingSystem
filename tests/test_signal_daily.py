from __future__ import annotations

import json

import pandas as pd

from scripts.signal_daily import (
    PROMOTION_CRITERIA,
    build_factor_gate_outputs,
    default_strategy_configs_payload,
    ensure_strategy_registry_metadata,
    initialize_strategy_configs_file,
    is_month_end,
    load_strategy_configs,
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


def test_build_factor_gate_outputs_runs_observation_mode(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / 'strategy_configs.json'
    config_path.write_text(json.dumps([
        {
            'strategy_id': 'strat_alpha004_v1',
            'strategy_name': 'Alpha004 Factor Strategy',
            'is_active': False,
            'status': 'observation',
            'factor_id': 'alpha004',
            'signal_type': 'factor_rank',
            'max_capital_pct': 0.25,
            'priority': 6,
            'rebalance_freq': 'monthly',
        }
    ], ensure_ascii=False, indent=2), encoding='utf-8')
    configs = load_strategy_configs(config_path)

    monkeypatch.setattr(
        'scripts.signal_daily.load_alpha004_snapshot',
        lambda lookback_rows=40, as_of_date=None: (
            pd.Series({'000001': 3.0, '600519': 2.0, '300750': 1.0, '000333': 0.5}),
            pd.Timestamp('2026-04-01'),
        ),
    )

    outputs = build_factor_gate_outputs(configs, gate_allowed=True, current_equity=100000.0)
    payload = outputs['strat_alpha004_v1']

    assert payload['is_active'] is False
    assert payload['gate_allowed'] is True
    assert payload['approved_capital_pct'] == 0.25
    assert payload['position_weights']
    assert payload['position_weights'][0]['account_weight'] > 0

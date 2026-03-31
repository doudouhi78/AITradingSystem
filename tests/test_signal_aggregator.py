from __future__ import annotations

from strategy_engine.signal_aggregator import aggregate_daily_signals
from strategy_engine.strategy_config import StrategyConfig


def test_aggregate_scales_when_total_capital_exceeds_limit() -> None:
    configs = [
        StrategyConfig(strategy_id='alpha', strategy_name='Alpha', is_active=True, max_capital_pct=0.50, priority=1),
        StrategyConfig(strategy_id='beta', strategy_name='Beta', is_active=True, max_capital_pct=0.50, priority=2),
    ]
    result = aggregate_daily_signals({'alpha': 1, 'beta': 1}, configs, gate_allowed=True, current_equity=100000)

    assert len(result) == 2
    assert result[0]['strategy_id'] == 'alpha'
    assert result[0]['approved_capital_pct'] == 0.4
    assert result[1]['approved_capital_pct'] == 0.4
    assert result[0]['reason'] == 'scaled_to_80pct_cap'


def test_gate_blocks_entries_but_keeps_exits() -> None:
    configs = [
        StrategyConfig(strategy_id='alpha', strategy_name='Alpha', is_active=True, max_capital_pct=0.30, priority=1),
        StrategyConfig(strategy_id='beta', strategy_name='Beta', is_active=True, max_capital_pct=0.30, priority=2),
    ]
    result = aggregate_daily_signals({'alpha': 1, 'beta': -1}, configs, gate_allowed=False, current_equity=50000)

    assert result[0]['action'] == 'blocked_entry'
    assert result[0]['blocked_by_gate'] is True
    assert result[1]['action'] == 'exit'
    assert result[1]['blocked_by_gate'] is False


def test_inactive_strategy_is_ignored() -> None:
    configs = [
        StrategyConfig(strategy_id='alpha', strategy_name='Alpha', is_active=False, max_capital_pct=0.50, priority=1),
        StrategyConfig(strategy_id='beta', strategy_name='Beta', is_active=True, max_capital_pct=0.30, priority=2),
    ]
    result = aggregate_daily_signals({'alpha': 1, 'beta': 1}, configs, gate_allowed=True, current_equity=50000)

    assert len(result) == 1
    assert result[0]['strategy_id'] == 'beta'
    assert result[0]['approved_capital_pct'] == 0.3

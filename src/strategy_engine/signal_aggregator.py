from __future__ import annotations

from typing import Any

from .strategy_config import StrategyConfig


def aggregate_daily_signals(
    strategy_signals: dict[str, int],
    strategy_configs: list[StrategyConfig],
    gate_allowed: bool,
    current_equity: float,
) -> list[dict[str, Any]]:
    if current_equity <= 0:
        raise ValueError('current_equity must be positive')

    configs = sorted(
        [config for config in strategy_configs if config.is_active],
        key=lambda item: (item.priority, item.strategy_id),
    )

    planned_entries: list[dict[str, Any]] = []
    execution_list: list[dict[str, Any]] = []
    for config in configs:
        signal = int(strategy_signals.get(config.strategy_id, 0) or 0)
        if signal == 0:
            continue
        if signal < 0:
            execution_list.append({
                'strategy_id': config.strategy_id,
                'strategy_name': config.strategy_name,
                'priority': config.priority,
                'signal': signal,
                'action': 'exit',
                'requested_capital_pct': 0.0,
                'approved_capital_pct': 0.0,
                'notional_capital': 0.0,
                'blocked_by_gate': False,
                'reason': 'exit signals are always allowed',
            })
            continue
        if not gate_allowed:
            execution_list.append({
                'strategy_id': config.strategy_id,
                'strategy_name': config.strategy_name,
                'priority': config.priority,
                'signal': signal,
                'action': 'blocked_entry',
                'requested_capital_pct': config.max_capital_pct,
                'approved_capital_pct': 0.0,
                'notional_capital': 0.0,
                'blocked_by_gate': True,
                'reason': 'gate blocked new entries',
            })
            continue
        planned_entries.append({
            'strategy_id': config.strategy_id,
            'strategy_name': config.strategy_name,
            'priority': config.priority,
            'signal': signal,
            'action': 'enter',
            'requested_capital_pct': float(config.max_capital_pct),
        })

    total_requested = sum(item['requested_capital_pct'] for item in planned_entries)
    scale = min(1.0, 0.80 / total_requested) if total_requested > 0 else 1.0
    for item in planned_entries:
        approved = round(item['requested_capital_pct'] * scale, 6)
        execution_list.append({
            **item,
            'approved_capital_pct': approved,
            'notional_capital': round(current_equity * approved, 2),
            'blocked_by_gate': False,
            'reason': 'scaled_to_80pct_cap' if scale < 1.0 else 'approved',
        })

    return sorted(execution_list, key=lambda row: (row['priority'], row['strategy_id']))

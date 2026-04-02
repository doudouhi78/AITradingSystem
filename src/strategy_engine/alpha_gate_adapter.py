from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _format_symbol(symbol: str) -> str:
    code = str(symbol).split('.')[0].zfill(6)
    if code.startswith(('600', '601', '603', '605', '688', '689', '900')):
        return f'{code}.SH'
    if code.startswith(('000', '001', '002', '003', '300', '301', '200')):
        return f'{code}.SZ'
    return code


def _cap_weights(weights: pd.Series, max_weight: float) -> pd.Series:
    if weights.empty:
        return weights
    if max_weight <= 0:
        raise ValueError('max_weight must be positive')
    result = weights.astype(float).copy()
    while True:
        over_mask = result > max_weight + 1e-12
        if not over_mask.any():
            break
        capped_total = float(over_mask.sum()) * max_weight
        uncapped = result.loc[~over_mask]
        residual = max(0.0, 1.0 - capped_total)
        result.loc[over_mask] = max_weight
        if uncapped.empty or residual <= 0:
            break
        uncapped_total = float(uncapped.sum())
        if uncapped_total <= 0:
            result.loc[~over_mask] = residual / len(uncapped)
        else:
            result.loc[~over_mask] = uncapped / uncapped_total * residual
    total = float(result.sum())
    return result / total if total > 0 else result


def adapt_factor_signal(factor_scores: pd.Series, gate_config: dict[str, Any]) -> dict[str, Any]:
    clean_scores = factor_scores.dropna().astype(float).sort_values(ascending=False)
    gate_action = str(gate_config.get('action', 'observe'))
    approved_capital_pct = float(gate_config.get('approved_capital_pct', 0.0) or 0.0)
    approved_notional = float(gate_config.get('notional_capital', 0.0) or 0.0)
    top_pct = float(gate_config.get('top_pct', 0.2) or 0.2)
    max_single_weight = float(gate_config.get('max_single_weight', 0.10) or 0.10)

    if clean_scores.empty:
        return {
            'gate_action': gate_action,
            'gate_allowed': False,
            'approved_capital_pct': approved_capital_pct,
            'notional_capital': approved_notional,
            'selection_count': 0,
            'position_weights': [],
            'reason': 'empty_factor_scores',
        }

    selection_count = max(1, int(math.ceil(len(clean_scores) * top_pct)))
    selected = clean_scores.head(selection_count)

    if approved_capital_pct <= 0 or gate_action == 'blocked_entry':
        return {
            'gate_action': gate_action,
            'gate_allowed': False,
            'approved_capital_pct': approved_capital_pct,
            'notional_capital': approved_notional,
            'selection_count': int(selection_count),
            'position_weights': [],
            'reason': 'gate_blocked_or_zero_capital',
        }

    shifted = selected - float(selected.min())
    if float(shifted.sum()) <= 0:
        raw = pd.Series(np.arange(len(selected), 0, -1, dtype=float), index=selected.index)
    else:
        raw = shifted + max(float(shifted.std(ddof=0)), 1e-6)
    strategy_weights = _cap_weights(raw / float(raw.sum()), max_weight=max_single_weight)

    positions = []
    for symbol, strategy_weight in strategy_weights.sort_values(ascending=False).items():
        account_weight = float(strategy_weight) * approved_capital_pct
        positions.append({
            'symbol': _format_symbol(str(symbol)),
            'raw_symbol': str(symbol).split('.')[0].zfill(6),
            'score': float(selected.loc[symbol]),
            'strategy_weight': round(float(strategy_weight), 6),
            'account_weight': round(account_weight, 6),
            'notional_capital': round(account_weight * approved_notional / approved_capital_pct, 2) if approved_capital_pct > 0 else 0.0,
        })

    return {
        'gate_action': gate_action,
        'gate_allowed': True,
        'approved_capital_pct': approved_capital_pct,
        'notional_capital': approved_notional,
        'selection_count': int(selection_count),
        'position_weights': positions,
        'reason': str(gate_config.get('reason', 'approved')),
    }

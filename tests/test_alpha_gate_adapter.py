from __future__ import annotations

import pandas as pd

from strategy_engine.alpha_gate_adapter import adapt_factor_signal


def test_adapt_factor_signal_scales_weights_by_score() -> None:
    scores = pd.Series({'000001': 3.0, '600519': 2.0, '300750': 1.0, '000333': 0.5})
    payload = adapt_factor_signal(
        scores,
        {
            'action': 'enter',
            'approved_capital_pct': 0.25,
            'notional_capital': 25000.0,
            'top_pct': 0.5,
            'max_single_weight': 0.7,
        },
    )

    assert payload['gate_allowed'] is True
    assert payload['selection_count'] == 2
    assert len(payload['position_weights']) == 2
    assert payload['position_weights'][0]['symbol'] == '000001.SZ'
    assert payload['position_weights'][0]['strategy_weight'] > payload['position_weights'][1]['strategy_weight']
    assert round(sum(item['strategy_weight'] for item in payload['position_weights']), 6) == 1.0
    assert round(sum(item['account_weight'] for item in payload['position_weights']), 6) == 0.25


def test_adapt_factor_signal_returns_empty_when_gate_blocks() -> None:
    scores = pd.Series({'000001': 1.0, '600519': 0.9, '300750': 0.8})
    payload = adapt_factor_signal(
        scores,
        {
            'action': 'blocked_entry',
            'approved_capital_pct': 0.0,
            'notional_capital': 0.0,
        },
    )

    assert payload['gate_allowed'] is False
    assert payload['position_weights'] == []
    assert payload['selection_count'] == 1

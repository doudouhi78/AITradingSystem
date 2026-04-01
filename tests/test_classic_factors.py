from __future__ import annotations

import pandas as pd

from alpha_research.factors import classic_factors


def _sample_factor_input() -> pd.DataFrame:
    dates = pd.date_range('2024-01-01', periods=300, freq='B')
    rows = []
    for asset_idx, asset in enumerate(['000001', '000002']):
        for i, date in enumerate(dates):
            rows.append({
                'date': date,
                'asset': asset,
                'close': 10 + asset_idx + i * 0.01,
                'high': 10.2 + asset_idx + i * 0.01,
                'low': 9.8 + asset_idx + i * 0.01,
                'amount': 1000000 + i * 10,
                'volume': 100000 + i,
            })
    frame = pd.DataFrame(rows).set_index(['date', 'asset'])
    return frame


def test_classic_factor_module_imports() -> None:
    assert hasattr(classic_factors, 'book_to_market')
    assert hasattr(classic_factors, 'beta_1y')


def test_price_only_classic_factors_return_series() -> None:
    factor_input = _sample_factor_input()
    momentum = classic_factors.momentum_1m(factor_input)
    low_vol = classic_factors.idiosyncratic_vol(factor_input)

    assert isinstance(momentum, pd.Series)
    assert isinstance(low_vol, pd.Series)
    assert momentum.name == 'momentum_1m'
    assert low_vol.name == 'idiosyncratic_vol'
    assert not momentum.empty
    assert not low_vol.empty

from __future__ import annotations

import pandas as pd


def factor_momentum_20d(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    momentum = prices.pct_change(20).shift(1)
    factor = momentum.stack(future_stack=True)
    factor.index = factor.index.set_names(["date", "asset"])
    factor.name = "momentum_20d"
    return factor.dropna()

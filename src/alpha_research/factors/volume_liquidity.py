from __future__ import annotations

import pandas as pd


def factor_turnover_20d(factor_input: pd.DataFrame) -> pd.Series:
    amount = factor_input["amount"].astype(float).unstack("asset")
    turnover_20d = amount.rolling(20).mean().shift(1)
    factor = turnover_20d.stack(future_stack=True)
    factor.index.names = ["date", "asset"]
    factor.name = "turnover_20d"
    return factor.dropna()

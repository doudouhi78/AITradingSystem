from __future__ import annotations

import pandas as pd
import pandas_ta as ta


def factor_rsi_14(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input["close"].astype(float).unstack("asset")
    result = {}
    for asset in close.columns:
        series = ta.rsi(close[asset], length=14)
        result[asset] = series.shift(1)
    factor = pd.DataFrame(result).stack(future_stack=True)
    factor.index.names = ["date", "asset"]
    factor.name = "rsi_14"
    return factor.dropna()


def factor_atr_14_normalized(factor_input: pd.DataFrame) -> pd.Series:
    high = factor_input["high"].astype(float).unstack("asset")
    low = factor_input["low"].astype(float).unstack("asset")
    close = factor_input["close"].astype(float).unstack("asset")
    result = {}
    for asset in close.columns:
        atr = ta.atr(high[asset], low[asset], close[asset], length=14)
        normalized = (atr / close[asset]).shift(1)
        result[asset] = normalized
    factor = pd.DataFrame(result).stack(future_stack=True)
    factor.index.names = ["date", "asset"]
    factor.name = "atr_14_normalized"
    return factor.dropna()

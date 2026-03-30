from __future__ import annotations

import numpy as np
import pandas as pd
import pandas_ta as ta


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    factor = frame.stack(future_stack=True)
    factor.index.names = ["date", "asset"]
    factor.name = name
    return factor.dropna()


def factor_rsi_14(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input["close"].astype(float).unstack("asset")
    result = {}
    for asset in close.columns:
        series = ta.rsi(close[asset], length=14)
        result[asset] = series.shift(1)
    return _stack_factor(pd.DataFrame(result), "rsi_14")


def factor_atr_14_normalized(factor_input: pd.DataFrame) -> pd.Series:
    high = factor_input["high"].astype(float).unstack("asset")
    low = factor_input["low"].astype(float).unstack("asset")
    close = factor_input["close"].astype(float).unstack("asset")
    result = {}
    for asset in close.columns:
        atr = ta.atr(high[asset], low[asset], close[asset], length=14)
        normalized = (atr / close[asset]).shift(1)
        result[asset] = normalized
    return _stack_factor(pd.DataFrame(result), "atr_14_normalized")


def factor_macd_signal(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input['close'].astype(float).unstack('asset')
    result = {}
    for asset in close.columns:
        macd = ta.macd(close[asset])
        if macd is None or macd.empty:
            continue
        result[asset] = macd.iloc[:, 0].sub(macd.iloc[:, 1]).shift(1)
    return _stack_factor(pd.DataFrame(result), 'macd_signal')


def factor_bbands_position(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input['close'].astype(float).unstack('asset')
    result = {}
    for asset in close.columns:
        bbands = ta.bbands(close[asset], length=20, std=2)
        if bbands is None or bbands.empty:
            continue
        lower = bbands.iloc[:, 0]
        upper = bbands.iloc[:, 2]
        position = ((close[asset] - lower) / (upper - lower + 1e-6)).shift(1)
        result[asset] = position
    return _stack_factor(pd.DataFrame(result), 'bbands_position')


def factor_momentum_quality(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input['close'].astype(float).unstack('asset')
    momentum = close.pct_change(20, fill_method=None)
    volatility = close.pct_change(fill_method=None).rolling(20).std()
    quality = (momentum / (volatility + 1e-6)).replace([np.inf, -np.inf], np.nan).shift(1)
    return _stack_factor(quality, 'momentum_quality')

from __future__ import annotations

import numpy as np
import pandas as pd


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    factor = frame.stack(future_stack=True)
    factor.index.names = ["date", "asset"]
    factor.name = name
    return factor.dropna()


def factor_turnover_20d(factor_input: pd.DataFrame) -> pd.Series:
    amount = factor_input["amount"].astype(float).unstack("asset")
    turnover_20d = amount.rolling(20).mean().shift(1)
    return _stack_factor(turnover_20d, "turnover_20d")


def factor_volume_price_divergence(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input["close"].astype(float).unstack("asset")
    amount = factor_input["amount"].astype(float).unstack("asset")
    price_change = close.pct_change(20, fill_method=None)
    amount_change = amount.pct_change(20, fill_method=None)
    divergence = (price_change / (amount_change + 1e-6)).replace([np.inf, -np.inf], np.nan).shift(1)
    return _stack_factor(divergence, "volume_price_divergence")


def factor_turnover_acceleration(factor_input: pd.DataFrame) -> pd.Series:
    amount = factor_input["amount"].astype(float).unstack("asset")
    fast = amount.rolling(5).mean()
    slow = amount.rolling(20).mean()
    acceleration = (fast / (slow + 1e-6)).replace([np.inf, -np.inf], np.nan).shift(1)
    return _stack_factor(acceleration, "turnover_acceleration")


def factor_volatility_20d(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input["close"].astype(float).unstack("asset")
    volatility = close.pct_change(fill_method=None).rolling(20).std().shift(1)
    return _stack_factor(volatility, "volatility_20d")

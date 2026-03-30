from __future__ import annotations

import pandas as pd


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    factor = frame.stack(future_stack=True)
    factor.index = factor.index.set_names(["date", "asset"])
    factor.name = name
    return factor.dropna()


def factor_momentum_5d(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    momentum = prices.pct_change(5, fill_method=None).shift(1)
    return _stack_factor(momentum, "momentum_5d")


def factor_momentum_10d(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    momentum = prices.pct_change(10, fill_method=None).shift(1)
    return _stack_factor(momentum, "momentum_10d")


def factor_momentum_20d(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    momentum = prices.pct_change(20, fill_method=None).shift(1)
    return _stack_factor(momentum, "momentum_20d")


def factor_momentum_60d(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    momentum = prices.pct_change(60, fill_method=None).shift(1)
    return _stack_factor(momentum, "momentum_60d")


def factor_momentum_1d_reversal(prices: pd.DataFrame) -> pd.Series:
    prices = prices.sort_index()
    reversal = -prices.pct_change(1, fill_method=None).shift(1)
    return _stack_factor(reversal, "momentum_1d_reversal")

from __future__ import annotations

import math

import pandas as pd

from ai_dev_os.risk.risk_config import ATRConfig


def wilder_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    config: ATRConfig = ATRConfig(),
) -> pd.Series:
    high = high.astype(float)
    low = low.astype(float)
    close = close.astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / config.period, adjust=False, min_periods=config.period).mean()
    return atr


def compute_stop_price(
    entry_price: float,
    atr_value: float,
    config: ATRConfig = ATRConfig(),
) -> float:
    return entry_price - atr_value * config.multiplier


def _self_test() -> bool:
    cfg = ATRConfig(period=3, multiplier=2.0)
    high = pd.Series([10.0, 11.0, 12.0, 11.5, 12.5])
    low = pd.Series([9.0, 9.5, 10.5, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.0, 10.2, 12.0])
    atr = wilder_atr(high, low, close, cfg)
    expected = 1.6679012345679012
    return bool(not math.isnan(float(atr.iloc[-1])) and abs(float(atr.iloc[-1]) - expected) < 0.001)


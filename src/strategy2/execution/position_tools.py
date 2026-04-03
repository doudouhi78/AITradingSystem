from __future__ import annotations

import math

import pandas as pd


REQUIRED_OHLC = ("high", "low", "close")


def _coerce_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    missing = [col for col in REQUIRED_OHLC if col not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    for col in REQUIRED_OHLC:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def calc_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    if window <= 0:
        raise ValueError("window must be positive")
    frame = _coerce_price_frame(df)
    prev_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = pd.Series(index=frame.index, dtype=float, name=f"atr_{window}")
    if len(true_range.dropna()) < window:
        return atr
    seed = float(true_range.iloc[:window].mean())
    atr.iloc[window - 1] = seed
    alpha = 1.0 / float(window)
    for idx in range(window, len(true_range)):
        prev_atr = atr.iloc[idx - 1]
        tr = true_range.iloc[idx]
        if pd.isna(prev_atr) or pd.isna(tr):
            atr.iloc[idx] = prev_atr
        else:
            atr.iloc[idx] = prev_atr + alpha * (tr - prev_atr)
    return atr


def calc_chandelier_exit(df: pd.DataFrame, atr_window: int = 14, k: float = 3.0) -> pd.Series:
    frame = _coerce_price_frame(df)
    highest_high = frame["high"].rolling(22, min_periods=22).max()
    atr = calc_atr(frame, window=atr_window)
    raw_stop = highest_high - float(k) * atr
    stop = raw_stop.copy()
    last = float("nan")
    for idx, value in stop.items():
        if pd.isna(value):
            continue
        if pd.isna(last):
            last = float(value)
        else:
            last = max(last, float(value))
        stop.loc[idx] = last
    stop.name = "chandelier_exit"
    return stop


def calc_atr_position_size(total_capital: float, risk_ratio: float, atr: float, k: float = 3.0) -> int:
    if total_capital <= 0 or risk_ratio <= 0 or atr <= 0 or k <= 0:
        return 0
    risk_per_lot = float(k) * float(atr) * 100.0
    if risk_per_lot <= 0:
        return 0
    risk_based = math.floor((float(total_capital) * float(risk_ratio)) / risk_per_lot)
    cap_based = math.floor((float(total_capital) * 0.25) / risk_per_lot)
    return max(min(risk_based, cap_based), 0)

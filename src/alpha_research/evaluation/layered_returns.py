from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _annualize(mean_return: float, period: int) -> float:
    if pd.isna(mean_return):
        return 0.0
    base = 1.0 + float(mean_return)
    if base <= 0:
        return -1.0
    return float(base ** (252 / period) - 1)


def _series_sharpe(series: pd.Series, period: int) -> float:
    series = pd.to_numeric(series, errors='coerce').dropna()
    if series.empty:
        return 0.0
    std = float(series.std(ddof=0))
    if std == 0:
        return 0.0
    return float(series.mean() / std * math.sqrt(252 / period))


def _max_drawdown(series: pd.Series) -> float:
    series = pd.to_numeric(series, errors='coerce').fillna(0.0)
    if series.empty:
        return 0.0
    equity = (1.0 + series).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    return float(drawdown.min())


def compute_layered_returns(factor_frame: pd.DataFrame, prices: pd.DataFrame, period: int = 10, quantiles: int = 5) -> dict[str, Any]:
    forward_returns = prices.pct_change(period, fill_method=None).shift(-period)
    benchmark = forward_returns.mean(axis=1)
    group_series: dict[int, list[float]] = {q: [] for q in range(1, quantiles + 1)}
    long_short: list[float] = []

    aligned_factor, aligned_returns = factor_frame.align(forward_returns, join='inner', axis=0)
    for date in aligned_factor.index:
        row_factor = pd.to_numeric(aligned_factor.loc[date], errors='coerce')
        row_return = pd.to_numeric(aligned_returns.loc[date], errors='coerce')
        valid = row_factor.notna() & row_return.notna()
        if valid.sum() < quantiles * 5:
            continue
        ranked = row_factor[valid].rank(method='first')
        buckets = pd.qcut(ranked, quantiles, labels=False) + 1
        excess = row_return[valid] - float(benchmark.loc[date])
        bucket_means: dict[int, float] = {}
        for q in range(1, quantiles + 1):
            mask = buckets == q
            if mask.sum() == 0:
                continue
            mean_ret = float(excess[mask].mean())
            group_series[q].append(mean_ret)
            bucket_means[q] = mean_ret
        if 1 in bucket_means and quantiles in bucket_means:
            long_short.append(bucket_means[quantiles] - bucket_means[1])

    annuals = {f'q{q}_annual_excess': _annualize(np.mean(vals) if vals else 0.0, period) for q, vals in group_series.items()}
    ordered = [annuals[f'q{q}_annual_excess'] for q in range(1, quantiles + 1)]
    is_monotonic = all(x <= y for x, y in zip(ordered, ordered[1:]))
    long_short_series = pd.Series(long_short, dtype=float)
    return {
        **annuals,
        'long_short_annual_return': _annualize(float(long_short_series.mean()) if not long_short_series.empty else 0.0, period),
        'long_short_sharpe': _series_sharpe(long_short_series, period),
        'long_short_max_drawdown': _max_drawdown(long_short_series),
        'is_monotonic': bool(is_monotonic),
    }

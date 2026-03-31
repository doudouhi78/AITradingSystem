from __future__ import annotations

import pandas as pd

from attribution.strategy_attribution import compute_rolling_alpha, load_returns_series, run_strategy_attribution


def test_load_returns_series_returns_aligned_series() -> None:
    strategy, benchmark = load_returns_series("exp-20260329-008-parquet-entry25-exit20")
    assert isinstance(strategy, pd.Series)
    assert isinstance(benchmark, pd.Series)
    assert len(strategy) == len(benchmark)
    assert strategy.index.equals(benchmark.index)


def test_strategy_attribution_output_format() -> None:
    idx = pd.date_range("2026-01-01", periods=70, freq="B")
    strategy = pd.Series(0.001, index=idx)
    benchmark = pd.Series(0.0006, index=idx)
    payload = run_strategy_attribution(strategy, benchmark)
    assert {"annual_return", "sharpe", "max_drawdown", "alpha", "beta", "benchmark_annual_return", "excess_return"} <= set(payload.keys())
    rolling = compute_rolling_alpha(strategy, benchmark, window_days=21)
    assert isinstance(rolling, pd.Series)

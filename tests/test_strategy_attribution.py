from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import attribution.strategy_attribution as strategy_module
from attribution.strategy_attribution import compute_rolling_alpha, load_returns_series, run_strategy_attribution

ROOT = Path(__file__).resolve().parents[1]


def test_load_returns_series_returns_aligned_series() -> None:
    strategy, benchmark = load_returns_series('exp-20260329-008-parquet-entry25-exit20')
    assert isinstance(strategy, pd.Series)
    assert isinstance(benchmark, pd.Series)
    assert len(strategy) == len(benchmark)
    assert strategy.index.equals(benchmark.index)


def test_strategy_attribution_output_format(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(strategy_module, 'OUTPUT_DIR', tmp_path)
    idx = pd.date_range('2026-01-01', periods=70, freq='B')
    strategy = pd.Series(0.001, index=idx)
    benchmark = pd.Series(0.0006, index=idx)
    payload = run_strategy_attribution(strategy, benchmark)
    assert {'annual_return', 'sharpe', 'max_drawdown', 'alpha', 'beta', 'benchmark_annual_return', 'excess_return', 'note'} <= set(payload.keys())
    rolling = compute_rolling_alpha(strategy, benchmark, window_days=21)
    assert isinstance(rolling, pd.Series)


def test_strategy_attribution_json_has_no_nan_literals(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(strategy_module, 'OUTPUT_DIR', tmp_path)
    idx = pd.date_range('2026-01-01', periods=70, freq='B')
    strategy = pd.Series(0.001, index=idx)
    benchmark = pd.Series(0.0006, index=idx)
    run_strategy_attribution(strategy, benchmark)
    output_path = tmp_path / 'strategy_attribution.json'
    content = output_path.read_text(encoding='utf-8')
    assert 'NaN' not in content
    payload = json.loads(content)
    assert 'note' in payload

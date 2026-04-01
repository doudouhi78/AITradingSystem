from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

from alpha_research.gpu_ic_calculator import GPUIcCalculator

try:
    import torch
except Exception:  # pragma: no cover
    torch = None  # type: ignore[assignment]


def _make_inputs(n_dates: int, n_symbols: int, n_factors: int, seed: int = 7) -> tuple[pd.DataFrame, pd.Series]:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2021-01-01", periods=n_dates, freq="B")
    symbols = [f"S{i:04d}" for i in range(n_symbols)]
    index = pd.MultiIndex.from_product([dates, symbols], names=["date", "symbol"])
    factor_values = rng.standard_normal((len(index), n_factors)).astype(np.float32)
    returns = rng.standard_normal(len(index)).astype(np.float32)

    factor_frame = pd.DataFrame(factor_values, index=index, columns=[f"factor_{i:03d}" for i in range(n_factors)])
    forward_returns = pd.Series(returns, index=index, name="forward_return")

    nan_mask = rng.random(factor_values.shape) < 0.03
    factor_frame = factor_frame.mask(nan_mask)
    return_mask = rng.random(len(index)) < 0.02
    forward_returns = forward_returns.mask(return_mask)
    return factor_frame, forward_returns


def _pandas_spearman_ic(factor_matrix: pd.DataFrame, forward_returns: pd.Series) -> pd.DataFrame:
    rows: list[dict[str, float]] = []
    for date, factor_slice in factor_matrix.groupby(level="date"):
        returns_slice = forward_returns.xs(date, level="date")
        row = {
            factor_name: float(factor_slice[factor_name].corr(returns_slice, method="spearman"))
            for factor_name in factor_matrix.columns
        }
        row["date"] = date
        rows.append(row)
    return pd.DataFrame(rows).set_index("date")[factor_matrix.columns]


def test_gpu_ic_matches_pandas_for_spearman() -> None:
    factor_matrix, forward_returns = _make_inputs(n_dates=12, n_symbols=40, n_factors=5)
    calculator = GPUIcCalculator(device="cuda" if torch is not None and torch.cuda.is_available() else "cpu", factor_chunk_size=4)

    actual = calculator.batch_compute_ic(factor_matrix, forward_returns, method="spearman")
    expected = _pandas_spearman_ic(factor_matrix, forward_returns)

    max_error = float(np.nanmax(np.abs(actual.to_numpy() - expected.to_numpy())))
    assert max_error < 1e-4

    icir = calculator.compute_icir(actual)
    expected_icir = expected.mean(axis=0).divide(expected.std(axis=0, ddof=0).replace(0.0, np.nan))
    pd.testing.assert_series_equal(icir, expected_icir, check_names=False, atol=1e-6, rtol=1e-6)


@pytest.mark.skipif(torch is None or not torch.cuda.is_available(), reason="requires CUDA")
def test_gpu_speed_benchmark(capfd: pytest.CaptureFixture[str]) -> None:
    factor_matrix, forward_returns = _make_inputs(n_dates=2500, n_symbols=1000, n_factors=80, seed=11)

    cpu_calculator = GPUIcCalculator(device="cpu", factor_chunk_size=8)
    gpu_calculator = GPUIcCalculator(device="cuda", factor_chunk_size=8)

    started = time.perf_counter()
    cpu_result = cpu_calculator.batch_compute_ic(factor_matrix, forward_returns, method="spearman")
    cpu_seconds = time.perf_counter() - started

    if torch is not None:
        torch.cuda.synchronize()
    started = time.perf_counter()
    gpu_result = gpu_calculator.batch_compute_ic(factor_matrix, forward_returns, method="spearman")
    if torch is not None:
        torch.cuda.synchronize()
    gpu_seconds = time.perf_counter() - started

    max_error = float(np.nanmax(np.abs(cpu_result.to_numpy() - gpu_result.to_numpy())))
    speedup = cpu_seconds / gpu_seconds if gpu_seconds > 0 else float("inf")
    print(f"GPU IC benchmark: CPU={cpu_seconds:.2f}s GPU={gpu_seconds:.2f}s speedup={speedup:.2f}x max_error={max_error:.6f}")

    captured = capfd.readouterr().out
    assert "GPU IC benchmark" in captured
    assert max_error < 1e-4


def test_gpu_ic_handles_edge_cases() -> None:
    dates = pd.date_range("2024-01-01", periods=2, freq="B")
    index = pd.MultiIndex.from_product([dates, ["AAA", "BBB"]], names=["date", "symbol"])
    factor_matrix = pd.DataFrame(
        {
            "all_nan": [np.nan, np.nan, np.nan, np.nan],
            "tiny": [1.0, 2.0, np.nan, np.nan],
        },
        index=index,
    )
    forward_returns = pd.Series([0.1, 0.2, 0.3, np.nan], index=index)

    calculator = GPUIcCalculator(device="cpu")
    actual = calculator.batch_compute_ic(factor_matrix, forward_returns, method="spearman")

    assert actual.loc[dates[0], "all_nan"] != actual.loc[dates[0], "all_nan"]
    assert actual.loc[dates[1], "all_nan"] != actual.loc[dates[1], "all_nan"]
    assert actual.loc[dates[1], "tiny"] != actual.loc[dates[1], "tiny"]
    assert float(calculator.compute_icir(actual).loc["all_nan"]) != float(calculator.compute_icir(actual).loc["all_nan"])

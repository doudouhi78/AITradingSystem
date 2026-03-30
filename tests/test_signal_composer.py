from __future__ import annotations

import pandas as pd

from alpha_research.signal_composer import compose_signal
from alpha_research.signal_composer import compute_daily_spearman_ic
from alpha_research.signal_composer import compute_forward_returns
from alpha_research.signal_composer import generate_top_n_signal


def _build_series(values: dict[tuple[str, str], float], name: str) -> pd.Series:
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp(date), asset) for date, asset in values],
        names=["date", "asset"],
    )
    return pd.Series(list(values.values()), index=index, name=name, dtype=float)


def test_compose_signal_returns_multiindex_series() -> None:
    factor_a = _build_series({
        ("2024-01-01", "AAA"): 1.0,
        ("2024-01-01", "BBB"): 2.0,
        ("2024-01-02", "AAA"): 3.0,
        ("2024-01-02", "BBB"): 1.0,
    }, "factor_a")
    factor_b = _build_series({
        ("2024-01-01", "AAA"): 2.0,
        ("2024-01-01", "BBB"): 1.0,
        ("2024-01-02", "AAA"): 1.0,
        ("2024-01-02", "BBB"): 3.0,
    }, "factor_b")

    composite = compose_signal({"factor_a": factor_a, "factor_b": factor_b}, {"factor_a": 0.7, "factor_b": 0.3})

    assert list(composite.index.names) == ["date", "asset"]
    assert composite.name == "composite_score"
    assert composite.loc[(pd.Timestamp("2024-01-01"), "BBB")] > composite.loc[(pd.Timestamp("2024-01-01"), "AAA")]


def test_generate_top_n_signal_marks_top_assets() -> None:
    composite = _build_series({
        ("2024-01-01", "AAA"): 0.9,
        ("2024-01-01", "BBB"): 0.8,
        ("2024-01-01", "CCC"): 0.1,
        ("2024-01-02", "AAA"): 0.2,
        ("2024-01-02", "BBB"): 0.7,
        ("2024-01-02", "CCC"): 0.6,
    }, "composite_score")

    signal = generate_top_n_signal(composite, top_pct=1/3)

    assert signal.loc[(pd.Timestamp("2024-01-01"), "AAA")] == 1
    assert signal.loc[(pd.Timestamp("2024-01-01"), "BBB")] == 0
    assert signal.loc[(pd.Timestamp("2024-01-02"), "BBB")] == 1


def test_compute_daily_spearman_ic_is_positive_for_aligned_signal() -> None:
    prices = pd.DataFrame(
        {
            "AAA": [10.0, 11.0, 12.0],
            "BBB": [10.0, 10.5, 10.6],
        },
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    )
    signal = _build_series({
        ("2024-01-01", "AAA"): 0.9,
        ("2024-01-01", "BBB"): 0.1,
        ("2024-01-02", "AAA"): 0.8,
        ("2024-01-02", "BBB"): 0.2,
    }, "signal")

    forward_returns = compute_forward_returns(prices, horizon=1)
    daily_ic = compute_daily_spearman_ic(signal, forward_returns)

    assert not daily_ic.empty
    assert (daily_ic > 0).all()

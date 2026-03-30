from __future__ import annotations

import math
from typing import Mapping

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


MultiFactorMap = Mapping[str, pd.Series]
WeightMap = Mapping[str, float]


def _validate_multiindex_series(series: pd.Series, factor_name: str) -> pd.Series:
    if not isinstance(series, pd.Series):
        raise TypeError(f"{factor_name} must be a pandas Series")
    if not isinstance(series.index, pd.MultiIndex) or series.index.nlevels != 2:
        raise ValueError(f"{factor_name} must use a MultiIndex(date, asset)")
    normalized = series.dropna().copy()
    normalized.index = normalized.index.set_names(["date", "asset"])
    return normalized.sort_index()


def _normalize_weights(factor_names: list[str], weights: WeightMap) -> dict[str, float]:
    raw_weights = {name: float(weights.get(name, 0.0)) for name in factor_names}
    total = sum(abs(value) for value in raw_weights.values())
    if total <= 0:
        raise ValueError("weights must contain at least one non-zero value")
    return {name: value / total for name, value in raw_weights.items()}


def compose_signal(factor_dict: MultiFactorMap, weights: WeightMap) -> pd.Series:
    if not factor_dict:
        raise ValueError("factor_dict cannot be empty")
    factor_names = list(factor_dict.keys())
    normalized_weights = _normalize_weights(factor_names, weights)

    composite_frame: pd.DataFrame | None = None
    for factor_name in factor_names:
        factor_series = _validate_multiindex_series(factor_dict[factor_name], factor_name)
        factor_frame = factor_series.unstack("asset").sort_index()
        ranked = factor_frame.rank(axis=1, method="average", pct=True)
        weighted = ranked * normalized_weights[factor_name]
        if composite_frame is None:
            composite_frame = weighted
        else:
            composite_frame = composite_frame.add(weighted, fill_value=0.0)

    assert composite_frame is not None
    composite_series = composite_frame.stack(future_stack=True).dropna()
    composite_series.index = composite_series.index.set_names(["date", "asset"])
    composite_series.name = "composite_score"
    return composite_series.sort_index()


def generate_top_n_signal(composite_score: pd.Series, top_pct: float = 0.1) -> pd.Series:
    if top_pct <= 0 or top_pct > 1:
        raise ValueError("top_pct must be in (0, 1]")
    score = _validate_multiindex_series(composite_score, "composite_score")
    score_frame = score.unstack("asset").sort_index()
    ranked = score_frame.rank(axis=1, method="first", ascending=False, pct=True)
    signal_frame = (ranked <= top_pct).astype(int).where(~score_frame.isna())
    signal = signal_frame.stack(future_stack=True).dropna().astype(int)
    signal.index = signal.index.set_names(["date", "asset"])
    signal.name = "top_n_signal"
    return signal.sort_index()


def compute_forward_returns(prices: pd.DataFrame, horizon: int = 10) -> pd.Series:
    if prices.empty:
        return pd.Series(dtype=float, name=f"forward_return_{horizon}d")
    frame = prices.astype(float).sort_index()
    forward = frame.shift(-horizon).div(frame).sub(1.0)
    stacked = forward.stack(future_stack=True).dropna()
    stacked.index = stacked.index.set_names(["date", "asset"])
    stacked.name = f"forward_return_{horizon}d"
    return stacked.sort_index()


def compute_daily_spearman_ic(signal: pd.Series, forward_returns: pd.Series) -> pd.Series:
    lhs = _validate_multiindex_series(signal, "signal")
    rhs = _validate_multiindex_series(forward_returns, "forward_returns")
    common_index = lhs.index.intersection(rhs.index)
    if common_index.empty:
        return pd.Series(dtype=float, name="daily_spearman_ic")
    joined = pd.concat([
        lhs.loc[common_index].rename("signal"),
        rhs.loc[common_index].rename("forward_return"),
    ], axis=1).dropna()
    if joined.empty:
        return pd.Series(dtype=float, name="daily_spearman_ic")

    def _per_day_ic(frame: pd.DataFrame) -> float:
        if len(frame) < 2:
            return math.nan
        if frame["signal"].nunique() < 2 or frame["forward_return"].nunique() < 2:
            return math.nan
        corr, _ = spearmanr(frame["signal"], frame["forward_return"])
        return float(corr) if not np.isnan(corr) else math.nan

    daily_ic = joined.groupby(level="date", sort=True).apply(_per_day_ic)
    daily_ic.name = "daily_spearman_ic"
    return daily_ic.dropna()


def compute_mean_spearman_ic(signal: pd.Series, forward_returns: pd.Series) -> float:
    daily_ic = compute_daily_spearman_ic(signal, forward_returns)
    if daily_ic.empty:
        return float("nan")
    return float(daily_ic.mean())

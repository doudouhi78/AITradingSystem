from __future__ import annotations

from typing import Any

import alphalens as al
import pandas as pd
from joblib import Parallel, delayed

PERIODS = (1, 5, 10, 20)


def _normalize_period_key(period: Any) -> str:
    text = str(period)
    return text.split("D")[0]


def _compute_decay_halflife(ic_mean: dict[str, float]) -> int | None:
    ordered = sorted(((int(k), abs(v)) for k, v in ic_mean.items()), key=lambda x: x[0])
    if not ordered:
        return None
    max_ic = max(v for _, v in ordered)
    if max_ic <= 0:
        return None
    threshold = max_ic / 2.0
    for period, value in ordered:
        if value <= threshold:
            return period
    return ordered[-1][0]


def evaluate_single_factor(factor_series: pd.Series, prices: pd.DataFrame, factor_name: str) -> dict[str, Any]:
    try:
        clean = al.utils.get_clean_factor_and_forward_returns(
            factor=factor_series,
            prices=prices,
            periods=PERIODS,
            quantiles=5,
            max_loss=0.30,
        )
        ic = al.performance.factor_information_coefficient(clean)
        ic_mean = {_normalize_period_key(col): float(ic[col].mean()) for col in ic.columns}
        icir = {
            _normalize_period_key(col): (float(ic[col].mean() / ic[col].std(ddof=0)) if float(ic[col].std(ddof=0)) != 0 else 0.0)
            for col in ic.columns
        }
        return {
            'factor_name': factor_name,
            'ic_mean': ic_mean,
            'icir': icir,
            'decay_halflife': _compute_decay_halflife(ic_mean),
            'n_obs': int(len(clean)),
            'error': None,
        }
    except Exception as exc:
        return {
            'factor_name': factor_name,
            'ic_mean': {},
            'icir': {},
            'decay_halflife': None,
            'n_obs': 0,
            'error': repr(exc),
        }


def batch_evaluate(factor_dict: dict[str, pd.Series], prices: pd.DataFrame, n_jobs: int = 4) -> list[dict[str, Any]]:
    return Parallel(n_jobs=n_jobs)(
        delayed(evaluate_single_factor)(series, prices, name)
        for name, series in factor_dict.items()
    )

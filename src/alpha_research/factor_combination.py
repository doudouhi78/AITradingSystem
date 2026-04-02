from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
SELECTED_FACTOR_PATHS = [
    ROOT / 'runtime' / 'alpha_research' / 'selected_factors.json',
    ROOT / 'runtime' / 'factor_registry' / 'selected_factors.json',
]
FACTOR_REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'


def _ensure_non_empty(factors: dict[str, pd.DataFrame]) -> None:
    if not factors:
        raise ValueError('factors is empty')


def _align_factors(factors: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    _ensure_non_empty(factors)
    dates: pd.Index | None = None
    assets: pd.Index | None = None
    for frame in factors.values():
        if not isinstance(frame, pd.DataFrame):
            raise TypeError('all factor values must be pandas DataFrame')
        dates = frame.index if dates is None else dates.union(frame.index)
        assets = frame.columns if assets is None else assets.union(frame.columns)
    assert dates is not None and assets is not None
    return {name: frame.reindex(index=dates, columns=assets) for name, frame in factors.items()}


def _cross_section_zscore(frame: pd.DataFrame) -> pd.DataFrame:
    mean = frame.mean(axis=1, skipna=True)
    std = frame.std(axis=1, ddof=0, skipna=True).replace(0.0, np.nan)
    z = frame.sub(mean, axis=0).div(std, axis=0)
    return z.fillna(0.0)


def _cross_section_rank(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame.rank(axis=1, method='average', pct=True)
    centered = ranked.sub(0.5)
    return centered.fillna(0.0)


def _normalize_output(frame: pd.DataFrame) -> pd.DataFrame:
    return _cross_section_zscore(frame).fillna(0.0)


def equal_weight(factors: dict[str, pd.DataFrame]) -> pd.DataFrame:
    aligned = _align_factors(factors)
    normalized = [_cross_section_zscore(frame) for frame in aligned.values()]
    combined = sum(normalized) / len(normalized)
    return _normalize_output(combined)


def ic_weight(factors: dict[str, pd.DataFrame], ic_scores: dict[str, float]) -> pd.DataFrame:
    aligned = _align_factors(factors)
    weights = {name: float(ic_scores.get(name, 0.0)) for name in aligned}
    total = sum(abs(weight) for weight in weights.values())
    if total == 0:
        return equal_weight(aligned)
    combined = None
    for name, frame in aligned.items():
        scaled = _cross_section_zscore(frame) * (weights[name] / total)
        combined = scaled if combined is None else combined.add(scaled, fill_value=0.0)
    assert combined is not None
    return _normalize_output(combined)


def rank_weight(factors: dict[str, pd.DataFrame]) -> pd.DataFrame:
    aligned = _align_factors(factors)
    ranked = [_cross_section_rank(frame) for frame in aligned.values()]
    combined = sum(ranked) / len(ranked)
    return _normalize_output(combined)


def load_ic_scores(registry_path: Path = FACTOR_REGISTRY_PATH) -> dict[str, float]:
    if not registry_path.exists():
        return {}
    payload = json.loads(registry_path.read_text(encoding='utf-8'))
    return {
        item['factor_name']: float(item.get('icir', item.get('icir_neutralized', 0.0)) or 0.0)
        for item in payload
        if 'factor_name' in item
    }


def load_selected_factor_names(paths: Iterable[Path] = SELECTED_FACTOR_PATHS) -> list[str]:
    for path in paths:
        if path.exists():
            payload = json.loads(path.read_text(encoding='utf-8'))
            names = payload.get('selected') if isinstance(payload, dict) else payload
            if names:
                return [str(name) for name in names]
    raise FileNotFoundError('selected_factors.json not found in supported locations')


def _compute_factor_frame(factor_names: list[str], start: str = '2023-01-01', end: str = '2023-12-31', top_n: int = 50) -> dict[str, pd.DataFrame]:
    from alpha_research.data_loader import load_factor_input, select_top_n_by_liquidity
    from alpha_research.factors import alpha101, classic_factors

    instruments = select_top_n_by_liquidity('stock', start, end, top_n=top_n)
    factor_input = load_factor_input(instruments, start, end, asset_type='stock').copy()
    factor_input['open'] = factor_input['close']

    frames: dict[str, pd.DataFrame] = {}
    for name in factor_names:
        func = getattr(alpha101, name, None) or getattr(classic_factors, name, None)
        if func is None:
            continue
        series = func(factor_input)
        if isinstance(series, pd.Series):
            frames[name] = series.unstack('asset').sort_index().fillna(0.0)
    return frames


def main() -> None:
    factor_names = load_selected_factor_names()
    factors = _compute_factor_frame(factor_names)
    ic_scores = load_ic_scores()
    equal_scores = equal_weight(factors)
    ic_scores_frame = ic_weight(factors, ic_scores)
    rank_scores = rank_weight(factors)

    for name, frame in [
        ('equal_weight', equal_scores),
        ('ic_weight', ic_scores_frame),
        ('rank_weight', rank_scores),
    ]:
        preview = frame.iloc[:5, : min(5, frame.shape[1])]
        print(f'[{name}]')
        print(preview.to_string())
        print()


if __name__ == '__main__':
    main()

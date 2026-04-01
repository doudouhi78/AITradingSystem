from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity  # noqa: E402
from alpha_research.factors import alpha101, classic_factors  # noqa: E402

REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'
REPORT_DIR = ROOT / 'runtime' / 'alpha_research' / 'factor_reports'
CORR_PATH = ROOT / 'runtime' / 'alpha_research' / 'factor_correlation_matrix.csv'
SELECTED_PATH = ROOT / 'runtime' / 'factor_registry' / 'selected_factors.json'
START = '2016-01-01'
END = '2026-03-31'
TOP_N = 50
THRESHOLD = 0.7
MIN_ASSETS = 10


def load_registry() -> list[dict]:
    return json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))


def build_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    instruments = select_top_n_by_liquidity('stock', START, END, top_n=TOP_N)
    prices = load_prices(instruments, START, END, asset_type='stock')
    factor_input = load_factor_input(instruments, START, END, asset_type='stock').copy()
    factor_input['open'] = factor_input['close']
    factor_input.index = factor_input.index.set_names(['date', 'asset'])
    return prices, factor_input


def compute_factor_series(name: str, factor_input: pd.DataFrame) -> pd.Series:
    if name.startswith('alpha'):
        func = getattr(alpha101, name)
    else:
        func = getattr(classic_factors, name)
    series = func(factor_input)
    if not isinstance(series, pd.Series):
        raise TypeError(f'{name} did not return Series')
    return series.rename(name)


def build_factor_frame(factor_names: list[str], factor_input: pd.DataFrame) -> pd.DataFrame:
    series_list = [compute_factor_series(name, factor_input) for name in factor_names]
    frame = pd.concat(series_list, axis=1)
    frame.index = frame.index.set_names(['date', 'asset'])
    frame = frame.sort_index()
    return frame


def average_spearman_corr(factor_frame: pd.DataFrame) -> pd.DataFrame:
    names = list(factor_frame.columns)
    sums = pd.DataFrame(0.0, index=names, columns=names)
    counts = pd.DataFrame(0.0, index=names, columns=names)

    for _, block in factor_frame.groupby(level='date', sort=True):
        cross = block.droplevel('date')
        cross = cross.dropna(axis=1, how='all')
        if cross.shape[0] < MIN_ASSETS or cross.shape[1] < 2:
            continue
        corr = cross.corr(method='spearman')
        valid = corr.notna().astype(float)
        sums.loc[corr.index, corr.columns] += corr.fillna(0.0)
        counts.loc[corr.index, corr.columns] += valid

    avg = sums.divide(counts.where(counts != 0))
    for name in names:
        avg.loc[name, name] = 1.0
    return avg


def prune_factors(corr_matrix: pd.DataFrame, registry: list[dict]) -> tuple[list[str], list[dict], list[tuple[str, str, float]]]:
    icir_map = {item['factor_name']: float(item.get('icir', 0.0) or 0.0) for item in registry}
    pairs: list[tuple[str, str, float]] = []
    names = list(corr_matrix.index)
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            corr = corr_matrix.loc[left, right]
            if pd.notna(corr) and abs(float(corr)) > THRESHOLD:
                pairs.append((left, right, float(corr)))
    pairs.sort(key=lambda item: abs(item[2]), reverse=True)

    removed: dict[str, str] = {}
    for left, right, corr in pairs:
        if left in removed or right in removed:
            continue
        left_icir = icir_map.get(left, float('-inf'))
        right_icir = icir_map.get(right, float('-inf'))
        if left_icir >= right_icir:
            drop, keep, keep_icir, drop_icir = right, left, left_icir, right_icir
        else:
            drop, keep, keep_icir, drop_icir = left, right, right_icir, left_icir
        removed[drop] = f'corr={corr:.2f} with {keep}, lower ICIR ({drop_icir:.4f} < {keep_icir:.4f})'

    selected = [name for name in names if name not in removed]
    removed_items = [{'factor': name, 'reason': removed[name]} for name in names if name in removed]
    return selected, removed_items, pairs


def main() -> None:
    registry = load_registry()
    factor_names = [item['factor_name'] for item in registry]
    missing_reports = [name for name in factor_names if not (REPORT_DIR / f'{name}_report.json').exists()]

    _, factor_input = build_market_inputs()
    factor_frame = build_factor_frame(factor_names, factor_input)
    corr_matrix = average_spearman_corr(factor_frame)
    corr_matrix.to_csv(CORR_PATH, encoding='utf-8-sig')

    selected, removed, pairs = prune_factors(corr_matrix, registry)
    payload = {
        'selected': selected,
        'removed': removed,
        'total_selected': len(selected),
        'total_removed': len(removed),
        'missing_report_files': missing_reports,
    }
    SELECTED_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps({
        'factor_count': len(factor_names),
        'high_corr_pairs': len(pairs),
        'selected_count': len(selected),
        'removed_count': len(removed),
        'top3_pairs': pairs[:3],
        'missing_reports': missing_reports,
        'corr_path': str(CORR_PATH),
        'selected_path': str(SELECTED_PATH),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

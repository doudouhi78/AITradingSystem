from __future__ import annotations

import inspect
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from alpha_research.data_loader import load_factor_input, load_prices
from alpha_research.factors import fundamental, sentiment

ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'phase2' / 'ic_batch_result.json'
REGISTRY_PATH = ROOT / 'runtime' / 'alpha_research' / 'factor_registry.json'
LEGACY_REGISTRY_PATH = ROOT / 'src' / 'alpha_research' / 'registry' / 'factor_registry.json'
CSI300_PATH = ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'
TRAIN_START = '2020-01-01'
TRAIN_END = '2024-06-30'
TARGET_FACTOR_NAMES = [
    'factor_pb_ratio',
    'factor_pe_ttm',
    'factor_northbound_flow_5d',
    'factor_margin_balance_change_5d',
]
MODULES = [fundamental, sentiment]
UNIVERSE_NAME = 'stock_csi300_latest'
PERIODS = (1, 5, 10, 20)


def load_factor_functions() -> dict[str, callable]:
    factor_functions: dict[str, callable] = {}
    for module in MODULES:
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if name in TARGET_FACTOR_NAMES:
                factor_functions[name] = obj
    return dict(sorted(factor_functions.items()))


def load_csi300_instruments() -> list[str]:
    csi300 = pd.read_parquet(CSI300_PATH)
    codes = csi300['instrument_code'].astype(str).str.zfill(6).tolist()
    stock_dir = ROOT / 'runtime' / 'market_data' / 'cn_stock'
    return [code for code in codes if (stock_dir / f'{code}.parquet').exists()]


def build_factor_series(func, prices: pd.DataFrame, factor_input: pd.DataFrame, instruments: list[str], start: str, end: str):
    params = list(inspect.signature(func).parameters.keys())
    if params == ['prices']:
        return func(prices)
    if params == ['factor_input']:
        return func(factor_input)
    if params == ['instruments', 'start', 'end']:
        return func(instruments, start, end)
    raise TypeError(f'unsupported factor signature for {func.__name__}: {params}')


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


def evaluate_cross_sectional_factor(factor_series: pd.Series, prices: pd.DataFrame, factor_name: str) -> dict[str, Any]:
    factor_frame = factor_series.unstack('asset').sort_index()
    factor_frame = factor_frame.reindex(columns=prices.columns)
    ic_mean: dict[str, float] = {}
    icir: dict[str, float] = {}
    n_obs = 0
    for period in PERIODS:
        forward_returns = prices.pct_change(period, fill_method=None).shift(-period)
        aligned_factor, aligned_returns = factor_frame.align(forward_returns, join='inner', axis=0)
        daily_ic = []
        for date in aligned_factor.index:
            row_factor = aligned_factor.loc[date]
            row_return = aligned_returns.loc[date]
            valid = row_factor.notna() & row_return.notna()
            if valid.sum() < 20:
                continue
            corr = row_factor[valid].corr(row_return[valid], method='spearman')
            if pd.notna(corr):
                daily_ic.append(float(corr))
        series = pd.Series(daily_ic, dtype=float)
        if period == 10:
            n_obs = int(len(series))
        if series.empty:
            ic_mean[str(period)] = 0.0
            icir[str(period)] = 0.0
            continue
        mean = float(series.mean())
        std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
        ic_mean[str(period)] = mean
        icir[str(period)] = mean / std if std else mean
    return {
        'factor_name': factor_name,
        'ic_mean': ic_mean,
        'icir': icir,
        'decay_halflife': _compute_decay_halflife(ic_mean),
        'n_obs': n_obs,
        'error': None,
        'evaluation_type': 'cross_sectional',
    }


def evaluate_market_factor(factor_series: pd.Series, prices: pd.DataFrame, factor_name: str) -> dict[str, Any]:
    factor = pd.to_numeric(factor_series, errors='coerce').sort_index().rename('factor')
    ic_mean: dict[str, float] = {}
    icir: dict[str, float] = {}
    n_obs = 0
    for period in PERIODS:
        forward_return = prices.mean(axis=1).pct_change(period, fill_method=None).shift(-period).rename('forward_return')
        aligned = pd.concat([factor, forward_return], axis=1).dropna()
        rolling_corr = aligned['factor'].rolling(60, min_periods=20).corr(aligned['forward_return']).dropna()
        if period == 10:
            n_obs = int(len(rolling_corr))
        if rolling_corr.empty:
            ic_mean[str(period)] = 0.0
            icir[str(period)] = 0.0
            continue
        mean = float(rolling_corr.mean())
        std = float(rolling_corr.std(ddof=0)) if len(rolling_corr) > 1 else 0.0
        ic_mean[str(period)] = mean
        icir[str(period)] = mean / std if std else mean
    return {
        'factor_name': factor_name,
        'ic_mean': ic_mean,
        'icir': icir,
        'decay_halflife': _compute_decay_halflife(ic_mean),
        'n_obs': n_obs,
        'error': None,
        'evaluation_type': 'market_timing',
    }


def _status_from_icir(icir_10d: float) -> str:
    score = abs(float(icir_10d))
    if score >= 0.5:
        return 'active_candidate'
    if score >= 0.2:
        return 'weak'
    return 'failed'


def _load_existing_registry() -> list[dict[str, Any]]:
    if REGISTRY_PATH.exists():
        return json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    return []


def _upsert_registry(existing: list[dict[str, Any]], additions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {item.get('factor_name', item.get('factor_id')): item for item in existing}
    for item in additions:
        merged[item['factor_name']] = item
    ordered = list(merged.values())
    ordered.sort(key=lambda item: item.get('factor_name', item.get('factor_id', '')))
    return ordered


def _build_registry_entries(results: list[dict[str, Any]], universe: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in results:
        ic_mean_10d = float(item.get('ic_mean', {}).get('10', 0.0) or 0.0)
        icir_10d = float(item.get('icir', {}).get('10', 0.0) or 0.0)
        records.append({
            'factor_name': item['factor_name'],
            'status': _status_from_icir(icir_10d),
            'ic_mean_10d': ic_mean_10d,
            'icir_10d': icir_10d,
            'decay_halflife_days': item.get('decay_halflife'),
            'universe': universe,
            'registered_date': datetime.now().strftime('%Y-%m-%d'),
            'evaluation_type': item.get('evaluation_type', 'cross_sectional'),
            'notes': item.get('error') or 'Phase 8B newly registered factor',
        })
    return records


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    factor_functions = load_factor_functions()
    instruments = load_csi300_instruments()
    prices = load_prices(instruments, TRAIN_START, TRAIN_END, asset_type='stock')
    factor_input = load_factor_input(instruments, TRAIN_START, TRAIN_END, asset_type='stock')

    results: list[dict[str, Any]] = []
    for factor_name, func in factor_functions.items():
        try:
            series = build_factor_series(func, prices, factor_input, instruments, TRAIN_START, TRAIN_END)
            if not isinstance(series, pd.Series) or series.empty:
                results.append({
                    'factor_name': factor_name,
                    'ic_mean': {},
                    'icir': {},
                    'decay_halflife': None,
                    'n_obs': 0,
                    'error': 'empty_factor_series',
                })
                continue
            if isinstance(series.index, pd.MultiIndex):
                results.append(evaluate_cross_sectional_factor(series, prices, factor_name))
            else:
                results.append(evaluate_market_factor(series, prices, factor_name))
        except Exception as exc:
            results.append({
                'factor_name': factor_name,
                'ic_mean': {},
                'icir': {},
                'decay_halflife': None,
                'n_obs': 0,
                'error': repr(exc),
            })

    result_payload = json.loads(OUT_PATH.read_text(encoding='utf-8')) if OUT_PATH.exists() else {
        'start': TRAIN_START,
        'end': TRAIN_END,
        'candidate_factor_count': 0,
        'candidate_factors': [],
        'universes': {},
        'summary': {},
    }
    universe_payload = {
        'asset_type': 'stock',
        'instruments': instruments,
        'factor_count': len([r for r in results if not r.get('error')]),
        'results': results,
        'deduplicated_passed_factors': [],
    }
    result_payload['start'] = TRAIN_START
    result_payload['end'] = TRAIN_END
    result_payload['candidate_factors'] = sorted(set(result_payload.get('candidate_factors', []) + list(factor_functions.keys())))
    result_payload['candidate_factor_count'] = len(result_payload['candidate_factors'])
    result_payload.setdefault('universes', {})[UNIVERSE_NAME] = universe_payload
    total_results = [r for u in result_payload['universes'].values() for r in u['results']]
    result_payload['summary'] = {
        'total_universes': len(result_payload['universes']),
        'total_factor_runs': len(total_results),
        'passed_count': sum(1 for r in total_results if r.get('passed')),
        'error_count': sum(1 for r in total_results if r.get('error')),
    }
    OUT_PATH.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2), encoding='utf-8')

    successful = [item for item in results if not item.get('error') and item.get('icir', {}).get('10') is not None]
    registry = _upsert_registry(_load_existing_registry(), _build_registry_entries(successful, UNIVERSE_NAME))
    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding='utf-8')
    LEGACY_REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps({
        'universe': UNIVERSE_NAME,
        'instrument_count': len(instruments),
        'evaluated_factor_count': len(results),
        'registered_total': len(registry),
        'new_factor_names': [item['factor_name'] for item in successful],
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

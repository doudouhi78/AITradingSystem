from __future__ import annotations

import argparse
import inspect
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity
from alpha_research.evaluation.layered_returns import compute_layered_returns
from alpha_research.factors import fundamental, sentiment, technical, price_momentum, volume_liquidity
from alpha_research.neutralization import neutralize_factor

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / 'runtime' / 'alpha_research' / 'factor_registry.json'
REPORT_DIR = ROOT / 'runtime' / 'alpha_research' / 'factor_reports'
INDUSTRY_PATH = ROOT / 'runtime' / 'classification_data' / 'industry_sw2.parquet'
TRAIN_START = '2020-01-01'
TRAIN_END = '2024-06-30'
PERIOD = 10
MODULES = [fundamental, sentiment, technical, price_momentum, volume_liquidity]


def load_factor_functions() -> dict[str, callable]:
    funcs: dict[str, callable] = {}
    for module in MODULES:
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if name.startswith('factor_'):
                funcs[name] = obj
    return funcs


def build_factor_series(func, prices: pd.DataFrame, factor_input: pd.DataFrame, instruments: list[str], start: str, end: str):
    params = list(inspect.signature(func).parameters.keys())
    if params == ['prices']:
        return func(prices)
    if params == ['factor_input']:
        return func(factor_input)
    if params == ['instruments', 'start', 'end']:
        return func(instruments, start, end)
    raise TypeError(f'unsupported factor signature for {func.__name__}: {params}')


def get_universe_info(universe_name: str) -> tuple[str, list[str]]:
    if 'etf' in universe_name:
        instruments = select_top_n_by_liquidity('etf', TRAIN_START, TRAIN_END, top_n=10)
        return 'etf', instruments
    csi300 = pd.read_parquet(ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet')
    codes = csi300['instrument_code'].astype(str).str.zfill(6).tolist()
    stock_dir = ROOT / 'runtime' / 'market_data' / 'cn_stock'
    instruments = [code for code in codes if (stock_dir / f'{code}.parquet').exists()]
    return 'stock', instruments


def compute_daily_ic_series(factor_frame: pd.DataFrame, prices: pd.DataFrame, period: int = PERIOD) -> pd.Series:
    forward_returns = prices.pct_change(period, fill_method=None).shift(-period)
    aligned_factor, aligned_returns = factor_frame.align(forward_returns, join='inner', axis=0)
    values = {}
    for date in aligned_factor.index:
        row_factor = pd.to_numeric(aligned_factor.loc[date], errors='coerce')
        row_return = pd.to_numeric(aligned_returns.loc[date], errors='coerce')
        valid = row_factor.notna() & row_return.notna()
        if valid.sum() < 20:
            continue
        corr = row_factor[valid].corr(row_return[valid], method='spearman')
        if pd.notna(corr):
            values[pd.Timestamp(date)] = float(corr)
    return pd.Series(values, dtype=float).sort_index()


def compute_market_ic_series(factor_series: pd.Series, prices: pd.DataFrame, period: int = PERIOD) -> pd.Series:
    forward_return = prices.mean(axis=1).pct_change(period, fill_method=None).shift(-period)
    aligned = pd.concat([pd.to_numeric(factor_series, errors='coerce').rename('factor'), forward_return.rename('fwd')], axis=1).dropna()
    return aligned['factor'].rolling(60, min_periods=20).corr(aligned['fwd']).dropna()


def compute_basic_metrics(ic_series: pd.Series) -> dict[str, float]:
    ic_series = pd.to_numeric(ic_series, errors='coerce').dropna()
    if ic_series.empty:
        return {'rank_ic_mean': 0.0, 'icir': 0.0, 'ic_positive_pct': 0.0}
    mean = float(ic_series.mean())
    std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
    return {
        'rank_ic_mean': mean,
        'icir': mean / std if std else mean,
        'ic_positive_pct': float((ic_series > 0).mean()),
    }


def build_size_proxy(prices: pd.DataFrame, factor_input: pd.DataFrame) -> pd.DataFrame:
    amount = factor_input['amount'].astype(float).unstack('asset').reindex_like(prices)
    close = prices.reindex_like(amount)
    proxy = close.mul(amount.where(amount.ne(0)), fill_value=float('nan')).astype(float)
    return proxy.apply(lambda row: (row - row.mean()) / (row.std(ddof=0) or 1.0), axis=1)


def load_industry_map(columns: list[str]) -> pd.Series:
    industry = pd.read_parquet(INDUSTRY_PATH, columns=['instrument_code', 'industry_name']).copy()
    industry['instrument_code'] = industry['instrument_code'].astype(str).str.zfill(6)
    return industry.drop_duplicates('instrument_code').set_index('instrument_code')['industry_name'].reindex(columns)


def compute_factor_correlations(target_name: str, target_series: pd.Series, registry: list[dict[str, Any]], factor_functions: dict[str, callable], asset_type: str, instruments: list[str], prices: pd.DataFrame, factor_input: pd.DataFrame) -> dict[str, float | None]:
    correlations: dict[str, float | None] = {}
    target_df = target_series.unstack('asset') if isinstance(target_series.index, pd.MultiIndex) else None
    for item in registry:
        other_name = item['factor_name']
        if other_name == target_name:
            correlations[other_name] = 1.0
            continue
        if other_name not in factor_functions:
            correlations[other_name] = None
            continue
        other_series = build_factor_series(factor_functions[other_name], prices, factor_input, instruments, TRAIN_START, TRAIN_END)
        if isinstance(target_series.index, pd.MultiIndex) != isinstance(other_series.index, pd.MultiIndex):
            correlations[other_name] = None
            continue
        if isinstance(other_series.index, pd.MultiIndex):
            other_df = other_series.unstack('asset')
            aligned_left, aligned_right = target_df.align(other_df, join='inner', axis=0)
            stacked = pd.concat([
                aligned_left.stack(future_stack=True).rename('left'),
                aligned_right.stack(future_stack=True).rename('right'),
            ], axis=1).dropna()
            correlations[other_name] = float(stacked['left'].corr(stacked['right'], method='spearman')) if not stacked.empty else None
        else:
            aligned = pd.concat([target_series.rename('left'), other_series.rename('right')], axis=1).dropna()
            correlations[other_name] = float(aligned['left'].corr(aligned['right'], method='spearman')) if not aligned.empty else None
    return correlations


def evaluate_factor(factor_name: str, registry_item: dict[str, Any], factor_functions: dict[str, callable]) -> dict[str, Any]:
    asset_type, instruments = get_universe_info(registry_item['universe'])
    prices = load_prices(instruments, TRAIN_START, TRAIN_END, asset_type=asset_type)
    factor_input = load_factor_input(instruments, TRAIN_START, TRAIN_END, asset_type=asset_type)
    factor_series = build_factor_series(factor_functions[factor_name], prices, factor_input, instruments, TRAIN_START, TRAIN_END)

    raw_ic_series = compute_daily_ic_series(factor_series.unstack('asset'), prices) if isinstance(factor_series.index, pd.MultiIndex) else compute_market_ic_series(factor_series, prices)
    basic_metrics = compute_basic_metrics(raw_ic_series)

    if isinstance(factor_series.index, pd.MultiIndex):
        factor_frame = factor_series.unstack('asset').reindex(columns=prices.columns)
        industry_map = load_industry_map(list(factor_frame.columns))
        size_proxy = build_size_proxy(prices, factor_input)
        industry_neutral = neutralize_factor(factor_frame, industry_map=industry_map, size_frame=None)
        size_neutral = neutralize_factor(factor_frame, industry_map=None, size_frame=size_proxy)
        industry_icir = compute_basic_metrics(compute_daily_ic_series(industry_neutral, prices))['icir']
        size_icir = compute_basic_metrics(compute_daily_ic_series(size_neutral, prices))['icir']
        layered = compute_layered_returns(factor_frame, prices, period=PERIOD)
    else:
        industry_icir = None
        size_icir = None
        layered = {
            'q1_annual_excess': 0.0,
            'q2_annual_excess': 0.0,
            'q3_annual_excess': 0.0,
            'q4_annual_excess': 0.0,
            'q5_annual_excess': 0.0,
            'long_short_annual_return': 0.0,
            'long_short_sharpe': 0.0,
            'long_short_max_drawdown': 0.0,
            'is_monotonic': False,
        }

    yearly_ic = {str(year): float(series.mean()) for year, series in raw_ic_series.groupby(raw_ic_series.index.year)} if not raw_ic_series.empty else {}
    registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    correlations = compute_factor_correlations(factor_name, factor_series, registry, factor_functions, asset_type, instruments, prices, factor_input)

    status = registry_item.get('status', 'failed')
    notes = registry_item.get('notes', '')
    return {
        'factor_name': factor_name,
        'evaluation_date': datetime.now().strftime('%Y-%m-%d'),
        'universe': registry_item['universe'],
        'sample_period': f'{TRAIN_START} ~ {TRAIN_END}',
        'basic_metrics': basic_metrics,
        'layered_returns': layered,
        'yearly_ic': yearly_ic,
        'neutralization_compare': {
            'raw_icir': basic_metrics['icir'],
            'industry_neutral_icir': industry_icir,
            'size_neutral_icir': size_icir,
        },
        'correlation_with_existing': correlations,
        'status': status,
        'notes': notes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--factor-name', type=str, default=None)
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    factor_functions = load_factor_functions()
    targets = [item for item in registry if args.factor_name is None or item['factor_name'] == args.factor_name]

    written = []
    for item in targets:
        factor_name = item['factor_name']
        if factor_name not in factor_functions:
            continue
        report = evaluate_factor(factor_name, item, factor_functions)
        path = REPORT_DIR / f'{factor_name}.json'
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        written.append(str(path))
    print(json.dumps({'report_count': len(written), 'reports': written}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()


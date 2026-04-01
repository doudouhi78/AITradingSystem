from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PRIMARY_ROOT = Path(r'D:\AITradingSystem')
INPUT_ROOT = PRIMARY_ROOT if (PRIMARY_ROOT / 'runtime' / 'fundamental_data').exists() else ROOT
FUNDAMENTAL_DIR = INPUT_ROOT / 'runtime' / 'fundamental_data'
VALUATION_PATH = FUNDAMENTAL_DIR / 'valuation_daily.parquet'
STOCK_BASIC_PATH = FUNDAMENTAL_DIR / 'stock_basic.parquet'
BENCHMARK_PATH = INPUT_ROOT / 'runtime' / 'market_data' / 'cn_etf' / '510300.parquet'
STATEMENT_DIRS = {
    'income': FUNDAMENTAL_DIR / 'income',
    'balance': FUNDAMENTAL_DIR / 'balance',
    'cashflow': FUNDAMENTAL_DIR / 'cashflow',
}


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    factor = frame.replace([np.inf, -np.inf], np.nan).stack(future_stack=True).dropna()
    factor.index = factor.index.set_names(['date', 'asset'])
    factor.name = name
    return factor


@lru_cache(maxsize=1)
def _symbol_to_tscode_map() -> dict[str, str]:
    if not STOCK_BASIC_PATH.exists():
        return {}
    df = pd.read_parquet(STOCK_BASIC_PATH, columns=['symbol', 'ts_code']).copy()
    df['symbol'] = df['symbol'].astype(str).str.zfill(6)
    df['ts_code'] = df['ts_code'].astype(str)
    return dict(zip(df['symbol'], df['ts_code']))


@lru_cache(maxsize=1)
def _benchmark_returns() -> pd.Series:
    if not BENCHMARK_PATH.exists():
        return pd.Series(dtype=float)
    df = pd.read_parquet(BENCHMARK_PATH, columns=['trade_date', 'close']).copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    close = pd.to_numeric(df['close'], errors='coerce')
    return close.set_axis(df['trade_date']).sort_index().pct_change(fill_method=None)


def _get_dates_and_assets(factor_input: pd.DataFrame) -> tuple[pd.DatetimeIndex, list[str]]:
    if factor_input.empty:
        return pd.DatetimeIndex([]), []
    dates = pd.DatetimeIndex(sorted(pd.to_datetime(factor_input.index.get_level_values('date')).unique()))
    assets = sorted({str(x).zfill(6) for x in factor_input.index.get_level_values('asset')})
    return dates, assets


def _pivot_close(factor_input: pd.DataFrame) -> pd.DataFrame:
    close = factor_input['close'].astype(float).unstack('asset').sort_index()
    close.columns = [str(col).zfill(6) for col in close.columns]
    return close


def _load_valuation_frame(assets: list[str], dates: pd.DatetimeIndex, column: str) -> pd.DataFrame:
    if not VALUATION_PATH.exists() or not assets or dates.empty:
        return pd.DataFrame(index=dates, columns=assets, dtype=float)
    df = pd.read_parquet(VALUATION_PATH, columns=['date', 'instrument_code', column]).copy()
    df['date'] = pd.to_datetime(df['date'])
    df['instrument_code'] = df['instrument_code'].astype(str).str.zfill(6)
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df = df[df['instrument_code'].isin(assets)]
    frame = df.pivot(index='date', columns='instrument_code', values=column).sort_index()
    return frame.reindex(index=dates, columns=assets)


def _quarter_ttm(frame: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.copy()
    frame['end_date'] = pd.to_datetime(frame['end_date'], errors='coerce')
    frame['announce_date'] = pd.to_datetime(frame['ann_date'], errors='coerce').fillna(pd.to_datetime(frame['f_ann_date'], errors='coerce'))
    frame[value_col] = pd.to_numeric(frame[value_col], errors='coerce')
    frame = frame.dropna(subset=['end_date', 'announce_date', value_col]).sort_values(['end_date', 'announce_date'])
    frame = frame.drop_duplicates('end_date', keep='last')
    values = frame.set_index('end_date')[value_col]
    ttm_values: dict[pd.Timestamp, float] = {}
    for end_date, value in values.items():
        if end_date.month == 12:
            ttm_values[end_date] = float(value)
            continue
        prev_year_end = pd.Timestamp(end_date.year - 1, 12, 31)
        prev_same_quarter = end_date - pd.DateOffset(years=1)
        if prev_year_end in values.index and prev_same_quarter in values.index:
            ttm_values[end_date] = float(value + values.loc[prev_year_end] - values.loc[prev_same_quarter])
        else:
            ttm_values[end_date] = np.nan
    out = frame[['announce_date', 'end_date']].copy()
    out[value_col] = out['end_date'].map(ttm_values)
    return out.dropna(subset=[value_col])


def _statement_daily_frame(statement: str, value_col: str, factor_input: pd.DataFrame, *, ttm: bool) -> pd.DataFrame:
    dates, assets = _get_dates_and_assets(factor_input)
    frame = pd.DataFrame(index=dates, columns=assets, dtype=float)
    symbol_map = _symbol_to_tscode_map()
    directory = STATEMENT_DIRS[statement]
    for asset in assets:
        ts_code = symbol_map.get(asset)
        if not ts_code:
            continue
        path = directory / f'{ts_code}.parquet'
        if not path.exists():
            continue
        raw = pd.read_parquet(path)
        if value_col not in raw.columns:
            continue
        prepared = _quarter_ttm(raw, value_col) if ttm else raw[['ann_date', 'f_ann_date', 'end_date', value_col]].copy()
        if prepared.empty:
            continue
        if not ttm:
            prepared['announce_date'] = pd.to_datetime(prepared['ann_date'], errors='coerce').fillna(pd.to_datetime(prepared['f_ann_date'], errors='coerce'))
            prepared[value_col] = pd.to_numeric(prepared[value_col], errors='coerce')
            prepared = prepared.dropna(subset=['announce_date', value_col]).sort_values(['announce_date', 'end_date']).drop_duplicates('end_date', keep='last')
        aligned = pd.merge_asof(
            pd.DataFrame({'date': dates}),
            prepared[['announce_date', value_col]].sort_values('announce_date'),
            left_on='date',
            right_on='announce_date',
            direction='backward',
        )
        frame[asset] = aligned[value_col].to_numpy(dtype=float)
    return frame


def _safe_ratio(numerator: pd.DataFrame, denominator: pd.DataFrame) -> pd.DataFrame:
    return numerator.divide(denominator.where(denominator != 0)).replace([np.inf, -np.inf], np.nan)


def book_to_market(factor_input: pd.DataFrame) -> pd.Series:
    dates, assets = _get_dates_and_assets(factor_input)
    pb = _load_valuation_frame(assets, dates, 'pb').where(lambda x: x > 0)
    return _stack_factor((1.0 / pb).shift(1), 'book_to_market')


def earnings_yield(factor_input: pd.DataFrame) -> pd.Series:
    dates, assets = _get_dates_and_assets(factor_input)
    pe_ttm = _load_valuation_frame(assets, dates, 'pe_ttm').where(lambda x: x > 0)
    return _stack_factor((1.0 / pe_ttm).shift(1), 'earnings_yield')


def sales_to_price(factor_input: pd.DataFrame) -> pd.Series:
    dates, assets = _get_dates_and_assets(factor_input)
    ps_ttm = _load_valuation_frame(assets, dates, 'ps_ttm').where(lambda x: x > 0)
    return _stack_factor((1.0 / ps_ttm).shift(1), 'sales_to_price')


def roe(factor_input: pd.DataFrame) -> pd.Series:
    income = _statement_daily_frame('income', 'n_income_attr_p', factor_input, ttm=True)
    equity = _statement_daily_frame('balance', 'total_hldr_eqy_exc_min_int', factor_input, ttm=False)
    avg_equity = (equity + equity.shift(252)) / 2.0
    factor = _safe_ratio(income, avg_equity).shift(1)
    return _stack_factor(factor, 'roe')


def gross_margin(factor_input: pd.DataFrame) -> pd.Series:
    revenue = _statement_daily_frame('income', 'revenue', factor_input, ttm=True)
    total_cogs = _statement_daily_frame('income', 'total_cogs', factor_input, ttm=True)
    factor = _safe_ratio(revenue - total_cogs, revenue).shift(1)
    return _stack_factor(factor, 'gross_margin')


def asset_turnover(factor_input: pd.DataFrame) -> pd.Series:
    revenue = _statement_daily_frame('income', 'revenue', factor_input, ttm=True)
    assets = _statement_daily_frame('balance', 'total_assets', factor_input, ttm=False)
    avg_assets = (assets + assets.shift(252)) / 2.0
    factor = _safe_ratio(revenue, avg_assets).shift(1)
    return _stack_factor(factor, 'asset_turnover')


def accruals(factor_input: pd.DataFrame) -> pd.Series:
    income = _statement_daily_frame('income', 'n_income_attr_p', factor_input, ttm=True)
    cfo = _statement_daily_frame('cashflow', 'n_cashflow_act', factor_input, ttm=True)
    assets = _statement_daily_frame('balance', 'total_assets', factor_input, ttm=False)
    factor = _safe_ratio(income - cfo, assets).shift(1)
    return _stack_factor(factor, 'accruals')


def momentum_12_1(factor_input: pd.DataFrame) -> pd.Series:
    close = _pivot_close(factor_input)
    factor = close.shift(21).divide(close.shift(252)).subtract(1.0).shift(1)
    return _stack_factor(factor, 'momentum_12_1')


def momentum_1m(factor_input: pd.DataFrame) -> pd.Series:
    close = _pivot_close(factor_input)
    factor = -close.pct_change(21, fill_method=None).shift(1)
    return _stack_factor(factor, 'momentum_1m')


def idiosyncratic_vol(factor_input: pd.DataFrame) -> pd.Series:
    close = _pivot_close(factor_input)
    returns = close.pct_change(fill_method=None)
    factor = returns.rolling(60, min_periods=40).std(ddof=0).shift(1)
    return _stack_factor(factor, 'idiosyncratic_vol')


def beta_1y(factor_input: pd.DataFrame) -> pd.Series:
    close = _pivot_close(factor_input)
    returns = close.pct_change(fill_method=None)
    benchmark = _benchmark_returns().reindex(close.index)
    variance = benchmark.rolling(252, min_periods=120).var(ddof=0)
    betas = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for column in close.columns:
        cov = returns[column].rolling(252, min_periods=120).cov(benchmark)
        betas[column] = cov.divide(variance.where(variance != 0))
    return _stack_factor(betas.shift(1), 'beta_1y')

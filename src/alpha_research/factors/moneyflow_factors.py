from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PRIMARY_ROOT = Path(r'D:\AITradingSystem')
INPUT_ROOT = PRIMARY_ROOT if (PRIMARY_ROOT / 'runtime' / 'fundamental_data').exists() else ROOT
FUNDAMENTAL_DIR = INPUT_ROOT / 'runtime' / 'fundamental_data'
MONEYFLOW_PATH = FUNDAMENTAL_DIR / 'moneyflow.parquet'
VALUATION_PATH = FUNDAMENTAL_DIR / 'valuation_daily.parquet'

_DATE_ALIASES = ('trade_date', 'date')
_ASSET_ALIASES = ('ts_code', 'instrument_code', 'symbol', 'asset')
_MAIN_INFLOW_ALIASES = ('net_mf_amount', 'main_net_inflow', 'net_main_inflow')
_BUY_ELG_ALIASES = ('buy_elg_amount', 'buy_xl_amount', 'buy_large_amount')
_SELL_ELG_ALIASES = ('sell_elg_amount', 'sell_xl_amount', 'sell_large_amount')
_MARKET_CAP_ALIASES = ('total_mv', 'circ_mv', 'market_cap')


def _empty_series(name: str) -> pd.Series:
    index = pd.MultiIndex.from_arrays([[], []], names=['date', 'asset'])
    return pd.Series(dtype=float, index=index, name=name)


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    if frame.empty:
        return _empty_series(name)
    factor = frame.replace([np.inf, -np.inf], np.nan).stack(future_stack=True).dropna()
    factor.index = factor.index.set_names(['date', 'asset'])
    factor.name = name
    return factor


def _get_dates_and_assets(factor_input: pd.DataFrame) -> tuple[pd.DatetimeIndex, list[str]]:
    if factor_input.empty:
        return pd.DatetimeIndex([]), []
    dates = pd.DatetimeIndex(sorted(pd.to_datetime(factor_input.index.get_level_values('date')).unique()))
    assets = sorted({str(x).zfill(6) for x in factor_input.index.get_level_values('asset')})
    return dates, assets


def _find_column(columns: pd.Index, aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        if alias in columns:
            return alias
    return None


def _normalize_asset(series: pd.Series) -> pd.Series:
    asset = series.astype(str).str.strip()
    asset = asset.str.replace(r'\.(SH|SZ|BJ)$', '', regex=True)
    return asset.str.zfill(6)


def _load_moneyflow_table() -> pd.DataFrame:
    if not MONEYFLOW_PATH.exists():
        raise FileNotFoundError(f'Moneyflow data file not found: {MONEYFLOW_PATH}')
    return pd.read_parquet(MONEYFLOW_PATH)


def _moneyflow_metric_frame(factor_input: pd.DataFrame, aliases: tuple[str, ...]) -> pd.DataFrame:
    dates, assets = _get_dates_and_assets(factor_input)
    if dates.empty or not assets:
        _load_moneyflow_table()
        return pd.DataFrame(index=dates, columns=assets, dtype=float)

    raw = _load_moneyflow_table()
    date_col = _find_column(raw.columns, _DATE_ALIASES)
    asset_col = _find_column(raw.columns, _ASSET_ALIASES)
    value_col = _find_column(raw.columns, aliases)
    if date_col is None or asset_col is None or value_col is None:
        return pd.DataFrame(index=dates, columns=assets, dtype=float)

    frame = raw[[date_col, asset_col, value_col]].copy()
    frame[date_col] = pd.to_datetime(frame[date_col], errors='coerce')
    frame[asset_col] = _normalize_asset(frame[asset_col])
    frame[value_col] = pd.to_numeric(frame[value_col], errors='coerce')
    frame = frame.dropna(subset=[date_col])
    frame = frame[frame[asset_col].isin(assets)]
    frame = frame.pivot_table(index=date_col, columns=asset_col, values=value_col, aggfunc='last').sort_index()
    return frame.reindex(index=dates, columns=assets)


def _pivot_factor_input_column(factor_input: pd.DataFrame, candidates: tuple[str, ...]) -> pd.DataFrame | None:
    for column in candidates:
        if column in factor_input.columns:
            frame = factor_input[column].astype(float).unstack('asset').sort_index()
            frame.columns = [str(col).zfill(6) for col in frame.columns]
            return frame
    return None


def _market_cap_frame(factor_input: pd.DataFrame) -> pd.DataFrame:
    dates, assets = _get_dates_and_assets(factor_input)
    if dates.empty or not assets:
        return pd.DataFrame(index=dates, columns=assets, dtype=float)

    direct = _pivot_factor_input_column(factor_input, _MARKET_CAP_ALIASES)
    if direct is not None:
        return direct.reindex(index=dates, columns=assets)

    if VALUATION_PATH.exists():
        raw = pd.read_parquet(VALUATION_PATH)
        date_col = _find_column(raw.columns, _DATE_ALIASES)
        asset_col = _find_column(raw.columns, ('instrument_code', 'ts_code', 'symbol'))
        value_col = _find_column(raw.columns, ('total_mv', 'circ_mv'))
        if date_col and asset_col and value_col:
            frame = raw[[date_col, asset_col, value_col]].copy()
            frame[date_col] = pd.to_datetime(frame[date_col], errors='coerce')
            frame[asset_col] = _normalize_asset(frame[asset_col])
            frame[value_col] = pd.to_numeric(frame[value_col], errors='coerce')
            frame = frame.dropna(subset=[date_col])
            frame = frame[frame[asset_col].isin(assets)]
            frame = frame.pivot_table(index=date_col, columns=asset_col, values=value_col, aggfunc='last').sort_index()
            return frame.reindex(index=dates, columns=assets)

    fallback = _moneyflow_metric_frame(factor_input, _MARKET_CAP_ALIASES)
    return fallback.reindex(index=dates, columns=assets)


def _amount_frame(factor_input: pd.DataFrame) -> pd.DataFrame:
    dates, assets = _get_dates_and_assets(factor_input)
    frame = _pivot_factor_input_column(factor_input, ('amount', 'turnover'))
    if frame is not None:
        return frame.reindex(index=dates, columns=assets)
    return pd.DataFrame(index=dates, columns=assets, dtype=float)


def _safe_ratio(numerator: pd.DataFrame, denominator: pd.DataFrame) -> pd.DataFrame:
    return numerator.divide(denominator.where(denominator != 0)).replace([np.inf, -np.inf], np.nan)


def mf_net_inflow_5d(factor_input: pd.DataFrame) -> pd.Series:
    inflow = _moneyflow_metric_frame(factor_input, _MAIN_INFLOW_ALIASES)
    market_cap = _market_cap_frame(factor_input)
    factor = _safe_ratio(inflow.rolling(5, min_periods=1).sum(), market_cap).shift(1)
    return _stack_factor(factor, 'mf_net_inflow_5d')


def mf_net_inflow_20d(factor_input: pd.DataFrame) -> pd.Series:
    inflow = _moneyflow_metric_frame(factor_input, _MAIN_INFLOW_ALIASES)
    market_cap = _market_cap_frame(factor_input)
    factor = _safe_ratio(inflow.rolling(20, min_periods=1).sum(), market_cap).shift(1)
    return _stack_factor(factor, 'mf_net_inflow_20d')


def mf_large_order_ratio(factor_input: pd.DataFrame) -> pd.Series:
    buy_elg = _moneyflow_metric_frame(factor_input, _BUY_ELG_ALIASES).rolling(5, min_periods=1).sum()
    sell_elg = _moneyflow_metric_frame(factor_input, _SELL_ELG_ALIASES).rolling(5, min_periods=1).sum()
    factor = _safe_ratio(buy_elg, buy_elg + sell_elg).shift(1)
    return _stack_factor(factor, 'mf_large_order_ratio')


def mf_smart_money(factor_input: pd.DataFrame) -> pd.Series:
    buy_elg = _moneyflow_metric_frame(factor_input, _BUY_ELG_ALIASES)
    sell_elg = _moneyflow_metric_frame(factor_input, _SELL_ELG_ALIASES)
    amount = _amount_frame(factor_input)
    net_elg = (buy_elg - sell_elg).rolling(5, min_periods=1).sum()
    total_amount = amount.rolling(5, min_periods=1).sum()
    factor = _safe_ratio(net_elg, total_amount).shift(1)
    return _stack_factor(factor, 'mf_smart_money')


def mf_inflow_acceleration(factor_input: pd.DataFrame) -> pd.Series:
    inflow = _moneyflow_metric_frame(factor_input, _MAIN_INFLOW_ALIASES)
    factor = (inflow.rolling(5, min_periods=1).sum() - inflow.rolling(20, min_periods=1).sum()).shift(1)
    return _stack_factor(factor, 'mf_inflow_acceleration')

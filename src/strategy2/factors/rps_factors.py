from __future__ import annotations

import numpy as np
import pandas as pd


EPSILON = 1e-12


def _normalize_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if 'trade_date' not in frame.columns:
        raise KeyError('missing trade_date column')
    frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
    if 'ts_code' not in frame.columns:
        if 'symbol' not in frame.columns:
            raise KeyError('missing ts_code/symbol column')
        symbols = frame['symbol'].astype(str).str.zfill(6)
        frame['ts_code'] = np.where(
            symbols.str.startswith(('600', '601', '603', '605', '688', '689', '900')),
            symbols + '.SH',
            np.where(symbols.str.startswith(('8', '4')), symbols + '.BJ', symbols + '.SZ'),
        )
    frame['ts_code'] = frame['ts_code'].astype(str).str.upper()
    return frame.dropna(subset=['trade_date', 'ts_code'])


def _normalize_member_history(member_history: pd.DataFrame) -> pd.DataFrame:
    frame = member_history.copy()
    frame['ts_code'] = frame['ts_code'].astype(str).str.upper()
    frame['index_code'] = frame['index_code'].astype(str).str.upper()
    frame['in_date'] = pd.to_datetime(frame['in_date'], errors='coerce')
    frame['out_date'] = pd.to_datetime(frame['out_date'], errors='coerce')
    return frame.dropna(subset=['ts_code', 'index_code', 'in_date'])


def _pivot_metric(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if value_col not in df.columns:
        raise KeyError(f'missing {value_col} column')
    frame = _normalize_price_frame(df)
    wide = frame.pivot_table(index='trade_date', columns='ts_code', values=value_col, aggfunc='last')
    return wide.sort_index().sort_index(axis=1).astype(float)


def _stack_frame(frame: pd.DataFrame, value_name: str, entity_name: str) -> pd.DataFrame:
    stacked = frame.replace([np.inf, -np.inf], np.nan).stack(future_stack=True).reset_index()
    stacked.columns = ['trade_date', entity_name, value_name]
    return stacked


def map_stock_to_sector_history(
    member_history: pd.DataFrame,
    trade_dates: pd.DatetimeIndex | list[pd.Timestamp],
    ts_codes: list[str] | None = None,
) -> pd.DataFrame:
    history = _normalize_member_history(member_history)
    if ts_codes is not None:
        codes = pd.Index([str(code).upper() for code in ts_codes])
        history = history.loc[history['ts_code'].isin(codes)]
    dates = pd.DatetimeIndex(pd.to_datetime(pd.Index(trade_dates), errors='coerce')).dropna().sort_values().unique()
    rows: list[pd.DataFrame] = []
    for date in dates:
        active = history.loc[(history['in_date'] <= date) & (history['out_date'].isna() | (history['out_date'] >= date))]
        if active.empty:
            continue
        active = active.sort_values(['ts_code', 'in_date', 'index_code']).drop_duplicates('ts_code', keep='last')
        payload = active[['ts_code', 'index_code']].copy()
        payload['trade_date'] = pd.Timestamp(date)
        rows.append(payload)
    if not rows:
        return pd.DataFrame(columns=['trade_date', 'ts_code', 'index_code'])
    result = pd.concat(rows, ignore_index=True)
    return result[['trade_date', 'ts_code', 'index_code']].sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_stock_rps(df: pd.DataFrame) -> pd.DataFrame:
    close = _pivot_metric(df, 'close')
    outputs: list[pd.DataFrame] = []
    for window in (20, 60, 120):
        ret = close.pct_change(periods=window, fill_method=None)
        rps = ret.rank(axis=1, pct=True) * 100.0
        outputs.append(_stack_frame(rps, f'rps_{window}', 'ts_code'))
    result = outputs[0]
    for item in outputs[1:]:
        result = result.merge(item, on=['trade_date', 'ts_code'], how='outer')
    return result.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_sector_rps(index_daily: pd.DataFrame) -> pd.DataFrame:
    frame = index_daily.copy()
    frame['ts_code'] = frame['ts_code'].astype(str).str.upper()
    frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
    if 'close' in frame.columns:
        close = frame.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
        returns_by_window = {window: close.pct_change(periods=window, fill_method=None) for window in (20, 60, 120)}
    elif 'pct_chg' in frame.columns:
        daily_ret = frame.pivot_table(index='trade_date', columns='ts_code', values='pct_chg', aggfunc='last').sort_index() / 100.0
        returns_by_window = {
            window: (1.0 + daily_ret).rolling(window, min_periods=window).apply(np.prod, raw=True) - 1.0
            for window in (20, 60, 120)
        }
    else:
        raise KeyError('sector index data must contain close or pct_chg')

    outputs: list[pd.DataFrame] = []
    for window, ret in returns_by_window.items():
        sector_rps = ret.rank(axis=1, pct=True) * 100.0
        outputs.append(_stack_frame(sector_rps, f'sector_rps_{window}', 'index_code'))
    result = outputs[0]
    for item in outputs[1:]:
        result = result.merge(item, on=['trade_date', 'index_code'], how='outer')
    return result.sort_values(['trade_date', 'index_code']).reset_index(drop=True)


def calc_sector_rps_approx(df: pd.DataFrame, stock_basic: pd.DataFrame) -> pd.DataFrame:
    prices = _normalize_price_frame(df)
    mapping = stock_basic[['ts_code', 'industry']].copy()
    mapping['ts_code'] = mapping['ts_code'].astype(str).str.upper()
    mapping['industry'] = mapping['industry'].fillna('UNKNOWN').astype(str)
    merged = prices[['trade_date', 'ts_code', 'close']].merge(mapping, on='ts_code', how='left')
    merged['industry'] = merged['industry'].fillna('UNKNOWN')

    close = merged.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    industry_map = mapping.drop_duplicates('ts_code').set_index('ts_code')['industry']

    outputs: list[pd.DataFrame] = []
    for window in (20, 60, 120):
        stock_ret = close.pct_change(periods=window, fill_method=None)
        sector_ret = stock_ret.T.groupby(industry_map.reindex(stock_ret.columns).fillna('UNKNOWN')).mean().T
        sector_rps = sector_ret.rank(axis=1, pct=True) * 100.0
        outputs.append(_stack_frame(sector_rps, f'sector_rps_{window}', 'industry'))
    result = outputs[0]
    for item in outputs[1:]:
        result = result.merge(item, on=['trade_date', 'industry'], how='outer')
    return result.sort_values(['trade_date', 'industry']).reset_index(drop=True)


def calc_sector_concentration(df: pd.DataFrame, stock_basic: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    prices = _normalize_price_frame(df)
    mapping = stock_basic[['ts_code', 'industry']].copy()
    mapping['ts_code'] = mapping['ts_code'].astype(str).str.upper()
    mapping['industry'] = mapping['industry'].fillna('UNKNOWN').astype(str)
    merged = prices[['trade_date', 'ts_code', 'close']].merge(mapping, on='ts_code', how='left')
    merged['industry'] = merged['industry'].fillna('UNKNOWN')

    close = merged.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    ret = close.pct_change(periods=window, fill_method=None).stack(future_stack=True).rename('ret').reset_index()
    ret.columns = ['trade_date', 'ts_code', 'ret']
    ret = ret.merge(mapping.drop_duplicates('ts_code'), on='ts_code', how='left')
    ret['industry'] = ret['industry'].fillna('UNKNOWN')
    ret['positive_ret'] = ret['ret'].clip(lower=0.0)
    total = ret.groupby(['trade_date', 'industry'])['positive_ret'].transform('sum')
    weights = np.where(total > EPSILON, ret['positive_ret'] / total, np.nan)
    ret['weight_sq'] = np.square(weights)
    concentration = ret.groupby(['trade_date', 'industry'])['weight_sq'].sum(min_count=1).reset_index()
    concentration = concentration.rename(columns={'weight_sq': 'concentration_ratio'})
    concentration['concentration_ratio'] = concentration['concentration_ratio'].fillna(1.0).clip(0.0, 1.0)
    return concentration.sort_values(['trade_date', 'industry']).reset_index(drop=True)

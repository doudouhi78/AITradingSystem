from __future__ import annotations

import numpy as np
import pandas as pd


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


def _stack_frame(frame: pd.DataFrame, value_name: str) -> pd.DataFrame:
    stacked = frame.replace([np.inf, -np.inf], np.nan).stack(future_stack=True).reset_index()
    stacked.columns = ['trade_date', 'ts_code', value_name]
    return stacked


def _to_ts_code(code: object) -> str:
    text = str(code).strip().upper().zfill(6)
    if text.startswith(('600', '601', '603', '605', '688', '689', '900')):
        return f'{text}.SH'
    if text.startswith(('8', '4')):
        return f'{text}.BJ'
    return f'{text}.SZ'


def calc_volume_zscore(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    frame = _normalize_price_frame(df)
    volume = frame.pivot_table(index='trade_date', columns='ts_code', values='volume', aggfunc='last').sort_index()
    mean = volume.rolling(window, min_periods=window).mean()
    std = volume.rolling(window, min_periods=window).std(ddof=0)
    zscore = (volume - mean).divide(std.replace(0.0, np.nan))
    return _stack_frame(zscore, 'volume_zscore').sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_turnover_deviation(df_valuation: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    frame = df_valuation.copy()
    date_col = 'date' if 'date' in frame.columns else 'trade_date'
    frame['trade_date'] = pd.to_datetime(frame[date_col], errors='coerce')
    frame['ts_code'] = frame['instrument_code'].map(_to_ts_code)
    frame['turnover_rate'] = pd.to_numeric(frame['turnover_rate'], errors='coerce')
    wide = frame.pivot_table(index='trade_date', columns='ts_code', values='turnover_rate', aggfunc='last').sort_index()
    mean = wide.rolling(window, min_periods=window).mean()
    deviation = wide.divide(mean.replace(0.0, np.nan))
    return _stack_frame(deviation, 'turnover_deviation').sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_ema_slope(df: pd.DataFrame, short: int = 20, long: int = 60) -> pd.DataFrame:
    frame = _normalize_price_frame(df)
    close = frame.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    short_ema = close.ewm(span=short, adjust=False, min_periods=short).mean()
    long_ema = close.ewm(span=long, adjust=False, min_periods=long).mean()
    slope = (short_ema - long_ema).divide(long_ema.replace(0.0, np.nan))
    return _stack_frame(slope, 'ema_slope').sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_bias(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    frame = _normalize_price_frame(df)
    close = frame.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    ma = close.rolling(window, min_periods=window).mean()
    bias = (close - ma).divide(ma.replace(0.0, np.nan)) * 100.0
    return _stack_frame(bias, 'bias').sort_values(['trade_date', 'ts_code']).reset_index(drop=True)

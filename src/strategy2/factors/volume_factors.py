from __future__ import annotations

from pathlib import Path

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_VALUATION_PATH = _REPO_ROOT / 'runtime' / 'fundamental_data' / 'valuation_daily.parquet'


def _normalize_ts_code(value: object) -> str:
    text = str(value or '').strip().upper()
    if not text:
        return ''
    if '.' in text:
        return text
    market = 'SH' if text.startswith(('6', '9')) else 'SZ'
    return f'{text.zfill(6)}.{market}'


def calc_volume_zscore(df: pd.DataFrame, window: int = 20) -> pd.Series:
    if 'volume' not in df.columns:
        raise ValueError('missing volume column')
    volume = pd.to_numeric(df['volume'], errors='coerce')
    mean = volume.rolling(window, min_periods=window).mean()
    std = volume.rolling(window, min_periods=window).std(ddof=0)
    return ((volume - mean) / std.replace(0.0, pd.NA)).astype(float).rename(f'volume_zscore_{window}')


def calc_turnover_deviation(
    df_valuation: pd.DataFrame,
    window: int = 20,
    ts_code_col: str = 'ts_code',
) -> pd.DataFrame:
    frame = df_valuation.copy()
    date_col = 'trade_date' if 'trade_date' in frame.columns else 'date'
    if date_col not in frame.columns:
        raise ValueError('missing trade date column')
    if 'turnover_rate' not in frame.columns:
        raise ValueError('missing turnover_rate column')
    if ts_code_col not in frame.columns:
        if 'instrument_code' in frame.columns:
            frame[ts_code_col] = frame['instrument_code'].map(_normalize_ts_code)
        else:
            raise ValueError('missing ts_code/instrument_code column')
    frame[ts_code_col] = frame[ts_code_col].map(_normalize_ts_code)
    frame[date_col] = pd.to_datetime(frame[date_col], errors='coerce')
    frame['turnover_rate'] = pd.to_numeric(frame['turnover_rate'], errors='coerce')
    frame = frame.dropna(subset=[ts_code_col, date_col]).sort_values([ts_code_col, date_col])
    rolling_mean = frame.groupby(ts_code_col)['turnover_rate'].transform(lambda s: s.rolling(window, min_periods=window).mean())
    frame['turnover_deviation'] = frame['turnover_rate'] / rolling_mean.replace(0.0, pd.NA)
    return frame[[date_col, ts_code_col, 'turnover_deviation']].rename(columns={date_col: 'trade_date'})


def calc_bias(df: pd.DataFrame, window: int = 20) -> pd.Series:
    if 'close' not in df.columns:
        raise ValueError('missing close column')
    close = pd.to_numeric(df['close'], errors='coerce')
    ma = close.rolling(window, min_periods=window).mean()
    return (((close - ma) / ma.replace(0.0, pd.NA)) * 100.0).astype(float).rename(f'bias_{window}')


def load_turnover_data(path: str | Path | None = None) -> pd.DataFrame:
    target = Path(path) if path is not None else _DEFAULT_VALUATION_PATH
    frame = pd.read_parquet(target, columns=['date', 'instrument_code', 'turnover_rate']).copy()
    frame['ts_code'] = frame['instrument_code'].map(_normalize_ts_code)
    return frame

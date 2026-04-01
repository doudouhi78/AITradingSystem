from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable
import warnings

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CLASSIFICATION_DIR = ROOT / 'runtime' / 'classification_data'
FUNDAMENTAL_DIR = ROOT / 'runtime' / 'fundamental_data'
MARKET_DATA_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
INDEX_COMPONENT_DIR = CLASSIFICATION_DIR / 'index_components'
INDEX_HISTORY_DIR = ROOT / 'runtime' / 'index_data' / 'index_components'

STOCK_BASIC_PATH = FUNDAMENTAL_DIR / 'stock_basic.parquet'
ST_HISTORY_PATH = FUNDAMENTAL_DIR / 'st_history.parquet'
SUSPEND_PATH = FUNDAMENTAL_DIR / 'suspend.parquet'
TRADE_CAL_PATH = FUNDAMENTAL_DIR / 'trade_cal.parquet'
LIMIT_LIST_PATH = FUNDAMENTAL_DIR / 'limit_list.parquet'

UNIVERSE_FILES = {
    'csi300': [INDEX_COMPONENT_DIR / 'csi300_latest.parquet', INDEX_HISTORY_DIR / 'csi300.parquet'],
    'csi500': [INDEX_COMPONENT_DIR / 'csi500_latest.parquet', INDEX_HISTORY_DIR / 'csi500.parquet'],
    'csi1000': [INDEX_COMPONENT_DIR / 'csi1000_latest.parquet', INDEX_HISTORY_DIR / 'csi1000.parquet'],
}
SUPPORTED_UNIVERSES = {'csi300', 'csi500', 'csi1000', 'all_a'}



def _warn(message: str) -> None:
    print(f'Warning: {message}')
    warnings.warn(message, stacklevel=2)



def _normalize_code(value: object) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    if '.' in text:
        text = text.split('.')[0]
    return text.zfill(6)



def _normalize_codes(values: Iterable[object]) -> list[str]:
    return [_normalize_code(value) for value in values if _normalize_code(value)]



def _normalize_date(value: str | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()



def _load_parquet(path: Path, *, date_columns: Iterable[str] = ()) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    for column in date_columns:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors='coerce')
    return frame


@lru_cache(maxsize=1)
def _load_trade_calendar() -> pd.DatetimeIndex:
    if not TRADE_CAL_PATH.exists():
        _warn(f'trade calendar missing: {TRADE_CAL_PATH}')
        return pd.DatetimeIndex([])
    frame = _load_parquet(TRADE_CAL_PATH, date_columns=['cal_date'])
    if 'is_open' in frame.columns:
        frame = frame.loc[frame['is_open'] == 1]
    dates = pd.to_datetime(frame['cal_date'], errors='coerce').dropna().sort_values().drop_duplicates()
    return pd.DatetimeIndex(dates)


@lru_cache(maxsize=1)
def _load_stock_basic() -> pd.DataFrame:
    if not STOCK_BASIC_PATH.exists():
        _warn(f'stock_basic missing: {STOCK_BASIC_PATH}')
        return pd.DataFrame(columns=['instrument_code', 'list_date', 'delist_date', 'list_status'])
    frame = _load_parquet(STOCK_BASIC_PATH, date_columns=['list_date', 'delist_date'])
    if 'instrument_code' not in frame.columns:
        source_col = 'symbol' if 'symbol' in frame.columns else 'ts_code'
        frame['instrument_code'] = frame[source_col].map(_normalize_code)
    else:
        frame['instrument_code'] = frame['instrument_code'].map(_normalize_code)
    return frame.dropna(subset=['instrument_code']).drop_duplicates(subset=['instrument_code']).reset_index(drop=True)


@lru_cache(maxsize=1)
def _load_st_history() -> pd.DataFrame:
    if not ST_HISTORY_PATH.exists():
        _warn(f'st_history missing: {ST_HISTORY_PATH}')
        return pd.DataFrame(columns=['instrument_code', 'start_date', 'end_date'])
    frame = _load_parquet(ST_HISTORY_PATH, date_columns=['start_date', 'end_date', 'ann_date'])
    source_col = 'ts_code' if 'ts_code' in frame.columns else 'instrument_code'
    frame['instrument_code'] = frame[source_col].map(_normalize_code)
    return frame.dropna(subset=['instrument_code']).reset_index(drop=True)


@lru_cache(maxsize=1)
def _load_suspend() -> pd.DataFrame:
    if not SUSPEND_PATH.exists():
        _warn(f'suspend file missing: {SUSPEND_PATH}')
        return pd.DataFrame(columns=['instrument_code', 'trade_date'])
    frame = _load_parquet(SUSPEND_PATH, date_columns=['trade_date'])
    source_col = 'ts_code' if 'ts_code' in frame.columns else 'instrument_code'
    frame['instrument_code'] = frame[source_col].map(_normalize_code)
    return frame.dropna(subset=['instrument_code', 'trade_date']).reset_index(drop=True)


@lru_cache(maxsize=1)
def _load_limit_list() -> pd.DataFrame:
    if not LIMIT_LIST_PATH.exists():
        return pd.DataFrame()
    frame = _load_parquet(LIMIT_LIST_PATH, date_columns=['trade_date'])
    source_col = 'ts_code' if 'ts_code' in frame.columns else 'instrument_code'
    frame['instrument_code'] = frame[source_col].map(_normalize_code)
    return frame.dropna(subset=['instrument_code', 'trade_date']).reset_index(drop=True)



def _resolve_universe_frame(universe_type: str, query_date: pd.Timestamp) -> pd.DataFrame:
    for path in UNIVERSE_FILES.get(universe_type, []):
        if not path.exists():
            continue
        date_columns = ['trade_date'] if 'history' in path.name or path.parent == INDEX_HISTORY_DIR else []
        frame = _load_parquet(path, date_columns=date_columns)
        if 'instrument_code' in frame.columns:
            frame['instrument_code'] = frame['instrument_code'].map(_normalize_code)
        elif 'symbol' in frame.columns:
            frame['instrument_code'] = frame['symbol'].map(_normalize_code)
        elif 'ts_code' in frame.columns:
            frame['instrument_code'] = frame['ts_code'].map(_normalize_code)
        else:
            first_col = frame.columns[0]
            frame['instrument_code'] = frame[first_col].map(_normalize_code)
        if 'trade_date' in frame.columns:
            eligible = frame.loc[frame['trade_date'] <= query_date]
            if not eligible.empty:
                latest_date = eligible['trade_date'].max()
                return eligible.loc[eligible['trade_date'] == latest_date].copy()
        return frame.copy()
    if universe_type == 'csi1000':
        _warn('csi1000 component file missing; fallback to all_a universe')
        return pd.DataFrame({'instrument_code': _load_all_a_codes()})
    _warn(f'universe component file missing for {universe_type}; returning empty universe')
    return pd.DataFrame(columns=['instrument_code'])



def _load_all_a_codes() -> list[str]:
    frame = _load_stock_basic()
    if not frame.empty:
        codes = frame['instrument_code'].dropna().astype(str).tolist()
        return sorted(set(code for code in codes if code))
    if not MARKET_DATA_DIR.exists():
        _warn(f'stock market data dir missing: {MARKET_DATA_DIR}')
        return []
    return sorted(path.stem for path in MARKET_DATA_DIR.glob('*.parquet'))



def _previous_trade_date(query_date: pd.Timestamp) -> pd.Timestamp | None:
    calendar = _load_trade_calendar()
    if len(calendar) == 0:
        return None
    eligible = calendar[calendar < query_date]
    if len(eligible) == 0:
        return None
    return pd.Timestamp(eligible[-1]).normalize()



def _previous_previous_trade_date(query_date: pd.Timestamp) -> pd.Timestamp | None:
    previous = _previous_trade_date(query_date)
    if previous is None:
        return None
    return _previous_trade_date(previous)



def _active_st_codes(query_date: pd.Timestamp) -> set[str]:
    frame = _load_st_history()
    if frame.empty:
        return set()
    if 'name' in frame.columns or 'change_reason' in frame.columns:
        st_mask = frame.get('name', pd.Series('', index=frame.index)).astype(str).str.contains(r'\\*?ST', case=False, na=False) | frame.get('change_reason', pd.Series('', index=frame.index)).astype(str).str.contains(r'\\*?ST', case=False, na=False)
    else:
        st_mask = pd.Series(True, index=frame.index)
    active = frame.loc[
        st_mask
        & (frame['start_date'].isna() | (frame['start_date'] <= query_date))
        & (frame['end_date'].isna() | (frame['end_date'] >= query_date))
    ]
    return set(active['instrument_code'].tolist())



def _recently_listed_codes(query_date: pd.Timestamp, min_trade_days: int = 252) -> set[str]:
    frame = _load_stock_basic()
    if frame.empty or 'list_date' not in frame.columns:
        return set()
    calendar = _load_trade_calendar()
    if len(calendar) == 0:
        cutoff = query_date - pd.Timedelta(days=365)
        filtered = frame.loc[pd.to_datetime(frame['list_date'], errors='coerce') > cutoff]
        return set(filtered['instrument_code'].tolist())
    eligible_calendar = calendar[calendar <= query_date]
    if len(eligible_calendar) == 0:
        return set(frame['instrument_code'].tolist())
    query_idx = len(eligible_calendar) - 1
    listed_codes: set[str] = set()
    for _, row in frame[['instrument_code', 'list_date']].dropna(subset=['list_date']).iterrows():
        list_date = pd.Timestamp(row['list_date']).normalize()
        list_idx = int(calendar.searchsorted(list_date, side='left'))
        if query_idx - list_idx + 1 < min_trade_days:
            listed_codes.add(str(row['instrument_code']))
    return listed_codes



def _delisted_codes(query_date: pd.Timestamp) -> set[str]:
    frame = _load_stock_basic()
    if frame.empty:
        return set()
    if 'delist_date' not in frame.columns:
        return set()
    filtered = frame.loc[pd.to_datetime(frame['delist_date'], errors='coerce').notna() & (pd.to_datetime(frame['delist_date'], errors='coerce') <= query_date)]
    return set(filtered['instrument_code'].tolist())



def _suspended_codes(query_date: pd.Timestamp) -> set[str]:
    frame = _load_suspend()
    if frame.empty:
        return set()
    filtered = frame.loc[frame['trade_date'] == query_date]
    return set(filtered['instrument_code'].tolist())


@lru_cache(maxsize=4096)
def _load_stock_daily(symbol: str) -> pd.DataFrame:
    path = MARKET_DATA_DIR / f'{symbol}.parquet'
    if not path.exists():
        return pd.DataFrame()
    frame = _load_parquet(path, date_columns=['trade_date'])
    if 'symbol' not in frame.columns:
        frame['symbol'] = symbol
    frame['symbol'] = frame['symbol'].map(_normalize_code)
    return frame.sort_values('trade_date').reset_index(drop=True)



def _limit_hit_from_limit_list(prev_trade_date: pd.Timestamp) -> set[str]:
    frame = _load_limit_list()
    if frame.empty:
        return set()
    filtered = frame.loc[frame['trade_date'] == prev_trade_date]
    if filtered.empty:
        return set()
    if 'limit' in filtered.columns:
        filtered = filtered.loc[filtered['limit'].astype(str).str.upper().isin({'U', 'UP'})]
    elif 'close' in filtered.columns and 'pre_close' in filtered.columns:
        pct = pd.to_numeric(filtered['close'], errors='coerce') / pd.to_numeric(filtered['pre_close'], errors='coerce') - 1.0
        filtered = filtered.loc[pct >= 0.099]
    return set(filtered['instrument_code'].tolist())



def _limit_hit_codes(query_date: pd.Timestamp, candidate_codes: Iterable[str]) -> set[str]:
    prev_trade_date = _previous_trade_date(query_date)
    prev_prev_trade_date = _previous_previous_trade_date(query_date)
    if prev_trade_date is None or prev_prev_trade_date is None:
        return set()
    limit_hits = _limit_hit_from_limit_list(prev_trade_date)
    if limit_hits:
        return limit_hits
    fallback_hits: set[str] = set()
    for code in candidate_codes:
        frame = _load_stock_daily(code)
        if frame.empty or 'close' not in frame.columns:
            continue
        subset = frame.loc[frame['trade_date'].isin([prev_prev_trade_date, prev_trade_date]), ['trade_date', 'close']].sort_values('trade_date')
        if len(subset) < 2:
            continue
        prev_close = float(subset.iloc[0]['close'])
        close = float(subset.iloc[1]['close'])
        if prev_close > 0 and (close / prev_close - 1.0) >= 0.099:
            fallback_hits.add(code)
    if fallback_hits:
        _warn('limit_list missing; fallback to previous-day close-to-close limit detection')
    return fallback_hits



def get_universe(date: str | pd.Timestamp, universe_type: str = 'csi1000') -> list[str]:
    query_date = _normalize_date(date)
    universe_type = universe_type.lower()
    if universe_type not in SUPPORTED_UNIVERSES:
        raise ValueError(f'unsupported universe_type: {universe_type}')

    if universe_type == 'all_a':
        base_codes = _load_all_a_codes()
    else:
        base_frame = _resolve_universe_frame(universe_type, query_date)
        base_codes = _normalize_codes(base_frame.get('instrument_code', []))

    if not base_codes:
        return []

    excluded_st = _active_st_codes(query_date)
    excluded_new = _recently_listed_codes(query_date)
    excluded_delisted = _delisted_codes(query_date)
    excluded_suspended = _suspended_codes(query_date)
    excluded_limit_hit = _limit_hit_codes(query_date, base_codes)

    blocked = excluded_st | excluded_new | excluded_delisted | excluded_suspended | excluded_limit_hit
    return sorted(code for code in set(base_codes) if code and code not in blocked)



def clear_caches() -> None:
    _load_trade_calendar.cache_clear()
    _load_stock_basic.cache_clear()
    _load_st_history.cache_clear()
    _load_suspend.cache_clear()
    _load_limit_list.cache_clear()
    _load_stock_daily.cache_clear()



from __future__ import annotations

from pathlib import Path
from typing import Iterable
import warnings

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CLASSIFICATION_DIR = ROOT / 'runtime' / 'classification_data'
FUNDAMENTAL_DIR = ROOT / 'runtime' / 'fundamental_data'
MARKET_DATA_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'

INDUSTRY_PATH = CLASSIFICATION_DIR / 'industry_sw2.parquet'
VALUATION_PATH = FUNDAMENTAL_DIR / 'valuation_daily.parquet'
STOCK_META_PATH = CLASSIFICATION_DIR / 'stock_meta.parquet'



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



def get_industry_labels(date: str | pd.Timestamp, instrument_codes: Iterable[str]) -> dict[str, str]:
    _ = pd.Timestamp(date)
    codes = [_normalize_code(code) for code in instrument_codes if _normalize_code(code)]
    if not codes:
        return {}
    if not INDUSTRY_PATH.exists():
        _warn(f'industry mapping missing: {INDUSTRY_PATH}')
        return {code: 'unknown' for code in codes}
    frame = pd.read_parquet(INDUSTRY_PATH).copy()
    source_col = 'instrument_code' if 'instrument_code' in frame.columns else frame.columns[0]
    name_col = 'industry_name' if 'industry_name' in frame.columns else frame.columns[1]
    frame['instrument_code'] = frame[source_col].map(_normalize_code)
    mapping = frame.dropna(subset=['instrument_code']).drop_duplicates(subset=['instrument_code']).set_index('instrument_code')[name_col].astype(str).to_dict()
    return {code: mapping.get(code, 'unknown') for code in codes}



def _load_daily_proxy(date: pd.Timestamp, codes: list[str]) -> dict[str, float]:
    proxy: dict[str, float] = {}
    for code in codes:
        path = MARKET_DATA_DIR / f'{code}.parquet'
        if not path.exists():
            continue
        frame = pd.read_parquet(path, columns=['trade_date', 'close', 'volume']).copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
        row = frame.loc[frame['trade_date'] == date]
        if row.empty:
            continue
        close = pd.to_numeric(row.iloc[-1]['close'], errors='coerce')
        volume = pd.to_numeric(row.iloc[-1]['volume'], errors='coerce')
        if pd.notna(close) and pd.notna(volume):
            proxy[code] = float(close) * float(volume)
    return proxy



def _resolve_size_metric(frame: pd.DataFrame) -> tuple[str | None, bool]:
    for column in ['circ_mv', 'float_market_cap', 'total_mv', 'total_market_cap']:
        if column in frame.columns:
            return column, False
    return None, True



def get_size_labels(date: str | pd.Timestamp, instrument_codes: Iterable[str]) -> dict[str, str]:
    query_date = pd.Timestamp(date).normalize()
    codes = [_normalize_code(code) for code in instrument_codes if _normalize_code(code)]
    if not codes:
        return {}

    size_values: dict[str, float] = {}
    valuation_frame = pd.DataFrame()
    if VALUATION_PATH.exists():
        valuation_frame = pd.read_parquet(VALUATION_PATH).copy()
        if 'date' in valuation_frame.columns:
            valuation_frame['date'] = pd.to_datetime(valuation_frame['date'], errors='coerce')
        source_col = 'instrument_code' if 'instrument_code' in valuation_frame.columns else 'ts_code'
        valuation_frame['instrument_code'] = valuation_frame[source_col].map(_normalize_code)
        metric_col, needs_fallback = _resolve_size_metric(valuation_frame)
        if metric_col is not None:
            daily = valuation_frame.loc[(valuation_frame['date'] == query_date) & valuation_frame['instrument_code'].isin(codes), ['instrument_code', metric_col]]
            size_values = daily.dropna(subset=[metric_col]).drop_duplicates(subset=['instrument_code']).set_index('instrument_code')[metric_col].astype(float).to_dict()
        else:
            needs_fallback = True
    else:
        _warn(f'valuation data missing: {VALUATION_PATH}')
        needs_fallback = True

    if len(size_values) < len(codes):
        if STOCK_META_PATH.exists():
            stock_meta = pd.read_parquet(STOCK_META_PATH).copy()
            source_col = 'instrument_code' if 'instrument_code' in stock_meta.columns else 'symbol'
            stock_meta['instrument_code'] = stock_meta[source_col].map(_normalize_code)
            metric_col, _ = _resolve_size_metric(stock_meta)
            if metric_col is not None:
                fallback = stock_meta.loc[stock_meta['instrument_code'].isin(codes), ['instrument_code', metric_col]]
                for code, value in fallback.dropna(subset=[metric_col]).drop_duplicates(subset=['instrument_code']).set_index('instrument_code')[metric_col].astype(float).to_dict().items():
                    size_values.setdefault(code, value)

    if len(size_values) < len(codes):
        _warn('real market-cap columns missing; fallback to close*volume proxy for size buckets')
        proxy_values = _load_daily_proxy(query_date, codes)
        for code, value in proxy_values.items():
            size_values.setdefault(code, value)

    ranked = pd.Series({code: size_values.get(code) for code in codes}).dropna().sort_values()
    if ranked.empty:
        return {code: 'unknown' for code in codes}
    lower = float(ranked.quantile(1 / 3))
    upper = float(ranked.quantile(2 / 3))

    labels: dict[str, str] = {}
    for code in codes:
        value = size_values.get(code)
        if value is None:
            labels[code] = 'unknown'
        elif value <= lower:
            labels[code] = 'small'
        elif value <= upper:
            labels[code] = 'mid'
        else:
            labels[code] = 'large'
    return labels

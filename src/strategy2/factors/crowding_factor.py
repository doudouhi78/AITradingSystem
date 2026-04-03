from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_MARKET_DIR = _REPO_ROOT / 'runtime' / 'market_data' / 'cn_stock'
_DEFAULT_STOCK_BASIC = _REPO_ROOT / 'runtime' / 'fundamental_data' / 'stock_basic.parquet'
_DEFAULT_CSI300 = _REPO_ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'


def _normalize_ts_code(value: object) -> str:
    text = str(value or '').strip().upper()
    if not text:
        return ''
    if '.' in text:
        symbol, market = text.split('.', 1)
        return f'{symbol.zfill(6)}.{market}'
    market = 'SH' if text.startswith(('6', '9')) else 'SZ'
    return f'{text.zfill(6)}.{market}'


def _normalize_symbol(value: object) -> str:
    return _normalize_ts_code(value).split('.', 1)[0]


def load_default_csi300_symbols(path: str | Path | None = None) -> list[str]:
    target = Path(path) if path is not None else _DEFAULT_CSI300
    frame = pd.read_parquet(target)
    source = 'instrument_code' if 'instrument_code' in frame.columns else 'symbol'
    return sorted(set(frame[source].astype(str).str.zfill(6)))


def load_industry_map(stock_basic_path: str | Path | None = None) -> pd.Series:
    target = Path(stock_basic_path) if stock_basic_path is not None else _DEFAULT_STOCK_BASIC
    frame = pd.read_parquet(target, columns=['symbol', 'industry']).copy()
    frame['symbol'] = frame['symbol'].astype(str).str.zfill(6)
    frame['industry'] = frame['industry'].fillna('UNKNOWN').astype(str)
    return frame.drop_duplicates('symbol', keep='last').set_index('symbol')['industry']


def load_stock_amount_data(
    symbols: Iterable[str],
    market_data_dir: str | Path | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    market_dir = Path(market_data_dir) if market_data_dir is not None else _DEFAULT_MARKET_DIR
    frames: list[pd.DataFrame] = []
    start_ts = pd.Timestamp(start) if start else None
    end_ts = pd.Timestamp(end) if end else None
    for symbol in sorted({_normalize_symbol(item) for item in symbols if _normalize_symbol(item)}):
        path = market_dir / f'{symbol}.parquet'
        if not path.exists():
            continue
        df = pd.read_parquet(path, columns=['trade_date', 'close', 'amount']).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        if start_ts is not None:
            df = df.loc[df['trade_date'] >= start_ts]
        if end_ts is not None:
            df = df.loc[df['trade_date'] <= end_ts]
        if df.empty:
            continue
        df['symbol'] = symbol
        df['ts_code'] = df['symbol'].map(_normalize_ts_code)
        frames.append(df[['trade_date', 'symbol', 'ts_code', 'close', 'amount']])
    if not frames:
        return pd.DataFrame(columns=['trade_date', 'symbol', 'ts_code', 'close', 'amount'])
    return pd.concat(frames, ignore_index=True)


def compute_crowding_features(
    symbols: Iterable[str],
    market_data_dir: str | Path | None = None,
    stock_basic_path: str | Path | None = None,
    start: str | None = None,
    end: str | None = None,
    percentile_window: int = 252,
    min_periods: int = 60,
) -> pd.DataFrame:
    frame = load_stock_amount_data(symbols, market_data_dir=market_data_dir, start=start, end=end)
    if frame.empty:
        return pd.DataFrame(columns=['trade_date', 'ts_code', 'industry', 'stock_share', 'stock_percentile', 'industry_share', 'industry_percentile', 'crowding_score'])
    industry_map = load_industry_map(stock_basic_path)
    frame['industry'] = frame['symbol'].map(industry_map).fillna('UNKNOWN')
    market_total = frame.groupby('trade_date')['amount'].sum().rename('market_total')
    frame = frame.merge(market_total, on='trade_date', how='left')
    frame['stock_share'] = frame['amount'] / frame['market_total'].replace(0.0, pd.NA)
    frame = frame.sort_values(['symbol', 'trade_date'])
    frame['stock_percentile'] = frame.groupby('symbol')['stock_share'].transform(
        lambda s: s.rolling(percentile_window, min_periods=min_periods).rank(pct=True)
    )

    industry_amount = frame.groupby(['trade_date', 'industry'], as_index=False)['amount'].sum().rename(columns={'amount': 'industry_amount'})
    industry_total = industry_amount.groupby('trade_date')['industry_amount'].transform('sum')
    industry_amount['industry_share'] = industry_amount['industry_amount'] / industry_total.replace(0.0, pd.NA)
    industry_amount = industry_amount.sort_values(['industry', 'trade_date'])
    industry_amount['industry_percentile'] = industry_amount.groupby('industry')['industry_share'].transform(
        lambda s: s.rolling(percentile_window, min_periods=min_periods).rank(pct=True)
    )

    merged = frame.merge(
        industry_amount[['trade_date', 'industry', 'industry_share', 'industry_percentile']],
        on=['trade_date', 'industry'],
        how='left',
    )
    merged['crowding_score'] = merged['stock_share'] * merged['industry_percentile']
    return merged[['trade_date', 'ts_code', 'industry', 'close', 'stock_share', 'stock_percentile', 'industry_share', 'industry_percentile', 'crowding_score']].sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def evaluate_crowding_ic(
    crowding_frame: pd.DataFrame,
    windows: tuple[int, ...] = (5, 20),
    factor_col: str = 'crowding_score',
    min_cross_section: int = 20,
) -> dict[str, dict[str, float]]:
    if crowding_frame.empty:
        return {str(window): {'ic_mean': 0.0, 'icir': 0.0, 'direction': 'flat'} for window in windows}
    working = crowding_frame[['trade_date', 'ts_code', 'close', factor_col]].copy()
    working['trade_date'] = pd.to_datetime(working['trade_date'], errors='coerce')
    working['close'] = pd.to_numeric(working['close'], errors='coerce')
    working[factor_col] = pd.to_numeric(working[factor_col], errors='coerce')
    price_wide = working.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    factor_wide = working.pivot_table(index='trade_date', columns='ts_code', values=factor_col, aggfunc='last').sort_index()
    results: dict[str, dict[str, float]] = {}
    for window in windows:
        forward_returns = price_wide.pct_change(window, fill_method=None).shift(-window)
        aligned_factor, aligned_returns = factor_wide.align(forward_returns, join='inner', axis=0)
        daily_ic: list[float] = []
        for trade_date in aligned_factor.index:
            x = pd.to_numeric(aligned_factor.loc[trade_date], errors='coerce')
            y = pd.to_numeric(aligned_returns.loc[trade_date], errors='coerce')
            valid = x.notna() & y.notna()
            if valid.sum() < min_cross_section:
                continue
            corr = x[valid].corr(y[valid], method='spearman')
            if pd.notna(corr):
                daily_ic.append(float(corr))
        series = pd.Series(daily_ic, dtype=float)
        mean = float(series.mean()) if not series.empty else 0.0
        std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
        results[str(window)] = {
            'ic_mean': mean,
            'icir': (mean / std if std else mean),
            'direction': 'negative' if mean < 0 else 'positive' if mean > 0 else 'flat',
        }
    return results

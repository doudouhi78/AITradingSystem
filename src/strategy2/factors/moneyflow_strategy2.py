from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_MARKET_DIR = _REPO_ROOT / 'runtime' / 'market_data' / 'cn_stock'


def _normalize_ts_code(value: object) -> str:
    text = str(value or '').strip().upper()
    if not text:
        return ''
    if '.' in text:
        symbol, market = text.split('.', 1)
        return f'{symbol.zfill(6)}.{market}'
    market = 'SH' if text.startswith(('6', '9')) else 'SZ'
    return f'{text.zfill(6)}.{market}'


def calc_large_order_net_ratio(df_moneyflow: pd.DataFrame) -> pd.DataFrame:
    frame = df_moneyflow.copy()
    frame['trade_date'] = pd.to_datetime(frame['trade_date'], format='%Y%m%d', errors='coerce')
    frame['ts_code'] = frame['ts_code'].map(_normalize_ts_code)
    cols = ['buy_lg_amount', 'buy_elg_amount', 'sell_lg_amount', 'sell_elg_amount']
    for col in cols:
        frame[col] = pd.to_numeric(frame[col], errors='coerce')
    buy_total = frame['buy_lg_amount'].fillna(0.0) + frame['buy_elg_amount'].fillna(0.0)
    sell_total = frame['sell_lg_amount'].fillna(0.0) + frame['sell_elg_amount'].fillna(0.0)
    denom = (buy_total + sell_total).replace(0.0, pd.NA)
    factor = ((buy_total - sell_total) / denom).clip(-1.0, 1.0)
    return pd.DataFrame({'trade_date': frame['trade_date'], 'ts_code': frame['ts_code'], 'value': factor}).dropna(subset=['trade_date', 'ts_code'])


def calc_super_large_net_flow(df_moneyflow: pd.DataFrame) -> pd.DataFrame:
    frame = df_moneyflow.copy()
    frame['trade_date'] = pd.to_datetime(frame['trade_date'], format='%Y%m%d', errors='coerce')
    frame['ts_code'] = frame['ts_code'].map(_normalize_ts_code)
    frame['buy_elg_amount'] = pd.to_numeric(frame['buy_elg_amount'], errors='coerce')
    frame['sell_elg_amount'] = pd.to_numeric(frame['sell_elg_amount'], errors='coerce')
    factor = frame['buy_elg_amount'].fillna(0.0) - frame['sell_elg_amount'].fillna(0.0)
    return pd.DataFrame({'trade_date': frame['trade_date'], 'ts_code': frame['ts_code'], 'value': factor}).dropna(subset=['trade_date', 'ts_code'])


def _load_future_returns(ts_codes: list[str], market_data_dir: str | Path | None = None, forward_days: int = 5) -> pd.DataFrame:
    market_dir = Path(market_data_dir) if market_data_dir is not None else _DEFAULT_MARKET_DIR
    frames: list[pd.DataFrame] = []
    for ts_code in sorted(set(ts_codes)):
        symbol = ts_code.split('.', 1)[0]
        path = market_dir / f'{symbol}.parquet'
        if not path.exists():
            continue
        df = pd.read_parquet(path, columns=['trade_date', 'close']).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.dropna(subset=['trade_date', 'close']).sort_values('trade_date')
        df['future_return'] = df['close'].shift(-forward_days) / df['close'] - 1.0
        df['ts_code'] = ts_code
        frames.append(df[['trade_date', 'ts_code', 'future_return']])
    if not frames:
        return pd.DataFrame(columns=['trade_date', 'ts_code', 'future_return'])
    return pd.concat(frames, ignore_index=True)


def evaluate_moneyflow_ic(
    factor_df: pd.DataFrame,
    market_data_dir: str | Path | None = None,
    forward_days: int = 5,
    min_cross_section: int = 20,
) -> dict[str, Any]:
    if factor_df.empty:
        return {'ic_mean': 0.0, 'icir': 0.0, 'sample_count': 0}
    normalized = factor_df.copy()
    normalized['trade_date'] = pd.to_datetime(normalized['trade_date'], errors='coerce')
    normalized['ts_code'] = normalized['ts_code'].map(_normalize_ts_code)
    normalized['value'] = pd.to_numeric(normalized['value'], errors='coerce')
    normalized = normalized.dropna(subset=['trade_date', 'ts_code', 'value'])
    future_returns = _load_future_returns(normalized['ts_code'].unique().tolist(), market_data_dir=market_data_dir, forward_days=forward_days)
    merged = normalized.merge(future_returns, on=['trade_date', 'ts_code'], how='inner').dropna(subset=['future_return'])
    daily_ic: dict[pd.Timestamp, float] = {}
    for trade_date, cross in merged.groupby('trade_date', sort=True):
        if len(cross) < min_cross_section:
            continue
        corr = cross['value'].corr(cross['future_return'], method='spearman')
        if pd.notna(corr):
            daily_ic[pd.Timestamp(trade_date)] = float(corr)
    ic_series = pd.Series(daily_ic, dtype=float).sort_index()
    if ic_series.empty:
        return {'ic_mean': 0.0, 'icir': 0.0, 'sample_count': 0}
    mean = float(ic_series.mean())
    std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
    return {'ic_mean': mean, 'icir': (mean / std if std else mean), 'sample_count': int(len(ic_series))}

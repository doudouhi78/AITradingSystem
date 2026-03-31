from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PRIMARY_ROOT = Path(r'D:\AITradingSystem')
INPUT_ROOT = PRIMARY_ROOT if (PRIMARY_ROOT / 'runtime' / 'alternative_data').exists() else ROOT
NORTHBOUND_PATH = INPUT_ROOT / 'runtime' / 'alternative_data' / 'northbound_flow.parquet'
MARGIN_PATH = INPUT_ROOT / 'runtime' / 'alternative_data' / 'margin_balance.parquet'


def _normalize_market_series(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors='coerce').sort_index()
    mean = series.rolling(60, min_periods=20).mean()
    std = series.rolling(60, min_periods=20).std(ddof=0)
    normalized = ((series - mean) / std.replace(0, np.nan)).shift(1)
    normalized.name = series.name
    return normalized.replace([np.inf, -np.inf], np.nan).dropna()


def factor_northbound_flow_5d(instruments: list[str], start: str, end: str) -> pd.Series:
    df = pd.read_parquet(NORTHBOUND_PATH).copy()
    df['date'] = pd.to_datetime(df['date'])
    mask = (df['date'] >= pd.Timestamp(start)) & (df['date'] <= pd.Timestamp(end))
    sample = df.loc[mask].sort_values('date')
    flow = sample.set_index('date')['net_buy_total'].astype(float).rolling(5, min_periods=5).sum()
    factor = _normalize_market_series(flow)
    factor.name = 'northbound_flow_5d'
    return factor


def factor_margin_balance_change_5d(instruments: list[str], start: str, end: str) -> pd.Series:
    df = pd.read_parquet(MARGIN_PATH).copy()
    df['date'] = pd.to_datetime(df['date'])
    mask = (df['date'] >= pd.Timestamp(start)) & (df['date'] <= pd.Timestamp(end))
    sample = df.loc[mask].sort_values('date')
    margin_change = -pd.to_numeric(sample.set_index('date')['margin_balance_total'], errors='coerce').pct_change(5, fill_method=None)
    factor = _normalize_market_series(margin_change)
    factor.name = 'margin_balance_change_5d'
    return factor

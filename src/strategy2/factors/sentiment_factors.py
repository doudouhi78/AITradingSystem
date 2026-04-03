from __future__ import annotations

import numpy as np
import pandas as pd

from .auxiliary_factors import calc_turnover_deviation, _normalize_price_frame

EPSILON = 1e-12


def _normalize_top_list(top_list: pd.DataFrame) -> pd.DataFrame:
    frame = top_list.copy()
    frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
    frame['ts_code'] = frame['ts_code'].astype(str).str.upper()
    numeric_cols = [
        'close',
        'pct_change',
        'turnover_rate',
        'amount',
        'l_sell',
        'l_buy',
        'l_amount',
        'net_amount',
        'net_rate',
        'amount_rate',
        'float_values',
    ]
    for column in numeric_cols:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors='coerce')
    return frame.dropna(subset=['trade_date', 'ts_code'])


def _cross_sectional_zscore(frame: pd.DataFrame) -> pd.DataFrame:
    def _zscore(row: pd.Series) -> pd.Series:
        values = pd.to_numeric(row, errors='coerce')
        mean = values.mean()
        std = values.std(ddof=0)
        if pd.isna(std) or std <= EPSILON:
            return values * 0.0
        return (values - mean) / std

    return frame.apply(_zscore, axis=1)


def calc_top_list_sentiment(top_list: pd.DataFrame, stock_basic: pd.DataFrame | None = None, window: int = 20) -> pd.DataFrame:
    frame = _normalize_top_list(top_list)
    daily = frame.groupby(['trade_date', 'ts_code'], as_index=False).agg(
        mention_count=('reason', 'count'),
        net_amount=('net_amount', 'sum'),
        amount_rate=('amount_rate', 'mean'),
        float_values=('float_values', 'last'),
        net_rate=('net_rate', 'mean'),
    )
    daily['net_strength'] = daily['net_amount'].divide(daily['float_values'].where(daily['float_values'].abs() > EPSILON))

    freq = daily.pivot_table(index='trade_date', columns='ts_code', values='mention_count', aggfunc='last').sort_index().fillna(0.0)
    net_strength = daily.pivot_table(index='trade_date', columns='ts_code', values='net_strength', aggfunc='last').sort_index().fillna(0.0)
    amount_rate = daily.pivot_table(index='trade_date', columns='ts_code', values='amount_rate', aggfunc='last').sort_index().fillna(0.0)

    freq_20 = freq.rolling(window, min_periods=1).sum()
    net_strength_20 = net_strength.rolling(window, min_periods=1).sum()
    amount_rate_20 = amount_rate.rolling(window, min_periods=1).mean()

    composite = (
        _cross_sectional_zscore(freq_20).fillna(0.0)
        + _cross_sectional_zscore(net_strength_20).fillna(0.0)
        + _cross_sectional_zscore(amount_rate_20).fillna(0.0)
    ) / 3.0

    result = composite.stack(future_stack=True).rename('sentiment_heat').reset_index()
    result.columns = ['trade_date', 'ts_code', 'sentiment_heat']
    result = result.merge(
        freq_20.stack(future_stack=True).rename('top_list_frequency_20').reset_index(),
        on=['trade_date', 'ts_code'],
        how='left',
    )
    result = result.merge(
        net_strength_20.stack(future_stack=True).rename('net_buy_strength_20').reset_index(),
        on=['trade_date', 'ts_code'],
        how='left',
    )
    result = result.merge(
        amount_rate_20.stack(future_stack=True).rename('amount_rate_20').reset_index(),
        on=['trade_date', 'ts_code'],
        how='left',
    )

    if stock_basic is not None and {'ts_code', 'industry'}.issubset(stock_basic.columns):
        mapping = stock_basic[['ts_code', 'industry']].copy()
        mapping['ts_code'] = mapping['ts_code'].astype(str).str.upper()
        mapping['industry'] = mapping['industry'].fillna('UNKNOWN').astype(str)
        result = result.merge(mapping.drop_duplicates('ts_code'), on='ts_code', how='left')
        result['industry'] = result['industry'].fillna('UNKNOWN')
        sector = result.groupby(['trade_date', 'industry'])['sentiment_heat'].mean().reset_index()
        sector = sector.rename(columns={'sentiment_heat': 'sector_sentiment_heat'})
        result = result.merge(sector, on=['trade_date', 'industry'], how='left')
    return result.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def calc_sector_divergence(price_df: pd.DataFrame, stock_basic: pd.DataFrame, return_window: int = 1, smooth_window: int = 5) -> pd.DataFrame:
    prices = _normalize_price_frame(price_df)
    mapping = stock_basic[['ts_code', 'industry']].copy()
    mapping['ts_code'] = mapping['ts_code'].astype(str).str.upper()
    mapping['industry'] = mapping['industry'].fillna('UNKNOWN').astype(str)
    close = prices.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    returns = close.pct_change(periods=return_window, fill_method=None)
    long_ret = returns.stack(future_stack=True).rename('stock_return').reset_index()
    long_ret.columns = ['trade_date', 'ts_code', 'stock_return']
    merged = long_ret.merge(mapping.drop_duplicates('ts_code'), on='ts_code', how='left')
    merged['industry'] = merged['industry'].fillna('UNKNOWN')
    divergence = merged.groupby(['trade_date', 'industry'])['stock_return'].std(ddof=0).reset_index()
    divergence = divergence.rename(columns={'stock_return': 'sector_divergence_raw'})
    pivot = divergence.pivot_table(index='trade_date', columns='industry', values='sector_divergence_raw', aggfunc='last').sort_index()
    smooth = pivot.rolling(smooth_window, min_periods=1).mean()
    result = smooth.stack(future_stack=True).rename('sector_divergence').reset_index()
    result.columns = ['trade_date', 'industry', 'sector_divergence']
    return result.sort_values(['trade_date', 'industry']).reset_index(drop=True)


def calc_sentiment_price_divergence(
    price_df: pd.DataFrame,
    valuation_df: pd.DataFrame,
    turnover_window: int = 20,
    price_window: int = 10,
    high_turnover_threshold: float = 1.8,
    low_turnover_threshold: float = 0.8,
    weak_price_threshold: float = 0.02,
    strong_price_threshold: float = 0.05,
) -> pd.DataFrame:
    prices = _normalize_price_frame(price_df)
    turnover = calc_turnover_deviation(valuation_df, window=turnover_window)
    close = prices.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index()
    price_return = close.pct_change(periods=price_window, fill_method=None)
    price_return_long = price_return.stack(future_stack=True).rename('price_return').reset_index()
    price_return_long.columns = ['trade_date', 'ts_code', 'price_return']
    merged = turnover.merge(price_return_long, on=['trade_date', 'ts_code'], how='left')
    merged['sentiment_price_divergence'] = merged['turnover_deviation'] - merged['price_return']
    merged['exhaustion_signal'] = (
        (merged['turnover_deviation'] >= high_turnover_threshold)
        & (merged['price_return'].fillna(0.0) <= weak_price_threshold)
    ).astype(int)
    merged['accumulation_signal'] = (
        (merged['turnover_deviation'] <= low_turnover_threshold)
        & (merged['price_return'].fillna(0.0) >= strong_price_threshold)
    ).astype(int)
    return merged.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)

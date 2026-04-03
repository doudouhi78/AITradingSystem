from __future__ import annotations

import pandas as pd

from src.strategy2.factors.sentiment_factors import (
    calc_sector_divergence,
    calc_sentiment_price_divergence,
    calc_top_list_sentiment,
)


def _make_stock_basic() -> pd.DataFrame:
    return pd.DataFrame(
        {
            'ts_code': ['000001.SZ', '000002.SZ', '600001.SH', '600002.SH'],
            'industry': ['科技', '科技', '银行', '银行'],
        }
    )


def test_calc_top_list_sentiment_ranks_hot_names_higher() -> None:
    top_list = pd.DataFrame(
        {
            'trade_date': ['2024-01-02', '2024-01-03', '2024-01-03', '2024-01-02'],
            'ts_code': ['000001.SZ', '000001.SZ', '000001.SZ', '600001.SH'],
            'reason': ['涨幅偏离', '涨幅偏离', '换手偏离', '涨幅偏离'],
            'net_amount': [100_000_000, 120_000_000, 80_000_000, -20_000_000],
            'amount_rate': [20.0, 25.0, 30.0, 5.0],
            'float_values': [1_000_000_000, 1_000_000_000, 1_000_000_000, 1_200_000_000],
            'net_rate': [10.0, 12.0, 8.0, -1.0],
        }
    )
    result = calc_top_list_sentiment(top_list, _make_stock_basic(), window=5)
    latest = result.loc[result['trade_date'] == result['trade_date'].max()].set_index('ts_code')
    assert latest.loc['000001.SZ', 'sentiment_heat'] > latest.loc['600001.SH', 'sentiment_heat']
    assert 'sector_sentiment_heat' in result.columns


def test_calc_sector_divergence_detects_more_dispersion() -> None:
    dates = pd.date_range('2024-01-01', periods=8, freq='B')
    rows = []
    tech_a = [10, 10.1, 10.4, 10.2, 10.8, 10.3, 11.0, 10.4]
    tech_b = [10, 9.9, 9.6, 9.8, 9.1, 9.7, 9.0, 9.6]
    bank_a = [10, 10.02, 10.04, 10.06, 10.08, 10.10, 10.12, 10.14]
    bank_b = [10, 10.01, 10.03, 10.05, 10.07, 10.09, 10.11, 10.13]
    series_map = {
        '000001.SZ': tech_a,
        '000002.SZ': tech_b,
        '600001.SH': bank_a,
        '600002.SH': bank_b,
    }
    for code, closes in series_map.items():
        for date, close in zip(dates, closes, strict=True):
            rows.append({'trade_date': date, 'ts_code': code, 'close': close})
    divergence = calc_sector_divergence(pd.DataFrame(rows), _make_stock_basic(), return_window=1, smooth_window=3)
    latest = divergence.loc[divergence['trade_date'] == divergence['trade_date'].max()].set_index('industry')
    assert latest.loc['科技', 'sector_divergence'] > latest.loc['银行', 'sector_divergence']


def test_calc_sentiment_price_divergence_flags_exhaustion_and_accumulation() -> None:
    dates = pd.date_range('2024-01-01', periods=25, freq='B')
    price_rows = []
    valuation_rows = []
    for idx, date in enumerate(dates):
        price_rows.append({'trade_date': date, 'ts_code': '000001.SZ', 'close': 10.0 + min(idx, 10) * 0.01})
        price_rows.append({'trade_date': date, 'ts_code': '600001.SH', 'close': 10.0 + idx * 0.08})
        valuation_rows.append({'date': date, 'instrument_code': '000001', 'turnover_rate': 1.0 if idx < 24 else 3.0})
        valuation_rows.append({'date': date, 'instrument_code': '600001', 'turnover_rate': 1.0 if idx < 24 else 0.4})
    result = calc_sentiment_price_divergence(pd.DataFrame(price_rows), pd.DataFrame(valuation_rows), turnover_window=5, price_window=10)
    latest = result.loc[result['trade_date'] == result['trade_date'].max()].set_index('ts_code')
    assert latest.loc['000001.SZ', 'exhaustion_signal'] == 1
    assert latest.loc['600001.SH', 'accumulation_signal'] == 1
    assert 'sentiment_price_divergence' in result.columns

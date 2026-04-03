from __future__ import annotations

import numpy as np
import pandas as pd

from src.strategy2.factors.auxiliary_factors import calc_bias, calc_ema_slope, calc_turnover_deviation, calc_volume_zscore
from src.strategy2.factors.rps_factors import calc_sector_concentration, calc_sector_rps_approx, calc_stock_rps


def _make_price_frame() -> pd.DataFrame:
    dates = pd.date_range('2023-01-01', periods=140, freq='B')
    rows = []
    configs = {
        '000001.SZ': ('Tech', 10.0, 0.40, 1000.0),
        '000002.SZ': ('Tech', 10.0, 0.20, 900.0),
        '600001.SH': ('Bank', 10.0, -0.05, 800.0),
        '600002.SH': ('Bank', 10.0, 0.01, 700.0),
    }
    for code, (_, base, slope, volume) in configs.items():
        for idx, date in enumerate(dates):
            close = base + slope * idx
            rows.append({
                'ts_code': code,
                'trade_date': date,
                'close': close,
                'volume': volume + idx * 5,
            })
    return pd.DataFrame(rows)


def _make_stock_basic() -> pd.DataFrame:
    return pd.DataFrame(
        {
            'ts_code': ['000001.SZ', '000002.SZ', '600001.SH', '600002.SH'],
            'industry': ['Tech', 'Tech', 'Bank', 'Bank'],
        }
    )


def test_calc_stock_rps_orders_cross_section() -> None:
    frame = _make_price_frame()
    result = calc_stock_rps(frame)
    assert {'trade_date', 'ts_code', 'rps_20', 'rps_60', 'rps_120'}.issubset(result.columns)
    latest = result.loc[result['trade_date'] == result['trade_date'].max()].set_index('ts_code')
    assert latest.loc['000001.SZ', 'rps_20'] > latest.loc['000002.SZ', 'rps_20']
    assert latest.loc['600001.SH', 'rps_20'] < latest.loc['600002.SH', 'rps_20']
    assert latest['rps_120'].between(0, 100).all()


def test_calc_sector_rps_and_concentration() -> None:
    prices = _make_price_frame()
    stock_basic = _make_stock_basic()
    sector = calc_sector_rps_approx(prices, stock_basic)
    concentration = calc_sector_concentration(prices, stock_basic, window=20)

    latest_sector = sector.loc[sector['trade_date'] == sector['trade_date'].max()].set_index('industry')
    assert latest_sector.loc['Tech', 'sector_rps_20'] > latest_sector.loc['Bank', 'sector_rps_20']

    latest_concentration = concentration.loc[concentration['trade_date'] == concentration['trade_date'].max()].set_index('industry')
    assert latest_concentration['concentration_ratio'].between(0, 1).all()
    assert latest_concentration.loc['Tech', 'concentration_ratio'] < latest_concentration.loc['Bank', 'concentration_ratio']


def test_auxiliary_factors_generate_expected_columns() -> None:
    prices = _make_price_frame()
    volume = calc_volume_zscore(prices, window=20)
    ema = calc_ema_slope(prices, short=5, long=20)
    bias = calc_bias(prices, window=20)

    valuation_rows = []
    for code in ['000001', '600001']:
        for date in pd.date_range('2023-01-01', periods=30, freq='B'):
            valuation_rows.append({'date': date, 'instrument_code': code, 'turnover_rate': 1.0 + (date.day % 5)})
    turnover = calc_turnover_deviation(pd.DataFrame(valuation_rows), window=5)

    assert 'volume_zscore' in volume.columns
    assert 'ema_slope' in ema.columns
    assert 'bias' in bias.columns
    assert 'turnover_deviation' in turnover.columns
    assert turnover['ts_code'].str.endswith(('.SZ', '.SH', '.BJ')).all()

    latest_ema = ema.loc[ema['trade_date'] == ema['trade_date'].max()].set_index('ts_code')
    assert latest_ema.loc['000001.SZ', 'ema_slope'] > 0
    latest_bias = bias.loc[bias['trade_date'] == bias['trade_date'].max()].set_index('ts_code')
    assert latest_bias.loc['000001.SZ', 'bias'] > latest_bias.loc['600001.SH', 'bias']

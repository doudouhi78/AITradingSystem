from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.strategy2.execution.position_tools import calc_atr, calc_chandelier_exit, calc_atr_position_size
from src.strategy2.factors.volume_factors import calc_volume_zscore, calc_turnover_deviation, calc_bias
from src.strategy2.factors.moneyflow_strategy2 import calc_large_order_net_ratio, calc_super_large_net_flow, evaluate_moneyflow_ic


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_calc_atr_uses_seed_mean_then_ewm() -> None:
    df = pd.DataFrame(
        {
            'high': [10, 11, 12, 13, 14],
            'low': [8, 9, 10, 11, 12],
            'close': [9, 10, 11, 12, 13],
        }
    )
    atr = calc_atr(df, window=3)
    assert pd.isna(atr.iloc[0])
    assert pd.isna(atr.iloc[1])
    assert atr.iloc[2] == pytest.approx(2.0)
    assert atr.iloc[3] == pytest.approx(2.0)
    assert atr.iloc[4] == pytest.approx(2.0)


def test_calc_chandelier_exit_only_moves_up() -> None:
    df = pd.DataFrame(
        {
            'high': list(range(10, 35)),
            'low': list(range(8, 33)),
            'close': list(range(9, 34)),
        }
    )
    stop = calc_chandelier_exit(df, atr_window=3, k=1.0)
    valid = stop.dropna()
    assert not valid.empty
    assert valid.diff().dropna().ge(0).all()


def test_calc_atr_position_size_respects_cap() -> None:
    lots = calc_atr_position_size(total_capital=50_000, risk_ratio=0.02, atr=1.5, k=3.0)
    assert lots == 2


def test_volume_related_factors() -> None:
    df = pd.DataFrame(
        {
            'close': range(1, 26),
            'volume': list(range(100, 2600, 100)),
        }
    )
    z = calc_volume_zscore(df, window=5)
    bias = calc_bias(df, window=5)
    assert z.iloc[-1] > 0
    assert bias.iloc[-1] > 0


def test_turnover_deviation_normalizes_instrument_code() -> None:
    df = pd.DataFrame(
        {
            'date': pd.date_range('2024-01-01', periods=25, freq='B').tolist() * 2,
            'instrument_code': ['000001'] * 25 + ['600000'] * 25,
            'turnover_rate': [1.0] * 24 + [2.0] + [2.0] * 24 + [4.0],
        }
    )
    result = calc_turnover_deviation(df, window=20)
    last_rows = result.dropna().groupby('ts_code').tail(1)
    by_code = dict(zip(last_rows['ts_code'], last_rows['turnover_deviation']))
    assert by_code['000001.SZ'] == pytest.approx(2.0 / 1.05, rel=1e-6)
    assert by_code['600000.SH'] == pytest.approx(4.0 / 2.1, rel=1e-6)


def test_moneyflow_factor_formulas() -> None:
    df = pd.DataFrame(
        {
            'ts_code': ['000001.SZ'],
            'trade_date': ['20240102'],
            'buy_lg_amount': [10.0],
            'buy_elg_amount': [30.0],
            'sell_lg_amount': [5.0],
            'sell_elg_amount': [15.0],
        }
    )
    ratio = calc_large_order_net_ratio(df)
    flow = calc_super_large_net_flow(df)
    assert ratio['value'].iloc[0] == pytest.approx(0.3333333333, rel=1e-6)
    assert flow['value'].iloc[0] == pytest.approx(15.0)


def test_evaluate_moneyflow_ic_with_mock_market_data(tmp_path) -> None:
    market_dir = tmp_path / 'runtime' / 'market_data' / 'cn_stock'
    dates = pd.date_range('2024-01-01', periods=8, freq='B')
    upward = pd.DataFrame({'trade_date': dates, 'close': [10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5]})
    downward = pd.DataFrame({'trade_date': dates, 'close': [13.5, 13, 12.5, 12, 11.5, 11, 10.5, 10]})
    _write_parquet(market_dir / '000001.parquet', upward)
    _write_parquet(market_dir / '000002.parquet', downward)

    factor = pd.DataFrame(
        {
            'trade_date': list(dates[:3]) * 2,
            'ts_code': ['000001.SZ'] * 3 + ['000002.SZ'] * 3,
            'value': [1.0, 1.0, 1.0, -1.0, -1.0, -1.0],
        }
    )
    metrics = evaluate_moneyflow_ic(factor, market_data_dir=market_dir, forward_days=5, min_cross_section=2)
    assert metrics['sample_count'] >= 1
    assert metrics['ic_mean'] > 0

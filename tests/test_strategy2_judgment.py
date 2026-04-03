from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.strategy2.factors.crowding_factor import compute_crowding_features, evaluate_crowding_ic
from src.strategy2.judgment_layer import MarketJudgmentLayer


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_compute_crowding_features_builds_shares_and_percentiles(tmp_path) -> None:
    market_dir = tmp_path / 'runtime' / 'market_data' / 'cn_stock'
    stock_basic_path = tmp_path / 'runtime' / 'fundamental_data' / 'stock_basic.parquet'
    dates = pd.date_range('2024-01-01', periods=8, freq='B')

    _write_parquet(
        stock_basic_path,
        pd.DataFrame([
            {'symbol': '000001', 'industry': '银行'},
            {'symbol': '000002', 'industry': '银行'},
        ]),
    )
    _write_parquet(market_dir / '000001.parquet', pd.DataFrame({'trade_date': dates, 'close': range(10, 18), 'amount': [100, 110, 120, 130, 140, 150, 160, 170]}))
    _write_parquet(market_dir / '000002.parquet', pd.DataFrame({'trade_date': dates, 'close': range(18, 10, -1), 'amount': [50, 55, 60, 65, 70, 75, 80, 85]}))

    features = compute_crowding_features(['000001', '000002'], market_data_dir=market_dir, stock_basic_path=stock_basic_path, percentile_window=3, min_periods=2)
    assert {'stock_share', 'industry_share', 'stock_percentile', 'industry_percentile', 'crowding_score'}.issubset(features.columns)
    latest = features.loc[features['trade_date'] == features['trade_date'].max()]
    assert latest['stock_share'].sum() == 1.0
    assert latest['industry_share'].iloc[0] == 1.0


def test_evaluate_crowding_ic_returns_negative_direction_on_mock_data(tmp_path) -> None:
    market_dir = tmp_path / 'runtime' / 'market_data' / 'cn_stock'
    stock_basic_path = tmp_path / 'runtime' / 'fundamental_data' / 'stock_basic.parquet'
    dates = pd.date_range('2024-01-01', periods=12, freq='B')
    _write_parquet(stock_basic_path, pd.DataFrame([
        {'symbol': '000001', 'industry': '科技'},
        {'symbol': '000002', 'industry': '科技'},
    ]))
    _write_parquet(market_dir / '000001.parquet', pd.DataFrame({'trade_date': dates, 'close': [10, 10.2, 10.4, 10.6, 10.8, 10.7, 10.5, 10.3, 10.1, 9.9, 9.7, 9.5], 'amount': [200, 220, 240, 260, 280, 300, 320, 340, 360, 380, 400, 420]}))
    _write_parquet(market_dir / '000002.parquet', pd.DataFrame({'trade_date': dates, 'close': [10, 9.8, 9.6, 9.4, 9.2, 9.3, 9.5, 9.7, 9.9, 10.1, 10.3, 10.5], 'amount': [100, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45]}))
    features = compute_crowding_features(['000001', '000002'], market_data_dir=market_dir, stock_basic_path=stock_basic_path, percentile_window=3, min_periods=2)
    report = evaluate_crowding_ic(features, windows=(5,), factor_col='stock_share', min_cross_section=2)
    assert report['5']['direction'] == 'negative'


def test_market_judgment_layer_can_query_and_measure_coverage(tmp_path) -> None:
    path = tmp_path / 'runtime' / 'index_data' / 'index_daily.parquet'
    dates = pd.date_range('2024-01-01', periods=80, freq='B')
    close = [100.0] * 60 + [90.0 - i for i in range(20)]
    _write_parquet(path, pd.DataFrame({'ts_code': ['000300.SH'] * len(dates), 'trade_date': dates, 'close': close}))
    layer = MarketJudgmentLayer.from_parquet(path=path, start='2024-01-01', end='2024-12-31')
    assert layer.can_enter(str(dates[59].date())) is True
    assert layer.can_enter(str(dates[-1].date())) is False
    coverage = layer.evaluate_drawdown_coverage()
    assert coverage.drawdown_coverage_ratio >= 0.5

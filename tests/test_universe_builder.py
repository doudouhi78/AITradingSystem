from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.data_pipeline import market_classifier, universe_builder



def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)



def test_get_universe_applies_all_filters(tmp_path, monkeypatch) -> None:
    classification_dir = tmp_path / 'runtime' / 'classification_data'
    fundamental_dir = tmp_path / 'runtime' / 'fundamental_data'
    market_dir = tmp_path / 'runtime' / 'market_data' / 'cn_stock'
    index_dir = classification_dir / 'index_components'

    trade_days = pd.date_range('2022-01-03', '2023-06-01', freq='B')
    _write_parquet(
        fundamental_dir / 'trade_cal.parquet',
        pd.DataFrame({'cal_date': trade_days, 'is_open': 1}),
    )
    _write_parquet(
        fundamental_dir / 'stock_basic.parquet',
        pd.DataFrame([
            {'symbol': '000001', 'list_date': '2020-01-01', 'delist_date': None},
            {'symbol': '000002', 'list_date': '2020-01-01', 'delist_date': None},
            {'symbol': '000003', 'list_date': '2023-05-15', 'delist_date': None},
            {'symbol': '000004', 'list_date': '2020-01-01', 'delist_date': '2023-05-31'},
            {'symbol': '000005', 'list_date': '2020-01-01', 'delist_date': None},
            {'symbol': '000006', 'list_date': '2020-01-01', 'delist_date': None},
        ]),
    )
    _write_parquet(
        fundamental_dir / 'st_history.parquet',
        pd.DataFrame([
            {'ts_code': '000002.SZ', 'start_date': '2023-01-01', 'end_date': None},
        ]),
    )
    _write_parquet(
        fundamental_dir / 'suspend.parquet',
        pd.DataFrame([
            {'ts_code': '000005.SZ', 'trade_date': '2023-06-01'},
        ]),
    )
    _write_parquet(
        index_dir / 'csi300_latest.parquet',
        pd.DataFrame({'instrument_code': ['000001', '000002', '000003', '000004', '000005', '000006']}),
    )
    _write_parquet(
        market_dir / '000001.parquet',
        pd.DataFrame([
            {'trade_date': '2023-05-30', 'close': 10.0, 'volume': 100},
            {'trade_date': '2023-05-31', 'close': 10.5, 'volume': 100},
        ]),
    )
    _write_parquet(
        market_dir / '000006.parquet',
        pd.DataFrame([
            {'trade_date': '2023-05-30', 'close': 10.0, 'volume': 100},
            {'trade_date': '2023-05-31', 'close': 11.1, 'volume': 100},
        ]),
    )

    monkeypatch.setattr(universe_builder, 'CLASSIFICATION_DIR', classification_dir)
    monkeypatch.setattr(universe_builder, 'FUNDAMENTAL_DIR', fundamental_dir)
    monkeypatch.setattr(universe_builder, 'MARKET_DATA_DIR', market_dir)
    monkeypatch.setattr(universe_builder, 'INDEX_COMPONENT_DIR', index_dir)
    monkeypatch.setattr(universe_builder, 'INDEX_HISTORY_DIR', tmp_path / 'runtime' / 'index_data' / 'index_components')
    monkeypatch.setattr(universe_builder, 'STOCK_BASIC_PATH', fundamental_dir / 'stock_basic.parquet')
    monkeypatch.setattr(universe_builder, 'ST_HISTORY_PATH', fundamental_dir / 'st_history.parquet')
    monkeypatch.setattr(universe_builder, 'SUSPEND_PATH', fundamental_dir / 'suspend.parquet')
    monkeypatch.setattr(universe_builder, 'TRADE_CAL_PATH', fundamental_dir / 'trade_cal.parquet')
    monkeypatch.setattr(universe_builder, 'LIMIT_LIST_PATH', fundamental_dir / 'limit_list.parquet')
    monkeypatch.setattr(universe_builder, 'UNIVERSE_FILES', {
        'csi300': [index_dir / 'csi300_latest.parquet'],
        'csi500': [],
        'csi1000': [],
    })
    universe_builder.clear_caches()

    result = universe_builder.get_universe('2023-06-01', 'csi300')
    assert result == ['000001']



def test_get_universe_falls_back_when_csi1000_missing(tmp_path, monkeypatch) -> None:
    fundamental_dir = tmp_path / 'runtime' / 'fundamental_data'
    _write_parquet(
        fundamental_dir / 'stock_basic.parquet',
        pd.DataFrame([
            {'symbol': '000001', 'list_date': '2020-01-01', 'delist_date': None},
            {'symbol': '000002', 'list_date': '2020-01-01', 'delist_date': None},
        ]),
    )
    _write_parquet(
        fundamental_dir / 'trade_cal.parquet',
        pd.DataFrame({'cal_date': pd.date_range('2022-01-03', '2023-06-01', freq='B'), 'is_open': 1}),
    )
    monkeypatch.setattr(universe_builder, 'FUNDAMENTAL_DIR', fundamental_dir)
    monkeypatch.setattr(universe_builder, 'MARKET_DATA_DIR', tmp_path / 'runtime' / 'market_data' / 'cn_stock')
    monkeypatch.setattr(universe_builder, 'STOCK_BASIC_PATH', fundamental_dir / 'stock_basic.parquet')
    monkeypatch.setattr(universe_builder, 'ST_HISTORY_PATH', fundamental_dir / 'st_history.parquet')
    monkeypatch.setattr(universe_builder, 'SUSPEND_PATH', fundamental_dir / 'suspend.parquet')
    monkeypatch.setattr(universe_builder, 'TRADE_CAL_PATH', fundamental_dir / 'trade_cal.parquet')
    monkeypatch.setattr(universe_builder, 'LIMIT_LIST_PATH', fundamental_dir / 'limit_list.parquet')
    monkeypatch.setattr(universe_builder, 'UNIVERSE_FILES', {'csi300': [], 'csi500': [], 'csi1000': []})
    universe_builder.clear_caches()

    with pytest.warns(UserWarning):
        result = universe_builder.get_universe('2023-06-01', 'csi1000')
    assert result == ['000001', '000002']



def test_market_classifier_returns_industry_and_size_labels(tmp_path, monkeypatch) -> None:
    classification_dir = tmp_path / 'runtime' / 'classification_data'
    fundamental_dir = tmp_path / 'runtime' / 'fundamental_data'
    _write_parquet(
        classification_dir / 'industry_sw2.parquet',
        pd.DataFrame([
            {'instrument_code': '000001', 'industry_name': '银行'},
            {'instrument_code': '000002', 'industry_name': '地产'},
            {'instrument_code': '000003', 'industry_name': '医药'},
        ]),
    )
    _write_parquet(
        fundamental_dir / 'valuation_daily.parquet',
        pd.DataFrame([
            {'date': '2023-06-01', 'instrument_code': '000001', 'circ_mv': 300.0},
            {'date': '2023-06-01', 'instrument_code': '000002', 'circ_mv': 200.0},
            {'date': '2023-06-01', 'instrument_code': '000003', 'circ_mv': 100.0},
        ]),
    )

    monkeypatch.setattr(market_classifier, 'CLASSIFICATION_DIR', classification_dir)
    monkeypatch.setattr(market_classifier, 'FUNDAMENTAL_DIR', fundamental_dir)
    monkeypatch.setattr(market_classifier, 'INDUSTRY_PATH', classification_dir / 'industry_sw2.parquet')
    monkeypatch.setattr(market_classifier, 'VALUATION_PATH', fundamental_dir / 'valuation_daily.parquet')
    monkeypatch.setattr(market_classifier, 'STOCK_META_PATH', classification_dir / 'stock_meta.parquet')
    monkeypatch.setattr(market_classifier, 'MARKET_DATA_DIR', tmp_path / 'runtime' / 'market_data' / 'cn_stock')

    industries = market_classifier.get_industry_labels('2023-06-01', ['000001', '000002', '000003'])
    sizes = market_classifier.get_size_labels('2023-06-01', ['000001', '000002', '000003'])

    assert industries == {'000001': '银行', '000002': '地产', '000003': '医药'}
    assert sizes == {'000001': 'large', '000002': 'mid', '000003': 'small'}

from __future__ import annotations

import pandas as pd
import pytest

from data_pipeline.qlib_data_adapter import QlibDataAdapter


def _write_mock_prices(path, symbols: list[str], periods: int = 100) -> None:
    dates = pd.date_range('2024-01-01', periods=periods, freq='B')
    rows = []
    for idx, symbol in enumerate(symbols, start=1):
        for offset, trade_date in enumerate(dates, start=1):
            base = idx * 10 + offset
            rows.append({
                'trade_date': trade_date,
                'ts_code': symbol,
                'open': float(base),
                'high': float(base + 1),
                'low': float(base - 1),
                'close': float(base + 0.5),
                'volume': float(base * 1000),
                'amount': float(base * 10000),
            })
    pd.DataFrame(rows).to_parquet(path, index=False)



def test_load_price_data_outputs_qlib_format(tmp_path) -> None:
    data_dir = tmp_path / 'prices'
    data_dir.mkdir()
    _write_mock_prices(data_dir / 'mock_prices.parquet', [f'{i:06d}.SZ' for i in range(1, 11)])

    adapter = QlibDataAdapter(str(data_dir))
    frame = adapter.load_price_data('2024-01-01', '2024-05-31')

    assert frame.index.names == ['datetime', 'instrument']
    assert {'$open', '$high', '$low', '$close', '$volume', '$amount'} <= set(frame.columns)
    assert frame.index.get_level_values('instrument')[0] == 'SZ000001'



def test_load_price_data_supports_stock_filter_and_round_trip(tmp_path) -> None:
    data_dir = tmp_path / 'prices'
    data_dir.mkdir()
    _write_mock_prices(data_dir / 'mock_prices.parquet', ['600519.SH', '000001.SZ', '300750.SZ'], periods=8)

    adapter = QlibDataAdapter(str(data_dir))
    frame = adapter.load_price_data('2024-01-01', '2024-01-31', stock_list=['SH600519', '000001.SZ'])

    instruments = set(frame.index.get_level_values('instrument'))
    assert instruments == {'SH600519', 'SZ000001'}
    restored = adapter.to_tushare_frame(frame)
    assert set(restored['ts_code']) == {'600519.SH', '000001.SZ'}



def test_get_label_uses_t_plus_1_open_to_future_open(tmp_path) -> None:
    data_dir = tmp_path / 'prices'
    data_dir.mkdir()
    dates = pd.date_range('2024-01-01', periods=5, freq='B')
    frame = pd.DataFrame([
        {'trade_date': dates[0], 'ts_code': '000001.SZ', 'open': 10, 'high': 10, 'low': 10, 'close': 10, 'volume': 1, 'amount': 1},
        {'trade_date': dates[1], 'ts_code': '000001.SZ', 'open': 11, 'high': 11, 'low': 11, 'close': 11, 'volume': 1, 'amount': 1},
        {'trade_date': dates[2], 'ts_code': '000001.SZ', 'open': 13, 'high': 13, 'low': 13, 'close': 13, 'volume': 1, 'amount': 1},
        {'trade_date': dates[3], 'ts_code': '000001.SZ', 'open': 14, 'high': 14, 'low': 14, 'close': 14, 'volume': 1, 'amount': 1},
        {'trade_date': dates[4], 'ts_code': '000001.SZ', 'open': 15, 'high': 15, 'low': 15, 'close': 15, 'volume': 1, 'amount': 1},
        {'trade_date': dates[0], 'ts_code': '600519.SH', 'open': 20, 'high': 20, 'low': 20, 'close': 20, 'volume': 1, 'amount': 1},
        {'trade_date': dates[1], 'ts_code': '600519.SH', 'open': 20, 'high': 20, 'low': 20, 'close': 20, 'volume': 1, 'amount': 1},
        {'trade_date': dates[2], 'ts_code': '600519.SH', 'open': 19, 'high': 19, 'low': 19, 'close': 19, 'volume': 1, 'amount': 1},
        {'trade_date': dates[3], 'ts_code': '600519.SH', 'open': 18, 'high': 18, 'low': 18, 'close': 18, 'volume': 1, 'amount': 1},
        {'trade_date': dates[4], 'ts_code': '600519.SH', 'open': 17, 'high': 17, 'low': 17, 'close': 17, 'volume': 1, 'amount': 1},
    ])
    frame.to_parquet(data_dir / 'prices.parquet', index=False)

    adapter = QlibDataAdapter(str(data_dir))
    price_df = adapter.load_price_data('2024-01-01', '2024-01-31')
    label = adapter.get_label(price_df, forward_days=1)

    day0_a = label.loc[(dates[0], 'SZ000001')]
    day0_b = label.loc[(dates[0], 'SH600519')]
    assert day0_a == 1.0
    assert day0_b == 0.5



def test_load_price_data_raises_clear_error_when_missing(tmp_path) -> None:
    adapter = QlibDataAdapter(str(tmp_path / 'missing_dir'))
    with pytest.raises(FileNotFoundError, match='data directory not found'):
        adapter.load_price_data('2024-01-01', '2024-01-31')

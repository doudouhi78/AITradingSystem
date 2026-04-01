from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline.tushare_downloader import TushareClient, _standardize_stock_daily


class DummyClient(TushareClient):
    def __init__(self) -> None:
        pass


def test_query_falls_back_to_native(monkeypatch) -> None:
    client = DummyClient()

    def fail_relay(api_name, params, fields=''):
        raise RuntimeError('relay_down')

    def ok_native(api_name, params, fields=''):
        return pd.DataFrame([{'ts_code': '000001.SZ', 'trade_date': '20260330'}])

    monkeypatch.setattr(client, '_relay_query', fail_relay)
    monkeypatch.setattr(client, '_native_query', ok_native)
    frame = TushareClient.query(client, 'daily', {'trade_date': '20260330'}, use_relay=True, retries=1)
    assert len(frame) == 1
    assert frame.iloc[0]['ts_code'] == '000001.SZ'


def test_standardize_stock_daily() -> None:
    daily = pd.DataFrame(
        [
            {'ts_code': '000001.SZ', 'trade_date': '20260330', 'open': '10', 'high': '11', 'low': '9.8', 'close': '10.5', 'vol': '1000', 'amount': '12000'},
            {'ts_code': '000001.SZ', 'trade_date': '20260331', 'open': '10.5', 'high': '11.2', 'low': '10.1', 'close': '11', 'vol': '1100', 'amount': '13000'},
        ]
    )
    stock_basic = pd.DataFrame([{'ts_code': '000001.SZ', 'symbol': '000001', 'list_date': '19910403', 'delist_date': None}])
    bars = _standardize_stock_daily(daily, stock_basic)
    assert list(bars.columns) == ['market', 'symbol', 'security_type', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustment_mode', 'is_suspended', 'listed_date', 'delisted_date']
    assert bars['symbol'].iloc[0] == '000001'
    assert bars['trade_date'].tolist() == ['2026-03-30', '2026-03-31']
    assert bars['adjustment_mode'].iloc[0] == 'none'

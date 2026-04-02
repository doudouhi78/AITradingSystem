from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.alpha_research.stock_filter import StockFilter


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_stock_filter_applies_all_rules(tmp_path) -> None:
    fundamental_dir = tmp_path / "runtime" / "fundamental_data"
    market_dir = tmp_path / "runtime" / "market_data" / "cn_stock"
    trade_dates = pd.date_range("2023-05-05", "2024-05-31", freq="B")

    _write_parquet(
        fundamental_dir / "stock_basic.parquet",
        pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "平安银行", "list_date": "1991-04-03"},
                {"ts_code": "000002.SZ", "name": "*ST测试", "list_date": "1991-04-03"},
                {"ts_code": "000003.SZ", "name": "次新股", "list_date": "2024-04-01"},
                {"ts_code": "000004.SZ", "name": "低流动", "list_date": "1991-04-03"},
                {"ts_code": "000005.SZ", "name": "停牌股", "list_date": "1991-04-03"},
            ]
        ),
    )

    liquid = pd.DataFrame(
        {
            "trade_date": trade_dates,
            "volume": [1_000_000.0] * len(trade_dates),
            "amount": [100_000_000.0] * len(trade_dates),
        }
    )
    low_liq = liquid.copy()
    low_liq["amount"] = 10_000_000.0
    halted = liquid.copy()
    halted.loc[halted["trade_date"] == pd.Timestamp("2024-05-31"), ["volume", "amount"]] = 0.0
    new_listing = liquid.loc[liquid["trade_date"] >= pd.Timestamp("2024-04-01")].copy()

    _write_parquet(market_dir / "000001.parquet", liquid)
    _write_parquet(market_dir / "000002.parquet", liquid)
    _write_parquet(market_dir / "000003.parquet", new_listing)
    _write_parquet(market_dir / "000004.parquet", low_liq)
    _write_parquet(market_dir / "000005.parquet", halted)

    stock_filter = StockFilter(data_dir=fundamental_dir, market_data_dir=market_dir)
    selected, reasons = stock_filter.filter(
        ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"],
        trade_date="2024-05-31",
        explain=True,
    )

    assert selected == ["000001.SZ"]
    assert reasons == {
        "000002.SZ": ["st"],
        "000003.SZ": ["new_listing"],
        "000004.SZ": ["low_liquidity"],
        "000005.SZ": ["halted"],
    }


def test_stock_filter_degrades_when_data_missing(tmp_path) -> None:
    fundamental_dir = tmp_path / "runtime" / "fundamental_data"
    market_dir = tmp_path / "runtime" / "market_data" / "cn_stock"

    _write_parquet(
        fundamental_dir / "stock_basic.parquet",
        pd.DataFrame([{"ts_code": "000001.SZ", "list_date": "1991-04-03"}]),
    )
    _write_parquet(
        market_dir / "000001.parquet",
        pd.DataFrame(
            [
                {"trade_date": "2024-05-30", "close": 10.0},
                {"trade_date": "2024-05-31", "close": 10.2},
            ]
        ),
    )

    stock_filter = StockFilter(data_dir=fundamental_dir, market_data_dir=market_dir)
    selected, reasons = stock_filter.filter(["000001.SZ"], trade_date="2024-05-31", min_list_days=0, explain=True)

    assert selected == ["000001.SZ"]
    assert reasons == {}


def test_stock_filter_can_disable_rules(tmp_path) -> None:
    fundamental_dir = tmp_path / "runtime" / "fundamental_data"
    market_dir = tmp_path / "runtime" / "market_data" / "cn_stock"
    trade_dates = pd.date_range("2024-05-01", "2024-05-31", freq="B")

    _write_parquet(
        fundamental_dir / "stock_basic.parquet",
        pd.DataFrame([{"ts_code": "000002.SZ", "name": "*ST测试", "list_date": "2024-05-01"}]),
    )
    _write_parquet(
        market_dir / "000002.parquet",
        pd.DataFrame({"trade_date": trade_dates, "volume": [0.0] * len(trade_dates), "amount": [0.0] * len(trade_dates)}),
    )

    stock_filter = StockFilter(data_dir=fundamental_dir, market_data_dir=market_dir)
    selected = stock_filter.filter(
        ["000002.SZ"],
        trade_date="2024-05-31",
        exclude_st=False,
        min_list_days=0,
        min_avg_amount=0,
        exclude_halt=False,
    )

    assert selected == ["000002.SZ"]

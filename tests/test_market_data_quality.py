from pathlib import Path

import pytest

pytest.importorskip("pandera", reason="pandera not installed; skipping market data quality tests")

import pandas as pd

from ai_dev_os.market_data_quality import validate_market_frame, validate_market_pool


def test_validate_market_frame_passes():
    frame = pd.DataFrame(
        [
            {
                "market": "CN",
                "symbol": "510300",
                "security_type": "etf",
                "trade_date": "2024-01-02",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100.0,
                "amount": 1000.0,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "",
                "delisted_date": "",
            },
            {
                "market": "CN",
                "symbol": "510300",
                "security_type": "etf",
                "trade_date": "2024-01-03",
                "open": 1.05,
                "high": 1.2,
                "low": 1.0,
                "close": 1.1,
                "volume": 120.0,
                "amount": 1200.0,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "",
                "delisted_date": "",
            },
        ]
    )
    result = validate_market_frame(frame, expected_market="CN", expected_security_type="etf", warmup_rows=0)
    assert result["status"] == "passed"


def test_validate_market_frame_fails_on_bounds():
    frame = pd.DataFrame(
        [
            {
                "market": "CN",
                "symbol": "510300",
                "security_type": "etf",
                "trade_date": "2024-01-02",
                "open": 1.0,
                "high": 0.8,
                "low": 0.9,
                "close": 1.05,
                "volume": 100.0,
                "amount": 1000.0,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "",
                "delisted_date": "",
            }
        ]
    )
    result = validate_market_frame(frame, expected_market="CN", expected_security_type="etf", warmup_rows=0)
    assert result["status"] == "failed"
    assert "price_bounds_consistent" in result["checks_failed"]


def test_validate_market_pool(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "cn_etf"
    data_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "symbol": "510300",
                "security_type": "etf",
                "trade_date": "2024-01-02",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100.0,
                "amount": 1000.0,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "",
                "delisted_date": "",
            }
        ]
    ).to_parquet(data_dir / "510300.parquet", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "symbol": "159915",
                "security_type": "etf",
                "trade_date": "2024-01-02",
                "open": 1.0,
                "high": 0.8,
                "low": 0.9,
                "close": 1.05,
                "volume": 100.0,
                "amount": 1000.0,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "",
                "delisted_date": "",
            }
        ]
    ).to_parquet(data_dir / "159915.parquet", index=False)

    monkeypatch.setattr(
        "ai_dev_os.market_data_quality.POOL_CONFIGS",
        {"cn_etf": type("Cfg", (), {"pool_name": "cn_etf", "market": "CN", "security_type": "etf", "data_dir": data_dir})()},
    )
    monkeypatch.setattr("ai_dev_os.market_data_quality.QUALITY_ROOT", tmp_path / "quality")

    result = validate_market_pool("cn_etf", warmup_rows=0)
    assert result["total_files"] == 2
    assert result["success_count"] == 1
    assert result["failed_count"] == 1
    assert Path(result["summary_path"]).exists()

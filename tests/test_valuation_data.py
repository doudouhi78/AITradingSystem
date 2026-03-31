from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\AITradingSystem")
VALUATION_PATH = ROOT / "runtime" / "fundamental_data" / "valuation_daily.parquet"


def test_coverage() -> None:
    assert VALUATION_PATH.exists(), f"missing valuation file: {VALUATION_PATH}"
    df = pd.read_parquet(VALUATION_PATH, columns=["instrument_code"])
    assert df["instrument_code"].nunique() > 1000


def test_time_range() -> None:
    df = pd.read_parquet(VALUATION_PATH, columns=["date"])
    df["date"] = pd.to_datetime(df["date"])
    assert df["date"].min() <= pd.Timestamp("2016-01-10")


def test_pb_range() -> None:
    df = pd.read_parquet(VALUATION_PATH, columns=["pb"])
    median_pb = pd.to_numeric(df["pb"], errors="coerce").dropna().median()
    assert 0.5 <= median_pb <= 20

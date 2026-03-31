from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline.fundamental_loader import (
    FINANCIAL_DIR,
    VALUATION_PATH,
    get_latest_financial,
)

ROOT = Path(r"D:\AITradingSystem")


def test_valuation_daily_exists_and_has_coverage() -> None:
    assert VALUATION_PATH.exists(), f"missing valuation file: {VALUATION_PATH}"
    df = pd.read_parquet(VALUATION_PATH, columns=["instrument_code"])
    assert df["instrument_code"].nunique() > 1000


def test_financial_quarterly_directory_has_csi300_coverage() -> None:
    assert FINANCIAL_DIR.exists(), f"missing dir: {FINANCIAL_DIR}"
    count = len(list(FINANCIAL_DIR.glob("*.parquet")))
    assert count >= 250, f"expected broad CSI300 coverage, got {count}"


def test_get_latest_financial_respects_announce_date() -> None:
    latest = get_latest_financial("000001", "2024-04-01")
    assert latest, "expected latest financial row"
    assert latest["report_date"] == "2023-12-31"
    assert latest["announce_date"] <= "2024-04-01"

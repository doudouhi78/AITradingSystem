from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("baostock", reason="baostock not installed; skipping financial data tests")

import pandas as pd

from data_pipeline.fundamental_loader import FINANCIAL_DIR, build_financial_quarterly, get_latest_financial


def _ensure_financial_files() -> list[Path]:
    files = sorted(FINANCIAL_DIR.glob("*.parquet"))
    if len(files) < 5:
        build_financial_quarterly()
        files = sorted(FINANCIAL_DIR.glob("*.parquet"))
    return files


def test_announce_date_alignment() -> None:
    _ensure_financial_files()
    record = get_latest_financial("000001", "2024-04-01")
    assert record, "no financial record returned"
    assert record["report_date"] == "2023-12-31"


def test_file_exists() -> None:
    files = _ensure_financial_files()
    assert len(files) >= 5


def test_schema() -> None:
    files = _ensure_financial_files()
    sample = pd.read_parquet(files[0])
    assert "announce_date" in sample.columns
    assert pd.api.types.is_datetime64_any_dtype(sample["announce_date"])

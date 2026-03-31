from __future__ import annotations

import pandas as pd

from data_pipeline.alternative_loader import MARGIN_PATH, NORTHBOUND_PATH


def test_northbound_flow_exists_and_start_date_is_early_enough() -> None:
    assert NORTHBOUND_PATH.exists(), f"missing {NORTHBOUND_PATH}"
    df = pd.read_parquet(NORTHBOUND_PATH)
    start = pd.to_datetime(df["date"]).min()
    assert start <= pd.Timestamp("2017-03-01")


def test_margin_balance_exists_and_start_date_is_early_enough() -> None:
    assert MARGIN_PATH.exists(), f"missing {MARGIN_PATH}"
    df = pd.read_parquet(MARGIN_PATH)
    start = pd.to_datetime(df["date"]).min()
    assert start <= pd.Timestamp("2013-01-01")

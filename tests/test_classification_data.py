from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\AITradingSystem")
CLASSIFICATION_DIR = ROOT / "runtime" / "classification_data"
INDEX_DIR = CLASSIFICATION_DIR / "index_components"


def test_stock_meta_exists_and_is_large_enough() -> None:
    path = CLASSIFICATION_DIR / "stock_meta.parquet"
    assert path.exists(), path
    df = pd.read_parquet(path)
    assert len(df) > 4000


def test_industry_sw2_exists_and_has_broad_coverage() -> None:
    path = CLASSIFICATION_DIR / "industry_sw2.parquet"
    assert path.exists(), path
    df = pd.read_parquet(path)
    assert len(df) > 3000


def test_csi300_component_count() -> None:
    path = INDEX_DIR / "csi300_latest.parquet"
    assert path.exists(), path
    df = pd.read_parquet(path)
    assert len(df) == 300


def test_csi500_component_count() -> None:
    path = INDEX_DIR / "csi500_latest.parquet"
    assert path.exists(), path
    df = pd.read_parquet(path)
    assert len(df) == 500

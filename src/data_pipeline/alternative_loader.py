from __future__ import annotations

from pathlib import Path

import akshare as ak
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
ALTERNATIVE_DIR = ROOT / "runtime" / "alternative_data"
NORTHBOUND_PATH = ALTERNATIVE_DIR / "northbound_flow.parquet"
MARGIN_PATH = ALTERNATIVE_DIR / "margin_balance.parquet"


def build_northbound_flow() -> pd.DataFrame:
    """
    Build historical northbound flow.

    Current AkShare stable interface only provides aggregate northbound net buy.
    Shanghai/Shenzhen split fields are retained for schema compatibility and set to null
    until a stable split historical source is introduced.
    """
    ALTERNATIVE_DIR.mkdir(parents=True, exist_ok=True)
    df = ak.stock_hsgt_hist_em().copy()
    out = pd.DataFrame({
        "date": pd.to_datetime(df["日期"], errors="coerce"),
        "net_buy_sh": pd.Series([pd.NA] * len(df), dtype="Float64"),
        "net_buy_sz": pd.Series([pd.NA] * len(df), dtype="Float64"),
        "net_buy_total": pd.to_numeric(df["当日成交净买额"], errors="coerce"),
    }).dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    out.to_parquet(NORTHBOUND_PATH, index=False)
    return out


def build_margin_balance() -> pd.DataFrame:
    ALTERNATIVE_DIR.mkdir(parents=True, exist_ok=True)
    sh = ak.macro_china_market_margin_sh().copy()
    sz = ak.macro_china_market_margin_sz().copy()

    sh_out = pd.DataFrame({
        "date": pd.to_datetime(sh["日期"], errors="coerce"),
        "margin_balance_sh": pd.to_numeric(sh["融资余额"], errors="coerce"),
        "short_balance_sh": pd.to_numeric(sh["融券余额"], errors="coerce"),
    })
    sz_out = pd.DataFrame({
        "date": pd.to_datetime(sz["日期"], errors="coerce"),
        "margin_balance_sz": pd.to_numeric(sz["融资余额"], errors="coerce"),
        "short_balance_sz": pd.to_numeric(sz["融券余额"], errors="coerce"),
    })
    out = sh_out.merge(sz_out, on="date", how="outer").sort_values("date").reset_index(drop=True)
    out["margin_balance_total"] = out[["margin_balance_sh", "margin_balance_sz"]].fillna(0).sum(axis=1)
    out["short_balance_total"] = out[["short_balance_sh", "short_balance_sz"]].fillna(0).sum(axis=1)
    out.to_parquet(MARGIN_PATH, index=False)
    return out

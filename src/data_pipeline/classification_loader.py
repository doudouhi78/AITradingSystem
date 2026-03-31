"""
classification_loader.py
Fetches stock classification data (metadata, industry mapping, index components)
from AkShare and stores them as Parquet files.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import requests

# ---- Proxy fix: disable env proxy for EastMoney requests and normalize push2 host.
import akshare.utils.func as _ak_func
import akshare.utils.request as _ak_request


def _patched_request_with_retry(url, params=None, headers=None, cookies=None, proxies=None, timeout=15):
    if re.search(r"https://(?:\d+\.)?push2\.eastmoney\.com", url):
        url = re.sub(r"https://(?:\d+\.)?push2\.eastmoney\.com", "https://push2.eastmoney.com", url)
    session = requests.Session()
    session.trust_env = False
    last_exception = None
    for _ in range(3):
        try:
            response = session.get(url, params=params, headers=headers, cookies=cookies, timeout=timeout, proxies={})
            response.raise_for_status()
            return response
        except Exception as e:
            last_exception = e
    raise last_exception


_ak_request.request_with_retry = _patched_request_with_retry
_ak_func.request_with_retry = _patched_request_with_retry

import akshare as ak  # noqa: E402
import pandas as pd  # noqa: E402
from akshare.stock_feature.stock_classify_sina import stock_classify_sina

ROOT = Path(r"D:\AITradingSystem")
CLASSIFICATION_DIR = ROOT / "runtime" / "classification_data"
INDEX_DIR = CLASSIFICATION_DIR / "index_components"


def _normalize_code(code: str) -> str:
    """Normalize stock code to 6-digit string without exchange suffix."""
    if not isinstance(code, str):
        code = str(code)
    # Strip exchange suffixes like .SZ, .SH, .BJ
    for suffix in [".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"]:
        if code.endswith(suffix):
            code = code[: -len(suffix)]
    return code.zfill(6)


def build_stock_meta() -> pd.DataFrame:
    """
    Fetch full A-share stock metadata and save to runtime/classification_data/stock_meta.parquet.

    Schema: instrument_code / name / list_date / delist_date / is_st / total_market_cap / float_market_cap
    """
    CLASSIFICATION_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CLASSIFICATION_DIR / "stock_meta.parquet"

    print("Fetching stock code/name list via ak.stock_info_a_code_name() ...")
    code_name_df = ak.stock_info_a_code_name()
    time.sleep(0.4)

    # Normalize columns: expect '股票代码' and '股票名称' or similar
    code_col = [c for c in code_name_df.columns if "代码" in c or "code" in c.lower()][0]
    name_col = [c for c in code_name_df.columns if "名称" in c or "name" in c.lower()][0]
    code_name_df = code_name_df[[code_col, name_col]].copy()
    code_name_df.columns = ["instrument_code", "name"]
    code_name_df["instrument_code"] = code_name_df["instrument_code"].apply(_normalize_code)

    print("Fetching spot data via ak.stock_zh_a_spot_em() for market cap ...")
    try:
        spot_df = ak.stock_zh_a_spot_em()
        time.sleep(0.4)
        # Find code and market cap columns
        spot_code_col = [c for c in spot_df.columns if "代码" in c or "code" in c.lower()][0]
        # Total market cap column (总市值)
        total_cap_col = [c for c in spot_df.columns if "总市值" in c]
        float_cap_col = [c for c in spot_df.columns if "流通市值" in c]
        select_cols = [spot_code_col]
        if total_cap_col:
            select_cols.append(total_cap_col[0])
        if float_cap_col:
            select_cols.append(float_cap_col[0])
        spot_sub = spot_df[select_cols].copy()
        spot_sub[spot_code_col] = spot_sub[spot_code_col].apply(_normalize_code)
        rename_map = {spot_code_col: "instrument_code"}
        if total_cap_col:
            rename_map[total_cap_col[0]] = "total_market_cap"
        if float_cap_col:
            rename_map[float_cap_col[0]] = "float_market_cap"
        spot_sub = spot_sub.rename(columns=rename_map)
        merged = code_name_df.merge(spot_sub, on="instrument_code", how="left")
    except Exception as e:
        print(f"Warning: spot data fetch failed: {e}. Continuing without market cap.")
        merged = code_name_df.copy()
        merged["total_market_cap"] = None
        merged["float_market_cap"] = None

    # Derive ST status from name
    merged["is_st"] = merged["name"].str.contains(r"ST|退", na=False)
    # list_date / delist_date: not provided by these interfaces, leave as None
    merged["list_date"] = None
    merged["delist_date"] = None

    # Ensure columns in required schema order
    cols = ["instrument_code", "name", "list_date", "delist_date", "is_st", "total_market_cap", "float_market_cap"]
    for c in cols:
        if c not in merged.columns:
            merged[c] = None
    result = merged[cols].drop_duplicates(subset=["instrument_code"]).reset_index(drop=True)

    result.to_parquet(out_path, index=False)
    print(f"stock_meta.parquet saved: {len(result)} rows -> {out_path}")
    return result


def build_industry_sw2() -> pd.DataFrame:
    """
    Build Shenwan level-2 industry mapping and save to runtime/classification_data/industry_sw2.parquet.

    Uses Sina's 申万二级分类接口 as the primary source because it returns direct
    stock-to-industry mappings and is more stable than EastMoney in the current network.
    Schema: instrument_code / industry_name / industry_code
    """
    CLASSIFICATION_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CLASSIFICATION_DIR / "industry_sw2.parquet"

    print("Fetching Shenwan level-2 industry mapping via stock_classify_sina('申万二级') ...")
    df = stock_classify_sina(symbol="申万二级")
    if df is None or df.empty:
        raise RuntimeError("stock_classify_sina('申万二级') returned empty data")

    code_col = "code" if "code" in df.columns else None
    industry_col = "class" if "class" in df.columns else None
    if code_col is None or industry_col is None:
        raise RuntimeError(f"unexpected schema from stock_classify_sina: {list(df.columns)}")

    result = df[[code_col, industry_col]].copy()
    result.columns = ["instrument_code", "industry_name"]
    result["instrument_code"] = result["instrument_code"].astype(str).apply(_normalize_code)
    industry_names = sorted(result["industry_name"].dropna().unique().tolist())
    industry_code_map = {name: str(i + 1).zfill(4) for i, name in enumerate(industry_names)}
    result["industry_code"] = result["industry_name"].map(industry_code_map)
    result = result.dropna(subset=["instrument_code", "industry_name"])        .drop_duplicates(subset=["instrument_code", "industry_name"])        .reset_index(drop=True)

    result.to_parquet(out_path, index=False)
    print(f"industry_sw2.parquet saved: {len(result)} rows -> {out_path}")
    return result


def build_index_components() -> None:
    """
    Fetch CSI 300 and CSI 500 current components via CS Index interface.
    Saves to runtime/classification_data/index_components/csi300_latest.parquet
    and csi500_latest.parquet.

    Schema: instrument_code / weight / index_code
    """
    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    for symbol, index_code, filename in [
        ("000300", "CSI300", "csi300_latest.parquet"),
        ("000905", "CSI500", "csi500_latest.parquet"),
    ]:
        out_path = INDEX_DIR / filename
        print(f"Fetching {index_code} components via ak.index_stock_cons_csindex(symbol='{symbol}') ...")
        cons_df = ak.index_stock_cons_csindex(symbol=symbol)
        time.sleep(0.4)
        if cons_df is None or cons_df.empty:
            raise RuntimeError(f"No component data returned for {index_code}")
        cons_df = cons_df.copy()
        cons_df["instrument_code"] = cons_df["成分券代码"].apply(_normalize_code)

        try:
            weight_df = ak.index_stock_cons_weight_csindex(symbol=symbol)
            time.sleep(0.4)
            weight_df = weight_df.copy()
            weight_df["instrument_code"] = weight_df["成分券代码"].apply(_normalize_code)
            weight_df = weight_df[["instrument_code", "权重"]].rename(columns={"权重": "weight"})
            result = cons_df[["instrument_code"]].merge(weight_df, on="instrument_code", how="left")
        except Exception:
            result = cons_df[["instrument_code"]].copy()
            result["weight"] = None

        result["index_code"] = index_code
        result = result[["instrument_code", "weight", "index_code"]].drop_duplicates(subset=["instrument_code"]).reset_index(drop=True)
        result.to_parquet(out_path, index=False)
        print(f"{filename} saved: {len(result)} components -> {out_path}")


if __name__ == "__main__":
    build_stock_meta()
    build_industry_sw2()
    build_index_components()

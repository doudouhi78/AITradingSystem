from __future__ import annotations

import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import baostock as bs
import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

from akshare.stock_fundamental.stock_finance_sina import stock_financial_analysis_indicator

ROOT = Path(r"D:\AITradingSystem")
CLASSIFICATION_DIR = ROOT / "runtime" / "classification_data"
FUNDAMENTAL_DIR = ROOT / "runtime" / "fundamental_data"
FINANCIAL_DIR = FUNDAMENTAL_DIR / "financial_quarterly"
MARKET_DATA_STOCK_DIR = ROOT / "runtime" / "market_data" / "cn_stock"
VALUATION_PATH = FUNDAMENTAL_DIR / "valuation_daily.parquet"
FAILED_VALUATION_PATH = FUNDAMENTAL_DIR / "failed_valuation.json"
FAILED_FINANCIAL_PATH = FUNDAMENTAL_DIR / "failed_financial.json"

VALUATION_SCHEMA = DataFrameSchema(
    {
        "date": Column(pa.DateTime),
        "instrument_code": Column(str),
        "pe_ttm": Column(float, nullable=True),
        "pb": Column(float, nullable=True, checks=Check(lambda s: s.dropna().gt(0).all())),
        "ps_ttm": Column(float, nullable=True),
    },
    checks=[Check(lambda df: df["instrument_code"].nunique() > 1000, error="coverage must exceed 1000 instruments")],
    strict=True,
)


def _available_stock_instruments() -> list[str]:
    return sorted(path.stem for path in MARKET_DATA_STOCK_DIR.glob("*.parquet") if not path.name.startswith("_"))


def _load_valuation_instruments() -> list[str]:
    stock_meta_path = CLASSIFICATION_DIR / "stock_meta.parquet"
    if not stock_meta_path.exists():
        return _available_stock_instruments()

    stock_meta = pd.read_parquet(stock_meta_path, columns=["instrument_code", "delist_date"])
    stock_meta["instrument_code"] = stock_meta["instrument_code"].astype(str).str.zfill(6)
    active = stock_meta[stock_meta["delist_date"].isna()]["instrument_code"].tolist() if "delist_date" in stock_meta.columns else stock_meta["instrument_code"].tolist()
    available = set(_available_stock_instruments())
    return sorted(code for code in active if code in available)


def _to_baostock_code(instrument_code: str) -> str:
    code = str(instrument_code).zfill(6)
    if code.startswith(("600", "601", "603", "605", "688")):
        return f"sh.{code}"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz.{code}"
    if code.startswith(("430", "440", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "880", "881", "882", "883", "884", "885", "886", "887", "888", "889", "920")):
        return f"bj.{code}"
    raise ValueError(f"unsupported instrument code for baostock: {code}")


def _query_baostock_valuation(bs_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,code,peTTM,pbMRQ,psTTM",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query failed: {rs.error_code} {rs.error_msg}")

    rows: list[list[str]] = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame(columns=["date", "code", "peTTM", "pbMRQ", "psTTM"])

    df = pd.DataFrame(rows, columns=rs.fields)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["peTTM", "pbMRQ", "psTTM"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["date"])


def _build_valuation_chunk(codes: list[str], start_date: str, end_date: str) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    frames: list[pd.DataFrame] = []
    failed: list[dict[str, str]] = []
    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login_result.error_code} {login_result.error_msg}")

    try:
        for instrument_code in codes:
            try:
                bs_code = _to_baostock_code(instrument_code)
                df = _query_baostock_valuation(bs_code, start_date=start_date, end_date=end_date)
                if df.empty:
                    failed.append({"instrument_code": instrument_code, "reason": "empty_result"})
                    continue
                out = pd.DataFrame(
                    {
                        "date": df["date"],
                        "instrument_code": instrument_code,
                        "pe_ttm": df["peTTM"],
                        "pb": df["pbMRQ"].where(df["pbMRQ"] > 0),
                        "ps_ttm": df["psTTM"],
                    }
                )
                out = out[(out["date"] >= pd.Timestamp(start_date)) & out["date"].notna()]
                if out.empty:
                    failed.append({"instrument_code": instrument_code, "reason": "empty_after_filter"})
                    continue
                frames.append(out)
            except Exception as exc:
                failed.append({"instrument_code": instrument_code, "reason": repr(exc)})
    finally:
        bs.logout()

    if frames:
        return pd.concat(frames, ignore_index=True), failed
    return pd.DataFrame(columns=["date", "instrument_code", "pe_ttm", "pb", "ps_ttm"]), failed


def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def validate_valuation_daily(df: pd.DataFrame) -> pd.DataFrame:
    return VALUATION_SCHEMA.validate(df, lazy=True)


def build_valuation_daily(instruments: list[str] | None = None) -> pd.DataFrame:
    """
    拉取全A股历史每日估值数据（PE/PB/PS）。
    数据源：baostock `query_history_k_data_plus`，使用原生历史字段 peTTM/pbMRQ/psTTM。
    返回：DataFrame，写入 runtime/fundamental_data/valuation_daily.parquet
    schema：date / instrument_code / pe_ttm / pb / ps_ttm
    """
    FUNDAMENTAL_DIR.mkdir(parents=True, exist_ok=True)
    if instruments is None:
        instruments = _load_valuation_instruments()

    start_date = "2015-01-01"
    end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    chunk_size = 20
    chunks = _chunked(instruments, chunk_size)
    max_workers = min(8, max(2, (os.cpu_count() or 4) // 2))

    failed: list[dict[str, str]] = []
    frames: list[pd.DataFrame] = []
    processed = 0
    total = len(instruments)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_build_valuation_chunk, chunk, start_date, end_date): chunk for chunk in chunks}
        for future in as_completed(futures):
            df_chunk, failed_chunk = future.result()
            if not df_chunk.empty:
                frames.append(df_chunk)
                processed += df_chunk["instrument_code"].nunique()
            processed += len(failed_chunk)
            failed.extend(failed_chunk)
            print(f"valuation progress: {min(processed, total)}/{total}")

    if not frames:
        raise RuntimeError("valuation_daily build produced no rows")

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["date", "instrument_code"]).reset_index(drop=True)
    validate_valuation_daily(result)
    result.to_parquet(VALUATION_PATH, index=False)
    FAILED_VALUATION_PATH.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _infer_announce_date(report_date: pd.Timestamp) -> pd.Timestamp:
    month_day = (report_date.month, report_date.day)
    if month_day == (3, 31):
        return report_date + pd.Timedelta(days=25)
    if month_day == (6, 30):
        return report_date + pd.Timedelta(days=45)
    if month_day == (9, 30):
        return report_date + pd.Timedelta(days=30)
    if month_day == (12, 31):
        return report_date + pd.Timedelta(days=90)
    return report_date + pd.Timedelta(days=45)


def _extract_financial_fields(df: pd.DataFrame, instrument_code: str) -> pd.DataFrame:
    col_map = {
        "日期": "report_date",
        "净资产收益率(%)": "roe",
        "总资产净利润率(%)": "roa",
        "销售毛利率(%)": "gross_margin",
        "销售净利率(%)": "net_margin",
        "资产负债率(%)": "debt_ratio",
        "摊薄每股收益(元)": "eps",
    }
    available = [c for c in col_map if c in df.columns]
    data = df[available].copy().rename(columns=col_map)
    data["report_date"] = pd.to_datetime(data["report_date"], errors="coerce")
    for col in ["roe", "roa", "gross_margin", "net_margin", "debt_ratio", "eps"]:
        if col not in data.columns:
            data[col] = math.nan
        else:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    data["announce_date"] = data["report_date"].apply(_infer_announce_date)
    data["instrument_code"] = instrument_code
    return data[["instrument_code", "report_date", "announce_date", "roe", "roa", "gross_margin", "net_margin", "debt_ratio", "eps"]].dropna(subset=["report_date"]).sort_values("report_date")


def build_financial_quarterly(instruments: list[str] | None = None) -> list[str]:
    FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)
    if instruments is None:
        csi300_path = CLASSIFICATION_DIR / "index_components" / "csi300_latest.parquet"
        instruments = pd.read_parquet(csi300_path)["instrument_code"].astype(str).str.zfill(6).tolist()
    written: list[str] = []
    failed: dict[str, str] = {}
    for idx, code in enumerate(instruments, start=1):
        try:
            raw = stock_financial_analysis_indicator(symbol=code, start_year="2015")
            if raw is None or raw.empty:
                failed[code] = 'empty'
                continue
            out = _extract_financial_fields(raw, code)
            if out.empty:
                failed[code] = 'empty_extracted'
                continue
            out.to_parquet(FINANCIAL_DIR / f"{code}.parquet", index=False)
            written.append(code)
            if idx % 50 == 0:
                print(f"financial progress: {idx}/{len(instruments)}")
            time.sleep(0.02)
        except Exception as e:
            failed[code] = repr(e)
            continue
    FAILED_FINANCIAL_PATH.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding='utf-8')
    return written


def get_latest_financial(instrument: str, signal_date: str) -> dict:
    path = FINANCIAL_DIR / f"{instrument}.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    df["announce_date"] = pd.to_datetime(df["announce_date"], errors="coerce")
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    cutoff = pd.Timestamp(signal_date)
    eligible = df[df["announce_date"] <= cutoff].sort_values(["announce_date", "report_date"])
    if eligible.empty:
        return {}
    row = eligible.iloc[-1].to_dict()
    for key in ["report_date", "announce_date"]:
        if pd.notna(row.get(key)):
            row[key] = pd.Timestamp(row[key]).strftime("%Y-%m-%d")
    return row


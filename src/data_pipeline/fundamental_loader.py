from __future__ import annotations

import json
import math
import time
from pathlib import Path

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

from akshare.stock_feature.stock_classify_sina import stock_classify_sina
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


def _load_stock_snapshot() -> pd.DataFrame:
    df = stock_classify_sina(symbol="申万二级").copy()
    df["instrument_code"] = df["code"].astype(str).str.zfill(6)
    for col in ["trade", "per", "pb", "mktcap", "nmc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _available_stock_instruments() -> list[str]:
    return sorted(path.stem for path in MARKET_DATA_STOCK_DIR.glob("*.parquet") if not path.name.startswith("_"))


def validate_valuation_daily(df: pd.DataFrame) -> pd.DataFrame:
    return VALUATION_SCHEMA.validate(df, lazy=True)


def build_valuation_daily(instruments: list[str] | None = None) -> pd.DataFrame:
    """
    Build daily valuation table in long format.

    Fallback note:
    - Current AkShare package in this repo does not expose `stock_a_lg_indicator`.
    - V1 implementation uses Sina snapshot valuation (`per`, `pb`) and projects them
      across historical prices by inferring current EPS/BPS anchors.
    - `ps_ttm` is reserved and currently left null until a stable daily sales valuation
      source is introduced.
    """
    FUNDAMENTAL_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = _load_stock_snapshot()
    if instruments is None:
        instruments = _available_stock_instruments()
    instruments = [code for code in instruments if code in set(snapshot["instrument_code"])]

    failed: list[dict[str, str]] = []
    frames: list[pd.DataFrame] = []
    snapshot_map = snapshot.drop_duplicates(subset=["instrument_code"]).set_index("instrument_code")
    for idx, code in enumerate(instruments, start=1):
        try:
            row = snapshot_map.loc[code]
            price_now = float(row.get("trade", float("nan")))
            pe_now = float(row.get("per", float("nan")))
            pb_now = float(row.get("pb", float("nan")))
            path = MARKET_DATA_STOCK_DIR / f"{code}.parquet"
            price_df = pd.read_parquet(path, columns=["trade_date", "close"]).copy()
            price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])
            price_df["close"] = pd.to_numeric(price_df["close"], errors="coerce")
            price_df = price_df.dropna(subset=["trade_date", "close"])
            if price_df.empty:
                failed.append({"instrument_code": code, "reason": "empty_price_df"})
                continue

            eps_ttm = price_now / pe_now if pd.notna(price_now) and pd.notna(pe_now) and pe_now > 0 else math.nan
            book_value = price_now / pb_now if pd.notna(price_now) and pd.notna(pb_now) and pb_now > 0 else math.nan

            out = pd.DataFrame({
                "date": price_df["trade_date"],
                "instrument_code": code,
                "pe_ttm": price_df["close"] / eps_ttm if pd.notna(eps_ttm) and eps_ttm != 0 else math.nan,
                "pb": price_df["close"] / book_value if pd.notna(book_value) and book_value > 0 else math.nan,
                "ps_ttm": math.nan,
            })
            frames.append(out)
            if idx % 200 == 0:
                print(f"valuation progress: {idx}/{len(instruments)}")
            time.sleep(0.02)
        except Exception as e:
            failed.append({"instrument_code": code, "reason": repr(e)})

    if not frames:
        raise RuntimeError("valuation_daily build produced no rows")
    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["date", "instrument_code"]).reset_index(drop=True)
    validate_valuation_daily(result)
    result.to_parquet(VALUATION_PATH, index=False)
    FAILED_VALUATION_PATH.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _infer_announce_date(report_date: pd.Timestamp) -> pd.Timestamp:
    # announce_date 近似规则（缺乏精确公告日数据时的保守估计）：
    # Q1（3月31日报告期）→ +25天 → 4月25日
    # Q2（6月30日报告期）→ +45天 → 8月14日
    # Q3（9月30日报告期）→ +30天 → 10月30日
    # Q4（12月31日报告期）→ +90天 → 次年3月31日
    # ⚠️ 这是保守近似，实际公告可能更早，但不会更晚（通常）
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
    """
    返回 signal_date 之前最新已公告的季报数据。
    使用 announce_date 而非 report_date 做过滤，防止前向偏差。
    """
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

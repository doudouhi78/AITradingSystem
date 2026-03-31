from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests

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
            time.sleep(0.2)
    raise last_exception


_ak_request.request_with_retry = _patched_request_with_retry
_ak_func.request_with_retry = _patched_request_with_retry

import akshare as ak  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(r"D:\AITradingSystem")
CLASSIFICATION_PATH = ROOT / "runtime" / "classification_data" / "index_components" / "csi300_latest.parquet"
FUNDAMENTAL_DIR = ROOT / "runtime" / "fundamental_data"
FINANCIAL_DIR = FUNDAMENTAL_DIR / "financial_quarterly"
FAILED_PATH = FUNDAMENTAL_DIR / "failed_financial.json"
FALLBACK_INSTRUMENTS = [
    "000001",
    "000002",
    "000651",
    "000858",
    "600036",
    "600519",
    "601318",
    "600276",
    "002415",
    "000333",
    "600900",
    "601166",
    "600030",
    "002594",
    "000568",
    "601888",
    "600031",
    "601919",
    "600028",
    "000725",
]
FINANCIAL_COLUMNS = [
    "report_date",
    "announce_date",
    "roe",
    "roa",
    "gross_margin",
    "net_margin",
    "debt_ratio",
    "eps",
]


def _normalize_code(code: str) -> str:
    if not isinstance(code, str):
        code = str(code)
    for suffix in [".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"]:
        if code.endswith(suffix):
            code = code[: -len(suffix)]
    return code.zfill(6)


def _to_em_symbol(code: str) -> str:
    code = _normalize_code(code)
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"


def _load_csi300_instruments() -> list[str]:
    if CLASSIFICATION_PATH.exists():
        df = pd.read_parquet(CLASSIFICATION_PATH)
        if "instrument_code" in df.columns:
            instruments = df["instrument_code"].astype(str).map(_normalize_code).dropna().unique().tolist()
            if instruments:
                return instruments
    return FALLBACK_INSTRUMENTS.copy()


def _approximate_announce_date(report_date: pd.Timestamp) -> pd.Timestamp:
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


def _extract_sina_financial(symbol: str) -> pd.DataFrame:
    df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year="2010")
    if df is None or df.empty:
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)
    candidate_date_cols = ["日期", "date"]
    date_col = next((c for c in candidate_date_cols if c in df.columns), None)
    if date_col is None:
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)

    def pick(*candidates: str) -> str | None:
        return next((c for c in candidates if c in df.columns), None)

    mapping = {
        "report_date": date_col,
        "roe": pick("净资产收益率(%)", "净资产收益率加权(%)"),
        "roa": pick("总资产净利润率(%)", "总资产报酬率(%)"),
        "gross_margin": pick("销售毛利率(%)"),
        "net_margin": pick("销售净利率(%)"),
        "debt_ratio": pick("资产负债率(%)"),
        "eps": pick("每股收益_调整后(元)", "每股收益_调整前(元)", "摊薄每股收益(元)"),
    }
    if any(v is None for k, v in mapping.items() if k != "report_date"):
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)

    result = df[[v for v in mapping.values() if v is not None]].copy()
    result = result.rename(columns={v: k for k, v in mapping.items() if v is not None})
    result["report_date"] = pd.to_datetime(result["report_date"], errors="coerce")
    for col in ["roe", "roa", "gross_margin", "net_margin", "debt_ratio", "eps"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    result["announce_date"] = result["report_date"].map(_approximate_announce_date)
    result = result.dropna(subset=["report_date"]).sort_values("report_date").drop_duplicates("report_date", keep="last")
    return result[FINANCIAL_COLUMNS].reset_index(drop=True)


def _extract_em_financial(symbol: str) -> pd.DataFrame:
    df = ak.stock_financial_analysis_indicator_em(symbol=_to_em_symbol(symbol), indicator="按报告期")
    if df is None or df.empty:
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)
    mapping = {
        "report_date": "REPORT_DATE",
        "announce_date": "NOTICE_DATE",
        "roe": "ROEJQ",
        "roa": "ZZCJLL",
        "gross_margin": "XSMLL",
        "net_margin": "XSJLL",
        "debt_ratio": "ZCFZL",
        "eps": "EPSJB",
    }
    missing = [source for source in mapping.values() if source not in df.columns]
    if missing:
        raise RuntimeError(f"financial EM schema missing columns: {missing}")
    result = df[list(mapping.values())].copy().rename(columns={v: k for k, v in mapping.items()})
    result["report_date"] = pd.to_datetime(result["report_date"], errors="coerce")
    result["announce_date"] = pd.to_datetime(result["announce_date"], errors="coerce")
    result["announce_date"] = result["announce_date"].fillna(result["report_date"].map(_approximate_announce_date))
    for col in ["roe", "roa", "gross_margin", "net_margin", "debt_ratio", "eps"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    result = result.dropna(subset=["report_date"]).sort_values("report_date").drop_duplicates("report_date", keep="last")
    return result[FINANCIAL_COLUMNS].reset_index(drop=True)


def _fetch_financial_quarterly(symbol: str) -> pd.DataFrame:
    try:
        result = _extract_sina_financial(symbol)
        if not result.empty:
            return result
    except Exception:
        pass
    return _extract_em_financial(symbol)


def build_financial_quarterly(instruments: list[str] | None = None) -> dict[str, object]:
    FINANCIAL_DIR.mkdir(parents=True, exist_ok=True)
    FUNDAMENTAL_DIR.mkdir(parents=True, exist_ok=True)
    instrument_list = instruments or _load_csi300_instruments()
    written: list[str] = []
    failed: dict[str, str] = {}

    for instrument in instrument_list:
        symbol = _normalize_code(instrument)
        try:
            df = _fetch_financial_quarterly(symbol)
            if df.empty:
                failed[symbol] = "empty_financial_data"
                continue
            out_path = FINANCIAL_DIR / f"{symbol}.parquet"
            df.to_parquet(out_path, index=False)
            written.append(symbol)
        except Exception as e:
            failed[symbol] = repr(e)
        time.sleep(0.3)

    FAILED_PATH.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "instrument_count": len(instrument_list),
        "written_count": len(written),
        "written_symbols": written,
        "failed_count": len(failed),
        "failed_symbols": failed,
    }


def get_latest_financial(instrument: str, signal_date: str) -> dict:
    path = FINANCIAL_DIR / f"{_normalize_code(instrument)}.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    df["announce_date"] = pd.to_datetime(df["announce_date"], errors="coerce")
    filtered = df[df["announce_date"] <= pd.Timestamp(signal_date)].sort_values("announce_date")
    if filtered.empty:
        return {}
    latest = filtered.iloc[-1].to_dict()
    for key in ["report_date", "announce_date"]:
        if pd.notna(latest.get(key)):
            latest[key] = pd.Timestamp(latest[key]).strftime("%Y-%m-%d")
    return latest


if __name__ == "__main__":
    result = build_financial_quarterly()
    print(json.dumps(result, ensure_ascii=False, indent=2))

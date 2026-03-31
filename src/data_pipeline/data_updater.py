from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Callable

import akshare as ak
import baostock as bs
import pandas as pd

from ai_dev_os.etf_breakout_runtime import to_fund_symbol
from ai_dev_os.market_data_v1 import BAR_COLUMNS, standardize_market_bars
from data_pipeline.alternative_loader import build_margin_balance, build_northbound_flow
from data_pipeline.fundamental_loader import VALUATION_PATH, build_valuation_daily

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT / "runtime"
MARKET_DATA_DIR = RUNTIME_DIR / "market_data"
ETF_DIR = MARKET_DATA_DIR / "cn_etf"
STOCK_DIR = MARKET_DATA_DIR / "cn_stock"
CLASSIFICATION_DIR = RUNTIME_DIR / "classification_data"
ALTERNATIVE_DIR = RUNTIME_DIR / "alternative_data"
UPDATE_LOG_DIR = RUNTIME_DIR / "update_log"
FAILED_UPDATE_PATH = MARKET_DATA_DIR / "failed_update.json"


def _disable_proxy_env() -> None:
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
        os.environ[key] = ""
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


_disable_proxy_env()


def _today() -> pd.Timestamp:
    return pd.Timestamp.now().normalize()


def _load_parquet_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=BAR_COLUMNS)
    return pd.read_parquet(path)


def _latest_trade_date(path: Path, column: str = "trade_date") -> pd.Timestamp | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=[column])
    if df.empty:
        return None
    series = pd.to_datetime(df[column], errors="coerce").dropna()
    if series.empty:
        return None
    return series.max().normalize()


def _business_day_lag(latest: pd.Timestamp | None, today: pd.Timestamp) -> int:
    if latest is None:
        return 999
    if latest >= today:
        return 0
    return max(len(pd.bdate_range(latest + pd.Timedelta(days=1), today)), 0)


def _standardize_increment(frame: pd.DataFrame, *, symbol: str, security_type: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    normalized = frame.rename(columns={"date": "trade_date"}).copy()
    return standardize_market_bars(normalized, market="CN", symbol=symbol, security_type=security_type)


def _merge_increment(existing: pd.DataFrame, incoming: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if existing.empty:
        merged = incoming.copy()
    elif incoming.empty:
        merged = existing.copy()
    else:
        merged = pd.concat([existing, incoming], ignore_index=True)
    before = len(existing)
    merged = merged.drop_duplicates(subset=["trade_date"], keep="last").sort_values("trade_date").reset_index(drop=True)
    added_rows = max(len(merged) - before, 0)
    return merged, added_rows


def _fetch_etf_increment(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = ak.fund_etf_hist_sina(symbol=to_fund_symbol(symbol)).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[(df["date"] >= pd.Timestamp(start_date)) & (df["date"] <= pd.Timestamp(end_date))].copy()
    return _standardize_increment(df, symbol=symbol, security_type="etf")


def _to_baostock_code(symbol: str) -> str:
    symbol = str(symbol).zfill(6)
    if symbol.startswith(("600", "601", "603", "605", "688")):
        return f"sh.{symbol}"
    if symbol.startswith(("000", "001", "002", "003", "300", "301", "302")):
        return f"sz.{symbol}"
    if symbol.startswith(("430", "440", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "880", "881", "882", "883", "884", "885", "886", "887", "888", "889", "920")):
        return f"bj.{symbol}"
    raise ValueError(f"unsupported symbol: {symbol}")


def _fetch_stock_increment(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login.error_code} {login.error_msg}")
    try:
        rs = bs.query_history_k_data_plus(
            _to_baostock_code(symbol),
            "date,code,open,high,low,close,volume,amount",
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
            return pd.DataFrame(columns=BAR_COLUMNS)
        frame = pd.DataFrame(rows, columns=rs.fields)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return _standardize_increment(frame, symbol=symbol, security_type="stock")
    finally:
        try:
            bs.logout()
        except Exception:
            pass


def update_single_market_file(
    file_path: Path,
    *,
    symbol: str,
    security_type: str,
    fetcher: Callable[[str, str, str], pd.DataFrame],
    today: pd.Timestamp | None = None,
) -> dict[str, Any]:
    today = today or _today()
    existing = _load_parquet_frame(file_path)
    latest = _latest_trade_date(file_path)
    start_date = (latest + pd.Timedelta(days=1)).strftime("%Y-%m-%d") if latest is not None else "2016-01-01"
    end_date = today.strftime("%Y-%m-%d")
    if latest is not None and latest >= today:
        return {"symbol": symbol, "status": "already_latest", "added_rows": 0, "latest_date": latest.strftime("%Y-%m-%d")}

    incoming = fetcher(symbol, start_date, end_date)
    merged, added_rows = _merge_increment(existing, incoming)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(file_path, index=False)
    latest_out = pd.to_datetime(merged["trade_date"], errors="coerce").max() if not merged.empty else latest
    return {
        "symbol": symbol,
        "status": "updated" if added_rows > 0 else "checked_no_new_rows",
        "added_rows": int(added_rows),
        "latest_date": latest_out.strftime("%Y-%m-%d") if pd.notna(latest_out) else None,
    }


def update_market_directory(
    *,
    symbols: list[str],
    data_dir: Path,
    security_type: str,
    fetcher: Callable[[str, str, str], pd.DataFrame],
    sleep_seconds: float = 0.0,
    today: pd.Timestamp | None = None,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    today = today or _today()
    for idx, symbol in enumerate(symbols, start=1):
        file_path = data_dir / f"{symbol}.parquet"
        try:
            result = update_single_market_file(file_path, symbol=symbol, security_type=security_type, fetcher=fetcher, today=today)
            results.append(result)
        except Exception as exc:
            failures.append({"symbol": symbol, "reason": repr(exc)})
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        if idx % 100 == 0:
            print(f"{security_type} update progress: {idx}/{len(symbols)}")
    return {
        "processed": len(results),
        "updated": sum(1 for item in results if item["status"] == "updated"),
        "added_rows": int(sum(item["added_rows"] for item in results)),
        "failures": failures,
        "details": results,
    }


def _load_csi300_symbols() -> list[str]:
    path = CLASSIFICATION_DIR / "index_components" / "csi300_latest.parquet"
    if not path.exists():
        return []
    df = pd.read_parquet(path)
    return df["instrument_code"].astype(str).str.zfill(6).tolist()


def update_alternative_data() -> dict[str, Any]:
    ALTERNATIVE_DIR.mkdir(parents=True, exist_ok=True)
    north_existing_max = _latest_trade_date(ALTERNATIVE_DIR / "northbound_flow.parquet", column="date")
    margin_existing_max = _latest_trade_date(ALTERNATIVE_DIR / "margin_balance.parquet", column="date")

    north = build_northbound_flow()
    margin = build_margin_balance()

    north_new_rows = int((pd.to_datetime(north["date"]) > north_existing_max).sum()) if north_existing_max is not None else len(north)
    margin_new_rows = int((pd.to_datetime(margin["date"]) > margin_existing_max).sum()) if margin_existing_max is not None else len(margin)
    return {
        "processed": 2,
        "updated": int(north_new_rows > 0) + int(margin_new_rows > 0),
        "added_rows": north_new_rows + margin_new_rows,
        "failures": [],
        "northbound_new_rows": north_new_rows,
        "margin_new_rows": margin_new_rows,
    }


def update_valuation_if_stale(today: pd.Timestamp | None = None) -> dict[str, Any]:
    today = today or _today()
    latest = _latest_trade_date(VALUATION_PATH, column="date")
    lag = _business_day_lag(latest, today)
    if latest is not None and lag <= 5:
        return {"status": "skipped_recent", "latest_date": latest.strftime("%Y-%m-%d"), "lag_bdays": lag, "added_rows": 0}

    before_rows = 0
    if VALUATION_PATH.exists():
        before_rows = len(pd.read_parquet(VALUATION_PATH, columns=["date"]))
    df = build_valuation_daily()
    return {
        "status": "rebuilt",
        "latest_date": pd.to_datetime(df["date"]).max().strftime("%Y-%m-%d"),
        "lag_bdays": lag,
        "added_rows": max(len(df) - before_rows, 0),
    }


def _write_failed_update(failures: dict[str, list[dict[str, str]]]) -> None:
    FAILED_UPDATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAILED_UPDATE_PATH.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")


def write_update_log(summary: dict[str, Any], log_dir: Path = UPDATE_LOG_DIR, run_date: pd.Timestamp | None = None) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    run_date = run_date or _today()
    path = log_dir / f"{run_date.strftime('%Y%m%d')}.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_daily_update(*, etf: bool = True, stocks: bool = True, alternative: bool = True, valuation: bool = True) -> dict[str, Any]:
    today = _today()
    summary: dict[str, Any] = {
        "run_date": today.strftime("%Y-%m-%d"),
        "modules": {},
        "failed_symbols": {},
    }

    if etf:
        etf_symbols = sorted(path.stem for path in ETF_DIR.glob("*.parquet") if not path.name.startswith("_"))
        etf_result = update_market_directory(symbols=etf_symbols, data_dir=ETF_DIR, security_type="etf", fetcher=_fetch_etf_increment, sleep_seconds=0.0, today=today)
        summary["modules"]["etf"] = etf_result
        summary["failed_symbols"]["etf"] = etf_result["failures"]

    if stocks:
        stock_symbols = _load_csi300_symbols()
        stock_result = update_market_directory(symbols=stock_symbols, data_dir=STOCK_DIR, security_type="stock", fetcher=_fetch_stock_increment, sleep_seconds=0.2, today=today)
        summary["modules"]["stocks"] = stock_result
        summary["failed_symbols"]["stocks"] = stock_result["failures"]

    if alternative:
        alt_result = update_alternative_data()
        summary["modules"]["alternative"] = alt_result
        summary["failed_symbols"]["alternative"] = alt_result["failures"]

    if valuation:
        valuation_result = update_valuation_if_stale(today=today)
        summary["modules"]["valuation"] = valuation_result
        summary["failed_symbols"]["valuation"] = []

    _write_failed_update(summary["failed_symbols"])
    log_path = write_update_log(summary, run_date=today)
    summary["update_log"] = str(log_path)
    return summary


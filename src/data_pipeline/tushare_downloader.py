from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ai_dev_os.market_data_v1 import BAR_COLUMNS

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT / "runtime"
MARKET_DATA_DIR = RUNTIME_DIR / "market_data"
STOCK_DIR = MARKET_DATA_DIR / "cn_stock"
FUNDAMENTAL_DIR = RUNTIME_DIR / "fundamental_data"
ALTERNATIVE_DIR = RUNTIME_DIR / "alternative_data"
INDEX_DIR = RUNTIME_DIR / "index_data"
DOWNLOAD_LOG_DIR = RUNTIME_DIR / "download_log"
PROGRESS_PATH = DOWNLOAD_LOG_DIR / "progress.json"

TOKEN = "009c49c7abe2f2bd16c823d4d8407f7e7fcbbc1883bf50eaae90ae5f"
RELAY_API_KEY = "huanghanchi"
RELAY_BASE_URL = "https://ai-tool.indevs.in"
NATIVE_API_URL = "http://api.tushare.pro"

INDEX_CODES = {
    "000300.SH": "csi300",
    "000905.SH": "csi500",
    "000852.SH": "csi1000",
    "000016.SH": "sz50",
    "399006.SZ": "cyb",
    "000688.SH": "star50",
}


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _to_frame(payload: dict[str, Any]) -> pd.DataFrame:
    data = payload.get("data") or {}
    fields = data.get("fields") or []
    items = data.get("items") or []
    return pd.DataFrame(items, columns=fields)


def _normalize_dates(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _quarter_ends(start_year: int = 2010, end_year: int | None = None) -> list[str]:
    end_year = end_year or pd.Timestamp.today().year
    periods: list[str] = []
    for year in range(start_year, end_year + 1):
        periods.extend([f"{year}0331", f"{year}0630", f"{year}0930", f"{year}1231"])
    return periods


def _month_starts(start: str = "2015-01-01", end: str | None = None) -> list[pd.Timestamp]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end or pd.Timestamp.today().strftime("%Y-%m-%d"))
    dates = pd.date_range(start_ts.replace(day=1), end_ts, freq="MS")
    return list(dates)


def _format_trade_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize_dates(frame, ["cal_date", "pretrade_date"])
    return frame.sort_values("cal_date").reset_index(drop=True)


def _standardize_stock_daily(frame: pd.DataFrame, stock_basic: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    meta = stock_basic[["ts_code", "symbol", "list_date", "delist_date"]].drop_duplicates("ts_code").copy()
    meta["symbol"] = meta["symbol"].astype(str).str.zfill(6)
    frame = frame.merge(meta, on="ts_code", how="left")
    out = pd.DataFrame(
        {
            "market": "CN",
            "symbol": frame["symbol"].astype(str).str.zfill(6),
            "security_type": "stock",
            "trade_date": pd.to_datetime(frame["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d"),
            "open": pd.to_numeric(frame["open"], errors="coerce"),
            "high": pd.to_numeric(frame["high"], errors="coerce"),
            "low": pd.to_numeric(frame["low"], errors="coerce"),
            "close": pd.to_numeric(frame["close"], errors="coerce"),
            "volume": pd.to_numeric(frame["vol"], errors="coerce"),
            "amount": pd.to_numeric(frame["amount"], errors="coerce"),
            "adjustment_mode": "none",
            "is_suspended": False,
            "listed_date": pd.to_datetime(frame["list_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna(""),
            "delisted_date": pd.to_datetime(frame["delist_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna(""),
        }
    )
    return out[BAR_COLUMNS].dropna(subset=["trade_date"]).sort_values(["symbol", "trade_date"]).reset_index(drop=True)


@dataclass
class BatchSummary:
    batch_name: str
    metrics: dict[str, Any]


class TushareClient:
    def __init__(
        self,
        *,
        token: str = TOKEN,
        relay_api_key: str = RELAY_API_KEY,
        relay_base_url: str = RELAY_BASE_URL,
        native_api_url: str = NATIVE_API_URL,
        timeout: int = 60,
    ) -> None:
        self.token = token
        self.relay_api_key = relay_api_key
        self.relay_base_url = relay_base_url.rstrip("/")
        self.native_api_url = native_api_url
        self.timeout = timeout
        self.session = requests.Session()

    def _relay_query(self, api_name: str, params: dict[str, Any], fields: str = "") -> pd.DataFrame:
        url = f"{self.relay_base_url}/tushare/pro/{api_name}"
        response = self.session.post(
            url,
            json=params,
            headers={"X-API-Key": self.relay_api_key, "Content-Type": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"relay_http_{response.status_code}")
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code", 0) not in (0, None):
            raise RuntimeError(f"relay_code_{payload.get('code')}: {payload.get('msg', '')}")
        return _to_frame(payload)

    def _native_query(self, api_name: str, params: dict[str, Any], fields: str = "") -> pd.DataFrame:
        payload = {"api_name": api_name, "token": self.token, "params": params, "fields": fields}
        response = self.session.post(self.native_api_url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"native_code_{data.get('code')}: {data.get('msg', '')}")
        return _to_frame(data)

    def query(
        self,
        api_name: str,
        params: dict[str, Any] | None = None,
        *,
        fields: str = "",
        use_relay: bool = True,
        retries: int = 3,
    ) -> pd.DataFrame:
        params = params or {}
        relay_error: str | None = None
        if use_relay:
            for _ in range(retries):
                try:
                    return self._relay_query(api_name, params, fields)
                except Exception as exc:
                    relay_error = repr(exc)
                    time.sleep(0.2)
        native_error: str | None = None
        for _ in range(retries):
            try:
                frame = self._native_query(api_name, params, fields)
                time.sleep(0.65)
                return frame
            except Exception as exc:
                native_error = repr(exc)
                message = str(exc)
                if "40203" in message:
                    time.sleep(65)
                elif "40204" in message:
                    time.sleep(15)
                else:
                    time.sleep(1.0)
        raise RuntimeError(f"{api_name} failed; relay={relay_error}; native={native_error}")

    def query_all_pages(
        self,
        api_name: str,
        params: dict[str, Any] | None = None,
        *,
        fields: str = "",
        page_size: int = 8000,
        use_relay: bool = True,
    ) -> pd.DataFrame:
        params = dict(params or {})
        frames: list[pd.DataFrame] = []
        offset = 0
        while True:
            page_params = dict(params)
            page_params["limit"] = page_size
            page_params["offset"] = offset
            frame = self.query(api_name, page_params, fields=fields, use_relay=use_relay)
            if frame.empty:
                break
            frames.append(frame)
            if len(frame) < page_size:
                break
            offset += page_size
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class TushareDownloader:
    def __init__(self, client: TushareClient | None = None) -> None:
        self.client = client or TushareClient()
        _ensure_dir(DOWNLOAD_LOG_DIR)
        _ensure_dir(STOCK_DIR)
        _ensure_dir(FUNDAMENTAL_DIR)
        _ensure_dir(ALTERNATIVE_DIR)
        _ensure_dir(INDEX_DIR / "index_weight")
        _ensure_dir(INDEX_DIR / "index_components")
        self.progress = self._load_progress()

    def _load_progress(self) -> dict[str, Any]:
        if PROGRESS_PATH.exists():
            return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
        return {"completed_batches": [], "datasets": {}, "failed": {}}

    def _save_progress(self) -> None:
        PROGRESS_PATH.write_text(json.dumps(self.progress, ensure_ascii=False, indent=2), encoding="utf-8")

    def _record_failure(self, batch_key: str, item: str, reason: str) -> None:
        failed_path = DOWNLOAD_LOG_DIR / f"failed_{batch_key}.json"
        payload: dict[str, Any] = {}
        if failed_path.exists():
            payload = json.loads(failed_path.read_text(encoding="utf-8"))
        payload[item] = reason
        failed_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.progress.setdefault("failed", {}).setdefault(batch_key, {})[item] = reason
        self._save_progress()

    def _mark_dataset(self, batch_key: str, dataset: str, marker: Any) -> None:
        self.progress.setdefault("datasets", {}).setdefault(batch_key, {})[dataset] = marker
        self._save_progress()

    def _mark_batch_completed(self, batch_key: str) -> None:
        completed = self.progress.setdefault("completed_batches", [])
        if batch_key not in completed:
            completed.append(batch_key)
        self._save_progress()

    def load_stock_basic(self) -> pd.DataFrame:
        path = FUNDAMENTAL_DIR / "stock_basic.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_parquet(path)
        return _normalize_dates(frame, ["list_date", "delist_date"])

    def download_batch_1(self) -> BatchSummary:
        stock_basic_frames: list[pd.DataFrame] = []
        for status in ["L", "D", "P"]:
            frame = self.client.query(
                "stock_basic",
                {"exchange": "", "list_status": status},
                fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs,list_status,exchange,fullname,enname,cnspell,act_name,act_ent_type,curr_type",
            )
            frame["list_status"] = status
            stock_basic_frames.append(frame)
        stock_basic = pd.concat(stock_basic_frames, ignore_index=True).drop_duplicates("ts_code").reset_index(drop=True)
        stock_basic = _normalize_dates(stock_basic, ["list_date", "delist_date"])
        stock_basic.to_parquet(FUNDAMENTAL_DIR / "stock_basic.parquet", index=False)
        self._mark_dataset("batch1", "stock_basic", {"rows": len(stock_basic)})

        namechange = self.client.query_all_pages("namechange")
        namechange = _normalize_dates(namechange, ["ann_date", "start_date", "end_date"])
        namechange.to_parquet(FUNDAMENTAL_DIR / "st_history.parquet", index=False)
        self._mark_dataset("batch1", "namechange", {"rows": len(namechange)})

        trade_cal = self.client.query("trade_cal", {"exchange": "SSE", "start_date": "20100101", "end_date": "20301231"})
        trade_cal = _format_trade_calendar(trade_cal)
        trade_cal.to_parquet(FUNDAMENTAL_DIR / "trade_cal.parquet", index=False)
        self._mark_dataset("batch1", "trade_cal", {"rows": len(trade_cal)})

        suspend_frames: list[pd.DataFrame] = []
        for year in range(2015, pd.Timestamp.today().year + 1):
            try:
                frame = self.client.query("suspend_d", {"start_date": f"{year}0101", "end_date": f"{year}1231"}, use_relay=False)
                if not frame.empty:
                    suspend_frames.append(frame)
            except Exception as exc:
                self._record_failure("batch1", f"suspend_d_{year}", repr(exc))
        suspend = pd.concat(suspend_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if suspend_frames else pd.DataFrame()
        if not suspend.empty:
            suspend = _normalize_dates(suspend, ["trade_date"])
        suspend.to_parquet(FUNDAMENTAL_DIR / "suspend.parquet", index=False)
        self._mark_dataset("batch1", "suspend_d", {"rows": len(suspend)})

        open_days = trade_cal.loc[(trade_cal["is_open"] == 1) & (trade_cal["cal_date"] >= pd.Timestamp("2015-01-01")), "cal_date"].dt.strftime("%Y%m%d").tolist()
        limit_frames: list[pd.DataFrame] = []
        for idx, trade_date in enumerate(open_days, start=1):
            try:
                frame = self.client.query("limit_list", {"trade_date": trade_date}, use_relay=False)
                if not frame.empty:
                    limit_frames.append(frame)
            except Exception as exc:
                self._record_failure("batch1", f"limit_list_{trade_date}", repr(exc))
            if idx % 250 == 0:
                self._mark_dataset("batch1", "limit_list_progress", {"processed_days": idx, "total_days": len(open_days)})
        limit_list = pd.concat(limit_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if limit_frames else pd.DataFrame()
        if not limit_list.empty and "trade_date" in limit_list.columns:
            limit_list["trade_date"] = pd.to_datetime(limit_list["trade_date"], errors="coerce")
        limit_list.to_parquet(FUNDAMENTAL_DIR / "limit_list.parquet", index=False)
        self._mark_dataset("batch1", "limit_list", {"rows": len(limit_list)})

        self._mark_batch_completed("batch1")
        return BatchSummary(
            batch_name="batch1",
            metrics={
                "stock_basic": int(len(stock_basic)),
                "st_history": int(len(namechange)),
                "trade_cal": int(len(trade_cal)),
                "suspend": int(len(suspend)),
                "limit_list": int(len(limit_list)),
            },
        )

    def _open_trade_dates(self, start_date: str = "20150101", end_date: str | None = None) -> list[str]:
        trade_cal_path = FUNDAMENTAL_DIR / "trade_cal.parquet"
        if trade_cal_path.exists():
            trade_cal = pd.read_parquet(trade_cal_path)
            trade_cal = _format_trade_calendar(trade_cal)
        else:
            trade_cal = _format_trade_calendar(self.client.query("trade_cal", {"exchange": "SSE", "start_date": "20100101", "end_date": "20301231"}))
        end_date = end_date or pd.Timestamp.today().strftime("%Y%m%d")
        mask = (
            (trade_cal["cal_date"] >= pd.Timestamp(start_date))
            & (trade_cal["cal_date"] <= pd.Timestamp(end_date))
            & (trade_cal["is_open"] == 1)
        )
        return trade_cal.loc[mask, "cal_date"].dt.strftime("%Y%m%d").tolist()

    def _write_stock_year_chunk(self, daily_frame: pd.DataFrame, stock_basic: pd.DataFrame) -> int:
        bars = _standardize_stock_daily(daily_frame, stock_basic)
        written = 0
        for symbol, group in bars.groupby("symbol"):
            path = STOCK_DIR / f"{symbol}.parquet"
            if path.exists():
                existing = pd.read_parquet(path)
                merged = pd.concat([existing, group], ignore_index=True).drop_duplicates(subset=["trade_date"], keep="last")
                merged = merged.sort_values("trade_date").reset_index(drop=True)
            else:
                merged = group.sort_values("trade_date").reset_index(drop=True)
            merged.to_parquet(path, index=False)
            written += 1
        return written

    def download_batch_2(self) -> BatchSummary:
        stock_basic = self.load_stock_basic()
        trade_dates = self._open_trade_dates("20150101")
        daily_basic_frames: list[pd.DataFrame] = []
        adj_factor_frames: list[pd.DataFrame] = []
        stock_file_count = 0

        grouped_by_year: dict[int, list[str]] = {}
        for trade_date in trade_dates:
            grouped_by_year.setdefault(int(trade_date[:4]), []).append(trade_date)

        for year, year_dates in sorted(grouped_by_year.items()):
            daily_frames: list[pd.DataFrame] = []
            for idx, trade_date in enumerate(year_dates, start=1):
                daily_frame = self.client.query("daily", {"trade_date": trade_date}, use_relay=False)
                if not daily_frame.empty:
                    daily_frames.append(daily_frame)
                basic_frame = self.client.query(
                    "daily_basic",
                    {"trade_date": trade_date},
                    fields="ts_code,trade_date,total_mv,circ_mv,pe,pb,ps,turnover_rate,volume_ratio",
                    use_relay=False,
                )
                if not basic_frame.empty:
                    daily_basic_frames.append(basic_frame)
                adj_frame = self.client.query("adj_factor", {"trade_date": trade_date}, use_relay=False)
                if not adj_frame.empty:
                    adj_factor_frames.append(adj_frame)
                if idx % 60 == 0:
                    self._mark_dataset("batch2", f"{year}_progress", {"processed_days": idx, "total_days": len(year_dates)})
            if daily_frames:
                year_daily = pd.concat(daily_frames, ignore_index=True)
                stock_file_count = max(stock_file_count, self._write_stock_year_chunk(year_daily, stock_basic))
            self._mark_dataset("batch2", f"{year}_done", {"days": len(year_dates)})

        valuation = pd.concat(daily_basic_frames, ignore_index=True).drop_duplicates(subset=["ts_code", "trade_date"]).reset_index(drop=True)
        valuation = valuation.merge(stock_basic[["ts_code", "symbol"]], on="ts_code", how="left")
        valuation["instrument_code"] = valuation["symbol"].astype(str).str.zfill(6)
        valuation["date"] = pd.to_datetime(valuation["trade_date"], errors="coerce")
        valuation["pe_ttm"] = pd.to_numeric(valuation["pe"], errors="coerce")
        valuation["ps_ttm"] = pd.to_numeric(valuation["ps"], errors="coerce")
        valuation["pb"] = pd.to_numeric(valuation["pb"], errors="coerce")
        valuation = valuation[
            ["date", "instrument_code", "total_mv", "circ_mv", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "turnover_rate", "volume_ratio"]
        ].sort_values(["date", "instrument_code"]).reset_index(drop=True)
        valuation.to_parquet(FUNDAMENTAL_DIR / "valuation_daily.parquet", index=False)

        adj_factor = pd.concat(adj_factor_frames, ignore_index=True).drop_duplicates(subset=["ts_code", "trade_date"]).reset_index(drop=True)
        adj_factor = adj_factor.merge(stock_basic[["ts_code", "symbol"]], on="ts_code", how="left")
        adj_factor["instrument_code"] = adj_factor["symbol"].astype(str).str.zfill(6)
        adj_factor["date"] = pd.to_datetime(adj_factor["trade_date"], errors="coerce")
        adj_factor = adj_factor[["date", "instrument_code", "adj_factor"]].sort_values(["date", "instrument_code"]).reset_index(drop=True)
        adj_factor.to_parquet(FUNDAMENTAL_DIR / "adj_factor.parquet", index=False)

        self._mark_batch_completed("batch2")
        return BatchSummary(
            batch_name="batch2",
            metrics={
                "daily_symbols": int(len(list(STOCK_DIR.glob("*.parquet")))),
                "valuation_rows": int(len(valuation)),
                "adj_factor_rows": int(len(adj_factor)),
                "stock_file_count_hint": int(stock_file_count),
            },
        )

    def _download_financial_statement(self, api_name: str, output_dir: Path, stock_codes: list[str]) -> int:
        _ensure_dir(output_dir)
        count = 0
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self.client.query, api_name, {"ts_code": ts_code}, use_relay=False): ts_code for ts_code in stock_codes
            }
            for idx, future in enumerate(as_completed(futures), start=1):
                ts_code = futures[future]
                try:
                    frame = future.result()
                    if frame.empty:
                        continue
                    frame.to_parquet(output_dir / f"{ts_code}.parquet", index=False)
                    count += 1
                except Exception as exc:
                    self._record_failure("batch3", f"{api_name}_{ts_code}", repr(exc))
                if idx % 500 == 0:
                    self._mark_dataset("batch3", f"{api_name}_progress", {"processed": idx, "total": len(stock_codes)})
        return count

    def download_batch_3(self) -> BatchSummary:
        stock_basic = self.load_stock_basic()
        stock_codes = stock_basic["ts_code"].dropna().astype(str).unique().tolist()
        income_count = self._download_financial_statement("income", FUNDAMENTAL_DIR / "income", stock_codes)
        balance_count = self._download_financial_statement("balancesheet", FUNDAMENTAL_DIR / "balance", stock_codes)
        cashflow_count = self._download_financial_statement("cashflow", FUNDAMENTAL_DIR / "cashflow", stock_codes)

        forecast_frames: list[pd.DataFrame] = []
        express_frames: list[pd.DataFrame] = []
        for year in range(2015, pd.Timestamp.today().year + 1):
            forecast = self.client.query("forecast", {"ann_date": f"{year}1231"}, use_relay=False)
            if not forecast.empty:
                forecast_frames.append(forecast)
            express = self.client.query("express", {"start_date": f"{year}0101", "end_date": f"{year}1231"}, use_relay=False)
            if not express.empty:
                express_frames.append(express)

        forecast_df = pd.concat(forecast_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if forecast_frames else pd.DataFrame()
        express_df = pd.concat(express_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if express_frames else pd.DataFrame()
        forecast_df.to_parquet(FUNDAMENTAL_DIR / "forecast.parquet", index=False)
        express_df.to_parquet(FUNDAMENTAL_DIR / "express.parquet", index=False)
        self._mark_batch_completed("batch3")
        return BatchSummary(
            batch_name="batch3",
            metrics={
                "income_files": income_count,
                "balance_files": balance_count,
                "cashflow_files": cashflow_count,
                "forecast_rows": int(len(forecast_df)),
                "express_rows": int(len(express_df)),
            },
        )

    def download_batch_4(self) -> BatchSummary:
        top10_frames: list[pd.DataFrame] = []
        for period in _quarter_ends(2015):
            frame = self.client.query("top10_holders", {"period": period}, use_relay=False)
            if not frame.empty:
                top10_frames.append(frame)
        top10 = pd.concat(top10_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if top10_frames else pd.DataFrame()
        top10.to_parquet(FUNDAMENTAL_DIR / "top10_holders.parquet", index=False)

        pledge = self.client.query_all_pages("pledge_stat", use_relay=False)
        pledge.to_parquet(FUNDAMENTAL_DIR / "pledge_stat.parquet", index=False)

        self._mark_batch_completed("batch4")
        return BatchSummary(
            batch_name="batch4",
            metrics={"top10_holders_rows": int(len(top10)), "pledge_rows": int(len(pledge))},
        )

    def download_batch_5(self) -> BatchSummary:
        trade_dates = self._open_trade_dates("20150101")
        moneyflow_frames: list[pd.DataFrame] = []
        top_list_frames: list[pd.DataFrame] = []
        for idx, trade_date in enumerate(trade_dates, start=1):
            moneyflow = self.client.query("moneyflow", {"trade_date": trade_date}, use_relay=False)
            if not moneyflow.empty:
                moneyflow_frames.append(moneyflow)
            top_list = self.client.query("top_list", {"trade_date": trade_date}, use_relay=False)
            if not top_list.empty:
                top_list_frames.append(top_list)
            if idx % 250 == 0:
                self._mark_dataset("batch5", "progress", {"processed_days": idx, "total_days": len(trade_dates)})
        moneyflow_df = pd.concat(moneyflow_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if moneyflow_frames else pd.DataFrame()
        top_list_df = pd.concat(top_list_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if top_list_frames else pd.DataFrame()
        moneyflow_df.to_parquet(ALTERNATIVE_DIR / "moneyflow.parquet", index=False)
        top_list_df.to_parquet(ALTERNATIVE_DIR / "top_list.parquet", index=False)
        self._mark_batch_completed("batch5")
        return BatchSummary(
            batch_name="batch5",
            metrics={"moneyflow_rows": int(len(moneyflow_df)), "top_list_rows": int(len(top_list_df))},
        )

    def download_batch_6(self) -> BatchSummary:
        index_basic_frames: list[pd.DataFrame] = []
        for market in ["SSE", "SZSE", "CSI"]:
            frame = self.client.query("index_basic", {"market": market}, use_relay=False)
            if not frame.empty:
                index_basic_frames.append(frame)
        index_basic = pd.concat(index_basic_frames, ignore_index=True).drop_duplicates("ts_code").reset_index(drop=True) if index_basic_frames else pd.DataFrame()
        index_basic.to_parquet(INDEX_DIR / "index_basic.parquet", index=False)

        daily_frames: list[pd.DataFrame] = []
        components_written = 0
        for ts_code, alias in INDEX_CODES.items():
            daily = self.client.query("index_daily", {"ts_code": ts_code, "start_date": "20150101", "end_date": pd.Timestamp.today().strftime("%Y%m%d")}, use_relay=False)
            if not daily.empty:
                daily_frames.append(daily)
            weight_frames: list[pd.DataFrame] = []
            for month_start in _month_starts("2015-01-01"):
                month_end = min(month_start + pd.offsets.MonthEnd(1), pd.Timestamp.today())
                weight = self.client.query(
                    "index_weight",
                    {"index_code": ts_code, "start_date": month_start.strftime("%Y%m%d"), "end_date": month_end.strftime("%Y%m%d")},
                    use_relay=False,
                )
                if not weight.empty:
                    weight_frames.append(weight)
            weight_df = pd.concat(weight_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if weight_frames else pd.DataFrame()
            weight_df.to_parquet(INDEX_DIR / "index_weight" / f"{alias}.parquet", index=False)
            components = weight_df[["con_code", "trade_date"]].rename(columns={"con_code": "ts_code"}) if not weight_df.empty else pd.DataFrame(columns=["ts_code", "trade_date"])
            if not components.empty:
                components["trade_date"] = pd.to_datetime(components["trade_date"], errors="coerce")
                components["symbol"] = components["ts_code"].astype(str).str.split(".").str[0]
                components.to_parquet(INDEX_DIR / "index_components" / f"{alias}.parquet", index=False)
                components_written += 1
        index_daily = pd.concat(daily_frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if daily_frames else pd.DataFrame()
        index_daily.to_parquet(INDEX_DIR / "index_daily.parquet", index=False)
        self._mark_batch_completed("batch6")
        return BatchSummary(
            batch_name="batch6",
            metrics={
                "index_basic_rows": int(len(index_basic)),
                "index_daily_rows": int(len(index_daily)),
                "components_written": components_written,
            },
        )

    def run_batch(self, batch_number: int) -> BatchSummary:
        mapping = {
            1: self.download_batch_1,
            2: self.download_batch_2,
            3: self.download_batch_3,
            4: self.download_batch_4,
            5: self.download_batch_5,
            6: self.download_batch_6,
        }
        return mapping[batch_number]()

    def run_all(self) -> list[BatchSummary]:
        results: list[BatchSummary] = []
        for batch_number in range(1, 7):
            results.append(self.run_batch(batch_number))
        return results

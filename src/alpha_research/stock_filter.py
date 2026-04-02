from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
import warnings

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _warn(message: str) -> None:
    warnings.warn(message, stacklevel=2)
    print(f"Warning: {message}")


def _normalize_ts_code(code: object) -> str:
    text = str(code or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        symbol, market = text.split(".", 1)
        market = market or "SZ"
        return f"{symbol.zfill(6)}.{market}"
    return f"{text.zfill(6)}.SZ"


def _symbol_from_ts_code(code: object) -> str:
    return _normalize_ts_code(code).split(".", 1)[0]


@dataclass
class _FilterResult:
    passed: list[str] = field(default_factory=list)
    excluded_reasons: dict[str, list[str]] = field(default_factory=dict)


class StockFilter:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        market_data_dir: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else _REPO_ROOT / "runtime" / "fundamental_data"
        self.market_data_dir = (
            Path(market_data_dir)
            if market_data_dir is not None
            else _REPO_ROOT / "runtime" / "market_data" / "cn_stock"
        )

    def filter(
        self,
        all_codes: list,
        trade_date: str,
        exclude_st: bool = True,
        min_list_days: int = 252,
        min_avg_amount: float = 5e7,
        exclude_halt: bool = True,
        explain: bool = False,
    ) -> list[str] | tuple[list[str], dict[str, list[str]]]:
        query_date = pd.Timestamp(trade_date).normalize()
        result = _FilterResult()

        for raw_code in all_codes:
            ts_code = _normalize_ts_code(raw_code)
            if not ts_code:
                continue

            reasons: list[str] = []
            symbol = _symbol_from_ts_code(ts_code)
            daily = self._load_daily(symbol)

            if exclude_st and self._is_st(ts_code, query_date):
                reasons.append("st")
            if min_list_days > 0 and self._is_new_listing(query_date, min_list_days, daily):
                reasons.append("new_listing")
            if min_avg_amount > 0 and self._is_low_liquidity(symbol, query_date, min_avg_amount, daily):
                reasons.append("low_liquidity")
            if exclude_halt and self._is_halted(symbol, query_date, daily):
                reasons.append("halted")

            if reasons:
                result.excluded_reasons[ts_code] = reasons
            else:
                result.passed.append(ts_code)

        if explain:
            return result.passed, result.excluded_reasons
        return result.passed

    @lru_cache(maxsize=1)
    def _load_stock_basic(self) -> pd.DataFrame:
        path = self.data_dir / "stock_basic.parquet"
        if not path.exists():
            _warn(f"stock_basic missing: {path}")
            return pd.DataFrame()
        frame = pd.read_parquet(path).copy()
        if "ts_code" in frame.columns:
            frame["ts_code"] = frame["ts_code"].map(_normalize_ts_code)
        elif "symbol" in frame.columns:
            frame["ts_code"] = frame["symbol"].map(_normalize_ts_code)
        else:
            _warn("stock_basic missing ts_code/symbol; ST rule will be skipped")
            return pd.DataFrame()
        if "list_date" in frame.columns:
            frame["list_date"] = pd.to_datetime(frame["list_date"], errors="coerce")
        return frame.dropna(subset=["ts_code"]).drop_duplicates(subset=["ts_code"], keep="last")

    @lru_cache(maxsize=1)
    def _load_st_history(self) -> pd.DataFrame:
        path = self.data_dir / "st_history.parquet"
        if not path.exists():
            return pd.DataFrame()
        frame = pd.read_parquet(path).copy()
        source = "ts_code" if "ts_code" in frame.columns else "instrument_code" if "instrument_code" in frame.columns else None
        if source is None:
            return pd.DataFrame()
        frame["ts_code"] = frame[source].map(_normalize_ts_code)
        for col in ("start_date", "end_date", "ann_date"):
            if col in frame.columns:
                frame[col] = pd.to_datetime(frame[col], errors="coerce")
        return frame.dropna(subset=["ts_code"])

    @lru_cache(maxsize=8192)
    def _load_daily(self, symbol: str) -> pd.DataFrame:
        path = self.market_data_dir / f"{symbol}.parquet"
        if not path.exists():
            _warn(f"daily parquet missing for {symbol}: {path}")
            return pd.DataFrame()
        frame = pd.read_parquet(path).copy()
        if "trade_date" not in frame.columns:
            _warn(f"daily parquet missing trade_date for {symbol}: {path}")
            return pd.DataFrame()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        for col in ("volume", "amount"):
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame.sort_values("trade_date").reset_index(drop=True)

    def _is_st(self, ts_code: str, query_date: pd.Timestamp) -> bool:
        stock_basic = self._load_stock_basic()
        if not stock_basic.empty:
            if "name" in stock_basic.columns:
                matched = stock_basic.loc[stock_basic["ts_code"] == ts_code, "name"]
                if not matched.empty:
                    return matched.astype(str).str.contains("ST", case=False, na=False).any()
            else:
                _warn("stock_basic has no name column; ST rule downgraded")
                return False

        st_history = self._load_st_history()
        if st_history.empty:
            return False
        if "name" not in st_history.columns:
            _warn("st_history has no name column; ST rule downgraded")
            return False
        active = st_history.loc[
            (st_history["ts_code"] == ts_code)
            & (st_history["start_date"].isna() | (st_history["start_date"] <= query_date))
            & (st_history["end_date"].isna() | (st_history["end_date"] >= query_date))
        ]
        if active.empty:
            return False
        return active["name"].astype(str).str.contains("ST", case=False, na=False).any()

    def _is_new_listing(
        self,
        query_date: pd.Timestamp,
        min_list_days: int,
        daily: pd.DataFrame,
    ) -> bool:
        if daily.empty:
            return False
        history = daily.loc[daily["trade_date"] <= query_date, "trade_date"].dropna().sort_values()
        if history.empty:
            return False
        return int(history.nunique()) < min_list_days

    def _is_low_liquidity(
        self,
        symbol: str,
        query_date: pd.Timestamp,
        min_avg_amount: float,
        daily: pd.DataFrame,
    ) -> bool:
        if daily.empty:
            return False
        if "amount" not in daily.columns:
            _warn(f"amount column missing for {symbol}; liquidity rule downgraded")
            return False
        sample = daily.loc[daily["trade_date"] <= query_date, ["trade_date", "amount"]].dropna(subset=["amount"]).tail(20)
        if sample.empty:
            return False
        return float(sample["amount"].mean()) < float(min_avg_amount)

    def _is_halted(self, symbol: str, query_date: pd.Timestamp, daily: pd.DataFrame) -> bool:
        if daily.empty:
            return False
        row = daily.loc[daily["trade_date"] == query_date]
        if row.empty:
            return False
        volume = pd.to_numeric(row["volume"], errors="coerce").iloc[0] if "volume" in row.columns else None
        amount = pd.to_numeric(row["amount"], errors="coerce").iloc[0] if "amount" in row.columns else None
        if volume is None and amount is None:
            _warn(f"volume/amount both missing for {symbol}; halt rule downgraded")
            return False
        return bool((volume == 0 if volume is not None else False) or (amount == 0 if amount is not None else False))

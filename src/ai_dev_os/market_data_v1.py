from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = REPO_ROOT / "runtime"
MARKET_POOL_ROOT = RUNTIME_ROOT / "market_pools"
MARKET_DATA_ROOT = RUNTIME_ROOT / "market_data"
DEFAULT_START_DATE = "2016-01-01"

US_ETF_WHITELIST = {
    "SPY": ("broad_index", "SP500", "SP500"),
    "QQQ": ("broad_index", "NASDAQ100", "NASDAQ100"),
    "IWM": ("broad_index", "Russell2000", "Russell2000"),
    "DIA": ("broad_index", "DowJones", "DowJones"),
    "XLK": ("sector", "Technology", "Technology"),
    "XLE": ("sector", "Energy", "Energy"),
    "XLF": ("sector", "Financials", "Financials"),
    "XLV": ("sector", "Healthcare", "Healthcare"),
    "XLI": ("sector", "Industrials", "Industrials"),
    "SMH": ("theme", "Semiconductor", "Semiconductor"),
    "SOXX": ("theme", "Semiconductor", "Semiconductor"),
}


@dataclass(frozen=True)
class PoolConfig:
    pool_name: str
    market: str
    security_type: str
    data_dir: Path
    pool_path: Path


POOL_CONFIGS = {
    "cn_etf": PoolConfig(
        pool_name="cn_etf",
        market="CN",
        security_type="etf",
        data_dir=MARKET_DATA_ROOT / "cn_etf",
        pool_path=MARKET_POOL_ROOT / "cn_etf_pool.csv",
    ),
    "cn_stock": PoolConfig(
        pool_name="cn_stock",
        market="CN",
        security_type="stock",
        data_dir=MARKET_DATA_ROOT / "cn_stock",
        pool_path=MARKET_POOL_ROOT / "cn_stock_elite_pool.csv",
    ),
    "us_etf": PoolConfig(
        pool_name="us_etf",
        market="US",
        security_type="etf",
        data_dir=MARKET_DATA_ROOT / "us_etf",
        pool_path=MARKET_POOL_ROOT / "us_etf_pool.csv",
    ),
    "us_stock": PoolConfig(
        pool_name="us_stock",
        market="US",
        security_type="stock",
        data_dir=MARKET_DATA_ROOT / "us_stock",
        pool_path=MARKET_POOL_ROOT / "us_stock_elite_pool.csv",
    ),
}

BAR_COLUMNS = [
    "market",
    "symbol",
    "security_type",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjustment_mode",
    "is_suspended",
    "listed_date",
    "delisted_date",
]

POOL_COLUMNS = {
    "base": ["market", "symbol", "security_type", "exchange"],
    "stock": [
        "industry_level_1",
        "industry_level_2",
        "market_cap",
        "float_market_cap",
        "is_st",
    ],
    "etf": ["etf_category", "etf_theme", "aum", "benchmark_index"],
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_columns(frame: pd.DataFrame, required: list[str], owner: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{owner} missing columns: {missing}")


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _normalize_float(value: Any, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _normalize_symbol(value: Any) -> str:
    return str(value).strip().upper()


def build_cn_etf_pool(spot_frame: pd.DataFrame, scale_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    _ensure_columns(spot_frame, ["symbol", "exchange", "turnover", "aum"], "cn_etf_spot")
    merged = spot_frame.copy() if scale_frame is None else spot_frame.merge(scale_frame, on="symbol", how="left", suffixes=("", "_scale"))
    leveraged = merged.get("is_leveraged", pd.Series([False] * len(merged), index=merged.index)).map(_normalize_bool)
    inverse = merged.get("is_inverse", pd.Series([False] * len(merged), index=merged.index)).map(_normalize_bool)
    filtered = merged[
        (pd.to_numeric(merged["turnover"], errors="coerce") >= 10_000_000)
        & (pd.to_numeric(merged["aum"], errors="coerce") >= 300_000_000)
        & (~leveraged)
        & (~inverse)
    ].copy()
    filtered["market"] = "CN"
    filtered["security_type"] = "etf"
    filtered["etf_category"] = filtered.get("etf_category", "unknown").fillna("unknown")
    filtered["benchmark_index"] = filtered.get("benchmark_index", "unknown").fillna("unknown")
    filtered["etf_theme"] = filtered.get("etf_theme", filtered.get("fund_name", "unknown")).fillna("unknown")
    filtered["listed_date"] = filtered.get("listed_date", "")
    columns = POOL_COLUMNS["base"] + POOL_COLUMNS["etf"] + ["listed_date"]
    return filtered[columns].sort_values("symbol").reset_index(drop=True)


def build_cn_stock_elite_pool(spot_frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "symbol",
        "exchange",
        "industry_level_1",
        "industry_level_2",
        "market_cap",
        "float_market_cap",
        "turnover",
        "is_st",
        "is_profitable_recent_2y",
        "is_sunset_industry",
        "is_one_shot_theme",
    ]
    _ensure_columns(spot_frame, required, "cn_stock_spot")
    filtered = spot_frame[
        (~spot_frame["is_st"].map(_normalize_bool))
        & (spot_frame["is_profitable_recent_2y"].map(_normalize_bool))
        & (~spot_frame["is_sunset_industry"].map(_normalize_bool))
        & (~spot_frame["is_one_shot_theme"].map(_normalize_bool))
        & (spot_frame["market_cap"] >= 8_000_000_000)
        & (spot_frame["turnover"] >= 100_000_000)
    ].copy()
    filtered["market"] = "CN"
    filtered["security_type"] = "stock"
    columns = POOL_COLUMNS["base"] + POOL_COLUMNS["stock"]
    return filtered[columns].sort_values("symbol").reset_index(drop=True)


def build_us_etf_pool(metadata_frame: pd.DataFrame) -> pd.DataFrame:
    _ensure_columns(metadata_frame, ["symbol", "exchange", "aum", "turnover"], "us_etf_metadata")
    leveraged = metadata_frame.get("is_leveraged", pd.Series([False] * len(metadata_frame), index=metadata_frame.index))
    inverse = metadata_frame.get("is_inverse", pd.Series([False] * len(metadata_frame), index=metadata_frame.index))
    filtered = metadata_frame[
        metadata_frame["symbol"].isin(US_ETF_WHITELIST)
        & (metadata_frame["aum"] >= 100_000_000)
        & (metadata_frame["turnover"] >= 5_000_000)
        & (~leveraged.map(_normalize_bool))
        & (~inverse.map(_normalize_bool))
    ].copy()
    filtered["market"] = "US"
    filtered["security_type"] = "etf"
    filtered["etf_category"] = filtered["symbol"].map(lambda s: US_ETF_WHITELIST[str(s)][0])
    filtered["benchmark_index"] = filtered["symbol"].map(lambda s: US_ETF_WHITELIST[str(s)][1])
    filtered["etf_theme"] = filtered["symbol"].map(lambda s: US_ETF_WHITELIST[str(s)][2])
    columns = POOL_COLUMNS["base"] + POOL_COLUMNS["etf"]
    return filtered[columns].sort_values("symbol").reset_index(drop=True)


def build_us_stock_elite_pool(metadata_frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "symbol",
        "exchange",
        "industry_level_1",
        "industry_level_2",
        "market_cap",
        "float_market_cap",
        "turnover",
        "is_profitable_recent_2y",
        "is_shell_like",
        "is_story_only",
        "chart_structure_ok",
    ]
    _ensure_columns(metadata_frame, required, "us_stock_metadata")
    filtered = metadata_frame[
        (metadata_frame["market_cap"] >= 2_000_000_000)
        & (metadata_frame["turnover"] >= 10_000_000)
        & (metadata_frame["is_profitable_recent_2y"].map(_normalize_bool))
        & (~metadata_frame["is_shell_like"].map(_normalize_bool))
        & (~metadata_frame["is_story_only"].map(_normalize_bool))
        & (metadata_frame["chart_structure_ok"].map(_normalize_bool))
    ].copy()
    filtered["market"] = "US"
    filtered["security_type"] = "stock"
    filtered["is_st"] = False
    columns = POOL_COLUMNS["base"] + POOL_COLUMNS["stock"]
    return filtered[columns].sort_values("symbol").reset_index(drop=True)


def write_pool_list(pool_name: str, pool_frame: pd.DataFrame) -> Path:
    config = POOL_CONFIGS[pool_name]
    config.pool_path.parent.mkdir(parents=True, exist_ok=True)
    pool_frame.to_csv(config.pool_path, index=False, encoding="utf-8")
    return config.pool_path


def load_pool_list(pool_name: str) -> pd.DataFrame:
    config = POOL_CONFIGS[pool_name]
    return pd.read_csv(config.pool_path, dtype={"symbol": str, "market": str, "security_type": str, "exchange": str})


def fetch_cn_etf_metadata(akshare_module: Any) -> pd.DataFrame:
    spot = akshare_module.fund_etf_spot_em().rename(
        columns={
            "代码": "symbol",
            "名称": "fund_name",
            "成交额": "turnover",
            "总市值": "aum",
        }
    )
    if hasattr(akshare_module, "fund_etf_fund_daily_em"):
        daily_meta = akshare_module.fund_etf_fund_daily_em().rename(
            columns={
                "基金代码": "symbol",
                "基金简称": "fund_name_meta",
                "类型": "fund_type",
            }
        )
        daily_meta["symbol"] = daily_meta["symbol"].astype(str).str.zfill(6)
        merged = spot.merge(daily_meta[["symbol", "fund_name_meta", "fund_type"]], on="symbol", how="left")
    else:
        merged = spot.copy()
        merged["fund_name_meta"] = merged.get("fund_name")
        merged["fund_type"] = "unknown"
    merged["symbol"] = merged["symbol"].astype(str).str.zfill(6)
    merged["fund_name"] = merged["fund_name"].fillna(merged.get("fund_name_meta"))
    merged["exchange"] = merged["symbol"].map(lambda s: "SSE" if str(s).startswith(("5", "6")) else "SZSE")
    merged["turnover"] = pd.to_numeric(merged["turnover"], errors="coerce")
    merged["aum"] = pd.to_numeric(merged["aum"], errors="coerce")
    names = merged.get("fund_name", pd.Series([""] * len(merged), index=merged.index)).fillna("").astype(str)
    merged["is_leveraged"] = names.str.contains("杠杆", case=False, na=False)
    merged["is_inverse"] = names.str.contains("反向|做空|inverse|short", case=False, na=False)
    merged["etf_category"] = "unknown"
    merged["benchmark_index"] = merged.get("fund_type", "unknown").fillna("unknown")
    merged["etf_theme"] = names.where(names.ne(""), "unknown")
    merged["listed_date"] = ""

    candidate = merged[
        (merged["turnover"] >= 10_000_000)
        & (merged["aum"] >= 300_000_000)
        & (~merged["is_leveraged"])
        & (~merged["is_inverse"])
        & (merged["fund_type"].isin(["指数型-股票", "指数型-海外股票"]) | merged["fund_type"].eq("unknown"))
    ].copy()
    return candidate[[
        "symbol",
        "exchange",
        "turnover",
        "aum",
        "listed_date",
        "is_leveraged",
        "is_inverse",
        "etf_category",
        "benchmark_index",
        "etf_theme",
        "fund_name",
    ]].dropna(subset=["turnover", "aum"]).drop_duplicates(subset=["symbol"])

def _fetch_cn_stock_industry_row(symbol: str, akshare_module: Any) -> dict[str, Any]:
    try:
        info = akshare_module.stock_individual_info_em(symbol=symbol)
        mapping = {str(row["item"]): row["value"] for _, row in info.iterrows()}
        industry = str(mapping.get("行业", "unknown") or "unknown")
        listed = str(mapping.get("上市时间", "") or "")
        if listed and listed.isdigit() and len(listed) == 8:
            listed = f"{listed[:4]}-{listed[4:6]}-{listed[6:]}"
        return {
            "symbol": symbol,
            "industry_level_1": industry,
            "industry_level_2": industry,
            "listed_date": listed,
        }
    except Exception:
        return {
            "symbol": symbol,
            "industry_level_1": "unknown",
            "industry_level_2": "unknown",
            "listed_date": "",
        }

def _fetch_cn_etf_listed_date(symbol: str, akshare_module: Any) -> str:
    try:
        info = akshare_module.fund_etf_fund_info_em(fund=symbol)
        if info.empty or "净值日期" not in info.columns:
            return ""
        listed = pd.to_datetime(info["净值日期"], errors="coerce").min()
        if pd.isna(listed):
            return ""
        return listed.strftime("%Y-%m-%d")
    except Exception:
        return ""


def fetch_cn_stock_metadata(akshare_module: Any) -> pd.DataFrame:
    spot = akshare_module.stock_zh_a_spot_em().rename(
        columns={
            "代码": "symbol",
            "成交额": "turnover",
            "总市值": "market_cap",
            "流通市值": "float_market_cap",
            "市盈率-动态": "pe_dynamic",
        }
    )
    spot["symbol"] = spot["symbol"].astype(str).str.zfill(6)
    st_symbols = set(akshare_module.stock_zh_a_st_em()["代码"].astype(str).str.zfill(6).tolist())
    coarse = spot[
        (pd.to_numeric(spot["market_cap"], errors="coerce") >= 8_000_000_000)
        & (pd.to_numeric(spot["turnover"], errors="coerce") >= 100_000_000)
    ].copy()
    coarse["exchange"] = coarse["symbol"].map(lambda s: "SSE" if str(s).startswith(("6", "9")) else "SZSE")
    coarse["is_st"] = coarse["symbol"].isin(st_symbols)
    coarse["is_profitable_recent_2y"] = pd.to_numeric(coarse["pe_dynamic"], errors="coerce") > 0
    coarse["is_sunset_industry"] = False
    coarse["is_one_shot_theme"] = False
    enrich_rows = [_fetch_cn_stock_industry_row(symbol, akshare_module) for symbol in coarse["symbol"].tolist()]
    enrich = pd.DataFrame(enrich_rows)
    merged = coarse.merge(enrich, on="symbol", how="left")
    return merged[
        [
            "symbol",
            "exchange",
            "industry_level_1",
            "industry_level_2",
            "market_cap",
            "float_market_cap",
            "turnover",
            "is_st",
            "is_profitable_recent_2y",
            "is_sunset_industry",
            "is_one_shot_theme",
            "listed_date",
        ]
    ].drop_duplicates(subset=["symbol"])


def fetch_us_etf_metadata(yfinance_module: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol, (category, benchmark, theme) in US_ETF_WHITELIST.items():
        ticker = yfinance_module.Ticker(symbol)
        info = getattr(ticker, "fast_info", {}) or {}
        rows.append(
            {
                "symbol": symbol,
                "exchange": str(info.get("exchange", "US") or "US"),
                "aum": _normalize_float(info.get("market_cap"), 0.0),
                "turnover": _normalize_float(info.get("last_price"), 0.0) * _normalize_float(info.get("last_volume"), 0.0),
                "is_leveraged": False,
                "is_inverse": False,
                "etf_category": category,
                "benchmark_index": benchmark,
                "etf_theme": theme,
            }
        )
    return pd.DataFrame(rows)


def fetch_us_stock_metadata(akshare_module: Any, yfinance_module: Any, candidate_limit: int = 120) -> pd.DataFrame:
    spot = akshare_module.stock_us_spot_em().rename(columns={"代码": "code", "总市值": "market_cap"})
    spot["symbol"] = spot["code"].astype(str).str.split(".").str[-1].str.upper()
    spot["market_cap"] = pd.to_numeric(spot["market_cap"], errors="coerce")
    candidates = spot.sort_values("market_cap", ascending=False).dropna(subset=["market_cap"]).head(candidate_limit)
    rows: list[dict[str, Any]] = []
    for symbol in candidates["symbol"].tolist():
        try:
            ticker = yfinance_module.Ticker(symbol)
            info = ticker.info or {}
            fast = getattr(ticker, "fast_info", {}) or {}
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": str(info.get("exchange", fast.get("exchange", "US")) or "US"),
                    "industry_level_1": str(info.get("sector", "unknown") or "unknown"),
                    "industry_level_2": str(info.get("industry", "unknown") or "unknown"),
                    "market_cap": _normalize_float(info.get("marketCap", fast.get("market_cap", 0.0))),
                    "float_market_cap": _normalize_float(info.get("marketCap", fast.get("market_cap", 0.0))),
                    "turnover": _normalize_float(fast.get("last_price", 0.0)) * _normalize_float(fast.get("last_volume", 0.0)),
                    "is_profitable_recent_2y": _normalize_float(info.get("trailingPE", 0.0)) > 0,
                    "is_shell_like": str(info.get("exchange", "")).upper() == "OTC",
                    "is_story_only": False,
                    "chart_structure_ok": True,
                }
            )
        except Exception:
            continue
    return pd.DataFrame(rows)


def generate_pool_lists(
    provider_modules: dict[str, Any] | None = None,
    selected_pools: list[str] | None = None,
) -> dict[str, str]:
    modules = load_market_source_modules() if provider_modules is None else provider_modules
    targets = selected_pools or ["cn_etf", "cn_stock", "us_etf", "us_stock"]
    generated: dict[str, str] = {}

    if "cn_etf" in targets:
        cn_etf_pool = build_cn_etf_pool(fetch_cn_etf_metadata(modules["akshare"]))
        generated["cn_etf"] = str(write_pool_list("cn_etf", cn_etf_pool))

    if "cn_stock" in targets:
        cn_stock_pool = build_cn_stock_elite_pool(fetch_cn_stock_metadata(modules["akshare"]))
        generated["cn_stock"] = str(write_pool_list("cn_stock", cn_stock_pool))

    if "us_etf" in targets:
        us_etf_pool = build_us_etf_pool(fetch_us_etf_metadata(modules["yfinance"]))
        generated["us_etf"] = str(write_pool_list("us_etf", us_etf_pool))

    if "us_stock" in targets:
        us_stock_pool = build_us_stock_elite_pool(fetch_us_stock_metadata(modules["akshare"], modules["yfinance"]))
        generated["us_stock"] = str(write_pool_list("us_stock", us_stock_pool))

    return generated

def standardize_market_bars(
    raw_frame: pd.DataFrame,
    *,
    market: str,
    symbol: str,
    security_type: str,
    adjustment_mode: str = "qfq",
    listed_date: str = "",
    delisted_date: str = "",
) -> pd.DataFrame:
    _ensure_columns(raw_frame, ["trade_date", "open", "high", "low", "close", "volume"], "raw_market_bars")
    frame = raw_frame.copy()
    if "amount" not in frame.columns:
        frame["amount"] = pd.to_numeric(frame["close"], errors="coerce") * pd.to_numeric(frame["volume"], errors="coerce")
    if "is_suspended" not in frame.columns:
        frame["is_suspended"] = False
    frame["market"] = market
    frame["symbol"] = symbol
    frame["security_type"] = security_type
    frame["adjustment_mode"] = adjustment_mode
    frame["listed_date"] = listed_date
    frame["delisted_date"] = delisted_date
    frame = frame[BAR_COLUMNS].copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["is_suspended"] = frame["is_suspended"].map(_normalize_bool)
    return frame.sort_values("trade_date").reset_index(drop=True)


def _normalize_akshare_history(
    raw_frame: pd.DataFrame,
    *,
    market: str,
    symbol: str,
    security_type: str,
    listed_date: str = "",
    delisted_date: str = "",
) -> pd.DataFrame:
    rename_map = {
        "日期": "trade_date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
    }
    frame = raw_frame.rename(columns=rename_map)
    if "amount" not in frame.columns:
        frame["amount"] = pd.to_numeric(frame.get("close"), errors="coerce") * pd.to_numeric(frame.get("volume"), errors="coerce")
    return standardize_market_bars(
        frame,
        market=market,
        symbol=symbol,
        security_type=security_type,
        adjustment_mode="qfq",
        listed_date=listed_date,
        delisted_date=delisted_date,
    )


def _normalize_yfinance_history(
    raw_frame: pd.DataFrame,
    *,
    market: str,
    symbol: str,
    security_type: str,
    listed_date: str = "",
    delisted_date: str = "",
) -> pd.DataFrame:
    if raw_frame.index.name is None:
        raw_frame = raw_frame.copy()
        raw_frame.index.name = "Date"
    frame = raw_frame.reset_index().rename(
        columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    return standardize_market_bars(
        frame,
        market=market,
        symbol=symbol,
        security_type=security_type,
        adjustment_mode="qfq",
        listed_date=listed_date,
        delisted_date=delisted_date,
    )


def fetch_cn_daily_bars(
    symbol: str,
    *,
    security_type: str,
    start_date: str,
    end_date: str,
    akshare_module: Any,
    listed_date: str = "",
    delisted_date: str = "",
) -> pd.DataFrame:
    start = start_date.replace("-", "")
    end = end_date.replace("-", "")
    if security_type == "stock":
        raw = akshare_module.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start, end_date=end, adjust="qfq")
    else:
        raw = akshare_module.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start, end_date=end, adjust="qfq")
    return _normalize_akshare_history(raw, market="CN", symbol=symbol, security_type=security_type, listed_date=listed_date, delisted_date=delisted_date)


def fetch_us_daily_bars(
    symbol: str,
    *,
    security_type: str,
    start_date: str,
    end_date: str,
    yfinance_module: Any,
    listed_date: str = "",
    delisted_date: str = "",
) -> pd.DataFrame:
    ticker = yfinance_module.Ticker(symbol)
    raw = ticker.history(start=start_date, end=end_date, auto_adjust=True, actions=False)
    return _normalize_yfinance_history(raw, market="US", symbol=symbol, security_type=security_type, listed_date=listed_date, delisted_date=delisted_date)


def validate_market_bars(frame: pd.DataFrame, *, warmup_rows: int = 0) -> dict[str, Any]:
    _ensure_columns(frame, BAR_COLUMNS, "market_bars")
    failed: list[str] = []
    passed: list[str] = []

    trade_dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    if trade_dates.is_monotonic_increasing:
        passed.append("trade_date_ascending")
    else:
        failed.append("trade_date_ascending")

    body = frame.iloc[warmup_rows:].copy() if warmup_rows > 0 else frame.copy()
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        series = pd.to_numeric(body[column], errors="coerce")
        if series.isna().any() or (series < 0).any():
            failed.append(f"{column}_non_negative_non_null")
        else:
            passed.append(f"{column}_non_negative_non_null")

    if (frame["adjustment_mode"] == "qfq").all():
        passed.append("adjustment_mode_consistent")
    else:
        failed.append("adjustment_mode_consistent")

    suspended_mask = frame["is_suspended"].map(_normalize_bool)
    if suspended_mask.any():
        suspended_prices = frame.loc[suspended_mask, ["open", "high", "low", "close"]]
        if suspended_prices.isna().all(axis=1).all():
            passed.append("suspension_no_fabricated_price")
        else:
            failed.append("suspension_no_fabricated_price")
    else:
        passed.append("suspension_no_fabricated_price")

    return {
        "status": "passed" if not failed else "failed",
        "checks_passed": passed,
        "checks_failed": failed,
        "validated_rows": int(len(frame)),
    }


def write_market_parquet(pool_name: str, symbol: str, frame: pd.DataFrame) -> Path:
    config = POOL_CONFIGS[pool_name]
    config.data_dir.mkdir(parents=True, exist_ok=True)
    path = config.data_dir / f"{symbol}.parquet"
    frame.to_parquet(path, index=False)
    return path


def write_pool_summary(
    pool_name: str,
    *,
    target_count: int,
    success_count: int,
    start_date: str,
    end_date: str,
    failed_symbols: list[str],
    skipped_symbols: list[str] | None = None,
) -> Path:
    config = POOL_CONFIGS[pool_name]
    config.data_dir.mkdir(parents=True, exist_ok=True)
    skipped_symbols = skipped_symbols or []
    payload = {
        "pool_name": pool_name,
        "target_count": target_count,
        "success_count": success_count,
        "failed_count": len(failed_symbols),
        "skipped_count": len(skipped_symbols),
        "start_date": start_date,
        "end_date": end_date,
        "updated_at": _now_iso(),
        "failed_symbols": failed_symbols,
        "skipped_symbols": skipped_symbols,
    }
    path = config.data_dir / "_summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return path


def load_market_source_modules() -> dict[str, Any]:
    import akshare as ak  # type: ignore
    import yfinance as yf  # type: ignore

    return {"akshare": ak, "yfinance": yf}


def fetch_pool_market_data(
    pool_name: str,
    *,
    start_date: str = DEFAULT_START_DATE,
    end_date: str,
    provider_modules: dict[str, Any] | None = None,
    skip_existing: bool = False,
) -> dict[str, Any]:
    config = POOL_CONFIGS[pool_name]
    modules = load_market_source_modules() if provider_modules is None else provider_modules
    pool_frame = load_pool_list(pool_name)
    failed_symbols: list[str] = []
    skipped_symbols: list[str] = []
    written_files: list[str] = []

    for row in pool_frame.to_dict(orient="records"):
        symbol = str(row["symbol"])
        listed_date = str(row.get("listed_date", "") or "")
        delisted_date = str(row.get("delisted_date", "") or "")
        target_path = config.data_dir / f"{symbol}.parquet"
        if skip_existing and target_path.exists():
            skipped_symbols.append(symbol)
            continue
        try:
            if config.market == "CN":
                frame = fetch_cn_daily_bars(
                    symbol,
                    security_type=config.security_type,
                    start_date=start_date,
                    end_date=end_date,
                    akshare_module=modules["akshare"],
                    listed_date=listed_date,
                    delisted_date=delisted_date,
                )
            else:
                frame = fetch_us_daily_bars(
                    symbol,
                    security_type=config.security_type,
                    start_date=start_date,
                    end_date=end_date,
                    yfinance_module=modules["yfinance"],
                    listed_date=listed_date,
                    delisted_date=delisted_date,
                )
            result = validate_market_bars(frame)
            if result["status"] != "passed":
                failed_symbols.append(symbol)
                continue
            written_files.append(str(write_market_parquet(pool_name, symbol, frame)))
        except Exception:
            failed_symbols.append(symbol)

    summary_path = write_pool_summary(
        pool_name,
        target_count=len(pool_frame),
        success_count=len(written_files),
        start_date=start_date,
        end_date=end_date,
        failed_symbols=failed_symbols,
        skipped_symbols=skipped_symbols,
    )
    return {
        "pool_name": pool_name,
        "symbol_count": int(len(pool_frame)),
        "written_files": written_files,
        "failed_symbols": failed_symbols,
        "skipped_symbols": skipped_symbols,
        "summary_path": str(summary_path),
    }

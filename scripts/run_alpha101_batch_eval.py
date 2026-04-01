from __future__ import annotations

import argparse
import contextlib
import csv
import gc
import json
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd


WORKTREE_ROOT = Path(__file__).resolve().parents[1]
PRIMARY_ROOT = Path(r"D:\AITradingSystem")
SRC_ROOT = WORKTREE_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.factors import alpha101  # noqa: E402
from alpha_research.gpu_ic_calculator import GPUIcCalculator  # noqa: E402


MARKET_COLUMNS = ["trade_date", "symbol", "open", "high", "low", "close", "volume", "amount", "is_suspended"]
SUMMARY_COLUMNS = [
    "factor_id",
    "factor_name",
    "ic_mean",
    "ic_std",
    "icir",
    "icir_neutralized",
    "category",
    "status",
]
EPSILON = 1e-12


@dataclass
class MarketInputs:
    dates: pd.DatetimeIndex
    symbols: list[str]
    open: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    close: pd.DataFrame
    volume: pd.DataFrame
    amount: pd.DataFrame
    returns: pd.DataFrame
    vwap: pd.DataFrame
    adv20: pd.DataFrame
    forward_returns_1d: pd.DataFrame
    base_mask: pd.DataFrame
    st_mask: pd.DataFrame
    log_mv: pd.DataFrame
    industry_codes: np.ndarray


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Alpha101 batch IC evaluation on CN stocks.")
    parser.add_argument("--root", type=Path, default=PRIMARY_ROOT, help="Primary repository root containing runtime data.")
    parser.add_argument("--start", default="2016-01-01", help="In-sample start date.")
    parser.add_argument("--end", default="2023-12-31", help="In-sample end date.")
    parser.add_argument("--oos-start", default="2024-01-01", help="Out-of-sample start date.")
    parser.add_argument("--oos-end", default=None, help="Out-of-sample end date. Defaults to latest available date.")
    parser.add_argument("--preload-start", default="2015-01-01", help="Warm-up data start date.")
    parser.add_argument("--batch-size", type=int, default=20, help="Factors per batch before forced GC.")
    parser.add_argument("--factors", nargs="*", default=None, help="Optional factor ids or names, e.g. 1 5 alpha010.")
    return parser.parse_args()


def _normalize_factor_name(token: str) -> str:
    token = token.strip().lower()
    if token.startswith("alpha"):
        suffix = token[5:]
    else:
        suffix = token
    return f"alpha{int(suffix):03d}"


def discover_factor_names(selected: list[str] | None) -> list[str]:
    all_names = [f"alpha{i:03d}" for i in alpha101.IMPLEMENTED_ALPHA_IDS]
    if not selected:
        return all_names
    selected_names = {_normalize_factor_name(item) for item in selected}
    return [name for name in all_names if name in selected_names]


def load_knowledge_base() -> dict[str, dict[str, Any]]:
    payload = json.loads((WORKTREE_ROOT / "src" / "alpha_research" / "knowledge_base" / "alpha101_library.json").read_text(encoding="utf-8"))
    return {item["id"]: item for item in payload}


def load_abnormal_symbols(root: Path) -> set[str]:
    payload = json.loads((root / "runtime" / "download_log" / "abnormal_files.json").read_text(encoding="utf-8"))
    return {str(item["symbol"]).zfill(6) for item in payload}


def _read_market_file(path: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=MARKET_COLUMNS)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.loc[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
    if df.empty:
        return df
    numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["is_suspended"] = df["is_suspended"].fillna(True).astype(bool)
    return df


def _prepare_listing_mask(
    dates: pd.DatetimeIndex,
    symbols: list[str],
    stock_basic: pd.DataFrame,
) -> pd.DataFrame:
    listed_dates = stock_basic.set_index("symbol")["list_date"].to_dict()
    date_values = dates.to_numpy(dtype="datetime64[D]")
    listed_values = np.array([pd.Timestamp(listed_dates.get(symbol)).to_datetime64() if symbol in listed_dates else np.datetime64("NaT") for symbol in symbols], dtype="datetime64[D]")
    valid_listed = ~np.isnat(listed_values)
    age_days = np.zeros((len(dates), len(symbols)), dtype=np.int32)
    if valid_listed.any():
        age_days[:, valid_listed] = (
            date_values[:, None].astype("datetime64[D]") - listed_values[valid_listed][None, :]
        ).astype("timedelta64[D]").astype(np.int32)
    listing_mask = age_days >= 60
    listing_mask[:, ~valid_listed] = False
    return pd.DataFrame(listing_mask, index=dates, columns=symbols, dtype=bool)


def _prepare_st_mask(
    dates: pd.DatetimeIndex,
    symbols: list[str],
    st_history: pd.DataFrame,
) -> pd.DataFrame:
    st_history = st_history.loc[st_history['name'].astype(str).str.contains('ST', na=False)].copy()
    st_history = st_history.sort_values(['ts_code', 'start_date', 'end_date']).drop_duplicates(['ts_code', 'start_date', 'end_date', 'name'], keep='last')
    st_mask = np.zeros((len(dates), len(symbols)), dtype=bool)
    symbol_to_col = {symbol: index for index, symbol in enumerate(symbols)}
    date_values = dates.to_numpy()
    for row in st_history.itertuples(index=False):
        symbol = str(row.ts_code).split(".")[0].zfill(6)
        if symbol not in symbol_to_col:
            continue
        start = pd.Timestamp(row.start_date)
        end = pd.Timestamp(row.end_date) if pd.notna(row.end_date) else dates[-1]
        left = date_values.searchsorted(np.datetime64(start), side="left")
        right = date_values.searchsorted(np.datetime64(end), side="right")
        if left < right:
            st_mask[left:right, symbol_to_col[symbol]] = True
    return pd.DataFrame(st_mask, index=dates, columns=symbols, dtype=bool)


def _prepare_valuation_matrix(
    root: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    dates: pd.DatetimeIndex,
    symbols: list[str],
) -> pd.DataFrame:
    valuation = pd.read_parquet(
        root / "runtime" / "fundamental_data" / "valuation_daily.parquet",
        columns=["date", "instrument_code", "total_mv"],
    )
    valuation["date"] = pd.to_datetime(valuation["date"])
    valuation = valuation.loc[(valuation["date"] >= start) & (valuation["date"] <= end)].copy()
    valuation["instrument_code"] = valuation["instrument_code"].astype(str).str.zfill(6)
    valuation["total_mv"] = pd.to_numeric(valuation["total_mv"], errors="coerce")
    valuation = valuation.sort_values(["date", "instrument_code"]).drop_duplicates(["date", "instrument_code"], keep="last")
    valuation = valuation.pivot(index="date", columns="instrument_code", values="total_mv")
    valuation = valuation.reindex(index=dates, columns=symbols)
    return np.log(valuation.replace(0.0, np.nan))


def load_market_inputs(root: Path, preload_start: str, oos_end: str | None) -> MarketInputs:
    market_dir = root / "runtime" / "market_data" / "cn_stock"
    preload_start_ts = pd.Timestamp(preload_start)
    oos_end_ts = pd.Timestamp(oos_end) if oos_end else pd.Timestamp.today().normalize()
    abnormal_symbols = load_abnormal_symbols(root)
    files = sorted(path for path in market_dir.glob("*.parquet") if path.stem != "_summary" and path.stem not in abnormal_symbols)

    open_data: dict[str, pd.Series] = {}
    high_data: dict[str, pd.Series] = {}
    low_data: dict[str, pd.Series] = {}
    close_data: dict[str, pd.Series] = {}
    volume_data: dict[str, pd.Series] = {}
    amount_data: dict[str, pd.Series] = {}
    suspended_data: dict[str, pd.Series] = {}

    for index, file_path in enumerate(files, start=1):
        df = _read_market_file(file_path, preload_start_ts, oos_end_ts)
        if df.empty:
            continue
        symbol = str(df["symbol"].iloc[0]).zfill(6)
        date_index = pd.DatetimeIndex(df["trade_date"])
        open_data[symbol] = pd.Series(df["open"].to_numpy(dtype=float), index=date_index)
        high_data[symbol] = pd.Series(df["high"].to_numpy(dtype=float), index=date_index)
        low_data[symbol] = pd.Series(df["low"].to_numpy(dtype=float), index=date_index)
        close_data[symbol] = pd.Series(df["close"].to_numpy(dtype=float), index=date_index)
        volume_data[symbol] = pd.Series(df["volume"].to_numpy(dtype=float), index=date_index)
        amount_data[symbol] = pd.Series(df["amount"].to_numpy(dtype=float), index=date_index)
        suspended_data[symbol] = pd.Series(df["is_suspended"].to_numpy(dtype=bool), index=date_index)
        if index % 500 == 0:
            print(f"[load] read {index}/{len(files)} market files", flush=True)

    close = pd.DataFrame(close_data).sort_index()
    symbols = [str(symbol) for symbol in close.columns.tolist()]
    dates = pd.DatetimeIndex(close.index)

    open_ = pd.DataFrame(open_data).reindex(index=dates, columns=symbols).astype(float)
    high = pd.DataFrame(high_data).reindex(index=dates, columns=symbols).astype(float)
    low = pd.DataFrame(low_data).reindex(index=dates, columns=symbols).astype(float)
    volume = pd.DataFrame(volume_data).reindex(index=dates, columns=symbols).astype(float)
    amount = pd.DataFrame(amount_data).reindex(index=dates, columns=symbols).astype(float)
    suspended = pd.DataFrame(suspended_data).reindex(index=dates, columns=symbols)
    suspended = suspended.where(pd.notna(suspended), True).astype(bool)

    stock_basic = pd.read_parquet(root / "runtime" / "fundamental_data" / "stock_basic.parquet")
    stock_basic["symbol"] = stock_basic["symbol"].astype(str).str.zfill(6)
    stock_basic["list_date"] = pd.to_datetime(stock_basic["list_date"], errors="coerce")
    stock_basic = stock_basic.drop_duplicates(subset=["symbol"], keep="last")

    st_history = pd.read_parquet(root / "runtime" / "fundamental_data" / "st_history.parquet")
    st_mask = _prepare_st_mask(dates=dates, symbols=symbols, st_history=st_history)
    listing_mask = _prepare_listing_mask(dates=dates, symbols=symbols, stock_basic=stock_basic)
    base_mask = (~suspended) & (~st_mask) & listing_mask

    log_mv = _prepare_valuation_matrix(
        root=root,
        start=preload_start_ts,
        end=dates[-1],
        dates=dates,
        symbols=symbols,
    )

    industry_series = stock_basic.set_index("symbol").reindex(symbols)["industry"].fillna("UNKNOWN")
    industry_codes = pd.Categorical(industry_series).codes.astype(np.int32)

    volume_nonzero = volume.replace(0.0, np.nan)
    vwap = amount.divide(volume_nonzero).replace([np.inf, -np.inf], np.nan)
    returns = close.pct_change(fill_method=None)
    adv20 = volume_nonzero.rolling(20, min_periods=20).mean()
    forward_returns_1d = close.shift(-1).divide(close).subtract(1.0)

    return MarketInputs(
        dates=dates,
        symbols=symbols,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume_nonzero,
        amount=amount,
        returns=returns,
        vwap=vwap,
        adv20=adv20,
        forward_returns_1d=forward_returns_1d,
        base_mask=base_mask,
        st_mask=st_mask,
        log_mv=log_mv,
        industry_codes=industry_codes,
    )


@contextlib.contextmanager
def factor_runtime_context(inputs: MarketInputs) -> Iterator[None]:
    original_load_inputs = alpha101._load_inputs
    original_stack_factor = alpha101._stack_factor

    def _patched_load_inputs(_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "asset_name": "symbol",
            "open": inputs.open,
            "high": inputs.high,
            "low": inputs.low,
            "close": inputs.close,
            "volume": inputs.volume,
            "amount": inputs.amount,
            "returns": inputs.returns,
            "vwap": inputs.vwap,
            "adv20": inputs.adv20,
        }

    def _patched_stack_factor(frame: pd.DataFrame, _name: str, _asset_name: str) -> pd.DataFrame:
        cleaned = frame.replace([np.inf, -np.inf], np.nan)
        return cleaned.astype(float)

    alpha101._load_inputs = _patched_load_inputs
    alpha101._stack_factor = _patched_stack_factor
    try:
        yield
    finally:
        alpha101._load_inputs = original_load_inputs
        alpha101._stack_factor = original_stack_factor


def _corr_from_ranks(left: np.ndarray, right: np.ndarray) -> float:
    if left.size < 3:
        return float("nan")
    left_rank = pd.Series(left).rank(method="average").to_numpy(dtype=float)
    right_rank = pd.Series(right).rank(method="average").to_numpy(dtype=float)
    left_center = left_rank - left_rank.mean()
    right_center = right_rank - right_rank.mean()
    denom = math.sqrt(float(np.dot(left_center, left_center) * np.dot(right_center, right_center)))
    if denom <= EPSILON:
        return float("nan")
    return float(np.dot(left_center, right_center) / denom)




def build_batch_factor_matrix(factor_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    stacked = {
        factor_name: frame.stack(dropna=False)
        for factor_name, frame in factor_frames.items()
    }
    return pd.DataFrame(stacked)

def _group_demean(values: np.ndarray, groups: np.ndarray) -> np.ndarray:
    group_count = int(groups.max()) + 1
    counts = np.bincount(groups, minlength=group_count).astype(float)
    sums = np.bincount(groups, weights=values, minlength=group_count).astype(float)
    means = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)
    return values - means[groups]


def neutralize_industry_size(values: np.ndarray, log_mv: np.ndarray, industry_codes: np.ndarray) -> np.ndarray:
    mask = np.isfinite(values) & np.isfinite(log_mv) & (industry_codes >= 0)
    if mask.sum() < 20:
        return np.full(values.shape, np.nan, dtype=float)
    y = values[mask].astype(float)
    x = log_mv[mask].astype(float)
    groups = industry_codes[mask].astype(np.int32)
    y_dm = _group_demean(y, groups)
    x_dm = _group_demean(x, groups)
    denom = float(np.dot(x_dm, x_dm))
    residual = y_dm if denom <= EPSILON else y_dm - (float(np.dot(x_dm, y_dm)) / denom) * x_dm
    output = np.full(values.shape, np.nan, dtype=float)
    output[np.flatnonzero(mask)] = residual
    return output


def _evaluate_period(
    factor_frame: pd.DataFrame,
    inputs: MarketInputs,
    period_mask: np.ndarray,
    precomputed_ic: pd.Series | None = None,
) -> dict[str, Any]:
    factor_array = factor_frame.reindex(index=inputs.dates, columns=inputs.symbols).to_numpy(dtype=float, copy=False)
    fwd_array = inputs.forward_returns_1d.to_numpy(dtype=float, copy=False)
    base_array = inputs.base_mask.to_numpy(dtype=bool, copy=False)
    mv_array = inputs.log_mv.to_numpy(dtype=float, copy=False)
    industry_codes = inputs.industry_codes

    ic_rows: list[dict[str, Any]] = []
    q_returns = [[] for _ in range(5)]
    long_short_series: list[float] = []
    usable_obs = 0

    for date_idx in np.flatnonzero(period_mask):
        x = factor_array[date_idx]
        y = fwd_array[date_idx]
        base = base_array[date_idx]
        mv = mv_array[date_idx]
        valid = base & np.isfinite(x) & np.isfinite(y)
        if valid.sum() < 20:
            continue
        usable_obs += int(valid.sum())
        x_valid = x[valid]
        y_valid = y[valid]
        current_date = inputs.dates[date_idx]

        if precomputed_ic is not None and current_date in precomputed_ic.index:
            ic_value = float(precomputed_ic.loc[current_date])
        else:
            ic_value = _corr_from_ranks(x_valid, y_valid)
        x_neutral = neutralize_industry_size(x, mv, industry_codes)
        neutral_valid = np.isfinite(x_neutral) & np.isfinite(y) & base
        neutral_ic = _corr_from_ranks(x_neutral[neutral_valid], y[neutral_valid]) if neutral_valid.sum() >= 20 else float("nan")

        ic_rows.append(
            {
                "date": current_date,
                "year": int(current_date.year),
                "ic": ic_value,
                "ic_neutralized": neutral_ic,
                "n_assets": int(valid.sum()),
            }
        )

        order = np.argsort(x_valid, kind="mergesort")
        buckets = np.array_split(order, 5)
        bucket_means = [float(np.nanmean(y_valid[bucket])) if len(bucket) else float("nan") for bucket in buckets]
        for bucket_index, bucket_mean in enumerate(bucket_means):
            q_returns[bucket_index].append(bucket_mean)
        if np.isfinite(bucket_means[0]) and np.isfinite(bucket_means[-1]):
            long_short_series.append(bucket_means[-1] - bucket_means[0])

    ic_df = pd.DataFrame(ic_rows)
    if ic_df.empty:
        return {
            "ic_series": [],
            "ic_mean": 0.0,
            "ic_std": 0.0,
            "icir": 0.0,
            "ic_mean_neutralized": 0.0,
            "ic_std_neutralized": 0.0,
            "icir_neutralized": 0.0,
            "ic_positive_pct": 0.0,
            "yearly_ic": {},
            "layered_returns": {f"q{i}_annual_excess": 0.0 for i in range(1, 6)} | {
                "long_short_annual_return": 0.0,
                "long_short_sharpe": 0.0,
                "long_short_max_drawdown": 0.0,
                "is_monotonic": False,
            },
            "n_dates": 0,
            "n_obs": 0,
        }

    ic_mean = float(ic_df["ic"].mean())
    ic_std = float(ic_df["ic"].std(ddof=0))
    icir = float(ic_mean / ic_std) if ic_std > EPSILON else 0.0
    neutral_mean = float(ic_df["ic_neutralized"].mean())
    neutral_std = float(ic_df["ic_neutralized"].std(ddof=0))
    neutral_icir = float(neutral_mean / neutral_std) if neutral_std > EPSILON else 0.0

    yearly_ic = {
        str(int(year)): float(group["ic"].mean())
        for year, group in ic_df.groupby("year")
    }

    q_annual = {
        f"q{bucket_index + 1}_annual_excess": float(np.nanmean(bucket_values) * 252.0) if bucket_values else 0.0
        for bucket_index, bucket_values in enumerate(q_returns)
    }
    q_curve = [q_annual[f"q{i}_annual_excess"] for i in range(1, 6)]
    monotonic_up = all(left <= right for left, right in zip(q_curve, q_curve[1:]))
    monotonic_down = all(left >= right for left, right in zip(q_curve, q_curve[1:]))

    ls_array = np.array(long_short_series, dtype=float)
    if ls_array.size:
        cumulative = np.cumsum(ls_array)
        peak = np.maximum.accumulate(cumulative)
        drawdown = cumulative - peak
        ls_annual = float(np.nanmean(ls_array) * 252.0)
        ls_std = float(np.nanstd(ls_array, ddof=0))
        ls_sharpe = float(np.nanmean(ls_array) / ls_std * math.sqrt(252.0)) if ls_std > EPSILON else 0.0
        max_drawdown = float(drawdown.min())
    else:
        ls_annual = 0.0
        ls_sharpe = 0.0
        max_drawdown = 0.0

    return {
        "ic_series": [
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "ic": float(row["ic"]),
                "ic_neutralized": float(row["ic_neutralized"]) if pd.notna(row["ic_neutralized"]) else None,
                "n_assets": int(row["n_assets"]),
            }
            for row in ic_rows
        ],
        "ic_mean": ic_mean,
        "ic_std": ic_std,
        "icir": icir,
        "ic_mean_neutralized": neutral_mean,
        "ic_std_neutralized": neutral_std,
        "icir_neutralized": neutral_icir,
        "ic_positive_pct": float((ic_df["ic"] > 0).mean()),
        "yearly_ic": yearly_ic,
        "layered_returns": q_annual
        | {
            "long_short_annual_return": ls_annual,
            "long_short_sharpe": ls_sharpe,
            "long_short_max_drawdown": max_drawdown,
            "is_monotonic": bool(monotonic_up or monotonic_down),
        },
        "n_dates": int(len(ic_df)),
        "n_obs": usable_obs,
    }


def status_from_icir(icir_neutralized: float) -> str:
    if icir_neutralized > 0.1:
        return "pass"
    if icir_neutralized >= 0.05:
        return "weak"
    return "fail"


def evaluate_factor(
    factor_name: str,
    factor_frame: pd.DataFrame,
    knowledge: dict[str, dict[str, Any]],
    inputs: MarketInputs,
    in_sample_mask: np.ndarray,
    oos_mask: np.ndarray,
    precomputed_ic: pd.Series | None = None,
) -> dict[str, Any]:
    meta = knowledge.get(factor_name, {})
    in_sample = _evaluate_period(
        factor_frame=factor_frame,
        inputs=inputs,
        period_mask=in_sample_mask,
        precomputed_ic=precomputed_ic,
    )
    out_sample = _evaluate_period(
        factor_frame=factor_frame,
        inputs=inputs,
        period_mask=oos_mask,
        precomputed_ic=precomputed_ic,
    )
    status = status_from_icir(in_sample["icir_neutralized"])
    factor_id = int(factor_name.replace("alpha", ""))
    return {
        "factor_id": factor_id,
        "factor_name": factor_name,
        "category": meta.get("category", "unknown"),
        "formula_original": meta.get("formula_original"),
        "implementation_status": meta.get("status"),
        "sample_period": {
            "in_sample": {
                "start": str(inputs.dates[in_sample_mask][0].date()) if in_sample_mask.any() else None,
                "end": str(inputs.dates[in_sample_mask][-1].date()) if in_sample_mask.any() else None,
            },
            "out_of_sample": {
                "start": str(inputs.dates[oos_mask][0].date()) if oos_mask.any() else None,
                "end": str(inputs.dates[oos_mask][-1].date()) if oos_mask.any() else None,
            },
        },
        "basic_metrics": {
            "ic_mean": in_sample["ic_mean"],
            "ic_std": in_sample["ic_std"],
            "icir": in_sample["icir"],
            "ic_positive_pct": in_sample["ic_positive_pct"],
            "ic_mean_neutralized": in_sample["ic_mean_neutralized"],
            "ic_std_neutralized": in_sample["ic_std_neutralized"],
            "icir_neutralized": in_sample["icir_neutralized"],
            "n_dates": in_sample["n_dates"],
            "n_obs": in_sample["n_obs"],
        },
        "layered_returns": in_sample["layered_returns"],
        "yearly_ic": in_sample["yearly_ic"],
        "neutralization_compare": {
            "raw_icir": in_sample["icir"],
            "industry_size_neutral_icir": in_sample["icir_neutralized"],
        },
        "out_of_sample_metrics": {
            "ic_mean": out_sample["ic_mean"],
            "ic_std": out_sample["ic_std"],
            "icir": out_sample["icir"],
            "ic_mean_neutralized": out_sample["ic_mean_neutralized"],
            "ic_std_neutralized": out_sample["ic_std_neutralized"],
            "icir_neutralized": out_sample["icir_neutralized"],
            "n_dates": out_sample["n_dates"],
            "n_obs": out_sample["n_obs"],
        },
        "status": status,
        "ic_series": in_sample["ic_series"],
    }


def write_factor_report(root: Path, factor_name: str, report: dict[str, Any]) -> None:
    report_dir = root / "runtime" / "alpha_research" / "factor_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    target = report_dir / f"{factor_name}_report.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def write_summary(root: Path, rows: list[dict[str, Any]]) -> Path:
    output_path = root / "runtime" / "alpha_research" / "alpha101_ic_summary.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in SUMMARY_COLUMNS})
    return output_path


def main() -> int:
    args = parse_args()
    factor_names = discover_factor_names(args.factors)
    if not factor_names:
        raise SystemExit("No implemented Alpha101 factors selected.")

    started_at = time.time()
    knowledge = load_knowledge_base()
    inputs = load_market_inputs(root=args.root, preload_start=args.preload_start, oos_end=args.oos_end)

    in_sample_mask = (inputs.dates >= pd.Timestamp(args.start)) & (inputs.dates <= pd.Timestamp(args.end))
    oos_mask = inputs.dates >= pd.Timestamp(args.oos_start)
    if args.oos_end:
        oos_mask &= inputs.dates <= pd.Timestamp(args.oos_end)

    summary_rows: list[dict[str, Any]] = []
    ic_calculator = GPUIcCalculator()
    print(f"[ic] batch calculator device={ic_calculator.device}", flush=True)
    with factor_runtime_context(inputs):
        dummy = pd.DataFrame()
        for batch_start in range(0, len(factor_names), args.batch_size):
            batch = factor_names[batch_start : batch_start + args.batch_size]
            print(f"[batch] factors {batch_start + 1}-{batch_start + len(batch)} / {len(factor_names)}", flush=True)
            factor_frames: dict[str, pd.DataFrame] = {}
            for factor_name in batch:
                factor_func = alpha101.ALPHA_FUNCTIONS[factor_name]
                factor_frames[factor_name] = factor_func(dummy)

            precomputed_ic = None
            if factor_frames:
                batch_factor_matrix = build_batch_factor_matrix(factor_frames)
                forward_returns = inputs.forward_returns_1d.stack(dropna=False)
                precomputed_ic = ic_calculator.batch_compute_ic(batch_factor_matrix, forward_returns, method="spearman")

            for factor_name in batch:
                factor_started_at = time.time()
                factor_frame = factor_frames[factor_name]
                report = evaluate_factor(
                    factor_name=factor_name,
                    factor_frame=factor_frame,
                    knowledge=knowledge,
                    inputs=inputs,
                    in_sample_mask=in_sample_mask,
                    oos_mask=oos_mask,
                    precomputed_ic=None if precomputed_ic is None else precomputed_ic[factor_name],
                )
                report["runtime_seconds"] = round(time.time() - factor_started_at, 3)
                write_factor_report(args.root, factor_name, report)
                summary_rows.append(
                    {
                        "factor_id": report["factor_id"],
                        "factor_name": factor_name,
                        "ic_mean": report["basic_metrics"]["ic_mean"],
                        "ic_std": report["basic_metrics"]["ic_std"],
                        "icir": report["basic_metrics"]["icir"],
                        "icir_neutralized": report["basic_metrics"]["icir_neutralized"],
                        "category": report["category"],
                        "status": report["status"],
                    }
                )
                print(
                    f"[factor] {factor_name} icir={report['basic_metrics']['icir']:.4f} "
                    f"neutralized={report['basic_metrics']['icir_neutralized']:.4f} "
                    f"status={report['status']} runtime={report['runtime_seconds']:.2f}s",
                    flush=True,
                )
                del factor_frame
                gc.collect()
            factor_frames.clear()

    summary_rows.sort(key=lambda item: item["factor_id"])
    summary_path = write_summary(args.root, summary_rows)
    elapsed = time.time() - started_at
    counts = pd.Series([row["status"] for row in summary_rows]).value_counts().to_dict()
    print(
        json.dumps(
            {
                "summary_path": str(summary_path),
                "factor_count": len(summary_rows),
                "pass": int(counts.get("pass", 0)),
                "weak": int(counts.get("weak", 0)),
                "fail": int(counts.get("fail", 0)),
                "runtime_seconds": round(elapsed, 2),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

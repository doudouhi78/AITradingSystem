from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import empyrical
import pandas as pd
import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet

ROOT = Path(r"D:\AITradingSystem")
EXPERIMENTS_DIR = ROOT / "runtime" / "experiments"
SIGNAL_DIR = ROOT / "runtime" / "paper_trading" / "signals"
BENCHMARK_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
OUTPUT_DIR = ROOT / "runtime" / "attribution" / "trade_diagnostics"
DEFAULT_FEES = 0.001
DEFAULT_SLIPPAGE = 0.001


def _parse_notes(notes: list[str] | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for note in notes or []:
        if "=" not in note:
            continue
        key, value = note.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _holding_bucket(days: int) -> str:
    if days <= 5:
        return "1-5日"
    if days <= 10:
        return "6-10日"
    if days <= 20:
        return "11-20日"
    return ">20日"


def _load_signal_gate_map() -> dict[str, str]:
    gate_map: dict[str, str] = {}
    if not SIGNAL_DIR.exists():
        return gate_map
    for path in SIGNAL_DIR.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            signal_date = str(payload.get("date", ""))[:10]
            gate = payload.get("gate_result") or {}
            if gate.get("allowed") is True:
                gate_map[signal_date] = "allowed"
            elif gate.get("blocked_by"):
                gate_map[signal_date] = f"blocked:{gate['blocked_by']}"
            else:
                gate_map[signal_date] = "unknown"
        except Exception:
            continue
    return gate_map


def _load_volatility_bucket_map() -> dict[str, str]:
    benchmark = pd.read_parquet(BENCHMARK_PATH).copy()
    benchmark["trade_date"] = pd.to_datetime(benchmark["trade_date"])
    close = pd.to_numeric(benchmark["close"], errors="coerce")
    rolling_vol = close.pct_change(fill_method=None).rolling(20).std()
    valid = rolling_vol.dropna()
    if valid.empty:
        return {}
    low_cut = float(valid.quantile(1 / 3))
    high_cut = float(valid.quantile(2 / 3))
    bucket_map: dict[str, str] = {}
    for trade_date, value in zip(benchmark["trade_date"], rolling_vol, strict=False):
        if pd.isna(value):
            continue
        if value <= low_cut:
            bucket = "low"
        elif value <= high_cut:
            bucket = "mid"
        else:
            bucket = "high"
        bucket_map[str(pd.Timestamp(trade_date).date())] = bucket
    return bucket_map


def _rebuild_breakout_portfolio(experiment_dir: Path) -> tuple[vbt.Portfolio, pd.DataFrame]:
    manifest = json.loads((experiment_dir / "manifest.json").read_text(encoding="utf-8"))
    inputs = json.loads((experiment_dir / "inputs.json").read_text(encoding="utf-8"))
    results = json.loads((experiment_dir / "results.json").read_text(encoding="utf-8"))

    instrument = str(manifest.get("instrument", "510300"))
    start = str(inputs["dataset_snapshot"]["date_range_start"])
    end = str(inputs["dataset_snapshot"]["date_range_end"])
    df = load_etf_from_parquet(instrument, start, end)
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)

    note_map = _parse_notes(results.get("metrics_summary", {}).get("notes"))
    entry_window = int(float(note_map.get("entry_window", 25)))
    exit_window = int(float(note_map.get("exit_window", 20)))
    ma_window_raw = int(float(note_map.get("ma_filter_window", 0)))
    ma_window = ma_window_raw if ma_window_raw > 0 else None
    position_fraction = float(note_map.get("position_fraction", 1.0))
    fees = float(note_map.get("fees", DEFAULT_FEES))
    slippage = float(note_map.get("slippage", DEFAULT_SLIPPAGE))
    entry_split_steps = int(float(note_map.get("entry_split_steps", 1)))

    prev_high = close.shift(1).rolling(entry_window).max()
    prev_low = close.shift(1).rolling(exit_window).min()
    raw_entries = close > prev_high
    if ma_window:
        raw_entries = raw_entries & (close > close.rolling(ma_window).mean())
    raw_exits = close < prev_low

    entries = raw_entries.shift(1, fill_value=False).astype(bool)
    exits = raw_exits.shift(1, fill_value=False).astype(bool)

    accumulate = entry_split_steps > 1
    if accumulate:
        tranche = position_fraction / entry_split_steps
        staged_entries = pd.Series(False, index=entries.index)
        staged_sizes = pd.Series(0.0, index=entries.index)
        entry_flags = entries.tolist()
        exit_flags = exits.tolist()
        for idx, is_entry in enumerate(entry_flags):
            if not is_entry:
                continue
            for offset in range(entry_split_steps):
                target_idx = idx + offset
                if target_idx >= len(entry_flags):
                    break
                if any(exit_flags[idx + 1 : target_idx + 1]):
                    break
                staged_entries.iat[target_idx] = True
                staged_sizes.iat[target_idx] += tranche
        entry_payload = staged_entries
        size = staged_sizes
        size_type = "percent"
    elif position_fraction >= 0.999999:
        entry_payload = entries
        size = float("inf")
        size_type = None
    else:
        entry_payload = entries
        size = position_fraction
        size_type = "percent"

    kwargs: dict[str, Any] = {
        "entries": entry_payload,
        "exits": exits,
        "init_cash": 1.0,
        "size": size,
        "fees": fees,
        "slippage": slippage,
        "freq": "1D",
        "direction": "longonly",
        "accumulate": accumulate,
    }
    if size_type is not None:
        kwargs["size_type"] = size_type
    portfolio = vbt.Portfolio.from_signals(open_, **kwargs)
    return portfolio, df


def load_trades(experiment_id: str) -> pd.DataFrame:
    experiment_dir = EXPERIMENTS_DIR / experiment_id
    manifest_path = experiment_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"missing manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("strategy_family") != "etf_trend_breakout":
        return pd.DataFrame(columns=["entry_date", "exit_date", "pnl_pct", "holding_days", "entry_month", "holding_bucket", "vol_bucket", "gate_status", "experiment_id"])

    portfolio, _ = _rebuild_breakout_portfolio(experiment_dir)
    readable = portfolio.trades.records_readable.copy()
    if readable.empty:
        return pd.DataFrame()

    vol_map = _load_volatility_bucket_map()
    gate_map = _load_signal_gate_map()
    records: list[dict[str, Any]] = []
    for _, row in readable.iterrows():
        entry_dt = pd.Timestamp(row["Entry Timestamp"])
        exit_dt = pd.Timestamp(row["Exit Timestamp"])
        entry_date = str(entry_dt.date())
        exit_date = str(exit_dt.date())
        holding_days = max(int((exit_dt - entry_dt).days), 1)
        records.append({
            "entry_date": entry_date,
            "exit_date": exit_date,
            "pnl_pct": float(row["Return"]) * 100.0,
            "holding_days": holding_days,
            "entry_month": entry_dt.strftime("%Y-%m"),
            "holding_bucket": _holding_bucket(holding_days),
            "vol_bucket": vol_map.get(entry_date, "unknown"),
            "gate_status": gate_map.get(entry_date, "unknown"),
            "experiment_id": experiment_id,
        })
    return pd.DataFrame(records)


def _summarize_group(df: pd.DataFrame, group_col: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for value, group in df.groupby(group_col, dropna=False):
        returns = group["pnl_pct"].astype(float) / 100.0
        sharpe = float(empyrical.sharpe_ratio(returns, annualization=252)) if len(returns) >= 2 else 0.0
        count = int(len(group))
        output.append({
            group_col: "unknown" if pd.isna(value) else str(value),
            "count": count,
            "win_rate": float((group["pnl_pct"] > 0).mean()) if count else 0.0,
            "avg_pnl_pct": float(group["pnl_pct"].mean()) if count else 0.0,
            "sharpe": 0.0 if pd.isna(sharpe) else sharpe,
            "insufficient_sample": count < 10,
        })
    return sorted(output, key=lambda item: item[group_col])


def run_trade_diagnostics(trade_df: pd.DataFrame) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if trade_df.empty:
        result = {"gate_status": [], "holding_bucket": [], "vol_bucket": [], "entry_month": []}
    else:
        result = {
            "gate_status": _summarize_group(trade_df, "gate_status"),
            "holding_bucket": _summarize_group(trade_df, "holding_bucket"),
            "vol_bucket": _summarize_group(trade_df, "vol_bucket"),
            "entry_month": _summarize_group(trade_df, "entry_month"),
        }
    for key, payload in result.items():
        (OUTPUT_DIR / f"{key}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return result

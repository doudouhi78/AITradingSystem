from __future__ import annotations

from pathlib import Path

import akshare as ak
import pandas as pd
import vectorbt as vbt


REPO_ROOT = Path(__file__).resolve().parents[2]
MARKET_DATA_ROOT = REPO_ROOT / "runtime" / "market_data"


def to_fund_symbol(instrument: str) -> str:
    if instrument.startswith(("sh", "sz")):
        return instrument
    if instrument.startswith("5"):
        return f"sh{instrument}"
    return f"sz{instrument}"


def load_etf_history(instrument: str, date_start: str, date_end: str) -> pd.DataFrame:
    df = ak.fund_etf_hist_sina(symbol=to_fund_symbol(instrument)).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.Timestamp(date_start)) & (df["date"] <= pd.Timestamp(date_end))].copy()
    df = df.sort_values("date").reset_index(drop=True)
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = df[column].astype(float)
    return df


def load_etf_from_parquet(instrument: str, date_start: str, date_end: str) -> pd.DataFrame:
    """从本地 Parquet 加载 ETF 日线数据。"""
    parquet_path = MARKET_DATA_ROOT / "cn_etf" / f"{instrument}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet data not found: {parquet_path}")

    df = pd.read_parquet(parquet_path).copy()
    df["date"] = pd.to_datetime(df["trade_date"])
    df = df[(df["date"] >= pd.Timestamp(date_start)) & (df["date"] <= pd.Timestamp(date_end))].copy()
    df = df.sort_values("date").reset_index(drop=True)
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = df[column].astype(float)
    return df[["date", "open", "high", "low", "close", "volume"]]


def _build_staged_entry_inputs(entries: pd.Series, exits: pd.Series, *, position_fraction: float, entry_split_steps: int) -> tuple[pd.Series, pd.Series]:
    staged_entries = pd.Series(False, index=entries.index)
    staged_sizes = pd.Series(0.0, index=entries.index)
    tranche = position_fraction / entry_split_steps
    entry_flags = entries.astype(bool).tolist()
    exit_flags = exits.astype(bool).tolist()

    for idx, is_entry in enumerate(entry_flags):
        if not is_entry:
            continue
        for offset in range(entry_split_steps):
            target_idx = idx + offset
            if target_idx >= len(entry_flags):
                break
            blocked = False
            for probe in range(idx + 1, target_idx + 1):
                if exit_flags[probe]:
                    blocked = True
                    break
            if blocked:
                break
            staged_entries.iat[target_idx] = True
            staged_sizes.iat[target_idx] = staged_sizes.iat[target_idx] + tranche

    return staged_entries.astype(bool), staged_sizes.astype(float)


def run_breakout_backtest(
    df: pd.DataFrame,
    *,
    entry_window: int,
    exit_window: int,
    ma_filter_window: int | None,
    fees: float,
    slippage: float,
    position_fraction: float = 1.0,
    entry_split_steps: int = 1,
) -> dict[str, float | int | list[str]]:
    if not 0 < position_fraction <= 1.0:
        raise ValueError('position_fraction must be in (0, 1]')
    if entry_split_steps < 1:
        raise ValueError('entry_split_steps must be >= 1')

    close = df["close"].astype(float)
    open_ = df["open"].astype(float)

    prev_high = close.shift(1).rolling(entry_window).max()
    prev_low = close.shift(1).rolling(exit_window).min()
    raw_entries = close > prev_high
    if ma_filter_window:
        ma_filter = close.rolling(ma_filter_window).mean()
        raw_entries = raw_entries & (close > ma_filter)
    raw_exits = close < prev_low

    entries = raw_entries.shift(1, fill_value=False).astype(bool)
    exits = raw_exits.shift(1, fill_value=False).astype(bool)

    if entry_split_steps == 1:
        if position_fraction >= 0.999999:
            size = float("inf")
            size_type = None
            accumulate = False
            entry_payload = entries
        else:
            size = position_fraction
            size_type = "percent"
            accumulate = False
            entry_payload = entries
    else:
        entry_payload, size = _build_staged_entry_inputs(
            entries,
            exits,
            position_fraction=position_fraction,
            entry_split_steps=entry_split_steps,
        )
        size_type = 'percent'
        accumulate = True

    pf_kwargs = {
        'entries': entry_payload,
        'exits': exits,
        'init_cash': 1.0,
        'size': size,
        'fees': fees,
        'slippage': slippage,
        'freq': '1D',
        'direction': 'longonly',
        'accumulate': accumulate,
    }
    if size_type is not None:
        pf_kwargs['size_type'] = size_type

    pf = vbt.Portfolio.from_signals(open_, **pf_kwargs)
    trades = int(pf.trades.count())
    return {
        "total_return": float(pf.total_return()),
        "annual_return": float(pf.annualized_return()),
        "annualized_return": float(pf.annualized_return()),
        "max_drawdown": float(pf.max_drawdown()),
        "sharpe": float(pf.sharpe_ratio()),
        "trade_count": trades,
        "trades": trades,
        "win_rate": float(pf.trades.win_rate()),
        "notes": [
            f"entry_window={entry_window}",
            f"exit_window={exit_window}",
            f"ma_filter_window={ma_filter_window or 0}",
            f"position_fraction={position_fraction}",
            f"entry_split_steps={entry_split_steps}",
            f"fees={fees}",
            f"slippage={slippage}",
        ],
    }

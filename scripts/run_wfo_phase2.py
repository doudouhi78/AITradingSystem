from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet, run_breakout_backtest
from ai_dev_os.gate import GateScheduler
from ai_dev_os.risk import compute_quantity, compute_stop_price, wilder_atr

ROOT = Path(__file__).resolve().parents[1]
RESULT_PATH = ROOT / "coordination" / "wfo_phase2_result.json"
TRAIN_DAYS = 252 * 3
TEST_DAYS = 252
STEP_DAYS = 252
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
FEE = 0.001


def compute_signal(history: pd.DataFrame) -> tuple[str, float, float, float]:
    close = history["close"].astype(float)
    last_close = float(close.iloc[-1])
    entry_threshold = float(close.shift(1).rolling(ENTRY_WINDOW).max().iloc[-1])
    exit_threshold = float(close.shift(1).rolling(EXIT_WINDOW).min().iloc[-1])
    if pd.isna(entry_threshold) or pd.isna(exit_threshold):
        return "HOLD", last_close, entry_threshold, exit_threshold
    if last_close > entry_threshold:
        return "BUY", last_close, entry_threshold, exit_threshold
    if last_close < exit_threshold:
        return "SELL", last_close, entry_threshold, exit_threshold
    return "HOLD", last_close, entry_threshold, exit_threshold


def sharpe_from_equity(values: list[float]) -> float:
    series = pd.Series(values, dtype=float)
    returns = series.pct_change().dropna()
    if returns.empty:
        return 0.0
    std = float(returns.std(ddof=0))
    if std == 0.0:
        return 0.0
    return float((returns.mean() / std) * (252 ** 0.5))


def run_window_with_gate(df_full: pd.DataFrame, test_df: pd.DataFrame) -> dict[str, float | int]:
    scheduler = GateScheduler()
    cash = 1.0
    shares = 0.0
    current_position_frac = 0.0
    pending_order: str | None = None
    pending_position_frac = 0.0
    equity_points: list[tuple[str, float]] = []
    gate_blocked = 0
    trade_count = 0

    for i, row in test_df.reset_index(drop=True).iterrows():
        open_px = float(row["open"])
        close_px = float(row["close"])

        if pending_order == "BUY" and shares == 0.0:
            invest_cash = cash * pending_position_frac
            if invest_cash > 0:
                shares = invest_cash / (open_px * (1.0 + FEE))
                cash -= invest_cash
                current_position_frac = pending_position_frac
                trade_count += 1
        elif pending_order == "SELL" and shares > 0.0:
            cash += shares * open_px * (1.0 - FEE)
            shares = 0.0
            current_position_frac = 0.0
            trade_count += 1
        pending_order = None
        pending_position_frac = 0.0

        equity = cash + shares * close_px
        date_str = str(row["date"].date())
        equity_points.append((date_str, float(equity)))
        if equity > 0 and shares > 0:
            current_position_frac = float((shares * close_px) / equity)

        history = df_full[df_full["date"] <= row["date"]]
        signal, last_close, _, _ = compute_signal(history)
        if i < len(test_df) - 1:
            next_open = float(test_df.iloc[i + 1]["open"])
            if signal == "BUY" and shares == 0.0:
                gate_result = scheduler.evaluate(
                    date=date_str,
                    equity_series=[v for _, v in equity_points],
                    etf_df=history.rename(columns={"date": "trade_date"}),
                )
                if not gate_result["allowed"]:
                    gate_blocked += 1
                else:
                    atr_series = wilder_atr(history["high"], history["low"], history["close"])
                    atr_val = float(atr_series.iloc[-1]) if pd.notna(atr_series.iloc[-1]) else None
                    if atr_val is not None:
                        entry_price = next_open
                        stop_price = compute_stop_price(entry_price, atr_val)
                        _, position_frac = compute_quantity(equity * 100000, entry_price, stop_price)
                        if position_frac > 0:
                            pending_order = "BUY"
                            pending_position_frac = float(position_frac)
            elif signal == "SELL" and shares > 0.0:
                pending_order = "SELL"

    equity_values = [v for _, v in equity_points]
    total_return = float(equity_values[-1] - 1.0) if equity_values else 0.0
    return {
        "sharpe": sharpe_from_equity(equity_values),
        "total_return": total_return,
        "trade_count": trade_count,
        "gate_blocked": gate_blocked,
    }


def main() -> None:
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01").copy()
    splitter = vbt.RollingSplitter()
    windows = []
    for i, sets in enumerate(splitter.split(df.index, window_len=TRAIN_DAYS + TEST_DAYS, set_lens=(TRAIN_DAYS,))):
        if i % STEP_DAYS != 0:
            continue
        train_idx, test_idx = sets
        train_df = df.iloc[list(train_idx)].reset_index(drop=True)
        test_df = df.iloc[list(test_idx)].reset_index(drop=True)
        train_metrics = run_breakout_backtest(
            train_df,
            entry_window=25,
            exit_window=20,
            ma_filter_window=None,
            fees=0.001,
            slippage=0.001,
            position_fraction=1.0,
            entry_split_steps=1,
        )
        phase2_test = run_window_with_gate(df[df["date"] <= test_df["date"].iloc[-1]].reset_index(drop=True), test_df)
        windows.append({
            "window_id": len(windows) + 1,
            "train_start": train_df["date"].iloc[0].strftime("%Y-%m-%d"),
            "train_end": train_df["date"].iloc[-1].strftime("%Y-%m-%d"),
            "test_start": test_df["date"].iloc[0].strftime("%Y-%m-%d"),
            "test_end": test_df["date"].iloc[-1].strftime("%Y-%m-%d"),
            "train_sharpe": float(train_metrics["sharpe"]),
            "test_sharpe": float(phase2_test["sharpe"]),
            "gate_blocked": int(phase2_test["gate_blocked"]),
            "trade_count": int(phase2_test["trade_count"]),
            "test_total_return": float(phase2_test["total_return"]),
        })
        if len(windows) >= 6:
            break

    train_mean = sum(w["train_sharpe"] for w in windows) / len(windows)
    test_mean = sum(w["test_sharpe"] for w in windows) / len(windows)
    ratio = 0.0 if train_mean == 0 else test_mean / train_mean
    gate_blocked_total = int(sum(w["gate_blocked"] for w in windows))
    payload = {
        "window_count": len(windows),
        "train_sharpe_mean": train_mean,
        "test_sharpe_mean": test_mean,
        "ratio": ratio,
        "ratio_gt_0_5": bool(ratio > 0.5),
        "gate_blocked_total": gate_blocked_total,
        "windows": windows,
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

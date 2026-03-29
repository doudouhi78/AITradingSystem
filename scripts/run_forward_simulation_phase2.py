from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_dev_os.gate import GateScheduler
from ai_dev_os.risk import compute_quantity, compute_stop_price, wilder_atr

ROOT = Path(r"D:\AITradingSystem")
DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
RESULT_PATH = ROOT / "coordination" / "phase2_forward_sim_result.json"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
FEE = 0.001
PHASE1_BASELINE = {
    "total_return": 0.0004,
    "max_drawdown": -0.021,
    "trade_count": 2,
    "slippage_exceed_count": 43,
}


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


def main() -> None:
    df = pd.read_parquet(DATA_PATH).sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    sim_df = df.tail(60).reset_index(drop=True)
    cutoff_date = str(df.iloc[-61]["trade_date"].date())
    sim_start = str(sim_df.iloc[0]["trade_date"].date())
    sim_end = str(sim_df.iloc[-1]["trade_date"].date())

    scheduler = GateScheduler()
    cash = 1.0
    shares = 0.0
    current_position_frac = 0.0
    pending_order: str | None = None
    pending_position_frac = 0.0
    equity_points: list[tuple[str, float]] = []
    slippages: list[float] = []
    gate_blocked = 0
    trade_count = 0
    daily_position_fracs: list[float] = []

    for i, row in sim_df.iterrows():
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
        date_str = str(row["trade_date"].date())
        equity_points.append((date_str, float(equity)))
        if equity > 0 and shares > 0:
            current_position_frac = float((shares * close_px) / equity)
        daily_position_fracs.append(current_position_frac)

        history = df[df["trade_date"] <= row["trade_date"]]
        signal, last_close, _, _ = compute_signal(history)

        if i < len(sim_df) - 1:
            next_open = float(sim_df.iloc[i + 1]["open"])
            slippage = (next_open - last_close) / last_close
            slippages.append(float(slippage))

            if signal == "BUY" and shares == 0.0:
                gate_result = scheduler.evaluate(
                    date=date_str,
                    equity_series=[v for _, v in equity_points],
                    etf_df=history,
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

    equity_series = pd.Series([v for _, v in equity_points], index=pd.to_datetime([d for d, _ in equity_points]))
    total_return = float(equity_series.iloc[-1] - 1.0)
    max_drawdown = float((equity_series / equity_series.cummax() - 1.0).min())
    avg_slippage = float(sum(slippages) / len(slippages)) if slippages else 0.0
    slippage_exceed_count = int(sum(1 for s in slippages if abs(s) > 0.001))
    avg_position_fraction = float(sum(daily_position_fracs) / len(daily_position_fracs)) if daily_position_fracs else 0.0

    result = {
        "phase": 2,
        "cutoff_date": cutoff_date,
        "sim_start": sim_start,
        "sim_end": sim_end,
        "trading_days": 60,
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "trade_count": trade_count,
        "gate_blocked_count": gate_blocked,
        "avg_position_fraction": avg_position_fraction,
        "avg_slippage": avg_slippage,
        "slippage_exceed_count": slippage_exceed_count,
        "comparison_vs_phase1": {
            "total_return_diff": total_return - PHASE1_BASELINE["total_return"],
            "max_drawdown_diff": max_drawdown - PHASE1_BASELINE["max_drawdown"],
            "trade_count_diff": trade_count - PHASE1_BASELINE["trade_count"],
            "gate_blocked": gate_blocked,
        },
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== Phase 2 前向模拟 vs Phase 1 对比 ===")
    print(f"{'':20} {'Phase 1':>10} {'Phase 2':>10} {'变化':>12}")
    print(f"{'总收益率':20} {PHASE1_BASELINE['total_return']*100:>9.2f}% {total_return*100:>9.2f}% {(total_return-PHASE1_BASELINE['total_return'])*100:>+11.2f}%")
    print(f"{'最大回撤':20} {PHASE1_BASELINE['max_drawdown']*100:>9.2f}% {max_drawdown*100:>9.2f}% {(max_drawdown-PHASE1_BASELINE['max_drawdown'])*100:>+11.2f}%")
    print(f"{'交易次数':20} {PHASE1_BASELINE['trade_count']:>10} {trade_count:>10} {trade_count-PHASE1_BASELINE['trade_count']:>+12}")
    print(f"{'Gate阻断次数':20} {0:>10} {gate_blocked:>10} {'—':>12}")
    print(f"{'平均仓位':20} {50.0:>9.1f}% {avg_position_fraction*100:>9.1f}% {'—':>12}")
    print(f"{'滑点超标次数':20} {PHASE1_BASELINE['slippage_exceed_count']:>10} {slippage_exceed_count:>10} {slippage_exceed_count-PHASE1_BASELINE['slippage_exceed_count']:>+12}")


if __name__ == "__main__":
    main()

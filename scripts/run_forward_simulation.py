from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
RESULT_PATH = ROOT / "coordination" / "phase5_forward_sim_result.json"
EQUITY_CSV = ROOT / "runtime" / "paper_trading" / "forward_sim_equity.csv"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
POSITION_FRACTION = 0.5
FEE = 0.001
ASSUMED_SLIPPAGE = 0.0005


def signal_for(history: pd.DataFrame) -> tuple[str, float, float, float]:
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
    cash, shares = 1.0, 0.0
    pending_order: str | None = None
    signal_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}
    trade_count = 0
    slippages: list[float] = []
    equity_points: list[tuple[str, float]] = []

    for i, row in sim_df.iterrows():
        open_px = float(row["open"])
        close_px = float(row["close"])
        if pending_order == "BUY" and shares == 0.0:
            invest_cash = cash * POSITION_FRACTION
            shares = invest_cash / (open_px * (1.0 + FEE))
            cash -= invest_cash
            trade_count += 1
        elif pending_order == "SELL" and shares > 0.0:
            cash += shares * open_px * (1.0 - FEE)
            shares = 0.0
            trade_count += 1
        pending_order = None

        equity = cash + shares * close_px
        equity_points.append((str(row["trade_date"].date()), float(equity)))

        history = df[df["trade_date"] <= row["trade_date"]]
        signal, last_close, entry_threshold, exit_threshold = signal_for(history)
        signal_counts[signal] += 1

        if i < len(sim_df) - 1:
            next_open = float(sim_df.iloc[i + 1]["open"])
            slippage = (next_open - last_close) / last_close
            slippages.append(float(slippage))
            if signal == "BUY" and shares == 0.0:
                pending_order = "BUY"
            elif signal == "SELL" and shares > 0.0:
                pending_order = "SELL"

    equity_series = pd.Series([v for _, v in equity_points], index=pd.to_datetime([d for d, _ in equity_points]))
    total_return = float(equity_series.iloc[-1] - 1.0)
    max_drawdown = float((equity_series / equity_series.cummax() - 1.0).min())
    avg_slippage = float(sum(slippages) / len(slippages)) if slippages else 0.0
    exceed_count = int(sum(1 for s in slippages if abs(s) > 0.001))

    result = {
        "cutoff_date": cutoff_date,
        "sim_start": sim_start,
        "sim_end": sim_end,
        "trading_days": 60,
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "trade_count": trade_count,
        "signal_buy": signal_counts["BUY"],
        "signal_sell": signal_counts["SELL"],
        "signal_hold": signal_counts["HOLD"],
        "avg_slippage": avg_slippage,
        "slippage_exceed_count": exceed_count,
        "daily_equity": equity_points,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    EQUITY_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(equity_points, columns=["date", "equity"]).to_csv(EQUITY_CSV, index=False, encoding="utf-8")

    conclusion = "滑点基本在预期内，策略前向区间表现可接受。" if abs(avg_slippage) <= ASSUMED_SLIPPAGE and total_return >= 0 else "滑点或前向表现偏弱，需要继续观察。"
    print("=== Phase 5 前向模拟报告 ===")
    print(f"模拟区间：{sim_start} 至 {sim_end}（60个交易日）")
    print(f"总收益率：{total_return:.1%}")
    print(f"最大回撤：{max_drawdown:.1%}")
    print(f"交易次数：{trade_count}（BUY {signal_counts['BUY']}次，SELL {signal_counts['SELL']}次）")
    print(f"平均滑点：{avg_slippage:.2%}（假设0.05%）")
    print(f"滑点超标：{exceed_count}次")
    print(f"结论：{conclusion}")


if __name__ == "__main__":
    main()

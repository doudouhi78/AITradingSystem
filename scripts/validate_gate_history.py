from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_dev_os.gate import GateScheduler

ROOT = Path(__file__).resolve().parents[1]
ETF_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
RESULT_PATH = ROOT / "coordination" / "gate_validation_result.json"


def main() -> None:
    df = pd.read_parquet(ETF_PATH).copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    history = df[(df["trade_date"] >= pd.Timestamp("2018-01-01")) & (df["trade_date"] <= pd.Timestamp("2018-12-31"))].copy()
    scheduler = GateScheduler()
    counts = {"TrendGate": 0, "DrawdownGate": 0, "BreadthGate": 0, "VolGate": 0}
    blocked_days = []
    base_close = float(df["close"].iloc[0])

    for trade_date in history["trade_date"]:
        hist_df = df[df["trade_date"] <= trade_date].copy()
        equity_series = (hist_df["close"].astype(float) / base_close).tolist()
        result = scheduler.evaluate(date=trade_date.strftime("%Y-%m-%d"), equity_series=equity_series, etf_df=hist_df)
        for gate_name, gate_result in result["gate_details"].items():
            if not gate_result["allowed"]:
                counts[gate_name] += 1
        if not result["allowed"]:
            blocked_days.append({
                "date": trade_date.strftime("%Y-%m-%d"),
                "blocked_by": result["blocked_by"],
                "reason": result["reason"],
            })

    payload = {
        "date_range": ["2018-01-01", "2018-12-31"],
        "trading_days": int(len(history)),
        "trend_gate_trigger_days": counts["TrendGate"],
        "drawdown_gate_trigger_days": counts["DrawdownGate"],
        "breadth_gate_trigger_days": counts["BreadthGate"],
        "vol_gate_trigger_days": counts["VolGate"],
        "total_blocked_days": len(blocked_days),
        "sample_blocked_days": blocked_days[:20],
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

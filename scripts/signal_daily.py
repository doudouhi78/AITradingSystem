from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from ai_dev_os.gate import GateScheduler
from ai_dev_os.risk import compute_quantity, compute_stop_price, wilder_atr

ROOT = Path(r"D:\AITradingSystem")
DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
SIGNAL_DIR = ROOT / "runtime" / "paper_trading" / "signals"
FORWARD_SIM_PATH = ROOT / "runtime" / "paper_trading" / "forward_sim_equity.csv"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
ACCOUNT_EQUITY = 100000.0


def load_equity_series() -> list[float]:
    if FORWARD_SIM_PATH.exists():
        df = pd.read_csv(FORWARD_SIM_PATH)
        if "equity" in df.columns and not df.empty:
            return df["equity"].astype(float).tolist()
    return [1.0]


def main() -> None:
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now().strftime("%Y-%m-%d")
    df = pd.read_parquet(DATA_PATH).sort_values("trade_date").reset_index(drop=True)
    latest = df.iloc[-1]
    close = df["close"].astype(float)
    entry_threshold = float(close.shift(1).rolling(ENTRY_WINDOW).max().iloc[-1])
    exit_threshold = float(close.shift(1).rolling(EXIT_WINDOW).min().iloc[-1])
    last_close = float(latest["close"])
    if last_close > entry_threshold:
        signal = "BUY"
    elif last_close < exit_threshold:
        signal = "SELL"
    else:
        signal = "HOLD"

    scheduler = GateScheduler()
    gate_result = scheduler.evaluate(
        date=str(latest["trade_date"])[:10],
        equity_series=load_equity_series(),
        etf_df=df,
    )
    gate_block_message = None
    if signal == "BUY" and not gate_result["allowed"]:
        signal = "HOLD"
        gate_block_message = f"Gate 阻断入场信号：{gate_result['blocked_by']} — {gate_result['reason']}"

    atr_value = None
    stop_price = None
    suggested_qty = 0
    position_frac = 0.0
    risk_warning = None
    if signal == "BUY":
        atr_series = wilder_atr(df["high"], df["low"], df["close"])
        atr_last = float(atr_series.iloc[-1]) if pd.notna(atr_series.iloc[-1]) else None
        if atr_last is None:
            risk_warning = "ATR 数据不足，无法生成止损与仓位建议。"
        else:
            atr_value = atr_last
            entry_price = float(latest["open"])
            stop_price = compute_stop_price(entry_price, atr_value)
            suggested_qty, position_frac = compute_quantity(ACCOUNT_EQUITY, entry_price, stop_price)

    rationale = f"最新数据日={str(latest['trade_date'])[:10]}，收盘价={last_close:.4f}，25日高点={entry_threshold:.4f}，20日低点={exit_threshold:.4f}"
    payload = {
        "date": run_date,
        "signal": signal,
        "close": last_close,
        "entry_threshold": entry_threshold,
        "exit_threshold": exit_threshold,
        "rationale": rationale,
        "gate_result": gate_result,
        "atr": atr_value,
        "stop_price": stop_price,
        "suggested_qty": suggested_qty,
        "position_fraction": position_frac,
        "risk_warning": risk_warning,
    }
    out_path = SIGNAL_DIR / f"{run_date.replace('-', '')}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"日期: {run_date}")
    print(f"信号: {signal}")
    print(f"依据: {rationale}")
    if gate_block_message:
        print(gate_block_message)
    if signal == "BUY":
        if risk_warning:
            print(risk_warning)
        else:
            print(f"建议仓位：{position_frac:.1%}（{suggested_qty}股），止损价：{stop_price:.3f}，ATR：{atr_value:.4f}")
    print("建议执行价: 次日开盘价（需人工填入）")
    print(f"信号文件: {out_path}")


if __name__ == "__main__":
    main()
    # 每日信号生成后自动更新报告
    try:
        import subprocess

        subprocess.run(
            [str(ROOT / ".venv" / "Scripts" / "python.exe"), "scripts/generate_report.py"],
            cwd=str(ROOT),
            check=True,
        )
        print("报告已更新：runtime/reports/strategy_report.html")
    except Exception as e:
        print(f"报告生成失败（不影响信号）：{e}")


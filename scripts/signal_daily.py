from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\AITradingSystem")
DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
SIGNAL_DIR = ROOT / "runtime" / "paper_trading" / "signals"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20


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
    rationale = f"最新数据日={str(latest['trade_date'])[:10]}，收盘价={last_close:.4f}，25日高点={entry_threshold:.4f}，20日低点={exit_threshold:.4f}"
    payload = {
        "date": run_date,
        "signal": signal,
        "close": last_close,
        "entry_threshold": entry_threshold,
        "exit_threshold": exit_threshold,
        "rationale": rationale,
    }
    out_path = SIGNAL_DIR / f"{run_date.replace('-', '')}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"日期: {run_date}")
    print(f"信号: {signal}")
    print(f"依据: {rationale}")
    print("建议执行价: 次日开盘价（需人工填入）")
    print(f"信号文件: {out_path}")


if __name__ == "__main__":
    main()

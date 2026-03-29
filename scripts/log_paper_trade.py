from __future__ import annotations

import argparse
import csv
from pathlib import Path

ROOT = Path(r"D:\AITradingSystem")
PAPER_DIR = ROOT / "runtime" / "paper_trading"
LOG_PATH = PAPER_DIR / "trade_log.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--signal", required=True)
    parser.add_argument("--assumed-price", required=True, type=float)
    parser.add_argument("--actual-open", default="")
    parser.add_argument("--slippage", default="")
    parser.add_argument("--position", default="")
    parser.add_argument("--notes", default="")
    args = parser.parse_args()

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    (PAPER_DIR / "signals").mkdir(parents=True, exist_ok=True)
    exists = LOG_PATH.exists()
    with LOG_PATH.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "signal", "assumed_price", "actual_open", "slippage", "position", "notes"])
        if not exists:
            writer.writeheader()
        writer.writerow({
            "date": args.date,
            "signal": args.signal,
            "assumed_price": args.assumed_price,
            "actual_open": args.actual_open,
            "slippage": args.slippage,
            "position": args.position,
            "notes": args.notes,
        })
    print(LOG_PATH)


if __name__ == "__main__":
    main()

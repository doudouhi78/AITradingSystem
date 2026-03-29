from __future__ import annotations

import json
from pathlib import Path

from ai_dev_os.research_session import run_experiment

RESULT_PATH = Path(r"D:\AITradingSystem\coordination\param_heatmap.json")


def main() -> None:
    grid = []
    entries = list(range(10, 41, 5))
    exits = list(range(10, 41, 5))
    for entry_window in entries:
        for exit_window in exits:
            metrics = run_experiment("510300", entry_window, exit_window)
            grid.append({
                "entry_window": entry_window,
                "exit_window": exit_window,
                "sharpe": float(metrics["sharpe"]),
            })

    RESULT_PATH.write_text(json.dumps({"grid": grid}, ensure_ascii=False, indent=2), encoding="utf-8")

    print("entry\\exit | " + " ".join(f"{x:>6}" for x in exits))
    for entry_window in entries:
        row = [item for item in grid if item["entry_window"] == entry_window]
        values = " ".join(f"{item['sharpe']:>6.2f}" for item in row)
        print(f"{entry_window:>10} | {values}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json

import pandas as pd

from attribution.trade_diagnostics import EXPERIMENTS_DIR, load_trades, run_trade_diagnostics


def main() -> None:
    frames: list[pd.DataFrame] = []
    for experiment_dir in sorted(EXPERIMENTS_DIR.iterdir()):
        if not experiment_dir.is_dir():
            continue
        df = load_trades(experiment_dir.name)
        if not df.empty:
            frames.append(df)
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result = run_trade_diagnostics(merged)
    print(json.dumps({key: len(value) for key, value in result.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

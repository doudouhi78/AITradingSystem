from __future__ import annotations

import argparse

import pandas as pd

from attribution.factor_attribution import run_factor_attribution
from attribution.report_generator import generate_monthly_report
from attribution.strategy_attribution import compute_rolling_alpha, load_returns_series, run_strategy_attribution
from attribution.trade_diagnostics import EXPERIMENTS_DIR, load_trades, run_trade_diagnostics

DEFAULT_EXPERIMENT = "exp-20260329-008-parquet-entry25-exit20"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    frames: list[pd.DataFrame] = []
    for experiment_dir in sorted(EXPERIMENTS_DIR.iterdir()):
        if experiment_dir.is_dir():
            df = load_trades(experiment_dir.name)
            if not df.empty:
                frames.append(df)
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    run_trade_diagnostics(merged)
    strategy_returns, benchmark_returns = load_returns_series(DEFAULT_EXPERIMENT)
    run_strategy_attribution(strategy_returns, benchmark_returns)
    compute_rolling_alpha(strategy_returns, benchmark_returns)
    run_factor_attribution()
    output = generate_monthly_report(args.year, args.month)
    print(output)


if __name__ == "__main__":
    main()

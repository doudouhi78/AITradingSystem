from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet

RESULT_PATH = Path(r"D:\AITradingSystem\coordination\mc_result.json")


def compute_max_drawdown_from_trade_returns(trade_returns: np.ndarray) -> float:
    equity = np.cumprod(1.0 + trade_returns)
    running_max = np.maximum.accumulate(equity)
    drawdowns = equity / running_max - 1.0
    return float(drawdowns.min())


def main() -> None:
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01")
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    prev_high = close.shift(1).rolling(25).max()
    prev_low = close.shift(1).rolling(20).min()
    entries = (close > prev_high).shift(1, fill_value=False).astype(bool)
    exits = (close < prev_low).shift(1, fill_value=False).astype(bool)
    pf = vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=float("inf"), fees=0.001, slippage=0.0005, freq="1D", direction="longonly", accumulate=False)
    trade_returns = pf.trades.records_readable["Return"].astype(float).to_numpy()

    rng = np.random.default_rng(42)
    sims = []
    for _ in range(1000):
        shuffled = rng.permutation(trade_returns)
        sims.append(compute_max_drawdown_from_trade_returns(shuffled))
    sims_arr = np.array(sims, dtype=float)

    result = {
        "n_simulations": 1000,
        "p_max_drawdown_gt_18pct": float(np.mean(sims_arr < -0.18)),
        "drawdown_mean": float(np.mean(sims_arr)),
        "drawdown_p5": float(np.percentile(sims_arr, 5)),
        "drawdown_p95": float(np.percentile(sims_arr, 95)),
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

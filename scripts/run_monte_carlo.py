from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet

RESULT_PATH = Path(__file__).resolve().parents[1] / "coordination" / "mc_result.json"
TARGET = 0.20
CANDIDATES = [0.5, 0.4, 0.3, 0.2]


def compute_max_drawdown_from_trade_returns(trade_returns: np.ndarray) -> float:
    equity = np.cumprod(1.0 + trade_returns)
    running_max = np.maximum.accumulate(equity)
    drawdowns = equity / running_max - 1.0
    return float(drawdowns.min())


def simulate(position_fraction: float) -> dict[str, float | int | bool]:
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01")
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    prev_high = close.shift(1).rolling(25).max()
    prev_low = close.shift(1).rolling(20).min()
    entries = (close > prev_high).shift(1, fill_value=False).astype(bool)
    exits = (close < prev_low).shift(1, fill_value=False).astype(bool)
    pf = vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=position_fraction, size_type="percent", fees=0.001, slippage=0.001, freq="1D", direction="longonly", accumulate=False)
    trade_returns = pf.trades.records_readable["Return"].astype(float).to_numpy() * position_fraction
    rng = np.random.default_rng(42)
    sims = [compute_max_drawdown_from_trade_returns(rng.permutation(trade_returns)) for _ in range(1000)]
    sims_arr = np.array(sims, dtype=float)
    p = float(np.mean(sims_arr < -0.18))
    return {
        "position_fraction": position_fraction,
        "n_simulations": 1000,
        "p_max_drawdown_gt_18pct": p,
        "drawdown_mean": float(np.mean(sims_arr)),
        "drawdown_p5": float(np.percentile(sims_arr, 5)),
        "drawdown_p95": float(np.percentile(sims_arr, 95)),
        "passed_lt_20pct": bool(p < TARGET),
    }


def main() -> None:
    tried: list[dict[str, float | int | bool]] = []
    chosen: dict[str, float | int | bool] | None = None
    for fraction in CANDIDATES:
        result = simulate(fraction)
        tried.append(result)
        if result["passed_lt_20pct"]:
            chosen = result
            break
    payload = dict(chosen or tried[-1])
    payload["target_probability"] = TARGET
    payload["all_trials"] = tried
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"p(max_dd > 18%) = {payload['p_max_drawdown_gt_18pct']:.3f}")


if __name__ == "__main__":
    main()


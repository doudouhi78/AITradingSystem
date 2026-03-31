from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import vectorbt as vbt
from hikyuu import HHV, LLV, PRICELIST, REF

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet

_ROOT = Path(__file__).resolve().parents[1]
BASELINE_RESULTS_PATH = _ROOT / "runtime" / "experiments" / "exp-20260329-008-parquet-entry25-exit20" / "results.json"
RESULT_PATH = _ROOT / "coordination" / "phase1_hikyuu_cross_validate_result.json"

ENTRY_WINDOW = 25
EXIT_WINDOW = 20
FEES = 0.001
SLIPPAGE = 0.0005
START_DATE = "2016-01-01"
INSTRUMENT = "510300"


def load_baseline_metrics() -> dict[str, float]:
    payload = json.loads(BASELINE_RESULTS_PATH.read_text(encoding="utf-8"))
    metrics = payload["metrics_summary"]
    return {
        "sharpe": float(metrics["sharpe"]),
        "max_drawdown": float(metrics["max_drawdown"]),
        "total_return": float(metrics["total_return"]),
    }


def indicator_to_series(ind, index: pd.Index) -> pd.Series:
    return pd.Series([float(ind[i]) for i in range(len(ind))], index=index, dtype=float)


def build_signals(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    close = df["close"].astype(float)
    k = PRICELIST(close.tolist())
    prev_high = indicator_to_series(REF(HHV(k, ENTRY_WINDOW), 1), close.index)
    prev_low = indicator_to_series(REF(LLV(k, EXIT_WINDOW), 1), close.index)
    entries = (close > prev_high).shift(1, fill_value=False).astype(bool)
    exits = (close < prev_low).shift(1, fill_value=False).astype(bool)
    return entries, exits


def run_hikyuu_backtest(df: pd.DataFrame, entries: pd.Series, exits: pd.Series) -> dict[str, float | int | str]:
    open_ = df["open"].astype(float)
    cash, shares, trade_count = 1.0, 0.0, 0
    equity_curve: list[float] = []
    for i in range(len(df)):
        px = float(open_.iat[i])
        if shares > 0 and bool(exits.iat[i]):
            cash = shares * px * (1.0 - SLIPPAGE) * (1.0 - FEES)
            shares = 0.0
            trade_count += 1
        if shares == 0 and bool(entries.iat[i]):
            shares = cash / (px * (1.0 + SLIPPAGE) * (1.0 + FEES))
            cash = 0.0
        equity_curve.append(cash if shares == 0 else shares * px)
    equity = pd.Series(equity_curve, index=df["date"])
    years = (df["date"].iloc[-1] - df["date"].iloc[0]).days / 365.25
    total_return = float(equity.iloc[-1] - 1.0)
    annual_return = float(equity.iloc[-1] ** (1 / years) - 1.0) if years > 0 else 0.0
    max_drawdown = float((equity / equity.cummax() - 1.0).min())
    pf = vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=float("inf"), fees=FEES, slippage=SLIPPAGE, freq="1D", direction="longonly", accumulate=False)
    return {
        "method": "Hikyuu indicators + daily execution loop",
        "data_start": df["date"].iloc[0].strftime("%Y-%m-%d"),
        "data_end": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "trade_count": int(trade_count),
        "sharpe": float(pf.sharpe_ratio()),
        "max_drawdown": max_drawdown,
        "total_return": total_return,
        "annual_return": annual_return,
    }


def main() -> None:
    baseline = load_baseline_metrics()
    df = load_etf_from_parquet(INSTRUMENT, START_DATE, "2100-01-01")
    entries, exits = build_signals(df)
    result = run_hikyuu_backtest(df, entries, exits)
    result["vectorbt_sharpe"] = baseline["sharpe"]
    result["vectorbt_max_drawdown"] = baseline["max_drawdown"]
    result["vectorbt_total_return"] = baseline["total_return"]
    result["sharpe_gap"] = float(abs(result["sharpe"] - baseline["sharpe"]))
    result["max_drawdown_gap"] = float(abs(result["max_drawdown"] - baseline["max_drawdown"]))
    result["passed"] = bool(result["sharpe_gap"] < 0.05 and result["max_drawdown_gap"] < 0.02)
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

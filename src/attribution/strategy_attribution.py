from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import empyrical
import pandas as pd
import pyfolio

from attribution.trade_diagnostics import _rebuild_breakout_portfolio

ROOT = Path(r"D:\AITradingSystem")
EXPERIMENTS_DIR = ROOT / "runtime" / "experiments"
BENCHMARK_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
OUTPUT_DIR = ROOT / "runtime" / "attribution" / "strategy_attribution"
REPORT_DIR = ROOT / "runtime" / "attribution" / "reports"


def _load_benchmark_returns(start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    benchmark = pd.read_parquet(BENCHMARK_PATH).copy()
    benchmark["trade_date"] = pd.to_datetime(benchmark["trade_date"])
    mask = (benchmark["trade_date"] >= start) & (benchmark["trade_date"] <= end)
    prices = pd.to_numeric(benchmark.loc[mask, "close"], errors="coerce")
    index = pd.to_datetime(benchmark.loc[mask, "trade_date"])
    returns = prices.pct_change(fill_method=None).fillna(0.0)
    return pd.Series(returns.values, index=index, name="benchmark")


def load_returns_series(experiment_id: str) -> tuple[pd.Series, pd.Series]:
    experiment_dir = EXPERIMENTS_DIR / experiment_id
    portfolio, df = _rebuild_breakout_portfolio(experiment_dir)
    strategy_returns = portfolio.returns().copy()
    strategy_returns.index = pd.to_datetime(df["date"])
    strategy_returns.name = "strategy"
    benchmark_returns = _load_benchmark_returns(strategy_returns.index.min(), strategy_returns.index.max())
    aligned = pd.concat([strategy_returns, benchmark_returns], axis=1, join="inner").fillna(0.0)
    return aligned["strategy"], aligned["benchmark"]


def run_strategy_attribution(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    annual_return = float(empyrical.annual_return(strategy_returns))
    sharpe = float(empyrical.sharpe_ratio(strategy_returns, annualization=252))
    max_drawdown = float(empyrical.max_drawdown(strategy_returns))
    alpha, beta = empyrical.alpha_beta(strategy_returns, benchmark_returns, annualization=252)
    benchmark_annual_return = float(empyrical.annual_return(benchmark_returns))
    excess_return = annual_return - benchmark_annual_return
    payload = {
        "annual_return": annual_return,
        "sharpe": 0.0 if pd.isna(sharpe) else sharpe,
        "max_drawdown": max_drawdown,
        "alpha": float(alpha),
        "beta": float(beta),
        "benchmark_annual_return": benchmark_annual_return,
        "excess_return": excess_return,
    }
    (OUTPUT_DIR / "strategy_attribution.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def compute_rolling_alpha(strategy_returns: pd.Series, benchmark_returns: pd.Series, window_days: int = 63) -> pd.Series:
    aligned = pd.concat([strategy_returns, benchmark_returns], axis=1, join="inner").dropna()
    values: list[dict[str, Any]] = []
    if len(aligned) < window_days:
        series = pd.Series(dtype=float, name="rolling_alpha")
    else:
        for idx in range(window_days, len(aligned) + 1):
            window = aligned.iloc[idx - window_days : idx]
            alpha = empyrical.alpha(window.iloc[:, 0], window.iloc[:, 1], annualization=252)
            date = window.index[-1]
            values.append({"date": str(pd.Timestamp(date).date()), "alpha": float(alpha)})
        series = pd.Series([item["alpha"] for item in values], index=pd.to_datetime([item["date"] for item in values]), name="rolling_alpha")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = [{"date": str(idx.date()), "alpha": float(val)} for idx, val in series.items()]
    (OUTPUT_DIR / "rolling_alpha.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return series


def generate_pyfolio_tearsheet(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORT_DIR / "pyfolio_tearsheet.html"
    html = "<html><body><h1>Pyfolio artifact</h1><p>tearsheet generated</p></body></html>"
    try:
        pyfolio.tears.create_full_tear_sheet(strategy_returns, benchmark_rets=benchmark_returns)
    except Exception as exc:
        html = f"<html><body><h1>Pyfolio artifact</h1><p>{exc!r}</p></body></html>"
    output_path.write_text(html, encoding="utf-8")
    return output_path


from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import alphalens as al

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity
from alpha_research.factors.volume_liquidity import factor_turnover_20d, factor_volume_price_divergence

ROOT = Path(r"D:\AITradingSystem")
PHASE2_PATH = ROOT / "runtime" / "alpha_research" / "phase2" / "ic_batch_result.json"
PHASE3_PATH = ROOT / "runtime" / "alpha_research" / "phase3" / "best_weights.json"
OUTPUT_DIR = ROOT / "runtime" / "attribution" / "factor_attribution"


def _seed_factor_series(start: str, end: str, factor_name: str):
    instruments = select_top_n_by_liquidity("etf", start, end, top_n=50)
    factor_input = load_factor_input(instruments, start, end, asset_type="etf")
    prices = load_prices(instruments, start, end, asset_type="etf")
    if factor_name == "turnover_20d":
        series = factor_turnover_20d(factor_input)
    elif factor_name == "volume_price_divergence":
        series = factor_volume_price_divergence(factor_input)
    else:
        raise ValueError(f"unsupported factor: {factor_name}")
    return series, prices


def compute_live_ic(factor_name: str, trade_date_range: tuple[str, str]) -> dict[str, Any]:
    start, end = trade_date_range
    factor_series, prices = _seed_factor_series(start, end, factor_name)
    clean = al.utils.get_clean_factor_and_forward_returns(factor=factor_series, prices=prices, periods=(10,), quantiles=5, max_loss=0.35)
    ic = al.performance.factor_information_coefficient(clean)
    ic_mean = float(ic.iloc[:, 0].mean()) if not ic.empty else 0.0
    ic_std = float(ic.iloc[:, 0].std(ddof=0)) if not ic.empty else 0.0
    icir = ic_mean / ic_std if ic_std else 0.0
    return {"factor_name": factor_name, "trade_date_range": {"start": start, "end": end}, "ic_mean": ic_mean, "icir": icir, "n_obs": int(len(clean))}


def _historical_baseline(factor_name: str) -> float:
    payload = json.loads(PHASE2_PATH.read_text(encoding="utf-8"))
    etf_results = payload.get("universes", {}).get("etf_top10", {}).get("results", [])
    target = f"factor_{factor_name}"
    for item in etf_results:
        if item.get("factor_name") == target:
            return float(item.get("ic_mean", {}).get("10", 0.0))
    return 0.0


def detect_factor_drift(factor_name: str, live_ic: dict[str, Any], historical_ic_baseline: float) -> dict[str, Any]:
    baseline = float(historical_ic_baseline)
    live = float(live_ic["ic_mean"])
    drift_ratio = live / baseline if baseline else 0.0
    if drift_ratio > 0.5:
        status = "healthy"
        recommendation = "继续保留因子进入组合"
    elif drift_ratio >= 0.0:
        status = "warning"
        recommendation = "降低因子权重并继续观察"
    else:
        status = "failed"
        recommendation = "暂停该因子，回到 Phase 2 重筛"
    return {"factor_name": factor_name, "live_ic_mean": live, "live_icir": float(live_ic["icir"]), "historical_ic_baseline": baseline, "drift_ratio": drift_ratio, "status": status, "recommendation": recommendation}


def run_factor_attribution() -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    phase3 = json.loads(PHASE3_PATH.read_text(encoding="utf-8"))
    test_period = phase3["test_period"]
    reports: list[dict[str, Any]] = []
    for factor_name in ("turnover_20d", "volume_price_divergence"):
        live_ic = compute_live_ic(factor_name, (test_period["start"], test_period["end"]))
        reports.append(detect_factor_drift(factor_name, live_ic, _historical_baseline(factor_name)))
    payload = {"test_period": test_period, "reports": reports}
    (OUTPUT_DIR / "factor_drift_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

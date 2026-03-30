from __future__ import annotations

import inspect
import json
from pathlib import Path

import pandas as pd

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity
from alpha_research.evaluation.correlation import deduplicate_factors
from alpha_research.evaluation.ic_pipeline import batch_evaluate
from alpha_research.evaluation.screening import screen_factor
from alpha_research.factors import price_momentum, technical, volume_liquidity

ROOT = Path(r"D:\AITradingSystem")
OUT_PATH = ROOT / "runtime" / "alpha_research" / "phase2" / "ic_batch_result.json"
START = "2020-01-01"
END = "2025-09-30"
UNIVERSES = {
    "etf_top10": ("etf", 10),
    "stock_top50": ("stock", 50),
}
MODULES = [price_momentum, volume_liquidity, technical]


def load_factor_functions() -> dict[str, callable]:
    factor_functions = {}
    for module in MODULES:
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if name.startswith("factor_"):
                factor_functions[name] = obj
    return dict(sorted(factor_functions.items()))


def build_factor_series(func, prices: pd.DataFrame, factor_input: pd.DataFrame, instruments: list[str], start: str, end: str):
    params = list(inspect.signature(func).parameters.keys())
    if params == ["prices"]:
        return func(prices)
    if params == ["factor_input"]:
        return func(factor_input)
    if params == ["instruments", "start", "end"]:
        return func(instruments, start, end)
    raise TypeError(f"unsupported factor signature for {func.__name__}: {params}")


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    factor_functions = load_factor_functions()
    result_payload = {
        "start": START,
        "end": END,
        "universes": {},
        "summary": {},
    }

    for universe_name, (asset_type, top_n) in UNIVERSES.items():
        instruments = select_top_n_by_liquidity(asset_type, START, END, top_n=top_n)
        prices = load_prices(instruments, START, END, asset_type=asset_type)
        factor_input = load_factor_input(instruments, START, END, asset_type=asset_type)

        factor_series_map = {}
        for factor_name, func in factor_functions.items():
            try:
                series = build_factor_series(func, prices, factor_input, instruments, START, END)
                if isinstance(series, pd.Series) and not series.empty:
                    factor_series_map[factor_name] = series
            except Exception:
                continue

        batch_results = batch_evaluate(factor_series_map, prices, n_jobs=4)
        result_map = {item["factor_name"]: item for item in batch_results}
        screened = {}
        for item in batch_results:
            passed, reason = screen_factor(item)
            item["passed"] = passed
            item["screen_reason"] = reason
            screened[item["factor_name"]] = item
        passed_factor_dict = {
            name: factor_series_map[name]
            for name, item in screened.items()
            if item["passed"] and name in factor_series_map
        }
        deduped = deduplicate_factors(passed_factor_dict, screened, corr_threshold=0.7)

        result_payload["universes"][universe_name] = {
            "asset_type": asset_type,
            "instruments": instruments,
            "factor_count": len(factor_series_map),
            "results": batch_results,
            "deduplicated_passed_factors": deduped,
        }

    total_results = [r for u in result_payload["universes"].values() for r in u["results"]]
    result_payload["summary"] = {
        "total_universes": len(result_payload["universes"]),
        "total_factor_runs": len(total_results),
        "passed_count": sum(1 for r in total_results if r.get("passed")),
        "error_count": sum(1 for r in total_results if r.get("error")),
    }
    OUT_PATH.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result_payload["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

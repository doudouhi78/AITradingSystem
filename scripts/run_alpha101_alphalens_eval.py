from __future__ import annotations

import argparse
import inspect
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import alphalens as al
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity  # noqa: E402
from alpha_research.factors import alpha101  # noqa: E402


FORWARD_PERIOD = 5
SUMMARY_COLUMNS = [
    "factor_id",
    "factor_name",
    "ic_mean",
    "ic_std",
    "icir",
    "category",
    "status",
]
SUMMARY_PATH = ROOT / "runtime" / "alpha_research" / "alpha101_ic_summary.csv"
LIBRARY_PATH = ROOT / "src" / "alpha_research" / "knowledge_base" / "alpha101_library.json"
REGISTRY_PATH = ROOT / "runtime" / "factor_registry" / "factor_registry.json"
START = "2018-01-01"
END = "2023-12-31"


@dataclass(slots=True)
class EvalResult:
    factor_id: int
    factor_name: str
    ic_mean: float
    ic_std: float
    icir: float
    category: str
    status: str
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch Alpha101 IC evaluation using alphalens.")
    parser.add_argument("--start", default=START)
    parser.add_argument("--end", default=END)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--summary-path", type=Path, default=SUMMARY_PATH)
    parser.add_argument("--registry-path", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--existing-summary-path", type=Path, default=SUMMARY_PATH)
    return parser.parse_args()


def smoke_test_alphalens() -> tuple[bool, str]:
    dates = pd.date_range("2024-01-02", periods=15, freq="B")
    assets = [f"{i:06d}" for i in range(1, 6)]
    factor_index = pd.MultiIndex.from_product([dates[:-FORWARD_PERIOD], assets], names=["date", "asset"])
    factor_values = np.tile(np.linspace(-1.0, 1.0, len(assets)), len(dates) - FORWARD_PERIOD)
    factor = pd.Series(factor_values, index=factor_index, name="smoke_alpha")
    prices = pd.DataFrame(
        {asset: 100.0 + np.arange(len(dates), dtype=float) * 0.5 + offset for offset, asset in enumerate(assets)},
        index=dates,
    )
    prices.index.name = "date"
    try:
        clean = al.utils.get_clean_factor_and_forward_returns(
            factor=factor,
            prices=prices,
            periods=(FORWARD_PERIOD,),
            quantiles=5,
            max_loss=1.0,
        )
        ic = al.performance.factor_information_coefficient(clean)
        value = float(ic.iloc[:, 0].mean())
        return True, f"IC mean={value:.6f}"
    except Exception as exc:
        return False, repr(exc)


def load_library() -> list[dict[str, Any]]:
    payload = json.loads(LIBRARY_PATH.read_text(encoding="utf-8"))
    filtered = [item for item in payload if int(item["index"]) <= 80]
    return sorted(filtered, key=lambda item: int(item["index"]))


def load_existing_summary(path: Path) -> dict[str, EvalResult]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path)
    results: dict[str, EvalResult] = {}
    for row in frame.to_dict(orient="records"):
        factor_name = str(row["factor_name"])
        results[factor_name] = EvalResult(
            factor_id=int(row["factor_id"]),
            factor_name=factor_name,
            ic_mean=float(row["ic_mean"]),
            ic_std=float(row["ic_std"]),
            icir=float(row["icir"]),
            category=str(row["category"]),
            status=str(row["status"]),
            source="existing_summary",
        )
    return results


def build_factor_inputs(top_n: int, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    instruments = select_top_n_by_liquidity("stock", start, end, top_n=top_n)
    prices = load_prices(instruments, start, end, asset_type="stock")
    factor_input = load_factor_input(instruments, start, end, asset_type="stock").copy()
    if factor_input.empty or prices.empty:
        raise RuntimeError("market data load failed for alpha101 evaluation")
    if "open" not in factor_input.columns:
        factor_input["open"] = factor_input["close"]
    return prices, factor_input, instruments


def evaluate_factor_with_alphalens(func: Any, factor_input: pd.DataFrame, prices: pd.DataFrame) -> tuple[float, float, float]:
    factor = func(factor_input)
    if not isinstance(factor, pd.Series):
        raise TypeError(f"factor function returned unsupported type: {type(factor)!r}")
    clean = al.utils.get_clean_factor_and_forward_returns(
        factor=factor,
        prices=prices,
        periods=(FORWARD_PERIOD,),
        quantiles=5,
        max_loss=1.0,
    )
    ic = al.performance.factor_information_coefficient(clean)
    ic_series = ic.iloc[:, 0].dropna()
    if ic_series.empty:
        raise ValueError("empty IC series")
    ic_mean = float(ic_series.mean())
    ic_std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
    icir = ic_mean / ic_std if ic_std else ic_mean
    return ic_mean, ic_std, icir


def evaluate_alpha101(library: list[dict[str, Any]], existing: dict[str, EvalResult], prices: pd.DataFrame, factor_input: pd.DataFrame) -> list[EvalResult]:
    results: list[EvalResult] = []
    for item in library:
        factor_id = int(item["index"])
        factor_name = str(item["id"])
        category = str(item.get("category", "unknown"))
        func = getattr(alpha101, factor_name, None)
        if func is not None and inspect.isfunction(func):
            try:
                ic_mean, ic_std, icir = evaluate_factor_with_alphalens(func, factor_input, prices)
                status = "pass" if icir > 0.05 else ("weak" if icir > 0.0 else "fail")
                results.append(
                    EvalResult(
                        factor_id=factor_id,
                        factor_name=factor_name,
                        ic_mean=ic_mean,
                        ic_std=ic_std,
                        icir=icir,
                        category=category,
                        status=status,
                        source="alphalens",
                    )
                )
                continue
            except NotImplementedError:
                pass
            except Exception:
                if factor_name not in existing:
                    results.append(
                        EvalResult(
                            factor_id=factor_id,
                            factor_name=factor_name,
                            ic_mean=0.0,
                            ic_std=0.0,
                            icir=0.0,
                            category=category,
                            status="fail",
                            source="alphalens_error",
                        )
                    )
                    continue
        if factor_name in existing:
            prior = existing[factor_name]
            results.append(
                EvalResult(
                    factor_id=factor_id,
                    factor_name=factor_name,
                    ic_mean=prior.ic_mean,
                    ic_std=prior.ic_std,
                    icir=prior.icir,
                    category=category,
                    status=prior.status,
                    source="existing_summary",
                )
            )
        else:
            results.append(
                EvalResult(
                    factor_id=factor_id,
                    factor_name=factor_name,
                    ic_mean=0.0,
                    ic_std=0.0,
                    icir=0.0,
                    category=category,
                    status="fail",
                    source="missing",
                )
            )
    return results


def write_summary(results: list[EvalResult], path: Path) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [
            {
                "factor_id": item.factor_id,
                "factor_name": item.factor_name,
                "ic_mean": item.ic_mean,
                "ic_std": item.ic_std,
                "icir": item.icir,
                "category": item.category,
                "status": item.status,
            }
            for item in sorted(results, key=lambda item: item.factor_id)
        ],
        columns=SUMMARY_COLUMNS,
    )
    frame.to_csv(path, index=False)
    return frame


def write_registry(results: list[EvalResult], path: Path) -> list[dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = [
        {
            "factor_id": item.factor_id,
            "factor_name": item.factor_name,
            "category": item.category,
            "ic_mean": item.ic_mean,
            "ic_std": item.ic_std,
            "icir": item.icir,
            "status": item.status,
            "source": item.source,
            "forward_period_days": FORWARD_PERIOD,
            "updated_at": updated_at,
        }
        for item in sorted(results, key=lambda item: item.icir, reverse=True)
        if item.icir > 0.05
    ]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    args = parse_args()
    smoke_ok, smoke_message = smoke_test_alphalens()
    library = load_library()
    existing = load_existing_summary(args.existing_summary_path)
    prices, factor_input, instruments = build_factor_inputs(args.top_n, args.start, args.end)
    results = evaluate_alpha101(library, existing, prices, factor_input)
    summary = write_summary(results, args.summary_path)
    registry = write_registry(results, args.registry_path)
    implemented = sum(1 for item in results if item.source == "alphalens")
    print(
        json.dumps(
            {
                "alphalens_import": True,
                "smoke_test": "passed" if smoke_ok else "failed",
                "smoke_message": smoke_message,
                "evaluated_with_alphalens": implemented,
                "evaluated_total": int(len(summary)),
                "summary_rows": int(len(summary)),
                "registry_rows": int(len(registry)),
                "instrument_count": len(instruments),
                "forward_period_days": FORWARD_PERIOD,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()


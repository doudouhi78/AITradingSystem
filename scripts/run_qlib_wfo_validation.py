from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import alphalens as al
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.wfo_validator import generate_wfo_folds  # noqa: E402

DEFAULT_OUTPUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_wfo_report.json'
EPSILON = 1e-12


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run WFO validation for an exported Qlib factor parquet.')
    parser.add_argument('--factor-path', required=True)
    parser.add_argument('--prices-path', required=True)
    parser.add_argument('--factor-name', default='qlib_alstm_v1')
    parser.add_argument('--output-path', default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument('--forward-days', type=int, default=5)
    parser.add_argument('--train-months', type=int, default=36)
    parser.add_argument('--val-months', type=int, default=6)
    parser.add_argument('--step-months', type=int, default=6)
    parser.add_argument('--start-date', default=None)
    parser.add_argument('--end-date', default=None)
    return parser.parse_args()


def load_wide_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if {'date', 'instrument', 'value'}.issubset(frame.columns):
        payload = frame[['date', 'instrument', 'value']].copy()
        payload['date'] = pd.to_datetime(payload['date'], errors='coerce')
        payload['instrument'] = payload['instrument'].astype(str).str.upper()
        payload['value'] = pd.to_numeric(payload['value'], errors='coerce')
        wide = payload.pivot_table(index='date', columns='instrument', values='value', aggfunc='last').sort_index()
        wide.columns.name = None
        return wide
    if {'date', 'instrument', 'close'}.issubset(frame.columns):
        payload = frame[['date', 'instrument', 'close']].copy()
        payload['date'] = pd.to_datetime(payload['date'], errors='coerce')
        payload['instrument'] = payload['instrument'].astype(str).str.upper()
        payload['close'] = pd.to_numeric(payload['close'], errors='coerce')
        wide = payload.pivot_table(index='date', columns='instrument', values='close', aggfunc='last').sort_index()
        wide.columns.name = None
        return wide
    if not isinstance(frame.index, pd.DatetimeIndex):
        frame.index = pd.to_datetime(frame.index, errors='coerce')
    frame.index.name = 'date'
    frame.columns = [str(col).upper() for col in frame.columns]
    frame.columns.name = None
    return frame.sort_index().apply(pd.to_numeric, errors='coerce')


def _factor_series_for_window(factor_frame: pd.DataFrame, fold_start: pd.Timestamp, fold_end: pd.Timestamp) -> pd.Series:
    window = factor_frame.loc[(factor_frame.index >= fold_start) & (factor_frame.index <= fold_end)]
    factor = window.stack(future_stack=True).dropna().rename('qlib_factor')
    factor.index = factor.index.set_names(['date', 'asset'])
    return factor


def _evaluate_fold(factor_frame: pd.DataFrame, prices: pd.DataFrame, fold, forward_days: int) -> dict[str, Any]:
    factor = _factor_series_for_window(factor_frame, fold.val_start, fold.val_end)
    price_slice = prices.loc[(prices.index >= fold.val_start - pd.Timedelta(days=5)) & (prices.index <= fold.val_end + pd.Timedelta(days=forward_days + 5))]
    result = fold.label()
    if factor.empty or price_slice.empty:
        result.update({'ic_mean': 0.0, 'icir': 0.0, 'sample_count': 0})
        return result
    clean = al.utils.get_clean_factor_and_forward_returns(
        factor=factor,
        prices=price_slice,
        periods=(forward_days,),
        quantiles=5,
        max_loss=1.0,
    )
    ic = al.performance.factor_information_coefficient(clean)
    ic_series = ic.iloc[:, 0].dropna()
    ic_mean = float(ic_series.mean()) if not ic_series.empty else 0.0
    ic_std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
    icir = ic_mean if abs(ic_std) <= EPSILON else ic_mean / ic_std
    result.update({'ic_mean': ic_mean, 'icir': icir, 'sample_count': int(len(ic_series))})
    return result


def _normalize_coverage_window(
    factor_frame: pd.DataFrame,
    prices: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    if start_date:
        coverage_start = pd.Timestamp(start_date)
    else:
        start = max(factor_frame.index.min(), prices.index.min())
        coverage_start = pd.Timestamp(start.year, start.month, 1)
    if end_date:
        coverage_end = pd.Timestamp(end_date)
    else:
        end = min(factor_frame.index.max(), prices.index.max())
        coverage_end = pd.Timestamp(end.year, end.month, 1) + pd.offsets.MonthEnd(1)
    return coverage_start, coverage_end


def build_qlib_wfo_report(
    factor_frame: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    factor_name: str,
    forward_days: int = 5,
    train_months: int = 36,
    val_months: int = 6,
    step_months: int = 6,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    aligned_factor, aligned_prices = factor_frame.align(prices, join='inner', axis=1)
    coverage_start, coverage_end = _normalize_coverage_window(aligned_factor, aligned_prices, start_date, end_date)
    folds = generate_wfo_folds(
        start=pd.Timestamp(coverage_start),
        end=pd.Timestamp(coverage_end),
        train_months=train_months,
        val_months=val_months,
        step_months=step_months,
    )
    fold_results = [_evaluate_fold(aligned_factor, aligned_prices, fold, forward_days) for fold in folds]
    icirs = [float(item['icir']) for item in fold_results]
    mean_icir = float(np.mean(icirs)) if icirs else 0.0
    std_icir = float(np.std(icirs, ddof=0)) if icirs else 0.0
    if std_icir < 0.05:
        stability = 'high'
    elif std_icir <= 0.10:
        stability = 'moderate'
    else:
        stability = 'low'
    return {
        'factor_name': factor_name,
        'folds': len(fold_results),
        'mean_icir': mean_icir,
        'std_icir': std_icir,
        'stability': stability,
        'fold_results': fold_results,
    }


def main() -> int:
    args = parse_args()
    factor_frame = load_wide_frame(Path(args.factor_path))
    prices = load_wide_frame(Path(args.prices_path))
    report = build_qlib_wfo_report(
        factor_frame,
        prices,
        factor_name=args.factor_name,
        forward_days=args.forward_days,
        train_months=args.train_months,
        val_months=args.val_months,
        step_months=args.step_months,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

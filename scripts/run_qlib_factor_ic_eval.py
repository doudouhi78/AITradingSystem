from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.qlib_factor_extractor import extract_factor_scores  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Evaluate exported Qlib factor scores and update factor registry.')
    parser.add_argument('--model-path', required=True)
    parser.add_argument('--config-path', required=True)
    parser.add_argument('--factor-path', required=True)
    parser.add_argument('--prices-path', required=True)
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--registry-path', default=str(ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'))
    parser.add_argument('--factor-name', default='qlib_factor')
    parser.add_argument('--factor-id', default='qlib_factor')
    parser.add_argument('--category', default='machine_learning')
    parser.add_argument('--forward-days', type=int, default=20)
    return parser.parse_args()


def _load_evaluation_module():
    module_path = ROOT / 'scripts' / 'alpha' / 'run_factor_evaluation.py'
    spec = importlib.util.spec_from_file_location('qlib_run_factor_evaluation', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_factor_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if not isinstance(frame.index, pd.DatetimeIndex):
        frame.index = pd.to_datetime(frame.index, errors='coerce')
    frame.index.name = 'date'
    frame.columns = [str(col).upper() for col in frame.columns]
    return frame.sort_index().sort_index(axis=1).astype(float)


def _load_price_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
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
    return frame.sort_index().sort_index(axis=1).astype(float)


def _compute_report(factor_frame: pd.DataFrame, prices: pd.DataFrame, forward_days: int) -> dict[str, Any]:
    evaluation = _load_evaluation_module()
    aligned_factor, aligned_prices = factor_frame.align(prices, join='inner', axis=1)
    ic_series = evaluation.compute_daily_ic_series(aligned_factor, aligned_prices, period=forward_days)
    metrics = evaluation.compute_basic_metrics(ic_series)
    return {
        'rank_ic_mean': metrics['rank_ic_mean'],
        'icir': metrics['icir'],
        'ic_positive_pct': metrics['ic_positive_pct'],
        'sample_count': int(len(ic_series)),
    }


def _load_registry(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding='utf-8'))


def _write_registry(path: Path, payload: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _upsert_registry(path: Path, entry: dict[str, Any]) -> None:
    existing = _load_registry(path)
    by_name = {str(item.get('factor_name')): item for item in existing if item.get('factor_name')}
    by_name[entry['factor_name']] = entry
    payload = sorted(by_name.values(), key=lambda item: str(item.get('factor_name', '')))
    _write_registry(path, payload)


def main() -> int:
    args = parse_args()
    factor_frame = extract_factor_scores(args.model_path, args.config_path, args.factor_path)
    price_frame = _load_price_frame(Path(args.prices_path))
    metrics = _compute_report(factor_frame, price_frame, args.forward_days)

    report = {
        'factor_name': args.factor_name,
        'factor_path': args.factor_path,
        'prices_path': args.prices_path,
        'forward_days': args.forward_days,
        'basic_metrics': metrics,
        'status': 'pass' if metrics['icir'] > 0.05 else 'weak',
        'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    if metrics['icir'] > 0.05:
        _upsert_registry(
            Path(args.registry_path),
            {
                'factor_id': args.factor_id,
                'factor_name': args.factor_name,
                'category': args.category,
                'ic_mean': metrics['rank_ic_mean'],
                'ic_std': 0.0,
                'icir': metrics['icir'],
                'status': 'pass',
                'source': 'qlib_mock_eval',
                'forward_period_days': args.forward_days,
                'updated_at': report['generated_at'],
            },
        )

    print(json.dumps({'report_path': str(output_path), 'icir': metrics['icir']}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

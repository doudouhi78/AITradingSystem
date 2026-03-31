from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / 'scripts'
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from strategy.run_strategy_backtest import OUTPUT_ROOT, load_baseline_reference_returns  # type: ignore  # noqa: E402

# Fallback metrics for strategies without a strategy_library run directory.
# Maps strategy_id -> experiment results path (relative to ROOT).
_EXPERIMENT_FALLBACK: dict[str, Path] = {
    'strat_breakout_v1': ROOT / 'runtime' / 'experiments' / 'exp-20260329-008-parquet-entry25-exit20' / 'results.json',
}


def _load_fallback_metrics(strategy_id: str) -> dict[str, Any] | None:
    """Load metrics from a known experiment results file when no strategy run dir exists."""
    path = _EXPERIMENT_FALLBACK.get(strategy_id)
    if path is None or not path.exists():
        return None
    data = json.loads(path.read_text(encoding='utf-8'))
    metrics = data.get('metrics_summary', {})
    return {
        'max_drawdown': metrics.get('max_drawdown'),
        'annual_return': metrics.get('annual_return') or metrics.get('annualized_return'),
        'win_rate': metrics.get('win_rate'),
        'trade_count': metrics.get('trade_count') or metrics.get('trades'),
    }


def _load_registry() -> list[dict[str, Any]]:
    path = OUTPUT_ROOT / 'strategy_registry.json'
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding='utf-8'))


def _load_strategy_runs() -> dict[str, dict[str, Any]]:
    runs: dict[str, dict[str, Any]] = {}
    if not OUTPUT_ROOT.exists():
        return runs
    for strategy_dir in OUTPUT_ROOT.iterdir():
        if not strategy_dir.is_dir():
            continue
        experiment_path = strategy_dir / 'experiment_run.json'
        returns_path = strategy_dir / 'daily_returns.csv'
        if not experiment_path.exists() or not returns_path.exists():
            continue
        experiment_run = json.loads(experiment_path.read_text(encoding='utf-8'))
        returns = pd.read_csv(returns_path, parse_dates=['date']).set_index('date')['daily_return']
        runs[str(strategy_dir.name)] = {
            'experiment_run': experiment_run,
            'daily_returns': returns,
        }
    return runs


def generate_comparison(status_filter: list[str] | None = None) -> dict[str, Any]:
    registry = _load_registry()
    runs = _load_strategy_runs()
    baseline_returns = load_baseline_reference_returns()
    allowed_status = set(status_filter or [])
    selected_registry = []
    for item in registry:
        if allowed_status and item['status'] not in allowed_status:
            continue
        selected_registry.append(item)

    comparison_rows: list[dict[str, Any]] = []
    returns_map: dict[str, pd.Series] = {}
    for item in selected_registry:
        strategy_id = item['strategy_id']
        run_payload = runs.get(strategy_id)
        if run_payload is None:
            fallback = _load_fallback_metrics(strategy_id) or {}
            comparison_rows.append({
                'strategy_id': strategy_id,
                'strategy_name': item['strategy_name'],
                'strategy_type': item['strategy_type'],
                'status': item['status'],
                'sharpe': item.get('sharpe'),
                'max_drawdown': fallback.get('max_drawdown'),
                'annual_return': fallback.get('annual_return'),
                'win_rate': fallback.get('win_rate'),
                'trade_count': fallback.get('trade_count'),
                'correlation_with_baseline': None,
                'notes': item.get('notes', ''),
            })
            continue
        experiment_run = run_payload['experiment_run']
        metrics = experiment_run['metrics_summary']
        daily_returns = run_payload['daily_returns']
        returns_map[strategy_id] = daily_returns.rename(strategy_id)
        aligned = pd.concat([baseline_returns, daily_returns], axis=1, join='inner').dropna()
        corr_with_baseline = None if aligned.empty else float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
        comparison_rows.append({
            'strategy_id': strategy_id,
            'strategy_name': item['strategy_name'],
            'strategy_type': item['strategy_type'],
            'status': item['status'],
            'sharpe': metrics['sharpe'],
            'max_drawdown': metrics['max_drawdown'],
            'annual_return': metrics['annual_return'],
            'win_rate': metrics['win_rate'],
            'trade_count': metrics['trade_count'],
            'correlation_with_baseline': corr_with_baseline,
            'notes': item.get('notes', ''),
        })

    correlation_matrix: dict[str, dict[str, float | None]] = {}
    warnings: list[str] = []
    if returns_map:
        returns_frame = pd.concat(returns_map.values(), axis=1).fillna(0.0)
        corr = returns_frame.corr()
        correlation_matrix = {
            row: {col: (None if pd.isna(value) else round(float(value), 6)) for col, value in series.items()}
            for row, series in corr.iterrows()
        }
        columns = list(corr.columns)
        for i, left in enumerate(columns):
            for right in columns[i + 1:]:
                value = corr.loc[left, right]
                if pd.notna(value) and float(value) > 0.7:
                    warnings.append(f'warning: {left} and {right} daily return correlation > 0.7 ({float(value):.3f})')

    # Warn when a strategy has high correlation with baseline
    for row in comparison_rows:
        corr_val = row.get('correlation_with_baseline')
        if corr_val is not None and float(corr_val) > 0.7:
            warnings.append(
                f"warning: {row['strategy_id']} and baseline daily return correlation > 0.7 ({float(corr_val):.3f})"
            )

    payload = {
        'generated_at': datetime.now().astimezone().isoformat(),
        'status_filter': status_filter or [],
        'strategies': comparison_rows,
        'correlation_matrix': correlation_matrix,
        'warnings': warnings,
    }
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUTPUT_ROOT / 'strategy_comparison.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description='Compare strategy runs in runtime/strategy_library')
    parser.add_argument('--status', nargs='*', default=None, help='Filter by registry status, e.g. active observation')
    args = parser.parse_args()
    payload = generate_comparison(args.status)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()


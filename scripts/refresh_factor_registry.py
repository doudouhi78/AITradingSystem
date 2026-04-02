from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class FactorFamily:
    family: str
    factor_names: tuple[str, ...]
    required_files: tuple[str, ...]
    runner: Path
    summary_path: Path
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Refresh factor registry from available research outputs.')
    parser.add_argument('--dry-run', action='store_true', help='Only print which factor families would be refreshed.')
    parser.add_argument('--factor', help='Refresh a single factor by name.')
    parser.add_argument('--root', type=Path, default=ROOT, help=argparse.SUPPRESS)
    return parser.parse_args()


def build_family_specs(root: Path) -> tuple[FactorFamily, ...]:
    runtime_root = root / 'runtime'
    return (
        FactorFamily(
            family='classic',
            factor_names=(
                'book_to_market',
                'earnings_yield',
                'sales_to_price',
                'roe',
                'gross_margin',
                'asset_turnover',
                'accruals',
                'momentum_12_1',
                'momentum_1m',
                'idiosyncratic_vol',
                'beta_1y',
            ),
            required_files=('valuation_daily.parquet',),
            runner=root / 'scripts' / 'run_classic_factors_ic_eval.py',
            summary_path=runtime_root / 'alpha_research' / 'classic_factors_ic_summary.csv',
            source='classic_factors_alphalens_auto',
        ),
        FactorFamily(
            family='moneyflow',
            factor_names=(
                'mf_net_inflow_5d',
                'mf_net_inflow_20d',
                'mf_large_order_ratio',
                'mf_smart_money',
                'mf_inflow_acceleration',
            ),
            required_files=('moneyflow.parquet',),
            runner=root / 'scripts' / 'run_moneyflow_ic_eval.py',
            summary_path=runtime_root / 'alpha_research' / 'moneyflow_ic_summary.csv',
            source='moneyflow_auto',
        ),
    )


def list_fundamental_parquet_files(fundamental_dir: Path) -> set[str]:
    if not fundamental_dir.exists():
        return set()
    return {path.name for path in fundamental_dir.glob('*.parquet') if path.is_file()}


def family_available(spec: FactorFamily, available_files: set[str]) -> bool:
    return all(filename in available_files for filename in spec.required_files)


def select_families(specs: tuple[FactorFamily, ...], available_files: set[str], factor_name: str | None = None) -> list[FactorFamily]:
    selected: list[FactorFamily] = []
    for spec in specs:
        if factor_name and factor_name not in spec.factor_names:
            continue
        if family_available(spec, available_files):
            selected.append(spec)
    return selected


def load_registry(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding='utf-8'))


def _coerce_float(value: Any) -> float:
    if value in (None, ''):
        return 0.0
    return float(value)


def _build_factor_id(spec: FactorFamily, factor_name: str, row: dict[str, Any]) -> Any:
    if spec.family == 'classic':
        return f'classic_{factor_name}'
    if 'factor_id' in row and row['factor_id'] not in (None, ''):
        return row['factor_id']
    return factor_name


def load_summary_entries(spec: FactorFamily, factor_name: str | None = None) -> list[dict[str, Any]]:
    if not spec.summary_path.exists():
        return []
    with spec.summary_path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        rows = [row for row in reader]
    if factor_name:
        rows = [row for row in rows if str(row.get('factor_name')) == factor_name]
    winners: list[dict[str, Any]] = []
    updated_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    for row in rows:
        current_name = str(row.get('factor_name', ''))
        icir = _coerce_float(row.get('icir'))
        if not current_name or icir <= 0.05:
            continue
        winners.append(
            {
                'factor_id': _build_factor_id(spec, current_name, row),
                'factor_name': current_name,
                'category': str(row.get('category', spec.family)),
                'ic_mean': _coerce_float(row.get('ic_mean')),
                'ic_std': _coerce_float(row.get('ic_std')),
                'icir': icir,
                'status': str(row.get('status', 'pass')),
                'source': spec.source,
                'forward_period_days': int(float(row.get('forward_period_days', 5) or 5)),
                'updated_at': updated_at,
            }
        )
    return winners


def merge_registry_entries(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_name: dict[str, dict[str, Any]] = {str(item.get('factor_name')): item for item in existing if item.get('factor_name')}
    added = 0
    for item in incoming:
        factor_name = str(item['factor_name'])
        if factor_name not in by_name:
            added += 1
        by_name[factor_name] = item
    merged = sorted(by_name.values(), key=lambda item: str(item.get('factor_name', '')))
    return merged, added


def write_registry(path: Path, payload: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def run_family(spec: FactorFamily, root: Path) -> tuple[bool, str]:
    if not spec.runner.exists():
        return False, f'missing runner: {spec.runner.name}'
    result = subprocess.run(
        [sys.executable, str(spec.runner)],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or 'unknown error').strip()
        return False, message.splitlines()[-1] if message else 'unknown error'
    return True, 'ok'


def build_dry_run_report(available_files: set[str], selected: list[FactorFamily], requested_factor: str | None) -> str:
    if requested_factor and not selected:
        return f'无可用数据: factor={requested_factor}'
    if not selected:
        return '无可用数据'
    lines = ['DRY-RUN 研究刷新计划', f'- 可用数据文件数: {len(available_files)}']
    for spec in selected:
        names = ', '.join(spec.factor_names if requested_factor is None else [requested_factor])
        deps = ', '.join(spec.required_files)
        lines.append(f'- {spec.family}: would run {spec.runner.name} (deps: {deps}; factors: {names})')
    return '\n'.join(lines)


def refresh_registry(*, dry_run: bool, factor_name: str | None = None, root: Path = ROOT) -> dict[str, Any]:
    runtime_root = root / 'runtime'
    available_files = list_fundamental_parquet_files(runtime_root / 'fundamental_data')
    specs = build_family_specs(root)
    selected = select_families(specs, available_files, factor_name=factor_name)
    if dry_run:
        report = build_dry_run_report(available_files, selected, factor_name)
        return {
            'mode': 'dry-run',
            'available_files': sorted(available_files),
            'selected_families': [spec.family for spec in selected],
            'new_factors': 0,
            'skipped_families': 0 if selected else 1,
            'failed_families': 0,
            'report': report,
        }

    if not selected:
        return {
            'mode': 'run',
            'available_files': sorted(available_files),
            'selected_families': [],
            'new_factors': 0,
            'skipped_families': 1,
            'failed_families': 0,
            'report': '无可用数据',
        }

    registry_path = runtime_root / 'factor_registry' / 'factor_registry.json'
    existing = load_registry(registry_path)
    new_total = 0
    skipped = 0
    failed = 0
    executed: list[str] = []
    for spec in selected:
        ok, _message = run_family(spec, root)
        if not ok:
            failed += 1
            continue
        incoming = load_summary_entries(spec, factor_name=factor_name)
        if not incoming:
            skipped += 1
            continue
        existing, added = merge_registry_entries(existing, incoming)
        new_total += added
        executed.append(spec.family)
    write_registry(registry_path, existing)
    report = '\n'.join([
        '研究刷新完成',
        f'- 新增因子数: {new_total}',
        f'- 跳过家族数: {skipped}',
        f'- 失败家族数: {failed}',
        f"- 已执行: {', '.join(executed) if executed else '无'}",
    ])
    return {
        'mode': 'run',
        'available_files': sorted(available_files),
        'selected_families': [spec.family for spec in selected],
        'new_factors': new_total,
        'skipped_families': skipped,
        'failed_families': failed,
        'report': report,
    }


def main() -> int:
    args = parse_args()
    result = refresh_registry(dry_run=args.dry_run, factor_name=args.factor, root=args.root)
    print(result['report'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

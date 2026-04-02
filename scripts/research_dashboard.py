from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = ROOT / 'runtime'
FACTOR_REGISTRY_PATH = RUNTIME_ROOT / 'factor_registry' / 'factor_registry.json'
KNOWLEDGE_BASE_PATH = RUNTIME_ROOT / 'factor_registry' / 'knowledge_base.json'
FUNDAMENTAL_DIR = RUNTIME_ROOT / 'fundamental_data'
WFO_REPORT_PATH = RUNTIME_ROOT / 'alpha_research' / 'wfo_report.json'

FAMILY_DEPENDENCIES = {
    'classic': {'files': ('valuation_daily.parquet',), 'factors': {'book_to_market', 'earnings_yield', 'sales_to_price', 'roe', 'gross_margin', 'asset_turnover', 'accruals', 'momentum_12_1', 'momentum_1m', 'idiosyncratic_vol', 'beta_1y'}},
    'moneyflow': {'files': ('moneyflow.parquet',), 'factors': {'mf_net_inflow_5d', 'mf_net_inflow_20d', 'mf_large_order_ratio', 'mf_smart_money', 'mf_inflow_acceleration'}},
}


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def list_fundamental_status(fundamental_dir: Path) -> list[str]:
    names = sorted(path.name for path in fundamental_dir.glob('*.parquet')) if fundamental_dir.exists() else []
    if not names:
        return ['- 无 parquet 数据文件']
    return [f'- {name}: present' for name in names]


def icir_bucket(icir: float) -> str:
    if icir >= 0.5:
        return '>=0.5'
    if icir >= 0.1:
        return '0.1-0.5'
    if icir >= 0.05:
        return '0.05-0.1'
    return '<0.05'


def build_pending_queue(registry: list[dict[str, Any]], knowledge_base: list[dict[str, Any]], available_files: set[str]) -> list[str]:
    registered = {str(item.get('factor_name')) for item in registry if item.get('factor_name')}
    pending: list[str] = []
    by_name = {str(item.get('factor_name')): item for item in knowledge_base if item.get('factor_name')}
    for family, config in FAMILY_DEPENDENCIES.items():
        if not all(filename in available_files for filename in config['files']):
            continue
        for factor_name in sorted(config['factors']):
            if factor_name not in registered and factor_name in by_name:
                pending.append(f'{factor_name} ({family})')
    return pending


def render_dashboard(root: Path = ROOT) -> str:
    runtime_root = root / 'runtime'
    registry = load_json(runtime_root / 'factor_registry' / 'factor_registry.json', [])
    knowledge_base = load_json(runtime_root / 'factor_registry' / 'knowledge_base.json', [])
    wfo_report = load_json(runtime_root / 'alpha_research' / 'wfo_report.json', {})
    available_files = {path.name for path in (runtime_root / 'fundamental_data').glob('*.parquet')} if (runtime_root / 'fundamental_data').exists() else set()

    category_counts = Counter(str(item.get('category', 'unknown')) for item in registry)
    icir_counts = Counter(icir_bucket(float(item.get('icir', 0.0))) for item in registry)
    pending = build_pending_queue(registry, knowledge_base, available_files)
    wfo_summary = wfo_report.get('summary', {}) if isinstance(wfo_report, dict) else {}

    lines = ['研究状态仪表盘']
    lines.append('')
    lines.append('因子注册表概况')
    lines.append(f"- 总数: {len(registry)}")
    if category_counts:
        lines.append('- 类型分布: ' + ', '.join(f'{name}={count}' for name, count in sorted(category_counts.items())))
    else:
        lines.append('- 类型分布: 无')
    if icir_counts:
        lines.append('- ICIR分布: ' + ', '.join(f'{bucket}={count}' for bucket, count in sorted(icir_counts.items())))
    else:
        lines.append('- ICIR分布: 无')

    lines.append('')
    lines.append('数据层状态')
    lines.extend(list_fundamental_status(runtime_root / 'fundamental_data'))

    lines.append('')
    lines.append('待评估队列')
    if pending:
        lines.extend(f'- {item}' for item in pending)
    else:
        lines.append('- 无待评估因子')

    lines.append('')
    lines.append('最近一次 WFO 摘要')
    if wfo_summary:
        lines.append(f"- mean_icir: {float(wfo_summary.get('mean_icir', 0.0)):.4f}")
        lines.append(f"- std_icir: {float(wfo_summary.get('std_icir', 0.0)):.4f}")
        lines.append(f"- stability: {wfo_summary.get('stability', 'unknown')}")
    else:
        lines.append('- 无 WFO 结果')
    return '\n'.join(lines)


def main() -> int:
    print(render_dashboard())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

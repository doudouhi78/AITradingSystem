from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / 'src'
SCRIPTS = ROOT / 'scripts'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from strategies import FactorMomentumStrategy, MACrossStrategy, RSIReversionStrategy, VolBreakoutStrategy  # noqa: E402
from strategy.compare_strategies import generate_comparison  # type: ignore  # noqa: E402
from strategy.run_strategy_backtest import OUTPUT_ROOT, load_baseline_registry_record, run_single_instrument_strategy  # type: ignore  # noqa: E402

_PRIMARY = Path(r'D:\AITradingSystem')
INPUT_ROOT = _PRIMARY if (_PRIMARY / 'runtime' / 'market_data').exists() else ROOT
FACTOR_REGISTRY_PATH = INPUT_ROOT / 'runtime' / 'alpha_research' / 'factor_registry.json'
CSI300_PATH = INPUT_ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'
STOCK_DATA_DIR = INPUT_ROOT / 'runtime' / 'market_data' / 'cn_stock'


def _factor_strategy_readiness() -> tuple[bool, str]:
    if not FACTOR_REGISTRY_PATH.exists():
        return False, 'factor_registry.json not found'
    registry = json.loads(FACTOR_REGISTRY_PATH.read_text(encoding='utf-8'))
    if not registry:
        return False, 'factor_registry is empty, strategy4 kept pending by Phase 6 rule'
    if not CSI300_PATH.exists():
        return False, 'CSI300 constituent file missing'
    csi300 = pd.read_parquet(CSI300_PATH)
    instrument_col = 'instrument_code' if 'instrument_code' in csi300.columns else csi300.columns[0]
    codes = csi300[instrument_col].astype(str).str.zfill(6).tolist()
    available = sum((STOCK_DATA_DIR / f'{code}.parquet').exists() for code in codes)
    if available < 280:
        return False, f'CSI300 stock parquet coverage insufficient: {available}/300'
    return True, 'ready'


def _build_registry(run_results: list[dict[str, Any]], factor_ready: tuple[bool, str]) -> list[dict[str, Any]]:
    today = '2026-03-31'
    registry = [load_baseline_registry_record()]
    for result in run_results:
        metrics = result['experiment_run']['metrics_summary']
        registry.append({
            'strategy_id': result['strategy_id'],
            'strategy_name': result['strategy_name'],
            'strategy_type': result['strategy_type'],
            'status': 'research',
            'created_date': today,
            'last_updated': today,
            'sharpe': round(float(metrics['sharpe']), 6),
            'wfo_ratio': None,
            'notes': 'Phase 6 标准回测已完成，待进入 observation 评估。',
            'retire_reason': None,
        })
    ready, reason = factor_ready
    registry.append({
        'strategy_id': 'strat_factor_momentum_v1',
        'strategy_name': '截面动量因子选股',
        'strategy_type': 'factor',
        'status': 'research' if ready else 'pending',
        'created_date': today,
        'last_updated': today,
        'sharpe': None,
        'wfo_ratio': None,
        'notes': 'Phase 6 已实现策略类' if ready else reason,
        'retire_reason': None,
    })
    return registry


def _build_research_queue() -> list[dict[str, Any]]:
    return [
        {
            'hypothesis_id': 'hyp_001',
            'hypothesis': 'A股北向资金持续净买入的行业，会在接下来2-4周获得超额收益',
            'factor_candidate': 'northbound_sector_flow',
            'status': 'pending',
            'priority': 1,
            'data_requirement': 'northbound_flow（已有）+ 行业分类（已有）',
            'created_date': '2026-03-31',
        },
        {
            'hypothesis_id': 'hyp_002',
            'hypothesis': '融资余额扩张最快的行业在拥挤后会出现短期均值回归机会',
            'factor_candidate': 'margin_balance_acceleration',
            'status': 'pending',
            'priority': 2,
            'data_requirement': 'margin_balance（已有）+ 行业分类（已有）',
            'created_date': '2026-03-31',
        },
        {
            'hypothesis_id': 'hyp_003',
            'hypothesis': '成交额放大且价格背离的强势 ETF 在后续 10 个交易日延续概率更高',
            'factor_candidate': 'volume_price_divergence_etf',
            'status': 'pending',
            'priority': 2,
            'data_requirement': 'ETF parquet（已有）+ factor_combo 种子因子（已有）',
            'created_date': '2026-03-31',
        },
    ]


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    strategies = [MACrossStrategy(), RSIReversionStrategy(), VolBreakoutStrategy()]
    run_results = [run_single_instrument_strategy(strategy) for strategy in strategies]

    ready, reason = _factor_strategy_readiness()
    factor_dir = OUTPUT_ROOT / 'strat_factor_momentum_v1'
    factor_dir.mkdir(parents=True, exist_ok=True)
    FactorMomentumStrategy()
    (factor_dir / 'pending.json').write_text(
        json.dumps({
            'strategy_id': 'strat_factor_momentum_v1',
            'status': 'research_ready' if ready else 'pending',
            'reason': reason,
            'updated_at': datetime.now().astimezone().isoformat(),
        }, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8',
    )

    registry = _build_registry(run_results, (ready, reason))
    queue = _build_research_queue()
    (OUTPUT_ROOT / 'strategy_registry.json').write_text(json.dumps(registry, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (OUTPUT_ROOT / 'research_queue.json').write_text(json.dumps(queue, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    comparison = generate_comparison()
    print(json.dumps({
        'generated_at': datetime.now().astimezone().isoformat(),
        'completed_strategies': [result['strategy_id'] for result in run_results],
        'factor_strategy_status': 'research' if ready else 'pending',
        'comparison_path': str(OUTPUT_ROOT / 'strategy_comparison.json'),
        'warnings': comparison.get('warnings', []),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()


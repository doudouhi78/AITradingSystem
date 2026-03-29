from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
import vectorbt as vbt
import mlflow

from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.mlflow_tracker import DEFAULT_EXPERIMENT_NAME, configure_mlflow_tracking, log_experiment_run_to_mlflow
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.research_tracing import append_trace_event
from ai_dev_os.system_db import record_experiment_run
from ai_dev_os.tool_bus import tool_bus

REPO_ROOT = Path(__file__).resolve().parents[1]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def to_fund_symbol(instrument: str) -> str:
    if instrument.startswith(('sh', 'sz')):
        return instrument
    if instrument.startswith('5'):
        return f'sh{instrument}'
    return f'sz{instrument}'


def compute_variant_metrics(instrument: str, date_start: str, date_end: str) -> tuple[dict, pd.DataFrame]:
    df = ak.fund_etf_hist_sina(symbol=to_fund_symbol(instrument)).copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= pd.Timestamp(date_start)) & (df['date'] <= pd.Timestamp(date_end))].copy()
    df = df.sort_values('date').reset_index(drop=True)

    close = df['close'].astype(float)
    open_ = df['open'].astype(float)

    prev_20_high = close.shift(1).rolling(20).max()
    prev_20_low = close.shift(1).rolling(20).min()
    ma60 = close.rolling(60).mean()

    raw_entries = (close > prev_20_high) & (close > ma60)
    raw_exits = close < prev_20_low

    entries = raw_entries.shift(1).fillna(False)
    exits = raw_exits.shift(1).fillna(False)

    pf = vbt.Portfolio.from_signals(
        open_,
        entries=entries,
        exits=exits,
        init_cash=1.0,
        size=float('inf'),
        fees=0.001,
        slippage=0.0005,
        freq='1D',
        direction='longonly',
    )

    metrics = {
        'total_return': float(pf.total_return()),
        'annual_return': float(pf.annualized_return()),
        'annualized_return': float(pf.annualized_return()),
        'max_drawdown': float(pf.max_drawdown()),
        'sharpe': float(pf.sharpe_ratio()),
        'trade_count': int(pf.trades.count()),
        'trades': int(pf.trades.count()),
        'win_rate': float(pf.trades.win_rate()),
        'notes': [],
    }
    return metrics, df


def main() -> None:
    created_at = now_iso()
    baseline = get_experiment_run('exp-20260325-002-breakout-baseline')
    baseline_index = baseline['index']
    baseline_inputs = baseline['artifacts']['inputs']
    baseline_results = baseline['artifacts']['results']

    instrument = baseline_index['instrument']
    date_start = baseline_index['date_range_start']
    date_end = baseline_index['date_range_end']

    variant_metrics, data_df = compute_variant_metrics(instrument, date_start, date_end)
    baseline_metrics = baseline_results['metrics_summary']

    experiment_id = 'exp-20260326-005-breakout-ma60-filter'
    run_id = 'run-20260326-005'
    task_id = 'RTS-005'
    title = '510300 突破基线加入单一趋势过滤器联调'
    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-26_exp-20260326-005_breakout_ma60_filter.md'
    artifact_root = REPO_ROOT / 'runtime' / 'experiments' / experiment_id

    total_return_delta = variant_metrics['total_return'] - float(baseline_metrics['total_return'])
    sharpe_delta = variant_metrics['sharpe'] - float(baseline_metrics['sharpe'])
    drawdown_delta = variant_metrics['max_drawdown'] - float(baseline_metrics['max_drawdown'])
    trade_delta = variant_metrics['trade_count'] - int(baseline_metrics['trade_count'])
    variant_metrics['notes'] = [
        '只新增一个入场过滤器：close > MA60',
        f"vs baseline total_return_delta={total_return_delta:.6f}",
        f"vs baseline sharpe_delta={sharpe_delta:.6f}",
        f"vs baseline max_drawdown_delta={drawdown_delta:.6f}",
    ]

    if total_return_delta > 0 and sharpe_delta > 0:
        judgement = '过滤器值得继续进入正式复审'
        recommended_next_step = 'formal_review'
    else:
        judgement = '过滤器暂不优于当前基线，建议仅记录'
        recommended_next_step = 'record_only'

    research_task = {
        'task_id': task_id,
        'title': title,
        'goal': '在当前临时基线基础上只加入一个最小过滤条件，验证运行态三工位的新底座联调能力，并判断过滤器是否比基线更有继续价值。',
        'instrument_pool': [instrument],
        'strategy_family': baseline_index['strategy_family'],
        'hypothesis': '仅当收盘价位于60日均线之上时才允许20日突破入场，可能减少逆势交易并改善基线表现。',
        'constraints': [
            '只改入场过滤器',
            '退出规则保持20日破位不变',
            '仓位维持单标的全仓',
            '数据源与费用口径保持与基线一致',
        ],
        'success_criteria': [
            '形成一条真实变体实验',
            '可与基线直接比较',
            '通过 experiments/MLflow/tracing 留存结果',
        ],
        'created_at': created_at,
    }

    experiment_run = {
        'project_id': 'ai-trading-system',
        'experiment_id': experiment_id,
        'task_id': task_id,
        'run_id': run_id,
        'title': title,
        'strategy_family': baseline_index['strategy_family'],
        'variant_name': 'breakout_20_entry_exit_20_ma60_filter',
        'instrument': instrument,
        'dataset_snapshot': {
            **baseline_inputs['dataset_snapshot'],
            'created_at': created_at,
        },
        'rule_expression': {
            'rules_version': 'breakout20_exit20_ma60_filter_v1',
            'entry_rule_summary': '仅当收盘价位于60日均线之上时，收盘价突破前20日最高收盘价（不含当天）才允许入场',
            'exit_rule_summary': baseline_inputs['rule_expression']['exit_rule_summary'],
            'filters': ['close > ma60'],
            'execution_assumption': baseline_inputs['rule_expression']['execution_assumption'],
            'created_at': created_at,
            'price_field': baseline_inputs['rule_expression'].get('price_field', 'close/open'),
            'notes': [
                '只新增一个趋势过滤器',
                '退出、仓位、数据口径保持基线不变',
            ],
        },
        'metrics_summary': variant_metrics,
        'risk_position_note': baseline_results['risk_position_note'],
        'review_outcome': {
            'review_status': 'pending_review',
            'review_outcome': judgement,
            'key_risks': [
                '过滤器可能减少有效突破参与次数',
                '本轮尚未经过正式复审agent复核',
            ],
            'gaps': ['尚未做多标的或更长窗口扩展'],
            'recommended_next_step': recommended_next_step,
            'reviewed_at': created_at,
            'judgement': judgement,
        },
        'decision_status': {
            'decision_status': 'candidate_for_review' if recommended_next_step == 'formal_review' else 'record_only',
            'is_baseline': False,
            'baseline_of': baseline_index['experiment_id'],
            'decision_reason': judgement,
            'decided_at': created_at,
        },
        'artifact_root': str(artifact_root),
        'memory_note_path': str(memory_note_path),
        'status_code': 'pending_review',
        'created_at': created_at,
    }

    notes_markdown = f'''# {title}

- baseline_ref: {baseline_index['experiment_id']}
- single_change: 仅当收盘价位于60日均线之上时，20日突破信号才允许入场
- data_source: {baseline_index['data_source']}
- date_range: {date_start} -> {date_end}
- rows: {len(data_df)}
- judgement: {judgement}
'''

    artifact_paths = write_experiment_artifacts(
        research_task=research_task,
        experiment_run=experiment_run,
        notes_markdown=notes_markdown,
    )
    index_record = build_experiment_index_record(experiment_run=experiment_run)
    record_experiment_run(index_record, artifacts=artifact_paths)
    mlflow_run_id = log_experiment_run_to_mlflow(
        research_task=research_task,
        experiment_run=experiment_run,
        experiment_name=DEFAULT_EXPERIMENT_NAME,
    )

    append_trace_event({
        'trace_id': 'trace-20260326-005',
        'span_id': 'run-20260326-005-span-01',
        'parent_span_id': '',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'research_executor',
        'step_code': 'task_intake',
        'step_label': '通过 project_mcp 读取基线与委托',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'baseline_experiment', 'artifact_path': baseline_index['artifact_root']}],
        'memory_refs': [],
        'metric_refs': [],
        'tags': ['project_mcp', 'baseline'],
        'notes': baseline_index['experiment_id'],
    })
    append_trace_event({
        'trace_id': 'trace-20260326-005',
        'span_id': 'run-20260326-005-span-02',
        'parent_span_id': 'run-20260326-005-span-01',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'research_executor',
        'step_code': 'rule_expression',
        'step_label': '形成 MA60 过滤器变体规则',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'inputs', 'artifact_path': artifact_paths['inputs']}],
        'memory_refs': [],
        'metric_refs': [],
        'tags': ['entry_filter', 'ma60'],
        'notes': 'single change only',
    })
    append_trace_event({
        'trace_id': 'trace-20260326-005',
        'span_id': 'run-20260326-005-span-03',
        'parent_span_id': 'run-20260326-005-span-02',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'research_executor',
        'step_code': 'backtest_run',
        'step_label': '完成变体回测并写入 MLflow',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'results', 'artifact_path': artifact_paths['results']}],
        'memory_refs': [],
        'metric_refs': ['metrics_summary'],
        'tags': ['vectorbt', 'mlflow'],
        'notes': mlflow_run_id,
    })
    append_trace_event({
        'trace_id': 'trace-20260326-005',
        'span_id': 'run-20260326-005-span-04',
        'parent_span_id': 'run-20260326-005-span-03',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'research_executor',
        'step_code': 'review_decision',
        'step_label': '形成交付复审的初步判断',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'note', 'artifact_path': str(memory_note_path)}],
        'memory_refs': [str(memory_note_path.relative_to(REPO_ROOT))],
        'metric_refs': ['metrics_summary'],
        'tags': ['pending_review'],
        'notes': judgement,
    })

    experiment_readback = tool_bus.call_tool('project_mcp', operation='get_experiment_run', experiment_id=experiment_id)
    trace_readback = tool_bus.call_tool('project_mcp', operation='get_trace_session', run_id=run_id)
    configure_mlflow_tracking()
    experiment = mlflow.get_experiment_by_name(DEFAULT_EXPERIMENT_NAME)
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.experiment_id = '{experiment_id}'",
    )

    memory_note_path.write_text(
        '\n'.join([
            f'# {title}',
            '',
            f'- 时间: {created_at}',
            f'- experiment_id: {experiment_id}',
            f'- run_id: {run_id}',
            f'- baseline_ref: {baseline_index["experiment_id"]}',
            '- single_change: 仅当收盘价位于60日均线之上时，20日突破信号才允许入场',
            '',
            '## 核心指标',
            '',
            f"- total_return: {variant_metrics['total_return']:.6f}",
            f"- annual_return: {variant_metrics['annual_return']:.6f}",
            f"- max_drawdown: {variant_metrics['max_drawdown']:.6f}",
            f"- sharpe: {variant_metrics['sharpe']:.6f}",
            f"- trade_count: {variant_metrics['trade_count']}",
            f"- win_rate: {variant_metrics['win_rate']:.6f}",
            '',
            '## 与基线比较',
            '',
            f'- total_return_delta: {total_return_delta:.6f}',
            f'- sharpe_delta: {sharpe_delta:.6f}',
            f'- max_drawdown_delta: {drawdown_delta:.6f}',
            f'- trade_count_delta: {trade_delta}',
            '',
            f'- judgement: {judgement}',
            f'- mlflow_run_count: {len(runs)}',
            f'- mcp_experiment_read_success: {experiment_readback["success"]}',
            f'- mcp_trace_read_success: {trace_readback["success"]}',
            f'- trace_event_count: {trace_readback["result"]["summary"]["event_count"]}',
        ]) + '\n',
        encoding='utf-8',
    )

    board_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / 'working_test_draft_board.md'
    board = board_path.read_text(encoding='utf-8')
    marker = '## 当前经验结论\n'
    entry = f'''### 2026-03-26 {experiment_id}\n- 题目：`{title}`\n- 进展：在基线突破规则上只增加一个 60 日均线入场过滤器，完成真实变体留存与回读\n- 当前判断：{judgement}\n- 当前补的主干能力：运行态三工位按新技术底座进行真实变体联调\n- 原始留存：[`runtime/experiments/{experiment_id}`](d:/AITradingSystem/runtime/experiments/{experiment_id})\n- 里程碑记录：[`{memory_note_path.name}`](d:/AITradingSystem/memory_v1/40_experience_base/{memory_note_path.name})\n\n'''
    if experiment_id not in board:
        board = board.replace(marker, entry + marker, 1)
    board_path.write_text(board, encoding='utf-8')

    blackbox_path = REPO_ROOT / 'memory_v1' / 'EXECUTION_BLACKBOX.md'
    with blackbox_path.open('a', encoding='utf-8') as fh:
        fh.write(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 跑 MA60 过滤器变体联调 | 验证新底座真实变体流程 | {experiment_id} 已写 experiments/MLflow/trace\n")

    print(json.dumps({
        'experiment_id': experiment_id,
        'run_id': run_id,
        'variant_metrics': variant_metrics,
        'baseline_metrics': baseline_metrics,
        'deltas': {
            'total_return_delta': total_return_delta,
            'sharpe_delta': sharpe_delta,
            'max_drawdown_delta': drawdown_delta,
            'trade_count_delta': trade_delta,
        },
        'artifact_root': str(artifact_root),
        'mlflow_run_count': int(len(runs)),
        'trace_event_count': int(trace_readback['result']['summary']['event_count']),
        'judgement': judgement,
    }, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()

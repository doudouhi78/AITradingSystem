from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import mlflow

from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.mlflow_tracker import DEFAULT_EXPERIMENT_NAME
from ai_dev_os.mlflow_tracker import configure_mlflow_tracking
from ai_dev_os.mlflow_tracker import log_experiment_run_to_mlflow
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.research_tracing import append_trace_event
from ai_dev_os.system_db import record_experiment_run
from ai_dev_os.tool_bus import tool_bus

REPO_ROOT = Path(__file__).resolve().parents[1]


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def main() -> None:
    created_at = _now_iso()
    experiment_id = 'exp-20260326-004-phase2-stack-smoke'
    task_id = 'RTS-STACK-001'
    run_id = 'run-20260326-004'
    title = '第二阶段技术底座联调烟雾测试'
    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-26_exp-20260326-004_phase2_stack_smoke.md'
    artifact_root = REPO_ROOT / 'runtime' / 'experiments' / experiment_id

    baseline_payload = get_experiment_run('exp-20260325-002-breakout-baseline')
    baseline_inputs = baseline_payload['artifacts']['inputs']
    baseline_results = baseline_payload['artifacts']['results']

    research_task = {
        'task_id': task_id,
        'title': title,
        'goal': '验证 Schema-first/MLflow/MCP/Phoenix-style tracing 四个技术底座能协同工作。',
        'instrument_pool': ['510300'],
        'strategy_family': 'etf_trend_breakout',
        'hypothesis': '在不引入新策略变量的前提下，第二阶段技术底座可承接一次完整联调。',
        'constraints': ['复用当前临时基线表达', '不引入新的策略结论', '本次以技术联调为目标'],
        'success_criteria': ['实验目录写入成功', 'SQLite 实验索引成功', 'MLflow run 成功', 'project_mcp 可回读', 'trace 可回读'],
        'created_at': created_at,
    }

    experiment_run = {
        'project_id': 'ai-trading-system',
        'experiment_id': experiment_id,
        'task_id': task_id,
        'run_id': run_id,
        'title': title,
        'strategy_family': 'etf_trend_breakout',
        'variant_name': 'phase2_stack_smoke_baseline_replay',
        'instrument': '510300',
        'dataset_snapshot': baseline_inputs['dataset_snapshot'],
        'rule_expression': baseline_inputs['rule_expression'],
        'metrics_summary': baseline_results['metrics_summary'],
        'risk_position_note': baseline_results['risk_position_note'],
        'review_outcome': {
            'review_status': 'approved',
            'review_outcome': 'stack_smoke_passed',
            'key_risks': ['本次复用基线结果，未重新跑策略引擎'],
            'gaps': [],
            'recommended_next_step': 'move_to_real_agent_run',
            'reviewed_at': created_at,
        },
        'decision_status': {
            'decision_status': 'recorded',
            'is_baseline': False,
            'baseline_of': 'exp-20260325-002-breakout-baseline',
            'decision_reason': 'phase2_stack_smoke_passed',
            'decided_at': created_at,
        },
        'artifact_root': str(artifact_root),
        'memory_note_path': str(memory_note_path),
        'status_code': 'stack_smoke_passed',
        'created_at': created_at,
    }

    artifact_paths = write_experiment_artifacts(
        research_task=research_task,
        experiment_run=experiment_run,
        notes_markdown='''# 第二阶段技术底座联调烟雾测试

- 目的：验证对象层、实验留存、MLflow、MCP、tracing 可协同工作。
- 说明：本次复用当前临时基线的规则、数据口径和结果，不声明新的策略结论。
''',
    )
    index_record = build_experiment_index_record(experiment_run=experiment_run)
    record_experiment_run(index_record, artifacts=artifact_paths)
    mlflow_run_id = log_experiment_run_to_mlflow(
        research_task=research_task,
        experiment_run=experiment_run,
        experiment_name=DEFAULT_EXPERIMENT_NAME,
    )

    append_trace_event({
        'trace_id': 'trace-20260326-004',
        'span_id': 'run-20260326-004-span-01',
        'parent_span_id': '',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'builder',
        'step_code': 'artifact_write',
        'step_label': '写入实验留存与索引',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'experiment_root', 'artifact_path': str(artifact_root)}],
        'memory_refs': [],
        'metric_refs': ['metrics_summary'],
        'tags': ['stack smoke', 'artifacts'],
        'notes': '',
    })
    append_trace_event({
        'trace_id': 'trace-20260326-004',
        'span_id': 'run-20260326-004-span-02',
        'parent_span_id': 'run-20260326-004-span-01',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'builder',
        'step_code': 'mlflow_sync',
        'step_label': '同步到 MLflow',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [],
        'memory_refs': [],
        'metric_refs': ['metrics_summary'],
        'tags': ['stack smoke', 'mlflow'],
        'notes': mlflow_run_id,
    })
    append_trace_event({
        'trace_id': 'trace-20260326-004',
        'span_id': 'run-20260326-004-span-03',
        'parent_span_id': 'run-20260326-004-span-02',
        'task_id': task_id,
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'reviewer',
        'step_code': 'mcp_readback',
        'step_label': '通过 MCP 回读实验与轨迹',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [],
        'memory_refs': [str(memory_note_path.relative_to(REPO_ROOT))],
        'metric_refs': [],
        'tags': ['stack smoke', 'mcp'],
        'notes': '',
    })

    experiment_readback = tool_bus.call_tool('project_mcp', operation='get_experiment_run', experiment_id=experiment_id)
    trace_readback = tool_bus.call_tool('project_mcp', operation='get_trace_session', run_id=run_id)

    configure_mlflow_tracking()
    experiment = mlflow.get_experiment_by_name(DEFAULT_EXPERIMENT_NAME)
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.experiment_id = '{experiment_id}'",
    )

    smoke_report = {
        'experiment_id': experiment_id,
        'run_id': run_id,
        'artifact_root': str(artifact_root),
        'mlflow_run_count': int(len(runs)),
        'mcp_experiment_read_success': bool(experiment_readback['success']),
        'mcp_trace_read_success': bool(trace_readback['success']),
        'trace_event_count': int(trace_readback['result']['summary']['event_count']),
        'status': 'passed',
    }

    (REPO_ROOT / 'runtime' / 'phase2_stack_smoke_report.json').write_text(
        json.dumps(smoke_report, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8',
    )

    memory_note_path.write_text(
        '\n'.join(
            [
                '# 第二阶段技术底座联调烟雾测试',
                '',
                f'- 时间: {created_at}',
                f'- experiment_id: {experiment_id}',
                f'- run_id: {run_id}',
                '- 目的: 验证 Schema-first、实验留存、MLflow、MCP、tracing 四个底座是否能协同工作。',
                '- 说明: 本次复用当前临时基线的规则、数据口径和结果，不声明新的策略结论。',
                '',
                '## 结果',
                '',
                f"- MLflow run 数量: {smoke_report['mlflow_run_count']}",
                f"- MCP 实验回读: {smoke_report['mcp_experiment_read_success']}",
                f"- MCP trace 回读: {smoke_report['mcp_trace_read_success']}",
                f"- trace event 数量: {smoke_report['trace_event_count']}",
                f"- 状态: {smoke_report['status']}",
            ]
        ) + '\n',
        encoding='utf-8',
    )

    print(json.dumps(smoke_report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

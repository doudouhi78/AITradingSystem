from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ai_dev_os.data_contracts import build_default_data_contract_spec
from ai_dev_os.data_contracts import validate_dataframe_against_contract
from ai_dev_os.etf_breakout_runtime import load_etf_history
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest
from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.mlflow_tracker import DEFAULT_EXPERIMENT_NAME
from ai_dev_os.mlflow_tracker import log_experiment_run_to_mlflow
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.research_tracing import append_trace_event
from ai_dev_os.system_db import record_experiment_run
from ai_dev_os.validation_store import write_validation_record
from ai_dev_os.variant_search import run_variant_search


REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_EXPERIMENT_ID = 'exp-20260325-002-breakout-baseline'


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def main() -> None:
    created_at = now_iso()
    baseline = get_experiment_run(BASELINE_EXPERIMENT_ID)
    index = baseline['index']
    artifacts = baseline['artifacts']
    research_task = artifacts['inputs']['research_task']
    dataset_snapshot = artifacts['inputs']['dataset_snapshot']
    rule_expression = artifacts['inputs']['rule_expression']
    metrics_summary = artifacts['results']['metrics_summary']
    risk_position_note = artifacts['results']['risk_position_note']
    review_outcome = artifacts['results']['review_outcome']
    execution_constraint = artifacts['results']['execution_constraint']
    opportunity_source = artifacts['inputs'].get('opportunity_source')

    instrument = index['instrument']
    date_start = index['date_range_start']
    date_end = index['date_range_end']
    case_file_id = artifacts['manifest'].get('case_file_id', '')

    df = load_etf_history(instrument, date_start, date_end)

    validation_id = 'VAL-20260328-001'
    contract = build_default_data_contract_spec(dataset_snapshot, created_at=created_at, warmup_rows=60)
    validation_record = validate_dataframe_against_contract(
        df,
        dataset_snapshot=dataset_snapshot,
        rule_expression=rule_expression,
        metrics_summary=metrics_summary,
        contract_spec=contract,
        validation_id=validation_id,
        experiment_id=BASELINE_EXPERIMENT_ID,
        task_id=research_task['task_id'],
        run_id=index['run_id'],
        title='510300 baseline data contract validation',
        created_at=created_at,
    )
    validation_path = write_validation_record(validation_record)

    search_spec = {
        'search_id': 'SEARCH-20260328-001',
        'title': '510300 breakout small variant search',
        'strategy_family': index['strategy_family'],
        'baseline_experiment_id': BASELINE_EXPERIMENT_ID,
        'objective_metric': 'sharpe',
        'objective_mode': 'maximize',
        'max_trials': 4,
        'parameter_space': {
            'entry_window': {'type': 'categorical', 'choices': [15, 25]},
            'exit_window': {'type': 'categorical', 'choices': [10, 30]},
            'ma_filter_window': {'type': 'categorical', 'choices': [0, 60]},
        },
        'constraints': ['single_instrument', 'small_search_space', 'same_data_contract'],
        'created_at': created_at,
    }

    def objective_fn(params: dict) -> float:
        metrics = run_breakout_backtest(
            df,
            entry_window=int(params['entry_window']),
            exit_window=int(params['exit_window']),
            ma_filter_window=None if int(params['ma_filter_window']) == 0 else int(params['ma_filter_window']),
            fees=0.001,
            slippage=0.0005,
        )
        return float(metrics['sharpe'])

    search_result = run_variant_search(search_spec, objective_fn=objective_fn)
    best_params = dict(search_result['best_params'])
    best_metrics = run_breakout_backtest(
        df,
        entry_window=int(best_params['entry_window']),
        exit_window=int(best_params['exit_window']),
        ma_filter_window=None if int(best_params['ma_filter_window']) == 0 else int(best_params['ma_filter_window']),
        fees=0.001,
        slippage=0.0005,
    )

    experiment_id = 'exp-20260328-006-optuna-candidate'
    run_id = 'run-20260328-006'
    title = '510300 小参数空间变体搜索候选'
    artifact_root = REPO_ROOT / 'runtime' / 'experiments' / experiment_id
    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-28_exp-20260328-006_optuna_candidate.md'
    memory_note_path.write_text(
        '\n'.join([
            f'# {title}',
            '',
            f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
            f'- search_id: {search_spec["search_id"]}',
            f'- validation_ref: {validation_id}',
            f'- params: {best_params}',
            f'- best_sharpe: {search_result["best_value"]:.6f}',
        ]) + '\n',
        encoding='utf-8',
    )

    candidate_experiment = {
        'project_id': 'ai-trading-system',
        'experiment_id': experiment_id,
        'task_id': research_task['task_id'],
        'run_id': run_id,
        'title': title,
        'strategy_family': index['strategy_family'],
        'variant_name': f"optuna_entry{best_params['entry_window']}_exit{best_params['exit_window']}_ma{best_params['ma_filter_window']}",
        'instrument': instrument,
        'dataset_snapshot': {**dataset_snapshot, 'created_at': created_at},
        'rule_expression': {
            'rules_version': f"optuna-{search_spec['search_id']}",
            'entry_rule_summary': f"收盘价突破前{best_params['entry_window']}日最高收盘价（不含当天）",
            'exit_rule_summary': f"收盘价跌破前{best_params['exit_window']}日最低收盘价（不含当天）",
            'filters': [] if int(best_params['ma_filter_window']) == 0 else [f"close > ma{best_params['ma_filter_window']}"] ,
            'execution_assumption': rule_expression['execution_assumption'],
            'created_at': created_at,
            'price_field': rule_expression.get('price_field', 'close/open'),
            'notes': [f"search_id={search_spec['search_id']}", f"best_params={best_params}"],
            'method_summary': '基于当前基线做小参数空间自动搜索。',
            'design_rationale': '用有限参数空间测试是否存在更值得继续的局部变体。',
        },
        'metrics_summary': {
            **best_metrics,
            'key_findings': [f"best_params={best_params}", f"baseline_sharpe={metrics_summary['sharpe']}"]
        },
        'risk_position_note': risk_position_note,
        'review_outcome': {
            'review_status': 'pending_review',
            'review_outcome': '待复审参数搜索候选',
            'key_risks': ['参数搜索可能引入局部过拟合'],
            'gaps': ['仅完成小参数空间搜索，尚未加入正式复审'],
            'recommended_next_step': 'formal_review',
            'reviewed_at': created_at,
            'judgement': '先作为候选变体保留，等待与基线做正式比较。',
            'review_method': 'Optuna 小参数空间搜索，目标指标为 Sharpe。',
            'review_reasoning': '搜索只负责生成候选，不负责决定晋升。',
        },
        'decision_status': {
            'decision_status': 'candidate_variant',
            'is_baseline': False,
            'baseline_of': BASELINE_EXPERIMENT_ID,
            'decision_reason': '由 Optuna 小参数空间搜索生成的待复审候选。',
            'decided_at': created_at,
        },
        'artifact_root': str(artifact_root),
        'memory_note_path': str(memory_note_path),
        'status_code': 'candidate_variant',
        'created_at': created_at,
        'opportunity_source': opportunity_source,
        'execution_constraint': execution_constraint,
        'case_file_id': case_file_id,
        'validation_record_ids': [validation_id],
        'search_spec_id': search_spec['search_id'],
    }

    notes_markdown = '\n'.join([
        f'# {title}',
        '',
        f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
        f'- validation_ref: {validation_id}',
        f'- search_id: {search_spec["search_id"]}',
        f'- params: {best_params}',
        f'- objective_metric: {search_spec["objective_metric"]}',
        f'- objective_value: {search_result["best_value"]:.6f}',
    ])

    artifact_paths = write_experiment_artifacts(
        research_task=research_task,
        experiment_run=candidate_experiment,
        notes_markdown=notes_markdown,
    )
    record_experiment_run(build_experiment_index_record(experiment_run=candidate_experiment), artifacts=artifact_paths)
    mlflow_run_id = log_experiment_run_to_mlflow(
        research_task=research_task,
        experiment_run=candidate_experiment,
        experiment_name=DEFAULT_EXPERIMENT_NAME,
    )

    append_trace_event(
        {
            'trace_id': 'trace-20260328-006',
            'span_id': 'run-20260328-006-span-01',
            'parent_span_id': '',
            'task_id': research_task['task_id'],
            'run_id': run_id,
            'experiment_id': experiment_id,
            'agent_role': 'research_executor',
            'step_code': 'data_contract_validation',
            'step_label': '基线数据契约校验',
            'event_kind': 'step',
            'status_code': validation_record['status_code'],
            'started_at': created_at,
            'finished_at': created_at,
            'duration_ms': 0,
            'artifact_refs': [{'artifact_kind': 'validation_record', 'artifact_path': validation_path}],
            'memory_refs': [],
            'metric_refs': [],
            'tags': ['pandera', 'data_contract'],
            'notes': validation_record['summary'],
        }
    )
    append_trace_event(
        {
            'trace_id': 'trace-20260328-006',
            'span_id': 'run-20260328-006-span-02',
            'parent_span_id': 'run-20260328-006-span-01',
            'task_id': research_task['task_id'],
            'run_id': run_id,
            'experiment_id': experiment_id,
            'agent_role': 'research_executor',
            'step_code': 'variant_search',
            'step_label': 'Optuna 小参数空间搜索',
            'event_kind': 'step',
            'status_code': 'completed',
            'started_at': created_at,
            'finished_at': created_at,
            'duration_ms': 0,
            'artifact_refs': [{'artifact_kind': 'experiment_artifact', 'artifact_path': str(artifact_root)}],
            'memory_refs': [str(memory_note_path)],
            'metric_refs': ['sharpe'],
            'tags': ['optuna', 'candidate_variant'],
            'notes': f"best_params={best_params}; mlflow_run_id={mlflow_run_id}",
        }
    )

    print(f'validation_record={validation_path}')
    print(f'candidate_experiment={experiment_id}')
    print(f'mlflow_run_id={mlflow_run_id}')


if __name__ == '__main__':
    main()

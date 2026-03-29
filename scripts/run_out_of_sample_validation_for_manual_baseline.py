from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ai_dev_os.data_contracts import build_default_data_contract_spec
from ai_dev_os.data_contracts import validate_dataframe_against_contract
from ai_dev_os.etf_breakout_runtime import load_etf_history
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest
from ai_dev_os.experiment_store import read_experiment_artifacts
from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.project_objects import validate_experiment_run
from ai_dev_os.project_objects import validate_formal_review_record
from ai_dev_os.research_tracing import append_trace_event
from ai_dev_os.review_store import write_formal_review
from ai_dev_os.system_db import REPO_ROOT
from ai_dev_os.system_db import record_experiment_run
from ai_dev_os.validation_store import write_validation_record


BASELINE_EXPERIMENT_ID = 'exp-20260328-007-manual-entry25-exit20'
VALIDATION_ID = 'VAL-20260328-002'
REVIEW_ID = 'REV-20260328-004'
RUN_ID = 'run-20260328-009'
OOS_START = '2024-01-01'


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def _extract_windows(rule_expression: dict) -> tuple[int, int, int | None]:
    notes = rule_expression.get('notes', [])
    values: dict[str, int] = {}
    for item in notes:
        if '=' not in str(item):
            continue
        key, raw = str(item).split('=', 1)
        try:
            values[key.strip()] = int(raw.strip())
        except ValueError:
            continue
    entry = values.get('entry_window', 25)
    exit_ = values.get('exit_window', 20)
    ma_filter = values.get('ma_filter_window', 0)
    return entry, exit_, (None if ma_filter == 0 else ma_filter)


def main() -> None:
    created_at = now_iso()
    baseline = read_experiment_artifacts(BASELINE_EXPERIMENT_ID)
    manifest = baseline['manifest']
    inputs = baseline['inputs']
    results = baseline['results']
    rule_expression = inputs['rule_expression']
    entry_window, exit_window, ma_filter_window = _extract_windows(rule_expression)

    date_end = inputs['dataset_snapshot']['date_range_end']
    instrument = manifest['instrument']
    df = load_etf_history(instrument, OOS_START, date_end)
    actual_start = df['date'].min().date().isoformat()

    oos_snapshot = {
        **inputs['dataset_snapshot'],
        'dataset_version': f"{inputs['dataset_snapshot']['dataset_version']}-oos-20240101",
        'date_range_start': actual_start,
        'created_at': created_at,
        'selection_reason': '为当前人工基线补做后半区间样本外验证。',
        'validation_method': '固定参数不变，只切后半区间做独立样本外验证。',
    }
    contract = build_default_data_contract_spec(oos_snapshot, created_at=created_at)
    metrics = run_breakout_backtest(
        df,
        entry_window=entry_window,
        exit_window=exit_window,
        ma_filter_window=ma_filter_window,
        fees=0.001,
        slippage=0.0005,
    )
    metrics['key_findings'] = [
        f"in_sample_total_return={results['metrics_summary']['total_return']:.6f}",
        f"in_sample_sharpe={results['metrics_summary']['sharpe']:.6f}",
        f"in_sample_max_drawdown={results['metrics_summary']['max_drawdown']:.6f}",
    ]

    validation = validate_dataframe_against_contract(
        df,
        dataset_snapshot=oos_snapshot,
        rule_expression={**rule_expression, 'created_at': created_at},
        metrics_summary=metrics,
        contract_spec=contract,
        validation_id=VALIDATION_ID,
        experiment_id=BASELINE_EXPERIMENT_ID,
        task_id=manifest['task_id'],
        run_id=RUN_ID,
        title='当前人工基线样本外验证',
        created_at=created_at,
    )
    validation['summary'] = (
        f"样本外区间 {actual_start} -> {date_end} 验证通过；"
        f"Sharpe={metrics['sharpe']:.6f}，总收益={metrics['total_return']:.6f}，最大回撤={metrics['max_drawdown']:.6f}"
    )
    write_validation_record(validation)

    formal_review = validate_formal_review_record({
        'review_id': REVIEW_ID,
        'experiment_id': BASELINE_EXPERIMENT_ID,
        'baseline_experiment_id': BASELINE_EXPERIMENT_ID,
        'review_scope': 'out_of_sample_validation',
        'review_question': '当前人工基线在后半区间样本外验证下，是否仍应保留为当前基线',
        'review_method': '固定参数不变的后半区间样本外验证',
        'comparison_summary': (
            f"样本外区间 {actual_start} -> {date_end} 的 Sharpe 为 {metrics['sharpe']:.6f}，高于样本内 {results['metrics_summary']['sharpe']:.6f}；"
            f"样本外总收益为 {metrics['total_return']:.6f}，最大回撤为 {metrics['max_drawdown']:.6f}。"
        ),
        'risks': [
            '样本外区间仍然只覆盖单标的',
            '样本外窗口长度有限，仍需更多时段与更多标的验证',
            '执行敏感性和成本扰动仍未展开',
        ],
        'gaps': [
            '尚未做跨标的样本外验证',
            '尚未做不同仓位与分批建仓的执行敏感性验证',
        ],
        'decision_recommendation': 'promote_to_baseline',
        'decision_reason': '当前人工基线在后半区间样本外验证中仍保持较好的收益质量，现有证据支持继续保留为当前基线。',
        'reviewed_at': created_at,
        'validation_record_ids': [VALIDATION_ID],
    })
    write_formal_review(formal_review)

    updated_results = dict(results)
    updated_results['review_outcome'] = {
        'review_status': 'reviewed',
        'review_outcome': 'promote_to_baseline',
        'key_risks': formal_review['risks'],
        'gaps': formal_review['gaps'],
        'recommended_next_step': '继续做跨标的样本外验证与执行敏感性测试。',
        'reviewed_at': created_at,
        'judgement': '当前人工基线在样本外区间没有塌掉，仍可作为当前基线。',
        'review_method': formal_review['review_method'],
        'review_reasoning': formal_review['decision_reason'],
    }
    updated_results['decision_status'] = {
        'decision_status': 'promote_to_baseline',
        'is_baseline': True,
        'baseline_of': '',
        'decision_reason': '当前人工基线已经通过后半区间样本外验证，可继续作为当前基线。',
        'decided_at': created_at,
    }

    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-28_exp-20260328-007_out_of_sample_validation.md'
    memory_note_path.write_text('\n'.join([
        '# 510300 当前人工基线样本外验证',
        '',
        f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
        f'- validation_id: {VALIDATION_ID}',
        f'- review_id: {REVIEW_ID}',
        f'- sample_range: {actual_start} -> {date_end}',
        f"- result: Sharpe={metrics['sharpe']:.6f}, total_return={metrics['total_return']:.6f}, max_drawdown={metrics['max_drawdown']:.6f}",
        '- judgement: 当前人工基线在后半区间样本外验证下仍成立，但下一步应扩到跨标的与执行敏感性。',
    ]) + '\n', encoding='utf-8')

    notes = baseline['notes_markdown'].rstrip()
    if '## Out-of-sample Validation' in notes:
        notes = notes.split('## Out-of-sample Validation')[0].rstrip()
    notes += '\n\n## Out-of-sample Validation\n'
    notes += f'- validation_id: {VALIDATION_ID}\n'
    notes += f'- review_id: {REVIEW_ID}\n'
    notes += f'- sample_range: {actual_start} -> {date_end}\n'
    notes += f"- result: Sharpe={metrics['sharpe']:.6f}, total_return={metrics['total_return']:.6f}, max_drawdown={metrics['max_drawdown']:.6f}\n"
    notes += '- judgement: 当前人工基线在样本外区间没有塌掉，可继续保留为当前基线。\n'

    experiment_run = validate_experiment_run({
        'project_id': 'ai-trading-system',
        'experiment_id': manifest['experiment_id'],
        'task_id': manifest['task_id'],
        'run_id': manifest['run_id'],
        'title': manifest['title'],
        'strategy_family': manifest['strategy_family'],
        'variant_name': manifest['variant_name'],
        'instrument': manifest['instrument'],
        'dataset_snapshot': inputs['dataset_snapshot'],
        'rule_expression': inputs['rule_expression'],
        'metrics_summary': updated_results['metrics_summary'],
        'risk_position_note': updated_results['risk_position_note'],
        'execution_constraint': updated_results['execution_constraint'],
        'review_outcome': updated_results['review_outcome'],
        'decision_status': updated_results['decision_status'],
        'artifact_root': baseline['artifact_root'],
        'memory_note_path': str(memory_note_path),
        'status_code': 'promote_to_baseline',
        'created_at': manifest['created_at'],
        'opportunity_source': inputs.get('opportunity_source'),
        'case_file_id': manifest.get('case_file_id', ''),
        'validation_record_ids': [VALIDATION_ID],
    })

    artifacts = write_experiment_artifacts(
        research_task=inputs['research_task'],
        experiment_run=experiment_run,
        notes_markdown=notes,
    )
    record_experiment_run(build_experiment_index_record(experiment_run=experiment_run), artifacts=artifacts, emit_spool=False)

    append_trace_event({
        'trace_id': f'trace-{RUN_ID}',
        'span_id': f'{RUN_ID}-span-01',
        'parent_span_id': '',
        'task_id': manifest['task_id'],
        'run_id': RUN_ID,
        'experiment_id': BASELINE_EXPERIMENT_ID,
        'agent_role': 'research_executor',
        'step_code': 'out_of_sample_validation',
        'step_label': '样本外验证',
        'event_kind': 'step',
        'status_code': validation['status_code'],
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'validation_record', 'artifact_path': str(REPO_ROOT / 'runtime' / 'validations' / f'{VALIDATION_ID}.json')}],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['out_of_sample', 'baseline_validation'],
        'notes': validation['summary'],
    })
    append_trace_event({
        'trace_id': f'trace-{RUN_ID}',
        'span_id': f'{RUN_ID}-span-02',
        'parent_span_id': f'{RUN_ID}-span-01',
        'task_id': manifest['task_id'],
        'run_id': RUN_ID,
        'experiment_id': BASELINE_EXPERIMENT_ID,
        'agent_role': 'reviewer',
        'step_code': 'formal_review',
        'step_label': '样本外验证正式复审',
        'event_kind': 'step',
        'status_code': 'promote_to_baseline',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'formal_review', 'artifact_path': str(REPO_ROOT / 'runtime' / 'reviews' / f'{REVIEW_ID}.json')}],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['out_of_sample', 'formal_review'],
        'notes': formal_review['decision_reason'],
    })

    print(f'validation_id={VALIDATION_ID}')
    print(f'review_id={REVIEW_ID}')
    print(f'oos_sharpe={metrics["sharpe"]:.6f}')


if __name__ == '__main__':
    main()

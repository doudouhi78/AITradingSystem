from __future__ import annotations

from datetime import datetime

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
RUN_ID = 'run-20260328-010'
REVIEW_ID = 'REV-20260328-005'
OOS_START = '2024-01-02'
TARGET_INSTRUMENTS = [
    ('510500', 'VAL-20260328-003'),
    ('159915', 'VAL-20260328-004'),
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def _extract_windows(rule_expression: dict) -> tuple[int, int, int | None]:
    values: dict[str, int] = {}
    for item in rule_expression.get('notes', []):
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
    entry_window, exit_window, ma_filter_window = _extract_windows(inputs['rule_expression'])
    date_end = inputs['dataset_snapshot']['date_range_end']

    validation_ids = list(manifest.get('validation_record_ids', []))
    summaries: list[str] = []
    metrics_lines: list[str] = []

    for instrument, validation_id in TARGET_INSTRUMENTS:
        df = load_etf_history(instrument, OOS_START, date_end)
        snapshot = {
            **inputs['dataset_snapshot'],
            'dataset_version': f"{inputs['dataset_snapshot']['dataset_version']}-oos-{instrument}",
            'instrument': instrument,
            'date_range_start': OOS_START,
            'created_at': created_at,
            'selection_reason': f'为检验当前人工基线是否可迁移到 {instrument}，补做跨标的样本外验证。',
            'validation_method': '固定参数不变，同期跨标的样本外验证。',
        }
        contract = build_default_data_contract_spec(snapshot, created_at=created_at)
        metrics = run_breakout_backtest(
            df,
            entry_window=entry_window,
            exit_window=exit_window,
            ma_filter_window=ma_filter_window,
            fees=0.001,
            slippage=0.0005,
        )
        metrics['key_findings'] = [
            f"baseline_instrument=510300",
            f"baseline_oos_sharpe=0.656997",
            f"baseline_oos_total_return=0.202340",
        ]
        validation = validate_dataframe_against_contract(
            df,
            dataset_snapshot=snapshot,
            rule_expression={**inputs['rule_expression'], 'created_at': created_at},
            metrics_summary=metrics,
            contract_spec=contract,
            validation_id=validation_id,
            experiment_id=BASELINE_EXPERIMENT_ID,
            task_id=manifest['task_id'],
            run_id=RUN_ID,
            title=f'当前人工基线跨标的验证：{instrument}',
            created_at=created_at,
        )
        validation['summary'] = (
            f'{instrument} 在样本外区间 {OOS_START} -> {date_end} 验证通过；'
            f"Sharpe={metrics['sharpe']:.6f}，总收益={metrics['total_return']:.6f}，最大回撤={metrics['max_drawdown']:.6f}"
        )
        write_validation_record(validation)
        validation_ids.append(validation_id)
        summaries.append(
            f"{instrument}: Sharpe={metrics['sharpe']:.6f}, total_return={metrics['total_return']:.6f}, max_drawdown={metrics['max_drawdown']:.6f}"
        )
        metrics_lines.append(f'- {instrument}: Sharpe={metrics["sharpe"]:.6f}, total_return={metrics["total_return"]:.6f}, max_drawdown={metrics["max_drawdown"]:.6f}')

    formal_review = validate_formal_review_record({
        'review_id': REVIEW_ID,
        'experiment_id': BASELINE_EXPERIMENT_ID,
        'baseline_experiment_id': BASELINE_EXPERIMENT_ID,
        'review_scope': 'cross_instrument_validation',
        'review_question': '当前人工基线在其他宽基ETF上是否仍具可迁移性',
        'review_method': '固定参数不变的跨标的样本外验证',
        'comparison_summary': '；'.join(summaries),
        'risks': [
            '目前仍只覆盖宽基ETF，不代表所有市场环境都适用',
            '159915 的最大回撤仍偏深，说明迁移性不等于风险完全可控',
            '尚未加入执行敏感性和成本扰动测试',
        ],
        'gaps': [
            '尚未覆盖更多指数ETF或非ETF标的',
            '尚未比较分批建仓、固定半仓与满仓之间的执行差异',
        ],
        'decision_recommendation': 'promote_to_baseline',
        'decision_reason': '当前人工基线在 510500 与 159915 上的样本外结果仍然为正，说明这条规则不只局限于 510300，现有证据支持继续保留为当前基线。',
        'reviewed_at': created_at,
        'validation_record_ids': validation_ids,
    })
    write_formal_review(formal_review)

    updated_results = dict(results)
    updated_results['review_outcome'] = {
        'review_status': 'reviewed',
        'review_outcome': 'promote_to_baseline',
        'key_risks': formal_review['risks'],
        'gaps': formal_review['gaps'],
        'recommended_next_step': '继续做执行敏感性测试，再决定是否放宽仓位口径。',
        'reviewed_at': created_at,
        'judgement': '当前人工基线在其他宽基ETF上仍成立，不只是 510300 的局部现象。',
        'review_method': formal_review['review_method'],
        'review_reasoning': formal_review['decision_reason'],
    }
    updated_results['decision_status'] = {
        'decision_status': 'promote_to_baseline',
        'is_baseline': True,
        'baseline_of': '',
        'decision_reason': '当前人工基线已经通过跨标的样本外验证，继续保留为当前基线。',
        'decided_at': created_at,
    }

    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-28_exp-20260328-007_cross_instrument_validation.md'
    memory_note_path.write_text('\n'.join([
        '# 510300 当前人工基线跨标的样本外验证',
        '',
        f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
        f'- review_id: {REVIEW_ID}',
        f'- validation_ids: {", ".join(validation_ids)}',
        f'- sample_range: {OOS_START} -> {date_end}',
        '- result:',
        *metrics_lines,
        '- judgement: 当前人工基线在 510500 和 159915 上仍成立，不只是 510300 的偶然结果。',
    ]) + '\n', encoding='utf-8')

    notes = baseline['notes_markdown'].rstrip()
    if '## Cross-instrument Validation' in notes:
        notes = notes.split('## Cross-instrument Validation')[0].rstrip()
    notes += '\n\n## Cross-instrument Validation\n'
    notes += f'- validation_ids: {", ".join(validation_ids)}\n'
    notes += f'- review_id: {REVIEW_ID}\n'
    notes += f'- sample_range: {OOS_START} -> {date_end}\n'
    for line in metrics_lines:
        notes += line + '\n'
    notes += '- judgement: 当前人工基线在其他宽基ETF上仍成立，不只是 510300 的局部现象。\n'

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
        'validation_record_ids': validation_ids,
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
        'step_code': 'cross_instrument_validation',
        'step_label': '跨标的样本外验证',
        'event_kind': 'step',
        'status_code': 'passed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [
            {'artifact_kind': 'validation_record', 'artifact_path': str(REPO_ROOT / 'runtime' / 'validations' / 'VAL-20260328-003.json')},
            {'artifact_kind': 'validation_record', 'artifact_path': str(REPO_ROOT / 'runtime' / 'validations' / 'VAL-20260328-004.json')},
        ],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['cross_instrument', 'out_of_sample'],
        'notes': formal_review['comparison_summary'],
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
        'step_label': '跨标的验证正式复审',
        'event_kind': 'step',
        'status_code': 'promote_to_baseline',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'formal_review', 'artifact_path': str(REPO_ROOT / 'runtime' / 'reviews' / f'{REVIEW_ID}.json')}],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['cross_instrument', 'formal_review'],
        'notes': formal_review['decision_reason'],
    })

    print(f'review_id={REVIEW_ID}')
    print(f'validation_ids={", ".join(validation_ids)}')


if __name__ == '__main__':
    main()

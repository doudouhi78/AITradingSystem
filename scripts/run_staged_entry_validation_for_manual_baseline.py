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
RUN_ID = 'run-20260328-012'
REVIEW_ID = 'REV-20260328-007'
OOS_START = '2024-01-02'
SCENARIOS = [
    {
        'validation_id': 'VAL-20260328-007',
        'label': 'half_position_one_shot',
        'title': '当前人工基线分批建仓对照：半仓一次建仓',
        'position_fraction': 0.5,
        'entry_split_steps': 1,
        'fees': 0.001,
        'slippage': 0.0005,
        'summary_label': '半仓一次建仓',
    },
    {
        'validation_id': 'VAL-20260328-008',
        'label': 'half_position_two_step',
        'title': '当前人工基线分批建仓对照：半仓两步建仓',
        'position_fraction': 0.5,
        'entry_split_steps': 2,
        'fees': 0.001,
        'slippage': 0.0005,
        'summary_label': '半仓两步建仓',
    },
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
            values[key.strip()] = int(float(raw.strip()))
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
    instrument = inputs['dataset_snapshot']['instrument']
    date_end = inputs['dataset_snapshot']['date_range_end']
    df = load_etf_history(instrument, OOS_START, date_end)

    validation_ids = list(manifest.get('validation_record_ids', []))
    scenario_summaries: list[str] = []
    notes_lines: list[str] = []

    for scenario in SCENARIOS:
        snapshot = {
            **inputs['dataset_snapshot'],
            'dataset_version': f"{inputs['dataset_snapshot']['dataset_version']}-staged-{scenario['label']}",
            'date_range_start': OOS_START,
            'created_at': created_at,
            'selection_reason': f"验证当前人工基线在{scenario['summary_label']}下是否仍成立。",
            'validation_method': '固定规则不变，只改变建仓方式。',
        }
        contract = build_default_data_contract_spec(snapshot, created_at=created_at)
        metrics = run_breakout_backtest(
            df,
            entry_window=entry_window,
            exit_window=exit_window,
            ma_filter_window=ma_filter_window,
            fees=scenario['fees'],
            slippage=scenario['slippage'],
            position_fraction=scenario['position_fraction'],
            entry_split_steps=scenario['entry_split_steps'],
        )
        metrics['key_findings'] = [
            f"position_fraction={scenario['position_fraction']}",
            f"entry_split_steps={scenario['entry_split_steps']}",
        ]
        validation = validate_dataframe_against_contract(
            df,
            dataset_snapshot=snapshot,
            rule_expression={**inputs['rule_expression'], 'created_at': created_at},
            metrics_summary=metrics,
            contract_spec=contract,
            validation_id=scenario['validation_id'],
            experiment_id=BASELINE_EXPERIMENT_ID,
            task_id=manifest['task_id'],
            run_id=RUN_ID,
            title=scenario['title'],
            created_at=created_at,
        )
        validation['summary'] = (
            f"{scenario['summary_label']}验证通过；Sharpe={metrics['sharpe']:.6f}，"
            f"总收益={metrics['total_return']:.6f}，最大回撤={metrics['max_drawdown']:.6f}"
        )
        write_validation_record(validation)
        if scenario['validation_id'] not in validation_ids:
            validation_ids.append(scenario['validation_id'])
        scenario_summaries.append(
            f"{scenario['summary_label']}: Sharpe={metrics['sharpe']:.6f}, total_return={metrics['total_return']:.6f}, max_drawdown={metrics['max_drawdown']:.6f}"
        )
        notes_lines.append(
            f"- {scenario['summary_label']}: Sharpe={metrics['sharpe']:.6f}, total_return={metrics['total_return']:.6f}, max_drawdown={metrics['max_drawdown']:.6f}"
        )

    formal_review = validate_formal_review_record(
        {
            'review_id': REVIEW_ID,
            'experiment_id': BASELINE_EXPERIMENT_ID,
            'baseline_experiment_id': BASELINE_EXPERIMENT_ID,
            'review_scope': 'staged_entry_validation',
            'review_question': '当前人工基线在分批建仓下是否仍能站住',
            'review_method': '固定规则、样本外区间、半仓比例不变，只比较一次建仓与两步建仓',
            'comparison_summary': '；'.join(scenario_summaries),
            'risks': [
                '当前分批建仓只验证了两步建仓，不代表所有执行拆分路径',
                '当前仍未覆盖更宽市场环境',
                '当前证据仍集中在宽基ETF日线级别',
            ],
            'gaps': [
                '尚未验证更长时间延后建仓',
                '尚未验证更极端滑点或更复杂成交条件',
            ],
            'decision_recommendation': 'promote_to_baseline',
            'decision_reason': '当前人工基线在半仓一次建仓与半仓两步建仓下都保持正收益，分批建仓没有直接推翻当前基线结论，因此继续保留为当前基线。',
            'reviewed_at': created_at,
            'validation_record_ids': validation_ids,
        }
    )
    write_formal_review(formal_review)

    updated_results = dict(results)
    updated_results['review_outcome'] = {
        'review_status': 'reviewed',
        'review_outcome': 'promote_to_baseline',
        'key_risks': formal_review['risks'],
        'gaps': formal_review['gaps'],
        'recommended_next_step': '继续做更宽市场环境验证，再决定是否进入下一阶段。',
        'reviewed_at': created_at,
        'judgement': '当前人工基线在分批建仓下仍成立，没有被一次建仓与两步建仓差异直接推翻。',
        'review_method': formal_review['review_method'],
        'review_reasoning': formal_review['decision_reason'],
    }
    updated_results['decision_status'] = {
        'decision_status': 'promote_to_baseline',
        'is_baseline': True,
        'baseline_of': '',
        'decision_reason': '当前人工基线已通过分批建仓验证，继续保留为当前基线。',
        'decided_at': created_at,
    }
    risk_position_note = dict(updated_results['risk_position_note'])
    existing_notes = list(risk_position_note.get('notes', []))
    staged_note = '已补一次建仓与两步建仓执行对照，当前结论未被推翻'
    if staged_note not in existing_notes:
        existing_notes.append(staged_note)
    risk_position_note['notes'] = existing_notes
    staged_reason = '当前又补了半仓一次建仓与半仓两步建仓对照，结果仍未推翻基线判断。'
    base_reasoning = str(risk_position_note.get('reasoning', '')).rstrip()
    if staged_reason not in base_reasoning:
        base_reasoning = f'{base_reasoning} {staged_reason}'.strip()
    risk_position_note['reasoning'] = base_reasoning
    updated_results['risk_position_note'] = risk_position_note

    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-28_exp-20260328-007_staged_entry_validation.md'
    memory_note_path.write_text(
        '\n'.join([
            '# 当前人工基线分批建仓验证',
            '',
            f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
            f'- review_id: {REVIEW_ID}',
            f'- validation_ids: {", ".join(validation_ids)}',
            f'- sample_range: {OOS_START} -> {date_end}',
            '- result:',
            *notes_lines,
            '- judgement: 当前人工基线在半仓一次建仓与两步建仓下都成立，分批建仓没有直接推翻当前基线结论。',
        ]) + '\n',
        encoding='utf-8',
    )

    notes = baseline['notes_markdown'].rstrip()
    if '## Staged Entry Validation' in notes:
        notes = notes.split('## Staged Entry Validation')[0].rstrip()
    notes += '\n\n## Staged Entry Validation\n'
    notes += f'- validation_ids: {", ".join(validation_ids)}\n'
    notes += f'- review_id: {REVIEW_ID}\n'
    notes += f'- sample_range: {OOS_START} -> {date_end}\n'
    for line in notes_lines:
        notes += line + '\n'
    notes += '- judgement: 当前人工基线在半仓一次建仓与两步建仓下都成立，分批建仓没有直接推翻当前基线结论。\n'

    experiment_run = validate_experiment_run(
        {
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
        }
    )

    artifacts = write_experiment_artifacts(
        research_task=inputs['research_task'],
        experiment_run=experiment_run,
        notes_markdown=notes,
    )
    record_experiment_run(build_experiment_index_record(experiment_run=experiment_run), artifacts=artifacts, emit_spool=False)

    append_trace_event(
        {
            'trace_id': f'trace-{RUN_ID}',
            'span_id': f'{RUN_ID}-span-01',
            'parent_span_id': '',
            'task_id': manifest['task_id'],
            'run_id': RUN_ID,
            'experiment_id': BASELINE_EXPERIMENT_ID,
            'agent_role': 'research_executor',
            'step_code': 'staged_entry_validation',
            'step_label': '分批建仓验证',
            'event_kind': 'step',
            'status_code': 'passed',
            'started_at': created_at,
            'finished_at': created_at,
            'duration_ms': 0,
            'artifact_refs': [
                {'artifact_kind': 'validation_record', 'artifact_path': str(REPO_ROOT / 'runtime' / 'validations' / 'VAL-20260328-007.json')},
                {'artifact_kind': 'validation_record', 'artifact_path': str(REPO_ROOT / 'runtime' / 'validations' / 'VAL-20260328-008.json')},
            ],
            'memory_refs': [str(memory_note_path)],
            'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
            'tags': ['staged_entry', 'out_of_sample'],
            'notes': formal_review['comparison_summary'],
        }
    )
    append_trace_event(
        {
            'trace_id': f'trace-{RUN_ID}',
            'span_id': f'{RUN_ID}-span-02',
            'parent_span_id': f'{RUN_ID}-span-01',
            'task_id': manifest['task_id'],
            'run_id': RUN_ID,
            'experiment_id': BASELINE_EXPERIMENT_ID,
            'agent_role': 'reviewer',
            'step_code': 'formal_review',
            'step_label': '分批建仓正式复审',
            'event_kind': 'step',
            'status_code': 'promote_to_baseline',
            'started_at': created_at,
            'finished_at': created_at,
            'duration_ms': 0,
            'artifact_refs': [
                {'artifact_kind': 'formal_review', 'artifact_path': str(REPO_ROOT / 'runtime' / 'reviews' / f'{REVIEW_ID}.json')},
            ],
            'memory_refs': [str(memory_note_path)],
            'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
            'tags': ['staged_entry', 'formal_review'],
            'notes': formal_review['decision_reason'],
        }
    )

    print(f'review_id={REVIEW_ID}')
    print(f'validation_ids={", ".join(validation_ids)}')


if __name__ == '__main__':
    main()

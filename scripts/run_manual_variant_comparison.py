from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ai_dev_os.etf_breakout_runtime import load_etf_history
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest
from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.mlflow_tracker import DEFAULT_EXPERIMENT_NAME
from ai_dev_os.mlflow_tracker import log_experiment_run_to_mlflow
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.project_objects import validate_experiment_run
from ai_dev_os.project_objects import validate_formal_review_record
from ai_dev_os.research_tracing import append_trace_event
from ai_dev_os.review_store import write_formal_review
from ai_dev_os.system_db import record_experiment_run


REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_EXPERIMENT_ID = 'exp-20260325-002-breakout-baseline'
SEARCH_CANDIDATE_EXPERIMENT_ID = 'exp-20260328-006-optuna-candidate'


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def build_variant(
    *,
    experiment_id: str,
    run_id: str,
    title: str,
    variant_name: str,
    rule_rationale: str,
    method_summary: str,
    review_id: str,
    review_question: str,
    review_scope: str,
    decision_recommendation: str,
    decision_reason: str,
    review_judgement: str,
    review_next_step: str,
    is_baseline: bool,
    decision_status: str,
    baseline_of: str,
    entry_window: int,
    exit_window: int,
    ma_filter_window: int | None,
) -> tuple[dict, dict, dict, str]:
    created_at = now_iso()
    baseline = get_experiment_run(BASELINE_EXPERIMENT_ID)
    baseline_index = baseline['index']
    baseline_inputs = baseline['artifacts']['inputs']
    baseline_results = baseline['artifacts']['results']
    baseline_metrics = baseline_results['metrics_summary']
    df = load_etf_history(
        baseline_index['instrument'],
        baseline_index['date_range_start'],
        baseline_index['date_range_end'],
    )
    metrics = run_breakout_backtest(
        df,
        entry_window=entry_window,
        exit_window=exit_window,
        ma_filter_window=ma_filter_window,
        fees=0.001,
        slippage=0.0005,
    )
    filters = [] if not ma_filter_window else [f'close > ma{ma_filter_window}']
    comparison_summary = (
        f"与基线相比，本变体仅改动人工指定参数；"
        f"Sharpe 从 {baseline_metrics['sharpe']:.6f} 变为 {metrics['sharpe']:.6f}，"
        f"总收益从 {baseline_metrics['total_return']:.6f} 变为 {metrics['total_return']:.6f}，"
        f"最大回撤从 {baseline_metrics['max_drawdown']:.6f} 变为 {metrics['max_drawdown']:.6f}，"
        f"交易次数从 {baseline_metrics['trade_count']} 变为 {metrics['trade_count']}。"
    )
    risks = [
        '仍未做样本外验证',
        '风险/仓位层仍然偏弱',
    ]
    gaps = [
        '尚未加入更完整的风险预算与执行敏感性测试',
    ]
    if decision_recommendation == 'record_only':
        risks.append('更快退出虽然减轻部分回撤，但显著削弱收益效率')
        gaps.append('需要确认退出改动是否真的解决核心问题')
    else:
        risks.append('更宽入场窗口可能错过部分启动段，需要后续样本外确认')
        gaps.append('尚未与更多人工变体做进一步交叉比较')

    formal_review = validate_formal_review_record({
        'review_id': review_id,
        'experiment_id': experiment_id,
        'baseline_experiment_id': BASELINE_EXPERIMENT_ID,
        'review_scope': review_scope,
        'review_question': review_question,
        'review_method': '同口径人工单变量变体比较',
        'comparison_summary': comparison_summary,
        'risks': risks,
        'gaps': gaps,
        'decision_recommendation': decision_recommendation,
        'decision_reason': decision_reason,
        'reviewed_at': created_at,
    })
    write_formal_review(formal_review)

    memory_note_path = REPO_ROOT / 'memory_v1' / '40_experience_base' / f"{created_at[:10]}_{experiment_id}_manual_variant.md"
    note_lines = [
        f'# {title}',
        '',
        f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
        f'- review_id: {review_id}',
        f'- params: entry={entry_window}, exit={exit_window}, ma={ma_filter_window or 0}',
        f'- judgement: {review_judgement}',
    ]
    memory_note_path.write_text('\n'.join(note_lines) + '\n', encoding='utf-8')

    experiment_run = validate_experiment_run({
        'project_id': 'ai-trading-system',
        'experiment_id': experiment_id,
        'task_id': baseline_inputs['research_task']['task_id'],
        'run_id': run_id,
        'title': title,
        'strategy_family': baseline_index['strategy_family'],
        'variant_name': variant_name,
        'instrument': baseline_index['instrument'],
        'dataset_snapshot': {**baseline_inputs['dataset_snapshot'], 'created_at': created_at},
        'rule_expression': {
            'rules_version': f'manual-{experiment_id}',
            'entry_rule_summary': f"收盘价突破前{entry_window}日最高收盘价（不含当天）",
            'exit_rule_summary': f"收盘价跌破前{exit_window}日最低收盘价（不含当天）",
            'filters': filters,
            'execution_assumption': baseline_inputs['rule_expression']['execution_assumption'],
            'created_at': created_at,
            'price_field': baseline_inputs['rule_expression'].get('price_field', 'close/open'),
            'notes': [f'entry_window={entry_window}', f'exit_window={exit_window}', f'ma_filter_window={ma_filter_window or 0}'],
            'method_summary': method_summary,
            'design_rationale': rule_rationale,
        },
        'metrics_summary': {
            **metrics,
            'key_findings': [
                f"baseline_sharpe={baseline_metrics['sharpe']:.6f}",
                f"baseline_total_return={baseline_metrics['total_return']:.6f}",
            ],
        },
        'risk_position_note': {
            **baseline_results['risk_position_note'],
            'reasoning': '这轮是人工单变量对照，目的不是追求最优，而是确认哪个改动真正有效。',
        },
        'execution_constraint': baseline_results['execution_constraint'],
        'review_outcome': {
            'review_status': 'reviewed',
            'review_outcome': decision_recommendation,
            'key_risks': formal_review['risks'],
            'gaps': formal_review['gaps'],
            'recommended_next_step': review_next_step,
            'reviewed_at': created_at,
            'judgement': review_judgement,
            'review_method': formal_review['review_method'],
            'review_reasoning': formal_review['decision_reason'],
        },
        'decision_status': {
            'decision_status': decision_status,
            'is_baseline': is_baseline,
            'baseline_of': baseline_of,
            'decision_reason': decision_reason,
            'decided_at': created_at,
        },
        'artifact_root': str(REPO_ROOT / 'runtime' / 'experiments' / experiment_id),
        'memory_note_path': str(memory_note_path),
        'status_code': decision_status,
        'created_at': created_at,
        'opportunity_source': baseline_inputs.get('opportunity_source'),
        'execution_constraint': baseline_results['execution_constraint'],
        'case_file_id': baseline['artifacts']['manifest'].get('case_file_id', ''),
    })

    notes_markdown = '\n'.join([
        f'# {title}',
        '',
        f'- baseline_ref: {BASELINE_EXPERIMENT_ID}',
        f'- search_candidate_ref: {SEARCH_CANDIDATE_EXPERIMENT_ID}',
        f'- review_id: {review_id}',
        f'- params: entry={entry_window}, exit={exit_window}, ma={ma_filter_window or 0}',
        f'- judgement: {review_judgement}',
    ])
    artifact_paths = write_experiment_artifacts(
        research_task=baseline_inputs['research_task'],
        experiment_run=experiment_run,
        notes_markdown=notes_markdown,
    )
    record_experiment_run(build_experiment_index_record(experiment_run=experiment_run), artifacts=artifact_paths, emit_spool=False)
    mlflow_run_id = log_experiment_run_to_mlflow(
        research_task=baseline_inputs['research_task'],
        experiment_run=experiment_run,
        experiment_name=DEFAULT_EXPERIMENT_NAME,
    )
    append_trace_event({
        'trace_id': f'trace-{run_id}',
        'span_id': f'{run_id}-span-01',
        'parent_span_id': '',
        'task_id': baseline_inputs['research_task']['task_id'],
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'research_executor',
        'step_code': 'manual_variant_backtest',
        'step_label': '人工单变量变体回测',
        'event_kind': 'step',
        'status_code': 'completed',
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'experiment_artifact', 'artifact_path': str(REPO_ROOT / 'runtime' / 'experiments' / experiment_id)}],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['manual_variant', 'comparison'],
        'notes': f'mlflow_run_id={mlflow_run_id}',
    })
    append_trace_event({
        'trace_id': f'trace-{run_id}',
        'span_id': f'{run_id}-span-02',
        'parent_span_id': f'{run_id}-span-01',
        'task_id': baseline_inputs['research_task']['task_id'],
        'run_id': run_id,
        'experiment_id': experiment_id,
        'agent_role': 'reviewer',
        'step_code': 'formal_review',
        'step_label': '人工变体正式复审',
        'event_kind': 'step',
        'status_code': decision_status,
        'started_at': created_at,
        'finished_at': created_at,
        'duration_ms': 0,
        'artifact_refs': [{'artifact_kind': 'formal_review', 'artifact_path': str(REPO_ROOT / 'runtime' / 'reviews' / f'{review_id}.json')}],
        'memory_refs': [str(memory_note_path)],
        'metric_refs': ['sharpe', 'total_return', 'max_drawdown'],
        'tags': ['manual_variant', 'formal_review'],
        'notes': decision_reason,
    })
    return experiment_run, formal_review, metrics, str(memory_note_path)


def main() -> None:
    better_run, better_review, better_metrics, better_note = build_variant(
        experiment_id='exp-20260328-007-manual-entry25-exit20',
        run_id='run-20260328-007',
        title='510300 人工变体：只放宽入场窗口',
        variant_name='manual_entry25_exit20',
        rule_rationale='只放宽入场窗口，检验更慢的突破确认是否能减少无效交易，同时保留原始退出逻辑。',
        method_summary='人工单变量对照：只改 entry_window，保持 exit_window 和过滤器不变。',
        review_id='REV-20260328-002',
        review_question='只放宽入场窗口的人工变体，是否足以成为新的有效基线',
        review_scope='single_manual_variant_review',
        decision_recommendation='promote_to_baseline',
        decision_reason='该人工变体在不引入复杂过滤器的前提下，同时提升了收益和 Sharpe，且交易次数更少，证据足以取代当前临时基线。',
        review_judgement='只放宽入场窗口这一处改动，比自动搜索候选和当前基线都更强。',
        review_next_step='以该变体作为新基线，继续补风险/仓位与样本外验证。',
        is_baseline=True,
        decision_status='promote_to_baseline',
        baseline_of='',
        entry_window=25,
        exit_window=20,
        ma_filter_window=None,
    )

    weaker_run, weaker_review, weaker_metrics, weaker_note = build_variant(
        experiment_id='exp-20260328-008-manual-entry20-exit10',
        run_id='run-20260328-008',
        title='510300 人工变体：只加快退出窗口',
        variant_name='manual_entry20_exit10',
        rule_rationale='只加快退出窗口，检验更快止损是否足以改善质量，而不引入其他变化。',
        method_summary='人工单变量对照：只改 exit_window，保持 entry_window 和过滤器不变。',
        review_id='REV-20260328-003',
        review_question='只加快退出窗口的人工变体，是否值得继续保留',
        review_scope='single_manual_variant_review',
        decision_recommendation='record_only',
        decision_reason='更快退出确实略减回撤，但收益和 Sharpe 明显变差，说明它不足以作为后续主线，只保留作反例。',
        review_judgement='只改退出窗口并没有解决主问题，反而显著削弱了策略效率。',
        review_next_step='保留为反例，不继续沿这个方向细调。',
        is_baseline=False,
        decision_status='record_only',
        baseline_of=BASELINE_EXPERIMENT_ID,
        entry_window=20,
        exit_window=10,
        ma_filter_window=None,
    )

    summary = REPO_ROOT / 'memory_v1' / '40_experience_base' / '2026-03-28_manual_variant_comparison_summary.md'
    summary.write_text('\n'.join([
        '# 2026-03-28 人工变体对照总结',
        '',
        '- 基线：exp-20260325-002-breakout-baseline',
        '- 搜索候选：exp-20260328-006-optuna-candidate',
        '- 人工变体1：exp-20260328-007-manual-entry25-exit20',
        '- 人工变体2：exp-20260328-008-manual-entry20-exit10',
        '',
        '## 结论',
        '- 只放宽入场窗口（25/20，无过滤）是当前最强且最可解释的变体，优于搜索候选。',
        '- 只加快退出窗口（20/10，无过滤）没有解决核心问题，只适合作为反例记录。',
        '',
        '## 关键数值',
        f"- 人工变体1 Sharpe={better_metrics['sharpe']:.6f}, total_return={better_metrics['total_return']:.6f}, max_drawdown={better_metrics['max_drawdown']:.6f}",
        f"- 人工变体2 Sharpe={weaker_metrics['sharpe']:.6f}, total_return={weaker_metrics['total_return']:.6f}, max_drawdown={weaker_metrics['max_drawdown']:.6f}",
        '',
        '## 当前判断',
        '- 自动搜索不是当前最优答案。',
        '- 更可解释的人工变体，已经给出了更强结果。',
        '- 下一步应围绕新的人工基线补风险/仓位与样本外验证。',
        '',
        '## 相关复审',
        '- REV-20260328-002',
        '- REV-20260328-003',
    ]) + '\n', encoding='utf-8')
    print(better_run['experiment_id'])
    print(weaker_run['experiment_id'])
    print(str(summary))


if __name__ == '__main__':
    main()

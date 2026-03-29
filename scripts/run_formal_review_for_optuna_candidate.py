from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ai_dev_os.experiment_store import read_experiment_artifacts
from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.project_objects import validate_experiment_run
from ai_dev_os.project_objects import validate_formal_review_record
from ai_dev_os.review_store import write_formal_review
from ai_dev_os.search_store import read_search_spec
from ai_dev_os.system_db import record_experiment_run


def _local_now() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def main() -> None:
    candidate_id = 'exp-20260328-006-optuna-candidate'
    baseline_id = 'exp-20260325-002-breakout-baseline'
    review_id = 'REV-20260328-001'
    reviewed_at = _local_now()

    candidate = read_experiment_artifacts(candidate_id)
    baseline = read_experiment_artifacts(baseline_id)

    c_metrics = candidate['results']['metrics_summary']
    b_metrics = baseline['results']['metrics_summary']
    validation_ids = list(candidate['manifest'].get('validation_record_ids', []))
    search_id = str(candidate['manifest'].get('search_spec_id', '') or '')
    search_spec = read_search_spec(search_id) if search_id else None

    comparison_summary = (
        f"与基线相比，候选把入场窗口调到25、退出窗口调到10，并增加 MA60 过滤；"
        f"Sharpe 从 {b_metrics['sharpe']:.6f} 提升到 {c_metrics['sharpe']:.6f}，"
        f"最大回撤从 {b_metrics['max_drawdown']:.6f} 改善到 {c_metrics['max_drawdown']:.6f}，"
        f"但总收益从 {b_metrics['total_return']:.6f} 降到 {c_metrics['total_return']:.6f}，"
        f"交易次数从 {b_metrics['trade_count']} 增到 {c_metrics['trade_count']}。"
    )

    formal_review = validate_formal_review_record({
        'review_id': review_id,
        'experiment_id': candidate_id,
        'baseline_experiment_id': baseline_id,
        'review_scope': 'single_variant_review',
        'review_question': '该 Optuna 候选是否足以直接取代当前突破基线',
        'review_method': '同数据口径指标比较 + ValidationRecord + VariantSearchSpec 轻量单审',
        'comparison_summary': comparison_summary,
        'risks': [
            '候选来自小参数空间搜索，存在局部过拟合风险',
            '交易次数增加，真实执行摩擦可能放大',
            '当前仍未补更完整的风险/仓位控制'
        ],
        'gaps': [
            '只完成单次候选与单基线比较，尚未做人工非搜索变体对照',
            '未做样本外验证',
            '未补执行敏感性与风险预算测试'
        ],
        'decision_recommendation': 'keep_as_candidate',
        'decision_reason': '该候选改善了 Sharpe 与回撤，但收益略降，证据还不足以直接晋升新基线，先保留为正式候选。',
        'reviewed_at': reviewed_at,
        'validation_record_ids': validation_ids,
        'search_spec_id': search_spec['search_id'] if search_spec else '',
    })
    write_formal_review(formal_review)

    updated_results = dict(candidate['results'])
    updated_results['review_outcome'] = {
        'review_status': 'reviewed',
        'review_outcome': 'keep_as_candidate',
        'key_risks': formal_review['risks'],
        'gaps': formal_review['gaps'],
        'recommended_next_step': '继续做人工变体比较，并补风险/仓位与样本外验证',
        'reviewed_at': reviewed_at,
        'judgement': '候选改善了质量指标，但不足以越过当前基线。',
        'review_method': formal_review['review_method'],
        'review_reasoning': formal_review['decision_reason'],
    }
    updated_results['decision_status'] = {
        'decision_status': 'keep_as_candidate',
        'is_baseline': False,
        'baseline_of': baseline_id,
        'decision_reason': formal_review['decision_reason'],
        'decided_at': reviewed_at,
    }

    experiment_run = validate_experiment_run({
        'experiment_id': candidate['manifest']['experiment_id'],
        'task_id': candidate['manifest']['task_id'],
        'run_id': candidate['manifest']['run_id'],
        'title': candidate['manifest']['title'],
        'strategy_family': candidate['manifest']['strategy_family'],
        'variant_name': candidate['manifest']['variant_name'],
        'instrument': candidate['manifest']['instrument'],
        'case_file_id': candidate['manifest'].get('case_file_id', ''),
        'opportunity_source': candidate['inputs']['opportunity_source'],
        'dataset_snapshot': candidate['inputs']['dataset_snapshot'],
        'rule_expression': candidate['inputs']['rule_expression'],
        'metrics_summary': updated_results['metrics_summary'],
        'risk_position_note': updated_results['risk_position_note'],
        'execution_constraint': updated_results['execution_constraint'],
        'review_outcome': updated_results['review_outcome'],
        'decision_status': updated_results['decision_status'],
        'artifact_root': candidate['artifact_root'],
        'memory_note_path': 'memory_v1/40_experience_base/2026-03-28_exp-20260328-006_optuna_candidate.md',
        'status_code': 'keep_as_candidate',
        'created_at': candidate['manifest']['created_at'],
        'project_id': 'ai-trading-system',
        'validation_record_ids': validation_ids,
        'search_spec_id': search_spec['search_id'] if search_spec else '',
    })

    notes = candidate['notes_markdown'].rstrip() + '\n\n## Formal Review\n- review_id: REV-20260328-001\n- decision: keep_as_candidate\n- reason: Sharpe 与回撤改善，但收益略降，证据不足以直接晋升基线。\n'
    artifacts = write_experiment_artifacts(
        research_task=candidate['inputs']['research_task'],
        experiment_run=experiment_run,
        notes_markdown=notes,
    )
    record_experiment_run(build_experiment_index_record(experiment_run=experiment_run), artifacts=artifacts, emit_spool=False)


if __name__ == '__main__':
    main()

import json

from ai_dev_os import experiment_store


def sample_research_task() -> dict:
    return {
        'task_id': 'RTS-TEST-002',
        'title': '研究任务',
        'goal': '验证实验目录写入',
        'instrument_pool': ['510300'],
        'strategy_family': 'etf_trend_breakout',
        'hypothesis': '突破表达可写入标准工件。',
        'constraints': ['最小样本'],
        'success_criteria': ['四个文件都写出'],
        'created_at': '2026-03-25T16:16:00+08:00',
        'opportunity_id': 'OPP-TEST-002',
        'case_file_id': 'CASE-TEST-002',
    }



def sample_experiment_run(root: str) -> dict:
    return {
        'experiment_id': 'exp-test-002',
        'task_id': 'RTS-TEST-002',
        'run_id': 'run-test-002',
        'title': '实验目录写入测试',
        'strategy_family': 'etf_trend_breakout',
        'variant_name': 'breakout_write_test',
        'instrument': '510300',
        'case_file_id': 'CASE-TEST-002',
        'opportunity_source': {
            'opportunity_id': 'OPP-TEST-002',
            'title': '最小样本机会',
            'source_type': 'sample_seed',
            'source_summary': '验证机会来源工件是否写入。',
            'market_context': '测试上下文',
            'prior_experience_refs': [],
            'why_now': '为验证工件链写入。',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'dataset_snapshot': {
            'dataset_version': 'dataset-test-002',
            'data_source': 'akshare.fund_etf_hist_sina',
            'instrument': '510300',
            'date_range_start': '2018-01-02',
            'date_range_end': '2026-03-24',
            'adjustment_mode': 'not_applicable',
            'cost_assumption': 'fee=0.001;slippage=0.0005',
            'missing_value_policy': 'drop_na_after_indicator_warmup',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'rule_expression': {
            'rules_version': 'rules-test-002',
            'entry_rule_summary': 'entry',
            'exit_rule_summary': 'exit',
            'filters': [],
            'execution_assumption': 'assumption',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'metrics_summary': {
            'total_return': 0.1,
            'annual_return': 0.02,
            'max_drawdown': -0.1,
            'sharpe': 0.3,
            'trade_count': 10,
            'win_rate': 0.4,
            'notes': [],
        },
        'risk_position_note': {
            'position_sizing_method': 'single_instrument_full_position',
            'max_position': 1.0,
            'risk_budget': '',
            'drawdown_tolerance': '',
            'exit_after_signal_policy': 'next_open',
            'notes': [],
        },
        'execution_constraint': {
            'execution_timing': 'next_open',
            'liquidity_requirement': '个人规模可承接',
            'slippage_assumption': '0.05%',
            'holding_capacity': '单账户个人级',
            'operational_constraints': ['日线执行'],
            'fit_for_operator': '可执行',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'review_outcome': {
            'review_status': 'approved',
            'review_outcome': 'ok',
            'key_risks': [],
            'gaps': [],
            'recommended_next_step': 'continue',
            'reviewed_at': '2026-03-25T16:16:00+08:00',
        },
        'decision_status': {
            'decision_status': 'recorded',
            'is_baseline': False,
            'baseline_of': '',
            'decision_reason': 'test',
            'decided_at': '2026-03-25T16:16:00+08:00',
        },
        'artifact_root': root,
        'memory_note_path': 'memory-note.md',
        'status_code': 'recorded',
        'created_at': '2026-03-25T16:16:00+08:00',
    }



def test_write_experiment_artifacts(tmp_path) -> None:
    experiment_store.EXPERIMENTS_ROOT = tmp_path
    artifacts = experiment_store.write_experiment_artifacts(
        research_task=sample_research_task(),
        experiment_run=sample_experiment_run(str(tmp_path / 'exp-test-002')),
        notes_markdown='# notes',
    )

    manifest = json.loads((tmp_path / 'exp-test-002' / 'manifest.json').read_text(encoding='utf-8'))
    inputs = json.loads((tmp_path / 'exp-test-002' / 'inputs.json').read_text(encoding='utf-8'))
    results = json.loads((tmp_path / 'exp-test-002' / 'results.json').read_text(encoding='utf-8'))

    assert artifacts['artifact_root'].endswith('exp-test-002')
    assert manifest['variant_name'] == 'breakout_write_test'
    assert manifest['case_file_id'] == 'CASE-TEST-002'
    assert inputs['research_task']['task_id'] == 'RTS-TEST-002'
    assert inputs['opportunity_source']['opportunity_id'] == 'OPP-TEST-002'
    assert results['decision_status']['decision_status'] == 'recorded'
    assert results['execution_constraint']['fit_for_operator'] == '可执行'

    loaded = experiment_store.read_experiment_artifacts('exp-test-002')
    assert loaded['manifest']['experiment_id'] == 'exp-test-002'
    assert loaded['inputs']['dataset_snapshot']['dataset_version'] == 'dataset-test-002'

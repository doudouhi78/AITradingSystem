from ai_dev_os.project_objects import build_experiment_artifact_payload
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.project_objects import validate_data_contract_spec
from ai_dev_os.project_objects import validate_experiment_run
from ai_dev_os.project_objects import validate_formal_review_record
from ai_dev_os.project_objects import validate_research_task
from ai_dev_os.project_objects import validate_validation_record
from ai_dev_os.project_objects import validate_variant_search_spec


def sample_research_task() -> dict:
    return {
        'task_id': 'RTS-TEST-001',
        'title': '510300 中期趋势突破验证',
        'goal': '验证 510300 的突破表达是否可作为临时基线。',
        'instrument_pool': ['510300'],
        'strategy_family': 'etf_trend_breakout',
        'hypothesis': '20日突破表达优于均线确认表达。',
        'constraints': ['单标的', '不做参数优化'],
        'success_criteria': ['形成可比较基线', '结果可复现'],
        'created_at': '2026-03-25T16:16:00+08:00',
        'opportunity_id': 'OPP-TEST-001',
        'case_file_id': 'CASE-TEST-001',
        'why_this_task': '该题目是为了把宽基ETF突破想法转成首个可比较基线。',
    }


def sample_experiment_run() -> dict:
    return {
        'experiment_id': 'exp-test-001',
        'task_id': 'RTS-TEST-001',
        'run_id': 'run-test-001',
        'title': '510300 中期趋势突破最小验证',
        'strategy_family': 'etf_trend_breakout',
        'variant_name': 'breakout_20_entry_exit_20',
        'instrument': '510300',
        'case_file_id': 'CASE-TEST-001',
        'opportunity_source': {
            'opportunity_id': 'OPP-TEST-001',
            'title': '宽基ETF中期趋势突破机会',
            'source_type': 'market_pattern',
            'source_summary': '宽基ETF在趋势明确阶段有顺势突破延续特征。',
            'market_context': 'A股宽基ETF，日线级别。',
            'prior_experience_refs': ['海龟突破', '趋势跟随经验'],
            'why_now': '当前项目需要先建立一个可解释、可比较的基线样本。',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'dataset_snapshot': {
            'dataset_version': 'dataset-20260325-001',
            'data_source': 'akshare.fund_etf_hist_sina',
            'instrument': '510300',
            'date_range_start': '2018-01-02',
            'date_range_end': '2026-03-24',
            'adjustment_mode': 'not_applicable',
            'cost_assumption': 'fee=0.001;slippage=0.0005',
            'missing_value_policy': 'drop_na_after_indicator_warmup',
            'created_at': '2026-03-25T16:16:00+08:00',
            'selection_reason': '使用统一历史区间来保证基线与变体可比较。',
            'validation_method': '日线历史回测，比较总收益、Sharpe、回撤和交易次数。',
        },
        'rule_expression': {
            'rules_version': 'rules-20260325-001',
            'entry_rule_summary': '收盘价突破前20日最高收盘价（不含当天）',
            'exit_rule_summary': '收盘价跌破前20日最低收盘价（不含当天）',
            'filters': [],
            'execution_assumption': '信号收盘生成，次日开盘执行，单标的全仓，费用0.1%，滑点0.05%',
            'created_at': '2026-03-25T16:16:00+08:00',
            'price_field': 'close/open',
            'notes': ['第2次小闭环'],
            'method_summary': '用最简单的趋势突破表达先形成比较基线。',
            'design_rationale': '先不加过滤器，避免多变量混入影响判断。',
        },
        'metrics_summary': {
            'total_return': 0.239506,
            'annual_return': 0.040106,
            'annualized_return': 0.040106,
            'max_drawdown': -0.254525,
            'sharpe': 0.305972,
            'trade_count': 25,
            'trades': 25,
            'win_rate': 0.36,
            'notes': ['形成当前更有希望的临时基线'],
            'key_findings': ['趋势突破优于首轮均线确认版', '回撤仍偏深'],
        },
        'risk_position_note': {
            'position_sizing_method': 'single_instrument_full_position',
            'max_position': 1.0,
            'risk_budget': '',
            'drawdown_tolerance': '',
            'exit_after_signal_policy': 'signal_on_close_execute_next_open',
            'notes': ['由最小研究闭环回填的初版风险/仓位说明'],
            'reasoning': '先用单标的满仓建立最小可比较口径，后续再补风控层。',
        },
        'execution_constraint': {
            'execution_timing': 'signal_on_close_execute_next_open',
            'liquidity_requirement': '宽基ETF日均成交额足以承接个人规模',
            'slippage_assumption': '0.05%',
            'holding_capacity': '单账户个人级',
            'operational_constraints': ['仅在日线级别执行', '不追求盘中跟踪'],
            'fit_for_operator': '适合低频执行的个人操作者。',
            'created_at': '2026-03-25T16:16:00+08:00',
        },
        'review_outcome': {
            'review_status': 'approved',
            'review_outcome': '进入首次复审联调',
            'key_risks': ['最大回撤仍偏深'],
            'gaps': ['未引入入场过滤器'],
            'recommended_next_step': '继续测试入场过滤器',
            'reviewed_at': '2026-03-25T16:16:00+08:00',
            'judgement': '同一趋势方向下，突破表达明显好于首轮均线确认版',
            'review_method': '同区间同成本口径对比基线与前一轮表达。',
            'review_reasoning': '先确认方向成立，再进入变体筛选。',
        },
        'decision_status': {
            'decision_status': 'baseline_candidate',
            'is_baseline': True,
            'baseline_of': '',
            'decision_reason': '当前更有希望的临时基线',
            'decided_at': '2026-03-25T16:16:00+08:00',
        },
        'artifact_root': 'D:/AITradingSystem/runtime/experiments/exp-test-001',
        'memory_note_path': 'D:/AITradingSystem/memory_v1/40_experience_base/2026-03-25_exp-test-001.md',
        'status_code': 'baseline_candidate',
        'created_at': '2026-03-25T16:16:00+08:00',
        'project_id': 'ai-trading-system',
        'validation_record_ids': ['VAL-TEST-001'],
        'search_spec_id': 'SEARCH-TEST-001',
    }


def sample_data_contract_spec() -> dict:
    return {
        'contract_id': 'contract-dataset-20260325-001',
        'title': '510300 dataset contract',
        'data_source': 'akshare.fund_etf_hist_sina',
        'instrument': '510300',
        'date_column': 'date',
        'required_columns': ['date', 'open', 'high', 'low', 'close', 'volume'],
        'non_nullable_columns': ['date', 'open', 'high', 'low', 'close', 'volume'],
        'non_negative_columns': ['open', 'high', 'low', 'close', 'volume'],
        'sort_column': 'date',
        'warmup_rows': 60,
        'expected_date_range_start': '2018-01-02',
        'expected_date_range_end': '2026-03-24',
        'instrument_bound_to_dataset': True,
        'validation_rules': ['required_columns_present'],
        'created_at': '2026-03-28T11:00:00+08:00',
    }


def sample_validation_record() -> dict:
    run = sample_experiment_run()
    return {
        'validation_id': 'VAL-TEST-001',
        'experiment_id': run['experiment_id'],
        'task_id': run['task_id'],
        'run_id': run['run_id'],
        'title': 'baseline data contract validation',
        'contract_id': 'contract-dataset-20260325-001',
        'dataset_snapshot': run['dataset_snapshot'],
        'rule_expression': run['rule_expression'],
        'metrics_summary': run['metrics_summary'],
        'validation_method': 'pandera_data_contract_v1',
        'status_code': 'passed',
        'checks_passed': ['required_columns_present'],
        'checks_failed': [],
        'summary': 'all checks passed',
        'validated_rows': 100,
        'created_at': '2026-03-28T11:00:00+08:00',
    }


def sample_variant_search_spec() -> dict:
    return {
        'search_id': 'SEARCH-TEST-001',
        'title': 'baseline breakout search',
        'strategy_family': 'etf_trend_breakout',
        'baseline_experiment_id': 'exp-test-001',
        'objective_metric': 'sharpe',
        'objective_mode': 'maximize',
        'max_trials': 3,
        'parameter_space': {
            'entry_window': {'type': 'int', 'low': 15, 'high': 25, 'step': 5},
            'exit_window': {'type': 'int', 'low': 10, 'high': 20, 'step': 10},
        },
        'constraints': ['single_instrument', 'small_search_space'],
        'created_at': '2026-03-28T11:00:00+08:00',
    }


def sample_formal_review_record() -> dict:
    return {
        'review_id': 'REV-TEST-001',
        'experiment_id': 'exp-test-001',
        'baseline_experiment_id': 'exp-test-000',
        'review_scope': 'single_variant_review',
        'review_question': '该候选是否应晋升为基线',
        'review_method': '同口径指标比较 + 风险缺口检查',
        'comparison_summary': 'Sharpe 略好，但收益略低，不足以直接晋升。',
        'risks': ['可能存在搜索过拟合'],
        'gaps': ['尚未补风险/仓位进一步收敛'],
        'decision_recommendation': 'keep_as_candidate',
        'decision_reason': '保留为候选，等待后续人工变体比较。',
        'reviewed_at': '2026-03-28T12:00:00+08:00',
        'validation_record_ids': ['VAL-TEST-001'],
        'search_spec_id': 'SEARCH-TEST-001',
    }


def test_project_objects_validate_and_build() -> None:
    task = validate_research_task(sample_research_task())
    run = validate_experiment_run(sample_experiment_run())
    payload = build_experiment_artifact_payload(research_task=task, experiment_run=run)
    index_record = build_experiment_index_record(experiment_run=run)

    assert payload['manifest']['experiment_id'] == 'exp-test-001'
    assert payload['manifest']['case_file_id'] == 'CASE-TEST-001'
    assert payload['manifest']['search_spec_id'] == 'SEARCH-TEST-001'
    assert payload['manifest']['validation_record_ids'] == ['VAL-TEST-001']
    assert payload['inputs']['dataset_snapshot']['dataset_version'] == 'dataset-20260325-001'
    assert payload['inputs']['opportunity_source']['opportunity_id'] == 'OPP-TEST-001'
    assert payload['results']['decision_status']['is_baseline'] is True
    assert payload['results']['execution_constraint']['fit_for_operator'] == '适合低频执行的个人操作者。'
    assert index_record['rules_version'] == 'rules-20260325-001'
    assert index_record['decision_status'] == 'baseline_candidate'


def test_project_objects_require_missing_field() -> None:
    broken = sample_experiment_run()
    del broken['decision_status']['decision_reason']
    try:
        validate_experiment_run(broken)
    except ValueError as exc:
        assert 'decision_status.decision_reason' in str(exc)
    else:
        raise AssertionError('expected ValueError for missing decision_reason')


def test_contract_validation_search_and_review_specs() -> None:
    contract = validate_data_contract_spec(sample_data_contract_spec())
    validation = validate_validation_record(sample_validation_record())
    search_spec = validate_variant_search_spec(sample_variant_search_spec())
    review = validate_formal_review_record(sample_formal_review_record())

    assert contract['warmup_rows'] == 60
    assert validation['validation_id'] == 'VAL-TEST-001'
    assert search_spec['objective_metric'] == 'sharpe'
    assert review['decision_recommendation'] == 'keep_as_candidate'

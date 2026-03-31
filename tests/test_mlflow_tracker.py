import pytest

pytest.importorskip("mlflow", reason="mlflow not installed; skipping mlflow tracker tests")

from ai_dev_os.mlflow_tracker import configure_mlflow_tracking
from ai_dev_os.mlflow_tracker import ensure_mlflow_experiment
from ai_dev_os.mlflow_tracker import log_experiment_run_to_mlflow


def sample_research_task() -> dict:
    return {
        'task_id': 'RTS-MLFLOW-001',
        'title': 'MLflow 接入测试',
        'goal': '验证本地 mlflow tracking 能记录实验比较对象。',
        'instrument_pool': ['510300'],
        'strategy_family': 'etf_trend_breakout',
        'hypothesis': 'MLflow 能承接当前实验参数、指标和标签。',
        'constraints': ['本地 file store'],
        'success_criteria': ['run 可写入', '可按 tag 找到'],
        'created_at': '2026-03-26T09:00:00+08:00',
    }


def sample_experiment_run(tmp_path) -> dict:
    artifact_root = tmp_path / 'exp-mlflow-001'
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / 'manifest.json').write_text('{}\n', encoding='utf-8')
    return {
        'project_id': 'ai-trading-system',
        'experiment_id': 'exp-mlflow-001',
        'task_id': 'RTS-MLFLOW-001',
        'run_id': 'run-mlflow-001',
        'title': 'MLflow 最小接入测试',
        'strategy_family': 'etf_trend_breakout',
        'variant_name': 'breakout_mlflow_test',
        'instrument': '510300',
        'dataset_snapshot': {
            'dataset_version': 'dataset-mlflow-001',
            'data_source': 'akshare.fund_etf_hist_sina',
            'instrument': '510300',
            'date_range_start': '2018-01-02',
            'date_range_end': '2026-03-24',
            'adjustment_mode': 'not_applicable',
            'cost_assumption': 'fee=0.001;slippage=0.0005',
            'missing_value_policy': 'drop_na_after_indicator_warmup',
            'created_at': '2026-03-26T09:00:00+08:00',
        },
        'rule_expression': {
            'rules_version': 'rules-mlflow-001',
            'entry_rule_summary': 'entry',
            'exit_rule_summary': 'exit',
            'filters': [],
            'execution_assumption': 'assumption',
            'created_at': '2026-03-26T09:00:00+08:00',
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
        'review_outcome': {
            'review_status': 'approved',
            'review_outcome': 'ok',
            'key_risks': [],
            'gaps': [],
            'recommended_next_step': 'continue',
            'reviewed_at': '2026-03-26T09:00:00+08:00',
        },
        'decision_status': {
            'decision_status': 'baseline_candidate',
            'is_baseline': True,
            'baseline_of': '',
            'decision_reason': 'baseline candidate',
            'decided_at': '2026-03-26T09:00:00+08:00',
        },
        'artifact_root': str(artifact_root),
        'memory_note_path': str(tmp_path / 'memory-note.md'),
        'status_code': 'baseline_candidate',
        'created_at': '2026-03-26T09:00:00+08:00',
    }


def test_log_experiment_run_to_mlflow(tmp_path, monkeypatch) -> None:
    import mlflow
    import ai_dev_os.mlflow_tracker as tracker

    monkeypatch.setattr(tracker, 'MLFLOW_ROOT', tmp_path / 'mlflow')
    monkeypatch.setattr(tracker, 'MLFLOW_TRACKING_DB', tmp_path / 'mlflow' / 'mlflow.db')
    monkeypatch.setattr(tracker, 'MLFLOW_ARTIFACTS_ROOT', tmp_path / 'mlflow' / 'artifacts')

    tracking_uri = configure_mlflow_tracking()
    experiment_id = ensure_mlflow_experiment()
    run_id = log_experiment_run_to_mlflow(
        research_task=sample_research_task(),
        experiment_run=sample_experiment_run(tmp_path),
    )

    assert tracking_uri.startswith('sqlite:///')
    assert experiment_id
    assert run_id

    runs = mlflow.search_runs(
        experiment_ids=[experiment_id],
        filter_string="tags.experiment_id = 'exp-mlflow-001'",
    )
    assert len(runs) == 1
    assert runs.iloc[0]['params.dataset_version'] == 'dataset-mlflow-001'
    assert float(runs.iloc[0]['metrics.total_return']) == 0.1

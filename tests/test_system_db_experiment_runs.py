import sqlite3

from ai_dev_os import system_db


def sample_record(tmp_path) -> dict:
    return {
        'project_id': 'ai-trading-system',
        'experiment_id': 'exp-test-003',
        'task_id': 'RTS-TEST-003',
        'run_id': 'run-test-003',
        'title': 'SQLite 实验索引测试',
        'strategy_family': 'etf_trend_breakout',
        'variant_name': 'breakout_sqlite_test',
        'instrument': '510300',
        'data_source': 'akshare.fund_etf_hist_sina',
        'date_range_start': '2018-01-02',
        'date_range_end': '2026-03-24',
        'entry_rule_summary': 'entry',
        'exit_rule_summary': 'exit',
        'execution_assumption': 'assumption',
        'metrics_summary': {'total_return': 0.1},
        'review_outcome': 'ok',
        'memory_note_path': 'note.md',
        'artifact_root': str(tmp_path / 'exp-test-003'),
        'status_code': 'baseline_candidate',
        'created_at': '2026-03-25T16:16:00+08:00',
        'dataset_version': 'dataset-test-003',
        'rules_version': 'rules-test-003',
        'decision_status': 'baseline_candidate',
        'is_baseline': True,
        'baseline_of': '',
        'cost_assumption': 'fee=0.001;slippage=0.0005',
    }


def test_record_experiment_run_with_schema_fields(tmp_path) -> None:
    system_db.DB_ROOT = tmp_path
    system_db.DB_PATH = tmp_path / 'system_facts.sqlite3'
    system_db.ensure_database()
    system_db.record_experiment_run(sample_record(tmp_path), artifacts={'manifest': str(tmp_path / 'manifest.json')}, emit_spool=False)

    conn = sqlite3.connect(system_db.DB_PATH)
    try:
        row = conn.execute(
            'SELECT dataset_version, rules_version, decision_status, is_baseline, baseline_of, cost_assumption FROM experiment_runs WHERE experiment_id = ?',
            ('exp-test-003',),
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == 'dataset-test-003'
    assert row[1] == 'rules-test-003'
    assert row[2] == 'baseline_candidate'
    assert row[3] == 1
    assert row[4] == ''
    assert row[5] == 'fee=0.001;slippage=0.0005'

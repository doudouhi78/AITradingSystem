from pathlib import Path

import pandas as pd

from ai_dev_os.data_contracts import build_default_data_contract_spec
from ai_dev_os.data_contracts import validate_dataframe_against_contract
from ai_dev_os.validation_store import read_validation_record
from ai_dev_os.validation_store import write_validation_record

from tests.test_project_objects import sample_experiment_run


def sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            'date': pd.date_range('2018-01-02', periods=70, freq='D'),
            'open': [1.0 + idx for idx in range(70)],
            'high': [1.2 + idx for idx in range(70)],
            'low': [0.8 + idx for idx in range(70)],
            'close': [1.1 + idx for idx in range(70)],
            'volume': [1000 + idx for idx in range(70)],
        }
    )


def test_pandera_validation_pass(tmp_path) -> None:
    df = sample_dataframe()
    run = sample_experiment_run()
    snapshot = dict(run['dataset_snapshot'])
    snapshot['date_range_end'] = '2018-03-12'
    contract = build_default_data_contract_spec(snapshot, created_at='2026-03-28T11:00:00+08:00', warmup_rows=10)

    record = validate_dataframe_against_contract(
        df,
        dataset_snapshot=snapshot,
        rule_expression=run['rule_expression'],
        metrics_summary=run['metrics_summary'],
        contract_spec=contract,
        validation_id='VAL-PASS-001',
        experiment_id=run['experiment_id'],
        task_id=run['task_id'],
        run_id=run['run_id'],
        title='pass validation',
        created_at='2026-03-28T11:00:00+08:00',
    )
    assert record['status_code'] == 'passed'
    assert 'required_columns_present' in record['checks_passed']

    from ai_dev_os import validation_store

    validation_store.VALIDATIONS_ROOT = tmp_path
    path = write_validation_record(record)
    loaded = read_validation_record('VAL-PASS-001')

    assert Path(path).exists()
    assert loaded['status_code'] == 'passed'


def test_pandera_validation_fail_on_missing_column() -> None:
    df = sample_dataframe().drop(columns=['volume'])
    run = sample_experiment_run()
    snapshot = dict(run['dataset_snapshot'])
    snapshot['date_range_end'] = '2018-03-12'
    contract = build_default_data_contract_spec(snapshot, created_at='2026-03-28T11:00:00+08:00', warmup_rows=10)

    record = validate_dataframe_against_contract(
        df,
        dataset_snapshot=snapshot,
        rule_expression=run['rule_expression'],
        metrics_summary=run['metrics_summary'],
        contract_spec=contract,
        validation_id='VAL-FAIL-001',
        experiment_id=run['experiment_id'],
        task_id=run['task_id'],
        run_id=run['run_id'],
        title='fail validation',
        created_at='2026-03-28T11:00:00+08:00',
    )
    assert record['status_code'] == 'failed'
    assert any(item.startswith('missing_columns:') for item in record['checks_failed'])

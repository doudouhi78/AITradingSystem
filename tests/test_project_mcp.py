from pathlib import Path

import pytest

from ai_dev_os import experiment_store
from ai_dev_os import review_store
from ai_dev_os import search_store
from ai_dev_os import validation_store
from ai_dev_os.project_mcp import get_current_baseline
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_mcp import get_formal_review
from ai_dev_os.project_mcp import get_search_spec
from ai_dev_os.project_mcp import get_validation_record
from ai_dev_os.project_mcp import list_experiment_runs
from ai_dev_os.project_mcp import list_formal_review_summaries
from ai_dev_os.project_mcp import list_memory_documents
from ai_dev_os.project_mcp import list_search_spec_summaries
from ai_dev_os.project_mcp import read_memory_document
from ai_dev_os.tool_bus import tool_bus


@pytest.fixture(autouse=True)
def _pin_repo_runtime_roots() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    experiment_store.EXPERIMENTS_ROOT = repo_root / 'runtime' / 'experiments'
    validation_store.VALIDATIONS_ROOT = repo_root / 'runtime' / 'validations'
    search_store.SEARCHES_ROOT = repo_root / 'runtime' / 'searches'
    review_store.REVIEWS_ROOT = repo_root / 'runtime' / 'reviews'


def test_list_memory_documents_contains_expected_entries() -> None:
    docs = list_memory_documents()
    names = {item['name'] for item in docs}
    assert 'vision' in names
    assert 'mainline' in names
    assert 'working_test' in names


def test_read_memory_document_returns_content() -> None:
    payload = read_memory_document('vision')
    assert payload['name'] == 'vision'
    assert '最终可稳定盈利' in payload['content']


def test_list_experiment_runs_returns_backfilled_records() -> None:
    rows = list_experiment_runs(limit=10)
    ids = {row['experiment_id'] for row in rows}
    assert 'exp-20260325-001-trend-following' in ids
    assert 'exp-20260325-002-breakout-baseline' in ids
    assert 'exp-20260325-003-breakout-exit-10d' in ids


def test_get_experiment_run_returns_index_artifacts_and_review_chain_objects() -> None:
    payload = get_experiment_run('exp-20260328-006-optuna-candidate')
    assert payload['index']['experiment_id'] == 'exp-20260328-006-optuna-candidate'
    assert payload['artifacts']['manifest']['search_spec_id'] == 'SEARCH-20260328-001'
    assert payload['validation_records'][0]['validation_id'] == 'VAL-20260328-001'
    assert payload['search_spec']['search_id'] == 'SEARCH-20260328-001'
    assert payload['formal_reviews'][0]['review_id'] == 'REV-20260328-001'


def test_get_current_baseline_returns_baseline_record() -> None:
    baseline = get_current_baseline()
    assert baseline['is_baseline'] == 1
    assert baseline['decision_status']


def test_validation_search_and_review_readers_work() -> None:
    validation = get_validation_record('VAL-20260328-001')
    assert validation['status_code'] == 'passed'

    search = get_search_spec('SEARCH-20260328-001')
    assert search['objective_metric'] == 'sharpe'

    summaries = list_search_spec_summaries(limit=10)
    ids = {item['search_id'] for item in summaries}
    assert 'SEARCH-20260328-001' in ids

    review = get_formal_review('REV-20260328-001')
    assert review['decision_recommendation'] == 'keep_as_candidate'

    review_summaries = list_formal_review_summaries(limit=10, experiment_id='exp-20260328-006-optuna-candidate')
    assert review_summaries[0]['review_id'] == 'REV-20260328-001'


def test_tool_bus_exposes_project_mcp_operations() -> None:
    list_result = tool_bus.call_tool('project_mcp', operation='list_experiment_runs', limit=2)
    assert list_result['success'] is True
    assert len(list_result['result']) >= 1

    baseline_result = tool_bus.call_tool('project_mcp', operation='get_current_baseline')
    assert baseline_result['success'] is True
    assert baseline_result['result']['is_baseline'] == 1

    validation_result = tool_bus.call_tool('project_mcp', operation='get_validation_record', validation_id='VAL-20260328-001')
    assert validation_result['success'] is True
    assert validation_result['result']['validation_id'] == 'VAL-20260328-001'

    search_result = tool_bus.call_tool('project_mcp', operation='get_search_spec', search_id='SEARCH-20260328-001')
    assert search_result['success'] is True
    assert search_result['result']['search_id'] == 'SEARCH-20260328-001'

    review_result = tool_bus.call_tool('project_mcp', operation='get_formal_review', review_id='REV-20260328-001')
    assert review_result['success'] is True
    assert review_result['result']['review_id'] == 'REV-20260328-001'

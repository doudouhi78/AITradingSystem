from fastapi.testclient import TestClient

from ai_dev_os.dashboard_api import app


client = TestClient(app)


def test_get_overview() -> None:
    response = client.get('/api/v1/overview')
    assert response.status_code == 200
    payload = response.json()
    assert payload['current_phase']
    assert payload['current_baseline']['experiment_id'] == 'exp-20260328-007-manual-entry25-exit20'


def test_get_experiments() -> None:
    response = client.get('/api/v1/experiments', params={'limit': 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload['items']
    assert any(item['experiment_id'] == 'exp-20260325-002-breakout-baseline' for item in payload['items'])


def test_get_experiment_detail() -> None:
    response = client.get('/api/v1/experiments/exp-20260326-005-breakout-ma60-filter')
    assert response.status_code == 200
    payload = response.json()
    assert payload['task_summary']['task_id']
    assert payload['artifact_links']['artifact_root']


def test_get_flow() -> None:
    response = client.get('/api/v1/flow')
    assert response.status_code == 200
    payload = response.json()
    assert 'recent_traces' in payload
    assert 'stage_status_counts' in payload


def test_get_trace_detail() -> None:
    response = client.get('/api/v1/traces/run-20260326-004')
    assert response.status_code == 200
    payload = response.json()
    assert payload['trace_summary']['run_id'] == 'run-20260326-004'
    assert payload['events']

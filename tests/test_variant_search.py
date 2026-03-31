import pytest

pytest.importorskip("optuna", reason="optuna not installed; skipping variant search tests")

from ai_dev_os.variant_search import run_variant_search

from tests.test_project_objects import sample_variant_search_spec


def test_optuna_variant_search_returns_ranked_trials() -> None:
    spec = sample_variant_search_spec()

    def objective_fn(params: dict) -> float:
        entry = params['entry_window']
        exit_ = params['exit_window']
        return float(-(abs(entry - 20) + abs(exit_ - 20)))

    result = run_variant_search(spec, objective_fn=objective_fn)

    assert result['search_id'] == 'SEARCH-TEST-001'
    assert isinstance(result['best_params'], dict)
    assert len(result['trials']) == spec['max_trials']
    assert result['trials'][0]['value'] >= result['trials'][-1]['value']

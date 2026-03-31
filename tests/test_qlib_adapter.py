import importlib

import pytest

from ai_dev_os.project_objects import FormalReviewRecord, ResearchTask, ValidationRecord
from ai_dev_os.qlib_adapter import (
    build_qlib_dataset_config,
    build_qlib_strategy_bridge,
    build_qlib_workflow_payload,
    ensure_qlib_available,
)

_qlib_available = importlib.util.find_spec("qlib") is not None


def _research_task() -> ResearchTask:
    return {
        "task_id": "TASK-QLIB-001",
        "title": "Qlib bridge check",
        "goal": "Keep qlib behind adapter only",
        "instrument_pool": ["510300"],
        "strategy_family": "trend_following",
        "hypothesis": "Breakout remains valid after adapter mapping",
        "constraints": ["single instrument"],
        "success_criteria": ["payload builds"],
        "created_at": "2026-03-28T12:00:00+08:00",
        "case_file_id": "CASE-001",
    }


def _experiment_run():
    return {
        "experiment_id": "exp-test-qlib-001",
        "task_id": "TASK-QLIB-001",
        "run_id": "run-test-qlib-001",
        "title": "Qlib compatible experiment",
        "strategy_family": "trend_following",
        "variant_name": "manual-entry25-exit20",
        "instrument": "510300",
        "dataset_snapshot": {
            "dataset_version": "DS-001",
            "data_source": "akshare",
            "instrument": "510300",
            "date_range_start": "2018-01-02",
            "date_range_end": "2026-03-24",
            "adjustment_mode": "qfq",
            "cost_assumption": "0.1% one-side fee",
            "missing_value_policy": "drop warmup nulls",
            "created_at": "2026-03-28T12:00:00+08:00",
        },
        "rule_expression": {
            "rules_version": "RULE-001",
            "entry_rule_summary": "close breaks 25-day high",
            "exit_rule_summary": "close breaks 20-day low",
            "filters": ["liquid ETF only"],
            "execution_assumption": "next bar open",
            "created_at": "2026-03-28T12:00:00+08:00",
        },
        "metrics_summary": {
            "total_return": 0.2,
            "annual_return": 0.1,
            "max_drawdown": -0.18,
            "sharpe": 1.1,
            "trade_count": 10,
            "win_rate": 0.4,
            "notes": [],
        },
        "risk_position_note": {
            "position_sizing_method": "fixed_fraction",
            "max_position": 0.5,
            "risk_budget": "0.5% per trade",
            "drawdown_tolerance": "12% review, 18% stop",
            "exit_after_signal_policy": "exit on signal",
            "notes": [],
        },
        "review_outcome": {
            "review_status": "reviewed",
            "review_outcome": "keep_as_candidate",
            "key_risks": ["single regime risk"],
            "gaps": ["need broader market validation"],
            "recommended_next_step": "qlib workflow assessment",
            "reviewed_at": "2026-03-28T12:00:00+08:00",
        },
        "decision_status": {
            "decision_status": "keep_as_candidate",
            "is_baseline": False,
            "baseline_of": "exp-20260325-002-breakout-baseline",
            "decision_reason": "Bridge test only",
            "decided_at": "2026-03-28T12:00:00+08:00",
        },
        "artifact_root": "d:/AITradingSystem/runtime/experiments/exp-test-qlib-001",
        "memory_note_path": "d:/AITradingSystem/memory_v1/40_experience_base/test.md",
        "status_code": "reviewed",
        "created_at": "2026-03-28T12:00:00+08:00",
        "execution_constraint": {
            "execution_timing": "next open",
            "liquidity_requirement": "ETF liquidity minimum",
            "slippage_assumption": "fixed 0.1%",
            "holding_capacity": "single position",
            "operational_constraints": ["no intraday execution"],
            "fit_for_operator": "acceptable",
            "created_at": "2026-03-28T12:00:00+08:00",
        },
        "case_file_id": "CASE-001",
    }


def _validation_record() -> ValidationRecord:
    run = _experiment_run()
    return {
        "validation_id": "VAL-TEST-001",
        "experiment_id": run["experiment_id"],
        "task_id": run["task_id"],
        "run_id": run["run_id"],
        "title": run["title"],
        "contract_id": "CONTRACT-TEST-001",
        "dataset_snapshot": run["dataset_snapshot"],
        "rule_expression": run["rule_expression"],
        "metrics_summary": run["metrics_summary"],
        "validation_method": "vectorbt",
        "status_code": "passed",
        "summary": "coherent",
        "checks_passed": ["payload_builds"],
        "checks_failed": [],
        "validated_rows": 252,
        "evidence_refs": [],
        "created_at": "2026-03-28T12:00:00+08:00",
    }


def _formal_review() -> FormalReviewRecord:
    return {
        "review_id": "REV-TEST-001",
        "experiment_id": "exp-test-qlib-001",
        "baseline_experiment_id": "exp-20260325-002-breakout-baseline",
        "review_scope": "bridge-review",
        "review_question": "Can qlib stay behind adapter",
        "review_method": "boundary review",
        "comparison_summary": "No business semantics leaked",
        "risks": ["qlib object model takeover"],
        "gaps": ["real qlib data workflow not wired yet"],
        "decision_recommendation": "keep_as_candidate",
        "decision_reason": "Adapter layer holds line",
        "reviewed_at": "2026-03-28T12:00:00+08:00",
    }


@pytest.mark.skipif(not _qlib_available, reason="qlib not installed")
def test_ensure_qlib_available():
    assert ensure_qlib_available()


def test_build_qlib_dataset_config():
    dataset = build_qlib_dataset_config(_experiment_run()["dataset_snapshot"])
    assert dataset["instrument"] == "510300"
    assert dataset["handler_kwargs"]["dataset_version"] == "DS-001"


def test_build_qlib_strategy_bridge():
    run = _experiment_run()
    bridge = build_qlib_strategy_bridge(
        run["rule_expression"],
        run["risk_position_note"],
        run["execution_constraint"],
    )
    assert bridge["rules_version"] == "RULE-001"
    assert bridge["risk_position"]["max_position"] == 0.5
    assert bridge["execution_constraint"]["execution_timing"] == "next open"


def test_build_qlib_workflow_payload():
    payload = build_qlib_workflow_payload(
        _research_task(),
        _experiment_run(),
        _validation_record(),
        _formal_review(),
    )
    assert payload["case_file_id"] == "CASE-001"
    assert payload["experiment_bridge"]["experiment_id"] == "exp-test-qlib-001"
    assert payload["validation_bridge"]["validation_id"] == "VAL-TEST-001"
    assert payload["formal_review_bridge"]["review_id"] == "REV-TEST-001"

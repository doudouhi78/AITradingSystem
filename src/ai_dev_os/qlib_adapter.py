from __future__ import annotations

from typing import Any

from ai_dev_os.project_objects import (
    DatasetSnapshot,
    ExecutionConstraint,
    ExperimentRun,
    FormalReviewRecord,
    ResearchTask,
    RiskPositionNote,
    RuleExpression,
    ValidationRecord,
    validate_dataset_snapshot,
    validate_execution_constraint,
    validate_experiment_run,
    validate_formal_review_record,
    validate_research_task,
    validate_risk_position_note,
    validate_rule_expression,
    validate_validation_record,
)


def ensure_qlib_available() -> str:
    import qlib  # type: ignore

    return getattr(qlib, "__version__", "unknown")


def build_qlib_dataset_config(
    dataset_snapshot: DatasetSnapshot | dict[str, Any],
) -> dict[str, Any]:
    snapshot = validate_dataset_snapshot(dataset_snapshot)
    return {
        "instrument": snapshot["instrument"],
        "start_time": snapshot["date_range_start"],
        "end_time": snapshot["date_range_end"],
        "freq": "day",
        "handler_kwargs": {
            "data_source": snapshot["data_source"],
            "adjustment_mode": snapshot["adjustment_mode"],
            "missing_value_policy": snapshot["missing_value_policy"],
            "dataset_version": snapshot["dataset_version"],
        },
    }


def build_qlib_strategy_bridge(
    rule_expression: RuleExpression | dict[str, Any],
    risk_position_note: RiskPositionNote | dict[str, Any] | None = None,
    execution_constraint: ExecutionConstraint | dict[str, Any] | None = None,
) -> dict[str, Any]:
    rule = validate_rule_expression(rule_expression)
    payload: dict[str, Any] = {
        "rules_version": rule["rules_version"],
        "entry_rule_summary": rule["entry_rule_summary"],
        "exit_rule_summary": rule["exit_rule_summary"],
        "filters": rule["filters"],
        "execution_assumption": rule["execution_assumption"],
    }
    if risk_position_note is not None:
        risk = validate_risk_position_note(risk_position_note)
        payload["risk_position"] = {
            "position_sizing_method": risk["position_sizing_method"],
            "max_position": risk["max_position"],
            "risk_budget": risk["risk_budget"],
            "drawdown_tolerance": risk["drawdown_tolerance"],
        }
    if execution_constraint is not None:
        execution = validate_execution_constraint(execution_constraint)
        payload["execution_constraint"] = {
            "execution_timing": execution["execution_timing"],
            "slippage_assumption": execution["slippage_assumption"],
            "liquidity_requirement": execution["liquidity_requirement"],
        }
    return payload


def build_qlib_workflow_payload(
    research_task: ResearchTask | dict[str, Any],
    experiment_run: ExperimentRun | dict[str, Any],
    validation_record: ValidationRecord | dict[str, Any] | None = None,
    formal_review_record: FormalReviewRecord | dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = validate_research_task(research_task)
    run = validate_experiment_run(experiment_run)
    payload: dict[str, Any] = {
        "case_file_id": run.get("case_file_id", task.get("case_file_id")),
        "research_task": {
            "task_id": task["task_id"],
            "title": task["title"],
            "goal": task["goal"],
            "strategy_family": task["strategy_family"],
            "hypothesis": task["hypothesis"],
        },
        "dataset": build_qlib_dataset_config(run["dataset_snapshot"]),
        "strategy_bridge": build_qlib_strategy_bridge(
            run["rule_expression"],
            run["risk_position_note"],
            run.get("execution_constraint"),
        ),
        "experiment_bridge": {
            "experiment_id": run["experiment_id"],
            "baseline_of": run["decision_status"]["baseline_of"],
            "decision_status": run["decision_status"]["decision_status"],
            "artifact_root": run["artifact_root"],
            "memory_note_path": run["memory_note_path"],
        },
    }
    if validation_record is not None:
        record = validate_validation_record(validation_record)
        payload["validation_bridge"] = {
            "validation_id": record["validation_id"],
            "validation_method": record["validation_method"],
            "summary": record["summary"],
            "status_code": record["status_code"],
            "checks_passed": record["checks_passed"],
            "checks_failed": record["checks_failed"],
        }
    if formal_review_record is not None:
        review = validate_formal_review_record(formal_review_record)
        payload["formal_review_bridge"] = {
            "review_id": review["review_id"],
            "review_scope": review["review_scope"],
            "decision_recommendation": review["decision_recommendation"],
            "decision_reason": review["decision_reason"],
        }
    return payload

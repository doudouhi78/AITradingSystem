from __future__ import annotations

from typing import Any
from typing import TypedDict

try:
    from typing import NotRequired
except ImportError:  # Python < 3.11
    from typing_extensions import NotRequired


PROJECT_OBJECT_SCHEMA_VERSION = "v4"


class OpportunitySource(TypedDict):
    opportunity_id: str
    title: str
    source_type: str
    source_summary: str
    market_context: str
    prior_experience_refs: list[str]
    why_now: str
    created_at: str


class ResearchTask(TypedDict):
    task_id: str
    title: str
    goal: str
    instrument_pool: list[str]
    strategy_family: str
    hypothesis: str
    constraints: list[str]
    success_criteria: list[str]
    created_at: str
    opportunity_id: NotRequired[str]
    case_file_id: NotRequired[str]
    why_this_task: NotRequired[str]


class RuleExpression(TypedDict):
    rules_version: str
    entry_rule_summary: str
    exit_rule_summary: str
    filters: list[str]
    execution_assumption: str
    created_at: str
    price_field: NotRequired[str]
    notes: NotRequired[list[str]]
    method_summary: NotRequired[str]
    design_rationale: NotRequired[str]


class DatasetSnapshot(TypedDict):
    dataset_version: str
    data_source: str
    instrument: str
    date_range_start: str
    date_range_end: str
    adjustment_mode: str
    cost_assumption: str
    missing_value_policy: str
    created_at: str
    selection_reason: NotRequired[str]
    validation_method: NotRequired[str]


class MetricsSummary(TypedDict):
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe: float
    trade_count: int
    win_rate: float
    notes: list[str]
    annualized_return: NotRequired[float]
    trades: NotRequired[int]
    key_findings: NotRequired[list[str]]


class RiskPositionNote(TypedDict):
    position_sizing_method: str
    max_position: float | str
    risk_budget: str
    drawdown_tolerance: str
    exit_after_signal_policy: str
    notes: list[str]
    reasoning: NotRequired[str]


class ExecutionConstraint(TypedDict):
    execution_timing: str
    liquidity_requirement: str
    slippage_assumption: str
    holding_capacity: str
    operational_constraints: list[str]
    fit_for_operator: str
    created_at: str


class ReviewOutcome(TypedDict):
    review_status: str
    review_outcome: str
    key_risks: list[str]
    gaps: list[str]
    recommended_next_step: str
    reviewed_at: str
    judgement: NotRequired[str]
    review_method: NotRequired[str]
    review_reasoning: NotRequired[str]


class FormalReviewRecord(TypedDict):
    review_id: str
    experiment_id: str
    baseline_experiment_id: str
    review_scope: str
    review_question: str
    review_method: str
    comparison_summary: str
    risks: list[str]
    gaps: list[str]
    decision_recommendation: str
    decision_reason: str
    reviewed_at: str
    validation_record_ids: NotRequired[list[str]]
    search_spec_id: NotRequired[str]


class DecisionStatus(TypedDict):
    decision_status: str
    is_baseline: bool
    baseline_of: str
    decision_reason: str
    decided_at: str


class StrategyCaseFile(TypedDict):
    case_file_id: str
    case_title: str
    lifecycle_stage: str
    current_status: str
    current_hypothesis: str
    related_task_ids: list[str]
    related_experiment_ids: list[str]
    baseline_experiment_id: str
    current_experiment_id: str
    created_at: str
    updated_at: str


class DataContractSpec(TypedDict):
    contract_id: str
    title: str
    data_source: str
    instrument: str
    date_column: str
    required_columns: list[str]
    non_nullable_columns: list[str]
    non_negative_columns: list[str]
    sort_column: str
    warmup_rows: int
    expected_date_range_start: str
    expected_date_range_end: str
    instrument_bound_to_dataset: bool
    validation_rules: list[str]
    created_at: str


class ValidationRecord(TypedDict):
    validation_id: str
    experiment_id: str
    task_id: str
    run_id: str
    title: str
    contract_id: str
    dataset_snapshot: DatasetSnapshot
    rule_expression: RuleExpression
    metrics_summary: MetricsSummary
    validation_method: str
    status_code: str
    checks_passed: list[str]
    checks_failed: list[str]
    summary: str
    validated_rows: int
    created_at: str
    artifact_path: NotRequired[str]


class VariantSearchSpec(TypedDict):
    search_id: str
    title: str
    strategy_family: str
    baseline_experiment_id: str
    objective_metric: str
    objective_mode: str
    max_trials: int
    parameter_space: dict[str, dict[str, Any]]
    constraints: list[str]
    created_at: str


class ExperimentRun(TypedDict):
    experiment_id: str
    task_id: str
    run_id: str
    title: str
    strategy_family: str
    variant_name: str
    instrument: str
    dataset_snapshot: DatasetSnapshot
    rule_expression: RuleExpression
    metrics_summary: MetricsSummary
    risk_position_note: RiskPositionNote
    review_outcome: ReviewOutcome
    decision_status: DecisionStatus
    artifact_root: str
    memory_note_path: str
    status_code: str
    created_at: str
    project_id: NotRequired[str]
    opportunity_source: NotRequired[OpportunitySource]
    execution_constraint: NotRequired[ExecutionConstraint]
    case_file_id: NotRequired[str]
    validation_record_ids: NotRequired[list[str]]
    search_spec_id: NotRequired[str]


def _require_non_empty_string(payload: dict[str, Any], field: str, owner: str) -> str:
    value = str(payload.get(field, "") or "").strip()
    if not value:
        raise ValueError(f"{owner}.{field} is required")
    return value


def _require_list(payload: dict[str, Any], field: str, owner: str) -> list[Any]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise ValueError(f"{owner}.{field} must be a list")
    return value


def _require_mapping(payload: Any, owner: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"{owner} must be a mapping")
    return dict(payload)


def _require_int(payload: dict[str, Any], field: str, owner: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int):
        raise ValueError(f"{owner}.{field} must be an integer")
    return value


def validate_opportunity_source(payload: OpportunitySource | dict[str, Any]) -> OpportunitySource:
    data = _require_mapping(payload, "opportunity_source")
    _require_non_empty_string(data, "opportunity_id", "opportunity_source")
    _require_non_empty_string(data, "title", "opportunity_source")
    _require_non_empty_string(data, "source_type", "opportunity_source")
    _require_non_empty_string(data, "source_summary", "opportunity_source")
    _require_non_empty_string(data, "market_context", "opportunity_source")
    _require_list(data, "prior_experience_refs", "opportunity_source")
    _require_non_empty_string(data, "why_now", "opportunity_source")
    _require_non_empty_string(data, "created_at", "opportunity_source")
    return data  # type: ignore[return-value]


def validate_research_task(payload: ResearchTask | dict[str, Any]) -> ResearchTask:
    data = _require_mapping(payload, "research_task")
    _require_non_empty_string(data, "task_id", "research_task")
    _require_non_empty_string(data, "title", "research_task")
    _require_non_empty_string(data, "goal", "research_task")
    _require_list(data, "instrument_pool", "research_task")
    _require_non_empty_string(data, "strategy_family", "research_task")
    _require_non_empty_string(data, "hypothesis", "research_task")
    _require_list(data, "constraints", "research_task")
    _require_list(data, "success_criteria", "research_task")
    _require_non_empty_string(data, "created_at", "research_task")
    return data  # type: ignore[return-value]


def validate_rule_expression(payload: RuleExpression | dict[str, Any]) -> RuleExpression:
    data = _require_mapping(payload, "rule_expression")
    _require_non_empty_string(data, "rules_version", "rule_expression")
    _require_non_empty_string(data, "entry_rule_summary", "rule_expression")
    _require_non_empty_string(data, "exit_rule_summary", "rule_expression")
    _require_list(data, "filters", "rule_expression")
    _require_non_empty_string(data, "execution_assumption", "rule_expression")
    _require_non_empty_string(data, "created_at", "rule_expression")
    return data  # type: ignore[return-value]


def validate_dataset_snapshot(payload: DatasetSnapshot | dict[str, Any]) -> DatasetSnapshot:
    data = _require_mapping(payload, "dataset_snapshot")
    _require_non_empty_string(data, "dataset_version", "dataset_snapshot")
    _require_non_empty_string(data, "data_source", "dataset_snapshot")
    _require_non_empty_string(data, "instrument", "dataset_snapshot")
    _require_non_empty_string(data, "date_range_start", "dataset_snapshot")
    _require_non_empty_string(data, "date_range_end", "dataset_snapshot")
    _require_non_empty_string(data, "adjustment_mode", "dataset_snapshot")
    _require_non_empty_string(data, "cost_assumption", "dataset_snapshot")
    _require_non_empty_string(data, "missing_value_policy", "dataset_snapshot")
    _require_non_empty_string(data, "created_at", "dataset_snapshot")
    return data  # type: ignore[return-value]


def validate_metrics_summary(payload: MetricsSummary | dict[str, Any]) -> MetricsSummary:
    data = _require_mapping(payload, "metrics_summary")
    for field in ["total_return", "annual_return", "max_drawdown", "sharpe", "trade_count", "win_rate"]:
        if field not in data:
            raise ValueError(f"metrics_summary.{field} is required")
    _require_list(data, "notes", "metrics_summary")
    return data  # type: ignore[return-value]


def validate_risk_position_note(payload: RiskPositionNote | dict[str, Any]) -> RiskPositionNote:
    data = _require_mapping(payload, "risk_position_note")
    _require_non_empty_string(data, "position_sizing_method", "risk_position_note")
    if "max_position" not in data:
        raise ValueError("risk_position_note.max_position is required")
    if "risk_budget" not in data:
        raise ValueError("risk_position_note.risk_budget is required")
    if "drawdown_tolerance" not in data:
        raise ValueError("risk_position_note.drawdown_tolerance is required")
    data["risk_budget"] = str(data.get("risk_budget", "") or "")
    data["drawdown_tolerance"] = str(data.get("drawdown_tolerance", "") or "")
    _require_non_empty_string(data, "exit_after_signal_policy", "risk_position_note")
    _require_list(data, "notes", "risk_position_note")
    return data  # type: ignore[return-value]


def validate_execution_constraint(payload: ExecutionConstraint | dict[str, Any]) -> ExecutionConstraint:
    data = _require_mapping(payload, "execution_constraint")
    _require_non_empty_string(data, "execution_timing", "execution_constraint")
    _require_non_empty_string(data, "liquidity_requirement", "execution_constraint")
    _require_non_empty_string(data, "slippage_assumption", "execution_constraint")
    _require_non_empty_string(data, "holding_capacity", "execution_constraint")
    _require_list(data, "operational_constraints", "execution_constraint")
    _require_non_empty_string(data, "fit_for_operator", "execution_constraint")
    _require_non_empty_string(data, "created_at", "execution_constraint")
    return data  # type: ignore[return-value]


def validate_review_outcome(payload: ReviewOutcome | dict[str, Any]) -> ReviewOutcome:
    data = _require_mapping(payload, "review_outcome")
    _require_non_empty_string(data, "review_status", "review_outcome")
    _require_non_empty_string(data, "review_outcome", "review_outcome")
    _require_list(data, "key_risks", "review_outcome")
    _require_list(data, "gaps", "review_outcome")
    _require_non_empty_string(data, "recommended_next_step", "review_outcome")
    _require_non_empty_string(data, "reviewed_at", "review_outcome")
    return data  # type: ignore[return-value]


def validate_formal_review_record(payload: FormalReviewRecord | dict[str, Any]) -> FormalReviewRecord:
    data = _require_mapping(payload, "formal_review_record")
    for field in [
        "review_id",
        "experiment_id",
        "baseline_experiment_id",
        "review_scope",
        "review_question",
        "review_method",
        "comparison_summary",
        "decision_recommendation",
        "decision_reason",
        "reviewed_at",
    ]:
        _require_non_empty_string(data, field, "formal_review_record")
    _require_list(data, "risks", "formal_review_record")
    _require_list(data, "gaps", "formal_review_record")
    if "validation_record_ids" in data:
        _require_list(data, "validation_record_ids", "formal_review_record")
    if "search_spec_id" in data:
        data["search_spec_id"] = str(data.get("search_spec_id", "") or "")
    allowed = {"promote_to_baseline", "keep_as_candidate", "record_only", "reject"}
    if data["decision_recommendation"] not in allowed:
        raise ValueError("formal_review_record.decision_recommendation must be one of promote_to_baseline, keep_as_candidate, record_only, reject")
    return data  # type: ignore[return-value]


def validate_decision_status(payload: DecisionStatus | dict[str, Any]) -> DecisionStatus:
    data = _require_mapping(payload, "decision_status")
    _require_non_empty_string(data, "decision_status", "decision_status")
    if "is_baseline" not in data:
        raise ValueError("decision_status.is_baseline is required")
    if not isinstance(data["is_baseline"], bool):
        raise ValueError("decision_status.is_baseline must be a bool")
    if "baseline_of" not in data:
        raise ValueError("decision_status.baseline_of is required")
    data["baseline_of"] = str(data.get("baseline_of", "") or "")
    _require_non_empty_string(data, "decision_reason", "decision_status")
    _require_non_empty_string(data, "decided_at", "decision_status")
    return data  # type: ignore[return-value]


def validate_strategy_case_file(payload: StrategyCaseFile | dict[str, Any]) -> StrategyCaseFile:
    data = _require_mapping(payload, "strategy_case_file")
    _require_non_empty_string(data, "case_file_id", "strategy_case_file")
    _require_non_empty_string(data, "case_title", "strategy_case_file")
    _require_non_empty_string(data, "lifecycle_stage", "strategy_case_file")
    _require_non_empty_string(data, "current_status", "strategy_case_file")
    _require_non_empty_string(data, "current_hypothesis", "strategy_case_file")
    _require_list(data, "related_task_ids", "strategy_case_file")
    _require_list(data, "related_experiment_ids", "strategy_case_file")
    data["baseline_experiment_id"] = str(data.get("baseline_experiment_id", "") or "")
    data["current_experiment_id"] = str(data.get("current_experiment_id", "") or "")
    _require_non_empty_string(data, "created_at", "strategy_case_file")
    _require_non_empty_string(data, "updated_at", "strategy_case_file")
    return data  # type: ignore[return-value]


def validate_data_contract_spec(payload: DataContractSpec | dict[str, Any]) -> DataContractSpec:
    data = _require_mapping(payload, "data_contract_spec")
    for field in [
        "contract_id",
        "title",
        "data_source",
        "instrument",
        "date_column",
        "sort_column",
        "expected_date_range_start",
        "expected_date_range_end",
        "created_at",
    ]:
        _require_non_empty_string(data, field, "data_contract_spec")
    _require_list(data, "required_columns", "data_contract_spec")
    _require_list(data, "non_nullable_columns", "data_contract_spec")
    _require_list(data, "non_negative_columns", "data_contract_spec")
    _require_list(data, "validation_rules", "data_contract_spec")
    _require_int(data, "warmup_rows", "data_contract_spec")
    if not isinstance(data.get("instrument_bound_to_dataset"), bool):
        raise ValueError("data_contract_spec.instrument_bound_to_dataset must be a bool")
    return data  # type: ignore[return-value]


def validate_validation_record(payload: ValidationRecord | dict[str, Any]) -> ValidationRecord:
    data = _require_mapping(payload, "validation_record")
    for field in [
        "validation_id",
        "experiment_id",
        "task_id",
        "run_id",
        "title",
        "contract_id",
        "validation_method",
        "status_code",
        "summary",
        "created_at",
    ]:
        _require_non_empty_string(data, field, "validation_record")
    data["dataset_snapshot"] = validate_dataset_snapshot(data.get("dataset_snapshot", {}))
    data["rule_expression"] = validate_rule_expression(data.get("rule_expression", {}))
    data["metrics_summary"] = validate_metrics_summary(data.get("metrics_summary", {}))
    _require_list(data, "checks_passed", "validation_record")
    _require_list(data, "checks_failed", "validation_record")
    _require_int(data, "validated_rows", "validation_record")
    return data  # type: ignore[return-value]


def validate_variant_search_spec(payload: VariantSearchSpec | dict[str, Any]) -> VariantSearchSpec:
    data = _require_mapping(payload, "variant_search_spec")
    for field in [
        "search_id",
        "title",
        "strategy_family",
        "baseline_experiment_id",
        "objective_metric",
        "objective_mode",
        "created_at",
    ]:
        _require_non_empty_string(data, field, "variant_search_spec")
    _require_int(data, "max_trials", "variant_search_spec")
    _require_list(data, "constraints", "variant_search_spec")
    if not isinstance(data.get("parameter_space"), dict) or not data["parameter_space"]:
        raise ValueError("variant_search_spec.parameter_space must be a non-empty mapping")
    objective_mode = str(data.get("objective_mode", "") or "")
    if objective_mode not in {"maximize", "minimize"}:
        raise ValueError("variant_search_spec.objective_mode must be maximize or minimize")
    return data  # type: ignore[return-value]


def validate_experiment_run(payload: ExperimentRun | dict[str, Any]) -> ExperimentRun:
    data = _require_mapping(payload, "experiment_run")
    for field in [
        "experiment_id",
        "task_id",
        "run_id",
        "title",
        "strategy_family",
        "variant_name",
        "instrument",
        "artifact_root",
        "memory_note_path",
        "status_code",
        "created_at",
    ]:
        _require_non_empty_string(data, field, "experiment_run")
    data["dataset_snapshot"] = validate_dataset_snapshot(data.get("dataset_snapshot", {}))
    data["rule_expression"] = validate_rule_expression(data.get("rule_expression", {}))
    data["metrics_summary"] = validate_metrics_summary(data.get("metrics_summary", {}))
    data["risk_position_note"] = validate_risk_position_note(data.get("risk_position_note", {}))
    data["review_outcome"] = validate_review_outcome(data.get("review_outcome", {}))
    data["decision_status"] = validate_decision_status(data.get("decision_status", {}))
    if "opportunity_source" in data:
        data["opportunity_source"] = validate_opportunity_source(data.get("opportunity_source", {}))
    if "execution_constraint" in data:
        data["execution_constraint"] = validate_execution_constraint(data.get("execution_constraint", {}))
    if "case_file_id" in data:
        data["case_file_id"] = str(data.get("case_file_id", "") or "")
    if "validation_record_ids" in data:
        _require_list(data, "validation_record_ids", "experiment_run")
    if "search_spec_id" in data:
        data["search_spec_id"] = str(data.get("search_spec_id", "") or "")
    return data  # type: ignore[return-value]


def build_experiment_artifact_payload(*, research_task: ResearchTask | dict[str, Any], experiment_run: ExperimentRun | dict[str, Any]) -> dict[str, dict[str, Any]]:
    task = validate_research_task(research_task)
    run = validate_experiment_run(experiment_run)
    manifest = {
        "experiment_id": run["experiment_id"],
        "task_id": run["task_id"],
        "run_id": run["run_id"],
        "title": run["title"],
        "strategy_family": run["strategy_family"],
        "variant_name": run["variant_name"],
        "instrument": run["instrument"],
        "status_code": run["status_code"],
        "created_at": run["created_at"],
    }
    if run.get("case_file_id"):
        manifest["case_file_id"] = run["case_file_id"]
    if run.get("search_spec_id"):
        manifest["search_spec_id"] = run["search_spec_id"]
    if run.get("validation_record_ids"):
        manifest["validation_record_ids"] = run["validation_record_ids"]
    inputs = {
        "research_task": task,
        "rule_expression": run["rule_expression"],
        "dataset_snapshot": run["dataset_snapshot"],
    }
    if "opportunity_source" in run:
        inputs["opportunity_source"] = run["opportunity_source"]
    results = {
        "metrics_summary": run["metrics_summary"],
        "risk_position_note": run["risk_position_note"],
        "review_outcome": run["review_outcome"],
        "decision_status": run["decision_status"],
    }
    if "execution_constraint" in run:
        results["execution_constraint"] = run["execution_constraint"]
    return {"manifest": manifest, "inputs": inputs, "results": results}


def build_experiment_index_record(*, experiment_run: ExperimentRun | dict[str, Any]) -> dict[str, Any]:
    run = validate_experiment_run(experiment_run)
    dataset_snapshot = run["dataset_snapshot"]
    rule_expression = run["rule_expression"]
    decision_status = run["decision_status"]
    metrics_summary = run["metrics_summary"]
    review_outcome = run["review_outcome"]
    return {
        "project_id": str(run.get("project_id", "") or "ai-trading-system"),
        "experiment_id": run["experiment_id"],
        "task_id": run["task_id"],
        "run_id": run["run_id"],
        "title": run["title"],
        "strategy_family": run["strategy_family"],
        "variant_name": run["variant_name"],
        "instrument": run["instrument"],
        "data_source": dataset_snapshot["data_source"],
        "date_range_start": dataset_snapshot["date_range_start"],
        "date_range_end": dataset_snapshot["date_range_end"],
        "entry_rule_summary": rule_expression["entry_rule_summary"],
        "exit_rule_summary": rule_expression["exit_rule_summary"],
        "execution_assumption": rule_expression["execution_assumption"],
        "metrics_summary": metrics_summary,
        "review_outcome": review_outcome["review_outcome"],
        "memory_note_path": run["memory_note_path"],
        "artifact_root": run["artifact_root"],
        "status_code": run["status_code"],
        "created_at": run["created_at"],
        "dataset_version": dataset_snapshot["dataset_version"],
        "rules_version": rule_expression["rules_version"],
        "decision_status": decision_status["decision_status"],
        "is_baseline": decision_status["is_baseline"],
        "baseline_of": decision_status["baseline_of"],
        "cost_assumption": dataset_snapshot["cost_assumption"],
    }

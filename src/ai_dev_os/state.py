from typing import Any
from typing import Literal
from typing import TypedDict

try:
    from typing import NotRequired
except ImportError:  # Python < 3.11
    from typing_extensions import NotRequired


ApprovalStatus = Literal["not_needed", "pending", "approved", "changes_requested"]
RiskLevel = Literal["low", "medium", "high"]
HumanDecision = Literal["", "approved", "changes_requested"]
ReviewStatus = Literal["approved", "changes_requested"]
ValidationStatus = Literal["approved", "changes_requested"]
ProjectSizeBand = Literal["S", "M", "L"]


class ModeledTask(TypedDict, total=False):
    goal: str
    scope: list[str]
    out_of_scope: list[str]
    expected_artifacts: list[str]
    acceptance_criteria: list[str]
    constraints: list[str]
    task_kind: str
    project_size_band: ProjectSizeBand
    requires_approval: bool
    execution_scope: dict[str, Any]


class StandardTaskUnit(TypedDict, total=False):
    goal: str
    scope: list[str]
    out_of_scope: list[str]
    expected_artifacts: list[str]
    acceptance_criteria: list[str]
    risk_level: RiskLevel
    constraints: list[str]
    task_kind: str
    project_size_band: ProjectSizeBand
    requires_approval: bool
    execution_scope: dict[str, Any]


class BuilderWorkingState(TypedDict, total=False):
    current_primary_goal: str
    current_subgoal: str
    accepted_scope: list[str]
    blocked_points: list[str]
    last_outcome: str
    next_step: str
    confirmed_anchors: list[str]
    grounded_data_sources: list[str]
    forbidden_patterns: list[str]
    validation_history: list[str]
    failure_summary: str
    failure_codes: list[str]
    failed_targets: list[str]
    repair_hints: list[str]
    updated_at: str
    updated_by: str
    cycle_index: int


class ProcessEvent(TypedDict, total=False):
    event_id: str
    timestamp: str
    project_id: str
    task_id: str
    node: str
    event_type: str
    status: str
    summary: str
    target: str
    duration_ms: int
    metadata: dict[str, Any]


class TaskCard(TypedDict):
    task_id: str
    goal: str
    task_profile: str
    scope_hint: list[str]
    constraints: list[str]
    acceptance_tests: list[str]
    risk_level: RiskLevel
    assigned_agents: list[str]
    orchestrator_brief: str
    scope: list[str]
    forbidden_changes: list[str]
    acceptance_criteria: list[str]
    rollback_plan: str
    memory_update_requirement: str
    out_of_scope: NotRequired[list[str]]
    expected_artifacts: NotRequired[list[str]]
    task_kind: NotRequired[str]
    project_size_band: NotRequired[ProjectSizeBand]
    requires_approval: NotRequired[bool]
    execution_scope: NotRequired[dict[str, Any]]
    raw_intent: NotRequired[str]
    intake_source: NotRequired[str]
    run_id: NotRequired[str]
    sample_id: NotRequired[str]
    executor_id: NotRequired[str]
    started_at: NotRequired[str]


class KernelInput(TypedDict):
    project_id: str
    goal: str
    human_decision: NotRequired[HumanDecision]
    raw_intent: NotRequired[str]
    modeled_task: NotRequired[ModeledTask]
    standard_task_unit: NotRequired[StandardTaskUnit]
    target_workspace_root: NotRequired[str]
    run_id: NotRequired[str]
    sample_id: NotRequired[str]
    executor_id: NotRequired[str]
    started_at: NotRequired[str]


class KernelState(TypedDict):
    project_id: str
    goal: str
    task_kind: str
    active_phase: str
    active_agent: str
    blocking_issue: str
    task_card: TaskCard
    scan_result: str
    build_result: str
    execution_result: str
    review_result: str
    review_status: ReviewStatus
    review_feedback: str
    validation_result: str
    validation_status: ValidationStatus
    validation_feedback: str
    risk_level: RiskLevel
    approval_required: bool
    approval_status: ApprovalStatus
    human_decision: HumanDecision
    recorder_summary: str
    artifacts: dict[str, Any]
    steps: list[str]
    rework_count: int
    max_rework_rounds: int




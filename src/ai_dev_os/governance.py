from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_GOVERNANCE = {
    "risk_keywords": ["architecture", "protocol", "schema", "delete", "migration"],
    "rework_keywords": ["refine", "iterate", "improve", "polish", "optimize"],
    "execution_whitelist": [
        "write_build_plan_json",
        "write_execution_runtime_plan",
        "write_execution_brief",
        "write_validation_checklist",
    ],
    "approval_required_risk_levels": ["high"],
    "project_constraints": {
        "allow_doctrine_modification_without_approval": False,
        "allow_external_runtime_write": False,
        "require_memory_update_after_record": True,
    },
    "task_constraints": {
        "default_scope": [
            "Analyze relevant context",
            "Draft implementation plan",
            "Review results and record state",
        ],
        "default_acceptance_criteria": [
            "Task card created",
            "Scan/build/review/record loop completed",
            "Control tower updated",
        ],
        "default_assigned_agents": ["builder", "reviewer", "recorder"],
        "rollback_plan": "Discard project runtime folder and rerun from mother template.",
        "memory_update_requirement": "Write task card, logs, and phase status to project memory.",
    },
    "optimization_task_constraints": {
        "default_scope": [
            "Inspect the current mother-template behavior",
            "Propose a bounded system improvement",
            "Validate impact before recording optimization result",
        ],
        "default_acceptance_criteria": [
            "Optimization task card created",
            "System improvement path executed through validator checkpoint",
            "Optimization goal documented for release review",
        ],
        "default_assigned_agents": ["builder", "reviewer", "validator", "recorder"],
        "rollback_plan": "Revert to the previous stable tag and compare system behavior before retrying the optimization task.",
        "memory_update_requirement": "Write rationale, validation result, and optimization outcome to system evolution memory.",
    },
    "forbidden_changes": [
        "Do not modify doctrine without human approval",
        "Do not touch external project runtime outside this project_id",
    ],
    "optimization_project_constraints": {
        "require_release_review_after_record": True,
        "require_human_approval_even_if_low_risk": True,
    },
}


@lru_cache(maxsize=1)
def load_governance() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parents[2] / "config" / "governance.json"
    if not config_path.exists():
        return DEFAULT_GOVERNANCE
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return {
        "risk_keywords": payload.get("risk_keywords", DEFAULT_GOVERNANCE["risk_keywords"]),
        "rework_keywords": payload.get("rework_keywords", DEFAULT_GOVERNANCE["rework_keywords"]),
        "execution_whitelist": payload.get("execution_whitelist", DEFAULT_GOVERNANCE["execution_whitelist"]),
        "approval_required_risk_levels": payload.get(
            "approval_required_risk_levels",
            DEFAULT_GOVERNANCE["approval_required_risk_levels"],
        ),
        "project_constraints": payload.get("project_constraints", DEFAULT_GOVERNANCE["project_constraints"]),
        "task_constraints": payload.get("task_constraints", DEFAULT_GOVERNANCE["task_constraints"]),
        "optimization_task_constraints": payload.get(
            "optimization_task_constraints",
            DEFAULT_GOVERNANCE["optimization_task_constraints"],
        ),
        "forbidden_changes": payload.get("forbidden_changes", DEFAULT_GOVERNANCE["forbidden_changes"]),
        "optimization_project_constraints": payload.get(
            "optimization_project_constraints",
            DEFAULT_GOVERNANCE["optimization_project_constraints"],
        ),
    }


def risk_keywords() -> tuple[str, ...]:
    return tuple(str(item).lower() for item in load_governance()["risk_keywords"])


def rework_keywords() -> tuple[str, ...]:
    return tuple(str(item).lower() for item in load_governance()["rework_keywords"])


def execution_whitelist() -> tuple[str, ...]:
    return tuple(str(item) for item in load_governance()["execution_whitelist"])


def approval_required_risk_levels() -> tuple[str, ...]:
    return tuple(str(item).lower() for item in load_governance()["approval_required_risk_levels"])


def requires_human_approval(risk_level: str, task_kind: str = "standard") -> bool:
    if task_kind == "system_optimization" and optimization_project_constraints().get(
        "require_human_approval_even_if_low_risk",
        False,
    ):
        return True
    return risk_level.lower() in approval_required_risk_levels()


def forbidden_changes() -> tuple[str, ...]:
    return tuple(str(item) for item in load_governance()["forbidden_changes"])


def task_constraints() -> dict[str, Any]:
    return dict(load_governance()["task_constraints"])


def optimization_task_constraints() -> dict[str, Any]:
    return dict(load_governance()["optimization_task_constraints"])


def project_constraints() -> dict[str, Any]:
    return dict(load_governance()["project_constraints"])


def optimization_project_constraints() -> dict[str, Any]:
    return dict(load_governance()["optimization_project_constraints"])

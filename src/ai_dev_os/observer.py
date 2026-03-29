from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.feedback_protocol import classify_observer_feedback
from ai_dev_os.io_utils import append_markdown
from ai_dev_os.io_utils import now_iso
from ai_dev_os.io_utils import write_json
from ai_dev_os.system_metrics import compare_with_source_baseline
from ai_dev_os.system_metrics import record_project_metrics


def _system_evolution_root(paths: dict[str, str]) -> Path:
    root = Path(paths["memory_root"]) / "system_evolution"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _slugify(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_")


def _extract_observations(state: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    observations: list[str] = []
    improvements: list[dict[str, Any]] = []

    if state.get("rework_count", 0) > 0:
        observations.append(f"Workflow required {state['rework_count']} rework round(s) before record.")
        improvements.append(
            {
                "type": "workflow_improvement",
                "title": "Reduce rework loops",
                "priority": "medium",
                "reason": f"Task needed {state['rework_count']} rework round(s).",
                "suggested_area": "builder_prompt_or_reviewer_policy",
            }
        )

    if state.get("approval_status") == "pending":
        observations.append("Run stopped at human approval gate.")
        improvements.append(
            {
                "type": "governance_review",
                "title": "Review high-risk approval burden",
                "priority": "medium",
                "reason": "Task is blocked on approval.",
                "suggested_area": "approval_policy",
            }
        )

    review_result = state.get("review_result", "")
    if "validation_gaps=" in review_result:
        observations.append("Reviewer reported validation gaps.")
        improvements.append(
            {
                "type": "validation_gap",
                "title": "Strengthen execution evidence",
                "priority": "high",
                "reason": "Reviewer output still references validation gaps.",
                "suggested_area": "execution_artifacts_or_validation_checks",
            }
        )

    if "issues=" in review_result:
        observations.append("Reviewer reported implementation issues.")
        improvements.append(
            {
                "type": "quality_issue",
                "title": "Tighten builder output quality",
                "priority": "medium",
                "reason": "Reviewer output still references issues.",
                "suggested_area": "builder_prompt",
            }
        )

    execution_result = state.get("execution_result", "")
    if "Warnings:" in execution_result:
        observations.append("Execution layer reported warnings.")
        improvements.append(
            {
                "type": "execution_warning",
                "title": "Reduce execution warnings",
                "priority": "high",
                "reason": execution_result,
                "suggested_area": "execution_engine",
            }
        )

    if not observations:
        observations.append("Run completed without obvious system-level issues.")

    return observations, improvements


def _candidate_goal(item: dict[str, Any]) -> str:
    area = item.get("suggested_area", "system_quality")
    title = item.get("title", "Improve system behavior")
    return f"Improve {area} to address: {title}"


def _candidate_acceptance_criteria(item: dict[str, Any]) -> list[str]:
    area = item.get("suggested_area", "system_quality")
    return [
        f"Document the current issue in {area}.",
        "Propose one concrete adjustment and its expected effect.",
        "Define one measurable validation check for the change.",
    ]


def _build_task_candidates(
    state: dict[str, Any],
    improvements: list[dict[str, Any]],
    observed_at: str,
) -> list[dict[str, Any]]:
    task_id = state["task_card"]["task_id"]
    project_id = state["project_id"]
    candidates: list[dict[str, Any]] = []
    for index, item in enumerate(improvements, start=1):
        slug = _slugify(item.get("title", f"improvement_{index}")) or f"improvement_{index}"
        candidate_id = f"{project_id}-{task_id}-{slug}-{index:02d}"
        candidates.append(
            {
                "candidate_id": candidate_id,
                "source_task_id": task_id,
                "source_project_id": project_id,
                "created_at": observed_at,
                "type": item.get("type", "system_improvement"),
                "title": item.get("title", "System improvement candidate"),
                "priority": item.get("priority", "medium"),
                "reason": item.get("reason", ""),
                "suggested_area": item.get("suggested_area", "system_quality"),
                "proposed_goal": _candidate_goal(item),
                "acceptance_criteria": _candidate_acceptance_criteria(item),
                "requires_human_review": True,
                "status": "proposed",
            }
        )
    return candidates


def _extract_metrics(state: dict[str, Any]) -> dict[str, Any]:
    execution_result = state.get("execution_result", "")
    review_result = state.get("review_result", "")
    return {
        "rework_count": state.get("rework_count", 0),
        "approval_pending": state.get("approval_status") == "pending",
        "approval_required": bool(state.get("approval_required", False)),
        "execution_warning_count": execution_result.count("Warnings:"),
        "review_issue_count": review_result.count("issues="),
        "review_validation_gap_count": review_result.count("validation_gaps="),
        "validation_status": state.get("validation_status", ""),
        "review_status": state.get("review_status", ""),
        "risk_level": state.get("risk_level", ""),
        "task_kind": state.get("task_kind", "standard"),
    }


def _compare_with_previous_run(root: Path, current_metrics: dict[str, Any]) -> dict[str, Any]:
    reports = sorted(root.glob("run_report_*.json"))
    if not reports:
        return {
            "has_previous_baseline": False,
            "summary": "No previous observer baseline for comparison.",
            "changes": {},
        }

    previous = json.loads(reports[-1].read_text(encoding="utf-8"))
    baseline = previous.get("metrics", {})
    changes: dict[str, Any] = {}
    for key in (
        "rework_count",
        "execution_warning_count",
        "review_issue_count",
        "review_validation_gap_count",
    ):
        if key in baseline:
            changes[key] = {
                "previous": baseline.get(key),
                "current": current_metrics.get(key),
                "delta": current_metrics.get(key, 0) - baseline.get(key, 0),
            }

    summary_parts: list[str] = []
    if "rework_count" in changes:
        delta = changes["rework_count"]["delta"]
        if delta < 0:
            summary_parts.append("rework_count improved")
        elif delta > 0:
            summary_parts.append("rework_count worsened")
    if "execution_warning_count" in changes:
        delta = changes["execution_warning_count"]["delta"]
        if delta < 0:
            summary_parts.append("execution warnings reduced")
        elif delta > 0:
            summary_parts.append("execution warnings increased")
    if "review_validation_gap_count" in changes:
        delta = changes["review_validation_gap_count"]["delta"]
        if delta < 0:
            summary_parts.append("validation gaps reduced")
        elif delta > 0:
            summary_parts.append("validation gaps increased")

    summary = ", ".join(summary_parts) if summary_parts else "Metrics changed but no clear improvement signal yet."
    return {
        "has_previous_baseline": True,
        "summary": summary,
        "changes": changes,
    }


def run_meta_observer(state: dict[str, Any], paths: dict[str, str]) -> dict[str, Any]:
    root = _system_evolution_root(paths)
    observed_at = now_iso()
    observations, improvements = _extract_observations(state)
    candidates = _build_task_candidates(state, improvements, observed_at)
    metrics = _extract_metrics(state)
    comparison = _compare_with_previous_run(root, metrics)
    source_comparison = compare_with_source_baseline(state=state, metrics=metrics, paths=paths)
    record_project_metrics(state=state, metrics=metrics, paths=paths, observed_at=observed_at)

    status_payload = {
        "status": "observer_v1_active",
        "last_observed_at": observed_at,
        "project_id": state["project_id"],
        "task_id": state["task_card"]["task_id"],
        "observation_count": len(observations),
        "improvement_count": len(improvements),
        "candidate_count": len(candidates),
        "latest_metrics": metrics,
        "comparison_summary": comparison["summary"],
        "source_comparison_summary": source_comparison["summary"],
    }
    status_payload["feedback_signal"] = classify_observer_feedback(
        observations=observations,
        improvements=improvements,
        approval_pending=state.get("approval_status") == "pending",
    )
    status_path = root / "observer_status.json"
    write_json(status_path, status_payload)

    run_report = {
        "observed_at": observed_at,
        "project_id": state["project_id"],
        "task_id": state["task_card"]["task_id"],
        "phase": state["active_phase"],
        "approval_status": state["approval_status"],
        "review_status": state["review_status"],
        "risk_level": state["risk_level"],
        "rework_count": state["rework_count"],
        "metrics": metrics,
        "comparison": comparison,
        "source_comparison": source_comparison,
        "feedback_signal": status_payload["feedback_signal"],
        "observations": observations,
        "improvements": improvements,
        "task_candidates": candidates,
    }
    report_path = root / f"run_report_{observed_at.replace(':', '-').replace('.', '-')}.json"
    write_json(report_path, run_report)

    append_markdown(
        root / "system_observations.md",
        "\n".join(
            [
                f"## Run Observation",
                f"- at: {observed_at}",
                f"- project_id: {state['project_id']}",
                f"- task_id: {state['task_card']['task_id']}",
                f"- metrics: {json.dumps(metrics, ensure_ascii=False)}",
                f"- comparison: {comparison['summary']}",
                f"- source_comparison: {source_comparison['summary']}",
                f"- feedback_signal: {json.dumps(status_payload['feedback_signal'], ensure_ascii=False)}",
                *[f"- note: {item}" for item in observations],
            ]
        ),
    )

    backlog_path = root / "improvement_backlog.json"
    backlog = _load_json(backlog_path, {"items": []})
    backlog.setdefault("items", []).extend(improvements)
    write_json(backlog_path, backlog)

    candidates_path = root / "improvement_task_candidates.json"
    candidate_payload = _load_json(candidates_path, {"items": []})
    candidate_payload.setdefault("items", []).extend(candidates)
    write_json(candidates_path, candidate_payload)

    cards_path = root / "improvement_task_cards.md"
    if candidates:
        sections: list[str] = []
        for item in candidates:
            sections.extend(
                [
                    "## Improvement Task Candidate",
                    f"- candidate_id: {item['candidate_id']}",
                    f"- title: {item['title']}",
                    f"- priority: {item['priority']}",
                    f"- suggested_area: {item['suggested_area']}",
                    f"- proposed_goal: {item['proposed_goal']}",
                    f"- requires_human_review: {str(item['requires_human_review']).lower()}",
                    "- acceptance_criteria:",
                    *[f"  - {criterion}" for criterion in item["acceptance_criteria"]],
                ]
            )
        append_markdown(cards_path, "\n".join(sections))

    summary_path = root / "weekly_meta_summary.md"
    append_markdown(
        summary_path,
        "\n".join(
            [
                "## Observer Summary",
                f"- at: {observed_at}",
                f"- observations: {len(observations)}",
                f"- improvements: {len(improvements)}",
                f"- task_candidates: {len(candidates)}",
                f"- comparison: {comparison['summary']}",
                f"- source_comparison: {source_comparison['summary']}",
                f"- latest_project: {state['project_id']}",
            ]
        ),
    )

    return {
        "status": status_payload["status"],
        "path": str(status_path),
        "report_path": str(report_path),
        "improvement_count": len(improvements),
        "candidate_count": len(candidates),
        "feedback_signal": status_payload["feedback_signal"],
        "source_comparison": source_comparison,
    }

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from pathlib import Path

from ai_dev_os.feedback_protocol import classify_release_feedback


ROOT = Path(__file__).resolve().parents[2]
RELEASE_ADVISOR_PATH = ROOT / "control_tower" / "release_advisor.json"


def _git_output(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )
    return completed.stdout.strip()


def _safe_git_output(*args: str) -> str:
    try:
        return _git_output(*args)
    except subprocess.CalledProcessError:
        return ""


def _dirty_entries() -> list[str]:
    output = _safe_git_output("status", "--short")
    if not output:
        return []
    return [line for line in output.splitlines() if line.strip()]


def _tracked_core_changes(entries: list[str]) -> list[str]:
    core_markers = (
        "src/ai_dev_os/",
        "scripts/dashboard_server.py",
        "config/",
        "langgraph.json",
    )
    changed: list[str] = []
    for line in entries:
        normalized = line.replace("\\", "/")
        if any(marker in normalized for marker in core_markers):
            changed.append(normalized)
    return changed


def _task_release_checks(task_context: Mapping[str, object]) -> tuple[str, str, list[str], dict[str, object]]:
    issues: list[str] = []
    evidence: dict[str, object] = {}

    status = str(task_context.get("status", ""))
    approval_status = str(task_context.get("approval_status", ""))
    validation_status = str(task_context.get("validation_status", ""))
    review_status = str(task_context.get("review_status", ""))
    risk_level = str(task_context.get("risk_level", ""))
    rework_count = int(task_context.get("rework_count", 0) or 0)
    source_project_id = str(task_context.get("source_project_id", ""))
    rollback_plan = str(task_context.get("rollback_plan", ""))
    last_summary = str(task_context.get("last_summary", ""))
    feedback_summary = str(task_context.get("feedback_summary", ""))
    interrupt_required = bool(task_context.get("interrupt_required", False))
    attention_light = str(task_context.get("attention_light", "green"))

    evidence["status"] = status or "unknown"
    evidence["approval_status"] = approval_status or "unknown"
    evidence["validation_status"] = validation_status or "unknown"
    evidence["review_status"] = review_status or "unknown"
    evidence["risk_level"] = risk_level or "unknown"
    evidence["rework_count"] = rework_count
    evidence["attention_light"] = attention_light or "unknown"

    if status != "completed":
        issues.append("optimization task has not completed yet")
    if approval_status != "approved":
        issues.append("optimization task has not cleared human approval")
    if validation_status and validation_status != "approved":
        issues.append("optimization task has unresolved validator status")
    if review_status and review_status != "approved":
        issues.append("optimization task has unresolved reviewer status")
    if not source_project_id:
        issues.append("optimization task is missing source project lineage")
    if not rollback_plan:
        issues.append("optimization task is missing rollback plan")
    if not last_summary:
        issues.append("optimization task is missing release summary evidence")
    if interrupt_required or attention_light == "red":
        issues.append("side-chain still marks the optimization task as interrupt-worthy")

    recommendation = "stable"
    summary = "Optimization task satisfies the current specialized release checks."
    actions = [
        "Confirm the optimization outcome is recorded in mother memory before release review.",
        "Keep rollback plan and source-project linkage visible in the release record.",
    ]

    if issues:
        recommendation = "hold_release"
        summary = "Optimization task is not ready for specialized release review."
        actions = ["Resolve the task-specific release issues before approval.", *[f"Fix: {item}" for item in issues[:4]]]
    elif rework_count > 0 or attention_light == "yellow" or not feedback_summary.startswith("No side-chain feedback"):
        recommendation = "review_before_release"
        summary = "Optimization task is complete, but a human should review residual caution signals before release approval."
        actions = [
            "Review residual rework and side-chain caution signals.",
            "Confirm that the optimization still represents an improvement over the source project.",
        ]

    return recommendation, summary, actions, {"issues": issues, "evidence": evidence}


def build_task_release_advice(task_context: Mapping[str, object]) -> dict[str, object]:
    recommendation, summary, actions, detail = _task_release_checks(task_context)
    return {
        "recommendation": recommendation,
        "summary": summary,
        "suggested_actions": actions,
        **detail,
    }


def build_release_advice(task_context: Mapping[str, object] | None = None) -> dict[str, object]:
    dirty = _dirty_entries()
    head = _safe_git_output("rev-parse", "--short", "HEAD")
    branch = _safe_git_output("rev-parse", "--abbrev-ref", "HEAD")
    latest_tag = _safe_git_output("describe", "--tags", "--abbrev=0")
    core_changes = _tracked_core_changes(dirty)
    push_state = _safe_git_output("status", "-sb").splitlines()
    ahead_line = push_state[0] if push_state else ""

    recommendation = "stable"
    summary = "当前版本状态稳定，可继续开发。"
    actions: list[str] = []

    if dirty:
        recommendation = "hold_release"
        summary = "工作区有未清理变更，不建议直接发布或打新标签。"
        actions.extend(
            [
                "先确认哪些变更属于当前阶段，哪些是运行时产物。",
                "只对稳定完成的能力做提交和标签。",
            ]
        )
    if core_changes:
        recommendation = "review_before_release"
        summary = "检测到核心层文件变更，建议先做一次验证再考虑标签。"
        actions.extend(
            [
                "优先验证 graph、agents、dashboard 相关链路。",
                "验证通过后再决定是否打阶段标签。",
            ]
        )
    if "ahead" in ahead_line:
        actions.append("当前本地分支领先远端，建议推送后再做发布判断。")

    if not actions:
        actions.append("当前不需要额外发布操作。")

    task_release: dict[str, object] | None = None
    if task_context:
        task_release = build_task_release_advice(task_context)
        task_recommendation = str(task_release["recommendation"])
        task_summary = str(task_release["summary"])
        task_actions = list(task_release["suggested_actions"])
        if task_recommendation == "hold_release":
            recommendation = "hold_release"
            summary = "系统优化任务未满足专用发布条件，当前不应通过发布复核。"
            actions = [*task_actions, *actions]
        elif task_recommendation == "review_before_release" and recommendation == "stable":
            recommendation = "review_before_release"
            summary = "系统优化任务已完成，但仍需按专用发布标准做一次人工复核。"
            actions = [*task_actions, *actions]

    payload = {
        "status": recommendation,
        "summary": summary,
        "branch": branch or "unknown",
        "head": head or "unknown",
        "latest_tag": latest_tag or "none",
        "dirty_count": len(dirty),
        "dirty_entries": dirty[:20],
        "core_change_count": len(core_changes),
        "core_changes": core_changes[:20],
        "suggested_actions": actions,
    }
    if task_release:
        payload["task_release"] = task_release
    payload["feedback_signal"] = classify_release_feedback(
        recommendation=recommendation,
        dirty_count=payload["dirty_count"],
        core_change_count=payload["core_change_count"],
    )
    RELEASE_ADVISOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    RELEASE_ADVISOR_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload



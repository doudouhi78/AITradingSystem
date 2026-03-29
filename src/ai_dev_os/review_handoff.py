from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.io_utils import write_json


def build_reviewer_handoff_packet(
    *,
    state: dict[str, Any],
    execution_report: dict[str, Any],
    execution_evidence: dict[str, Any],
    target_workspace_root: str,
    source_workspace_root: str = "",
    patch_path: str = "",
) -> dict[str, Any]:
    project_id = str(state.get("project_id", "") or "")
    task_card = dict(state.get("task_card", {}) or {})
    task_id = str(task_card.get("task_id", "") or "")
    runtime = dict(execution_report.get("runtime", {}) or {})
    runtime_execution = dict(runtime.get("execution", {}) or {})
    runtime_actions = list(runtime_execution.get("actions", []) or [])
    command_results: list[dict[str, Any]] = []
    for item in runtime_actions[:12]:
        command_results.append(
            {
                "kind": str(item.get("kind", "") or ""),
                "status": str(item.get("status", "") or ""),
                "returncode": item.get("returncode"),
                "duration_ms": int(item.get("duration_ms", 0) or 0),
                "stdout_excerpt": str(item.get("stdout", "") or "")[:240],
                "stderr_excerpt": str(item.get("stderr", "") or "")[:240],
            }
        )
    test_results = {
        "runtime_status": str(execution_evidence.get("runtime_status", "") or ""),
        "git_diff_status": str(execution_evidence.get("git_diff_status", "") or ""),
        "lint_status": str(execution_evidence.get("lint_status", "") or ""),
        "pytest_status": str(execution_evidence.get("pytest_status", "") or ""),
        "compile_status": str(execution_evidence.get("compile_status", "") or ""),
        "install_dep_status": str(execution_evidence.get("install_dep_status", "") or ""),
    }
    review_focus_points: list[str] = []
    for key in ("git_diff_status", "lint_status", "pytest_status", "compile_status", "install_dep_status"):
        value = str(test_results.get(key, "") or "")
        if value and value != "passed":
            review_focus_points.append(f"{key}={value}")
    for warning in list(execution_evidence.get("warnings", []) or [])[:4]:
        review_focus_points.append(f"warning: {str(warning)}")
    if not review_focus_points:
        review_focus_points.append("Review changed files and verify builder output matches execution evidence.")
    review_focus_points = [str(item)[:180] for item in review_focus_points[:6]]
    builder_self_assessment = {
        "changed_scope": list(execution_evidence.get("changed_files", []) or [])[:8],
        "known_risks": [str(item)[:160] for item in list(execution_evidence.get("warnings", []) or [])[:4]],
        "unverified_points": [
            key for key, value in test_results.items()
            if str(value or "").strip() and str(value or "").strip() != "passed"
        ][:5],
        "confidence": "medium" if review_focus_points else "high",
    }
    return {
        "schema_version": "reviewer_handoff_packet.v1",
        "project_id": project_id,
        "task_id": task_id,
        "source_role": "builder_execution",
        "target_role": "reviewer",
        "target_workspace_root": target_workspace_root,
        "source_workspace_root": str(source_workspace_root or ""),
        "goal": str(state.get("goal", "") or ""),
        "rework_count": int(state.get("rework_count", 0) or 0),
        "execution_summary": str(execution_report.get("summary", "") or ""),
        "changed_files": list(execution_evidence.get("changed_files", []) or [])[:20],
        "patch_path": str(patch_path or ""),
        "git_diff_summary": str(execution_evidence.get("git_diff_summary", "") or ""),
        "command_results": command_results,
        "test_results": test_results,
        "builder_self_assessment": builder_self_assessment,
        "blocker": str(state.get("blocking_issue", "") or state.get("review_feedback", "") or ""),
        "review_focus_points": review_focus_points,
        "truncation_flags": {
            "review_focus_points": len(review_focus_points) >= 6,
        },
    }


def persist_reviewer_handoff_packet(*, project_root: Path, payload: dict[str, Any]) -> Path:
    path = project_root / "artifacts" / "reviewer_handoff_packet.json"
    write_json(path, payload)
    return path


def persist_reviewer_patch_artifact(*, project_root: Path, patch_text: str) -> Path:
    path = project_root / "artifacts" / "reviewer_patch.diff"
    path.write_text(str(patch_text or ""), encoding="utf-8")
    return path


def load_reviewer_handoff_packet(*, project_root: Path) -> dict[str, Any]:
    path = project_root / "artifacts" / "reviewer_handoff_packet.json"
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
MOTHER_MEMORY_ROOT = ROOT / "mother_memory"
METRICS_ROOT = MOTHER_MEMORY_ROOT / "metrics"
PROJECT_BASELINES_PATH = METRICS_ROOT / "project_baselines.json"
OPTIMIZATION_HISTORY_PATH = METRICS_ROOT / "optimization_effectiveness.json"


def _load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _optimization_metadata(paths: dict[str, str]) -> dict[str, Any]:
    project_root = Path(paths["project_root"])
    path = project_root / "artifacts" / "optimization_metadata.json"
    return _load_json(path, {}) if path.exists() else {}


def record_project_metrics(*, state: dict[str, Any], metrics: dict[str, Any], paths: dict[str, str], observed_at: str) -> None:
    payload = _load_json(PROJECT_BASELINES_PATH, {"projects": {}})
    project_id = state["project_id"]
    payload.setdefault("projects", {})[project_id] = {
        "project_id": project_id,
        "task_kind": state.get("task_kind", "standard"),
        "observed_at": observed_at,
        "metrics": metrics,
    }
    _write_json(PROJECT_BASELINES_PATH, payload)

    metadata = _optimization_metadata(paths)
    source_project_id = metadata.get("source_project_id", "")
    if state.get("task_kind") == "system_optimization" and source_project_id:
        history = _load_json(OPTIMIZATION_HISTORY_PATH, {"items": []})
        items = [item for item in history.get("items", []) if item.get("project_id") != project_id]
        items.append(
            {
                "project_id": project_id,
                "source_project_id": source_project_id,
                "source_candidate_id": metadata.get("source_candidate_id", ""),
                "recorded_at": observed_at,
                "metrics": metrics,
            }
        )
        history["items"] = items
        _write_json(OPTIMIZATION_HISTORY_PATH, history)


def compare_with_source_baseline(*, state: dict[str, Any], metrics: dict[str, Any], paths: dict[str, str]) -> dict[str, Any]:
    metadata = _optimization_metadata(paths)
    source_project_id = metadata.get("source_project_id", "")
    if state.get("task_kind") != "system_optimization" or not source_project_id:
        return {
            "has_source_baseline": False,
            "summary": "No cross-project source baseline available.",
            "changes": {},
            "source_project_id": source_project_id,
        }

    baselines = _load_json(PROJECT_BASELINES_PATH, {"projects": {}})
    source = baselines.get("projects", {}).get(source_project_id)
    if not source:
        return {
            "has_source_baseline": False,
            "summary": f"No stored baseline found for source project {source_project_id}.",
            "changes": {},
            "source_project_id": source_project_id,
        }

    baseline = source.get("metrics", {})
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
                "current": metrics.get(key),
                "delta": metrics.get(key, 0) - baseline.get(key, 0),
            }

    summary_parts: list[str] = []
    if "rework_count" in changes:
        delta = changes["rework_count"]["delta"]
        if delta < 0:
            summary_parts.append("rework_count improved vs source project")
        elif delta > 0:
            summary_parts.append("rework_count worsened vs source project")
    if "execution_warning_count" in changes:
        delta = changes["execution_warning_count"]["delta"]
        if delta < 0:
            summary_parts.append("execution warnings reduced vs source project")
        elif delta > 0:
            summary_parts.append("execution warnings increased vs source project")
    if "review_validation_gap_count" in changes:
        delta = changes["review_validation_gap_count"]["delta"]
        if delta < 0:
            summary_parts.append("validation gaps reduced vs source project")
        elif delta > 0:
            summary_parts.append("validation gaps increased vs source project")

    summary = ", ".join(summary_parts) if summary_parts else "Cross-project comparison found no clear improvement signal yet."
    return {
        "has_source_baseline": True,
        "summary": summary,
        "changes": changes,
        "source_project_id": source_project_id,
    }

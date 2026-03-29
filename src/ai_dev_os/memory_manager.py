from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.io_utils import append_markdown
from ai_dev_os.io_utils import now_iso
from ai_dev_os.io_utils import write_json


def _memory_root(paths: dict[str, str]) -> Path:
    return Path(paths["memory_root"])


def _snapshots_dir(paths: dict[str, str]) -> Path:
    root = _memory_root(paths) / "snapshots"
    root.mkdir(parents=True, exist_ok=True)
    return root


def initialize_memory_indexes(project_id: str, task_card: dict[str, Any], paths: dict[str, str]) -> None:
    memory_root = _memory_root(paths)
    index_path = memory_root / "memory_index.json"
    timeline_path = memory_root / "timeline.md"
    if not index_path.exists():
        write_json(
            index_path,
            {
                "project_id": project_id,
                "created_at": now_iso(),
                "latest_task_id": task_card["task_id"],
                "entries": [],
            },
        )
    if not timeline_path.exists():
        timeline_path.write_text("# Project Timeline\n", encoding="utf-8")
    append_timeline_entry(
        paths,
        title="Task Created",
        lines=[
            f"task_id: {task_card['task_id']}",
            f"goal: {task_card['goal']}",
            "phase: task_card_created",
        ],
    )
    append_memory_index(
        paths,
        {
            "timestamp": now_iso(),
            "type": "task_created",
            "task_id": task_card["task_id"],
            "phase": "task_card_created",
        },
    )


def append_timeline_entry(paths: dict[str, str], *, title: str, lines: list[str]) -> None:
    timeline_path = _memory_root(paths) / "timeline.md"
    content = [f"## {title}", f"- at: {now_iso()}"]
    content.extend(f"- {line}" for line in lines)
    append_markdown(timeline_path, "\n".join(content))


def append_memory_index(paths: dict[str, str], entry: dict[str, Any]) -> None:
    index_path = _memory_root(paths) / "memory_index.json"
    if index_path.exists():
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    else:
        payload = {"project_id": "", "created_at": now_iso(), "latest_task_id": "", "entries": []}
    payload["latest_task_id"] = entry.get("task_id", payload.get("latest_task_id", ""))
    payload.setdefault("entries", []).append(entry)
    write_json(index_path, payload)


def archive_phase_snapshot(paths: dict[str, str], *, phase: str, state: dict[str, Any]) -> str:
    snapshot_path = _snapshots_dir(paths) / f"{phase}_{now_iso().replace(':', '-').replace('.', '-')}.json"
    write_json(snapshot_path, state)
    append_memory_index(
        paths,
        {
            "timestamp": now_iso(),
            "type": "snapshot",
            "task_id": state["task_card"]["task_id"],
            "phase": phase,
            "path": str(snapshot_path),
        },
    )
    return str(snapshot_path)


def write_project_summary(paths: dict[str, str], state: dict[str, Any]) -> str:
    summary_path = _memory_root(paths) / "project_state" / "latest_summary.md"
    lines = [
        "# Latest Project Summary",
        f"- updated_at: {now_iso()}",
        f"- project_id: {state['project_id']}",
        f"- task_id: {state['task_card']['task_id']}",
        f"- phase: {state['active_phase']}",
        f"- active_agent: {state['active_agent']}",
        f"- risk_level: {state['risk_level']}",
        f"- approval_status: {state['approval_status']}",
        f"- review_status: {state['review_status']}",
        f"- rework_count: {state['rework_count']}",
        "",
        "## Goal",
        state["goal"],
        "",
        "## Latest Execution Result",
        state["execution_result"] or "N/A",
        "",
        "## Latest Review Result",
        state["review_result"] or "N/A",
        "",
        "## Recorder Summary",
        state["recorder_summary"] or "N/A",
    ]
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    append_memory_index(
        paths,
        {
            "timestamp": now_iso(),
            "type": "summary",
            "task_id": state["task_card"]["task_id"],
            "phase": state["active_phase"],
            "path": str(summary_path),
        },
    )
    return str(summary_path)

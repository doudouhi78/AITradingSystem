"""
Execution Journal — 实时记录每个 agent 节点的执行动作。

每条日志包含：
  - agent：谁在执行
  - task：在做什么（本轮任务描述）
  - why：为什么这么做
  - result：做完了什么（完成后填入）
  - duration_ms：耗时
  - status：running / completed / failed
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
JOURNAL_PATH = REPO_ROOT / "control_tower" / "execution_journal.json"
MAX_ENTRIES = 100

_AGENT_WHY = {
    "orchestrator": "分析目标，建立任务卡和执行路径",
    "builder": "构建方案或代码，产出可审查工件",
    "reviewer": "质量审查，判断 builder 产出是否合格",
    "validator": "硬巡检，验证边界条件和治理约束",
    "approval": "等待人工审批决策",
    "auto_approve": "满足自动放行条件，跳过人工审批",
    "recorder": "归档结果，写入项目记忆",
}

_PHASE_TASK = {
    "task_card_created": "目标建模：建立任务卡和执行路径",
    "building": "方案构建：产出方案或代码",
    "reviewing": "质量审查：评估 builder 产出",
    "validating": "边界巡检：验证治理约束",
    "pending_approval": "人工确认：等待审批",
    "auto_approved": "自动放行：满足放行条件",
    "recording": "归档：写入记忆和历史",
    "completed": "已完成归档",
}


def _read_journal() -> dict[str, Any]:
    if JOURNAL_PATH.exists():
        try:
            return json.loads(JOURNAL_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"entries": [], "updated_at": ""}


def _write_journal(data: dict[str, Any]) -> None:
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = JOURNAL_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    for _ in range(8):
        try:
            os.replace(tmp, JOURNAL_PATH)
            return
        except PermissionError:
            time.sleep(0.1)


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def journal_node_started(
    *,
    entry_id: str,
    project_id: str,
    agent: str,
    phase: str,
    goal: str,
    extra_task: str = "",
) -> None:
    """节点开始时写入 running 条目。"""
    phase_task = _PHASE_TASK.get(phase, phase)
    task = extra_task or phase_task
    if goal and agent == "orchestrator" and "目标建模" in task:
        task = f"目标建模：{goal[:80]}"
    elif goal and agent == "builder" and extra_task:
        task = f"构建方案：{extra_task[:80]}"

    entry = {
        "entry_id": entry_id,
        "project_id": project_id,
        "agent": agent,
        "phase": phase,
        "task": task,
        "why": _AGENT_WHY.get(agent, agent),
        "status": "running",
        "started_at": _now_iso(),
        "finished_at": "",
        "duration_ms": 0,
        "result": "",
    }
    data = _read_journal()
    entries = [e for e in data.get("entries", []) if e.get("entry_id") != entry_id]
    entries.insert(0, entry)
    data["entries"] = entries[:MAX_ENTRIES]
    data["updated_at"] = _now_iso()
    _write_journal(data)


def journal_node_completed(
    *,
    entry_id: str,
    duration_ms: float,
    result: str = "",
    status: str = "completed",
) -> None:
    """节点完成时更新对应条目。"""
    data = _read_journal()
    entries = data.get("entries", [])
    for entry in entries:
        if entry.get("entry_id") == entry_id:
            entry["status"] = status
            entry["finished_at"] = _now_iso()
            entry["duration_ms"] = round(duration_ms, 0)
            entry["result"] = result[:300] if result else ""
            break
    data["updated_at"] = _now_iso()
    _write_journal(data)


def get_journal_entries(limit: int = 30) -> list[dict[str, Any]]:
    """读取最近 N 条日志（最新在前）。"""
    data = _read_journal()
    return data.get("entries", [])[:limit]

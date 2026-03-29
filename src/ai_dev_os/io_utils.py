import json
import shutil
from datetime import timezone
from datetime import datetime
from pathlib import Path
import os
import time
from typing import Any

from ai_dev_os.system_db import ingest_runtime_state_snapshot

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_ROOT = REPO_ROOT / "memory_template"
RUNTIME_ROOT = REPO_ROOT / "runtime" / "projects"
ARCHIVE_ROOT = REPO_ROOT / "runtime" / "archive"
CONTROL_TOWER_PATH = REPO_ROOT / "control_tower" / "status.json"
SSOT_STATE_PATH = REPO_ROOT / "control_tower" / "ssot_state.json"


def ensure_project_scaffold(project_id: str) -> Path:
    project_root = RUNTIME_ROOT / project_id
    memory_root = project_root / "memory"
    for source in TEMPLATE_ROOT.rglob("*"):
        target = memory_root / source.relative_to(TEMPLATE_ROOT)
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    (project_root / "artifacts").mkdir(parents=True, exist_ok=True)
    return project_root


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    last_error: PermissionError | None = None
    for _ in range(8):
        try:
            os.replace(temp_path, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.1)
    if temp_path.exists():
        temp_path.unlink(missing_ok=True)
    if last_error is not None:
        raise last_error


def append_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    separator = "\n" if existing and not existing.endswith("\n") else ""
    path.write_text(f"{existing}{separator}{content}\n", encoding="utf-8")


def project_root(project_id: str) -> Path:
    return ensure_project_scaffold(project_id)


def archive_project(project_id: str) -> Path:
    source = RUNTIME_ROOT / project_id
    if not source.exists():
        raise FileNotFoundError(f"Project {project_id} does not exist.")
    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_ROOT / project_id
    if target.exists():
        timestamp = now_iso().replace(":", "-").replace(".", "-")
        target = ARCHIVE_ROOT / f"{project_id}_{timestamp}"
    shutil.move(str(source), str(target))
    return target


def state_snapshot_path(project_id: str) -> Path:
    return project_root(project_id) / "artifacts" / "state_snapshot.json"


def save_state_snapshot(project_id: str, state: dict[str, Any]) -> None:
    snapshot_path = state_snapshot_path(project_id)
    write_json(snapshot_path, state)
    ingest_runtime_state_snapshot(project_id=project_id, state=state, snapshot_path=str(snapshot_path))


def load_state_snapshot(project_id: str) -> dict[str, Any]:
    path = state_snapshot_path(project_id)
    if not path.exists():
        raise FileNotFoundError(f"No state snapshot found for project_id={project_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


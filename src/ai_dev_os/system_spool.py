from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SPOOL_ROOT = REPO_ROOT / "runtime" / "system_spool"
PENDING_ROOT = SPOOL_ROOT / "pending"
PROCESSED_ROOT = SPOOL_ROOT / "processed"


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def append_spool_record(*, kind: str, payload: dict[str, Any], producer_repo_root: str = "") -> str:
    PENDING_ROOT.mkdir(parents=True, exist_ok=True)
    envelope = {
        "record_id": str(uuid.uuid4()),
        "recorded_at": _now(),
        "kind": str(kind or "").strip(),
        "producer_repo_root": producer_repo_root or str(REPO_ROOT),
        "payload": payload,
    }
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
    path = PENDING_ROOT / f"{stamp}-{kind}-{envelope['record_id'][:8]}.json"
    path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)


def processed_dir_for(repo_root: str | Path) -> Path:
    base = Path(repo_root).resolve()
    return base / "runtime" / "system_spool" / "processed"

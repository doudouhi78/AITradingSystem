from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.project_objects import ValidationRecord
from ai_dev_os.project_objects import validate_validation_record


REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATIONS_ROOT = REPO_ROOT / "runtime" / "validations"


def ensure_validations_root() -> Path:
    VALIDATIONS_ROOT.mkdir(parents=True, exist_ok=True)
    return VALIDATIONS_ROOT


def validation_path(validation_id: str) -> Path:
    return ensure_validations_root() / f"{validation_id}.json"


def write_validation_record(validation_record: ValidationRecord | dict[str, Any]) -> str:
    record = validate_validation_record(validation_record)
    path = validation_path(record["validation_id"])
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)


def read_validation_record(validation_id: str) -> dict[str, Any]:
    path = validation_path(validation_id)
    return json.loads(path.read_text(encoding="utf-8"))

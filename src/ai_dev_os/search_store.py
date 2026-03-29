from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.project_objects import VariantSearchSpec
from ai_dev_os.project_objects import validate_variant_search_spec


REPO_ROOT = Path(__file__).resolve().parents[2]
SEARCHES_ROOT = REPO_ROOT / "runtime" / "searches"


def ensure_searches_root() -> Path:
    SEARCHES_ROOT.mkdir(parents=True, exist_ok=True)
    return SEARCHES_ROOT


def search_spec_path(search_id: str) -> Path:
    return ensure_searches_root() / f"{search_id}.json"


def write_search_spec(search_spec: VariantSearchSpec | dict[str, Any]) -> str:
    spec = validate_variant_search_spec(search_spec)
    path = search_spec_path(spec["search_id"])
    path.write_text(json.dumps(spec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)


def read_search_spec(search_id: str) -> dict[str, Any]:
    path = search_spec_path(search_id)
    return json.loads(path.read_text(encoding="utf-8"))


def list_search_specs(limit: int = 20) -> list[dict[str, Any]]:
    ensure_searches_root()
    payloads: list[dict[str, Any]] = []
    for path in sorted(SEARCHES_ROOT.glob('*.json'), reverse=True)[:limit]:
        payloads.append(json.loads(path.read_text(encoding='utf-8')))
    return payloads

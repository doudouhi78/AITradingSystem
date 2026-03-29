from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.project_objects import FormalReviewRecord
from ai_dev_os.project_objects import validate_formal_review_record


REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEWS_ROOT = REPO_ROOT / "runtime" / "reviews"


def ensure_reviews_root() -> Path:
    REVIEWS_ROOT.mkdir(parents=True, exist_ok=True)
    return REVIEWS_ROOT


def review_path(review_id: str) -> Path:
    return ensure_reviews_root() / f"{review_id}.json"


def write_formal_review(formal_review: FormalReviewRecord | dict[str, Any]) -> str:
    record = validate_formal_review_record(formal_review)
    path = review_path(record["review_id"])
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)


def read_formal_review(review_id: str) -> dict[str, Any]:
    path = review_path(review_id)
    return json.loads(path.read_text(encoding="utf-8"))


def list_formal_reviews(*, limit: int = 20, experiment_id: str = '', baseline_experiment_id: str = '') -> list[dict[str, Any]]:
    ensure_reviews_root()
    payloads: list[dict[str, Any]] = []
    for path in REVIEWS_ROOT.glob('*.json'):
        payload = json.loads(path.read_text(encoding='utf-8'))
        if experiment_id and payload.get('experiment_id') != experiment_id:
            continue
        if baseline_experiment_id and payload.get('baseline_experiment_id') != baseline_experiment_id:
            continue
        payloads.append(payload)
    payloads.sort(key=lambda item: (str(item.get('reviewed_at', '')), str(item.get('review_id', ''))), reverse=True)
    return payloads[:limit]

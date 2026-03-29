from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_dev_os.system_db import record_sample_usage_fact

ROOT = Path(__file__).resolve().parents[2]
SAMPLE_LIBRARY_PATH = ROOT / "test_assets" / "sample_library_v1.json"
SAMPLE_USAGE_LATEST = ROOT / "mother_memory" / "diagnostics" / "sample_library_usage_latest.json"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_sample_library() -> dict[str, Any]:
    if not SAMPLE_LIBRARY_PATH.exists():
        return {"schema_version": "sample_library.v1", "tiers": {}}
    return json.loads(SAMPLE_LIBRARY_PATH.read_text(encoding="utf-8"))


def load_sample_usage() -> dict[str, Any]:
    if not SAMPLE_USAGE_LATEST.exists():
        return {"schema_version": "sample_library_usage.v1", "generated_at": _now(), "samples": {}}
    return json.loads(SAMPLE_USAGE_LATEST.read_text(encoding="utf-8"))


def _sample_metadata(sample_id: str) -> dict[str, Any]:
    library = load_sample_library()
    for tier, items in (library.get("tiers", {}) or {}).items():
        for item in items or []:
            if str(item.get("sample_id", "")) == sample_id:
                meta = dict(item)
                meta["tier"] = tier
                return meta
    return {"sample_id": sample_id, "tier": "unknown"}


def record_sample_usage(*, sample_id: str, run_id: str, mode: str, status: str, failure_class: str = "", near_upper_bound: bool = False, over_upper_bound: bool = False, must_split: bool = False, trigger_dimensions: list[str] | None = None) -> dict[str, Any]:
    usage = load_sample_usage()
    samples = dict(usage.get("samples", {}) or {})
    record = dict(samples.get(sample_id, {}) or {})
    metadata = _sample_metadata(sample_id)
    history = list(record.get("recent_history", []) or [])
    history.append({
        "run_id": run_id,
        "mode": mode,
        "status": status,
        "failure_class": failure_class,
        "near_upper_bound": bool(near_upper_bound),
        "over_upper_bound": bool(over_upper_bound),
        "must_split": bool(must_split),
        "trigger_dimensions": list(trigger_dimensions or []),
        "timestamp": _now(),
    })
    history = history[-20:]
    use_count = int(record.get("use_count", 0) or 0) + 1
    failure_count = int(record.get("failure_count", 0) or 0) + (0 if status == "passed" else 1)
    near_count = int(record.get("near_upper_bound_count", 0) or 0) + (1 if near_upper_bound else 0)
    over_count = int(record.get("over_upper_bound_count", 0) or 0) + (1 if over_upper_bound else 0)
    split_count = int(record.get("must_split_count", 0) or 0) + (1 if must_split else 0)

    aggregate_triggers: dict[str, int] = dict(record.get("trigger_counts", {}) or {})
    for item in trigger_dimensions or []:
        key = str(item)
        aggregate_triggers[key] = int(aggregate_triggers.get(key, 0) or 0) + 1

    samples[sample_id] = {
        "sample_id": sample_id,
        "tier": metadata.get("tier", "unknown"),
        "sample_family": metadata.get("sample_family", ""),
        "purpose": metadata.get("purpose", ""),
        "expected_signal": metadata.get("expected_signal", ""),
        "use_count": use_count,
        "failure_count": failure_count,
        "near_upper_bound_count": near_count,
        "over_upper_bound_count": over_count,
        "must_split_count": split_count,
        "last_run_id": run_id,
        "last_mode": mode,
        "last_status": status,
        "last_failure_class": failure_class,
        "trigger_counts": aggregate_triggers,
        "recent_history": history,
    }
    usage["generated_at"] = _now()
    usage["samples"] = samples
    SAMPLE_USAGE_LATEST.write_text(json.dumps(usage, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    record_sample_usage_fact(samples[sample_id])
    return samples[sample_id]

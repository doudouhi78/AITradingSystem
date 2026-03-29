# [READ-ONLY REFERENCE] 本文件停止新增功能，仅作参考。2026-03-29
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import TypedDict


REPO_ROOT = Path(__file__).resolve().parents[2]
TRACE_ROOT = REPO_ROOT / "runtime" / "traces"


class ArtifactRef(TypedDict):
    artifact_kind: str
    artifact_path: str


class ResearchTraceEvent(TypedDict):
    trace_id: str
    span_id: str
    parent_span_id: str
    task_id: str
    run_id: str
    experiment_id: str
    agent_role: str
    step_code: str
    step_label: str
    event_kind: str
    status_code: str
    started_at: str
    finished_at: str
    duration_ms: int
    artifact_refs: list[ArtifactRef]
    memory_refs: list[str]
    metric_refs: list[str]
    tags: list[str]
    notes: str


TRACE_REQUIRED_FIELDS = {
    'trace_id',
    'span_id',
    'parent_span_id',
    'task_id',
    'run_id',
    'experiment_id',
    'agent_role',
    'step_code',
    'step_label',
    'event_kind',
    'status_code',
    'started_at',
    'finished_at',
    'duration_ms',
    'artifact_refs',
    'memory_refs',
    'metric_refs',
    'tags',
    'notes',
}


def _ensure_trace_root() -> Path:
    TRACE_ROOT.mkdir(parents=True, exist_ok=True)
    return TRACE_ROOT


def trace_log_path(run_id: str) -> Path:
    return _ensure_trace_root() / f"{run_id}.jsonl"


def trace_summary_path(run_id: str) -> Path:
    return _ensure_trace_root() / f"{run_id}.summary.json"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def validate_trace_event(event: ResearchTraceEvent | dict[str, Any]) -> ResearchTraceEvent:
    payload = dict(event)
    missing = sorted(field for field in TRACE_REQUIRED_FIELDS if field not in payload)
    if missing:
        raise ValueError(f"missing required trace fields: {', '.join(missing)}")
    if not isinstance(payload['duration_ms'], int):
        raise TypeError('duration_ms must be int')
    for field in ('artifact_refs', 'memory_refs', 'metric_refs', 'tags'):
        if not isinstance(payload[field], list):
            raise TypeError(f'{field} must be list')
    return payload  # type: ignore[return-value]


def append_trace_event(event: ResearchTraceEvent | dict[str, Any]) -> dict[str, str]:
    payload = validate_trace_event(event)
    run_id = str(payload['run_id'])
    log_path = trace_log_path(run_id)
    with log_path.open('a', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + '\n')
    summary = summarize_trace_session(run_id)
    summary_path = trace_summary_path(run_id)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return {
        'log_path': str(log_path),
        'summary_path': str(summary_path),
    }


def load_trace_events(run_id: str) -> list[dict[str, Any]]:
    log_path = trace_log_path(run_id)
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text(encoding='utf-8').splitlines() if line.strip()]


def summarize_trace_session(run_id: str) -> dict[str, Any]:
    events = load_trace_events(run_id)
    if not events:
        return {
            'run_id': run_id,
            'trace_id': '',
            'experiment_id': '',
            'task_id': '',
            'event_count': 0,
            'agent_roles': [],
            'step_codes': [],
            'status_codes': [],
            'started_at': '',
            'finished_at': '',
        }
    agent_roles = sorted({str(item['agent_role']) for item in events})
    step_codes = [str(item['step_code']) for item in events]
    status_codes = [str(item['status_code']) for item in events]
    return {
        'run_id': run_id,
        'trace_id': str(events[0]['trace_id']),
        'experiment_id': str(events[0]['experiment_id']),
        'task_id': str(events[0]['task_id']),
        'event_count': len(events),
        'agent_roles': agent_roles,
        'step_codes': step_codes,
        'status_codes': status_codes,
        'started_at': str(events[0]['started_at']),
        'finished_at': str(events[-1]['finished_at']),
    }


def list_trace_sessions(limit: int = 20) -> list[dict[str, Any]]:
    _ensure_trace_root()
    summary_files = sorted(TRACE_ROOT.glob('*.summary.json'), reverse=True)
    return [json.loads(path.read_text(encoding='utf-8')) for path in summary_files[:limit]]


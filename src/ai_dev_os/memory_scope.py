from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.io_utils import now_iso
from ai_dev_os.io_utils import write_json
from ai_dev_os.mother_memory import get_mother_memory_context
from ai_dev_os.project_memory import get_project_memory_context


MEMORY_SCOPE_READ_ORDER = [
    'task_or_handoff',
    'session_memory',
    'role_memory',
    'project_memory',
    'mother_memory',
]

PROJECT_MEMORY_ALLOWED_KINDS = {
    'spec_confirmed',
    'boundary_confirmed',
    'decision_confirmed',
    'blocker_confirmed',
    'review_conclusion',
    'validation_conclusion',
    'phase_checkpoint',
}

PROJECT_MEMORY_BLOCKED_METADATA_KEYS = {
    'raw_stream',
    'raw_log',
    'raw_logs',
    'full_prompt',
    'prompt',
    'prompt_text',
    'transcript',
    'full_transcript',
    'token_stream',
    'shell_output',
    'stdout',
    'stderr',
}


ROLE_ONBOARDING_SOURCE_FILES = (
    'ROLE_IDENTITY.md',
    'ROLE_ONBOARDING_PACKET.md',
    'ROLE_WARMUP_REPORT.md',
    'ROLE_WARMUP_PROMPT.txt',
)

ROLE_MEMORY_ALLOWED_NAMES = {
    'ROLE_IDENTITY.md',
    'ROLE_ONBOARDING_PACKET.md',
    'ROLE_WARMUP_REPORT.md',
    'ROLE_WARMUP_PROMPT.txt',
    'AGENTS.md',
}


def _safe_read(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ''
    try:
        return path.read_text(encoding='utf-8').strip()
    except Exception:
        return ''


def _clip_text(value: object, *, limit: int = 1000) -> str:
    text = ' '.join(str(value or '').split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + '...'


def _sanitize_project_fact_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(metadata or {})
    clean: dict[str, Any] = {}
    for key, value in payload.items():
        normalized_key = str(key or '').strip()
        if not normalized_key:
            continue
        if normalized_key in PROJECT_MEMORY_BLOCKED_METADATA_KEYS:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            clean[normalized_key] = value
            continue
        if isinstance(value, (list, tuple)):
            clean[normalized_key] = [_clip_text(item, limit=160) for item in value[:8]]
            continue
        if isinstance(value, dict):
            clean[normalized_key] = {
                str(child_key): _clip_text(child_value, limit=180)
                for child_key, child_value in list(value.items())[:8]
                if str(child_key or '').strip() and str(child_key) not in PROJECT_MEMORY_BLOCKED_METADATA_KEYS
            }
            continue
        clean[normalized_key] = _clip_text(value, limit=180)
    return clean


def _sync_role_onboarding_snapshot(role: str) -> str:
    settings = get_agent_settings(role)
    workspace_root = Path(settings.workspace_root)
    memory_root = Path(settings.memory_root)
    role_dir = workspace_root / '.role'
    target = memory_root / 'role_identity' / 'onboarding_snapshot.json'
    target.parent.mkdir(parents=True, exist_ok=True)

    sources: list[dict[str, str]] = []
    combined_parts: list[str] = []
    fingerprint_parts: list[str] = []
    for name in ROLE_ONBOARDING_SOURCE_FILES:
        source = role_dir / name
        if not source.exists() or not source.is_file():
            continue
        raw_text = _safe_read(source)
        if not raw_text:
            continue
        clipped = _clip_text(raw_text, limit=1200)
        digest = hashlib.sha1(raw_text.encode('utf-8')).hexdigest()
        sources.append({'path': str(source), 'sha1': digest})
        combined_parts.append(f'[{name}]\n{clipped}')
        fingerprint_parts.append(f'{name}:{digest}')

    fingerprint = '|'.join(fingerprint_parts)
    existing_fingerprint = ''
    if target.exists():
        try:
            existing_payload = json.loads(target.read_text(encoding='utf-8'))
            existing_fingerprint = str(existing_payload.get('fingerprint', '') or '')
        except Exception:
            existing_fingerprint = ''

    if fingerprint and fingerprint != existing_fingerprint:
        write_json(
            target,
            {
                'role': role,
                'updated_at': now_iso(),
                'fingerprint': fingerprint,
                'sources': sources,
                'combined_excerpt': '\n\n'.join(combined_parts),
            },
        )
    elif fingerprint and not target.exists():
        write_json(
            target,
            {
                'role': role,
                'updated_at': now_iso(),
                'fingerprint': fingerprint,
                'sources': sources,
                'combined_excerpt': '\n\n'.join(combined_parts),
            },
        )
    return str(target)


def _collect_recent_role_docs(role: str, *, limit: int = 3) -> list[Path]:
    settings = get_agent_settings(role)
    candidates: list[Path] = []
    workspace_root = Path(settings.workspace_root)
    role_dir = workspace_root / '.role'
    if role_dir.exists():
        for suffix in ('*.md', '*.txt'):
            candidates.extend(role_dir.rglob(suffix))
    agents_md = workspace_root / 'AGENTS.md'
    if agents_md.exists() and agents_md.is_file():
        candidates.append(agents_md)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in sorted(candidates, key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True):
        if not path.is_file() or path.name not in ROLE_MEMORY_ALLOWED_NAMES:
            continue
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
        if len(unique) >= limit:
            break
    return unique


def load_mother_memory_excerpt(
    query_text: str = '',
    *,
    mode: str = 'default',
    combination_sensitive: bool = False,
) -> dict[str, Any]:
    payload = get_mother_memory_context(query_text, mode=mode, combination_sensitive=combination_sensitive)
    return {
        'scope': 'mother_memory',
        'excerpt': str(payload.get('combined_context', '') or ''),
        'sources': [item.get('path', '') for item in list(payload.get('relevant_docs', []) or [])],
    }


def load_project_memory_excerpt(
    paths: dict[str, str],
    query_text: str = '',
    *,
    mode: str = 'default',
    rework_count: int = 0,
) -> dict[str, Any]:
    payload = get_project_memory_context(paths, query_text, mode=mode, rework_count=rework_count)
    return {
        'scope': 'project_memory',
        'excerpt': str(payload.get('combined_context', '') or ''),
        'sources': [item.get('path', '') for item in list(payload.get('relevant_docs', []) or [])],
    }


def load_role_memory_excerpt(role: str, *, limit: int = 3, snippet_limit: int = 800) -> dict[str, Any]:
    _sync_role_onboarding_snapshot(role)
    docs = _collect_recent_role_docs(role, limit=limit)
    snippets: list[str] = []
    sources: list[str] = []
    for path in docs:
        text = _safe_read(path)
        if not text:
            continue
        snippets.append(f'[{path.name}]\n{_clip_text(text, limit=snippet_limit)}')
        sources.append(str(path))
    return {
        'scope': 'role_memory',
        'excerpt': '\n\n'.join(snippets),
        'sources': sources,
    }


def load_session_memory_excerpt(state: dict[str, Any], *, role: str) -> dict[str, Any]:
    task_card = dict(state.get('task_card', {}) or {})
    artifacts = dict(state.get('artifacts', {}) or {})
    payload = {
        'task_id': str(task_card.get('task_id', '') or ''),
        'goal': str(state.get('goal', '') or ''),
        'active_phase': str(state.get('active_phase', '') or ''),
        'active_agent': str(state.get('active_agent', '') or ''),
        'blocking_issue': str(state.get('blocking_issue', '') or ''),
        'rework_count': int(state.get('rework_count', 0) or 0),
        'review_feedback': _clip_text(state.get('review_feedback', ''), limit=420),
        'validation_feedback': _clip_text(state.get('validation_feedback', ''), limit=420),
        'builder_working_state': artifacts.get('builder_working_state', {}),
        'reviewer_handoff_packet': dict(artifacts.get('reviewer_handoff_packet', {}) or {}),
    }
    return {
        'scope': 'session_memory',
        'excerpt': json.dumps(payload, ensure_ascii=False, indent=2),
        'sources': [f'state:{role}:session'],
    }


def build_memory_scope_bundle(
    state: dict[str, Any],
    *,
    role: str,
    query_text: str = '',
    include_mother: bool = True,
    include_project: bool = True,
    mode: str = 'default',
) -> dict[str, Any]:
    artifacts = dict(state.get('artifacts', {}) or {})
    paths = dict(artifacts.get('paths', {}) or {})
    session_memory = load_session_memory_excerpt(state, role=role)
    role_memory = load_role_memory_excerpt(role)
    project_memory = load_project_memory_excerpt(paths, query_text, mode=mode, rework_count=int(state.get('rework_count', 0) or 0)) if paths and include_project else {'scope': 'project_memory', 'excerpt': '', 'sources': []}
    mother_mode = 'builder' if role == 'builder' else 'default'
    mother_memory = load_mother_memory_excerpt(query_text, mode=mother_mode, combination_sensitive=bool((artifacts.get('orchestrator_analysis', {}) or {}).get('combination_sensitive', False))) if include_mother else {'scope': 'mother_memory', 'excerpt': '', 'sources': []}
    layers = {
        'session_memory': session_memory,
        'role_memory': role_memory,
        'project_memory': project_memory,
        'mother_memory': mother_memory,
    }
    combined_sections: list[str] = []
    if session_memory.get('excerpt'):
        combined_sections.append('[Session Memory]\n' + str(session_memory['excerpt']))
    if role_memory.get('excerpt'):
        combined_sections.append('[Role Memory]\n' + str(role_memory['excerpt']))
    if project_memory.get('excerpt'):
        combined_sections.append('[Project Memory]\n' + str(project_memory['excerpt']))
    if mother_memory.get('excerpt'):
        combined_sections.append('[Mother Memory]\n' + str(mother_memory['excerpt']))
    return {
        'schema_version': 'memory_scope_bundle.v1',
        'role': role,
        'read_order': list(MEMORY_SCOPE_READ_ORDER),
        'layers': layers,
        'combined_context': '\n\n'.join(section for section in combined_sections if section),
    }


def write_project_memory_fact(
    paths: dict[str, str],
    *,
    kind: str,
    title: str,
    summary: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    normalized_kind = str(kind or '').strip()
    if normalized_kind not in PROJECT_MEMORY_ALLOWED_KINDS:
        allowed = ', '.join(sorted(PROJECT_MEMORY_ALLOWED_KINDS))
        raise ValueError(f'Unsupported project memory fact kind: {normalized_kind}. Allowed kinds: {allowed}')
    memory_root = Path(paths['memory_root'])
    target = memory_root / 'project_state' / 'facts.jsonl'
    target.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        'timestamp': now_iso(),
        'kind': normalized_kind,
        'title': _clip_text(title, limit=140),
        'summary': _clip_text(summary, limit=600),
        'metadata': _sanitize_project_fact_metadata(metadata),
    }
    with target.open('a', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + '\n')
    return str(target)


def append_project_decision(paths: dict[str, str], *, title: str, summary: str, metadata: dict[str, Any] | None = None) -> str:
    return write_project_memory_fact(
        paths,
        kind='decision_confirmed',
        title=title,
        summary=summary,
        metadata=metadata,
    )


def write_session_memory(paths: dict[str, str], *, role: str, payload: dict[str, Any]) -> str:
    project_root = Path(paths['project_root'])
    target = project_root / 'artifacts' / 'session_memory' / f'{role}_latest.json'
    write_json(target, payload)
    return str(target)


def write_role_memory(role: str, *, title: str, summary: str, metadata: dict[str, Any] | None = None) -> str:
    settings = get_agent_settings(role)
    target = Path(settings.memory_root) / 'records' / 'role_memory.jsonl'
    target.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        'timestamp': now_iso(),
        'role': role,
        'title': title,
        'summary': summary,
        'metadata': dict(metadata or {}),
    }
    with target.open('a', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + '\n')
    return str(target)

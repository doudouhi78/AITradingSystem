from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ai_dev_os.experiment_store import read_experiment_artifacts
from ai_dev_os.research_tracing import list_trace_sessions
from ai_dev_os.research_tracing import load_trace_events
from ai_dev_os.research_tracing import summarize_trace_session
from ai_dev_os.review_store import list_formal_reviews
from ai_dev_os.review_store import read_formal_review
from ai_dev_os.search_store import list_search_specs
from ai_dev_os.search_store import read_search_spec
from ai_dev_os.system_db import DB_PATH
from ai_dev_os.system_db import REPO_ROOT
from ai_dev_os.validation_store import read_validation_record


MEMORY_DOCUMENTS = {
    'vision': REPO_ROOT / 'memory_v1' / '00_root' / 'vision.md',
    'mainline': REPO_ROOT / 'memory_v1' / '10_mainline' / 'project_mainline_view.md',
    'working_draft': REPO_ROOT / 'memory_v1' / '20_progress' / 'working_draft_board.md',
    'working_test': REPO_ROOT / 'memory_v1' / '40_experience_base' / 'working_test_draft_board.md',
    'execution_blackbox': REPO_ROOT / 'memory_v1' / 'EXECUTION_BLACKBOX.md',
    'trace_sessions': REPO_ROOT / 'runtime' / 'traces',
}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_memory_documents() -> list[dict[str, str]]:
    return [{'name': name, 'path': str(path)} for name, path in MEMORY_DOCUMENTS.items()]


def read_memory_document(name: str) -> dict[str, Any]:
    if name not in MEMORY_DOCUMENTS:
        raise ValueError(f'unknown memory document: {name}')
    path = MEMORY_DOCUMENTS[name]
    return {'name': name, 'path': str(path), 'content': path.read_text(encoding='utf-8')}


def list_experiment_runs(*, limit: int = 20, strategy_family: str = '', status_code: str = '') -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if strategy_family:
        clauses.append('strategy_family = ?')
        params.append(strategy_family)
    if status_code:
        clauses.append('status_code = ?')
        params.append(status_code)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    query = f'''
        SELECT experiment_id, task_id, run_id, title, strategy_family, variant_name, instrument,
               dataset_version, rules_version, decision_status, is_baseline, baseline_of,
               status_code, created_at
        FROM experiment_runs
        {where}
        ORDER BY datetime(created_at) DESC, experiment_id DESC
        LIMIT ?
    '''
    params.append(limit)
    conn = _connect()
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def get_experiment_run(experiment_id: str) -> dict[str, Any]:
    conn = _connect()
    try:
        row = conn.execute('SELECT * FROM experiment_runs WHERE experiment_id = ?', (experiment_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f'experiment_id {experiment_id} not found')
    record = dict(row)
    metrics_summary_json = record.pop('metrics_summary_json', '') or '{}'
    record['metrics_summary'] = json.loads(metrics_summary_json)
    artifacts = read_experiment_artifacts(experiment_id)
    manifest = artifacts['manifest']
    validation_records = [read_validation_record(validation_id) for validation_id in manifest.get('validation_record_ids', [])]
    search_spec = read_search_spec(str(manifest.get('search_spec_id', '')).strip()) if manifest.get('search_spec_id') else None
    formal_reviews = list_formal_reviews(limit=20, experiment_id=experiment_id)
    return {
        'index': record,
        'artifacts': artifacts,
        'validation_records': validation_records,
        'search_spec': search_spec,
        'formal_reviews': formal_reviews,
    }


def get_current_baseline() -> dict[str, Any]:
    conn = _connect()
    try:
        row = conn.execute(
            '''
            SELECT experiment_id, task_id, run_id, title, strategy_family, variant_name, instrument,
                   dataset_version, rules_version, decision_status, is_baseline, baseline_of,
                   status_code, created_at
            FROM experiment_runs
            WHERE is_baseline = 1
            ORDER BY datetime(created_at) DESC, experiment_id DESC
            LIMIT 1
            '''
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError('no baseline experiment found')
    return dict(row)


def get_validation_record(validation_id: str) -> dict[str, Any]:
    return read_validation_record(validation_id)


def list_search_spec_summaries(limit: int = 20) -> list[dict[str, Any]]:
    return list_search_specs(limit=limit)


def get_search_spec(search_id: str) -> dict[str, Any]:
    return read_search_spec(search_id)


def list_formal_review_summaries(limit: int = 20, experiment_id: str = '', baseline_experiment_id: str = '') -> list[dict[str, Any]]:
    return list_formal_reviews(limit=limit, experiment_id=experiment_id, baseline_experiment_id=baseline_experiment_id)


def get_formal_review(review_id: str) -> dict[str, Any]:
    return read_formal_review(review_id)


def get_trace_session(run_id: str) -> dict[str, Any]:
    events = load_trace_events(run_id)
    if not events:
        raise ValueError(f'run_id {run_id} trace not found')
    return {'summary': summarize_trace_session(run_id), 'events': events}


def list_trace_session_summaries(limit: int = 20) -> list[dict[str, Any]]:
    return list_trace_sessions(limit=limit)

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from ai_dev_os.system_spool import append_spool_record


REPO_ROOT = Path(__file__).resolve().parents[2]
DB_ROOT = REPO_ROOT / "runtime" / "system_facts"
DB_PATH = DB_ROOT / "system_facts.sqlite3"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ensure_db_root() -> None:
    DB_ROOT.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_db_root()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _timestamp_ms(value: str | None) -> int:
    if not value:
        return 0
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return 0


def _executor_id_from_repo_root(repo_root: str | None) -> str:
    path = str(repo_root or "").lower()
    if "replica_a" in path:
        return "replica_a"
    if "replica_b" in path:
        return "replica_b"
    if path:
        return "main"
    return ""


def _target_fields(target: str | None) -> tuple[str, str]:
    text = str(target or "").strip()
    if not text:
        return "", ""
    if any(sep in text for sep in ("\\", "/")):
        return "path", text
    if " " in text:
        return "command", text
    return "object", text


def _reason_code_from_event(event: dict[str, Any]) -> str:
    metadata = dict(event.get("metadata", {}) or {})
    if isinstance(metadata.get("reason_code"), str) and metadata["reason_code"]:
        return str(metadata["reason_code"])
    if event.get("event_type") == "error":
        return str(metadata.get("error_code", "") or "error")
    return ""


def _phase_code_from_event(event: dict[str, Any]) -> str:
    metadata = dict(event.get("metadata", {}) or {})
    if isinstance(metadata.get("phase"), str) and metadata["phase"]:
        return str(metadata["phase"])
    if isinstance(metadata.get("to_phase"), str) and metadata["to_phase"]:
        return str(metadata["to_phase"])
    return ""


def _path_mode_from_state(state: dict[str, Any]) -> str:
    triggers = dict((state.get("artifacts", {}) or {}).get("dynamic_triggers", {}) or {})
    routing = dict(triggers.get("routing_state", {}) or {})
    return str(routing.get("current_path_mode") or triggers.get("path_mode") or "")


@dataclass(frozen=True)
class RetentionRule:
    hot_days: int
    warm_days: int
    purge_days: int


RETENTION_RULES = {
    "process_events": RetentionRule(hot_days=7, warm_days=30, purge_days=365),
    "validation_results": RetentionRule(hot_days=14, warm_days=90, purge_days=365),
    "validation_run_summaries": RetentionRule(hot_days=14, warm_days=90, purge_days=365),
    "experiment_runs": RetentionRule(hot_days=30, warm_days=180, purge_days=540),
    "sample_usage_history": RetentionRule(hot_days=30, warm_days=180, purge_days=540),
    "artifact_index": RetentionRule(hot_days=30, warm_days=180, purge_days=540),
}


EXPERIMENT_RUN_OPTIONAL_COLUMNS = {
    "dataset_version": "TEXT",
    "rules_version": "TEXT",
    "decision_status": "TEXT",
    "is_baseline": "INTEGER NOT NULL DEFAULT 0",
    "baseline_of": "TEXT NOT NULL DEFAULT ''",
    "cost_assumption": "TEXT",
}


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_table_columns(conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table_name)
    for column_name, definition in columns.items():
        if column_name in existing:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def ensure_database() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS process_events (
              event_id TEXT PRIMARY KEY,
              timestamp_text TEXT NOT NULL,
              timestamp_ms INTEGER NOT NULL,
              project_id TEXT,
              task_id TEXT,
              run_id TEXT,
              sample_id TEXT,
              executor_id TEXT,
              node_code TEXT,
              event_type_code TEXT,
              status_code TEXT,
              summary_text TEXT,
              target_type TEXT,
              target_id TEXT,
              duration_ms INTEGER,
              reason_code TEXT,
              phase_code TEXT,
              path_mode_code TEXT,
              error_code TEXT,
              metadata_json TEXT,
              storage_tier TEXT NOT NULL DEFAULT 'hot',
              archived_at TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_process_events_ts ON process_events(timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_project ON process_events(project_id, timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_run ON process_events(run_id, timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_sample ON process_events(sample_id, timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_executor ON process_events(executor_id, timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_type ON process_events(event_type_code, node_code, timestamp_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_process_events_tier ON process_events(storage_tier, timestamp_ms DESC);

            CREATE TABLE IF NOT EXISTS task_runtime_facts (
              project_id TEXT PRIMARY KEY,
              task_id TEXT,
              run_id TEXT,
              sample_id TEXT,
              executor_id TEXT,
              goal TEXT,
              active_phase TEXT,
              active_agent TEXT,
              task_kind TEXT,
              review_status TEXT,
              validation_status TEXT,
              approval_status TEXT,
              rework_count INTEGER,
              risk_level TEXT,
              runtime_backend TEXT,
              runtime_status TEXT,
              runtime_duration_ms INTEGER,
              failure_class TEXT,
              failure_disposition TEXT,
              path_mode_code TEXT,
              routing_state_json TEXT,
              builder_working_state_json TEXT,
              target_workspace_root TEXT,
              updated_at TEXT NOT NULL,
              storage_tier TEXT NOT NULL DEFAULT 'hot'
            );
            CREATE INDEX IF NOT EXISTS idx_task_runtime_run ON task_runtime_facts(run_id);
            CREATE INDEX IF NOT EXISTS idx_task_runtime_sample ON task_runtime_facts(sample_id);
            CREATE INDEX IF NOT EXISTS idx_task_runtime_executor ON task_runtime_facts(executor_id);
            CREATE INDEX IF NOT EXISTS idx_task_runtime_phase ON task_runtime_facts(active_phase, updated_at DESC);

            CREATE TABLE IF NOT EXISTS validation_run_summaries (
              summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT,
              summary_kind TEXT,
              tier TEXT,
              batch_id TEXT,
              wave_id TEXT,
              mode TEXT,
              status_code TEXT,
              passed INTEGER,
              total_cases INTEGER,
              completed_cases INTEGER,
              failed_cases INTEGER,
              failure_counts_json TEXT,
              paths_json TEXT,
              source TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              storage_tier TEXT NOT NULL DEFAULT 'hot'
            );
            CREATE INDEX IF NOT EXISTS idx_validation_run_summaries_run ON validation_run_summaries(run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_validation_run_summaries_kind ON validation_run_summaries(summary_kind, created_at DESC);

            CREATE TABLE IF NOT EXISTS validation_results (
              result_id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT,
              project_id TEXT,
              task_id TEXT,
              sample_id TEXT,
              executor_id TEXT,
              repo_root TEXT,
              specimen_root TEXT,
              phase TEXT,
              status_code TEXT,
              failure_class TEXT,
              failure_disposition TEXT,
              review_status TEXT,
              validation_status TEXT,
              near_upper_bound INTEGER,
              over_upper_bound INTEGER,
              must_split INTEGER,
              upper_bound_trigger_dimensions_json TEXT,
              docker_mount_preview_json TEXT,
              result_path TEXT,
              source TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              storage_tier TEXT NOT NULL DEFAULT 'hot'
            );
            CREATE INDEX IF NOT EXISTS idx_validation_results_run ON validation_results(run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_validation_results_sample ON validation_results(sample_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_validation_results_executor ON validation_results(executor_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_validation_results_failure ON validation_results(failure_class, created_at DESC);

            CREATE TABLE IF NOT EXISTS experiment_runs (
              experiment_id TEXT PRIMARY KEY,
              task_id TEXT,
              run_id TEXT,
              title TEXT NOT NULL,
              strategy_family TEXT,
              variant_name TEXT,
              instrument TEXT,
              data_source TEXT,
              date_range_start TEXT,
              date_range_end TEXT,
              entry_rule_summary TEXT,
              exit_rule_summary TEXT,
              execution_assumption TEXT,
              metrics_summary_json TEXT,
              review_outcome TEXT,
              memory_note_path TEXT,
              artifact_root TEXT NOT NULL,
              status_code TEXT NOT NULL DEFAULT 'recorded',
              dataset_version TEXT,
              rules_version TEXT,
              decision_status TEXT,
              is_baseline INTEGER NOT NULL DEFAULT 0,
              baseline_of TEXT NOT NULL DEFAULT '',
              cost_assumption TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              storage_tier TEXT NOT NULL DEFAULT 'hot'
            );
            CREATE INDEX IF NOT EXISTS idx_experiment_runs_task ON experiment_runs(task_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_experiment_runs_run ON experiment_runs(run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_experiment_runs_family ON experiment_runs(strategy_family, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_experiment_runs_instrument ON experiment_runs(instrument, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_experiment_runs_status ON experiment_runs(status_code, created_at DESC);

            CREATE TABLE IF NOT EXISTS sample_usage_history (
              history_id INTEGER PRIMARY KEY AUTOINCREMENT,
              sample_id TEXT NOT NULL,
              run_id TEXT,
              mode_code TEXT,
              status_code TEXT,
              failure_class TEXT,
              near_upper_bound INTEGER,
              over_upper_bound INTEGER,
              must_split INTEGER,
              trigger_dimensions_json TEXT,
              recorded_at TEXT NOT NULL,
              storage_tier TEXT NOT NULL DEFAULT 'hot'
            );
            CREATE INDEX IF NOT EXISTS idx_sample_usage_history_sample ON sample_usage_history(sample_id, recorded_at DESC);
            CREATE INDEX IF NOT EXISTS idx_sample_usage_history_run ON sample_usage_history(run_id, recorded_at DESC);

            CREATE TABLE IF NOT EXISTS sample_usage_rollups (
              sample_id TEXT PRIMARY KEY,
              tier TEXT,
              sample_family TEXT,
              purpose TEXT,
              expected_signal TEXT,
              use_count INTEGER,
              failure_count INTEGER,
              near_upper_bound_count INTEGER,
              over_upper_bound_count INTEGER,
              must_split_count INTEGER,
              last_run_id TEXT,
              last_mode TEXT,
              last_status TEXT,
              last_failure_class TEXT,
              trigger_counts_json TEXT,
              recent_history_json TEXT,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS artifact_index (
              project_id TEXT NOT NULL,
              artifact_key TEXT NOT NULL,
              artifact_path TEXT NOT NULL,
              task_id TEXT,
              run_id TEXT,
              sample_id TEXT,
              executor_id TEXT,
              artifact_kind TEXT,
              produced_at TEXT NOT NULL,
              storage_tier TEXT NOT NULL DEFAULT 'hot',
              PRIMARY KEY (project_id, artifact_key, artifact_path)
            );
            CREATE INDEX IF NOT EXISTS idx_artifact_index_run ON artifact_index(run_id, produced_at DESC);
            CREATE INDEX IF NOT EXISTS idx_artifact_index_sample ON artifact_index(sample_id, produced_at DESC);

            CREATE VIEW IF NOT EXISTS recent_process_events AS
            SELECT * FROM process_events WHERE storage_tier IN ('hot', 'warm');

            CREATE VIEW IF NOT EXISTS archived_process_events AS
            SELECT * FROM process_events WHERE storage_tier = 'archive';

            CREATE VIEW IF NOT EXISTS callback_event_coverage AS
            SELECT
              project_id,
              run_id,
              node_code,
              MAX(CASE WHEN json_extract(metadata_json, '$.callback_layer') = 'step' THEN 1 ELSE 0 END) AS has_step_callback,
              MAX(CASE WHEN json_extract(metadata_json, '$.callback_layer') = 'task' THEN 1 ELSE 0 END) AS has_task_callback,
              SUM(CASE WHEN event_type_code = 'artifact_read' THEN 1 ELSE 0 END) AS artifact_read_count,
              SUM(CASE WHEN event_type_code IN ('file_write', 'file_edit') THEN 1 ELSE 0 END) AS file_change_count,
              SUM(CASE WHEN event_type_code = 'command_finished' THEN 1 ELSE 0 END) AS command_count,
              SUM(CASE WHEN event_type_code IN ('reviewer_decision', 'validator_decision') THEN 1 ELSE 0 END) AS judgement_count,
              MAX(timestamp_ms) AS last_event_ts
            FROM process_events
            GROUP BY project_id, run_id, node_code;

            CREATE VIEW IF NOT EXISTS callback_contract_gaps AS
            SELECT
              project_id,
              run_id,
              node_code,
              has_step_callback,
              has_task_callback,
              artifact_read_count,
              file_change_count,
              command_count,
              judgement_count,
              CASE
                WHEN node_code IN ('orchestrator', 'builder', 'reviewer', 'validator') AND has_task_callback = 0 THEN 'missing_task_callback'
                WHEN node_code = 'builder' AND artifact_read_count = 0 THEN 'builder_missing_artifact_read'
                WHEN node_code = 'builder' AND file_change_count = 0 THEN 'builder_missing_file_change'
                WHEN node_code = 'reviewer' AND judgement_count = 0 THEN 'reviewer_missing_decision_event'
                WHEN node_code = 'validator' AND judgement_count = 0 THEN 'validator_missing_decision_event'
                ELSE ''
              END AS gap_code,
              last_event_ts
            FROM callback_event_coverage;
            """
        )
        _ensure_table_columns(conn, "experiment_runs", EXPERIMENT_RUN_OPTIONAL_COLUMNS)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_runs_dataset_version ON experiment_runs(dataset_version, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_runs_rules_version ON experiment_runs(rules_version, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_runs_decision_status ON experiment_runs(decision_status, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_runs_is_baseline ON experiment_runs(is_baseline, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_runs_baseline_of ON experiment_runs(baseline_of, created_at DESC)")
        conn.commit()
    finally:
        conn.close()


def _store_process_events(
    conn: sqlite3.Connection,
    *,
    events: list[dict[str, Any]],
    project_id: str,
    task_id: str,
    run_id: str = "",
    sample_id: str = "",
    executor_id: str = "",
    path_mode_code: str = "",
) -> None:
    if not events:
        return
    rows: list[tuple[Any, ...]] = []
    for event in events:
        target_type, target_id = _target_fields(event.get("target"))
        metadata = dict(event.get("metadata", {}) or {})
        rows.append(
            (
                str(event.get("event_id", "")),
                str(event.get("timestamp", "")),
                _timestamp_ms(str(event.get("timestamp", ""))),
                project_id,
                task_id,
                run_id or str(metadata.get("run_id", "") or ""),
                sample_id or str(metadata.get("sample_id", "") or ""),
                executor_id or str(metadata.get("executor_id", "") or ""),
                str(event.get("node", "")),
                str(event.get("event_type", "")),
                str(event.get("status", "")),
                str(event.get("summary", "")),
                target_type,
                target_id,
                int(event.get("duration_ms", 0) or 0),
                _reason_code_from_event(event),
                _phase_code_from_event(event),
                path_mode_code or str(metadata.get("path_mode_code", "") or ""),
                str(metadata.get("error_code", "") or ""),
                _json(metadata),
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO process_events (
          event_id, timestamp_text, timestamp_ms, project_id, task_id, run_id, sample_id, executor_id,
          node_code, event_type_code, status_code, summary_text, target_type, target_id, duration_ms,
          reason_code, phase_code, path_mode_code, error_code, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def ingest_runtime_state_snapshot(*, project_id: str, state: dict[str, Any], snapshot_path: str, emit_spool: bool = True) -> None:
    ensure_database()
    conn = _connect()
    try:
        artifacts = dict(state.get("artifacts", {}) or {})
        task_card = dict(state.get("task_card", {}) or {})
        input_task_payload = dict(artifacts.get("input_task_payload", {}) or {})
        visibility_artifacts = dict(artifacts.get("visibility_artifacts", {}) or {})
        process_events = list(((visibility_artifacts.get("process_events") or {}).get("latest") or (artifacts.get("process_events") or {}).get("latest") or []))
        dynamic_triggers = dict(visibility_artifacts.get("dynamic_triggers", {}) or artifacts.get("dynamic_triggers", {}) or {})
        routing_state = dict(dynamic_triggers.get("routing_state", {}) or {})
        builder_working_state = dict(artifacts.get("builder_working_state", {}) or {})
        input_task_payload = dict(artifacts.get("input_task_payload", {}) or {})
        path_mode_code = _path_mode_from_state(state)
        task_id = str(task_card.get("task_id", ""))
        run_id = str(task_card.get("run_id", "") or input_task_payload.get("run_id", "") or "")
        sample_id = str(task_card.get("sample_id", "") or input_task_payload.get("sample_id", "") or "")
        executor_id = str(task_card.get("executor_id", "") or input_task_payload.get("executor_id", "") or "")
        target_workspace_root = str(input_task_payload.get("target_workspace_root", "") or "")
        _store_process_events(
            conn,
            events=process_events,
            project_id=project_id,
            task_id=task_id,
            run_id=run_id,
            sample_id=sample_id,
            executor_id=executor_id,
            path_mode_code=path_mode_code,
        )
        conn.execute(
            """
            INSERT INTO task_runtime_facts (
              project_id, task_id, run_id, sample_id, executor_id, goal, active_phase, active_agent,
              task_kind, review_status, validation_status, approval_status, rework_count, risk_level,
              runtime_backend, runtime_status, runtime_duration_ms, failure_class, failure_disposition,
              path_mode_code, routing_state_json, builder_working_state_json, target_workspace_root, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
              task_id=excluded.task_id,
              run_id=excluded.run_id,
              sample_id=excluded.sample_id,
              executor_id=excluded.executor_id,
              goal=excluded.goal,
              active_phase=excluded.active_phase,
              active_agent=excluded.active_agent,
              task_kind=excluded.task_kind,
              review_status=excluded.review_status,
              validation_status=excluded.validation_status,
              approval_status=excluded.approval_status,
              rework_count=excluded.rework_count,
              risk_level=excluded.risk_level,
              runtime_backend=excluded.runtime_backend,
              runtime_status=excluded.runtime_status,
              runtime_duration_ms=excluded.runtime_duration_ms,
              failure_class=excluded.failure_class,
              failure_disposition=excluded.failure_disposition,
              path_mode_code=excluded.path_mode_code,
              routing_state_json=excluded.routing_state_json,
              builder_working_state_json=excluded.builder_working_state_json,
              target_workspace_root=excluded.target_workspace_root,
              updated_at=excluded.updated_at
            """,
            (
                project_id,
                task_id,
                run_id,
                sample_id,
                executor_id,
                str(state.get("goal", "")),
                str(state.get("active_phase", "")),
                str(state.get("active_agent", "")),
                str(state.get("task_kind", "")),
                str(state.get("review_status", "")),
                str(state.get("validation_status", "")),
                str(state.get("approval_status", "")),
                int(state.get("rework_count", 0) or 0),
                str(state.get("risk_level", "")),
                str(((artifacts.get("execution_runtime") or {}).get("backend", ""))),
                str(((artifacts.get("execution_runtime") or {}).get("status", ""))),
                int(((artifacts.get("execution_runtime") or {}).get("duration_ms", 0)) or 0),
                str(((artifacts.get("failure_state") or {}).get("failure_class", ""))),
                str(((artifacts.get("failure_state") or {}).get("disposition", ""))),
                path_mode_code,
                _json(routing_state),
                _json(builder_working_state),
                target_workspace_root,
                _utc_now(),
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO artifact_index (
              project_id, artifact_key, artifact_path, task_id, run_id, sample_id, executor_id, artifact_kind, produced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                "state_snapshot",
                str(snapshot_path),
                task_id,
                run_id,
                sample_id,
                executor_id,
                "runtime_state_snapshot",
                _utc_now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="runtime_state_snapshot",
            payload={"project_id": project_id, "snapshot_path": str(snapshot_path)},
            producer_repo_root=str(REPO_ROOT),
        )


def ingest_control_tower_state(
    *,
    project_id: str,
    task_id: str,
    phase: str,
    agent: str,
    status_path: str,
    ssot_path: str,
    failure_class: str,
    failure_disposition: str,
    path_mode_code: str = "",
    emit_spool: bool = True,
) -> None:
    ensure_database()
    conn = _connect()
    try:
        now = _utc_now()
        conn.executemany(
            """
            INSERT OR REPLACE INTO artifact_index (
              project_id, artifact_key, artifact_path, task_id, run_id, sample_id, executor_id, artifact_kind, produced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (project_id, "control_tower_status", status_path, task_id, "", "", "", "control_tower_status", now),
                (project_id, "ssot_state", ssot_path, task_id, "", "", "", "ssot_state", now),
            ],
        )
        conn.execute(
            """
            INSERT INTO task_runtime_facts (
              project_id, task_id, goal, active_phase, active_agent, failure_class, failure_disposition, path_mode_code, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
              task_id=excluded.task_id,
              active_phase=excluded.active_phase,
              active_agent=excluded.active_agent,
              failure_class=excluded.failure_class,
              failure_disposition=excluded.failure_disposition,
              path_mode_code=excluded.path_mode_code,
              updated_at=excluded.updated_at
            """,
            (project_id, task_id, "", phase, agent, failure_class, failure_disposition, path_mode_code, now),
        )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="control_tower_state",
            payload={
                "project_id": project_id,
                "task_id": task_id,
                "phase": phase,
                "agent": agent,
                "status_path": status_path,
                "ssot_path": ssot_path,
                "failure_class": failure_class,
                "failure_disposition": failure_disposition,
                "path_mode_code": path_mode_code,
            },
            producer_repo_root=str(REPO_ROOT),
        )


def record_validation_result(payload: dict[str, Any], *, source: str, emit_spool: bool = True) -> None:
    ensure_database()
    conn = _connect()
    try:
        repo_root = str(payload.get("repo_root", "") or "")
        conn.execute(
            """
            INSERT INTO validation_results (
              run_id, project_id, task_id, sample_id, executor_id, repo_root, specimen_root, phase,
              status_code, failure_class, failure_disposition, review_status, validation_status,
              near_upper_bound, over_upper_bound, must_split, upper_bound_trigger_dimensions_json,
              docker_mount_preview_json, result_path, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("run_id", "") or ""),
                str(payload.get("project_id", "") or ""),
                str(payload.get("task_id", "") or ""),
                str(payload.get("sample_id", "") or ""),
                _executor_id_from_repo_root(repo_root),
                repo_root,
                str(payload.get("specimen_root", "") or ""),
                str(payload.get("phase", "") or ""),
                str(payload.get("status", "") or ""),
                str(payload.get("failure_class", "") or ""),
                str(payload.get("failure_disposition", "") or ""),
                str(payload.get("review_status", "") or ""),
                str(payload.get("validation_status", "") or ""),
                int(bool(payload.get("near_upper_bound", False))),
                int(bool(payload.get("over_upper_bound", False))),
                int(bool(payload.get("must_split", False))),
                _json(list(payload.get("upper_bound_trigger_dimensions", []) or [])),
                _json(list(payload.get("docker_mount_preview", []) or [])),
                str(payload.get("result_json", "") or payload.get("result_path", "") or ""),
                source,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="validation_result",
            payload={"source": source, "result": payload},
            producer_repo_root=str(REPO_ROOT),
        )


def record_validation_run_summary(report: dict[str, Any], *, source: str, emit_spool: bool = True) -> None:
    ensure_database()
    conn = _connect()
    try:
        aggregate = dict(report.get("aggregate", {}) or {})
        failure_counts = dict(report.get("failure_counts", {}) or aggregate.get("failure_counts", {}) or {})
        total_cases = int(report.get("total_cases", 0) or aggregate.get("total_cases", 0) or len(list(report.get("results", []) or [])))
        completed_cases = int(report.get("completed_cases", 0) or aggregate.get("completed_cases", 0) or 0)
        failed_cases = int(aggregate.get("failed_cases", 0) or (total_cases - int(aggregate.get("passed_cases", 0) or 0)))
        conn.execute(
            """
            INSERT INTO validation_run_summaries (
              run_id, summary_kind, tier, batch_id, wave_id, mode, status_code, passed, total_cases, completed_cases,
              failed_cases, failure_counts_json, paths_json, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(report.get("run_id", "") or ""),
                str(report.get("summary_kind", "") or source),
                str(report.get("tier", "") or report.get("result", {}).get("tier", "") or ""),
                str(report.get("batch_id", "") or ""),
                str(report.get("wave", "") or ""),
                str(report.get("mode", "") or report.get("result", {}).get("mode", "") or ""),
                str(report.get("status", "") or ("completed" if report.get("passed") else "failed")),
                int(bool(report.get("passed", False))),
                total_cases,
                completed_cases,
                failed_cases,
                _json(failure_counts),
                _json(dict(report.get("paths", {}) or {})),
                source,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="validation_run_summary",
            payload={"source": source, "report": report},
            producer_repo_root=str(REPO_ROOT),
        )


def record_experiment_run(
    record: dict[str, Any],
    *,
    artifacts: dict[str, str] | None = None,
    emit_spool: bool = True,
) -> None:
    ensure_database()
    conn = _connect()
    try:
        experiment_id = str(record.get("experiment_id", "") or "")
        if not experiment_id:
            raise ValueError("experiment_id is required")
        artifact_root = str(record.get("artifact_root", "") or "")
        if not artifact_root:
            raise ValueError("artifact_root is required")
        artifact_map = dict(artifacts or {})
        now = _utc_now()
        conn.execute(
            """
            INSERT INTO experiment_runs (
              experiment_id, task_id, run_id, title, strategy_family, variant_name, instrument,
              data_source, date_range_start, date_range_end, entry_rule_summary, exit_rule_summary,
              execution_assumption, metrics_summary_json, review_outcome, memory_note_path,
              artifact_root, status_code, dataset_version, rules_version, decision_status,
              is_baseline, baseline_of, cost_assumption, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(experiment_id) DO UPDATE SET
              task_id=excluded.task_id,
              run_id=excluded.run_id,
              title=excluded.title,
              strategy_family=excluded.strategy_family,
              variant_name=excluded.variant_name,
              instrument=excluded.instrument,
              data_source=excluded.data_source,
              date_range_start=excluded.date_range_start,
              date_range_end=excluded.date_range_end,
              entry_rule_summary=excluded.entry_rule_summary,
              exit_rule_summary=excluded.exit_rule_summary,
              execution_assumption=excluded.execution_assumption,
              metrics_summary_json=excluded.metrics_summary_json,
              review_outcome=excluded.review_outcome,
              memory_note_path=excluded.memory_note_path,
              artifact_root=excluded.artifact_root,
              status_code=excluded.status_code,
              dataset_version=excluded.dataset_version,
              rules_version=excluded.rules_version,
              decision_status=excluded.decision_status,
              is_baseline=excluded.is_baseline,
              baseline_of=excluded.baseline_of,
              cost_assumption=excluded.cost_assumption,
              updated_at=excluded.updated_at
            """,
            (
                experiment_id,
                str(record.get("task_id", "") or ""),
                str(record.get("run_id", "") or ""),
                str(record.get("title", "") or ""),
                str(record.get("strategy_family", "") or ""),
                str(record.get("variant_name", "") or ""),
                str(record.get("instrument", "") or ""),
                str(record.get("data_source", "") or ""),
                str(record.get("date_range_start", "") or ""),
                str(record.get("date_range_end", "") or ""),
                str(record.get("entry_rule_summary", "") or ""),
                str(record.get("exit_rule_summary", "") or ""),
                str(record.get("execution_assumption", "") or ""),
                _json(dict(record.get("metrics_summary", {}) or {})),
                str(record.get("review_outcome", "") or ""),
                str(record.get("memory_note_path", "") or ""),
                artifact_root,
                str(record.get("status_code", "") or "recorded"),
                str(record.get("dataset_version", "") or ""),
                str(record.get("rules_version", "") or ""),
                str(record.get("decision_status", "") or ""),
                int(bool(record.get("is_baseline", False))),
                str(record.get("baseline_of", "") or ""),
                str(record.get("cost_assumption", "") or ""),
                str(record.get("created_at", "") or now),
                now,
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO artifact_index (
              project_id, artifact_key, artifact_path, task_id, run_id, sample_id, executor_id, artifact_kind, produced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(record.get("project_id", "ai-trading-system") or "ai-trading-system"),
                experiment_id,
                artifact_root,
                str(record.get("task_id", "") or ""),
                str(record.get("run_id", "") or ""),
                "",
                "",
                "experiment_root",
                now,
            ),
        )
        for artifact_kind, artifact_path in artifact_map.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO artifact_index (
                  project_id, artifact_key, artifact_path, task_id, run_id, sample_id, executor_id, artifact_kind, produced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(record.get("project_id", "ai-trading-system") or "ai-trading-system"),
                    experiment_id,
                    str(artifact_path),
                    str(record.get("task_id", "") or ""),
                    str(record.get("run_id", "") or ""),
                    "",
                    "",
                    artifact_kind,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="experiment_run",
            payload={"record": record, "artifacts": artifacts or {}},
            producer_repo_root=str(REPO_ROOT),
        )


def record_sample_usage_fact(record: dict[str, Any], *, emit_spool: bool = True) -> None:
    ensure_database()
    conn = _connect()
    try:
        sample_id = str(record.get("sample_id", "") or "")
        conn.execute(
            """
            INSERT INTO sample_usage_rollups (
              sample_id, tier, sample_family, purpose, expected_signal, use_count, failure_count,
              near_upper_bound_count, over_upper_bound_count, must_split_count, last_run_id, last_mode,
              last_status, last_failure_class, trigger_counts_json, recent_history_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sample_id) DO UPDATE SET
              tier=excluded.tier,
              sample_family=excluded.sample_family,
              purpose=excluded.purpose,
              expected_signal=excluded.expected_signal,
              use_count=excluded.use_count,
              failure_count=excluded.failure_count,
              near_upper_bound_count=excluded.near_upper_bound_count,
              over_upper_bound_count=excluded.over_upper_bound_count,
              must_split_count=excluded.must_split_count,
              last_run_id=excluded.last_run_id,
              last_mode=excluded.last_mode,
              last_status=excluded.last_status,
              last_failure_class=excluded.last_failure_class,
              trigger_counts_json=excluded.trigger_counts_json,
              recent_history_json=excluded.recent_history_json,
              updated_at=excluded.updated_at
            """,
            (
                sample_id,
                str(record.get("tier", "") or ""),
                str(record.get("sample_family", "") or ""),
                str(record.get("purpose", "") or ""),
                str(record.get("expected_signal", "") or ""),
                int(record.get("use_count", 0) or 0),
                int(record.get("failure_count", 0) or 0),
                int(record.get("near_upper_bound_count", 0) or 0),
                int(record.get("over_upper_bound_count", 0) or 0),
                int(record.get("must_split_count", 0) or 0),
                str(record.get("last_run_id", "") or ""),
                str(record.get("last_mode", "") or ""),
                str(record.get("last_status", "") or ""),
                str(record.get("last_failure_class", "") or ""),
                _json(dict(record.get("trigger_counts", {}) or {})),
                _json(list(record.get("recent_history", []) or [])),
                _utc_now(),
            ),
        )
        history = list(record.get("recent_history", []) or [])
        if history:
            latest = dict(history[-1])
            conn.execute(
                """
                INSERT INTO sample_usage_history (
                  sample_id, run_id, mode_code, status_code, failure_class, near_upper_bound,
                  over_upper_bound, must_split, trigger_dimensions_json, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample_id,
                    str(latest.get("run_id", "") or ""),
                    str(latest.get("mode", "") or ""),
                    str(latest.get("status", "") or ""),
                    str(latest.get("failure_class", "") or ""),
                    int(bool(latest.get("near_upper_bound", False))),
                    int(bool(latest.get("over_upper_bound", False))),
                    int(bool(latest.get("must_split", False))),
                    _json(list(latest.get("trigger_dimensions", []) or [])),
                    str(latest.get("timestamp", "") or _utc_now()),
                ),
            )
        conn.commit()
    finally:
        conn.close()
    if emit_spool:
        append_spool_record(
            kind="sample_usage_fact",
            payload={"record": record},
            producer_repo_root=str(REPO_ROOT),
        )


def apply_retention_policies() -> dict[str, int]:
    ensure_database()
    conn = _connect()
    counts: dict[str, int] = {}
    try:
        now = datetime.now(UTC)
        for table_name, rule in RETENTION_RULES.items():
            hot_cutoff = int((now - timedelta(days=rule.hot_days)).timestamp() * 1000)
            warm_cutoff = int((now - timedelta(days=rule.warm_days)).timestamp() * 1000)
            if table_name == "process_events":
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='warm' WHERE timestamp_ms > 0 AND timestamp_ms < ? AND storage_tier='hot'",
                    (hot_cutoff,),
                )
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='archive', archived_at=? WHERE timestamp_ms > 0 AND timestamp_ms < ? AND storage_tier != 'archive'",
                    (_utc_now(), warm_cutoff),
                )
            elif table_name in {"validation_results", "validation_run_summaries", "experiment_runs"}:
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='warm' WHERE strftime('%s', created_at) * 1000 < ? AND storage_tier='hot'",
                    (hot_cutoff,),
                )
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='archive' WHERE strftime('%s', created_at) * 1000 < ? AND storage_tier != 'archive'",
                    (warm_cutoff,),
                )
            elif table_name == "sample_usage_history":
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='warm' WHERE strftime('%s', recorded_at) * 1000 < ? AND storage_tier='hot'",
                    (hot_cutoff,),
                )
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='archive' WHERE strftime('%s', recorded_at) * 1000 < ? AND storage_tier != 'archive'",
                    (warm_cutoff,),
                )
            else:
                conn.execute(
                    f"UPDATE {table_name} SET storage_tier='archive' WHERE strftime('%s', produced_at) * 1000 < ? AND storage_tier != 'archive'",
                    (warm_cutoff,),
                )
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE storage_tier='archive'").fetchone()[0]
            counts[table_name] = int(count or 0)
        conn.commit()
    finally:
        conn.close()
    return counts


def purge_archived_records() -> dict[str, int]:
    ensure_database()
    conn = _connect()
    deleted: dict[str, int] = {}
    try:
        now = datetime.now(UTC)
        for table_name, rule in RETENTION_RULES.items():
            purge_cutoff = int((now - timedelta(days=rule.purge_days)).timestamp() * 1000)
            if table_name == "process_events":
                cursor = conn.execute(
                    f"DELETE FROM {table_name} WHERE storage_tier='archive' AND timestamp_ms > 0 AND timestamp_ms < ?",
                    (purge_cutoff,),
                )
            elif table_name in {"validation_results", "validation_run_summaries", "experiment_runs"}:
                cursor = conn.execute(
                    f"DELETE FROM {table_name} WHERE storage_tier='archive' AND strftime('%s', created_at) * 1000 < ?",
                    (purge_cutoff,),
                )
            elif table_name == "sample_usage_history":
                cursor = conn.execute(
                    f"DELETE FROM {table_name} WHERE storage_tier='archive' AND strftime('%s', recorded_at) * 1000 < ?",
                    (purge_cutoff,),
                )
            else:
                cursor = conn.execute(
                    f"DELETE FROM {table_name} WHERE storage_tier='archive' AND strftime('%s', produced_at) * 1000 < ?",
                    (purge_cutoff,),
                )
            deleted[table_name] = int(cursor.rowcount or 0)
        conn.commit()
    finally:
        conn.close()
    return deleted





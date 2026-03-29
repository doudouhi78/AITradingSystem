from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import mlflow

from ai_dev_os.experiment_store import read_experiment_artifacts
from ai_dev_os.project_objects import ExperimentRun
from ai_dev_os.project_objects import ResearchTask
from ai_dev_os.project_objects import validate_experiment_run
from ai_dev_os.project_objects import validate_research_task
from ai_dev_os.system_db import DB_PATH
from ai_dev_os.system_db import REPO_ROOT


MLFLOW_ROOT = REPO_ROOT / "runtime" / "mlflow"
MLFLOW_TRACKING_DB = MLFLOW_ROOT / "mlflow.db"
MLFLOW_ARTIFACTS_ROOT = MLFLOW_ROOT / "artifacts"
DEFAULT_EXPERIMENT_NAME = "ai-trading-system"


def ensure_mlflow_tracking_root() -> Path:
    MLFLOW_ROOT.mkdir(parents=True, exist_ok=True)
    MLFLOW_ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    return MLFLOW_ROOT


def configure_mlflow_tracking() -> str:
    ensure_mlflow_tracking_root()
    db_uri = f"sqlite:///{MLFLOW_TRACKING_DB.resolve().as_posix()}"
    mlflow.set_tracking_uri(db_uri)
    return db_uri


def ensure_mlflow_experiment(name: str = DEFAULT_EXPERIMENT_NAME) -> str:
    configure_mlflow_tracking()
    experiment = mlflow.get_experiment_by_name(name)
    if experiment is not None:
        return experiment.experiment_id
    return mlflow.create_experiment(name, artifact_location=MLFLOW_ARTIFACTS_ROOT.resolve().as_uri())


def _coerce_params(experiment_run: ExperimentRun) -> dict[str, str]:
    dataset_snapshot = experiment_run["dataset_snapshot"]
    rule_expression = experiment_run["rule_expression"]
    decision_status = experiment_run["decision_status"]
    return {
        "task_id": experiment_run["task_id"],
        "run_id": experiment_run["run_id"],
        "strategy_family": experiment_run["strategy_family"],
        "variant_name": experiment_run["variant_name"],
        "instrument": experiment_run["instrument"],
        "dataset_version": dataset_snapshot["dataset_version"],
        "data_source": dataset_snapshot["data_source"],
        "date_range_start": dataset_snapshot["date_range_start"],
        "date_range_end": dataset_snapshot["date_range_end"],
        "adjustment_mode": dataset_snapshot["adjustment_mode"],
        "cost_assumption": dataset_snapshot["cost_assumption"],
        "missing_value_policy": dataset_snapshot["missing_value_policy"],
        "rules_version": rule_expression["rules_version"],
        "entry_rule_summary": rule_expression["entry_rule_summary"],
        "exit_rule_summary": rule_expression["exit_rule_summary"],
        "decision_status": decision_status["decision_status"],
        "baseline_of": decision_status["baseline_of"],
        "status_code": experiment_run["status_code"],
    }


def _coerce_metrics(experiment_run: ExperimentRun) -> dict[str, float]:
    metrics_summary = dict(experiment_run["metrics_summary"])
    metrics: dict[str, float] = {}
    for source_key, target_key in [
        ("total_return", "total_return"),
        ("annual_return", "annual_return"),
        ("annualized_return", "annualized_return"),
        ("max_drawdown", "max_drawdown"),
        ("sharpe", "sharpe"),
        ("trade_count", "trade_count"),
        ("trades", "trades"),
        ("win_rate", "win_rate"),
    ]:
        if source_key not in metrics_summary:
            continue
        try:
            metrics[target_key] = float(metrics_summary[source_key])
        except (TypeError, ValueError):
            continue
    return metrics


def _coerce_tags(research_task: ResearchTask, experiment_run: ExperimentRun) -> dict[str, str]:
    review_outcome = experiment_run["review_outcome"]
    decision_status = experiment_run["decision_status"]
    risk_position_note = experiment_run["risk_position_note"]
    return {
        "experiment_id": experiment_run["experiment_id"],
        "title": experiment_run["title"],
        "goal": research_task["goal"],
        "hypothesis": research_task["hypothesis"],
        "review_outcome": review_outcome["review_outcome"],
        "review_status": review_outcome["review_status"],
        "recommended_next_step": review_outcome["recommended_next_step"],
        "is_baseline": str(decision_status["is_baseline"]).lower(),
        "decision_reason": decision_status["decision_reason"],
        "memory_note_path": experiment_run["memory_note_path"],
        "artifact_root": experiment_run["artifact_root"],
        "position_sizing_method": risk_position_note["position_sizing_method"],
        "exit_after_signal_policy": risk_position_note["exit_after_signal_policy"],
        "schema_version": "project_objects_v4",
    }


def log_experiment_run_to_mlflow(
    *,
    research_task: ResearchTask | dict[str, Any],
    experiment_run: ExperimentRun | dict[str, Any],
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
) -> str:
    task = validate_research_task(research_task)
    run = validate_experiment_run(experiment_run)
    experiment_id = ensure_mlflow_experiment(experiment_name)
    run_name = f"{run['variant_name']}::{run['instrument']}"
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as active_run:
        mlflow.log_params(_coerce_params(run))
        mlflow.log_metrics(_coerce_metrics(run))
        mlflow.set_tags(_coerce_tags(task, run))
        artifact_root = Path(run["artifact_root"])
        if artifact_root.exists():
            mlflow.log_artifacts(str(artifact_root), artifact_path="experiment_artifacts")
        mlflow.log_text(json.dumps(task, ensure_ascii=False, indent=2), "project_objects/research_task.json")
        mlflow.log_text(json.dumps(run, ensure_ascii=False, indent=2), "project_objects/experiment_run.json")
        return active_run.info.run_id


def _fetch_experiment_index_record(experiment_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM experiment_runs WHERE experiment_id = ?", (experiment_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f"experiment_id {experiment_id} not found in sqlite index")
    payload = dict(row)
    metrics_summary_json = payload.get("metrics_summary_json", "") or "{}"
    payload["metrics_summary"] = json.loads(metrics_summary_json)
    return payload


def build_experiment_run_from_storage(experiment_id: str) -> tuple[ResearchTask, ExperimentRun]:
    artifacts = read_experiment_artifacts(experiment_id)
    record = _fetch_experiment_index_record(experiment_id)
    research_task = artifacts["inputs"]["research_task"]
    experiment_run: ExperimentRun = {
        "project_id": str(record.get("project_id", "") or "ai-trading-system"),
        "experiment_id": str(record["experiment_id"]),
        "task_id": str(record.get("task_id", "") or ""),
        "run_id": str(record.get("run_id", "") or ""),
        "title": str(record.get("title", "") or artifacts["manifest"].get("title", "")),
        "strategy_family": str(record.get("strategy_family", "") or artifacts["manifest"].get("strategy_family", "")),
        "variant_name": str(record.get("variant_name", "") or artifacts["manifest"].get("variant_name", "")),
        "instrument": str(record.get("instrument", "") or artifacts["manifest"].get("instrument", "")),
        "dataset_snapshot": artifacts["inputs"]["dataset_snapshot"],
        "rule_expression": artifacts["inputs"]["rule_expression"],
        "metrics_summary": artifacts["results"]["metrics_summary"],
        "risk_position_note": artifacts["results"]["risk_position_note"],
        "review_outcome": artifacts["results"]["review_outcome"],
        "decision_status": artifacts["results"]["decision_status"],
        "artifact_root": artifacts["artifact_root"],
        "memory_note_path": str(record.get("memory_note_path", "") or ""),
        "status_code": str(record.get("status_code", "") or artifacts["manifest"].get("status_code", "")),
        "created_at": str(record.get("created_at", "") or artifacts["manifest"].get("created_at", "")),
    }
    return research_task, experiment_run


def sync_existing_experiment_to_mlflow(
    experiment_id: str,
    *,
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
) -> str:
    research_task, experiment_run = build_experiment_run_from_storage(experiment_id)
    return log_experiment_run_to_mlflow(
        research_task=research_task,
        experiment_run=experiment_run,
        experiment_name=experiment_name,
    )


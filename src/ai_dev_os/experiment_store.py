from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.project_objects import ExperimentRun
from ai_dev_os.project_objects import ResearchTask
from ai_dev_os.project_objects import build_experiment_artifact_payload


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS_ROOT = REPO_ROOT / "runtime" / "experiments"


def ensure_experiments_root() -> Path:
    EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)
    return EXPERIMENTS_ROOT


def experiment_root(experiment_id: str) -> Path:
    return ensure_experiments_root() / experiment_id


def read_experiment_artifacts(experiment_id: str) -> dict[str, Any]:
    root = experiment_root(experiment_id)
    manifest_path = root / "manifest.json"
    inputs_path = root / "inputs.json"
    results_path = root / "results.json"
    notes_path = root / "notes.md"
    return {
        "artifact_root": str(root),
        "manifest": json.loads(manifest_path.read_text(encoding="utf-8")),
        "inputs": json.loads(inputs_path.read_text(encoding="utf-8")),
        "results": json.loads(results_path.read_text(encoding="utf-8")),
        "notes_markdown": notes_path.read_text(encoding="utf-8"),
        "manifest_path": str(manifest_path),
        "inputs_path": str(inputs_path),
        "results_path": str(results_path),
        "notes_path": str(notes_path),
    }


def write_experiment_artifacts(
    *,
    research_task: ResearchTask | dict[str, Any],
    experiment_run: ExperimentRun | dict[str, Any],
    notes_markdown: str,
) -> dict[str, str]:
    payload = build_experiment_artifact_payload(
        research_task=research_task,
        experiment_run=experiment_run,
    )
    manifest = payload["manifest"]
    inputs = payload["inputs"]
    results = payload["results"]
    root = experiment_root(str(manifest["experiment_id"]))
    root.mkdir(parents=True, exist_ok=True)

    manifest_path = root / "manifest.json"
    inputs_path = root / "inputs.json"
    results_path = root / "results.json"
    notes_path = root / "notes.md"

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    inputs_path.write_text(
        json.dumps(inputs, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    results_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    notes_path.write_text(notes_markdown.rstrip() + "\n", encoding="utf-8")

    return {
        "artifact_root": str(root),
        "manifest": str(manifest_path),
        "inputs": str(inputs_path),
        "results": str(results_path),
        "notes": str(notes_path),
    }

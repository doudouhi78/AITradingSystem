import hashlib
import json
import shutil
from pathlib import Path

from langgraph.graph import END
from langgraph.graph import START
from langgraph.graph import StateGraph

from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.registry import get_agent_spec
from ai_dev_os.registry import PRIMARY_AGENT_ORDER
from ai_dev_os.state import KernelInput
from ai_dev_os.state import KernelState
from ai_dev_os.trigger_protocol import build_trigger_protocol
from ai_dev_os.trigger_protocol import reassess_trigger_protocol


_SHARED_WORKSPACE_SYNC_DIRS = (
    "config",
    "doctrine",
    "scripts",
    "src",
    "test_assets",
    "tests",
)

_SHARED_WORKSPACE_SYNC_FILES = (
    ".env",
    ".gitignore",
    "langgraph.json",
    "MAINLINE_EXECUTION_CHECKLIST_v1.md",
    "package-lock.json",
    "package.json",
    "pyproject.toml",
    "pytest.ini",
    "README.md",
    "requirements.txt",
    "SYSTEM_OPERATOR_ONBOARDING_v1.md",
)

_SHARED_WORKSPACE_TRANSIENTS = (
    "$null",
    "BUILD_PLAN.json",
    "CODEX_BUILDER_EXECUTE_TASK.md",
    "CODEX_BUILDER_TASK.md",
    "CODEX_ORCHESTRATOR_TASK.md",
    "CODEX_REVIEW_TASK.md",
    "ORCHESTRATOR_TASK_SHAPING.json",
    "QWEN_BUILDER_TASK.md",
    "REVIEW_ASSESSMENT.json",
)


_SHARED_WORKSPACE_BASELINE_FILENAME = "shared_workspace_baseline.json"


def _hash_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _shared_workspace_trackable(candidate: Path, workspace_root: Path) -> bool:
    rel = candidate.relative_to(workspace_root).as_posix()
    if '/__pycache__/' in f'/{rel}/' or rel.endswith('.pyc'):
        return False
    return True


def _snapshot_shared_workspace(workspace_root: Path) -> dict[str, object]:
    files_by_hash: dict[str, str] = {}
    for dirname in _SHARED_WORKSPACE_SYNC_DIRS:
        base_dir = workspace_root / dirname
        if not base_dir.exists():
            continue
        for candidate in sorted(path for path in base_dir.rglob('*') if path.is_file() and _shared_workspace_trackable(path, workspace_root)):
            rel = candidate.relative_to(workspace_root).as_posix()
            files_by_hash[rel] = _hash_file(candidate)
    for filename in _SHARED_WORKSPACE_SYNC_FILES:
        candidate = workspace_root / filename
        if candidate.exists() and candidate.is_file():
            files_by_hash[candidate.relative_to(workspace_root).as_posix()] = _hash_file(candidate)
    return {
        "dirs": list(_SHARED_WORKSPACE_SYNC_DIRS),
        "files": list(_SHARED_WORKSPACE_SYNC_FILES),
        "files_by_hash": files_by_hash,
    }


def _write_shared_workspace_baseline(workspace_root: Path) -> str:
    role_dir = workspace_root / '.role'
    role_dir.mkdir(parents=True, exist_ok=True)
    baseline_path = role_dir / _SHARED_WORKSPACE_BASELINE_FILENAME
    baseline_path.write_text(json.dumps(_snapshot_shared_workspace(workspace_root), ensure_ascii=False, indent=2) + "\n", encoding='utf-8')
    return str(baseline_path)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sync_shared_workspace_to_repo_baseline(target_workspace_root: str) -> None:
    workspace_root = Path(str(target_workspace_root or "").strip())
    if not workspace_root:
        return
    workspace_root = workspace_root.resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    repo_root = _repo_root()

    for transient_name in _SHARED_WORKSPACE_TRANSIENTS:
        transient_path = workspace_root / transient_name
        if transient_path.exists() and transient_path.is_file():
            transient_path.unlink()

    for cache_name in (".pytest_cache", ".ruff_cache"):
        cache_path = workspace_root / cache_name
        if cache_path.exists():
            shutil.rmtree(cache_path, ignore_errors=True)

    for dirname in _SHARED_WORKSPACE_SYNC_DIRS:
        source_dir = repo_root / dirname
        target_dir = workspace_root / dirname
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        if source_dir.exists():
            shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)

    for filename in _SHARED_WORKSPACE_SYNC_FILES:
        source_file = repo_root / filename
        target_file = workspace_root / filename
        if source_file.exists() and source_file.is_file():
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, target_file)


def initialize_state(data: KernelInput) -> KernelState:
    target_workspace_root = str(data.get("target_workspace_root", "") or "").strip()
    if not target_workspace_root:
        target_workspace_root = str(get_agent_settings("builder").workspace_root or "").strip()
    _sync_shared_workspace_to_repo_baseline(target_workspace_root)
    workspace_baseline_path = _write_shared_workspace_baseline(Path(target_workspace_root).resolve()) if target_workspace_root else ""
    return {
        "project_id": data["project_id"],
        "goal": data["goal"],
        "task_kind": "standard",
        "active_phase": "idle",
        "active_agent": "orchestrator",
        "blocking_issue": "",
        "task_card": {},
        "scan_result": "",
        "build_result": "",
        "execution_result": "",
        "review_result": "",
        "review_status": "not_run",
        "review_feedback": "",
        "validation_result": "",
        "validation_status": "not_run",
        "validation_feedback": "",
        "risk_level": "low",
        "approval_required": False,
        "approval_status": "not_needed",
        "human_decision": data.get("human_decision", ""),
        "recorder_summary": "",
        "artifacts": {
            "input_task_payload": {
                "raw_intent": data.get("raw_intent", data["goal"]),
                "modeled_task": dict(data.get("modeled_task", {}) or {}),
                "standard_task_unit": dict(data.get("standard_task_unit", {}) or {}),
                "target_workspace_root": target_workspace_root,
                "run_id": str(data.get("run_id", "") or "").strip(),
                "sample_id": str(data.get("sample_id", "") or "").strip(),
                "executor_id": str(data.get("executor_id", "") or "").strip(),
                "started_at": str(data.get("started_at", "") or "").strip(),
                "workspace_baseline_path": workspace_baseline_path,
            }
        },
        "steps": [],
        "rework_count": 0,
        "max_rework_rounds": 2,
    }


def _task_profile(state: KernelState) -> str:
    return str(state.get("artifacts", {}).get("orchestrator_analysis", {}).get("task_profile", "routine"))


def _recent_process_events(state: KernelState) -> list[dict[str, object]]:
    artifacts = state.get("artifacts", {}) or {}
    raw = artifacts.get("process_events") or {}
    if isinstance(raw, dict):
        events = raw.get("latest") or raw.get("history") or []
    elif isinstance(raw, list):
        events = raw
    else:
        events = []
    return [event for event in events if isinstance(event, dict)][-12:]


def _dynamic_trigger_plan(state: KernelState) -> dict[str, bool | str]:
    trigger_plan = state.get("artifacts", {}).get("dynamic_triggers", {})
    if trigger_plan:
        updated = reassess_trigger_protocol(trigger_plan, recent_events=_recent_process_events(state))
        state.setdefault("artifacts", {})["dynamic_triggers"] = updated
        return updated
    updated = build_trigger_protocol(
        task_kind=state.get("task_kind", "standard"),
        task_profile=_task_profile(state),
        rework_count=state.get("rework_count", 0),
    )
    state.setdefault("artifacts", {})["dynamic_triggers"] = updated
    return updated


def route_after_review(state: KernelState) -> str:
    if state["review_status"] == "changes_requested":
        return get_agent_spec("approval").node_id if state.get("approval_required", False) else get_agent_spec("builder").node_id
    return get_agent_spec("approval").node_id if state["approval_required"] else get_agent_spec("auto_approve").node_id


def route_after_orchestrator(state: KernelState) -> str:
    _dynamic_trigger_plan(state)
    response = dict(state.get("artifacts", {}).get("orchestrator_response", {}) or {})
    response_type = str(response.get("type", "") or "").strip().lower()
    if response_type in {"clarification_request", "split_request", "escalation_request"}:
        return "end"
    return get_agent_spec("builder").node_id



def route_after_approval(state: KernelState) -> str:
    if state["approval_status"] == "approved":
        return get_agent_spec("recorder").node_id
    if state.get("approval_status") == "changes_requested" and state.get("review_status") == "changes_requested":
        return get_agent_spec("builder").node_id
    return "end"


def build_graph():
    builder = StateGraph(KernelState, input_schema=KernelInput, output_schema=KernelState)
    builder.add_node("initialize_state", initialize_state)

    for role in PRIMARY_AGENT_ORDER:
        spec = get_agent_spec(role)
        builder.add_node(spec.node_id, spec.handler)

    for role in ("approval", "auto_approve"):
        spec = get_agent_spec(role)
        builder.add_node(spec.node_id, spec.handler)

    orchestrator = get_agent_spec("orchestrator").node_id
    planner = get_agent_spec("builder").node_id
    reviewer = get_agent_spec("reviewer").node_id
    approval = get_agent_spec("approval").node_id
    auto_approve = get_agent_spec("auto_approve").node_id
    recorder = get_agent_spec("recorder").node_id

    builder.add_edge(START, "initialize_state")
    builder.add_edge("initialize_state", orchestrator)
    builder.add_conditional_edges(
        orchestrator,
        route_after_orchestrator,
        {
            planner: planner,
        },
    )
    builder.add_edge(planner, reviewer)
    builder.add_conditional_edges(
        reviewer,
        route_after_review,
        {
            planner: planner,
            approval: approval,
            auto_approve: auto_approve,
        },
    )
    builder.add_conditional_edges(
        approval,
        route_after_approval,
        {
            recorder: recorder,
            "end": END,
        },
    )
    builder.add_edge(auto_approve, recorder)
    builder.add_edge(recorder, END)
    return builder.compile()


graph = build_graph()





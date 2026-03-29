from __future__ import annotations

import hashlib
import json
import re
import subprocess

from pathlib import Path
from time import perf_counter

from ai_dev_os.builder_backend import get_builder_backend_name
from ai_dev_os.builder_backend import get_builder_plan_with_backend_diagnostics
from ai_dev_os.execution_engine import run_controlled_execution
from ai_dev_os.execution_runtime import RuntimeAction
from ai_dev_os.execution_runtime import build_docker_command
from ai_dev_os.execution_runtime import execute_runtime_actions
from ai_dev_os.execution_runtime import load_execution_runtime_config
from ai_dev_os.execution_runtime import runtime_execution_to_dict
from ai_dev_os.feedback_protocol import classify_reviewer_feedback
from ai_dev_os.feedback_protocol import classify_validator_feedback
from ai_dev_os.governance import forbidden_changes
from ai_dev_os.governance_contract import build_governance_contract
from ai_dev_os.governance import optimization_project_constraints
from ai_dev_os.governance import optimization_task_constraints
from ai_dev_os.governance import project_constraints
from ai_dev_os.governance import rework_keywords
from ai_dev_os.governance import risk_keywords
from ai_dev_os.governance import requires_human_approval
from ai_dev_os.governance import task_constraints
from ai_dev_os.io_utils import CONTROL_TOWER_PATH
from ai_dev_os.io_utils import SSOT_STATE_PATH
from ai_dev_os.io_utils import append_markdown
from ai_dev_os.io_utils import ensure_project_scaffold
from ai_dev_os.io_utils import now_iso
from ai_dev_os.io_utils import save_state_snapshot
from ai_dev_os.io_utils import write_json
from ai_dev_os.llm_interface import BuilderPlanCallError
from ai_dev_os.llm_interface import get_orchestrator_task_design
from ai_dev_os.llm_interface import get_reviewer_assessment
from ai_dev_os.memory_manager import append_memory_index
from ai_dev_os.memory_manager import append_timeline_entry
from ai_dev_os.memory_manager import archive_phase_snapshot
from ai_dev_os.memory_manager import initialize_memory_indexes
from ai_dev_os.memory_manager import write_project_summary
from ai_dev_os.observer import run_meta_observer
from ai_dev_os.project_runtime_context import summarize_project_runtime
from ai_dev_os.review_handoff import build_reviewer_handoff_packet
from ai_dev_os.review_handoff import load_reviewer_handoff_packet
from ai_dev_os.review_handoff import persist_reviewer_handoff_packet
from ai_dev_os.review_handoff import persist_reviewer_patch_artifact
from ai_dev_os.memory_scope import build_memory_scope_bundle
from ai_dev_os.execution_journal import journal_node_completed
from ai_dev_os.execution_journal import journal_node_started
from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.state import BuilderWorkingState
from ai_dev_os.state import KernelState
from ai_dev_os.state import ProcessEvent
from ai_dev_os.state import TaskCard
from ai_dev_os.trigger_protocol import build_trigger_protocol
from ai_dev_os.trigger_protocol import reassess_trigger_protocol
from ai_dev_os.system_db import ingest_control_tower_state
from ai_dev_os.validation_contract import load_validation_contract
from ai_dev_os.tool_bus import collect_git_diff_evidence
from ai_dev_os.role_memory import append_lessons
from ai_dev_os.role_memory import clear_working_memory
from ai_dev_os.role_memory import compress_lessons
from ai_dev_os.role_memory import ensure_role_memory_scaffold
from ai_dev_os.role_memory import get_role_memory_dir
from ai_dev_os.role_memory import list_promotion_candidates
from ai_dev_os.role_memory import should_compress_lessons
from ai_dev_os.role_memory import write_working_memory


CONTROL_ARTIFACT_KEYS = {
    "paths",
    "governance",
    "governance_contract",
    "orchestrator_analysis",
    "project_memory_context",
    "failure_state",
    "builder_working_state",
}
VISIBILITY_ARTIFACT_KEYS = {
    "observer",
    "side_feedback",
    "dynamic_triggers",
    "inspection_state",
    "human_visibility",
    "process_events",
    "agent_round_reports",
}
DIAGNOSTIC_ARTIFACT_KEYS = {
    "active_node_diagnostic",
    "node_diagnostics",
    "last_node_diagnostic",
}
OUTPUT_ARTIFACT_KEYS = {
    "governance_outputs",
    "human_outputs",
}

TASK_CARD_REQUIRED_FIELDS = (
    'task_id',
    'goal',
    'task_profile',
    'scope_hint',
    'constraints',
    'acceptance_tests',
    'risk_level',
    'assigned_agents',
    'orchestrator_brief',
)


def _safe_list_preview(items: object, *, limit: int = 4) -> list[str]:
    values = list(items or []) if isinstance(items, list) else []
    return [str(item).strip() for item in values[:limit] if str(item).strip()]


def _update_role_working_memory(
    role: str,
    *,
    task_id: str,
    goal: str,
    status: str,
    facts: list[str] | None = None,
    decisions: list[str] | None = None,
    progress: str = '',
) -> None:
    try:
        ensure_role_memory_scaffold(role)
        write_working_memory(
            role,
            task_id,
            goal,
            status=status,
            facts=facts or [],
            decisions=decisions or [],
            progress=progress,
        )
    except Exception:
        pass


def _lesson_slug(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r'[^a-z0-9]+', '-', str(value or '').strip().lower()).strip('-')
    return cleaned[:48] or fallback



def _lesson_entry(lesson_id: str, task_profile: str, lines: list[str]) -> str:
    payload = [f"## L-{lesson_id}", f"Tags: [{task_profile}] [all]"]
    payload.extend(str(item).strip() for item in lines if str(item).strip())
    return "\n".join(payload)



def _derive_role_lessons(state: KernelState) -> dict[str, list[str]]:
    task_card = dict(state.get('task_card', {}) or {})
    task_id = str(task_card.get('task_id', '') or state.get('project_id', '') or 'unknown-task')
    task_profile = str(task_card.get('task_profile', 'routine') or 'routine').strip().lower() or 'routine'
    goal = str(state.get('goal', '') or task_card.get('goal', '') or '').strip()
    goal_short = _short_text(goal, 96)
    lesson_suffix = _lesson_slug(task_id, fallback='task')
    risk_level = str(task_card.get('risk_level', 'low') or 'low').strip().lower()
    scope_preview = ', '.join(_safe_list_preview(task_card.get('scope', []), limit=3))
    rework_count = int(state.get('rework_count', 0) or 0)

    response = dict(((state.get('artifacts', {}) or {}).get('orchestrator_response', {}) or {})
                    or ((state.get('artifacts', {}) or {}).get('orchestrator_analysis', {}) or {}))
    execution_evidence = dict(((state.get('artifacts', {}) or {}).get('execution_evidence', {}) or {}))
    changed_files = _safe_list_preview(execution_evidence.get('changed_files', []), limit=4)
    review_status = str(state.get('review_status', '') or '').strip().lower()
    review_feedback = _short_text(state.get('review_feedback', '') or state.get('review_result', ''), 180)
    build_result_text = str(state.get('build_result', '') or '')
    builder_working_state = dict((((state.get('artifacts', {}) or {}).get('builder_working_state', {}) or {})))

    # 从 BUILD_PLAN.json 取 confidence（如果 builder 输出了完整记录）
    build_plan = dict((state.get('artifacts', {}) or {}).get('build_plan', {}) or {})
    confidence = str(build_plan.get('confidence', '') or builder_working_state.get('confidence', '') or 'unknown').strip().lower()

    lessons: dict[str, list[str]] = {'orchestrator': [], 'builder': [], 'reviewer': []}

    # ── Orchestrator lessons ──────────────────────────────────────────
    response_type = str(response.get('type', '') or '').strip().lower()
    if response_type in {'clarification_request', 'split_request', 'escalation_request'}:
        reason = _short_text(response.get('reason', '') or state.get('blocking_issue', ''), 160)
        lessons['orchestrator'].append(_lesson_entry(
            f'orchestrator-{response_type}-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                f'- Returned {response_type} instead of forcing a task card.',
                f'- Reason: {reason}',
            ],
        ))
    else:
        # 任务卡质量反馈：是否一次通过，还是 Builder 多次返工
        brief = _short_text(response.get('builder_brief', '') or task_card.get('orchestrator_brief', ''), 140)
        outcome = 'first-pass approved' if rework_count == 0 and review_status == 'approved' \
            else f'required {rework_count} rework round(s)' if rework_count > 0 \
            else 'outcome not recorded'
        lessons['orchestrator'].append(_lesson_entry(
            f'orchestrator-task-card-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                f'- task_profile={task_profile}, risk_level={risk_level}, scope={scope_preview or "not specified"}.',
                f'- Downstream outcome: {outcome}.',
                f'- Builder brief: {brief or "none"}.',
            ],
        ))

    # ── Builder lessons ───────────────────────────────────────────────
    if 'input_rejection' in build_result_text:
        rejection_reason = _short_text(build_result_text, 200)
        lessons['builder'].append(_lesson_entry(
            f'builder-input-rejection-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                '- Builder rejected the task card before starting — correct behavior when input is invalid.',
                f'- Rejection detail: {rejection_reason}',
            ],
        ))
    elif 'checkpoint' in build_result_text or 'escalation_request' in build_result_text or builder_working_state.get('checkpoint_required'):
        blocker = _short_text(state.get('blocking_issue', '') or builder_working_state.get('blocker', ''), 160)
        lessons['builder'].append(_lesson_entry(
            f'builder-checkpoint-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                '- Builder emitted checkpoint/escalation when direction or premise required human intervention.',
                f'- Blocker: {blocker or "none recorded"}.',
                f'- Confidence at checkpoint: {confidence}.',
            ],
        ))
    else:
        verification_status = str(execution_evidence.get('runtime_status', '') or 'not_recorded').strip()
        rework_note = f'passed Reviewer after {rework_count} rework(s)' if rework_count > 0 else 'passed Reviewer on first submission'
        lessons['builder'].append(_lesson_entry(
            f'builder-direct-execution-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                f'- Changed: {", ".join(changed_files) if changed_files else "no changed_files recorded"}.',
                f'- Confidence: {confidence}, verification: {verification_status}.',
                f'- Review outcome: {rework_note}.',
            ],
        ))

    # ── Reviewer lessons ──────────────────────────────────────────────
    if review_status == 'changes_requested':
        lessons['reviewer'].append(_lesson_entry(
            f'reviewer-rework-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                f'- Requested rework (round {rework_count}). Builder confidence was {confidence}.',
                f'- Issues found: {review_feedback or "none recorded"}.',
                f'- Files reviewed: {", ".join(changed_files) if changed_files else "no changed_files evidence"}.',
            ],
        ))
    elif review_status:
        lessons['reviewer'].append(_lesson_entry(
            f'reviewer-{review_status}-{lesson_suffix}',
            task_profile,
            [
                f'- Goal: {goal_short}',
                f'- Decision: {review_status}. Rework rounds before approval: {rework_count}.',
                f'- Files reviewed: {", ".join(changed_files) if changed_files else "no changed_files evidence"}.',
                f'- Builder confidence at approval: {confidence}.',
            ],
        ))

    return lessons


def _finalize_role_memory_after_record(state: KernelState) -> dict[str, object]:
    flags: dict[str, object] = {'lessons_need_compression': {}, 'promotion_candidates': {}}
    lessons_by_role = _derive_role_lessons(state)
    for role in ('orchestrator', 'builder', 'reviewer'):
        try:
            append_lessons(role, lessons_by_role.get(role, []))
            if should_compress_lessons(role):
                compress_lessons(role)
            flags['lessons_need_compression'][role] = should_compress_lessons(role)
            candidates = list_promotion_candidates(role)
            flags['promotion_candidates'][role] = candidates
            report_path = get_role_memory_dir(role) / 'promotion_candidates.json'
            report_payload = {
                'role': role,
                'task_id': str(((state.get('task_card') or {}) if isinstance(state.get('task_card'), dict) else {}).get('task_id', '')),
                'generated_at': now_iso(),
                'candidates': candidates,
            }
            report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding='utf-8')
            clear_working_memory(role)
        except Exception:
            flags['lessons_need_compression'][role] = False
            flags['promotion_candidates'][role] = []
    try:
        clear_working_memory('recorder')
    except Exception:
        pass
    return flags


def _git_diff_stat_for_workspace(workspace_root: str) -> str:
    root = str(workspace_root or '').strip()
    if not root:
        return ''
    try:
        completed = subprocess.run(
            ['git', '-C', root, 'diff', '--stat'],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )
    except Exception:
        return ''
    if completed.returncode != 0:
        return ''
    return str(completed.stdout or '').strip()


def _readonly_workspace_violation(role: str, before_stat: str, after_stat: str) -> dict[str, object]:
    before = str(before_stat or '').strip()
    after = str(after_stat or '').strip()
    if after == before:
        return {}
    return {
        'role': role,
        'before_diff_stat': before,
        'after_diff_stat': after,
        'status': 'readonly_violation',
    }


def _hash_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()

def _trackable_changed_file(rel_path: str) -> bool:
    normalized = str(rel_path or '').strip().replace('\\', '/')
    if not normalized:
        return False
    if '/__pycache__/' in f'/{normalized}/' or normalized.endswith('.pyc'):
        return False
    return True

def _baseline_changed_files_for_workspace(workspace_root: str, *, limit: int = 8) -> list[str]:
    root = Path(str(workspace_root or '').strip())
    if not root:
        return []
    baseline_path = root / '.role' / 'shared_workspace_baseline.json'
    if not baseline_path.exists():
        return []
    try:
        baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
    except Exception:
        return []
    dirs = [str(item).strip() for item in list(baseline.get('dirs', []) or []) if str(item).strip()]
    files = [str(item).strip() for item in list(baseline.get('files', []) or []) if str(item).strip()]
    expected = dict(baseline.get('files_by_hash', {}) or {})
    current: dict[str, str] = {}
    for dirname in dirs:
        base_dir = root / dirname
        if not base_dir.exists():
            continue
        for candidate in sorted(path for path in base_dir.rglob('*') if path.is_file()):
            rel = candidate.relative_to(root).as_posix()
            if not _trackable_changed_file(rel):
                continue
            current[rel] = _hash_file(candidate)
    for filename in files:
        candidate = root / filename
        if candidate.exists() and candidate.is_file():
            rel = candidate.relative_to(root).as_posix()
            if not _trackable_changed_file(rel):
                continue
            current[rel] = _hash_file(candidate)
    changed = sorted(rel for rel in ({rel for rel, digest in current.items() if expected.get(rel) != digest} | {rel for rel in expected if rel not in current}) if _trackable_changed_file(rel))
    return changed[: max(1, limit)]

def _baseline_diff_summary_for_workspace(workspace_root: str, *, limit: int = 8) -> str:
    changed = _baseline_changed_files_for_workspace(workspace_root, limit=limit)
    if not changed:
        return ''
    label = 'file' if len(changed) == 1 else 'files'
    return f"{len(changed)} changed {label}: " + ", ".join(changed[: max(1, limit)])



def _git_changed_files_for_workspace(workspace_root: str, *, limit: int = 8) -> list[str]:
    baseline_files = _baseline_changed_files_for_workspace(workspace_root, limit=limit)
    if baseline_files:
        return baseline_files
    root = str(workspace_root or '').strip()
    if not root:
        return []
    try:
        completed = subprocess.run(
            ['git', '-C', root, 'diff', '--name-only'],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )
    except Exception:
        return []
    if completed.returncode != 0:
        return []
    files = [
        rel for rel in
        [line.strip().replace('\\', '/') for line in str(completed.stdout or '').splitlines() if line.strip()]
        if _trackable_changed_file(rel)
    ]
    return files[: max(1, limit)]


def _compact_memory_scope_bundle(bundle: dict[str, object] | None) -> dict[str, object]:
    scope = dict(bundle or {})
    layers = dict(scope.get('layers', {}) or {})
    compact_layers: dict[str, object] = {}
    for name, payload in layers.items():
        payload_dict = dict(payload or {}) if isinstance(payload, dict) else {}
        compact_layers[str(name)] = {
            'excerpt': _clip_text(str(payload_dict.get('excerpt', '') or ''), limit=600),
            'sources': list(payload_dict.get('sources', []) or [])[:6],
            'score': payload_dict.get('score', 0),
        }
    return {
        'schema_version': scope.get('schema_version', ''),
        'layers': compact_layers,
        'combined_context_excerpt': _clip_text(str(scope.get('combined_context', '') or ''), limit=1200),
    }


def _build_orchestrator_scene_scan(state: KernelState, working_goal: str) -> str:
    workspace_root = str(get_agent_settings("orchestrator").workspace_root or "").strip()
    if not workspace_root:
        return ""

    changed_files = _git_changed_files_for_workspace(workspace_root, limit=6)
    candidate_files = _extract_candidate_relative_paths(working_goal)
    selected_files: list[str] = []
    for rel_path in [*changed_files, *candidate_files]:
        normalized = str(rel_path or "").strip().replace('\\', '/')
        if not normalized or normalized in selected_files:
            continue
        selected_files.append(normalized)
        if len(selected_files) >= 4:
            break

    sections: list[str] = []
    if changed_files:
        sections.append("鏈€杩戝彉鏇存枃浠?\n- " + "\n- ".join(changed_files[:6]))
    if candidate_files:
        sections.append("鐩爣鍊欓€夋枃浠?\n- " + "\n- ".join(candidate_files[:6]))

    for rel_path in selected_files:
        excerpt = _grounding_excerpt_for_file(Path(workspace_root) / rel_path)
        if excerpt:
            sections.append(f"[{rel_path}]\n{excerpt}")

    return _clip_text("\n\n".join(sections).strip(), limit=2200)

TASK_AND_HANDOFF_LIMITS = {
    'scan_result_chars': 2200,
    'project_memory_chars': 1800,
    'orchestrator_brief_chars': 420,
    'review_feedback_chars': 520,
    'builder_working_state_chars': 2600,
    'review_focus_points': 6,
    'builder_self_assessment_chars': 220,
}


def _with_artifact_layers(artifacts: dict[str, object] | None) -> dict[str, object]:
    base = dict(artifacts or {})
    control = dict(base.get("control_artifacts", {}) or {})
    visibility = dict(base.get("visibility_artifacts", {}) or {})
    diagnostic = dict(base.get("diagnostic_artifacts", {}) or {})

    for key in CONTROL_ARTIFACT_KEYS:
        if key in base:
            control[key] = base[key]
    for key in VISIBILITY_ARTIFACT_KEYS:
        if key in base:
            visibility[key] = base[key]
    for key in DIAGNOSTIC_ARTIFACT_KEYS:
        if key in base:
            diagnostic[key] = base[key]

    output = dict(base.get("output_artifacts", {}) or {})
    for key in OUTPUT_ARTIFACT_KEYS:
        if key in base:
            output[key] = base[key]
    base["control_artifacts"] = control
    base["visibility_artifacts"] = visibility
    base["diagnostic_artifacts"] = diagnostic
    base["output_artifacts"] = output
    return base


FAILURE_DISPOSITIONS = {
    "": {"action": "continue", "retryable": False, "human_attention": False},
    "code_generation_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "patch_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "test_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "lint_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "environment_failed": {"action": "auto_retry", "retryable": True, "human_attention": True},
    "permission_failed": {"action": "enter_human_approval", "retryable": False, "human_attention": True},
    "review_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "validation_failed": {"action": "builder_rewrite", "retryable": True, "human_attention": False},
    "approval_blocked": {"action": "enter_human_approval", "retryable": False, "human_attention": True},
    "dependency_install_failed": {"action": "enter_human_approval", "retryable": False, "human_attention": True},
    "validation_contract_breach": {"action": "terminate_and_audit", "retryable": False, "human_attention": True},
}


def _failure_signal(failure_class: str, *, summary: str, source: str) -> dict[str, object]:
    disposition = dict(FAILURE_DISPOSITIONS.get(failure_class, FAILURE_DISPOSITIONS[""]))
    return {
        "failure_class": failure_class,
        "disposition": disposition.get("action", "continue"),
        "retryable": bool(disposition.get("retryable", False)),
        "human_attention": bool(disposition.get("human_attention", False)),
        "source": source,
        "summary": summary,
    }


def _classify_failure_state(state: KernelState) -> dict[str, object]:
    artifacts = _with_artifact_layers(state.get("artifacts", {}))
    execution_evidence = dict(artifacts.get("execution_evidence", {}) or {})
    review_assessment = dict(artifacts.get("review_assessment", {}) or {})
    blocking_issue = str(state.get("blocking_issue", "") or "").strip()
    combined_text = " ".join(
        [
            blocking_issue,
            str(state.get("execution_result", "") or ""),
            str(state.get("review_result", "") or ""),
            str(state.get("validation_result", "") or ""),
            str(execution_evidence.get("runtime_detail", "") or ""),
            str(execution_evidence.get("git_diff_summary", "") or ""),
            str(execution_evidence.get("lint_stderr", "") or ""),
            str(execution_evidence.get("pytest_stderr", "") or ""),
            str(execution_evidence.get("compile_stderr", "") or ""),
            str(execution_evidence.get("install_dep_detail", "") or ""),
        ]
    ).lower()

    def has_any(*tokens: str) -> bool:
        return any(token in combined_text for token in tokens if token)

    if has_any("validation_contract_breach", "execution_source=", "model_source="):
        return _failure_signal("validation_contract_breach", summary=blocking_issue or "Validation contract was breached by an out-of-band evaluation or disallowed execution source.", source="validation")
    if execution_evidence.get("install_dep_status") == "failed" or has_any("install_dep failed", "approved install_dep failed", "dependency install failed"):
        return _failure_signal("dependency_install_failed", summary="Approved dependency installation failed.", source="execution")
    if has_any("permission denied", "outside allowed root", "permissionerror", "access is denied"):
        return _failure_signal("permission_failed", summary=blocking_issue or "Permission boundary blocked the requested action.", source="execution")
    if execution_evidence.get("git_diff_status") == "failed":
        return _failure_signal("patch_failed", summary="git diff evidence collection failed or patch surface is inconsistent.", source="execution")
    if execution_evidence.get("lint_status") == "failed":
        return _failure_signal("lint_failed", summary="Lint verification failed.", source="execution")
    if execution_evidence.get("pytest_status") == "failed":
        return _failure_signal("test_failed", summary="Pytest verification failed.", source="execution")
    if execution_evidence.get("compile_status") == "failed":
        return _failure_signal("code_generation_failed", summary="Compile step failed after code generation.", source="execution")
    if str(execution_evidence.get("runtime_status", "")) in {"failed", "blocked", "disabled"} or has_any("docker probe failed", "docker binary not found", "runtime disabled", "docker desktop", "runtime action failed"):
        return _failure_signal("environment_failed", summary=blocking_issue or str(execution_evidence.get("runtime_detail", "") or "Runtime environment failed."), source="execution")
    if state.get("validation_status") == "changes_requested":
        return _failure_signal("validation_failed", summary=blocking_issue or str(state.get("validation_feedback", "") or "Validator requested remediation."), source="validator")
    effective_review_decision = str(
        review_assessment.get("effective_decision")
        or review_assessment.get("decision")
        or state.get("review_status", "")
        or ""
    ).strip().lower()
    if effective_review_decision == "changes_requested":
        return _failure_signal("review_failed", summary=blocking_issue or str(state.get("review_feedback", "") or "Reviewer requested rework."), source="reviewer")
    if state.get("approval_status") == "pending":
        return _failure_signal("approval_blocked", summary=blocking_issue or "Task is waiting for human approval.", source="approval")
    if state.get("approval_status") == "changes_requested":
        return _failure_signal("approval_blocked", summary=blocking_issue or "Human approval blocked progression and requested changes.", source="approval")
    return _failure_signal("", summary="", source="")


def _output_artifacts(state: KernelState) -> dict[str, object]:
    artifacts = state.get("artifacts", {}) or {}
    output = dict(artifacts.get("output_artifacts", {}) or {})
    for key in OUTPUT_ARTIFACT_KEYS:
        if key not in output and key in artifacts:
            output[key] = artifacts[key]
    return output


def _refresh_output_layers(state: KernelState) -> dict[str, object]:
    artifacts = _with_artifact_layers(state.get("artifacts", {}))
    output = dict(artifacts.get("output_artifacts", {}) or {})
    execution_runtime = dict(state.get("artifacts", {}).get("execution_runtime", {}) or {})
    failure_state = _classify_failure_state({**state, "artifacts": artifacts})
    artifacts["failure_state"] = failure_state
    output["governance_outputs"] = {
        "phase": state.get("active_phase", ""),
        "active_agent": state.get("active_agent", ""),
        "review_status": state.get("review_status", ""),
        "validation_status": state.get("validation_status", ""),
        "approval_status": state.get("approval_status", ""),
        "risk_level": state.get("risk_level", ""),
        "rework_count": state.get("rework_count", 0),
        "blocking_issue": state.get("blocking_issue", ""),
        "execution_runtime_backend": execution_runtime.get("backend", ""),
        "execution_runtime_status": execution_runtime.get("status", ""),
        "execution_runtime_detail": execution_runtime.get("detail", ""),
        "execution_runtime_severity": execution_runtime.get("severity", ""),
        "execution_runtime_should_interrupt": execution_runtime.get("should_interrupt", False),
        "execution_runtime_duration_ms": execution_runtime.get("duration_ms", 0),
        "failure_class": failure_state.get("failure_class", ""),
        "failure_disposition": failure_state.get("disposition", "continue"),
        "failure_source": failure_state.get("source", ""),
        "failure_retryable": failure_state.get("retryable", False),
        "failure_human_attention": failure_state.get("human_attention", False),
    }
    output["human_outputs"] = {
        "execution_explanation": state.get("execution_result", ""),
        "review_explanation": state.get("review_result", ""),
        "validation_explanation": state.get("validation_result", ""),
        "review_feedback_text": state.get("review_feedback", ""),
        "validation_feedback_text": state.get("validation_feedback", ""),
        "recorder_explanation": state.get("recorder_summary", ""),
        "execution_runtime_summary": execution_runtime.get("detail", ""),
        "execution_runtime_status_label": execution_runtime.get("status", ""),
        "execution_runtime_duration_label": f"{int(execution_runtime.get('duration_ms', 0))} ms" if execution_runtime.get("duration_ms") else "",
        "failure_summary": failure_state.get("summary", ""),
        "failure_class_label": failure_state.get("failure_class", ""),
        "failure_disposition_label": failure_state.get("disposition", "continue"),
    }
    artifacts["governance_outputs"] = output["governance_outputs"]
    artifacts["human_outputs"] = output["human_outputs"]
    artifacts["output_artifacts"] = output
    return artifacts


def _visibility_artifacts(state: KernelState) -> dict[str, object]:
    artifacts = state.get("artifacts", {}) or {}
    visibility = dict(artifacts.get("visibility_artifacts", {}) or {})
    for key in VISIBILITY_ARTIFACT_KEYS:
        if key not in visibility and key in artifacts:
            visibility[key] = artifacts[key]
    return visibility


def _guarded_task_profile(goal: str, task_kind: str, orchestrator_design: dict[str, object]) -> tuple[str, bool, str, str]:
    goal_lower = goal.lower()
    heuristic_profile = "routine"
    capability_family_hits: set[str] = set()
    governance_hit = any(token in goal_lower for token in ("doctrine", "policy", "schema", "approval", "governance", "architecture"))
    coordination_hit = any(token in goal_lower for token in ("workspace", "handoff", "integration", "coordination", "checkpoint"))
    coordination_module_hit = "module" in goal_lower and any(
        token in goal_lower for token in ("multiple", "across", "multi-", "multi ", "inter-", "cross-")
    )
    release_hit = any(token in goal_lower for token in ("release", "rollback", "launch", "readiness"))
    evidence_hit = any(token in goal_lower for token in ("memory", "retrieval", "diagnostic", "runtime", "snapshot", "report"))

    if task_kind == "system_optimization":
        heuristic_profile = "governance_sensitive"
        capability_family_hits.add("governance")
    elif governance_hit:
        heuristic_profile = "governance_sensitive"
        capability_family_hits.add("governance")
    elif coordination_hit or coordination_module_hit:
        heuristic_profile = "coordination_sensitive"
        capability_family_hits.add("coordination")
    elif release_hit:
        heuristic_profile = "release_sensitive"
        capability_family_hits.add("release")
    elif evidence_hit:
        heuristic_profile = "evidence_sensitive"
        capability_family_hits.add("evidence")

    if governance_hit:
        capability_family_hits.add("governance")
    if coordination_hit or coordination_module_hit:
        capability_family_hits.add("coordination")
    if release_hit:
        capability_family_hits.add("release")
    if evidence_hit:
        capability_family_hits.add("evidence")

    llm_profile = str(orchestrator_design.get("task_profile", "routine") or "routine")
    heuristic_combination = len(capability_family_hits) >= 2
    if bool(orchestrator_design.get("combination_sensitive", False)) and heuristic_combination:
        heuristic_profile = "combination_sensitive"

    profile_order = {
        "routine": 0,
        "evidence_sensitive": 1,
        "release_sensitive": 1,
        "coordination_sensitive": 2,
        "governance_sensitive": 3,
        "combination_sensitive": 4,
    }
    guarded_profile = llm_profile
    fallback_applied = False
    fallback_reason = "llm_profile_accepted"
    confidence = "high" if llm_profile == heuristic_profile else "medium"
    if profile_order.get(heuristic_profile, 0) > profile_order.get(llm_profile, 0):
        guarded_profile = heuristic_profile
        fallback_applied = True
        fallback_reason = f"heuristic_guard_promoted_{llm_profile}_to_{heuristic_profile}"
        confidence = "low"
    elif (
        llm_profile == "evidence_sensitive"
        and heuristic_profile == "routine"
        and not evidence_hit
        and not governance_hit
        and not release_hit
        and not coordination_hit
        and not coordination_module_hit
    ):
        guarded_profile = "routine"
        fallback_applied = True
        fallback_reason = "bounded_guard_demoted_evidence_sensitive_to_routine"
        confidence = "medium"
    elif (
        llm_profile == "combination_sensitive"
        and heuristic_profile == "release_sensitive"
        and not heuristic_combination
        and release_hit
        and not governance_hit
        and not coordination_hit
        and not coordination_module_hit
    ):
        guarded_profile = "release_sensitive"
        fallback_applied = True
        fallback_reason = "bounded_guard_demoted_combination_sensitive_to_release_sensitive"
        confidence = "medium"
    return guarded_profile, fallback_applied, fallback_reason, confidence


def _dynamic_trigger_payload(
    *,
    task_kind: str,
    task_profile: str,
    rework_count: int = 0,
    fallback_applied: bool = False,
    fallback_reason: str = "",
    classification_confidence: str = "medium",
) -> dict[str, object]:
    return build_trigger_protocol(
        task_kind=task_kind,
        task_profile=task_profile,
        rework_count=rework_count,
        fallback_applied=fallback_applied,
        fallback_reason=fallback_reason,
        classification_confidence=classification_confidence,
    )


def _inspection_state_update(
    state: KernelState,
    *,
    stage: str,
    trigger_class: str,
    decision: str,
    summary: str,
    escalation_target: str = "",
) -> dict[str, object]:
    previous = state.get("artifacts", {}).get("inspection_state", {}) or {}
    history = list(previous.get("history", []))
    history.append(
        {
            "stage": stage,
            "trigger_class": trigger_class,
            "decision": decision,
            "summary": summary,
            "escalation_target": escalation_target,
            "recorded_at": now_iso(),
        }
    )
    return {
        "last_stage": stage,
        "last_trigger_class": trigger_class,
        "last_decision": decision,
        "last_summary": summary,
        "last_escalation_target": escalation_target,
        "history": history[-6:],
        "human_visibility_note": "Human operators should be able to see recent inspections and escalation decisions without opening raw logs.",
    }


def _human_visibility_update(
    state: KernelState,
    *,
    stage: str,
    summary: str,
    recommendation: str,
    level: str = "",
    reasons: list[str] | None = None,
) -> dict[str, object]:
    previous = state.get("artifacts", {}).get("human_visibility", {}) or {}
    history = list(previous.get("history", []))
    trigger_plan = state.get("artifacts", {}).get("dynamic_triggers", {}) or {}
    checkpoint = trigger_plan.get("human_visibility_checkpoint", {}) or {}
    entry = {
        "stage": stage,
        "level": level or checkpoint.get("level", "optional"),
        "summary": summary,
        "recommendation": recommendation,
        "reasons": list(reasons or checkpoint.get("reasons", [])),
        "recorded_at": now_iso(),
    }
    history.append(entry)
    return {
        "current_level": entry["level"],
        "current_summary": summary,
        "current_recommendation": recommendation,
        "current_reasons": entry["reasons"],
        "history": history[-6:],
    }


LLM_EVENT_AGENTS = {"orchestrator", "builder", "reviewer", "validator"}


def _compact_event_metadata(value: object, *, depth: int = 0) -> object:
    if depth >= 2:
        return _clip_text(str(value or ""), limit=180)
    if isinstance(value, dict):
        compacted: dict[str, object] = {}
        for key, item in value.items():
            key_text = str(key or "")
            if key_text in {"text", "item", "raw", "content", "combined_context", "full_output"}:
                continue
            compacted[key_text] = _compact_event_metadata(item, depth=depth + 1)
        return compacted
    if isinstance(value, list):
        return [_compact_event_metadata(item, depth=depth + 1) for item in list(value)[:6]]
    if isinstance(value, str):
        return _clip_text(value, limit=180)
    return value


def _process_events_state(state: KernelState) -> dict[str, object]:
    previous = state.get("artifacts", {}).get("process_events", {}) or {}
    return {
        "latest": list(previous.get("latest", []))[-80:],
        "last_event": dict(previous.get("last_event", {}) or {}),
    }


def _normalize_callback_event(
    state: KernelState,
    *,
    node: str,
    event_type: str,
    status: str,
    summary: str,
    target: str = "",
    duration_ms: int = 0,
    metadata: dict[str, object] | None = None,
    callback_layer: str = "step",
) -> ProcessEvent:
    task_card = dict(state.get("task_card", {}) or {})
    normalized_metadata = _compact_event_metadata(dict(metadata or {}))
    normalized_metadata.setdefault("callback_layer", callback_layer)
    normalized_metadata.setdefault("project_id", str(state.get("project_id", "")))
    normalized_metadata.setdefault("task_id", str(task_card.get("task_id", "")))
    normalized_metadata.setdefault("run_id", str(state.get("run_id", "")))
    normalized_metadata.setdefault("source_engine_id", str(normalized_metadata.get("source_engine_id", "") or state.get("engine_id", "")))
    return {
        "event_id": f"{node}:{event_type}:{now_iso()}",
        "timestamp": now_iso(),
        "project_id": str(state.get("project_id", "")),
        "task_id": str(task_card.get("task_id", "")),
        "node": node,
        "event_type": event_type,
        "status": status,
        "summary": summary,
        "target": target,
        "duration_ms": int(duration_ms or 0),
        "metadata": normalized_metadata,
    }



def emit_step_event(
    state: KernelState,
    *,
    node: str,
    event_type: str,
    status: str,
    summary: str,
    target: str = "",
    duration_ms: int = 0,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    process_events = _process_events_state(state)
    event = _normalize_callback_event(
        state,
        node=node,
        event_type=event_type,
        status=status,
        summary=summary,
        target=target,
        duration_ms=duration_ms,
        metadata=metadata,
        callback_layer="step",
    )
    return _append_process_event_record(process_events, event)



def emit_task_event(
    state: KernelState,
    *,
    node: str,
    event_type: str,
    status: str,
    summary: str,
    target: str = "",
    duration_ms: int = 0,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    process_events = _process_events_state(state)
    event = _normalize_callback_event(
        state,
        node=node,
        event_type=event_type,
        status=status,
        summary=summary,
        target=target,
        duration_ms=duration_ms,
        metadata=metadata,
        callback_layer="task",
    )
    return _append_process_event_record(process_events, event)



def _append_process_event(
    state: KernelState,
    *,
    node: str,
    event_type: str,
    status: str,
    summary: str,
    target: str = "",
    duration_ms: int = 0,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return emit_step_event(
        state,
        node=node,
        event_type=event_type,
        status=status,
        summary=summary,
        target=target,
        duration_ms=duration_ms,
        metadata=metadata,
    )


def _append_process_event_record(
    process_events: dict[str, object],
    event: ProcessEvent,
) -> dict[str, object]:
    latest = list(process_events.get("latest", []))
    latest.append(event)
    return {
        "latest": latest[-80:],
        "last_event": event,
    }


def _latest_process_events(state: KernelState, limit: int = 12) -> list[dict[str, object]]:
    process_events = _process_events_state(state)
    return list(process_events.get("latest", []))[-limit:]

def _persist_routing_reassessment(
    state: KernelState,
    *,
    recent_events: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    artifacts = state.setdefault("artifacts", {})
    trigger_plan = dict(artifacts.get("dynamic_triggers", {}) or {})
    if not trigger_plan:
        return trigger_plan
    updated = reassess_trigger_protocol(
        trigger_plan,
        recent_events=recent_events or _latest_process_events(state),
    )
    artifacts["dynamic_triggers"] = updated
    return updated



def _append_artifact_read_event(
    state: KernelState,
    *,
    node: str,
    target: str,
    summary: str,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return _append_process_event(
        state,
        node=node,
        event_type="artifact_read",
        status="completed",
        summary=summary,
        target=target,
        metadata=metadata or {},
    )


def _append_state_transition_event(
    state: KernelState,
    *,
    node: str,
    event_type: str,
    summary: str,
    from_phase: str = "",
    to_phase: str = "",
    status: str = "changed",
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = dict(metadata or {})
    if from_phase:
        payload["from_phase"] = from_phase
    if to_phase:
        payload["to_phase"] = to_phase
    return _append_process_event(
        state,
        node=node,
        event_type=event_type,
        status=status,
        summary=summary,
        metadata=payload,
    )


def _flush_live_progress(
    state: KernelState,
    *,
    phase: str,
    agent: str,
    blocking_issue: str = "",
) -> KernelState:
    synced_state = {
        **state,
        "active_phase": phase,
        "active_agent": agent,
        "artifacts": _refresh_output_layers(state),
    }
    _sync_control_tower(synced_state, phase=phase, agent=agent, blocking_issue=blocking_issue)
    save_state_snapshot(synced_state["project_id"], synced_state)
    return synced_state



def _make_llm_stream_callback(
    state: KernelState,
    *,
    node: str,
    phase: str,
    agent: str,
    process_events_ref: dict[str, object],
    artifact_overrides: dict[str, object] | None = None,
):
    flush_markers = {"llm_tool_use_started", "llm_stream_completed", "llm_stream_error"}
    tracker = {"last_flush": perf_counter(), "sequence": 0}
    base_artifacts = dict(state.get("artifacts", {}) or {})
    static_overrides = dict(artifact_overrides or {})

    def _callback(stream_event: dict[str, object]) -> None:
        tracker["sequence"] += 1
        metadata = dict(stream_event.get("metadata", {}) or {})
        metadata.setdefault("stream_source", str(stream_event.get("stream_source", "llm")) or "llm")
        metadata["stream_sequence"] = tracker["sequence"]
        current_events = dict(process_events_ref.get("value", {}) or _process_events_state(state))
        updated_events = emit_step_event(
            {**state, "artifacts": {**base_artifacts, "process_events": current_events}},
            node=node,
            event_type=str(stream_event.get("event_type", "llm_stream_event") or "llm_stream_event"),
            status=str(stream_event.get("status", "running") or "running"),
            summary=str(stream_event.get("summary", "") or "")[:220],
            target=str(stream_event.get("target", "") or ""),
            metadata=metadata,
        )
        process_events_ref["value"] = updated_events
        should_flush = str(stream_event.get("event_type", "") or "") in flush_markers or (perf_counter() - tracker["last_flush"]) >= 1.0
        if should_flush:
            tracker["last_flush"] = perf_counter()
            _flush_live_progress(
                {
                    **state,
                    "artifacts": {
                        **base_artifacts,
                        **static_overrides,
                        "process_events": updated_events,
                    },
                },
                phase=phase,
                agent=agent,
            )

    return _callback


SSOT_MEMORY_LINKS = [
    "mother_memory/iterations/current_master_handoff_v1.md",
    "mother_memory/iterations/current_execution_methods_state_v1.md",
    "mother_memory/iterations/validation_operations_manual_v1.md",
    "mother_memory/iterations/current_handoff_v1.md",
    "mother_memory/iterations/current_mainline_plan_v1.md",
    "mother_memory/iterations/standard_unit_validation_to_intake_pipeline_v1.md",
    "mother_memory/iterations/standard_task_unit_v1.md",
    "mother_memory/iterations/validation_system_v1.md",
    "mother_memory/iterations/validation_policy_grid_v1.md",
    "mother_memory/iterations/task_intake_and_standard_unit_summary_v1.md",
]


def _build_ssot_state(state: KernelState, *, phase: str, agent: str, blocking_issue: str = "") -> dict[str, object]:
    task_id = state["task_card"].get("task_id", "") if state.get("task_card") else ""
    visibility = _visibility_artifacts(state)
    outputs = _output_artifacts(state)
    governance_outputs = dict(outputs.get("governance_outputs", {}) or {})
    human_outputs = dict(outputs.get("human_outputs", {}) or {})
    builder_call_diagnostics = dict((state.get("artifacts", {}) or {}).get("builder_call_diagnostics", {}) or {})
    task_card = dict(state.get("task_card", {}) or {})
    intake_assessment = dict((state.get("artifacts", {}) or {}).get("intake_assessment", {}) or {})

    return {
        "schema_version": "ssot_state.v1",
        "updated_at": now_iso(),
        "mainline_state": {
            "current_mainline": "standard_entry_definition_to_validation_bound_to_policy_grid_to_outer_intake_slicing",
            "summary": "Use validation to find and stabilize the standard task unit upper bound, then push that bound back into outer task modeling.",
            "pipeline": [
                "entry_hypothesis",
                "validation_hits_upper_bound",
                "iterative_tightening",
                "policy_grid_learning",
                "freeze_standard_upper_bound",
                "outer_intake_slicing",
            ],
            "current_priority": "stabilize inner-layer standard task unit before expanding outer-task modeling or cockpit productization",
            "next_focus": [
                "continue using standard_task_unit_v1 as the working intake bound",
                "run validation tiers to locate drift near the upper bound",
                "tighten parameters round by round and retain evidence for policy-grid learning",
            ],
        },
        "execution_state": {
            "project_id": state.get("project_id", ""),
            "task_id": task_id,
            "task_kind": state.get("task_kind", "standard"),
            "current_phase": phase,
            "active_agent": agent,
            "blocking_issue": blocking_issue,
            "approval_status": state.get("approval_status", "not_needed"),
            "pending_human_approval": state.get("approval_status") == "pending",
            "review_status": state.get("review_status", ""),
            "validation_status": state.get("validation_status", ""),
            "rework_count": state.get("rework_count", 0),
            "runtime_backend": str(governance_outputs.get("execution_runtime_backend", "")),
            "runtime_status": str(governance_outputs.get("execution_runtime_status", "")),
            "runtime_detail": str(human_outputs.get("execution_runtime_summary") or governance_outputs.get("execution_runtime_detail", "")),
            "runtime_severity": str(governance_outputs.get("execution_runtime_severity", "")),
            "runtime_duration_ms": int(governance_outputs.get("execution_runtime_duration_ms", 0) or 0),
            "failure_class": str(governance_outputs.get("failure_class", "")),
            "failure_disposition": str(governance_outputs.get("failure_disposition", "continue")),
            "failure_source": str(governance_outputs.get("failure_source", "")),
            "failure_summary": str(human_outputs.get("failure_summary") or blocking_issue or ""),
            "bounded_execution": True,
            "execution_plane": "3024",
            "control_plane": "3032",
            "action_executor": {
                "phase": "phase_2_closed_loop",
                "actions": ["run_tests", "git_diff", "write_file", "edit_file", "run_lint", "install_dep"],
                "install_dep_policy": "require_human_approval_by_default",
            },
            "builder_backend": get_builder_backend_name(),
            "builder_call_diagnostics": builder_call_diagnostics,
        },
        "validation_state": {
            "current_goal": "validate and refine the standard-task-unit upper bound rather than just testing connectivity",
            "current_stage": "bottleneck_closure",
            "current_round_mode": "iterative_tightening",
            "tiers": {
                "micro": {"status": "executable", "parallelizable": True, "purpose": "fast protocol and failure-route regression"},
                "structured": {"status": "executable", "parallelizable": True, "purpose": "standard-unit closed-loop verification"},
                "batch_smoke": {"status": "executable", "parallelizable": False, "purpose": "near-real project path verification"},
            },
            "sample_principles": [
                "samples_must_have_breadth",
                "samples_must_be_designed",
                "samples_must_not_be_ad_hoc",
                "samples_should_cover_false_approve_and_false_reject",
            ],
            "nightly_direction": {
                "day": ["micro", "structured", "batch_smoke_mock"],
                "night": ["micro", "structured", "batch_smoke_mock", "batch_smoke_live"],
                "requires": ["precheck", "fail_fast", "summary", "handoff", "alert_levels_info_warning_critical"],
                "openclaw_role": "escalation_handoff_not_primary_scheduler",
            },
        },
        "validation_contract": {
            **load_validation_contract(),
            "enforced_execution_source": "system_pipeline",
            "model_source_policy": "formal_validation_must_stay_inside_system_pipeline; assistant_direct_testing_forbidden_except_route_connectivity",
        },
        "standard_unit_state": {
            "working_definition": "standard_task_unit_v1",
            "interpretation": "largest safe task granularity the inner system is currently expected to deliver reliably",
            "status": "working_hypothesis_under_validation",
            "current_assessment": {
                "size_within_standard_task_unit_v1": bool(intake_assessment.get("size_within_standard_task_unit_v1", False)),
                "near_upper_bound": bool(intake_assessment.get("near_upper_bound", False)),
                "over_upper_bound": bool(intake_assessment.get("over_upper_bound", False)),
                "must_split": bool(intake_assessment.get("must_split", False)),
                "upper_bound_trigger_dimensions": list(intake_assessment.get("upper_bound_trigger_dimensions", []) or []),
                "estimated_core_file_count": int(intake_assessment.get("estimated_core_file_count", 0) or 0),
                "estimated_action_count": int(intake_assessment.get("estimated_action_count", 0) or 0),
                "project_size_band": str(intake_assessment.get("project_size_band", "")),
            },
            "upper_bound_assumption": {
                "single_primary_goal": True,
                "core_file_count_range": "1-5",
                "action_type_count_range": "1-3",
                "primary_module_span": "single_module_domain_preferred",
                "must_define": ["scope", "out_of_scope", "acceptance_criteria"],
                "closure_expectation": "single_run_or_small_rework_loop",
            },
            "task_flow": ["raw_intent", "modeled_task", "standard_task_unit"],
            "learning_rule": "validation results are used to discover, tighten, and eventually freeze the real upper bound",
            "current_task_card_snapshot": {
                "goal": task_card.get("goal", state.get("goal", "")),
                "scope": list(task_card.get("scope", []) or []),
                "out_of_scope": list(task_card.get("out_of_scope", []) or []),
                "acceptance_criteria": list(task_card.get("acceptance_criteria", []) or []),
                "project_size_band": task_card.get("project_size_band", ""),
                "task_kind": task_card.get("task_kind", state.get("task_kind", "standard")),
                "requires_approval": bool(task_card.get("requires_approval", False)),
            },
        },
        "memory_links": {
            "retrieval_priority": SSOT_MEMORY_LINKS,
            "primary_handoff": "mother_memory/iterations/current_master_handoff_v1.md",
            "methods_state": "mother_memory/iterations/current_execution_methods_state_v1.md",
            "validation_manual": "mother_memory/iterations/validation_operations_manual_v1.md",
        },
        "alerts": {
            "validation_contract_breach_active": str(governance_outputs.get("failure_class", "")) == "validation_contract_breach",
            "active_failure_class": str(governance_outputs.get("failure_class", "")),
            "active_failure_disposition": str(governance_outputs.get("failure_disposition", "continue")),
            "human_attention_required": bool(governance_outputs.get("failure_human_attention", False)) or str(governance_outputs.get("failure_class", "")) == "validation_contract_breach",
        },
        "control_tower_state": {
            "observer_status": visibility.get("observer", {}).get("status", "not_initialized"),
            "side_feedback": visibility.get("side_feedback", {}),
            "dynamic_triggers": visibility.get("dynamic_triggers", {}),
            "inspection_state": visibility.get("inspection_state", {}),
            "human_visibility": visibility.get("human_visibility", {}),
            "process_events": _latest_process_events(state),
        },
    }


def _sync_control_tower(state: KernelState, *, phase: str, agent: str, blocking_issue: str = "") -> None:
    task_id = state["task_card"].get("task_id", "") if state.get("task_card") else ""
    visibility = _visibility_artifacts(state)
    outputs = _output_artifacts(state)
    governance_outputs = dict(outputs.get("governance_outputs", {}) or {})
    human_outputs = dict(outputs.get("human_outputs", {}) or {})
    builder_call_diagnostics = dict((state.get("artifacts", {}) or {}).get("builder_call_diagnostics", {}) or {})
    status_payload = {
        "current_phase": phase,
        "current_task": task_id,
        "task_kind": state.get("task_kind", "standard"),
        "active_agent": agent,
        "blocking_issue": blocking_issue,
        "pending_human_approval": state.get("approval_status") == "pending",
        "last_execution_result": str(human_outputs.get("execution_explanation") or state.get("execution_result", "")),
        "last_review_result": str(human_outputs.get("review_explanation") or state.get("review_result", "")),
        "execution_runtime_backend": str(governance_outputs.get("execution_runtime_backend", "")),
        "execution_runtime_status": str(governance_outputs.get("execution_runtime_status", "")),
        "execution_runtime_detail": str(human_outputs.get("execution_runtime_summary") or governance_outputs.get("execution_runtime_detail", "")),
        "execution_runtime_severity": str(governance_outputs.get("execution_runtime_severity", "")),
        "execution_runtime_should_interrupt": bool(governance_outputs.get("execution_runtime_should_interrupt", False)),
        "execution_runtime_duration_ms": int(governance_outputs.get("execution_runtime_duration_ms", 0) or 0),
        "failure_class": str(governance_outputs.get("failure_class", "")),
        "failure_disposition": str(governance_outputs.get("failure_disposition", "continue")),
        "failure_source": str(governance_outputs.get("failure_source", "")),
        "failure_summary": str(human_outputs.get("failure_summary") or blocking_issue or ""),
        "project_id": state.get("project_id", ""),
        "rework_count": state.get("rework_count", 0),
        "system_observer_status": visibility.get("observer", {}).get("status", "not_initialized"),
        "side_feedback": visibility.get("side_feedback", {}),
        "dynamic_triggers": visibility.get("dynamic_triggers", {}),
        "inspection_state": visibility.get("inspection_state", {}),
        "human_visibility": visibility.get("human_visibility", {}),
        "updated_at": now_iso(),
    }
    write_json(CONTROL_TOWER_PATH, status_payload)
    write_json(SSOT_STATE_PATH, _build_ssot_state(state, phase=phase, agent=agent, blocking_issue=blocking_issue))
    ingest_control_tower_state(
        project_id=str(state.get("project_id", "") or ""),
        task_id=task_id,
        phase=phase,
        agent=agent,
        status_path=str(CONTROL_TOWER_PATH),
        ssot_path=str(SSOT_STATE_PATH),
        failure_class=str(governance_outputs.get("failure_class", "") or ""),
        failure_disposition=str(governance_outputs.get("failure_disposition", "") or ""),
        path_mode_code=str((visibility.get("dynamic_triggers", {}) or {}).get("path_mode", "") or ""),
    )


def _project_paths(project_id: str) -> dict[str, str]:
    project_root = ensure_project_scaffold(project_id)
    memory_root = project_root / "memory"
    return {
        "project_root": str(project_root),
        "memory_root": str(memory_root),
        "backlog": str(memory_root / "tasks/backlog/task-card.md"),
        "in_progress": str(memory_root / "tasks/in_progress/task-card.md"),
        "completed": str(memory_root / "tasks/completed/task-card.md"),
        "execution_log": str(memory_root / "journal/execution_log/log.md"),
        "decision_log": str(memory_root / "journal/decision_log/log.md"),
        "phase": str(memory_root / "project_state/current_phase.md"),
        "module_status": str(memory_root / "project_state/module_status.md"),
    }


def _mark_node_started(state: KernelState, *, node_id: str, agent: str, phase: str) -> tuple[KernelState, float]:
    diagnostic = {
        "node_id": node_id,
        "agent": agent,
        "phase": phase,
        "started_at": now_iso(),
        "status": "running",
        "rework_count": state.get("rework_count", 0),
        "approval_status": state.get("approval_status", "not_needed"),
    }
    start_artifacts = _with_artifact_layers({
        **state.get("artifacts", {}),
        "active_node_diagnostic": diagnostic,
    })
    started_state = {
        **state,
        "active_phase": phase,
        "active_agent": agent,
        "artifacts": start_artifacts,
    }
    started_state["artifacts"]["process_events"] = _append_process_event(
        started_state,
        node=agent,
        event_type="node_entered",
        status="running",
        summary=f"Entered {agent} during {phase}.",
        metadata={"phase": phase, "node_id": node_id},
    )
    if agent in LLM_EVENT_AGENTS:
        started_state["artifacts"]["process_events"] = _append_process_event(
            started_state,
            node=agent,
            event_type="llm_turn_started",
            status="running",
            summary=f"{agent} started a new model turn.",
            metadata={"phase": phase, "node_id": node_id},
        )
    _sync_control_tower(started_state, phase=phase, agent=agent, blocking_issue=state.get("blocking_issue", ""))
    save_state_snapshot(state["project_id"], started_state)
    # 鎵ц鏃ュ織锛氳褰曡妭鐐瑰紑濮?
    builder_brief = (state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}).get("builder_brief", "")
    journal_node_started(
        entry_id=node_id,
        project_id=state.get("project_id", ""),
        agent=agent,
        phase=phase,
        goal=state.get("goal", ""),
        extra_task=builder_brief if agent == "builder" else "",
    )
    return started_state, perf_counter()


def _extract_node_result_summary(agent: str, state: KernelState) -> str:
    """Extract a short per-node result summary for execution journaling."""
    if agent == "orchestrator":
        analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
        profile = state.get("artifacts", {}).get("governance", {}).get("task_profile", "")
        brief = analysis.get("builder_brief", "")
        return f"任务画像：{profile} / {brief[:120]}" if brief else f"任务画像：{profile}"
    if agent == "builder":
        raw = state.get("build_result", "") or ""
        return raw[:200]
    if agent == "reviewer":
        status = state.get("review_status", "")
        feedback = state.get("review_feedback", "") or ""
        return f"审查结论：{status} / {feedback[:120]}"
    return str(state.get("blocking_issue", "") or "")[:200]



def _finalize_node(
    state_before: KernelState,
    next_state: KernelState,
    *,
    node_id: str,
    agent: str,
    phase: str,
    started_at_perf: float,
) -> KernelState:
    active_diag = state_before.get("artifacts", {}).get("active_node_diagnostic", {}) or {}
    diagnostics = list(next_state.get("artifacts", {}).get("node_diagnostics", []))
    diagnostics.append(
        {
            "node_id": node_id,
            "agent": agent,
            "phase": phase,
            "started_at": active_diag.get("started_at", now_iso()),
            "finished_at": now_iso(),
            "duration_ms": round((perf_counter() - started_at_perf) * 1000, 2),
            "status": "completed",
            "rework_count": next_state.get("rework_count", 0),
            "approval_status": next_state.get("approval_status", "not_needed"),
        }
    )
    final_artifacts = _with_artifact_layers({
        **next_state.get("artifacts", {}),
        "node_diagnostics": diagnostics,
        "last_node_diagnostic": diagnostics[-1],
    })
    temp_state = {
        **next_state,
        "artifacts": final_artifacts,
    }
    final_artifacts["process_events"] = _append_process_event(
        temp_state,
        node=agent,
        event_type="node_exited",
        status="completed",
        summary=f"Exited {agent} with completed status.",
        duration_ms=diagnostics[-1].get("duration_ms", 0),
        metadata={"phase": phase, "node_id": node_id},
    )
    if agent in LLM_EVENT_AGENTS:
        final_artifacts["process_events"] = _append_process_event(
            temp_state,
            node=agent,
            event_type="llm_turn_finished",
            status="completed",
            summary=f"{agent} finished the current model turn.",
            duration_ms=diagnostics[-1].get("duration_ms", 0),
            metadata={"phase": phase, "node_id": node_id},
        )
    if agent in {"orchestrator", "builder", "reviewer", "validator"}:
        report_payload = _agent_round_report_payload(agent, next_state)
        report_summary = _agent_round_report_summary(agent, next_state)
        visibility_artifacts = dict(final_artifacts.get("visibility_artifacts", {}) or {})
        round_reports = dict(visibility_artifacts.get("agent_round_reports", {}) or {})
        latest_by_agent = dict(round_reports.get("latest_by_agent", {}) or {})
        history = list(round_reports.get("history", []) or [])
        latest_by_agent[agent] = report_payload
        history.append(report_payload)
        round_reports["latest_by_agent"] = latest_by_agent
        round_reports["history"] = history[-24:]
        visibility_artifacts["agent_round_reports"] = round_reports
        final_artifacts["visibility_artifacts"] = visibility_artifacts
        if report_summary:
            final_artifacts["process_events"] = emit_task_event(
                temp_state,
                node=agent,
                event_type="agent_round_report",
                status=str(report_payload.get("status", "completed") or "completed"),
                summary=report_summary,
                target=str(next_state.get("goal", "") or ""),
                duration_ms=diagnostics[-1].get("duration_ms", 0),
                metadata={
                    "phase": phase,
                    "node_id": node_id,
                    "rework_count": int(next_state.get("rework_count", 0) or 0),
                    "report": report_payload,
                },
            )
    final_state = {
        **next_state,
        "artifacts": _refresh_output_layers({
            **next_state,
            "artifacts": final_artifacts,
        }),
    }
    final_state["artifacts"].pop("active_node_diagnostic", None)
    _sync_control_tower(final_state, phase=final_state["active_phase"], agent=final_state["active_agent"], blocking_issue=final_state.get("blocking_issue", ""))
    save_state_snapshot(final_state["project_id"], final_state)
    result_summary = _extract_node_result_summary(agent, final_state)
    journal_node_completed(
        entry_id=node_id,
        duration_ms=(perf_counter() - started_at_perf) * 1000,
        result=result_summary,
    )
    return final_state


def _finalize_node_failure(
    state_before: KernelState,
    *,
    node_id: str,
    agent: str,
    phase: str,
    started_at_perf: float,
    error_summary: str,
    extra_artifacts: dict[str, object] | None = None,
) -> KernelState:
    active_diag = state_before.get("artifacts", {}).get("active_node_diagnostic", {}) or {}
    diagnostics = list(state_before.get("artifacts", {}).get("node_diagnostics", []))
    failure_diag = {
        "node_id": node_id,
        "agent": agent,
        "phase": phase,
        "started_at": active_diag.get("started_at", now_iso()),
        "finished_at": now_iso(),
        "duration_ms": round((perf_counter() - started_at_perf) * 1000, 2),
        "status": "failed",
        "rework_count": state_before.get("rework_count", 0),
        "approval_status": state_before.get("approval_status", "not_needed"),
        "error_summary": error_summary,
    }
    artifacts = {
        **state_before.get("artifacts", {}),
        "node_diagnostics": [*diagnostics, failure_diag],
        "last_node_diagnostic": failure_diag,
        **(extra_artifacts or {}),
    }
    temp_failed_state = {
        **state_before,
        "active_phase": phase,
        "active_agent": agent,
        "blocking_issue": error_summary,
        "artifacts": _with_artifact_layers(artifacts),
    }
    temp_failed_state["artifacts"]["process_events"] = _append_process_event(
        temp_failed_state,
        node=agent,
        event_type="error",
        status="failed",
        summary=error_summary,
        duration_ms=failure_diag.get("duration_ms", 0),
        metadata={"phase": phase, "node_id": node_id},
    )
    if agent in {"orchestrator", "builder", "reviewer", "validator"}:
        report_payload = {
            "agent": agent,
            "goal": _short_text(str(state_before.get("goal", "") or ""), 96),
            "action": f"{agent} 节点执行失败。",
            "result": _short_text(error_summary, 140),
            "blocker": _short_text(error_summary, 96),
            "next_step": "检查失败原因，并决定是否返工、重试或人工介入。",
            "round_index": int(state_before.get("rework_count", 0) or 0) + 1,
            "status": "failed",
        }
        temp_failed_state["artifacts"]["process_events"] = emit_task_event(
            temp_failed_state,
            node=agent,
            event_type="agent_round_report",
            status="failed",
            summary=(
                f"goal={report_payload['goal']} | action={report_payload['action']} | "
                f"result={report_payload['result']} | blocker={report_payload['blocker']} | next={report_payload['next_step']}"
            ),
            target=str(state_before.get("goal", "") or ""),
            duration_ms=failure_diag.get("duration_ms", 0),
            metadata={"phase": phase, "node_id": node_id, "report": report_payload},
        )
    _persist_routing_reassessment(temp_failed_state)
    artifacts = temp_failed_state["artifacts"]
    failed_state = {
        **state_before,
        "active_phase": phase,
        "active_agent": agent,
        "blocking_issue": error_summary,
        "artifacts": _refresh_output_layers({
            **state_before,
            "artifacts": _with_artifact_layers(artifacts),
        }),
    }
    failed_state["artifacts"].pop("active_node_diagnostic", None)
    _sync_control_tower(failed_state, phase=phase, agent=agent, blocking_issue=error_summary)
    save_state_snapshot(failed_state["project_id"], failed_state)
    journal_node_completed(
        entry_id=node_id,
        duration_ms=(perf_counter() - started_at_perf) * 1000,
        result=f"failed: {error_summary[:200]}",
    )
    return failed_state
def _format_orchestrator_guidance(state: KernelState, *, mode: str = "default") -> str:
    analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
    governance = state.get("artifacts", {}).get("governance", {}) or {}
    governance_contract = state.get("artifacts", {}).get("governance_contract", {}) or {}

    parts: list[str] = []
    task_profile = analysis.get("task_profile") or "routine"
    governance_weight = governance.get("governance_weight") or analysis.get("governance_weight") or "routine"
    builder_brief = analysis.get("builder_brief", "")
    approval_hint = governance.get("approval_hint") or analysis.get("approval_hint")
    boundary_notes = analysis.get("boundary_notes", []) or []

    parts.append(f"task_profile={task_profile}")
    parts.append(f"governance_weight={governance_weight}")
    if builder_brief:
        parts.append(f"builder_brief={builder_brief}")
    capability_mix = analysis.get("capability_mix", []) or []
    interaction_risks = analysis.get("interaction_risks", []) or []
    if analysis.get("combination_sensitive"):
        parts.append("combination_sensitive=true")
    coordination_subtype = analysis.get("coordination_subtype", "")
    if coordination_subtype:
        parts.append(f"coordination_subtype={coordination_subtype}")
    if capability_mix:
        parts.append("capability_mix=" + " | ".join(str(item) for item in capability_mix[:5]))
    if interaction_risks:
        parts.append("interaction_risks=" + " | ".join(str(item) for item in interaction_risks[:4]))
    if boundary_notes:
        parts.append("boundary_notes=" + " | ".join(str(item) for item in boundary_notes[:4]))
    if approval_hint:
        parts.append("approval_hint=true")
    if governance_contract:
        if governance_contract.get("family_semantics"):
            parts.append(f"family_semantics={governance_contract['family_semantics']}")
        if governance_contract.get("approved_surface_definition"):
            parts.append(f"approved_surface_definition={governance_contract['approved_surface_definition']}")
        if governance_contract.get("disallowed_surface_definition"):
            parts.append(f"disallowed_surface_definition={governance_contract['disallowed_surface_definition']}")
        if governance_contract.get("policy_document_write_surface_hint"):
            parts.append(f"policy_document_write_surface_hint={governance_contract['policy_document_write_surface_hint']}")
        if governance_contract.get("referenced_read_only_schema_surfaces_hint"):
            parts.append(
                "referenced_read_only_schema_surfaces_hint="
                + " | ".join(str(item) for item in governance_contract.get("referenced_read_only_schema_surfaces_hint", [])[:4])
            )
        if governance_contract.get("contract_subtype"):
            parts.append(f"contract_subtype={governance_contract['contract_subtype']}")
        if governance_contract.get("exact_allowed_roots_hint"):
            parts.append(
                "exact_allowed_roots_hint="
                + " | ".join(str(item) for item in governance_contract.get("exact_allowed_roots_hint", [])[:4])
            )
        if governance_contract.get("exact_denied_roots_hint"):
            parts.append(
                "exact_denied_roots_hint="
                + " | ".join(str(item) for item in governance_contract.get("exact_denied_roots_hint", [])[:4])
            )
        if governance_contract.get("denied_path_tokens_hint"):
            parts.append(
                "denied_path_tokens_hint="
                + " | ".join(str(item) for item in governance_contract.get("denied_path_tokens_hint", [])[:4])
            )
        if governance_contract.get("canonicalization_expectation"):
            parts.append(f"canonicalization_expectation={governance_contract['canonicalization_expectation']}")
    if mode == "reviewer":
        reviewer_priority = []
        preferred_prefixes = (
            "task_profile=",
            "governance_weight=",
            "coordination_subtype=",
            "family_semantics=",
            "approved_surface_definition=",
            "disallowed_surface_definition=",
            "policy_document_write_surface_hint=",
            "referenced_read_only_schema_surfaces_hint=",
            "contract_subtype=",
            "canonicalization_expectation=",
        )
        for item in parts:
            if item.startswith(preferred_prefixes):
                reviewer_priority.append(item)
        return "\n".join(reviewer_priority[:8])
    return "\n".join(parts)


def _extract_build_plan_payload(build_result: str) -> dict[str, object]:
    text = build_result.strip()
    if not text:
        return {}
    if text.startswith("`"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        payload = json.loads(text[start : end + 1])
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    structured_output = payload.get("structured_output")
    if isinstance(structured_output, dict):
        return structured_output
    return payload
def _normalize_change_plan_payload(plan: dict[str, object]) -> dict[str, object]:
    normalized = dict(plan or {})
    direct_execution = bool(normalized.get("direct_execution", False))
    raw_change_plan = normalized.get("change_plan", {})
    change_plan = dict(raw_change_plan if isinstance(raw_change_plan, dict) else {})

    raw_changes = change_plan.get("changes", [])
    normalized_changes: list[dict[str, str]] = []
    if isinstance(raw_changes, list):
        for index, item in enumerate(raw_changes, start=1):
            if isinstance(item, dict):
                target = str(item.get("target", "")).strip() or f"planned_change_{index}"
                action_type = str(item.get("action_type", "edit_file")).strip() or "edit_file"
                why = str(item.get("why", "")).strip() or f"Implement change for {target}."
                risk_level = str(item.get("risk_level", "medium")).strip() or "medium"
                payload = dict(item.get("payload", {}) or {}) if isinstance(item.get("payload", {}), dict) else {}
            else:
                target = f"planned_change_{index}"
                action_type = "edit_file"
                why = str(item).strip() or f"Implement change #{index}."
                risk_level = "medium"
                payload = {}
            normalized_change = {
                "target": target,
                "action_type": action_type,
                "why": why,
                "risk_level": risk_level,
            }
            if payload:
                normalized_change["payload"] = payload
            normalized_changes.append(normalized_change)
    checkpoint_payload = normalized.get("checkpoint", {})
    checkpoint_required = bool(normalized.get("checkpoint_required", False))
    def _is_checkpoint_action(action_type: str) -> bool:
        normalized_action = str(action_type or "").strip().lower()
        return bool(normalized_action) and (
            normalized_action == "defer_edit_until_checkpoint"
            or "checkpoint" in normalized_action
        )
    if isinstance(checkpoint_payload, dict) and checkpoint_payload:
        checkpoint_required = True
    if not checkpoint_required and any(_is_checkpoint_action(str(item.get("action_type", "") or "")) for item in normalized_changes):
        checkpoint_required = True
    if checkpoint_required:
        normalized["status"] = "checkpoint"
        normalized["checkpoint_required"] = True
        normalized["direct_execution"] = False
        if not isinstance(checkpoint_payload, dict):
            checkpoint_payload = {}
        change_checkpoint = next((item for item in normalized_changes if _is_checkpoint_action(str(item.get("action_type", "") or ""))), {})
        change_checkpoint_payload = dict(change_checkpoint.get("payload", {}) or {}) if isinstance(change_checkpoint.get("payload", {}), dict) else {}
        top_level_question = str(normalized.get("checkpoint_question", "") or "").strip()
        top_level_options_raw = normalized.get("checkpoint_options", [])
        top_level_options = [str(item).strip() for item in top_level_options_raw if str(item).strip()] if isinstance(top_level_options_raw, list) else []
        reason = str(checkpoint_payload.get("reason", "") or change_checkpoint.get("why", "") or normalized.get("summary", "") or "Builder reached a decision boundary.").strip()
        question = str(
            checkpoint_payload.get("question", "")
            or checkpoint_payload.get("decision_required", "")
            or change_checkpoint_payload.get("decision_question", "")
            or top_level_question
            or "需要人确认后才能继续。"
        ).strip()
        options = checkpoint_payload.get("options", []) if isinstance(checkpoint_payload.get("options", []), list) else []
        merged_options = [str(item).strip() for item in options if str(item).strip()]
        if not merged_options:
            payload_options = []
            explicit_options = change_checkpoint_payload.get("options", []) if isinstance(change_checkpoint_payload.get("options", []), list) else []
            payload_options.extend(str(item).strip() for item in explicit_options if str(item).strip())
            for key in ("option_A", "option_B", "option_C"):
                value = str(change_checkpoint_payload.get(key, "") or "").strip()
                if value:
                    payload_options.append(value)
            merged_options = payload_options
        if not merged_options and top_level_options:
            merged_options = top_level_options
        if question == "需要人确认后才能继续。" and merged_options:
            question = "请在以下候选方案中选择其一后继续。"
        normalized["checkpoint"] = {
            "reason": reason,
            "question": question,
            "options": merged_options,
        }
        normalized["checkpoint_question"] = question
        normalized["checkpoint_options"] = merged_options
        normalized["direct_execution"] = False
        existing_changed_files = normalized.get("changed_files", [])
        if isinstance(existing_changed_files, list):
            kept_changed_files = []
            for item in existing_changed_files:
                path_text = str(item or "").strip().replace('\\', '/')
                if not path_text:
                    continue
                lowered = path_text.lower()
                if lowered.endswith('/build_plan.json') or lowered.endswith('build_plan.json'):
                    continue
                if lowered.startswith('runtime/') or '/codex_runtime/' in lowered:
                    continue
                kept_changed_files.append(str(item).strip())
            normalized["changed_files"] = kept_changed_files
    elif not normalized_changes and not direct_execution:
        fallback_steps = [str(item).strip() for item in normalized.get("implementation_steps", []) if str(item).strip()]
        summary = str(normalized.get("summary", "")).strip() or "Implement the planned change safely."
        normalized_changes = [
            {
                "target": "src/ai_dev_os",
                "action_type": "edit_file",
                "why": fallback_steps[0] if fallback_steps else summary,
                "risk_level": "medium",
            }
        ]

    raw_verification = change_plan.get("verification", {})
    verification = dict(raw_verification if isinstance(raw_verification, dict) else {})
    commands = [str(item).strip() for item in verification.get("commands", []) if str(item).strip()]
    expected_signals = [str(item).strip() for item in verification.get("expected_signals", []) if str(item).strip()]
    validation_checks = [str(item).strip() for item in normalized.get("validation_checks", []) if str(item).strip()]
    if not commands:
        commands = [item for item in validation_checks if any(token in item.lower() for token in ("pytest", "ruff", "compile", "test", "lint"))]
    if not commands:
        commands = ["python -m pytest -q"]
    if not expected_signals:
        expected_signals = validation_checks or ["Validation checks complete without high-severity failures."]

    approval_policy = dict(change_plan.get("approval_policy", {}) if isinstance(change_plan.get("approval_policy", {}), dict) else {})
    approval_policy["default"] = str(approval_policy.get("default", "no_extra_approval") or "no_extra_approval")
    approval_policy["high_risk_actions"] = str(approval_policy.get("high_risk_actions", "require_human_approval") or "require_human_approval")

    change_plan["changes"] = normalized_changes
    change_plan["verification"] = {
        "commands": commands,
        "expected_signals": expected_signals,
    }
    change_plan["rollback_hint"] = str(change_plan.get("rollback_hint", "")).strip() or "Revert the touched files or restore the previous known-good patch."
    change_plan["approval_policy"] = approval_policy
    normalized["change_plan"] = change_plan
    return normalized


def _repair_direct_execution_record(plan: dict[str, object], task_card: dict[str, object]) -> dict[str, object]:
    normalized = dict(plan or {})
    if not bool(normalized.get("direct_execution", False)):
        return normalized

    scope = [str(item).strip() for item in list(task_card.get("scope", []) or []) if str(item).strip()]
    raw_changed_files = [str(item).strip() for item in list(normalized.get("changed_files", []) or []) if str(item).strip()]
    changed_files = list(dict.fromkeys(raw_changed_files))
    if not changed_files and len(scope) == 1:
        changed_files = [scope[0]]
    if changed_files:
        normalized["changed_files"] = changed_files

    change_plan = dict(normalized.get("change_plan", {}) or {})
    raw_changes = list(change_plan.get("changes", []) or [])
    repaired_changes: list[dict[str, object]] = []
    for index, item in enumerate(raw_changes, start=1):
        if not isinstance(item, dict):
            repaired_changes.append(item)
            continue
        candidate = dict(item)
        target = str(candidate.get("target", "") or "").strip()
        if target.startswith("planned_change_"):
            if len(changed_files) >= index:
                candidate["target"] = changed_files[index - 1]
            elif len(changed_files) == 1:
                candidate["target"] = changed_files[0]
            elif len(scope) == 1:
                candidate["target"] = scope[0]
        repaired_changes.append(candidate)
    if repaired_changes:
        change_plan["changes"] = repaired_changes
        normalized["change_plan"] = change_plan

    if "verification_status" not in normalized:
        normalized["verification_status"] = "passed" if normalized.get("direct_execution") else "partial"
    if "status" not in normalized:
        normalized["status"] = "completed"
    return normalized


def _reviewer_evidence_gate_issues(execution_evidence: dict[str, object], assessment: dict[str, object]) -> list[str]:
    issues: list[str] = []
    changed_files = execution_evidence.get("changed_files", [])
    git_diff_summary = str(execution_evidence.get("git_diff_summary", "") or "").strip()
    command_results = execution_evidence.get("command_results", [])
    test_results = execution_evidence.get("test_results", [])
    builder_record = execution_evidence.get("builder_record", {}) if isinstance(execution_evidence.get("builder_record", {}), dict) else {}
    verification_status = str(builder_record.get("verification_status", "") or "").strip().lower()

    if not isinstance(changed_files, list) or not [str(item).strip() for item in changed_files if str(item).strip()]:
        issues.append("evidence_missing_changed_files")
    if not git_diff_summary:
        issues.append("evidence_missing_git_diff_summary")
    if not isinstance(command_results, list) or not command_results:
        issues.append("evidence_missing_command_results")
    if not isinstance(test_results, list) or not test_results:
        issues.append("evidence_missing_test_results")
    if verification_status in {"", "partial", "unknown"}:
        issues.append("evidence_verification_not_strong")

    decision = str(assessment.get("decision", "") or "").strip().lower()
    if decision == "approved" and issues:
        return issues
    return []

def _builder_output_status(plan: dict[str, object], task_card: dict[str, object]) -> str:
    payload = dict(plan or {})
    if isinstance(payload.get("input_rejection"), dict):
        return "rejected"
    if isinstance(payload.get("checkpoint"), dict) or isinstance(payload.get("escalation_request"), dict):
        return "blocked"
    change_plan = dict(payload.get("change_plan", {}) or {})
    changes = [dict(item) for item in list(change_plan.get("changes", []) or []) if isinstance(item, dict)]
    action_types = {str(item.get("action_type", "") or "").strip().lower() for item in changes}
    if bool(payload.get("checkpoint_required", False)) or any(("checkpoint" in action_type) or action_type == "defer_edit_until_checkpoint" for action_type in action_types):
        return "blocked"


    summary = str(payload.get("summary", "") or "").lower()
    goal = str(task_card.get("goal", "") or "").lower()
    orchestrator_brief = str(task_card.get("orchestrator_brief", "") or "").lower()
    change_plan = dict(payload.get("change_plan", {}) or {})
    changes = [dict(item) for item in list(change_plan.get("changes", []) or []) if isinstance(item, dict)]
    action_types = {str(item.get("action_type", "") or "").strip().lower() for item in changes}
    direct_execution = bool(payload.get("direct_execution", False))

    decision_keywords = ("保留还是", "决定", "取舍", "方向", "whether to", "keep or", "retain or", "remove or")
    high_impact_action = bool({"delete_file", "remove_file"} & action_types)
    if (
        str(task_card.get("task_profile", "") or "").strip().lower() != "routine"
        and ("checkpoint" in orchestrator_brief or "不要硬猜" in orchestrator_brief)
        and not direct_execution
        and (high_impact_action or any(token in goal for token in decision_keywords) or any(token in summary for token in decision_keywords))
    ):
        return "blocked"
    return "completed"


def _build_plan_contract_issues(state: KernelState) -> list[str]:
    plan = _normalize_change_plan_payload(_extract_build_plan_payload(state.get("build_result", "")))
    if not plan:
        return ["build_plan_missing_or_unparseable"]
    if bool(plan.get("direct_execution", False)):
        return []

    task_profile = str(((state.get("artifacts", {}) or {}).get("orchestrator_analysis", {}) or {}).get("task_profile", "routine") or "routine").strip().lower()
    change_plan = dict(plan.get("change_plan", {}) or {})
    changes = list(change_plan.get("changes", []) or [])
    if not changes:
        return ["build_plan_missing_changes"]

    issues: list[str] = []
    executable_source_changes = 0

    for index, item in enumerate(changes, start=1):
        if not isinstance(item, dict):
            issues.append(f"change_{index}_not_object")
            continue
        action_type = str(item.get("action_type", "") or "").strip().lower()
        target = str(item.get("target", "") or "").strip()
        payload = dict(item.get("payload", {}) or {}) if isinstance(item.get("payload", {}), dict) else {}
        if action_type not in {"edit_file", "write_file"}:
            continue
        if not target:
            issues.append(f"change_{index}_missing_target")
            continue
        if action_type == "write_file":
            if str(payload.get("content", "") or "").strip():
                executable_source_changes += 1
            else:
                issues.append(f"change_{index}_write_missing_content")
            continue
        edit_mode = str(payload.get("edit_mode", "replace_text") or "replace_text").strip().lower()
        if edit_mode == "append":
            if str(payload.get("append_text", "") or "").strip():
                executable_source_changes += 1
            else:
                issues.append(f"change_{index}_append_missing_append_text")
            continue
        old_text = str(payload.get("old_text", "") or "").strip()
        new_text = str(payload.get("new_text", "") or "").strip()
        if old_text and new_text:
            executable_source_changes += 1
        else:
            issues.append(f"change_{index}_replace_missing_old_or_new_text")

    if executable_source_changes == 0:
        issues.append("no_executable_source_change")
    if task_profile == "routine" and executable_source_changes == 0:
        issues.append("routine_requires_minimum_executable_payload")
    return list(dict.fromkeys(issues))


def _validate_governance_contract(state: KernelState) -> list[str]:
    contract = state.get("artifacts", {}).get("governance_contract", {}) or {}
    if not contract:
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable governance-policy payload"]

    issues: list[str] = []
    for field in contract.get("required_fields", []):
        if field not in plan or not plan.get(field):
            issues.append(f"missing governance contract field: {field}")

    allowed_roots = [str(item) for item in plan.get("exact_allowed_roots", [])]
    denied_roots = [str(item) for item in plan.get("exact_denied_roots", [])]
    denied_tokens = [str(item) for item in plan.get("denied_path_tokens", [])]
    negative_tests = [str(item) for item in plan.get("negative_tests", [])]
    positive_tests = [str(item) for item in plan.get("positive_tests", [])]
    canonicalization_rule = str(plan.get("canonicalization_rule", "")).strip()
    policy_document_write_surface = str(plan.get("policy_document_write_surface", "")).strip()
    referenced_surfaces = [str(item) for item in plan.get("referenced_read_only_schema_surfaces", [])]

    for hint in contract.get("exact_allowed_roots_hint", []):
        if str(hint) not in allowed_roots:
            issues.append(f"exact_allowed_roots missing required hint: {hint}")
    for hint in contract.get("exact_denied_roots_hint", []):
        if str(hint) not in denied_roots:
            issues.append(f"exact_denied_roots missing required hint: {hint}")
    for hint in contract.get("denied_path_tokens_hint", []):
        if str(hint) not in denied_tokens:
            issues.append(f"denied_path_tokens missing required hint: {hint}")

    strict_binding_fields = {str(item) for item in contract.get("strict_binding_fields", [])}
    if "exact_allowed_roots" in strict_binding_fields:
        expected = [str(item) for item in contract.get("exact_allowed_roots_hint", [])]
        if allowed_roots != expected:
            issues.append("exact_allowed_roots drifted from contract-bound allowed surface")
    if "exact_denied_roots" in strict_binding_fields:
        expected = [str(item) for item in contract.get("exact_denied_roots_hint", [])]
        if denied_roots != expected:
            issues.append("exact_denied_roots drifted from contract-bound denied surface")
    if "denied_path_tokens" in strict_binding_fields:
        expected = [str(item) for item in contract.get("denied_path_tokens_hint", [])]
        if denied_tokens != expected:
            issues.append("denied_path_tokens drifted from contract-bound deny-token list")

    if contract.get("policy_document_write_surface_hint"):
        expected = str(contract.get("policy_document_write_surface_hint", "")).strip()
        if policy_document_write_surface != expected:
            issues.append("policy_document_write_surface drifted from contract-bound write surface")
    if contract.get("referenced_read_only_schema_surfaces_hint"):
        expected = [str(item) for item in contract.get("referenced_read_only_schema_surfaces_hint", [])]
        if referenced_surfaces != expected:
            issues.append("referenced_read_only_schema_surfaces drifted from contract-bound read-only surface list")

    if canonicalization_rule and "canonical" not in canonicalization_rule.lower() and "absolute" not in canonicalization_rule.lower():
        issues.append("canonicalization_rule is not explicit enough about canonical/absolute path handling")

    if not positive_tests:
        issues.append("positive_tests missing")
    if not negative_tests:
        issues.append("negative_tests missing")

    return issues


def _validate_coordination_contract(state: KernelState) -> list[str]:
    analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
    if analysis.get("task_profile") not in {"coordination_sensitive", "combination_sensitive"}:
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable coordination payload"]

    issues: list[str] = []
    coordination_map = plan.get("module_coordination_map", {})
    checkpoints = [str(item) for item in plan.get("integration_checkpoints", [])]
    handoff_risks = [str(item) for item in plan.get("handoff_risks", [])]

    if not isinstance(coordination_map, dict) or len(coordination_map) < 3:
        issues.append("module_coordination_map must cover at least 3 module surfaces")
    if not checkpoints:
        issues.append("integration_checkpoints missing")
    if len(checkpoints) < 2:
        issues.append("integration_checkpoints too shallow for coordination-sensitive task")
    if not handoff_risks:
        issues.append("handoff_risks missing")

    return issues


def _validate_release_structure(state: KernelState) -> list[str]:
    analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
    if analysis.get("task_profile") != "release_sensitive":
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable release payload"]

    issues: list[str] = []
    readiness = [str(item) for item in plan.get("release_readiness_checks", [])]
    rollback = [str(item) for item in plan.get("rollback_evidence", [])]
    observation = [str(item) for item in plan.get("post_release_observation_rules", [])]

    if len(readiness) < 2:
        issues.append("release_readiness_checks too shallow")
    if not rollback:
        issues.append("rollback_evidence missing")
    if not observation:
        issues.append("post_release_observation_rules missing")

    return issues


def _validate_combination_structure(state: KernelState) -> list[str]:
    analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
    if analysis.get("task_profile") != "combination_sensitive":
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable combination payload"]

    issues: list[str] = []
    priority_order = [str(item) for item in plan.get("priority_order", [])]
    conflicts = [str(item) for item in plan.get("cross_capability_conflicts", [])]
    stability_checks = [str(item) for item in plan.get("stability_checks", [])]
    capability_phase_ownership = [str(item) for item in plan.get("capability_phase_ownership", [])]
    capability_mix = [str(item) for item in analysis.get("capability_mix", [])]

    if len(priority_order) < 2:
        issues.append("priority_order too shallow for combination-sensitive task")
    if not conflicts:
        issues.append("cross_capability_conflicts missing")
    if len(stability_checks) < 2:
        issues.append("stability_checks too shallow for combination-sensitive task")
    if len(capability_phase_ownership) < 2:
        issues.append("capability_phase_ownership too shallow for combination-sensitive task")
    if capability_mix:
        alias_map = {
            "memory_retrieval_evidence_validation": ["memory", "retrieval", "evidence"],
            "workspace_coordination_checkpoints": ["workspace", "coordination", "checkpoint"],
            "release_specialization_rules": ["release", "rollback", "readiness"],
            "governed_delivery_flow": ["governance", "delivery", "phase", "approval"],
            "retrieval_evidence": ["retrieval", "evidence", "memory"],
            "coordination": ["coordination", "handoff", "workspace"],
            "release": ["release", "rollback", "readiness"],
            "governance": ["governance", "approval", "policy"],
        }
        searchable_steps = " || ".join(priority_order + conflicts + stability_checks).lower()
        searchable_ownership = " || ".join(capability_phase_ownership).lower()
        missing = []
        ownership_missing = []
        for item in capability_mix:
            aliases = alias_map.get(item, [item])
            if not any(alias.lower() in searchable_steps for alias in aliases):
                missing.append(item)
            if not any(alias.lower() in searchable_ownership for alias in aliases):
                ownership_missing.append(item)
        if len(missing) == len(capability_mix):
            issues.append("combination payload does not reflect orchestrator capability_mix")
        if len(ownership_missing) == len(capability_mix):
            issues.append("capability_phase_ownership does not reflect orchestrator capability_mix")

    return issues


def _validate_workspace_flow_contract(state: KernelState) -> list[str]:
    analysis = state.get("artifacts", {}).get("orchestrator_analysis", {}) or {}
    if analysis.get("coordination_subtype") != "workspace_flow":
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable workspace-flow payload"]

    issues: list[str] = []
    checkpoint_field_contracts = [str(item) for item in plan.get("checkpoint_field_contracts", [])]
    governance_stage_bindings = [str(item) for item in plan.get("governance_stage_bindings", [])]
    dashboard_freshness_contracts = [str(item) for item in plan.get("dashboard_freshness_contracts", [])]
    freshness_field_bindings = [str(item) for item in plan.get("freshness_field_bindings", [])]

    if len(checkpoint_field_contracts) < 2:
        issues.append("checkpoint_field_contracts too shallow for workspace_flow task")
    if len(governance_stage_bindings) < 2:
        issues.append("governance_stage_bindings too shallow for workspace_flow task")
    if len(dashboard_freshness_contracts) < 1:
        issues.append("dashboard_freshness_contracts missing for workspace_flow task")
    if len(freshness_field_bindings) < 1:
        issues.append("freshness_field_bindings missing for workspace_flow task")

    checkpoint_text = " ".join(checkpoint_field_contracts).lower()
    binding_text = " ".join(governance_stage_bindings).lower()
    freshness_text = " ".join(dashboard_freshness_contracts).lower()
    freshness_binding_text = " ".join(freshness_field_bindings).lower()

    if not any(token in checkpoint_text for token in ("field", "type", "threshold", "unit", "trigger", "format", "owner")):
        issues.append("checkpoint_field_contracts do not look field-level enough")
    if not any(token in binding_text for token in ("stage", "phase", "闃舵", "before", "after", "pre-", "post-")):
        issues.append("governance_stage_bindings do not look stage-bound enough")
    if not any(token in freshness_text for token in ("fresh", "鏂伴矞", "ttl", "latency", "delay", "source", "瀛楁", "field")):
        issues.append("dashboard_freshness_contracts do not bind freshness to explicit data contracts")
    if not any(token in freshness_binding_text for token in ("->", "=>", "field_contract", "freshness_contract", "鏄犲皠", "bind", "id")):
        issues.append("freshness_field_bindings do not express explicit freshness-to-field mapping")

    return issues


def _validate_validation_hub_contract(state: KernelState) -> list[str]:
    goal_text = (state.get("goal", "") or "").lower()
    if "validation hub" not in goal_text and "楠岃瘉涓績" not in state.get("goal", ""):
        return []

    plan = _extract_build_plan_payload(state.get("build_result", ""))
    if not plan:
        return ["build_result missing machine-readable validation-hub payload"]

    issues: list[str] = []
    evidence_chain_format = [str(item) for item in plan.get("evidence_chain_format", [])]
    governance_gate_conditions = [str(item) for item in plan.get("governance_gate_conditions", [])]
    governance_binding_modes = [str(item) for item in plan.get("governance_binding_modes", [])]

    if len(evidence_chain_format) < 2:
        issues.append("evidence_chain_format too shallow for validation_hub task")
    if len(governance_gate_conditions) < 2:
        issues.append("governance_gate_conditions too shallow for validation_hub task")
    if len(governance_binding_modes) < 2:
        issues.append("governance_binding_modes too shallow for validation_hub task")

    evidence_text = " ".join(evidence_chain_format).lower()
    gate_text = " ".join(governance_gate_conditions).lower()
    binding_text = " ".join(governance_binding_modes).lower()

    if not any(token in evidence_text for token in ("field", "瀛楁", "format", "鏍煎紡", "schema", "璇佹嵁", "timestamp", "confidence")):
        issues.append("evidence_chain_format does not look explicit enough")
    if not any(token in gate_text for token in ("approval", "gate", "threshold", "status", "condition")):
        issues.append("governance_gate_conditions do not look explicit enough")
    if not any(token in binding_text for token in ("auto", "manual", "浜哄伐", "鑷姩", "trigger", "瑙﹀彂")):
        issues.append("governance_binding_modes do not express auto/manual trigger modes clearly")

    return issues


def _mentions_doctrine_change(text: str) -> bool:
    normalized = str(text or "").lower()
    doctrine_tokens = (
        'doctrine/',
        'mother_memory/',
        'principle',
        'doctrine',
        'governance contract',
        'governance_contract',
    )
    return any(token in normalized for token in doctrine_tokens)


def _doctrine_change_requires_gate(state: KernelState, combined_output: str) -> bool:
    if not _mentions_doctrine_change(combined_output):
        return False

    contract = state.get("artifacts", {}).get("governance_contract", {}) or {}
    plan = _extract_build_plan_payload(state.get("build_result", ""))
    denied_tokens = {str(item) for item in contract.get("denied_path_tokens_hint", [])}
    denied_tokens.update(str(item) for item in plan.get("denied_path_tokens", []))
    denied_roots = {str(item) for item in plan.get("exact_denied_roots", [])}

    if "doctrine/" in denied_tokens or any("doctrine" in item.lower() for item in denied_roots):
        return False

    return True


def _normalize_build_plan_against_contract(build_result: str, contract: dict[str, object]) -> str:
    if not contract:
        return build_result

    plan = _extract_build_plan_payload(build_result)
    if not plan:
        return build_result

    def merge_list(field: str, additions: list[object]) -> None:
        existing = [str(item) for item in plan.get(field, [])]
        merged = list(dict.fromkeys([*existing, *[str(item) for item in additions if str(item)]]))
        if merged:
            plan[field] = merged

    strict_binding_fields = {str(item) for item in contract.get("strict_binding_fields", [])}

    def bind_list(field: str, values: list[object]) -> None:
        bound = [str(item) for item in values if str(item)]
        if bound:
            plan[field] = bound

    if "exact_allowed_roots" in strict_binding_fields:
        bind_list("exact_allowed_roots", contract.get("exact_allowed_roots_hint", []))
    else:
        merge_list("exact_allowed_roots", contract.get("exact_allowed_roots_hint", []))
    if "exact_denied_roots" in strict_binding_fields:
        bind_list("exact_denied_roots", contract.get("exact_denied_roots_hint", []))
    else:
        merge_list("exact_denied_roots", contract.get("exact_denied_roots_hint", []))
    if "denied_path_tokens" in strict_binding_fields:
        bind_list("denied_path_tokens", contract.get("denied_path_tokens_hint", []))
    else:
        merge_list("denied_path_tokens", contract.get("denied_path_tokens_hint", []))
    merge_list("negative_tests", contract.get("negative_test_expectations", []))
    merge_list("positive_tests", contract.get("positive_test_expectations", []))

    canonicalization_rule = str(plan.get("canonicalization_rule", "")).strip()
    if (
        contract.get("canonicalization_expectation")
        and (
            not canonicalization_rule
            or ("canonical" not in canonicalization_rule.lower() and "absolute" not in canonicalization_rule.lower())
        )
    ):
        plan["canonicalization_rule"] = str(contract["canonicalization_expectation"])
    if contract.get("policy_document_write_surface_hint") and not str(plan.get("policy_document_write_surface", "")).strip():
        plan["policy_document_write_surface"] = str(contract["policy_document_write_surface_hint"])
    if contract.get("referenced_read_only_schema_surfaces_hint") and not plan.get("referenced_read_only_schema_surfaces"):
        plan["referenced_read_only_schema_surfaces"] = [
            str(item) for item in contract.get("referenced_read_only_schema_surfaces_hint", [])
        ]

    plan = _normalize_change_plan_payload(plan)

    return json.dumps(plan, ensure_ascii=False, indent=2)



_STANDARD_TASK_UNIT_V1_LIMITS = {
    "max_core_files": 5,
    "max_action_types": 3,
    "preferred_module_scope": "single_module_preferred",
    "rework_expectation": "single_or_small_rework_loop",
}

_STANDARD_ACTION_HINTS = (
    "write_file",
    "edit_file",
    "run_tests",
    "run_lint",
    "install_dep",
    "git_diff",
)


def _normalize_task_card(task_card: dict[str, object]) -> TaskCard:
    normalized = dict(task_card or {})
    normalized['scope_hint'] = _clean_text_list(normalized.get('scope_hint') or normalized.get('scope', []))
    normalized['constraints'] = _clean_text_list(normalized.get('constraints', []))
    normalized['acceptance_tests'] = _clean_text_list(normalized.get('acceptance_tests') or normalized.get('acceptance_criteria', []))
    normalized['assigned_agents'] = _clean_text_list(normalized.get('assigned_agents', []))
    normalized['orchestrator_brief'] = str(normalized.get('orchestrator_brief', '') or '').strip()
    normalized['task_profile'] = str(normalized.get('task_profile', '') or 'routine').strip() or 'routine'
    normalized['goal'] = str(normalized.get('goal', '') or '').strip()
    normalized['task_id'] = str(normalized.get('task_id', '') or '').strip()
    risk_level = str(normalized.get('risk_level', '') or 'medium').strip() or 'medium'
    normalized['risk_level'] = risk_level if risk_level in {'low', 'medium', 'high'} else 'medium'
    return normalized  # type: ignore[return-value]


def _validate_task_card_contract(task_card: dict[str, object]) -> dict[str, object]:
    normalized = _normalize_task_card(task_card)
    missing_fields: list[str] = []
    for field_name in TASK_CARD_REQUIRED_FIELDS:
        value = normalized.get(field_name)
        if isinstance(value, list):
            if not _clean_text_list(value):
                missing_fields.append(field_name)
        elif not str(value or '').strip():
            missing_fields.append(field_name)
    return {
        'schema_version': 'task_card_contract.v1',
        'missing_fields': missing_fields,
        'passed': not missing_fields,
        'required_fields': list(TASK_CARD_REQUIRED_FIELDS),
    }


def _task_card_contract_error_summary(validation: dict[str, object]) -> str:
    missing = list(validation.get('missing_fields', []) or [])
    if not missing:
        return ''
    return 'Task card contract failed. Missing required fields: ' + ', '.join(str(item) for item in missing)


def _current_review_feedback_excerpt(state: KernelState) -> str:
    return _clip_text(str(state.get('review_feedback', '') or ''), limit=TASK_AND_HANDOFF_LIMITS['review_feedback_chars'])


def _reviewer_evidence_posture(execution_evidence: dict[str, object], assessment: dict[str, object]) -> dict[str, object]:
    issues = _reviewer_evidence_gate_issues(execution_evidence, assessment)
    source_workspace_root = str(
        execution_evidence.get('review_source_workspace_root', '')
        or execution_evidence.get('source_workspace_root', '')
        or ''
    ).strip()
    changed_files = [str(item).strip() for item in list(execution_evidence.get('changed_files', []) or []) if str(item).strip()]
    git_diff_summary = str(execution_evidence.get('git_diff_summary', '') or '').strip()
    runtime_status = str(execution_evidence.get('runtime_status', '') or '').strip().lower()
    command_results = list(execution_evidence.get('command_results', []) or [])
    test_results = list(execution_evidence.get('test_results', []) or [])
    model_decision = str(assessment.get('decision', '') or '').strip().lower()

    scene_available = bool(source_workspace_root)
    evidence_available = bool(changed_files or git_diff_summary or command_results or test_results)
    judgeable = scene_available or evidence_available
    severe = []
    if not judgeable:
        severe.append('review_scene_unavailable')
    if runtime_status in {'failed', 'error'}:
        severe.append(f'runtime_status={runtime_status}')
    if model_decision == 'approved' and not scene_available and not (changed_files or git_diff_summary):
        severe.append('approved_without_scene_or_diff_anchor')

    return {
        'judgeable': judgeable and not severe,
        'scene_available': scene_available,
        'evidence_debt': issues,
        'hard_blockers': severe,
    }


def _build_validator_input_bundle(
    state: KernelState,
    *,
    execution_evidence: dict[str, object],
    review_assessment: dict[str, object],
    intake_assessment: dict[str, object],
    trigger_plan: dict[str, object],
) -> dict[str, object]:
    evidence_summary = {
        'runtime_status': str(execution_evidence.get('runtime_status', '') or ''),
        'git_diff_status': str(execution_evidence.get('git_diff_status', '') or ''),
        'lint_status': str(execution_evidence.get('lint_status', '') or ''),
        'pytest_status': str(execution_evidence.get('pytest_status', '') or ''),
        'compile_status': str(execution_evidence.get('compile_status', '') or ''),
        'install_dep_status': str(execution_evidence.get('install_dep_status', '') or ''),
        'warnings': [str(item)[:160] for item in list(execution_evidence.get('warnings', []) or [])[:4]],
        'changed_files': list(execution_evidence.get('changed_files', []) or [])[:12],
    }
    review_summary = {
        'decision': str(review_assessment.get('effective_decision') or review_assessment.get('decision') or state.get('review_status', '') or '').strip(),
        'summary': _clip_text(review_assessment.get('summary', '') or state.get('review_result', ''), limit=260),
        'validation_gaps': [str(item)[:160] for item in list(review_assessment.get('validation_gaps', []) or [])[:4]],
        'issues': [str(item)[:160] for item in list(review_assessment.get('issues', []) or [])[:4]],
    }
    gate_conditions = {
        'validator_required': bool(trigger_plan.get('validator_required', True)),
        'validator_reason': str(trigger_plan.get('validator_reason', '') or ''),
        'should_split': bool(intake_assessment.get('should_split', False)),
        'approval_required': bool(state.get('approval_required', False)),
        'risk_level': str(state.get('risk_level', '') or ''),
        'task_kind': str(state.get('task_kind', '') or ''),
    }
    return {
        'schema_version': 'validator_input_bundle.v1',
        'evidence_summary': evidence_summary,
        'review_summary': review_summary,
        'gate_conditions': gate_conditions,
        'approval_escalation_context': {
            'approval_required': bool(state.get('approval_required', False)),
            'approval_status': str(state.get('approval_status', '') or ''),
            'risk_level': str(state.get('risk_level', '') or ''),
        },
    }


def _build_builder_input_bundle(
    state: KernelState,
    *,
    project_memory_context: str,
    orchestrator_guidance: str,
    builder_working_state: str,
) -> dict[str, object]:
    task_card = _normalize_task_card(dict(state.get('task_card', {}) or {}))
    task_profile = str(task_card.get('task_profile', 'routine') or 'routine').strip().lower()
    if task_profile == 'routine':
        execution_scope_value = task_card.get('execution_scope', {}) or {}
        if isinstance(execution_scope_value, dict):
            execution_scope = dict(execution_scope_value)
        elif execution_scope_value:
            execution_scope = {'mode': str(execution_scope_value)}
        else:
            execution_scope = {}
        task_card = {
            'task_id': task_card.get('task_id', ''),
            'goal': task_card.get('goal', ''),
            'task_profile': task_profile,
            'scope': list(task_card.get('scope', []) or []),
            'constraints': list(task_card.get('constraints', []) or []),
            'acceptance_criteria': list(task_card.get('acceptance_criteria', []) or []),
            'execution_scope': execution_scope,
            'orchestrator_brief': task_card.get('orchestrator_brief', ''),
            'out_of_scope': list(task_card.get('out_of_scope', []) or []),
        }
    raw_scan_result = str(state.get('scan_result', '') or '')
    grounding_scan = _build_builder_grounding_scan(state, task_card, str(task_card.get('orchestrator_brief', '') or orchestrator_guidance or ''))
    combined_scan_result = raw_scan_result
    if grounding_scan:
        combined_scan_result = (combined_scan_result + '\n\n' + grounding_scan).strip() if combined_scan_result.strip() else grounding_scan
    scan_result = _clip_text(combined_scan_result, limit=TASK_AND_HANDOFF_LIMITS['scan_result_chars'])
    project_memory_excerpt = _clip_text(project_memory_context, limit=TASK_AND_HANDOFF_LIMITS['project_memory_chars'])
    builder_working_state_excerpt = _clip_text(builder_working_state, limit=TASK_AND_HANDOFF_LIMITS['builder_working_state_chars'])
    review_feedback_excerpt = _current_review_feedback_excerpt(state)
    orchestrator_brief = _clip_text(
        str(task_card.get('orchestrator_brief') or orchestrator_guidance or ''),
        limit=TASK_AND_HANDOFF_LIMITS['orchestrator_brief_chars'],
    )
    bundle = {
        'schema_version': 'builder_input_bundle.v2',
        'task_card': task_card,
        'scan_result': scan_result,
        'grounding_scan_excerpt': _clip_text(grounding_scan, limit=900),
        'project_memory_excerpt': project_memory_excerpt,
        'orchestrator_brief': orchestrator_brief,
        'builder_working_state': builder_working_state_excerpt,
        'review_feedback': review_feedback_excerpt,
        'rework_count': int(state.get('rework_count', 0) or 0),
        'truncation_flags': {
            'scan_result': len(combined_scan_result or '') > TASK_AND_HANDOFF_LIMITS['scan_result_chars'],
            'grounding_scan_excerpt': len(grounding_scan or '') > 900,
            'project_memory_excerpt': len(project_memory_context or '') > TASK_AND_HANDOFF_LIMITS['project_memory_chars'],
            'orchestrator_brief': len(str(task_card.get('orchestrator_brief') or orchestrator_guidance or '')) > TASK_AND_HANDOFF_LIMITS['orchestrator_brief_chars'],
            'builder_working_state': len(builder_working_state or '') > TASK_AND_HANDOFF_LIMITS['builder_working_state_chars'],
            'review_feedback': len(str(state.get('review_feedback', '') or '')) > TASK_AND_HANDOFF_LIMITS['review_feedback_chars'],
        },
    }
    return bundle


def _clean_text_list(value: object) -> list[str]:
    items = value if isinstance(value, list) else []
    return [str(item).strip() for item in items if str(item).strip()]



def _merge_unique_lists(*groups: object) -> list[str]:
    merged: list[str] = []
    for group in groups:
        for item in _clean_text_list(group):
            if item not in merged:
                merged.append(item)
    return merged


def _clip_text(value: object, *, limit: int = 240) -> str:
    text = ' '.join(str(value or '').split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _extract_candidate_relative_paths(*values: object) -> list[str]:
    pattern = re.compile(r"(?<![A-Za-z0-9_])([A-Za-z0-9_./\-]+\.(?:py|js|ts|tsx|jsx|json|md|yaml|yml|html|css))(?![A-Za-z0-9_])")
    results: list[str] = []
    for value in values:
        for match in pattern.findall(str(value or '')):
            candidate = str(match).replace('\\', '/').strip('./')
            if candidate and candidate not in results:
                results.append(candidate)
    return results


def _grounding_excerpt_for_file(file_path: Path, *, max_chars: int = 1200) -> str:
    try:
        lines = file_path.read_text(encoding='utf-8').splitlines()
    except Exception:
        return ''
    matched_indexes: list[int] = []
    for index, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if (
            stripped.startswith('def ')
            or stripped.startswith('async def ')
            or stripped.startswith('function ')
            or '<section id=' in lowered
            or 'page-home' in lowered
            or 'page-config' in lowered
            or 'page-params' in lowered
            or 'agent_configs' in lowered
            or 'currentpageid' in lowered
            or '/api/agent_configs' in lowered
            or 'renderagentconfigs' in lowered
            or 'switchpage(' in lowered
            or 'display:none' in lowered
            or '.page-nav' in lowered
            or 'role_config_summary' in lowered
            or 'current role' in lowered
            or '褰撳墠瑙掕壊閰嶇疆鎽樿' in lowered
        ):
            matched_indexes.append(index)
    excerpt_lines: list[str] = []
    if matched_indexes:
        emitted: set[int] = set()
        for match_index in matched_indexes[:8]:
            start = max(1, match_index - 1)
            end = min(len(lines), match_index + 1)
            excerpt_lines.append(f"-- snippet around L{match_index} --")
            for line_no in range(start, end + 1):
                if line_no in emitted:
                    continue
                emitted.add(line_no)
                raw_line = lines[line_no - 1].rstrip()
                if raw_line.strip():
                    excerpt_lines.append(f"L{line_no}: {raw_line}")
            if len('\n'.join(excerpt_lines)) >= max_chars:
                break
    if not excerpt_lines:
        excerpt_lines = [f"L{index}: {line.rstrip()}" for index, line in enumerate(lines[:25], start=1) if line.strip()]
    excerpt = '\n'.join(excerpt_lines)
    return excerpt[:max_chars].rstrip()


def _build_builder_grounding_scan(state: KernelState, task_card: TaskCard, orchestrator_brief: str) -> str:
    workspace_root_raw = str(get_agent_settings('builder').workspace_root or '').strip()
    if not workspace_root_raw:
        return ''
    workspace_root = Path(workspace_root_raw)
    task_profile = str(task_card.get('task_profile', 'routine') or 'routine').strip().lower()
    execution_scope_value = task_card.get('execution_scope') or {}
    if isinstance(execution_scope_value, dict):
        execution_scope = dict(execution_scope_value)
    elif execution_scope_value:
        execution_scope = {'mode': str(execution_scope_value)}
    else:
        execution_scope = {}
    max_core_files = int(execution_scope.get('max_core_files', 0) or 0)
    routine_bias = task_profile == 'routine' and max_core_files <= 2
    candidates = _extract_candidate_relative_paths(
        state.get('goal', ''),
        task_card.get('goal', ''),
        orchestrator_brief,
        task_card.get('scope_hint', []),
        task_card.get('constraints', []),
        task_card.get('acceptance_tests', []),
    )
    preferred_targets: list[str] = []
    deferred_targets: list[str] = []
    for rel_path in candidates:
        normalized = str(rel_path or '').strip().replace('\\', '/')
        if not normalized:
            continue
        if normalized in preferred_targets or normalized in deferred_targets:
            continue
        if routine_bias and normalized in {'config/agents.json', 'src/ai_dev_os/agent_settings.py'}:
            deferred_targets.append(normalized)
            continue
        preferred_targets.append(normalized)
    ordered_candidates = preferred_targets + deferred_targets[:1]
    scan_limit = 2 if routine_bias else 3
    blocks: list[str] = []
    for rel_path in ordered_candidates[:scan_limit]:
        file_path = (workspace_root / rel_path).resolve()
        try:
            file_path.relative_to(workspace_root.resolve())
        except Exception:
            continue
        if not file_path.exists() or not file_path.is_file():
            continue
        excerpt = _grounding_excerpt_for_file(file_path)
        if excerpt:
            blocks.append(f"[grounding:{rel_path}]\n{excerpt}")
    return '\n\n'.join(blocks)


def _builder_working_state(state: KernelState) -> BuilderWorkingState:
    return dict((state.get('artifacts', {}) or {}).get('builder_working_state', {}) or {})


def _normalize_execution_failure_brief(execution_report: dict[str, object], execution_result: str) -> dict[str, object]:
    warnings = _clean_text_list(execution_report.get('warnings', []))
    actions = [dict(item) for item in list(execution_report.get('actions', []) or []) if isinstance(item, dict)]
    failed_actions = [item for item in actions if str(item.get('status', 'passed')).lower() != 'passed']
    failure_codes = _merge_unique_lists(
        [str(item.get('error_code', '') or '').strip() for item in failed_actions],
        [warning.split(':', 2)[1].strip() for warning in warnings if warning.count(':') >= 2],
    )[:8]
    failed_targets = _merge_unique_lists(
        [str(item.get('target', '') or '').strip() for item in failed_actions],
        [warning.split(':', 2)[0].strip() for warning in warnings if warning.count(':') >= 2],
    )[:6]
    hints: list[str] = []
    forbidden_retries: list[str] = []
    code_set = {code for code in failure_codes if code}
    warning_text = ' '.join(warnings).lower()
    if 'target_text_not_found' in code_set or 'target text not found' in warning_text:
        hints.append('Re-scout the real file anchor before generating a new old_text snippet.')
        forbidden_retries.append('Do not keep retrying the same unmatched old_text anchor.')
    if 'source_edit_failed' in code_set:
        hints.append('Confirm the target snippet still exists, then shrink the patch scope.')
    if 'source_write_failed' in code_set:
        hints.append('Check the target path and output content format before writing the file again.')
    runtime_status = str(execution_report.get('runtime_status', '') or '').strip().lower()
    if runtime_status and runtime_status not in {'passed', 'approved', 'success'}:
        hints.append(f'Handle the runtime failure first: {runtime_status}.')
    if str(execution_report.get('git_diff_status', '') or '').strip().lower() == 'failed':
        hints.append('Reconfirm the change really landed, then collect diff evidence again.')
    if 'compile_failed' in code_set:
        hints.append('Fix syntax or import errors first before expanding functionality.')
    if 'pytest_failed' in code_set:
        hints.append('Restore the smallest validation command first before broadening the change.')
    if not failure_codes and warnings:
        hints.append('Work through warnings one by one, starting with the first blocking signal.')
    if 'agents.json' in str(execution_result or ''):
        forbidden_retries.append('Do not create a parallel configuration path through agents.json.')
    summary = ' ; '.join(hints[:3]) if hints else (_short_text(warnings[0], 140) if warnings else 'Execution hit a recoverable failure and needs a narrower re-scouted pass.')
    return {
        'failure_summary': summary,
        'failure_codes': [item for item in failure_codes if item],
        'failed_targets': [item for item in failed_targets if item],
        'repair_hints': hints[:6],
        'forbidden_retries': _merge_unique_lists(forbidden_retries, warnings)[:8],
    }


def _extract_grounded_data_sources(*values: object) -> list[str]:
    patterns = [
        r"/api/[A-Za-z0-9_./-]+",
        r"get_[A-Za-z0-9_]+\(",
        r"list_[A-Za-z0-9_]+\(",
        r"[A-Z_]+_PATH",
        r"fetch\('([^']+)'\)",
        r'fetch\("([^"]+)"\)',
    ]
    results: list[str] = []
    for value in values:
        raw = str(value or '')
        for pattern in patterns:
            for match in re.finditer(pattern, raw):
                token = match.group(1) if match.lastindex else match.group(0)
                token = str(token).rstrip('(').strip()
                if token and token not in results:
                    results.append(token)
    return results[:8]


def _extract_confirmed_anchors_from_plan(plan: dict[str, object]) -> list[str]:
    raw_change_plan = plan.get('change_plan', {})
    change_plan = dict(raw_change_plan if isinstance(raw_change_plan, dict) else {})
    changes = change_plan.get('changes', []) if isinstance(change_plan.get('changes', []), list) else []
    anchors: list[str] = []
    for item in changes:
        if not isinstance(item, dict):
            continue
        payload = dict(item.get('payload', {}) or {})
        old_text = str(payload.get('old_text', '') or '').strip()
        if old_text:
            single = ' '.join(old_text.split())
            if single and single not in anchors:
                anchors.append(single[:180])
    return anchors[:6]


def _format_builder_working_state_for_prompt(state: KernelState) -> str:
    working_state = _builder_working_state(state)
    if not working_state:
        return ''
    visible = {
        'current_primary_goal': str(working_state.get('current_primary_goal', '') or ''),
        'current_subgoal': 'Establish the smallest grounded implementation path for the current task.',
        'accepted_scope': list(working_state.get('accepted_scope', []) or []),
        'blocked_points': list(working_state.get('blocked_points', []) or []),
        'confirmed_anchors': list(working_state.get('confirmed_anchors', []) or []),
        'grounded_data_sources': list(working_state.get('grounded_data_sources', []) or []),
        'forbidden_patterns': list(working_state.get('forbidden_patterns', []) or []),
        'validation_history': list(working_state.get('validation_history', []) or []),
        'failure_summary': str(working_state.get('failure_summary', '') or ''),
        'failure_codes': list(working_state.get('failure_codes', []) or []),
        'failed_targets': list(working_state.get('failed_targets', []) or []),
        'repair_hints': list(working_state.get('repair_hints', []) or []),
        'last_outcome': str(working_state.get('last_outcome', '') or ''),
        'next_step': str(working_state.get('next_step', '') or ''),
        'cycle_index': int(working_state.get('cycle_index', 0) or 0),
        'updated_by': str(working_state.get('updated_by', '') or ''),
    }
    return json.dumps(visible, ensure_ascii=False, indent=2)


def _initialize_builder_working_state(goal: str, task_card: TaskCard) -> BuilderWorkingState:
    accepted_scope = _clean_text_list(task_card.get('scope', []))
    return {
        'current_primary_goal': goal,
        'current_subgoal': 'Establish the smallest grounded implementation path for the current task.',
        'accepted_scope': accepted_scope,
        'blocked_points': [],
        'confirmed_anchors': [],
        'grounded_data_sources': [],
        'forbidden_patterns': [],
        'validation_history': [],
        'last_outcome': 'Task card created and ready for builder planning.',
        'next_step': 'Start with the smallest grounding scout, then produce and execute the smallest grounded implementation.',
        'updated_at': now_iso(),
        'updated_by': 'orchestrator',
        'cycle_index': 0,
    }


def _update_builder_working_state_from_builder(state: KernelState, build_result: str) -> BuilderWorkingState:
    previous = dict(_builder_working_state(state))
    plan = _extract_build_plan_payload(build_result)
    implementation_steps = _clean_text_list(plan.get('implementation_steps', []))
    validation_checks = _clean_text_list(plan.get('validation_checks', []))
    raw_change_plan = plan.get('change_plan', {})
    change_plan = dict(raw_change_plan if isinstance(raw_change_plan, dict) else {})
    changes = change_plan.get('changes', []) if isinstance(change_plan.get('changes', []), list) else []
    accepted_scope = _merge_unique_lists(
        previous.get('accepted_scope', []),
        state.get('task_card', {}).get('scope', []),
        [item.get('target', '') for item in changes if isinstance(item, dict)],
    )
    blocked_points = _merge_unique_lists(
        _clean_text_list(previous.get('blocked_points', []))[:3],
        [state.get('review_feedback', '')],
        [state.get('blocking_issue', '')],
    )[:6]
    next_step = implementation_steps[0] if implementation_steps else 'Proceed with the generated build plan.'
    last_outcome = _clip_text(plan.get('summary', '') or build_result, limit=260)
    if validation_checks:
        last_outcome = _clip_text(f"{last_outcome} Validation focus: {validation_checks[0]}", limit=260)
    validation_history = _merge_unique_lists(previous.get('validation_history', []), validation_checks)[:6]
    return {
        'current_primary_goal': str(previous.get('current_primary_goal') or state.get('goal', '')),
        'current_subgoal': _clip_text(next_step, limit=180),
        'accepted_scope': accepted_scope,
        'blocked_points': blocked_points,
        'confirmed_anchors': _merge_unique_lists(previous.get('confirmed_anchors', []), _extract_confirmed_anchors_from_plan(plan))[:6],
        'grounded_data_sources': _merge_unique_lists(previous.get('grounded_data_sources', []), _extract_grounded_data_sources(build_result, state.get('scan_result', ''), state.get('goal', '')))[:8],
        'forbidden_patterns': _merge_unique_lists(previous.get('forbidden_patterns', []), [state.get('blocking_issue', '')], [state.get('review_feedback', '')])[:8],
        'validation_history': validation_history,
        'last_outcome': last_outcome,
        'next_step': _clip_text(next_step, limit=180),
        'updated_at': now_iso(),
        'updated_by': 'builder',
        'cycle_index': int(state.get('rework_count', 0) or 0),
    }


def _align_builder_working_state(working_state: BuilderWorkingState, *, builder_status: str, blocking_issue: str = '') -> BuilderWorkingState:
    normalized = dict(working_state or {})
    blocked_points_existing = normalized.get('blocked_points', []) if isinstance(normalized.get('blocked_points', []), list) else []
    first_blocker = str(blocked_points_existing[0] or '').strip() if blocked_points_existing else ''
    blocker = str(blocking_issue or first_blocker or '').strip()
    if builder_status == 'blocked':
        blocked_points = _merge_unique_lists(normalized.get('blocked_points', []), [blocker] if blocker else [])[:6]
        normalized['blocked_points'] = blocked_points
        normalized['last_outcome'] = _clip_text(blocker or str(normalized.get('last_outcome', '') or 'Builder paused and needs downstream handling.'), limit=260)
        normalized['next_step'] = _clip_text('Wait for human or upstream confirmation before continuing.', limit=180)
    elif builder_status == 'rejected':
        normalized['last_outcome'] = _clip_text(blocker or str(normalized.get('last_outcome', '') or 'Builder rejected the current input contract.'), limit=260)
        normalized['next_step'] = _clip_text('Wait for upstream task-card correction before continuing.', limit=180)
    else:
        normalized['blocked_points'] = _clean_text_list(normalized.get('blocked_points', []))[:6]
    normalized['updated_at'] = now_iso()
    normalized['updated_by'] = 'builder'
    return normalized


def _update_builder_working_state_from_execution(
    state: KernelState,
    execution_report: dict[str, object],
    execution_result: str,
) -> BuilderWorkingState:
    previous = dict(_builder_working_state(state))
    warnings = _clean_text_list(execution_report.get('warnings', []))
    runtime_status = str(execution_report.get('runtime_status', '') or '')
    failure_brief = _normalize_execution_failure_brief(execution_report, execution_result)
    failure_summary = str(failure_brief.get('failure_summary') or execution_result or '').strip()
    failure_codes = _clean_text_list(failure_brief.get('failure_codes', []))
    failed_targets = _clean_text_list(failure_brief.get('failed_targets', []))
    repair_hints = _clean_text_list(failure_brief.get('repair_hints', []))
    forbidden_retries = _clean_text_list(failure_brief.get('forbidden_retries', []))

    blocked_points = [
        item for item in _merge_unique_lists(previous.get('blocked_points', []), warnings, failed_targets)
        if item
    ][:8]
    if runtime_status and runtime_status not in {'passed', 'approved', 'success'}:
        blocked_points = _merge_unique_lists(blocked_points, [f'runtime_status={runtime_status}'])[:8]

    next_step = (
        'Use the previous failure reason, repair hints, and confirmed anchors to self-repair with a narrower change surface.'
        if blocked_points or int(execution_report.get('failed_count', 0) or 0) > 0
        else 'Hand off the latest implementation and evidence for review.'
    )

    validation_history = _merge_unique_lists(
        previous.get('validation_history', []),
        [failure_summary] if failure_summary else [],
        warnings,
    )[:8]

    forbidden_patterns = _merge_unique_lists(
        previous.get('forbidden_patterns', []),
        warnings,
        forbidden_retries,
    )[:8]

    grounded_data_sources = _merge_unique_lists(
        previous.get('grounded_data_sources', []),
        _extract_grounded_data_sources(failure_summary, execution_result),
    )[:8]

    return {
        'current_primary_goal': str(previous.get('current_primary_goal') or state.get('goal', '')),
        'current_subgoal': str(previous.get('current_subgoal') or 'Execute the current build plan.'),
        'accepted_scope': _clean_text_list(previous.get('accepted_scope', [])) or _clean_text_list(state.get('task_card', {}).get('scope', [])),
        'blocked_points': blocked_points,
        'confirmed_anchors': list(previous.get('confirmed_anchors', []) or []),
        'grounded_data_sources': grounded_data_sources,
        'forbidden_patterns': forbidden_patterns,
        'validation_history': validation_history,
        'failure_summary': _clip_text(failure_summary, limit=260),
        'failure_codes': failure_codes[:8],
        'failed_targets': failed_targets[:8],
        'repair_hints': repair_hints[:8],
        'last_outcome': _clip_text(failure_summary or execution_result, limit=260),
        'next_step': next_step,
        'updated_at': now_iso(),
        'updated_by': 'execution',
        'cycle_index': int(state.get('rework_count', 0) or 0),
    }


def _builder_round_context_summary(state: KernelState) -> str:
    working_state = _builder_working_state(state)
    primary_goal = str(state.get('goal', '') or '').strip()
    subgoal = str(working_state.get('current_subgoal', '') or '').strip()
    blocked = list(working_state.get('blocked_points', []) or [])
    next_step = str(working_state.get('next_step', '') or 'Continue the current builder loop.').strip()
    cycle_index = int(working_state.get('cycle_index', 0) or 0)
    review_feedback = str(state.get('review_feedback', '') or '').strip()
    if review_feedback:
        round_reason = 'Starting a new repair pass because reviewer feedback is present.'
    elif int(state.get('rework_count', 0) or 0) > 0:
        round_reason = 'Continuing the current loop after a prior rework.'
    else:
        round_reason = 'First builder pass for the current task.'
    parts: list[str] = []
    if primary_goal:
        parts.append(f'goal={primary_goal}')
    if subgoal:
        parts.append(f'subgoal={subgoal}')
    parts.append(f'reason={round_reason}')
    if blocked:
        parts.append(f'blocked={blocked[0]}')
    if next_step:
        parts.append(f'next={next_step}')
    parts.append(f'cycle={cycle_index + 1}')
    return ' | '.join(parts)



def _short_text(value: object, limit: int = 140) -> str:
    text = str(value or '').strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + '...'



def _validator_mode_label(trigger_plan: dict[str, object]) -> str:
    if not bool(trigger_plan.get('validator_required', True)):
        return 'light_boundary_check'
    reason = str(trigger_plan.get('validator_reason', '') or '').strip()
    return f'hard_validation({reason})' if reason else 'hard_validation'



def _agent_round_report_payload(agent: str, state: KernelState) -> dict[str, object]:
    goal = _short_text(state.get('goal', '') or '', 96)
    blocking = _short_text(state.get('blocking_issue', '') or '', 96)
    payload: dict[str, object] = {
        'agent': agent,
        'goal': goal,
        'action': '',
        'result': '',
        'blocker': blocking or 'no explicit blocker',
        'next_step': 'continue to the next stage',
        'round_index': int(state.get('rework_count', 0) or 0) + 1,
        'status': 'completed',
    }
    if agent == 'orchestrator':
        analysis = dict((state.get('artifacts', {}).get('control_artifacts', {}) or {}).get('orchestrator_analysis', {}) or {})
        payload.update({
            'action': 'shape the task and produce a structured handoff',
            'result': _short_text(analysis.get('builder_brief', '') or f"task_profile={analysis.get('task_profile', 'routine')}", 120),
            'next_step': 'handoff to builder using the structured task card',
            'status': 'ready',
        })
        return payload
    if agent == 'builder':
        working_state = _builder_working_state(state)
        subgoal = _short_text(working_state.get('current_subgoal', '') or '', 84)
        last_outcome = _short_text(working_state.get('last_outcome', '') or '', 110)
        next_step = _short_text(working_state.get('next_step', '') or 'handoff to reviewer for observation and judgment', 84)
        blocked_points = list(working_state.get('blocked_points', []) or [])
        payload.update({
            'action': f"produce or revise the current build plan{subgoal and f' (subgoal={subgoal})' or ''}",
            'result': last_outcome or 'new builder output generated',
            'blocker': _short_text(blocked_points[0] if blocked_points else blocking or 'no clear blocker', 84),
            'next_step': next_step,
        })
        return payload
    if agent == 'reviewer':
        review_status = str(state.get('review_status', '') or 'approved').strip() or 'approved'
        review_feedback = _short_text(state.get('review_feedback', '') or state.get('review_result', ''), 110)
        next_step = 'handoff to recorder'
        if review_status == 'changes_requested':
            next_step = 'return to builder with repair hints'
        payload.update({
            'action': 'inspect evidence, workspace state, and acceptance alignment',
            'result': f"review_status={review_status}{review_feedback and f' / {review_feedback}' or ''}",
            'blocker': _short_text(review_feedback or blocking or ('review not approved' if review_status == 'changes_requested' else 'no explicit blocker'), 96),
            'next_step': next_step,
            'status': review_status,
        })
        return payload
    return payload



def _agent_round_report_summary(agent: str, state: KernelState) -> str:
    report = _agent_round_report_payload(agent, state)
    if not any(str(report.get(key, '') or '').strip() for key in ('action', 'result', 'next_step')):
        return ''
    parts = [
        f"goal={report.get('goal', '')}",
        f"action={report.get('action', '')}",
        f"result={report.get('result', '')}",
        f"blocker={report.get('blocker', '')}",
        f"next={report.get('next_step', '')}",
    ]
    return ' | '.join(parts)



def _reviewer_round_context_summary(state: KernelState, execution_evidence: dict[str, object], prior_feedback: str) -> str:
    primary_goal = str(state.get('goal', '') or '').strip()
    review_result = str(state.get('review_result', '') or '').strip()
    runtime_status = str(execution_evidence.get('runtime_status', '') or '').strip()
    next_step = 'produce a review decision and choose approve or rework'
    if runtime_status and runtime_status not in {'passed', 'approved', 'success'}:
        next_step = 'decide whether the runtime issues require returning to builder'
    if prior_feedback:
        round_reason = 'continuing review with prior feedback in context'
    elif review_result:
        round_reason = 'starting a new review pass after prior execution output'
    else:
        round_reason = 'first reviewer pass for the current task'
    parts: list[str] = []
    if primary_goal:
        parts.append(f'goal={primary_goal}')
    parts.append(f'reason={round_reason}')
    if runtime_status:
        parts.append(f'runtime={runtime_status}')
    blocked = str(state.get('blocking_issue', '') or '').strip()
    if blocked:
        parts.append(f'blocked={blocked[:120]}')
    if prior_feedback:
        parts.append(f'prior_feedback={prior_feedback[:120]}')
    parts.append(f'next={next_step}')
    parts.append(f"round={int(state.get('rework_count', 0) or 0) + 1}")
    return ' | '.join(parts)



def _validator_round_context_summary(
    state: KernelState,
    execution_evidence: dict[str, object],
    review_assessment: dict[str, object],
    intake_assessment: dict[str, object],
    trigger_plan: dict[str, object],
) -> str:
    primary_goal = str(state.get('goal', '') or '').strip()
    runtime_status = str(execution_evidence.get('runtime_status', '') or '').strip()
    validator_required = bool(trigger_plan.get('validator_required', True))
    periodic_reason = str(trigger_plan.get('validator_reason', '') or '').strip()
    review_decision = str(review_assessment.get('decision', '') or '').strip().lower()
    review_summary = str(review_assessment.get('summary', '') or '').strip()
    missing_fields = list(intake_assessment.get('missing_structured_fields', []) or [])
    should_split = bool(intake_assessment.get('should_split', False))
    next_step = 'complete validation and decide approve or rework'
    if not validator_required:
        round_reason = 'validator is running in light boundary-check mode'
    elif periodic_reason:
        round_reason = f'validator triggered by rule: {periodic_reason}'
    elif state.get('rework_count', 0) or review_decision == 'changes_requested':
        round_reason = 'validator entered after rework or a reviewer rejection'
    else:
        round_reason = 'validator reached at the current stage boundary'
    blocked_points: list[str] = []
    if should_split:
        blocked_points.append('task should be split before continuation')
    if missing_fields:
        blocked_points.append(f"missing structured fields: {', '.join(str(item) for item in missing_fields[:3])}")
    if runtime_status and runtime_status not in {'passed', 'approved', 'success'}:
        blocked_points.append(f'runtime_status={runtime_status}')
    parts: list[str] = []
    if primary_goal:
        parts.append(f'goal={primary_goal}')
    parts.append(f'reason={round_reason}')
    if runtime_status:
        parts.append(f'runtime={runtime_status}')
    if review_summary:
        parts.append(f'review={review_summary[:120]}')
    if blocked_points:
        parts.append(f'blocked={blocked_points[0]}')
    parts.append(f'next={next_step}')
    parts.append(f"round={int(state.get('rework_count', 0) or 0) + 1}")
    return ' | '.join(parts)


def _looks_like_core_file(item: str) -> bool:
    lowered = item.lower().strip()
    return ('/' in lowered) or ('\\' in lowered) or ('.' in Path(lowered).name)



def _estimate_core_file_count(scope: list[str]) -> int:
    return sum(1 for item in scope if _looks_like_core_file(item))



def _estimate_action_count(goal: str, scope: list[str], expected_artifacts: list[str]) -> int:
    text = ' '.join([goal, *scope, *expected_artifacts]).lower()
    hits = sum(1 for token in _STANDARD_ACTION_HINTS if token in text)
    return max(1, min(len(_STANDARD_ACTION_HINTS), hits or 1))



def _infer_project_size_band(core_file_count: int, action_count: int) -> str:
    if core_file_count > 8 or action_count > 4:
        return 'L'
    if core_file_count > _STANDARD_TASK_UNIT_V1_LIMITS['max_core_files'] or action_count > _STANDARD_TASK_UNIT_V1_LIMITS['max_action_types']:
        return 'M'
    return 'S'



def _default_expected_artifacts(task_kind: str) -> list[str]:
    artifacts = [
        'Structured build plan',
        'Execution runtime plan',
        'Execution report',
        'Validation checklist',
    ]
    if task_kind == 'system_optimization':
        artifacts.append('Release review evidence')
    return artifacts



def _default_execution_scope(task_kind: str) -> dict[str, object]:
    scope = dict(_STANDARD_TASK_UNIT_V1_LIMITS)
    scope['task_kind'] = task_kind
    scope['allowed_action_types'] = ['write_file', 'edit_file', 'run_tests', 'run_lint']
    if task_kind == 'system_optimization':
        scope['allowed_action_types'].append('install_dep')
    return scope



def _default_constraints(*, task_kind: str, project_policy: dict[str, object], governance_contract: dict[str, object], orchestrator_design: dict[str, object]) -> list[str]:
    constraints: list[str] = []
    if not project_policy.get('allow_doctrine_modification_without_approval', False):
        constraints.append('Doctrine surfaces stay protected unless explicitly approved.')
    if not project_policy.get('allow_external_runtime_write', False):
        constraints.append('Writes stay project-local and bounded to the active runtime root.')
    contract_subtype = str(governance_contract.get('contract_subtype', '')).strip()
    if contract_subtype:
        constraints.append(f'Governance contract subtype: {contract_subtype}.')
    constraints.extend(_clean_text_list(orchestrator_design.get('boundary_notes', [])))
    if task_kind == 'system_optimization':
        constraints.append('Optimization tasks require stricter release review and rollback readiness.')
    return list(dict.fromkeys(constraints))



def _risk_level_from_context(goal: str, governance_weight: str) -> str:
    goal_lower = goal.lower()
    if governance_weight == 'high' or any(word in goal_lower for word in risk_keywords()):
        return 'high'
    return 'medium'



def _build_standard_task_card(
    *,
    state: KernelState,
    task_kind: str,
    task_policy: dict[str, object],
    project_policy: dict[str, object],
    governance_contract: dict[str, object],
    orchestrator_design: dict[str, object],
) -> tuple[TaskCard, dict[str, object], str]:
    intake_payload = dict(state.get('artifacts', {}).get('input_task_payload', {}) or {})
    modeled_task = dict(intake_payload.get('modeled_task', {}) or {})
    standard_task_unit = dict(intake_payload.get('standard_task_unit', {}) or {})
    raw_intent = str(intake_payload.get('raw_intent') or state.get('goal', '')).strip() or str(state.get('goal', '')).strip()
    working_goal = str(standard_task_unit.get('goal') or modeled_task.get('goal') or state.get('goal', '')).strip() or str(state.get('goal', '')).strip()

    intake_source = 'goal_only'
    if modeled_task:
        intake_source = 'modeled_task'
    if standard_task_unit:
        intake_source = 'standard_task_unit'

    explicit_scope = _merge_unique_lists(standard_task_unit.get('scope', []), modeled_task.get('scope', []))
    explicit_out_of_scope = _merge_unique_lists(standard_task_unit.get('out_of_scope', []), modeled_task.get('out_of_scope', []))
    explicit_expected_artifacts = _merge_unique_lists(standard_task_unit.get('expected_artifacts', []), modeled_task.get('expected_artifacts', []))
    explicit_acceptance = _merge_unique_lists(standard_task_unit.get('acceptance_criteria', []), modeled_task.get('acceptance_criteria', []))
    explicit_constraints = _merge_unique_lists(standard_task_unit.get('constraints', []), modeled_task.get('constraints', []))

    scope = _merge_unique_lists(
        explicit_scope,
        orchestrator_design.get('scope_additions', []),
    )
    if not scope:
        scope = _merge_unique_lists(task_policy['default_scope'])
    out_of_scope = _merge_unique_lists(
        explicit_out_of_scope,
        orchestrator_design.get('boundary_notes', []),
    )
    expected_artifacts = _merge_unique_lists(
        explicit_expected_artifacts,
        _default_expected_artifacts(task_kind),
    )
    acceptance_criteria = _merge_unique_lists(
        explicit_acceptance,
        orchestrator_design.get('acceptance_additions', []),
    )
    if not acceptance_criteria:
        acceptance_criteria = _merge_unique_lists(task_policy['default_acceptance_criteria'])
    constraints = _merge_unique_lists(
        explicit_constraints,
        _default_constraints(
            task_kind=task_kind,
            project_policy=project_policy,
            governance_contract=governance_contract,
            orchestrator_design=orchestrator_design,
        ),
    )

    execution_scope = _default_execution_scope(task_kind)
    modeled_execution_scope_raw = modeled_task.get('execution_scope', {}) or {}
    standard_execution_scope_raw = standard_task_unit.get('execution_scope', {}) or {}
    modeled_execution_scope = dict(modeled_execution_scope_raw) if isinstance(modeled_execution_scope_raw, dict) else ({'mode': str(modeled_execution_scope_raw)} if modeled_execution_scope_raw else {})
    standard_execution_scope = dict(standard_execution_scope_raw) if isinstance(standard_execution_scope_raw, dict) else ({'mode': str(standard_execution_scope_raw)} if standard_execution_scope_raw else {})
    execution_scope.update(modeled_execution_scope)
    execution_scope.update(standard_execution_scope)

    core_file_count = _estimate_core_file_count(scope)
    action_count = int(standard_execution_scope.get('estimated_action_count') or modeled_execution_scope.get('estimated_action_count') or 0)
    if action_count <= 0:
        action_count = _estimate_action_count(working_goal, scope, expected_artifacts)
    execution_scope['estimated_core_file_count'] = core_file_count
    execution_scope['estimated_action_count'] = action_count

    project_size_band = str(standard_task_unit.get('project_size_band') or modeled_task.get('project_size_band') or _infer_project_size_band(core_file_count, action_count)).strip() or 'S'
    if project_size_band not in {'S', 'M', 'L'}:
        project_size_band = _infer_project_size_band(core_file_count, action_count)

    risk_level = str(standard_task_unit.get('risk_level') or _risk_level_from_context(working_goal, str(orchestrator_design.get('governance_weight', 'routine')))).strip() or 'medium'
    if risk_level not in {'low', 'medium', 'high'}:
        risk_level = _risk_level_from_context(working_goal, str(orchestrator_design.get('governance_weight', 'routine')))

    requires_approval = bool(standard_task_unit.get('requires_approval', modeled_task.get('requires_approval', orchestrator_design.get('approval_hint', False))))

    max_core_files = int(execution_scope.get('max_core_files', _STANDARD_TASK_UNIT_V1_LIMITS['max_core_files']) or _STANDARD_TASK_UNIT_V1_LIMITS['max_core_files'])
    max_action_types = int(execution_scope.get('max_action_types', _STANDARD_TASK_UNIT_V1_LIMITS['max_action_types']) or _STANDARD_TASK_UNIT_V1_LIMITS['max_action_types'])
    within_standard_unit = core_file_count <= max_core_files and action_count <= max_action_types and project_size_band != 'L'

    upper_bound_trigger_dimensions: list[str] = []
    if core_file_count > max_core_files:
        upper_bound_trigger_dimensions.append('core_files_exceeded')
    elif core_file_count >= max(1, max_core_files - 1):
        upper_bound_trigger_dimensions.append('core_files_near_limit')
    if action_count > max_action_types:
        upper_bound_trigger_dimensions.append('action_types_exceeded')
    elif action_count >= max(1, max_action_types - 1):
        upper_bound_trigger_dimensions.append('action_types_near_limit')
    if project_size_band == 'L':
        upper_bound_trigger_dimensions.append('project_size_band_exceeded')
    elif project_size_band == 'M':
        upper_bound_trigger_dimensions.append('project_size_band_near_limit')

    missing_fields: list[str] = []
    if not explicit_scope:
        missing_fields.append('scope')
    if not explicit_out_of_scope:
        missing_fields.append('out_of_scope')
    if not explicit_expected_artifacts:
        missing_fields.append('expected_artifacts')
    if not explicit_acceptance:
        missing_fields.append('acceptance_criteria')
    if not (standard_task_unit.get('execution_scope') or modeled_task.get('execution_scope')):
        missing_fields.append('execution_scope')

    if missing_fields:
        upper_bound_trigger_dimensions.append('structured_fields_missing')
    if risk_level == 'high':
        upper_bound_trigger_dimensions.append('high_risk_task')

    near_upper_bound = within_standard_unit and any(item.endswith('near_limit') for item in upper_bound_trigger_dimensions)
    over_upper_bound = not within_standard_unit
    must_split = over_upper_bound

    task_card: TaskCard = {
        'task_id': f"{state['project_id']}-001",
        'goal': working_goal,
        'task_profile': str(orchestrator_design.get('task_profile', 'routine') or 'routine'),
        'scope_hint': scope,
        'constraints': constraints,
        'acceptance_tests': acceptance_criteria,
        'risk_level': risk_level,
        'assigned_agents': list(task_policy['default_assigned_agents']),
        'orchestrator_brief': _clip_text(orchestrator_design.get('builder_brief', '') or 'Keep scope minimal and concrete.', limit=TASK_AND_HANDOFF_LIMITS['orchestrator_brief_chars']),
        'scope': scope,
        'forbidden_changes': list(forbidden_changes()),
        'acceptance_criteria': acceptance_criteria,
        'rollback_plan': str(task_policy['rollback_plan']),
        'memory_update_requirement': str(task_policy['memory_update_requirement']),
        'out_of_scope': out_of_scope,
        'expected_artifacts': expected_artifacts,
        'task_kind': task_kind,
        'project_size_band': project_size_band,
        'requires_approval': requires_approval,
        'execution_scope': execution_scope,
        'raw_intent': raw_intent,
        'intake_source': intake_source,
        'run_id': str(intake_payload.get('run_id', '') or ''),
        'sample_id': str(intake_payload.get('sample_id', '') or ''),
        'executor_id': str(intake_payload.get('executor_id', '') or ''),
        'started_at': str(intake_payload.get('started_at', '') or ''),
    }

    intake_assessment = {
        'input_source': intake_source,
        'raw_intent': raw_intent,
        'working_goal': working_goal,
        'modeled_task_present': bool(modeled_task),
        'standard_task_unit_present': bool(standard_task_unit),
        'estimated_core_file_count': core_file_count,
        'estimated_action_count': action_count,
        'project_size_band': project_size_band,
        'risk_level': risk_level,
        'requires_approval': requires_approval,
        'max_core_files': max_core_files,
        'max_action_types': max_action_types,
        'size_within_standard_task_unit_v1': within_standard_unit,
        'near_upper_bound': near_upper_bound,
        'over_upper_bound': over_upper_bound,
        'should_split': must_split,
        'must_split': must_split,
        'upper_bound_trigger_dimensions': upper_bound_trigger_dimensions,
        'missing_structured_fields': missing_fields,
        'summary': (
            'Structured intake is still within standard_task_unit_v1 but is approaching the current upper bound.'
            if near_upper_bound
            else 'Structured intake accepted within standard_task_unit_v1.'
            if within_standard_unit
            else 'Task looks larger than standard_task_unit_v1 and should be modeled or split further.'
        ),
    }
    return task_card, intake_assessment, working_goal


def create_task_card(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="orchestrator_task_card", agent="orchestrator", phase="task_card_created")
    project_id = state.get("project_id", f"default-project-{int(__import__('time').time())}")
    task_kind = "system_optimization" if project_id.startswith("sysopt_") else "standard"
    task_policy = optimization_task_constraints() if task_kind == "system_optimization" else task_constraints()
    project_policy = project_constraints()
    optimization_policy = optimization_project_constraints()

    intake_payload = dict(state.get("artifacts", {}).get("input_task_payload", {}) or {})
    modeled_task = dict(intake_payload.get("modeled_task", {}) or {})
    standard_task_unit = dict(intake_payload.get("standard_task_unit", {}) or {})
    working_goal = str(standard_task_unit.get("goal") or modeled_task.get("goal") or state["goal"]).strip() or state["goal"]

    _update_role_working_memory(
        'orchestrator',
        task_id=str(((state.get('task_card', {}) or {}).get('task_id', '') or f'{project_id}-orchestrator')),
        goal=working_goal,
        status='in_progress',
        facts=[f'task_kind={task_kind}', f'project_id={project_id}'],
        decisions=[],
        progress='正在轻量 scout 真实现场，并判断应该出 task_card、clarification_request、split_request 或 escalation_request。',
    )

    orchestrator_memory_scope = build_memory_scope_bundle(
        state,
        role='orchestrator',
        query_text=working_goal,
        include_project=False,
        mode='default',
    )
    orchestrator_scene_scan = _build_orchestrator_scene_scan(state, working_goal)
    orchestrator_events_ref = {"value": _process_events_state(state)}
    orchestrator_stream_callback = _make_llm_stream_callback(
        state,
        node="orchestrator",
        phase="task_card_created",
        agent="orchestrator",
        process_events_ref=orchestrator_events_ref,
    )
    orchestrator_workspace_root = str(get_agent_settings("orchestrator").workspace_root or "").strip()
    orchestrator_diff_before = _git_diff_stat_for_workspace(orchestrator_workspace_root)
    orchestrator_design = get_orchestrator_task_design(
        project_id=project_id,
        goal=working_goal,
        task_kind=task_kind,
        default_scope=list(task_policy["default_scope"]),
        default_acceptance_criteria=list(task_policy["default_acceptance_criteria"]),
        default_assigned_agents=list(task_policy["default_assigned_agents"]),
        mother_memory_context=str(orchestrator_memory_scope.get("combined_context", "") or ""),
        scene_scan_excerpt=orchestrator_scene_scan,
        stream_event_callback=orchestrator_stream_callback,
    )
    orchestrator_diff_after = _git_diff_stat_for_workspace(orchestrator_workspace_root)
    orchestrator_readonly_violation = _readonly_workspace_violation("orchestrator", orchestrator_diff_before, orchestrator_diff_after)
    if orchestrator_readonly_violation:
        return _finalize_node_failure(
            state,
            node_id="orchestrator_task_card",
            agent="orchestrator",
            phase="task_card_created",
            started_at_perf=started_at_perf,
            error_summary="Orchestrator violated shared workspace read-only policy.",
            extra_artifacts={"orchestrator_readonly_violation": orchestrator_readonly_violation},
        )
    orchestrator_response_type = str(orchestrator_design.get("type", "") or "").strip().lower()
    if orchestrator_response_type in {"clarification_request", "split_request", "escalation_request"}:
        response_reason = str(orchestrator_design.get("reason", "") or orchestrator_design.get("ambiguity", "") or orchestrator_response_type).strip()
        _update_role_working_memory(
            'orchestrator',
            task_id=str(((state.get('task_card', {}) or {}).get('task_id', '') or f'{project_id}-orchestrator')),
            goal=working_goal,
            status='blocked',
            facts=[f'response_type={orchestrator_response_type}', f'task_kind={task_kind}'],
            decisions=[response_reason],
            progress='Orchestrator 未继续出卡，已通过结构化回执把问题回吐到上游或人工。',
        )
        next_state = {
            **state,
            "active_phase": "orchestrator_returned",
            "active_agent": "orchestrator",
            "blocking_issue": response_reason,
            "artifacts": {
                **state["artifacts"],
                "orchestrator_scene_scan": orchestrator_scene_scan,
                "orchestrator_analysis": orchestrator_design,
                "orchestrator_response": orchestrator_design,
                "process_events": orchestrator_events_ref["value"],
            },
            "steps": [*state["steps"], "orchestrator_return"],
        }
        return _finalize_node(
            state,
            next_state,
            node_id="orchestrator_task_card",
            agent="orchestrator",
            phase="task_card_created",
            started_at_perf=started_at_perf,
        )

    guarded_profile, fallback_applied, fallback_reason, classification_confidence = _guarded_task_profile(
        working_goal,
        task_kind,
        orchestrator_design,
    )
    orchestrator_design["task_profile"] = guarded_profile
    governance_contract = build_governance_contract(
        goal=working_goal,
        task_profile=guarded_profile,
    )

    task_card, intake_assessment, normalized_goal = _build_standard_task_card(
        state={**state, "project_id": project_id, "goal": working_goal},
        task_kind=task_kind,
        task_policy=task_policy,
        project_policy=project_policy,
        governance_contract=governance_contract,
        orchestrator_design=orchestrator_design,
    )
    task_card_contract = _validate_task_card_contract(task_card)
    paths = _project_paths(project_id)
    dynamic_triggers = _dynamic_trigger_payload(
        task_kind=task_kind,
        task_profile=guarded_profile,
        fallback_applied=fallback_applied,
        fallback_reason=fallback_reason,
        classification_confidence=classification_confidence,
    )

    _update_role_working_memory(
        'orchestrator',
        task_id=str(task_card.get('task_id', '') or f'{project_id}-orchestrator'),
        goal=normalized_goal,
        status='completed',
        facts=[f'task_profile={guarded_profile}', f'risk_level={task_card.get("risk_level", "")}', *[f'scope={item}' for item in _safe_list_preview(task_card.get('scope', []), limit=3)]],
        decisions=[str(orchestrator_design.get('builder_brief', '') or '').strip()],
        progress='Task card 已生成，等待 Builder 接手。',
    )

    next_state = {
        **state,
        "goal": normalized_goal,
        "task_kind": task_kind,
        "active_phase": "task_card_created",
        "active_agent": "orchestrator",
        "blocking_issue": "",
        "task_card": task_card,
        "artifacts": {
            **state["artifacts"],
            "paths": paths,
            "memory_scope_bundle": _compact_memory_scope_bundle(orchestrator_memory_scope),
            "orchestrator_scene_scan": orchestrator_scene_scan,
            "orchestrator_analysis": orchestrator_design,
            "governance_contract": governance_contract,
            "intake_assessment": intake_assessment,
            "task_card_contract": task_card_contract,
            "governance": {
                "project_constraints": project_policy,
                "optimization_project_constraints": optimization_policy,
                "task_kind": task_kind,
                "task_profile": guarded_profile,
                "classification_confidence": classification_confidence,
                "governance_weight": orchestrator_design.get("governance_weight", "routine"),
                "contract_subtype": governance_contract.get("contract_subtype", ""),
                "approval_hint": orchestrator_design.get("approval_hint", False),
                "boundary_notes": orchestrator_design.get("boundary_notes", []),
                "standard_task_unit_limits": dict(_STANDARD_TASK_UNIT_V1_LIMITS),
            },
            "dynamic_triggers": dynamic_triggers,
            "orchestrator_readonly_violation": orchestrator_readonly_violation,
            "human_visibility": _human_visibility_update(
                state,
                stage="orchestrator",
                summary="Task route and trigger plan established.",
                recommendation="Inspect downgraded paths when confidence is low or governance family is non-routine.",
            ),
            "builder_working_state": _initialize_builder_working_state(normalized_goal, task_card),
            "process_events": orchestrator_events_ref["value"],
        },
        "steps": [*state["steps"], "create_task_card"],
    }
    append_markdown(
        CONTROL_TOWER_PATH.with_name("task_history.md"),
        "\n".join(
            [
                f"## {task_card['task_id']}",
                f"- goal: {task_card['goal']}",
                f"- created_at: {now_iso()}",
            ]
        ),
    )
    append_markdown(
        Path(paths["backlog"]),
        "\n".join(
            [
                f"# {task_card['task_id']}",
                f"Goal: {task_card['goal']}",
                f"Raw intent: {task_card.get('raw_intent', task_card['goal'])}",
                "Status: created",
                f"Governance weight: {orchestrator_design.get('governance_weight', 'routine')}",
                f"Intake source: {task_card.get('intake_source', 'goal_only')}",
                f"Project size band: {task_card.get('project_size_band', 'S')}",
                f"Builder brief: {orchestrator_design.get('builder_brief', 'Keep scope minimal and concrete.')}",
                f"Intake assessment: {intake_assessment.get('summary', '')}",
            ]
        ),
    )
    initialize_memory_indexes(project_id, task_card, paths)
    return _finalize_node(
        state,
        next_state,
        node_id="orchestrator_task_card",
        agent="orchestrator",
        phase="task_card_created",
        started_at_perf=started_at_perf,
    )


def builder_agent(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="builder_plan", agent="builder", phase="building")
    task_profile = str((state.get('task_card', {}) or {}).get('task_profile', 'routine') or 'routine').strip().lower()
    builder_task_card = dict(state.get('task_card', {}) or {})
    _update_role_working_memory(
        'builder',
        task_id=str(builder_task_card.get('task_id', '') or state.get('project_id', 'builder-task')),
        goal=str(state.get('goal', '') or ''),
        status='in_progress',
        facts=[f'task_profile={task_profile}', *[f'scope={item}' for item in _safe_list_preview(builder_task_card.get('scope', []), limit=3)]],
        decisions=[f'rework_count={int(state.get("rework_count", 0) or 0)}'],
        progress='Builder 正在 direct_execution 施工，并准备输出 BUILD_PLAN.json 施工记录。',
    )
    builder_memory_scope = build_memory_scope_bundle(
        state,
        role='builder',
        query_text=str(state['goal']),
        include_mother=(task_profile != 'routine'),
        include_project=(task_profile != 'routine'),
        mode='builder',
    )
    orchestrator_guidance = _format_orchestrator_guidance(state)
    builder_working_state = _format_builder_working_state_for_prompt(state)
    task_card_contract = _validate_task_card_contract(dict(state.get('task_card', {}) or {}))
    builder_input_bundle = _build_builder_input_bundle(
        state,
        project_memory_context=str((((builder_memory_scope.get('layers', {}) or {}).get('project_memory', {}) or {}).get('excerpt', '')) or ''),
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
    )
    builder_events = _process_events_state(state)
    if state.get("scan_result"):
        builder_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": builder_events}},
            node="builder",
            target="scan_result",
            summary="Builder loaded context summary.",
        )
    if state.get("review_feedback"):
        builder_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": builder_events}},
            node="builder",
            target="review_feedback",
            summary="Builder loaded the latest review feedback.",
        )
    if builder_working_state.strip():
        builder_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": builder_events}},
            node="builder",
            target="builder_working_state",
            summary="Builder loaded the current working state.",
        )
    if str(builder_memory_scope.get("combined_context", "") or ""):
        builder_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": builder_events}},
            node="builder",
            target="memory_scope_bundle",
            summary="Builder loaded layered memory scope bundle.",
        )
    context_summary = _builder_round_context_summary(state)
    if context_summary:
        builder_events = _append_process_event(
            {**state, "artifacts": {"process_events": builder_events}},
            node="builder",
            event_type="agent_round_context",
            status="ready",
            summary=context_summary,
            target=str(state.get("goal", "") or ""),
            metadata={
                "rework_count": int(state.get("rework_count", 0) or 0),
                "has_review_feedback": bool(str(state.get("review_feedback", "") or "").strip()),
            },
        )
    builder_events = emit_step_event(
        {**state, "artifacts": {**state.get("artifacts", {}), "process_events": builder_events}},
        node="builder",
        event_type="builder_input_bundle_ready",
        status="ready",
        summary=f"Builder input bundle ready | task_card_ok={task_card_contract.get('passed', False)}",
        target=str(state.get("goal", "") or ""),
        metadata={
            "task_card_contract": task_card_contract,
            "builder_input_bundle_schema": str(builder_input_bundle.get("schema_version", "")),
            "truncation_flags": dict(builder_input_bundle.get("truncation_flags", {}) or {}),
        },
    )
    if not bool(task_card_contract.get('passed', False)):
        contract_error = _task_card_contract_error_summary(task_card_contract)
        _finalize_node_failure(
            state,
            node_id="builder_plan",
            agent="builder",
            phase="building",
            started_at_perf=started_at_perf,
            error_summary=contract_error,
            extra_artifacts={
                "memory_scope_bundle": _compact_memory_scope_bundle(builder_memory_scope),
                "process_events": builder_events,
                "task_card_contract": task_card_contract,
                "builder_input_bundle": builder_input_bundle,
            },
        )
        raise BuilderPlanCallError(contract_error, diagnostics={
            "role": "builder",
            "mode": "contract_guard",
            "task_card_contract": task_card_contract,
            "builder_input_bundle": builder_input_bundle,
        })
    state = _flush_live_progress(
        {
            **state,
            "artifacts": {
                **state.get("artifacts", {}),
                "process_events": builder_events,
            },
        },
        phase="building",
        agent="builder",
    )
    builder_events_ref = {"value": builder_events}
    builder_stream_callback = _make_llm_stream_callback(
        state,
        node="builder",
        phase="building",
        agent="builder",
        process_events_ref=builder_events_ref,
    )
    try:
        build_result, builder_call_diagnostics = get_builder_plan_with_backend_diagnostics(
            goal=state["goal"],
            task_card=dict(builder_input_bundle.get("task_card", {}) or state["task_card"]),
            scan_result=str(builder_input_bundle.get("scan_result", "") or state["scan_result"]),
            project_memory_context=str(builder_input_bundle.get("project_memory_excerpt", "") or (((builder_memory_scope.get("layers", {}) or {}).get("project_memory", {}) or {}).get("excerpt", ""))),
            orchestrator_guidance=str(builder_input_bundle.get("orchestrator_brief", "") or orchestrator_guidance),
            builder_working_state=str(builder_input_bundle.get("builder_working_state", "") or builder_working_state),
            review_feedback=str(builder_input_bundle.get("review_feedback", "") or state["review_feedback"]),
            rework_count=state["rework_count"],
            stream_event_callback=builder_stream_callback,
        )
    except BuilderPlanCallError as exc:
        _finalize_node_failure(
            state,
            node_id="builder_plan",
            agent="builder",
            phase="building",
            started_at_perf=started_at_perf,
            error_summary=f"Builder live call failed: {str(exc)}",
            extra_artifacts={
                "memory_scope_bundle": _compact_memory_scope_bundle(builder_memory_scope),
                "builder_call_diagnostics": exc.diagnostics,
                "process_events": builder_events_ref["value"],
            },
        )
        raise BuilderPlanCallError(str(exc), diagnostics=dict(exc.diagnostics)) from exc
    builder_events = dict(builder_events_ref.get("value", builder_events) or builder_events)
    build_result = _normalize_build_plan_against_contract(
        build_result,
        state.get("artifacts", {}).get("governance_contract", {}) or {},
    )
    build_plan_payload = _extract_build_plan_payload(build_result)
    if build_plan_payload:
        build_result = json.dumps(
            _repair_direct_execution_record(
                _normalize_change_plan_payload(build_plan_payload),
                builder_task_card,
            ),
            ensure_ascii=False,
            indent=2,
        )
        build_plan_payload = _extract_build_plan_payload(build_result)
    builder_status = _builder_output_status(build_plan_payload, builder_task_card) if build_plan_payload else 'completed'
    _update_role_working_memory(
        'builder',
        task_id=str(builder_task_card.get('task_id', '') or state.get('project_id', 'builder-task')),
        goal=str(state.get('goal', '') or ''),
        status=builder_status,
        facts=[f'task_profile={task_profile}'],
        decisions=[f'build_result_status={builder_status}'],
        progress='Builder has produced the current round output and is waiting for downstream handling.',
    )
    if builder_status == 'blocked' and not str(state.get("blocking_issue", "") or "").strip():
        checkpoint_payload = (build_plan_payload.get("checkpoint") or {}) if isinstance(build_plan_payload, dict) else {}
        escalation_payload = (build_plan_payload.get("escalation_request") or {}) if isinstance(build_plan_payload, dict) else {}
        checkpoint_reason = ''
        if isinstance(checkpoint_payload, dict):
            checkpoint_reason = str(
                checkpoint_payload.get("reason")
                or checkpoint_payload.get("why_now")
                or checkpoint_payload.get("question")
                or ''
            ).strip()
        escalation_reason = ''
        if isinstance(escalation_payload, dict):
            escalation_reason = str(escalation_payload.get("reason") or escalation_payload.get("decision_required") or '').strip()
        blocking_issue = checkpoint_reason or escalation_reason or "Builder reached a decision boundary and should checkpoint before continuing."
    else:
        blocking_issue = ""
    next_state = {
        **state,
        "active_phase": "building",
        "active_agent": "builder",
        "build_result": build_result,
        "blocking_issue": blocking_issue,
        "artifacts": {
            **state["artifacts"],
            "memory_scope_bundle": builder_memory_scope,
            "builder_backend": get_builder_backend_name(),
            "builder_call_diagnostics": builder_call_diagnostics,
            "task_card_contract": task_card_contract,
            "builder_input_bundle": builder_input_bundle,
            "builder_working_state": _align_builder_working_state(_update_builder_working_state_from_builder(state, build_result), builder_status=builder_status, blocking_issue=blocking_issue),
        },
        "builder_working_state": _align_builder_working_state(_update_builder_working_state_from_builder(state, build_result), builder_status=builder_status, blocking_issue=blocking_issue),
        "steps": [*state["steps"], "builder_agent"],
    }
    return _finalize_node(
        state,
        next_state,
        node_id="builder_plan",
        agent="builder",
        phase="building",
        started_at_perf=started_at_perf,
    )


def _execution_self_repair_reasons_from_state(state: KernelState) -> list[str]:
    artifacts = dict(state.get("artifacts", {}) or {})
    execution_evidence = dict(artifacts.get("execution_evidence", {}) or {})
    execution_runtime = dict(artifacts.get("execution_runtime", {}) or {})
    warnings = [str(item) for item in list(execution_evidence.get("warnings", []) or [])]
    warnings_text = " ".join(warnings).lower()
    reasons: list[str] = []
    runtime_status = str(execution_evidence.get("runtime_status") or execution_runtime.get("status") or "").strip().lower()
    if runtime_status and runtime_status not in {"passed", "", "not_needed", "approved", "success"}:
        reasons.append(f"runtime_status={runtime_status}")
    if str(execution_evidence.get("git_diff_status", "") or "").strip().lower() == "failed":
        reasons.append("git_diff_failed")
    if str(execution_evidence.get("compile_status", "") or "").strip().lower() == "failed":
        reasons.append("compile_failed")
    if str(execution_evidence.get("pytest_status", "") or "").strip().lower() == "failed":
        reasons.append("pytest_failed")
    if "target text not found" in warnings_text or "source edit failed" in warnings_text:
        reasons.append("grounding_failed")
    contract_status = str(execution_evidence.get("build_plan_contract_status", "") or "").strip().lower()
    if contract_status == "failed":
        reasons.append("build_plan_invalid")
    changed_files = list(execution_evidence.get("changed_files", []) or [])
    if not changed_files:
        reasons.append("no_changed_files")
    return list(dict.fromkeys(reasons))


def _review_required_after_clean_execution_state(state: KernelState) -> list[str]:
    reasons: list[str] = []
    if bool(state.get("approval_required", False)):
        reasons.append("approval_required")
    task_profile = str(((state.get("artifacts", {}) or {}).get("orchestrator_analysis", {}) or {}).get("task_profile", "routine") or "routine").strip().lower()
    if task_profile and task_profile != "routine":
        reasons.append(f"task_profile={task_profile}")
    risk_level = str(state.get("risk_level", "") or "").strip().lower()
    if risk_level in {"high", "critical"}:
        reasons.append(f"risk_level={risk_level}")
    changed_files = list(dict(state.get("artifacts", {}) or {}).get("execution_evidence", {}).get("changed_files", []) or [])
    if len(changed_files) >= 4:
        reasons.append(f"changed_files={len(changed_files)}")
    return list(dict.fromkeys(reasons))


def reviewer_agent(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="reviewer_gate", agent="reviewer", phase="reviewing")
    reviewer_task_card = dict(state.get('task_card', {}) or {})
    _update_role_working_memory(
        'reviewer',
        task_id=str(reviewer_task_card.get('task_id', '') or state.get('project_id', 'review-task')),
        goal=str(state.get('goal', '') or ''),
        status='in_progress',
        facts=[f'rework_count={int(state.get("rework_count", 0) or 0)}'],
        decisions=[],
        progress='Reviewer 正在收集证据并只读检查真实现场。',
    )
    intake_payload = dict(state.get("artifacts", {}).get("input_task_payload", {}) or {})
    target_workspace_root_raw = str(intake_payload.get("target_workspace_root", "") or "").strip()
    target_workspace_root = Path(target_workspace_root_raw).resolve() if target_workspace_root_raw else None
    reviewer_memory_scope: dict[str, object] = {}
    prior_feedback = ""
    reviewer_workspace_root = str(get_agent_settings("reviewer").workspace_root or "").strip()
    execution_evidence = dict(state.get("artifacts", {}).get("execution_evidence", {}) or {})
    handoff_packet = dict(state.get("artifacts", {}).get("reviewer_handoff_packet", {}) or {})
    patch_path_raw = str(handoff_packet.get("patch_path", "") or "").strip()
    review_source_workspace_root = str(handoff_packet.get("source_workspace_root", "") or "").strip()

    if not state.get("execution_result") or not execution_evidence or not handoff_packet:
        paths = dict(state.get("artifacts", {}).get("paths", {}) or {})
        execution_report = run_controlled_execution(
            project_id=state["project_id"],
            task_id=state["task_card"]["task_id"],
            project_root=Path(paths["project_root"]),
            memory_paths=paths,
            build_result=state.get("build_result", ""),
            target_workspace_root=target_workspace_root,
        )
        report_artifact_path = next((Path(str(item)).resolve() for item in list(execution_report.get("artifacts", []) or []) if Path(str(item)).name == "execution_report.json"), None)
        if report_artifact_path and report_artifact_path.exists():
            try:
                full_execution_report = json.loads(report_artifact_path.read_text(encoding="utf-8"))
                if isinstance(full_execution_report, dict):
                    merged_report = dict(execution_report)
                    merged_report["runtime"] = dict(full_execution_report.get("runtime", {}) or {})
                    merged_report["actions"] = list(full_execution_report.get("actions", []) or [])
                    execution_report = merged_report
            except Exception:
                pass
        artifact_names = ", ".join(Path(item).name for item in list(execution_report.get("artifacts", []) or []))
        failed_count = int(execution_report.get("failed_count", 0) or 0)
        success_count = int(execution_report.get("success_count", 0) or 0)
        total_actions = int(execution_report.get("total_actions", 0) or 0)
        execution_status = str(execution_report.get("status", "passed") or "passed").strip().lower()
        execution_result = (
            f"Controlled verification finished with issues. {success_count}/{total_actions} actions succeeded, {failed_count} failed. Artifacts: {artifact_names}. Summary: {execution_report['summary']}"
            if execution_status == "failed" or failed_count > 0
            else f"Controlled verification completed. {success_count}/{total_actions} actions succeeded. Artifacts: {artifact_names}. Summary: {execution_report['summary']}"
        )
        execution_evidence = _execution_evidence_snapshot({**state, "artifacts": {**state.get("artifacts", {}), "paths": paths}})
        diff_root = target_workspace_root or (Path(reviewer_workspace_root).resolve() if reviewer_workspace_root else None)
        patch_path = ""
        if diff_root:
            patch_evidence = collect_git_diff_evidence(allowed_root=diff_root, diff_mode="full")
            patch_text = str(patch_evidence.get("stdout", "") or "")
            if patch_text.strip():
                patch_path = str(persist_reviewer_patch_artifact(project_root=Path(paths['project_root']), patch_text=patch_text))
        handoff_packet = build_reviewer_handoff_packet(
            state=state,
            execution_report=execution_report,
            execution_evidence=execution_evidence,
            target_workspace_root=reviewer_workspace_root,
            source_workspace_root=str(target_workspace_root) if target_workspace_root else reviewer_workspace_root,
            patch_path=patch_path,
        )
        reviewer_handoff_path = persist_reviewer_handoff_packet(project_root=Path(paths['project_root']), payload=handoff_packet)
        handoff_packet = {**handoff_packet, "packet_path": str(reviewer_handoff_path)}
        patch_path_raw = patch_path
        review_source_workspace_root = str(handoff_packet.get("source_workspace_root", "") or reviewer_workspace_root).strip()
        state = {
            **state,
            "execution_result": execution_result,
            "artifacts": {
                **state.get("artifacts", {}),
                "execution_evidence": execution_evidence,
                "reviewer_handoff_packet": handoff_packet,
                "builder_working_state": _update_builder_working_state_from_execution(state, execution_report, execution_result),
            },
        }
    reviewer_events = _process_events_state(state)
    if execution_evidence:
        reviewer_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": reviewer_events}},
            node="reviewer",
            target="execution_evidence",
            summary="Reviewer loaded execution evidence for stage review.",
            metadata={"keys": list(execution_evidence.keys())[:8]},
        )
    if handoff_packet:
        reviewer_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": reviewer_events}},
            node="reviewer",
            target="reviewer_handoff_packet",
            summary="Reviewer loaded the structured handoff packet.",
            metadata={"keys": list(handoff_packet.keys())[:8]},
        )
    if review_source_workspace_root:
        reviewer_events = _append_process_event(
            {**state, "artifacts": {"process_events": reviewer_events}},
            node="reviewer",
            event_type="shared_workspace_readonly",
            status="ready",
            summary="Reviewer will inspect the builder workspace as read-only ground truth instead of rebuilding a second scene.",
            target=review_source_workspace_root,
            metadata={
                "patch_path": patch_path_raw,
                "reviewer_workspace_root": reviewer_workspace_root,
            },
        )
    if review_source_workspace_root:
        execution_evidence = {**execution_evidence, "review_source_workspace_root": review_source_workspace_root, "review_workspace_mode": "shared_readonly"}
    context_summary = _reviewer_round_context_summary(state, execution_evidence, prior_feedback)
    if context_summary:
        reviewer_events = _append_process_event(
            {**state, "artifacts": {"process_events": reviewer_events}},
            node="reviewer",
            event_type="agent_round_context",
            status="ready",
            summary=context_summary,
            target=str(state.get("goal", "") or ""),
            metadata={
                "rework_count": int(state.get("rework_count", 0) or 0),
                "runtime_status": str(execution_evidence.get("runtime_status", "") or ""),
                "has_prior_feedback": bool(prior_feedback),
            },
        )
    state = _flush_live_progress(
        {
            **state,
            "artifacts": {
                **state.get("artifacts", {}),
                "process_events": reviewer_events,
                "reviewer_handoff_packet": handoff_packet,
            },
        },
        phase="reviewing",
        agent="reviewer",
    )
    reviewer_events_ref = {"value": reviewer_events}
    reviewer_stream_callback = _make_llm_stream_callback(
        state,
        node="reviewer",
        phase="reviewing",
        agent="reviewer",
        process_events_ref=reviewer_events_ref,
        artifact_overrides={
            "reviewer_handoff_packet": handoff_packet,
        },
    )
    reviewer_live_workspace_root = review_source_workspace_root or reviewer_workspace_root
    reviewer_diff_before = _git_diff_stat_for_workspace(reviewer_live_workspace_root)
    assessment = get_reviewer_assessment(
        goal=state["goal"],
        task_card=state["task_card"],
        scan_result="",
        execution_result=state["execution_result"],
        execution_evidence=execution_evidence,
        source_workspace_root=review_source_workspace_root or reviewer_workspace_root,
        rework_count=state["rework_count"],
        stream_event_callback=reviewer_stream_callback,
    )
    reviewer_diff_after = _git_diff_stat_for_workspace(reviewer_live_workspace_root)
    reviewer_readonly_violation = _readonly_workspace_violation("reviewer", reviewer_diff_before, reviewer_diff_after)
    if reviewer_readonly_violation:
        return _finalize_node_failure(
            state,
            node_id="reviewer_assessment",
            agent="reviewer",
            phase="reviewing",
            started_at_perf=started_at_perf,
            error_summary="Reviewer violated shared workspace read-only policy.",
            extra_artifacts={
                "reviewer_handoff_packet": handoff_packet,
                "reviewer_readonly_violation": reviewer_readonly_violation,
                "process_events": reviewer_events_ref.get("value", reviewer_events),
            },
        )
    reviewer_events = dict(reviewer_events_ref.get("value", reviewer_events) or reviewer_events)
    goal = state["goal"].lower()
    risk_level = "high" if any(word in goal for word in risk_keywords()) else "medium"
    model_decision = str(assessment.get("decision", "approved")).strip().lower()
    summary = str(assessment.get("summary", "Review completed.")).strip()
    feedback = str(assessment.get("feedback", "")).strip()
    issues = assessment.get("issues", []) or []
    validation_gaps = assessment.get("validation_gaps", []) or []
    escalation_payload = dict(assessment.get("escalation_request", {}) or {}) if isinstance(assessment.get("escalation_request", {}), dict) else {}
    should_escalate = model_decision == "escalate" or bool(escalation_payload)

    evidence_posture = _reviewer_evidence_posture(execution_evidence, assessment)
    evidence_gate_issues = [str(item) for item in list(evidence_posture.get('evidence_debt', []) or [])]
    evidence_hard_blockers = [str(item) for item in list(evidence_posture.get('hard_blockers', []) or [])]
    evidence_judgeable = bool(evidence_posture.get('judgeable', False))
    should_rework = model_decision == "changes_requested"
    if evidence_gate_issues:
        existing_issues = [str(item) for item in issues]
        existing_gaps = [str(item) for item in validation_gaps]
        for item in evidence_gate_issues:
            if item not in existing_issues:
                existing_issues.append(item)
            if item not in existing_gaps:
                existing_gaps.append(item)
        issues = existing_issues
        validation_gaps = existing_gaps
    if evidence_hard_blockers and not should_escalate:
        should_rework = True
        model_decision = "changes_requested"
        for item in evidence_hard_blockers:
            if item not in issues:
                issues.append(item)
            if item not in validation_gaps:
                validation_gaps.append(item)
        if not summary or "无法判断" not in summary:
            summary = "关键证据与现场锚点不足，当前无法可靠判断任务是否完成。"
        if not feedback or "判断" not in feedback:
            feedback = "请补齐最小可判定证据，或确保 Reviewer 可稳定读取真实现场后再复核。"

    reviewer_decision_summary = (
        f"Reviewer decision={model_decision or 'approved'} | issues={len(issues)} | validation_gaps={len(validation_gaps)} | risk={risk_level}"
    )
    reviewer_decision_metadata = {
        "model_decision": model_decision,
        "risk_level": risk_level,
        "issue_count": len(issues),
        "validation_gap_count": len(validation_gaps),
        "issues": [str(item) for item in issues[:5]],
        "validation_gaps": [str(item) for item in validation_gaps[:5]],
        "readonly_violation": reviewer_readonly_violation,
        "evidence_judgeable": evidence_judgeable,
        "evidence_debt_count": len(evidence_gate_issues),
        "evidence_hard_blocker_count": len(evidence_hard_blockers),
    }
    reviewer_events = emit_step_event(
        {**state, "artifacts": {**state.get("artifacts", {}), "process_events": reviewer_events}},
        node="reviewer",
        event_type="reviewer_decision",
        status="escalate" if should_escalate else ("changes_requested" if should_rework else "approved"),
        summary=reviewer_decision_summary,
        target=str(state.get("goal", "") or ""),
        metadata=reviewer_decision_metadata,
    )

    if should_escalate:
        escalation_reason = str(escalation_payload.get("reason", "") or summary or "Reviewer identified a boundary that requires human decision.").strip()
        decision_required = str(escalation_payload.get("decision_required", "") or feedback or "需要人工确认后再继续。")
        escalation_options = escalation_payload.get("options", []) if isinstance(escalation_payload.get("options", []), list) else []
        escalation_recommendation = str(escalation_payload.get("recommendation", "") or "建议人工确认后再继续。")
        current_state = str(escalation_payload.get("current_state", "") or summary)
        normalized_assessment = {
            **assessment,
            "model_decision": model_decision,
            "effective_decision": "escalation_request",
            "escalation_request": {
                "reason": escalation_reason,
                "decision_required": decision_required,
                "options": [str(item) for item in escalation_options if str(item).strip()],
                "recommendation": escalation_recommendation,
                "current_state": current_state,
            },
        }
        _update_role_working_memory(
            'reviewer',
            task_id=str(reviewer_task_card.get('task_id', '') or state.get('project_id', 'review-task')),
            goal=str(state.get('goal', '') or ''),
            status='blocked',
            facts=[f'risk_level={risk_level}', 'escalation_request=true'],
            decisions=[escalation_reason],
            progress='Reviewer 认为该问题超出返工范围，已升级给人工决策。',
        )
        next_state = {
            **state,
            "active_phase": "reviewing",
            "active_agent": "reviewer",
            "review_result": f"Review escalated for human decision. Summary: {summary}",
            "review_status": "approved",
            "review_feedback": escalation_reason,
            "validation_result": "Reviewer escalated for human decision. " + escalation_reason,
            "validation_status": "approved",
            "validation_feedback": escalation_reason,
            "risk_level": "high",
            "approval_required": True,
            "approval_status": "pending",
            "blocking_issue": escalation_reason,
            "artifacts": {
                **state["artifacts"],
                "process_events": reviewer_events,
                "memory_scope_bundle": reviewer_memory_scope,
                "review_assessment": normalized_assessment,
                "execution_evidence": execution_evidence,
                "reviewer_readonly_violation": reviewer_readonly_violation,
                "inspection_state": _inspection_state_update(
                    state,
                    stage="reviewer",
                    trigger_class="hard",
                    decision="escalation_request",
                    summary="Reviewer escalated the case for human decision.",
                    escalation_target="approval",
                ),
                "human_visibility": _human_visibility_update(
                    state,
                    stage="reviewer",
                    summary="Reviewer escalated a high-risk decision for human review.",
                    recommendation=decision_required,
                    level="required",
                    reasons=["reviewer_escalation_request"],
                ),
            },
            "steps": [*state["steps"], "reviewer_agent"],
        }
        next_state["artifacts"]["process_events"] = _append_state_transition_event(
            next_state,
            node="reviewer",
            event_type="escalation_entered",
            summary="Task entered human approval after reviewer escalation.",
            from_phase=str(state.get("active_phase", "")),
            to_phase="awaiting_human_approval",
            status="escalate",
            metadata={"risk_level": "high"},
        )
        _persist_routing_reassessment(next_state)
        return _finalize_node(
            state,
            next_state,
            node_id="reviewer_gate",
            agent="reviewer",
            phase="reviewing",
            started_at_perf=started_at_perf,
        )

    if should_rework and state["rework_count"] < state["max_rework_rounds"]:
        feedback_signal = classify_reviewer_feedback(
            decision="changes_requested",
            risk_level=risk_level,
            issues=[str(item) for item in issues],
            validation_gaps=[str(item) for item in validation_gaps],
        )
        review_feedback = feedback or "Reviewer requested one revision before approval. Strengthen implementation detail and validation coverage."
        details = []
        if issues:
            details.append("issues=" + "; ".join(str(item) for item in issues[:3]))
        if validation_gaps:
            details.append("validation_gaps=" + "; ".join(str(item) for item in validation_gaps[:3]))
        detail_text = f" {' | '.join(details)}" if details else ""
        review_result = (
            f"Review completed. Reviewer recommends rework before continuation. Pending human confirmation. Rework round {state['rework_count'] + 1} "
            f"of {state['max_rework_rounds']}. Summary: {summary}.{detail_text}"
        )
        normalized_assessment = {
            **assessment,
            "model_decision": model_decision,
            "effective_decision": "changes_requested",
            "evidence_judgeable": evidence_judgeable,
            "evidence_debt": [str(item) for item in evidence_gate_issues],
        }
        _update_role_working_memory(
            'reviewer',
            task_id=str(reviewer_task_card.get('task_id', '') or state.get('project_id', 'review-task')),
            goal=str(state.get('goal', '') or ''),
            status='blocked',
            facts=[f'risk_level={risk_level}', 'human_confirmation_required=true'],
            decisions=[review_feedback],
            progress='Reviewer 建议返工，已先上浮给人工确认轻重，再决定是否回 Builder。',
        )
        next_state = {
            **state,
            "active_phase": "awaiting_human_approval",
            "active_agent": "reviewer",
            "review_result": review_result,
            "review_status": "changes_requested",
            "review_feedback": review_feedback,
            "validation_result": "Reviewer validation requested rework. " + review_feedback,
            "validation_status": "changes_requested",
            "validation_feedback": review_feedback,
            "risk_level": risk_level,
            "approval_required": True,
            "approval_status": "pending",
            "blocking_issue": review_feedback,
            "artifacts": {
                **state["artifacts"],
                "process_events": reviewer_events,
                "memory_scope_bundle": reviewer_memory_scope,
                "review_assessment": normalized_assessment,
                "execution_evidence": execution_evidence,
                "reviewer_readonly_violation": reviewer_readonly_violation,
                "inspection_state": _inspection_state_update(
                    state,
                    stage="reviewer",
                    trigger_class="hard",
                    decision="changes_requested_pending_human_confirmation",
                    summary="Reviewer requested rework and is waiting for human confirmation before returning to Builder.",
                    escalation_target="approval",
                ),
                "human_visibility": _human_visibility_update(
                    state,
                    stage="reviewer",
                    summary="Reviewer recommends rework before continuation.",
                    recommendation="Confirm whether this should return to Builder, or allow it to proceed despite the current review findings.",
                    level="required",
                    reasons=["reviewer_requested_rework"],
                ),
                "side_feedback": {
                    **state["artifacts"].get("side_feedback", {}),
                    "reviewer": feedback_signal,
                },
                "review_governance_signal": {
                    "severity": "rework_recommended",
                    "judgeable": evidence_judgeable,
                    "evidence_debt": [str(item) for item in evidence_gate_issues[:5]],
                    "hard_blockers": [str(item) for item in evidence_hard_blockers[:5]],
                },
            },
            "rework_count": state["rework_count"] + 1,
            "steps": [*state["steps"], "reviewer_agent"],
        }
        next_state["artifacts"]["process_events"] = _append_state_transition_event(
            next_state,
            node="reviewer",
            event_type="rework_pending_human_confirmation",
            summary="Reviewer recommended rework and sent it to human confirmation before builder retry.",
            from_phase=str(state.get("active_phase", "")),
            to_phase="awaiting_human_approval",
            status="changes_requested",
            metadata={"rework_count": next_state.get("rework_count", 0)},
        )
        _persist_routing_reassessment(next_state)
        return _finalize_node(
            state,
            next_state,
            node_id="reviewer_gate",
            agent="reviewer",
            phase="awaiting_human_approval",
            started_at_perf=started_at_perf,
        )

    approval_required = requires_human_approval(risk_level, state.get("task_kind", "standard"))
    details = []
    if issues:
        details.append("issues=" + "; ".join(str(item) for item in issues[:3]))
    if validation_gaps:
        details.append("validation_gaps=" + "; ".join(str(item) for item in validation_gaps[:3]))
    detail_text = f" {' | '.join(details)}" if details else ""
    evidence_debt_text = ""
    if evidence_gate_issues and evidence_judgeable and not evidence_hard_blockers:
        evidence_debt_text = " evidence_debt=" + "; ".join(str(item) for item in evidence_gate_issues[:3])
    review_result = (
        f"Review completed. Risk level is {risk_level}. Summary: {summary}. "
        + ("Human approval required before recording." if approval_required else "No human approval gate required.")
        + detail_text
        + evidence_debt_text
    )
    feedback_signal = classify_reviewer_feedback(
        decision="approved",
        risk_level=risk_level,
        issues=[str(item) for item in issues],
        validation_gaps=[str(item) for item in validation_gaps],
    )
    normalized_assessment = {
        **assessment,
        "model_decision": model_decision,
        "effective_decision": "approved",
        "evidence_judgeable": evidence_judgeable,
        "evidence_debt": [str(item) for item in evidence_gate_issues],
    }
    _update_role_working_memory(
        'reviewer',
        task_id=str(reviewer_task_card.get('task_id', '') or state.get('project_id', 'review-task')),
        goal=str(state.get('goal', '') or ''),
        status='completed',
        facts=[f'risk_level={risk_level}', f'approval_required={approval_required}'],
        decisions=[summary],
        progress='Reviewer 已完成观察和判断，结果已写回主链。',
    )
    next_state = {
        **state,
        "active_phase": "reviewing",
        "active_agent": "reviewer",
        "review_result": review_result,
        "review_status": "approved",
        "review_feedback": "",
        "validation_result": "Reviewer validation passed. " + summary,
        "validation_status": "approved",
        "validation_feedback": "",
        "risk_level": risk_level,
        "approval_required": approval_required,
        "approval_status": "pending" if approval_required else "not_needed",
        "blocking_issue": "",
        "artifacts": {
            **state["artifacts"],
            "review_assessment": normalized_assessment,
            "execution_evidence": execution_evidence,
            "inspection_state": _inspection_state_update(
                state,
                stage="reviewer",
                trigger_class="hard" if approval_required else "soft",
                decision="approved_with_governance_signal" if approval_required else "approved",
                summary="Reviewer completed stage review.",
                escalation_target="approval" if approval_required else "recorder",
            ),
            "human_visibility": _human_visibility_update(
                state,
                stage="reviewer",
                summary="Reviewer completed stage review.",
                recommendation="Inspect this review if approval is required or if the task remains on a non-routine path.",
                level="required" if approval_required else "",
                reasons=["approval_required_after_review"] if approval_required else None,
            ),
            "side_feedback": {
                **state["artifacts"].get("side_feedback", {}),
                "reviewer": feedback_signal,
            },
            "review_governance_signal": {
                "severity": "warning" if (issues or validation_gaps) else "ok",
                "judgeable": evidence_judgeable,
                "evidence_debt": [str(item) for item in evidence_gate_issues[:5]],
                "hard_blockers": [str(item) for item in evidence_hard_blockers[:5]],
            },
        },
        "steps": [*state["steps"], "reviewer_agent"],
    }
    return _finalize_node(
        state,
        next_state,
        node_id="reviewer_gate",
        agent="reviewer",
        phase="reviewing",
        started_at_perf=started_at_perf,
    )


def validator_agent(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="validator_gate", agent="validator", phase="validating")
    validation_issues: list[str] = []
    evidence_gaps: list[str] = []
    acceptance_criteria = state["task_card"].get("acceptance_criteria", [])
    execution_evidence = dict(state.get("artifacts", {}).get("execution_evidence", {}) or _execution_evidence_snapshot(state))
    review_assessment = dict(state.get("artifacts", {}).get("review_assessment", {}) or {})
    intake_assessment = dict(state.get("artifacts", {}).get("intake_assessment", {}) or {})
    trigger_plan = dict(state.get("artifacts", {}).get("dynamic_triggers", {}) or {})
    validator_required = bool(trigger_plan.get("validator_required", True))
    periodic_reason = str(trigger_plan.get("validator_reason", ""))
    light_path_exit = bool(state.get("rework_count", 0) > 0 or periodic_reason == "light_path_exit_after_rework")
    strict_validation = validator_required
    validator_input_bundle = _build_validator_input_bundle(
        state,
        execution_evidence=execution_evidence,
        review_assessment=review_assessment,
        intake_assessment=intake_assessment,
        trigger_plan=trigger_plan,
    )
    validator_events = _process_events_state(state)
    if execution_evidence:
        validator_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": validator_events}},
            node="validator",
            target="execution_evidence",
            summary="Validator loaded execution evidence for stage validation.",
            metadata={"keys": list(execution_evidence.keys())[:8]},
        )
    if review_assessment:
        validator_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": validator_events}},
            node="validator",
            target="review_assessment",
            summary="Validator loaded reviewer assessment.",
        )
    if intake_assessment:
        validator_events = _append_artifact_read_event(
            {**state, "artifacts": {"process_events": validator_events}},
            node="validator",
            target="intake_assessment",
            summary="Validator loaded intake assessment.",
        )
    context_summary = _validator_round_context_summary(
        state,
        execution_evidence,
        review_assessment,
        intake_assessment,
        trigger_plan,
    )
    if context_summary:
        validator_events = _append_process_event(
            {**state, "artifacts": {"process_events": validator_events}},
            node="validator",
            event_type="agent_round_context",
            status="ready",
            summary=context_summary,
            target=str(state.get("goal", "") or ""),
            metadata={
                "rework_count": int(state.get("rework_count", 0) or 0),
                "validator_required": validator_required,
                "periodic_reason": periodic_reason,
            },
        )
    validator_events = emit_step_event(
        {**state, "artifacts": {**state.get("artifacts", {}), "process_events": validator_events}},
        node="validator",
        event_type="validator_input_bundle_ready",
        status="ready",
        summary="Validator input bundle ready for gate-only validation.",
        target=str(state.get("goal", "") or ""),
        metadata={
            "validator_input_bundle_schema": str(validator_input_bundle.get("schema_version", "")),
            "gate_conditions": dict(validator_input_bundle.get("gate_conditions", {}) or {}),
        },
    )
    state = {
        **state,
        "artifacts": {
            **state.get("artifacts", {}),
            "process_events": validator_events,
            "validator_input_bundle": validator_input_bundle,
        },
    }
    forbidden = [item.lower() for item in state["task_card"].get("forbidden_changes", [])]
    combined_output = " ".join(
        [
            state.get("goal", ""),
            state.get("build_result", ""),
            state.get("execution_result", ""),
            str((validator_input_bundle.get("review_summary", {}) or {}).get("summary", "") or ""),
            " ".join(list((validator_input_bundle.get("review_summary", {}) or {}).get("validation_gaps", []) or [])),
        ]
    ).lower()

    if strict_validation:
        if not state.get("build_result"):
            evidence_gaps.append("build_result missing")
        if not state.get("execution_result"):
            evidence_gaps.append("execution_result missing")
        if not state.get("review_result"):
            evidence_gaps.append("review_result missing")
        if not execution_evidence:
            evidence_gaps.append("execution_evidence missing")

    current_task_card = dict(state.get("task_card", {}) or {})
    current_missing_structured_fields: list[str] = []
    for field_name in ("scope", "out_of_scope", "expected_artifacts", "acceptance_criteria", "execution_scope"):
        value = current_task_card.get(field_name)
        if isinstance(value, list) and value:
            continue
        if isinstance(value, dict) and value:
            continue
        if isinstance(value, str) and value.strip():
            continue
        current_missing_structured_fields.append(field_name)

    if review_assessment.get("validation_gaps"):
        validation_issues.append("reviewer reported validation gaps")
    if intake_assessment.get("should_split"):
        validation_issues.append("task exceeds standard_task_unit_v1 and must be split before continuation")
    for field_name in current_missing_structured_fields:
        validation_issues.append(f"missing structured intake field: {field_name}")

    if execution_evidence.get("warnings"):
        validation_issues.append("execution engine reported warnings")
    if execution_evidence and execution_evidence.get("runtime_status") not in {"passed", ""}:
        validation_issues.append(f"runtime_status={execution_evidence.get('runtime_status')}")
    if execution_evidence and execution_evidence.get("git_diff_status") not in {"passed", ""}:
        validation_issues.append(f"git_diff_status={execution_evidence.get('git_diff_status')}")
    if execution_evidence and execution_evidence.get("lint_status") not in {"passed", ""}:
        validation_issues.append(f"lint_status={execution_evidence.get('lint_status')}")
    if execution_evidence and execution_evidence.get("pytest_status") not in {"passed", ""}:
        validation_issues.append(f"pytest_status={execution_evidence.get('pytest_status')}")
    if execution_evidence and execution_evidence.get("compile_status") not in {"passed", ""}:
        validation_issues.append(f"compile_status={execution_evidence.get('compile_status')}")
    if execution_evidence and execution_evidence.get("install_dep_status") == 'failed':
        validation_issues.append("approved dependency install failed")
    if state.get("approval_required") and state.get("risk_level") != "high":
        if state.get("task_kind") != "system_optimization":
            validation_issues.append("approval_required set without high risk classification")
    if state.get("task_kind") == "system_optimization" and not state.get("approval_required"):
        validation_issues.append("system optimization task bypassed mandatory approval policy")

    if _doctrine_change_requires_gate(state, combined_output) and not state.get("approval_required"):
        validation_issues.append("doctrine-related change detected without approval gate")
    if "external runtime" in combined_output:
        validation_issues.append("external runtime access mentioned in plan or execution")

    for item in forbidden:
        token = item.replace("Do not ", "").replace("do not ", "").lower()
        if token and token in combined_output:
            validation_issues.append(f"forbidden pattern matched: {item}")

    validation_issues.extend(_validate_governance_contract(state))
    validation_issues.extend(_validate_coordination_contract(state))
    validation_issues.extend(_validate_release_structure(state))
    validation_issues.extend(_validate_combination_structure(state))
    validation_issues.extend(_validate_workspace_flow_contract(state))
    validation_issues.extend(_validate_validation_hub_contract(state))

    missing_acceptance: list[str] = []
    if strict_validation and acceptance_criteria:
        acceptance_map = {
            "task card created": bool(state["task_card"].get("task_id")),
            "scan/build/review/record loop completed": bool(state.get("build_result") and state.get("review_result")),
            "system improvement path executed through validator checkpoint": True,
            "optimization goal documented for release review": bool(
                state.get("task_kind") != "system_optimization"
                or state.get("artifacts", {}).get("governance", {}).get("optimization_project_constraints", {}).get("require_release_review_after_record")
            ),
            "control tower updated": True,
        }
        for criterion in acceptance_criteria:
            matched = False
            for key, satisfied in acceptance_map.items():
                if key in criterion.lower():
                    matched = True
                    if not satisfied:
                        missing_acceptance.append(criterion)
                    break
            if not matched:
                continue

    issue_lines = [*validation_issues, *evidence_gaps, *[f"missing acceptance: {item}" for item in missing_acceptance]]
    intake_blockers_present = bool(intake_assessment.get("should_split") or current_missing_structured_fields)
    trigger_class = "periodic" if strict_validation else "stage_boundary"
    validator_decision_summary = (
        f"Validator decision={'changes_requested' if issue_lines else 'approved'} | issues={len(validation_issues)} | evidence_gaps={len(evidence_gaps)} | acceptance_gaps={len(missing_acceptance)}"
    )
    validator_events = emit_step_event(
        {**state, "artifacts": {**state.get("artifacts", {}), "process_events": validator_events}},
        node="validator",
        event_type="validator_decision",
        status="changes_requested" if issue_lines else "approved",
        summary=validator_decision_summary,
        target=str(state.get("goal", "") or ""),
        metadata={
            "trigger_class": trigger_class,
            "strict_validation": strict_validation,
            "validation_issue_count": len(validation_issues),
            "evidence_gap_count": len(evidence_gaps),
            "acceptance_gap_count": len(missing_acceptance),
            "sample_issues": [str(item) for item in issue_lines[:5]],
        },
    )
    failure_summary = "Validator requested remediation: " + "; ".join(issue_lines[:4])
    if issue_lines and (state["rework_count"] < state["max_rework_rounds"] or intake_blockers_present):
        feedback_signal = classify_validator_feedback(
            validation_status="changes_requested",
            issue_lines=issue_lines,
        )
        next_state = {
            **state,
            "active_phase": "validating",
            "active_agent": "validator",
            "validation_result": "Validation failed. " + "; ".join(issue_lines),
            "validation_status": "changes_requested",
            "validation_feedback": failure_summary,
            "review_feedback": failure_summary if light_path_exit else state.get("review_feedback", ""),
            "blocking_issue": failure_summary,
            "artifacts": {
                **state["artifacts"],
                "process_events": validator_events,
                "validator_input_bundle": validator_input_bundle,
                "inspection_state": _inspection_state_update(
                    state,
                    stage="validator",
                    trigger_class=trigger_class,
                    decision="changes_requested",
                    summary=(
                        "Validator escalated a light-path task after exit conditions were met."
                        if light_path_exit else "Validator hard inspection requested remediation."
                    ),
                    escalation_target="builder",
                ),
                "human_visibility": _human_visibility_update(
                    state,
                    stage="validator",
                    summary=(
                        "Light-path task exited into validator remediation."
                        if light_path_exit else "Validator inspection failed and escalated the task."
                    ),
                    recommendation=(
                        "Split oversized tasks or fill the missing structured intake fields before allowing the task to proceed."
                        if intake_assessment.get("should_split") or current_missing_structured_fields
                        else "Review missing evidence, risk signals, or scope drift before allowing the task to proceed."
                    ),
                    level="required" if strict_validation else "recommended",
                    reasons=["validator_inspection_failed"],
                ),
                "side_feedback": {
                    **state["artifacts"].get("side_feedback", {}),
                    "validator": feedback_signal,
                },
            },
            "rework_count": state["rework_count"] + 1,
            "steps": [*state["steps"], "validator_agent"],
        }
        next_state["artifacts"]["process_events"] = _append_state_transition_event(
            next_state,
            node="validator",
            event_type="rework_entered",
            summary="Task entered rework after validator requested remediation.",
            from_phase=str(state.get("active_phase", "")),
            to_phase="validating",
            status="changes_requested",
            metadata={"rework_count": next_state.get("rework_count", 0), "trigger_class": trigger_class},
        )
        _persist_routing_reassessment(next_state)
        return _finalize_node(
            state,
            next_state,
            node_id="validator_gate",
            agent="validator",
            phase="validating",
            started_at_perf=started_at_perf,
        )

    result = "Validation passed."
    details: list[str] = []
    if validation_issues:
        details.append("issues=" + "; ".join(validation_issues[:3]))
    if evidence_gaps:
        details.append("evidence_gaps=" + "; ".join(evidence_gaps[:3]))
    if missing_acceptance:
        details.append("acceptance_gaps=" + "; ".join(missing_acceptance[:3]))
    if details:
        result += " " + " | ".join(details)
    feedback_signal = classify_validator_feedback(
        validation_status="approved",
        issue_lines=issue_lines,
    )
    next_state = {
        **state,
        "active_phase": "validating",
        "active_agent": "validator",
        "validation_result": result,
        "validation_status": "approved",
        "validation_feedback": "",
        "blocking_issue": "",
        "artifacts": {
            **state["artifacts"],
            "process_events": validator_events,
            "validator_input_bundle": validator_input_bundle,
            "inspection_state": _inspection_state_update(
                state,
                stage="validator",
                trigger_class=trigger_class,
                decision="approved",
                summary=(
                    "Validator approved a light-path task at stage boundary."
                    if not strict_validation else "Validator hard inspection passed."
                ),
                escalation_target="approval" if state.get("approval_required") else "auto_approve",
            ),
            "human_visibility": _human_visibility_update(
                state,
                stage="validator",
                summary=(
                    "Stage-boundary validator check passed."
                    if not strict_validation else "Periodic hard inspection passed."
                ),
                recommendation="Human can inspect this checkpoint if the task was previously downgraded or is approaching approval/release.",
            ),
            "side_feedback": {
                **state["artifacts"].get("side_feedback", {}),
                "validator": feedback_signal,
            },
        },
        "steps": [*state["steps"], "validator_agent"],
    }
    return _finalize_node(
        state,
        next_state,
        node_id="validator_gate",
        agent="validator",
        phase="validating",
        started_at_perf=started_at_perf,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_json_artifact(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _summarize_action_output(text: str, limit: int = 240) -> str:
    normalized = ' '.join(str(text or '').split())
    return normalized[:limit] if len(normalized) > limit else normalized


def _execution_evidence_snapshot(state: KernelState) -> dict[str, object]:
    artifacts = _with_artifact_layers(state.get('artifacts', {}))
    control_artifacts = dict(artifacts.get('control_artifacts', {}) or {})
    paths = dict(control_artifacts.get('paths', {}) or artifacts.get('paths', {}) or {})
    project_root_raw = str(paths.get('project_root', '')).strip()
    if not project_root_raw:
        return {}
    project_root = Path(project_root_raw)
    report = _load_json_artifact(project_root / 'artifacts' / 'execution_report.json')
    if not report:
        return {}
    runtime = dict(((report.get('runtime', {}) or {}).get('execution', {}) or {}))
    observations = dict(((report.get('runtime', {}) or {}).get('observations', {}) or {}))
    actions = list(runtime.get('actions', []) or [])

    def _pick(kind: str) -> dict[str, object]:
        for item in actions:
            if str(item.get('kind', '')) == kind:
                return dict(item)
        return {}

    git_diff = dict(observations.get('git_diff', {}) or {})
    lint = _pick('run_lint.ruff')
    compileall = _pick('run_tests.compileall')
    pytest_action = _pick('run_tests.pytest')
    install_dep = dict(observations.get('install_dep_approval', {}) or {})
    workspace_root = str((state.get('artifacts', {}) or {}).get('input_task_payload', {}).get('target_workspace_root', '') or '')
    changed_files = _baseline_changed_files_for_workspace(workspace_root, limit=12)
    baseline_summary = _baseline_diff_summary_for_workspace(workspace_root, limit=12)
    if not changed_files:
        for line in str(git_diff.get('stdout', '') or '').splitlines():
            if '|' in line:
                changed_files.append(line.split('|', 1)[0].strip())
    return {
        'runtime_status': str(runtime.get('status', '')),
        'runtime_detail': str(runtime.get('detail', '')),
        'runtime_severity': str(runtime.get('severity', '')),
        'warnings': list(report.get('warnings', []) or []),
        'git_diff_status': str(git_diff.get('status', '')),
        'git_diff_summary': baseline_summary or _summarize_action_output(str(git_diff.get('stdout', '') or git_diff.get('stderr', '') or '')),
        'changed_files': changed_files[:12],
        'lint_status': str(lint.get('status', '')),
        'lint_returncode': lint.get('returncode'),
        'lint_stdout': _summarize_action_output(str(lint.get('stdout', '') or '')),
        'lint_stderr': _summarize_action_output(str(lint.get('stderr', '') or '')),
        'pytest_status': str(pytest_action.get('status', '')),
        'pytest_returncode': pytest_action.get('returncode'),
        'pytest_stdout': _summarize_action_output(str(pytest_action.get('stdout', '') or '')),
        'pytest_stderr': _summarize_action_output(str(pytest_action.get('stderr', '') or '')),
        'compile_status': str(compileall.get('status', '')),
        'compile_returncode': compileall.get('returncode'),
        'compile_stderr': _summarize_action_output(str(compileall.get('stderr', '') or '')),
        'install_dep_status': str(install_dep.get('status', '')),
        'install_dep_detail': _summarize_action_output(str(install_dep.get('detail', '') or '')),
    }


def _run_approved_install_dep(state: KernelState) -> tuple[dict[str, object], str, bool]:
    artifacts = _with_artifact_layers(state.get('artifacts', {}))
    control_artifacts = dict(artifacts.get('control_artifacts', {}) or {})
    paths = dict(control_artifacts.get('paths', {}) or artifacts.get('paths', {}) or {})
    project_root_raw = str(paths.get('project_root', '')).strip()
    if not project_root_raw:
        return artifacts, '', True
    project_root = Path(project_root_raw)
    runtime_plan_path = project_root / 'artifacts' / 'execution_runtime_plan.json'
    execution_report_path = project_root / 'artifacts' / 'execution_report.json'
    runtime_plan = _load_json_artifact(runtime_plan_path)
    if not runtime_plan:
        return artifacts, '', True
    action_plan = list(runtime_plan.get('action_plan', []) or [])
    pending_specs = [
        spec for spec in action_plan
        if str(spec.get('action_type', '')) == 'install_dep'
        and str(spec.get('approval_state', '')) == 'pending_human_approval'
    ]
    if not pending_specs:
        return artifacts, '', True

    runtime_config = load_execution_runtime_config()
    source_root = runtime_config.docker.source_mount_target
    scratch_root = runtime_config.docker.scratch_root
    runtime_actions: list[RuntimeAction] = []
    docker_previews: list[list[str]] = []
    for index, spec in enumerate(pending_specs, start=1):
        payload = dict(spec.get('payload', {}) or {})
        installer_tool = str(payload.get('installer_tool', 'pip') or 'pip')
        packages = [str(item).strip() for item in payload.get('packages', []) if str(item).strip()]
        requirements_file = str(payload.get('requirements_file', '') or '').strip().replace('\\', '/')
        install_target = f"{scratch_root}/install_vendor_{index}"
        if requirements_file:
            install_source = f"{source_root}/{requirements_file.strip('./')}"
            install_expr = f"python -m {installer_tool} install --disable-pip-version-check --no-input --target {install_target} -r {install_source}"
        else:
            install_expr = f"python -m {installer_tool} install --disable-pip-version-check --no-input --target {install_target} {' '.join(packages)}"
        command = f"rm -rf {install_target} && mkdir -p {install_target} && {install_expr}"
        action = RuntimeAction(
            kind=f"install_dep.{installer_tool}",
            command=['sh', '-lc', command],
            working_subdir='.',
            timeout_seconds=int(spec.get('timeout_seconds') or max(runtime_config.docker.default_timeout_seconds, 300)),
            network_enabled=True,
        )
        runtime_actions.append(action)
        docker_previews.append(build_docker_command(workspace_root=_repo_root(), action=action))

    runtime_execution = execute_runtime_actions(workspace_root=_repo_root(), actions=runtime_actions)
    runtime_result = runtime_execution_to_dict(runtime_execution)
    executed_at = now_iso()
    for spec in action_plan:
        if str(spec.get('action_type', '')) != 'install_dep' or str(spec.get('approval_state', '')) != 'pending_human_approval':
            continue
        spec['approval_state'] = 'approved'
        spec['execution_state'] = 'passed' if runtime_execution.status == 'passed' else 'failed'
        spec['executed_at'] = executed_at
        spec['execution_detail'] = runtime_execution.detail

    runtime_plan['action_plan'] = action_plan
    runtime_plan.setdefault('observations', {})['install_dep_approval'] = runtime_result
    runtime_plan.setdefault('docker_command_preview', []).extend(docker_previews)
    write_json(runtime_plan_path, runtime_plan)

    execution_report = _load_json_artifact(execution_report_path)
    if execution_report:
        runtime_section = dict(execution_report.get('runtime', {}) or {})
        runtime_section.setdefault('observations', {})['install_dep_approval'] = runtime_result
        runtime_section.setdefault('docker_command_preview', []).extend(docker_previews)
        execution_report['runtime'] = runtime_section
        execution_report.setdefault('approved_actions', {})['install_dep'] = runtime_result
        write_json(execution_report_path, execution_report)

    execution_runtime = dict(artifacts.get('execution_runtime', {}) or {})
    execution_runtime.update({
        'backend': runtime_execution.backend,
        'status': runtime_execution.status,
        'detail': runtime_execution.detail,
        'severity': runtime_execution.severity,
        'should_interrupt': runtime_execution.should_interrupt,
        'duration_ms': runtime_execution.duration_ms,
    })
    artifacts['execution_runtime'] = execution_runtime
    artifacts['install_dep_approval'] = runtime_result
    append_markdown(
        Path(paths['execution_log']),
        "\n".join([
            '## Approved Dependency Install',
            f"- status: {runtime_execution.status}",
            f"- detail: {runtime_execution.detail}",
            f"- duration_ms: {runtime_execution.duration_ms}",
        ]),
    )
    summary = f"Approved install_dep executed: {runtime_execution.detail}"
    return artifacts, summary, runtime_execution.status == 'passed'


def approval_node(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="approval_gate", agent="human_director", phase=state.get("active_phase", "awaiting_human_approval"))
    decision = state.get("human_decision", "")
    if decision == "approved":
        updated_artifacts, install_summary, install_ok = _run_approved_install_dep(state)
        execution_result = str(state.get("execution_result", "") or "").strip()
        if install_summary:
            execution_result = f"{execution_result} {install_summary}".strip()
        if not install_ok:
            next_state = {
                **state,
                "active_phase": "changes_requested",
                "active_agent": "human_director",
                "blocking_issue": install_summary or "Approved install_dep failed during execution.",
                "approval_status": "changes_requested",
                "execution_result": execution_result,
                "artifacts": updated_artifacts,
                "steps": [*state["steps"], "approval_node"],
            }
            observer_result = run_meta_observer(next_state, state["artifacts"]["paths"])
            next_state["artifacts"] = {
                **next_state["artifacts"],
                "observer": observer_result,
            }
            return _finalize_node(
                state,
                next_state,
                node_id="approval_gate",
                agent="human_director",
                phase="changes_requested",
                started_at_perf=started_at_perf,
            )
        next_state = {
            **state,
            "active_phase": "human_approved",
            "active_agent": "human_director",
            "blocking_issue": "",
            "approval_status": "approved",
            "execution_result": execution_result,
            "artifacts": updated_artifacts,
            "steps": [*state["steps"], "approval_node"],
        }
        next_state["artifacts"]["process_events"] = _append_state_transition_event(
            next_state,
            node="human_director",
            event_type="approval_resumed",
            summary="Human approval resumed the task and allowed it to continue.",
            from_phase=str(state.get("active_phase", "")),
            to_phase="human_approved",
            status="approved",
            metadata={"decision": "approved"},
        )
        return _finalize_node(
            state,
            next_state,
            node_id="approval_gate",
            agent="human_director",
            phase="human_approved",
            started_at_perf=started_at_perf,
        )
    if decision == "changes_requested":
        next_state = {
            **state,
            "active_phase": "changes_requested",
            "active_agent": "human_director",
            "blocking_issue": "Human requested changes before record.",
            "approval_status": "changes_requested",
            "steps": [*state["steps"], "approval_node"],
        }
        observer_result = run_meta_observer(next_state, state["artifacts"]["paths"])
        next_state["artifacts"] = {
            **next_state["artifacts"],
            "observer": observer_result,
        }
        return _finalize_node(
            state,
            next_state,
            node_id="approval_gate",
            agent="human_director",
            phase="changes_requested",
            started_at_perf=started_at_perf,
        )
    next_state = {
        **state,
        "active_phase": "awaiting_human_approval",
        "active_agent": "human_director",
        "blocking_issue": "High risk task requires human approval.",
        "approval_status": "pending",
        "steps": [*state["steps"], "approval_node"],
    }
    next_state["artifacts"]["process_events"] = _append_state_transition_event(
        next_state,
        node="human_director",
        event_type="approval_waiting",
        summary="Task is waiting for human approval before continuing.",
        from_phase=str(state.get("active_phase", "")),
        to_phase="awaiting_human_approval",
        status="pending",
    )
    _persist_routing_reassessment(next_state)
    observer_result = run_meta_observer(next_state, state["artifacts"]["paths"])
    next_state["artifacts"] = {
        **next_state["artifacts"],
        "observer": observer_result,
    }
    return _finalize_node(
        state,
        next_state,
        node_id="approval_gate",
        agent="human_director",
        phase="awaiting_human_approval",
        started_at_perf=started_at_perf,
    )


def auto_approve_node(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="auto_approve", agent=state["active_agent"], phase=state["active_phase"])
    next_state = {
        **state,
        "approval_status": "approved",
        "blocking_issue": "",
        "steps": [*state["steps"], "auto_approve_node"],
    }
    return _finalize_node(
        state,
        next_state,
        node_id="auto_approve",
        agent=next_state["active_agent"],
        phase=next_state["active_phase"],
        started_at_perf=started_at_perf,
    )


def _recorder_input_issues(state: KernelState, paths: dict[str, str]) -> list[str]:
    issues: list[str] = []
    task_card = dict(state.get("task_card", {}) or {})
    task_id = str(task_card.get("task_id", "") or "").strip()
    goal = str(task_card.get("goal", "") or state.get("goal", "")).strip()
    review_status = str(state.get("review_status", "") or "").strip().lower()
    approval_status = str(state.get("approval_status", "") or "").strip().lower()
    validation_status = str(state.get("validation_status", "") or "").strip().lower()
    if not task_id:
        issues.append("missing_task_id")
    if not goal:
        issues.append("missing_goal")
    if review_status not in {"approved"}:
        issues.append(f"review_status_not_recordable:{review_status or 'missing'}")
    if approval_status not in {"approved", "not_needed"}:
        issues.append(f"approval_status_not_recordable:{approval_status or 'missing'}")
    if validation_status and validation_status not in {"approved", "not_needed"}:
        issues.append(f"validation_status_not_recordable:{validation_status}")
    required_paths = ["memory_root", "in_progress", "completed", "execution_log", "decision_log", "phase", "module_status"]
    for key in required_paths:
        value = str(paths.get(key, "") or "").strip()
        if not value:
            issues.append(f"missing_path:{key}")
    return issues


def _recorder_conflict_reason(state: KernelState, paths: dict[str, str]) -> str:
    task_card = dict(state.get("task_card", {}) or {})
    task_id = str(task_card.get("task_id", "") or "").strip()
    if not task_id:
        return ""
    completed_path = Path(str(paths.get("completed", "") or ""))
    if not completed_path.exists():
        return ""
    try:
        completed_text = completed_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    marker = f"# {task_id}\n"
    if marker in completed_text:
        return f"task_id {task_id} 已存在归档记录，继续写入会造成冲突。"
    return ""


def recorder_agent(state: KernelState) -> KernelState:
    state, started_at_perf = _mark_node_started(state, node_id="record_memory", agent="recorder", phase="recorded")
    _update_role_working_memory(
        'recorder',
        task_id=str(((state.get('task_card', {}) or {}).get('task_id', '') or state.get('project_id', 'record-task'))),
        goal=str(state.get('goal', '') or ''),
        status='in_progress',
        facts=[f'approval_status={str(state.get("approval_status", "") or "")}', f'review_status={str(state.get("review_status", "") or "")}'],
        decisions=[],
        progress='Recorder 正在归档，并准备提炼 lessons 与清理 working_memory。',
    )
    paths = state["artifacts"]["paths"]
    input_issues = _recorder_input_issues(state, paths)
    conflict_reason = "" if input_issues else _recorder_conflict_reason(state, paths)
    if input_issues:
        task_card = dict(state.get("task_card", {}) or {})
        rejection_payload = {
            "type": "archive_rejection",
            "from_role": "recorder",
            "to_role": "reviewer",
            "task_id": str(task_card.get("task_id", "") or state.get("project_id", "record-task")),
            "reason": "Recorder 输入不完整或状态不满足归档条件。",
            "missing_fields": [item for item in input_issues if item.startswith("missing_")],
            "unresolvable_conflicts": [item for item in input_issues if not item.startswith("missing_")],
            "recommendation": "请先补齐 task_id/goal/paths，并确保 review_status=approved、approval_status=approved 或 not_needed 后再归档。",
        }
        _update_role_working_memory(
            'recorder',
            task_id=str(task_card.get('task_id', '') or state.get('project_id', 'record-task')),
            goal=str(state.get('goal', '') or ''),
            status='rejected',
            facts=input_issues[:5],
            decisions=[rejection_payload['reason']],
            progress='Recorder 拒绝归档，已回吐 archive_rejection。',
        )
        next_state = {
            **state,
            "active_phase": "recording_rejected",
            "active_agent": "recorder",
            "recorder_summary": rejection_payload["reason"],
            "blocking_issue": rejection_payload["reason"],
            "artifacts": {
                **state.get("artifacts", {}),
                "recorder_outcome": rejection_payload,
            },
            "steps": [*state["steps"], "recorder_agent"],
        }
        return _finalize_node(
            state,
            next_state,
            node_id="record_memory",
            agent="recorder",
            phase="recording_rejected",
            started_at_perf=started_at_perf,
        )
    if conflict_reason:
        task_card = dict(state.get("task_card", {}) or {})
        escalation_payload = {
            "type": "conflict_escalation",
            "from_role": "recorder",
            "to_role": "human",
            "task_id": str(task_card.get("task_id", "") or state.get("project_id", "record-task")),
            "reason": conflict_reason,
            "decision_required": "是否允许覆盖既有归档，或需要先合并/重命名任务记录。",
            "options": [
                "保持旧记录，停止本次归档并人工对账。",
                "允许以新 task_id 重新归档。",
                "人工确认后覆盖旧记录（不推荐）。",
            ],
            "recommendation": "优先保持旧记录并人工对账，确认是否重复归档。",
            "current_state": "Recorder 检测到同 task_id 归档冲突。",
        }
        _update_role_working_memory(
            'recorder',
            task_id=str(task_card.get('task_id', '') or state.get('project_id', 'record-task')),
            goal=str(state.get('goal', '') or ''),
            status='blocked',
            facts=[conflict_reason],
            decisions=[escalation_payload['decision_required']],
            progress='Recorder 检测到归档冲突，已升级给人工处理。',
        )
        next_state = {
            **state,
            "active_phase": "awaiting_human_approval",
            "active_agent": "recorder",
            "recorder_summary": conflict_reason,
            "approval_required": True,
            "approval_status": "pending",
            "blocking_issue": conflict_reason,
            "artifacts": {
                **state.get("artifacts", {}),
                "recorder_outcome": escalation_payload,
            },
            "steps": [*state["steps"], "recorder_agent"],
        }
        return _finalize_node(
            state,
            next_state,
            node_id="record_memory",
            agent="recorder",
            phase="awaiting_human_approval",
            started_at_perf=started_at_perf,
        )
    summary = (
        f"Task {state['task_card']['task_id']} recorded with review={state['risk_level']} "
        f"approval={state['approval_status']} validation={state['validation_status']} rework_rounds={state['rework_count']}."
    )
    append_markdown(Path(paths["in_progress"]), f"# {state['task_card']['task_id']}\nGoal: {state['task_card']['goal']}\nStatus: recorded at {now_iso()}")
    append_markdown(Path(paths["completed"]), f"# {state['task_card']['task_id']}\nResult: {summary}")
    append_markdown(Path(paths["execution_log"]), f"## Execution\n- goal: {state['goal']}\n- execution: {state['execution_result']}\n- review: {state['review_result']}\n- validation: {state['validation_result']}\n- rework_count: {state['rework_count']}")
    append_markdown(Path(paths["decision_log"]), f"## Decision\n- approval_status: {state['approval_status']}\n- review_status: {state['review_status']}\n- validation_status: {state['validation_status']}\n- recorder_summary: {summary}")
    Path(paths["phase"]).write_text(
        f"current_phase: recorded\nactive_agent: recorder\nrework_count: {state['rework_count']}\nupdated_at: {now_iso()}\n",
        encoding="utf-8",
    )
    Path(paths["module_status"]).write_text(
        f"kernel_workflow: healthy\nproject_id: {state.get('project_id', 'unknown')}\nreview_status: {state['review_status']}\n",
        encoding="utf-8",
    )
    next_state = {
        **state,
        "active_phase": "recorded",
        "active_agent": "recorder",
        "recorder_summary": summary,
        "steps": [*state["steps"], "recorder_agent"],
    }
    next_state["artifacts"]["process_events"] = _append_state_transition_event(
        next_state,
        node="recorder",
        event_type="recorded",
        summary="Task reached recorded state.",
        from_phase=str(state.get("active_phase", "")),
        to_phase="recorded",
        status="completed",
    )
    append_timeline_entry(
        paths,
        title="Task Recorded",
        lines=[
            f"task_id: {state['task_card']['task_id']}",
            f"phase: {next_state['active_phase']}",
            f"approval_status: {state['approval_status']}",
            f"review_status: {state['review_status']}",
            f"validation_status: {state['validation_status']}",
        ],
    )
    append_memory_index(
        paths,
        {
            "timestamp": now_iso(),
            "type": "task_recorded",
            "task_id": state["task_card"]["task_id"],
            "phase": next_state["active_phase"],
            "approval_status": state["approval_status"],
            "review_status": state["review_status"],
            "validation_status": state["validation_status"],
            "rework_count": state["rework_count"],
        },
    )
    archive_phase_snapshot(paths, phase=next_state["active_phase"], state=next_state)
    write_project_summary(paths, next_state)
    role_memory_flags = _finalize_role_memory_after_record(next_state)
    observer_result = run_meta_observer(next_state, paths)
    next_state["artifacts"] = {
        **next_state["artifacts"],
        "observer": observer_result,
        "role_memory": role_memory_flags,
        "side_feedback": {
            **next_state["artifacts"].get("side_feedback", {}),
            "observer": observer_result.get("feedback_signal", {}),
        },
    }
    return _finalize_node(
        state,
        next_state,
        node_id="record_memory",
        agent="recorder",
        phase="recorded",
        started_at_perf=started_at_perf,
    )


































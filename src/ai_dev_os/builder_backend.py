from __future__ import annotations

import importlib.util
from time import perf_counter
from typing import Any
from typing import Callable


StreamEventCallback = Callable[[dict[str, Any]], None]

from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.claude_code_backend import ClaudeCodeCallError
from ai_dev_os.claude_code_backend import run_claude_code_prompt
from ai_dev_os.codex_backend import CodexCallError
from ai_dev_os.codex_backend import run_codex_prompt
from ai_dev_os.opencode_backend import OpenCodeCallError
from ai_dev_os.opencode_backend import run_opencode_prompt
from ai_dev_os.qwen_code_backend import QwenCodeCallError
from ai_dev_os.qwen_code_backend import run_qwen_code_prompt
from ai_dev_os.llm_interface import BuilderPlanCallError
from ai_dev_os.llm_interface import build_builder_call_diagnostics
from ai_dev_os.llm_interface import BUILDER_JSON_SCHEMA
from ai_dev_os.llm_interface import build_builder_prompt
from ai_dev_os.llm_interface import get_builder_plan_with_diagnostics as get_llm_builder_plan_with_diagnostics


CLAUDE_CODE_SYSTEM_PROMPT = "You are the Builder Agent for an AI Dev OS kernel. Return only valid JSON."


def get_builder_backend_name() -> str:
    settings = get_agent_settings("builder")
    return str(settings.backend or "llm").strip().lower() or "llm"


def get_builder_plan_with_backend_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    backend = get_builder_backend_name()
    if backend == "llm":
        plan, diagnostics = get_llm_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
            stream_event_callback=stream_event_callback,
        )
        diagnostics = dict(diagnostics)
        diagnostics.setdefault("backend", "llm")
        return plan, diagnostics
    if backend == "claude_code":
        return _get_claude_code_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
            stream_event_callback=stream_event_callback,
        )
    if backend == "opencode":
        return _get_opencode_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
            stream_event_callback=stream_event_callback,
        )
    if backend == "qwen_code":
        return _get_qwen_code_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
            stream_event_callback=stream_event_callback,
        )
    if backend == "codex":
        return _get_codex_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
            stream_event_callback=stream_event_callback,
        )
    if backend == "openhands":
        return _get_openhands_builder_plan_with_diagnostics(
            goal=goal,
            task_card=task_card,
            scan_result=scan_result,
            project_memory_context=project_memory_context,
            orchestrator_guidance=orchestrator_guidance,
            builder_working_state=builder_working_state,
            review_feedback=review_feedback,
            rework_count=rework_count,
        )
    raise BuilderPlanCallError(
        f"Unsupported builder backend: {backend}",
        diagnostics={
            "role": "builder",
            "backend": backend,
            "mode": "live",
            "call_status": "failed",
            "exception_type": "UnsupportedBuilderBackend",
            "exception_message": f"Unsupported builder backend: {backend}",
        },
    )


def _get_claude_code_builder_plan_with_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    settings = get_agent_settings("builder")
    prompt = build_builder_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    diagnostics = build_builder_call_diagnostics(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
        prompt=prompt,
    )
    diagnostics["backend"] = "claude_code"
    diagnostics["workspace_root"] = settings.workspace_root
    diagnostics["memory_root"] = settings.memory_root
    diagnostics["session_id"] = settings.session_id
    try:
        result, claude_diagnostics = run_claude_code_prompt(
            role="builder",
            system_prompt=CLAUDE_CODE_SYSTEM_PROMPT,
            user_prompt=prompt,
            cwd=settings.workspace_root,
            session_id=settings.session_id,
            continue_session=bool(rework_count),
            append_system_prompt=f"Role memory root: {settings.memory_root}",
            json_schema=BUILDER_JSON_SCHEMA,
            model=settings.model,
            base_url=settings.base_url,
            api_key=settings.api_key,
            stream_event_callback=stream_event_callback,
        )
    except ClaudeCodeCallError as exc:
        merged = dict(diagnostics)
        merged.update(exc.diagnostics)
        raise BuilderPlanCallError(str(exc), diagnostics=merged) from exc
    diagnostics.update(claude_diagnostics)
    return result, diagnostics


def _get_opencode_builder_plan_with_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    settings = get_agent_settings("builder")
    prompt = build_builder_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    diagnostics = build_builder_call_diagnostics(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
        prompt=prompt,
    )
    diagnostics["backend"] = "opencode"
    diagnostics["workspace_root"] = settings.workspace_root
    diagnostics["memory_root"] = settings.memory_root
    diagnostics["session_id"] = settings.session_id
    try:
        result, opencode_diagnostics = run_opencode_prompt(
            role="builder",
            system_prompt=CLAUDE_CODE_SYSTEM_PROMPT,
            user_prompt=prompt,
            cwd=settings.workspace_root,
            model=settings.model,
            provider=settings.provider,
            base_url=settings.base_url,
            api_key=settings.api_key,
            memory_root=settings.memory_root,
            stream_event_callback=stream_event_callback,
        )
    except OpenCodeCallError as exc:
        merged = dict(diagnostics)
        merged.update(exc.diagnostics)
        raise BuilderPlanCallError(str(exc), diagnostics=merged) from exc
    diagnostics.update(opencode_diagnostics)
    return result, diagnostics


def _get_codex_builder_plan_with_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    settings = get_agent_settings("builder")
    prompt = build_builder_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    diagnostics = build_builder_call_diagnostics(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
        prompt=prompt,
    )
    diagnostics["backend"] = "codex"
    diagnostics["workspace_root"] = settings.workspace_root
    diagnostics["memory_root"] = settings.memory_root
    diagnostics["session_id"] = settings.session_id
    try:
        result, codex_diagnostics = run_codex_prompt(
            role="builder",
            system_prompt=CLAUDE_CODE_SYSTEM_PROMPT,
            user_prompt=prompt,
            cwd=settings.workspace_root,
            model=settings.model,
            provider=settings.provider,
            base_url=settings.base_url,
            api_key=settings.api_key,
            memory_root=settings.memory_root,
            stream_event_callback=stream_event_callback,
            task_packet_name="CODEX_BUILDER_TASK.md",
            output_filename="BUILD_PLAN.json",
            assistant_sentinel="BUILD_PLAN_WRITTEN",
            execution_contract_lines=[
                "Inspect the real target files before changing anything.",
                "Act as the grounded mainhand: edit source files directly in the shared workspace, then write BUILD_PLAN.json as a structured execution record with direct_execution=true.",
                "Keep edits low-diff, stay inside the task card scope, and avoid unrelated files.",
                "Only spend time constructing exact old_text/new_text payloads when they are materially useful; do not let JSON planning become the main work.",
            ],
            reasoning_effort="medium",
            sandbox_mode="workspace-write",
            dangerously_bypass_approvals_and_sandbox=True,
        )
    except CodexCallError as exc:
        merged = dict(diagnostics)
        merged.update(exc.diagnostics)
        raise BuilderPlanCallError(str(exc), diagnostics=merged) from exc
    diagnostics.update(codex_diagnostics)
    return result, diagnostics


# Adapter seam only: when OpenHands runtime is available later, keep the same contract
# and implement the real call here without touching builder_agent again.
def _get_qwen_code_builder_plan_with_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    settings = get_agent_settings("builder")
    prompt = build_builder_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    diagnostics = build_builder_call_diagnostics(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
        prompt=prompt,
    )
    diagnostics["backend"] = "qwen_code"
    diagnostics["workspace_root"] = settings.workspace_root
    diagnostics["memory_root"] = settings.memory_root
    diagnostics["session_id"] = settings.session_id
    execution_contract_lines = [
        "Act as the grounded mainhand: inspect real workspace files, implement the changes directly by editing source files, then write BUILD_PLAN.json with '\"direct_execution\": true' to document what you did.",
        "Prefer low-diff edits and stay inside the task card scope; do not touch unrelated files.",
        "Before drafting any edit_file payload, read the real target file and ground on its actual structure.",
        "Every change_plan target must use the real workspace-relative path (for example scripts/dashboard_server.py), not a bare filename. If you are not sure of the unique relative path, scout first and do not guess.",
        "Do not invent helper names, function names, template blocks, or old_text snippets from assumptions about how the file should look.",
        "Every replace_text old_text must come from a real exact match in the current workspace file. If you cannot find a stable anchor, do not emit a fake replace_text payload.",
        "Do not invent new API endpoints, routes, or fetch targets unless the task card explicitly allows them or the grounding scan shows an existing endpoint to reuse.",
        "For multi-change tasks, every edit_file.replace_text old_text must be grounded; do not let only the first patch be exact while later patches are guessed.",
        "If grounding snippets show exact lines, copy those lines verbatim for old_text instead of rewriting selectors, tag order, spacing, or indentation from memory.",
        "For HTML/CSS/template edits, prefer single-line exact anchors for replace_text. Use multi-line old_text only when the exact full block was already observed in the grounding scan or real file read.",
        "If the grounding scan only proves a single selector line or opening tag, do not expand it into an imagined full block; anchor on the proven single line and insert the minimal adjacent content around it.",
        "If grounding is insufficient, say so in risks/implementation_steps and avoid emitting non-executable source edits.",
        "For single-file UI/template tasks, do not emit more replace_text patches than the number of stable exact anchors you have already grounded from the real file.",
        "If you have only one stable exact anchor, prefer one grounded patch plus explicit remaining risks instead of adding speculative second or third patches.",
        "If the task requires current/live/existing config data, do not satisfy it with placeholder values like -, mock data, or invented state fields; reuse an existing grounded data source already visible in the file or scan.",
        "If the grounding scan shows concrete existing data sources such as helper functions, existing endpoints, or current state objects, you must choose from those grounded candidates and name which one you reused in the summary or implementation_steps.",
        "If grounded data sources are available, do not introduce a new fetch target, new state container, or new config file path unless the task card explicitly allows it.",
        "If the task asks for current/live/existing data and you cannot wire one of the grounded data sources, stop and report a blocked risk instead of shipping a placeholder-only implementation.",
        "If the grounding scan shows an existing API endpoint, state object, or render function, prefer wiring that existing source into the new UI instead of creating a parallel fake path.",
        "If no code change is needed, explain why in summary and leave explicit evidence in change_plan.verification.expected_signals.",
    ]
    try:
        result, qwen_diagnostics = run_qwen_code_prompt(
            role="builder",
            system_prompt=CLAUDE_CODE_SYSTEM_PROMPT,
            user_prompt=prompt,
            cwd=settings.workspace_root,
            model=settings.model,
            base_url=settings.base_url,
            api_key=settings.api_key,
            memory_root=settings.memory_root,
            stream_event_callback=stream_event_callback,
            execution_contract_lines=execution_contract_lines,
        )
    except QwenCodeCallError as exc:
        merged = dict(diagnostics)
        merged.update(exc.diagnostics)
        raise BuilderPlanCallError(str(exc), diagnostics=merged) from exc
    diagnostics.update(qwen_diagnostics)
    return result, diagnostics


def _get_openhands_builder_plan_with_diagnostics(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
) -> tuple[str, dict[str, Any]]:
    started_at = perf_counter()
    diagnostics: dict[str, Any] = {
        "role": "builder",
        "backend": "openhands",
        "mode": "live",
        "call_status": "failed",
        "integration_status": "adapter_only",
        "goal_excerpt": goal[:200],
        "task_id": str(task_card.get("task_id", "") or ""),
        "rework_count": int(rework_count or 0),
    }
    if importlib.util.find_spec("openhands") is None:
        diagnostics.update(
            {
                "call_duration_ms": round((perf_counter() - started_at) * 1000, 2),
                "exception_type": "OpenHandsUnavailable",
                "exception_message": "OpenHands runtime is not installed in this environment.",
            }
        )
        raise BuilderPlanCallError(
            "Builder backend 'openhands' is selected, but OpenHands runtime is not installed.",
            diagnostics=diagnostics,
        )
    diagnostics.update(
        {
            "call_duration_ms": round((perf_counter() - started_at) * 1000, 2),
            "exception_type": "OpenHandsNotIntegrated",
            "exception_message": "OpenHands runtime is present, but the builder adapter is not implemented yet.",
        }
    )
    raise BuilderPlanCallError(
        "Builder backend 'openhands' is selected, but the OpenHands adapter is not implemented yet.",
        diagnostics=diagnostics,
    )



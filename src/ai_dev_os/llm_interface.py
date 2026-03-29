from __future__ import annotations

import json
import re
from time import perf_counter
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Iterable

from openai import OpenAI

from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.agent_settings import runtime_settings
from ai_dev_os.claude_code_backend import run_claude_code_prompt
from ai_dev_os.codex_backend import CodexCallError
from ai_dev_os.codex_backend import run_codex_prompt
from ai_dev_os.opencode_backend import OpenCodeCallError
from ai_dev_os.opencode_backend import run_opencode_prompt
from ai_dev_os.qwen_code_backend import QwenCodeCallError
from ai_dev_os.qwen_code_backend import run_qwen_code_prompt
from ai_dev_os.role_memory import build_role_memory_context
from ai_dev_os.validation_contract import model_source_for_role


REQUEST_TIMEOUT_SECONDS = 90.0
MAX_RETRIES = 0

StreamEventCallback = Callable[[dict[str, Any]], None]

ORCHESTRATOR_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "task_profile": {"type": "string"},
        "governance_weight": {"type": "string"},
        "combination_sensitive": {"type": "boolean"},
        "coordination_subtype": {"type": "string"},
        "capability_mix": {"type": "array", "items": {"type": "string"}},
        "interaction_risks": {"type": "array", "items": {"type": "string"}},
        "scope_additions": {"type": "array", "items": {"type": "string"}},
        "acceptance_additions": {"type": "array", "items": {"type": "string"}},
        "boundary_notes": {"type": "array", "items": {"type": "string"}},
        "builder_brief": {"type": "string"},
        "approval_hint": {"type": "boolean"}
    },
    "required": [
        "task_profile", "governance_weight", "combination_sensitive", "coordination_subtype",
        "capability_mix", "interaction_risks", "scope_additions", "acceptance_additions",
        "boundary_notes", "builder_brief", "approval_hint"
    ],
    "additionalProperties": True
}, ensure_ascii=False)

BUILDER_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "implementation_steps": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
        "validation_checks": {"type": "array", "items": {"type": "string"}},
        "change_plan": {"type": "object"}
    },
    "required": ["summary", "implementation_steps", "risks", "validation_checks", "change_plan"],
    "additionalProperties": True
}, ensure_ascii=False)

REVIEWER_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "decision": {"type": "string"},
        "escalation_request": {"type": "object"},
        "summary": {"type": "string"},
        "feedback": {"type": "string"},
        "issues": {"type": "array", "items": {"type": "string"}},
        "validation_gaps": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["decision", "summary", "feedback", "issues", "validation_gaps"],
    "additionalProperties": True
})
MAINHAND_ROLE_DOC_PATHS = [
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "mainhand_seed_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "mainhand_config_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "mainhand_lessons_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "mainhand_samples_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "mainhand_acceptance_v1.md",
]
REVIEWER_ROLE_DOC_PATHS = [
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "4_reviewer" / "reviewer_seed_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "4_reviewer" / "reviewer_config_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "4_reviewer" / "reviewer_lessons_v1.md",
]
ORCHESTRATOR_ROLE_DOC_PATHS = [
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "2_orchestrator" / "orchestrator_seed_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "2_orchestrator" / "orchestrator_config_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "2_orchestrator" / "orchestrator_lessons_v1.md",
    Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "2_orchestrator" / "orchestrator_samples_v1.md",
]


class LLMConfigurationError(RuntimeError):
    pass


class BuilderPlanCallError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: dict[str, Any]):
        super().__init__(message)
        self.diagnostics = diagnostics



def _load_role_sample_directory_excerpt(sample_dir: Path, *, max_chars: int = 1200) -> str:
    if not sample_dir.exists() or not sample_dir.is_dir():
        return ""
    chunks: list[str] = []
    for path in sorted(sample_dir.glob("sample_*.md")):
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not text:
            continue
        cleaned = "\n".join(line.rstrip() for line in text.splitlines()[:80]).strip()
        if cleaned:
            chunks.append(f"[{path.name}]\n{cleaned}")
    combined = "\n\n".join(chunks).strip()
    if len(combined) <= max_chars:
        return combined
    return combined[:max_chars].rstrip() + "\n..."


def _truncate_block(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n..."


def _load_orchestrator_role_training_excerpt(max_chars: int = 2200) -> str:
    doc_budget = int(max_chars * 0.62)
    sample_budget = max_chars - doc_budget - 40
    doc_chunks: list[str] = []
    for path in ORCHESTRATOR_ROLE_DOC_PATHS:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not text:
            continue
        cleaned = "\n".join(line.rstrip() for line in text.splitlines()[:80]).strip()
        if cleaned:
            doc_chunks.append(f"[{path.name}]\n{cleaned}")
    doc_text = _truncate_block("\n\n".join(doc_chunks).strip(), doc_budget)
    sample_excerpt = _load_role_sample_directory_excerpt(
        Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "2_orchestrator" / "samples",
        max_chars=max(sample_budget, 400),
    )
    chunks = [chunk for chunk in [doc_text, f"[samples]\n{sample_excerpt}" if sample_excerpt else ""] if chunk]
    combined = "\n\n".join(chunks).strip()
    return _truncate_block(combined, max_chars)


def _load_mainhand_role_training_excerpt(max_chars: int = 2600) -> str:
    doc_budget = int(max_chars * 0.62)
    sample_budget = max_chars - doc_budget - 40
    doc_chunks: list[str] = []
    for path in MAINHAND_ROLE_DOC_PATHS:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not text:
            continue
        cleaned = "\n".join(line.rstrip() for line in text.splitlines()[:100]).strip()
        if cleaned:
            doc_chunks.append(f"[{path.name}]\n{cleaned}")
    doc_text = _truncate_block("\n\n".join(doc_chunks).strip(), doc_budget)
    sample_excerpt = _load_role_sample_directory_excerpt(
        Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "3_mainhand" / "samples",
        max_chars=max(sample_budget, 500),
    )
    chunks = [chunk for chunk in [doc_text, f"[samples]\n{sample_excerpt}" if sample_excerpt else ""] if chunk]
    combined = "\n\n".join(chunks).strip()
    return _truncate_block(combined, max_chars)


def _load_reviewer_role_training_excerpt(max_chars: int = 1500) -> str:
    doc_budget = int(max_chars * 0.62)
    sample_budget = max_chars - doc_budget - 40
    doc_chunks: list[str] = []
    for path in REVIEWER_ROLE_DOC_PATHS:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not text:
            continue
        cleaned = "\n".join(line.rstrip() for line in text.splitlines()[:90]).strip()
        if cleaned:
            doc_chunks.append(f"[{path.name}]\n{cleaned}")
    doc_text = _truncate_block("\n\n".join(doc_chunks).strip(), doc_budget)
    sample_excerpt = _load_role_sample_directory_excerpt(
        Path(__file__).resolve().parents[2] / "memory_v3" / "70_runtime_roles" / "codex_zone" / "4_reviewer" / "samples",
        max_chars=max(sample_budget, 400),
    )
    chunks = [chunk for chunk in [doc_text, f"[samples]\n{sample_excerpt}" if sample_excerpt else ""] if chunk]
    combined = "\n\n".join(chunks).strip()
    return _truncate_block(combined, max_chars)

class JsonExtractionError(ValueError):
    def __init__(self, message: str, *, raw_text: str = ""):
        super().__init__(message)
        self.raw_text = raw_text


class LLMInterface:
    def is_configured(self) -> bool:
        return bool(runtime_settings.api_key)

    def _build_client(self, role: str) -> OpenAI:
        settings = get_agent_settings(role)
        if not settings.api_key:
            raise LLMConfigurationError(f"LLM provider is not configured for role '{role}'.")
        return OpenAI(api_key=settings.api_key, base_url=settings.base_url, timeout=REQUEST_TIMEOUT_SECONDS, max_retries=MAX_RETRIES)

    def chat(
        self,
        *,
        role: str,
        system_prompt: str,
        user_prompt: str,
        stream_event_callback: StreamEventCallback | None = None,
    ) -> str:
        settings = get_agent_settings(role)
        client = self._build_client(role)
        request_kwargs = {
            "model": settings.model,
            "temperature": settings.temperature,
            "timeout": REQUEST_TIMEOUT_SECONDS,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if stream_event_callback is None:
            response = client.chat.completions.create(**request_kwargs)
            content = response.choices[0].message.content
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, Iterable):
                parts: list[str] = []
                for item in content:
                    text = getattr(item, "text", None)
                    if text:
                        parts.append(text)
                return "\n".join(parts).strip()
            return str(content).strip()

        stream_event_callback(
            {
                "event_type": "llm_stream_started",
                "status": "running",
                "summary": f"{role} started direct model streaming.",
                "metadata": {
                    "stream_source": "direct_llm",
                    "role": role,
                    "provider": settings.provider,
                    "model": settings.model,
                },
            }
        )
        parts: list[str] = []
        try:
            stream = client.chat.completions.create(stream=True, **request_kwargs)
            for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                delta = getattr(choices[0], "delta", None)
                if delta is None:
                    continue
                content_delta = getattr(delta, "content", None)
                if isinstance(content_delta, str) and content_delta:
                    parts.append(content_delta)
                    stream_event_callback(
                        {
                            "event_type": "llm_text_delta",
                            "status": "running",
                            "summary": content_delta[:160],
                            "metadata": {
                                "stream_source": "direct_llm",
                                "role": role,
                                "provider": settings.provider,
                                "model": settings.model,
                                "delta_chars": len(content_delta),
                            },
                        }
                    )
            final_text = "".join(parts).strip()
            stream_event_callback(
                {
                    "event_type": "llm_assistant_message",
                    "status": "completed",
                    "summary": final_text[:220],
                    "metadata": {
                        "stream_source": "direct_llm",
                        "role": role,
                        "provider": settings.provider,
                        "model": settings.model,
                        "message_chars": len(final_text),
                    },
                }
            )
            stream_event_callback(
                {
                    "event_type": "llm_stream_completed",
                    "status": "completed",
                    "summary": f"{role} finished direct model streaming.",
                    "metadata": {
                        "stream_source": "direct_llm",
                        "role": role,
                        "provider": settings.provider,
                        "model": settings.model,
                        "message_chars": len(final_text),
                    },
                }
            )
            return final_text
        except Exception as exc:
            stream_event_callback(
                {
                    "event_type": "llm_stream_error",
                    "status": "failed",
                    "summary": str(exc)[:220],
                    "metadata": {
                        "stream_source": "direct_llm",
                        "role": role,
                        "provider": settings.provider,
                        "model": settings.model,
                        "error_type": type(exc).__name__,
                    },
                }
            )
            raise

    def should_mock(self, role: str) -> bool:
        settings = get_agent_settings(role)
        return settings.mode == "mock" or not settings.api_key

    def mock_scan(self, goal: str, task_card: dict[str, Any]) -> str:
        scope = ", ".join(task_card.get("scope", []))
        return (
            f"Mock scan for '{goal}'. Relevant scope: {scope}. "
            "Focus on minimal safe implementation boundaries, impacted files, and validation entry points."
        )

    def mock_plan(self, goal: str, scan_result: str, review_feedback: str = "") -> str:
        payload: dict[str, Any] = {
            "summary": f"Mock build plan for: {goal}",
            "implementation_steps": [
                "Inspect the relevant module boundaries.",
                "Implement the smallest safe change for the stated goal.",
                "Run targeted validation and record findings.",
            ],
            "risks": [
                "This is a mock response because no real LLM configuration was detected.",
            ],
            "validation_checks": [
                "Review impacted files.",
                "Run focused smoke checks.",
            ],
            "change_plan": {
                "changes": [
                    {
                        "target": "src/ai_dev_os",
                        "action_type": "edit_file",
                        "why": "Implement the smallest safe change for the goal.",
                        "risk_level": "medium",
                    }
                ],
                "verification": {
                    "commands": ["python -m pytest -q"],
                    "expected_signals": ["Targeted checks pass and no new high-severity warnings appear."],
                },
                "rollback_hint": "Revert the touched files or restore the previous known-good patch.",
                "approval_policy": {
                    "default": "no_extra_approval",
                    "high_risk_actions": "require_human_approval",
                },
            },
        }
        if scan_result:
            payload["scan_context"] = scan_result[:400]
        if review_feedback:
            payload["review_feedback"] = review_feedback[:400]
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def mock_review(self, goal: str, build_result: str, execution_result: str) -> str:
        payload = {
            "decision": "approved",
            "summary": f"Mock review for {goal}",
            "feedback": "No major issues found in mock mode.",
            "issues": [],
            "validation_gaps": [],
        }
        if not build_result:
            payload["decision"] = "changes_requested"
            payload["feedback"] = "Build result is empty."
            payload["issues"] = ["Missing build plan."]
        if not execution_result:
            payload["validation_gaps"] = ["Execution result is empty."]
        return json.dumps(payload, ensure_ascii=False, indent=2)


llm_interface = LLMInterface()


def current_model_source(role: str) -> dict[str, Any]:
    return model_source_for_role(role)


def _unwrap_agent_json_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Model response does not contain a JSON object.")
    structured_output = payload.get("structured_output")
    if isinstance(structured_output, dict):
        return structured_output
    return payload


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    try:
        return _unwrap_agent_json_payload(json.loads(text))
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    candidate_error: ValueError | None = None
    for start, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            try:
                return _unwrap_agent_json_payload(payload)
            except ValueError as exc:
                candidate_error = exc
                continue

    if candidate_error is not None:
        raise candidate_error
    raise ValueError("Model response does not contain a JSON object.")




def _is_architecture_decision_goal(goal: str) -> bool:
    goal_lower = goal.lower()
    return any(
        token in goal_lower
        for token in (
            "是否应该",
            "要不要",
            "未来方向",
            "系统方向",
            "架构决策",
            "直接决定",
            "决定未来方向",
            "合并成一个角色",
            "保持分离",
        )
    )


def _should_force_orchestrator_clarification(goal: str) -> bool:
    text = (goal or "").strip()
    if not text:
        return True
    broad_phrases = (
        "整体优化",
        "全面优化",
        "整体梳理",
        "梳理一下",
        "优化一下",
        "完善一下",
        "看一下怎么优化",
        "看看怎么优化",
        "都优化",
        "全部优化",
        "系统优化",
        "整体提升",
    )
    if any(token in text for token in broad_phrases):
        return True
    if len(text) <= 14 and any(token in text for token in ("优化", "梳理", "完善", "提升", "改进", "看看")):
        return True
    return False


def _should_force_orchestrator_split(goal: str) -> bool:
    text = (goal or "").strip()
    if not text:
        return False
    if _is_architecture_decision_goal(text):
        return False
    action_verb_matches = re.findall(
        r"(新增|增加|修改|重构|拆分|梳理|优化|修复|补充|接入|实现|整理|更新|迁移|合并|显示|支持)",
        text,
    )
    connector_hits = sum(text.count(token) for token in ("同时", "并且", "另外", "以及", "再", "顺便"))
    if "；" in text or ";" in text or "\n" in text:
        connector_hits += 1
    distinct_actions = len(set(action_verb_matches))
    return connector_hits > 0 and distinct_actions >= 2


def _is_explicit_ui_task(task_kind: str, goal: str, default_scope: list[str]) -> bool:
    kind = str(task_kind or "").strip().lower()
    if kind in {"ui_panel_update", "ui_update", "dashboard_update", "cockpit_update"}:
        return True
    goal_lower = (goal or "").lower()
    ui_goal_tokens = (
        "页面",
        "卡片",
        "摘要区块",
        "首页",
        "dashboard",
        "cockpit",
        "ui",
        "界面",
        "展示",
        "渲染",
    )
    if any(token in goal_lower for token in ui_goal_tokens):
        return True
    return False

def build_builder_prompt(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
) -> str:
    task_profile = str(task_card.get("task_profile", "routine") or "routine").strip().lower()
    execution_scope_value = task_card.get("execution_scope") or {}
    if isinstance(execution_scope_value, dict):
        execution_scope = dict(execution_scope_value)
    elif execution_scope_value:
        execution_scope = {"mode": str(execution_scope_value)}
    else:
        execution_scope = {}
    max_core_files = int(execution_scope.get("max_core_files", 0) or 0)
    single_file_bias = max_core_files <= 2
    training_excerpt = _load_mainhand_role_training_excerpt()
    role_memory_context = build_role_memory_context("builder", task_type=task_profile, max_chars=1200, include_seed=False, include_config=False)
    if task_profile == "routine":
        return f"""
你是 AI Dev OS 母本中的 Builder Agent。
你的任务是基于任务卡和真实文件扫描结果，生成最小、可执行、grounded 的实现方案。
不要输出闲聊，不要输出 markdown 代码块。必须只返回 JSON 对象。

JSON 字段必须包含：
- summary: 字符串
- implementation_steps: 字符串数组
- risks: 字符串数组
- validation_checks: 字符串数组
- change_plan: 对象，包含 changes / verification / rollback_hint / approval_policy
- direct_execution: 布尔值；如果你已直接在工作区完成源码改动，设为 true
- status: 字符串；completed / checkpoint / rejected 三选一

如果输出 checkpoint，还必须额外包含：
- checkpoint_required: true
- checkpoint: 对象，至少包含 reason / question / options

项目目标:
{goal}

任务卡:
{json.dumps(task_card, ensure_ascii=False, indent=2)}

扫描结果:
{scan_result}

当前工作台:
{builder_working_state or '暂无可用工作台。按任务卡和真实文件扫描建立最小实现闭环。'}

返工轮次:
{rework_count}

审查反馈:
{review_feedback or '无'}

角色训练样板摘要（优先对齐这些行为样板，不要自创新风格）:
{training_excerpt or '暂无角色样板摘要。'}

角色运行记忆摘要（只吸收与当前任务最相关的短记忆，不要被旧上下文带偏）:
{role_memory_context or '暂无角色运行记忆摘要。'}

要求:
1. 先吸收当前工作台和扫描结果，再决定方案；优先延续已确认的 anchors / data sources / repair hints。
2. 只做最小必要改动，不扩张范围，不额外设计新功能。
3. 优先直接在真实文件里完成最小改动；不要把主要精力浪费在先构造 JSON patch 上。
4. target 必须是相对工作区路径。
5. 如果你还没有直接改文件，action_type 是 edit_file 时优先给可执行 payload：
   - edit_mode: "replace_text" 或 "append"
   - replace_text 必须提供 old_text / new_text
   - append 必须提供 append_text
6. 如果 action_type 是 edit_file 且 edit_mode=replace_text，old_text 必须来自真实文件里的精确原文；找不到锚点就不要编造 patch。
7. 如果你已经直接改好了文件，可以把 BUILD_PLAN 当事后记录；此时 direct_execution=true，change_plan.changes 可以为空或只保留轻量记录，但 changed_files 必须列出真实的 workspace 相对路径，verification_status 必须明确写 passed / failed / partial，summary 必须反映真实施工结果。
8. 如果 direct_execution=true，禁止在 change_plan 里使用 planned_change_1 这类占位 target；如需保留 changes 记录，target 必须是真实相对路径。
9. 如果任务卡没有允许新增 API / endpoint / route，就不要新造。
10. validation_checks 和 change_plan.verification 只写可执行、最小化的检查项。
11. rollback_hint 要具体；approval_policy 要短、硬、可执行。
12. 如果这轮遇到已知失败点，明确避开，不要换个写法重复踩坑。
13. 如果任务卡或 execution_scope 显示这是单文件/低扩散任务，默认工作模式应收敛在任务卡已点名的目标文件及其直接 grounding 片段上；不要为了“更稳妥”主动扩大扫描面。
14. 对 routine 小任务，默认先基于已有 grounding 直接完成最小实现；只有在缺少关键锚点、缺少真实数据入口、或任务卡明确涉及配置链/运行链时，才扩读第二个文件。
15. 如果你决定突破默认收敛范围，必须在 implementation_steps 或 risks 里用一句短话说明原因；没有明确理由，就不要扩读 `config/agents.json`、`src/ai_dev_os/agent_settings.py` 等支撑文件。
16. 这些约束是默认工作模式，不是绝对禁令；当真实锚点明确表明需要联动第二个文件时，可以扩展，但要保持改动面最小。
17. 即使只改一个文件，也要守最小范围：不要顺手清理同文件里与当前 goal 无关的文案、标签、命名、注释或历史残留；只有 acceptance 或真实锚点明确要求时，才允许碰这些非目标片段。
18. 如果你发现同文件里存在顺手可修的旧问题，默认保持不动；最多在 risks 或 summary 里提一句，不要借本任务一并清理。
19. 如果 task_card 缺关键字段、goal 不可执行、forbidden 与 acceptance 明显冲突、或目标文件根本不可读，不要硬撑，直接返回 input_rejection。
20. 如果任务要求你决定系统方向、裁决保留还是删除某层、迁移关键适配层、或在多个合理实现方向中替系统拍板，这超出普通施工职责；默认不要自己拍板，直接返回 checkpoint，并把已知现场、选项、推荐理由写清楚。
21. 如果施工中遇到方向岔路、阶段完成后需要人确认、或发现任务卡前提假设有误，不要硬猜，直接返回 checkpoint。
22. checkpoint 不允许只写在 summary 或 change_plan 里；必须显式输出 `status="checkpoint"`、`checkpoint_required=true`，并提供 `checkpoint.reason`、`checkpoint.question`、`checkpoint.options`。
23. `checkpoint.question` 必须具体写出“需要人决定什么”，不要只写“需要人确认后才能继续”这类空话。
24. `checkpoint.options` 至少给 2 个可执行候选方案；如果是 A/B 取舍，直接写成 A 和 B 的完整方案描述。
25. 如果你判断任务无需 checkpoint，应显式输出 `status="completed"`，不要同时留下模糊的 checkpoint 文案。
26. 你的首要责任是把活干成；但输入不合格或需要人拍板时，必须明确回吐结构化出口，而不是把问题伪装成普通施工失败。

输出风格:
- summary 控制在 1-2 句
- implementation_steps / risks / validation_checks 都尽量短
- 不要写大而空的架构分析
- 优先 grounded、最小、可执行
""".strip()

    validation_hub_sensitive = "validation hub" in goal.lower() or "验证中心" in goal
    return f"""
你是 AI Dev OS 母本中的 Builder Agent。
你的任务是为当前项目生成结构化实现方案，不要输出闲聊，不要输出 markdown 代码块。
必须返回 JSON 对象，且只返回 JSON。
JSON 字段必须包含：
- summary: 字符串
- implementation_steps: 字符串数组
- risks: 字符串数组
- validation_checks: 字符串数组
- change_plan: 对象，包含 changes / verification / rollback_hint / approval_policy
- direct_execution: 布尔值；如果你已直接在工作区完成源码改动，设为 true

如果任务是 governance-sensitive，JSON 还必须额外包含：
- exact_allowed_roots: 字符串数组
- exact_denied_roots: 字符串数组
- canonicalization_rule: 字符串
- negative_tests: 字符串数组

如果任务是 coordination-sensitive，JSON 还必须额外包含：
- module_coordination_map: 对象，键是模块名，值是该模块职责
- integration_checkpoints: 字符串数组
- handoff_risks: 字符串数组

如果 Orchestrator 提示 `coordination_subtype=workspace_flow`，JSON 还必须额外包含：
- checkpoint_field_contracts: 字符串数组
- governance_stage_bindings: 字符串数组
- dashboard_freshness_contracts: 字符串数组
- freshness_field_bindings: 字符串数组

如果任务是 release-sensitive，JSON 还必须额外包含：
- release_readiness_checks: 字符串数组
- rollback_evidence: 字符串数组
- post_release_observation_rules: 字符串数组

如果任务是 combination-sensitive，JSON 还必须额外包含：
- priority_order: 字符串数组
- cross_capability_conflicts: 字符串数组
- stability_checks: 字符串数组
- capability_phase_ownership: 对象数组，每个对象必须包含：phase / primary_capability / secondary_capabilities / weight / reason

如果任务是 validation-hub-sensitive，JSON 还必须额外包含：
- evidence_chain_format: 字符串数组
- governance_gate_conditions: 字符串数组
- governance_binding_modes: 字符串数组

如果 contract_subtype 是 policy_schema，JSON 还必须额外包含：
- policy_document_write_surface: 字符串
- referenced_read_only_schema_surfaces: 字符串数组

项目目标:
{goal}

任务卡:
{json.dumps(task_card, ensure_ascii=False, indent=2)}

扫描结果:
{scan_result}

当前工作台（这是你在本轮闭环里的短程连续状态；优先延续这里已经确认的目标、边界、卡点和下一步，不要每轮重新发散）:
{builder_working_state or '暂无可用工作台。按任务卡与主控提示建立最小实现闭环。'}

返工轮次:
{rework_count}

审查反馈:
{review_feedback or '无'}

角色训练样板摘要（优先对齐这些行为样板，不要自创新风格）:
{training_excerpt or '暂无角色样板摘要。'}

角色运行记忆摘要（只吸收与当前任务最相关的短记忆，不要被旧上下文带偏）:
{role_memory_context or '暂无角色运行记忆摘要。'}

要求:
0. 先吸收当前工作台，再开始本轮行动。优先复用其中已经确认的 confirmed_anchors 和 grounded_data_sources，先读懂 failure_summary / repair_hints / failed_targets，避开 forbidden_patterns 与 validation_history 里已经证明失败的路线。
0.1 本轮开始前，先完成一轮最小 scout：阅读真实目标文件、确认可用锚点、确认可复用数据入口，再决定改动方案。
0.2 如果上一轮 blocked_points / validation_history 已指出失败原因，本轮必须显式避开同类错误，不要换个写法重复犯错。
0.3 如果 grounded_data_sources 已列出可复用的真实数据入口，你必须从这些入口中选择并复用；不要新造平行数据通路，也不要用 placeholder 或静态占位替代真实数据。
1. 方案必须紧扣目标，不要扩张范围。
2. 优先给出最小可落地方案。
3. 如果存在返工反馈，必须显式吸收反馈。
4. validation_checks 只写可执行检查项。
5. change_plan 必须结构化，且执行层后续会优先消费 change_plan，而不是自由文本。
6. change_plan.changes 中每一项至少说明：target / action_type / why / risk_level。
7. change_plan.verification 中至少说明：commands / expected_signals。
8. change_plan.rollback_hint 必须是明确可执行的回退提示；change_plan.approval_policy 必须说明默认审批策略和高风险动作策略。
9. 如果项目记忆里已有相关决策或历史摘要，优先延续已有方向，不要无必要改道。
10. 如果 Orchestrator 已明确边界、治理重量、禁止扩张方向，必须直接体现在方案里。
11. 如果任务是 governance-sensitive，implementation_steps 和 validation_checks 必须显式覆盖：
   - 精确允许根路径
   - 精确拒绝根路径
   - 路径规范化后再匹配
   - 至少一个负向拒绝测试
12. 如果任务是 governance-sensitive，不要把这些要求只写在 summary 里，必须把它们落实到额外 JSON 字段中。
13. 如果 Orchestrator 已经明确写出 family_semantics / approved_surface_definition / disallowed_surface_definition，必须严格按这些定义输出，不要自己改写任务家族语义。
14. governance-sensitive 任务的目标输出骨架示例：
   {{
     "summary": "...",
     "implementation_steps": ["..."],
     "risks": ["..."],
     "validation_checks": ["..."],
     "exact_allowed_roots": ["/runtime/projects/*/memory/"],
     "exact_denied_roots": ["/mother_memory/"],
     "denied_path_tokens": ["doctrine/"],
     "canonicalization_rule": "Normalize to canonical absolute path before matching.",
     "negative_tests": ["..."],
     "positive_tests": ["..."]
   }}
15. 如果 contract_subtype 是 policy_schema，必须显式区分：
   - policy_document_write_surface：策略文档真正写入的 project-local 路径
   - referenced_read_only_schema_surfaces：只读引用的 config schema surface
   这两者不能混成同一个 surface。

16. 如果任务是 combination-sensitive，capability_phase_ownership 的标准示例必须类似：
   [
     {{
       "phase": "phase_1",
       "primary_capability": "memory_retrieval",
       "secondary_capabilities": ["release_sensitive"],
       "weight": 0.6,
       "reason": "This phase is driven by evidence retrieval before release validation."
     }}
   ]
   不允许只返回纯字符串数组。
17. 如果任务是 coordination-sensitive，必须显式给出：
   - module_coordination_map：至少覆盖 3 个协调模块或职责面
   - integration_checkpoints：说明跨模块交接怎么验
   - handoff_risks：说明最可能导致返工的交接风险
18. 如果任务是 release-sensitive，必须显式给出：
   - release_readiness_checks：发布前必须满足的检查项
   - rollback_evidence：回滚就绪需要什么证据
   - post_release_observation_rules：发布后怎么观察效果
19. 如果任务是 combination-sensitive，必须显式给出：
   - priority_order：本任务里几种能力面谁先谁后
   - cross_capability_conflicts：哪些能力要求可能互相拉扯
20. 如果 action_type 是 edit_file，payload 必须可被执行层直接消费：
   - edit_mode: "replace_text" 或 "append"
   - replace_text 模式必须提供 old_text / new_text，且 old_text 必须是目标文件里可精确匹配的原文片段
   - append 模式必须提供 append_text
21. 如果 action_type 是 write_file，payload 必须提供 content；target 必须是相对工作区路径。
22. 不要只给“去修改某文件”的抽象计划；对于你真正打算执行的源码改动，change_plan.changes 里必须尽量给出最小可执行 payload。
23. 小任务优先使用 edit_file + replace_text，避免大块重写整个文件。
24. 在输出任何 edit_file payload 之前，必须先基于真实工作区文件做 grounding：
24.a edit_file 的 target 也必须是相对工作区路径，例如 scripts/dashboard_server.py；不要只写裸文件名 dashboard_server.py。
24.b 如果同名文件在工作区里可能有多个位置，必须写出能唯一定位的相对路径；不要把路径解析交给执行层猜。
   - 先读取目标文件，再决定 edit payload。
   - 不要凭想象发明函数名、模板名、变量名或旧文本片段。
   - old_text 必须来自真实文件中可精确匹配的原文，而不是你推测文件“应该长什么样”。
25. 如果你无法从真实文件中找到稳定锚点，不要硬写 payload。此时应：
   - 在 implementation_steps 里明确写出需要先定位哪个真实块；
   - 在 risks 里说明 grounding 不足；
   - 在 change_plan.changes 中暂不输出不可执行的 replace_text patch。
26. 如果任务卡或 Orchestrator 没有允许新增 API / endpoint / route，而 grounding 片段里也没有现成接口可复用，就禁止脑补新 API。
27. 对于多条 change_plan.changes，不能只让第一条 grounded。每一条 edit_file.replace_text 的 old_text 都必须来自真实文件中的精确片段。
28. 如果 grounding 片段已经给出某一行或某一小段，优先逐字复用这些真实片段作为 old_text，不要自己改写顺序、空格、缩进或选择器排列。
29. 对 HTML/CSS/模板任务，优先使用单行精确锚点做 replace_text；只有在你能保留原始缩进和换行时，才使用多行 old_text。
30. 如果 grounding 片段里展示的是 `L114: ...` 这种真实行，请优先直接拷贝该行的原始内容作为 old_text，而不是自己重新格式化成“更漂亮”的版本。
31. Orchestrator 负责目标、边界、验收信号；你负责在这些边界内做真实文件 grounding。不要把抽象目标直接翻译成脑补代码。
32. 对单文件 UI/模板任务，如果 grounding 片段只提供了 1 个稳定精确锚点，就最多输出 1 条 grounded replace_text patch；剩余改动请写入 risks 或 implementation_steps，而不要继续猜第二、第三个 patch。
33. 如果某个额外改动点没有在真实文件里找到精确原文，请先放弃该 patch，而不是用“看起来合理”的 CSS/HTML/JS 片段去凑。
34. 如果任务要求“显示当前生效配置/现有数据源内容/真实运行态信息”，禁止用 `-`、静态占位文本、mock 数据或伪字段名冒充完成；必须复用 grounding 片段中已经出现的现有数据入口。
35. 如果 grounding 片段已经出现现有 API、现有状态对象或现有渲染函数，请优先复用它们；不要新造平行的数据通路。
   - stability_checks：如何验证这些能力组合在一起时不会漂移
   - capability_phase_ownership：明确写出 phase -> capability owner 的映射，不要只描述 capability 的功能
20. 如果 Orchestrator 已明确给出 `coordination_subtype=workspace_flow`，必须额外显式给出：
   - checkpoint_field_contracts：每个关键 checkpoint 的字段级判定标准
   - governance_stage_bindings：治理触发条件绑定到具体流程阶段
   - dashboard_freshness_contracts：dashboard 新鲜度与具体数据字段/来源的绑定关系
   - freshness_field_bindings：把 freshness contract 和具体 field contract 用显式 ID/名称绑定起来
21. 如果任务明显是在设计 validation hub / 验证中心，必须额外显式给出：
   - evidence_chain_format：定义证据链在各交接点必须具备的字段格式
   - governance_gate_conditions：定义每个 release / stage gate 的具体治理门禁条件、阈值或审批状态字段
   - governance_binding_modes：明确每个治理绑定是自动还是人工，以及触发条件是什么
""".strip()


def _rough_token_estimate(text: str) -> int:
    normalized = text or ""
    return max(1, (len(normalized) + 3) // 4) if normalized else 0


def build_builder_call_diagnostics(
    *,
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    review_feedback: str = "",
    builder_working_state: str = "",
    rework_count: int = 0,
    prompt: str | None = None,
) -> dict[str, Any]:
    prompt_text = prompt or build_builder_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    return {
        "role": "builder",
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "max_retries": MAX_RETRIES,
        "goal_chars": len(goal or ""),
        "task_card_chars": len(json.dumps(task_card, ensure_ascii=False, indent=2)),
        "scan_result_chars": len(scan_result or ""),
        "project_memory_chars": len(project_memory_context or ""),
        "orchestrator_guidance_chars": len(orchestrator_guidance or ""),
        "review_feedback_chars": len(review_feedback or ""),
        "builder_working_state_chars": len(builder_working_state or ""),
        "prompt_chars": len(prompt_text),
        "prompt_token_estimate": _rough_token_estimate(prompt_text),
        "rework_count": rework_count,
    }


def build_reviewer_prompt(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    execution_result: str,
    execution_evidence: dict[str, Any] | None = None,
    source_workspace_root: str = "",
    rework_count: int = 0,
) -> str:
    validation_hub_sensitive = "validation hub" in goal.lower() or "验证中心" in goal
    review_task_type = str(task_card.get("task_profile", "routine") or "routine").strip().lower()
    training_excerpt = _load_reviewer_role_training_excerpt()
    role_memory_context = build_role_memory_context("reviewer", task_type=review_task_type, max_chars=900, include_seed=False, include_config=False)
    governance_contract = task_card.get("governance_contract", {}) if isinstance(task_card.get("governance_contract", {}), dict) else {}
    contract_subtype = str(task_card.get("contract_subtype", "") or "").strip().lower()
    coordination_subtype = str(task_card.get("coordination_subtype", "") or "").strip().lower()
    task_text = json.dumps(task_card, ensure_ascii=False)
    lower_task_text = task_text.lower()
    governance_sensitive = "governance-sensitive" in lower_task_text or bool(governance_contract)
    coordination_sensitive = "coordination-sensitive" in lower_task_text
    release_sensitive = "release-sensitive" in lower_task_text
    combination_sensitive = "combination-sensitive" in lower_task_text
    review_requirements = [
        "1. 判断当前结果是否足以进入记录阶段。",
        "2. 你是独立审查者，只基于任务卡、执行证据、handoff packet 与只读真实工作区复核做判断；不要依赖上游过程性想法。",
        "3. 必须优先依据执行证据、真实 changed files、git diff 和最小验证结果判断，不要只依据自然语言摘要。",
        "4. 如果方案含糊、验证不充分、范围失控，就返回 changes_requested。",
        "5. 如果发现的是证据不足或 reviewer 工作区同步不足，要在 summary / feedback 里明确写成证据链问题，不要混同为功能失败。",
        "6. feedback 必须给出明确改进方向。",
        "7. 不要讨论模型限制，只做审查结论。",
        "8. 如果问题超出返工范围，或已经涉及架构边界/数据安全/审查主链方向调整，优先输出 escalation_request，而不是继续让 Builder 返工。",
    ]
    if governance_sensitive:
        review_requirements.extend([
            "8. 如果任务是 governance-sensitive，重点检查是否真的写清了：exact_allowed_roots、exact_denied_roots、canonicalization_rule、negative_tests。",
            "9. 如果 Orchestrator 已明确给出 family_semantics / approved_surface_definition / disallowed_surface_definition，重点检查 Builder 是否偏离这些家族定义。",
        ])
    if contract_subtype == "policy_schema":
        review_requirements.append("10. 如果 contract_subtype=policy_schema，重点检查 Builder 是否区分 policy_document_write_surface 与 referenced_read_only_schema_surfaces；若混层，直接判 changes_requested。")
    if coordination_sensitive:
        review_requirements.append("11. 如果任务是 coordination-sensitive，重点检查 Builder 是否明确给出了 module_coordination_map、integration_checkpoints、handoff_risks；若只有泛泛协作描述，直接判 changes_requested。")
    if release_sensitive:
        review_requirements.append("12. 如果任务是 release-sensitive，重点检查 Builder 是否明确给出了 release_readiness_checks、rollback_evidence、post_release_observation_rules；缺失则直接判 changes_requested。")
    if combination_sensitive:
        review_requirements.append("13. 如果任务是 combination-sensitive，重点检查 Builder 是否明确给出了 priority_order、cross_capability_conflicts、stability_checks、capability_phase_ownership；缺失则直接判 changes_requested。")
    if coordination_subtype == "workspace_flow":
        review_requirements.append("14. 如果 coordination_subtype=workspace_flow，重点检查 Builder 是否明确给出了 checkpoint_field_contracts、governance_stage_bindings、dashboard_freshness_contracts、freshness_field_bindings；缺失则直接判 changes_requested。")
    if validation_hub_sensitive:
        review_requirements.append("15. 如果任务明显是在设计 validation hub / 验证中心，重点检查 Builder 是否明确给出了 evidence_chain_format、governance_gate_conditions、governance_binding_modes；缺失则直接判 changes_requested。")
    review_requirements.append("16. 如果你选择升级，必须输出 escalation_request={reason, decision_required, options, recommendation, current_state}，同时将 decision 设为 escalate。")
    review_requirements_text = "\n".join(review_requirements)
    return f"""
你是 AI Dev OS 母本中的 Reviewer Agent。
你的任务是审查当前方案和执行结果。
必须返回 JSON 对象，且只返回 JSON。
JSON 字段必须包含：
- decision: 字符串，只能是 approved、changes_requested 或 escalate
- summary: 字符串
- feedback: 字符串
- issues: 字符串数组
- validation_gaps: 字符串数组

项目目标:
{goal}

任务卡:
{json.dumps(task_card, ensure_ascii=False, indent=2)}

执行结果摘要:
{execution_result}

执行证据（这是主审查依据，必须优先基于这些证据和你在只读真实工作区里的独立复核做判断）:
{json.dumps(execution_evidence or {}, ensure_ascii=False, indent=2)}

真实工作区根路径（只读复核目标）:
{source_workspace_root or "未提供；若当前后端共享工作区，则以当前工作目录为准。"}

返工轮次:
{rework_count}

角色训练样板摘要:
{training_excerpt or "暂无 Reviewer 训练样板摘要。"}

角色运行记忆摘要:
{role_memory_context or "暂无角色运行记忆摘要。"}

审查要求:
{review_requirements_text}
""".strip()


def _heuristic_orchestrator_design(goal: str, task_kind: str) -> dict[str, Any]:
    goal_lower = goal.lower()
    governance_keywords = ("doctrine", "policy", "schema", "approval", "governance", "architecture")
    evidence_keywords = ("diagnostic", "artifact", "runtime", "memory", "report", "trend", "snapshot")
    release_keywords = ("release", "rollback", "checklist")
    coordination_keywords = ("workspace", "module", "dashboard", "diagnostic", "release", "checkpoint", "coordination")

    governance_sensitive = task_kind == "system_optimization" or any(word in goal_lower for word in governance_keywords)
    evidence_sensitive = any(word in goal_lower for word in evidence_keywords)
    release_sensitive = any(word in goal_lower for word in release_keywords)
    coordination_hits = sum(1 for word in coordination_keywords if word in goal_lower)
    coordination_sensitive = coordination_hits >= 3
    coordination_subtype = ""

    scope_additions: list[str] = []
    acceptance_additions: list[str] = []
    boundary_notes: list[str] = []
    builder_brief: list[str] = []
    capability_mix: list[str] = []
    interaction_risks: list[str] = []

    if governance_sensitive:
        capability_mix.append("governance")
        scope_additions.extend(
            [
                "Clarify which policy/schema surface is allowed to change.",
                "Keep doctrine and core architecture files out of direct modification scope.",
                "State exact allowed roots and exact denied roots instead of only relative examples.",
            ]
        )
        acceptance_additions.extend(
            [
                "State explicit doctrine and architecture boundaries before implementation.",
                "Define preventative controls that stop doctrine-adjacent drift.",
                "Include at least one boundary-case validation for governance-sensitive behavior.",
                "Make path normalization explicit before allow-list or deny-list matching.",
                "Include at least one negative test proving denied-root access is blocked after canonicalization.",
            ]
        )
        boundary_notes.extend(
            [
                "Treat doctrine and architecture policy as protected unless explicitly approved.",
                "Prefer local policy/schema shaping over broad system-wide rule rewrites.",
                "For governance-sensitive tasks, exact denied roots and exact allowed roots must be provable, not only discussed.",
            ]
        )
        builder_brief.append("This task is governance-sensitive. Be explicit about what is inside and outside scope.")

    if evidence_sensitive:
        capability_mix.append("retrieval_evidence")
        scope_additions.append("Define the minimum local evidence sources that the feature is allowed to read.")
        acceptance_additions.extend(
            [
                "Use field-level validation checks instead of only high-level summary checks.",
                "Describe behavior for missing, empty, or malformed local runtime inputs.",
            ]
        )
        boundary_notes.append("Keep evidence handling local to runtime/projects and mother_memory inputs already allowed.")
        builder_brief.append("This task needs concrete validation detail and edge-case handling.")

    if release_sensitive:
        capability_mix.append("release")
        acceptance_additions.append("Make rollback readiness and release review criteria explicit.")
        builder_brief.append("Release-related logic should stay checklist-oriented and reversible.")

    if coordination_sensitive:
        capability_mix.append("coordination")
        if "workspace" in goal_lower:
            coordination_subtype = "workspace_flow"
        scope_additions.extend(
            [
                "Define the main coordination surfaces across modules before implementation.",
                "Make module handoff boundaries explicit instead of only naming modules loosely.",
            ]
        )
        acceptance_additions.extend(
            [
                "State at least one integration checkpoint for each critical handoff.",
                "Describe the module coordination map clearly enough that a later execution step can follow it.",
            ]
        )
        if coordination_subtype == "workspace_flow":
            scope_additions.extend(
                [
                    "Define checkpoint_field_contracts for each critical integration checkpoint.",
                    "Bind governance review triggers to explicit workflow stages rather than generic review notes.",
                    "Define dashboard_freshness_contracts tied to specific data fields and sources.",
                ]
            )
            acceptance_additions.extend(
                [
                    "Checkpoint contracts specify explicit field-level pass/fail rules.",
                    "Governance triggers are stage-bound instead of loosely attached to the whole flow.",
                    "Dashboard freshness is tied to explicit data contracts rather than generic recency language.",
                ]
            )
            builder_brief.append("This coordination task is a workspace_flow subtype. Make field-level checkpoints, governance-stage bindings, and dashboard freshness contracts explicit.")
        boundary_notes.append("Do not leave module ownership or integration checkpoints implicit in medium-project workspace tasks.")
        builder_brief.append("This task is coordination-sensitive. Make module roles, handoffs, and integration checkpoints explicit.")

    combination_sensitive = len(set(capability_mix)) >= 2
    if combination_sensitive:
        scope_additions.append("Decide which capability surface is primary and which are supporting, instead of treating all of them as equal.")
        acceptance_additions.extend(
            [
                "State the priority order across combined capability surfaces.",
                "Describe at least one conflict between capability surfaces and how to keep the task stable anyway.",
                "Define explicit stability checks for the combined task, not just family-specific checks.",
            ]
        )
        boundary_notes.append("Combined medium-project tasks must not flatten retrieval, coordination, and release concerns into one undifferentiated plan.")
        builder_brief.append("This task is combination-sensitive. Make the primary capability, support capabilities, conflict points, and stability checks explicit.")
        if "retrieval_evidence" in capability_mix and "coordination" in capability_mix:
            interaction_risks.append("retrieval detail may be too broad and blur module ownership or handoff boundaries")
        if "coordination" in capability_mix and "release" in capability_mix:
            interaction_risks.append("coordination design may skip concrete release checkpoints if module flow dominates")
        if "retrieval_evidence" in capability_mix and "release" in capability_mix:
            interaction_risks.append("release readiness may become generic if evidence retrieval is not tied to explicit release gates")

    if combination_sensitive:
        task_profile = "combination_sensitive"
    elif governance_sensitive:
        task_profile = "governance_sensitive"
    elif coordination_sensitive:
        task_profile = "coordination_sensitive"
    elif evidence_sensitive:
        task_profile = "evidence_sensitive"
    elif release_sensitive:
        task_profile = "release_sensitive"
    else:
        task_profile = "routine"

    governance_weight = "high" if governance_sensitive else "attention" if evidence_sensitive or release_sensitive or coordination_sensitive else "routine"
    return {
        "task_profile": task_profile,
        "governance_weight": governance_weight,
        "combination_sensitive": combination_sensitive,
        "coordination_subtype": coordination_subtype,
        "capability_mix": list(dict.fromkeys(capability_mix)),
        "interaction_risks": interaction_risks,
        "scope_additions": scope_additions,
        "acceptance_additions": acceptance_additions,
        "boundary_notes": boundary_notes,
        "builder_brief": " ".join(builder_brief) or "Keep scope minimal and concrete.",
        "approval_hint": governance_weight == "high",
    }


def build_orchestrator_opencode_prompt(
    project_id: str,
    goal: str,
    *,
    task_kind: str,
    default_scope: list[str],
    default_acceptance_criteria: list[str],
    default_assigned_agents: list[str],
    mother_memory_context: str = "",
    scene_scan_excerpt: str = "",
) -> str:
    memory_excerpt = (mother_memory_context or "暂无可用母体记忆。").strip()
    role_memory_context = build_role_memory_context("orchestrator", task_type=task_kind or "routine", max_chars=1000, include_seed=False, include_config=False)
    if len(memory_excerpt) > 1200:
        memory_excerpt = memory_excerpt[:1200] + "..."
    training_excerpt = _load_orchestrator_role_training_excerpt()
    return f"""
你是 AI Dev OS 的 Orchestrator。
你的唯一任务：根据下面信息，输出一个任务塑形 JSON。
不要解释，不要寒暄，不要输出 markdown 代码块，只返回一个 JSON 对象。

允许四种输出：
- 标准 task shaping JSON
- clarification_request JSON（目标模糊、前提不成立时）
- split_request JSON（多个独立任务混在一起时）
- escalation_request JSON（超出 Orchestrator 判断边界、需要人拍板时）

标准 task shaping JSON 必须包含字段：
- task_profile
- governance_weight
- combination_sensitive
- coordination_subtype
- capability_mix
- interaction_risks
- scope_additions
- acceptance_additions
- boundary_notes
- builder_brief
- approval_hint

字段约束：
- task_profile 只能是 routine / evidence_sensitive / release_sensitive / coordination_sensitive / governance_sensitive / combination_sensitive
- governance_weight 只能是 routine / attention / high
- combination_sensitive 和 approval_hint 必须是布尔值
- 其余列表字段必须返回字符串数组
- builder_brief 必须是一句简短、可执行、低噪音的边界提醒，不要写实现步骤

项目 ID: {project_id}
项目目标: {goal}
任务类型: {task_kind}
默认 scope: {json.dumps(default_scope, ensure_ascii=False)}
默认 acceptance: {json.dumps(default_acceptance_criteria, ensure_ascii=False)}
默认 assigned_agents: {json.dumps(default_assigned_agents, ensure_ascii=False)}
母体记忆摘要: {memory_excerpt}
真实工程现场摘要: {(scene_scan_excerpt or "暂无可用现场摘要。").strip()}
角色训练样板摘要: {training_excerpt or "暂无角色样板摘要。"}
角色运行记忆摘要: {role_memory_context or "暂无角色运行记忆摘要。"}

输出风格要求：
- 目标模糊时优先 clarification_request，不要强行出卡
- 多个独立目标时优先 split_request，不要塞成一张卡
- 优先最小塑形，不要扩张任务
- 如果没有特别强的治理信号，就保持 routine
- 不要发明不存在的高风险
- builder_brief 要短，不超过 30 个英文词或 40 个汉字；它只描述目标、边界、完成信号，不描述步骤
- 先吸收最小必要记忆，再自查少量关键项目文件；不要在不了解真实结构时直接空塑形
- 如果任务与驾驶舱/UI/配置展示有关，优先自查：
  - .role/ROLE_IDENTITY.md
  - .role/ROLE_ONBOARDING_PACKET.md
  - config/agents.json
  - scripts/dashboard_server.py
  - src/ai_dev_os/agent_settings.py
- 只把“你确认过的真实结构”转成 scope / acceptance / boundary；不要把猜测写进任务卡

输出示例骨架：
{{
  "task_profile": "routine",
  "governance_weight": "routine",
  "combination_sensitive": false,
  "coordination_subtype": "",
  "capability_mix": [],
  "interaction_risks": [],
  "scope_additions": [],
  "acceptance_additions": [],
  "boundary_notes": [],
  "builder_brief": "Keep scope minimal and concrete.",
  "approval_hint": false
}}
"""


def build_orchestrator_prompt(
    project_id: str,
    goal: str,
    *,
    task_kind: str,
    default_scope: list[str],
    default_acceptance_criteria: list[str],
    default_assigned_agents: list[str],
    mother_memory_context: str = "",
    scene_scan_excerpt: str = "",
) -> str:
    training_excerpt = _load_orchestrator_role_training_excerpt()
    role_memory_context = build_role_memory_context("orchestrator", task_type=task_kind or "routine", max_chars=1000, include_seed=False, include_config=False)
    return f"""
你是 AI Dev OS 母本中的 Orchestrator。
你的任务不是自由扩张任务，而是把任务卡塑形得更清楚、更不容易返工。
必须只返回 JSON 对象。

允许四种输出：
- 标准 task shaping JSON
- clarification_request JSON（目标模糊、前提不成立时）
- split_request JSON（多个独立任务混在一起时）
- escalation_request JSON（超出 Orchestrator 判断边界、需要人拍板时）

标准 task shaping JSON 字段必须包含：
- task_profile: 字符串，只能是 routine / evidence_sensitive / release_sensitive / coordination_sensitive / governance_sensitive / combination_sensitive
- governance_weight: 字符串，只能是 routine / attention / high
- combination_sensitive: 布尔值
- coordination_subtype: 字符串，可为空；若存在，只能用于更细的协调型任务说明
- capability_mix: 字符串数组
- interaction_risks: 字符串数组
- scope_additions: 字符串数组
- acceptance_additions: 字符串数组
- boundary_notes: 字符串数组
- builder_brief: 字符串
- approval_hint: 布尔值

项目 ID:
{project_id}

项目目标:
{goal}

当前任务类型:
{task_kind}

默认 scope:
{json.dumps(default_scope, ensure_ascii=False, indent=2)}

默认 acceptance_criteria:
{json.dumps(default_acceptance_criteria, ensure_ascii=False, indent=2)}

默认 assigned_agents:
{json.dumps(default_assigned_agents, ensure_ascii=False, indent=2)}

母体记忆（包含设计原则、当前阶段和试生产复盘）:
{mother_memory_context or '暂无可用母体记忆。'}

真实工程现场摘要（优先依据这个判断当前代码现状，而不是只依据旧记忆）:
{scene_scan_excerpt or '暂无可用现场摘要。'}

角色训练样板摘要（优先对齐这些行为样板，不要自创新风格）:
{training_excerpt or '暂无角色样板摘要。'}

角色运行记忆摘要:
{role_memory_context or '暂无角色运行记忆摘要。'}

要求：
1. 只做任务塑形，不要改任务目标本身。
2. 如果目标模糊、范围过宽、前提不成立，优先返回 clarification_request。
3. 如果输入包含多个独立目标，优先返回 split_request。
4. 如果任务涉及系统方向、架构取舍、超出 Orchestrator 能安全裁决的边界，返回 escalation_request。
2. 如果任务碰 doctrine / policy / schema / approval / architecture，必须提高治理敏感度。
3. 如果任务碰 diagnostics / runtime artifact / memory / report / trend，必须把验证标准收紧到字段级或边界级。
4. 不要输出泛泛建议，输出必须能直接合并进 task card。`Orchestrator` 不是实现者，不要替 builder 设计步骤级方案。
5. builder_brief 必须是一段简短、可执行的边界提醒，只负责说清目标、边界、验收信号，不负责给实现步骤。
6. 如果任务属于 governance-sensitive，优先把“精确允许根路径 / 精确拒绝根路径 / 规范化后再匹配 / 负向测试”塑形进任务卡。
7. 如果任务属于 coordination-sensitive，优先把“模块分工 / 交接边界 / 集成检查点 / 交接风险”塑形进任务卡。
8. 如果任务属于 combination-sensitive，优先明确：
   - 这是哪几类能力组合在一起
   - 谁是主能力，谁是辅助能力
   - 这些能力组合时最容易产生什么冲突
   - 应该如何做稳定性检查
9. 如果任务属于 workspace_flow 这一类协调任务，还应明确：
   - integration checkpoint 的字段级判定标准
   - governance review 触发在哪些具体阶段
   - dashboard freshness 绑定到哪些具体字段和来源
""".strip()



def get_orchestrator_task_design(
    *,
    project_id: str,
    goal: str,
    task_kind: str,
    default_scope: list[str],
    default_acceptance_criteria: list[str],
    default_assigned_agents: list[str],
    mother_memory_context: str = "",
    scene_scan_excerpt: str = "",
    stream_event_callback: StreamEventCallback | None = None,
) -> dict[str, Any]:
    if llm_interface.should_mock("orchestrator"):
        return _heuristic_orchestrator_design(goal, task_kind)

    prompt = build_orchestrator_prompt(
        project_id=project_id,
        goal=goal,
        task_kind=task_kind,
        default_scope=default_scope,
        default_acceptance_criteria=default_acceptance_criteria,
        default_assigned_agents=default_assigned_agents,
        mother_memory_context=mother_memory_context,
        scene_scan_excerpt=scene_scan_excerpt,
    )
    orchestrator_settings = get_agent_settings("orchestrator")
    backend = str(orchestrator_settings.backend or "llm").strip().lower() or "llm"
    try:
        if backend == "claude_code":
            raw, _ = run_claude_code_prompt(
                role="orchestrator",
                system_prompt="You are the Orchestrator for an AI Dev OS kernel. Return only valid JSON.",
                user_prompt=prompt,
                cwd=orchestrator_settings.workspace_root,
                session_id=orchestrator_settings.session_id,
                continue_session=False,
                append_system_prompt=f"Role memory root: {orchestrator_settings.memory_root}",
                json_schema=ORCHESTRATOR_JSON_SCHEMA,
                model=orchestrator_settings.model,
                base_url=orchestrator_settings.base_url,
                api_key=orchestrator_settings.api_key,
                stream_event_callback=stream_event_callback,
            )
        elif backend == "opencode":
            raw, _ = run_opencode_prompt(
                role="orchestrator",
                system_prompt="Return only one valid JSON object for task shaping. No markdown. No commentary.",
                user_prompt=build_orchestrator_opencode_prompt(
                    project_id=project_id,
                    goal=goal,
                    task_kind=task_kind,
                    default_scope=default_scope,
                    default_acceptance_criteria=default_acceptance_criteria,
                    default_assigned_agents=default_assigned_agents,
                    mother_memory_context=mother_memory_context,
                    scene_scan_excerpt=scene_scan_excerpt,
                ),
                cwd=orchestrator_settings.workspace_root,
                model=orchestrator_settings.model,
                provider=orchestrator_settings.provider,
                base_url=orchestrator_settings.base_url,
                api_key=orchestrator_settings.api_key,
                memory_root=orchestrator_settings.memory_root,
                stream_event_callback=stream_event_callback,
            )
        elif backend == "qwen_code":
            raw, _ = run_qwen_code_prompt(
                role="orchestrator",
                system_prompt="Return only one valid JSON object for task shaping. No markdown. No commentary.",
                user_prompt=build_orchestrator_opencode_prompt(
                    project_id=project_id,
                    goal=goal,
                    task_kind=task_kind,
                    default_scope=default_scope,
                    default_acceptance_criteria=default_acceptance_criteria,
                    default_assigned_agents=default_assigned_agents,
                    mother_memory_context=mother_memory_context,
                    scene_scan_excerpt=scene_scan_excerpt,
                ),
                cwd=orchestrator_settings.workspace_root,
                model=orchestrator_settings.model,
                base_url=orchestrator_settings.base_url,
                api_key=orchestrator_settings.api_key,
                memory_root=orchestrator_settings.memory_root,
                stream_event_callback=stream_event_callback,
                task_packet_name="QWEN_ORCHESTRATOR_TASK.md",
                output_filename="ORCHESTRATOR_TASK_SHAPING.json",
                assistant_sentinel="ORCHESTRATOR_JSON_WRITTEN",
                execution_contract_lines=[
                    "Do not modify any source files for this task shaping step.",
                    "You may inspect local files if needed, but only return the shaping JSON contract.",
                    "Keep builder_brief short, hard, and directly actionable.",
                    "Before shaping, read .role/ROLE_IDENTITY.md and .role/ROLE_ONBOARDING_PACKET.md.",
                    "For cockpit/UI/config tasks, inspect config/agents.json, scripts/dashboard_server.py, and src/ai_dev_os/agent_settings.py before deciding scope.",
                    "Use those files to learn the real structure, then return the shaping JSON only.",
                ],
                approval_mode="yolo",
            )
        elif backend == "codex":
            raw, _ = run_codex_prompt(
                role="orchestrator",
                system_prompt="Return only one valid JSON object for task shaping. No markdown. No commentary.",
                user_prompt=build_orchestrator_opencode_prompt(
                    project_id=project_id,
                    goal=goal,
                    task_kind=task_kind,
                    default_scope=default_scope,
                    default_acceptance_criteria=default_acceptance_criteria,
                    default_assigned_agents=default_assigned_agents,
                    mother_memory_context=mother_memory_context,
                    scene_scan_excerpt=scene_scan_excerpt,
                ),
                cwd=orchestrator_settings.workspace_root,
                model=orchestrator_settings.model,
                provider=orchestrator_settings.provider,
                base_url=orchestrator_settings.base_url,
                api_key=orchestrator_settings.api_key,
                memory_root=orchestrator_settings.memory_root,
                stream_event_callback=stream_event_callback,
                task_packet_name="CODEX_ORCHESTRATOR_TASK.md",
                output_filename="ORCHESTRATOR_TASK_SHAPING.json",
                assistant_sentinel="ORCHESTRATOR_JSON_WRITTEN",
                execution_contract_lines=[
                    "Do not modify any source files for this task shaping step.",
                    "You may inspect local files if needed, but only return the shaping JSON contract.",
                    "Keep builder_brief short, hard, and directly actionable.",
                    "Align first to the injected orchestrator role samples, lessons, and config summary before shaping.",
                    "Read only the minimum targeted files needed for this specific task; do not expand scanning just to feel safer.",
                    "For cockpit/UI/config tasks, default to the smallest relevant subset of: scripts/dashboard_server.py, src/ai_dev_os/agent_settings.py, config/agents.json.",
                    "Use local role/example files only if AGENTS.md points you there or the targeted scan is insufficient.",
                    "Do not do broad repo exploration for task shaping unless the targeted scan is insufficient.",
                    "Use those files to learn the real structure, then return the shaping JSON only.",
                ],
                reasoning_effort="medium",
                sandbox_mode="workspace-write",
                dangerously_bypass_approvals_and_sandbox=True,
            )
        else:
            raw = llm_interface.chat(
                role="orchestrator",
                system_prompt="You are the Orchestrator for an AI Dev OS kernel. Return only valid JSON.",
                user_prompt=prompt,
                stream_event_callback=stream_event_callback,
            )
        payload = _extract_json_object(raw)
        payload_type = str(payload.get("type", "") or "").strip().lower()
        if payload_type in {"clarification_request", "split_request", "escalation_request"}:
            payload["backend"] = backend
            return payload
        scope_additions = [str(item) for item in payload.get("scope_additions", [])]
        acceptance_additions = [str(item) for item in payload.get("acceptance_additions", [])]
        boundary_notes = [str(item) for item in payload.get("boundary_notes", [])]
        builder_brief = str(payload.get("builder_brief", "Keep scope minimal and concrete."))
        goal_lower = goal.lower()
        architecture_decision_task = _is_architecture_decision_goal(goal)
        if _should_force_orchestrator_clarification(goal):
            return {
                "type": "clarification_request",
                "reason": "目标仍然过宽或过于模糊，继续出卡会把多个方向硬揉在一起。",
                "questions": [
                    "请明确这轮只优先解决哪一个具体目标。",
                    "请补充你希望看到的完成信号，而不是笼统的优化方向。",
                ],
                "recommendation": "先把目标收成一个可验收的单一结果，再进入任务塑形。",
                "backend": backend,
            }
        if _should_force_orchestrator_split(goal):
            return {
                "type": "split_request",
                "reason": "输入里混入了多个独立动作目标，强行出一张卡会让 Builder 承担不必要的组合风险。",
                "suggested_slices": [
                    "把本轮目标收成一个主目标，其他目标拆成后续卡。",
                    "先确认优先级最高的那条，再分别塑形成独立任务。",
                ],
                "recommendation": "先拆分，再逐张出卡。",
                "backend": backend,
            }
        if architecture_decision_task:
            return {
                "type": "escalation_request",
                "reason": "这是系统方向或架构取舍问题，超出 Orchestrator 可安全裁决的边界。",
                "decision_required": "需要人确认角色边界或未来架构方向。",
                "options": ["保持当前角色分离", "合并角色后再重设边界"],
                "recommendation": "建议先保持角色分离，再基于真实工程数据决定是否合并。",
                "current_state": "当前主线以 Builder 为执行核心、Reviewer 为低频关口。",
                "backend": backend,
            }
        ui_task = _is_explicit_ui_task(task_kind, goal, default_scope)
        first_scope = str(default_scope[0]).strip() if default_scope else ""
        if ui_task and first_scope and (scope_additions or acceptance_additions or boundary_notes):
            if not scope_additions:
                scope_additions = [f"只在 {first_scope} 的现有页面或模板渲染块内改动，不扩散到其他文件。"]
            if not acceptance_additions:
                acceptance_additions = [
                    "页面上能直接看到新的摘要卡片或摘要区块。",
                    "卡片内容来自现有数据来源，不新增 API。",
                ]
            if not any("append" in item.lower() or "文件末尾" in item for item in boundary_notes):
                boundary_notes.append("禁止在文件末尾直接追加 HTML 或模板片段。")
        return {
            "task_profile": str(payload.get("task_profile", "routine")),
            "governance_weight": str(payload.get("governance_weight", "routine")),
            "combination_sensitive": bool(payload.get("combination_sensitive", False)),
            "coordination_subtype": str(payload.get("coordination_subtype", "")).strip(),
            "capability_mix": [str(item) for item in payload.get("capability_mix", [])],
            "interaction_risks": [str(item) for item in payload.get("interaction_risks", [])],
            "scope_additions": scope_additions[:3],
            "acceptance_additions": acceptance_additions[:3],
            "boundary_notes": boundary_notes[:4],
            "builder_brief": builder_brief,
            "approval_hint": bool(payload.get("approval_hint", False)),
            "backend": backend,
        }
    except Exception:
        return _heuristic_orchestrator_design(goal, task_kind)


def get_builder_plan(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    project_memory_context: str = "",
    orchestrator_guidance: str = "",
    builder_working_state: str = "",
    review_feedback: str = "",
    rework_count: int = 0,
) -> str:
    plan, _ = get_builder_plan_with_diagnostics(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        project_memory_context=project_memory_context,
        orchestrator_guidance=orchestrator_guidance,
        builder_working_state=builder_working_state,
        review_feedback=review_feedback,
        rework_count=rework_count,
    )
    return plan


def get_builder_plan_with_diagnostics(
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
    if llm_interface.should_mock("builder"):
        return llm_interface.mock_plan(goal, scan_result, review_feedback), {
            "role": "builder",
            "mode": "mock",
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "max_retries": MAX_RETRIES,
        }

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
    started_at = perf_counter()
    try:
        result = llm_interface.chat(
            role="builder",
            system_prompt="You are the Builder Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            stream_event_callback=stream_event_callback,
        )
        diagnostics["mode"] = "live"
        diagnostics["call_status"] = "completed"
        diagnostics["call_duration_ms"] = round((perf_counter() - started_at) * 1000, 2)
        return result, diagnostics
    except Exception as exc:
        diagnostics["mode"] = "live"
        diagnostics["call_status"] = "failed"
        diagnostics["call_duration_ms"] = round((perf_counter() - started_at) * 1000, 2)
        diagnostics["exception_type"] = type(exc).__name__
        diagnostics["exception_message"] = str(exc)
        raise BuilderPlanCallError(str(exc), diagnostics=diagnostics) from exc

def get_reviewer_assessment(
    goal: str,
    task_card: dict[str, Any],
    scan_result: str,
    execution_result: str,
    execution_evidence: dict[str, Any] | None = None,
    source_workspace_root: str = "",
    rework_count: int = 0,
    stream_event_callback: StreamEventCallback | None = None,
) -> dict[str, Any]:
    if llm_interface.should_mock("reviewer"):
        return _extract_json_object(llm_interface.mock_review(goal, "", execution_result))

    prompt = build_reviewer_prompt(
        goal=goal,
        task_card=task_card,
        scan_result=scan_result,
        execution_result=execution_result,
        execution_evidence=execution_evidence,
        source_workspace_root=source_workspace_root,
        rework_count=rework_count,
    )
    reviewer_settings = get_agent_settings("reviewer")
    backend = str(reviewer_settings.backend or "llm").strip().lower() or "llm"
    if backend == "claude_code":
        raw, _ = run_claude_code_prompt(
            role="reviewer",
            system_prompt="You are the Reviewer Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            cwd=reviewer_settings.workspace_root,
            session_id=reviewer_settings.session_id,
            continue_session=bool(rework_count),
            append_system_prompt=f"Role memory root: {reviewer_settings.memory_root}",
            json_schema=REVIEWER_JSON_SCHEMA,
            model=reviewer_settings.model,
            base_url=reviewer_settings.base_url,
            api_key=reviewer_settings.api_key,
            stream_event_callback=stream_event_callback,
        )
    elif backend == "opencode":
        raw, _ = run_opencode_prompt(
            role="reviewer",
            system_prompt="You are the Reviewer Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            cwd=reviewer_settings.workspace_root,
            model=reviewer_settings.model,
            provider=reviewer_settings.provider,
            base_url=reviewer_settings.base_url,
            api_key=reviewer_settings.api_key,
            memory_root=reviewer_settings.memory_root,
            stream_event_callback=stream_event_callback,
        )
    elif backend == "qwen_code":
        raw, _ = run_qwen_code_prompt(
            role="reviewer",
            system_prompt="You are the Reviewer Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            cwd=reviewer_settings.workspace_root,
            model=reviewer_settings.model,
            base_url=reviewer_settings.base_url,
            api_key=reviewer_settings.api_key,
            memory_root=reviewer_settings.memory_root,
            stream_event_callback=stream_event_callback,
            task_packet_name="QWEN_REVIEW_TASK.md",
            output_filename="REVIEW_ASSESSMENT.json",
            assistant_sentinel="REVIEW_JSON_WRITTEN",
            execution_contract_lines=[
                "Do not modify any source files during review.",
                "Inspect evidence, changed files, and outputs as needed before deciding.",
                "Return a compact gate judgment: approved, changes_requested, or escalate.",
            ],
            approval_mode="yolo",
        )
    elif backend == "codex":
        raw, _ = run_codex_prompt(
            role="reviewer",
            system_prompt="You are the Reviewer Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            cwd=reviewer_settings.workspace_root,
            model=reviewer_settings.model,
            provider=reviewer_settings.provider,
            base_url=reviewer_settings.base_url,
            api_key=reviewer_settings.api_key,
            memory_root=reviewer_settings.memory_root,
            stream_event_callback=stream_event_callback,
            task_packet_name="CODEX_REVIEW_TASK.md",
            output_filename="REVIEW_ASSESSMENT.json",
            assistant_sentinel="REVIEW_JSON_WRITTEN",
            execution_contract_lines=[
                "Do not modify any source files during review.",
                "Inspect evidence, changed files, and outputs as needed before deciding.",
                "Return a compact gate judgment: approved, changes_requested, or escalate.",
            ],
            reasoning_effort="medium",
            sandbox_mode="workspace-write",
            dangerously_bypass_approvals_and_sandbox=True,
        )
    else:
        raw = llm_interface.chat(
            role="reviewer",
            system_prompt="You are the Reviewer Agent for an AI Dev OS kernel. Return only valid JSON.",
            user_prompt=prompt,
            stream_event_callback=stream_event_callback,
        )
    try:
        return _extract_json_object(raw)
    except ValueError:
        fallback_summary = "Reviewer output was not valid JSON; treating this as changes requested for safe fallback."
        fallback_feedback = (raw or "").strip()
        if len(fallback_feedback) > 1200:
            fallback_feedback = fallback_feedback[:1197] + "..."
        return {
            "decision": "changes_requested",
            "summary": fallback_summary,
            "feedback": fallback_feedback or "Reviewer returned non-JSON output.",
            "issues": ["reviewer_output_not_json"],
            "validation_gaps": ["review_contract_unstructured_output"],
            "raw_output_unstructured": True,
        }

































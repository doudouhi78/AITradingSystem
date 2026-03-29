from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from typing import Callable


StreamEventCallback = Callable[[dict[str, Any]], None]


class OpenCodeCallError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: dict[str, Any]):
        super().__init__(message)
        self.diagnostics = diagnostics


def opencode_command() -> list[str] | None:
    command = shutil.which("opencode")
    if not command:
        return None
    command_path = Path(command)
    if os.name == "nt":
        cmd_candidate = command_path.with_suffix(".cmd")
        ps1_candidate = command_path.with_suffix(".ps1")
        if cmd_candidate.exists():
            return [str(cmd_candidate)]
        if ps1_candidate.exists():
            shell = shutil.which("pwsh") or shutil.which("powershell")
            if not shell:
                return None
            return [shell, "-File", str(ps1_candidate)]
    if command_path.suffix.lower() == ".ps1":
        shell = shutil.which("pwsh") or shutil.which("powershell")
        if not shell:
            return None
        return [shell, "-File", str(command_path)]
    return [str(command_path)]


def opencode_is_available() -> bool:
    return bool(opencode_command())


def _write_opencode_config(workspace_root: Path, *, provider: str, model: str, base_url: str) -> str:
    provider_key = provider.strip() or "runtime_provider"
    payload = {
        "$schema": "https://opencode.ai/config.json",
        "provider": {
            provider_key: {
                "npm": "@ai-sdk/openai-compatible",
                "name": provider_key,
                "options": {
                    "baseURL": base_url.rstrip("/"),
                    "apiKey": "{env:AI_DEV_OS_OPENCODE_API_KEY}",
                    "timeout": 300000,
                    "chunkTimeout": 30000,
                },
                "models": {
                    model: {
                        "name": model,
                        "limit": {
                            "context": 262144,
                            "output": 65536,
                        },
                    }
                },
            }
        },
    }
    (workspace_root / "opencode.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return provider_key


def _summarize(text: str, limit: int = 220) -> str:
    cleaned = " ".join(str(text or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def _write_task_packet(workspace_root: Path, *, role: str, system_prompt: str, user_prompt: str) -> Path:
    packet_path = workspace_root / "OPENCODE_RUNTIME_TASK.md"
    content = "\n".join(
        [
            "# OpenCode Runtime Task Packet",
            "",
            f"role: {role}",
            "",
            "## System Prompt",
            system_prompt.strip(),
            "",
            "## User Prompt",
            user_prompt.strip(),
            "",
            "## Execution Rules",
            "- Read this task packet first before responding.",
            "- Do not reply with a readiness/capability handshake.",
            "- Execute the task described above directly.",
            "- If the task asks for structured JSON, return only the requested JSON/result.",
        ]
    ).strip() + "\n"
    packet_path.write_text(content, encoding="utf-8")
    return packet_path


def _translate_event(raw: dict[str, Any]) -> dict[str, Any] | None:
    raw_type = str(raw.get("type") or "")
    part = dict(raw.get("part") or {})
    metadata: dict[str, Any] = {
        "stream_source": "opencode",
        "raw_type": raw_type,
        "session_id": raw.get("sessionID", ""),
    }
    if raw_type == "step_start":
        metadata["snapshot"] = part.get("snapshot")
        return {
            "event_type": "llm_turn_started",
            "status": "running",
            "summary": "OpenCode 开始新一轮步骤。",
            "metadata": metadata,
        }
    if raw_type == "step_finish":
        metadata["snapshot"] = part.get("snapshot")
        metadata["reason"] = part.get("reason")
        metadata["tokens"] = part.get("tokens")
        return {
            "event_type": "llm_turn_finished",
            "status": "completed",
            "summary": _summarize(str(part.get("reason") or "OpenCode 完成一步。")),
            "metadata": metadata,
        }
    if raw_type == "text":
        text = str(part.get("text") or "")
        metadata["message_id"] = part.get("messageID")
        metadata["text"] = text
        return {
            "event_type": "llm_assistant_message",
            "status": "running",
            "summary": _summarize(text),
            "metadata": metadata,
        }
    if raw_type == "tool_use":
        tool = str(part.get("tool") or "unknown").lower()
        state = dict(part.get("state") or {})
        input_data = dict(state.get("input") or {})
        metadata.update({
            "tool": tool,
            "tool_status": state.get("status"),
            "tool_input": input_data,
        })
        target = str(input_data.get("filePath") or input_data.get("pattern") or input_data.get("command") or "")
        if tool in {"read", "glob"}:
            return {
                "event_type": "artifact_read",
                "status": str(state.get("status") or "completed"),
                "summary": _summarize(str(state.get("title") or target or tool)),
                "target": target,
                "metadata": metadata,
            }
        if tool == "todowrite":
            todos = (state.get("metadata") or {}).get("todos") or []
            metadata["todos"] = todos
            return {
                "event_type": "agent_round_context",
                "status": str(state.get("status") or "completed"),
                "summary": _summarize(f"计划更新：{len(todos)} 个待办。"),
                "metadata": metadata,
            }
        if tool == "write":
            return {
                "event_type": "file_write",
                "status": str(state.get("status") or "completed"),
                "summary": _summarize(str(state.get("title") or target or tool)),
                "target": target,
                "metadata": metadata,
            }
        if tool == "edit":
            return {
                "event_type": "file_edit",
                "status": str(state.get("status") or "completed"),
                "summary": _summarize(str(state.get("title") or target or tool)),
                "target": target,
                "metadata": metadata,
            }
        if tool == "bash":
            return {
                "event_type": "command_finished",
                "status": str(state.get("status") or "completed"),
                "summary": _summarize(str(input_data.get("command") or state.get("title") or tool)),
                "target": str(input_data.get("command") or ""),
                "metadata": metadata,
            }
        return {
            "event_type": "action_started",
            "status": str(state.get("status") or "completed"),
            "summary": _summarize(str(state.get("title") or tool)),
            "target": target,
            "metadata": metadata,
        }
    if raw_type == "error":
        error_payload = dict(raw.get("error") or {})
        metadata["error"] = error_payload
        return {
            "event_type": "llm_stream_error",
            "status": "failed",
            "summary": _summarize(str((error_payload.get("data") or {}).get("message") or error_payload.get("name") or "OpenCode 运行失败")),
            "metadata": metadata,
        }
    return None


def run_opencode_prompt(
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    cwd: str,
    model: str,
    provider: str,
    base_url: str,
    api_key: str,
    memory_root: str = "",
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    command = opencode_command()
    diagnostics: dict[str, Any] = {
        "role": role,
        "backend": "opencode",
        "provider": provider,
        "model": model,
        "cwd": cwd,
        "memory_root": memory_root,
        "call_status": "failed",
    }
    if not command:
        diagnostics["exception_type"] = "OpenCodeUnavailable"
        diagnostics["exception_message"] = "OpenCode CLI is not installed."
        raise OpenCodeCallError("OpenCode CLI is not installed.", diagnostics=diagnostics)

    workspace_root = Path(cwd).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    provider_key = _write_opencode_config(workspace_root, provider=provider, model=model, base_url=base_url)
    task_packet = _write_task_packet(
        workspace_root,
        role=role,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    env = os.environ.copy()
    env["AI_DEV_OS_OPENCODE_API_KEY"] = api_key
    prompt = (
        f"Read ./{task_packet.name} first, then execute the task exactly as written. "
        "Do not output a readiness/capability handshake. "
        "Return only the task result requested in that file."
    )
    process = subprocess.Popen(
        [*command, "run", "--dir", ".", "--format", "json", "-m", f"{provider_key}/{model}", prompt],
        cwd=str(workspace_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if stream_event_callback:
        stream_event_callback({
            "event_type": "llm_stream_started",
            "status": "running",
            "summary": f"{role} started OpenCode runtime.",
            "metadata": {"stream_source": "opencode", "provider": provider, "model": model},
        })
    raw_lines: list[str] = []
    text_parts: list[str] = []
    translated_count = 0
    try:
        assert process.stdout is not None
        for line in process.stdout:
            raw_lines.append(line)
            stripped = line.strip()
            if not stripped:
                continue
            try:
                raw_event = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            translated = _translate_event(raw_event)
            if translated and stream_event_callback:
                translated_count += 1
                stream_event_callback(translated)
            if str(raw_event.get("type") or "") == "text":
                text = str((raw_event.get("part") or {}).get("text") or "")
                if text:
                    text_parts.append(text)
        stderr_text = process.stderr.read() if process.stderr else ""
        return_code = process.wait()
    finally:
        if process.stdout:
            process.stdout.close()
        if process.stderr:
            process.stderr.close()

    raw_log_dir = Path(memory_root).resolve() / "opencode_runtime"
    raw_log_dir.mkdir(parents=True, exist_ok=True)
    raw_log_path = raw_log_dir / f"{role}_last_run.raw.jsonl"
    raw_log_path.write_text("".join(raw_lines), encoding="utf-8")
    final_text = "\n".join(part.strip() for part in text_parts if part.strip()).strip()
    diagnostics.update({
        "call_status": "completed" if return_code == 0 else "failed",
        "returncode": return_code,
        "stream_event_count": translated_count,
        "raw_log_path": str(raw_log_path),
        "task_packet_path": str(task_packet),
        "message_chars": len(final_text),
        "stderr": stderr_text[:2000],
    })
    if return_code != 0:
        if stream_event_callback:
            stream_event_callback({
                "event_type": "llm_stream_error",
                "status": "failed",
                "summary": _summarize(stderr_text or f"OpenCode exited with status {return_code}"),
                "metadata": {"stream_source": "opencode", "provider": provider, "model": model, "returncode": return_code},
            })
        diagnostics["exception_type"] = "OpenCodeNonZeroExit"
        diagnostics["exception_message"] = stderr_text or f"OpenCode exited with status {return_code}"
        raise OpenCodeCallError(diagnostics["exception_message"], diagnostics=diagnostics)
    if stream_event_callback:
        stream_event_callback({
            "event_type": "llm_stream_completed",
            "status": "completed",
            "summary": f"{role} finished OpenCode runtime.",
            "metadata": {"stream_source": "opencode", "provider": provider, "model": model, "message_chars": len(final_text)},
        })
    return final_text, diagnostics

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from time import monotonic
from time import perf_counter
from typing import Any
from typing import Callable


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_WORKDIR = ROOT_DIR
DEFAULT_CALL_TIMEOUT_SECONDS = int(os.environ.get('CLAUDE_CODE_CALL_TIMEOUT_SECONDS', '420') or '420')
StreamEventCallback = Callable[[dict[str, Any]], None]


class ClaudeCodeCallError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: dict[str, Any]):
        super().__init__(message)
        self.diagnostics = diagnostics


def claude_code_command() -> list[str] | None:
    npm_root = Path(os.environ.get('APPDATA', '')) / 'npm'
    cli_js = npm_root / 'node_modules' / '@anthropic-ai' / 'claude-code' / 'cli.js'
    if cli_js.exists():
        local_node = npm_root / ('node.exe' if os.name == 'nt' else 'node')
        node_bin = str(local_node) if local_node.exists() else shutil.which('node')
        if node_bin:
            return [node_bin, str(cli_js)]
    command = shutil.which('claude')
    if command:
        return [command]
    return None


def claude_code_is_available() -> bool:
    return bool(claude_code_command())


def run_claude_code_prompt(
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    cwd: str | Path | None = None,
    continue_session: bool = False,
    session_id: str = "",
    append_system_prompt: str = "",
    json_schema: str = "",
    model: str = "",
    base_url: str = "",
    api_key: str = "",
    stream_event_callback: StreamEventCallback | None = None,
) -> tuple[str, dict[str, Any]]:
    started_at = perf_counter()
    command = claude_code_command()
    diagnostics: dict[str, Any] = {
        "role": role,
        "backend": "claude_code",
        "call_status": "failed",
        "continue_session": bool(continue_session),
        "session_id": session_id.strip(),
        "stream_mode": bool(stream_event_callback),
        "configured_model": model.strip(),
        "configured_base_url": base_url.strip(),
    }
    if not command:
        diagnostics.update(
            {
                "exception_type": "ClaudeCodeUnavailable",
                "exception_message": "Claude Code CLI is not available on PATH.",
                "call_duration_ms": round((perf_counter() - started_at) * 1000, 2),
            }
        )
        raise ClaudeCodeCallError("Claude Code CLI is not available on PATH.", diagnostics=diagnostics)

    workdir = Path(cwd or DEFAULT_WORKDIR).resolve()
    prompt = _compose_prompt(role=role, system_prompt=system_prompt, user_prompt=user_prompt)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if base_url.strip():
        env["ANTHROPIC_BASE_URL"] = base_url.strip()
    if api_key.strip():
        env["ANTHROPIC_AUTH_TOKEN"] = api_key.strip()

    invocation_mode = "resume" if session_id.strip() and continue_session else "session_id"
    command_args = _build_command_args(
        command=command,
        session_id=session_id,
        invocation_mode=invocation_mode,
        append_system_prompt=append_system_prompt,
        json_schema=json_schema,
        prompt=prompt,
        stream_mode=bool(stream_event_callback),
        model=model,
    )

    try:
        completed, payload, stream_stats = _execute_claude_invocation(
            command=command,
            session_id=session_id,
            continue_session=continue_session,
            invocation_mode=invocation_mode,
            append_system_prompt=append_system_prompt,
            json_schema=json_schema,
            prompt=prompt,
            model=model,
            workdir=workdir,
            env=env,
            stream_event_callback=stream_event_callback,
        )
        invocation_mode = str(stream_stats.get("session_invocation_mode", invocation_mode) or invocation_mode)
    except Exception as exc:
        diagnostics.update(
            {
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "command": list(stream_stats.get("command", command_args))[:-1],
                "working_directory": str(workdir),
                "call_duration_ms": round((perf_counter() - started_at) * 1000, 2),
            }
        )
        raise ClaudeCodeCallError(str(exc), diagnostics=diagnostics) from exc

    stderr_text = (completed.stderr or completed.stdout or "").strip()

    diagnostics.update(
        {
            "command": list(stream_stats.get("command", command_args))[:-1],
            "working_directory": str(workdir),
            "returncode": int(completed.returncode),
            "stdout_chars": len(completed.stdout or ""),
            "stderr_chars": len(completed.stderr or ""),
            "call_duration_ms": round((perf_counter() - started_at) * 1000, 2),
            "session_invocation_mode": str(stream_stats.get("session_invocation_mode", invocation_mode) or invocation_mode),
            **stream_stats,
        }
    )
    if completed.returncode != 0:
        diagnostics.update(
            {
                "exception_type": "ClaudeCodeNonZeroExit",
                "exception_message": stderr_text[:1000],
            }
        )
        raise ClaudeCodeCallError(
            f"Claude Code returned non-zero exit status {completed.returncode}.",
            diagnostics=diagnostics,
        )

    result = _extract_result_text(payload, completed.stdout)
    diagnostics["call_status"] = "completed"
    if isinstance(payload, dict):
        for key in ("session_id", "cost_usd", "duration_ms", "total_tokens", "model", "duration_ms", "subtype"):
            if key in payload:
                diagnostics[key] = payload[key]
        usage = payload.get("usage")
        if isinstance(usage, dict):
            diagnostics["usage"] = usage
    diagnostics["result_chars"] = len(result or "")
    return result, diagnostics


def _execute_claude_invocation(
    *,
    command: list[str],
    session_id: str,
    continue_session: bool,
    invocation_mode: str,
    append_system_prompt: str,
    json_schema: str,
    prompt: str,
    model: str,
    workdir: Path,
    env: dict[str, str],
    stream_event_callback: StreamEventCallback | None,
) -> tuple[subprocess.CompletedProcess[str], Any, dict[str, Any]]:
    current_mode = invocation_mode
    command_args = _build_command_args(
        command=command,
        session_id=session_id,
        invocation_mode=current_mode,
        append_system_prompt=append_system_prompt,
        json_schema=json_schema,
        prompt=prompt,
        stream_mode=bool(stream_event_callback),
        model=model,
    )
    if stream_event_callback:
        completed, payload, stream_stats = _run_streaming_command(
            command_args=command_args,
            workdir=workdir,
            env=env,
            timeout_seconds=DEFAULT_CALL_TIMEOUT_SECONDS,
            event_callback=stream_event_callback,
        )
    else:
        completed = _run_command(
            command_args=command_args,
            workdir=workdir,
            env=env,
            timeout_seconds=DEFAULT_CALL_TIMEOUT_SECONDS,
        )
        payload = _parse_claude_code_payload(completed.stdout)
        stream_stats = {}

    stderr_text = (completed.stderr or completed.stdout or "").strip()
    if completed.returncode != 0 and session_id.strip() and not continue_session and "already in use" in stderr_text:
        current_mode = "resume"
        command_args = _build_command_args(
            command=command,
            session_id=session_id,
            invocation_mode=current_mode,
            append_system_prompt=append_system_prompt,
            json_schema=json_schema,
            prompt=prompt,
            stream_mode=bool(stream_event_callback),
            model=model,
        )
        if stream_event_callback:
            completed, payload, stream_stats = _run_streaming_command(
                command_args=command_args,
                workdir=workdir,
                env=env,
                timeout_seconds=DEFAULT_CALL_TIMEOUT_SECONDS,
                event_callback=stream_event_callback,
            )
        else:
            completed = _run_command(
                command_args=command_args,
                workdir=workdir,
                env=env,
                timeout_seconds=DEFAULT_CALL_TIMEOUT_SECONDS,
            )
            payload = _parse_claude_code_payload(completed.stdout)
            stream_stats = {}
    stream_stats = dict(stream_stats)
    stream_stats["session_invocation_mode"] = current_mode
    stream_stats["command"] = command_args
    return completed, payload, stream_stats


def _build_command_args(
    *,
    command: list[str],
    session_id: str,
    invocation_mode: str,
    append_system_prompt: str,
    json_schema: str,
    prompt: str,
    stream_mode: bool,
    model: str,
) -> list[str]:
    command_args = [
        *command,
        "-p",
        "--dangerously-skip-permissions",
    ]
    if stream_mode:
        command_args.extend([
            "--verbose",
            "--output-format",
            "stream-json",
            "--include-partial-messages",
        ])
    else:
        command_args.extend([
            "--output-format",
            "json",
        ])
    normalized_model = model.strip()
    if normalized_model:
        command_args.extend(["--model", normalized_model])
    normalized_session_id = session_id.strip()
    if normalized_session_id:
        if invocation_mode == "resume":
            command_args.extend(["--resume", normalized_session_id])
        else:
            command_args.extend(["--session-id", normalized_session_id])
    if append_system_prompt.strip():
        command_args.extend(["--append-system-prompt", append_system_prompt.strip()])
    if json_schema.strip():
        command_args.extend(["--json-schema", json_schema.strip()])
    command_args.append(prompt)
    return command_args


def _run_command(*, command_args: list[str], workdir: Path, env: dict[str, str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command_args,
        cwd=str(workdir),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=max(1, int(timeout_seconds or DEFAULT_CALL_TIMEOUT_SECONDS)),
    )


def _run_streaming_command(
    *,
    command_args: list[str],
    workdir: Path,
    env: dict[str, str],
    timeout_seconds: int,
    event_callback: StreamEventCallback,
) -> tuple[subprocess.CompletedProcess[str], Any, dict[str, Any]]:
    process = subprocess.Popen(
        command_args,
        cwd=str(workdir),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None

    line_queue: Queue[str | None] = Queue()

    def _reader() -> None:
        try:
            for raw_line in process.stdout:
                line_queue.put(raw_line)
        finally:
            line_queue.put(None)

    reader = Thread(target=_reader, daemon=True)
    reader.start()

    raw_lines: list[str] = []
    result_payload: Any = {}
    stream_state: dict[str, Any] = {"buffers": {}, "session_id": "", "model": "", "emitted": 0}
    started = monotonic()
    reached_end = False

    while not reached_end:
        if monotonic() - started > max(1, int(timeout_seconds or DEFAULT_CALL_TIMEOUT_SECONDS)):
            process.kill()
            raise TimeoutError(f"Claude Code stream call exceeded {timeout_seconds} seconds.")
        try:
            item = line_queue.get(timeout=0.25)
        except Empty:
            if process.poll() is not None and line_queue.empty():
                reached_end = True
            continue
        if item is None:
            reached_end = True
            continue
        raw_lines.append(item)
        line = item.rstrip("\n")
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        emitted, result_payload = _emit_stream_events(record=record, state=stream_state, callback=event_callback, current_result=result_payload)
        stream_state["emitted"] += emitted

    returncode = process.wait(timeout=5)
    stdout_text = "".join(raw_lines)
    completed = subprocess.CompletedProcess(command_args, returncode, stdout_text, "")
    payload = result_payload or _parse_claude_code_payload(stdout_text)
    stream_stats = {
        "stream_event_count": int(stream_state.get("emitted", 0) or 0),
        "stream_session_id": str(stream_state.get("session_id", "") or ""),
        "stream_model": str(stream_state.get("model", "") or ""),
    }
    return completed, payload, stream_stats


def _emit_stream_events(*, record: dict[str, Any], state: dict[str, Any], callback: StreamEventCallback, current_result: Any) -> tuple[int, Any]:
    emitted = 0
    record_type = str(record.get("type", "") or "")
    session_id = str(record.get("session_id", "") or state.get("session_id", "") or "")
    if session_id:
        state["session_id"] = session_id

    if record_type == "system":
        model = str(record.get("model", "") or "")
        if model:
            state["model"] = model
        callback(
            {
                "event_type": "llm_stream_started",
                "status": "started",
                "summary": f"Claude Code stream started ({model or 'unknown model'}).",
                "target": session_id,
                "metadata": {
                    "session_id": session_id,
                    "model": model,
                    "raw_type": record_type,
                    "permission_mode": str(record.get("permissionMode", "") or ""),
                },
            }
        )
        return 1, current_result

    if record_type == "stream_event":
        event = dict(record.get("event", {}) or {})
        event_type = str(event.get("type", "") or "")
        if event_type == "content_block_start":
            block = dict(event.get("content_block", {}) or {})
            block_type = str(block.get("type", "") or "")
            if block_type == "tool_use":
                callback(
                    {
                        "event_type": "llm_tool_use_started",
                        "status": "running",
                        "summary": f"Claude Code started tool {str(block.get('name', '') or 'unknown')}.",
                        "target": str(block.get("name", "") or ""),
                        "metadata": {
                            "session_id": session_id,
                            "model": str(state.get("model", "") or ""),
                            "raw_type": event_type,
                            "block_type": block_type,
                            "tool_name": str(block.get("name", "") or ""),
                            "tool_id": str(block.get("id", "") or ""),
                            "index": int(event.get("index", 0) or 0),
                        },
                    }
                )
                return 1, current_result
            if block_type == "thinking":
                callback(
                    {
                        "event_type": "llm_thinking_started",
                        "status": "running",
                        "summary": "Claude Code started a thinking block.",
                        "target": session_id,
                        "metadata": {
                            "session_id": session_id,
                            "model": str(state.get("model", "") or ""),
                            "raw_type": event_type,
                            "block_type": block_type,
                            "index": int(event.get("index", 0) or 0),
                        },
                    }
                )
                return 1, current_result
            return 0, current_result
        if event_type == "content_block_delta":
            emitted += _buffer_stream_delta(event=event, state=state, callback=callback)
            return emitted, current_result
        if event_type == "content_block_stop":
            emitted += _flush_buffered_stream_chunks(index=int(event.get("index", 0) or 0), state=state, callback=callback)
            return emitted, current_result
        return 0, current_result

    if record_type == "assistant":
        message = dict(record.get("message", {}) or {})
        content = list(message.get("content", []) or [])
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = str(item.get("text", "") or "").strip()
                if text:
                    text_parts.append(text)
        if text_parts:
            callback(
                {
                    "event_type": "llm_assistant_message",
                    "status": "completed",
                    "summary": " ".join(text_parts)[:220],
                    "target": session_id,
                    "metadata": {
                        "session_id": session_id,
                        "model": str(message.get("model", "") or state.get("model", "") or ""),
                        "raw_type": record_type,
                    },
                }
            )
            return 1, current_result
        return 0, current_result

    if record_type == "result":
        current_result = record
        subtype = str(record.get("subtype", "") or "")
        callback(
            {
                "event_type": "llm_stream_completed" if not record.get("is_error") else "llm_stream_error",
                "status": "completed" if not record.get("is_error") else "failed",
                "summary": str(record.get("result", "") or subtype or "Claude Code stream finished.")[:220],
                "target": session_id,
                "metadata": {
                    "session_id": session_id,
                    "model": str(state.get("model", "") or ""),
                    "raw_type": record_type,
                    "subtype": subtype,
                    "duration_ms": int(record.get("duration_ms", 0) or 0),
                    "num_turns": int(record.get("num_turns", 0) or 0),
                },
            }
        )
        return 1, current_result

    return 0, current_result


def _buffer_stream_delta(*, event: dict[str, Any], state: dict[str, Any], callback: StreamEventCallback) -> int:
    delta = dict(event.get("delta", {}) or {})
    delta_type = str(delta.get("type", "") or "")
    if delta_type not in {"thinking_delta", "text_delta", "input_json_delta"}:
        return 0
    piece = str(delta.get("thinking") or delta.get("text") or delta.get("partial_json") or "")
    if not piece:
        return 0
    index = int(event.get("index", 0) or 0)
    key = (index, delta_type)
    buffers = state.setdefault("buffers", {})
    buffers[key] = str(buffers.get(key, "")) + piece
    if len(buffers[key]) < 90:
        return 0
    return _emit_buffer_chunk(index=index, delta_type=delta_type, state=state, callback=callback)


def _flush_buffered_stream_chunks(*, index: int, state: dict[str, Any], callback: StreamEventCallback) -> int:
    buffers = state.setdefault("buffers", {})
    keys = [key for key in list(buffers.keys()) if key[0] == index]
    emitted = 0
    for _, delta_type in keys:
        emitted += _emit_buffer_chunk(index=index, delta_type=delta_type, state=state, callback=callback)
    return emitted


def _emit_buffer_chunk(*, index: int, delta_type: str, state: dict[str, Any], callback: StreamEventCallback) -> int:
    buffers = state.setdefault("buffers", {})
    key = (index, delta_type)
    text = str(buffers.get(key, "") or "")
    if not text:
        return 0
    event_type = {
        "thinking_delta": "llm_thinking_delta",
        "text_delta": "llm_text_delta",
        "input_json_delta": "llm_tool_input_delta",
    }.get(delta_type, "llm_stream_delta")
    callback(
        {
            "event_type": event_type,
            "status": "running",
            "summary": text[:220],
            "target": str(state.get("session_id", "") or ""),
            "metadata": {
                "session_id": str(state.get("session_id", "") or ""),
                "model": str(state.get("model", "") or ""),
                "delta_type": delta_type,
                "index": index,
                "chunk_chars": len(text),
            },
        }
    )
    buffers[key] = ""
    return 1


def _compose_prompt(*, role: str, system_prompt: str, user_prompt: str) -> str:
    return (
        f"You are acting as the {role} role inside AI Dev OS.\n"
        "Follow the system instruction exactly.\n\n"
        "<SYSTEM>\n"
        f"{system_prompt.strip()}\n"
        "</SYSTEM>\n\n"
        "<USER>\n"
        f"{user_prompt.strip()}\n"
        "</USER>"
    )


def _parse_claude_code_payload(stdout: str) -> Any:
    text = (stdout or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {"raw_output": text}


def _extract_result_text(payload: Any, fallback_stdout: str) -> str:
    if isinstance(payload, dict):
        for key in ("result", "content", "text", "completion"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        message = payload.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("text") or item.get("content")
                        if isinstance(text, str) and text.strip():
                            parts.append(text.strip())
                if parts:
                    return "\n".join(parts).strip()
        structured_output = payload.get("structured_output")
        if structured_output is not None:
            if isinstance(structured_output, str) and structured_output.strip():
                return structured_output.strip()
            return json.dumps(structured_output, ensure_ascii=False)
        if "raw_output" in payload and isinstance(payload["raw_output"], str):
            return payload["raw_output"].strip()
    if isinstance(payload, list):
        parts: list[str] = []
        for item in payload:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
            elif isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        if parts:
            return "\n".join(parts).strip()
    return (fallback_stdout or "").strip()

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from typing import Callable

StreamEventCallback = Callable[[dict[str, Any]], None]


class CodexCallError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: dict[str, Any]):
        super().__init__(message)
        self.diagnostics = diagnostics


def codex_command() -> list[str] | None:
    command = shutil.which("codex.cmd") or shutil.which("codex") or shutil.which("codex.exe")
    if not command:
        return None
    return [command]


def _summarize(text: str, limit: int = 220) -> str:
    cleaned = " ".join(str(text or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def _extract_json_candidate(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        lines = raw.splitlines()
        if len(lines) >= 3:
            raw = "\n".join(lines[1:-1]).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(raw[start : end + 1])
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _plan_payload_richness(text: str) -> int:
    payload = _extract_json_candidate(text)
    if not payload:
        return -1
    score = 0
    change_plan = payload.get("change_plan")
    if isinstance(change_plan, dict):
        score += 5
        changes = change_plan.get("changes")
        if isinstance(changes, list):
            score += len(changes) * 3
            for item in changes:
                if not isinstance(item, dict):
                    continue
                action_type = str(item.get("action_type", "") or "").strip().lower()
                if action_type in {"edit_file", "write_file"}:
                    score += 4
                payload_obj = item.get("payload")
                if isinstance(payload_obj, dict) and payload_obj:
                    score += 10
                    if any(str(payload_obj.get(key, "") or "").strip() for key in ("old_text", "new_text", "append_text", "content")):
                        score += 15
    if isinstance(payload.get("implementation_steps"), list):
        score += len(payload.get("implementation_steps") or [])
    if isinstance(payload.get("validation_checks"), list):
        score += len(payload.get("validation_checks") or [])
    return score


def _prefer_richer_output(*candidates: tuple[str, str]) -> tuple[str, str]:
    best_text = ""
    best_source = ""
    best_score = -1
    for source, text in candidates:
        candidate_text = str(text or "").strip()
        if not candidate_text:
            continue
        score = _plan_payload_richness(candidate_text)
        if score > best_score:
            best_text = candidate_text
            best_source = source
            best_score = score
    return best_text, best_source


def _normalize_base_url(provider: str, base_url: str) -> str:
    normalized = str(base_url or "").rstrip("/")
    if provider.strip().lower() == "easycodex" and normalized.endswith("/v1"):
        return normalized[:-3]
    return normalized


def _write_task_packet(
    packet_path: Path,
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    output_target: str,
    assistant_sentinel: str,
    execution_contract_lines: list[str] | None = None,
) -> Path:
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    contract_lines = execution_contract_lines or []
    contract_block = "\n".join([f"- {line}" for line in contract_lines]) if contract_lines else "- No extra execution contract lines."
    content = "\n".join(
        [
            f"# Codex Task Packet ({role})",
            "",
            f"role: {role}",
            "",
            "## System Prompt",
            system_prompt.strip(),
            "",
            "## User Prompt",
            user_prompt.strip(),
            "",
            "## Execution Contract",
            f"- Create {output_target}.",
            f"- {output_target} must contain the final JSON object only.",
            "- Do not greet, do not explain, do not ask follow-up questions.",
            "- At key transitions, emit a very short agent_message describing current judgment, chosen path, and next step.",
            "- Keep each decision update to one or two short sentences; do not turn decision updates into long reports.",
            f"- After writing {output_target}, reply with exactly {assistant_sentinel}.",
            contract_block,
        ]
    ) + "\n"
    packet_path.write_text(content, encoding="utf-8")
    return packet_path


def _write_codex_home(
    *,
    codex_home: Path,
    provider: str,
    model: str,
    base_url: str,
    api_key: str,
    reasoning_effort: str,
) -> tuple[Path, Path]:
    codex_home.mkdir(parents=True, exist_ok=True)
    provider_key = provider.strip().lower() or "custom"
    config_path = codex_home / "config.toml"
    auth_path = codex_home / "auth.json"
    config_text = "\n".join(
        [
            f'model_provider = "{provider_key}"',
            f'model = "{model}"',
            f'review_model = "{model}"',
            f'model_reasoning_effort = "{reasoning_effort}"',
            "disable_response_storage = true",
            'network_access = "enabled"',
            "model_context_window = 1000000",
            "model_auto_compact_token_limit = 90000",
            "",
            "[windows]",
            'sandbox = "unelevated"',
            "",
            f"[model_providers.{provider_key}]",
            f'name = "{provider_key}"',
            f'base_url = "{_normalize_base_url(provider, base_url)}"',
            'wire_api = "responses"',
            "requires_openai_auth = true",
            "",
        ]
    )
    config_path.write_text(config_text, encoding="utf-8")
    auth_path.write_text(json.dumps({"OPENAI_API_KEY": api_key}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return config_path, auth_path


def _emit_from_raw(raw: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(raw.get("type") or "")
    if event_type == "thread.started":
        return {
            "event_type": "llm_stream_started",
            "status": "running",
            "summary": "Codex thread started.",
            "metadata": {"stream_source": "codex", "thread_id": raw.get("thread_id")},
        }
    if event_type == "turn.started":
        return {
            "event_type": "agent_round_context",
            "status": "running",
            "summary": "Codex turn started.",
            "metadata": {"stream_source": "codex"},
        }
    if event_type == "item.completed":
        item = dict(raw.get("item") or {})
        item_type = str(item.get("type") or "")
        if item_type == "agent_message":
            text = str(item.get("text") or "")
            return {
                "event_type": "llm_assistant_message",
                "status": "running",
                "summary": _summarize(text),
                "metadata": {
                    "stream_source": "codex",
                    "item_id": item.get("id"),
                    "message_preview": _summarize(text, limit=140),
                },
            }
        return {
            "event_type": "action_finished",
            "status": "running",
            "summary": _summarize(json.dumps(item, ensure_ascii=False)),
            "metadata": {
                "stream_source": "codex",
                "item_id": item.get("id"),
                "item_type": item_type,
            },
        }
    if event_type == "turn.completed":
        return {
            "event_type": "llm_stream_completed",
            "status": "completed",
            "summary": "Codex turn completed.",
            "metadata": {
                "stream_source": "codex",
                "usage": raw.get("usage"),
            },
        }
    if event_type == "error":
        return {
            "event_type": "llm_stream_error",
            "status": "failed",
            "summary": _summarize(str(raw.get("message") or raw.get("error") or "Codex error")),
            "metadata": {
                "stream_source": "codex",
                "error": raw,
            },
        }
    return None


def run_codex_prompt(
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
    task_packet_name: str = "CODEX_TASK.md",
    output_filename: str = "CODEX_OUTPUT.json",
    assistant_sentinel: str = "CODEX_JSON_WRITTEN",
    execution_contract_lines: list[str] | None = None,
    reasoning_effort: str = "high",
    sandbox_mode: str = "workspace-write",
    launch_prompt: str | None = None,
    dangerously_bypass_approvals_and_sandbox: bool = False,
) -> tuple[str, dict[str, Any]]:
    command = codex_command()
    diagnostics: dict[str, Any] = {
        "role": role,
        "backend": "codex",
        "provider": provider,
        "model": model,
        "cwd": cwd,
        "memory_root": memory_root,
        "call_status": "failed",
        "task_packet_name": task_packet_name,
        "output_filename": output_filename,
        "sandbox_mode": sandbox_mode,
        "reasoning_effort": reasoning_effort,
        "dangerously_bypass_approvals_and_sandbox": dangerously_bypass_approvals_and_sandbox,
    }
    if not command:
        diagnostics["exception_type"] = "CodexUnavailable"
        diagnostics["exception_message"] = "Codex CLI is not available."
        raise CodexCallError("Codex CLI is not available.", diagnostics=diagnostics)

    workspace_root = Path(cwd).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    runtime_root = Path(memory_root).resolve() / "codex_runtime"
    packet = _write_task_packet(
        runtime_root / task_packet_name,
        role=role,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        output_target=str((runtime_root / output_filename).resolve()),
        assistant_sentinel=assistant_sentinel,
        execution_contract_lines=execution_contract_lines,
    )

    codex_home = runtime_root / "codex_home"
    raw_log_path = runtime_root / f"{role}_last_run.raw.jsonl"
    runtime_root.mkdir(parents=True, exist_ok=True)
    config_path, auth_path = _write_codex_home(
        codex_home=codex_home,
        provider=provider,
        model=model,
        base_url=base_url,
        api_key=api_key,
        reasoning_effort=reasoning_effort,
    )

    env = os.environ.copy()
    env["CODEX_HOME"] = str(codex_home)

    task_prompt = launch_prompt or f'Read "{packet}" and follow it exactly.'
    output_path = runtime_root / output_filename
    stale_paths = {
        output_path,
        raw_log_path,
    }
    if role.strip().lower() == "builder":
        stale_paths.update(
            {
                runtime_root / "execution_report.json",
                runtime_root / "execution_runtime_plan.json",
                runtime_root / "validation_checklist.json",
                runtime_root / "build_plan.json",
                runtime_root / "execution_brief.md",
                runtime_root / "validation_checklist.md",
            }
        )
    for stale_path in stale_paths:
        try:
            if stale_path.exists():
                stale_path.unlink()
        except OSError:
            pass
    final_text = ""
    output_ready_at = None
    grace_seconds = 5.0
    max_wait_seconds = 180.0 if role.strip().lower() == "builder" else 120.0
    started_wait_at = time.monotonic()
    timed_out = False
    with raw_log_path.open("w", encoding="utf-8") as raw_log_handle:
        proc = subprocess.Popen(
            [
                *command,
                "exec",
                "--skip-git-repo-check",
                *(
                    ["--dangerously-bypass-approvals-and-sandbox"]
                    if dangerously_bypass_approvals_and_sandbox
                    else ["--sandbox", sandbox_mode]
                ),
                "--json",
                "-m",
                model,
                task_prompt,
            ],
            cwd=str(workspace_root),
            stdout=raw_log_handle,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        try:
            while True:
                return_code = proc.poll()
                now = time.monotonic()
                if output_path.exists() and output_path.stat().st_size > 0:
                    if output_ready_at is None:
                        output_ready_at = now
                    elif return_code is None and (now - output_ready_at) >= grace_seconds:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        return_code = proc.wait(timeout=5)
                        break
                if return_code is None and (now - started_wait_at) >= max_wait_seconds:
                    timed_out = True
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    return_code = proc.wait(timeout=5)
                    break
                if return_code is not None:
                    break
                time.sleep(0.5)
            stderr_text = proc.stderr.read() if proc.stderr else ""
        finally:
            if proc.stderr:
                proc.stderr.close()

    raw_lines = raw_log_path.read_text(encoding="utf-8", errors="replace").splitlines(True) if raw_log_path.exists() else []
    for line in raw_lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        mapped = _emit_from_raw(raw)
        if mapped and stream_event_callback:
            stream_event_callback(mapped)
        if str(raw.get("type") or "") == "item.completed":
            item = dict(raw.get("item") or {})
            if str(item.get("type") or "") == "agent_message":
                final_text = str(item.get("text") or "").strip()

    file_output_text = output_path.read_text(encoding="utf-8", errors="replace").strip() if output_path.exists() else ""
    output_text, output_source = _prefer_richer_output(
        ("file", file_output_text),
        ("assistant_result", final_text),
    )
    call_status = "completed"
    if not output_text:
        call_status = "failed"
    elif return_code != 0:
        call_status = "completed_with_warnings"
    diagnostics.update(
        {
            "call_status": call_status,
            "returncode": return_code,
            "timed_out": timed_out,
            "max_wait_seconds": max_wait_seconds,
            "raw_log_path": str(raw_log_path),
            "task_packet_path": str(packet),
            "config_path": str(config_path),
            "auth_path": str(auth_path),
            "stderr": stderr_text[:4000],
            "result_text_preview": _summarize(final_text, limit=240),
            "output_path": str(output_path) if output_path.exists() else "",
            "output_source": output_source,
            "file_output_richness": _plan_payload_richness(file_output_text),
            "assistant_output_richness": _plan_payload_richness(final_text),
        }
    )
    if timed_out and not output_text:
        diagnostics["exception_type"] = "CodexTimeout"
        diagnostics["exception_message"] = f"Codex subprocess exceeded internal timeout of {int(max_wait_seconds)}s."
        raise CodexCallError(diagnostics["exception_message"], diagnostics=diagnostics)
    if not output_text:
        diagnostics["exception_type"] = "CodexNonZeroExit" if return_code != 0 else "CodexMissingOutput"
        diagnostics["exception_message"] = stderr_text or f"Codex did not produce {output_filename}."
        raise CodexCallError(diagnostics["exception_message"], diagnostics=diagnostics)
    return output_text, diagnostics





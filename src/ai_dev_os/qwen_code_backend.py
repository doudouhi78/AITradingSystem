from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from typing import Callable

StreamEventCallback = Callable[[dict[str, Any]], None]


class QwenCodeCallError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: dict[str, Any]):
        super().__init__(message)
        self.diagnostics = diagnostics


def qwen_code_command() -> list[str] | None:
    npm = shutil.which('npx.cmd') or shutil.which('npx')
    if not npm:
        return None
    return [npm, '@qwen-code/qwen-code']


def _summarize(text: str, limit: int = 220) -> str:
    cleaned = ' '.join(str(text or '').split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + '...'


def _repair_common_plan_json(text: str) -> str:
    lines = str(text or '').splitlines()
    if not lines:
        return str(text or '')

    repaired: list[str] = []
    payload_indents: list[int] = []

    def _indent(value: str) -> int:
        return len(value) - len(value.lstrip(' '))

    for index, line in enumerate(lines):
        stripped = line.strip()
        if '"payload": {' in stripped:
            payload_indents.append(_indent(line))
        if payload_indents and stripped == '}':
            current_indent = _indent(line)
            while payload_indents and current_indent < payload_indents[-1]:
                repaired.append(' ' * payload_indents[-1] + '}')
                payload_indents.pop()
        repaired.append(line)

    while payload_indents:
        repaired.append(' ' * payload_indents.pop() + '}')

    lines = repaired
    repaired = []
    for index, line in enumerate(lines):
        stripped = line.rstrip()
        repaired.append(line if not stripped else line.rstrip())
        if index >= len(lines) - 1:
            continue
        current = repaired[-1].rstrip()
        next_line = lines[index + 1].lstrip()
        if not current or current.endswith(','):
            continue
        if not next_line.startswith('"'):
            continue
        if ':' not in current:
            continue
        if current.endswith(('"', '}', ']')):
            repaired[-1] = current + ','
    return '\n'.join(repaired)


def _extract_json_candidate(text: str) -> dict[str, Any] | None:
    raw = str(text or '').strip()
    if not raw:
        return None
    if raw.startswith('```'):
        lines = raw.splitlines()
        if len(lines) >= 3:
            raw = '\n'.join(lines[1:-1]).strip()
    start = raw.find('{')
    end = raw.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = raw[start : end + 1]
    try:
        payload = json.loads(candidate)
    except Exception:
        try:
            payload = json.loads(_repair_common_plan_json(candidate))
        except Exception:
            return None
    return payload if isinstance(payload, dict) else None


def _plan_payload_richness(text: str) -> int:
    payload = _extract_json_candidate(text)
    if not payload:
        return -1
    score = 0
    change_plan = payload.get('change_plan')
    if isinstance(change_plan, dict):
        score += 5
        changes = change_plan.get('changes')
        if isinstance(changes, list):
            score += len(changes) * 3
            for item in changes:
                if not isinstance(item, dict):
                    continue
                action_type = str(item.get('action_type', '') or '').strip().lower()
                if action_type in {'edit_file', 'write_file'}:
                    score += 4
                payload_obj = item.get('payload')
                if isinstance(payload_obj, dict) and payload_obj:
                    score += 10
                    if any(str(payload_obj.get(key, '') or '').strip() for key in ('old_text', 'new_text', 'append_text', 'content')):
                        score += 15
    if isinstance(payload.get('implementation_steps'), list):
        score += len(payload.get('implementation_steps') or [])
    if isinstance(payload.get('validation_checks'), list):
        score += len(payload.get('validation_checks') or [])
    return score


def _prefer_richer_output(*candidates: tuple[str, str]) -> tuple[str, str]:
    best_text = ''
    best_source = ''
    best_score = -1
    for source, text in candidates:
        candidate_text = str(text or '').strip()
        if not candidate_text:
            continue
        score = _plan_payload_richness(candidate_text)
        if score > best_score:
            best_text = candidate_text
            best_source = source
            best_score = score
    return best_text, best_source


def _write_task_packet(
    workspace_root: Path,
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    task_packet_name: str,
    output_filename: str,
    assistant_sentinel: str,
    execution_contract_lines: list[str] | None = None,
) -> Path:
    packet_path = workspace_root / task_packet_name
    contract_lines = execution_contract_lines or []
    contract_block = '\n'.join([f'- {line}' for line in contract_lines]) if contract_lines else '- No extra execution contract lines.'
    content = '\n'.join([
        f'# Qwen Code Task Packet ({role})',
        '',
        f'role: {role}',
        '',
        '## System Prompt',
        system_prompt.strip(),
        '',
        '## User Prompt',
        user_prompt.strip(),
        '',
        '## Execution Contract',
        f'- Create {output_filename} in the current working directory.',
        f'- {output_filename} must contain the final JSON object only.',
        '- Do not greet, do not explain, do not ask follow-up questions.',
        f'- After writing {output_filename}, reply with exactly {assistant_sentinel}.',
        contract_block,
    ]) + '\n'
    packet_path.write_text(content, encoding='utf-8')
    return packet_path


def _emit_from_raw(raw: dict[str, Any]) -> dict[str, Any] | None:
    et = str(raw.get('type') or '')
    session_id = str(raw.get('session_id') or raw.get('sessionID') or '')
    if et == 'system' and str(raw.get('subtype') or '') == 'init':
        return {
            'event_type': 'llm_stream_started',
            'status': 'running',
            'summary': f"Qwen Code runtime init ({raw.get('model') or ''})",
            'metadata': {
                'stream_source': 'qwen_code',
                'session_id': session_id,
                'cwd': raw.get('cwd'),
                'tools': raw.get('tools'),
                'model': raw.get('model'),
                'permission_mode': raw.get('permission_mode'),
                'agents': raw.get('agents'),
                'qwen_code_version': raw.get('qwen_code_version'),
            },
        }
    if et == 'assistant':
        msg = dict(raw.get('message') or {})
        content = list(msg.get('content') or [])
        if not content:
            return None
        item = content[0]
        if item.get('type') == 'tool_use':
            name = str(item.get('name') or '')
            event_type = {
                'read_file': 'artifact_read',
                'list_directory': 'artifact_read',
                'grep_search': 'artifact_read',
                'glob': 'artifact_read',
                'todo_write': 'agent_round_context',
                'write_file': 'file_write',
                'edit': 'file_edit',
                'run_shell_command': 'command_finished',
                'save_memory': 'agent_round_context',
            }.get(name, 'action_started')
            return {
                'event_type': event_type,
                'status': 'running',
                'summary': _summarize(f"{name}: {json.dumps(item.get('input') or {}, ensure_ascii=False)}"),
                'metadata': {
                    'stream_source': 'qwen_code',
                    'session_id': session_id,
                    'tool_name': name,
                    'tool_use_id': item.get('id'),
                    'tool_input': item.get('input'),
                    'usage': msg.get('usage'),
                },
            }
        if item.get('type') == 'text':
            text = str(item.get('text') or '')
            return {
                'event_type': 'llm_assistant_message',
                'status': 'running',
                'summary': _summarize(text),
                'metadata': {
                    'stream_source': 'qwen_code',
                    'session_id': session_id,
                    'text': text,
                    'usage': msg.get('usage'),
                    'model': msg.get('model'),
                },
            }
    if et == 'result':
        is_error = bool(raw.get('is_error'))
        return {
            'event_type': 'llm_stream_error' if is_error else 'llm_stream_completed',
            'status': 'failed' if is_error else 'completed',
            'summary': _summarize(str(raw.get('result') or ('error' if is_error else 'completed'))),
            'metadata': {
                'stream_source': 'qwen_code',
                'session_id': session_id,
                'result': raw.get('result'),
                'duration_ms': raw.get('duration_ms'),
                'duration_api_ms': raw.get('duration_api_ms'),
                'num_turns': raw.get('num_turns'),
                'usage': raw.get('usage'),
                'stats': raw.get('stats'),
                'permission_denials': raw.get('permission_denials'),
            },
        }
    return None


def run_qwen_code_prompt(
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    cwd: str,
    model: str,
    base_url: str,
    api_key: str,
    memory_root: str = '',
    stream_event_callback: StreamEventCallback | None = None,
    task_packet_name: str = 'QWEN_BUILDER_TASK.md',
    output_filename: str = 'BUILD_PLAN.json',
    assistant_sentinel: str = 'BUILD_PLAN_WRITTEN',
    execution_contract_lines: list[str] | None = None,
    approval_mode: str = 'yolo',
    launch_prompt: str | None = None,
) -> tuple[str, dict[str, Any]]:
    command = qwen_code_command()
    diagnostics: dict[str, Any] = {
        'role': role,
        'backend': 'qwen_code',
        'model': model,
        'cwd': cwd,
        'memory_root': memory_root,
        'call_status': 'failed',
        'task_packet_name': task_packet_name,
        'output_filename': output_filename,
        'approval_mode': approval_mode,
    }
    if not command:
        diagnostics['exception_type'] = 'QwenCodeUnavailable'
        diagnostics['exception_message'] = 'Qwen Code CLI is not available.'
        raise QwenCodeCallError('Qwen Code CLI is not available.', diagnostics=diagnostics)

    workspace_root = Path(cwd).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    packet = _write_task_packet(
        workspace_root,
        role=role,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        task_packet_name=task_packet_name,
        output_filename=output_filename,
        assistant_sentinel=assistant_sentinel,
        execution_contract_lines=execution_contract_lines,
    )

    env = os.environ.copy()
    env['OPENAI_API_KEY'] = api_key
    env['OPENAI_BASE_URL'] = base_url.rstrip('/')
    env['QWEN_CODE_DISABLE_TELEMETRY'] = '1'

    task_prompt = launch_prompt or f'Read {packet.name} and follow it exactly.'
    proc = subprocess.Popen(
        [
            *command,
            '--auth-type', 'openai',
            '-m', model,
            '-o', 'stream-json',
            '--approval-mode', approval_mode,
            task_prompt,
        ],
        cwd=str(workspace_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env,
    )

    raw_lines: list[str] = []
    final_text = ''
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            raw_lines.append(line)
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
            if str(raw.get('type') or '') == 'result':
                final_text = str(raw.get('result') or '').strip()
        stderr_text = proc.stderr.read() if proc.stderr else ''
        return_code = proc.wait()
    finally:
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()

    raw_log_dir = Path(memory_root).resolve() / 'qwen_code_runtime'
    raw_log_dir.mkdir(parents=True, exist_ok=True)
    raw_log_path = raw_log_dir / f'{role}_last_run.raw.json'
    raw_log_path.write_text(''.join(raw_lines), encoding='utf-8')

    output_path = workspace_root / output_filename
    file_output_text = output_path.read_text(encoding='utf-8').strip() if output_path.exists() else ''
    output_text, output_source = _prefer_richer_output(
        ('file', file_output_text),
        ('assistant_result', final_text),
    )
    diagnostics.update({
        'call_status': 'completed' if return_code == 0 else 'failed',
        'returncode': return_code,
        'raw_log_path': str(raw_log_path),
        'task_packet_path': str(packet),
        'stderr': stderr_text[:4000],
        'result_text': final_text,
        'output_path': str(output_path) if output_path.exists() else '',
        'output_source': output_source,
        'file_output_richness': _plan_payload_richness(file_output_text),
        'assistant_output_richness': _plan_payload_richness(final_text),
    })
    if output_text:
        repaired_payload = _extract_json_candidate(output_text)
        if repaired_payload:
            output_text = json.dumps(repaired_payload, ensure_ascii=False, indent=2)
    if return_code != 0 or not output_text:
        diagnostics['exception_type'] = 'QwenCodeNonZeroExit' if return_code != 0 else 'QwenCodeMissingOutput'
        diagnostics['exception_message'] = stderr_text or f'Qwen Code did not produce {output_filename}.'
        raise QwenCodeCallError(diagnostics['exception_message'], diagnostics=diagnostics)
    return output_text, diagnostics




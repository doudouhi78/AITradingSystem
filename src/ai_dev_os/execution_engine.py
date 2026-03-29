from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_dev_os.execution_runtime import ExecutionRuntimeConfig
from ai_dev_os.execution_runtime import RuntimeAction
from ai_dev_os.execution_runtime import build_runtime_command
from ai_dev_os.execution_runtime import execute_runtime_actions
from ai_dev_os.execution_runtime import load_execution_runtime_config
from ai_dev_os.execution_runtime import probe_execution_runtime
from ai_dev_os.execution_runtime import runtime_execution_to_dict
from ai_dev_os.governance import execution_whitelist
from ai_dev_os.io_utils import now_iso
from ai_dev_os.tool_bus import collect_git_diff_evidence
from ai_dev_os.tool_bus import execute_edit_file_action
from ai_dev_os.tool_bus import execute_write_file_action
from ai_dev_os.tool_bus import plan_edit_file
from ai_dev_os.tool_bus import plan_git_diff
from ai_dev_os.tool_bus import plan_install_dep
from ai_dev_os.tool_bus import plan_write_file
from ai_dev_os.tool_bus import plan_run_lint
from ai_dev_os.tool_bus import plan_run_tests
from ai_dev_os.tool_bus import write_project_file
from ai_dev_os.tool_bus import write_project_json


@dataclass(frozen=True)
class ExecutionAction:
    name: str
    target: Path
    description: str
    content: str | dict[str, Any]


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Build result does not contain a JSON object.")
    return json.loads(text[start : end + 1])


def _to_markdown_list(items: list[str], fallback: str) -> str:
    if not items:
        return f"- {fallback}"
    return "\n".join(f"- {item}" for item in items)
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


def _baseline_git_diff_evidence(workspace_root: Path, *, limit: int = 12) -> dict[str, Any]:
    baseline_path = workspace_root / '.role' / 'shared_workspace_baseline.json'
    if not baseline_path.exists():
        return {'action_type': 'git_diff', 'status': 'failed', 'returncode': None, 'stdout': '', 'stderr': 'shared workspace baseline missing', 'duration_ms': 0, 'changed_files': []}
    try:
        baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
    except Exception as exc:
        return {'action_type': 'git_diff', 'status': 'failed', 'returncode': None, 'stdout': '', 'stderr': f'baseline read failed: {exc}', 'duration_ms': 0, 'changed_files': []}
    dirs = [str(item).strip() for item in list(baseline.get('dirs', []) or []) if str(item).strip()]
    files = [str(item).strip() for item in list(baseline.get('files', []) or []) if str(item).strip()]
    expected = dict(baseline.get('files_by_hash', {}) or {})
    current: dict[str, str] = {}
    for dirname in dirs:
        base_dir = workspace_root / dirname
        if not base_dir.exists():
            continue
        for candidate in sorted(path for path in base_dir.rglob('*') if path.is_file()):
            rel = candidate.relative_to(workspace_root).as_posix()
            if not _trackable_changed_file(rel):
                continue
            current[rel] = _hash_file(candidate)
    for filename in files:
        candidate = workspace_root / filename
        if candidate.exists() and candidate.is_file():
            rel = candidate.relative_to(workspace_root).as_posix()
            if not _trackable_changed_file(rel):
                continue
            current[rel] = _hash_file(candidate)
    changed = sorted(rel for rel in ({rel for rel, digest in current.items() if expected.get(rel) != digest} | {rel for rel in expected if rel not in current}) if _trackable_changed_file(rel))[: max(1, limit)]
    label = 'file' if len(changed) == 1 else 'files'
    summary = '' if not changed else f'{len(changed)} changed {label}: ' + ', '.join(changed)
    return {'action_type': 'git_diff', 'status': 'passed', 'returncode': 0, 'stdout': summary, 'stderr': '', 'duration_ms': 0, 'changed_files': changed}




def _normalize_change_plan(plan: dict[str, Any]) -> dict[str, Any]:
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
    if not normalized_changes and not direct_execution:
        fallback_steps = [str(item).strip() for item in normalized.get("implementation_steps", []) if str(item).strip()]
        summary = str(normalized.get("summary", "")).strip() or "Implement the planned change safely."
        normalized_changes = [{
            "target": "src/ai_dev_os",
            "action_type": "edit_file",
            "why": fallback_steps[0] if fallback_steps else summary,
            "risk_level": "medium",
        }]

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
    change_plan["verification"] = {"commands": commands, "expected_signals": expected_signals}
    change_plan["rollback_hint"] = str(change_plan.get("rollback_hint", "")).strip() or "Revert the touched files or restore the previous known-good patch."
    change_plan["approval_policy"] = approval_policy
    normalized["change_plan"] = change_plan
    return normalized


def _plan_dependency_candidates(plan: dict[str, Any]) -> list[str]:
    direct = [str(item).strip() for item in (plan.get('required_dependencies') or plan.get('dependencies') or []) if str(item).strip()]
    if direct:
        return direct
    change_plan = dict(plan.get('change_plan', {}) or {})
    changes = change_plan.get('changes', [])
    packages: list[str] = []
    if isinstance(changes, list):
        for item in changes:
            if not isinstance(item, dict):
                continue
            if str(item.get('action_type', '')).strip() != 'install_dep':
                continue
            payload = item.get('payload', {}) or {}
            if isinstance(payload, dict):
                packages.extend(str(pkg).strip() for pkg in payload.get('packages', []) if str(pkg).strip())
    return list(dict.fromkeys(packages))


def _change_plan_step_lines(change_plan: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for item in change_plan.get('changes', []):
        if not isinstance(item, dict):
            continue
        target = str(item.get('target', '')).strip() or 'unspecified_target'
        action_type = str(item.get('action_type', 'edit_file')).strip() or 'edit_file'
        why = str(item.get('why', '')).strip() or 'No rationale provided.'
        risk_level = str(item.get('risk_level', 'medium')).strip() or 'medium'
        lines.append(f"{action_type} -> {target} | risk={risk_level} | why={why}")
    return lines


def _change_plan_validation_lines(change_plan: dict[str, Any]) -> list[str]:
    verification = dict(change_plan.get('verification', {}) or {})
    commands = [f"command: {str(item).strip()}" for item in verification.get('commands', []) if str(item).strip()]
    expected = [f"expect: {str(item).strip()}" for item in verification.get('expected_signals', []) if str(item).strip()]
    return [*commands, *expected]


def _derive_batch_safe_test_targets(plan: dict[str, Any], *, workspace_root: Path) -> list[str]:
    verification = dict((plan.get("change_plan", {}) or {}).get("verification", {}) or {})
    command_candidates = [str(item).strip() for item in verification.get("commands", []) if str(item).strip()]
    validation_candidates = [str(item).strip() for item in plan.get("validation_checks", []) if str(item).strip()]
    repo_root = workspace_root.resolve()
    targets: list[str] = []
    seen: set[str] = set()
    for text in [*command_candidates, *validation_candidates]:
        for match in re.findall(r"[\w./\-]*test[\w./\-]*\.py", text):
            normalized = str(match).replace("\\", "/").lstrip("./")
            candidate = repo_root / normalized
            if not candidate.exists():
                continue
            rel = str(candidate.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
            if rel in seen:
                continue
            seen.add(rel)
            targets.append(rel)
    return targets


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _relative_to_project_root(path: Path, project_root: Path) -> str:
    return str(path.resolve().relative_to(project_root.resolve())).replace('\\', '/')


def _planned_edit_specs(*, project_root: Path, memory_paths: dict[str, str]) -> list[dict[str, Any]]:
    execution_log_relative = _relative_to_project_root(Path(memory_paths['execution_log']), project_root)
    planned = plan_edit_file(
        allowed_root=project_root,
        relative_path=execution_log_relative,
        edit_mode='append',
        timeout_seconds=10,
        human_approval_policy='no_extra_approval',
        rollback_hint='删除本次追加的 execution log 片段即可回滚。',
    )
    if not planned.get('success'):
        raise RuntimeError(f"Failed to plan edit_file action for execution_log: {planned.get('error', 'unknown error')}")
    spec = dict(planned.get('result', {}) or {})
    spec['logical_name'] = 'append_execution_log'
    spec['description'] = 'Append structured execution evidence to execution_log.'
    return [spec]


def _planned_install_dep_specs(*, plan: dict[str, Any], runtime_config: ExecutionRuntimeConfig, workspace_root: Path) -> list[dict[str, Any]]:
    dependency_candidates = _plan_dependency_candidates(plan)
    packages = [str(item).strip() for item in dependency_candidates if str(item).strip()]
    if not packages:
        return []
    planned = plan_install_dep(
        allowed_root=workspace_root,
        timeout_seconds=max(int(runtime_config.docker.default_timeout_seconds), 300),
        packages=packages,
        installer_tool='pip',
        human_approval_policy='require_human_approval',
        rollback_hint='依赖安装会改变运行环境；建议记录镜像标签或回退到上一个已知可用镜像。',
    )
    if not planned.get('success'):
        raise RuntimeError(f"Failed to plan install_dep action: {planned.get('error', 'unknown error')}")
    spec = dict(planned.get('result', {}) or {})
    spec['approval_state'] = 'pending_human_approval'
    spec['execution_state'] = 'not_executed'
    spec['description'] = 'Dependency installation is deferred until explicit human approval.'
    return [spec]


def _planned_write_specs(*, project_root: Path, execution_actions: list[ExecutionAction]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for action in execution_actions:
        content_format = 'json' if isinstance(action.content, dict) else 'text'
        planned = plan_write_file(
            allowed_root=project_root,
            relative_path=_relative_to_project_root(action.target, project_root),
            timeout_seconds=10,
            content_format=content_format,
            human_approval_policy='no_extra_approval',
            rollback_hint='删除或覆盖该目标文件即可回滚。',
        )
        if not planned.get('success'):
            raise RuntimeError(f"Failed to plan write_file action for {action.name}: {planned.get('error', 'unknown error')}")
        spec = dict(planned.get('result', {}) or {})
        spec['logical_name'] = action.name
        spec['description'] = action.description
        specs.append(spec)
    return specs


def _normalized_source_action_type(action_type: str) -> str:
    normalized = str(action_type or '').strip().lower()
    alias_map = {
        'edit': 'edit_file',
        'edit_file': 'edit_file',
        'write': 'write_file',
        'write_file': 'write_file',
    }
    return alias_map.get(normalized, normalized)


def _resolve_workspace_relative_target(target: str, workspace_root: Path) -> str:
    normalized = str(target or '').strip().replace('\\', '/')
    if not normalized:
        return normalized
    if '/' in normalized:
        return normalized
    matches: list[str] = []
    try:
        for candidate in workspace_root.rglob(normalized):
            if candidate.is_file():
                try:
                    matches.append(candidate.relative_to(workspace_root).as_posix())
                except Exception:
                    continue
                if len(matches) > 1:
                    break
    except Exception:
        return normalized
    return matches[0] if len(matches) == 1 else normalized


def _planned_source_change_specs(*, plan: dict[str, Any], workspace_root: Path) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    change_plan = dict(plan.get('change_plan', {}) or {})
    for index, item in enumerate(list(change_plan.get('changes', []) or []), start=1):
        if not isinstance(item, dict):
            continue
        action_type = _normalized_source_action_type(str(item.get('action_type', '')))
        if action_type not in {'edit_file', 'write_file'}:
            continue
        target = _resolve_workspace_relative_target(str(item.get('target', '')).strip(), workspace_root)
        payload = dict(item.get('payload', {}) or {})
        if not target:
            continue
        if action_type == 'write_file':
            planned = plan_write_file(
                allowed_root=workspace_root,
                relative_path=target,
                timeout_seconds=10,
                content_format=str(payload.get('content_format', 'text') or 'text'),
                human_approval_policy='no_extra_approval',
                rollback_hint='删除或覆盖目标源码文件即可回滚。',
            )
            if not planned.get('success'):
                raise RuntimeError(f"Failed to plan source write for {target}: {planned.get('error', 'unknown error')}")
            spec = dict(planned.get('result', {}) or {})
            spec['logical_name'] = f'source_write_{index}'
            spec['description'] = str(item.get('why', '')).strip() or f'Write source file {target}'
            spec['_source_content'] = payload.get('content', '')
            specs.append(spec)
            continue

        edit_mode = str(payload.get('edit_mode', 'replace_text') or 'replace_text')
        planned = plan_edit_file(
            allowed_root=workspace_root,
            relative_path=target,
            edit_mode=edit_mode,
            timeout_seconds=10,
            human_approval_policy='no_extra_approval',
            rollback_hint='根据 diff 证据回滚源码改动。',
        )
        if not planned.get('success'):
            raise RuntimeError(f"Failed to plan source edit for {target}: {planned.get('error', 'unknown error')}")
        spec = dict(planned.get('result', {}) or {})
        spec['logical_name'] = f'source_edit_{index}'
        spec['description'] = str(item.get('why', '')).strip() or f'Edit source file {target}'
        spec['_source_old_text'] = str(payload.get('old_text', '') or '')
        spec['_source_new_text'] = str(payload.get('new_text', '') or '')
        spec['_source_append_text'] = str(payload.get('append_text', '') or '')
        specs.append(spec)
    return specs


def _execute_source_change_specs(*, specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for spec in specs:
        payload = dict(spec.get('payload', {}) or {})
        edit_mode = str(payload.get('edit_mode', 'replace_text') or 'replace_text')
        if str(spec.get('action_type', '')) == 'write_file':
            outcome = execute_write_file_action(spec=spec, content=spec.get('_source_content', ''))
            result = {
                'name': spec.get('logical_name', 'source_write'),
                'target': str(Path(spec.get('allowed_root', '')) / payload.get('relative_path', '')),
                'description': spec.get('description', 'Structured source write'),
                'tool_bus': True,
                'write_action': {k: v for k, v in spec.items() if not str(k).startswith('_source_')},
                'status': str(outcome.get('status', 'failed') or 'failed'),
            }
            if outcome.get('status') != 'passed':
                result['error'] = str(outcome.get('error', 'unknown error') or 'unknown error')
                result['error_code'] = 'source_write_failed'
            results.append(result)
            continue

        if edit_mode == 'append':
            outcome = execute_edit_file_action(spec=spec, content=str(spec.get('_source_append_text', '') or ''))
        else:
            outcome = execute_edit_file_action(
                spec=spec,
                old_text=str(spec.get('_source_old_text', '') or ''),
                new_text=str(spec.get('_source_new_text', '') or ''),
            )
        result = {
            'name': spec.get('logical_name', 'source_edit'),
            'target': str(Path(spec.get('allowed_root', '')) / payload.get('relative_path', '')),
            'description': spec.get('description', 'Structured source edit'),
            'tool_bus': True,
            'edit_action': {k: v for k, v in spec.items() if not str(k).startswith('_source_')},
            'status': str(outcome.get('status', 'failed') or 'failed'),
        }
        if outcome.get('status') != 'passed':
            error_text = str(outcome.get('error', 'unknown error') or 'unknown error')
            result['error'] = error_text
            result['error_code'] = 'target_text_not_found' if 'target text not found' in error_text.lower() else 'source_edit_failed'
        results.append(result)
    return results

def _structured_action_bundle(*, plan: dict[str, Any], runtime_config: ExecutionRuntimeConfig, workspace_root: Path) -> tuple[list[dict[str, Any]], list[RuntimeAction], dict[str, Any]]:
    default_timeout = runtime_config.host.default_timeout_seconds if runtime_config.backend == 'host_bounded' else runtime_config.docker.default_timeout_seconds
    git_diff_spec_result = plan_git_diff(
        allowed_root=workspace_root,
        timeout_seconds=30,
        target_paths=['.'],
        diff_mode='stat',
        human_approval_policy='no_extra_approval',
        rollback_hint='只读 diff 证据动作，无需回滚。',
    )
    if not git_diff_spec_result.get("success"):
        raise RuntimeError(f"Failed to plan git_diff action: {git_diff_spec_result.get('error', 'unknown error')}")
    git_diff_spec = dict(git_diff_spec_result.get("result", {}) or {})
    git_diff_result = _baseline_git_diff_evidence(workspace_root)


    lint_spec_result = plan_run_lint(
        allowed_root=workspace_root,
        timeout_seconds=default_timeout,
        targets=["src/ai_dev_os"],
        lint_tool="ruff",
        lint_select=["E9", "F63", "F7", "F82"],
        human_approval_policy="no_extra_approval",
        rollback_hint="Lint 为只读校验动作，无需回滚。",
    )
    if not lint_spec_result.get("success"):
        raise RuntimeError(f"Failed to plan run_lint action: {lint_spec_result.get('error', 'unknown error')}")
    lint_spec = dict(lint_spec_result.get("result", {}) or {})
    lint_payload = dict(lint_spec.get("payload", {}) or {})
    lint_payload["working_subdir"] = "."

    pytest_targets = _derive_batch_safe_test_targets(plan, workspace_root=workspace_root)
    pythonpath = str((workspace_root / 'src').resolve()) if runtime_config.backend == 'host_bounded' else f"{runtime_config.docker.scratch_root}/src"
    if pytest_targets:
        spec_result = plan_run_tests(
            allowed_root=workspace_root,
            tests=pytest_targets,
            timeout_seconds=default_timeout,
            source_subdir="src",
            include_readonly_files=["README.md"],
            pythonpath=pythonpath,
            human_approval_policy="no_extra_approval",
            rollback_hint="测试动作只读执行；无需回滚代码，只需保留失败证据。",
        )
        if not spec_result.get("success"):
            raise RuntimeError(f"Failed to plan run_tests action: {spec_result.get('error', 'unknown error')}")
        spec = dict(spec_result.get("result", {}) or {})
    else:
        spec = {
            "action_type": "run_tests",
            "runtime_backend": runtime_config.backend,
            "allowed_root": str(workspace_root),
            "timeout_seconds": default_timeout,
            "rollback_hint": "测试动作只读执行；当前未声明显式 batch-safe pytest 目标，仅运行 compileall 证据。",
            "human_approval_policy": "no_extra_approval",
            "payload": {
                "tests": [],
                "source_subdir": "src",
                "include_readonly_files": ["README.md"],
                "pythonpath": pythonpath,
                "working_subdir": ".",
            },
        }
    payload = dict(spec.get("payload", {}) or {})

    lint_select = ",".join(str(item) for item in lint_payload.get("lint_select", ["E9", "F63", "F7", "F82"]))
    lint_tool = str(lint_payload.get("lint_tool", "ruff"))
    runtime_actions = []
    if runtime_config.backend == 'host_bounded':
        import shutil
        configured_python = Path(runtime_config.host.python_executable) if runtime_config.host.python_executable else None
        py_exec = str(configured_python) if configured_python and configured_python.exists() else 'python'
        configured_ruff = Path(runtime_config.host.ruff_executable) if runtime_config.host.ruff_executable else None
        ruff_exec = str(configured_ruff) if configured_ruff and configured_ruff.exists() else (shutil.which('ruff') or '')
        lint_targets = [str(target).replace('\\', '/') for target in lint_payload.get("targets", ["src/ai_dev_os"])]
        lint_command = [ruff_exec or py_exec, 'check', '--select', lint_select, *lint_targets] if ruff_exec else [py_exec, '-m', lint_tool, 'check', '--select', lint_select, *lint_targets]
        runtime_actions = [
            RuntimeAction(
                kind="run_lint.ruff",
                command=lint_command,
                working_subdir=str(lint_payload.get("working_subdir", ".")),
                timeout_seconds=int(lint_spec.get("timeout_seconds") or default_timeout),
            ),
            RuntimeAction(
                kind="run_tests.compileall",
                command=[py_exec, '-m', 'compileall', '-q', '-f', str(payload.get('source_subdir', 'src'))],
                working_subdir=str(payload.get("working_subdir", ".")),
                timeout_seconds=int(spec.get("timeout_seconds") or default_timeout),
            ),
        ]
        if pytest_targets:
            runtime_actions.append(
                RuntimeAction(
                    kind="run_tests.pytest",
                    command=[py_exec, '-m', 'pytest', '-q', *[str(item) for item in payload.get("tests", [])]],
                    working_subdir=str(payload.get("working_subdir", ".")),
                    timeout_seconds=int(spec.get("timeout_seconds") or default_timeout),
                    env={"PYTHONPATH": str(payload.get("pythonpath", pythonpath))},
                )
            )
    else:
        source_root = runtime_config.docker.source_mount_target
        scratch_root = runtime_config.docker.scratch_root
        sandbox_source = f"{scratch_root}/{payload.get('source_subdir', 'src').strip('./')}"
        prepare_parts = [
            f"rm -rf {scratch_root}",
            f"mkdir -p {sandbox_source}",
            f"cp -R {source_root}/{payload.get('source_subdir', 'src').strip('./')}/. {sandbox_source}/",
        ]
        copy_targets = list(payload.get("tests", []) or []) + list(payload.get("include_readonly_files", []) or [])
        for rel in copy_targets:
            prepare_parts.append(f"cp {source_root}/{str(rel).strip('./')} {scratch_root}/")
        prepare_parts.append(f"find {sandbox_source} -type d -name __pycache__ -prune -exec rm -rf {{}} +")
        prepare = " && ".join(prepare_parts)
        lint_targets = " ".join(f"{scratch_root}/{str(target).strip('./')}" for target in lint_payload.get("targets", ["src/ai_dev_os"]))
        test_targets = " ".join(payload.get("tests", []))
        runtime_actions = [
            RuntimeAction(
                kind="run_lint.ruff",
                command=["sh", "-lc", f"{prepare} && cd {scratch_root} && python -m {lint_tool} check --select {lint_select} {lint_targets}"],
                working_subdir=str(lint_payload.get("working_subdir", ".")),
                timeout_seconds=int(lint_spec.get("timeout_seconds") or default_timeout),
            ),
            RuntimeAction(
                kind="run_tests.compileall",
                command=["sh", "-lc", f"{prepare} && python -m compileall -q -f {sandbox_source} && find {sandbox_source} -type d -name __pycache__ -prune -exec rm -rf {{}} +"],
                working_subdir=str(payload.get("working_subdir", ".")),
                timeout_seconds=int(spec.get("timeout_seconds") or default_timeout),
            ),
        ]
        if pytest_targets:
            runtime_actions.append(
                RuntimeAction(
                    kind="run_tests.pytest",
                    command=["sh", "-lc", f"{prepare} && cd {scratch_root} && PYTHONPATH={payload.get('pythonpath', sandbox_source)} python -m pytest -q {test_targets}"],
                    working_subdir=str(payload.get("working_subdir", ".")),
                    timeout_seconds=int(spec.get("timeout_seconds") or default_timeout),
                    env={"PYTHONPATH": str(payload.get("pythonpath", sandbox_source))},
                )
            )
    action_plan = [
        git_diff_spec,
        {
            "action_type": str(lint_spec.get("action_type", "run_lint")),
            "runtime_backend": str(runtime_config.backend),
            "allowed_root": str(lint_spec.get("allowed_root", workspace_root)),
            "timeout_seconds": int(lint_spec.get("timeout_seconds") or default_timeout),
            "rollback_hint": str(lint_spec.get("rollback_hint", "")),
            "human_approval_policy": str(lint_spec.get("human_approval_policy", "no_extra_approval")),
            "payload": lint_payload,
            "expanded_runtime_kinds": [runtime_actions[0].kind],
        },
        {
            "action_type": str(spec.get("action_type", "run_tests")),
            "runtime_backend": str(runtime_config.backend),
            "allowed_root": str(spec.get("allowed_root", workspace_root)),
            "timeout_seconds": int(spec.get("timeout_seconds") or default_timeout),
            "rollback_hint": str(spec.get("rollback_hint", "")),
            "human_approval_policy": str(spec.get("human_approval_policy", "no_extra_approval")),
            "payload": payload,
            "expanded_runtime_kinds": [action.kind for action in runtime_actions],
        },
    ]
    return action_plan, runtime_actions, git_diff_result


def _runtime_report_payload(*, project_root: Path, plan: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    runtime_config = load_execution_runtime_config()
    runtime_probe = probe_execution_runtime(force=bool(runtime_config.enabled), workspace_root=workspace_root)
    action_plan, runtime_actions, git_diff_result = _structured_action_bundle(plan=plan, runtime_config=runtime_config, workspace_root=workspace_root)
    runtime_preview = [
        build_runtime_command(workspace_root=workspace_root, action=action)
        for action in runtime_actions
    ]
    runtime_execution = execute_runtime_actions(workspace_root=workspace_root, actions=runtime_actions)
    return {
        "mode": "bounded_runtime",
        "config": {
            "backend": runtime_config.backend,
            "enabled": runtime_config.enabled,
            "docker": asdict(runtime_config.docker),
            "host": asdict(runtime_config.host),
        },
        "probe": asdict(runtime_probe),
        "action_plan": action_plan,
        "observations": {"git_diff": git_diff_result},
        "runtime_command_preview": runtime_preview,
        "docker_command_preview": runtime_preview if runtime_config.backend == 'docker' else [],
        "execution": runtime_execution_to_dict(runtime_execution),
    }


class ControlledExecutionEngine:
    """Whitelisted execution layer that only writes project-local artifacts."""

    def run(
        self,
        *,
        project_id: str,
        task_id: str,
        project_root: Path,
        memory_paths: dict[str, str],
        build_result: str,
        target_workspace_root: Path | None = None,
    ) -> dict[str, Any]:
        artifacts_root = project_root / "artifacts"
        generated_root = artifacts_root / "generated"
        generated_root.mkdir(parents=True, exist_ok=True)

        workspace_root = (target_workspace_root or _repo_root()).resolve()

        warnings: list[str] = []
        try:
            plan = _extract_json_object(build_result)
        except Exception as exc:
            plan = {
                "summary": "Build result could not be parsed as JSON. Fallback execution artifacts were generated.",
                "implementation_steps": [],
                "risks": [str(exc)],
                "validation_checks": [],
            }
            warnings.append(str(exc))

        plan = _normalize_change_plan(plan)
        summary = str(plan.get("summary", "")).strip() or "No summary provided."
        implementation_steps = _change_plan_step_lines(dict(plan.get('change_plan', {}) or {})) or [str(item) for item in plan.get("implementation_steps", [])]
        risks = [str(item) for item in plan.get("risks", [])]
        validation_checks = _change_plan_validation_lines(dict(plan.get('change_plan', {}) or {})) or [str(item) for item in plan.get("validation_checks", [])]
        dependency_candidates = _plan_dependency_candidates(plan)
        if dependency_candidates and not plan.get('required_dependencies') and not plan.get('dependencies'):
            plan['required_dependencies'] = dependency_candidates

        direct_execution = bool(plan.get("direct_execution", False))
        if direct_execution:
            source_change_specs = []
            source_change_results = []
        else:
            source_change_specs = _planned_source_change_specs(plan=plan, workspace_root=workspace_root)
            source_change_results = _execute_source_change_specs(specs=source_change_specs)
        source_change_failures = [item for item in source_change_results if str(item.get('status', '')).lower() != 'passed']
        for failure in source_change_failures:
            error_code = str(failure.get('error_code', 'source_change_failed') or 'source_change_failed')
            logical_name = str(failure.get('name', 'source_change') or 'source_change')
            error_text = str(failure.get('error', 'unknown error') or 'unknown error')
            warnings.append(f"{logical_name}:{error_code}:{error_text}")
        runtime_payload = _runtime_report_payload(project_root=project_root, plan=plan, workspace_root=workspace_root)
        runtime_payload['action_plan'] = [*source_change_specs, *list(runtime_payload.get('action_plan', []) or [])]
        planned_install_dep_specs = _planned_install_dep_specs(plan=plan, runtime_config=load_execution_runtime_config(), workspace_root=workspace_root)

        execution_actions = [
            ExecutionAction(
                name="write_build_plan_json",
                target=artifacts_root / "build_plan.json",
                description="Persist the structured build plan for later review and replay.",
                content=plan,
            ),
            ExecutionAction(
                name="write_execution_runtime_plan",
                target=artifacts_root / "execution_runtime_plan.json",
                description="Persist the bounded runtime backend plan, probe result, and command previews.",
                content=runtime_payload,
            ),
            ExecutionAction(
                name="write_execution_brief",
                target=generated_root / "execution_brief.md",
                description="Write a concise execution brief derived from the build plan.",
                content=(
                    f"# Execution Brief\n"
                    f"- project_id: {project_id}\n"
                    f"- task_id: {task_id}\n"
                    f"- generated_at: {now_iso()}\n"
                    f"- runtime_backend: {runtime_payload['config']['backend']}\n"
                    f"- runtime_enabled: {runtime_payload['config']['enabled']}\n"
                    f"- runtime_available: {runtime_payload['probe']['available']}\n\n"
                    f"## Summary\n{summary}\n\n"
                    f"## Implementation Steps\n{_to_markdown_list(implementation_steps, 'No implementation steps were provided.')}\n\n"
                    f"## Risks\n{_to_markdown_list(risks, 'No explicit risks were provided.')}\n\n"
                    f"## Change Plan Rollback\n- {str((plan.get('change_plan', {}) or {}).get('rollback_hint', 'No rollback hint was provided.'))}\n"
                ),
            ),
            ExecutionAction(
                name="write_validation_checklist",
                target=generated_root / "validation_checklist.md",
                description="Write a validation checklist for downstream review and execution.",
                content=(
                    f"# Validation Checklist\n"
                    f"- project_id: {project_id}\n"
                    f"- task_id: {task_id}\n"
                    f"- generated_at: {now_iso()}\n\n"
                    f"{_to_markdown_list(validation_checks, 'No validation checks were provided.')}\n"
                ),
            ),
        ]
        allowed_actions = set(execution_whitelist())
        execution_actions = [action for action in execution_actions if action.name in allowed_actions]
        planned_write_specs = _planned_write_specs(project_root=project_root, execution_actions=execution_actions)
        planned_edit_specs = _planned_edit_specs(project_root=project_root, memory_paths=memory_paths)
        runtime_payload['action_plan'].extend(planned_install_dep_specs)
        runtime_payload['action_plan'].extend(planned_write_specs)
        runtime_payload['action_plan'].extend(planned_edit_specs)

        results: list[dict[str, Any]] = [*source_change_results]
        for action, spec in zip(execution_actions, planned_write_specs):
            outcome = execute_write_file_action(spec=spec, content=action.content)
            if outcome.get('status') != 'passed':
                raise RuntimeError(f"Tool Bus write failed for {action.name}: {outcome.get('error', 'unknown error')}")
            results.append(
                {
                    "name": action.name,
                    "target": str(action.target),
                    "description": action.description,
                    "tool_bus": True,
                    "write_action": spec,
                }
            )

        runtime_exec = runtime_payload["execution"]
        runtime_action_lines = []
        for action_result in runtime_exec.get("actions", []):
            runtime_action_lines.append(
                f"  - {action_result['kind']}: {action_result['status']} / returncode={action_result.get('returncode')} / duration_ms={action_result.get('duration_ms', 0)}"
            )
            if action_result.get("stdout"):
                runtime_action_lines.append(f"    stdout: {str(action_result['stdout'])[:240]}")
            if action_result.get("stderr"):
                runtime_action_lines.append(f"    stderr: {str(action_result['stderr'])[:240]}")
        runtime_action_text = "\n".join(runtime_action_lines) if runtime_action_lines else "  - none"
        git_diff_obs = dict(runtime_payload.get("observations", {}).get("git_diff", {}) or {})
        git_diff_text = f"status={git_diff_obs.get('status', 'unknown')} / duration_ms={git_diff_obs.get('duration_ms', 0)}"
        if git_diff_obs.get("stdout"):
            git_diff_text += f" / {str(git_diff_obs.get('stdout', ''))[:240]}"
        elif git_diff_obs.get("stderr"):
            git_diff_text += f" / {str(git_diff_obs.get('stderr', ''))[:240]}"
        execution_log_append = (
            f"## Controlled Execution\n"
            f"- project_id: {project_id}\n"
            f"- task_id: {task_id}\n"
            f"- actions: {', '.join(action['name'] for action in results)}\n"
            f"- runtime_backend: {runtime_payload['config']['backend']}\n"
            f"- runtime_enabled: {runtime_payload['config']['enabled']}\n"
            f"- runtime_available: {runtime_payload['probe']['available']} ({runtime_payload['probe']['detail']})\n"
            f"- runtime_status: {runtime_exec['status']} / severity={runtime_exec['severity']} / interrupt={runtime_exec['should_interrupt']}\n"
            f"- runtime_duration_ms: {runtime_exec['duration_ms']}\n"
            f"- git_diff:\n"
            f"  - {git_diff_text}\n"
            f"- runtime_actions:\n"
            f"{runtime_action_text}\n"
            f"- warnings: {', '.join(warnings) if warnings else 'none'}\n"
            f"- generated_at: {now_iso()}"
        )
        for spec in planned_edit_specs:
            outcome = execute_edit_file_action(spec=spec, content=execution_log_append)
            if outcome.get('status') != 'passed':
                raise RuntimeError(f"Tool Bus edit failed for {spec.get('logical_name', 'edit_file')}: {outcome.get('error', 'unknown error')}")
            results.append(
                {
                    'name': spec.get('logical_name', 'edit_file'),
                    'target': str(Path(project_root) / spec.get('payload', {}).get('relative_path', '')),
                    'description': spec.get('description', 'Structured file edit'),
                    'tool_bus': True,
                    'edit_action': spec,
                }
            )

        report = {
            "project_id": project_id,
            "task_id": task_id,
            "executed_at": now_iso(),
            "mode": "controlled",
            "runtime_mode": runtime_payload['mode'],
            "runtime": runtime_payload,
            "actions": results,
            "warnings": warnings,
        }
        report_path = artifacts_root / "execution_report.json"
        report_outcome = write_project_json(
            path=report_path,
            payload=report,
            allowed_root=project_root,
        )
        if not report_outcome.get("success"):
            raise RuntimeError(f"Tool Bus write failed for execution report: {report_outcome.get('error', 'unknown error')}")

        failed_actions = [action for action in results if str(action.get('status', 'passed')).lower() != 'passed']

        return {
            "success_count": len([action for action in results if str(action.get('status', 'passed')).lower() == 'passed']),
            "failed_count": len(failed_actions),
            "total_actions": len(results),
            "warnings": warnings,
            "artifacts": [action["target"] for action in results] + [str(report_path)],
            "summary": summary,
            "status": "failed" if failed_actions else "passed",
            "runtime_mode": runtime_payload['mode'],
            "runtime_backend": runtime_payload['config']['backend'],
            "runtime_available": runtime_payload['probe']['available'],
            "runtime_status": runtime_payload['execution']['status'],
            "runtime_detail": runtime_payload['execution']['detail'],
            "runtime_severity": runtime_payload['execution']['severity'],
            "runtime_should_interrupt": runtime_payload['execution']['should_interrupt'],
            "runtime_duration_ms": runtime_payload['execution']['duration_ms'],
            "git_diff_status": str((runtime_payload.get('observations', {}).get('git_diff', {}) or {}).get('status', '')),
            "git_diff_detail": str((runtime_payload.get('observations', {}).get('git_diff', {}) or {}).get('stdout') or (runtime_payload.get('observations', {}).get('git_diff', {}) or {}).get('stderr') or ''),
        }


execution_engine = ControlledExecutionEngine()


def run_controlled_execution(
    *,
    project_id: str,
    task_id: str,
    project_root: Path,
    memory_paths: dict[str, str],
    build_result: str,
    target_workspace_root: Path | None = None,
) -> dict[str, Any]:
    return execution_engine.run(
        project_id=project_id,
        task_id=task_id,
        project_root=project_root,
        memory_paths=memory_paths,
        build_result=build_result,
        target_workspace_root=target_workspace_root,
    )






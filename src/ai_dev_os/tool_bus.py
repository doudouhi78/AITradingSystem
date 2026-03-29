# [READ-ONLY REFERENCE] 本文件停止新增功能，仅作参考。2026-03-29
"""Tool Bus - 统一工具调用接口层。

当前主线只启用最安全的一层：
- 项目目录内的受控文件操作

更高风险的能力如 shell/git/code execution 仍然保留实现，
但默认不接入主执行链，避免 LLM 直接获得越权执行面。
"""

import subprocess
import os
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, Union
from datetime import datetime
import json

from ai_dev_os.project_mcp import get_current_baseline
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_mcp import list_experiment_runs
from ai_dev_os.project_mcp import list_memory_documents
from ai_dev_os.project_mcp import read_memory_document
from ai_dev_os.project_mcp import list_trace_session_summaries
from ai_dev_os.project_mcp import get_trace_session
from ai_dev_os.project_mcp import get_validation_record
from ai_dev_os.project_mcp import list_search_spec_summaries
from ai_dev_os.project_mcp import get_search_spec
from ai_dev_os.project_mcp import get_formal_review
from ai_dev_os.project_mcp import list_formal_review_summaries


@dataclass(frozen=True)
class ExecutionActionSpec:
    action_type: str
    runtime_backend: str
    allowed_root: str
    timeout_seconds: int
    rollback_hint: str
    human_approval_policy: str
    payload: Dict[str, Any]


class ToolBus:
    """统一工具总线，管理所有工具调用"""
    
    def __init__(self):
        self.tools = {
            'file_operations': self._file_operations,
            'shell_commands': self._shell_commands,
            'git_operations': self._git_operations,
            'code_execution': self._code_execution,
            'execution_actions': self._execution_actions,
            'project_mcp': self._project_mcp,
        }
    
    def call_tool(self, tool_name: str, **kwargs) -> Dict[str, Any]:
        """统一工具调用入口"""
        if tool_name not in self.tools:
            return {
                "success": False,
                "error": f"Tool '{tool_name}' not found",
                "timestamp": datetime.now().isoformat(),
                "result": None
            }
        
        try:
            # 记录工具调用日志
            self._log_tool_call(tool_name, kwargs)
            
            # 执行工具
            result = self.tools[tool_name](**kwargs)
            
            # 记录成功结果
            self._log_tool_result(tool_name, kwargs, result, True)
            
            return {
                "success": True,
                "error": None,
                "timestamp": datetime.now().isoformat(),
                "result": result
            }
        except Exception as e:
            # 记录错误结果
            error_result = {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "result": None
            }
            self._log_tool_result(tool_name, kwargs, error_result, False)
            return error_result
    
    def _log_tool_call(self, tool_name: str, params: Dict[str, Any]) -> None:
        """记录工具调用日志"""
        log_entry = {
            "tool_name": tool_name,
            "params": params,
            "call_time": datetime.now().isoformat(),
            "type": "tool_call"
        }
        self._write_log(log_entry)
    
    def _log_tool_result(self, tool_name: str, params: Dict[str, Any], result: Dict[str, Any], success: bool) -> None:
        """记录工具执行结果"""
        log_entry = {
            "tool_name": tool_name,
            "params": params,
            "result": result,
            "success": success,
            "call_time": datetime.now().isoformat(),
            "type": "tool_result"
        }
        self._write_log(log_entry)
    
    def _write_log(self, log_entry: Dict[str, Any]) -> None:
        """写入日志到文件"""
        # 创建日志目录
        log_dir = Path("runtime/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 写入日志文件
        log_file = log_dir / f"tool_bus_{datetime.now().strftime('%Y%m%d')}.jsonl"
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry) + '\n')

    def _resolve_allowed_path(self, file_path: Path, allowed_root: Optional[str] = None) -> Path:
        resolved = file_path.resolve()
        if not allowed_root:
            return resolved
        allowed = Path(allowed_root).resolve()
        if resolved != allowed and allowed not in resolved.parents:
            raise PermissionError(f"Path '{resolved}' is outside allowed root '{allowed}'.")
        return resolved
    
    def _file_operations(self, operation: str, **kwargs) -> Any:
        """文件操作工具"""
        file_path = Path(kwargs.get('path', ''))
        allowed_root = kwargs.get('allowed_root')
        resolved_path = self._resolve_allowed_path(file_path, allowed_root)
        
        if operation == 'read':
            return resolved_path.read_text(encoding=kwargs.get('encoding', 'utf-8'))
        elif operation == 'write':
            content = kwargs.get('content', '')
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            resolved_path.write_text(content, encoding=kwargs.get('encoding', 'utf-8'))
            return f"File written: {resolved_path}"
        elif operation == 'append':
            content = kwargs.get('content', '')
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            with open(resolved_path, 'a', encoding=kwargs.get('encoding', 'utf-8')) as handle:
                handle.write(content)
            return f"File appended: {resolved_path}"
        elif operation == 'replace_text':
            old_text = kwargs.get('old_text', '')
            new_text = kwargs.get('new_text', '')
            current = resolved_path.read_text(encoding=kwargs.get('encoding', 'utf-8'))
            if old_text not in current:
                raise ValueError(f"Target text not found in {resolved_path}")
            resolved_path.write_text(current.replace(old_text, new_text, 1), encoding=kwargs.get('encoding', 'utf-8'))
            return f"File edited: {resolved_path}"
        elif operation == 'exists':
            return resolved_path.exists()
        elif operation == 'list_dir':
            return [str(p) for p in resolved_path.iterdir()]
        elif operation == 'mkdir':
            resolved_path.mkdir(parents=True, exist_ok=True)
            return f"Directory created: {resolved_path}"
        elif operation == 'delete':
            if resolved_path.is_file():
                resolved_path.unlink()
                return f"File deleted: {resolved_path}"
            elif resolved_path.is_dir():
                import shutil
                shutil.rmtree(resolved_path)
                return f"Directory deleted: {resolved_path}"
            else:
                return f"Path does not exist: {resolved_path}"
        else:
            raise ValueError(f"Unsupported file operation: {operation}")
    
    def _shell_commands(self, command: str, cwd: Optional[str] = None, timeout: int = 30) -> Dict[str, str]:
        """执行Shell命令。默认禁用，直到显式接入治理白名单。"""
        raise NotImplementedError(
            "shell_commands is not enabled in the mainline. "
            "Requires explicit governance whitelist approval before use."
        )
    
    def _git_operations(self, operation: str, **kwargs) -> Any:
        """Git操作工具。默认禁用，直到显式接入治理白名单。"""
        raise NotImplementedError(
            "git_operations is not enabled in the mainline. "
            "Requires explicit governance whitelist approval before use."
        )
    
    def _execution_actions(self, operation: str, **kwargs) -> Dict[str, Any]:
        """结构化执行动作规划。只生成动作协议，不直接获得宿主机执行权。"""
        allowed_root = str(self._resolve_allowed_path(Path(kwargs.get('allowed_root', '')), kwargs.get('allowed_root')))
        timeout_seconds = int(kwargs.get('timeout_seconds', 120))
        if operation == 'plan_run_lint':
            targets = [str(item) for item in kwargs.get('targets', ['src/ai_dev_os'])] or ['src/ai_dev_os']
            spec = ExecutionActionSpec(
                action_type='run_lint',
                runtime_backend=str(kwargs.get('runtime_backend', 'docker')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', 'Lint 为只读校验动作，无需回滚。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'no_extra_approval')),
                payload={
                    'targets': targets,
                    'lint_tool': str(kwargs.get('lint_tool', 'ruff')),
                    'lint_select': [str(item) for item in kwargs.get('lint_select', ['E9', 'F63', 'F7', 'F82'])],
                    'working_subdir': str(kwargs.get('working_subdir', '.')),
                },
            )
            return asdict(spec)
        if operation == 'plan_install_dep':
            packages = [str(item).strip() for item in kwargs.get('packages', []) if str(item).strip()]
            requirements_file = str(kwargs.get('requirements_file', '')).strip()
            if not packages and not requirements_file:
                raise ValueError('packages or requirements_file is required for plan_install_dep')
            spec = ExecutionActionSpec(
                action_type='install_dep',
                runtime_backend=str(kwargs.get('runtime_backend', 'docker')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', '依赖安装会改变运行环境；建议记录镜像标签或回退到上一个已知可用镜像。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'require_human_approval')),
                payload={
                    'packages': packages,
                    'requirements_file': requirements_file,
                    'installer_tool': str(kwargs.get('installer_tool', 'pip')),
                    'working_subdir': str(kwargs.get('working_subdir', '.')),
                },
            )
            return asdict(spec)
        if operation == 'plan_run_tests':
            tests = [str(item) for item in kwargs.get('tests', [])]
            if not tests:
                raise ValueError('tests is required for plan_run_tests')
            source_subdir = str(kwargs.get('source_subdir', 'src'))
            include_readonly_files = [str(item) for item in kwargs.get('include_readonly_files', [])]
            spec = ExecutionActionSpec(
                action_type='run_tests',
                runtime_backend=str(kwargs.get('runtime_backend', 'docker')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', '测试动作只读执行；无需回滚代码，只需保留失败证据。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'no_extra_approval')),
                payload={
                    'source_subdir': source_subdir,
                    'tests': tests,
                    'include_readonly_files': include_readonly_files,
                    'pythonpath': str(kwargs.get('pythonpath', '/sandbox/src')),
                    'working_subdir': str(kwargs.get('working_subdir', '.')),
                },
            )
            return asdict(spec)
        if operation == 'plan_git_diff':
            target_paths = [str(item) for item in kwargs.get('target_paths', ['.'])] or ['.']
            spec = ExecutionActionSpec(
                action_type='git_diff',
                runtime_backend=str(kwargs.get('runtime_backend', 'host_readonly')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', '只读 diff 证据动作，无需回滚。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'no_extra_approval')),
                payload={
                    'target_paths': target_paths,
                    'diff_mode': str(kwargs.get('diff_mode', 'stat')),
                    'working_subdir': str(kwargs.get('working_subdir', '.')),
                },
            )
            return asdict(spec)
        if operation == 'plan_write_file':
            relative_path = str(kwargs.get('relative_path', '')).strip()
            if not relative_path:
                raise ValueError('relative_path is required for plan_write_file')
            spec = ExecutionActionSpec(
                action_type='write_file',
                runtime_backend=str(kwargs.get('runtime_backend', 'tool_bus_file_ops')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', '删除或覆盖该目标文件即可回滚。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'no_extra_approval')),
                payload={
                    'relative_path': relative_path,
                    'content_format': str(kwargs.get('content_format', 'text')),
                    'encoding': str(kwargs.get('encoding', 'utf-8')),
                    'create_parents': bool(kwargs.get('create_parents', True)),
                },
            )
            return asdict(spec)
        if operation == 'plan_edit_file':
            relative_path = str(kwargs.get('relative_path', '')).strip()
            if not relative_path:
                raise ValueError('relative_path is required for plan_edit_file')
            edit_mode = str(kwargs.get('edit_mode', 'append'))
            if edit_mode not in {'append', 'replace_text'}:
                raise ValueError('edit_mode must be append or replace_text')
            spec = ExecutionActionSpec(
                action_type='edit_file',
                runtime_backend=str(kwargs.get('runtime_backend', 'tool_bus_file_ops')),
                allowed_root=allowed_root,
                timeout_seconds=timeout_seconds,
                rollback_hint=str(kwargs.get('rollback_hint', '根据 diff 证据回滚到上一个文件版本。')),
                human_approval_policy=str(kwargs.get('human_approval_policy', 'no_extra_approval')),
                payload={
                    'relative_path': relative_path,
                    'edit_mode': edit_mode,
                    'encoding': str(kwargs.get('encoding', 'utf-8')),
                },
            )
            return asdict(spec)
        raise ValueError(f"Unsupported execution action operation: {operation}")


    def _project_mcp(self, operation: str, **kwargs) -> Any:
        """Project-local read-only MCP surface for memory and experiment objects."""
        if operation == 'list_memory_documents':
            return list_memory_documents()
        if operation == 'read_memory_document':
            return read_memory_document(str(kwargs.get('name', '')).strip())
        if operation == 'list_experiment_runs':
            return list_experiment_runs(
                limit=int(kwargs.get('limit', 20) or 20),
                strategy_family=str(kwargs.get('strategy_family', '') or ''),
                status_code=str(kwargs.get('status_code', '') or ''),
            )
        if operation == 'get_experiment_run':
            return get_experiment_run(str(kwargs.get('experiment_id', '')).strip())
        if operation == 'get_current_baseline':
            return get_current_baseline()
        if operation == 'get_validation_record':
            return get_validation_record(str(kwargs.get('validation_id', '')).strip())
        if operation == 'list_search_specs':
            return list_search_spec_summaries(limit=int(kwargs.get('limit', 20) or 20))
        if operation == 'get_search_spec':
            return get_search_spec(str(kwargs.get('search_id', '')).strip())
        if operation == 'list_formal_reviews':
            return list_formal_review_summaries(
                limit=int(kwargs.get('limit', 20) or 20),
                experiment_id=str(kwargs.get('experiment_id', '') or ''),
                baseline_experiment_id=str(kwargs.get('baseline_experiment_id', '') or ''),
            )
        if operation == 'get_formal_review':
            return get_formal_review(str(kwargs.get('review_id', '')).strip())
        if operation == 'list_trace_sessions':
            return list_trace_session_summaries(limit=int(kwargs.get('limit', 20) or 20))
        if operation == 'get_trace_session':
            return get_trace_session(str(kwargs.get('run_id', '')).strip())
        raise ValueError(f"Unsupported project_mcp operation: {operation}")

    def _code_execution(self, language: str, code: str, **kwargs) -> Dict[str, str]:
        """代码执行工具。默认禁用，直到显式接入治理白名单。"""
        raise NotImplementedError(
            "code_execution is not enabled in the mainline. "
            "Requires explicit governance whitelist approval before use."
        )


def plan_run_lint(*, allowed_root: Union[str, Path], timeout_seconds: int = 90, targets: list[str] | None = None, lint_tool: str = 'ruff', lint_select: list[str] | None = None, human_approval_policy: str = 'no_extra_approval', rollback_hint: str = 'Lint 为只读校验动作，无需回滚。') -> Dict[str, Any]:
    """Plan a structured run_lint action."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_run_lint',
        allowed_root=str(allowed_root),
        timeout_seconds=timeout_seconds,
        targets=list(targets or ['src/ai_dev_os']),
        lint_tool=lint_tool,
        lint_select=list(lint_select or ['E9', 'F63', 'F7', 'F82']),
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='docker',
    )


def plan_install_dep(*, allowed_root: Union[str, Path], timeout_seconds: int = 300, packages: list[str] | None = None, requirements_file: str = '', installer_tool: str = 'pip', human_approval_policy: str = 'require_human_approval', rollback_hint: str = '依赖安装会改变运行环境；建议记录镜像标签或回退到上一个已知可用镜像。') -> Dict[str, Any]:
    """Plan a structured install_dep action. Default policy requires explicit human approval."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_install_dep',
        allowed_root=str(allowed_root),
        timeout_seconds=timeout_seconds,
        packages=list(packages or []),
        requirements_file=requirements_file,
        installer_tool=installer_tool,
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='docker',
    )


def plan_run_tests(*, allowed_root: Union[str, Path], tests: list[str], timeout_seconds: int, source_subdir: str = 'src', include_readonly_files: list[str] | None = None, pythonpath: str = '/sandbox/src', human_approval_policy: str = 'no_extra_approval', rollback_hint: str = '测试动作只读执行；无需回滚代码，只需保留失败证据。') -> Dict[str, Any]:
    """Plan a structured run_tests action for the bounded runtime."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_run_tests',
        allowed_root=str(allowed_root),
        tests=tests,
        timeout_seconds=timeout_seconds,
        source_subdir=source_subdir,
        include_readonly_files=list(include_readonly_files or []),
        pythonpath=pythonpath,
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='docker',
    )


def plan_git_diff(*, allowed_root: Union[str, Path], timeout_seconds: int = 30, target_paths: list[str] | None = None, diff_mode: str = 'stat', human_approval_policy: str = 'no_extra_approval', rollback_hint: str = '只读 diff 证据动作，无需回滚。') -> Dict[str, Any]:
    """Plan a structured git_diff action."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_git_diff',
        allowed_root=str(allowed_root),
        timeout_seconds=timeout_seconds,
        target_paths=list(target_paths or ['.']),
        diff_mode=diff_mode,
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='host_readonly',
    )


def plan_write_file(*, allowed_root: Union[str, Path], relative_path: str, timeout_seconds: int = 10, content_format: str = 'text', human_approval_policy: str = 'no_extra_approval', rollback_hint: str = '删除或覆盖该目标文件即可回滚。') -> Dict[str, Any]:
    """Plan a structured write_file action."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_write_file',
        allowed_root=str(allowed_root),
        relative_path=relative_path,
        timeout_seconds=timeout_seconds,
        content_format=content_format,
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='tool_bus_file_ops',
    )


def plan_edit_file(*, allowed_root: Union[str, Path], relative_path: str, edit_mode: str = 'append', timeout_seconds: int = 10, human_approval_policy: str = 'no_extra_approval', rollback_hint: str = '根据 diff 证据回滚到上一个文件版本。') -> Dict[str, Any]:
    """Plan a structured edit_file action."""
    return tool_bus.call_tool(
        'execution_actions',
        operation='plan_edit_file',
        allowed_root=str(allowed_root),
        relative_path=relative_path,
        edit_mode=edit_mode,
        timeout_seconds=timeout_seconds,
        human_approval_policy=human_approval_policy,
        rollback_hint=rollback_hint,
        runtime_backend='tool_bus_file_ops',
    )


def execute_edit_file_action(*, spec: Dict[str, Any], content: str = '', old_text: str = '', new_text: str = '') -> Dict[str, Any]:
    """Execute a structured edit_file action via safe file operations."""
    payload = dict(spec.get('payload', {}) or {})
    allowed_root = str(spec.get('allowed_root', ''))
    relative_path = str(payload.get('relative_path', '')).strip()
    edit_mode = str(payload.get('edit_mode', 'append'))
    if not allowed_root or not relative_path:
        return {'action_type': 'edit_file', 'status': 'failed', 'error': 'allowed_root and relative_path are required'}
    target = Path(allowed_root) / relative_path
    if edit_mode == 'append':
        result = tool_bus.call_tool(
            'file_operations',
            operation='append',
            path=str(target),
            content=content,
            allowed_root=allowed_root,
            encoding=str(payload.get('encoding', 'utf-8')),
        )
    else:
        result = tool_bus.call_tool(
            'file_operations',
            operation='replace_text',
            path=str(target),
            old_text=old_text,
            new_text=new_text,
            allowed_root=allowed_root,
            encoding=str(payload.get('encoding', 'utf-8')),
        )
    return {
        'action_type': 'edit_file',
        'status': 'passed' if result.get('success') else 'failed',
        'target': str(target),
        'result': result.get('result'),
        'error': result.get('error'),
        'relative_path': relative_path,
        'edit_mode': edit_mode,
    }


def execute_write_file_action(*, spec: Dict[str, Any], content: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Execute a structured write_file action via the existing safe file_operations path."""
    payload = dict(spec.get('payload', {}) or {})
    allowed_root = str(spec.get('allowed_root', ''))
    relative_path = str(payload.get('relative_path', '')).strip()
    if not allowed_root or not relative_path:
        return {'action_type': 'write_file', 'status': 'failed', 'error': 'allowed_root and relative_path are required'}
    target = Path(allowed_root) / relative_path
    if payload.get('content_format') == 'json' and isinstance(content, dict):
        result = write_project_json(path=target, payload=content, allowed_root=allowed_root)
    else:
        rendered = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False, indent=2) + '\n'
        result = write_project_file(path=target, content=rendered, allowed_root=allowed_root)
    return {
        'action_type': 'write_file',
        'status': 'passed' if result.get('success') else 'failed',
        'target': str(target),
        'result': result.get('result'),
        'error': result.get('error'),
        'relative_path': relative_path,
    }


def collect_git_diff_evidence(*, allowed_root: Union[str, Path], timeout_seconds: int = 30, target_paths: list[str] | None = None, diff_mode: str = 'stat') -> Dict[str, Any]:
    """Collect read-only git diff evidence under an allowed root."""
    root = tool_bus._resolve_allowed_path(Path(str(allowed_root)), str(allowed_root))
    args = ['git', '-C', str(root), 'diff']
    if diff_mode == 'stat':
        args.append('--stat')
    args.extend(['--', *(list(target_paths or ['.']))])
    started = datetime.now()
    try:
        proc = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout_seconds, check=False)
        duration_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            'action_type': 'git_diff',
            'status': 'passed' if proc.returncode == 0 else 'failed',
            'returncode': proc.returncode,
            'stdout': (proc.stdout or '').strip(),
            'stderr': (proc.stderr or '').strip(),
            'duration_ms': duration_ms,
            'command': args,
            'target_paths': list(target_paths or ['.']),
            'diff_mode': diff_mode,
        }
    except subprocess.TimeoutExpired as exc:
        duration_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {
            'action_type': 'git_diff',
            'status': 'failed',
            'returncode': None,
            'stdout': ((exc.stdout or '') if isinstance(exc.stdout, str) else '').strip(),
            'stderr': f'Timed out after {timeout_seconds}s.',
            'duration_ms': duration_ms,
            'command': args,
            'target_paths': list(target_paths or ['.']),
            'diff_mode': diff_mode,
        }


# 全局ToolBus实例
tool_bus = ToolBus()


def call_tool_safely(tool_name: str, **kwargs) -> Dict[str, Any]:
    """安全调用工具的便捷函数"""
    return tool_bus.call_tool(tool_name, **kwargs)


def write_project_file(*, path: Union[str, Path], content: str, allowed_root: Union[str, Path]) -> Dict[str, Any]:
    """Only allow project-local file writes through the tool bus."""
    return tool_bus.call_tool(
        "file_operations",
        operation="write",
        path=str(path),
        content=content,
        allowed_root=str(allowed_root),
        encoding="utf-8",
    )


def write_project_json(*, path: Union[str, Path], payload: Dict[str, Any], allowed_root: Union[str, Path]) -> Dict[str, Any]:
    """Project-local JSON write helper for the controlled execution layer."""
    return tool_bus.call_tool(
        "file_operations",
        operation="write",
        path=str(path),
        content=json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        allowed_root=str(allowed_root),
        encoding="utf-8",
    )


def list_project_directory(*, path: Union[str, Path], allowed_root: Union[str, Path]) -> Dict[str, Any]:
    """Project-local directory listing helper."""
    return tool_bus.call_tool(
        "file_operations",
        operation="list_dir",
        path=str(path),
        allowed_root=str(allowed_root),
    )





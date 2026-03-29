from __future__ import annotations

import json
import shutil
import subprocess
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "backend": "docker",
    "enabled": False,
    "docker": {
        "image": "langgraphlab-runtime:py311",
        "source_mount_target": "/workspace_ro",
        "scratch_root": "/sandbox",
        "source_mount_readonly": True,
        "network_enabled": False,
        "default_timeout_seconds": 120,
        "allow_host_env_passthrough": False,
    },
    "host": {
        "default_timeout_seconds": 120,
        "allowed_workspace_roots": ["runtime/host_experiment_workspaces"],
        "blocked_prefixes": ["C:\\", "E:\\", "F:\\"],
    },
}


@dataclass(frozen=True)
class DockerRuntimeConfig:
    image: str
    source_mount_target: str
    scratch_root: str
    source_mount_readonly: bool
    network_enabled: bool
    default_timeout_seconds: int
    allow_host_env_passthrough: bool


@dataclass(frozen=True)
class HostRuntimeConfig:
    default_timeout_seconds: int
    allowed_workspace_roots: tuple[str, ...]
    blocked_prefixes: tuple[str, ...]
    python_executable: str
    ruff_executable: str


@dataclass(frozen=True)
class ExecutionRuntimeConfig:
    backend: str
    enabled: bool
    docker: DockerRuntimeConfig
    host: HostRuntimeConfig


@dataclass(frozen=True)
class RuntimeProbeResult:
    backend: str
    enabled: bool
    available: bool
    detail: str


@dataclass(frozen=True)
class RuntimeAction:
    kind: str
    command: list[str]
    working_subdir: str = "."
    timeout_seconds: int | None = None
    env: dict[str, str] | None = None
    network_enabled: bool | None = None


@dataclass(frozen=True)
class RuntimeActionResult:
    kind: str
    command: list[str]
    status: str
    returncode: int | None
    stdout: str
    stderr: str
    duration_ms: int


@dataclass(frozen=True)
class RuntimeExecutionResult:
    backend: str
    status: str
    detail: str
    severity: str
    should_interrupt: bool
    actions: list[RuntimeActionResult]
    duration_ms: int


def _excerpt(text: str, limit: int = 220) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _config_path() -> Path:
    return _repo_root() / "config" / "execution_runtime.json"


def _normalize_host_root(entry: str) -> str:
    value = str(entry or "").strip()
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        path = (_repo_root() / value).resolve()
    else:
        path = path.resolve()
    return str(path)

def _normalize_host_tool(entry: str) -> str:
    value = str(entry or "").strip()
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        path = (_repo_root() / value).resolve()
    else:
        path = path.resolve()
    return str(path)


def load_execution_runtime_config() -> ExecutionRuntimeConfig:
    payload = dict(DEFAULT_RUNTIME_CONFIG)
    path = _config_path()
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        payload.update(loaded)
        payload["docker"] = {**DEFAULT_RUNTIME_CONFIG["docker"], **dict(loaded.get("docker", {}))}
        payload["host"] = {**DEFAULT_RUNTIME_CONFIG["host"], **dict(loaded.get("host", {}))}
    docker = payload["docker"]
    host = payload["host"]
    allowed_workspace_roots = tuple(
        item for item in (_normalize_host_root(entry) for entry in host.get("allowed_workspace_roots", [])) if item
    )
    blocked_prefixes = tuple(str(item) for item in host.get("blocked_prefixes", []))
    python_executable = _normalize_host_tool(host.get("python_executable", ""))
    ruff_executable = _normalize_host_tool(host.get("ruff_executable", ""))
    return ExecutionRuntimeConfig(
        backend=str(payload.get("backend", "docker")),
        enabled=bool(payload.get("enabled", False)),
        docker=DockerRuntimeConfig(
            image=str(docker.get("image", "langgraphlab-runtime:py311")),
            source_mount_target=str(docker.get("source_mount_target", "/workspace_ro")),
            scratch_root=str(docker.get("scratch_root", "/sandbox")),
            source_mount_readonly=bool(docker.get("source_mount_readonly", True)),
            network_enabled=bool(docker.get("network_enabled", False)),
            default_timeout_seconds=int(docker.get("default_timeout_seconds", 120)),
            allow_host_env_passthrough=bool(docker.get("allow_host_env_passthrough", False)),
        ),
        host=HostRuntimeConfig(
            default_timeout_seconds=int(host.get("default_timeout_seconds", 120)),
            allowed_workspace_roots=allowed_workspace_roots,
            blocked_prefixes=blocked_prefixes,
            python_executable=python_executable,
            ruff_executable=ruff_executable,
        ),
    )


def _validate_host_workspace(workspace_root: Path, config: HostRuntimeConfig) -> tuple[bool, str]:
    root = workspace_root.resolve()
    root_text = str(root)
    for prefix in config.blocked_prefixes:
        if root_text.lower().startswith(prefix.lower()):
            return False, f"workspace root {root} is blocked by prefix policy"
    if not config.allowed_workspace_roots:
        return True, "host workspace policy not configured; allowing current workspace"
    for allowed in config.allowed_workspace_roots:
        try:
            root.relative_to(Path(allowed).resolve())
            return True, f"host workspace allowed under {allowed}"
        except ValueError:
            continue
    return False, f"workspace root {root} is outside allowed host experiment roots"


def probe_execution_runtime(*, force: bool = False, workspace_root: Path | None = None) -> RuntimeProbeResult:
    config = load_execution_runtime_config()
    if not config.enabled and not force:
        return RuntimeProbeResult(config.backend, config.enabled, False, "runtime disabled; probe skipped")
    if config.backend == "docker":
        docker_bin = shutil.which("docker")
        if not docker_bin:
            return RuntimeProbeResult("docker", config.enabled, False, "docker binary not found")
        try:
            proc = subprocess.run(
                [docker_bin, "version", "--format", "{{.Server.Version}}"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=8,
                check=False,
            )
        except Exception as exc:
            return RuntimeProbeResult("docker", config.enabled, False, f"docker probe failed: {exc}")
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "docker server unavailable").strip()
            return RuntimeProbeResult("docker", config.enabled, False, detail)
        return RuntimeProbeResult("docker", config.enabled, True, (proc.stdout or "available").strip())
    if config.backend == "host_bounded":
        root = workspace_root or _repo_root()
        allowed, detail = _validate_host_workspace(root, config.host)
        return RuntimeProbeResult("host_bounded", config.enabled, allowed, detail)
    return RuntimeProbeResult(config.backend, config.enabled, False, "Unsupported backend.")


def build_docker_command(*, workspace_root: Path, action: RuntimeAction) -> list[str]:
    config = load_execution_runtime_config()
    docker_bin = shutil.which("docker") or "docker"
    source_target = config.docker.source_mount_target
    scratch_root = config.docker.scratch_root
    workdir = f"{scratch_root}/{action.working_subdir.strip('./')}" if action.working_subdir not in {"", "."} else scratch_root
    mount = f"type=bind,src={workspace_root.resolve()},dst={source_target}"
    if config.docker.source_mount_readonly:
        mount += ",readonly"
    command = [
        docker_bin,
        "run",
        "--rm",
        "--workdir",
        workdir,
        "--mount",
        mount,
    ]
    env = dict(action.env or {})
    env.setdefault("SOURCE_ROOT", source_target)
    env.setdefault("SANDBOX_ROOT", scratch_root)
    if env:
        for key, value in env.items():
            command.extend(["--env", f"{key}={value}"])
    network_enabled = config.docker.network_enabled if action.network_enabled is None else bool(action.network_enabled)
    if not network_enabled:
        command.extend(["--network", "none"])
    command.append(config.docker.image)
    command.extend(action.command)
    return command


def build_host_command(*, workspace_root: Path, action: RuntimeAction) -> list[str]:
    del workspace_root
    return list(action.command)


def build_runtime_command(*, workspace_root: Path, action: RuntimeAction) -> list[str]:
    config = load_execution_runtime_config()
    if config.backend == "docker":
        return build_docker_command(workspace_root=workspace_root, action=action)
    if config.backend == "host_bounded":
        return build_host_command(workspace_root=workspace_root, action=action)
    raise ValueError(f"Unsupported runtime backend: {config.backend}")


def execute_runtime_actions(*, workspace_root: Path, actions: list[RuntimeAction]) -> RuntimeExecutionResult:
    config = load_execution_runtime_config()
    probe = probe_execution_runtime(workspace_root=workspace_root)
    if not config.enabled:
        return RuntimeExecutionResult(
            backend=config.backend,
            status="disabled",
            detail="Execution runtime is disabled in config.",
            severity="info",
            should_interrupt=False,
            actions=[],
            duration_ms=0,
        )
    if not probe.available:
        return RuntimeExecutionResult(
            backend=config.backend,
            status="blocked",
            detail=probe.detail,
            severity="red",
            should_interrupt=True,
            actions=[],
            duration_ms=0,
        )
    results: list[RuntimeActionResult] = []
    total_duration_ms = 0
    for action in actions:
        command = build_runtime_command(workspace_root=workspace_root, action=action)
        timeout = action.timeout_seconds or (config.host.default_timeout_seconds if config.backend == "host_bounded" else config.docker.default_timeout_seconds)
        started = time.monotonic()
        try:
            if config.backend == "host_bounded":
                working_dir = (workspace_root / action.working_subdir).resolve() if action.working_subdir not in {"", "."} else workspace_root.resolve()
                allowed, detail = _validate_host_workspace(working_dir, config.host)
                if not allowed:
                    raise RuntimeError(detail)
                env = None
                if action.env:
                    env = {**action.env, **dict()}
                    merged = {}
                    merged.update({k: v for k, v in __import__('os').environ.items()})
                    merged.update(action.env)
                    env = merged
                proc = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout,
                    check=False,
                    cwd=str(working_dir),
                    env=env,
                )
            else:
                proc = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout,
                    check=False,
                )
            duration_ms = int((time.monotonic() - started) * 1000)
            total_duration_ms += duration_ms
            action_result = RuntimeActionResult(
                kind=action.kind,
                command=command,
                status="passed" if proc.returncode == 0 else "failed",
                returncode=proc.returncode,
                stdout=(proc.stdout or "").strip(),
                stderr=(proc.stderr or "").strip(),
                duration_ms=duration_ms,
            )
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            total_duration_ms += duration_ms
            action_result = RuntimeActionResult(
                kind=action.kind,
                command=command,
                status="failed",
                returncode=None,
                stdout=((exc.stdout or "") if isinstance(exc.stdout, str) else "").strip(),
                stderr=(f"Timed out after {timeout}s." + (f" {(exc.stderr or '').strip()}" if isinstance(exc.stderr, str) and exc.stderr.strip() else "")).strip(),
                duration_ms=duration_ms,
            )
        except Exception as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            total_duration_ms += duration_ms
            action_result = RuntimeActionResult(
                kind=action.kind,
                command=command,
                status="failed",
                returncode=None,
                stdout="",
                stderr=f"Runtime invocation error: {exc}",
                duration_ms=duration_ms,
            )
        results.append(action_result)
        if action_result.status != "passed":
            evidence = _excerpt(action_result.stderr or action_result.stdout or "No stdout/stderr captured.")
            returncode_label = f"exit {action_result.returncode}" if action_result.returncode is not None else "no exit code"
            return RuntimeExecutionResult(
                backend=config.backend,
                status="failed",
                detail=f"Runtime action failed: {action.kind} ({returncode_label}) / {evidence}",
                severity="yellow",
                should_interrupt=False,
                actions=results,
                duration_ms=total_duration_ms,
            )
    return RuntimeExecutionResult(
        backend=config.backend,
        status="passed",
        detail=f"Executed {len(results)} runtime actions.",
        severity="green",
        should_interrupt=False,
        actions=results,
        duration_ms=total_duration_ms,
    )


def runtime_execution_to_dict(result: RuntimeExecutionResult) -> dict[str, Any]:
    return {
        "backend": result.backend,
        "status": result.status,
        "detail": result.detail,
        "severity": result.severity,
        "should_interrupt": result.should_interrupt,
        "duration_ms": result.duration_ms,
        "actions": [asdict(item) for item in result.actions],
    }

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
ROOT_ENV_PATH = ROOT_DIR / ".env"
AGENT_CONFIG_PATH = ROOT_DIR / "config" / "agents.json"
MODEL_PROVIDER_CONFIG_PATH = ROOT_DIR / "config" / "model_providers.local.json"
if ROOT_ENV_PATH.exists():
    load_dotenv(ROOT_ENV_PATH, override=False)

DEFAULT_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"
DEFAULT_CODER_MODEL = "qwen3-coder-plus"
DEFAULT_STRATEGIC_MODEL = "qwen3.5-plus"
DEFAULT_REASONING_MODEL = "qwen3-max-2026-01-23"
FORMAL_WORKSPACE_ROOT = ROOT_DIR / "runtime" / "formal_workspaces"
FORMAL_MEMORY_ROOT = ROOT_DIR / "runtime" / "formal_role_memory"
DEFAULT_ROLE_WORKSPACES = {
    "orchestrator": FORMAL_WORKSPACE_ROOT / "orchestrator_workspace",
    "builder": FORMAL_WORKSPACE_ROOT / "builder_workspace",
    "reviewer": FORMAL_WORKSPACE_ROOT / "reviewer_workspace",
}
DEFAULT_ROLE_MEMORY_ROOTS = {
    "orchestrator": FORMAL_MEMORY_ROOT / "orchestrator",
    "builder": FORMAL_MEMORY_ROOT / "builder",
    "reviewer": FORMAL_MEMORY_ROOT / "reviewer",
}


@dataclass(frozen=True)
class AgentRuntimeSettings:
    role: str
    mode: str
    backend: str
    model: str
    temperature: float
    provider: str
    base_url: str
    api_key: str
    workspace_root: str
    memory_root: str
    session_id: str


@dataclass(frozen=True)
class ModelProvider:
    name: str
    base_url: str
    api_key: str
    models: tuple[str, ...]


class RuntimeSettings:
    def __init__(self) -> None:
        self.reload()

    def reload(self) -> None:
        if ROOT_ENV_PATH.exists():
            load_dotenv(ROOT_ENV_PATH, override=True)
        self.base_url = os.getenv("LLM_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
        self.api_key = os.getenv("LLM_API_KEY", "").strip()
        self.default_model = os.getenv("DEFAULT_MODEL", DEFAULT_STRATEGIC_MODEL)
        self.strategic_model = os.getenv("STRATEGIC_MODEL", DEFAULT_STRATEGIC_MODEL)
        self.coder_model = os.getenv("CODER_MODEL", DEFAULT_CODER_MODEL)
        self.reasoning_model = os.getenv("REASONING_MODEL", DEFAULT_REASONING_MODEL)
        self.providers = self._load_model_providers()
        self.agent_config = self._load_agent_config()

    def _load_agent_config(self) -> dict[str, dict[str, str | float]]:
        if AGENT_CONFIG_PATH.exists():
            return json.loads(AGENT_CONFIG_PATH.read_text(encoding="utf-8"))
        return {}

    def _load_model_providers(self) -> dict[str, ModelProvider]:
        if not MODEL_PROVIDER_CONFIG_PATH.exists():
            return {}
        payload = json.loads(MODEL_PROVIDER_CONFIG_PATH.read_text(encoding="utf-8"))
        providers: dict[str, ModelProvider] = {}
        for item in payload.get("providers", []):
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            providers[name] = ModelProvider(
                name=name,
                base_url=str(item.get("base_url", "")).rstrip("/"),
                api_key=str(item.get("api_key", "")).strip(),
                models=tuple(str(model) for model in item.get("models", [])),
            )
        return providers

    def agent(self, role: str) -> AgentRuntimeSettings:
        prefix = role.upper()
        stored = self.agent_config.get(role, {})
        mode = str(stored.get("mode", os.getenv(f"{prefix}_MODE", "live"))).strip().lower() or "live"
        backend = str(stored.get("backend", os.getenv(f"{prefix}_BACKEND", "llm"))).strip().lower() or "llm"
        model = str(stored.get("model", os.getenv(f"{prefix}_MODEL", self._fallback_model(role))))
        temperature_raw = str(stored.get("temperature", os.getenv(f"{prefix}_TEMPERATURE", self._fallback_temperature(role))))
        try:
            temperature = float(temperature_raw)
        except ValueError:
            temperature = float(self._fallback_temperature(role))
        provider_name = self._resolve_provider_name(role, model, stored)
        provider = self.providers.get(provider_name) if provider_name else None
        base_url = provider.base_url if provider and provider.base_url else self.base_url
        api_key = provider.api_key if provider and provider.api_key else self.api_key
        workspace_root = self._resolve_workspace_root(role, stored)
        memory_root = self._resolve_memory_root(role, stored)
        session_id = self._resolve_session_id(role, stored)
        return AgentRuntimeSettings(
            role=role,
            mode=mode,
            backend=backend,
            model=model,
            temperature=temperature,
            provider=provider.name if provider else "default_env",
            base_url=base_url,
            api_key=api_key,
            workspace_root=workspace_root,
            memory_root=memory_root,
            session_id=session_id,
        )

    def _resolve_provider_name(self, role: str, model: str, stored: dict[str, str | float]) -> str:
        explicit = str(stored.get("provider", "")).strip()
        if explicit and explicit in self.providers:
            return explicit
        prefix = role.upper()
        env_provider = os.getenv(f"{prefix}_PROVIDER", "").strip()
        if env_provider and env_provider in self.providers:
            return env_provider
        inferred = self.infer_provider_by_model(model)
        if inferred:
            return inferred
        return ""

    def _fallback_model(self, role: str) -> str:
        if role == "builder":
            return self.coder_model
        if role == "reviewer":
            return self.reasoning_model
        return self.strategic_model or self.default_model

    def _fallback_temperature(self, role: str) -> str:
        if role in {"builder", "reviewer"}:
            return "0.1"
        return "0.2"

    def infer_provider_by_model(self, model: str) -> str:
        normalized = model.strip().lower()
        for provider in self.providers.values():
            for candidate in provider.models:
                if candidate.strip().lower() == normalized:
                    return provider.name
        return ""

    def _resolve_workspace_root(self, role: str, stored: dict[str, str | float]) -> str:
        explicit = str(stored.get("workspace_root", os.getenv(f"{role.upper()}_WORKSPACE_ROOT", ""))).strip()
        path = Path(explicit) if explicit else DEFAULT_ROLE_WORKSPACES.get(role, ROOT_DIR)
        return str(path.resolve())

    def _resolve_memory_root(self, role: str, stored: dict[str, str | float]) -> str:
        explicit = str(stored.get("memory_root", os.getenv(f"{role.upper()}_MEMORY_ROOT", ""))).strip()
        path = Path(explicit) if explicit else DEFAULT_ROLE_MEMORY_ROOTS.get(role, FORMAL_MEMORY_ROOT / role)
        path.mkdir(parents=True, exist_ok=True)
        return str(path.resolve())

    def _resolve_session_id(self, role: str, stored: dict[str, str | float]) -> str:
        return str(stored.get("session_id", os.getenv(f"{role.upper()}_SESSION_ID", ""))).strip()


runtime_settings = RuntimeSettings()


def get_agent_settings(role: str) -> AgentRuntimeSettings:
    runtime_settings.reload()
    return runtime_settings.agent(role)


def list_agent_settings(roles: list[str]) -> list[AgentRuntimeSettings]:
    runtime_settings.reload()
    return [runtime_settings.agent(role) for role in roles]


def list_model_providers() -> list[ModelProvider]:
    runtime_settings.reload()
    return list(runtime_settings.providers.values())


def _read_env_lines() -> list[str]:
    if not ROOT_ENV_PATH.exists():
        return []
    return ROOT_ENV_PATH.read_text(encoding="utf-8").splitlines()


def _write_env_lines(lines: list[str]) -> None:
    ROOT_ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def update_agent_settings(
    role: str,
    *,
    mode: str,
    model: str,
    temperature: float,
    provider: str = "",
    backend: str | None = None,
    workspace_root: str | None = None,
    memory_root: str | None = None,
    session_id: str | None = None,
) -> AgentRuntimeSettings:
    payload = runtime_settings._load_agent_config()
    normalized_provider = provider.strip()
    current = dict(payload.get(role, {}) or {})
    next_payload: dict[str, str | float] = {
        "mode": mode.strip().lower(),
        "backend": (backend.strip().lower() if isinstance(backend, str) and backend.strip() else str(current.get("backend", "llm")).strip().lower() or "llm"),
        "model": model.strip(),
        "temperature": float(temperature),
        "provider": normalized_provider or runtime_settings.infer_provider_by_model(model),
    }
    if isinstance(workspace_root, str) and workspace_root.strip():
        next_payload["workspace_root"] = str(Path(workspace_root.strip()).resolve())
    elif current.get("workspace_root"):
        next_payload["workspace_root"] = str(current.get("workspace_root"))
    if isinstance(memory_root, str) and memory_root.strip():
        next_payload["memory_root"] = str(Path(memory_root.strip()).resolve())
    elif current.get("memory_root"):
        next_payload["memory_root"] = str(current.get("memory_root"))
    if isinstance(session_id, str) and session_id.strip():
        next_payload["session_id"] = session_id.strip()
    elif current.get("session_id"):
        next_payload["session_id"] = str(current.get("session_id"))
    payload[role] = next_payload
    AGENT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    AGENT_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    runtime_settings.reload()
    return runtime_settings.agent(role)

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_dev_os.agent_settings import get_agent_settings


VALIDATION_CONTRACT_PATH = Path(__file__).resolve().parents[2] / "config" / "validation_contract.json"
FORMAL_ROLE_LIVE_SET = ("orchestrator", "builder", "reviewer")


def load_validation_contract() -> dict[str, Any]:
    if not VALIDATION_CONTRACT_PATH.exists():
        return {
            "schema_version": "validation_contract.v1",
            "default_execution_source": "system_pipeline",
            "allow_assistant_direct_substitution": False,
            "require_in_system_model": True,
            "require_runtime_provenance": True,
            "allowed_validation_modes": ["mock", "live"],
            "allowed_execution_sources": ["system_pipeline"],
            "allowed_model_sources": ["system_live", "system_mock"],
        }
    return json.loads(VALIDATION_CONTRACT_PATH.read_text(encoding="utf-8"))


def assert_system_pipeline(execution_source: str) -> None:
    contract = load_validation_contract()
    allowed = {str(item) for item in contract.get("allowed_execution_sources", [])}
    if execution_source not in allowed:
        raise RuntimeError(
            f"validation_contract_breach: execution_source={execution_source!r} is not allowed; expected one of {sorted(allowed)}."
        )


def assert_validation_mode(mode: str) -> None:
    contract = load_validation_contract()
    allowed = {str(item) for item in contract.get("allowed_validation_modes", [])}
    if mode not in allowed:
        raise RuntimeError(
            f"validation_contract_breach: validation_mode={mode!r} is not allowed; expected one of {sorted(allowed)}."
        )


def model_source_for_role(role: str) -> dict[str, Any]:
    settings = get_agent_settings(role)
    source = "system_mock" if settings.mode == "mock" or not settings.api_key else "system_live"
    return {
        "role": role,
        "source": source,
        "mode": settings.mode,
        "provider": settings.provider,
        "model": settings.model,
        "base_url": settings.base_url,
        "configured": bool(settings.api_key),
        "backend": settings.backend,
    }


def assert_role_uses_system_model(role: str) -> dict[str, Any]:
    contract = load_validation_contract()
    allowed = {str(item) for item in contract.get("allowed_model_sources", [])}
    snapshot = model_source_for_role(role)
    if snapshot["source"] not in allowed:
        raise RuntimeError(
            f"validation_contract_breach: role={role!r} model_source={snapshot['source']!r} is not allowed; expected one of {sorted(allowed)}."
        )
    return snapshot


def system_model_snapshot(roles: list[str]) -> dict[str, Any]:
    return {
        "roles": [assert_role_uses_system_model(role) for role in roles],
        "contract": load_validation_contract(),
    }


def assert_formal_roles_use_live(roles: tuple[str, ...] = FORMAL_ROLE_LIVE_SET) -> dict[str, Any]:
    snapshots = [assert_role_uses_system_model(role) for role in roles]
    non_live = [item for item in snapshots if item.get("source") != "system_live"]
    if non_live:
        details = ", ".join(f"{item['role']}={item['source']}" for item in non_live)
        raise RuntimeError(
            "validation_contract_breach: formal role execution must use system_live for "
            f"{', '.join(roles)}; got {details}."
        )
    return {"roles": snapshots, "required_live_roles": list(roles)}


def contract_goal_snapshot() -> dict[str, Any]:
    contract = load_validation_contract()
    return {
        "testing_goal": contract.get("testing_goal", ""),
        "assistant_direct_testing_allowed": bool(contract.get("assistant_direct_testing_allowed", False)),
        "assistant_direct_testing_exception": str(contract.get("assistant_direct_testing_exception", "")),
        "system_mock_allowed": bool(contract.get("system_mock_allowed", True)),
        "system_live_allowed": bool(contract.get("system_live_allowed", True)),
    }

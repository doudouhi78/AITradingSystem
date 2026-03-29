from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ai_dev_os.agents import approval_node
from ai_dev_os.agents import auto_approve_node
from ai_dev_os.agents import builder_agent
from ai_dev_os.agents import create_task_card
from ai_dev_os.agents import recorder_agent
from ai_dev_os.agents import reviewer_agent
from ai_dev_os.agent_settings import get_agent_settings
from ai_dev_os.state import KernelState


@dataclass(frozen=True)
class AgentNodeSpec:
    role: str
    node_id: str
    handler: Callable[[KernelState], KernelState]
    description: str


AGENT_REGISTRY: dict[str, AgentNodeSpec] = {
    "orchestrator": AgentNodeSpec(
        role="orchestrator",
        node_id="orchestrator_task_card",
        handler=create_task_card,
        description="Create a task card and initialize project runtime artifacts.",
    ),
    "builder": AgentNodeSpec(
        role="builder",
        node_id="builder_plan",
        handler=builder_agent,
        description="Prepare or revise the build plan.",
    ),
    "reviewer": AgentNodeSpec(
        role="reviewer",
        node_id="reviewer_gate",
        handler=reviewer_agent,
        description="Review outputs, decide approval or request rework.",
    ),
    "approval": AgentNodeSpec(
        role="approval",
        node_id="approval_gate",
        handler=approval_node,
        description="Represent the human approval gate for risky changes.",
    ),
    "auto_approve": AgentNodeSpec(
        role="auto_approve",
        node_id="auto_approve",
        handler=auto_approve_node,
        description="Automatically approve low-risk tasks.",
    ),
    "recorder": AgentNodeSpec(
        role="recorder",
        node_id="record_memory",
        handler=recorder_agent,
        description="Write runtime results into project memory and control tower.",
    ),
}


PRIMARY_AGENT_ORDER = ("orchestrator", "builder", "reviewer", "recorder")
CONFIGURABLE_AGENT_ROLES = ("orchestrator", "builder", "reviewer")


def get_agent_spec(role: str) -> AgentNodeSpec:
    return AGENT_REGISTRY[role]


def list_registered_agents() -> list[AgentNodeSpec]:
    return [AGENT_REGISTRY[role] for role in AGENT_REGISTRY]


def list_configurable_agents() -> list[dict[str, str | float]]:
    items: list[dict[str, str | float]] = []
    for role in CONFIGURABLE_AGENT_ROLES:
        settings = get_agent_settings(role)
        items.append(
            {
                "role": role,
                "node_id": AGENT_REGISTRY[role].node_id,
                "provider": settings.provider,
                "mode": settings.mode,
                "backend": settings.backend,
                "model": settings.model,
                "temperature": settings.temperature,
                "workspace_root": settings.workspace_root,
                "memory_root": settings.memory_root,
                "session_id": settings.session_id,
            }
        )
    return items





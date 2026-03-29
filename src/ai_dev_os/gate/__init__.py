from typing import Optional, TypedDict


class GateDecision(TypedDict):
    allowed: bool
    blocked_by: Optional[str]
    reason: Optional[str]
    gate_details: dict


from .gate_scheduler import GateScheduler

__all__ = ["GateDecision", "GateScheduler"]

from __future__ import annotations

from ai_dev_os.gate import GateDecision
from ai_dev_os.gate.gate_config import DrawdownGateConfig


class DrawdownGate:
    def __init__(self, config: DrawdownGateConfig | None = None) -> None:
        self.config = config or DrawdownGateConfig()

    def evaluate(self, equity_series: list[float]) -> GateDecision:
        if not equity_series:
            return {
                "allowed": True,
                "blocked_by": None,
                "reason": None,
                "gate_details": {"drawdown": 0.0, "threshold": self.config.max_drawdown_threshold},
            }
        peak = max(equity_series)
        current = equity_series[-1]
        drawdown = 0.0 if peak <= 0 else (current - peak) / peak
        allowed = drawdown >= -self.config.max_drawdown_threshold
        reason = None if allowed else f"drawdown={drawdown:.2%}"
        return {
            "allowed": allowed,
            "blocked_by": None if allowed else "DrawdownGate",
            "reason": reason,
            "gate_details": {"drawdown": drawdown, "threshold": self.config.max_drawdown_threshold},
        }

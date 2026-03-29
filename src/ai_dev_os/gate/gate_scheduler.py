from __future__ import annotations

import pandas as pd

from ai_dev_os.gate import GateDecision
from ai_dev_os.gate.breadth_gate import BreadthGate
from ai_dev_os.gate.drawdown_gate import DrawdownGate
from ai_dev_os.gate.gate_config import BreadthGateConfig, DrawdownGateConfig, GateSchedulerConfig, TrendGateConfig, VolGateConfig
from ai_dev_os.gate.trend_gate import TrendGate
from ai_dev_os.gate.vol_gate import VolGate


class GateScheduler:
    def __init__(
        self,
        config: GateSchedulerConfig | None = None,
        drawdown_config: DrawdownGateConfig | None = None,
        trend_config: TrendGateConfig | None = None,
        breadth_config: BreadthGateConfig | None = None,
        vol_config: VolGateConfig | None = None,
    ) -> None:
        self.config = config or GateSchedulerConfig()
        self.drawdown_gate = DrawdownGate(drawdown_config)
        self.trend_gate = TrendGate(trend_config)
        self.breadth_gate = BreadthGate(breadth_config)
        self.vol_gate = VolGate(vol_config)

    def evaluate(
        self,
        date: str,
        equity_series: list[float],
        etf_df: pd.DataFrame,
    ) -> GateDecision:
        close = etf_df["close"].astype(float)
        details = {
            "DrawdownGate": self.drawdown_gate.evaluate(equity_series),
            "TrendGate": self.trend_gate.evaluate(close),
            "BreadthGate": self.breadth_gate.evaluate(date),
            "VolGate": self.vol_gate.evaluate(close),
        }
        failing = [(name, payload) for name, payload in details.items() if not payload["allowed"]]
        if self.config.merge_strategy == "strict" and failing:
            blocked_by, first = failing[0]
            return {
                "allowed": False,
                "blocked_by": blocked_by,
                "reason": first["reason"],
                "gate_details": details,
            }
        if self.config.merge_strategy != "strict":
            raise NotImplementedError(
                f"merge_strategy='{self.config.merge_strategy}' is not implemented. Only 'strict' is supported."
            )
        return {
            "allowed": True,
            "blocked_by": None,
            "reason": None,
            "gate_details": details,
        }

from __future__ import annotations

import pandas as pd

from ai_dev_os.gate import GateDecision
from ai_dev_os.gate.gate_config import TrendGateConfig


class TrendGate:
    def __init__(self, config: TrendGateConfig | None = None) -> None:
        self.config = config or TrendGateConfig()

    def evaluate(self, price_series: pd.Series) -> GateDecision:
        close = price_series.astype(float).dropna()
        if len(close) < self.config.ma_window:
            return {
                "allowed": True,
                "blocked_by": None,
                "reason": None,
                "gate_details": {"close": None, "ma": None, "ma_window": self.config.ma_window},
            }
        ma = close.rolling(self.config.ma_window).mean().iloc[-1]
        latest = close.iloc[-1]
        allowed = bool(latest >= ma)
        reason = None if allowed else f"close={latest:.4f} < ma{self.config.ma_window}={ma:.4f}"
        return {
            "allowed": allowed,
            "blocked_by": None if allowed else "TrendGate",
            "reason": reason,
            "gate_details": {"close": float(latest), "ma": float(ma), "ma_window": self.config.ma_window},
        }

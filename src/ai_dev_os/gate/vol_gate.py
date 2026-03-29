from __future__ import annotations

import pandas as pd

from ai_dev_os.gate import GateDecision
from ai_dev_os.gate.gate_config import VolGateConfig


class VolGate:
    def __init__(self, config: VolGateConfig | None = None) -> None:
        self.config = config or VolGateConfig()

    def evaluate(self, price_series: pd.Series) -> GateDecision:
        close = price_series.astype(float).dropna()
        returns = close.pct_change()
        rolling_vol = returns.rolling(self.config.vol_window).std().dropna()
        if len(rolling_vol) < self.config.vol_lookback:
            return {
                "allowed": True,
                "blocked_by": None,
                "reason": None,
                "gate_details": {"current_vol": None, "threshold": None},
            }
        current_vol = float(rolling_vol.iloc[-1])
        threshold = float(rolling_vol.iloc[-self.config.vol_lookback :].quantile(self.config.vol_percentile_threshold))
        allowed = current_vol <= threshold
        reason = None if allowed else f"vol={current_vol:.4f} > p{int(self.config.vol_percentile_threshold*100)}={threshold:.4f}"
        return {
            "allowed": allowed,
            "blocked_by": None if allowed else "VolGate",
            "reason": reason,
            "gate_details": {"current_vol": current_vol, "threshold": threshold},
        }

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class StrategyConfig:
    strategy_id: str
    strategy_name: str
    is_active: bool = False
    max_capital_pct: float = 0.30
    priority: int = 1
    rebalance_freq: str = "daily"
    status: str = "research"
    signal_type: str = "discrete"
    factor_id: str | None = None
    days_in_forward_sim: int = 0
    forward_sharpe: float | None = None

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

from __future__ import annotations

import math

from ai_dev_os.risk.risk_config import PositionSizingConfig


def compute_quantity(
    account_equity: float,
    entry_price: float,
    stop_price: float,
    config: PositionSizingConfig = PositionSizingConfig(),
) -> tuple[int, float]:
    risk_per_share = entry_price - stop_price
    if risk_per_share <= 0:
        return 0, 0.0
    risk_amount = account_equity * config.risk_per_trade
    qty = math.floor(risk_amount / risk_per_share)
    if qty < 1:
        return 0, 0.0

    raw_position_fraction = qty * entry_price / account_equity
    if raw_position_fraction < config.min_position_fraction:
        return 0, 0.0
    capped_position_fraction = min(raw_position_fraction, config.max_position_fraction)
    capped_qty = math.floor((account_equity * capped_position_fraction) / entry_price)
    if capped_qty < 1:
        return 0, 0.0
    position_fraction = capped_qty * entry_price / account_equity
    return capped_qty, position_fraction


def _self_test() -> bool:
    qty, position_fraction = compute_quantity(100000, 10.0, 9.0)
    return qty == 1000 and abs(position_fraction - 0.10) < 0.001

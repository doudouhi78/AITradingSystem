from __future__ import annotations

import pandas as pd

from scripts.signal_daily import is_month_end



def test_is_month_end_true_for_last_trading_day() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-03-27',
        '2026-03-30',
        '2026-03-31',
        '2026-04-01',
    ])
    assert is_month_end('2026-03-31', trading_dates)



def test_is_month_end_false_before_last_trading_day() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-03-27',
        '2026-03-30',
        '2026-03-31',
        '2026-04-01',
    ])
    assert not is_month_end('2026-03-30', trading_dates)



def test_is_month_end_handles_short_month() -> None:
    trading_dates = pd.DatetimeIndex([
        '2026-02-25',
        '2026-02-26',
        '2026-02-27',
        '2026-03-02',
    ])
    assert is_month_end('2026-02-27', trading_dates)

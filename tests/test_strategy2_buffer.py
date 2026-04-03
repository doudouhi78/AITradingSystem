from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.strategy2.execution.buffer_selector import BufferConfig, BufferSelector


class _Gate:
    def __init__(self, blocked: set[pd.Timestamp] | None = None) -> None:
        self.blocked = {pd.Timestamp(item) for item in (blocked or set())}

    def can_enter(self, trade_date) -> bool:
        return pd.Timestamp(trade_date) not in self.blocked


def test_buffer_selector_retains_names_until_exit_threshold() -> None:
    dates = pd.to_datetime(['2024-01-02', '2024-02-01', '2024-03-01'])
    score = pd.DataFrame(
        {
            '000001.SZ': [0.9, 0.3, 0.2],
            '000002.SZ': [0.8, 0.9, 0.95],
            '000003.SZ': [0.7, 0.8, 0.85],
            '000004.SZ': [0.1, 0.7, 0.75],
        },
        index=dates,
    )
    selector = BufferSelector(score_frame=score, judgment_layer=_Gate(), higher_score_better=True)
    stop_flags = pd.DataFrame(False, index=dates, columns=score.columns)
    result = selector.simulate(BufferConfig(entry_top_n=2, exit_top_m=3), stop_flags=stop_flags)
    holdings = result.rebalances['holding_count'].tolist()
    assert holdings == [2, 2, 2]
    assert result.rebalances['turnover'].iloc[1] < 1.0


def test_buffer_selector_market_filter_forces_cash() -> None:
    dates = pd.to_datetime(['2024-01-02', '2024-02-01'])
    score = pd.DataFrame({'000001.SZ': [1.0, 1.0], '000002.SZ': [0.9, 0.8]}, index=dates)
    selector = BufferSelector(score_frame=score, judgment_layer=_Gate({dates[1]}), higher_score_better=True)
    stop_flags = pd.DataFrame(False, index=dates, columns=score.columns)
    result = selector.simulate(BufferConfig(entry_top_n=1, exit_top_m=2), stop_flags=stop_flags)
    assert result.rebalances['holding_count'].tolist() == [1, 0]
    assert result.market_filter_rate == 0.5


def test_buffer_selector_stop_flags_force_exit() -> None:
    dates = pd.to_datetime(['2024-01-02', '2024-02-01'])
    score = pd.DataFrame({'000001.SZ': [1.0, 0.9], '000002.SZ': [0.8, 0.85]}, index=dates)
    selector = BufferSelector(score_frame=score, judgment_layer=_Gate(), higher_score_better=True)
    stop_flags = pd.DataFrame(False, index=dates, columns=score.columns)
    stop_flags.loc[dates[1], '000001.SZ'] = True
    result = selector.simulate(BufferConfig(entry_top_n=1, exit_top_m=2), stop_flags=stop_flags)
    assert result.rebalances['stop_exit_count'].iloc[1] == 1
    assert result.stop_trigger_rate > 0

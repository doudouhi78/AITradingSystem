from __future__ import annotations

import pandas as pd

from .base_strategy import BaseStrategy


class MACrossStrategy(BaseStrategy):
    strategy_id = "strat_ma_cross_v1"
    strategy_name = "双均线趋势跟随"
    strategy_type = "trend"

    def __init__(self, short_window: int = 10, long_window: int = 30) -> None:
        self.short_window = short_window
        self.long_window = long_window

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        short_ma = data['close'].rolling(self.short_window).mean()
        long_ma = data['close'].rolling(self.long_window).mean()
        regime = (short_ma > long_ma).astype(int).shift(1).fillna(0)
        signal = regime.diff().fillna(regime).clip(-1, 1).astype(int)
        signal.name = 'signal'
        return signal

    def entry_summary(self) -> str:
        return f"short_ma({self.short_window}) 上穿 long_ma({self.long_window})，次日开盘入场"

    def exit_summary(self) -> str:
        return f"short_ma({self.short_window}) 下穿 long_ma({self.long_window})，次日开盘出场"

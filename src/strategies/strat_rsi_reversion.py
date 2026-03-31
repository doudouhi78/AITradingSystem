from __future__ import annotations

import pandas as pd
import pandas_ta as ta

from .base_strategy import BaseStrategy


class RSIReversionStrategy(BaseStrategy):
    strategy_id = "strat_rsi_reversion_v1"
    strategy_name = "RSI 均值回归"
    strategy_type = "reversion"

    def __init__(self, rsi_period: int = 14, oversold: float = 30, overbought: float = 70) -> None:
        self.rsi_period = rsi_period
        self.oversold = oversold
        self.overbought = overbought

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        rsi = ta.rsi(data['close'], length=self.rsi_period)
        rsi_exec = rsi.shift(1)
        prev_rsi_exec = rsi_exec.shift(1)
        entry = (prev_rsi_exec <= self.oversold) & (rsi_exec > self.oversold)
        exit_ = rsi_exec >= self.overbought
        signal = pd.Series(0, index=data.index, dtype=int)
        signal.loc[entry.fillna(False)] = 1
        signal.loc[exit_.fillna(False)] = -1
        signal.name = 'signal'
        return signal

    def entry_summary(self) -> str:
        return f"RSI({self.rsi_period}) 从超卖阈值 {self.oversold} 上穿，次日开盘入场"

    def exit_summary(self) -> str:
        return f"RSI({self.rsi_period}) 高于 {self.overbought}，次日开盘出场"

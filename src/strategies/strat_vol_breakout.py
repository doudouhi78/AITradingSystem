from __future__ import annotations

import pandas as pd
import pandas_ta as ta

from .base_strategy import BaseStrategy


class VolBreakoutStrategy(BaseStrategy):
    strategy_id = "strat_vol_breakout_v1"
    strategy_name = "布林带波动突破"
    strategy_type = "reversion"

    def __init__(self, bb_period: int = 20, bb_std: float = 2.0) -> None:
        self.bb_period = bb_period
        self.bb_std = bb_std

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        bbands = ta.bbands(data['close'], length=self.bb_period, std=self.bb_std)
        lower_key = f'BBL_{self.bb_period}_{self.bb_std}'
        upper_key = f'BBU_{self.bb_period}_{self.bb_std}'
        if lower_key not in bbands.columns:
            lower_key = next(column for column in bbands.columns if column.startswith('BBL_'))
        if upper_key not in bbands.columns:
            upper_key = next(column for column in bbands.columns if column.startswith('BBU_'))
        lower_band = bbands[lower_key]
        upper_band = bbands[upper_key]
        below = data['close'].shift(2) < lower_band.shift(2)
        recover = data['close'].shift(1) >= lower_band.shift(1)
        entry = below & recover
        exit_ = data['close'].shift(1) >= upper_band.shift(1)
        signal = pd.Series(0, index=data.index, dtype=int)
        signal.loc[entry.fillna(False)] = 1
        signal.loc[exit_.fillna(False)] = -1
        signal.name = 'signal'
        return signal

    def entry_summary(self) -> str:
        return f"价格前一日重新站上布林下轨（{self.bb_period}, {self.bb_std}），当日开盘入场"

    def exit_summary(self) -> str:
        return f"价格前一日触及布林上轨（{self.bb_period}, {self.bb_std}），当日开盘出场"

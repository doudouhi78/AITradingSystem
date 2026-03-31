from __future__ import annotations

import pandas as pd

from .base_strategy import BaseStrategy


class FactorMomentumStrategy(BaseStrategy):
    strategy_id = "strat_factor_momentum_v1"
    strategy_name = "截面动量因子选股"
    strategy_type = "factor"

    def __init__(self, top_n: int = 20, rebalance_freq: str = 'ME') -> None:
        self.top_n = top_n
        self.rebalance_freq = rebalance_freq

    def generate_signals(self, factor_scores: pd.Series | pd.DataFrame) -> dict[pd.Timestamp, list[str]]:
        if isinstance(factor_scores, pd.DataFrame):
            if {'date', 'instrument', 'composite_score'}.issubset(factor_scores.columns):
                series = factor_scores.set_index(['date', 'instrument'])['composite_score']
            elif 'composite_score' in factor_scores.columns and isinstance(factor_scores.index, pd.MultiIndex):
                series = factor_scores['composite_score']
            else:
                raise ValueError('factor_scores dataframe must contain composite_score')
        else:
            series = factor_scores
        if not isinstance(series.index, pd.MultiIndex) or series.index.nlevels != 2:
            raise ValueError('factor_scores must use MultiIndex(date, instrument)')
        normalized = series.dropna().copy()
        normalized.index = normalized.index.set_names(['date', 'instrument'])
        score_frame = normalized.unstack('instrument').sort_index()
        rebalance_dates = score_frame.resample(self.rebalance_freq).last().dropna(how='all').index
        signals: dict[pd.Timestamp, list[str]] = {}
        for rebalance_date in rebalance_dates:
            latest = score_frame.loc[:rebalance_date].iloc[-1].dropna().sort_values(ascending=False)
            signals[pd.Timestamp(rebalance_date)] = latest.head(self.top_n).index.tolist()
        return signals

    def entry_summary(self) -> str:
        return f"每月末按 composite_score 选前 {self.top_n} 名，下一交易日等权买入"

    def exit_summary(self) -> str:
        return "下一次月末再平衡时卖出不在 top N 的持仓"

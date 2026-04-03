from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .position_tools import calc_chandelier_exit
from ..judgment_layer import MarketJudgmentLayer


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_MARKET_DIR = _REPO_ROOT / 'runtime' / 'market_data' / 'cn_stock'


def _normalize_ts_code(value: object) -> str:
    text = str(value or '').strip().upper()
    if not text:
        return ''
    if '.' in text:
        symbol, market = text.split('.', 1)
        return f'{symbol.zfill(6)}.{market}'
    market = 'SH' if text.startswith(('6', '9')) else 'SZ'
    return f'{text.zfill(6)}.{market}'


@dataclass(frozen=True)
class BufferConfig:
    entry_top_n: int
    exit_top_m: int
    atr_window: int = 14
    chandelier_k: float = 3.0

    def __post_init__(self) -> None:
        if self.entry_top_n <= 0 or self.exit_top_m <= 0:
            raise ValueError('entry_top_n and exit_top_m must be positive')
        if self.exit_top_m < self.entry_top_n:
            raise ValueError('exit_top_m must be >= entry_top_n')


@dataclass
class BufferSimulationResult:
    rebalances: pd.DataFrame
    average_monthly_turnover: float
    stop_trigger_rate: float
    market_filter_rate: float


class BufferSelector:
    def __init__(
        self,
        score_frame: pd.DataFrame,
        judgment_layer: MarketJudgmentLayer | None = None,
        market_data_dir: str | Path | None = None,
        higher_score_better: bool = True,
    ) -> None:
        self.score_frame = score_frame.copy()
        if not isinstance(self.score_frame.index, pd.DatetimeIndex):
            self.score_frame.index = pd.to_datetime(self.score_frame.index, errors='coerce')
        self.score_frame = self.score_frame.sort_index()
        self.score_frame.columns = [_normalize_ts_code(col) for col in self.score_frame.columns]
        self.judgment_layer = judgment_layer
        self.market_data_dir = Path(market_data_dir) if market_data_dir is not None else _DEFAULT_MARKET_DIR
        self.higher_score_better = higher_score_better

    def monthly_rebalance_dates(self) -> list[pd.Timestamp]:
        grouped = self.score_frame.index.to_series().groupby(self.score_frame.index.to_period('M')).min()
        return [pd.Timestamp(value) for value in grouped.tolist()]

    def _ranked_scores(self, trade_date: pd.Timestamp) -> pd.Series:
        series = self.score_frame.loc[trade_date].dropna()
        return series.sort_values(ascending=not self.higher_score_better)

    def build_stop_flags(
        self,
        symbols: Iterable[str],
        rebalance_dates: list[pd.Timestamp],
        atr_window: int = 14,
        chandelier_k: float = 3.0,
    ) -> pd.DataFrame:
        index = pd.DatetimeIndex(rebalance_dates, name='trade_date')
        flags = pd.DataFrame(False, index=index, columns=[_normalize_ts_code(symbol) for symbol in symbols], dtype=bool)
        for ts_code in flags.columns:
            symbol = ts_code.split('.', 1)[0]
            path = self.market_data_dir / f'{symbol}.parquet'
            if not path.exists():
                continue
            frame = pd.read_parquet(path, columns=['trade_date', 'high', 'low', 'close']).copy()
            frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
            frame = frame.dropna(subset=['trade_date']).sort_values('trade_date')
            stop_line = calc_chandelier_exit(frame[['high', 'low', 'close']], atr_window=atr_window, k=chandelier_k)
            status = pd.DataFrame({'trade_date': frame['trade_date'], 'close': pd.to_numeric(frame['close'], errors='coerce'), 'stop': stop_line})
            status['stop_triggered'] = (status['close'] < status['stop']).fillna(False)
            mapped = status.set_index('trade_date')['stop_triggered'].astype('boolean').reindex(index, method='ffill')
            flags[ts_code] = mapped.fillna(False).astype(bool)
        return flags

    def simulate(self, config: BufferConfig, stop_flags: pd.DataFrame | None = None) -> BufferSimulationResult:
        rebalance_dates = self.monthly_rebalance_dates()
        if stop_flags is None:
            candidate_symbols: set[str] = set()
            for trade_date in rebalance_dates:
                ranked = self._ranked_scores(trade_date).head(config.exit_top_m)
                candidate_symbols.update(ranked.index.tolist())
            stop_flags = self.build_stop_flags(candidate_symbols, rebalance_dates, atr_window=config.atr_window, chandelier_k=config.chandelier_k)
        stop_flags = stop_flags.reindex(index=pd.DatetimeIndex(rebalance_dates), columns=self.score_frame.columns, fill_value=False)

        current: set[str] = set()
        rows: list[dict[str, object]] = []
        total_hold_observations = 0
        total_stop_exits = 0
        market_blocked_count = 0

        for trade_date in rebalance_dates:
            ranked = self._ranked_scores(trade_date)
            ranks = {code: idx + 1 for idx, code in enumerate(ranked.index.tolist())}
            market_allowed = True if self.judgment_layer is None else self.judgment_layer.can_enter(trade_date)
            if not market_allowed:
                market_blocked_count += 1

            stop_triggered = {
                code for code in current
                if bool(stop_flags.reindex(index=[trade_date], columns=[code], fill_value=False).iloc[0, 0])
            }
            total_hold_observations += len(current)
            total_stop_exits += len(stop_triggered)

            if not market_allowed:
                target: list[str] = []
            else:
                retained = [
                    code for code in current
                    if code not in stop_triggered and ranks.get(code, 10 ** 9) <= config.exit_top_m
                ]
                target = list(retained)
                for code in ranked.index:
                    if len(target) >= config.entry_top_n:
                        break
                    if ranks[code] <= config.entry_top_n and code not in target:
                        target.append(code)

            target_set = set(target)
            if current:
                turnover = len(target_set - current) / max(len(current), 1)
            else:
                turnover = len(target_set) / max(len(target_set), 1) if target_set else 0.0

            rows.append(
                {
                    'rebalance_date': pd.Timestamp(trade_date),
                    'holding_count': len(target_set),
                    'turnover': float(turnover),
                    'market_allowed': bool(market_allowed),
                    'stop_exit_count': len(stop_triggered),
                }
            )
            current = target_set

        rebalances = pd.DataFrame(rows)
        avg_turnover = float(rebalances['turnover'].mean()) if not rebalances.empty else 0.0
        stop_rate = (total_stop_exits / total_hold_observations) if total_hold_observations else 0.0
        market_rate = (market_blocked_count / len(rebalance_dates)) if rebalance_dates else 0.0
        return BufferSimulationResult(
            rebalances=rebalances,
            average_monthly_turnover=avg_turnover,
            stop_trigger_rate=float(stop_rate),
            market_filter_rate=float(market_rate),
        )

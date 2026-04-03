from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_INDEX_PATH = _REPO_ROOT / 'runtime' / 'index_data' / 'index_daily.parquet'


@dataclass
class JudgmentCoverage:
    forbidden_ratio: float
    drawdown_coverage_ratio: float
    max_drawdown: float
    peak_date: str
    trough_date: str


class MarketJudgmentLayer:
    def __init__(self, signal_frame: pd.DataFrame, ma_window: int = 60) -> None:
        self.ma_window = ma_window
        frame = signal_frame.copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
        frame['close'] = pd.to_numeric(frame['close'], errors='coerce')
        frame = frame.dropna(subset=['trade_date', 'close']).sort_values('trade_date').reset_index(drop=True)
        frame['ma'] = frame['close'].rolling(ma_window, min_periods=ma_window).mean()
        frame['allow_entry'] = (frame['close'] >= frame['ma']).fillna(False)
        frame['cummax'] = frame['close'].cummax()
        frame['drawdown'] = frame['close'] / frame['cummax'] - 1.0
        self.frame = frame

    @classmethod
    def from_parquet(
        cls,
        path: str | Path | None = None,
        index_code: str = '000300.SH',
        start: str | None = None,
        end: str | None = None,
        ma_window: int = 60,
    ) -> 'MarketJudgmentLayer':
        target = Path(path) if path is not None else _DEFAULT_INDEX_PATH
        frame = pd.read_parquet(target).copy()
        if 'ts_code' in frame.columns:
            frame = frame.loc[frame['ts_code'] == index_code].copy()
        if start is not None:
            frame = frame.loc[pd.to_datetime(frame['trade_date'], errors='coerce') >= pd.Timestamp(start)]
        if end is not None:
            frame = frame.loc[pd.to_datetime(frame['trade_date'], errors='coerce') <= pd.Timestamp(end)]
        return cls(frame[['trade_date', 'close']], ma_window=ma_window)

    def can_enter(self, trade_date: str | pd.Timestamp) -> bool:
        query_date = pd.Timestamp(trade_date).normalize()
        eligible = self.frame.loc[self.frame['trade_date'] <= query_date]
        if eligible.empty:
            return False
        return bool(eligible.iloc[-1]['allow_entry'])

    def signal_series(self) -> pd.Series:
        return self.frame.set_index('trade_date')['allow_entry'].astype(bool)

    def evaluate_drawdown_coverage(self) -> JudgmentCoverage:
        if self.frame.empty:
            return JudgmentCoverage(0.0, 0.0, 0.0, '', '')
        trough_pos = int(self.frame['drawdown'].idxmin())
        peak_value = float(self.frame.loc[trough_pos, 'cummax'])
        peak_pos = int(self.frame.loc[:trough_pos].index[self.frame.loc[:trough_pos, 'close'] == peak_value][0])
        interval = self.frame.loc[peak_pos:trough_pos].copy()
        forbidden_ratio = float((~self.frame['allow_entry']).mean())
        coverage_ratio = float((~interval['allow_entry']).mean()) if not interval.empty else 0.0
        return JudgmentCoverage(
            forbidden_ratio=forbidden_ratio,
            drawdown_coverage_ratio=coverage_ratio,
            max_drawdown=float(self.frame.loc[trough_pos, 'drawdown']),
            peak_date=self.frame.loc[peak_pos, 'trade_date'].strftime('%Y-%m-%d'),
            trough_date=self.frame.loc[trough_pos, 'trade_date'].strftime('%Y-%m-%d'),
        )

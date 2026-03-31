from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_dev_os.gate import GateDecision
from ai_dev_os.gate.gate_config import BreadthGateConfig

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PRIMARY = Path(r'D:\AITradingSystem')
ROOT = _PRIMARY if (_PRIMARY / 'runtime' / 'market_data').exists() else _REPO_ROOT
STOCK_DIR = ROOT / "runtime" / "market_data" / "cn_stock"
_BREADTH_CACHE: pd.DataFrame | None = None


def _build_breadth_cache() -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for path in STOCK_DIR.glob("*.parquet"):
        df = pd.read_parquet(path, columns=["trade_date", "close"]).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)
        df["prev_close"] = df["close"].shift(1)
        df = df.dropna(subset=["prev_close"])
        df["adv"] = (df["close"].astype(float) > df["prev_close"].astype(float)).astype(int)
        df["count"] = 1
        records.append(df[["trade_date", "adv", "count"]])
    if not records:
        return pd.DataFrame(columns=["adv_ratio"])
    merged = pd.concat(records, ignore_index=True)
    grouped = merged.groupby("trade_date", as_index=True)[["adv", "count"]].sum()
    grouped["adv_ratio"] = grouped["adv"] / grouped["count"]
    return grouped[["adv_ratio"]].sort_index()


class BreadthGate:
    def __init__(self, config: BreadthGateConfig | None = None) -> None:
        self.config = config or BreadthGateConfig()

    def _get_cache(self) -> pd.DataFrame:
        global _BREADTH_CACHE
        if _BREADTH_CACHE is None:
            _BREADTH_CACHE = _build_breadth_cache()
        return _BREADTH_CACHE

    def evaluate(self, date: str) -> GateDecision:
        breadth = self._get_cache()
        ts = pd.Timestamp(date)
        if ts not in breadth.index:
            return {
                "allowed": True,
                "blocked_by": None,
                "reason": None,
                "gate_details": {"adv_ratio": None, "threshold": self.config.min_adv_ratio},
            }
        adv_ratio = float(breadth.loc[ts, "adv_ratio"])
        allowed = adv_ratio >= self.config.min_adv_ratio
        reason = None if allowed else f"adv_ratio={adv_ratio:.2%} < {self.config.min_adv_ratio:.2%}"
        return {
            "allowed": allowed,
            "blocked_by": None if allowed else "BreadthGate",
            "reason": reason,
            "gate_details": {"adv_ratio": adv_ratio, "threshold": self.config.min_adv_ratio},
        }

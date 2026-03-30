from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from alpha_research.data_loader import load_factor_input, select_top_n_by_liquidity
from alpha_research.factors.volume_liquidity import factor_turnover_20d, factor_volume_price_divergence
from alpha_research.signal_composer import compose_signal, generate_top_n_signal
from ai_dev_os.gate.gate_scheduler import GateScheduler

ROOT = Path(r"D:\AITradingSystem")
PHASE3_ROOT = ROOT / "runtime" / "alpha_research" / "phase3"
ETF_DATA_DIR = ROOT / "runtime" / "market_data" / "cn_etf"
DEFAULT_START = "2020-01-01"
DEFAULT_TOP_N = 50
DEFAULT_TOP_PCT = 0.10
BEST_WEIGHTS_PATH = PHASE3_ROOT / "best_weights.json"


def load_seed_factor_series(start: str = DEFAULT_START, end: str | None = None, top_n: int = DEFAULT_TOP_N) -> tuple[list[str], dict[str, pd.Series]]:
    effective_end = end or datetime.now().strftime("%Y-%m-%d")
    instruments = select_top_n_by_liquidity("etf", start, effective_end, top_n=top_n)
    factor_input = load_factor_input(instruments, start, effective_end, asset_type="etf")
    factor_map = {
        "factor_turnover_20d": factor_turnover_20d(factor_input),
        "factor_volume_price_divergence": factor_volume_price_divergence(factor_input),
    }
    return instruments, factor_map


def load_best_weights() -> dict[str, float]:
    if BEST_WEIGHTS_PATH.exists():
        payload = json.loads(BEST_WEIGHTS_PATH.read_text(encoding="utf-8"))
        weights = payload.get("best_weights", {})
        if weights:
            return {str(key): float(value) for key, value in weights.items()}
    return {
        "factor_turnover_20d": 0.5,
        "factor_volume_price_divergence": 0.5,
    }


def _load_gate_market_frame(symbol: str = "510300", start: str = DEFAULT_START) -> pd.DataFrame:
    path = ETF_DATA_DIR / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path).copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    mask = frame["trade_date"] >= pd.Timestamp(start)
    return frame.loc[mask].sort_values("trade_date").reset_index(drop=True)


def build_equity_proxy(signal: pd.Series, gate_frame: pd.DataFrame) -> list[float]:
    close = gate_frame["close"].astype(float)
    returns = close.pct_change(fill_method=None).fillna(0.0)
    equity = (1.0 + returns).cumprod()
    return equity.tolist()


def main() -> None:
    PHASE3_ROOT.mkdir(parents=True, exist_ok=True)
    instruments, factor_map = load_seed_factor_series()
    weights = load_best_weights()
    composite = compose_signal(factor_map, weights)
    signal = generate_top_n_signal(composite, top_pct=DEFAULT_TOP_PCT)
    latest_date = signal.index.get_level_values("date").max()
    latest_signal = signal.xs(latest_date, level="date")
    candidates = latest_signal[latest_signal == 1].index.tolist()

    gate_frame = _load_gate_market_frame()
    gate_frame = gate_frame.loc[gate_frame["trade_date"] <= latest_date].copy()
    equity_series = build_equity_proxy(signal, gate_frame)
    gate_scheduler = GateScheduler()
    gate_decision = gate_scheduler.evaluate(str(pd.Timestamp(latest_date).date()), equity_series, gate_frame)

    payload = {
        "gate_interface": "GateScheduler.evaluate(date, equity_series, etf_df)",
        "latest_signal_date": str(pd.Timestamp(latest_date).date()),
        "weights": weights,
        "candidate_count": len(candidates) if gate_decision["allowed"] else 0,
        "candidate_symbols": candidates if gate_decision["allowed"] else [],
        "gate_allowed": bool(gate_decision["allowed"]),
        "gate_blocked_by": gate_decision["blocked_by"],
        "gate_reason": gate_decision["reason"],
        "gate_details": gate_decision["gate_details"],
        "universe_size": len(instruments),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_dev_os.risk.atr_stop import _self_test as atr_self_test
from ai_dev_os.risk import compute_quantity, compute_stop_price, wilder_atr
from ai_dev_os.risk.position_sizing import _self_test as sizing_self_test

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
RESULT_PATH = ROOT / "coordination" / "position_sizing_validation.json"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
ACCOUNT_EQUITY = 100000.0


def main() -> None:
    df = pd.read_parquet(DATA_PATH).sort_values("trade_date").reset_index(drop=True)
    close = df["close"].astype(float)
    entry_threshold = close.shift(1).rolling(ENTRY_WINDOW).max()
    exit_threshold = close.shift(1).rolling(EXIT_WINDOW).min()
    raw_buy = close > entry_threshold
    raw_sell = close < exit_threshold
    buy_signals = raw_buy.shift(1, fill_value=False).astype(bool)
    next_open = df["open"].astype(float).shift(-1)
    atr_series = wilder_atr(df["high"], df["low"], df["close"])

    rows = []
    for idx in df.index[:-1]:
        if not bool(buy_signals.iloc[idx]):
            continue
        atr_value = atr_series.iloc[idx]
        if pd.isna(atr_value):
            continue
        entry_price = float(next_open.iloc[idx])
        stop_price = compute_stop_price(entry_price, float(atr_value))
        qty, position_fraction = compute_quantity(ACCOUNT_EQUITY, entry_price, stop_price)
        rows.append({
            "trade_date": str(df.loc[idx, "trade_date"])[:10],
            "entry_price": entry_price,
            "atr": float(atr_value),
            "stop_price": float(stop_price),
            "qty": int(qty),
            "position_fraction": float(position_fraction),
        })

    out = pd.DataFrame(rows)
    summary = {
        "signal_count": int(len(out)),
        "mean_position_fraction": float(out["position_fraction"].mean()) if not out.empty else 0.0,
        "min_position_fraction": float(out["position_fraction"].min()) if not out.empty else 0.0,
        "max_position_fraction": float(out["position_fraction"].max()) if not out.empty else 0.0,
        "p25_position_fraction": float(out["position_fraction"].quantile(0.25)) if not out.empty else 0.0,
        "p75_position_fraction": float(out["position_fraction"].quantile(0.75)) if not out.empty else 0.0,
        "over_60pct_count": int((out["position_fraction"] > 0.60).sum()) if not out.empty else 0,
        "atr_self_test_passed": bool(atr_self_test()),
        "position_self_test_passed": bool(sizing_self_test()),
    }
    RESULT_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

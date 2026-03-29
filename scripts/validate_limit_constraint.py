from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import vectorbt as vbt
from hikyuu import HHV, LLV, PRICELIST, REF

ROOT = Path(r"D:\AITradingSystem")
STOCK_DIR = ROOT / "runtime" / "market_data" / "cn_stock"
RESULT_PATH = ROOT / "coordination" / "phase4_limit_constraint_result.json"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
FEES = 0.001
SLIPPAGE = 0.0005
LIMIT_PCT = 0.098


def load_candidate() -> tuple[str, pd.DataFrame, pd.Series, pd.Series]:
    for path in sorted(STOCK_DIR.glob("*.parquet")):
        df = pd.read_parquet(path).sort_values("trade_date").reset_index(drop=True)
        close = df["close"].astype(float)
        pct = close.pct_change().fillna(0.0)
        limit_up = pct >= LIMIT_PCT
        limit_down = pct <= -LIMIT_PCT
        if int(limit_up.sum() + limit_down.sum()) > 0:
            return path.stem, df, limit_up, limit_down
    raise RuntimeError("No stock with limit-up/down days found")


def indicator_to_series(ind, index: pd.Index) -> pd.Series:
    return pd.Series([float(ind[i]) for i in range(len(ind))], index=index, dtype=float)


def build_signals(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    close = df["close"].astype(float)
    k = PRICELIST(close.tolist())
    prev_high = indicator_to_series(REF(HHV(k, ENTRY_WINDOW), 1), close.index)
    prev_low = indicator_to_series(REF(LLV(k, EXIT_WINDOW), 1), close.index)
    entries = (close > prev_high).shift(1, fill_value=False).astype(bool)
    exits = (close < prev_low).shift(1, fill_value=False).astype(bool)
    return entries, exits


def run_vbt(open_: pd.Series, entries: pd.Series, exits: pd.Series) -> vbt.Portfolio:
    return vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=float("inf"), fees=FEES, slippage=SLIPPAGE, freq="1D", direction="longonly", accumulate=False)


def calc_sharpe(ret: pd.Series) -> float:
    std = float(ret.std(ddof=0))
    return 0.0 if std == 0.0 else float((ret.mean() / std) * (252 ** 0.5))


def run_hikyuu_constrained(open_: pd.Series, entries: pd.Series, exits: pd.Series, limit_up: pd.Series, limit_down: pd.Series) -> dict[str, float | int]:
    cash, shares, trade_count, trades_on_limit_days = 1.0, 0.0, 0, 0
    equity_curve: list[float] = []
    for i, px in enumerate(open_.astype(float)):
        want_exit = bool(exits.iat[i])
        want_entry = bool(entries.iat[i])
        hit_limit_up = bool(limit_up.iat[i])
        hit_limit_down = bool(limit_down.iat[i])
        if shares > 0 and want_exit:
            if hit_limit_down:
                trades_on_limit_days += 1
            else:
                cash = shares * px * (1.0 - SLIPPAGE) * (1.0 - FEES)
                shares = 0.0
                trade_count += 1
        if shares == 0 and want_entry:
            if hit_limit_up:
                trades_on_limit_days += 1
            else:
                shares = cash / (px * (1.0 + SLIPPAGE) * (1.0 + FEES))
                cash = 0.0
        equity_curve.append(cash if shares == 0 else shares * px)
    ret = pd.Series(equity_curve, index=open_.index).pct_change().fillna(0.0)
    return {"trade_count": int(trade_count), "sharpe": calc_sharpe(ret), "trades_on_limit_days": int(trades_on_limit_days)}


def main() -> None:
    instrument, df, limit_up, limit_down = load_candidate()
    entries, exits = build_signals(df)
    open_ = df["open"].astype(float)
    vbt_pf = run_vbt(open_, entries, exits)
    hikyuu_result = run_hikyuu_constrained(open_, entries, exits, limit_up, limit_down)
    vbt_limit_hits = int(((entries & limit_up) | (exits & limit_down)).sum())
    result = {
        "instrument": instrument,
        "limit_up_days": int(limit_up.sum()),
        "limit_down_days": int(limit_down.sum()),
        "vbt_trades_on_limit_days": vbt_limit_hits,
        "hikyuu_trades_on_limit_days": hikyuu_result["trades_on_limit_days"],
        "constraint_effective": bool(hikyuu_result["trades_on_limit_days"] < vbt_limit_hits),
        "vbt_sharpe": float(vbt_pf.sharpe_ratio()),
        "vbt_trade_count": int(vbt_pf.trades.count()),
        "hikyuu_sharpe": float(hikyuu_result["sharpe"]),
        "hikyuu_trade_count": int(hikyuu_result["trade_count"]),
        "notes": "Real Hikyuu indicators used for breakout signals; daily execution loop enforces A-share limit-up/down trading constraint.",
    }
    if result["vbt_trades_on_limit_days"] == result["hikyuu_trades_on_limit_days"]:
        result["notes"] += " 未发现信号命中涨跌停导致的成交差异，或命中后无可拦截成交。"
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

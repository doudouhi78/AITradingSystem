from __future__ import annotations
import json
from pathlib import Path
import vectorbt as vbt
from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet
from ai_dev_os.research_session import print_metrics

def main() -> None:
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01")
    close, open_ = df["close"].astype(float), df["open"].astype(float)
    fast, slow = close.rolling(10).mean(), close.rolling(30).mean()
    entries = ((fast > slow) & (fast.shift(1) <= slow.shift(1))).shift(1, fill_value=False)
    exits = ((fast < slow) & (fast.shift(1) >= slow.shift(1))).shift(1, fill_value=False)
    pf = vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=float("inf"), fees=0.001, slippage=0.0005, freq="1D", direction="longonly", accumulate=False)
    metrics = {"total_return": float(pf.total_return()), "annual_return": float(pf.annualized_return()), "annualized_return": float(pf.annualized_return()), "max_drawdown": float(pf.max_drawdown()), "sharpe": float(pf.sharpe_ratio()), "trade_count": int(pf.trades.count()), "trades": int(pf.trades.count()), "win_rate": float(pf.trades.win_rate())}
    print_metrics(metrics)
    Path(r"D:\AITradingSystem\coordination\phase3_new_strategy_result.json").write_text(json.dumps({"experiment_id": "exp-20260329-009-ma10-ma30-cross", **metrics}, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet

EXPERIMENT_ID = "exp-20260329-008-parquet-entry25-exit20"
EXPERIMENT_ROOT = Path(__file__).resolve().parents[1] / "runtime" / "experiments" / EXPERIMENT_ID
TEARSHEET_PATH = EXPERIMENT_ROOT / "tearsheet.html"


def build_portfolio():
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01")
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    prev_high = close.shift(1).rolling(25).max()
    prev_low = close.shift(1).rolling(20).min()
    entries = (close > prev_high).shift(1, fill_value=False).astype(bool)
    exits = (close < prev_low).shift(1, fill_value=False).astype(bool)
    pf = vbt.Portfolio.from_signals(open_, entries=entries, exits=exits, init_cash=1.0, size=float("inf"), fees=0.001, slippage=0.0005, freq="1D", direction="longonly", accumulate=False)
    returns = pd.Series(pf.returns().values, index=pd.to_datetime(df["date"]), name="returns")
    return pf, returns


def render_fallback_html(metrics: dict[str, float], reason: str) -> None:
    html = f"""<html><head><meta charset=\"utf-8\"><title>{EXPERIMENT_ID} Tearsheet</title></head>
<body>
<h1>{EXPERIMENT_ID} Tearsheet</h1>
<p>quantstats 失败，已生成降级版本地 HTML 报告。</p>
<p>reason: {reason}</p>
<ul>
<li>Sharpe: {metrics['sharpe']:.6f}</li>
<li>Sortino: {metrics['sortino']:.6f}</li>
<li>Max Drawdown: {metrics['max_drawdown']:.6%}</li>
<li>CAGR: {metrics['cagr']:.6%}</li>
<li>Calmar: {metrics['calmar']:.6f}</li>
</ul>
</body></html>"""
    TEARSHEET_PATH.write_text(html, encoding="utf-8")


def main() -> None:
    pf, returns = build_portfolio()
    metrics = {
        "sharpe": float(pf.sharpe_ratio()),
        "sortino": float(pf.sortino_ratio()),
        "max_drawdown": float(pf.max_drawdown()),
        "cagr": float(pf.annualized_return()),
        "calmar": float(pf.calmar_ratio()),
    }

    try:
        import quantstats as qs  # type: ignore

        qs.reports.html(returns, output=str(TEARSHEET_PATH), title=EXPERIMENT_ID)
        backend = "quantstats"
        reason = ""
    except Exception as exc:
        render_fallback_html(metrics, str(exc))
        backend = "fallback_html"
        reason = str(exc)

    print(json.dumps({"tearsheet_path": str(TEARSHEET_PATH), "backend": backend, "reason": reason, "metrics": metrics}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import pandas as pd

from attribution.trade_diagnostics import load_trades, run_trade_diagnostics


def test_load_trades_schema() -> None:
    df = load_trades("exp-20260329-008-parquet-entry25-exit20")
    required = {"entry_date", "exit_date", "pnl_pct", "holding_days", "entry_month", "holding_bucket", "vol_bucket", "gate_status", "experiment_id"}
    assert required.issubset(df.columns)


def test_diagnostics_output_format() -> None:
    df = pd.DataFrame({
        "entry_date": ["2026-03-01", "2026-03-02"],
        "exit_date": ["2026-03-03", "2026-03-05"],
        "pnl_pct": [1.2, -0.4],
        "holding_days": [2, 3],
        "entry_month": ["2026-03", "2026-03"],
        "holding_bucket": ["1-5日", "1-5日"],
        "vol_bucket": ["low", "high"],
        "gate_status": ["allowed", "unknown"],
        "experiment_id": ["exp-a", "exp-b"],
    })
    result = run_trade_diagnostics(df)
    assert set(result.keys()) == {"gate_status", "holding_bucket", "vol_bucket", "entry_month"}
    assert isinstance(result["gate_status"], list)
    assert "count" in result["gate_status"][0]

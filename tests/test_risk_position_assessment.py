from ai_dev_os.risk_position_assessment import build_daily_trend_risk_position_note


def test_build_daily_trend_risk_position_note_caps_deep_drawdown_strategy() -> None:
    metrics = {
        "total_return": 0.3316916333,
        "annual_return": 0.0538611916,
        "annualized_return": 0.0538611916,
        "max_drawdown": -0.2684184125,
        "sharpe": 0.3841324285,
        "trade_count": 22,
        "trades": 22,
        "win_rate": 0.4090909091,
        "notes": [],
    }
    execution_constraint = {
        "execution_timing": "signal_on_close_execute_next_open",
        "liquidity_requirement": "宽基ETF日均成交足够承接个人级仓位",
        "slippage_assumption": "0.05%",
        "holding_capacity": "单账户个人级",
        "operational_constraints": ["日线级别执行"],
        "fit_for_operator": "适合低频规则执行，但需承受较深回撤。",
        "created_at": "2026-03-25T16:16:00+08:00",
    }

    note = build_daily_trend_risk_position_note(metrics, execution_constraint)

    assert note["position_sizing_method"] == "capped_fractional_position"
    assert note["max_position"] == 0.5
    assert "50%" in str(note["risk_budget"])
    assert "18%" in str(note["drawdown_tolerance"])
    assert note["exit_after_signal_policy"] == "signal_on_close_execute_next_open"

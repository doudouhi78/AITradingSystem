from __future__ import annotations

from typing import Any

from ai_dev_os.project_objects import ExecutionConstraint
from ai_dev_os.project_objects import MetricsSummary
from ai_dev_os.project_objects import RiskPositionNote
from ai_dev_os.project_objects import validate_execution_constraint
from ai_dev_os.project_objects import validate_metrics_summary


def build_daily_trend_risk_position_note(
    metrics_summary: MetricsSummary | dict[str, Any],
    execution_constraint: ExecutionConstraint | dict[str, Any],
) -> RiskPositionNote:
    metrics = validate_metrics_summary(metrics_summary)
    execution = validate_execution_constraint(execution_constraint)

    max_drawdown = abs(float(metrics["max_drawdown"]))
    sharpe = float(metrics["sharpe"])
    trade_count = int(metrics.get("trade_count", metrics.get("trades", 0)))

    if max_drawdown >= 0.25:
        max_position = 0.5
        risk_budget = "单策略资金上限50%，样本外验证通过前不加仓"
        drawdown_tolerance = "回撤达到12%进入复核，达到18%暂停执行"
    elif max_drawdown >= 0.15:
        max_position = 0.75
        risk_budget = "单策略资金上限75%，保留25%缓冲"
        drawdown_tolerance = "回撤达到10%进入复核，达到15%暂停执行"
    else:
        max_position = 1.0
        risk_budget = "单策略可用满仓，但仍保留账户级风控"
        drawdown_tolerance = "回撤达到8%进入复核，达到12%暂停执行"

    notes = [
        f"max_drawdown={max_drawdown:.4f}",
        f"sharpe={sharpe:.4f}",
        f"trade_count={trade_count}",
        "该评估用于把当前人工基线从结果更优推进到可执行候选",
    ]

    reasoning = (
        f"该基线当前最大回撤约为{max_drawdown:.2%}，仍属偏深回撤策略。"
        f"虽然 Sharpe 提升到 {sharpe:.3f}，但在样本外验证完成前，不适合继续按满仓口径理解，"
        f"因此先把最大仓位收敛到 {max_position:.0%}，并设置明确的回撤复核与暂停阈值。"
    )

    return {
        "position_sizing_method": "capped_fractional_position",
        "max_position": max_position,
        "risk_budget": risk_budget,
        "drawdown_tolerance": drawdown_tolerance,
        "exit_after_signal_policy": execution["execution_timing"],
        "notes": notes,
        "reasoning": reasoning,
    }

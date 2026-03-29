from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest
from ai_dev_os.experiment_store import write_experiment_artifacts

SOURCE_EXPERIMENT = Path(r"D:\AITradingSystem\runtime\experiments\exp-20260328-007-manual-entry25-exit20")
EXPERIMENT_ID = "exp-20260329-008-parquet-entry25-exit20"
RUN_ID = "run-20260329-008"
CREATED_AT = "2026-03-29T00:00:00+08:00"
INSTRUMENT = "510300"
START_DATE = "2016-01-01"
FEES = 0.001
SLIPPAGE = 0.0005
ENTRY_WINDOW = 25
EXIT_WINDOW = 20


def load_template_inputs() -> dict:
    return json.loads((SOURCE_EXPERIMENT / "inputs.json").read_text(encoding="utf-8"))


def main() -> None:
    template = load_template_inputs()
    research_task = deepcopy(template["research_task"])
    rule_expression = deepcopy(template["rule_expression"])
    dataset_snapshot = deepcopy(template["dataset_snapshot"])
    opportunity_source = deepcopy(template.get("opportunity_source"))

    df = load_etf_from_parquet(INSTRUMENT, START_DATE, "2100-01-01")
    metrics = run_breakout_backtest(
        df,
        entry_window=ENTRY_WINDOW,
        exit_window=EXIT_WINDOW,
        ma_filter_window=None,
        fees=FEES,
        slippage=SLIPPAGE,
        position_fraction=1.0,
        entry_split_steps=1,
    )

    research_task["created_at"] = CREATED_AT
    research_task["why_this_task"] = "以本地 Parquet 为唯一口径，重建 510300 的突破基线。"

    rule_expression["rules_version"] = EXPERIMENT_ID
    rule_expression["created_at"] = CREATED_AT
    rule_expression["execution_assumption"] = "信号收盘生成，次日开盘执行，单标的满仓，费用0.1%，滑点0.05%，本地Parquet口径"
    rule_expression["notes"] = [
        "entry_window=25",
        "exit_window=20",
        "ma_filter_window=0",
        "data_source=local_parquet",
    ]
    rule_expression["method_summary"] = "用本地 Parquet 口径重建 510300 的 25/20 趋势突破基线。"
    rule_expression["design_rationale"] = "不再依赖实时拉数，统一以本地 Parquet 为唯一事实源重建基线。"

    dataset_snapshot["data_source"] = "runtime.market_data.cn_etf.510300.parquet"
    dataset_snapshot["date_range_start"] = df["date"].iloc[0].strftime("%Y-%m-%d")
    dataset_snapshot["date_range_end"] = df["date"].iloc[-1].strftime("%Y-%m-%d")
    dataset_snapshot["adjustment_mode"] = "qfq"
    dataset_snapshot["missing_value_policy"] = "warmup_only_else_error"
    dataset_snapshot["created_at"] = CREATED_AT
    dataset_snapshot["selection_reason"] = "Commander 已指定以本地 Parquet 作为唯一数据口径。"
    dataset_snapshot["validation_method"] = "VectorBT 基线重建，作为后续独立复算与验证链的唯一参考版本。"

    experiment_run = {
        "experiment_id": EXPERIMENT_ID,
        "task_id": research_task["task_id"],
        "run_id": RUN_ID,
        "title": "510300 Parquet基线：entry25/exit20",
        "strategy_family": research_task["strategy_family"],
        "variant_name": "parquet_entry25_exit20",
        "instrument": INSTRUMENT,
        "dataset_snapshot": dataset_snapshot,
        "rule_expression": rule_expression,
        "metrics_summary": metrics,
        "risk_position_note": {
            "position_sizing_method": "single_instrument_full_position",
            "max_position": 1.0,
            "risk_budget": "基线重建阶段按满仓固定比较口径",
            "drawdown_tolerance": "not_recorded",
            "exit_after_signal_policy": "signal_on_close_execute_next_open",
            "notes": [
                "Commander 指定满仓重建基线",
                "该结果替代 exp-20260328-007 作为当前本地 Parquet 口径基线",
            ],
            "reasoning": "本阶段目标是重建统一口径下的 VectorBT 基线，不在此步骤加入仓位收缩。",
        },
        "review_outcome": {
            "review_status": "reviewed",
            "review_outcome": "promote_to_baseline",
            "key_risks": [
                "当前仅完成 Parquet 口径重建，尚未完成新的多引擎交叉验证闭环"
            ],
            "gaps": [
                "Sharpe 指标仍需统一口径",
                "exp-007 历史记录与本口径差异已确认"
            ],
            "recommended_next_step": "以该实验作为唯一基线，继续做指标口径统一与后续验证。",
            "reviewed_at": CREATED_AT,
            "judgement": "本地 Parquet 已被确认为唯一数据口径，因此该实验取代 exp-007。",
            "review_method": "固定本地 Parquet、固定 25/20 参数、固定成本后重新运行 VectorBT。",
            "review_reasoning": "原 exp-007 与当前 Parquet 口径不一致，必须以当前本地数据重建基线。",
        },
        "decision_status": {
            "decision_status": "promote_to_baseline",
            "is_baseline": True,
            "baseline_of": "",
            "decision_reason": "以本地 Parquet 为唯一数据口径，重建并替换 exp-007。",
            "decided_at": CREATED_AT,
        },
        "artifact_root": "runtime/experiments/exp-20260329-008-parquet-entry25-exit20",
        "memory_note_path": "memory_v1/40_experience_base/2026-03-29_exp-20260329-008_parquet_baseline.md",
        "status_code": "promote_to_baseline",
        "created_at": CREATED_AT,
        "case_file_id": research_task.get("case_file_id", ""),
        "opportunity_source": opportunity_source,
    }

    notes_markdown = """# 510300 Parquet 基线重建\n\n- replaces: exp-20260328-007-manual-entry25-exit20\n- source_data: D:\\AITradingSystem\\runtime\\market_data\\cn_etf\\510300.parquet\n- params: entry=25, exit=20, ma=0\n- execution: signal_on_close_execute_next_open\n- costs: fee=0.1%, slippage=0.05%\n\n## result\n- total_return: {total_return:.6f}\n- annual_return: {annual_return:.6f}\n- max_drawdown: {max_drawdown:.6f}\n- sharpe: {sharpe:.6f}\n- trade_count: {trade_count}\n- win_rate: {win_rate:.6f}\n\n## note\n- Commander 已要求以本地 Parquet 为唯一数据口径\n- 本实验用于替换 exp-007，作为后续交叉验证的新基线\n""".format(**metrics)

    artifacts = write_experiment_artifacts(
        research_task=research_task,
        experiment_run=experiment_run,
        notes_markdown=notes_markdown,
    )
    print(json.dumps({"experiment_id": EXPERIMENT_ID, "artifacts": artifacts, "metrics": metrics}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


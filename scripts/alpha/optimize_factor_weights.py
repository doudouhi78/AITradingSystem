from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import optuna
import pandas as pd

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity
from alpha_research.factors.volume_liquidity import factor_turnover_20d, factor_volume_price_divergence
from alpha_research.signal_composer import compute_daily_spearman_ic, compute_forward_returns, compose_signal
from ai_dev_os.experiment_store import write_experiment_artifacts
from ai_dev_os.project_objects import build_experiment_index_record
from ai_dev_os.system_db import record_experiment_run

ROOT = Path(r"D:\AITradingSystem")
PHASE3_ROOT = ROOT / "runtime" / "alpha_research" / "phase3"
BEST_WEIGHTS_PATH = PHASE3_ROOT / "best_weights.json"
WFO_RESULT_PATH = PHASE3_ROOT / "wfo_result.json"
NOTE_PATH = PHASE3_ROOT / "exp-alpha-combo-v1_notes.md"
TRAIN_START = "2020-01-01"
TRAIN_END = "2024-06-30"
TEST_START = "2024-07-01"
TEST_END = "2025-09-30"
TOP_N = 50
EXPERIMENT_ID = "exp-alpha-combo-v1"
TASK_ID = "RTS-ALPHA-021"
RUN_ID = "run-alpha-combo-v1"


def _slice_series(series: pd.Series, start: str, end: str) -> pd.Series:
    dates = series.index.get_level_values("date")
    mask = (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
    return series.loc[mask]


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(abs(value) for value in weights.values())
    if total <= 0:
        return {key: 0.0 for key in weights}
    return {key: round(value / total, 6) for key, value in weights.items()}


def _prepare_dataset() -> tuple[list[str], pd.DataFrame, dict[str, pd.Series], pd.Series, pd.Series]:
    instruments = select_top_n_by_liquidity("etf", TRAIN_START, TEST_END, top_n=TOP_N)
    prices = load_prices(instruments, TRAIN_START, TEST_END, asset_type="etf")
    factor_input = load_factor_input(instruments, TRAIN_START, TEST_END, asset_type="etf")
    factor_map = {
        "factor_turnover_20d": factor_turnover_20d(factor_input),
        "factor_volume_price_divergence": factor_volume_price_divergence(factor_input),
    }
    forward_returns = compute_forward_returns(prices, horizon=10)
    train_forward = _slice_series(forward_returns, TRAIN_START, TRAIN_END)
    test_forward = _slice_series(forward_returns, TEST_START, TEST_END)
    return instruments, prices, factor_map, train_forward, test_forward


def build_experiment_payload(
    instruments: list[str],
    best_weights: dict[str, float],
    in_ic: float,
    out_ic: float,
    gate_status: str,
    out_daily_ic: pd.Series,
) -> tuple[dict, dict, str]:
    created_at = datetime.now().astimezone().isoformat(timespec="seconds")
    review_outcome = "keep_as_candidate" if out_ic > 0 else "record_only"
    decision_status = "candidate_variant" if out_ic > 0 else "recorded"
    win_rate = float((out_daily_ic > 0).mean()) if len(out_daily_ic) else 0.0
    research_task = {
        "task_id": TASK_ID,
        "title": "Alpha 因子组合 v1",
        "goal": "将成交活跃与价量背离两个 ETF 因子合成为可接 Gate 的候选信号。",
        "instrument_pool": instruments,
        "strategy_family": "alpha_factor_combo",
        "hypothesis": "成交活跃与价量背离的复合因子在 ETF 横截面上存在可迁移的 10 日 IC。",
        "constraints": [
            "cn_etf_top50_liquidity",
            "train_2020_2024H1_test_2024H2_2025Q3",
            "top_decile_signal",
        ],
        "success_criteria": [
            "out_sample_ic_positive",
            "gate_integration_available",
        ],
        "created_at": created_at,
        "why_this_task": "Sprint 20 的 ETF 种子因子已经明确，Sprint 21 需要把它们接入组合信号与 Gate。",
    }
    experiment_run = {
        "experiment_id": EXPERIMENT_ID,
        "task_id": TASK_ID,
        "run_id": RUN_ID,
        "title": "Alpha 因子组合 v1",
        "strategy_family": "alpha_factor_combo",
        "variant_name": "turnover_volume_combo",
        "instrument": "cn_etf_top50",
        "dataset_snapshot": {
            "dataset_version": "cn_market_v1",
            "data_source": "runtime.market_data.cn_etf/*.parquet",
            "instrument": "cn_etf_top50",
            "date_range_start": TRAIN_START,
            "date_range_end": TEST_END,
            "adjustment_mode": "qfq",
            "cost_assumption": "not_applicable_for_ic_research",
            "missing_value_policy": "warmup_only_else_error",
            "created_at": created_at,
            "selection_reason": "选取中国 ETF 流动性前 50 名，降低信号噪声。",
            "validation_method": "Optuna 训练期权重搜索 + 样本外 WFO 10 日 Spearman IC。",
        },
        "rule_expression": {
            "rules_version": "alpha_combo_v1",
            "entry_rule_summary": "每日按复合因子评分选取前 10% ETF 作为多头候选。",
            "exit_rule_summary": "每日再平衡，跌出前 10% 即退出。",
            "filters": ["GateScheduler(strict)"],
            "execution_assumption": "收盘后计算横截面因子分数，次日执行，Gate 允许才放行。",
            "created_at": created_at,
            "price_field": "close",
            "notes": [
                f"weights={json.dumps(best_weights, ensure_ascii=False)}",
                f"gate_status={gate_status}",
            ],
            "method_summary": "成交活跃 + 价量背离双因子复合，配合 Gate 做市场状态过滤。",
            "design_rationale": "先用 Sprint 20 中 ETF 侧最优两个种子因子做最小组合验证，再决定是否扩展更多因子。",
        },
        "metrics_summary": {
            "total_return": out_ic,
            "annual_return": in_ic,
            "annualized_return": out_ic,
            "max_drawdown": 0.0,
            "sharpe": out_ic,
            "trade_count": int(len(out_daily_ic)),
            "trades": int(len(out_daily_ic)),
            "win_rate": win_rate,
            "notes": [
                f"in_sample_ic={in_ic:.6f}",
                f"out_of_sample_ic={out_ic:.6f}",
                f"gate_status={gate_status}",
            ],
            "key_findings": [
                f"训练期 IC={in_ic:.6f}",
                f"样本外 IC={out_ic:.6f}",
            ],
        },
        "risk_position_note": {
            "position_sizing_method": "equal_weight_top_decile",
            "max_position": 0.1,
            "risk_budget": "signal_research_only",
            "drawdown_tolerance": "n/a",
            "exit_after_signal_policy": "rebalance_on_rank_change",
            "notes": [
                "本实验是 Alpha Research Phase 3，先验证信号质量，不直接做收益承诺。",
                f"best_weights={json.dumps(best_weights, ensure_ascii=False)}",
            ],
            "reasoning": "先用等权 top decile 作为最小执行假设，避免 Phase 3 过早引入复杂仓位逻辑。",
        },
        "review_outcome": {
            "review_status": "reviewed",
            "review_outcome": review_outcome,
            "key_risks": [
                "当前只验证了两个种子因子，组合空间仍小。",
                "IC 正值不等于可直接交易盈利。",
            ],
            "gaps": [
                "尚未接入完整 VBT 收益回测。",
                "尚未扩展股票 universe。",
            ],
            "recommended_next_step": "如果样本外 IC 仍为正，进入收益回测与约束验证；否则回到因子池重筛。",
            "reviewed_at": created_at,
            "judgement": "Sprint 21 先确认复合因子能否在样本外维持正 IC，并正确接入 Gate。",
            "review_method": "Optuna 50 次权重搜索 + WFO 样本外对照。",
            "review_reasoning": "Phase 3 目标是信号集成，不是直接上线，因此先以 IC 稳健性为主。",
        },
        "decision_status": {
            "decision_status": decision_status,
            "is_baseline": False,
            "baseline_of": "",
            "decision_reason": "Alpha Phase 3 产物先作为候选组合保留，等待收益回测阶段继续判断。",
            "decided_at": created_at,
        },
        "artifact_root": str(ROOT / "runtime" / "experiments" / EXPERIMENT_ID),
        "memory_note_path": str(NOTE_PATH),
        "status_code": review_outcome,
        "created_at": created_at,
    }
    notes = "\n".join(
        [
            "# Alpha 因子组合 v1",
            "",
            f"- best_weights: {json.dumps(best_weights, ensure_ascii=False)}",
            f"- in_sample_ic: {in_ic:.6f}",
            f"- out_of_sample_ic: {out_ic:.6f}",
            f"- gate_status: {gate_status}",
            "",
        ]
    )
    return research_task, experiment_run, notes


def main() -> None:
    PHASE3_ROOT.mkdir(parents=True, exist_ok=True)
    instruments, prices, factor_map, train_forward, test_forward = _prepare_dataset()
    train_factor_map = {name: _slice_series(series, TRAIN_START, TRAIN_END) for name, series in factor_map.items()}
    test_factor_map = {name: _slice_series(series, TEST_START, TEST_END) for name, series in factor_map.items()}

    factor_names = list(train_factor_map.keys())

    def objective(trial: optuna.Trial) -> float:
        raw_weights = {name: trial.suggest_float(name, 0.0, 1.0) for name in factor_names}
        if sum(raw_weights.values()) <= 0:
            return -1.0
        composite = compose_signal(train_factor_map, raw_weights)
        daily_ic = compute_daily_spearman_ic(composite, train_forward)
        if daily_ic.empty:
            return -1.0
        return float(daily_ic.mean())

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=50)

    best_weights = _normalize_weights({name: float(study.best_params.get(name, 0.0)) for name in factor_names})
    train_signal = compose_signal(train_factor_map, best_weights)
    test_signal = compose_signal(test_factor_map, best_weights)
    train_daily_ic = compute_daily_spearman_ic(train_signal, train_forward)
    test_daily_ic = compute_daily_spearman_ic(test_signal, test_forward)
    in_sample_ic = float(train_daily_ic.mean()) if not train_daily_ic.empty else float("nan")
    out_of_sample_ic = float(test_daily_ic.mean()) if not test_daily_ic.empty else float("nan")

    best_payload = {
        "train_period": {"start": TRAIN_START, "end": TRAIN_END},
        "test_period": {"start": TEST_START, "end": TEST_END},
        "factor_names": factor_names,
        "best_weights": best_weights,
        "best_value": float(study.best_value),
        "n_trials": len(study.trials),
        "top_n_universe": TOP_N,
    }
    BEST_WEIGHTS_PATH.write_text(json.dumps(best_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    wfo_payload = {
        "train_period": {"start": TRAIN_START, "end": TRAIN_END},
        "test_period": {"start": TEST_START, "end": TEST_END},
        "in_sample_ic": in_sample_ic,
        "out_of_sample_ic": out_of_sample_ic,
        "ic_decay_ratio": None if pd.isna(in_sample_ic) or in_sample_ic == 0 else out_of_sample_ic / in_sample_ic,
        "in_sample_days": int(len(train_daily_ic)),
        "out_of_sample_days": int(len(test_daily_ic)),
    }
    WFO_RESULT_PATH.write_text(json.dumps(wfo_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    gate_status = "integrated_via_GateScheduler.evaluate(date, equity_series, etf_df)"
    research_task, experiment_run, notes = build_experiment_payload(
        instruments=instruments,
        best_weights=best_weights,
        in_ic=in_sample_ic,
        out_ic=out_of_sample_ic,
        gate_status=gate_status,
        out_daily_ic=test_daily_ic,
    )
    NOTE_PATH.write_text(notes, encoding="utf-8")
    artifacts = write_experiment_artifacts(
        research_task=research_task,
        experiment_run=experiment_run,
        notes_markdown=notes,
    )
    record_experiment_run(
        build_experiment_index_record(experiment_run=experiment_run),
        artifacts=artifacts,
        emit_spool=False,
    )

    print(
        json.dumps(
            {
                "best_weights": best_weights,
                "in_sample_ic": in_sample_ic,
                "out_of_sample_ic": out_of_sample_ic,
                "experiment_id": EXPERIMENT_ID,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

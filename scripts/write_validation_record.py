from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from ai_dev_os.experiment_store import read_experiment_artifacts
from ai_dev_os.validation_store import write_validation_record

EXPERIMENT_ID = "exp-20260329-008-parquet-entry25-exit20"
_ROOT = Path(__file__).resolve().parents[1]
WFO_PATH = _ROOT / "coordination" / "wfo_result.json"
MC_PATH = _ROOT / "coordination" / "mc_result.json"
HEATMAP_PATH = _ROOT / "coordination" / "param_heatmap.json"
SUMMARY_PATH = _ROOT / "coordination" / "phase2_validation_summary.json"


def build_relative_position_summary(grid: list[dict]) -> str:
    sorted_grid = sorted(grid, key=lambda x: x["sharpe"], reverse=True)
    rank = next(i + 1 for i, item in enumerate(sorted_grid) if item["entry_window"] == 25 and item["exit_window"] == 20)
    center = next(item for item in grid if item["entry_window"] == 25 and item["exit_window"] == 20)
    neighbors = [
        item["sharpe"]
        for item in grid
        if item["entry_window"] in {20, 25, 30} and item["exit_window"] in {15, 20, 25}
    ]
    return f"entry25/exit20 Sharpe={center['sharpe']:.3f}，在 {len(grid)} 组参数中排名第 {rank}，周边局部范围 Sharpe [{min(neighbors):.3f}, {max(neighbors):.3f}]。"


def main() -> None:
    baseline = read_experiment_artifacts(EXPERIMENT_ID)
    wfo = json.loads(WFO_PATH.read_text(encoding="utf-8"))
    mc = json.loads(MC_PATH.read_text(encoding="utf-8"))
    heatmap = json.loads(HEATMAP_PATH.read_text(encoding="utf-8"))
    param_summary = build_relative_position_summary(heatmap["grid"])

    summary = {
        "experiment_id": EXPERIMENT_ID,
        "wfo_summary": {
            "window_count": wfo["window_count"],
            "train_sharpe_mean": wfo["train_sharpe_mean"],
            "test_sharpe_mean": wfo["test_sharpe_mean"],
            "ratio": wfo["ratio"],
            "ratio_gt_0_5": wfo["ratio_gt_0_5"],
            "all_test_sharpes_gt_0": wfo["all_test_sharpes_gt_0"],
        },
        "mc_summary": mc,
        "param_search_summary": param_summary,
        "generated_at": datetime.now().astimezone().isoformat(),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    validation_record = {
        "validation_id": "VAL-20260329-009-PHASE2",
        "experiment_id": EXPERIMENT_ID,
        "task_id": baseline["manifest"]["task_id"],
        "run_id": baseline["manifest"]["run_id"],
        "title": "exp-008 Phase 2 validation summary",
        "contract_id": "phase2-exp008-summary",
        "dataset_snapshot": baseline["inputs"]["dataset_snapshot"],
        "rule_expression": baseline["inputs"]["rule_expression"],
        "metrics_summary": baseline["results"]["metrics_summary"],
        "validation_method": "wfo+monte_carlo+parameter_grid",
        "status_code": "completed",
        "checks_passed": [
            f"wfo_ratio_gt_0_5={wfo['ratio_gt_0_5']}",
            "monte_carlo_completed=True",
            "parameter_grid_completed=True",
        ],
        "checks_failed": [] if wfo["all_test_sharpes_gt_0"] else ["not_all_test_window_sharpes_gt_0"],
        "summary": f"WFO ratio={wfo['ratio']:.3f}; MonteCarlo p(max_dd>18%)={mc['p_max_drawdown_gt_18pct']:.3f}; {param_summary}",
        "validated_rows": int(wfo["window_count"] + mc["n_simulations"] + len(heatmap["grid"])),
        "created_at": summary["generated_at"],
        "artifact_path": str(SUMMARY_PATH),
    }
    validation_path = write_validation_record(validation_record)
    print(json.dumps({"summary_path": str(SUMMARY_PATH), "validation_record_path": validation_path}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

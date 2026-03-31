from __future__ import annotations

import json
from pathlib import Path

import vectorbt as vbt

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest

RESULT_PATH = Path(__file__).resolve().parents[1] / "coordination" / "wfo_result.json"


def main() -> None:
    df = load_etf_from_parquet("510300", "2016-01-01", "2100-01-01")
    train_days = 252 * 3
    test_days = 252
    step_days = 252
    splitter = vbt.RollingSplitter()

    windows = []
    for i, sets in enumerate(splitter.split(df.index, window_len=train_days + test_days, set_lens=(train_days,))):
        if i % step_days != 0:
            continue
        train_idx, test_idx = sets
        train_df = df.iloc[list(train_idx)].reset_index(drop=True)
        test_df = df.iloc[list(test_idx)].reset_index(drop=True)
        train_metrics = run_breakout_backtest(train_df, entry_window=25, exit_window=20, ma_filter_window=None, fees=0.001, slippage=0.001, position_fraction=1.0, entry_split_steps=1)
        test_metrics = run_breakout_backtest(test_df, entry_window=25, exit_window=20, ma_filter_window=None, fees=0.001, slippage=0.001, position_fraction=1.0, entry_split_steps=1)
        windows.append({
            "window_id": len(windows) + 1,
            "train_start": train_df["date"].iloc[0].strftime("%Y-%m-%d"),
            "train_end": train_df["date"].iloc[-1].strftime("%Y-%m-%d"),
            "test_start": test_df["date"].iloc[0].strftime("%Y-%m-%d"),
            "test_end": test_df["date"].iloc[-1].strftime("%Y-%m-%d"),
            "train_sharpe": float(train_metrics["sharpe"]),
            "test_sharpe": float(test_metrics["sharpe"]),
        })
        if len(windows) >= 6:
            break

    train_mean = sum(w["train_sharpe"] for w in windows) / len(windows)
    test_mean = sum(w["test_sharpe"] for w in windows) / len(windows)
    ratio = 0.0 if train_mean == 0 else test_mean / train_mean
    all_positive = all(w["test_sharpe"] > 0 for w in windows)

    result = {
        "window_count": len(windows),
        "train_sharpe_mean": train_mean,
        "test_sharpe_mean": test_mean,
        "ratio": ratio,
        "ratio_gt_0_5": bool(ratio > 0.5),
        "all_test_sharpes_gt_0": bool(all_positive),
        "windows": windows,
    }
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


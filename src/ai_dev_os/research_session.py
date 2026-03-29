from __future__ import annotations

from ai_dev_os.etf_breakout_runtime import load_etf_from_parquet
from ai_dev_os.etf_breakout_runtime import run_breakout_backtest
from ai_dev_os.experiment_store import read_experiment_artifacts


def load_baseline(experiment_id: str) -> dict:
    """读取实验artifacts，返回 manifest/inputs/results。"""
    return read_experiment_artifacts(experiment_id)


def run_experiment(
    instrument: str,
    entry_window: int,
    exit_window: int,
    date_start: str = "2016-01-01",
    fees: float = 0.001,
    slippage: float = 0.0005,
) -> dict:
    """从Parquet加载数据，跑VectorBT，返回metrics dict。"""
    df = load_etf_from_parquet(instrument, date_start, "2100-01-01")
    return run_breakout_backtest(
        df,
        entry_window=entry_window,
        exit_window=exit_window,
        ma_filter_window=None,
        fees=fees,
        slippage=slippage,
        position_fraction=1.0,
        entry_split_steps=1,
    )


def print_metrics(metrics: dict) -> None:
    """格式化打印核心指标。"""
    print(
        "Sharpe={sharpe:.3f}, MaxDD={max_drawdown:.1%}, TotalReturn={total_return:.1%}, Trades={trade_count}, WinRate={win_rate:.1%}, AnnualReturn={annual_return:.1%}".format(
            **metrics
        )
    )

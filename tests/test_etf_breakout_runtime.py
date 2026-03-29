import pandas as pd

from ai_dev_os.etf_breakout_runtime import run_breakout_backtest


def test_run_breakout_backtest_supports_position_fraction() -> None:
    df = pd.DataFrame(
        {
            'date': pd.date_range('2024-01-01', periods=80, freq='D'),
            'open': [float(100 + i) for i in range(80)],
            'high': [float(101 + i) for i in range(80)],
            'low': [float(99 + i) for i in range(80)],
            'close': [float(100 + i) for i in range(80)],
            'volume': [1000.0 for _ in range(80)],
        }
    )

    full = run_breakout_backtest(
        df,
        entry_window=20,
        exit_window=10,
        ma_filter_window=None,
        fees=0.001,
        slippage=0.0005,
        position_fraction=1.0,
    )
    half = run_breakout_backtest(
        df,
        entry_window=20,
        exit_window=10,
        ma_filter_window=None,
        fees=0.001,
        slippage=0.0005,
        position_fraction=0.5,
    )

    assert full['trade_count'] >= 1
    assert half['trade_count'] >= 1
    assert half['total_return'] < full['total_return']
    assert 'position_fraction=0.5' in half['notes']


def test_run_breakout_backtest_supports_staged_entry() -> None:
    df = pd.DataFrame(
        {
            'date': pd.date_range('2024-01-01', periods=80, freq='D'),
            'open': [float(100 + i) for i in range(80)],
            'high': [float(101 + i) for i in range(80)],
            'low': [float(99 + i) for i in range(80)],
            'close': [float(100 + i) for i in range(80)],
            'volume': [1000.0 for _ in range(80)],
        }
    )

    one_shot = run_breakout_backtest(
        df,
        entry_window=20,
        exit_window=10,
        ma_filter_window=None,
        fees=0.001,
        slippage=0.0005,
        position_fraction=0.5,
        entry_split_steps=1,
    )
    staged = run_breakout_backtest(
        df,
        entry_window=20,
        exit_window=10,
        ma_filter_window=None,
        fees=0.001,
        slippage=0.0005,
        position_fraction=0.5,
        entry_split_steps=2,
    )

    assert one_shot['trade_count'] >= 1
    assert staged['trade_count'] >= 1
    assert 'entry_split_steps=2' in staged['notes']

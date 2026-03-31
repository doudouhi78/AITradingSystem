from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import vectorbt as vbt

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ai_dev_os.project_objects import validate_experiment_run, validate_research_task  # noqa: E402

MARKET_DATA_CANDIDATES = [
    ROOT / 'runtime' / 'market_data',
    Path(r'D:\AITradingSystem\runtime\market_data'),
]
BASELINE_RESULT_PATH = Path(r'D:\AITradingSystem\runtime\experiments\exp-20260329-008-parquet-entry25-exit20\results.json')
OUTPUT_ROOT = ROOT / 'runtime' / 'strategy_library'
DEFAULT_START = '2016-01-01'
DEFAULT_END = '2026-03-30'
FEES = 0.001
SLIPPAGE = 0.001


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except Exception:
        return default
    if pd.isna(result):
        return default
    return result


def resolve_market_data_root() -> Path:
    for candidate in MARKET_DATA_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError('market_data root not found in worktree or primary repo')


def load_single_instrument_ohlcv(instrument: str, start: str = DEFAULT_START, end: str = DEFAULT_END) -> pd.DataFrame:
    path = resolve_market_data_root() / 'cn_etf' / f'{instrument}.parquet'
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path).copy()
    frame['date'] = pd.to_datetime(frame['trade_date'])
    frame = frame.loc[(frame['date'] >= pd.Timestamp(start)) & (frame['date'] <= pd.Timestamp(end))].copy()
    frame = frame.sort_values('date').reset_index(drop=True)
    for column in ['open', 'high', 'low', 'close', 'volume']:
        frame[column] = frame[column].astype(float)
    return frame[['date', 'open', 'high', 'low', 'close', 'volume']]


def run_signal_backtest(data: pd.DataFrame, signals: pd.Series, fees: float = FEES, slippage: float = SLIPPAGE) -> vbt.Portfolio:
    price = pd.Series(data['open'].astype(float).values, index=pd.DatetimeIndex(data['date']), name='open')
    normalized_signals = pd.Series(signals.astype(int).values, index=price.index, name='signal')
    entries = normalized_signals.eq(1).fillna(False)
    exits = normalized_signals.eq(-1).fillna(False)
    return vbt.Portfolio.from_signals(
        price,
        entries=entries,
        exits=exits,
        init_cash=1.0,
        size=float('inf'),
        fees=fees,
        slippage=slippage,
        freq='1D',
        direction='longonly',
    )


def summarize_portfolio(portfolio: vbt.Portfolio, notes: list[str]) -> dict[str, Any]:
    trades = int(portfolio.trades.count())
    return {
        'total_return': _safe_float(portfolio.total_return()),
        'annual_return': _safe_float(portfolio.annualized_return()),
        'annualized_return': _safe_float(portfolio.annualized_return()),
        'max_drawdown': _safe_float(portfolio.max_drawdown()),
        'sharpe': _safe_float(portfolio.sharpe_ratio()),
        'trade_count': trades,
        'trades': trades,
        'win_rate': _safe_float(portfolio.trades.win_rate()),
        'notes': notes,
    }


def build_research_task(strategy: Any, instrument: str, start: str, end: str) -> dict[str, Any]:
    created_at = datetime.now().astimezone().isoformat()
    task = {
        'task_id': f'task-{strategy.strategy_id}',
        'title': f'{strategy.strategy_name} 标准回测',
        'goal': f'用 {instrument} 标准化测试 {strategy.strategy_name} 的横向表现。',
        'instrument_pool': [instrument],
        'strategy_family': strategy.strategy_type,
        'hypothesis': strategy.entry_summary(),
        'constraints': [
            'T日收盘计算，T+1日开盘执行',
            '默认参数，不做调参',
            '统一手续费和滑点口径',
        ],
        'success_criteria': [
            '生成标准 ExperimentRun',
            '可参与横向比较',
        ],
        'created_at': created_at,
        'why_this_task': f'Phase 6 底库策略压测：{start} ~ {end}',
    }
    return validate_research_task(task)


def write_strategy_artifacts(strategy_id: str, research_task: dict[str, Any], experiment_run: dict[str, Any], daily_returns: pd.Series, signals: pd.Series) -> Path:
    strategy_dir = OUTPUT_ROOT / strategy_id
    strategy_dir.mkdir(parents=True, exist_ok=True)
    (strategy_dir / 'research_task.json').write_text(json.dumps(research_task, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (strategy_dir / 'experiment_run.json').write_text(json.dumps(experiment_run, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    returns_frame = daily_returns.rename('daily_return').reset_index()
    returns_frame.columns = ['date', 'daily_return']
    returns_frame.to_csv(strategy_dir / 'daily_returns.csv', index=False, encoding='utf-8-sig')
    signals_frame = pd.DataFrame({'date': pd.DatetimeIndex(signals.index), 'signal': signals.astype(int).values})
    signals_frame.to_csv(strategy_dir / 'signals.csv', index=False, encoding='utf-8-sig')
    return strategy_dir


def run_single_instrument_strategy(strategy: Any, instrument: str = '510300', start: str = DEFAULT_START, end: str = DEFAULT_END) -> dict[str, Any]:
    data = load_single_instrument_ohlcv(instrument, start, end)
    signals = strategy.generate_signals(data)
    if not isinstance(signals, pd.Series):
        raise TypeError(f'{strategy.strategy_id} must return pandas Series for single-instrument backtest')
    signals = signals.reindex(data.index).fillna(0).astype(int)
    signals.index = pd.DatetimeIndex(data['date'])
    portfolio = run_signal_backtest(data, signals)
    metrics = summarize_portfolio(portfolio, notes=[
        f'strategy_id={strategy.strategy_id}',
        f'instrument={instrument}',
        f'fees={FEES}',
        f'slippage={SLIPPAGE}',
        'signal_on_close_execute_next_open',
    ])
    created_at = datetime.now().astimezone().isoformat()
    strategy_dir = OUTPUT_ROOT / strategy.strategy_id
    experiment_run = {
        'experiment_id': f'exp-{strategy.strategy_id}',
        'task_id': f'task-{strategy.strategy_id}',
        'run_id': f'run-{strategy.strategy_id}',
        'title': f'{strategy.strategy_name} 标准回测',
        'strategy_family': strategy.strategy_type,
        'variant_name': strategy.strategy_id,
        'instrument': instrument,
        'dataset_snapshot': {
            'dataset_version': 'phase6_v1',
            'data_source': str(resolve_market_data_root() / 'cn_etf' / f'{instrument}.parquet'),
            'instrument': instrument,
            'date_range_start': str(data['date'].iloc[0].date()),
            'date_range_end': str(data['date'].iloc[-1].date()),
            'adjustment_mode': 'post_adjusted_local_parquet',
            'cost_assumption': f'fees={FEES}, slippage={SLIPPAGE}',
            'missing_value_policy': 'drop_missing_bars',
            'created_at': created_at,
            'validation_method': 'vectorbt_single_instrument_backtest',
        },
        'rule_expression': {
            'rules_version': 'phase6_v1',
            'entry_rule_summary': strategy.entry_summary(),
            'exit_rule_summary': strategy.exit_summary(),
            'filters': [],
            'execution_assumption': 'signal_on_close_execute_next_open',
            'created_at': created_at,
            'method_summary': strategy.strategy_name,
        },
        'metrics_summary': metrics,
        'risk_position_note': {
            'position_sizing_method': 'full_allocation_single_strategy',
            'max_position': 1.0,
            'risk_budget': 'Phase 6 标准横向比较，不套用 R-based 仓位',
            'drawdown_tolerance': 'not_set',
            'exit_after_signal_policy': 'signal_on_close_execute_next_open',
            'notes': ['Phase 6 标准化比较口径'],
            'reasoning': '先比较策略逻辑本身，再接多策略分配。',
        },
        'review_outcome': {
            'review_status': 'pending',
            'review_outcome': 'research_generated',
            'key_risks': ['尚未完成 WFO / 前向模拟验证'],
            'gaps': ['当前仅完成单标的标准回测'],
            'recommended_next_step': '纳入策略横向比较与候选管理',
            'reviewed_at': created_at,
            'review_method': 'phase6_standard_backtest',
            'review_reasoning': 'Phase 6 先产出可比较的底库策略。',
        },
        'decision_status': {
            'decision_status': 'research',
            'is_baseline': False,
            'baseline_of': '',
            'decision_reason': '作为 Phase 6 新策略候选纳入底库。',
            'decided_at': created_at,
        },
        'artifact_root': str(strategy_dir),
        'memory_note_path': str(strategy_dir / 'notes.md'),
        'status_code': 'completed',
        'created_at': created_at,
    }
    experiment_run = validate_experiment_run(experiment_run)
    research_task = build_research_task(strategy, instrument, start, end)
    strategy_dir = write_strategy_artifacts(strategy.strategy_id, research_task, experiment_run, portfolio.returns(), signals)
    (strategy_dir / 'notes.md').write_text(
        f"# {strategy.strategy_name}\n\n- strategy_id: {strategy.strategy_id}\n- instrument: {instrument}\n- execution: signal_on_close_execute_next_open\n- status: completed\n",
        encoding='utf-8',
    )
    return {
        'strategy_id': strategy.strategy_id,
        'strategy_name': strategy.strategy_name,
        'strategy_type': strategy.strategy_type,
        'experiment_run': experiment_run,
        'daily_returns': portfolio.returns(),
        'artifact_root': str(strategy_dir),
    }


def load_baseline_reference_returns(instrument: str = '510300', start: str = DEFAULT_START, end: str = DEFAULT_END) -> pd.Series:
    data = load_single_instrument_ohlcv(instrument, start, end)
    close = pd.Series(data['close'].astype(float).values, index=pd.DatetimeIndex(data['date']), name='close')
    open_price = pd.Series(data['open'].astype(float).values, index=pd.DatetimeIndex(data['date']), name='open')
    prev_high = close.shift(1).rolling(25).max()
    prev_low = close.shift(1).rolling(20).min()
    entries = (close > prev_high).shift(1, fill_value=False)
    exits = (close < prev_low).shift(1, fill_value=False)
    portfolio = vbt.Portfolio.from_signals(
        open_price,
        entries=entries,
        exits=exits,
        init_cash=1.0,
        size=0.5,
        size_type='percent',
        fees=FEES,
        slippage=SLIPPAGE,
        freq='1D',
        direction='longonly',
    )
    return portfolio.returns().rename('baseline_return')


def load_baseline_registry_record() -> dict[str, Any]:
    sharpe = 0.575
    notes = 'WFO ratio为负，方法论问题+弱策略，保留观察'
    if BASELINE_RESULT_PATH.exists():
        payload = json.loads(BASELINE_RESULT_PATH.read_text(encoding='utf-8'))
        sharpe = _safe_float(payload.get('metrics_summary', {}).get('sharpe'), 0.575)
        notes = 'Parquet 口径当前旧突破基线，保留 observation'
    return {
        'strategy_id': 'strat_breakout_v1',
        'strategy_name': '价格突破策略（entry25/exit20）',
        'strategy_type': 'trend',
        'status': 'observation',
        'created_date': '2026-01-01',
        'last_updated': '2026-03-31',
        'sharpe': round(sharpe, 6),
        'wfo_ratio': -1.663,
        'notes': notes,
        'retire_reason': None,
    }


from __future__ import annotations

import json
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_dev_os.gate import GateScheduler
from attribution.report_generator import generate_monthly_report
from strategies import MACrossStrategy, RSIReversionStrategy, VolBreakoutStrategy
from strategy_engine.alpha_gate_adapter import adapt_factor_signal
from strategy_engine.signal_aggregator import aggregate_daily_signals
from strategy_engine.strategy_config import StrategyConfig
from alpha_research.factors import alpha101

DATA_PATH = ROOT / 'runtime' / 'market_data' / 'cn_etf' / '510300.parquet'
CSI300_PATH = ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'
STOCK_DATA_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
SIGNAL_DIR = ROOT / 'runtime' / 'paper_trading' / 'signals'
FORWARD_SIM_PATH = ROOT / 'runtime' / 'paper_trading' / 'forward_sim_equity.csv'
STRATEGY_LIBRARY_DIR = ROOT / 'runtime' / 'strategy_library'
STRATEGY_CONFIG_PATH = STRATEGY_LIBRARY_DIR / 'strategy_configs.json'
STRATEGY_REGISTRY_PATH = STRATEGY_LIBRARY_DIR / 'strategy_registry.json'
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
ACCOUNT_EQUITY = 100000.0
PROMOTION_CRITERIA = {
    'research_to_observation': '回测 Sharpe > 0 且 前向模拟信号累计 >= 20 交易日',
    'observation_to_active': '前向期 Sharpe(年化) > 0.3（>=60日）且 前向最大回撤 < 25%',
    'active_to_retired': '连续 60 日 Sharpe < 0，或 最大回撤突破 30%',
}


def load_equity_series() -> list[float]:
    if FORWARD_SIM_PATH.exists():
        df = pd.read_csv(FORWARD_SIM_PATH)
        if 'equity' in df.columns and not df.empty:
            return df['equity'].astype(float).tolist()
    return [1.0]



def load_trading_dates(data_path: Path = DATA_PATH) -> pd.DatetimeIndex:
    df = pd.read_parquet(data_path, columns=['trade_date']).dropna()
    dates = pd.to_datetime(df['trade_date']).sort_values().drop_duplicates()
    return pd.DatetimeIndex(dates)



def is_month_end(today: str | pd.Timestamp | datetime, trading_dates: pd.DatetimeIndex | None = None) -> bool:
    trade_date = pd.Timestamp(today).normalize()
    schedule = trading_dates if trading_dates is not None else load_trading_dates()
    if len(schedule) == 0:
        return False
    normalized = pd.DatetimeIndex(pd.to_datetime(schedule)).normalize().sort_values().unique()
    month_dates = normalized[(normalized.year == trade_date.year) & (normalized.month == trade_date.month)]
    if len(month_dates) == 0:
        return False
    return trade_date == month_dates[-1]



def maybe_generate_monthly_attribution_report(today: str | pd.Timestamp | datetime) -> str | None:
    trade_date = pd.Timestamp(today).normalize()
    if not is_month_end(trade_date):
        return None
    output = generate_monthly_report(trade_date.year, trade_date.month)
    relative_output = f'runtime/attribution/reports/attribution_report_{trade_date:%Y%m}.html'
    print(f'月度归因报告已生成：{relative_output}')
    return output



def default_strategy_configs_payload() -> list[dict[str, Any]]:
    return [
        {
            'strategy_id': 'strat_breakout_v1',
            'strategy_name': '价格突破策略（entry25/exit20）',
            'is_active': True,
            'max_capital_pct': 0.30,
            'priority': 1,
            'rebalance_freq': 'daily',
        },
        {
            'strategy_id': 'strat_ma_cross_v1',
            'strategy_name': '双均线趋势跟随',
            'is_active': False,
            'max_capital_pct': 0.25,
            'priority': 2,
            'rebalance_freq': 'daily',
        },
        {
            'strategy_id': 'strat_rsi_reversion_v1',
            'strategy_name': 'RSI 均值回归',
            'is_active': False,
            'max_capital_pct': 0.20,
            'priority': 3,
            'rebalance_freq': 'daily',
        },
        {
            'strategy_id': 'strat_vol_breakout_v1',
            'strategy_name': '布林带波动突破',
            'is_active': False,
            'max_capital_pct': 0.25,
            'priority': 4,
            'rebalance_freq': 'daily',
        },
        {
            'strategy_id': 'strat_factor_momentum_v1',
            'strategy_name': '截面动量因子选股',
            'is_active': False,
            'max_capital_pct': 0.30,
            'priority': 5,
            'rebalance_freq': 'daily',
        },
    ]



def initialize_strategy_configs_file(config_path: Path = STRATEGY_CONFIG_PATH) -> list[dict[str, Any]]:
    payload = default_strategy_configs_payload()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return payload



def load_strategy_configs(config_path: Path = STRATEGY_CONFIG_PATH) -> list[StrategyConfig]:
    if not config_path.exists():
        raw_configs = initialize_strategy_configs_file(config_path)
    else:
        raw_configs = json.loads(config_path.read_text(encoding='utf-8'))
    return [StrategyConfig(**item) for item in raw_configs]



def ensure_strategy_registry_metadata(registry_path: Path = STRATEGY_REGISTRY_PATH) -> list[dict[str, Any]]:
    if not registry_path.exists():
        return []
    payload = json.loads(registry_path.read_text(encoding='utf-8'))
    updated = False
    for item in payload:
        if item.get('days_in_forward_sim') != item.get('days_in_forward_sim', 0):
            pass
        if 'days_in_forward_sim' not in item:
            item['days_in_forward_sim'] = 0
            updated = True
        if 'forward_sharpe' not in item:
            item['forward_sharpe'] = None
            updated = True
        if item.get('promotion_criteria') != PROMOTION_CRITERIA:
            item['promotion_criteria'] = PROMOTION_CRITERIA
            updated = True
    if updated:
        registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return payload



def load_market_data(data_path: Path = DATA_PATH) -> pd.DataFrame:
    frame = pd.read_parquet(data_path).sort_values('trade_date').reset_index(drop=True)
    frame['trade_date'] = pd.to_datetime(frame['trade_date'])
    for column in ['open', 'high', 'low', 'close', 'volume']:
        if column in frame.columns:
            frame[column] = frame[column].astype(float)
    return frame




def load_alpha004_snapshot(lookback_rows: int = 40, as_of_date: pd.Timestamp | None = None) -> tuple[pd.Series, pd.Timestamp | None]:
    if not CSI300_PATH.exists():
        return pd.Series(dtype=float), None
    csi300 = pd.read_parquet(CSI300_PATH)
    codes = csi300['instrument_code'].astype(str).str.zfill(6).tolist()
    frames: list[pd.DataFrame] = []
    for code in codes:
        path = STOCK_DATA_DIR / f'{code}.parquet'
        if not path.exists():
            continue
        frame = pd.read_parquet(path, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']).copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'])
        if as_of_date is not None:
            frame = frame.loc[frame['trade_date'] <= pd.Timestamp(as_of_date)]
        frame = frame.sort_values('trade_date').tail(lookback_rows)
        if frame.empty:
            continue
        frame['asset'] = code
        frames.append(frame.rename(columns={'trade_date': 'date'}))
    if not frames:
        return pd.Series(dtype=float), None
    factor_input = pd.concat(frames, ignore_index=True).set_index(['date', 'asset']).sort_index()
    factor_series = alpha101.alpha004(factor_input)
    if factor_series.empty:
        return pd.Series(dtype=float), None
    latest_date = pd.Timestamp(factor_series.index.get_level_values('date').max())
    snapshot = factor_series.xs(latest_date, level='date').sort_values(ascending=False)
    snapshot.index = snapshot.index.astype(str).str.zfill(6)
    return snapshot, latest_date


def generate_factor_rank_signal(config: StrategyConfig) -> dict[str, Any]:
    factor_id = str(config.factor_id or '')
    if factor_id != 'alpha004':
        return {
            'signal': 0,
            'rationale': f'未实现的 factor_rank 因子：{factor_id}',
            'strategy_name': config.strategy_name,
            'strategy_type': 'factor',
            'signal_type': 'factor_rank',
            'factor_id': factor_id,
        }
    snapshot, snapshot_date = load_alpha004_snapshot()
    if snapshot.empty or snapshot_date is None:
        return {
            'signal': 0,
            'rationale': 'alpha004 截面因子快照为空，跳过当日 factor_rank 生成。',
            'strategy_name': config.strategy_name,
            'strategy_type': 'factor',
            'signal_type': 'factor_rank',
            'factor_id': factor_id,
        }
    bucket = max(1, int(np.ceil(len(snapshot) * 0.2)))
    top_candidates = snapshot.head(bucket).index.tolist()
    bottom_candidates = snapshot.tail(bucket).index.tolist()
    return {
        'signal': 0,
        'rationale': f'alpha004 截面排名已计算：前20%={bucket}只，后20%={bucket}只；observation 模式下不直接产生实盘信号。',
        'strategy_name': config.strategy_name,
        'strategy_type': 'factor',
        'signal_type': 'factor_rank',
        'factor_id': factor_id,
        'snapshot_date': str(snapshot_date.date()),
        'universe_size': int(len(snapshot)),
        'top_candidates': top_candidates[:10],
        'bottom_candidates': bottom_candidates[:10],
    }

def generate_breakout_signal(data: pd.DataFrame) -> tuple[int, dict[str, Any]]:
    close = data['close'].astype(float)
    latest = data.iloc[-1]
    entry_threshold = float(close.shift(1).rolling(ENTRY_WINDOW).max().iloc[-1])
    exit_threshold = float(close.shift(1).rolling(EXIT_WINDOW).min().iloc[-1])
    last_close = float(latest['close'])
    if last_close > entry_threshold:
        signal = 1
        state = 'entry'
    elif last_close < exit_threshold:
        signal = -1
        state = 'exit'
    else:
        signal = 0
        state = 'hold'
    rationale = (
        f"收盘价={last_close:.4f}，{ENTRY_WINDOW}日高点={entry_threshold:.4f}，"
        f"{EXIT_WINDOW}日低点={exit_threshold:.4f}，breakout_state={state}"
    )
    return signal, {
        'signal': signal,
        'state': state,
        'rationale': rationale,
        'entry_threshold': entry_threshold,
        'exit_threshold': exit_threshold,
        'close': last_close,
    }



def _series_signal_payload(strategy: Any, signal: int, rationale: str) -> dict[str, Any]:
    return {
        'signal': signal,
        'rationale': rationale,
        'strategy_name': strategy.strategy_name,
        'strategy_type': strategy.strategy_type,
    }



def build_strategy_signal_details(data: pd.DataFrame, configs: list[StrategyConfig]) -> dict[str, dict[str, Any]]:
    config_map = {config.strategy_id: config for config in configs}
    details: dict[str, dict[str, Any]] = {}

    breakout_signal, breakout_meta = generate_breakout_signal(data)
    details['strat_breakout_v1'] = {
        **breakout_meta,
        'strategy_name': '价格突破策略（entry25/exit20）',
        'strategy_type': 'trend',
    }

    strategies = [MACrossStrategy(), RSIReversionStrategy(), VolBreakoutStrategy()]
    for strategy in strategies:
        series = strategy.generate_signals(data).reindex(data.index).fillna(0).astype(int)
        signal = int(series.iloc[-1])
        details[strategy.strategy_id] = _series_signal_payload(
            strategy,
            signal,
            f'{strategy.strategy_name} 最新执行信号={signal}（基于收盘后计算，次日开盘执行）',
        )

    details['strat_factor_momentum_v1'] = {
        'signal': 0,
        'rationale': 'Phase 8A 暂不接入截面选股调仓逻辑，先记录占位信号。',
        'strategy_name': '截面动量因子选股',
        'strategy_type': 'factor',
    }

    for config in configs:
        if config.signal_type == 'factor_rank' and config.strategy_id not in details:
            details[config.strategy_id] = generate_factor_rank_signal(config)

    for strategy_id, payload in details.items():
        config = config_map.get(strategy_id)
        payload['is_active'] = bool(config.is_active) if config else False
        payload['max_capital_pct'] = float(config.max_capital_pct) if config else 0.0
        payload['priority'] = int(config.priority) if config else 99
    return details



def enrich_aggregated_trades(aggregated_trades: list[dict[str, Any]], configs: list[StrategyConfig]) -> list[dict[str, Any]]:
    config_map = {config.strategy_id: config for config in configs}
    enriched: list[dict[str, Any]] = []
    for trade in aggregated_trades:
        config = config_map.get(str(trade.get('strategy_id', '')))
        enriched.append({
            **trade,
            'max_capital_pct': float(config.max_capital_pct) if config else 0.0,
        })
    return enriched



def build_factor_gate_outputs(
    configs: list[StrategyConfig],
    gate_allowed: bool,
    current_equity: float,
    as_of_date: pd.Timestamp | None = None,
) -> dict[str, dict[str, Any]]:
    outputs: dict[str, dict[str, Any]] = {}
    for config in configs:
        if config.signal_type != 'factor_rank':
            continue
        factor_id = str(config.factor_id or '')
        if factor_id != 'alpha004':
            continue
        snapshot, snapshot_date = load_alpha004_snapshot(as_of_date=as_of_date)
        if snapshot.empty or snapshot_date is None:
            outputs[config.strategy_id] = {
                'snapshot_date': None,
                'selection_count': 0,
                'position_weights': [],
                'reason': 'empty_factor_snapshot',
            }
            continue
        simulated_config = replace(config, is_active=True)
        gate_trades = aggregate_daily_signals(
            strategy_signals={config.strategy_id: 1},
            strategy_configs=[simulated_config],
            gate_allowed=gate_allowed,
            current_equity=current_equity,
        )
        gate_trade = gate_trades[0] if gate_trades else {
            'strategy_id': config.strategy_id,
            'strategy_name': config.strategy_name,
            'action': 'blocked_entry',
            'requested_capital_pct': float(config.max_capital_pct),
            'approved_capital_pct': 0.0,
            'notional_capital': 0.0,
            'reason': 'no_gate_trade_generated',
        }
        adapter_output = adapt_factor_signal(
            snapshot,
            {
                **gate_trade,
                'top_pct': 0.2,
                'max_single_weight': 0.10,
            },
        )
        outputs[config.strategy_id] = {
            **adapter_output,
            'snapshot_date': str(snapshot_date.date()),
            'status': config.status,
            'is_active': config.is_active,
        }
    return outputs



def find_recent_gate_allowed_date(
    scheduler: GateScheduler,
    etf_df: pd.DataFrame,
    equity_series: list[float],
    lookback_days: int = 90,
) -> pd.Timestamp | None:
    unique_dates = pd.DatetimeIndex(pd.to_datetime(etf_df['trade_date'])).sort_values().unique()
    for trade_date in reversed(unique_dates[-lookback_days:]):
        subset = etf_df.loc[etf_df['trade_date'] <= trade_date].copy()
        if subset.empty:
            continue
        gate_result = scheduler.evaluate(
            date=str(pd.Timestamp(trade_date).date()),
            equity_series=equity_series,
            etf_df=subset,
        )
        if bool(gate_result['allowed']):
            return pd.Timestamp(trade_date).normalize()
    return None



def main() -> None:
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    strategy_configs = load_strategy_configs()
    ensure_strategy_registry_metadata()

    df = load_market_data()
    latest = df.iloc[-1]
    trade_date = pd.Timestamp(latest['trade_date']).normalize()
    run_date = datetime.now().astimezone().isoformat()

    scheduler = GateScheduler()
    equity_series = load_equity_series()
    gate_result = scheduler.evaluate(
        date=str(trade_date.date()),
        equity_series=equity_series,
        etf_df=df,
    )

    strategy_signal_details = build_strategy_signal_details(df, strategy_configs)
    strategy_signals = {strategy_id: int(payload['signal']) for strategy_id, payload in strategy_signal_details.items()}
    aggregated_trades = aggregate_daily_signals(
        strategy_signals=strategy_signals,
        strategy_configs=strategy_configs,
        gate_allowed=bool(gate_result['allowed']),
        current_equity=ACCOUNT_EQUITY,
    )
    aggregated_trades = enrich_aggregated_trades(aggregated_trades, strategy_configs)
    factor_gate_outputs = build_factor_gate_outputs(
        configs=strategy_configs,
        gate_allowed=bool(gate_result['allowed']),
        current_equity=ACCOUNT_EQUITY,
        as_of_date=trade_date,
    )
    factor_gate_demo_outputs: dict[str, dict[str, Any]] = {}
    if not any(payload.get('position_weights') for payload in factor_gate_outputs.values()):
        demo_date = find_recent_gate_allowed_date(scheduler, df, equity_series)
        if demo_date is not None:
            factor_gate_demo_outputs = build_factor_gate_outputs(
                configs=strategy_configs,
                gate_allowed=True,
                current_equity=ACCOUNT_EQUITY,
                as_of_date=demo_date,
            )
            for payload in factor_gate_demo_outputs.values():
                payload['demo_date'] = str(demo_date.date())
    for strategy_id, gate_payload in factor_gate_outputs.items():
        if strategy_id in strategy_signal_details:
            strategy_signal_details[strategy_id]['gate_adapter'] = gate_payload

    payload = {
        'date': str(trade_date.date()),
        'generated_at': run_date,
        'instrument': '510300',
        'account_equity': ACCOUNT_EQUITY,
        'gate_result': gate_result,
        'strategy_signals': strategy_signal_details,
        'aggregated_trades': aggregated_trades,
        'factor_gate_outputs': factor_gate_outputs,
        'factor_gate_demo_outputs': factor_gate_demo_outputs,
    }
    out_path = SIGNAL_DIR / f'{trade_date:%Y%m%d}.json'
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'交易日: {trade_date:%Y-%m-%d}')
    print(f"Gate状态: {'允许' if gate_result['allowed'] else '阻断'}（blocked_by={gate_result['blocked_by']}, reason={gate_result['reason']}）")
    for strategy_id, detail in strategy_signal_details.items():
        print(
            f"{strategy_id}: signal={detail['signal']} active={detail['is_active']} "
            f"max_capital_pct={detail['max_capital_pct']:.2f} rationale={detail['rationale']}"
        )
        gate_adapter = detail.get('gate_adapter')
        if gate_adapter and gate_adapter.get('position_weights'):
            sample = ', '.join(
                f"{item['symbol']}: {item['account_weight'] * 100:.2f}%"
                for item in gate_adapter['position_weights'][:3]
            )
            print(f"{strategy_id} gate_weights: {sample}")
        elif strategy_id in factor_gate_demo_outputs and factor_gate_demo_outputs[strategy_id].get('position_weights'):
            demo_payload = factor_gate_demo_outputs[strategy_id]
            sample = ', '.join(
                f"{item['symbol']}: {item['account_weight'] * 100:.2f}%"
                for item in demo_payload['position_weights'][:3]
            )
            print(f"{strategy_id} gate_weights_demo[{demo_payload['demo_date']}]: {sample}")
    print(f'aggregated_trades={len(aggregated_trades)}')
    print(f'信号文件: {out_path}')
    maybe_generate_monthly_attribution_report(trade_date)


if __name__ == '__main__':
    main()
    try:
        import subprocess

        subprocess.run(
            [str(ROOT / '.venv' / 'Scripts' / 'python.exe'), 'scripts/generate_report.py'],
            cwd=str(ROOT),
            check=True,
        )
        print('报告已更新：runtime/reports/strategy_report.html')
    except Exception as e:
        print(f'报告生成失败（不影响信号）：{e}')



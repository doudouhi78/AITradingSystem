from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import vectorbt as vbt

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

COMMISSION = 0.0003
STAMP_DUTY = 0.001
SLIPPAGE = 0.001
TOP_N = 50
REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'tra_pool_b_backtest.json'
ALPHA004_COST_SHARPE = 0.43
TEST_START = '2023-01-01'
TEST_END = '2024-12-31'


def _load_filtered_module():
    module_path = ROOT / 'scripts' / 'run_qlib_tra_filtered.py'
    spec = importlib.util.spec_from_file_location('tra_filtered_helper', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _resolve_factor_path() -> Path:
    candidates = [
        ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_factor.parquet',
        ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_factor.parquet',
    ]
    best_path: Path | None = None
    best_score: tuple[int, int] = (-1, -1)
    for path in candidates:
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        score = (int(frame.shape[1]), int(frame.shape[0]))
        index = pd.to_datetime(frame.index, errors='coerce') if not isinstance(frame.index, pd.DatetimeIndex) else frame.index
        if index.min() <= pd.Timestamp(TEST_START) and index.max() >= pd.Timestamp(TEST_END) and score > best_score:
            best_path = path
            best_score = score
    if best_path is None:
        alpha_dir = ROOT / 'runtime' / 'alpha_research'
        for path in sorted(alpha_dir.glob('*.parquet')):
            frame = pd.read_parquet(path)
            if frame.shape[1] > best_score[0]:
                best_path = path
                best_score = (int(frame.shape[1]), int(frame.shape[0]))
    if best_path is None:
        raise FileNotFoundError('no TRA factor parquet found under runtime/alpha_research')
    return best_path


def _load_factor_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame.index = pd.to_datetime(frame.index, errors='coerce')
    frame.index.name = 'date'
    frame.columns = [str(col).upper() for col in frame.columns]
    return frame.loc[(frame.index >= pd.Timestamp(TEST_START)) & (frame.index <= pd.Timestamp(TEST_END))].sort_index().sort_index(axis=1)


def _load_price_frame() -> pd.DataFrame:
    prices_path = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_test_prices.parquet'
    frame = pd.read_parquet(prices_path)
    payload = frame[['date', 'instrument', 'close']].copy()
    payload['date'] = pd.to_datetime(payload['date'], errors='coerce')
    payload['instrument'] = payload['instrument'].astype(str).str.upper()
    payload['close'] = pd.to_numeric(payload['close'], errors='coerce')
    wide = payload.pivot_table(index='date', columns='instrument', values='close', aggfunc='last').sort_index()
    wide.columns.name = None
    return wide.loc[(wide.index >= pd.Timestamp(TEST_START)) & (wide.index <= pd.Timestamp(TEST_END))]


def first_trading_days(index: pd.DatetimeIndex) -> list[pd.Timestamp]:
    return list(pd.Series(index, index=index).groupby(index.to_period('M')).min())


def equal_weight_targets(symbols: list[str], columns: list[str]) -> pd.Series:
    targets = pd.Series(0.0, index=columns, dtype=float)
    if not symbols:
        return targets
    weight = 1.0 / len(symbols)
    targets.loc[symbols] = weight
    return targets


def portfolio_metrics(value: pd.Series) -> dict[str, float]:
    value = value.dropna()
    if value.empty or len(value) < 2:
        return {'annual_return': 0.0, 'sharpe': 0.0, 'max_drawdown': 0.0}
    returns = value.pct_change(fill_method=None).dropna()
    years = max((value.index[-1] - value.index[0]).days / 365.25, 1 / 252)
    annual_return = float((value.iloc[-1] / value.iloc[0]) ** (1 / years) - 1)
    std = float(returns.std(ddof=0))
    sharpe = float(np.sqrt(252) * returns.mean() / std) if std else 0.0
    drawdown = value / value.cummax() - 1.0
    return {
        'annual_return': annual_return,
        'sharpe': sharpe,
        'max_drawdown': float(drawdown.min()),
    }


def main() -> int:
    helper = _load_filtered_module()
    factor_path = _resolve_factor_path()
    factor_frame = _load_factor_frame(factor_path)
    price_frame = _load_price_frame()
    factor_frame, price_frame = factor_frame.align(price_frame, join='inner', axis=1)
    factor_frame, price_frame = factor_frame.align(price_frame, join='inner', axis=0)

    codes = [str(code).upper() for code in factor_frame.columns]
    dates = pd.DatetimeIndex(factor_frame.index)
    masks = helper._build_rule_masks(codes, dates)
    eligible_mask = factor_frame.notna() & (~(masks['st'] | masks['new_listing'] | masks['low_liquidity'] | masks['halted']))

    target_weights = pd.DataFrame(np.nan, index=price_frame.index, columns=price_frame.columns, dtype=float)
    fee_rates = pd.DataFrame(0.0, index=price_frame.index, columns=price_frame.columns, dtype=float)
    slippage_rates = pd.DataFrame(0.0, index=price_frame.index, columns=price_frame.columns, dtype=float)
    rebalances: list[dict[str, Any]] = []

    prev_targets = pd.Series(0.0, index=price_frame.columns, dtype=float)
    prev_rebalance: pd.Timestamp | None = None
    for rebalance_date in first_trading_days(price_frame.index):
        scores = factor_frame.loc[rebalance_date].where(eligible_mask.loc[rebalance_date]).dropna()
        if scores.empty:
            continue
        longs = scores.sort_values(ascending=False).head(TOP_N).index.tolist()
        targets = equal_weight_targets(longs, list(price_frame.columns))

        current_weights = prev_targets.copy()
        if prev_rebalance is not None and float(prev_targets.sum()) > 0:
            price_rel = price_frame.loc[rebalance_date].divide(price_frame.loc[prev_rebalance]).replace([np.inf, -np.inf], np.nan).fillna(1.0)
            drifted = prev_targets.multiply(price_rel, fill_value=0.0)
            total = float(drifted.sum())
            current_weights = drifted / total if total > 0 else prev_targets.copy()

        delta = targets - current_weights
        changed = delta.abs() > 1e-10
        buys = delta > 1e-10
        sells = delta < -1e-10

        target_weights.loc[rebalance_date] = targets.to_numpy(dtype=float)
        fee_rates.loc[rebalance_date, buys[buys].index] = COMMISSION
        fee_rates.loc[rebalance_date, sells[sells].index] = COMMISSION + STAMP_DUTY
        slippage_rates.loc[rebalance_date, changed[changed].index] = SLIPPAGE

        turnover = float(0.5 * delta.abs().sum())
        rebalances.append({
            'rebalance_date': rebalance_date.strftime('%Y-%m-%d'),
            'holding_count': int(len(longs)),
            'turnover': turnover,
        })
        prev_targets = targets
        prev_rebalance = rebalance_date

    pf = vbt.Portfolio.from_orders(
        close=price_frame,
        size=target_weights,
        size_type='targetpercent',
        direction='longonly',
        cash_sharing=True,
        init_cash=1.0,
        fees=fee_rates,
        slippage=slippage_rates,
        freq='1D',
    )

    metrics = portfolio_metrics(pf.value())
    avg_turnover = float(np.mean([item['turnover'] for item in rebalances])) if rebalances else 0.0
    sharpe = float(metrics['sharpe'])
    conclusion = '可用' if sharpe > ALPHA004_COST_SHARPE else ('需调整' if sharpe > 0 else '不可用')

    report = {
        'test_period': {'start': TEST_START, 'end': TEST_END},
        'factor_path': str(factor_path),
        'strategy': {
            'pool': 'Pool B',
            'filters': ['st', 'new_listing', 'low_liquidity', 'halted'],
            'holding': f'Top{TOP_N} equal_weight',
            'rebalance': 'monthly_first_trading_day',
            'costs': {
                'commission_buy_sell': COMMISSION,
                'stamp_duty_sell': STAMP_DUTY,
                'slippage_buy_sell': SLIPPAGE,
            },
        },
        'out_of_sample': {
            'sharpe_after_cost': sharpe,
            'annual_return': float(metrics['annual_return']),
            'max_drawdown': float(metrics['max_drawdown']),
            'avg_monthly_turnover': avg_turnover,
            'alpha004_cost_sharpe': ALPHA004_COST_SHARPE,
        },
        'rebalances': rebalances,
        'conclusion': conclusion,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

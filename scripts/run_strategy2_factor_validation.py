from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from strategy2.factors import (
    calc_bias,
    calc_ema_slope,
    calc_sector_concentration,
    calc_sector_rps_approx,
    calc_stock_rps,
    calc_turnover_deviation,
    calc_volume_zscore,
)

START = '2018-01-01'
END = '2024-12-31'
BASE_PERIOD = 20
RPS_WINDOWS = [5, 10, 20, 40, 60]
MARKET_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
FUNDAMENTAL_DIR = ROOT / 'runtime' / 'fundamental_data'
REPORT_PATH = ROOT / 'runtime' / 'strategy2' / 'factor_validation_report.md'


def _load_eval_module():
    module_path = ROOT / 'scripts' / 'alpha' / 'run_factor_evaluation.py'
    spec = importlib.util.spec_from_file_location('strategy2_factor_eval', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _symbol_to_ts(symbol: str) -> str:
    text = str(symbol).strip().zfill(6)
    if text.startswith(('600', '601', '603', '605', '688', '689', '900')):
        return f'{text}.SH'
    if text.startswith(('8', '4')):
        return f'{text}.BJ'
    return f'{text}.SZ'


def load_market_data(start: str, end: str) -> pd.DataFrame:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    rows: list[pd.DataFrame] = []
    files = sorted(MARKET_DIR.glob('*.parquet'))
    for idx, path in enumerate(files, start=1):
        frame = pd.read_parquet(path, columns=['trade_date', 'symbol', 'close', 'volume']).copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
        frame = frame.loc[(frame['trade_date'] >= start_ts) & (frame['trade_date'] <= end_ts)]
        if frame.empty:
            continue
        frame['ts_code'] = frame['symbol'].astype(str).str.zfill(6).map(_symbol_to_ts)
        rows.append(frame[['ts_code', 'trade_date', 'close', 'volume']])
        if idx % 500 == 0:
            print(f'market_progress={idx}/{len(files)}', flush=True)
    if not rows:
        raise FileNotFoundError('no market rows loaded')
    market = pd.concat(rows, ignore_index=True)
    return market.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def _factor_to_wide(frame: pd.DataFrame, value_col: str) -> pd.DataFrame:
    wide = frame.pivot_table(index='trade_date', columns='ts_code', values=value_col, aggfunc='last')
    return wide.sort_index().sort_index(axis=1)


def _mean_cross_section_corr(left: pd.DataFrame, right: pd.DataFrame) -> float:
    aligned_left, aligned_right = left.align(right, join='inner', axis=1)
    values: list[float] = []
    for date in aligned_left.index.intersection(aligned_right.index):
        cross = pd.concat([
            aligned_left.loc[date].rename('left'),
            aligned_right.loc[date].rename('right'),
        ], axis=1).dropna()
        if len(cross) < 20:
            continue
        corr = cross['left'].corr(cross['right'], method='spearman')
        if pd.notna(corr):
            values.append(float(corr))
    return float(np.mean(values)) if values else 0.0


def _metric_row(name: str, metrics: dict[str, float], pass_rule: bool) -> dict[str, Any]:
    return {
        'factor': name,
        'ic_mean': float(metrics['rank_ic_mean']),
        'icir': float(metrics['icir']),
        'passed': bool(pass_rule),
    }


def _compute_metrics(eval_module, factor_frame: pd.DataFrame, prices: pd.DataFrame, period: int) -> dict[str, Any]:
    aligned_factor, aligned_prices = factor_frame.align(prices, join='inner', axis=1)
    aligned_factor, aligned_prices = aligned_factor.align(aligned_prices, join='inner', axis=0)
    ic_series = eval_module.compute_daily_ic_series(aligned_factor, aligned_prices, period=period)
    metrics = eval_module.compute_basic_metrics(ic_series)
    return {
        'ic_mean': float(metrics['rank_ic_mean']),
        'icir': float(metrics['icir']),
        'sample_count': int(len(ic_series)),
    }


def main() -> int:
    eval_module = _load_eval_module()
    stock_basic = pd.read_parquet(FUNDAMENTAL_DIR / 'stock_basic.parquet')[['ts_code', 'industry']].copy()
    stock_basic['ts_code'] = stock_basic['ts_code'].astype(str).str.upper()
    valuation = pd.read_parquet(FUNDAMENTAL_DIR / 'valuation_daily.parquet').copy()
    valuation['date'] = pd.to_datetime(valuation['date'], errors='coerce')
    valuation = valuation.loc[(valuation['date'] >= pd.Timestamp(START)) & (valuation['date'] <= pd.Timestamp(END))].copy()

    market = load_market_data(START, END)
    prices = market.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last').sort_index().sort_index(axis=1)

    stock_rps = calc_stock_rps(market)
    sector_rps = calc_sector_rps_approx(market, stock_basic)
    sector_concentration = calc_sector_concentration(market, stock_basic)
    volume_zscore = calc_volume_zscore(market)
    turnover_deviation = calc_turnover_deviation(valuation)
    ema_slope = calc_ema_slope(market)
    bias = calc_bias(market)

    sector_factor = sector_rps.copy()
    sector_factor['sector_rps_approx'] = sector_factor[['sector_rps_20', 'sector_rps_60', 'sector_rps_120']].mean(axis=1)
    sector_stock = stock_basic[['ts_code', 'industry']].drop_duplicates('ts_code').merge(
        sector_factor[['trade_date', 'industry', 'sector_rps_approx']],
        on='industry',
        how='left',
    )

    factor_frames = {
        'rps_20': _factor_to_wide(stock_rps, 'rps_20'),
        'rps_60': _factor_to_wide(stock_rps, 'rps_60'),
        'rps_120': _factor_to_wide(stock_rps, 'rps_120'),
        'sector_rps_approx': _factor_to_wide(sector_stock, 'sector_rps_approx'),
        'volume_zscore': _factor_to_wide(volume_zscore, 'volume_zscore'),
        'turnover_deviation': _factor_to_wide(turnover_deviation, 'turnover_deviation'),
        'ema_slope': _factor_to_wide(ema_slope, 'ema_slope'),
        'bias': _factor_to_wide(bias, 'bias'),
    }

    results: list[dict[str, Any]] = []
    for name in ('rps_20', 'rps_60', 'rps_120', 'sector_rps_approx', 'volume_zscore', 'turnover_deviation'):
        metrics = _compute_metrics(eval_module, factor_frames[name], prices, BASE_PERIOD)
        if name.startswith('rps_'):
            passed = metrics['icir'] > 0.3
        elif name == 'sector_rps_approx':
            stock_composite = factor_frames['rps_20'].add(factor_frames['rps_60'], fill_value=0).add(factor_frames['rps_120'], fill_value=0) / 3.0
            sector_corr = _mean_cross_section_corr(factor_frames[name], stock_composite)
            passed = sector_corr > 0.4
            metrics['sector_cross_corr'] = sector_corr
        elif name == 'volume_zscore':
            passed = metrics['ic_mean'] > 0.02
        else:
            passed = metrics['icir'] > 0.0
        row = _metric_row(name, {'rank_ic_mean': metrics['ic_mean'], 'icir': metrics['icir']}, passed)
        if 'sector_cross_corr' in metrics:
            row['sector_cross_corr'] = metrics['sector_cross_corr']
        results.append(row)

    rps_multi: dict[str, dict[int, dict[str, float]]] = {}
    best_choice: dict[str, Any] | None = None
    for name in ('rps_20', 'rps_60', 'rps_120'):
        rps_multi[name] = {}
        for window in RPS_WINDOWS:
            metrics = _compute_metrics(eval_module, factor_frames[name], prices, window)
            rps_multi[name][window] = metrics
            score = abs(metrics['icir'])
            if best_choice is None or score > best_choice['score']:
                best_choice = {
                    'factor': name,
                    'window': window,
                    'ic_mean': metrics['ic_mean'],
                    'icir': metrics['icir'],
                    'score': score,
                }

    sector_corr_value = next((row.get('sector_cross_corr') for row in results if row['factor'] == 'sector_rps_approx'), None)
    report_lines = [
        '# Sprint 54A Factor Validation',
        '',
        f'- Sample: {START} ~ {END}',
        f'- Universe rows: {len(market):,}',
        f'- Stock count: {market["ts_code"].nunique():,}',
        f'- Sector concentration rows: {len(sector_concentration):,}',
        '',
        '## Base 20D Validation',
        '',
        '| 因子 | IC均值 | ICIR | 达标 |',
        '|------|-------|------|------|',
    ]
    for row in results:
        report_lines.append(f"| {row['factor']} | {row['ic_mean']:.4f} | {row['icir']:.4f} | {'✅' if row['passed'] else '❌'} |")

    report_lines += [
        '',
        '## RPS Multi-Window Analysis',
        '',
        '| 因子 | 5日 | 10日 | 20日 | 40日 | 60日 | 最佳窗口 |',
        '|------|-----|------|------|------|------|---------|',
    ]
    for name in ('rps_20', 'rps_60', 'rps_120'):
        cells = []
        best_window = max(rps_multi[name].items(), key=lambda item: abs(item[1]['icir']))
        for window in RPS_WINDOWS:
            metrics = rps_multi[name][window]
            cells.append(f"IC={metrics['ic_mean']:.4f}, ICIR={metrics['icir']:.4f}")
        best_label = f"{best_window[0]}日 ({'正向' if best_window[1]['icir'] >= 0 else '反向'})"
        report_lines.append(f"| {name} | " + ' | '.join(cells) + f" | {best_label} |")

    if best_choice is None:
        best_summary = {'window': 'N/A', 'direction': 'N/A'}
    else:
        best_summary = {
            'window': f"{best_choice['window']}日",
            'direction': '正向' if best_choice['icir'] >= 0 else '反向',
            'factor': best_choice['factor'],
            'ic_mean': best_choice['ic_mean'],
            'icir': best_choice['icir'],
        }

    report_lines += [
        '',
        '## 结论',
        f"- RPS因子有效信号周期: {best_summary['window']}",
        f"- 信号方向: {best_summary['direction']}",
        f"- 最佳组合: {best_summary.get('factor', 'N/A')} / IC={best_summary.get('ic_mean', 0.0):.4f} / ICIR={best_summary.get('icir', 0.0):.4f}" if best_choice is not None else '- 最佳组合: N/A',
        f"- 策略二建议持仓周期: {best_summary['window']}",
        '',
        '## 补充验证',
        f'- sector_rps_approx 与 stock_rps composite 截面相关性: {sector_corr_value:.4f}' if sector_corr_value is not None else '- sector correlation: N/A',
        f"- extra factors computed: ema_slope rows={len(ema_slope):,}, bias rows={len(bias):,}",
        '',
        '## JSON Summary',
        '```json',
        json.dumps({'results': results, 'rps_multi_window': rps_multi, 'best_summary': best_summary}, ensure_ascii=False, indent=2),
        '```',
    ]
    REPORT_PATH.write_text('\n'.join(report_lines) + '\n', encoding='utf-8')
    print(json.dumps({'report_path': str(REPORT_PATH), 'rps_multi_window': rps_multi, 'best_summary': best_summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

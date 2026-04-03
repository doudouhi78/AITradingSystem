from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from strategy2.factors import calc_sector_rps, calc_stock_rps, map_stock_to_sector_history

START = '2018-01-01'
END = '2024-12-31'
WINDOWS = [5, 20, 60]
MARKET_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
INDEX_DIR = ROOT / 'runtime' / 'index_data'
REPORT_PATH = ROOT / 'runtime' / 'strategy2' / 'factor_validation_report.md'
APPROX_CORR = 0.364


def _load_eval_module():
    module_path = ROOT / 'scripts' / 'alpha' / 'run_factor_evaluation.py'
    spec = importlib.util.spec_from_file_location('strategy2_factor_eval_v2', module_path)
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
        frame = pd.read_parquet(path, columns=['trade_date', 'symbol', 'close']).copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
        frame = frame.loc[(frame['trade_date'] >= start_ts) & (frame['trade_date'] <= end_ts)]
        if frame.empty:
            continue
        frame['ts_code'] = frame['symbol'].astype(str).str.zfill(6).map(_symbol_to_ts)
        rows.append(frame[['ts_code', 'trade_date', 'close']])
        if idx % 500 == 0:
            print(f'market_progress={idx}/{len(files)}', flush=True)
    return pd.concat(rows, ignore_index=True).sort_values(['trade_date', 'ts_code']).reset_index(drop=True)


def _compute_metrics(eval_module, factor_frame: pd.DataFrame, prices: pd.DataFrame, period: int) -> dict[str, float]:
    aligned_factor, aligned_prices = factor_frame.align(prices, join='inner', axis=1)
    aligned_factor, aligned_prices = aligned_factor.align(aligned_prices, join='inner', axis=0)
    ic_series = eval_module.compute_daily_ic_series(aligned_factor, aligned_prices, period=period)
    metrics = eval_module.compute_basic_metrics(ic_series)
    return {
        'ic_mean': float(metrics['rank_ic_mean']),
        'icir': float(metrics['icir']),
        'sample_count': int(len(ic_series)),
    }


def _factor_to_wide(frame: pd.DataFrame, entity_col: str, value_col: str) -> pd.DataFrame:
    wide = frame.pivot_table(index='trade_date', columns=entity_col, values=value_col, aggfunc='last')
    return wide.sort_index().sort_index(axis=1)


def main() -> int:
    eval_module = _load_eval_module()
    index_daily = pd.read_parquet(INDEX_DIR / 'sw_industry_index_daily.parquet').copy()
    index_daily['trade_date'] = pd.to_datetime(index_daily['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    index_daily = index_daily.loc[(index_daily['trade_date'] >= pd.Timestamp(START)) & (index_daily['trade_date'] <= pd.Timestamp(END))].copy()
    member_history = pd.read_parquet(INDEX_DIR / 'sw_industry_member_history.parquet').copy()

    sector_rps = calc_sector_rps(index_daily)
    sector_prices = _factor_to_wide(index_daily.rename(columns={'ts_code': 'index_code'}), 'index_code', 'close')

    sector_results: dict[str, dict[int, dict[str, float]]] = {}
    best_choice: dict[str, Any] | None = None
    for factor_name in ('sector_rps_20', 'sector_rps_60', 'sector_rps_120'):
        frame = _factor_to_wide(sector_rps, 'index_code', factor_name)
        sector_results[factor_name] = {}
        for window in WINDOWS:
            metrics = _compute_metrics(eval_module, frame, sector_prices, window)
            sector_results[factor_name][window] = metrics
            score = abs(metrics['icir'])
            if best_choice is None or score > best_choice['score']:
                best_choice = {
                    'factor': factor_name,
                    'window': window,
                    'ic_mean': metrics['ic_mean'],
                    'icir': metrics['icir'],
                    'score': score,
                }

    market = load_market_data(START, END)
    stock_prices = _factor_to_wide(market, 'ts_code', 'close')
    stock_rps = calc_stock_rps(market)
    stock_rps_60 = _factor_to_wide(stock_rps, 'ts_code', 'rps_60')

    membership = map_stock_to_sector_history(member_history, stock_rps_60.index, list(stock_rps_60.columns))
    strong_sector_map = sector_rps[['trade_date', 'index_code', 'sector_rps_60']].copy()
    membership = membership.merge(strong_sector_map, on=['trade_date', 'index_code'], how='left')
    membership['is_strong_sector'] = membership['sector_rps_60'] >= 75.0
    strong_membership = membership.loc[membership['is_strong_sector']].copy()
    if strong_membership.empty:
        strong_stock_factor = stock_rps_60 * pd.NA
    else:
        strong_mask = strong_membership.assign(flag=True).pivot_table(index='trade_date', columns='ts_code', values='flag', aggfunc='last')
        strong_mask = strong_mask.reindex(index=stock_rps_60.index, columns=stock_rps_60.columns)
        strong_mask = strong_mask.where(strong_mask.notna(), False).astype(bool)
        strong_stock_factor = stock_rps_60.where(strong_mask)

    compare_window = int(best_choice['window']) if best_choice is not None else 20
    full_stock_metrics = _compute_metrics(eval_module, stock_rps_60, stock_prices, compare_window)
    strong_stock_metrics = _compute_metrics(eval_module, strong_stock_factor, stock_prices, compare_window)

    best_icir = float(best_choice['icir']) if best_choice is not None else 0.0
    best_window = int(best_choice['window']) if best_choice is not None else 20
    direction = '正向' if best_icir >= 0 else '反向'

    lines = [
        '# Sprint 54A-v2 Factor Validation',
        '',
        f'- Sample: {START} ~ {END}',
        f'- Industry index rows: {len(index_daily):,}',
        f'- Member history rows: {len(member_history):,}',
        '',
        '## 精确版行业RPS多窗口IC验证',
        '',
        '| 因子 | 5日ICIR | 20日ICIR | 60日ICIR | 最佳窗口 | 方向 |',
        '|------|--------|---------|---------|---------|------|',
    ]
    for factor_name in ('sector_rps_20', 'sector_rps_60', 'sector_rps_120'):
        m5 = sector_results[factor_name][5]['icir']
        m20 = sector_results[factor_name][20]['icir']
        m60 = sector_results[factor_name][60]['icir']
        best_local = max(sector_results[factor_name].items(), key=lambda item: abs(item[1]['icir']))
        local_direction = '正向' if best_local[1]['icir'] >= 0 else '反向'
        lines.append(f'| {factor_name} | {m5:.4f} | {m20:.4f} | {m60:.4f} | {best_local[0]}日 | {local_direction} |')

    lines.extend([
        '',
        '## 与近似版对比',
        f'- 近似版sector_rps截面相关性：{APPROX_CORR:.3f}',
        f'- 精确版最佳ICIR：{best_icir:.4f}',
        '',
        '## 个股层面增强验证',
        f'- 全市场 rps_60 @ {compare_window}日 ICIR：{full_stock_metrics["icir"]:.4f}',
        f'- 强行业内 rps_60 @ {compare_window}日 ICIR：{strong_stock_metrics["icir"]:.4f}',
        '',
        '## 结论',
        f'- 行业RPS信号方向：{direction}',
        f'- 建议策略二使用的行业信号周期：{best_window}日',
        '',
        '## JSON Summary',
        '```json',
        json.dumps({
            'sector_results': sector_results,
            'best_choice': best_choice,
            'approx_corr': APPROX_CORR,
            'full_stock_metrics': full_stock_metrics,
            'strong_stock_metrics': strong_stock_metrics,
        }, ensure_ascii=False, indent=2),
        '```',
    ])
    REPORT_PATH.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps({
        'report_path': str(REPORT_PATH),
        'sector_results': sector_results,
        'best_choice': best_choice,
        'full_stock_metrics': full_stock_metrics,
        'strong_stock_metrics': strong_stock_metrics,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

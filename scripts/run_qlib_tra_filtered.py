from __future__ import annotations

import importlib.util
import json
import pickle
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.qlib_factor_extractor import extract_factor_scores
from alpha_research.stock_filter import StockFilter

MODEL_PATH = ROOT / 'runtime' / 'models' / 'qlib_tra_tushare_v1.pkl'
CONFIG_PATH = ROOT / 'src' / 'alpha_research' / 'qlib_model_configs' / 'tra_config_csi300.yaml'
FACTOR_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_factor.parquet'
PRICES_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_test_prices.parquet'
BASE_REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_ic_report.json'
OUTPUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_filtered_ic_report.json'
FUNDAMENTAL_DIR = ROOT / 'runtime' / 'fundamental_data'
MARKET_DATA_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
POOL_A_AVG_STOCKS = 5735
MIN_LIST_DAYS = 252
MIN_AVG_AMOUNT = 5e7


def _load_eval_module():
    module_path = ROOT / 'scripts' / 'alpha' / 'run_factor_evaluation.py'
    spec = importlib.util.spec_from_file_location('factor_eval_for_filtered', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_model_payload() -> dict[str, Any]:
    with MODEL_PATH.open('rb') as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, dict) or 'predictions' not in payload:
        raise TypeError(f'unexpected model payload: {type(payload)!r}')
    return payload


def _load_factor_frame() -> pd.DataFrame:
    _load_model_payload()
    frame = extract_factor_scores(str(MODEL_PATH), str(CONFIG_PATH), str(FACTOR_PATH))
    frame.index = pd.to_datetime(frame.index, errors='coerce')
    frame.index.name = 'date'
    frame.columns = [str(col).upper() for col in frame.columns]
    return frame.sort_index().sort_index(axis=1)


def _load_price_frame() -> pd.DataFrame:
    frame = pd.read_parquet(PRICES_PATH)
    payload = frame[['date', 'instrument', 'close']].copy()
    payload['date'] = pd.to_datetime(payload['date'], errors='coerce')
    payload['instrument'] = payload['instrument'].astype(str).str.upper()
    payload['close'] = pd.to_numeric(payload['close'], errors='coerce')
    wide = payload.pivot_table(index='date', columns='instrument', values='close', aggfunc='last').sort_index()
    wide.columns.name = None
    return wide


def _pool_a_icir() -> float:
    report = json.loads(BASE_REPORT_PATH.read_text(encoding='utf-8'))
    return float(report['basic_metrics']['icir'])


def _conclusion(pool_a: float, pool_b: float) -> str:
    delta = pool_b - pool_a
    if delta > 0.02:
        return '过滤后信号更强'
    if delta < -0.02:
        return '过滤后信号更弱'
    return '过滤后信号基本持平'


def _load_open_calendar() -> pd.DatetimeIndex:
    trade_cal = pd.read_parquet(FUNDAMENTAL_DIR / 'trade_cal.parquet')
    trade_cal = trade_cal.loc[trade_cal['is_open'] == 1].copy()
    dates = pd.to_datetime(trade_cal['cal_date'], errors='coerce').dropna().sort_values().unique()
    return pd.DatetimeIndex(dates)


def _build_st_codes(stock_filter: StockFilter, codes: list[str]) -> set[str]:
    stock_basic = stock_filter._load_stock_basic()
    if stock_basic.empty or 'name' not in stock_basic.columns:
        return set()
    stock_basic = stock_basic.copy()
    stock_basic['ts_code'] = stock_basic['ts_code'].astype(str).str.upper()
    mask = stock_basic['name'].astype(str).str.contains('ST', case=False, na=False)
    return set(stock_basic.loc[mask & stock_basic['ts_code'].isin(codes), 'ts_code'])


def _build_rule_masks(codes: list[str], dates: pd.DatetimeIndex) -> dict[str, pd.DataFrame]:
    stock_filter = StockFilter(data_dir=FUNDAMENTAL_DIR, market_data_dir=MARKET_DATA_DIR)
    open_calendar = _load_open_calendar()
    calendar_index = {ts.normalize(): idx for idx, ts in enumerate(open_calendar)}
    st_codes = _build_st_codes(stock_filter, codes)

    st_mask = pd.DataFrame(False, index=dates, columns=codes)
    if st_codes:
        st_mask.loc[:, sorted(st_codes)] = True

    new_listing_mask = pd.DataFrame(False, index=dates, columns=codes)
    low_liquidity_mask = pd.DataFrame(False, index=dates, columns=codes)
    halted_mask = pd.DataFrame(False, index=dates, columns=codes)

    for idx, code in enumerate(codes, start=1):
        symbol = code.split('.', 1)[0]
        path = MARKET_DATA_DIR / f'{symbol}.parquet'
        if not path.exists():
            continue
        daily = pd.read_parquet(path, columns=['trade_date', 'volume', 'amount']).copy()
        if daily.empty:
            continue
        daily['trade_date'] = pd.to_datetime(daily['trade_date'], errors='coerce')
        daily = daily.dropna(subset=['trade_date']).sort_values('trade_date').drop_duplicates('trade_date', keep='last')
        if daily.empty:
            continue

        first_trade = pd.Timestamp(daily['trade_date'].iloc[0]).normalize()
        first_idx = calendar_index.get(first_trade)
        if first_idx is None:
            valid_first = open_calendar[open_calendar >= first_trade]
            first_idx = calendar_index.get(valid_first[0].normalize()) if len(valid_first) else None
        if first_idx is not None:
            eligible_idx = first_idx + MIN_LIST_DAYS - 1
            if eligible_idx < len(open_calendar):
                eligible_date = open_calendar[eligible_idx].normalize()
                new_listing_mask.loc[dates < eligible_date, code] = True
            else:
                new_listing_mask.loc[:, code] = True

        daily['amount'] = pd.to_numeric(daily['amount'], errors='coerce')
        daily['volume'] = pd.to_numeric(daily['volume'], errors='coerce')
        daily['low_liquidity'] = daily['amount'].rolling(20, min_periods=1).mean() < MIN_AVG_AMOUNT
        daily['halted'] = (daily['volume'] == 0) | (daily['amount'] == 0)
        daily = daily.set_index('trade_date').reindex(dates)
        low_liquidity_values = daily['low_liquidity'].where(daily['low_liquidity'].notna(), False).astype(bool).to_numpy()
        halted_values = daily['halted'].where(daily['halted'].notna(), False).astype(bool).to_numpy()
        low_liquidity_mask.loc[:, code] = low_liquidity_values
        halted_mask.loc[:, code] = halted_values

        if idx % 500 == 0:
            print(f'prep_progress={idx}/{len(codes)}', flush=True)

    return {
        'st': st_mask,
        'new_listing': new_listing_mask,
        'low_liquidity': low_liquidity_mask,
        'halted': halted_mask,
    }


def main() -> int:
    eval_module = _load_eval_module()
    factor_frame = _load_factor_frame()
    price_frame = _load_price_frame()
    aligned_factor, aligned_prices = factor_frame.align(price_frame, join='inner', axis=1)
    aligned_factor, aligned_prices = aligned_factor.align(aligned_prices, join='inner', axis=0)

    codes = [str(code).upper() for code in aligned_factor.columns]
    dates = pd.DatetimeIndex(aligned_factor.index)
    masks = _build_rule_masks(codes, dates)

    available_mask = aligned_factor.notna()
    combined_exclusion = masks['st'] | masks['new_listing'] | masks['low_liquidity'] | masks['halted']
    selected_mask = available_mask & (~combined_exclusion)
    filtered_factor = aligned_factor.where(selected_mask)

    ic_series = eval_module.compute_daily_ic_series(filtered_factor, aligned_prices, period=20)
    metrics = eval_module.compute_basic_metrics(ic_series)

    available_counts = available_mask.sum(axis=1).replace(0, pd.NA)
    pool_b_avg = float(selected_mask.sum(axis=1).mean()) if len(selected_mask.index) else 0.0
    filter_stats = {}
    for rule in ('st', 'new_listing', 'low_liquidity', 'halted'):
        removed = (available_mask & masks[rule]).sum(axis=1)
        ratio = (removed / available_counts).fillna(0.0)
        filter_stats[f'{rule}_removed_pct'] = round(float(ratio.mean() * 100.0), 2)

    payload = {
        'pool_a_icir': 0.7667,
        'pool_b_icir': float(metrics['icir']),
        'pool_a_avg_stocks': POOL_A_AVG_STOCKS,
        'pool_b_avg_stocks': round(pool_b_avg, 2),
        'filter_stats': filter_stats,
        'pool_b_ic_mean': float(metrics['rank_ic_mean']),
        'pool_b_ic_positive_pct': float(metrics['ic_positive_pct']),
        'pool_b_sample_count': int(len(ic_series)),
        'conclusion': _conclusion(_pool_a_icir(), float(metrics['icir'])),
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

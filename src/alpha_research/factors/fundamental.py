from __future__ import annotations

from pathlib import Path
from typing import Any

import akshare as ak
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PRIMARY_ROOT = Path(r'D:\AITradingSystem')
INPUT_ROOT = PRIMARY_ROOT if (PRIMARY_ROOT / 'runtime' / 'fundamental_data').exists() else ROOT
VALUATION_FILE = INPUT_ROOT / 'runtime' / 'fundamental_data' / 'valuation_daily.parquet'
FINANCIAL_DIR = INPUT_ROOT / 'runtime' / 'fundamental_data' / 'financial_quarterly'


def _stack_factor(frame: pd.DataFrame, name: str) -> pd.Series:
    factor = frame.stack(future_stack=True)
    factor.index.names = ['date', 'asset']
    factor.name = name
    return factor.replace([np.inf, -np.inf], np.nan).dropna()


def _cross_sectional_rank(frame: pd.DataFrame, *, ascending: bool) -> pd.DataFrame:
    ranked = frame.rank(axis=1, pct=True, ascending=ascending)
    return ranked.shift(1)


def _load_valuation_frame(instruments: list[str], start: str, end: str, column: str) -> pd.DataFrame:
    if not VALUATION_FILE.exists():
        return pd.DataFrame()
    instruments = [str(code).zfill(6) for code in instruments]
    df = pd.read_parquet(VALUATION_FILE, columns=['date', 'instrument_code', column]).copy()
    df['date'] = pd.to_datetime(df['date'])
    df['instrument_code'] = df['instrument_code'].astype(str).str.zfill(6)
    mask = (
        (df['date'] >= pd.Timestamp(start))
        & (df['date'] <= pd.Timestamp(end))
        & (df['instrument_code'].isin(instruments))
    )
    sample = df.loc[mask].copy()
    if sample.empty:
        return pd.DataFrame()
    sample[column] = pd.to_numeric(sample[column], errors='coerce')
    frame = sample.pivot(index='date', columns='instrument_code', values=column).sort_index()
    frame.columns.name = None
    return frame.reindex(columns=instruments)


def _get_latest_financial_local(instrument: str, signal_date: str) -> dict[str, Any]:
    path = FINANCIAL_DIR / f'{instrument}.parquet'
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    df['announce_date'] = pd.to_datetime(df['announce_date'], errors='coerce')
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
    cutoff = pd.Timestamp(signal_date)
    eligible = df[df['announce_date'] <= cutoff].sort_values(['announce_date', 'report_date'])
    if eligible.empty:
        return {}
    row = eligible.iloc[-1].to_dict()
    for key in ['report_date', 'announce_date']:
        if pd.notna(row.get(key)):
            row[key] = pd.Timestamp(row[key]).strftime('%Y-%m-%d')
    return row


def probe_pb_interfaces() -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    try:
        df = ak.stock_a_all_pb()
        probes.append({
            'interface': 'stock_a_all_pb',
            'status': 'market_level_only',
            'columns': list(df.columns),
            'note': '全市场PB，不是个股日频PB，不能直接用于个股横截面因子。',
        })
    except Exception as e:
        probes.append({'interface': 'stock_a_all_pb', 'status': 'error', 'note': repr(e)})

    try:
        df = ak.stock_financial_analysis_indicator('000001', start_year='2020')
        probes.append({
            'interface': 'stock_financial_analysis_indicator',
            'status': 'quarterly_available',
            'columns': list(df.columns),
            'note': '季度财务指标可用，可用每股净资产近似构造PB，但不含公告延迟处理。',
        })
    except Exception as e:
        probes.append({'interface': 'stock_financial_analysis_indicator', 'status': 'error', 'note': repr(e)})

    try:
        df = ak.stock_financial_analysis_indicator_em(symbol='SZ000001', indicator='按报告期')
        probes.append({
            'interface': 'stock_financial_analysis_indicator_em',
            'status': 'available',
            'columns': list(df.columns),
            'note': '接口可用，可用于后续替代新浪源。',
        })
    except Exception as e:
        probes.append({'interface': 'stock_financial_analysis_indicator_em', 'status': 'error', 'note': repr(e)})
    return probes


def factor_pb_ratio_approx(instruments: list[str], start: str, end: str) -> pd.Series:
    """
    ⚠️ KNOWN ISSUE: No announcement lag handling.
    Quarterly values are forward-filled but publication delay (~20 days) is not modeled.
    This factor must NOT be used in any IC evaluation or WFO until fixed.
    Status: excluded from run_ic_batch.py
    """
    dfs = []
    for symbol in instruments:
        try:
            fin = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start[:4])
            if fin.empty:
                continue
            nav_col = None
            for candidate in ['每股净资产_调整后(元)', '每股净资产_调整前(元)', '调整后的每股净资产(元)']:
                if candidate in fin.columns:
                    nav_col = candidate
                    break
            if nav_col is None or '日期' not in fin.columns:
                continue
            temp = fin[['日期', nav_col]].copy()
            temp = temp.rename(columns={'日期': 'date', nav_col: 'book_value_per_share'})
            temp['date'] = pd.to_datetime(temp['date'], errors='coerce')
            temp['book_value_per_share'] = pd.to_numeric(temp['book_value_per_share'], errors='coerce')
            temp = temp.dropna().sort_values('date')
            if temp.empty:
                continue
            temp['asset'] = symbol
            dfs.append(temp)
        except Exception:
            continue

    if not dfs:
        return pd.Series([], dtype=float, name='pb_ratio')

    quarterly = pd.concat(dfs, ignore_index=True).sort_values(['asset', 'date'])
    frames = []
    for symbol in instruments:
        asset_quarterly = quarterly[quarterly['asset'] == symbol].copy()
        if asset_quarterly.empty:
            continue
        dates = pd.date_range(start=start, end=end, freq='B')
        daily = pd.DataFrame({'date': dates})
        merged = pd.merge_asof(daily.sort_values('date'), asset_quarterly[['date', 'book_value_per_share']].sort_values('date'), on='date', direction='backward')
        merged['asset'] = symbol
        frames.append(merged)

    if not frames:
        return pd.Series([], dtype=float, name='pb_ratio')
    result = pd.concat(frames, ignore_index=True).dropna(subset=['book_value_per_share'])
    result['factor_value'] = (1.0 / result['book_value_per_share'].astype(float)).shift(1)
    result = result.dropna(subset=['factor_value']).set_index(['date', 'asset'])['factor_value']
    result.index.names = ['date', 'asset']
    result.name = 'pb_ratio'
    return result


def factor_pb_ratio(instruments: list[str], start: str, end: str) -> pd.Series:
    frame = _load_valuation_frame(instruments, start, end, 'pb')
    if frame.empty:
        return pd.Series(dtype=float, name='pb_ratio')
    frame = frame.where(frame > 0)
    ranked = _cross_sectional_rank(frame, ascending=False)
    return _stack_factor(ranked, 'pb_ratio')


def factor_pe_ttm(instruments: list[str], start: str, end: str) -> pd.Series:
    frame = _load_valuation_frame(instruments, start, end, 'pe_ttm')
    if frame.empty:
        return pd.Series(dtype=float, name='pe_ttm')
    frame = frame.where(frame > 0)
    ranked = _cross_sectional_rank(frame, ascending=False)
    return _stack_factor(ranked, 'pe_ttm')


def factor_roe_ttm(instruments: list[str], start: str, end: str) -> pd.Series:
    business_days = pd.date_range(start=start, end=end, freq='B')
    instruments = [str(code).zfill(6) for code in instruments]
    result = pd.DataFrame(index=business_days, columns=instruments, dtype=float)
    for instrument in instruments:
        if not (FINANCIAL_DIR / f'{instrument}.parquet').exists():
            continue
        values = []
        for trade_date in business_days:
            latest = _get_latest_financial_local(instrument, trade_date.strftime('%Y-%m-%d'))
            value = latest.get('roe') if latest else None
            values.append(pd.to_numeric(value, errors='coerce'))
        result[instrument] = values
    ranked = _cross_sectional_rank(result, ascending=True)
    return _stack_factor(ranked, 'roe_ttm')


def factor_log_market_cap(factor_input: pd.DataFrame) -> pd.Series:
    close = factor_input['close'].astype(float).unstack('asset')
    volume = factor_input['volume'].astype(float).unstack('asset')
    approx_market_cap = (close * volume).replace(0, np.nan)
    factor = -np.log(approx_market_cap).shift(1)
    factor = factor.stack(future_stack=True)
    factor.index.names = ['date', 'asset']
    factor.name = 'log_market_cap'
    return factor.replace([np.inf, -np.inf], np.nan).dropna()

from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbt as vbt


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
SCRIPT_ROOT = ROOT / 'scripts'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity  # noqa: E402
from alpha_research.factors import pysr_factors  # noqa: E402
from run_feature_factory import compute_factor_series, normalize_with_cupy  # noqa: E402


START = '2016-01-01'
END = '2023-12-31'
TRAIN_END = '2021-12-31'
OOS_START = '2022-01-01'
TOP_N = 300
REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'
CSI300_PATH = ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'
MODEL_PATH = ROOT / 'runtime' / 'models' / 'lgbm_factor_synthesis_v1.pkl'
REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'factor_backtest_report.json'
ST_PATH = ROOT / 'runtime' / 'fundamental_data' / 'st_history.parquet'


def load_top_factors() -> list[str]:
    registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    eligible = [item for item in registry if not str(item.get('factor_name', '')).startswith('pysr_')]
    eligible = sorted(eligible, key=lambda item: float(item.get('icir', item.get('icir_neutralized', 0.0))), reverse=True)
    return [item['factor_name'] for item in eligible[:5]]


def build_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    if CSI300_PATH.exists():
        csi300 = pd.read_parquet(CSI300_PATH)
        stock_dir = ROOT / 'runtime' / 'market_data' / 'cn_stock'
        instruments = [code for code in csi300['instrument_code'].astype(str).str.zfill(6).tolist() if (stock_dir / f'{code}.parquet').exists()]
    else:
        instruments = select_top_n_by_liquidity('stock', START, END, top_n=TOP_N)
    prices = load_prices(instruments, START, END, asset_type='stock')
    factor_input = load_factor_input(instruments, START, END, asset_type='stock').copy()
    factor_input['open'] = factor_input['close']
    market_rows = []
    stock_dir = ROOT / 'runtime' / 'market_data' / 'cn_stock'
    for symbol in instruments:
        path = stock_dir / f'{symbol}.parquet'
        frame = pd.read_parquet(path, columns=['trade_date', 'symbol', 'close', 'is_suspended']).copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'])
        frame = frame.loc[(frame['trade_date'] >= pd.Timestamp(START)) & (frame['trade_date'] <= pd.Timestamp(END))]
        market_rows.append(frame)
    market = pd.concat(market_rows, ignore_index=True)
    market['symbol'] = market['symbol'].astype(str).str.zfill(6)
    return prices, factor_input, market, instruments


def build_st_mask(dates: pd.DatetimeIndex, assets: list[str]) -> pd.DataFrame:
    mask = pd.DataFrame(False, index=dates, columns=assets)
    if not ST_PATH.exists():
        return mask
    st = pd.read_parquet(ST_PATH, columns=['ts_code', 'name', 'start_date', 'end_date']).copy()
    st = st.loc[st['name'].astype(str).str.contains('ST', na=False)]
    st['symbol'] = st['ts_code'].astype(str).str.split('.').str[0].str.zfill(6)
    for row in st.itertuples(index=False):
        if row.symbol not in mask.columns:
            continue
        start = pd.Timestamp(row.start_date)
        end = pd.Timestamp(row.end_date) if pd.notna(row.end_date) else dates[-1]
        mask.loc[(mask.index >= start) & (mask.index <= end), row.symbol] = True
    return mask


def build_normalized_panel(base_factors: list[str], factor_input: pd.DataFrame) -> pd.DataFrame:
    series_list = [compute_factor_series(name, factor_input) for name in base_factors]
    frame = pd.concat(series_list, axis=1).reset_index()
    frame['asset'] = frame['asset'].astype(str).str.zfill(6)
    frame['date'] = pd.to_datetime(frame['date'])
    frame = frame.dropna(subset=base_factors, how='all').copy()
    frame = normalize_with_cupy(frame, base_factors)
    return frame.set_index(['date', 'asset']).sort_index()


def month_end_dates(index: pd.DatetimeIndex) -> list[pd.Timestamp]:
    return list(pd.Series(index, index=index).groupby(index.to_period('M')).max())


def next_trading_dates(index: pd.DatetimeIndex, dates: list[pd.Timestamp]) -> dict[pd.Timestamp, pd.Timestamp | None]:
    mapping: dict[pd.Timestamp, pd.Timestamp | None] = {}
    for date in dates:
        pos = index.searchsorted(date, side='right')
        mapping[date] = index[pos] if pos < len(index) else None
    return mapping


def build_signal_frames(prices: pd.DataFrame, market: pd.DataFrame, normalized_panel: pd.DataFrame, top_factors: list[str], model_bundle: dict[str, object]) -> dict[str, pd.DataFrame]:
    signals: dict[str, pd.DataFrame] = {}
    for name in top_factors:
        signals[name] = normalized_panel[name].unstack('asset').reindex(index=prices.index, columns=prices.columns)
    model_features = model_bundle['feature_columns']
    lgbm_rows = normalized_panel[model_features].copy()
    lgbm_rows = lgbm_rows.loc[lgbm_rows.notna().any(axis=1)].copy()
    lgbm_pred = model_bundle['model'].predict(lgbm_rows[model_features])
    lgbm_series = pd.Series(lgbm_pred, index=lgbm_rows.index, name='lgbm_synthetic')
    signals['lgbm_synthetic'] = lgbm_series.unstack('asset').reindex(index=prices.index, columns=prices.columns)
    pysr_rows = normalized_panel[pysr_factors.TOP_FACTORS].copy()
    pysr_rows = pysr_rows.loc[pysr_rows.notna().any(axis=1)].copy()
    pysr_series = pysr_factors.pysr_factor_3(pysr_rows).rename('pysr_formula_3')
    signals['pysr_formula_3'] = pysr_series.unstack('asset').reindex(index=prices.index, columns=prices.columns)
    return signals


def build_weights(signal_frame: pd.DataFrame, suspended: pd.DataFrame, st_mask: pd.DataFrame) -> pd.DataFrame:
    weights = pd.DataFrame(0.0, index=signal_frame.index, columns=signal_frame.columns)
    signal_dates = month_end_dates(signal_frame.index)
    exec_map = next_trading_dates(signal_frame.index, signal_dates)
    for i, signal_date in enumerate(signal_dates[:-1]):
        entry_date = exec_map[signal_date]
        next_entry = exec_map[signal_dates[i + 1]]
        if entry_date is None or next_entry is None:
            continue
        scores = signal_frame.loc[signal_date].copy()
        eligible = (~suspended.loc[signal_date].fillna(True)) & (~st_mask.loc[signal_date].fillna(False))
        scores = scores.where(eligible)
        valid = scores.dropna()
        if len(valid) < 200:
            continue
        ranked = valid.rank(method='first')
        buckets = pd.qcut(ranked, 5, labels=False) + 1
        longs = buckets[buckets == 5].index.tolist()
        shorts = buckets[buckets == 1].index.tolist()
        period = weights.index[(weights.index >= entry_date) & (weights.index < next_entry)]
        if longs:
            weights.loc[period, longs] = 0.5 / len(longs)
        if shorts:
            weights.loc[period, shorts] = -0.5 / len(shorts)
    return weights


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
    return {'annual_return': annual_return, 'sharpe': sharpe, 'max_drawdown': float(drawdown.min())}


def slice_metrics(pf: vbt.Portfolio, start: str | None = None, end: str | None = None) -> dict[str, float]:
    value = pf.value()
    if start:
        value = value.loc[value.index >= pd.Timestamp(start)]
    if end:
        value = value.loc[value.index <= pd.Timestamp(end)]
    return portfolio_metrics(value)


def main() -> None:
    top_factors = load_top_factors()
    prices, factor_input, market, instruments = build_market_inputs()
    with MODEL_PATH.open('rb') as handle:
        model_bundle = pickle.load(handle)
    base_factors = sorted(set(top_factors + list(model_bundle['feature_columns']) + list(pysr_factors.TOP_FACTORS)))
    normalized_panel = build_normalized_panel(base_factors, factor_input)
    suspended = market.pivot(index='trade_date', columns='symbol', values='is_suspended').reindex(index=prices.index, columns=prices.columns).fillna(True)
    st_mask = build_st_mask(prices.index, list(prices.columns))
    signal_frames = build_signal_frames(prices, market, normalized_panel, top_factors, model_bundle)

    results = []
    for factor_id, signal_frame in signal_frames.items():
        weights = build_weights(signal_frame, suspended, st_mask)
        pf = vbt.Portfolio.from_orders(
            prices,
            size=weights,
            size_type='targetpercent',
            direction='both',
            cash_sharing=True,
            init_cash=1.0,
            fees=0.0,
            slippage=0.0,
            freq='1D',
        )
        results.append({
            'factor_id': factor_id,
            'in_sample': slice_metrics(pf, end=TRAIN_END),
            'out_of_sample': slice_metrics(pf, start=OOS_START, end=END),
        })

    payload = {'factors': results}
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

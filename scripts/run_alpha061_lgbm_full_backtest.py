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
from alpha_research.factors import alpha101, pysr_factors  # noqa: E402
from run_feature_factory import compute_factor_series, normalize_with_cupy  # noqa: E402


START = '2016-01-01'
END = '2023-12-31'
TRAIN_END = '2021-12-31'
OOS_START = '2022-01-01'
TOP_N = 300
MAX_WEIGHT = 0.05
COMMISSION = 0.0003
STAMP_DUTY = 0.001
SLIPPAGE = 0.001
CSI300_PATH = ROOT / 'runtime' / 'classification_data' / 'index_components' / 'csi300_latest.parquet'
ST_PATH = ROOT / 'runtime' / 'fundamental_data' / 'st_history.parquet'
STOCK_BASIC_PATH = ROOT / 'runtime' / 'fundamental_data' / 'stock_basic.parquet'
MODEL_PATH = ROOT / 'runtime' / 'models' / 'lgbm_factor_synthesis_v1.pkl'
ALPHA061_REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'alpha061_full_backtest.json'
LGBM_REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'lgbm_synthetic_full_backtest.json'


def build_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    if CSI300_PATH.exists():
        csi300 = pd.read_parquet(CSI300_PATH)
        stock_dir = ROOT / 'runtime' / 'market_data' / 'cn_stock'
        instruments = [
            code
            for code in csi300['instrument_code'].astype(str).str.zfill(6).tolist()
            if (stock_dir / f'{code}.parquet').exists()
        ]
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
    for row in st.itertuples(index=False):
        symbol = str(row.ts_code).split('.')[0].zfill(6)
        if symbol not in mask.columns:
            continue
        start = pd.Timestamp(row.start_date)
        end = pd.Timestamp(row.end_date) if pd.notna(row.end_date) else dates[-1]
        mask.loc[(mask.index >= start) & (mask.index <= end), symbol] = True
    return mask


def build_listing_mask(dates: pd.DatetimeIndex, assets: list[str]) -> pd.DataFrame:
    stock_basic = pd.read_parquet(STOCK_BASIC_PATH).copy()
    stock_basic['symbol'] = stock_basic['symbol'].astype(str).str.zfill(6)
    stock_basic['list_date'] = pd.to_datetime(stock_basic['list_date'], errors='coerce')
    listed_dates = stock_basic.drop_duplicates('symbol', keep='last').set_index('symbol')['list_date'].to_dict()

    date_values = dates.to_numpy(dtype='datetime64[D]')
    listed_values = np.array(
        [pd.Timestamp(listed_dates.get(symbol)).to_datetime64() if symbol in listed_dates else np.datetime64('NaT') for symbol in assets],
        dtype='datetime64[D]',
    )
    valid = ~np.isnat(listed_values)
    age_days = np.zeros((len(dates), len(assets)), dtype=np.int32)
    if valid.any():
        age_days[:, valid] = (
            date_values[:, None].astype('datetime64[D]') - listed_values[valid][None, :]
        ).astype('timedelta64[D]').astype(np.int32)
    mask = age_days >= 60
    mask[:, ~valid] = False
    return pd.DataFrame(mask, index=dates, columns=assets, dtype=bool)


def month_end_dates(index: pd.DatetimeIndex) -> list[pd.Timestamp]:
    return list(pd.Series(index, index=index).groupby(index.to_period('M')).max())


def next_trading_dates(index: pd.DatetimeIndex, dates: list[pd.Timestamp]) -> dict[pd.Timestamp, pd.Timestamp | None]:
    mapping: dict[pd.Timestamp, pd.Timestamp | None] = {}
    for date in dates:
        pos = index.searchsorted(date, side='right')
        mapping[date] = index[pos] if pos < len(index) else None
    return mapping


def equal_weight_targets(symbols: list[str], columns: list[str]) -> pd.Series:
    targets = pd.Series(0.0, index=columns, dtype=float)
    if not symbols:
        return targets
    base_weight = min(MAX_WEIGHT, 1.0 / len(symbols))
    targets.loc[symbols] = base_weight
    total = float(targets.sum())
    if total > 0:
        targets /= total
    return targets


def build_rebalance_plan(
    prices: pd.DataFrame,
    signal_frame: pd.DataFrame,
    suspended: pd.DataFrame,
    st_mask: pd.DataFrame,
    listing_mask: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
    target_weights = pd.DataFrame(np.nan, index=prices.index, columns=prices.columns, dtype=float)
    fee_rates = pd.DataFrame(0.0, index=prices.index, columns=prices.columns, dtype=float)
    slippage_rates = pd.DataFrame(0.0, index=prices.index, columns=prices.columns, dtype=float)
    rebalances: list[dict[str, object]] = []

    signal_dates = month_end_dates(prices.index)
    exec_map = next_trading_dates(prices.index, signal_dates)
    prev_targets = pd.Series(0.0, index=prices.columns, dtype=float)
    prev_entry_date: pd.Timestamp | None = None

    for signal_date in signal_dates[:-1]:
        entry_date = exec_map[signal_date]
        if entry_date is None:
            continue
        scores = signal_frame.loc[signal_date].copy()
        eligible = (~suspended.loc[signal_date].fillna(True)) & (~st_mask.loc[signal_date].fillna(False)) & listing_mask.loc[signal_date].fillna(False)
        scores = scores.where(eligible)
        valid = scores.dropna()
        if len(valid) < 200:
            continue

        ranked = valid.rank(method='first')
        buckets = pd.qcut(ranked, 5, labels=False) + 1
        longs = buckets[buckets == 5].index.tolist()
        targets = equal_weight_targets(longs, list(prices.columns))

        current_weights = prev_targets.copy()
        if prev_entry_date is not None and prev_targets.sum() > 0:
            price_rel = prices.loc[entry_date].divide(prices.loc[prev_entry_date]).replace([np.inf, -np.inf], np.nan).fillna(1.0)
            drifted = prev_targets.multiply(price_rel, fill_value=0.0)
            drifted_total = float(drifted.sum())
            current_weights = drifted / drifted_total if drifted_total > 0 else prev_targets.copy()

        delta = targets - current_weights
        changed = delta.abs() > 1e-10
        buys = delta > 1e-10
        sells = delta < -1e-10

        target_weights.loc[entry_date] = targets.to_numpy(dtype=float)
        fee_rates.loc[entry_date, buys[buys].index] = COMMISSION
        fee_rates.loc[entry_date, sells[sells].index] = COMMISSION + STAMP_DUTY
        slippage_rates.loc[entry_date, changed[changed].index] = SLIPPAGE

        turnover = float(0.5 * delta.abs().sum())
        rebalances.append({
            'signal_date': signal_date.strftime('%Y-%m-%d'),
            'entry_date': entry_date.strftime('%Y-%m-%d'),
            'holding_count': int(len(longs)),
            'turnover': turnover,
        })
        prev_targets = targets
        prev_entry_date = entry_date

    return target_weights, fee_rates, slippage_rates, rebalances


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


def annualized_turnover(rebalances: list[dict[str, object]], start: str | None = None, end: str | None = None) -> float:
    selected = []
    for item in rebalances:
        entry = pd.Timestamp(str(item['entry_date']))
        if start and entry < pd.Timestamp(start):
            continue
        if end and entry > pd.Timestamp(end):
            continue
        selected.append(item)
    if not selected:
        return 0.0
    start_date = pd.Timestamp(str(selected[0]['entry_date']))
    end_date = pd.Timestamp(str(selected[-1]['entry_date']))
    years = max((end_date - start_date).days / 365.25, 1 / 12)
    total_turnover = float(sum(float(item['turnover']) for item in selected))
    return total_turnover / years


def slice_metrics(pf: vbt.Portfolio, rebalances: list[dict[str, object]], start: str | None = None, end: str | None = None) -> dict[str, float]:
    value = pf.value()
    if start:
        value = value.loc[value.index >= pd.Timestamp(start)]
    if end:
        value = value.loc[value.index <= pd.Timestamp(end)]
    metrics = portfolio_metrics(value)
    metrics['turnover_annualized'] = annualized_turnover(rebalances, start=start, end=end)
    return metrics


def build_normalized_panel(factor_names: list[str], factor_input: pd.DataFrame) -> pd.DataFrame:
    series_list = [compute_factor_series(name, factor_input) for name in factor_names]
    frame = pd.concat(series_list, axis=1).reset_index()
    frame['asset'] = frame['asset'].astype(str).str.zfill(6)
    frame['date'] = pd.to_datetime(frame['date'])
    frame = frame.dropna(subset=factor_names, how='all').copy()
    frame = normalize_with_cupy(frame, factor_names)
    return frame.set_index(['date', 'asset']).sort_index()


def build_signal_frames(prices: pd.DataFrame, factor_input: pd.DataFrame) -> dict[str, pd.DataFrame]:
    with MODEL_PATH.open('rb') as handle:
        model_bundle = pickle.load(handle)

    feature_cols = list(model_bundle['feature_columns'])
    factor_names = sorted(set(feature_cols + list(pysr_factors.TOP_FACTORS)))
    normalized_panel = build_normalized_panel(factor_names, factor_input)

    alpha061_series = alpha101.alpha061(factor_input).rename('alpha061')
    alpha061_frame = alpha061_series.unstack('asset').reindex(index=prices.index, columns=prices.columns)

    lgbm_rows = normalized_panel[feature_cols].copy()
    lgbm_rows = lgbm_rows.loc[lgbm_rows.notna().any(axis=1)].copy()
    lgbm_pred = model_bundle['model'].predict(lgbm_rows[feature_cols])
    lgbm_series = pd.Series(lgbm_pred, index=lgbm_rows.index, name='lgbm_synthetic')
    lgbm_frame = lgbm_series.unstack('asset').reindex(index=prices.index, columns=prices.columns)

    return {
        'alpha061': alpha061_frame,
        'lgbm_synthetic': lgbm_frame,
    }


def build_report(
    factor_id: str,
    pf_cost: vbt.Portfolio,
    pf_gross: vbt.Portfolio,
    rebalances: list[dict[str, object]],
    instrument_count: int,
) -> dict[str, object]:
    in_sample = slice_metrics(pf_cost, rebalances, end=TRAIN_END)
    oos_cost = slice_metrics(pf_cost, rebalances, start=OOS_START, end=END)
    oos_gross = slice_metrics(pf_gross, rebalances, start=OOS_START, end=END)
    oos_cost['sharpe_cost_drag'] = float(oos_gross['sharpe'] - oos_cost['sharpe'])
    return {
        'factor_id': factor_id,
        'universe': 'csi300_latest_with_filters',
        'config': {
            'start': START,
            'end': END,
            'train_end': TRAIN_END,
            'oos_start': OOS_START,
            'quantile': 'Q5 / Top20%',
            'weighting': 'equal_weight',
            'max_stock_weight': MAX_WEIGHT,
            'rebalance': 'monthly',
            'costs': {
                'commission': COMMISSION,
                'stamp_duty_sell': STAMP_DUTY,
                'slippage_per_side': SLIPPAGE,
            },
            'constraints': ['exclude_st', 'exclude_suspended', 'exclude_listing_lt_60d'],
            'instrument_count': instrument_count,
        },
        'in_sample': in_sample,
        'out_of_sample': {
            **oos_cost,
            'gross_sharpe': oos_gross['sharpe'],
        },
        'rebalances': rebalances,
    }


def main() -> None:
    prices, factor_input, market, instruments = build_market_inputs()
    signal_frames = build_signal_frames(prices, factor_input)

    suspended = market.pivot(index='trade_date', columns='symbol', values='is_suspended').reindex(index=prices.index, columns=prices.columns).fillna(True)
    st_mask = build_st_mask(prices.index, list(prices.columns))
    listing_mask = build_listing_mask(prices.index, list(prices.columns))

    outputs: dict[str, Path] = {
        'alpha061': ALPHA061_REPORT_PATH,
        'lgbm_synthetic': LGBM_REPORT_PATH,
    }
    reports: dict[str, dict[str, object]] = {}

    for factor_id, signal_frame in signal_frames.items():
        target_weights, fee_rates, slippage_rates, rebalances = build_rebalance_plan(
            prices=prices,
            signal_frame=signal_frame,
            suspended=suspended,
            st_mask=st_mask,
            listing_mask=listing_mask,
        )
        pf_cost = vbt.Portfolio.from_orders(
            close=prices,
            size=target_weights,
            size_type='targetpercent',
            direction='longonly',
            cash_sharing=True,
            init_cash=1.0,
            fees=fee_rates,
            slippage=slippage_rates,
            freq='1D',
        )
        pf_gross = vbt.Portfolio.from_orders(
            close=prices,
            size=target_weights,
            size_type='targetpercent',
            direction='longonly',
            cash_sharing=True,
            init_cash=1.0,
            fees=0.0,
            slippage=0.0,
            freq='1D',
        )
        report = build_report(factor_id, pf_cost, pf_gross, rebalances, len(instruments))
        outputs[factor_id].write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        reports[factor_id] = report

    print(json.dumps(reports, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
THEME_INDEX_PATH = REPO_ROOT / 'runtime' / 'alternative_data' / 'ths_theme' / 'ths_index.parquet'
THEME_MEMBER_PATH = REPO_ROOT / 'runtime' / 'alternative_data' / 'ths_theme' / 'ths_member.parquet'
MARKET_DATA_DIR = REPO_ROOT / 'runtime' / 'market_data' / 'cn_stock'
FUNDAMENTAL_DATA_DIR = REPO_ROOT / 'runtime' / 'fundamental_data'
OUTPUT_PATH = REPO_ROOT / 'runtime' / 'strategy2' / 'theme_features.parquet'


@dataclass(frozen=True)
class ThemeFeatureConfig:
    start_date: str = '2017-01-01'
    end_date: str = '2024-12-31'
    min_theme_members: int = 10
    windows: tuple[int, ...] = (5, 10, 20)
    rank_windows: tuple[int, ...] = (5, 20)
    heat_windows: tuple[int, ...] = (5, 20)
    heat_baseline: int = 60
    min_list_days: int = 252
    min_avg_amount: float = 5e7


def _normalize_ts_code(code: object) -> str:
    text = str(code or '').strip().upper()
    if not text:
        return ''
    if '.' in text:
        symbol, market = text.split('.', 1)
        normalized_symbol = symbol.zfill(6) if symbol.isdigit() else symbol
        return f'{normalized_symbol}.{market}'
    return f'{text.zfill(6)}.SZ' if text.isdigit() else text


def _safe_divide(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    out = np.full(numer.shape, np.nan, dtype=np.float32)
    np.divide(numer, denom, out=out, where=denom > 0)
    return out


def load_theme_inputs(
    theme_index_path: str | Path = THEME_INDEX_PATH,
    theme_member_path: str | Path = THEME_MEMBER_PATH,
    *,
    min_theme_members: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    theme_index = pd.read_parquet(theme_index_path).copy()
    theme_member = pd.read_parquet(theme_member_path).copy()
    theme_index['ts_code'] = theme_index['ts_code'].map(_normalize_ts_code)
    theme_member['ts_code'] = theme_member['ts_code'].map(_normalize_ts_code)
    theme_member['con_code'] = theme_member['con_code'].map(_normalize_ts_code)
    theme_index['count'] = pd.to_numeric(theme_index.get('count'), errors='coerce')
    valid_themes = theme_index.loc[theme_index['count'] >= float(min_theme_members), 'ts_code'].dropna().unique()
    theme_index = theme_index.loc[theme_index['ts_code'].isin(valid_themes)].copy()
    theme_member = theme_member.loc[theme_member['ts_code'].isin(valid_themes)].copy()
    theme_member = theme_member.dropna(subset=['ts_code', 'con_code']).drop_duplicates(['ts_code', 'con_code'])
    return theme_index.reset_index(drop=True), theme_member.reset_index(drop=True)


def load_market_panels(
    market_data_dir: str | Path,
    ts_codes: Sequence[str],
    *,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
    market_data_dir = Path(market_data_dir)
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    close_frames: list[pd.Series] = []
    amount_frames: list[pd.Series] = []
    volume_frames: list[pd.Series] = []
    listed_dates: dict[str, pd.Timestamp] = {}

    for idx, ts_code in enumerate(sorted({_normalize_ts_code(code) for code in ts_codes})):
        symbol = ts_code.split('.', 1)[0]
        path = market_data_dir / f'{symbol}.parquet'
        if not path.exists():
            continue
        daily = pd.read_parquet(path, columns=['trade_date', 'close', 'amount', 'volume', 'listed_date']).copy()
        daily['trade_date'] = pd.to_datetime(daily['trade_date'], errors='coerce')
        daily = daily.loc[(daily['trade_date'] >= start) & (daily['trade_date'] <= end)]
        if daily.empty:
            continue
        daily = daily.sort_values('trade_date')
        listed = pd.to_datetime(daily['listed_date'], errors='coerce').dropna()
        if not listed.empty:
            listed_dates[ts_code] = listed.iloc[0].normalize()
        else:
            listed_dates[ts_code] = daily['trade_date'].iloc[0].normalize()
        close_frames.append(daily.set_index('trade_date')['close'].astype(np.float32).rename(ts_code))
        amount_frames.append(daily.set_index('trade_date')['amount'].astype(np.float32).rename(ts_code))
        volume_frames.append(daily.set_index('trade_date')['volume'].astype(np.float32).rename(ts_code))
        if idx > 0 and idx % 1000 == 0:
            print(f'market_progress={idx}/{len(ts_codes)}')

    close = pd.concat(close_frames, axis=1).sort_index().sort_index(axis=1)
    amount = pd.concat(amount_frames, axis=1).reindex_like(close)
    volume = pd.concat(volume_frames, axis=1).reindex_like(close)
    listed_series = pd.Series(listed_dates, dtype='datetime64[ns]').reindex(close.columns)
    return close, amount, volume, listed_series


def build_pool_b_mask(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    volume: pd.DataFrame,
    listed_dates: pd.Series,
    *,
    data_dir: str | Path = FUNDAMENTAL_DATA_DIR,
    min_list_days: int = 252,
    min_avg_amount: float = 5e7,
) -> pd.DataFrame:
    data_dir = Path(data_dir)
    trade_dates = close.index
    columns = close.columns

    listed_matrix = pd.DataFrame(
        np.broadcast_to(listed_dates.to_numpy(dtype='datetime64[ns]'), (len(trade_dates), len(columns))),
        index=trade_dates,
        columns=columns,
    )
    trade_date_matrix = pd.DataFrame(
        np.broadcast_to(trade_dates.to_numpy(dtype='datetime64[ns]')[:, None], (len(trade_dates), len(columns))),
        index=trade_dates,
        columns=columns,
    )
    listed_ok = (trade_date_matrix - listed_matrix).apply(lambda col: col.dt.days).ge(min_list_days)

    avg_amount = amount.rolling(20, min_periods=20).mean()
    liquidity_ok = avg_amount.ge(float(min_avg_amount))

    halted_ok = volume.fillna(0).gt(0) & amount.fillna(0).gt(0)
    has_price = close.notna()

    st_codes: set[str] = set()
    stock_basic_path = data_dir / 'stock_basic.parquet'
    stock_basic = pd.read_parquet(stock_basic_path).copy() if stock_basic_path.exists() else pd.DataFrame()
    if not stock_basic.empty and 'ts_code' in stock_basic.columns:
        stock_basic['ts_code'] = stock_basic['ts_code'].map(_normalize_ts_code)
    if not stock_basic.empty and 'name' in stock_basic.columns:
        st_codes.update(
            stock_basic.loc[
                stock_basic['name'].astype(str).str.contains('ST', case=False, na=False),
                'ts_code',
            ].dropna().map(_normalize_ts_code)
        )
    st_history_path = data_dir / 'st_history.parquet'
    st_history = pd.read_parquet(st_history_path).copy() if st_history_path.exists() else pd.DataFrame()
    if not st_history.empty and 'ts_code' in st_history.columns:
        st_history['ts_code'] = st_history['ts_code'].map(_normalize_ts_code)
        for col in ('start_date', 'end_date'):
            if col in st_history.columns:
                st_history[col] = pd.to_datetime(st_history[col], errors='coerce')
        if 'name' in st_history.columns:
            st_history = st_history.loc[st_history['name'].astype(str).str.contains('ST', case=False, na=False)].copy()
        else:
            st_history = pd.DataFrame(columns=['ts_code', 'start_date', 'end_date'])
    st_mask = pd.DataFrame(False, index=trade_dates, columns=columns)
    if st_codes:
        active_codes = [code for code in columns if code in st_codes]
        if active_codes:
            st_mask.loc[:, active_codes] = True
    if not st_history.empty:
        for row in st_history[['ts_code', 'start_date', 'end_date']].dropna(subset=['ts_code']).itertuples(index=False):
            code = _normalize_ts_code(row.ts_code)
            if code not in st_mask.columns:
                continue
            start = pd.Timestamp(row.start_date) if pd.notna(row.start_date) else trade_dates.min()
            end = pd.Timestamp(row.end_date) if pd.notna(row.end_date) else trade_dates.max()
            st_mask.loc[(trade_dates >= start) & (trade_dates <= end), code] = True

    pool_mask = has_price & listed_ok & liquidity_ok & halted_ok & ~st_mask
    return pool_mask


def _build_membership_matrix(theme_member: pd.DataFrame, stock_codes: Sequence[str]) -> tuple[np.ndarray, list[str], dict[str, list[int]]]:
    stock_codes = list(stock_codes)
    stock_index = {code: idx for idx, code in enumerate(stock_codes)}
    theme_codes = sorted(theme_member['ts_code'].unique())
    theme_index = {code: idx for idx, code in enumerate(theme_codes)}
    matrix = np.zeros((len(stock_codes), len(theme_codes)), dtype=np.float32)
    members_by_theme: dict[str, list[int]] = {code: [] for code in theme_codes}
    for row in theme_member[['ts_code', 'con_code']].itertuples(index=False):
        stock_pos = stock_index.get(_normalize_ts_code(row.con_code))
        theme_pos = theme_index.get(_normalize_ts_code(row.ts_code))
        if stock_pos is None or theme_pos is None:
            continue
        matrix[stock_pos, theme_pos] = 1.0
        members_by_theme[theme_codes[theme_pos]].append(stock_pos)
    return matrix, theme_codes, members_by_theme


def _stack_feature(frame: pd.DataFrame, mask: pd.DataFrame, name: str) -> pd.Series:
    masked = frame.where(mask)
    stacked = masked.stack(dropna=False)
    stacked.name = name
    return stacked


def _compute_rank_feature(signal: pd.DataFrame, members_by_theme: dict[str, list[int]], theme_codes: list[str], membership: np.ndarray) -> pd.DataFrame:
    score_sum = np.zeros(signal.shape, dtype=np.float32)
    score_cnt = np.zeros(signal.shape, dtype=np.float32)
    for theme_code in theme_codes:
        member_positions = members_by_theme.get(theme_code, [])
        if len(member_positions) < 2:
            continue
        subset = signal.iloc[:, member_positions]
        ranks = subset.rank(axis=1, pct=True)
        values = ranks.to_numpy(dtype=np.float32)
        score_sum[:, member_positions] += np.nan_to_num(values, nan=0.0)
        score_cnt[:, member_positions] += np.isfinite(values).astype(np.float32)
    feature = _safe_divide(score_sum, score_cnt)
    return pd.DataFrame(feature, index=signal.index, columns=signal.columns)


def build_theme_feature_matrix_from_panels(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    theme_member: pd.DataFrame,
    pool_mask: pd.DataFrame,
    *,
    config: ThemeFeatureConfig | None = None,
) -> pd.DataFrame:
    cfg = config or ThemeFeatureConfig()
    close = close.sort_index().sort_index(axis=1).astype(np.float32)
    amount = amount.reindex_like(close).astype(np.float32)
    pool_mask = pool_mask.reindex_like(close).fillna(False).astype(bool)
    membership, theme_codes, members_by_theme = _build_membership_matrix(theme_member, list(close.columns))
    if membership.shape[1] == 0:
        raise ValueError('No valid themes remain after membership filtering')

    stock_theme_counts = membership.sum(axis=1, keepdims=True)
    feature_series: list[pd.Series] = []

    for window in cfg.windows:
        stock_signal = close.pct_change(window, fill_method=None)
        numer = np.nan_to_num(stock_signal.to_numpy(dtype=np.float32), nan=0.0) @ membership
        denom = np.isfinite(stock_signal.to_numpy(dtype=np.float32)).astype(np.float32) @ membership
        theme_avg = _safe_divide(numer, denom)
        stock_avg = _safe_divide(np.nan_to_num(theme_avg, nan=0.0) @ membership.T, stock_theme_counts.T)
        feature = pd.DataFrame(stock_avg, index=close.index, columns=close.columns)
        feature_series.append(_stack_feature(feature, pool_mask, f'theme_avg_ret_{window}'))

    for window in cfg.rank_windows:
        stock_signal = close.pct_change(window, fill_method=None)
        rank_feature = _compute_rank_feature(stock_signal, members_by_theme, theme_codes, membership)
        feature_series.append(_stack_feature(rank_feature, pool_mask, f'theme_rank_pct_{window}'))

    amount_base = amount.replace(0, np.nan)
    for window in cfg.heat_windows:
        stock_liq = amount_base.rolling(window, min_periods=window).mean()
        numer = np.nan_to_num(stock_liq.to_numpy(dtype=np.float32), nan=0.0) @ membership
        denom = np.isfinite(stock_liq.to_numpy(dtype=np.float32)).astype(np.float32) @ membership
        theme_liq = _safe_divide(numer, denom)
        theme_heat = _safe_divide(theme_liq, pd.DataFrame(theme_liq, index=close.index, columns=theme_codes).rolling(cfg.heat_baseline, min_periods=20).mean().to_numpy(dtype=np.float32))
        stock_heat = _safe_divide(np.nan_to_num(theme_heat, nan=0.0) @ membership.T, stock_theme_counts.T)
        feature = pd.DataFrame(stock_heat, index=close.index, columns=close.columns)
        feature_series.append(_stack_feature(feature, pool_mask, f'theme_heat_{window}'))

    member_count_feature = pd.DataFrame(
        np.broadcast_to(stock_theme_counts.T, close.shape).astype(np.float32),
        index=close.index,
        columns=close.columns,
    )
    feature_series.append(_stack_feature(member_count_feature, pool_mask, 'theme_member_count'))

    features = pd.concat(feature_series, axis=1)
    features.index = features.index.set_names(['trade_date', 'ts_code'])
    features = features.dropna(how='all').sort_index()
    return features


def scan_feature_ic(
    features: pd.DataFrame,
    close: pd.DataFrame,
    pool_mask: pd.DataFrame,
    *,
    feature_names: Sequence[str] | None = None,
    horizons: Sequence[int] = (3, 5, 10),
) -> pd.DataFrame:
    selected = list(feature_names or features.columns[:5])
    close = close.sort_index().sort_index(axis=1)
    pool_mask = pool_mask.reindex_like(close).fillna(False)
    rows: list[dict[str, object]] = []
    for feature_name in selected:
        wide_feature = features[feature_name].unstack('ts_code').reindex_like(close)
        row: dict[str, object] = {'feature_name': feature_name}
        best = np.nan
        for horizon in horizons:
            future_return = close.shift(-horizon).div(close).sub(1.0)
            excess_return = future_return.sub(future_return.where(pool_mask).mean(axis=1), axis=0)
            daily_ic: list[float] = []
            for trade_date in close.index:
                mask = pool_mask.loc[trade_date]
                x = wide_feature.loc[trade_date].where(mask)
                y = excess_return.loc[trade_date].where(mask)
                aligned = pd.concat([x.rename('x'), y.rename('y')], axis=1).dropna()
                if len(aligned) < 20:
                    continue
                daily_ic.append(float(aligned['x'].corr(aligned['y'])))
            series = pd.Series(daily_ic, dtype=float)
            ic_mean = float(series.mean()) if not series.empty else np.nan
            icir = float(ic_mean / series.std(ddof=0)) if len(series) > 1 and series.std(ddof=0) > 0 else np.nan
            row[f'ic_{horizon}d'] = ic_mean
            row[f'icir_{horizon}d'] = icir
            if np.isnan(best) or (np.isfinite(icir) and abs(icir) > abs(best)):
                best = icir
        row['best_icir'] = best
        rows.append(row)
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values('best_icir', key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    return result


def build_theme_feature_matrix(
    *,
    config: ThemeFeatureConfig | None = None,
    theme_index_path: str | Path = THEME_INDEX_PATH,
    theme_member_path: str | Path = THEME_MEMBER_PATH,
    market_data_dir: str | Path = MARKET_DATA_DIR,
    fundamental_data_dir: str | Path = FUNDAMENTAL_DATA_DIR,
    output_path: str | Path = OUTPUT_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = config or ThemeFeatureConfig()
    _, theme_member = load_theme_inputs(theme_index_path, theme_member_path, min_theme_members=cfg.min_theme_members)
    stock_codes = sorted(theme_member['con_code'].unique())
    close, amount, volume, listed_dates = load_market_panels(
        market_data_dir,
        stock_codes,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
    )
    pool_mask = build_pool_b_mask(
        close,
        amount,
        volume,
        listed_dates,
        data_dir=fundamental_data_dir,
        min_list_days=cfg.min_list_days,
        min_avg_amount=cfg.min_avg_amount,
    )
    features = build_theme_feature_matrix_from_panels(close, amount, theme_member, pool_mask, config=cfg)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(output_path)
    ic_scan = scan_feature_ic(
        features,
        close,
        pool_mask,
        feature_names=[
            'theme_avg_ret_5',
            'theme_avg_ret_10',
            'theme_avg_ret_20',
            'theme_rank_pct_20',
            'theme_heat_20',
        ],
    )
    return features, ic_scan


__all__ = [
    'ThemeFeatureConfig',
    'build_pool_b_mask',
    'build_theme_feature_matrix',
    'build_theme_feature_matrix_from_panels',
    'load_market_panels',
    'load_theme_inputs',
    'scan_feature_ic',
]

from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path

import alphalens as al
import cupy as cp
import lightgbm as lgb
import numpy as np
import pandas as pd
import shap
from scipy.stats import spearmanr


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity  # noqa: E402
from alpha_research.factors import alpha101, classic_factors  # noqa: E402


FORWARD_DAYS = 5
START = '2016-01-01'
END = '2026-03-31'
TRAIN_END = '2021-12-31'
VALID_START = '2022-01-01'
VALID_END = '2023-12-31'
OOS_START = '2024-01-01'
MODEL_PATH = ROOT / 'runtime' / 'models' / 'lgbm_factor_synthesis_v1.pkl'
SHAP_PATH = ROOT / 'runtime' / 'models' / 'lgbm_shap_importance.json'
REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'synthetic_factor_report.json'
REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'
ALPHA_SUMMARY_PATH = ROOT / 'runtime' / 'alpha_research' / 'alpha101_ic_summary.csv'
TOP_N = 50


def load_factor_names() -> list[str]:
    registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    names = [item['factor_name'] for item in registry]
    allowed = [name for name in names if name.startswith('alpha') or name == 'momentum_12_1']
    return allowed


def build_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    instruments = select_top_n_by_liquidity('stock', START, END, top_n=TOP_N)
    prices = load_prices(instruments, START, END, asset_type='stock')
    factor_input = load_factor_input(instruments, START, END, asset_type='stock').copy()
    factor_input['open'] = factor_input['close']
    return prices, factor_input, instruments


def compute_factor_series(name: str, factor_input: pd.DataFrame) -> pd.Series:
    if name.startswith('alpha'):
        func = getattr(alpha101, name)
    else:
        func = getattr(classic_factors, name)
    series = func(factor_input)
    if not isinstance(series, pd.Series):
        raise TypeError(f'{name} did not return Series')
    return series.rename(name)


def build_feature_frame(factor_names: list[str], prices: pd.DataFrame, factor_input: pd.DataFrame) -> pd.DataFrame:
    series_list = [compute_factor_series(name, factor_input) for name in factor_names]
    factors = pd.concat(series_list, axis=1)
    target = prices.pct_change(FORWARD_DAYS, fill_method=None).shift(-FORWARD_DAYS).stack(future_stack=True).rename('target_return')
    target.index = target.index.set_names(['date', 'asset'])
    frame = factors.join(target, how='inner').reset_index()
    frame['asset'] = frame['asset'].astype(str).str.zfill(6)
    frame['date'] = pd.to_datetime(frame['date'])
    frame = frame.dropna(subset=['target_return'])
    feature_cols = [col for col in frame.columns if col not in {'date', 'asset', 'target_return', 'target_rank'}]
    frame = frame.loc[frame[feature_cols].notna().any(axis=1)].copy()
    frame['target_rank'] = frame.groupby('date')['target_return'].rank(method='average', pct=True)
    return frame.sort_values(['date', 'asset']).reset_index(drop=True)


def normalize_with_cupy(frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for date, idx in out.groupby('date').groups.items():
        block = out.loc[idx, feature_cols].to_numpy(dtype=float, copy=True)
        gpu = cp.asarray(block)
        mean = cp.nanmean(gpu, axis=0)
        std = cp.nanstd(gpu, axis=0)
        std = cp.where(std == 0, 1.0, std)
        clipped = cp.clip(gpu, mean - 3.0 * std, mean + 3.0 * std)
        clipped_mean = cp.nanmean(clipped, axis=0)
        clipped_std = cp.nanstd(clipped, axis=0)
        clipped_std = cp.where(clipped_std == 0, 1.0, clipped_std)
        normalized = (clipped - clipped_mean) / clipped_std
        out.loc[idx, feature_cols] = cp.asnumpy(normalized)
    return out


def split_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = frame.loc[frame['date'] <= pd.Timestamp(TRAIN_END)].copy()
    valid = frame.loc[(frame['date'] >= pd.Timestamp(VALID_START)) & (frame['date'] <= pd.Timestamp(VALID_END))].copy()
    oos = frame.loc[frame['date'] >= pd.Timestamp(OOS_START)].copy()
    return train, valid, oos


def fit_model(train: pd.DataFrame, valid: pd.DataFrame, feature_cols: list[str]) -> tuple[lgb.LGBMRegressor, str]:
    params = {
        'objective': 'regression',
        'metric': 'l2',
        'num_leaves': 64,
        'learning_rate': 0.05,
        'n_estimators': 500,
        'verbosity': -1,
        'random_state': 42,
    }
    callbacks = [lgb.early_stopping(50, verbose=False)]
    for device in ['gpu', 'cpu']:
        try:
            model = lgb.LGBMRegressor(**params, device=device)
            model.fit(
                train[feature_cols],
                train['target_rank'],
                eval_set=[(valid[feature_cols], valid['target_rank'])],
                callbacks=callbacks,
            )
            return model, device
        except Exception:
            continue
    raise RuntimeError('LightGBM training failed on both gpu and cpu')


def ic_stats(frame: pd.DataFrame, score_col: str) -> dict[str, float | int | None]:
    ics: list[float] = []
    for _, cross in frame.groupby('date', sort=True):
        sample = cross[[score_col, 'target_return']].dropna()
        if len(sample) < 5 or sample[score_col].nunique() < 2 or sample['target_return'].nunique() < 2:
            continue
        ic = spearmanr(sample[score_col], sample['target_return']).statistic
        if ic is not None and np.isfinite(ic):
            ics.append(float(ic))
    if not ics:
        return {'mean': None, 'std': None, 'icir': None, 'count': 0}
    mean = float(np.mean(ics))
    std = float(np.std(ics))
    return {'mean': mean, 'std': std, 'icir': (mean / std if std else mean), 'count': len(ics)}


def best_single_factor(valid: pd.DataFrame, feature_cols: list[str]) -> tuple[str, dict[str, float | int | None]]:
    scored = {name: ic_stats(valid.rename(columns={name: 'score'}), 'score') for name in feature_cols}
    best_name = max(feature_cols, key=lambda name: (scored[name]['icir'] if scored[name]['icir'] is not None else -999.0))
    return best_name, scored[best_name]


def shap_importance(model: lgb.LGBMRegressor, valid: pd.DataFrame, feature_cols: list[str]) -> dict[str, float]:
    sample = valid[feature_cols].sample(min(5000, len(valid)), random_state=42) if len(valid) > 5000 else valid[feature_cols]
    explainer = shap.TreeExplainer(model)
    values = explainer.shap_values(sample)
    values = np.asarray(values, dtype=float)
    importance = np.abs(values).mean(axis=0)
    ranking = sorted(zip(feature_cols, importance, strict=True), key=lambda item: item[1], reverse=True)
    return {name: float(value) for name, value in ranking}


def alphalens_stats(predictions: pd.Series, prices: pd.DataFrame) -> dict[str, float | int | None]:
    clean = al.utils.get_clean_factor_and_forward_returns(
        factor=predictions,
        prices=prices,
        periods=(FORWARD_DAYS,),
        quantiles=5,
        max_loss=1.0,
    )
    if clean.empty:
        clean = al.utils.get_clean_factor_and_forward_returns(
            factor=predictions,
            prices=prices,
            periods=(FORWARD_DAYS,),
            bins=5,
            max_loss=1.0,
        )
    ic = al.performance.factor_information_coefficient(clean)
    series = ic.iloc[:, 0].dropna()
    if series.empty:
        return {'ic_mean': None, 'ic_std': None, 'icir': None, 'count': 0}
    mean = float(series.mean())
    std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
    return {'ic_mean': mean, 'ic_std': std, 'icir': (mean / std if std else mean), 'count': int(len(series))}


def main() -> None:
    factor_names = load_factor_names()
    prices, factor_input, instruments = build_market_inputs()
    dataset = build_feature_frame(factor_names, prices, factor_input)
    feature_cols = [col for col in dataset.columns if col not in {'date', 'asset', 'target_return', 'target_rank'}]
    dataset = normalize_with_cupy(dataset, feature_cols)
    train, valid, oos = split_frame(dataset)
    model, device = fit_model(train, valid, feature_cols)

    valid = valid.copy()
    oos = oos.copy()
    valid['prediction'] = model.predict(valid[feature_cols])
    oos['prediction'] = model.predict(oos[feature_cols]) if not oos.empty else np.nan
    valid_stats = ic_stats(valid, 'prediction')
    best_name, best_stats = best_single_factor(valid, feature_cols)
    shap_rank = shap_importance(model, valid, feature_cols)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open('wb') as handle:
        pickle.dump({'model': model, 'feature_columns': feature_cols, 'device': device}, handle)
    SHAP_PATH.write_text(json.dumps(shap_rank, ensure_ascii=False, indent=2), encoding='utf-8')

    valid_pred_series = valid.set_index(['date', 'asset'])['prediction']
    valid_pred_series.index = valid_pred_series.index.set_names(['date', 'asset'])
    best_factor_series = valid.set_index(['date', 'asset'])[best_name]
    best_factor_series.index = best_factor_series.index.set_names(['date', 'asset'])
    valid_prices = prices.loc[prices.index >= pd.Timestamp(VALID_START)].copy()
    synthetic_alphalens = alphalens_stats(valid_pred_series, valid_prices)
    best_alphalens = alphalens_stats(best_factor_series, valid_prices)

    report = {
        'device': device,
        'factor_names': factor_names,
        'feature_count': len(feature_cols),
        'train_rows': int(len(train)),
        'valid_rows': int(len(valid)),
        'oos_rows': int(len(oos)),
        'validation_ic': valid_stats,
        'best_single_factor': {'factor_name': best_name, **best_stats},
        'synthetic_alphalens': synthetic_alphalens,
        'best_single_factor_alphalens': best_alphalens,
        'shap_top10': list(shap_rank.items())[:10],
        'model_path': str(MODEL_PATH),
        'shap_path': str(SHAP_PATH),
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

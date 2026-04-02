from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import alphalens as al
import lightgbm as lgb
import numpy as np
import pandas as pd

from alpha_research.data_loader import load_factor_input
from alpha_research.data_loader import load_prices
from alpha_research.data_loader import select_top_n_by_liquidity
from alpha_research.factors import alpha101
from alpha_research.factors import classic_factors


ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / 'runtime' / 'models' / 'lgbm_factor_synthesis_v1.pkl'
SELECTED_FACTORS_PATH = ROOT / 'runtime' / 'factor_registry' / 'selected_factors.json'
REPORT_PATH = ROOT / 'runtime' / 'alpha_research' / 'wfo_report.json'
LOOKBACK_START = '2015-01-01'
COVERAGE_START = pd.Timestamp('2016-01-01')
COVERAGE_END = pd.Timestamp('2023-12-31')
FORWARD_DAYS = 5
TOP_N = 50


@dataclass(slots=True)
class WFOFold:
    fold: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    val_start: pd.Timestamp
    val_end: pd.Timestamp

    def label(self) -> dict[str, str]:
        return {
            'fold': self.fold,
            'train': f"{self.train_start.strftime('%Y-%m')}~{self.train_end.strftime('%Y-%m')}",
            'val': f"{self.val_start.strftime('%Y-%m')}~{self.val_end.strftime('%Y-%m')}",
        }


def generate_wfo_folds(
    start: pd.Timestamp = COVERAGE_START,
    end: pd.Timestamp = COVERAGE_END,
    train_months: int = 36,
    val_months: int = 6,
    step_months: int = 6,
) -> list[WFOFold]:
    folds: list[WFOFold] = []
    train_start = pd.Timestamp(start.year, start.month, 1)
    fold_no = 1
    while True:
        train_end = train_start + pd.DateOffset(months=train_months) - pd.Timedelta(days=1)
        val_start = train_end + pd.Timedelta(days=1)
        val_end = val_start + pd.DateOffset(months=val_months) - pd.Timedelta(days=1)
        if val_end > end:
            break
        folds.append(WFOFold(fold=fold_no, train_start=train_start, train_end=train_end, val_start=val_start, val_end=val_end))
        train_start = train_start + pd.DateOffset(months=step_months)
        fold_no += 1
    return folds


class WFOValidator:
    def __init__(self) -> None:
        self.model_params = self._load_model_params()
        self.selected_factors = self._load_selected_factors()

    def _load_model_params(self) -> dict[str, Any]:
        payload = pickle.load(MODEL_PATH.open('rb'))
        model = payload['model']
        params = model.get_params()
        params['device'] = 'gpu'
        params['verbosity'] = -1
        return params

    def _load_selected_factors(self) -> list[str]:
        payload = json.loads(SELECTED_FACTORS_PATH.read_text(encoding='utf-8'))
        return list(payload.get('selected', []))

    def _compute_factor_series(self, factor_name: str, factor_input: pd.DataFrame) -> pd.Series:
        if factor_name.startswith('alpha'):
            func = getattr(alpha101, factor_name)
        else:
            func = getattr(classic_factors, factor_name)
        series = func(factor_input)
        if not isinstance(series, pd.Series):
            raise TypeError(f'{factor_name} must return a Series')
        return series.rename(factor_name)

    def build_dataset(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        instruments = select_top_n_by_liquidity('stock', LOOKBACK_START, str(COVERAGE_END.date()), top_n=TOP_N)
        prices = load_prices(instruments, LOOKBACK_START, str(COVERAGE_END + pd.Timedelta(days=10)), asset_type='stock')
        factor_input = load_factor_input(instruments, LOOKBACK_START, str(COVERAGE_END + pd.Timedelta(days=10)), asset_type='stock').copy()
        factor_input['open'] = factor_input['close']

        factor_series = [self._compute_factor_series(name, factor_input) for name in self.selected_factors]
        factors = pd.concat(factor_series, axis=1)
        future_return = prices.pct_change(FORWARD_DAYS, fill_method=None).shift(-FORWARD_DAYS).stack(future_stack=True).rename('target_return')
        future_return.index = future_return.index.set_names(['date', 'asset'])
        frame = factors.join(future_return, how='inner').reset_index()
        frame['date'] = pd.to_datetime(frame['date'])
        frame['asset'] = frame['asset'].astype(str).str.zfill(6)
        frame = frame.loc[(frame['date'] >= COVERAGE_START) & (frame['date'] <= COVERAGE_END)].copy()
        feature_cols = [col for col in frame.columns if col not in {'date', 'asset', 'target_return', 'target_rank'}]
        frame['target_rank'] = frame.groupby('date')['target_return'].rank(method='average', pct=True)
        frame = self._normalize_features(frame, feature_cols)
        return frame.sort_values(['date', 'asset']).reset_index(drop=True), prices

    def _normalize_features(self, frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
        out = frame.copy()
        for date, idx in out.groupby('date').groups.items():
            block = out.loc[idx, feature_cols].astype(float)
            lower = block.mean() - 3.0 * block.std(ddof=0).replace(0, np.nan)
            upper = block.mean() + 3.0 * block.std(ddof=0).replace(0, np.nan)
            clipped = block.clip(lower=lower, upper=upper, axis=1)
            zscore = (clipped - clipped.mean()) / clipped.std(ddof=0).replace(0, np.nan)
            out.loc[idx, feature_cols] = zscore.to_numpy()
        return out

    def _fit_fold_model(self, train_frame: pd.DataFrame, feature_cols: list[str]) -> lgb.LGBMRegressor:
        model = lgb.LGBMRegressor(**self.model_params)
        model.fit(train_frame[feature_cols], train_frame['target_rank'])
        return model

    def _evaluate_fold(self, model: lgb.LGBMRegressor, val_frame: pd.DataFrame, prices: pd.DataFrame, fold: WFOFold) -> dict[str, Any]:
        scored = val_frame.copy()
        feature_cols = [col for col in scored.columns if col not in {'date', 'asset', 'target_return', 'target_rank'}]
        scored['prediction'] = model.predict(scored[feature_cols])
        factor = scored.set_index(['date', 'asset'])['prediction'].sort_index()
        factor.index = factor.index.set_names(['date', 'asset'])
        price_slice = prices.loc[(prices.index >= fold.val_start - pd.Timedelta(days=5)) & (prices.index <= fold.val_end + pd.Timedelta(days=10))]
        clean = al.utils.get_clean_factor_and_forward_returns(
            factor=factor,
            prices=price_slice,
            periods=(FORWARD_DAYS,),
            quantiles=5,
            max_loss=1.0,
        )
        ic = al.performance.factor_information_coefficient(clean)
        ic_series = ic.iloc[:, 0].dropna()
        ic_mean = float(ic_series.mean()) if not ic_series.empty else 0.0
        ic_std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
        icir = ic_mean / ic_std if ic_std else ic_mean
        result = fold.label()
        result.update({'ic_mean': ic_mean, 'icir': icir})
        return result

    def run(self) -> dict[str, Any]:
        dataset, prices = self.build_dataset()
        feature_cols = [col for col in dataset.columns if col not in {'date', 'asset', 'target_return', 'target_rank'}]
        folds = generate_wfo_folds()
        results: list[dict[str, Any]] = []
        for fold in folds:
            train_mask = (dataset['date'] >= fold.train_start) & (dataset['date'] <= fold.train_end)
            val_mask = (dataset['date'] >= fold.val_start) & (dataset['date'] <= fold.val_end)
            train_frame = dataset.loc[train_mask].dropna(subset=['target_rank']).copy()
            val_frame = dataset.loc[val_mask].dropna(subset=['target_rank']).copy()
            if train_frame.empty or val_frame.empty:
                continue
            model = self._fit_fold_model(train_frame, feature_cols)
            results.append(self._evaluate_fold(model, val_frame, prices, fold))
        icirs = [float(item['icir']) for item in results]
        mean_icir = float(np.mean(icirs)) if icirs else 0.0
        std_icir = float(np.std(icirs, ddof=0)) if icirs else 0.0
        if std_icir < 0.05:
            stability = 'stable'
        elif std_icir <= 0.10:
            stability = 'moderate'
        else:
            stability = 'unstable'
        report = {
            'folds': results,
            'summary': {
                'mean_icir': mean_icir,
                'std_icir': std_icir,
                'stability': stability,
            },
        }
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        return report

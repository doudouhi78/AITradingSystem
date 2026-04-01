from __future__ import annotations

import json
import pickle
from dataclasses import dataclass, field
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_runtime_root() -> Path:
    return _project_root() / "runtime"


def _default_data_root() -> Path:
    from alpha_research.feature_factory import _default_data_root as feature_default_data_root

    return feature_default_data_root()


@dataclass(slots=True)
class LGBMTrainer:
    training_window_years: int = 5
    validation_ratio: float = 0.2
    forward_days: int = 20
    max_train_year: int = 2024
    runtime_root: Path = field(default_factory=_default_runtime_root)
    data_root: Path = field(default_factory=_default_data_root)
    model_params: dict[str, object] = field(
        default_factory=lambda: {
            "objective": "regression",
            "metric": "l2",
            "learning_rate": 0.05,
            "n_estimators": 300,
            "num_leaves": 31,
            "min_data_in_leaf": 50,
            "feature_fraction": 0.8,
            "verbosity": -1,
            "random_state": 42,
        }
    )
    models: dict[int, dict[str, object]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.runtime_root = Path(self.runtime_root)
        self.data_root = Path(self.data_root)

    @property
    def model_dir(self) -> Path:
        return self.runtime_root / "alpha_research" / "models"

    @property
    def market_data_dir(self) -> Path:
        return self.data_root / "market_data" / "cn_stock"

    @property
    def feature_dir(self) -> Path:
        return self.runtime_root / "alpha_research" / "feature_matrix"

    def fit(self, feature_matrix: pd.DataFrame | None = None) -> dict[int, dict[str, object]]:
        feature_frame = self.load_feature_matrix() if feature_matrix is None else feature_matrix.copy()
        training_frame = self.prepare_training_frame(feature_frame)
        if training_frame.empty:
            return {}

        years = sorted(training_frame["trade_date"].dt.year.unique().tolist())
        available_model_years = [
            year
            for year in years
            if year <= self.max_train_year and year - self.training_window_years >= min(years)
        ]
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.models = {}

        for model_year in available_model_years:
            train_start_year = model_year - self.training_window_years
            train_mask = (
                (training_frame["trade_date"].dt.year >= train_start_year)
                & (training_frame["trade_date"].dt.year < model_year)
            )
            oos_mask = training_frame["trade_date"].dt.year == model_year
            train_window = training_frame.loc[train_mask].copy()
            oos_frame = training_frame.loc[oos_mask].copy()
            if train_window.empty:
                continue

            train_frame, valid_frame = self._split_train_valid(train_window)
            feature_columns = [column for column in train_window.columns if column not in {"trade_date", "symbol", "target"}]
            if not feature_columns:
                continue
            model, actual_device = self._train_single_model(train_frame, valid_frame, feature_columns)
            importance = self._feature_importance(model, feature_columns)
            evaluation = {
                "model_year": model_year,
                "train_window_years": [train_start_year, model_year - 1],
                "actual_device": actual_device,
                "train_rows": int(len(train_frame)),
                "valid_rows": int(len(valid_frame)),
                "oos_rows": int(len(oos_frame)),
                "in_sample_ic": self._mean_ic(train_window, model, feature_columns),
                "validation_ic": self._mean_ic(valid_frame, model, feature_columns),
                "out_of_sample_ic": self._mean_ic(oos_frame, model, feature_columns),
            }
            self._persist_model_artifacts(model_year, model, feature_columns, actual_device, importance, evaluation)
            self.models[model_year] = {
                "model": model,
                "feature_columns": feature_columns,
                "device": actual_device,
                "importance": importance,
                "evaluation": evaluation,
            }
        return self.models

    def predict(self, date: str | pd.Timestamp, feature_matrix: pd.DataFrame) -> pd.Series:
        if not self.models:
            raise ValueError("trainer has no fitted models")
        timestamp = pd.Timestamp(date)
        candidate_years = sorted(year for year in self.models if year <= timestamp.year)
        if not candidate_years:
            candidate_years = sorted(self.models)
        model_year = candidate_years[-1]
        model_bundle = self.models[model_year]
        row_frame = self._slice_prediction_frame(feature_matrix, timestamp, model_bundle["feature_columns"])
        if row_frame.empty:
            return pd.Series(dtype=float)
        scores = model_bundle["model"].predict(row_frame[model_bundle["feature_columns"]])
        return pd.Series(scores, index=row_frame["symbol"], name="score", dtype=float).sort_index()

    def load_feature_matrix(self, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
        if not self.feature_dir.exists():
            return pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])
        frames = [pd.read_parquet(path) for path in sorted(self.feature_dir.glob("*.parquet"))]
        if not frames:
            return pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])
        frame = pd.concat(frames, ignore_index=True)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        if start_date:
            frame = frame.loc[frame["trade_date"] >= pd.Timestamp(start_date)]
        if end_date:
            frame = frame.loc[frame["trade_date"] <= pd.Timestamp(end_date)]
        return frame.reset_index(drop=True)

    def prepare_training_frame(self, feature_matrix: pd.DataFrame) -> pd.DataFrame:
        if feature_matrix.empty:
            return pd.DataFrame(columns=["trade_date", "symbol", "target"])
        frame = feature_matrix.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        frame = frame.loc[frame["trade_date"].dt.year <= self.max_train_year].copy()
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "symbol", "target"])

        wide = (
            frame.pivot_table(index=["trade_date", "symbol"], columns="factor_name", values="value", aggfunc="last")
            .sort_index()
            .reset_index()
        )
        target_frame = self._build_target_frame(
            symbols=wide["symbol"].drop_duplicates().tolist(),
            start_date=wide["trade_date"].min(),
            end_date=wide["trade_date"].max(),
        )
        training = wide.merge(target_frame, how="inner", on=["trade_date", "symbol"])
        return training.sort_values(["trade_date", "symbol"]).reset_index(drop=True)

    def _slice_prediction_frame(
        self,
        feature_matrix: pd.DataFrame,
        trade_date: pd.Timestamp,
        feature_columns: list[str],
    ) -> pd.DataFrame:
        frame = feature_matrix.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        if {"factor_name", "value"}.issubset(frame.columns):
            frame = frame.loc[frame["trade_date"] == trade_date]
            if frame.empty:
                return pd.DataFrame(columns=["symbol", *feature_columns])
            wide = (
                frame.pivot_table(index=["trade_date", "symbol"], columns="factor_name", values="value", aggfunc="last")
                .reset_index()
            )
        else:
            wide = frame.loc[frame["trade_date"] == trade_date].copy()
        for column in feature_columns:
            if column not in wide.columns:
                wide[column] = np.nan
        return wide[["symbol", *feature_columns]].copy()

    def _build_target_frame(self, symbols: list[str], start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        close_frames: list[pd.DataFrame] = []
        for symbol in symbols:
            path = self.market_data_dir / f"{symbol}.parquet"
            if not path.exists():
                continue
            frame = pd.read_parquet(path, columns=["trade_date", "close"]).copy()
            frame["trade_date"] = pd.to_datetime(frame["trade_date"])
            frame["symbol"] = symbol
            frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
            close_frames.append(frame)
        if not close_frames:
            return pd.DataFrame(columns=["trade_date", "symbol", "target"])

        close_frame = pd.concat(close_frames, ignore_index=True)
        close_frame = close_frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        close_frame["future_return"] = (
            close_frame.groupby("symbol", sort=False)["close"].shift(-self.forward_days) / close_frame["close"] - 1.0
        )
        close_frame = close_frame.loc[
            (close_frame["trade_date"] >= start_date) & (close_frame["trade_date"] <= end_date)
        ].copy()
        close_frame["target"] = close_frame.groupby("trade_date")["future_return"].rank(method="average", pct=True)
        return close_frame[["trade_date", "symbol", "target"]].dropna(subset=["target"])

    def _split_train_valid(self, train_window: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        unique_dates = sorted(train_window["trade_date"].drop_duplicates().tolist())
        if len(unique_dates) <= 1:
            return train_window.copy(), train_window.iloc[0:0].copy()
        split_index = max(1, int(len(unique_dates) * (1.0 - self.validation_ratio)))
        split_index = min(split_index, len(unique_dates) - 1)
        split_date = unique_dates[split_index - 1]
        train_frame = train_window.loc[train_window["trade_date"] <= split_date].copy()
        valid_frame = train_window.loc[train_window["trade_date"] > split_date].copy()
        return train_frame, valid_frame

    def _train_single_model(
        self,
        train_frame: pd.DataFrame,
        valid_frame: pd.DataFrame,
        feature_columns: list[str],
    ) -> tuple[lgb.LGBMRegressor, str]:
        gpu_params = {**self.model_params, "device": "gpu"}
        cpu_params = {**self.model_params, "device": "cpu"}

        try:
            model = self._fit_estimator(gpu_params, train_frame, valid_frame, feature_columns)
            return model, "gpu"
        except Exception:
            model = self._fit_estimator(cpu_params, train_frame, valid_frame, feature_columns)
            return model, "cpu"

    def _fit_estimator(
        self,
        params: dict[str, object],
        train_frame: pd.DataFrame,
        valid_frame: pd.DataFrame,
        feature_columns: list[str],
    ) -> lgb.LGBMRegressor:
        model = lgb.LGBMRegressor(**params)
        fit_kwargs: dict[str, object] = {}
        if not valid_frame.empty:
            fit_kwargs["eval_set"] = [(valid_frame[feature_columns], valid_frame["target"])]
            fit_kwargs["callbacks"] = [lgb.early_stopping(30, verbose=False)]
        model.fit(train_frame[feature_columns], train_frame["target"], **fit_kwargs)
        return model

    def _feature_importance(self, model: lgb.LGBMRegressor, feature_columns: list[str]) -> dict[str, float]:
        gains = np.asarray(model.booster_.feature_importance(importance_type="gain"), dtype=float)
        total = float(gains.sum())
        if total <= 0:
            return {feature: 0.0 for feature in feature_columns}
        return {feature: float(gain / total) for feature, gain in zip(feature_columns, gains, strict=True)}

    def _mean_ic(self, frame: pd.DataFrame, model: lgb.LGBMRegressor, feature_columns: list[str]) -> dict[str, float | int | None]:
        if frame.empty:
            return {"mean": None, "std": None, "count": 0}
        scored = frame.copy()
        scored["prediction"] = model.predict(scored[feature_columns])
        ics: list[float] = []
        for _, cross_section in scored.groupby("trade_date", sort=True):
            if cross_section["target"].nunique() < 2 or cross_section["prediction"].nunique() < 2:
                continue
            ic = spearmanr(cross_section["prediction"], cross_section["target"], nan_policy="omit").statistic
            if ic is not None and np.isfinite(ic):
                ics.append(float(ic))
        if not ics:
            return {"mean": None, "std": None, "count": 0}
        return {"mean": float(np.mean(ics)), "std": float(np.std(ics)), "count": len(ics)}

    def _persist_model_artifacts(
        self,
        model_year: int,
        model: lgb.LGBMRegressor,
        feature_columns: list[str],
        device: str,
        importance: dict[str, float],
        evaluation: dict[str, object],
    ) -> None:
        bundle = {
            "model": model,
            "feature_columns": feature_columns,
            "device": device,
        }
        with (self.model_dir / f"lgbm_{model_year}.pkl").open("wb") as handle:
            pickle.dump(bundle, handle)
        (self.model_dir / f"lgbm_{model_year}_importance.json").write_text(
            json.dumps(importance, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.model_dir / f"lgbm_{model_year}_eval.json").write_text(
            json.dumps(evaluation, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

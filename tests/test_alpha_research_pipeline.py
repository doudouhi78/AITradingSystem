from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_research.feature_factory import FeatureFactory
from alpha_research.lgbm_trainer import LGBMTrainer


REQUIRED_MARKET_COLUMNS = [
    "market",
    "symbol",
    "security_type",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjustment_mode",
    "is_suspended",
    "listed_date",
    "delisted_date",
]


def _write_market_file(path: Path, frame: pd.DataFrame) -> None:
    payload = frame.copy()
    for column in REQUIRED_MARKET_COLUMNS:
        if column not in payload.columns:
            payload[column] = ""
    payload = payload[REQUIRED_MARKET_COLUMNS]
    path.parent.mkdir(parents=True, exist_ok=True)
    payload.to_parquet(path, index=False)


def test_feature_factory_builds_monthly_matrix_and_skips_failed_factors(tmp_path: Path) -> None:
    data_root = tmp_path / "data_runtime"
    runtime_root = tmp_path / "runtime"
    market_dir = data_root / "market_data" / "cn_stock"
    dates = pd.bdate_range("2024-01-02", periods=25)
    symbols = ["000001", "000002", "000003"]

    for idx, symbol in enumerate(symbols, start=1):
        base = 10 + idx
        frame = pd.DataFrame(
            {
                "market": "CN",
                "symbol": symbol,
                "security_type": "stock",
                "trade_date": dates,
                "open": base + np.linspace(0.1, 1.1, len(dates)),
                "high": base + np.linspace(0.3, 1.3, len(dates)),
                "low": base + np.linspace(0.0, 1.0, len(dates)),
                "close": base + np.linspace(0.2, 1.2, len(dates)) + idx * 0.05,
                "volume": 1_000_000 + idx * 10_000 + np.arange(len(dates)) * 100,
                "amount": 20_000_000 + idx * 1_000_000 + np.arange(len(dates)) * 500,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "2010-01-01",
                "delisted_date": "",
            }
        )
        if symbol == "000002":
            frame.loc[frame.index[10], "is_suspended"] = True
        _write_market_file(market_dir / f"{symbol}.parquet", frame)

    _write_market_file(
        market_dir / "000004.parquet",
        pd.DataFrame(
            {
                "market": "CN",
                "symbol": "000004",
                "security_type": "stock",
                "trade_date": dates,
                "open": 9.0,
                "high": 9.2,
                "low": 8.8,
                "close": 9.1,
                "volume": 900_000,
                "amount": 9_000_000,
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "2010-01-01",
                "delisted_date": "",
            }
        ),
    )

    (data_root / "classification_data").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "instrument_code": ["000001", "000002", "000003", "000004"],
            "industry_name": ["Bank", "Bank", "Broker", "Utility"],
        }
    ).to_parquet(data_root / "classification_data" / "industry_sw2.parquet", index=False)

    (data_root / "fundamental_data").mkdir(parents=True, exist_ok=True)
    valuation = pd.DataFrame(
        [
            {
                "date": date,
                "instrument_code": symbol,
                "total_mv": 1.0e7 + idx * 1.0e6 + i * 1.0e5,
                "circ_mv": 8.0e6 + idx * 1.0e6,
            }
            for i, date in enumerate(dates)
            for idx, symbol in enumerate(["000001", "000002", "000003", "000004"], start=1)
        ]
    )
    valuation.to_parquet(data_root / "fundamental_data" / "valuation_daily.parquet", index=False)

    (data_root / "download_log").mkdir(parents=True, exist_ok=True)
    (data_root / "download_log" / "abnormal_files.json").write_text(
        json.dumps([{"symbol": "000004"}], ensure_ascii=False),
        encoding="utf-8",
    )

    registry_path = runtime_root / "alpha_research" / "factor_registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            [
                {"factor_name": "alpha101", "icir_neutralized": 0.12},
                {"factor_name": "alpha088", "icir_neutralized": 0.10},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    factory = FeatureFactory(
        data_root=data_root,
        runtime_root=runtime_root,
        factor_registry_paths=(registry_path,),
        use_torch_if_available=False,
    )
    feature_matrix = factory.build_feature_matrix(
        symbols=["000001", "000002", "000003", "000004"],
        factor_names=["alpha101", "alpha088"],
    )

    assert set(feature_matrix.columns) == {"trade_date", "symbol", "factor_name", "value"}
    assert feature_matrix["factor_name"].eq("alpha101").all()
    assert "000004" not in set(feature_matrix["symbol"])
    suspended_date = dates[10]
    suspended_rows = feature_matrix.loc[
        (feature_matrix["trade_date"] == suspended_date) & (feature_matrix["symbol"] == "000002")
    ]
    assert suspended_rows.empty
    means = feature_matrix.groupby("trade_date")["value"].mean().abs()
    assert (means < 1e-8).all()
    assert (runtime_root / "alpha_research" / "feature_matrix" / "202401.parquet").exists()

    failures = json.loads((runtime_root / "alpha_research" / "feature_factory_failures.json").read_text(encoding="utf-8"))
    assert any(item["factor_name"] == "alpha088" for item in failures)


def test_lgbm_trainer_trains_with_cpu_fallback_and_predicts(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data_runtime"
    runtime_root = tmp_path / "runtime"
    market_dir = data_root / "market_data" / "cn_stock"
    dates = pd.bdate_range("2022-01-03", "2024-12-31")
    symbols = [f"{idx:06d}" for idx in range(1, 7)]
    rng = np.random.default_rng(7)
    feature_rows: list[dict[str, object]] = []

    for symbol_idx, symbol in enumerate(symbols):
        latent = np.sin(np.arange(len(dates)) / 17.0 + symbol_idx / 3.0) + symbol_idx * 0.15
        noise = rng.normal(0.0, 0.2, len(dates))
        returns = 0.0015 * latent + rng.normal(0.0, 0.0005, len(dates))
        close = 20.0 * np.cumprod(1.0 + returns)
        market_frame = pd.DataFrame(
            {
                "market": "CN",
                "symbol": symbol,
                "security_type": "stock",
                "trade_date": dates,
                "open": close * 0.995,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1_000_000 + symbol_idx * 100_000,
                "amount": close * (1_000_000 + symbol_idx * 100_000),
                "adjustment_mode": "qfq",
                "is_suspended": False,
                "listed_date": "2010-01-01",
                "delisted_date": "",
            }
        )
        _write_market_file(market_dir / f"{symbol}.parquet", market_frame)
        for trade_date, factor_alpha, factor_noise in zip(dates, latent, noise, strict=True):
            feature_rows.append({"trade_date": trade_date, "symbol": symbol, "factor_name": "factor_alpha", "value": float(factor_alpha)})
            feature_rows.append({"trade_date": trade_date, "symbol": symbol, "factor_name": "factor_noise", "value": float(factor_noise)})

    feature_matrix = pd.DataFrame(feature_rows)
    trainer = LGBMTrainer(
        training_window_years=1,
        validation_ratio=0.2,
        runtime_root=runtime_root,
        data_root=data_root,
        model_params={
            "objective": "regression",
            "metric": "l2",
            "learning_rate": 0.05,
            "n_estimators": 80,
            "num_leaves": 15,
            "min_data_in_leaf": 5,
            "feature_fraction": 0.8,
            "verbosity": -1,
            "random_state": 42,
        },
    )

    original_fit = LGBMTrainer._fit_estimator

    def forcing_cpu(self, params, train_frame, valid_frame, feature_columns):
        if params["device"] == "gpu":
            raise RuntimeError("gpu unavailable in test")
        return original_fit(self, params, train_frame, valid_frame, feature_columns)

    monkeypatch.setattr(LGBMTrainer, "_fit_estimator", forcing_cpu)
    models = trainer.fit(feature_matrix)

    assert models
    assert set(models).issuperset({2023, 2024})
    assert models[2024]["device"] == "cpu"
    assert (runtime_root / "alpha_research" / "models" / "lgbm_2024.pkl").exists()
    importance = json.loads((runtime_root / "alpha_research" / "models" / "lgbm_2024_importance.json").read_text(encoding="utf-8"))
    assert set(importance) == {"factor_alpha", "factor_noise"}
    assert abs(sum(importance.values()) - 1.0) < 1e-6 or sum(importance.values()) == 0.0

    prediction_date = pd.Timestamp("2024-12-02")
    scores = trainer.predict(prediction_date, feature_matrix)
    assert list(scores.index) == sorted(symbols)
    assert np.isfinite(scores.to_numpy()).all()

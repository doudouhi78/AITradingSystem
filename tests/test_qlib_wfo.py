from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _load_module():
    script_path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_qlib_wfo_validation.py'
    spec = importlib.util.spec_from_file_location('run_qlib_wfo_validation', script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _mock_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range('2016-01-01', '2023-12-31')
    assets = ['000001.SZ', '000002.SZ', '000003.SZ', '000004.SZ', '600519.SH']
    base_scores = pd.Series([2.0, 1.0, 0.0, -1.0, -2.0], index=assets)
    factor = pd.DataFrame([base_scores.values for _ in dates], index=dates, columns=assets)

    prices = pd.DataFrame(index=dates, columns=assets, dtype=float)
    for idx, asset in enumerate(assets):
        price = 100.0 + idx * 5.0
        series = []
        score = base_scores[asset]
        for day in range(len(dates)):
            price *= 1.0 + 0.0005 + score * 0.0002 + np.sin(day / 20.0) * 0.00005
            series.append(price)
        prices[asset] = series
    return factor, prices


def test_build_qlib_wfo_report_uses_wfo_splits() -> None:
    module = _load_module()
    factor, prices = _mock_frames()

    report = module.build_qlib_wfo_report(
        factor,
        prices,
        factor_name='qlib_alstm_v1',
        forward_days=5,
        train_months=36,
        val_months=6,
        step_months=6,
        start_date='2016-01-01',
        end_date='2023-12-31',
    )

    assert report['factor_name'] == 'qlib_alstm_v1'
    assert report['folds'] == 10
    assert len(report['fold_results']) == 10
    assert report['stability'] in {'high', 'moderate', 'low'}
    assert report['fold_results'][0]['fold'] == 1
    assert report['fold_results'][0]['train'].startswith('2016-01')


def test_run_qlib_wfo_validation_writes_expected_report(tmp_path) -> None:
    module = _load_module()
    factor, prices = _mock_frames()
    factor_path = tmp_path / 'qlib_factor.parquet'
    prices_path = tmp_path / 'prices.parquet'
    output_path = tmp_path / 'qlib_wfo_report.json'
    factor.to_parquet(factor_path)
    prices.to_parquet(prices_path)

    report = module.build_qlib_wfo_report(
        factor,
        prices,
        factor_name='qlib_alstm_v1',
        forward_days=5,
        train_months=36,
        val_months=6,
        step_months=6,
        start_date='2016-01-01',
        end_date='2023-12-31',
    )
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    saved = json.loads(output_path.read_text(encoding='utf-8'))

    assert set(saved) == {'factor_name', 'folds', 'mean_icir', 'std_icir', 'stability', 'fold_results'}
    assert isinstance(saved['fold_results'], list)
    assert saved['folds'] == len(saved['fold_results'])
    assert 'icir' in saved['fold_results'][0]
    assert saved['mean_icir'] >= 0.0

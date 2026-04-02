from __future__ import annotations

import pandas as pd
import pytest

from alpha_research.factors import moneyflow_factors


@pytest.fixture()
def moneyflow_setup(tmp_path, monkeypatch):
    dates = pd.date_range('2024-01-01', periods=25, freq='B')
    rows = []
    for asset, multiplier in [('000001', 1.0), ('000002', 2.0)]:
        for idx, date in enumerate(dates, start=1):
            rows.append({
                'trade_date': date,
                'ts_code': f'{asset}.SZ',
                'net_mf_amount': multiplier * idx,
                'buy_elg_amount': multiplier * 2.0,
                'sell_elg_amount': multiplier * 1.0,
            })
    moneyflow = pd.DataFrame(rows)
    moneyflow_path = tmp_path / 'moneyflow.parquet'
    moneyflow.to_parquet(moneyflow_path, index=False)
    monkeypatch.setattr(moneyflow_factors, 'MONEYFLOW_PATH', moneyflow_path)
    monkeypatch.setattr(moneyflow_factors, 'VALUATION_PATH', tmp_path / 'missing_valuation.parquet')

    factor_rows = []
    for asset, multiplier in [('000001', 1.0), ('000002', 2.0)]:
        for idx, date in enumerate(dates, start=1):
            factor_rows.append({
                'date': date,
                'asset': asset,
                'close': 10.0 + multiplier + idx * 0.01,
                'amount': multiplier * 10.0,
                'total_mv': multiplier * 100.0,
            })
    factor_input = pd.DataFrame(factor_rows).set_index(['date', 'asset']).sort_index()
    return factor_input


def test_moneyflow_factor_module_imports() -> None:
    assert hasattr(moneyflow_factors, 'mf_net_inflow_5d')
    assert hasattr(moneyflow_factors, 'mf_inflow_acceleration')


def test_moneyflow_factors_compute_expected_values(moneyflow_setup) -> None:
    factor_input = moneyflow_setup

    inflow_5d = moneyflow_factors.mf_net_inflow_5d(factor_input)
    inflow_20d = moneyflow_factors.mf_net_inflow_20d(factor_input)
    large_ratio = moneyflow_factors.mf_large_order_ratio(factor_input)
    smart_money = moneyflow_factors.mf_smart_money(factor_input)
    acceleration = moneyflow_factors.mf_inflow_acceleration(factor_input)

    assert inflow_5d.loc[(pd.Timestamp('2024-02-02'), '000001')] == pytest.approx(1.10)
    assert inflow_5d.loc[(pd.Timestamp('2024-02-02'), '000002')] == pytest.approx(1.10)
    assert inflow_20d.loc[(pd.Timestamp('2024-02-02'), '000001')] == pytest.approx(2.90)
    assert large_ratio.loc[(pd.Timestamp('2024-02-02'), '000001')] == pytest.approx(2.0 / 3.0)
    assert smart_money.loc[(pd.Timestamp('2024-02-02'), '000001')] == pytest.approx(0.10)
    assert acceleration.loc[(pd.Timestamp('2024-02-02'), '000001')] == pytest.approx(-180.0)


def test_moneyflow_factors_handle_empty_and_single_day_inputs(tmp_path, monkeypatch) -> None:
    single_day = pd.DataFrame([
        {
            'trade_date': pd.Timestamp('2024-01-02'),
            'ts_code': '000001.SZ',
            'net_mf_amount': 10.0,
            'buy_elg_amount': 6.0,
            'sell_elg_amount': 4.0,
        }
    ])
    moneyflow_path = tmp_path / 'moneyflow.parquet'
    single_day.to_parquet(moneyflow_path, index=False)
    monkeypatch.setattr(moneyflow_factors, 'MONEYFLOW_PATH', moneyflow_path)
    monkeypatch.setattr(moneyflow_factors, 'VALUATION_PATH', tmp_path / 'missing_valuation.parquet')

    empty_index = pd.MultiIndex.from_arrays([[], []], names=['date', 'asset'])
    empty_input = pd.DataFrame(columns=['amount', 'total_mv'], index=empty_index)
    empty_result = moneyflow_factors.mf_net_inflow_5d(empty_input)
    assert empty_result.empty
    assert empty_result.index.names == ['date', 'asset']

    single_input = pd.DataFrame([
        {'date': pd.Timestamp('2024-01-02'), 'asset': '000001', 'amount': 100.0, 'total_mv': 50.0}
    ]).set_index(['date', 'asset'])
    single_result = moneyflow_factors.mf_smart_money(single_input)
    assert single_result.empty


def test_moneyflow_factors_raise_clear_error_when_file_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(moneyflow_factors, 'MONEYFLOW_PATH', tmp_path / 'missing_moneyflow.parquet')
    monkeypatch.setattr(moneyflow_factors, 'VALUATION_PATH', tmp_path / 'missing_valuation.parquet')

    factor_input = pd.DataFrame([
        {'date': pd.Timestamp('2024-01-02'), 'asset': '000001', 'amount': 100.0, 'total_mv': 50.0}
    ]).set_index(['date', 'asset'])

    with pytest.raises(FileNotFoundError, match='Moneyflow data file not found'):
        moneyflow_factors.mf_net_inflow_5d(factor_input)


def test_moneyflow_factors_handle_nan_without_inf(tmp_path, monkeypatch) -> None:
    moneyflow = pd.DataFrame([
        {'trade_date': pd.Timestamp('2024-01-02'), 'ts_code': '000001.SZ', 'net_mf_amount': 10.0, 'buy_elg_amount': 5.0, 'sell_elg_amount': 0.0},
        {'trade_date': pd.Timestamp('2024-01-03'), 'ts_code': '000001.SZ', 'net_mf_amount': None, 'buy_elg_amount': None, 'sell_elg_amount': 0.0},
    ])
    moneyflow_path = tmp_path / 'moneyflow.parquet'
    moneyflow.to_parquet(moneyflow_path, index=False)
    monkeypatch.setattr(moneyflow_factors, 'MONEYFLOW_PATH', moneyflow_path)
    monkeypatch.setattr(moneyflow_factors, 'VALUATION_PATH', tmp_path / 'missing_valuation.parquet')

    factor_input = pd.DataFrame([
        {'date': pd.Timestamp('2024-01-02'), 'asset': '000001', 'amount': 100.0, 'total_mv': 50.0},
        {'date': pd.Timestamp('2024-01-03'), 'asset': '000001', 'amount': 0.0, 'total_mv': 50.0},
    ]).set_index(['date', 'asset'])

    ratio = moneyflow_factors.mf_large_order_ratio(factor_input)
    smart_money = moneyflow_factors.mf_smart_money(factor_input)

    assert not ratio.replace([float('inf'), float('-inf')], pd.NA).isna().all()
    assert not smart_money.replace([float('inf'), float('-inf')], pd.NA).isna().all()
    assert ratio.notna().all()
    assert smart_money.notna().all()

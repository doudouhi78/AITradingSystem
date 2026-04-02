from __future__ import annotations

import pandas as pd

from alpha_research.qlib_factor_extractor import convert_qlib_instrument, extract_factor_scores


def test_convert_qlib_instrument_reverses_exchange_prefix() -> None:
    assert convert_qlib_instrument('SH600519') == '600519.SH'
    assert convert_qlib_instrument('SZ000001') == '000001.SZ'
    assert convert_qlib_instrument('BJ430047') == '430047.BJ'
    assert convert_qlib_instrument('600519.SH') == '600519.SH'


def test_extract_factor_scores_converts_long_predictions_to_standard_factor(tmp_path) -> None:
    config_path = tmp_path / 'alstm_config.yaml'
    config_path.write_text('model:\n  class: ALSTM\n', encoding='utf-8')

    raw = pd.DataFrame(
        {
            'datetime': ['2024-01-02', '2024-01-02', '2024-01-03', '2024-01-03'],
            'instrument': ['SH600519', 'SZ000001', 'SH600519', 'SZ000001'],
            'score': [2.0, 1.0, 1.0, 3.0],
        }
    )
    model_path = tmp_path / 'predictions.parquet'
    raw.to_parquet(model_path, index=False)
    output_path = tmp_path / 'factor.parquet'

    factor = extract_factor_scores(str(model_path), str(config_path), str(output_path))

    assert list(factor.columns) == ['000001.SZ', '600519.SH']
    assert factor.index.name == 'date'
    assert output_path.exists()
    assert factor.loc[pd.Timestamp('2024-01-02'), '600519.SH'] == 1.0
    assert factor.loc[pd.Timestamp('2024-01-02'), '000001.SZ'] == -1.0
    assert abs(float(factor.loc[pd.Timestamp('2024-01-03')].mean())) < 1e-9


def test_extract_factor_scores_accepts_wide_pickle_payload(tmp_path) -> None:
    config_path = tmp_path / 'tra_config.yaml'
    config_path.write_text('model:\n  class: TRA\n', encoding='utf-8')

    wide = pd.DataFrame(
        {
            'SH600519': [1.0, 2.0],
            'SZ000001': [3.0, 1.0],
        },
        index=pd.to_datetime(['2024-01-02', '2024-01-03']),
    )
    model_path = tmp_path / 'predictions.pkl'
    wide.to_pickle(model_path)
    output_path = tmp_path / 'wide_factor.parquet'

    factor = extract_factor_scores(str(model_path), str(config_path), str(output_path))

    assert list(factor.columns) == ['000001.SZ', '600519.SH']
    assert factor.loc[pd.Timestamp('2024-01-02'), '000001.SZ'] == 1.0
    assert factor.loc[pd.Timestamp('2024-01-02'), '600519.SH'] == -1.0
    pd.testing.assert_frame_equal(factor, pd.read_parquet(output_path))

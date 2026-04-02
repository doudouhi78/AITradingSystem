from __future__ import annotations

import pandas as pd
import pytest

from alpha_research.factor_combination import equal_weight, ic_weight, rank_weight


def sample_factors() -> dict[str, pd.DataFrame]:
    idx = pd.to_datetime(['2023-01-03', '2023-01-04'])
    cols = ['000001', '600519', '300750']
    f1 = pd.DataFrame([[1.0, 2.0, 3.0], [3.0, None, 1.0]], index=idx, columns=cols)
    f2 = pd.DataFrame([[3.0, 2.0, 1.0], [1.0, 2.0, None]], index=idx, columns=cols)
    return {'factor_a': f1, 'factor_b': f2}


def test_equal_weight_returns_aligned_frame() -> None:
    result = equal_weight(sample_factors())
    assert list(result.columns) == ['000001', '600519', '300750']
    assert result.shape == (2, 3)
    assert result.isna().sum().sum() == 0


def test_ic_weight_uses_scores_and_preserves_cross_section() -> None:
    result = ic_weight(sample_factors(), {'factor_a': 0.8, 'factor_b': 0.2})
    assert result.shape == (2, 3)
    assert result.isna().sum().sum() == 0
    assert result.loc[pd.Timestamp('2023-01-03'), '300750'] > result.loc[pd.Timestamp('2023-01-03'), '000001']


def test_ic_weight_falls_back_to_equal_weight_when_all_zero() -> None:
    factors = sample_factors()
    pd.testing.assert_frame_equal(ic_weight(factors, {'factor_a': 0.0, 'factor_b': 0.0}), equal_weight(factors))


def test_rank_weight_handles_nans_with_fill_zero() -> None:
    result = rank_weight(sample_factors())
    assert result.isna().sum().sum() == 0
    assert result.shape == (2, 3)


def test_single_factor_supported() -> None:
    single = {'factor_a': sample_factors()['factor_a']}
    result = equal_weight(single)
    assert result.shape == (2, 3)
    assert result.isna().sum().sum() == 0


def test_empty_factor_dict_raises() -> None:
    with pytest.raises(ValueError):
        equal_weight({})

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_research.factors import alpha101


IMPLEMENTED_ROUND_IDS = [
    21, 22, 25, 28, 30, 31, 32, 36, 41, 47, 56, 61, 68, 71, 72, 73, 81, 92, 98, 101,
]
UNIMPLEMENTED_IDS = [29, 48, 58, 63, 76, 88, 90, 96, 100]


def _sample_factor_input() -> pd.DataFrame:
    dates = pd.date_range("2022-01-01", periods=320, freq="D")
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    rows: list[dict[str, float | pd.Timestamp | str]] = []
    base_line = np.arange(len(dates), dtype=float)

    for idx, symbol in enumerate(symbols):
        angle = base_line / 11.0 + idx * 0.7
        close = 24.0 + idx * 0.15 + 0.03 * base_line + 2.4 * np.sin(angle) + 1.1 * np.cos(base_line / 37.0 - idx * 0.6)
        open_ = close * (1.0 + 0.006 * np.sin(angle / 3.0) + 0.003 * np.cos(base_line / 19.0 + idx))
        high = np.maximum(open_, close) * (1.012 + 0.001 * idx)
        low = np.minimum(open_, close) * (0.988 - 0.0005 * idx)
        volume = 1_100_000.0 + idx * 25_000.0 + 8_000.0 * base_line + 220_000.0 * np.sin(base_line / 9.0 + idx * 1.1) + 140_000.0 * np.cos(base_line / 17.0 - idx * 0.4)
        amount = close * volume * (1.0 + 0.01 * np.cos(angle))

        for date, open_v, high_v, low_v, close_v, volume_v, amount_v in zip(
            dates,
            open_,
            high,
            low,
            close,
            volume,
            amount,
            strict=True,
        ):
            rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "open": float(open_v),
                    "high": float(high_v),
                    "low": float(low_v),
                    "close": float(close_v),
                    "volume": float(volume_v),
                    "amount": float(amount_v),
                }
            )

    return pd.DataFrame(rows).set_index(["date", "symbol"]).sort_index()


def test_helper_operators_produce_expected_values() -> None:
    dates = pd.date_range("2024-01-01", periods=4, freq="D")
    frame = pd.DataFrame({"AAA": [1.0, 2.0, 3.0, 4.0], "BBB": [4.0, 3.0, 2.0, 1.0]}, index=dates)

    ranked = alpha101.rank(frame)
    shifted = alpha101.delay(frame, 1)
    diffed = alpha101.delta(frame, 2)
    summed = alpha101.ts_sum(frame, 2)
    decayed = alpha101.decay_linear(frame, 3)
    corr = alpha101.correlation(frame, frame, 3)

    assert ranked.loc[dates[-1], "AAA"] > ranked.loc[dates[-1], "BBB"]
    assert shifted.loc[dates[1], "AAA"] == 1.0
    assert diffed.loc[dates[2], "AAA"] == 2.0
    assert summed.loc[dates[1], "BBB"] == 7.0
    assert decayed.loc[dates[2], "AAA"] == pytest.approx((1.0 * 1 + 2.0 * 2 + 3.0 * 3) / 6.0)
    assert corr.loc[dates[-1], "AAA"] == pytest.approx(1.0)


def test_module_exposes_all_101_alpha_functions_and_aliases() -> None:
    assert len(alpha101.ALPHA_FUNCTIONS) == 101
    assert alpha101.factor_alpha001 is alpha101.alpha001
    assert alpha101.factor_alpha101 is alpha101.alpha101
    assert 101 in alpha101.IMPLEMENTED_ALPHA_IDS
    assert 29 not in alpha101.IMPLEMENTED_ALPHA_IDS
    assert len(alpha101.IMPLEMENTED_ALPHA_IDS) > 50


@pytest.mark.parametrize("alpha_id", [1, 5, 10, 15, 20])
def test_first_twenty_alphas_still_return_non_empty_multiindex_series(alpha_id: int) -> None:
    factor_input = _sample_factor_input()
    name = f"alpha{alpha_id:03d}"
    result = alpha101.ALPHA_FUNCTIONS[name](factor_input)
    assert isinstance(result, pd.Series)
    assert result.name == name
    assert isinstance(result.index, pd.MultiIndex)
    assert list(result.index.names) == ["date", "symbol"]
    assert not result.empty
    assert result.notna().any()


@pytest.mark.parametrize("alpha_id", IMPLEMENTED_ROUND_IDS)
def test_round_two_implemented_alphas_return_non_empty_multiindex_series(alpha_id: int) -> None:
    factor_input = _sample_factor_input()
    name = f"alpha{alpha_id:03d}"
    result = alpha101.ALPHA_FUNCTIONS[name](factor_input)
    assert isinstance(result, pd.Series)
    assert result.name == name
    assert isinstance(result.index, pd.MultiIndex)
    assert list(result.index.names) == ["date", "symbol"]
    assert not result.empty
    assert result.notna().any()


@pytest.mark.parametrize("alpha_id", UNIMPLEMENTED_IDS)
def test_unimplemented_alphas_raise_not_implemented(alpha_id: int) -> None:
    factor_input = _sample_factor_input()
    with pytest.raises(NotImplementedError):
        alpha101.ALPHA_FUNCTIONS[f"alpha{alpha_id:03d}"](factor_input)


def test_alpha101_matches_manual_formula() -> None:
    factor_input = _sample_factor_input()
    result = alpha101.alpha101(factor_input).unstack()
    expected = ((factor_input["close"] - factor_input["open"]) / ((factor_input["high"] - factor_input["low"]) + 0.001)).unstack()
    expected = expected.loc[result.index, result.columns]
    pd.testing.assert_frame_equal(result, expected)


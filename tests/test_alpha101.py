from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_research.factors import alpha101


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

    frame = pd.DataFrame(rows).set_index(["date", "symbol"]).sort_index()
    return frame


def test_helper_operators_produce_expected_values() -> None:
    dates = pd.date_range("2024-01-01", periods=4, freq="D")
    frame = pd.DataFrame(
        {
            "AAA": [1.0, 2.0, 3.0, 4.0],
            "BBB": [4.0, 3.0, 2.0, 1.0],
        },
        index=dates,
    )

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
    assert alpha101.IMPLEMENTED_ALPHA_IDS == tuple(range(1, 21))


def test_first_twenty_alphas_return_non_empty_multiindex_series() -> None:
    factor_input = _sample_factor_input()

    for alpha_id in range(1, 21):
        name = f"alpha{alpha_id:03d}"
        result = alpha101.ALPHA_FUNCTIONS[name](factor_input)
        assert isinstance(result, pd.Series), name
        assert result.name == name
        assert isinstance(result.index, pd.MultiIndex)
        assert list(result.index.names) == ["date", "symbol"]
        assert not result.empty, name
        assert result.notna().any(), name


def test_placeholder_alphas_raise_not_implemented() -> None:
    factor_input = _sample_factor_input()

    with pytest.raises(NotImplementedError):
        alpha101.alpha021(factor_input)
    with pytest.raises(NotImplementedError):
        alpha101.alpha050(factor_input)
    with pytest.raises(NotImplementedError):
        alpha101.alpha101(factor_input)


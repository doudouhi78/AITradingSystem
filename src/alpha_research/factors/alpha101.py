from __future__ import annotations

"""Alpha101 factor library.

Source: Kakushadze, Z. (2015), "101 Formulaic Alphas".

This module keeps the implementation in a single file so the factor engine can
load a uniform function surface. The first 20 factors are implemented with
common vectorized operators; the remaining functions are explicit placeholders
and raise ``NotImplementedError`` until they are filled in.
"""

from typing import Callable

import numpy as np
import pandas as pd


EPSILON = 1e-12
IMPLEMENTED_ALPHA_IDS = tuple(range(1, 21))


def _asset_level_name(df: pd.DataFrame) -> str:
    if isinstance(df.index, pd.MultiIndex) and len(df.index.names) >= 2:
        return df.index.names[-1] or "asset"
    return "asset"


def _require_multiindex(df: pd.DataFrame) -> None:
    if not isinstance(df.index, pd.MultiIndex) or df.index.nlevels < 2:
        raise ValueError("alpha101 factors require a MultiIndex index of (date, symbol/asset)")


def _pivot(df: pd.DataFrame, column: str) -> pd.DataFrame:
    _require_multiindex(df)
    if column not in df.columns:
        raise KeyError(f"missing required column: {column}")
    frame = pd.to_numeric(df[column], errors="coerce").unstack(level=-1)
    return frame.sort_index().astype(float)


def _stack_factor(frame: pd.DataFrame, name: str, asset_name: str) -> pd.Series:
    factor = frame.replace([np.inf, -np.inf], np.nan).stack(future_stack=True).dropna()
    factor.index.names = ["date", asset_name]
    factor.name = name
    return factor.astype(float)


def _load_inputs(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    asset_name = _asset_level_name(df)
    open_ = _pivot(df, "open")
    high = _pivot(df, "high")
    low = _pivot(df, "low")
    close = _pivot(df, "close")
    volume = _pivot(df, "volume").replace(0.0, np.nan)
    amount = _pivot(df, "amount")
    returns = close.pct_change(fill_method=None)
    vwap = amount.divide(volume).replace([np.inf, -np.inf], np.nan)
    adv20 = sma(volume, 20)
    return {
        "asset_name": asset_name,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "returns": returns,
        "vwap": vwap,
        "adv20": adv20,
    }


def rank(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rank(axis=1, pct=True)


def delay(frame: pd.DataFrame, period: int) -> pd.DataFrame:
    return frame.shift(period)


def delta(frame: pd.DataFrame, period: int) -> pd.DataFrame:
    return frame.diff(period)


def ts_sum(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).sum()


def sma(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).mean()


def stddev(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).std(ddof=0)


def correlation(left: pd.DataFrame, right: pd.DataFrame, window: int) -> pd.DataFrame:
    return left.rolling(window, min_periods=window).corr(right)


def covariance(left: pd.DataFrame, right: pd.DataFrame, window: int) -> pd.DataFrame:
    return left.rolling(window, min_periods=window).cov(right)


def signed_power(frame: pd.DataFrame, exponent: float) -> pd.DataFrame:
    return np.sign(frame) * np.power(np.abs(frame), exponent)


def scale(frame: pd.DataFrame, factor: float = 1.0) -> pd.DataFrame:
    denom = frame.abs().sum(axis=1).replace(0.0, np.nan)
    return frame.mul(factor).div(denom, axis=0)


def ts_rank(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).apply(
        lambda values: pd.Series(values).rank(pct=True).iloc[-1],
        raw=False,
    )


def ts_min(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).min()


def ts_max(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).max()


def ts_argmin(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).apply(lambda values: float(np.argmin(values) + 1), raw=True)


def ts_argmax(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).apply(lambda values: float(np.argmax(values) + 1), raw=True)


def product(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return frame.rolling(window, min_periods=window).apply(np.prod, raw=True)


def decay_linear(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    weights = np.arange(1, window + 1, dtype=float)
    weights /= weights.sum()
    return frame.rolling(window, min_periods=window).apply(lambda values: float(np.dot(values, weights)), raw=True)


def _not_implemented(alpha_id: int) -> str:
    return f"Alpha{alpha_id:03d} is not implemented yet."


def alpha001(df: pd.DataFrame) -> pd.Series:
    """Alpha001: rank(ts_argmax(signedpower(cond(returns<0,stddev(returns,20),close), 2), 5)) - 0.5."""

    data = _load_inputs(df)
    base = data["close"].where(data["returns"] >= 0, stddev(data["returns"], 20))
    factor = rank(ts_argmax(signed_power(base, 2), 5)) - 0.5
    return _stack_factor(factor, "alpha001", data["asset_name"])


def alpha002(df: pd.DataFrame) -> pd.Series:
    """Alpha002: -correlation(rank(delta(log(volume), 2)), rank((close-open)/open), 6)."""

    data = _load_inputs(df)
    log_volume = np.log(data["volume"])
    left = rank(delta(log_volume, 2))
    right = rank((data["close"] - data["open"]).divide(data["open"]).replace([np.inf, -np.inf], np.nan))
    factor = -correlation(left, right, 6)
    return _stack_factor(factor, "alpha002", data["asset_name"])


def alpha003(df: pd.DataFrame) -> pd.Series:
    """Alpha003: -correlation(rank(open), rank(volume), 10)."""

    data = _load_inputs(df)
    factor = -correlation(rank(data["open"]), rank(data["volume"]), 10)
    return _stack_factor(factor, "alpha003", data["asset_name"])


def alpha004(df: pd.DataFrame) -> pd.Series:
    """Alpha004: -ts_rank(rank(low), 9)."""

    data = _load_inputs(df)
    factor = -ts_rank(rank(data["low"]), 9)
    return _stack_factor(factor, "alpha004", data["asset_name"])


def alpha005(df: pd.DataFrame) -> pd.Series:
    """Alpha005: rank(open-sma(vwap,10)) * (-abs(rank(close-vwap)))."""

    data = _load_inputs(df)
    factor = rank(data["open"] - sma(data["vwap"], 10)) * (-rank((data["close"] - data["vwap"]).abs()))
    return _stack_factor(factor, "alpha005", data["asset_name"])


def alpha006(df: pd.DataFrame) -> pd.Series:
    """Alpha006: -correlation(open, volume, 10)."""

    data = _load_inputs(df)
    factor = -correlation(data["open"], data["volume"], 10)
    return _stack_factor(factor, "alpha006", data["asset_name"])


def alpha007(df: pd.DataFrame) -> pd.Series:
    """Alpha007: cond(volume>adv20, -ts_rank(abs(delta(close,7)),60)*sign(delta(close,7)), -1)."""

    data = _load_inputs(df)
    close_delta = delta(data["close"], 7)
    active = data["volume"] > data["adv20"]
    ranked = -ts_rank(close_delta.abs(), 60) * np.sign(close_delta)
    factor = ranked.where(active, -1.0)
    return _stack_factor(factor, "alpha007", data["asset_name"])


def alpha008(df: pd.DataFrame) -> pd.Series:
    """Alpha008: -rank(sum(open,5)*sum(returns,5)-delay(sum(open,5)*sum(returns,5),10))."""

    data = _load_inputs(df)
    signal = ts_sum(data["open"], 5) * ts_sum(data["returns"], 5)
    factor = -rank(signal - delay(signal, 10))
    return _stack_factor(factor, "alpha008", data["asset_name"])


def alpha009(df: pd.DataFrame) -> pd.Series:
    """Alpha009: conditional sign-flip based on 5-day extrema of close delta."""

    data = _load_inputs(df)
    close_delta = delta(data["close"], 1)
    positive = ts_min(close_delta, 5) > 0
    negative = ts_max(close_delta, 5) < 0
    factor = close_delta.where(positive | negative, -close_delta)
    return _stack_factor(factor, "alpha009", data["asset_name"])


def alpha010(df: pd.DataFrame) -> pd.Series:
    """Alpha010: rank(signedpower(cond(delta(close,1)<0,stddev(returns,20),close),2))."""

    data = _load_inputs(df)
    close_delta = delta(data["close"], 1)
    base = data["close"].where(close_delta >= 0, stddev(data["returns"], 20))
    factor = rank(signed_power(base, 2))
    return _stack_factor(factor, "alpha010", data["asset_name"])


def alpha011(df: pd.DataFrame) -> pd.Series:
    """Alpha011: (rank(ts_max(vwap-close,3))+rank(ts_min(vwap-close,3)))*rank(delta(volume,3))."""

    data = _load_inputs(df)
    spread = data["vwap"] - data["close"]
    factor = (rank(ts_max(spread, 3)) + rank(ts_min(spread, 3))) * rank(delta(data["volume"], 3))
    return _stack_factor(factor, "alpha011", data["asset_name"])


def alpha012(df: pd.DataFrame) -> pd.Series:
    """Alpha012: sign(delta(volume,1)) * (-delta(close,1))."""

    data = _load_inputs(df)
    factor = np.sign(delta(data["volume"], 1)) * (-delta(data["close"], 1))
    return _stack_factor(factor, "alpha012", data["asset_name"])


def alpha013(df: pd.DataFrame) -> pd.Series:
    """Alpha013: -rank(covariance(rank(close), rank(volume), 5))."""

    data = _load_inputs(df)
    factor = -rank(covariance(rank(data["close"]), rank(data["volume"]), 5))
    return _stack_factor(factor, "alpha013", data["asset_name"])


def alpha014(df: pd.DataFrame) -> pd.Series:
    """Alpha014: -rank(delta(returns,3)) * correlation(open, volume, 10)."""

    data = _load_inputs(df)
    factor = -rank(delta(data["returns"], 3)) * correlation(data["open"], data["volume"], 10)
    return _stack_factor(factor, "alpha014", data["asset_name"])


def alpha015(df: pd.DataFrame) -> pd.Series:
    """Alpha015: -sum(rank(correlation(rank(high), rank(volume), 3)), 3)."""

    data = _load_inputs(df)
    corr_rank = rank(correlation(rank(data["high"]), rank(data["volume"]), 3))
    factor = -ts_sum(corr_rank, 3)
    return _stack_factor(factor, "alpha015", data["asset_name"])


def alpha016(df: pd.DataFrame) -> pd.Series:
    """Alpha016: -rank(covariance(rank(high), rank(volume), 5))."""

    data = _load_inputs(df)
    factor = -rank(covariance(rank(data["high"]), rank(data["volume"]), 5))
    return _stack_factor(factor, "alpha016", data["asset_name"])


def alpha017(df: pd.DataFrame) -> pd.Series:
    """Alpha017: -rank(ts_rank(close,10)) * rank(delta(delta(close,1),1)) * rank(ts_rank(volume/adv20,5))."""

    data = _load_inputs(df)
    liquidity = data["volume"].divide(data["adv20"] + EPSILON)
    factor = (
        -rank(ts_rank(data["close"], 10))
        * rank(delta(delta(data["close"], 1), 1))
        * rank(ts_rank(liquidity, 5))
    )
    return _stack_factor(factor, "alpha017", data["asset_name"])


def alpha018(df: pd.DataFrame) -> pd.Series:
    """Alpha018: -rank(stddev(abs(close-open),5) + (close-open) + correlation(close,open,10))."""

    data = _load_inputs(df)
    spread = data["close"] - data["open"]
    factor = -rank(stddev(spread.abs(), 5) + spread + correlation(data["close"], data["open"], 10))
    return _stack_factor(factor, "alpha018", data["asset_name"])


def alpha019(df: pd.DataFrame) -> pd.Series:
    """Alpha019: -sign((close-delay(close,7))+delta(close,7)) * (1 + rank(1 + ts_sum(returns,250)))."""

    data = _load_inputs(df)
    momentum = (data["close"] - delay(data["close"], 7)) + delta(data["close"], 7)
    factor = -np.sign(momentum) * (1.0 + rank(1.0 + ts_sum(data["returns"], 250)))
    return _stack_factor(factor, "alpha019", data["asset_name"])


def alpha020(df: pd.DataFrame) -> pd.Series:
    """Alpha020: -rank(open-delay(high,1)) * rank(open-delay(close,1)) * rank(open-delay(low,1))."""

    data = _load_inputs(df)
    factor = (
        -rank(data["open"] - delay(data["high"], 1))
        * rank(data["open"] - delay(data["close"], 1))
        * rank(data["open"] - delay(data["low"], 1))
    )
    return _stack_factor(factor, "alpha020", data["asset_name"])

def alpha021(df: pd.DataFrame) -> pd.Series:
    """Alpha021 placeholder."""
    raise NotImplementedError(_not_implemented(21))

def alpha022(df: pd.DataFrame) -> pd.Series:
    """Alpha022 placeholder."""
    raise NotImplementedError(_not_implemented(22))

def alpha023(df: pd.DataFrame) -> pd.Series:
    """Alpha023 placeholder."""
    raise NotImplementedError(_not_implemented(23))

def alpha024(df: pd.DataFrame) -> pd.Series:
    """Alpha024 placeholder."""
    raise NotImplementedError(_not_implemented(24))

def alpha025(df: pd.DataFrame) -> pd.Series:
    """Alpha025 placeholder."""
    raise NotImplementedError(_not_implemented(25))

def alpha026(df: pd.DataFrame) -> pd.Series:
    """Alpha026 placeholder."""
    raise NotImplementedError(_not_implemented(26))

def alpha027(df: pd.DataFrame) -> pd.Series:
    """Alpha027 placeholder."""
    raise NotImplementedError(_not_implemented(27))

def alpha028(df: pd.DataFrame) -> pd.Series:
    """Alpha028 placeholder."""
    raise NotImplementedError(_not_implemented(28))

def alpha029(df: pd.DataFrame) -> pd.Series:
    """Alpha029 placeholder."""
    raise NotImplementedError(_not_implemented(29))

def alpha030(df: pd.DataFrame) -> pd.Series:
    """Alpha030 placeholder."""
    raise NotImplementedError(_not_implemented(30))

def alpha031(df: pd.DataFrame) -> pd.Series:
    """Alpha031 placeholder."""
    raise NotImplementedError(_not_implemented(31))

def alpha032(df: pd.DataFrame) -> pd.Series:
    """Alpha032 placeholder."""
    raise NotImplementedError(_not_implemented(32))

def alpha033(df: pd.DataFrame) -> pd.Series:
    """Alpha033 placeholder."""
    raise NotImplementedError(_not_implemented(33))

def alpha034(df: pd.DataFrame) -> pd.Series:
    """Alpha034 placeholder."""
    raise NotImplementedError(_not_implemented(34))

def alpha035(df: pd.DataFrame) -> pd.Series:
    """Alpha035 placeholder."""
    raise NotImplementedError(_not_implemented(35))

def alpha036(df: pd.DataFrame) -> pd.Series:
    """Alpha036 placeholder."""
    raise NotImplementedError(_not_implemented(36))

def alpha037(df: pd.DataFrame) -> pd.Series:
    """Alpha037 placeholder."""
    raise NotImplementedError(_not_implemented(37))

def alpha038(df: pd.DataFrame) -> pd.Series:
    """Alpha038 placeholder."""
    raise NotImplementedError(_not_implemented(38))

def alpha039(df: pd.DataFrame) -> pd.Series:
    """Alpha039 placeholder."""
    raise NotImplementedError(_not_implemented(39))

def alpha040(df: pd.DataFrame) -> pd.Series:
    """Alpha040 placeholder."""
    raise NotImplementedError(_not_implemented(40))

def alpha041(df: pd.DataFrame) -> pd.Series:
    """Alpha041 placeholder."""
    raise NotImplementedError(_not_implemented(41))

def alpha042(df: pd.DataFrame) -> pd.Series:
    """Alpha042 placeholder."""
    raise NotImplementedError(_not_implemented(42))

def alpha043(df: pd.DataFrame) -> pd.Series:
    """Alpha043 placeholder."""
    raise NotImplementedError(_not_implemented(43))

def alpha044(df: pd.DataFrame) -> pd.Series:
    """Alpha044 placeholder."""
    raise NotImplementedError(_not_implemented(44))

def alpha045(df: pd.DataFrame) -> pd.Series:
    """Alpha045 placeholder."""
    raise NotImplementedError(_not_implemented(45))

def alpha046(df: pd.DataFrame) -> pd.Series:
    """Alpha046 placeholder."""
    raise NotImplementedError(_not_implemented(46))

def alpha047(df: pd.DataFrame) -> pd.Series:
    """Alpha047 placeholder."""
    raise NotImplementedError(_not_implemented(47))

def alpha048(df: pd.DataFrame) -> pd.Series:
    """Alpha048 placeholder."""
    raise NotImplementedError(_not_implemented(48))

def alpha049(df: pd.DataFrame) -> pd.Series:
    """Alpha049 placeholder."""
    raise NotImplementedError(_not_implemented(49))

def alpha050(df: pd.DataFrame) -> pd.Series:
    """Alpha050 placeholder."""
    raise NotImplementedError(_not_implemented(50))

def alpha051(df: pd.DataFrame) -> pd.Series:
    """Alpha051 placeholder."""
    raise NotImplementedError(_not_implemented(51))

def alpha052(df: pd.DataFrame) -> pd.Series:
    """Alpha052 placeholder."""
    raise NotImplementedError(_not_implemented(52))

def alpha053(df: pd.DataFrame) -> pd.Series:
    """Alpha053 placeholder."""
    raise NotImplementedError(_not_implemented(53))

def alpha054(df: pd.DataFrame) -> pd.Series:
    """Alpha054 placeholder."""
    raise NotImplementedError(_not_implemented(54))

def alpha055(df: pd.DataFrame) -> pd.Series:
    """Alpha055 placeholder."""
    raise NotImplementedError(_not_implemented(55))

def alpha056(df: pd.DataFrame) -> pd.Series:
    """Alpha056 placeholder."""
    raise NotImplementedError(_not_implemented(56))

def alpha057(df: pd.DataFrame) -> pd.Series:
    """Alpha057 placeholder."""
    raise NotImplementedError(_not_implemented(57))

def alpha058(df: pd.DataFrame) -> pd.Series:
    """Alpha058 placeholder."""
    raise NotImplementedError(_not_implemented(58))

def alpha059(df: pd.DataFrame) -> pd.Series:
    """Alpha059 placeholder."""
    raise NotImplementedError(_not_implemented(59))

def alpha060(df: pd.DataFrame) -> pd.Series:
    """Alpha060 placeholder."""
    raise NotImplementedError(_not_implemented(60))

def alpha061(df: pd.DataFrame) -> pd.Series:
    """Alpha061 placeholder."""
    raise NotImplementedError(_not_implemented(61))

def alpha062(df: pd.DataFrame) -> pd.Series:
    """Alpha062 placeholder."""
    raise NotImplementedError(_not_implemented(62))

def alpha063(df: pd.DataFrame) -> pd.Series:
    """Alpha063 placeholder."""
    raise NotImplementedError(_not_implemented(63))

def alpha064(df: pd.DataFrame) -> pd.Series:
    """Alpha064 placeholder."""
    raise NotImplementedError(_not_implemented(64))

def alpha065(df: pd.DataFrame) -> pd.Series:
    """Alpha065 placeholder."""
    raise NotImplementedError(_not_implemented(65))

def alpha066(df: pd.DataFrame) -> pd.Series:
    """Alpha066 placeholder."""
    raise NotImplementedError(_not_implemented(66))

def alpha067(df: pd.DataFrame) -> pd.Series:
    """Alpha067 placeholder."""
    raise NotImplementedError(_not_implemented(67))

def alpha068(df: pd.DataFrame) -> pd.Series:
    """Alpha068 placeholder."""
    raise NotImplementedError(_not_implemented(68))

def alpha069(df: pd.DataFrame) -> pd.Series:
    """Alpha069 placeholder."""
    raise NotImplementedError(_not_implemented(69))

def alpha070(df: pd.DataFrame) -> pd.Series:
    """Alpha070 placeholder."""
    raise NotImplementedError(_not_implemented(70))

def alpha071(df: pd.DataFrame) -> pd.Series:
    """Alpha071 placeholder."""
    raise NotImplementedError(_not_implemented(71))

def alpha072(df: pd.DataFrame) -> pd.Series:
    """Alpha072 placeholder."""
    raise NotImplementedError(_not_implemented(72))

def alpha073(df: pd.DataFrame) -> pd.Series:
    """Alpha073 placeholder."""
    raise NotImplementedError(_not_implemented(73))

def alpha074(df: pd.DataFrame) -> pd.Series:
    """Alpha074 placeholder."""
    raise NotImplementedError(_not_implemented(74))

def alpha075(df: pd.DataFrame) -> pd.Series:
    """Alpha075 placeholder."""
    raise NotImplementedError(_not_implemented(75))

def alpha076(df: pd.DataFrame) -> pd.Series:
    """Alpha076 placeholder."""
    raise NotImplementedError(_not_implemented(76))

def alpha077(df: pd.DataFrame) -> pd.Series:
    """Alpha077 placeholder."""
    raise NotImplementedError(_not_implemented(77))

def alpha078(df: pd.DataFrame) -> pd.Series:
    """Alpha078 placeholder."""
    raise NotImplementedError(_not_implemented(78))

def alpha079(df: pd.DataFrame) -> pd.Series:
    """Alpha079 placeholder."""
    raise NotImplementedError(_not_implemented(79))

def alpha080(df: pd.DataFrame) -> pd.Series:
    """Alpha080 placeholder."""
    raise NotImplementedError(_not_implemented(80))

def alpha081(df: pd.DataFrame) -> pd.Series:
    """Alpha081 placeholder."""
    raise NotImplementedError(_not_implemented(81))

def alpha082(df: pd.DataFrame) -> pd.Series:
    """Alpha082 placeholder."""
    raise NotImplementedError(_not_implemented(82))

def alpha083(df: pd.DataFrame) -> pd.Series:
    """Alpha083 placeholder."""
    raise NotImplementedError(_not_implemented(83))

def alpha084(df: pd.DataFrame) -> pd.Series:
    """Alpha084 placeholder."""
    raise NotImplementedError(_not_implemented(84))

def alpha085(df: pd.DataFrame) -> pd.Series:
    """Alpha085 placeholder."""
    raise NotImplementedError(_not_implemented(85))

def alpha086(df: pd.DataFrame) -> pd.Series:
    """Alpha086 placeholder."""
    raise NotImplementedError(_not_implemented(86))

def alpha087(df: pd.DataFrame) -> pd.Series:
    """Alpha087 placeholder."""
    raise NotImplementedError(_not_implemented(87))

def alpha088(df: pd.DataFrame) -> pd.Series:
    """Alpha088 placeholder."""
    raise NotImplementedError(_not_implemented(88))

def alpha089(df: pd.DataFrame) -> pd.Series:
    """Alpha089 placeholder."""
    raise NotImplementedError(_not_implemented(89))

def alpha090(df: pd.DataFrame) -> pd.Series:
    """Alpha090 placeholder."""
    raise NotImplementedError(_not_implemented(90))

def alpha091(df: pd.DataFrame) -> pd.Series:
    """Alpha091 placeholder."""
    raise NotImplementedError(_not_implemented(91))

def alpha092(df: pd.DataFrame) -> pd.Series:
    """Alpha092 placeholder."""
    raise NotImplementedError(_not_implemented(92))

def alpha093(df: pd.DataFrame) -> pd.Series:
    """Alpha093 placeholder."""
    raise NotImplementedError(_not_implemented(93))

def alpha094(df: pd.DataFrame) -> pd.Series:
    """Alpha094 placeholder."""
    raise NotImplementedError(_not_implemented(94))

def alpha095(df: pd.DataFrame) -> pd.Series:
    """Alpha095 placeholder."""
    raise NotImplementedError(_not_implemented(95))

def alpha096(df: pd.DataFrame) -> pd.Series:
    """Alpha096 placeholder."""
    raise NotImplementedError(_not_implemented(96))

def alpha097(df: pd.DataFrame) -> pd.Series:
    """Alpha097 placeholder."""
    raise NotImplementedError(_not_implemented(97))

def alpha098(df: pd.DataFrame) -> pd.Series:
    """Alpha098 placeholder."""
    raise NotImplementedError(_not_implemented(98))

def alpha099(df: pd.DataFrame) -> pd.Series:
    """Alpha099 placeholder."""
    raise NotImplementedError(_not_implemented(99))

def alpha100(df: pd.DataFrame) -> pd.Series:
    """Alpha100 placeholder."""
    raise NotImplementedError(_not_implemented(100))

def alpha101(df: pd.DataFrame) -> pd.Series:
    """Alpha101 placeholder."""
    raise NotImplementedError(_not_implemented(101))

ALPHA_FUNCTIONS: dict[str, Callable[[pd.DataFrame], pd.Series]] = {
    f"alpha{i:03d}": globals()[f"alpha{i:03d}"] for i in range(1, 102)
}

for _name, _func in list(ALPHA_FUNCTIONS.items()):
    globals()[f"factor_{_name}"] = _func

__all__ = [
    "ALPHA_FUNCTIONS",
    "IMPLEMENTED_ALPHA_IDS",
    "correlation",
    "covariance",
    "decay_linear",
    "delay",
    "delta",
    "product",
    "rank",
    "scale",
    "signed_power",
    "sma",
    "stddev",
    "ts_argmax",
    "ts_argmin",
    "ts_max",
    "ts_min",
    "ts_rank",
    "ts_sum",
] + list(ALPHA_FUNCTIONS.keys()) + [f"factor_alpha{i:03d}" for i in range(1, 102)]



# Builder-3 appended implementations for alpha021+.
def safe_divide(left: pd.DataFrame, right: pd.DataFrame | float) -> pd.DataFrame:
    return left.divide(right).replace([np.inf, -np.inf], np.nan)


def binary_signal(condition: pd.DataFrame, true_value: float = 1.0, false_value: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        np.where(condition, true_value, false_value),
        index=condition.index,
        columns=condition.columns,
        dtype=float,
    )


def variable_signed_power(base: pd.DataFrame, exponent: pd.DataFrame) -> pd.DataFrame:
    return np.sign(base) * np.power(np.abs(base), exponent)


def frame_min(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return left.where(left <= right, right)


def frame_max(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return left.where(left >= right, right)


def int_window(window: float) -> int:
    return max(int(round(window)), 1)


def alpha021(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    sma2 = sma(data["close"], 2)
    sma8 = sma(data["close"], 8)
    sigma8 = stddev(data["close"], 8)
    volume_ratio = safe_divide(data["volume"], data["adv20"])
    factor = binary_signal((sma8 + sigma8) < sma2, -1.0, np.nan)
    factor = factor.where(~factor.isna(), binary_signal(sma2 < (sma8 - sigma8), 1.0, np.nan))
    factor = factor.where(~factor.isna(), binary_signal(volume_ratio >= 1.0, 1.0, -1.0))
    return _stack_factor(factor, "alpha021", data["asset_name"])


def alpha022(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -(delta(correlation(data["high"], data["volume"], 5), 5) * rank(stddev(data["close"], 20)))
    return _stack_factor(factor, "alpha022", data["asset_name"])


def alpha023(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = (-delta(data["high"], 2)).where(sma(data["high"], 20) < data["high"], 0.0)
    return _stack_factor(factor, "alpha023", data["asset_name"])


def alpha024(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    trend = safe_divide(delta(sma(data["close"], 100), 100), delay(data["close"], 100))
    factor = (-(data["close"] - ts_min(data["close"], 100))).where(trend <= 0.05, -delta(data["close"], 3))
    return _stack_factor(factor, "alpha024", data["asset_name"])


def alpha025(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = rank(((-data["returns"]) * data["adv20"]) * data["vwap"] * (data["high"] - data["close"]))
    return _stack_factor(factor, "alpha025", data["asset_name"])


def alpha026(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -ts_max(correlation(ts_rank(data["volume"], 5), ts_rank(data["high"], 5), 5), 3)
    return _stack_factor(factor, "alpha026", data["asset_name"])


def alpha027(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    signal = rank(sma(correlation(rank(data["volume"]), rank(data["vwap"]), 6), 2))
    factor = binary_signal(signal > 0.5, -1.0, 1.0)
    return _stack_factor(factor, "alpha027", data["asset_name"])


def alpha028(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = scale(correlation(data["adv20"], data["low"], 5) + ((data["high"] + data["low"]) / 2.0) - data["close"])
    return _stack_factor(factor, "alpha028", data["asset_name"])


def alpha029(df: pd.DataFrame) -> pd.Series:
    # The published shorthand is inconsistent about whether `min(product(..., 1), 5)` is
    # an elementwise min or a time-series minimum. Keep it disabled until clarified.
    raise NotImplementedError("Alpha029 requires formula disambiguation before implementation.")


def alpha030(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    sign_sum = (
        np.sign(data["close"] - delay(data["close"], 1))
        + np.sign(delay(data["close"], 1) - delay(data["close"], 2))
        + np.sign(delay(data["close"], 2) - delay(data["close"], 3))
    )
    factor = (1.0 - rank(sign_sum)) * safe_divide(ts_sum(data["volume"], 5), ts_sum(data["volume"], 20))
    return _stack_factor(factor, "alpha030", data["asset_name"])


def alpha031(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    term1 = rank(rank(rank(decay_linear(-rank(rank(delta(data["close"], 10))), 10))))
    term2 = rank(-delta(data["close"], 3))
    term3 = np.sign(scale(correlation(data["adv20"], data["low"], 12)))
    factor = term1 + term2 + term3
    return _stack_factor(factor, "alpha031", data["asset_name"])

def alpha032(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = scale(sma(data["close"], 7) - data["close"]) + (20.0 * scale(correlation(data["vwap"], delay(data["close"], 5), 230)))
    return _stack_factor(factor, "alpha032", data["asset_name"])


def alpha033(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = rank(-(1.0 - safe_divide(data["open"], data["close"])))
    return _stack_factor(factor, "alpha033", data["asset_name"])


def alpha034(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    volatility_ratio = safe_divide(stddev(data["returns"], 2), stddev(data["returns"], 5))
    factor = rank((1.0 - rank(volatility_ratio)) + (1.0 - rank(delta(data["close"], 1))))
    return _stack_factor(factor, "alpha034", data["asset_name"])


def alpha035(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = ts_rank(data["volume"], 32) * (1.0 - ts_rank((data["close"] + data["high"]) - data["low"], 16)) * (1.0 - ts_rank(data["returns"], 32))
    return _stack_factor(factor, "alpha035", data["asset_name"])


def alpha036(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    term1 = 2.21 * rank(correlation(data["close"] - data["open"], delay(data["volume"], 1), 15))
    term2 = 0.7 * rank(data["open"] - data["close"])
    term3 = 0.73 * rank(ts_rank(delay(-data["returns"], 6), 5))
    term4 = rank(correlation(data["vwap"], data["adv20"], 6).abs())
    term5 = 0.6 * rank((sma(data["close"], 200) - data["open"]) * (data["close"] - data["open"]))
    factor = term1 + term2 + term3 + term4 + term5
    return _stack_factor(factor, "alpha036", data["asset_name"])


def alpha037(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = rank(correlation(delay(data["open"] - data["close"], 1), data["close"], 200)) + rank(data["open"] - data["close"])
    return _stack_factor(factor, "alpha037", data["asset_name"])


def alpha038(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -rank(ts_rank(data["close"], 10)) * rank(safe_divide(data["close"], data["open"]))
    return _stack_factor(factor, "alpha038", data["asset_name"])


def alpha039(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    liquidity = safe_divide(data["volume"], data["adv20"])
    factor = -rank(delta(data["close"], 7) * (1.0 - rank(decay_linear(liquidity, 9)))) * (1.0 + rank(ts_sum(data["returns"], 250)))
    return _stack_factor(factor, "alpha039", data["asset_name"])


def alpha040(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -rank(stddev(data["high"], 10)) * correlation(data["high"], data["volume"], 10)
    return _stack_factor(factor, "alpha040", data["asset_name"])


def alpha041(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = np.sqrt(data["high"] * data["low"]) - data["vwap"]
    return _stack_factor(factor, "alpha041", data["asset_name"])


def alpha042(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = safe_divide(rank(data["vwap"] - data["close"]), rank(data["vwap"] + data["close"]))
    return _stack_factor(factor, "alpha042", data["asset_name"])


def alpha043(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = ts_rank(safe_divide(data["volume"], data["adv20"]), 20) * ts_rank(-delta(data["close"], 7), 8)
    return _stack_factor(factor, "alpha043", data["asset_name"])


def alpha044(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -correlation(data["high"], rank(data["volume"]), 5)
    return _stack_factor(factor, "alpha044", data["asset_name"])


def alpha045(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    term1 = rank(sma(delay(data["close"], 5), 20))
    term2 = correlation(data["close"], data["volume"], 2)
    term3 = rank(correlation(ts_sum(data["close"], 5), ts_sum(data["close"], 20), 2))
    factor = -(term1 * term2 * term3)
    return _stack_factor(factor, "alpha045", data["asset_name"])


def alpha046(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    slope = ((delay(data["close"], 20) - delay(data["close"], 10)) / 10.0) - ((delay(data["close"], 10) - data["close"]) / 10.0)
    factor = binary_signal(slope > 0.25, -1.0, np.nan)
    factor = factor.where(~factor.isna(), binary_signal(slope < 0.0, 1.0, -(data["close"] - delay(data["close"], 1))))
    return _stack_factor(factor, "alpha046", data["asset_name"])


def alpha047(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    high_mean = sma(data["high"], 5)
    factor = (((rank(1.0 / data["close"]) * data["volume"]) / (data["adv20"] + EPSILON)) * safe_divide(data["high"] * rank(data["high"] - data["close"]), high_mean)) - rank(data["vwap"] - delay(data["vwap"], 5))
    return _stack_factor(factor, "alpha047", data["asset_name"])

def alpha048(df: pd.DataFrame) -> pd.Series:
    # The current factor surface only receives OHLCV+amount data and does not carry sector/subindustry labels.
    raise NotImplementedError("Alpha048 requires industry classifications for neutralization.")


def alpha049(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    slope = ((delay(data["close"], 20) - delay(data["close"], 10)) / 10.0) - ((delay(data["close"], 10) - data["close"]) / 10.0)
    factor = binary_signal(slope < -0.1, 1.0, 0.0) + (-(data["close"] - delay(data["close"], 1))).where(slope >= -0.1, 0.0)
    return _stack_factor(factor, "alpha049", data["asset_name"])


def alpha050(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -ts_max(rank(correlation(rank(data["volume"]), rank(data["vwap"]), 5)), 5)
    return _stack_factor(factor, "alpha050", data["asset_name"])


def alpha051(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    slope = ((delay(data["close"], 20) - delay(data["close"], 10)) / 10.0) - ((delay(data["close"], 10) - data["close"]) / 10.0)
    factor = binary_signal(slope < -0.05, 1.0, 0.0) + (-(data["close"] - delay(data["close"], 1))).where(slope >= -0.05, 0.0)
    return _stack_factor(factor, "alpha051", data["asset_name"])


def alpha052(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    low_min = ts_min(data["low"], 5)
    return_spread = (ts_sum(data["returns"], 240) - ts_sum(data["returns"], 20)) / 220.0
    factor = ((-low_min + delay(low_min, 5)) * rank(return_spread)) * ts_rank(data["volume"], 5)
    return _stack_factor(factor, "alpha052", data["asset_name"])


def alpha053(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    spread = safe_divide((data["close"] - data["low"]) - (data["high"] - data["close"]), data["close"] - data["low"])
    factor = -delta(spread, 9)
    return _stack_factor(factor, "alpha053", data["asset_name"])


def alpha054(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    numerator = -((data["low"] - data["close"]) * np.power(data["open"], 5))
    denominator = (data["low"] - data["high"]) * np.power(data["close"], 5)
    factor = safe_divide(numerator, denominator)
    return _stack_factor(factor, "alpha054", data["asset_name"])


def alpha055(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    price_position = safe_divide(data["close"] - ts_min(data["low"], 12), ts_max(data["high"], 12) - ts_min(data["low"], 12))
    factor = -correlation(rank(price_position), rank(data["volume"]), 6)
    return _stack_factor(factor, "alpha055", data["asset_name"])


def alpha056(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    # The original formula uses market capitalization. With the constrained input schema,
    # traded amount is the closest available size proxy that keeps the factor operational.
    cap_proxy = data["amount"]
    return_ratio = safe_divide(ts_sum(data["returns"], 10), ts_sum(ts_sum(data["returns"], 2), 3))
    factor = -(rank(return_ratio) * rank(data["returns"] * cap_proxy))
    return _stack_factor(factor, "alpha056", data["asset_name"])


def alpha057(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = -safe_divide(data["close"] - data["vwap"], decay_linear(rank(ts_argmax(data["close"], 30)), 2))
    return _stack_factor(factor, "alpha057", data["asset_name"])


def alpha058(df: pd.DataFrame) -> pd.Series:
    # Sector labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha058 requires sector classifications for neutralization.")


def alpha059(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha059 requires industry classifications for neutralization.")


def alpha060(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    intraday = safe_divide(((data["close"] - data["low"]) - (data["high"] - data["close"])) * data["volume"], data["high"] - data["low"])
    factor = -(2.0 * scale(rank(intraday)) - scale(rank(ts_argmax(data["close"], 10))))
    return _stack_factor(factor, "alpha060", data["asset_name"])


def alpha061(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv180 = sma(data["volume"], 180)
    factor = binary_signal(rank(data["vwap"] - ts_min(data["vwap"], int_window(16.1219))) < rank(correlation(data["vwap"], adv180, int_window(17.9282))))
    return _stack_factor(factor, "alpha061", data["asset_name"])


def alpha062(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    left = rank(correlation(data["vwap"], ts_sum(data["adv20"], int_window(22.4101)), int_window(9.91009)))
    right_inner = (rank(data["open"]) + rank(data["open"])) < (rank((data["high"] + data["low"]) / 2.0) + rank(data["high"]))
    right = rank(binary_signal(right_inner))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha062", data["asset_name"])


def alpha063(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha063 requires industry classifications for neutralization.")


def alpha064(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv120 = sma(data["volume"], 120)
    blended = (data["open"] * 0.178404) + (data["low"] * (1.0 - 0.178404))
    left = rank(correlation(ts_sum(blended, int_window(12.7054)), ts_sum(adv120, int_window(12.7054)), int_window(16.6208)))
    right_base = (((data["high"] + data["low"]) / 2.0) * 0.178404) + (data["vwap"] * (1.0 - 0.178404))
    right = rank(delta(right_base, int_window(3.69741)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha064", data["asset_name"])


def alpha065(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv60 = sma(data["volume"], 60)
    blended = (data["open"] * 0.00817205) + (data["vwap"] * (1.0 - 0.00817205))
    left = rank(correlation(blended, ts_sum(adv60, int_window(8.6911)), int_window(6.40374)))
    right = rank(data["open"] - ts_min(data["open"], int_window(13.635)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha065", data["asset_name"])


def alpha066(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    term1 = rank(decay_linear(delta(data["vwap"], int_window(3.51013)), int_window(7.23052)))
    ratio = safe_divide(data["low"] - data["vwap"], data["open"] - ((data["high"] + data["low"]) / 2.0))
    term2 = ts_rank(decay_linear(ratio, int_window(11.4157)), int_window(6.72611))
    factor = -(term1 + term2)
    return _stack_factor(factor, "alpha066", data["asset_name"])

def alpha067(df: pd.DataFrame) -> pd.Series:
    # Sector and subindustry labels are not part of the current input contract.
    raise NotImplementedError("Alpha067 requires sector/subindustry classifications for neutralization.")


def alpha068(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv15 = sma(data["volume"], 15)
    left = ts_rank(correlation(rank(data["high"]), rank(adv15), int_window(8.91644)), int_window(13.9333))
    right = rank(delta((data["close"] * 0.518371) + (data["low"] * (1.0 - 0.518371)), int_window(1.06157)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha068", data["asset_name"])


def alpha069(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha069 requires industry classifications for neutralization.")


def alpha070(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha070 requires industry classifications for neutralization.")


def alpha071(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv180 = sma(data["volume"], 180)
    left = ts_rank(
        decay_linear(
            correlation(ts_rank(data["close"], int_window(3.43976)), ts_rank(adv180, int_window(12.0647)), int_window(18.0175)),
            int_window(4.20501),
        ),
        int_window(15.6948),
    )
    right = ts_rank(decay_linear(np.power(rank((data["low"] + data["open"]) - (data["vwap"] + data["vwap"])), 2), int_window(16.4662)), int_window(4.4388))
    factor = frame_max(left, right)
    return _stack_factor(factor, "alpha071", data["asset_name"])


def alpha072(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv40 = sma(data["volume"], 40)
    left = rank(decay_linear(correlation((data["high"] + data["low"]) / 2.0, adv40, int_window(8.93345)), int_window(10.1519)))
    right = rank(decay_linear(correlation(ts_rank(data["vwap"], int_window(3.72469)), ts_rank(data["volume"], int_window(18.5188)), int_window(6.86671)), int_window(2.95011)))
    factor = safe_divide(left, right)
    return _stack_factor(factor, "alpha072", data["asset_name"])


def alpha073(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    left = rank(decay_linear(delta(data["vwap"], int_window(4.72775)), int_window(2.91864)))
    blended = (data["open"] * 0.147155) + (data["low"] * (1.0 - 0.147155))
    right = ts_rank(decay_linear(-safe_divide(delta(blended, int_window(2.03608)), blended), int_window(3.33829)), int_window(16.7411))
    factor = -frame_max(left, right)
    return _stack_factor(factor, "alpha073", data["asset_name"])


def alpha074(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv30 = sma(data["volume"], 30)
    left = rank(correlation(data["close"], ts_sum(adv30, int_window(37.4843)), int_window(15.1365)))
    right = rank(correlation(rank((data["high"] * 0.0261661) + (data["vwap"] * (1.0 - 0.0261661))), rank(data["volume"]), int_window(11.4791)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha074", data["asset_name"])


def alpha075(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv50 = sma(data["volume"], 50)
    left = rank(correlation(data["vwap"], data["volume"], int_window(4.24304)))
    right = rank(correlation(rank(data["low"]), rank(adv50), int_window(12.4413)))
    factor = binary_signal(left < right)
    return _stack_factor(factor, "alpha075", data["asset_name"])


def alpha076(df: pd.DataFrame) -> pd.Series:
    # Sector labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha076 requires sector classifications for neutralization.")


def alpha077(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv40 = sma(data["volume"], 40)
    left = rank(decay_linear(((data["high"] + data["low"]) / 2.0) - data["vwap"], int_window(20.0451)))
    right = rank(decay_linear(correlation((data["high"] + data["low"]) / 2.0, adv40, int_window(3.1614)), int_window(5.64125)))
    factor = frame_min(left, right)
    return _stack_factor(factor, "alpha077", data["asset_name"])


def alpha078(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv40 = sma(data["volume"], 40)
    blended = (data["low"] * 0.352233) + (data["vwap"] * (1.0 - 0.352233))
    left = rank(correlation(ts_sum(blended, int_window(19.7428)), ts_sum(adv40, int_window(19.7428)), int_window(6.83313)))
    right = rank(correlation(rank(data["vwap"]), rank(data["volume"]), int_window(5.77492)))
    factor = np.power(left, right)
    return _stack_factor(factor, "alpha078", data["asset_name"])


def alpha079(df: pd.DataFrame) -> pd.Series:
    # Sector labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha079 requires sector classifications for neutralization.")


def alpha080(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha080 requires industry classifications for neutralization.")


def alpha081(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv10 = sma(data["volume"], 10)
    left_corr = correlation(data["vwap"], ts_sum(adv10, int_window(49.6054)), int_window(8.47743))
    left = rank(np.log(product(rank(np.power(rank(left_corr), 4)), int_window(14.9655)).clip(lower=EPSILON)))
    right = rank(correlation(rank(data["vwap"]), rank(data["volume"]), int_window(5.07914)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha081", data["asset_name"])


def alpha082(df: pd.DataFrame) -> pd.Series:
    # Sector labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha082 requires sector classifications for neutralization.")


def alpha083(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    normalized_range = safe_divide(data["high"] - data["low"], sma(data["close"], 5))
    numerator = rank(delay(normalized_range, 2)) * rank(rank(data["volume"]))
    denominator = safe_divide(normalized_range, data["vwap"] - data["close"])
    factor = safe_divide(numerator, denominator)
    return _stack_factor(factor, "alpha083", data["asset_name"])


def alpha084(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    base = ts_rank(data["vwap"] - ts_max(data["vwap"], int_window(15.3217)), int_window(20.7127))
    factor = variable_signed_power(base, delta(data["close"], int_window(4.96796)))
    return _stack_factor(factor, "alpha084", data["asset_name"])


def alpha085(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv30 = sma(data["volume"], 30)
    left = rank(correlation((data["high"] * 0.876703) + (data["close"] * (1.0 - 0.876703)), adv30, int_window(9.61331)))
    right = rank(correlation(ts_rank((data["high"] + data["low"]) / 2.0, int_window(3.70596)), ts_rank(data["volume"], int_window(10.1595)), int_window(7.11408)))
    factor = np.power(left, right)
    return _stack_factor(factor, "alpha085", data["asset_name"])


def alpha086(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    left = ts_rank(correlation(data["close"], ts_sum(data["adv20"], int_window(14.7444)), int_window(6.00049)), int_window(20.4195))
    right = rank(data["close"] - data["vwap"])
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha086", data["asset_name"])

def alpha087(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha087 requires industry classifications for neutralization.")


def alpha088(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv60 = sma(data["volume"], 60)
    left = rank(decay_linear((rank(data["open"]) + rank(data["low"])) - (rank(data["high"]) + rank(data["close"])), int_window(8.06882)))
    right = ts_rank(decay_linear(correlation(ts_rank(data["close"], int_window(8.44728)), ts_rank(adv60, int_window(20.6966)), int_window(8.01266)), int_window(6.65053)), int_window(2.61957))
    factor = frame_min(left, right)
    return _stack_factor(factor, "alpha088", data["asset_name"])


def alpha089(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha089 requires industry classifications for neutralization.")


def alpha090(df: pd.DataFrame) -> pd.Series:
    # Subindustry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha090 requires subindustry classifications for neutralization.")


def alpha091(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha091 requires industry classifications for neutralization.")


def alpha092(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv30 = sma(data["volume"], 30)
    bullish = binary_signal((((data["high"] + data["low"]) / 2.0) + data["close"]) < (data["low"] + data["open"]))
    left = ts_rank(decay_linear(bullish, int_window(14.7221)), int_window(18.8683))
    right = ts_rank(decay_linear(correlation(rank(data["low"]), rank(adv30), int_window(7.58555)), int_window(6.94024)), int_window(6.80584))
    factor = frame_min(left, right)
    return _stack_factor(factor, "alpha092", data["asset_name"])


def alpha093(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha093 requires industry classifications for neutralization.")


def alpha094(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv60 = sma(data["volume"], 60)
    left = rank(data["vwap"] - ts_min(data["vwap"], int_window(11.5783)))
    right = ts_rank(correlation(ts_rank(data["vwap"], int_window(19.6462)), ts_rank(adv60, int_window(4.02992)), int_window(18.0926)), int_window(2.70756))
    factor = -np.power(left, right)
    return _stack_factor(factor, "alpha094", data["asset_name"])


def alpha095(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv40 = sma(data["volume"], 40)
    left = rank(data["open"] - ts_min(data["open"], int_window(12.4105)))
    corr_rank = rank(correlation(ts_sum((data["high"] + data["low"]) / 2.0, int_window(19.1351)), ts_sum(adv40, int_window(19.1351)), int_window(12.8742)))
    right = ts_rank(np.power(corr_rank, 5), int_window(11.7584))
    factor = binary_signal(left < right)
    return _stack_factor(factor, "alpha095", data["asset_name"])


def alpha096(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv60 = sma(data["volume"], 60)
    left = ts_rank(decay_linear(correlation(rank(data["vwap"]), rank(data["volume"]), int_window(3.83878)), int_window(4.16783)), int_window(8.38151))
    corr_argmax = ts_argmax(correlation(ts_rank(data["close"], int_window(7.45404)), ts_rank(adv60, int_window(4.13242)), int_window(3.65459)), int_window(12.6556))
    right = ts_rank(decay_linear(corr_argmax, int_window(14.0365)), int_window(13.4143))
    factor = -frame_max(left, right)
    return _stack_factor(factor, "alpha096", data["asset_name"])


def alpha097(df: pd.DataFrame) -> pd.Series:
    # Industry labels are not part of the current input contract, so indneutralize cannot be reproduced faithfully.
    raise NotImplementedError("Alpha097 requires industry classifications for neutralization.")


def alpha098(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv5 = sma(data["volume"], 5)
    adv15 = sma(data["volume"], 15)
    left = rank(decay_linear(correlation(data["vwap"], ts_sum(adv5, int_window(26.4719)), int_window(4.58418)), int_window(7.18088)))
    corr_argmin = ts_argmin(correlation(rank(data["open"]), rank(adv15), int_window(20.8187)), int_window(8.62571))
    right = rank(decay_linear(ts_rank(corr_argmin, int_window(6.95668)), int_window(8.07206)))
    factor = left - right
    return _stack_factor(factor, "alpha098", data["asset_name"])


def alpha099(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    adv60 = sma(data["volume"], 60)
    left = rank(correlation(ts_sum((data["high"] + data["low"]) / 2.0, int_window(19.8975)), ts_sum(adv60, int_window(19.8975)), int_window(8.8136)))
    right = rank(correlation(data["low"], data["volume"], int_window(6.28259)))
    factor = binary_signal(left < right, -1.0, 0.0)
    return _stack_factor(factor, "alpha099", data["asset_name"])


def alpha100(df: pd.DataFrame) -> pd.Series:
    # Subindustry labels and true neutralization operators are not part of the current input contract.
    raise NotImplementedError("Alpha100 requires subindustry classifications for neutralization.")


def alpha101(df: pd.DataFrame) -> pd.Series:
    data = _load_inputs(df)
    factor = safe_divide(data["close"] - data["open"], (data["high"] - data["low"]) + 0.001)
    return _stack_factor(factor, "alpha101", data["asset_name"])


IMPLEMENTED_ALPHA_IDS = tuple(sorted({
    *range(1, 21),
    21, 22, 23, 24, 25, 26, 27, 28,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    49, 50, 51, 52, 53, 54, 55, 56, 57,
    60, 61, 62, 64, 65, 66, 68,
    71, 72, 73, 74, 75, 77, 78,
    81, 83, 84, 85, 86, 88,
    92, 94, 95, 96, 98, 99, 101,
}))

ALPHA_FUNCTIONS = {f"alpha{i:03d}": globals()[f"alpha{i:03d}"] for i in range(1, 102)}
for _name, _func in list(ALPHA_FUNCTIONS.items()):
    globals()[f"factor_{_name}"] = _func

__all__ = [
    "ALPHA_FUNCTIONS",
    "IMPLEMENTED_ALPHA_IDS",
    "correlation",
    "covariance",
    "decay_linear",
    "delay",
    "delta",
    "product",
    "rank",
    "scale",
    "signed_power",
    "sma",
    "stddev",
    "ts_argmax",
    "ts_argmin",
    "ts_max",
    "ts_min",
    "ts_rank",
    "ts_sum",
] + list(ALPHA_FUNCTIONS.keys()) + [f"factor_alpha{i:03d}" for i in range(1, 102)]

def alpha088(df: pd.DataFrame) -> pd.Series:
    # Under the current rolling operator semantics this formulation collapses to all-NaN on finite samples.
    raise NotImplementedError("Alpha088 needs refined operator semantics to avoid all-NaN output.")


def alpha096(df: pd.DataFrame) -> pd.Series:
    # Under the current rolling operator semantics this formulation collapses to all-NaN on finite samples.
    raise NotImplementedError("Alpha096 needs refined operator semantics to avoid all-NaN output.")

IMPLEMENTED_ALPHA_IDS = tuple(sorted({
    *range(1, 21),
    21, 22, 23, 24, 25, 26, 27, 28,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    49, 50, 51, 52, 53, 54, 55, 56, 57,
    60, 61, 62, 64, 65, 66, 68,
    71, 72, 73, 74, 75, 77, 78,
    81, 83, 84, 85, 86,
    92, 94, 95, 98, 99, 101,
}))
ALPHA_FUNCTIONS = {f"alpha{i:03d}": globals()[f"alpha{i:03d}"] for i in range(1, 102)}
for _name, _func in list(ALPHA_FUNCTIONS.items()):
    globals()[f"factor_{_name}"] = _func

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



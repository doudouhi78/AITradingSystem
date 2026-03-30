from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\AITradingSystem")
ETF_DIR = ROOT / "runtime" / "market_data" / "cn_etf"


def _load_single_asset(symbol: str, start: str, end: str) -> pd.DataFrame:
    path = ETF_DIR / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing parquet for {symbol}: {path}")
    df = pd.read_parquet(path).copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    mask = (df["trade_date"] >= pd.Timestamp(start)) & (df["trade_date"] <= pd.Timestamp(end))
    return df.loc[mask].sort_values("trade_date").reset_index(drop=True)


def load_prices(instruments: list[str], start: str, end: str) -> pd.DataFrame:
    frames: list[pd.Series] = []
    for symbol in instruments:
        df = _load_single_asset(symbol, start, end)
        series = df.set_index("trade_date")["close"].astype(float).rename(symbol)
        frames.append(series)
    if not frames:
        return pd.DataFrame()
    prices = pd.concat(frames, axis=1).sort_index()
    prices.index.name = "date"
    return prices


def load_factor_input(instruments: list[str], start: str, end: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in instruments:
        df = _load_single_asset(symbol, start, end).copy()
        df["close"] = df["close"].astype(float)
        df["amount"] = df["amount"].astype(float)
        df["asset"] = symbol
        frames.append(df[["trade_date", "asset", "close", "amount"]])
    if not frames:
        return pd.DataFrame(columns=["close", "amount"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.rename(columns={"trade_date": "date"}).sort_values(["date", "asset"]).reset_index(drop=True)
    return merged.set_index(["date", "asset"])

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\AITradingSystem")
ETF_DIR = ROOT / "runtime" / "market_data" / "cn_etf"
STOCK_DIR = ROOT / "runtime" / "market_data" / "cn_stock"
CSI300_PATH = ROOT / "runtime" / "classification_data" / "index_components" / "csi300_latest.parquet"


def _get_data_dir(asset_type: str) -> Path:
    if asset_type == "etf":
        return ETF_DIR
    if asset_type == "stock":
        return STOCK_DIR
    raise ValueError(f"unsupported asset_type={asset_type}")


def _load_single_asset(symbol: str, start: str, end: str, asset_type: str = "etf") -> pd.DataFrame:
    path = _get_data_dir(asset_type) / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing parquet for {symbol}: {path}")
    df = pd.read_parquet(path).copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    mask = (df["trade_date"] >= pd.Timestamp(start)) & (df["trade_date"] <= pd.Timestamp(end))
    return df.loc[mask].sort_values("trade_date").reset_index(drop=True)


def select_top_n_by_liquidity(asset_type: str, start: str, end: str, top_n: int = 50) -> list[str]:
    data_dir = _get_data_dir(asset_type)
    rows: list[dict[str, float | str]] = []
    candidate_symbols: set[str] | None = None
    if asset_type == "stock":
        csi300 = pd.read_parquet(CSI300_PATH)
        candidate_symbols = set(csi300["instrument_code"].astype(str).str.zfill(6))
        top_n = min(top_n, 200)
    for path in data_dir.glob("*.parquet"):
        if path.name.startswith("_"):
            continue
        if candidate_symbols is not None and path.stem not in candidate_symbols:
            continue
        try:
            df = pd.read_parquet(path, columns=["trade_date", "amount"]).copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            mask = (df["trade_date"] >= pd.Timestamp(start)) & (df["trade_date"] <= pd.Timestamp(end))
            sample = df.loc[mask]
            if sample.empty:
                continue
            avg_amount = float(pd.to_numeric(sample["amount"], errors="coerce").mean())
            if avg_amount <= 0:
                continue
            if asset_type == "stock" and avg_amount <= 50_000_000:
                continue
            rows.append({"symbol": path.stem, "avg_amount": avg_amount})
        except Exception:
            continue
    if not rows:
        return []
    ranked = pd.DataFrame(rows).sort_values("avg_amount", ascending=False).head(top_n)
    return ranked["symbol"].tolist()


def load_prices(instruments: list[str], start: str, end: str, asset_type: str = "etf") -> pd.DataFrame:
    frames: list[pd.Series] = []
    for symbol in instruments:
        df = _load_single_asset(symbol, start, end, asset_type=asset_type)
        series = pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce").rename(symbol)
        frames.append(series)
    if not frames:
        return pd.DataFrame()
    prices = pd.concat(frames, axis=1).sort_index()
    prices.index.name = "date"
    return prices


def load_factor_input(instruments: list[str], start: str, end: str, asset_type: str = "etf") -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in instruments:
        df = _load_single_asset(symbol, start, end, asset_type=asset_type).copy()
        for col in ["close", "high", "low", "amount", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["asset"] = symbol
        frames.append(df[["trade_date", "asset", "close", "high", "low", "amount", "volume"]])
    if not frames:
        return pd.DataFrame(columns=["close", "high", "low", "amount", "volume"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.rename(columns={"trade_date": "date"}).sort_values(["date", "asset"]).reset_index(drop=True)
    return merged.set_index(["date", "asset"])

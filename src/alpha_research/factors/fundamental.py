from __future__ import annotations

from typing import Any

import akshare as ak
import pandas as pd


def probe_pb_interfaces() -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    # market-level only, not stock-specific daily PB
    try:
        df = ak.stock_a_all_pb()
        probes.append({
            "interface": "stock_a_all_pb",
            "status": "market_level_only",
            "columns": list(df.columns),
            "note": "全市场PB，不是个股日频PB，不能直接用于个股横截面因子。",
        })
    except Exception as e:
        probes.append({"interface": "stock_a_all_pb", "status": "error", "note": repr(e)})

    try:
        df = ak.stock_financial_analysis_indicator("000001", start_year="2020")
        probes.append({
            "interface": "stock_financial_analysis_indicator",
            "status": "quarterly_available",
            "columns": list(df.columns),
            "note": "季度财务指标可用，可用每股净资产近似构造PB，但不含公告延迟处理。",
        })
    except Exception as e:
        probes.append({"interface": "stock_financial_analysis_indicator", "status": "error", "note": repr(e)})

    try:
        df = ak.stock_financial_analysis_indicator_em(symbol="SZ000001", indicator="按报告期")
        probes.append({
            "interface": "stock_financial_analysis_indicator_em",
            "status": "available",
            "columns": list(df.columns),
            "note": "接口可用，可用于后续替代新浪源。",
        })
    except Exception as e:
        probes.append({"interface": "stock_financial_analysis_indicator_em", "status": "error", "note": repr(e)})
    return probes


def factor_pb_ratio_approx(instruments: list[str], start: str, end: str) -> pd.Series:
    dfs = []
    for symbol in instruments:
        try:
            fin = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start[:4])
            if fin.empty:
                continue
            nav_col = None
            for candidate in ["每股净资产_调整后(元)", "每股净资产_调整前(元)", "调整后的每股净资产(元)"]:
                if candidate in fin.columns:
                    nav_col = candidate
                    break
            if nav_col is None or "日期" not in fin.columns:
                continue
            temp = fin[["日期", nav_col]].copy()
            temp = temp.rename(columns={"日期": "date", nav_col: "book_value_per_share"})
            temp["date"] = pd.to_datetime(temp["date"], errors="coerce")
            temp["book_value_per_share"] = pd.to_numeric(temp["book_value_per_share"], errors="coerce")
            temp = temp.dropna().sort_values("date")
            if temp.empty:
                continue
            temp["asset"] = symbol
            dfs.append(temp)
        except Exception:
            continue

    if not dfs:
        return pd.Series([], dtype=float, name="pb_ratio")

    quarterly = pd.concat(dfs, ignore_index=True).sort_values(["asset", "date"])
    frames = []
    for symbol in instruments:
        asset_quarterly = quarterly[quarterly["asset"] == symbol].copy()
        if asset_quarterly.empty:
            continue
        dates = pd.date_range(start=start, end=end, freq='B')
        daily = pd.DataFrame({'date': dates})
        merged = pd.merge_asof(daily.sort_values('date'), asset_quarterly[['date', 'book_value_per_share']].sort_values('date'), on='date', direction='backward')
        merged['asset'] = symbol
        frames.append(merged)

    if not frames:
        return pd.Series([], dtype=float, name='pb_ratio')
    result = pd.concat(frames, ignore_index=True).dropna(subset=['book_value_per_share'])
    result['factor_value'] = (1.0 / result['book_value_per_share'].astype(float)).shift(1)
    result = result.dropna(subset=['factor_value']).set_index(['date', 'asset'])['factor_value']
    result.index.names = ['date', 'asset']
    result.name = 'pb_ratio'
    return result

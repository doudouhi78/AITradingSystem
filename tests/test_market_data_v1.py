from pathlib import Path

import pandas as pd

from ai_dev_os.market_data_v1 import (
    DEFAULT_START_DATE,
    build_cn_etf_pool,
    build_cn_stock_elite_pool,
    build_us_etf_pool,
    build_us_stock_elite_pool,
    fetch_pool_market_data,
    generate_pool_lists,
    load_pool_list,
    standardize_market_bars,
    validate_market_bars,
    write_market_parquet,
    write_pool_list,
    write_pool_summary,
)


def test_build_cn_etf_pool():
    spot = pd.DataFrame(
        [
            {
                "symbol": "510300",
                "exchange": "SSE",
                "turnover": 50_000_000,
                "aum": 10_000_000_000,
                "listed_date": "2020-01-01",
                "is_leveraged": False,
                "is_inverse": False,
                "etf_category": "ETF",
                "benchmark_index": "宽基",
                "etf_theme": "沪深300",
                "fund_name": "沪深300ETF",
            },
            {
                "symbol": "999999",
                "exchange": "SSE",
                "turnover": 5_000_000,
                "aum": 100_000_000,
                "listed_date": "2024-01-01",
                "is_leveraged": False,
                "is_inverse": False,
                "etf_category": "ETF",
                "benchmark_index": "主题",
                "etf_theme": "测试",
                "fund_name": "测试ETF",
            },
        ]
    )
    pool = build_cn_etf_pool(spot)
    assert pool["symbol"].tolist() == ["510300"]
    assert pool.iloc[0]["market"] == "CN"


def test_build_cn_stock_elite_pool():
    spot = pd.DataFrame(
        [
            {
                "symbol": "600519",
                "exchange": "SSE",
                "industry_level_1": "Consumer",
                "industry_level_2": "Liquor",
                "market_cap": 200_000_000_000,
                "float_market_cap": 180_000_000_000,
                "turnover": 300_000_000,
                "is_st": False,
                "is_profitable_recent_2y": True,
                "is_sunset_industry": False,
                "is_one_shot_theme": False,
            },
            {
                "symbol": "000001",
                "exchange": "SZSE",
                "industry_level_1": "Other",
                "industry_level_2": "Other",
                "market_cap": 1_000_000_000,
                "float_market_cap": 900_000_000,
                "turnover": 1_000,
                "is_st": True,
                "is_profitable_recent_2y": False,
                "is_sunset_industry": True,
                "is_one_shot_theme": True,
            },
        ]
    )
    pool = build_cn_stock_elite_pool(spot)
    assert pool["symbol"].tolist() == ["600519"]
    assert pool.iloc[0]["security_type"] == "stock"


def test_build_us_etf_pool():
    metadata = pd.DataFrame(
        [
            {"symbol": "SPY", "exchange": "NYSE", "aum": 500_000_000, "turnover": 10_000_000, "is_leveraged": False, "is_inverse": False},
            {"symbol": "TQQQ", "exchange": "NASDAQ", "aum": 500_000_000, "turnover": 10_000_000, "is_leveraged": True, "is_inverse": False},
        ]
    )
    pool = build_us_etf_pool(metadata)
    assert pool["symbol"].tolist() == ["SPY"]


def test_build_us_stock_elite_pool():
    metadata = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "exchange": "NASDAQ",
                "industry_level_1": "Technology",
                "industry_level_2": "Consumer Electronics",
                "market_cap": 2_500_000_000_000,
                "float_market_cap": 2_300_000_000_000,
                "turnover": 50_000_000,
                "is_profitable_recent_2y": True,
                "is_shell_like": False,
                "is_story_only": False,
                "chart_structure_ok": True,
            },
            {
                "symbol": "OTC1",
                "exchange": "OTC",
                "industry_level_1": "Other",
                "industry_level_2": "Other",
                "market_cap": 100_000_000,
                "float_market_cap": 80_000_000,
                "turnover": 1_000,
                "is_profitable_recent_2y": False,
                "is_shell_like": True,
                "is_story_only": True,
                "chart_structure_ok": False,
            },
        ]
    )
    pool = build_us_stock_elite_pool(metadata)
    assert pool["symbol"].tolist() == ["AAPL"]


def test_write_and_load_pool_list(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {"cn_etf": type("Cfg", (), {"pool_path": tmp_path / "cn_etf_pool.csv"})()},
    )
    frame = pd.DataFrame([{"market": "CN", "symbol": "510300", "security_type": "etf", "exchange": "SSE"}])
    write_pool_list("cn_etf", frame)
    loaded = load_pool_list("cn_etf")
    assert loaded.iloc[0]["symbol"] == "510300"


def test_generate_pool_lists(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {
            "cn_etf": type("Cfg", (), {"pool_path": tmp_path / "cn_etf_pool.csv"})(),
            "cn_stock": type("Cfg", (), {"pool_path": tmp_path / "cn_stock_elite_pool.csv"})(),
            "us_etf": type("Cfg", (), {"pool_path": tmp_path / "us_etf_pool.csv"})(),
            "us_stock": type("Cfg", (), {"pool_path": tmp_path / "us_stock_elite_pool.csv"})(),
        },
    )

    class StubAk:
        @staticmethod
        def fund_etf_spot_em():
            return pd.DataFrame([
                {"代码": "510300", "名称": "沪深300ETF", "成交额": 50_000_000, "总市值": 10_000_000_000},
                {"代码": "159915", "名称": "创业板ETF", "成交额": 40_000_000, "总市值": 8_000_000_000},
            ])

        @staticmethod
        def fund_etf_category_sina():
            return pd.DataFrame([
                {"基金代码": "510300", "基金简称": "沪深300ETF", "基金类别": "ETF", "投资类别": "股票基金", "上市日期": "2020-01-01", "基金份额": 1000000000, "净值": 4.0},
                {"基金代码": "159915", "基金简称": "创业板ETF", "基金类别": "ETF", "投资类别": "股票基金", "上市日期": "2020-01-01", "基金份额": 500000000, "净值": 3.0},
            ])

        @staticmethod
        def stock_zh_a_spot_em():
            return pd.DataFrame([
                {"代码": "600519", "成交额": 300_000_000, "总市值": 200_000_000_000, "流通市值": 180_000_000_000, "市盈率-动态": 20.0},
            ])

        @staticmethod
        def stock_zh_a_st_em():
            return pd.DataFrame([{"代码": "000001"}])

        @staticmethod
        def stock_individual_info_em(symbol: str):
            return pd.DataFrame([
                {"item": "行业", "value": "Consumer"},
                {"item": "上市时间", "value": "20010427"},
            ])

        @staticmethod
        def stock_us_spot_em():
            return pd.DataFrame([
                {"代码": "106.AAPL", "总市值": 2_500_000_000_000},
            ])

    class StubTicker:
        def __init__(self, symbol: str):
            self.symbol = symbol
            self.fast_info = {"exchange": "US", "market_cap": 500_000_000, "last_price": 100.0, "last_volume": 100_000}
            self.info = {"exchange": "NASDAQ", "sector": "Technology", "industry": "Consumer Electronics", "marketCap": 2_500_000_000_000, "trailingPE": 25.0}

    class StubYF:
        @staticmethod
        def Ticker(symbol: str):
            return StubTicker(symbol)

    result = generate_pool_lists(provider_modules={"akshare": StubAk(), "yfinance": StubYF()})
    assert set(result.keys()) == {"cn_etf", "cn_stock", "us_etf", "us_stock"}
    assert Path(result["cn_etf"]).exists()
    assert Path(result["us_stock"]).exists()


def test_standardize_validate_and_write_market_bars(tmp_path: Path, monkeypatch):
    raw = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10, "amount": 100, "is_suspended": False},
            {"trade_date": "2024-01-03", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 20, "amount": 200, "is_suspended": False},
        ]
    )
    frame = standardize_market_bars(raw, market="CN", symbol="510300", security_type="etf")
    result = validate_market_bars(frame)
    assert result["status"] == "passed"

    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {"cn_etf": type("Cfg", (), {"data_dir": tmp_path / "cn_etf"})()},
    )
    path = write_market_parquet("cn_etf", "510300", frame)
    assert path.exists()


def test_write_pool_summary(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {"cn_etf": type("Cfg", (), {"data_dir": tmp_path / "cn_etf"})()},
    )
    path = write_pool_summary(
        "cn_etf",
        target_count=3,
        success_count=2,
        start_date=DEFAULT_START_DATE,
        end_date="2026-03-28",
        failed_symbols=["159915"],
        skipped_symbols=["510300"],
    )
    content = path.read_text(encoding="utf-8")
    assert path.exists()
    assert '"target_count": 3' in content
    assert '"success_count": 2' in content
    assert '"159915"' in content
    assert '"510300"' in content


def test_fetch_pool_market_data_with_stubbed_provider(tmp_path: Path, monkeypatch):
    pool_path = tmp_path / "cn_etf_pool.csv"
    pd.DataFrame(
        [
            {"market": "CN", "symbol": "510300", "security_type": "etf", "exchange": "SSE"},
            {"market": "CN", "symbol": "159915", "security_type": "etf", "exchange": "SZSE"},
        ]
    ).to_csv(pool_path, index=False)

    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {
            "cn_etf": type(
                "Cfg",
                (),
                {"pool_name": "cn_etf", "market": "CN", "security_type": "etf", "pool_path": pool_path, "data_dir": tmp_path / "cn_etf"},
            )(),
        },
    )

    def fake_fetch_cn_daily_bars(symbol, **kwargs):
        if symbol == "159915":
            raise RuntimeError("network error")
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "symbol": symbol,
                    "security_type": "etf",
                    "trade_date": "2024-01-02",
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "adjustment_mode": "qfq",
                    "is_suspended": False,
                    "listed_date": "2010-01-01",
                    "delisted_date": "",
                }
            ]
        )

    monkeypatch.setattr("ai_dev_os.market_data_v1.fetch_cn_daily_bars", fake_fetch_cn_daily_bars)
    result = fetch_pool_market_data(
        "cn_etf",
        start_date=DEFAULT_START_DATE,
        end_date="2026-03-28",
        provider_modules={"akshare": object()},
    )
    assert result["symbol_count"] == 2
    assert len(result["written_files"]) == 1
    assert result["failed_symbols"] == ["159915"]
    assert result["skipped_symbols"] == []


def test_fetch_pool_market_data_skip_existing(tmp_path: Path, monkeypatch):
    pool_path = tmp_path / "cn_stock_elite_pool.csv"
    pd.DataFrame(
        [
            {"market": "CN", "symbol": "000001", "security_type": "stock", "exchange": "SZSE"},
            {"market": "CN", "symbol": "000002", "security_type": "stock", "exchange": "SZSE"},
        ]
    ).to_csv(pool_path, index=False)

    data_dir = tmp_path / "cn_stock"
    data_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {
            "market": "CN",
            "symbol": "000001",
            "security_type": "stock",
            "trade_date": "2024-01-02",
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "volume": 100.0,
            "amount": 1000.0,
            "adjustment_mode": "qfq",
            "is_suspended": False,
            "listed_date": "2010-01-01",
            "delisted_date": "",
        }
    ]).to_parquet(data_dir / "000001.parquet", index=False)

    monkeypatch.setattr(
        "ai_dev_os.market_data_v1.POOL_CONFIGS",
        {
            "cn_stock": type(
                "Cfg",
                (),
                {"pool_name": "cn_stock", "market": "CN", "security_type": "stock", "pool_path": pool_path, "data_dir": data_dir},
            )(),
        },
    )

    def fake_fetch_cn_daily_bars(symbol, **kwargs):
        return pd.DataFrame(
            [
                {
                    "market": "CN",
                    "symbol": symbol,
                    "security_type": "stock",
                    "trade_date": "2024-01-02",
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "adjustment_mode": "qfq",
                    "is_suspended": False,
                    "listed_date": "2010-01-01",
                    "delisted_date": "",
                }
            ]
        )

    monkeypatch.setattr("ai_dev_os.market_data_v1.fetch_cn_daily_bars", fake_fetch_cn_daily_bars)
    result = fetch_pool_market_data(
        "cn_stock",
        start_date=DEFAULT_START_DATE,
        end_date="2026-03-28",
        provider_modules={"akshare": object()},
        skip_existing=True,
    )
    assert result["skipped_symbols"] == ["000001"]
    assert len(result["written_files"]) == 1
    assert result["failed_symbols"] == []

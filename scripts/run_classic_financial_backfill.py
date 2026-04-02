from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import alphalens as al
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices  # noqa: E402
from alpha_research.factors import classic_factors  # noqa: E402
FORWARD_PERIOD = 5
SUMMARY_PATH = ROOT / 'runtime' / 'alpha_research' / 'classic_factors_ic_summary.csv'
REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'
KNOWLEDGE_BASE_PATH = ROOT / 'runtime' / 'factor_registry' / 'knowledge_base.json'
START = '2018-01-01'
END = '2023-12-31'
MAX_SYMBOLS = 500
KNOWLEDGE_ENTRIES = [
    {'factor_name': 'book_to_market', 'category': 'value', 'formula': 'net_assets / total_market_cap', 'description': '账面市值比 = 净资产 / 总市值'},
    {'factor_name': 'earnings_yield', 'category': 'value', 'formula': 'net_profit_ttm / total_market_cap', 'description': '盈利收益率 = 净利润TTM / 总市值'},
    {'factor_name': 'sales_to_price', 'category': 'value', 'formula': 'revenue_ttm / total_market_cap', 'description': '营收市值比 = 营业收入TTM / 总市值'},
    {'factor_name': 'roe', 'category': 'quality', 'formula': 'net_profit_ttm / average_equity', 'description': '净资产收益率 = 净利润TTM / 平均净资产'},
    {'factor_name': 'gross_margin', 'category': 'quality', 'formula': '(revenue_ttm - total_cogs_ttm) / revenue_ttm', 'description': '毛利率 = 毛利润 / 营业收入'},
    {'factor_name': 'asset_turnover', 'category': 'quality', 'formula': 'revenue_ttm / average_total_assets', 'description': '资产周转率 = 营业收入TTM / 平均总资产'},
    {'factor_name': 'accruals', 'category': 'quality', 'formula': '(net_profit_ttm - operating_cashflow_ttm) / total_assets', 'description': '应计项目 = (净利润 - 经营现金流) / 总资产'},
    {'factor_name': 'momentum_12_1', 'category': 'momentum', 'formula': 'return_252d_ex_last_21d', 'description': '12-1月动量 = 过去252日收益率，跳过最近21日'},
    {'factor_name': 'momentum_1m', 'category': 'momentum', 'formula': '-return_21d', 'description': '1月反转 = 过去21日收益率取负'},
    {'factor_name': 'idiosyncratic_vol', 'category': 'low_volatility', 'formula': 'stddev(daily_returns, 60)', 'description': '特质波动率 = 过去60日日收益率标准差'},
    {'factor_name': 'beta_1y', 'category': 'low_volatility', 'formula': 'cov(stock, hs300)/var(hs300) over 252d', 'description': '市场Beta = 过去252日对沪深300回归斜率'},
]

TARGET_FACTORS = [
    ('roe', 'quality'),
    ('gross_margin', 'quality'),
    ('asset_turnover', 'quality'),
    ('accruals', 'quality'),
]


@dataclass(slots=True)
class EvalResult:
    factor_name: str
    category: str
    ic_mean: float
    ic_std: float
    icir: float
    status: str


def ensure_knowledge_base() -> None:
    existing: list[dict] = []
    if KNOWLEDGE_BASE_PATH.exists():
        existing = json.loads(KNOWLEDGE_BASE_PATH.read_text(encoding='utf-8'))
    existing_map = {item['factor_name']: item for item in existing if 'factor_name' in item}
    for item in KNOWLEDGE_ENTRIES:
        existing_map[item['factor_name']] = item
    payload = [existing_map[name] for name in sorted(existing_map)]
    KNOWLEDGE_BASE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def discover_symbols() -> list[str]:
    balance = {p.stem.split('.')[0] for p in (ROOT / 'runtime' / 'fundamental_data' / 'balance').glob('*.parquet')}
    income = {p.stem.split('.')[0] for p in (ROOT / 'runtime' / 'fundamental_data' / 'income').glob('*.parquet')}
    cashflow = {p.stem.split('.')[0] for p in (ROOT / 'runtime' / 'fundamental_data' / 'cashflow').glob('*.parquet')}
    common = sorted(balance & income & cashflow)
    symbols = [code for code in common if (ROOT / 'runtime' / 'market_data' / 'cn_stock' / f'{code}.parquet').exists()]
    return symbols[:MAX_SYMBOLS]


def build_inputs() -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    symbols = discover_symbols()
    prices = load_prices(symbols, START, END, asset_type='stock')
    factor_input = load_factor_input(symbols, START, END, asset_type='stock')
    return prices, factor_input, symbols


def evaluate_factor(factor_name: str, category: str, prices: pd.DataFrame, factor_input: pd.DataFrame) -> EvalResult:
    func = getattr(classic_factors, factor_name)
    factor = func(factor_input)
    clean = al.utils.get_clean_factor_and_forward_returns(
        factor=factor,
        prices=prices,
        periods=(FORWARD_PERIOD,),
        quantiles=5,
        max_loss=1.0,
    )
    ic = al.performance.factor_information_coefficient(clean)
    ic_series = ic.iloc[:, 0].dropna()
    ic_mean = float(ic_series.mean()) if len(ic_series) else 0.0
    ic_std = float(ic_series.std(ddof=0)) if len(ic_series) > 1 else 0.0
    icir = ic_mean / ic_std if ic_std else ic_mean
    status = 'pass' if icir > 0.05 else ('weak' if icir > 0.0 else 'fail')
    return EvalResult(factor_name, category, ic_mean, ic_std, icir, status)


def update_summary(results: list[EvalResult]) -> None:
    frame = pd.DataFrame([asdict(item) for item in results])
    if SUMMARY_PATH.exists():
        existing = pd.read_csv(SUMMARY_PATH)
        existing = existing[~existing['factor_name'].isin(frame['factor_name'])]
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_csv(SUMMARY_PATH, index=False)


def update_registry(results: list[EvalResult]) -> int:
    existing = []
    if REGISTRY_PATH.exists():
        existing = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    target_names = {item.factor_name for item in results}
    payload = [item for item in existing if item.get('factor_name') not in target_names]
    updated_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    winners = [
        {
            'factor_id': f'classic_{item.factor_name}',
            'factor_name': item.factor_name,
            'category': item.category,
            'ic_mean': item.ic_mean,
            'ic_std': item.ic_std,
            'icir': item.icir,
            'status': item.status,
            'source': 'classic_factors_alphalens_backfill',
            'forward_period_days': FORWARD_PERIOD,
            'updated_at': updated_at,
        }
        for item in results
        if item.icir > 0.05
    ]
    payload.extend(winners)
    REGISTRY_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return len(winners)


def main() -> None:
    ensure_knowledge_base()
    prices, factor_input, symbols = build_inputs()
    results = [evaluate_factor(name, category, prices, factor_input) for name, category in TARGET_FACTORS]
    update_summary(results)
    added = update_registry(results)
    print(json.dumps({
        'symbols': len(symbols),
        'results': [asdict(item) for item in results],
        'registry_added': added,
        'summary_path': str(SUMMARY_PATH),
        'registry_path': str(REGISTRY_PATH),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

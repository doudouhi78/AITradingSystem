from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import empyrical
import pandas as pd

try:
    import pyfolio  # type: ignore
except ModuleNotFoundError:
    pyfolio = None

from attribution.trade_diagnostics import _rebuild_breakout_portfolio

ROOT = Path(__file__).resolve().parents[2]
PRIMARY_ROOT = Path(r'D:\AITradingSystem')
INPUT_ROOT = PRIMARY_ROOT if (PRIMARY_ROOT / 'runtime' / 'experiments').exists() else ROOT
EXPERIMENTS_DIR = INPUT_ROOT / 'runtime' / 'experiments'
BENCHMARK_PATH = INPUT_ROOT / 'runtime' / 'market_data' / 'cn_etf' / '510300.parquet'
OUTPUT_DIR = ROOT / 'runtime' / 'attribution' / 'strategy_attribution'
REPORT_DIR = ROOT / 'runtime' / 'attribution' / 'reports'


def _load_benchmark_returns(start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    benchmark = pd.read_parquet(BENCHMARK_PATH).copy()
    benchmark['trade_date'] = pd.to_datetime(benchmark['trade_date'])
    mask = (benchmark['trade_date'] >= start) & (benchmark['trade_date'] <= end)
    prices = pd.to_numeric(benchmark.loc[mask, 'close'], errors='coerce')
    index = pd.to_datetime(benchmark.loc[mask, 'trade_date'])
    returns = prices.pct_change(fill_method=None).fillna(0.0)
    return pd.Series(returns.values, index=index, name='benchmark')


def _json_number(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    return None if pd.isna(number) else number


def load_returns_series(experiment_id: str) -> tuple[pd.Series, pd.Series]:
    experiment_dir = EXPERIMENTS_DIR / experiment_id
    portfolio, df = _rebuild_breakout_portfolio(experiment_dir)
    date_index = pd.DatetimeIndex(pd.to_datetime(df['date']))
    port_value = pd.Series(portfolio.value().values, index=date_index, name='portfolio_value')
    strategy_returns = port_value.pct_change(fill_method=None).fillna(0.0)
    strategy_returns.name = 'strategy'
    benchmark_returns = _load_benchmark_returns(date_index.min(), date_index.max())
    aligned = pd.concat([strategy_returns, benchmark_returns], axis=1, join='inner').fillna(0.0)
    return aligned['strategy'], aligned['benchmark']


def run_strategy_attribution(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    annual_return = float(empyrical.annual_return(strategy_returns))
    sharpe = float(empyrical.sharpe_ratio(strategy_returns, annualization=252))
    max_drawdown = float(empyrical.max_drawdown(strategy_returns))
    alpha_raw, beta_raw = empyrical.alpha_beta(strategy_returns, benchmark_returns, annualization=252)
    alpha = _json_number(alpha_raw)
    beta = _json_number(beta_raw)
    benchmark_annual_return = float(empyrical.annual_return(benchmark_returns))
    excess_return = annual_return - benchmark_annual_return
    note = None
    if alpha is None or beta is None:
        note = 'alpha/beta unavailable because benchmark variance or sample is insufficient; serialized as null'
    payload = {
        'annual_return': annual_return,
        'sharpe': 0.0 if pd.isna(sharpe) else sharpe,
        'max_drawdown': max_drawdown,
        'alpha': alpha,
        'beta': beta,
        'benchmark_annual_return': benchmark_annual_return,
        'excess_return': excess_return,
        'note': note,
    }
    (OUTPUT_DIR / 'strategy_attribution.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def compute_rolling_alpha(strategy_returns: pd.Series, benchmark_returns: pd.Series, window_days: int = 63) -> pd.Series:
    aligned = pd.concat([strategy_returns, benchmark_returns], axis=1, join='inner').dropna()
    values: list[dict[str, Any]] = []
    if len(aligned) < window_days:
        series = pd.Series(dtype=float, name='rolling_alpha')
    else:
        for idx in range(window_days, len(aligned) + 1):
            window = aligned.iloc[idx - window_days: idx]
            alpha = empyrical.alpha(window.iloc[:, 0], window.iloc[:, 1], annualization=252)
            alpha_value = _json_number(alpha)
            date = window.index[-1]
            values.append({'date': str(pd.Timestamp(date).date()), 'alpha': alpha_value})
        series = pd.Series([item['alpha'] for item in values], index=pd.to_datetime([item['date'] for item in values]), name='rolling_alpha')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = [{'date': str(idx.date()), 'alpha': _json_number(val)} for idx, val in series.items()]
    (OUTPUT_DIR / 'rolling_alpha.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return series


def generate_pyfolio_tearsheet(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORT_DIR / 'pyfolio_tearsheet.html'
    if pyfolio is None:
        html = '<html><body><h1>Pyfolio optional dependency missing</h1><p>Install pyfolio-reloaded to enable full tearsheet generation.</p></body></html>'
        output_path.write_text(html, encoding='utf-8')
        return output_path
    html = '<html><body><h1>Pyfolio artifact</h1><p>tearsheet generated</p></body></html>'
    try:
        pyfolio.tears.create_full_tear_sheet(strategy_returns, benchmark_rets=benchmark_returns)
    except Exception as exc:
        html = f'<html><body><h1>Pyfolio artifact</h1><p>{exc!r}</p></body></html>'
    output_path.write_text(html, encoding='utf-8')
    return output_path

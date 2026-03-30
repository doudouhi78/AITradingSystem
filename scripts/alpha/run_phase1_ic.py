from __future__ import annotations

import json
from pathlib import Path

import alphalens as al
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity
from alpha_research.factors.fundamental import factor_pb_ratio_approx, probe_pb_interfaces
from alpha_research.factors.price_momentum import factor_momentum_20d
from alpha_research.factors.volume_liquidity import factor_turnover_20d

ROOT = Path(r"D:\AITradingSystem")
OUT_DIR = ROOT / "runtime" / "alpha_research" / "phase1"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUT_DIR / "factor_log.json"
START = "2020-01-01"
END = "2025-09-30"
PERIODS = (1, 5, 10, 20)


def summarize_html(title: str, instruments: list[str], n_obs: int, ic_mean: dict[str, float], icir: dict[str, float], notes: str) -> str:
    period_keys = sorted(ic_mean.keys(), key=lambda x: int(x))
    rows = ''.join(f'<tr><td>{k}d</td><td>{ic_mean[k]:.6f}</td><td>{icir[k]:.6f}</td></tr>' for k in period_keys)
    return f'''<!doctype html><html><head><meta charset="utf-8"><title>{title}</title></head><body>
<h2>{title}</h2>
<p>标的：{', '.join(instruments)}</p>
<p>区间：{START} 至 {END}</p>
<p>样本数：{n_obs}</p>
<table border="1" cellspacing="0" cellpadding="6"><tr><th>周期</th><th>IC均值</th><th>ICIR</th></tr>{rows}</table>
<p>{notes}</p>
</body></html>'''


def evaluate_factor(prices: pd.DataFrame, factor: pd.Series, html_path: Path, title: str, instruments: list[str], notes: str) -> tuple[dict[str, float], dict[str, float], int, str]:
    clean = al.utils.get_clean_factor_and_forward_returns(
        factor=factor,
        prices=prices,
        periods=PERIODS,
        max_loss=1.0,
    )
    al.tears.create_full_tear_sheet(clean, long_short=False)
    plt.close('all')
    ic = al.performance.factor_information_coefficient(clean)
    ic_mean = {str(col).split('D')[0]: float(ic[col].mean()) for col in ic.columns}
    icir = {str(col).split('D')[0]: float(ic[col].mean() / ic[col].std(ddof=0)) if float(ic[col].std(ddof=0)) != 0 else 0.0 for col in ic.columns}
    n_obs = int(len(clean))
    html_path.write_text(summarize_html(title, instruments, n_obs, ic_mean, icir, notes), encoding='utf-8')
    mean_20 = ic_mean.get('20', 0.0)
    if mean_20 > 0.02:
        conclusion = '有效'
    elif mean_20 > 0.0:
        conclusion = '存疑'
    else:
        conclusion = '无效'
    return ic_mean, icir, n_obs, conclusion


def main() -> None:
    universes = {
        'etf_top10': select_top_n_by_liquidity('etf', START, END, top_n=10),
        'stock_top50': select_top_n_by_liquidity('stock', START, END, top_n=50),
    }
    factor_log = []
    pb_probe = probe_pb_interfaces()
    pb_probe_note = '; '.join(f"{x['interface']}={x['status']}" for x in pb_probe)

    for universe_name, instruments in universes.items():
        asset_type = 'etf' if universe_name.startswith('etf') else 'stock'
        universe_dir = OUT_DIR / ('etf_universe' if asset_type == 'etf' else 'stock_universe')
        universe_dir.mkdir(parents=True, exist_ok=True)
        prices = load_prices(instruments, START, END, asset_type=asset_type)
        factor_input = load_factor_input(instruments, START, END, asset_type=asset_type)

        momentum = factor_momentum_20d(prices)
        m_html = universe_dir / 'momentum_20d_tearsheet.html'
        m_ic_mean, m_icir, m_obs, m_conclusion = evaluate_factor(
            prices, momentum, m_html, f'{universe_name} momentum_20d', instruments, 'Uses 20-day price momentum with shift(1).',
        )
        factor_log.append({
            'factor_id': f'momentum_20d_{asset_type}',
            'factor_name': 'momentum_20d',
            'universe': universe_name,
            'hypothesis': '价格延续性：过去20日强势标的在接下来N日仍跑赢',
            'test_date': '2026-03-30',
            'train_period': f'{START} to {END}',
            'instruments': instruments,
            'ic_mean': m_ic_mean,
            'icir': m_icir,
            'n_observations': m_obs,
            'conclusion': m_conclusion,
            'notes': 'shift(1) applied to avoid look-ahead bias.',
        })

        turnover = factor_turnover_20d(factor_input)
        t_html = universe_dir / 'turnover_20d_tearsheet.html'
        t_ic_mean, t_icir, t_obs, t_conclusion = evaluate_factor(
            prices, turnover, t_html, f'{universe_name} turnover_20d', instruments, 'Uses amount rolling mean(20) as turnover proxy, shifted by 1 day.',
        )
        factor_log.append({
            'factor_id': f'turnover_20d_{asset_type}',
            'factor_name': 'turnover_20d',
            'universe': universe_name,
            'hypothesis': '成交额持续活跃的标的在接下来N日仍更强',
            'test_date': '2026-03-30',
            'train_period': f'{START} to {END}',
            'instruments': instruments,
            'ic_mean': t_ic_mean,
            'icir': t_icir,
            'n_observations': t_obs,
            'conclusion': t_conclusion,
            'notes': 'Uses amount as turnover proxy; true turnover deferred to Phase 2.',
        })

        if asset_type == 'stock':
            pb_factor = factor_pb_ratio_approx(instruments, START, END)
            if not pb_factor.empty:
                pb_html = universe_dir / 'pb_ratio_tearsheet.html'
                pb_ic_mean, pb_icir, pb_obs, pb_conclusion = evaluate_factor(
                    prices, pb_factor, pb_html, f'{universe_name} pb_ratio', instruments, 'Approximate PB uses quarterly net asset per share from AkShare; no announcement lag handling.',
                )
                factor_log.append({
                    'factor_id': 'pb_ratio_stock',
                    'factor_name': 'pb_ratio',
                    'universe': universe_name,
                    'hypothesis': '低PB标的未来收益更高（用PB倒数表示）',
                    'test_date': '2026-03-30',
                    'train_period': f'{START} to {END}',
                    'instruments': instruments,
                    'ic_mean': pb_ic_mean,
                    'icir': pb_icir,
                    'n_observations': pb_obs,
                    'conclusion': pb_conclusion,
                    'notes': 'Phase 1 approximation using quarterly net asset per share; no announcement lag handling.',
                })
            else:
                factor_log.append({
                    'factor_id': 'pb_ratio_stock',
                    'factor_name': 'pb_ratio',
                    'universe': universe_name,
                    'hypothesis': '低PB标的未来收益更高（用PB倒数表示）',
                    'test_date': '2026-03-30',
                    'train_period': f'{START} to {END}',
                    'instruments': instruments,
                    'ic_mean': {},
                    'icir': {},
                    'n_observations': 0,
                    'conclusion': '数据接口待确认',
                    'notes': pb_probe_note,
                })
        else:
            factor_log.append({
                'factor_id': 'pb_ratio_etf',
                'factor_name': 'pb_ratio',
                'universe': universe_name,
                'hypothesis': 'ETF 不适用个股PB因子',
                'test_date': '2026-03-30',
                'train_period': f'{START} to {END}',
                'instruments': instruments,
                'ic_mean': {},
                'icir': {},
                'n_observations': 0,
                'conclusion': '数据接口待确认',
                'notes': 'PB ratio is stock-specific; ETF universe skipped in Phase 1.',
            })

    LOG_PATH.write_text(json.dumps(factor_log, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({
        'universes': universes,
        'record_count': len(factor_log),
        'pb_probe': pb_probe,
        'factor_log': str(LOG_PATH),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

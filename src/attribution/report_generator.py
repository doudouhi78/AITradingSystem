from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[2]
ATTR_ROOT = ROOT / 'runtime' / 'attribution'
REPORT_DIR = ATTR_ROOT / 'reports'


def _read_json(path: Path, fallback: Any):
    if path.exists():
        return json.loads(path.read_text(encoding='utf-8'))
    return fallback


def _safe_number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _health_dashboard(strategy: dict[str, Any], factor_reports: list[dict[str, Any]], diagnostics: dict[str, Any]) -> str:
    alpha = _safe_number(strategy.get('alpha'))
    alpha_color = '#16a34a' if alpha > 0 else '#dc2626'
    failed_factors = sum(1 for item in factor_reports if item.get('status') == 'failed')
    gate_groups = diagnostics.get('gate_status', [])
    blocked = sum(item.get('count', 0) for item in gate_groups if str(item.get('gate_status', '')).startswith('blocked'))
    return (
        "<div style='display:flex;gap:16px'>"
        f"<div style='padding:12px;border:1px solid #ccc'><h3>Alpha</h3><p style='color:{alpha_color}'>{alpha:.4f}</p></div>"
        f"<div style='padding:12px;border:1px solid #ccc'><h3>Factor</h3><p>{failed_factors} failed</p></div>"
        f"<div style='padding:12px;border:1px solid #ccc'><h3>Gate</h3><p>{blocked} blocked trades</p></div>"
        '</div>'
    )


def _build_rolling_alpha_plot(payload: list[dict[str, Any]]) -> str:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[item.get('date') for item in payload], y=[item.get('alpha') for item in payload], mode='lines', name='rolling_alpha'))
    fig.update_layout(title='Rolling Alpha (63d)', height=320, margin=dict(l=40, r=20, t=50, b=40))
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def _build_factor_drift_plot(reports: list[dict[str, Any]]) -> str:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[item.get('factor_name') for item in reports],
        y=[item.get('drift_ratio') for item in reports],
        marker_color=[
            '#16a34a' if item.get('status') == 'healthy' else '#f59e0b' if item.get('status') == 'warning' else '#dc2626'
            for item in reports
        ],
    ))
    fig.update_layout(title='Factor Drift Ratio', height=320, margin=dict(l=40, r=20, t=50, b=40))
    return fig.to_html(full_html=False, include_plotlyjs=False)


def generate_monthly_report(year: int, month: int) -> str:
    trade_diag = {
        'gate_status': _read_json(ATTR_ROOT / 'trade_diagnostics' / 'gate_status.json', []),
        'holding_bucket': _read_json(ATTR_ROOT / 'trade_diagnostics' / 'holding_bucket.json', []),
        'vol_bucket': _read_json(ATTR_ROOT / 'trade_diagnostics' / 'vol_bucket.json', []),
        'entry_month': _read_json(ATTR_ROOT / 'trade_diagnostics' / 'entry_month.json', []),
    }
    strategy = _read_json(ATTR_ROOT / 'strategy_attribution' / 'strategy_attribution.json', {})
    rolling_alpha = _read_json(ATTR_ROOT / 'strategy_attribution' / 'rolling_alpha.json', [])
    factor_drift = _read_json(ATTR_ROOT / 'factor_attribution' / 'factor_drift_report.json', {'reports': []})
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORT_DIR / f'attribution_report_{year}{month:02d}.html'
    gate_rows = ''.join(
        f"<tr><td>{row['gate_status']}</td><td>{row['count']}</td><td>{row['win_rate']:.2%}</td><td>{row['avg_pnl_pct']:.2f}</td><td>{row['sharpe']:.3f}</td></tr>"
        for row in trade_diag['gate_status']
    )
    alpha_value = _safe_number(strategy.get('alpha'))
    beta_value = _safe_number(strategy.get('beta'))
    excess_return = _safe_number(strategy.get('excess_return'))
    html = (
        f"<html><head><meta charset='utf-8'><title>Attribution Report {year}{month:02d}</title></head><body>"
        f"<h1>Attribution Report {year}-{month:02d}</h1>"
        f"<h2>1. 系统健康仪表盘</h2>{_health_dashboard(strategy, factor_drift.get('reports', []), trade_diag)}"
        f"<h2>2. 交易诊断表格</h2><table border='1' cellspacing='0' cellpadding='6'><tr><th>Gate</th><th>Count</th><th>Win Rate</th><th>Avg PnL %</th><th>Sharpe</th></tr>{gate_rows}</table>"
        f"<h2>3. 策略层归因</h2><p>Alpha={alpha_value:.4f} / Beta={beta_value:.4f} / Excess Return={excess_return:.4f}</p>{_build_rolling_alpha_plot(rolling_alpha)}"
        f"<h2>4. 因子状态</h2>{_build_factor_drift_plot(factor_drift.get('reports', []))}</body></html>"
    )
    output_path.write_text(html, encoding='utf-8')
    return str(output_path)

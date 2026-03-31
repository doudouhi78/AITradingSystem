from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from jinja2 import Template

ROOT = Path(__file__).resolve().parents[1]
COORD = ROOT / "coordination"
REPORT_DIR = ROOT / "runtime" / "reports"
REPORT_PATH = REPORT_DIR / "strategy_report.html"
SIGNAL_DIR = ROOT / "runtime" / "paper_trading" / "signals"
PAPER_LOG = ROOT / "runtime" / "paper_trading" / "paper_trade_log.csv"
EXPERIMENT_RESULTS = ROOT / "runtime" / "experiments" / "exp-20260329-008-parquet-entry25-exit20" / "results.json"

TEMPLATE = Template("""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>Strategy Report</title>
  <style>
    body { font-family: Arial, sans-serif; background: #fff; color: #111; margin: 24px; }
    h1,h2 { margin-bottom: 8px; }
    .block { border: 1px solid #ddd; padding: 16px; margin-bottom: 16px; }
    .ok { color: #0a7a28; font-weight: bold; }
    .bad { color: #b00020; font-weight: bold; }
    table { border-collapse: collapse; width: 100%; margin-top: 8px; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    .metric { font-weight: bold; }
  </style>
</head>
<body>
  <h1>510300 趋势突破（entry=25, exit=20）</h1>
  <div class="block">
    <h2>区块1：策略基本信息</h2>
    <p><span class="metric">当前状态：</span>{{ status }}</p>
    <p><span class="metric">报告生成时间：</span>{{ generated_at }}</p>
  </div>

  <div class="block">
    <h2>区块2：回测核心指标（修正后口径）</h2>
    <table>
      <tr><th>指标</th><th>数值</th></tr>
      <tr><td>Sharpe</td><td><strong>{{ metrics.sharpe }}</strong></td></tr>
      <tr><td>最大回撤</td><td><strong>{{ metrics.max_drawdown }}</strong></td></tr>
      <tr><td>年化收益</td><td><strong>{{ metrics.annual_return }}</strong></td></tr>
      <tr><td>交易次数</td><td><strong>{{ metrics.trade_count }}</strong></td></tr>
      <tr><td>胜率</td><td><strong>{{ metrics.win_rate }}</strong></td></tr>
      <tr><td>WFO ratio</td><td><strong>{{ wfo_ratio }}</strong></td></tr>
      <tr><td>MC P(max_dd&gt;18%)</td><td><strong>{{ mc_p }}</strong></td></tr>
    </table>
  </div>

  <div class="block">
    <h2>区块3：Gate 系统状态（当日）</h2>
    <p>今日是否允许入场：
      {% if gate.allowed %}<span class="ok">允许</span>{% else %}<span class="bad">阻断</span>{% endif %}
    </p>
    <table>
      <tr><th>Gate</th><th>状态</th><th>原因</th></tr>
      {% for name, detail in gate.details.items() %}
      <tr>
        <td>{{ name }}</td>
        <td>{% if detail.allowed %}<span class="ok">允许</span>{% else %}<span class="bad">阻断</span>{% endif %}</td>
        <td>{{ detail.reason or '-' }}</td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <div class="block">
    <h2>区块4：仓位管理参数</h2>
    <p><span class="metric">ATR period：</span>14</p>
    <p><span class="metric">ATR multiplier：</span>2.0</p>
    <p><span class="metric">risk_per_trade：</span>1.0%</p>
    <p><span class="metric">当前建议仓位：</span>{{ position }}</p>
    <p><span class="metric">当前建议止损：</span>{{ stop_price }}</p>
  </div>

  <div class="block">
    <h2>区块5：仿真日志摘要</h2>
    {% if paper_rows %}
    <table>
      <tr><th>date</th><th>signal</th><th>assumed_price</th><th>actual_open</th><th>slippage</th><th>position</th><th>notes</th></tr>
      {% for row in paper_rows %}
      <tr>
        <td>{{ row.date }}</td><td>{{ row.signal }}</td><td>{{ row.assumed_price }}</td><td>{{ row.actual_open }}</td><td>{{ row.slippage }}</td><td>{{ row.position }}</td><td>{{ row.notes }}</td>
      </tr>
      {% endfor %}
    </table>
    <p><span class="metric">平均滑点：</span>{{ avg_slippage }}</p>
    {% else %}
    <p>当前无 `paper_trade_log.csv`，仅完成信号级仿真。</p>
    {% endif %}
  </div>
</body>
</html>
""")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_signal_payload() -> dict:
    signal_files = sorted(SIGNAL_DIR.glob("*.json"))
    if not signal_files:
        return {}
    return load_json(signal_files[-1])


def load_paper_rows() -> tuple[list[dict], str]:
    if not PAPER_LOG.exists():
        return [], "-"
    import csv
    rows = []
    slippages = []
    with PAPER_LOG.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            try:
                slippages.append(float(row.get("slippage") or 0))
            except ValueError:
                pass
    avg = sum(slippages) / len(slippages) if slippages else 0.0
    return rows[-10:], f"{avg:.4%}"


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    module_a = load_json(COORD / "module_a_result.json")
    wfo = load_json(COORD / "wfo_result.json")
    mc = load_json(COORD / "mc_result.json")
    signal = latest_signal_payload()
    experiment_results = load_json(EXPERIMENT_RESULTS)
    paper_rows, avg_slippage = load_paper_rows()

    gate_payload = signal.get("gate_result", {})
    details = gate_payload.get("gate_details", {})
    gate_view = {
        "allowed": gate_payload.get("allowed", True),
        "details": {name: {"allowed": d.get("allowed", True), "reason": d.get("reason")} for name, d in details.items()},
    }
    metrics = experiment_results["metrics_summary"]
    html = TEMPLATE.render(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        status="仿真中",
        metrics={
            "sharpe": f"{metrics['sharpe']:.4f}",
            "max_drawdown": f"{metrics['max_drawdown']:.2%}",
            "annual_return": f"{metrics['annual_return']:.2%}",
            "trade_count": metrics['trade_count'],
            "win_rate": f"{metrics['win_rate']:.2%}",
        },
        wfo_ratio=f"{wfo['ratio']:.4f}",
        mc_p=f"{mc['p_max_drawdown_gt_18pct']:.3f}",
        gate=gate_view,
        position=(f"{signal.get('position_fraction', 0.0):.1%}（{signal.get('suggested_qty', 0)}股）" if signal else "-"),
        stop_price=(f"{signal.get('stop_price'):.3f}" if signal.get('stop_price') is not None else "-"),
        paper_rows=paper_rows,
        avg_slippage=avg_slippage,
    )
    REPORT_PATH.write_text(html, encoding="utf-8")
    print(f"report={REPORT_PATH}")
    print(f"size={REPORT_PATH.stat().st_size}")
    print(f"sharpe={metrics['sharpe']:.4f}")
    print(f"wfo_ratio={wfo['ratio']:.4f}")
    print(f"gate_allowed={gate_view['allowed']}")


if __name__ == '__main__':
    main()

from __future__ import annotations

import json
from pathlib import Path

import alphalens as al
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from alpha_research.data_loader import load_prices
from alpha_research.factors.price_momentum import factor_momentum_20d

ROOT = Path(r"D:\AITradingSystem")
OUT_DIR = ROOT / "runtime" / "alpha_research" / "phase1"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TEARSHEET_PATH = OUT_DIR / "momentum_20d_tearsheet.html"
LOG_PATH = OUT_DIR / "factor_log.json"
START = "2020-01-01"
END = "2025-09-30"
PERIODS = (1, 5, 10, 20)


def select_instruments() -> list[str]:
    etf_dir = ROOT / "runtime" / "market_data" / "cn_etf"
    rows = []
    for path in etf_dir.glob("*.parquet"):
        if path.name.startswith("_"):
            continue
        df = pd.read_parquet(path, columns=["trade_date", "amount"]).copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        mask = (df["trade_date"] >= pd.Timestamp(START)) & (df["trade_date"] <= pd.Timestamp(END))
        sample = df.loc[mask]
        if sample.empty:
            continue
        rows.append({"symbol": path.stem, "avg_amount": float(sample["amount"].astype(float).mean())})
    picked = pd.DataFrame(rows).sort_values("avg_amount", ascending=False).head(10)
    return picked["symbol"].tolist()


def build_html(metrics: dict[str, dict[str, float]], instruments: list[str], n_obs: int) -> str:
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>momentum_20d tearsheet</title></head><body>
<h2>momentum_20d 因子 AlphaLens 评估</h2>
<p>标的：{', '.join(instruments)}</p>
<p>区间：{START} 至 {END}</p>
<p>样本数：{n_obs}</p>
<table border="1" cellspacing="0" cellpadding="6">
<tr><th>周期</th><th>IC均值</th><th>ICIR</th></tr>
{''.join(f'<tr><td>{k}d</td><td>{metrics["ic_mean"][k]:.6f}</td><td>{metrics["icir"][k]:.6f}</td></tr>' for k in ['1','5','10','20'])}
</table>
<p>注：图表由 AlphaLens 生成，数值从 clean factor data 直接汇总。</p>
</body></html>"""


def main() -> None:
    instruments = select_instruments()
    prices = load_prices(instruments, START, END)
    factor = factor_momentum_20d(prices)
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

    html = build_html({"ic_mean": ic_mean, "icir": icir}, instruments, n_obs)
    TEARSHEET_PATH.write_text(html, encoding='utf-8')

    mean_20 = ic_mean.get("20", 0.0)
    if mean_20 > 0.02:
        conclusion = "有效"
    elif mean_20 > 0.0:
        conclusion = "存疑"
    else:
        conclusion = "无效"

    payload = [
        {
            "factor_id": "momentum_20d",
            "hypothesis": "价格延续性：过去20日强势标的在接下来N日仍跑赢",
            "test_date": "2026-03-30",
            "train_period": f"{START} to {END}",
            "instruments": instruments,
            "ic_mean": ic_mean,
            "icir": icir,
            "n_observations": n_obs,
            "conclusion": conclusion,
            "notes": "ETF universe only, top-10 by average amount; factor uses shift(1) to avoid look-ahead bias."
        }
    ]
    LOG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps({
        "alphalens_version": getattr(al, "__version__", "unknown"),
        "instruments": instruments,
        "n_observations": n_obs,
        "ic_mean": ic_mean,
        "icir": icir,
        "tearsheet": str(TEARSHEET_PATH),
        "factor_log": str(LOG_PATH),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

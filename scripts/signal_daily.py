from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_dev_os.gate import GateScheduler
from ai_dev_os.risk import compute_quantity, compute_stop_price, wilder_atr
from attribution.report_generator import generate_monthly_report

DATA_PATH = ROOT / "runtime" / "market_data" / "cn_etf" / "510300.parquet"
SIGNAL_DIR = ROOT / "runtime" / "paper_trading" / "signals"
FORWARD_SIM_PATH = ROOT / "runtime" / "paper_trading" / "forward_sim_equity.csv"
ENTRY_WINDOW = 25
EXIT_WINDOW = 20
ACCOUNT_EQUITY = 100000.0


def load_equity_series() -> list[float]:
    if FORWARD_SIM_PATH.exists():
        df = pd.read_csv(FORWARD_SIM_PATH)
        if "equity" in df.columns and not df.empty:
            return df["equity"].astype(float).tolist()
    return [1.0]


def load_trading_dates(data_path: Path = DATA_PATH) -> pd.DatetimeIndex:
    df = pd.read_parquet(data_path, columns=['trade_date']).dropna()
    dates = pd.to_datetime(df['trade_date']).sort_values().drop_duplicates()
    return pd.DatetimeIndex(dates)



def is_month_end(today: str | pd.Timestamp | datetime, trading_dates: pd.DatetimeIndex | None = None) -> bool:
    trade_date = pd.Timestamp(today).normalize()
    schedule = trading_dates if trading_dates is not None else load_trading_dates()
    if len(schedule) == 0:
        return False
    normalized = pd.DatetimeIndex(pd.to_datetime(schedule)).normalize().sort_values().unique()
    month_dates = normalized[(normalized.year == trade_date.year) & (normalized.month == trade_date.month)]
    if len(month_dates) == 0:
        return False
    return trade_date == month_dates[-1]



def maybe_generate_monthly_attribution_report(today: str | pd.Timestamp | datetime) -> str | None:
    trade_date = pd.Timestamp(today).normalize()
    if not is_month_end(trade_date):
        return None
    output = generate_monthly_report(trade_date.year, trade_date.month)
    relative_output = f"runtime/attribution/reports/attribution_report_{trade_date:%Y%m}.html"
    print(f"月度归因报告已生成：{relative_output}")
    return output



def main() -> None:
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now().strftime("%Y-%m-%d")
    df = pd.read_parquet(DATA_PATH).sort_values("trade_date").reset_index(drop=True)
    latest = df.iloc[-1]
    trade_date = pd.Timestamp(latest['trade_date']).normalize()
    close = df["close"].astype(float)
    entry_threshold = float(close.shift(1).rolling(ENTRY_WINDOW).max().iloc[-1])
    exit_threshold = float(close.shift(1).rolling(EXIT_WINDOW).min().iloc[-1])
    last_close = float(latest["close"])
    if last_close > entry_threshold:
        signal = "BUY"
    elif last_close < exit_threshold:
        signal = "SELL"
    else:
        signal = "HOLD"

    scheduler = GateScheduler()
    gate_result = scheduler.evaluate(
        date=str(latest["trade_date"])[:10],
        equity_series=load_equity_series(),
        etf_df=df,
    )
    gate_block_message = None
    if signal == "BUY" and not gate_result["allowed"]:
        signal = "HOLD"
        gate_block_message = f"Gate 阻断入场信号：{gate_result['blocked_by']} — {gate_result['reason']}"

    atr_value = None
    stop_price = None
    suggested_qty = 0
    position_frac = 0.0
    risk_warning = None
    if signal == "BUY":
        atr_series = wilder_atr(df["high"], df["low"], df["close"])
        atr_last = float(atr_series.iloc[-1]) if pd.notna(atr_series.iloc[-1]) else None
        if atr_last is None:
            risk_warning = "ATR 数据不足，无法生成止损与仓位建议。"
        else:
            atr_value = atr_last
            entry_price = float(latest["open"])
            stop_price = compute_stop_price(entry_price, atr_value)
            suggested_qty, position_frac = compute_quantity(ACCOUNT_EQUITY, entry_price, stop_price)

    rationale = f"最新数据日={str(latest['trade_date'])[:10]}，收盘价={last_close:.4f}，25日高点={entry_threshold:.4f}，20日低点={exit_threshold:.4f}"
    payload = {
        "date": run_date,
        "signal": signal,
        "close": last_close,
        "entry_threshold": entry_threshold,
        "exit_threshold": exit_threshold,
        "rationale": rationale,
        "gate_result": gate_result,
        "atr": atr_value,
        "stop_price": stop_price,
        "suggested_qty": suggested_qty,
        "position_fraction": position_frac,
        "risk_warning": risk_warning,
    }
    out_path = SIGNAL_DIR / f"{run_date.replace('-', '')}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"日期: {run_date}")
    print(f"信号: {signal}")
    print(f"依据: {rationale}")
    print(
        "Gate状态: "
        f"{'允许' if gate_result['allowed'] else '阻断'}"
        f"（blocked_by={gate_result['blocked_by']}, reason={gate_result['reason']}）"
    )
    if gate_block_message:
        print(gate_block_message)
    if signal == "BUY":
        if risk_warning:
            print(risk_warning)
        else:
            print(f"建议仓位：{position_frac:.1%}（{suggested_qty}股），止损价：{stop_price:.3f}，ATR：{atr_value:.4f}")
    print("建议执行价: 次日开盘价（需人工填入）")
    print(f"信号文件: {out_path}")
    maybe_generate_monthly_attribution_report(trade_date)


if __name__ == "__main__":
    main()
    # 每日信号生成后自动更新报告
    try:
        import subprocess

        subprocess.run(
            [str(ROOT / ".venv" / "Scripts" / "python.exe"), "scripts/generate_report.py"],
            cwd=str(ROOT),
            check=True,
        )
        print("报告已更新：runtime/reports/strategy_report.html")
    except Exception as e:
        print(f"报告生成失败（不影响信号）：{e}")

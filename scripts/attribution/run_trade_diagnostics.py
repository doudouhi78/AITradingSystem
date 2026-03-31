from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from attribution.trade_diagnostics import EXPERIMENTS_DIR, load_trades, run_trade_diagnostics  # noqa: E402


DIMENSION_LABELS = {
    'gate_status': 'gate',
    'holding_bucket': 'holding',
    'vol_bucket': 'vol',
    'entry_month': 'month',
}


def _summarize_dimension(name: str, rows: list[dict]) -> None:
    print(f'[{DIMENSION_LABELS[name]}]')
    if not rows:
        print('  no data')
        return
    ranked = sorted(rows, key=lambda item: (float(item.get('avg_pnl', 0.0)), float(item.get('win_rate', 0.0))))
    bottom = ranked[0]
    top = ranked[-1]
    group_key = name
    print(
        '  top: '
        f"{top.get(group_key)} | count={top.get('count')} | avg_pnl={float(top.get('avg_pnl', 0.0)):.2f}% | "
        f"win_rate={float(top.get('win_rate', 0.0)):.2%} | avg_holding={float(top.get('avg_holding', 0.0)):.1f}"
    )
    print(
        '  bottom: '
        f"{bottom.get(group_key)} | count={bottom.get('count')} | avg_pnl={float(bottom.get('avg_pnl', 0.0)):.2f}% | "
        f"win_rate={float(bottom.get('win_rate', 0.0)):.2%} | avg_holding={float(bottom.get('avg_holding', 0.0)):.1f}"
    )


def main() -> None:
    frames: list[pd.DataFrame] = []
    for experiment_dir in sorted(EXPERIMENTS_DIR.iterdir()):
        if not experiment_dir.is_dir():
            continue
        if not (experiment_dir / 'manifest.json').exists():
            continue
        try:
            trade_df = load_trades(experiment_dir.name)
        except Exception as exc:
            print(f'skip {experiment_dir.name}: {exc}')
            continue
        if not trade_df.empty:
            frames.append(trade_df)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result = run_trade_diagnostics(merged)
    print(f'loaded_experiments={len(frames)} total_trades={len(merged)}')
    for dimension in ['gate_status', 'holding_bucket', 'vol_bucket', 'entry_month']:
        _summarize_dimension(dimension, result.get(dimension, []))


if __name__ == '__main__':
    main()

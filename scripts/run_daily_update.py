from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_pipeline.data_updater import run_daily_update


def main() -> int:
    parser = argparse.ArgumentParser(description='Run daily data updater pipeline')
    parser.add_argument('--etf', action='store_true', help='Only update ETF market data')
    parser.add_argument('--stocks', action='store_true', help='Only update CSI300 stock market data')
    args = parser.parse_args()

    only_specific = args.etf or args.stocks
    summary = run_daily_update(
        etf=args.etf or not only_specific,
        stocks=args.stocks or not only_specific,
        alternative=not only_specific,
        valuation=not only_specific,
    )

    modules = summary.get('modules', {})
    print('Daily update summary:')
    for name, payload in modules.items():
        if isinstance(payload, dict):
            print(f"- {name}: {json.dumps(payload, ensure_ascii=False)}")
    print(f"- update_log: {summary.get('update_log')}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

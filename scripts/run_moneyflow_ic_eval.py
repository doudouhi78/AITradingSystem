from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from alpha_research.factors import moneyflow_factors


FACTOR_FUNCTIONS = {
    'mf_net_inflow_5d': moneyflow_factors.mf_net_inflow_5d,
    'mf_net_inflow_20d': moneyflow_factors.mf_net_inflow_20d,
    'mf_large_order_ratio': moneyflow_factors.mf_large_order_ratio,
    'mf_smart_money': moneyflow_factors.mf_smart_money,
    'mf_inflow_acceleration': moneyflow_factors.mf_inflow_acceleration,
}
MARKET_INPUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'factor_input_sample.parquet'


def main() -> int:
    if not MARKET_INPUT_PATH.exists():
        raise FileNotFoundError(f'Factor input file not found: {MARKET_INPUT_PATH}')

    factor_input = pd.read_parquet(MARKET_INPUT_PATH)
    if {'date', 'asset'}.issubset(factor_input.columns):
        factor_input = factor_input.set_index(['date', 'asset']).sort_index()

    summary = {}
    for name, func in FACTOR_FUNCTIONS.items():
        factor = func(factor_input)
        summary[name] = int(len(factor))

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

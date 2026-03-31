from __future__ import annotations

import json

from attribution.strategy_attribution import compute_rolling_alpha, generate_pyfolio_tearsheet, load_returns_series, run_strategy_attribution

DEFAULT_EXPERIMENT = "exp-20260329-008-parquet-entry25-exit20"


def main() -> None:
    strategy_returns, benchmark_returns = load_returns_series(DEFAULT_EXPERIMENT)
    payload = run_strategy_attribution(strategy_returns, benchmark_returns)
    rolling = compute_rolling_alpha(strategy_returns, benchmark_returns)
    tearsheet = generate_pyfolio_tearsheet(strategy_returns, benchmark_returns)
    print(json.dumps({"metrics": payload, "rolling_alpha_points": int(len(rolling)), "tearsheet": str(tearsheet)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

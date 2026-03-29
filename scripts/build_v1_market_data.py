from __future__ import annotations

import argparse
from datetime import datetime

from ai_dev_os.market_data_v1 import DEFAULT_START_DATE, fetch_pool_market_data, generate_pool_lists


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate China pool lists and build V1 market data.")
    parser.add_argument("--pool", choices=["china", "cn_etf", "cn_stock"], default="china")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--skip-generate", action="store_true")
    args = parser.parse_args()

    pools = ["cn_etf", "cn_stock"] if args.pool == "china" else [args.pool]
    if not args.skip_generate:
        generated = generate_pool_lists(selected_pools=pools)
        for pool_name in pools:
            print(f"generated_pool={pool_name} path={generated[pool_name]}")

    for pool_name in pools:
        result = fetch_pool_market_data(
            pool_name,
            start_date=args.start_date,
            end_date=args.end_date,
            skip_existing=(pool_name == "cn_stock"),
        )
        print(
            f"pool={result['pool_name']} symbols={result['symbol_count']} "
            f"written={len(result['written_files'])} skipped={len(result['skipped_symbols'])} failed={len(result['failed_symbols'])} "
            f"summary={result['summary_path']}"
        )


if __name__ == "__main__":
    main()

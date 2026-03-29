from __future__ import annotations

import argparse

from ai_dev_os.market_data_quality import validate_market_pool


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate market data pools with Pandera.")
    parser.add_argument("--pool", choices=["china", "cn_etf", "cn_stock"], default="china")
    parser.add_argument("--warmup-rows", type=int, default=60)
    args = parser.parse_args()

    pools = ["cn_etf", "cn_stock"] if args.pool == "china" else [args.pool]
    for pool_name in pools:
        result = validate_market_pool(pool_name, warmup_rows=args.warmup_rows)
        print(
            f"pool={result['pool_name']} total={result['total_files']} passed={result['success_count']} "
            f"failed={result['failed_count']} summary={result['summary_path']}"
        )


if __name__ == "__main__":
    main()

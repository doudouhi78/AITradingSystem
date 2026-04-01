from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_pipeline.tushare_downloader import TushareDownloader


def main() -> int:
    parser = argparse.ArgumentParser(description='Download A-share full data foundation via Tushare Pro')
    parser.add_argument('--batch', type=int, choices=[1, 2, 3, 4, 5, 6], help='Only run a specific batch')
    parser.add_argument('--resume', action='store_true', help='Use progress file for resume semantics')
    args = parser.parse_args()

    downloader = TushareDownloader()
    if args.batch:
        result = downloader.run_batch(args.batch)
        print(json.dumps({result.batch_name: result.metrics}, ensure_ascii=False, indent=2, default=str))
        return 0

    results = downloader.run_all()
    payload = {item.batch_name: item.metrics for item in results}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

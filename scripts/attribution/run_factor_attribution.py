from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from attribution.factor_attribution import run_factor_attribution  # noqa: E402


def main() -> None:
    print('Running factor attribution ...')
    result = run_factor_attribution()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print('factor_drift_report.json written.')


if __name__ == '__main__':
    main()

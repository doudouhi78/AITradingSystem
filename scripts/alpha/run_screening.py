from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(r"D:\AITradingSystem")
IN_PATH = ROOT / "runtime" / "alpha_research" / "phase2" / "ic_batch_result.json"


def main() -> None:
    if not IN_PATH.exists():
        raise FileNotFoundError(IN_PATH)
    payload = json.loads(IN_PATH.read_text(encoding="utf-8"))
    summary = {}
    for universe_name, universe_payload in payload.get("universes", {}).items():
        results = universe_payload.get("results", [])
        summary[universe_name] = {
            "passed": [r["factor_name"] for r in results if r.get("passed")],
            "rejected": [r["factor_name"] for r in results if not r.get("passed")],
            "deduplicated_passed_factors": universe_payload.get("deduplicated_passed_factors", []),
        }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

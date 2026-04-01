from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


@dataclass
class CheckResult:
    ok: bool
    label: str
    detail: str

    def render(self) -> str:
        prefix = "[✓]" if self.ok else "[!]"
        return f"{prefix} {self.label}: {self.detail}"


def resolve_python_executable() -> str:
    candidates = [
        Path.cwd() / ".venv" / "Scripts" / "python.exe",
        Path(__file__).resolve().parents[1] / ".venv" / "Scripts" / "python.exe",
        Path(r"D:\AITradingSystem\.venv\Scripts\python.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [resolve_python_executable(), "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )


def parse_nvidia_smi() -> Optional[dict[str, str]]:
    if shutil.which("nvidia-smi") is None:
        return None

    cmd = [
        "nvidia-smi",
        "--query-gpu=name,memory.total,driver_version,cuda_version",
        "--format=csv,noheader,nounits",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0 or not proc.stdout.strip():
        return None

    parts = [part.strip() for part in proc.stdout.strip().split(",")]
    if len(parts) < 4:
        return None
    return {
        "name": parts[0],
        "memory_total_mb": parts[1],
        "driver_version": parts[2],
        "cuda_version": parts[3],
    }


def verify_torch() -> CheckResult:
    code = """
import json
try:
    import torch
except Exception as exc:
    print(json.dumps({"ok": False, "error": f"import failed: {exc}"}))
else:
    if not torch.cuda.is_available():
        print(json.dumps({"ok": False, "error": "torch installed but CUDA unavailable"}))
    else:
        print(json.dumps({
            "ok": True,
            "name": torch.cuda.get_device_name(0),
            "torch_version": torch.__version__,
            "cuda_runtime": torch.version.cuda,
        }))
"""
    proc = run_python(code)
    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except json.JSONDecodeError:
        payload = {"ok": False, "error": (proc.stderr.strip() or "unknown error")}

    if payload.get("ok"):
        detail = (
            f"{payload['name']}, CUDA {payload['cuda_runtime']}, "
            f"torch {payload['torch_version']}"
        )
        return CheckResult(True, "PyTorch CUDA", detail)

    smi = parse_nvidia_smi()
    if smi:
        detail = (
            f"unavailable ({payload.get('error', 'not installed')}); "
            f"system GPU={smi['name']}, {int(smi['memory_total_mb']) / 1024:.1f}GB, "
            f"driver CUDA={smi['cuda_version']}"
        )
    else:
        detail = f"unavailable ({payload.get('error', 'not installed')})"
    return CheckResult(False, "PyTorch CUDA", detail)


def verify_lightgbm() -> CheckResult:
    code = r"""
import json
import numpy as np

try:
    import lightgbm as lgb
except Exception as exc:
    print(json.dumps({"ok": False, "error": f"import failed: {exc}"}))
    raise SystemExit(0)

rng = np.random.default_rng(42)
X = rng.random((128, 8), dtype=np.float32)
y = (X[:, 0] + X[:, 1] > 1.0).astype(np.int32)
train_set = lgb.Dataset(X, label=y)
params = {
    "objective": "binary",
    "metric": "binary_logloss",
    "device": "gpu",
    "verbosity": -1,
    "num_leaves": 15,
    "min_data_in_leaf": 5,
    "seed": 42,
}

try:
    lgb.train(params, train_set, num_boost_round=5)
    print(json.dumps({"ok": True, "version": lgb.__version__}))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc), "version": lgb.__version__}))
"""
    proc = run_python(code)
    try:
        payload = json.loads(proc.stdout.strip().splitlines()[0])
    except (json.JSONDecodeError, IndexError):
        payload = {"ok": False, "error": proc.stderr.strip() or "unknown error"}

    if payload.get("ok"):
        return CheckResult(True, "LightGBM GPU", "OK")
    return CheckResult(
        False,
        "LightGBM GPU",
        f"failed ({payload.get('error', 'unknown error')})",
    )


def verify_cudf() -> CheckResult:
    code = """
import json
try:
    import cudf
except Exception as exc:
    print(json.dumps({"ok": False, "error": f"import failed: {exc}"}))
else:
    frame = cudf.DataFrame({"a": [1, 2, 3]})
    print(json.dumps({"ok": True, "rows": int(len(frame)), "version": cudf.__version__}))
"""
    proc = run_python(code)
    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except json.JSONDecodeError:
        payload = {"ok": False, "error": proc.stderr.strip() or "unknown error"}

    if payload.get("ok"):
        return CheckResult(
            True,
            "cuDF",
            f"OK (cudf {payload['version']}, rows={payload['rows']})",
        )
    return CheckResult(False, "cuDF", "不可用（Windows限制），已切换PyTorch tensor方案")


def main() -> int:
    results = [verify_torch(), verify_lightgbm(), verify_cudf()]
    for result in results:
        print(result.render())

    mandatory_passed = all(result.ok for result in results[:2])
    return 0 if mandatory_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

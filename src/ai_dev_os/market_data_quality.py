from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from ai_dev_os.market_data_v1 import BAR_COLUMNS, MARKET_DATA_ROOT, POOL_CONFIGS

QUALITY_ROOT = MARKET_DATA_ROOT / "quality"
PRICE_COLUMNS = ["open", "high", "low", "close"]
NUMERIC_COLUMNS = PRICE_COLUMNS + ["volume", "amount"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "market": Column(str, nullable=False, coerce=True),
            "symbol": Column(str, nullable=False, coerce=True),
            "security_type": Column(str, nullable=False, coerce=True),
            "trade_date": Column(pa.DateTime, nullable=False, coerce=True),
            "open": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "high": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "low": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "close": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "volume": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "amount": Column(float, nullable=False, checks=Check.ge(0), coerce=True),
            "adjustment_mode": Column(str, nullable=False, coerce=True),
            "is_suspended": Column(bool, nullable=False, coerce=True),
            "listed_date": Column(str, nullable=True, coerce=True),
            "delisted_date": Column(str, nullable=True, coerce=True),
        },
        strict=True,
        coerce=True,
    )


def validate_market_frame(frame: pd.DataFrame, *, expected_market: str, expected_security_type: str, warmup_rows: int = 0) -> dict[str, Any]:
    checks_passed: list[str] = []
    checks_failed: list[str] = []

    missing = [column for column in BAR_COLUMNS if column not in frame.columns]
    if missing:
        return {
            "status": "failed",
            "checks_passed": checks_passed,
            "checks_failed": [f"missing_columns:{','.join(missing)}"],
            "validated_rows": int(len(frame)),
        }

    normalized = frame.copy()
    try:
        normalized = _schema().validate(normalized)
        checks_passed.append("pandera_schema_validation")
    except Exception as exc:
        checks_failed.append(f"pandera_schema_validation:{type(exc).__name__}")
        return {
            "status": "failed",
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
            "validated_rows": int(len(frame)),
        }

    if normalized["trade_date"].is_monotonic_increasing and not normalized["trade_date"].duplicated().any():
        checks_passed.append("trade_date_sorted_unique")
    else:
        checks_failed.append("trade_date_sorted_unique")

    body = normalized.iloc[warmup_rows:] if warmup_rows > 0 else normalized
    null_violations = [column for column in NUMERIC_COLUMNS if body[column].isna().any()]
    if null_violations:
        checks_failed.append(f"null_after_warmup:{','.join(null_violations)}")
    else:
        checks_passed.append("null_after_warmup")

    price_bounds_ok = (
        (normalized["high"] >= normalized[["open", "close", "low"]].max(axis=1))
        & (normalized["low"] <= normalized[["open", "close", "high"]].min(axis=1))
    ).all()
    if price_bounds_ok:
        checks_passed.append("price_bounds_consistent")
    else:
        checks_failed.append("price_bounds_consistent")

    if normalized["market"].nunique() == 1 and str(normalized["market"].iloc[0]) == expected_market:
        checks_passed.append("market_consistent")
    else:
        checks_failed.append("market_consistent")

    if normalized["security_type"].nunique() == 1 and str(normalized["security_type"].iloc[0]) == expected_security_type:
        checks_passed.append("security_type_consistent")
    else:
        checks_failed.append("security_type_consistent")

    if normalized["adjustment_mode"].nunique() == 1 and str(normalized["adjustment_mode"].iloc[0]) == "qfq":
        checks_passed.append("adjustment_mode_qfq")
    else:
        checks_failed.append("adjustment_mode_qfq")

    if normalized.loc[normalized["is_suspended"], PRICE_COLUMNS].isna().all(axis=1).all():
        checks_passed.append("suspension_no_fabricated_price")
    elif not normalized["is_suspended"].any():
        checks_passed.append("suspension_no_fabricated_price")
    else:
        checks_failed.append("suspension_no_fabricated_price")

    return {
        "status": "passed" if not checks_failed else "failed",
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "validated_rows": int(len(normalized)),
    }


def quality_summary_path(pool_name: str) -> Path:
    QUALITY_ROOT.mkdir(parents=True, exist_ok=True)
    return QUALITY_ROOT / f"{pool_name}_quality_summary.json"


def validate_market_pool(pool_name: str, *, warmup_rows: int = 60) -> dict[str, Any]:
    config = POOL_CONFIGS[pool_name]
    data_dir = config.data_dir
    parquet_files = sorted(data_dir.glob("*.parquet"))
    failures: list[dict[str, Any]] = []
    success_count = 0

    for path in parquet_files:
        try:
            frame = pd.read_parquet(path)
            result = validate_market_frame(frame, expected_market=config.market, expected_security_type=config.security_type, warmup_rows=warmup_rows)
            if result["status"] == "passed":
                success_count += 1
            else:
                failures.append({"symbol": path.stem, "checks_failed": result["checks_failed"]})
        except Exception as exc:
            failures.append({"symbol": path.stem, "checks_failed": [f"read_or_validate:{type(exc).__name__}"]})

    payload = {
        "pool_name": pool_name,
        "total_files": len(parquet_files),
        "success_count": success_count,
        "failed_count": len(failures),
        "updated_at": _now_iso(),
        "failures": failures,
    }
    summary_path = quality_summary_path(pool_name)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload["summary_path"] = str(summary_path)
    return payload

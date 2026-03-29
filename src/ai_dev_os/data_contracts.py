from __future__ import annotations

from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check
from pandera.pandas import Column
from pandera.pandas import DataFrameSchema

from ai_dev_os.project_objects import DataContractSpec
from ai_dev_os.project_objects import DatasetSnapshot
from ai_dev_os.project_objects import MetricsSummary
from ai_dev_os.project_objects import RuleExpression
from ai_dev_os.project_objects import ValidationRecord
from ai_dev_os.project_objects import validate_data_contract_spec
from ai_dev_os.project_objects import validate_dataset_snapshot
from ai_dev_os.project_objects import validate_metrics_summary
from ai_dev_os.project_objects import validate_rule_expression


DEFAULT_REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]


def build_default_data_contract_spec(
    dataset_snapshot: DatasetSnapshot | dict[str, Any],
    *,
    created_at: str,
    warmup_rows: int = 60,
) -> DataContractSpec:
    snapshot = validate_dataset_snapshot(dataset_snapshot)
    return validate_data_contract_spec(
        {
            "contract_id": f"contract-{snapshot['dataset_version']}",
            "title": f"{snapshot['instrument']} data contract",
            "data_source": snapshot["data_source"],
            "instrument": snapshot["instrument"],
            "date_column": "date",
            "required_columns": list(DEFAULT_REQUIRED_COLUMNS),
            "non_nullable_columns": ["date", "open", "high", "low", "close", "volume"],
            "non_negative_columns": ["open", "high", "low", "close", "volume"],
            "sort_column": "date",
            "warmup_rows": warmup_rows,
            "expected_date_range_start": snapshot["date_range_start"],
            "expected_date_range_end": snapshot["date_range_end"],
            "instrument_bound_to_dataset": True,
            "validation_rules": [
                "required_columns_present",
                "time_series_sorted",
                "non_nullable_after_warmup",
                "non_negative_prices_and_volume",
                "date_range_matches_snapshot",
            ],
            "created_at": created_at,
        }
    )


def _build_schema(contract_spec: DataContractSpec) -> DataFrameSchema:
    columns: dict[str, Column] = {}
    for name in contract_spec["required_columns"]:
        if name == contract_spec["date_column"]:
            dtype = pa.DateTime
        else:
            dtype = float
        nullable = name not in contract_spec["non_nullable_columns"]
        checks: list[Check] = []
        if name in contract_spec["non_negative_columns"]:
            checks.append(Check.ge(0))
        columns[name] = Column(dtype, nullable=nullable, checks=checks, coerce=True)
    return DataFrameSchema(columns, coerce=True, strict=False)


def validate_dataframe_against_contract(
    df: pd.DataFrame,
    *,
    dataset_snapshot: DatasetSnapshot | dict[str, Any],
    rule_expression: RuleExpression | dict[str, Any],
    metrics_summary: MetricsSummary | dict[str, Any],
    contract_spec: DataContractSpec | dict[str, Any],
    validation_id: str,
    experiment_id: str,
    task_id: str,
    run_id: str,
    title: str,
    created_at: str,
) -> ValidationRecord:
    snapshot = validate_dataset_snapshot(dataset_snapshot)
    rule = validate_rule_expression(rule_expression)
    metrics = validate_metrics_summary(metrics_summary)
    contract = validate_data_contract_spec(contract_spec)

    checks_passed: list[str] = []
    checks_failed: list[str] = []

    required_columns = contract["required_columns"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        checks_failed.append(f"missing_columns:{','.join(missing_columns)}")
    else:
        checks_passed.append("required_columns_present")

    normalized = df.copy()
    if contract["date_column"] in normalized.columns:
        normalized[contract["date_column"]] = pd.to_datetime(normalized[contract["date_column"]])

    if not missing_columns:
        try:
            schema = _build_schema(contract)
            schema.validate(normalized[required_columns], lazy=True)
            checks_passed.append("pandera_schema_validation")
        except Exception as exc:
            checks_failed.append(f"pandera_schema_validation:{type(exc).__name__}")

    sort_column = contract["sort_column"]
    if sort_column in normalized.columns:
        series = normalized[sort_column]
        if series.is_monotonic_increasing:
            checks_passed.append("time_series_sorted")
        else:
            checks_failed.append("time_series_sorted")
    else:
        checks_failed.append(f"missing_sort_column:{sort_column}")

    if not missing_columns and len(normalized) > contract["warmup_rows"]:
        warmup_frame = normalized.iloc[contract["warmup_rows"]:]
        nullable_columns = contract["non_nullable_columns"]
        null_violations = [column for column in nullable_columns if warmup_frame[column].isna().any()]
        if null_violations:
            checks_failed.append(f"warmup_non_nullable:{','.join(null_violations)}")
        else:
            checks_passed.append("warmup_non_nullable_after_warmup")
    elif missing_columns:
        checks_failed.append("warmup_check_skipped_due_to_missing_columns")
    else:
        checks_failed.append("insufficient_rows_for_warmup_check")

    date_column = contract["date_column"]
    if date_column in normalized.columns and not normalized.empty:
        actual_start = normalized[date_column].min().date().isoformat()
        actual_end = normalized[date_column].max().date().isoformat()
        expected_start = contract["expected_date_range_start"]
        expected_end = contract["expected_date_range_end"]
        if actual_start == expected_start and actual_end == expected_end:
            checks_passed.append("date_range_matches_snapshot")
        else:
            checks_failed.append(f"date_range_mismatch:{actual_start}->{actual_end}")
    else:
        checks_failed.append("date_range_unavailable")

    instrument_match = snapshot["instrument"] == contract["instrument"]
    if contract["instrument_bound_to_dataset"] and instrument_match:
        checks_passed.append("instrument_matches_snapshot")
    elif contract["instrument_bound_to_dataset"]:
        checks_failed.append("instrument_matches_snapshot")

    status_code = "passed" if not checks_failed else "failed"
    summary = "; ".join(checks_failed) if checks_failed else "all data contract checks passed"
    record: ValidationRecord = {
        "validation_id": validation_id,
        "experiment_id": experiment_id,
        "task_id": task_id,
        "run_id": run_id,
        "title": title,
        "contract_id": contract["contract_id"],
        "dataset_snapshot": snapshot,
        "rule_expression": rule,
        "metrics_summary": metrics,
        "validation_method": "pandera_data_contract_v1",
        "status_code": status_code,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "summary": summary,
        "validated_rows": int(len(normalized)),
        "created_at": created_at,
    }
    return record

from __future__ import annotations

import json
import pickle
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


_CODE_PATTERN = re.compile(r'^(SH|SZ|BJ)(\d{6})$', re.IGNORECASE)


def convert_qlib_instrument(code: str) -> str:
    text = str(code).strip().upper()
    match = _CODE_PATTERN.match(text)
    if match:
        exchange, symbol = match.groups()
        return f'{symbol}.{exchange}'
    if re.match(r'^\d{6}\.(SH|SZ|BJ)$', text):
        return text
    return text


def _load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f'Config file not found: {path}')
    if yaml is not None:
        payload = yaml.safe_load(path.read_text(encoding='utf-8'))
        return payload if isinstance(payload, dict) else {}
    return {'raw_text': path.read_text(encoding='utf-8')}


def _load_raw_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f'Model output not found: {path}')
    suffix = path.suffix.lower()
    if suffix == '.parquet':
        return pd.read_parquet(path)
    if suffix == '.csv':
        return pd.read_csv(path)
    if suffix == '.json':
        payload = json.loads(path.read_text(encoding='utf-8'))
        return pd.DataFrame(payload)
    if suffix in {'.pkl', '.pickle'}:
        with path.open('rb') as handle:
            payload = pickle.load(handle)
        if isinstance(payload, pd.DataFrame):
            return payload
        if isinstance(payload, dict) and 'predictions' in payload:
            return pd.DataFrame(payload['predictions'])
        raise TypeError(f'Unsupported pickle payload type: {type(payload)!r}')
    raise ValueError(f'Unsupported model output format: {path.suffix}')


def _normalize_prediction_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(dtype=float)

    if isinstance(raw.index, pd.MultiIndex) and set(raw.index.names) >= {'date', 'instrument'}:
        long_frame = raw.reset_index()
    else:
        long_frame = raw.copy()

    date_col = next((name for name in ('datetime', 'date', 'trade_date') if name in long_frame.columns), None)
    instrument_col = next((name for name in ('instrument', 'code', 'asset') if name in long_frame.columns), None)
    score_col = next((name for name in ('score', 'pred', 'prediction', 'value') if name in long_frame.columns), None)

    if date_col and instrument_col and score_col:
        payload = long_frame[[date_col, instrument_col, score_col]].copy()
        payload[date_col] = pd.to_datetime(payload[date_col], errors='coerce')
        payload[instrument_col] = payload[instrument_col].map(convert_qlib_instrument)
        payload[score_col] = pd.to_numeric(payload[score_col], errors='coerce')
        payload = payload.dropna(subset=[date_col])
        frame = payload.pivot_table(index=date_col, columns=instrument_col, values=score_col, aggfunc='last').sort_index()
        frame.columns.name = None
        return frame

    frame = raw.copy()
    if not isinstance(frame.index, pd.DatetimeIndex):
        frame.index = pd.to_datetime(frame.index, errors='coerce')
    frame = frame.sort_index()
    frame.columns = [convert_qlib_instrument(column) for column in frame.columns]
    frame.columns.name = None
    return frame.apply(pd.to_numeric, errors='coerce')


def _zscore_cross_section(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.astype(float)

    def _normalize(row: pd.Series) -> pd.Series:
        values = pd.to_numeric(row, errors='coerce')
        mean = values.mean()
        std = values.std(ddof=0)
        if pd.isna(std) or std == 0:
            return values * 0.0
        return (values - mean) / std

    normalized = frame.apply(_normalize, axis=1)
    return normalized.astype(float)


def extract_factor_scores(model_path: str, config_path: str, output_path: str) -> pd.DataFrame:
    """
    Extract model prediction scores into the standard factor matrix format.
    """
    model_output = Path(model_path)
    config_file = Path(config_path)
    target_path = Path(output_path)

    _load_config(config_file)
    raw_predictions = _load_raw_predictions(model_output)
    factor_frame = _normalize_prediction_frame(raw_predictions)
    factor_frame = _zscore_cross_section(factor_frame)

    factor_frame.index.name = 'date'
    factor_frame = factor_frame.sort_index().sort_index(axis=1)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    factor_frame.to_parquet(target_path)
    return factor_frame

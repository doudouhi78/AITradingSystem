from __future__ import annotations

import numpy as np
import pandas as pd


def _normalize_size_frame(size_frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    aligned = size_frame.reindex(columns=columns)
    return aligned.apply(lambda row: (row - row.mean()) / (row.std(ddof=0) or 1.0), axis=1)


def neutralize_factor(
    factor_frame: pd.DataFrame,
    industry_map: pd.Series | None = None,
    size_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Cross-sectional OLS neutralization on each date, returning residuals with same shape."""
    frame = factor_frame.copy().sort_index()
    result = pd.DataFrame(index=frame.index, columns=frame.columns, dtype=float)
    normalized_size = _normalize_size_frame(size_frame, list(frame.columns)) if size_frame is not None else None
    industry_dummy = None
    if industry_map is not None:
        industry_dummy = pd.get_dummies(industry_map.reindex(frame.columns).fillna('UNKNOWN'), prefix='industry', dtype=float)

    for date in frame.index:
        y = pd.to_numeric(frame.loc[date], errors='coerce')
        valid_mask = y.notna()
        if valid_mask.sum() < 5:
            result.loc[date] = y
            continue

        cols = list(y.index[valid_mask])
        design_parts = []
        if industry_dummy is not None:
            design_parts.append(industry_dummy.reindex(cols).reset_index(drop=True))
        if normalized_size is not None:
            size_values = pd.to_numeric(normalized_size.loc[date, cols], errors='coerce')
            design_parts.append(pd.DataFrame({'size': size_values.values}))

        if not design_parts:
            result.loc[date, cols] = y.loc[cols]
            continue

        X = pd.concat(design_parts, axis=1)
        X = X.loc[:, X.notna().any(axis=0)]
        valid_rows = ~X.isna().any(axis=1)
        y_valid = y.loc[cols].reset_index(drop=True)[valid_rows]
        X_valid = X.loc[valid_rows]
        if len(y_valid) < max(5, X_valid.shape[1] + 1):
            result.loc[date, cols] = y.loc[cols]
            continue

        X_matrix = np.column_stack([np.ones(len(X_valid)), X_valid.to_numpy(dtype=float)])
        beta, *_ = np.linalg.lstsq(X_matrix, y_valid.to_numpy(dtype=float), rcond=None)
        fitted = X_matrix @ beta
        residual = y_valid.to_numpy(dtype=float) - fitted
        residual_series = pd.Series(residual, index=pd.Index(cols)[valid_rows], dtype=float)
        result.loc[date, residual_series.index] = residual_series

    return result

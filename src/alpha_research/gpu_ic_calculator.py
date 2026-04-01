from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

try:
    import torch
except Exception:  # pragma: no cover - torch import failure should fall back silently
    torch = None  # type: ignore[assignment]


ArrayMethod = Literal["spearman", "pearson"]
EPSILON = 1e-12


@dataclass(slots=True)
class _PanelInputs:
    dates: pd.Index
    symbols: pd.Index
    factor_names: pd.Index
    factor_array: np.ndarray
    forward_array: np.ndarray


class GPUIcCalculator:
    def __init__(
        self,
        *,
        device: str | None = None,
        dtype: str = "float32",
        factor_chunk_size: int = 16,
    ) -> None:
        self.dtype = np.float32 if dtype == "float32" else np.float64
        self.factor_chunk_size = max(1, int(factor_chunk_size))
        self.device = self._resolve_device(device)
        self.using_gpu = self.device == "cuda"

    def batch_compute_ic(
        self,
        factor_matrix: pd.DataFrame,
        forward_returns: pd.Series,
        method: str = "spearman",
    ) -> pd.DataFrame:
        normalized_method = self._normalize_method(method)
        panel = self._prepare_panel(factor_matrix=factor_matrix, forward_returns=forward_returns)
        if panel.factor_array.size == 0:
            return pd.DataFrame(index=pd.Index(panel.dates, name="date"), columns=panel.factor_names, dtype=float)
        if self.using_gpu:
            try:
                result = self._batch_compute_torch(panel=panel, method=normalized_method)
                return pd.DataFrame(result, index=pd.Index(panel.dates, name="date"), columns=panel.factor_names, dtype=float)
            except Exception:
                pass
        result = self._batch_compute_numpy(panel=panel, method=normalized_method)
        return pd.DataFrame(result, index=pd.Index(panel.dates, name="date"), columns=panel.factor_names, dtype=float)

    def compute_icir(self, ic_series: pd.DataFrame) -> pd.Series:
        if ic_series.empty:
            return pd.Series(dtype=float)
        means = ic_series.mean(axis=0)
        stds = ic_series.std(axis=0, ddof=0)
        return means.divide(stds.where(stds.abs() > EPSILON))

    def _resolve_device(self, device: str | None) -> str:
        if device is not None:
            lowered = device.lower()
            if lowered == "cuda" and torch is not None and torch.cuda.is_available():
                return "cuda"
            return "cpu"
        if torch is not None and torch.cuda.is_available():
            return "cuda"
        return "cpu"

    def _normalize_method(self, method: str) -> ArrayMethod:
        lowered = method.lower().strip()
        if lowered not in {"spearman", "pearson"}:
            raise ValueError(f"Unsupported IC method: {method}")
        return lowered  # type: ignore[return-value]

    def _prepare_panel(self, factor_matrix: pd.DataFrame, forward_returns: pd.Series) -> _PanelInputs:
        if not isinstance(factor_matrix.index, pd.MultiIndex) or factor_matrix.index.nlevels != 2:
            raise ValueError("factor_matrix must use a MultiIndex of (date, symbol)")
        if not isinstance(forward_returns.index, pd.MultiIndex) or forward_returns.index.nlevels != 2:
            raise ValueError("forward_returns must use a MultiIndex of (date, symbol)")
        aligned_index = factor_matrix.index.union(forward_returns.index).sort_values()
        factor_aligned = factor_matrix.reindex(aligned_index)
        returns_aligned = forward_returns.reindex(aligned_index)
        dates = aligned_index.get_level_values(0).unique()
        symbols = aligned_index.get_level_values(1).unique()
        full_index = pd.MultiIndex.from_product([dates, symbols], names=aligned_index.names)
        factor_dense = factor_aligned.reindex(full_index)
        returns_dense = returns_aligned.reindex(full_index)
        n_dates = len(dates)
        n_symbols = len(symbols)
        n_factors = factor_dense.shape[1]
        factor_array = factor_dense.to_numpy(dtype=self.dtype, copy=True).reshape(n_dates, n_symbols, n_factors)
        forward_array = returns_dense.to_numpy(dtype=self.dtype, copy=True).reshape(n_dates, n_symbols)
        return _PanelInputs(
            dates=dates,
            symbols=symbols,
            factor_names=factor_dense.columns.copy(),
            factor_array=factor_array,
            forward_array=forward_array,
        )

    def _batch_compute_numpy(self, panel: _PanelInputs, method: ArrayMethod) -> np.ndarray:
        n_dates, _, n_factors = panel.factor_array.shape
        result = np.full((n_dates, n_factors), np.nan, dtype=np.float64)
        for start in range(0, n_factors, self.factor_chunk_size):
            stop = min(start + self.factor_chunk_size, n_factors)
            x = panel.factor_array[:, :, start:stop].astype(np.float64, copy=False)
            y = panel.forward_array[:, :, None].astype(np.float64, copy=False)
            valid = np.isfinite(x) & np.isfinite(y)
            if method == "spearman":
                left = self._rank_numpy(x, valid)
                right = self._rank_numpy(np.broadcast_to(y, x.shape), valid)
            else:
                left = np.where(valid, x, np.nan)
                right = np.where(valid, np.broadcast_to(y, x.shape), np.nan)
            result[:, start:stop] = self._corr_numpy(left, right, valid)
        return result

    def _rank_numpy(self, values: np.ndarray, valid: np.ndarray) -> np.ndarray:
        filled = np.where(valid, values, np.inf)
        order = np.argsort(filled, axis=1, kind="mergesort")
        positions = np.broadcast_to(np.arange(values.shape[1], dtype=np.float64)[None, :, None] + 1.0, values.shape)
        ranks = np.empty_like(positions, dtype=np.float64)
        np.put_along_axis(ranks, order, positions, axis=1)
        return np.where(valid, ranks, np.nan)

    def _corr_numpy(self, left: np.ndarray, right: np.ndarray, valid: np.ndarray) -> np.ndarray:
        counts = valid.sum(axis=1, dtype=np.int32)
        left_zero = np.where(valid, left, 0.0)
        right_zero = np.where(valid, right, 0.0)
        left_sum = left_zero.sum(axis=1)
        right_sum = right_zero.sum(axis=1)
        left_mean = np.divide(left_sum, counts, out=np.zeros_like(left_sum), where=counts > 1)
        right_mean = np.divide(right_sum, counts, out=np.zeros_like(right_sum), where=counts > 1)
        left_center = np.where(valid, left - left_mean[:, None, :], 0.0)
        right_center = np.where(valid, right - right_mean[:, None, :], 0.0)
        numerator = (left_center * right_center).sum(axis=1)
        left_norm = np.sqrt((left_center * left_center).sum(axis=1))
        right_norm = np.sqrt((right_center * right_center).sum(axis=1))
        denom = left_norm * right_norm
        corr = np.divide(numerator, denom, out=np.full_like(numerator, np.nan), where=(counts > 1) & (denom > EPSILON))
        corr[counts <= 1] = np.nan
        return corr

    def _batch_compute_torch(self, panel: _PanelInputs, method: ArrayMethod) -> np.ndarray:
        if torch is None:
            raise RuntimeError("torch is unavailable")
        device = torch.device(self.device)
        y_all = torch.as_tensor(panel.forward_array, device=device, dtype=torch.float32)
        n_dates, _, n_factors = panel.factor_array.shape
        outputs: list[torch.Tensor] = []
        for start in range(0, n_factors, self.factor_chunk_size):
            stop = min(start + self.factor_chunk_size, n_factors)
            x = torch.as_tensor(panel.factor_array[:, :, start:stop], device=device, dtype=torch.float32)
            y = y_all.unsqueeze(-1).expand(-1, -1, stop - start)
            valid = torch.isfinite(x) & torch.isfinite(y)
            if method == "spearman":
                left = self._rank_torch(x, valid)
                right = self._rank_torch(y, valid)
            else:
                nan_value = torch.full_like(x, float("nan"))
                left = torch.where(valid, x, nan_value)
                right = torch.where(valid, y, nan_value)
            outputs.append(self._corr_torch(left, right, valid))
            del x, y, valid, left, right
            if device.type == "cuda":
                torch.cuda.empty_cache()
        return torch.cat(outputs, dim=1).detach().cpu().numpy().astype(np.float64, copy=False)

    def _rank_torch(self, values: "torch.Tensor", valid: "torch.Tensor") -> "torch.Tensor":
        filled = torch.where(valid, values, torch.full_like(values, float("inf")))
        order = torch.argsort(filled, dim=1, stable=True)
        positions = torch.arange(values.shape[1], device=values.device, dtype=values.dtype).view(1, -1, 1) + 1.0
        positions = positions.expand_as(values)
        ranks = torch.empty_like(values)
        ranks.scatter_(1, order, positions)
        return torch.where(valid, ranks, torch.full_like(ranks, float("nan")))

    def _corr_torch(self, left: "torch.Tensor", right: "torch.Tensor", valid: "torch.Tensor") -> "torch.Tensor":
        counts = valid.sum(dim=1)
        mask = valid.to(left.dtype)
        left_zero = torch.where(valid, left, torch.zeros_like(left))
        right_zero = torch.where(valid, right, torch.zeros_like(right))
        safe_counts = counts.clamp_min(1).to(left.dtype)
        left_mean = left_zero.sum(dim=1) / safe_counts
        right_mean = right_zero.sum(dim=1) / safe_counts
        left_center = torch.where(valid, left - left_mean.unsqueeze(1), torch.zeros_like(left))
        right_center = torch.where(valid, right - right_mean.unsqueeze(1), torch.zeros_like(right))
        numerator = (left_center * right_center).sum(dim=1)
        left_norm = torch.sqrt((left_center * left_center).sum(dim=1))
        right_norm = torch.sqrt((right_center * right_center).sum(dim=1))
        denom = left_norm * right_norm
        corr = numerator / denom.clamp_min(EPSILON)
        invalid = (counts <= 1) | (~torch.isfinite(denom)) | (denom <= EPSILON)
        corr = torch.where(invalid, torch.full_like(corr, float("nan")), corr)
        return corr

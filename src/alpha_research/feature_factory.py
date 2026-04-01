from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_research.factors import alpha101

LOGGER = logging.getLogger(__name__)

try:
    import torch
except Exception:  # pragma: no cover - optional dependency
    torch = None


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _external_system_root() -> Path | None:
    env_root = os.getenv("AI_TRADING_SYSTEM_ROOT")
    if env_root:
        return Path(env_root)
    parts = Path(__file__).resolve().parts
    if ".claude" in parts:
        return Path(*parts[:parts.index(".claude")])
    return None


def _default_data_root() -> Path:
    external_root = _external_system_root()
    if external_root and (external_root / "runtime" / "market_data" / "cn_stock").exists():
        return external_root / "runtime"
    return _project_root() / "runtime"


def _to_timestamp(value: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    return pd.Timestamp(value)


def _implemented_factor_names() -> list[str]:
    return [f"alpha{factor_id:03d}" for factor_id in alpha101.IMPLEMENTED_ALPHA_IDS]


@dataclass(slots=True)
class FeatureFactory:
    icir_threshold: float = 0.05
    winsorize_sigma: float = 3.0
    data_root: Path = field(default_factory=_default_data_root)
    runtime_root: Path = field(default_factory=lambda: _project_root() / "runtime")
    factor_registry_paths: tuple[Path, ...] = field(default_factory=tuple)
    use_torch_if_available: bool = True

    def __post_init__(self) -> None:
        self.data_root = Path(self.data_root)
        self.runtime_root = Path(self.runtime_root)
        if not self.factor_registry_paths:
            self.factor_registry_paths = (
                self.runtime_root / "alpha_research" / "factor_registry.json",
                self.data_root / "alpha_research" / "factor_registry.json",
                _project_root() / "src" / "alpha_research" / "registry" / "factor_registry.json",
            )

    @property
    def market_data_dir(self) -> Path:
        return self.data_root / "market_data" / "cn_stock"

    @property
    def output_dir(self) -> Path:
        return self.runtime_root / "alpha_research" / "feature_matrix"

    @property
    def failure_log_path(self) -> Path:
        return self.runtime_root / "alpha_research" / "feature_factory_failures.json"

    def build_feature_matrix(
        self,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        symbols: list[str] | None = None,
        max_symbols: int | None = None,
        factor_names: list[str] | None = None,
        write_output: bool = True,
    ) -> pd.DataFrame:
        market_frame = self.load_market_data(start_date=start_date, end_date=end_date, symbols=symbols, max_symbols=max_symbols)
        if market_frame.empty:
            return pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])

        selected_factors = factor_names or self.select_factor_names()
        records: list[pd.DataFrame] = []
        failures: list[dict[str, str]] = []

        factor_input = market_frame.set_index(["trade_date", "symbol"]).sort_index()
        for factor_name in selected_factors:
            func = alpha101.ALPHA_FUNCTIONS.get(factor_name)
            if func is None:
                failures.append({"factor_name": factor_name, "error": "missing factor implementation"})
                continue
            try:
                factor_series = func(factor_input)
                factor_frame = factor_series.rename("raw_value").reset_index()
                symbol_column = next(column for column in factor_frame.columns if column not in {"date", "raw_value"})
                factor_frame = factor_frame.rename(columns={"date": "trade_date", symbol_column: "symbol"})
                cleaned = self._clean_factor_frame(factor_frame, market_frame, factor_name)
                if not cleaned.empty:
                    records.append(cleaned)
            except Exception as exc:  # pragma: no cover - exercised via test with failing factor
                LOGGER.warning("factor %s failed: %s", factor_name, exc)
                failures.append({"factor_name": factor_name, "error": str(exc)})

        result = (
            pd.concat(records, ignore_index=True)
            if records
            else pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])
        )
        if not result.empty:
            result = result.sort_values(["trade_date", "symbol", "factor_name"]).reset_index(drop=True)
        if write_output:
            self.write_monthly_partitions(result)
        self.write_failure_log(failures)
        return result

    def select_factor_names(self) -> list[str]:
        fallback = _implemented_factor_names()
        for path in self.factor_registry_paths:
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            selected = self._extract_registry_factor_names(payload)
            if selected:
                return selected
        # TODO: 等IC评估完成后填入有效因子列表
        return fallback

    def load_market_data(
        self,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        symbols: list[str] | None = None,
        max_symbols: int | None = None,
    ) -> pd.DataFrame:
        start_ts = _to_timestamp(start_date)
        end_ts = _to_timestamp(end_date)
        abnormal_symbols = self.load_abnormal_symbols()
        universe = symbols or self._discover_symbols(max_symbols=max_symbols)

        frames: list[pd.DataFrame] = []
        for symbol in universe:
            if symbol in abnormal_symbols:
                continue
            path = self.market_data_dir / f"{symbol}.parquet"
            if not path.exists():
                continue
            frame = pd.read_parquet(
                path,
                columns=[
                    "trade_date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "is_suspended",
                ],
            ).copy()
            frame["trade_date"] = pd.to_datetime(frame["trade_date"])
            if start_ts is not None:
                frame = frame.loc[frame["trade_date"] >= start_ts]
            if end_ts is not None:
                frame = frame.loc[frame["trade_date"] <= end_ts]
            if frame.empty:
                continue
            frames.append(frame)

        if not frames:
            return pd.DataFrame(
                columns=[
                    "trade_date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "is_suspended",
                    "industry_name",
                    "market_cap",
                ]
            )

        market_frame = pd.concat(frames, ignore_index=True)
        market_frame["symbol"] = market_frame["symbol"].astype(str).str.zfill(6)
        numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
        for column in numeric_columns:
            market_frame[column] = pd.to_numeric(market_frame[column], errors="coerce")
        market_frame["is_suspended"] = market_frame["is_suspended"].fillna(False).astype(bool)

        industry_frame = self._load_industry_frame()
        if not industry_frame.empty:
            market_frame = market_frame.merge(industry_frame, how="left", on="symbol")
        else:
            market_frame["industry_name"] = "unknown"

        cap_frame = self._load_market_cap_frame()
        if not cap_frame.empty:
            market_frame = market_frame.merge(cap_frame, how="left", on=["trade_date", "symbol"])
        else:
            market_frame["market_cap"] = np.nan

        market_frame["industry_name"] = market_frame["industry_name"].fillna("unknown")
        market_frame["market_cap"] = pd.to_numeric(market_frame["market_cap"], errors="coerce")
        return market_frame.sort_values(["trade_date", "symbol"]).reset_index(drop=True)

    def load_abnormal_symbols(self) -> set[str]:
        path = self.data_root / "download_log" / "abnormal_files.json"
        if not path.exists():
            return set()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            symbols = [entry.get("symbol") if isinstance(entry, dict) else entry for entry in payload]
            return {str(symbol).zfill(6) for symbol in symbols if symbol}
        return set()

    def write_monthly_partitions(self, feature_frame: pd.DataFrame) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if feature_frame.empty:
            return
        frame = feature_frame.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["yyyymm"] = frame["trade_date"].dt.strftime("%Y%m")
        for yyyymm, month_frame in frame.groupby("yyyymm", sort=True):
            out_path = self.output_dir / f"{yyyymm}.parquet"
            month_frame.drop(columns=["yyyymm"]).to_parquet(out_path, index=False)

    def write_failure_log(self, failures: list[dict[str, str]]) -> None:
        self.failure_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.failure_log_path.write_text(
            json.dumps(failures, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _discover_symbols(self, max_symbols: int | None = None) -> list[str]:
        symbols = sorted(path.stem for path in self.market_data_dir.glob("*.parquet"))
        if max_symbols is not None:
            return symbols[:max_symbols]
        return symbols

    def _extract_registry_factor_names(self, payload: object) -> list[str]:
        records: list[dict[str, object]] = []
        if isinstance(payload, list):
            records = [record for record in payload if isinstance(record, dict)]
        elif isinstance(payload, dict):
            if isinstance(payload.get("factors"), list):
                records = [record for record in payload["factors"] if isinstance(record, dict)]
            else:
                for key, value in payload.items():
                    if isinstance(value, dict):
                        record = {"factor_name": key, **value}
                        records.append(record)

        selected: list[str] = []
        fallback_names = set(_implemented_factor_names())
        for record in records:
            factor_name = str(record.get("factor_name") or record.get("name") or "").strip()
            if not factor_name:
                continue
            icir_value = record.get("icir_neutralized")
            if icir_value is None:
                continue
            try:
                if float(icir_value) > self.icir_threshold and factor_name in fallback_names:
                    selected.append(factor_name)
            except (TypeError, ValueError):
                continue
        return sorted(set(selected))

    def _load_industry_frame(self) -> pd.DataFrame:
        path = self.data_root / "classification_data" / "industry_sw2.parquet"
        if not path.exists():
            return pd.DataFrame(columns=["symbol", "industry_name"])
        frame = pd.read_parquet(path, columns=["instrument_code", "industry_name"]).copy()
        frame["symbol"] = frame["instrument_code"].astype(str).str.zfill(6)
        return frame[["symbol", "industry_name"]].drop_duplicates(subset=["symbol"])

    def _load_market_cap_frame(self) -> pd.DataFrame:
        path = self.data_root / "fundamental_data" / "valuation_daily.parquet"
        if not path.exists():
            return pd.DataFrame(columns=["trade_date", "symbol", "market_cap"])
        frame = pd.read_parquet(path, columns=["date", "instrument_code", "total_mv", "circ_mv"]).copy()
        frame["trade_date"] = pd.to_datetime(frame["date"])
        frame["symbol"] = frame["instrument_code"].astype(str).str.zfill(6)
        total_mv = pd.to_numeric(frame["total_mv"], errors="coerce")
        circ_mv = pd.to_numeric(frame["circ_mv"], errors="coerce")
        frame["market_cap"] = total_mv.fillna(circ_mv)
        return frame[["trade_date", "symbol", "market_cap"]]

    def _clean_factor_frame(self, factor_frame: pd.DataFrame, market_frame: pd.DataFrame, factor_name: str) -> pd.DataFrame:
        merged = factor_frame.merge(
            market_frame[["trade_date", "symbol", "is_suspended", "industry_name", "market_cap"]],
            how="left",
            on=["trade_date", "symbol"],
        )
        merged = merged.loc[~merged["is_suspended"].fillna(False)].copy()
        if merged.empty:
            return pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])

        cleaned_rows: list[pd.DataFrame] = []
        for trade_date, cross_section in merged.groupby("trade_date", sort=True):
            processed = self._process_cross_section(cross_section)
            if processed.empty:
                continue
            processed["trade_date"] = trade_date
            processed["factor_name"] = factor_name
            cleaned_rows.append(processed[["trade_date", "symbol", "factor_name", "value"]])
        if not cleaned_rows:
            return pd.DataFrame(columns=["trade_date", "symbol", "factor_name", "value"])
        return pd.concat(cleaned_rows, ignore_index=True)

    def _process_cross_section(self, frame: pd.DataFrame) -> pd.DataFrame:
        working = frame[["symbol", "raw_value", "industry_name", "market_cap"]].copy()
        working["raw_value"] = pd.to_numeric(working["raw_value"], errors="coerce")
        working["market_cap"] = pd.to_numeric(working["market_cap"], errors="coerce")
        working = working.dropna(subset=["raw_value"])
        if len(working) < 2:
            return pd.DataFrame(columns=["symbol", "value"])

        winsorized = self._winsorize(working["raw_value"].to_numpy(dtype=float))
        if np.isnan(winsorized).all():
            return pd.DataFrame(columns=["symbol", "value"])
        working["winsorized"] = winsorized
        working["value"] = self._neutralize(
            working["winsorized"].to_numpy(dtype=float),
            working["industry_name"].fillna("unknown").astype(str).to_numpy(),
            working["market_cap"].to_numpy(dtype=float),
        )
        working["value"] = self._standardize(working["value"].to_numpy(dtype=float))
        working = working.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
        return working[["symbol", "value"]]

    def _winsorize(self, values: np.ndarray) -> np.ndarray:
        if len(values) == 0:
            return values
        mean = np.nanmean(values)
        std = np.nanstd(values)
        if not np.isfinite(std) or std == 0:
            return values.astype(float, copy=True)
        lower = mean - self.winsorize_sigma * std
        upper = mean + self.winsorize_sigma * std
        if self.use_torch_if_available and torch is not None:
            tensor = torch.as_tensor(values, dtype=torch.float32)
            return torch.clamp(tensor, min=float(lower), max=float(upper)).cpu().numpy()
        return np.clip(values, lower, upper)

    def _neutralize(self, values: np.ndarray, industries: np.ndarray, market_caps: np.ndarray) -> np.ndarray:
        valid_mask = np.isfinite(values)
        if valid_mask.sum() < 2:
            return values

        design_parts = [np.ones(valid_mask.sum(), dtype=float)]
        log_cap = np.log(np.clip(market_caps[valid_mask], a_min=1e-12, a_max=None))
        if np.isfinite(log_cap).any() and np.nanstd(log_cap) > 0:
            cap_centered = log_cap - np.nanmean(log_cap)
            design_parts.append(cap_centered)
        industry_dummies = pd.get_dummies(pd.Series(industries[valid_mask], dtype="string"), dummy_na=False)
        if not industry_dummies.empty and industry_dummies.shape[1] > 1:
            design_parts.append(industry_dummies.iloc[:, 1:].to_numpy(dtype=float))

        if len(design_parts) == 1:
            result = values.astype(float, copy=True)
            result[valid_mask] = values[valid_mask] - np.nanmean(values[valid_mask])
            return result

        X = np.column_stack(design_parts)
        y = values[valid_mask]
        beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        residual = y - X @ beta
        result = np.full_like(values, np.nan, dtype=float)
        result[valid_mask] = residual
        return result

    def _standardize(self, values: np.ndarray) -> np.ndarray:
        valid_mask = np.isfinite(values)
        if valid_mask.sum() < 2:
            return values
        mean = np.nanmean(values[valid_mask])
        std = np.nanstd(values[valid_mask])
        if not np.isfinite(std) or std == 0:
            result = np.zeros_like(values, dtype=float)
            result[~valid_mask] = np.nan
            return result
        result = np.full_like(values, np.nan, dtype=float)
        result[valid_mask] = (values[valid_mask] - mean) / std
        return result

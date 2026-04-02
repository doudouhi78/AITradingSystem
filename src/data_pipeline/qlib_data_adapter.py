from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


class QlibDataAdapter:
    """Convert Tushare-style OHLCV parquet data into Qlib-compatible tabular format.

    Qlib commonly expects a MultiIndex DataFrame indexed by ``datetime`` and ``instrument``
    with feature columns named like ``$open``, ``$close`` and ``$volume``. This adapter keeps
    the transformation logic explicit and reversible so the project can remain source-of-truth
    on the original Tushare-style layout.
    """

    COLUMN_MAP = {
        'open': '$open',
        'high': '$high',
        'low': '$low',
        'close': '$close',
        'volume': '$volume',
        'amount': '$amount',
    }
    REVERSE_COLUMN_MAP = {value: key for key, value in COLUMN_MAP.items()}
    SYMBOL_COLUMNS = ('ts_code', 'symbol', 'instrument_code', 'instrument')
    DATE_COLUMNS = ('trade_date', 'date', 'datetime')

    def __init__(self, data_dir: str = 'runtime/fundamental_data') -> None:
        self.data_dir = Path(data_dir)

    @staticmethod
    def to_qlib_code(symbol: str) -> str:
        code = str(symbol).strip().upper()
        if '.' not in code:
            digits = ''.join(ch for ch in code if ch.isdigit())
            if len(digits) == 6:
                if digits.startswith(('600', '601', '603', '605', '688', '689', '900')):
                    return f'SH{digits}'
                return f'SZ{digits}'
            raise ValueError(f'unsupported symbol format: {symbol}')
        local, exchange = code.split('.', 1)
        return f'{exchange}{local}'

    @staticmethod
    def to_tushare_code(symbol: str) -> str:
        code = str(symbol).strip().upper()
        if '.' in code:
            return code
        if len(code) < 8:
            raise ValueError(f'unsupported qlib code: {symbol}')
        return f'{code[2:]}.{code[:2]}'

    def _resolve_files(self) -> list[Path]:
        if not self.data_dir.exists():
            raise FileNotFoundError(f'data directory not found: {self.data_dir}')
        files = sorted(self.data_dir.glob('*.parquet'))
        if not files:
            raise FileNotFoundError(f'no parquet files found in {self.data_dir}')
        return files

    def _normalize_stock_list(self, stock_list: Iterable[str] | None) -> set[str] | None:
        if stock_list is None:
            return None
        normalized: set[str] = set()
        for symbol in stock_list:
            symbol_text = str(symbol).strip().upper()
            normalized.add(self.to_tushare_code(symbol_text) if '.' not in symbol_text else symbol_text)
        return normalized

    def _detect_symbol_column(self, frame: pd.DataFrame, fallback: str | None = None) -> pd.Series:
        for column in self.SYMBOL_COLUMNS:
            if column in frame.columns:
                values = frame[column].astype(str).str.upper()
                if column != 'ts_code' and values.str.contains(r'^[0-9]{6}$').all():
                    values = values.map(self.to_qlib_code).map(self.to_tushare_code)
                return values
        if fallback is None:
            raise KeyError('missing symbol column; expected one of ts_code/symbol/instrument_code/instrument')
        return pd.Series([fallback] * len(frame), index=frame.index, dtype='object')

    def _detect_date_column(self, frame: pd.DataFrame) -> pd.Series:
        for column in self.DATE_COLUMNS:
            if column in frame.columns:
                return pd.to_datetime(frame[column], errors='coerce')
        raise KeyError('missing date column; expected one of trade_date/date/datetime')

    def load_price_data(
        self,
        start_date: str,
        end_date: str,
        stock_list: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load Tushare-style parquet files and convert them into Qlib-style features.

        Mapping rules:
        - ``open/high/low/close/volume/amount`` -> ``$open/$high/$low/$close/$volume/$amount``
        - ``600519.SH`` -> ``SH600519`` for the Qlib instrument key
        - output index is ``MultiIndex([datetime, instrument])``
        """
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        selected = self._normalize_stock_list(stock_list)
        frames: list[pd.DataFrame] = []

        for path in self._resolve_files():
            frame = pd.read_parquet(path).copy()
            if frame.empty:
                continue
            fallback_symbol = None
            stem = path.stem.upper()
            if '.' in stem:
                fallback_symbol = stem
            elif len(stem) >= 8 and stem[:2] in {'SH', 'SZ'}:
                fallback_symbol = self.to_tushare_code(stem)
            dates = self._detect_date_column(frame)
            symbols = self._detect_symbol_column(frame, fallback=fallback_symbol)
            frame = frame.assign(datetime=dates, tushare_symbol=symbols)
            frame = frame.dropna(subset=['datetime']).copy()
            frame['tushare_symbol'] = frame['tushare_symbol'].astype(str).str.upper()
            if selected is not None:
                frame = frame.loc[frame['tushare_symbol'].isin(selected)].copy()
            if frame.empty:
                continue
            frame = frame.loc[(frame['datetime'] >= start_ts) & (frame['datetime'] <= end_ts)].copy()
            if frame.empty:
                continue
            feature_cols = [column for column in self.COLUMN_MAP if column in frame.columns]
            if not feature_cols:
                continue
            rename_map = {column: self.COLUMN_MAP[column] for column in feature_cols}
            subset = frame[['datetime', 'tushare_symbol', *feature_cols]].rename(columns=rename_map)
            subset['instrument'] = subset['tushare_symbol'].map(self.to_qlib_code)
            frames.append(subset.drop(columns=['tushare_symbol']))

        if not frames:
            raise FileNotFoundError(
                f'no matching price rows found in {self.data_dir} for {start_date}..{end_date}'
            )

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values(['datetime', 'instrument']).drop_duplicates(['datetime', 'instrument'], keep='last')
        return result.set_index(['datetime', 'instrument']).sort_index()

    def to_tushare_frame(self, qlib_frame: pd.DataFrame) -> pd.DataFrame:
        """Convert a Qlib-style MultiIndex frame back to a flat Tushare-style layout."""
        if list(qlib_frame.index.names) != ['datetime', 'instrument']:
            raise ValueError("expected MultiIndex names ['datetime', 'instrument']")
        frame = qlib_frame.reset_index().copy()
        frame['ts_code'] = frame['instrument'].map(self.to_tushare_code)
        rename_map = {column: self.REVERSE_COLUMN_MAP[column] for column in qlib_frame.columns if column in self.REVERSE_COLUMN_MAP}
        frame = frame.rename(columns=rename_map)
        ordered = ['datetime', 'ts_code', *rename_map.values()]
        return frame[ordered].rename(columns={'datetime': 'trade_date'})

    def get_label(self, price_df: pd.DataFrame, forward_days: int = 20) -> pd.Series:
        """Compute a leak-free Qlib label using T+1 open entry and T+forward_days+1 open exit.

        For each date ``T``:
        - enter at ``open[T+1]``
        - exit at ``open[T+forward_days+1]``
        - label is the cross-sectional percentile rank of that future open-to-open return
        """
        if '$open' not in price_df.columns:
            raise KeyError("price_df must contain '$open'")
        open_frame = price_df['$open'].unstack('instrument').sort_index()
        entry_open = open_frame.shift(-1)
        exit_open = open_frame.shift(-(forward_days + 1))
        future_return = exit_open.divide(entry_open).subtract(1.0)
        label = future_return.rank(axis=1, method='average', pct=True)
        label = label.stack(future_stack=True).rename('label')
        label.index = label.index.set_names(['datetime', 'instrument'])
        return label.sort_index()

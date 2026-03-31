from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline.data_updater import update_single_market_file, write_update_log


BAR_COLUMNS = [
    "market",
    "symbol",
    "security_type",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjustment_mode",
    "is_suspended",
    "listed_date",
    "delisted_date",
]


def _bars(dates: list[str], symbol: str = '510300', security_type: str = 'etf') -> pd.DataFrame:
    rows = []
    for idx, day in enumerate(dates, start=1):
        rows.append(
            {
                'market': 'CN',
                'symbol': symbol,
                'security_type': security_type,
                'trade_date': day,
                'open': float(idx),
                'high': float(idx) + 0.1,
                'low': float(idx) - 0.1,
                'close': float(idx),
                'volume': float(idx * 100),
                'amount': float(idx * 1000),
                'adjustment_mode': 'qfq',
                'is_suspended': False,
                'listed_date': '',
                'delisted_date': '',
            }
        )
    return pd.DataFrame(rows, columns=BAR_COLUMNS)


def test_incremental_etf(tmp_path: Path) -> None:
    file_path = tmp_path / '510300.parquet'
    existing = _bars(['2026-03-24', '2026-03-25'])
    existing.to_parquet(file_path, index=False)

    def fake_fetcher(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        assert symbol == '510300'
        assert start_date == '2026-03-26'
        return _bars(['2026-03-26', '2026-03-27'])

    result = update_single_market_file(file_path, symbol='510300', security_type='etf', fetcher=fake_fetcher, today=pd.Timestamp('2026-03-31'))
    updated = pd.read_parquet(file_path)

    assert result['added_rows'] == 2
    assert len(updated) == 4
    assert updated['trade_date'].tolist() == ['2026-03-24', '2026-03-25', '2026-03-26', '2026-03-27']


def test_update_log(tmp_path: Path) -> None:
    payload = {
        'run_date': '2026-03-31',
        'modules': {'etf': {'processed': 1, 'updated': 1, 'added_rows': 2, 'failures': []}},
        'failed_symbols': {'etf': []},
    }
    path = write_update_log(payload, log_dir=tmp_path, run_date=pd.Timestamp('2026-03-31'))
    assert path.exists()
    saved = pd.read_json(path)
    text = path.read_text(encoding='utf-8')
    assert 'run_date' in text
    assert 'modules' in text
    assert 'failed_symbols' in text

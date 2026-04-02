from __future__ import annotations

import json
import pickle
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
import yaml

import qlib
from qlib.data import D
from qlib.data.dataset.handler import DataHandlerLP
from qlib.tests.config import get_dataset_config
from qlib.utils import init_instance_by_config
from qlib.contrib.model.pytorch_tra import RNN, TRA

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.qlib_factor_extractor import convert_qlib_instrument
from data_pipeline.qlib_data_adapter import QlibDataAdapter

CONFIG_PATH = ROOT / 'src' / 'alpha_research' / 'qlib_model_configs' / 'tra_config_csi300.yaml'
SOURCE_DIR = ROOT / 'runtime' / 'market_data' / 'cn_stock'
FUNDAMENTAL_DIR = ROOT / 'runtime' / 'fundamental_data'
PROVIDER_URI = ROOT / 'runtime' / 'qlib_data' / 'tushare_cn'
PRICE_OUTPUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_tra_tushare_test_prices.parquet'
DUMP_METADATA_PATH = PROVIDER_URI / 'dump_metadata.json'
IC_EVAL_SCRIPT = ROOT / 'scripts' / 'run_qlib_factor_ic_eval.py'
FACTOR_NAME = 'qlib_tra_tushare_v1'

FIELDS = ('open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'factor')


@dataclass
class SegmentData:
    x: np.ndarray
    y: np.ndarray
    dates: list[str]
    instruments: list[str]


@dataclass
class DumpStats:
    stock_count: int
    coverage_start: str
    coverage_end: str
    calendar_days: int


@dataclass
class TrainStats:
    train_samples: int
    valid_samples: int
    test_samples: int
    epochs_ran: int
    peak_mem_gb: float
    best_valid_ic: float
    ic_mean: float
    ic_std: float
    icir: float
    training_minutes: float
    dump_minutes: float
    feature_minutes: float


def load_config() -> dict[str, Any]:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding='utf-8'))


def _load_stock_code_map() -> dict[str, str]:
    stock_basic = pd.read_parquet(FUNDAMENTAL_DIR / 'stock_basic.parquet')
    stock_basic = stock_basic.copy()
    stock_basic['symbol'] = stock_basic['symbol'].astype(str).str.zfill(6)
    stock_basic['ts_code'] = stock_basic['ts_code'].astype(str).str.upper()
    stock_basic['list_date'] = pd.to_datetime(stock_basic['list_date'], errors='coerce')
    stock_basic['delist_date'] = pd.to_datetime(stock_basic['delist_date'], errors='coerce')
    stock_basic = stock_basic.sort_values(['symbol', 'list_date', 'ts_code']).drop_duplicates(['symbol'], keep='last')
    return dict(zip(stock_basic['symbol'], stock_basic['ts_code']))


def _build_calendar() -> pd.DatetimeIndex:
    trade_cal = pd.read_parquet(FUNDAMENTAL_DIR / 'trade_cal.parquet')
    trade_cal = trade_cal.loc[trade_cal['is_open'] == 1].copy()
    dates = pd.to_datetime(trade_cal['cal_date'], errors='coerce').dropna().sort_values().unique()
    return pd.DatetimeIndex(dates)


def _convert_symbol_to_qlib(symbol: str, code_map: dict[str, str]) -> str:
    ts_code = code_map.get(str(symbol).zfill(6))
    if ts_code:
        local, exchange = ts_code.split('.', 1)
        return f'{exchange.upper()}{local}'
    return QlibDataAdapter.to_qlib_code(str(symbol).zfill(6))


def build_tushare_qlib_dump(force: bool = False) -> DumpStats:
    if DUMP_METADATA_PATH.exists() and not force:
        payload = json.loads(DUMP_METADATA_PATH.read_text(encoding='utf-8'))
        return DumpStats(
            stock_count=int(payload['stock_count']),
            coverage_start=str(payload['coverage_start']),
            coverage_end=str(payload['coverage_end']),
            calendar_days=int(payload['calendar_days']),
        )

    PROVIDER_URI.mkdir(parents=True, exist_ok=True)
    (PROVIDER_URI / 'features').mkdir(parents=True, exist_ok=True)
    code_map = _load_stock_code_map()
    calendar = _build_calendar()
    calendar_text = [ts.strftime('%Y-%m-%d') for ts in calendar]

    calendar_dir = PROVIDER_URI / 'calendars'
    calendar_dir.mkdir(parents=True, exist_ok=True)
    (calendar_dir / 'day.txt').write_text('\n'.join(calendar_text) + '\n', encoding='utf-8')

    instrument_rows: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    coverage_start: pd.Timestamp | None = None
    coverage_end: pd.Timestamp | None = None

    files = sorted(SOURCE_DIR.glob('*.parquet'))
    for idx, path in enumerate(files, start=1):
        frame = pd.read_parquet(path, columns=['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        if frame.empty:
            continue
        frame = frame.copy()
        frame['trade_date'] = pd.to_datetime(frame['trade_date'], errors='coerce')
        frame = frame.dropna(subset=['trade_date']).sort_values('trade_date')
        if frame.empty:
            continue
        symbol = str(frame['symbol'].iloc[0]).zfill(6)
        qlib_code = _convert_symbol_to_qlib(symbol, code_map)
        start_dt = pd.Timestamp(frame['trade_date'].iloc[0]).normalize()
        end_dt = pd.Timestamp(frame['trade_date'].iloc[-1]).normalize()
        coverage_start = start_dt if coverage_start is None else min(coverage_start, start_dt)
        coverage_end = end_dt if coverage_end is None else max(coverage_end, end_dt)
        instrument_rows[qlib_code] = [(start_dt, end_dt)]

        frame = frame.drop_duplicates('trade_date', keep='last').set_index('trade_date')
        frame = frame.reindex(calendar[(calendar >= start_dt) & (calendar <= end_dt)])
        volume = pd.to_numeric(frame['volume'], errors='coerce').astype(float)
        amount = pd.to_numeric(frame['amount'], errors='coerce').astype(float)
        vwap = amount.divide(volume.replace(0.0, np.nan))
        factor = pd.Series(1.0, index=frame.index, dtype=float)
        feature_payload = {
            'open': pd.to_numeric(frame['open'], errors='coerce').astype(float),
            'high': pd.to_numeric(frame['high'], errors='coerce').astype(float),
            'low': pd.to_numeric(frame['low'], errors='coerce').astype(float),
            'close': pd.to_numeric(frame['close'], errors='coerce').astype(float),
            'volume': volume,
            'amount': amount,
            'vwap': vwap,
            'factor': factor,
        }
        start_index = int(calendar.get_loc(start_dt))
        feature_dir = PROVIDER_URI / 'features' / qlib_code.lower()
        feature_dir.mkdir(parents=True, exist_ok=True)
        for field, series in feature_payload.items():
            output_path = feature_dir / f'{field}.day.bin'
            values = series.to_numpy(dtype=np.float32)
            payload = np.concatenate((np.array([float(start_index)], dtype=np.float32), values)).astype('<f4')
            payload.tofile(output_path)
        if idx % 500 == 0:
            print(f'dump_progress={idx}/{len(files)}', flush=True)

    instrument_dir = PROVIDER_URI / 'instruments'
    instrument_dir.mkdir(parents=True, exist_ok=True)
    instrument_lines = []
    for inst, periods in instrument_rows.items():
        for start_dt, end_dt in periods:
            instrument_lines.append(f'{inst}\t{start_dt.strftime("%Y-%m-%d")}\t{end_dt.strftime("%Y-%m-%d")}')
    (instrument_dir / 'all.txt').write_text('\n'.join(instrument_lines) + '\n', encoding='utf-8')

    payload = {
        'stock_count': len(instrument_rows),
        'coverage_start': coverage_start.strftime('%Y-%m-%d') if coverage_start is not None else '',
        'coverage_end': coverage_end.strftime('%Y-%m-%d') if coverage_end is not None else '',
        'calendar_days': len(calendar),
        'generated_at': pd.Timestamp.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source_dir': str(SOURCE_DIR),
    }
    DUMP_METADATA_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return DumpStats(
        stock_count=int(payload['stock_count']),
        coverage_start=str(payload['coverage_start']),
        coverage_end=str(payload['coverage_end']),
        calendar_days=int(payload['calendar_days']),
    )


def init_dataset(config: dict[str, Any]):
    train_start, train_end = config['dataset']['train']
    valid_start, valid_end = config['dataset']['valid']
    test_start, test_end = config['dataset']['test']
    dataset_config = get_dataset_config(
        dataset_class='Alpha158',
        train=(train_start, train_end),
        valid=(valid_start, valid_end),
        test=(test_start, test_end),
        handler_kwargs={
            'start_time': train_start,
            'end_time': test_end,
            'fit_start_time': train_start,
            'fit_end_time': train_end,
            'instruments': 'all',
        },
    )
    return init_instance_by_config(dataset_config)


def prepare_segment(dataset, segment: str) -> SegmentData:
    frame = dataset.prepare(segment, col_set=['feature', 'label'], data_key=DataHandlerLP.DK_L)
    feature_frame = frame['feature'].astype(float).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    label_frame = frame['label'].astype(float).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    idx = feature_frame.index
    if isinstance(idx, pd.MultiIndex):
        dates = pd.to_datetime(idx.get_level_values(0), errors='coerce').strftime('%Y-%m-%d').tolist()
        instruments = [str(x).upper() for x in idx.get_level_values(1)]
    else:
        dates = pd.to_datetime(idx, errors='coerce').strftime('%Y-%m-%d').tolist()
        instruments = ['UNKNOWN'] * len(feature_frame)
    x = feature_frame.to_numpy(dtype=np.float32).reshape(len(feature_frame), 1, feature_frame.shape[1])
    y = label_frame.iloc[:, 0].to_numpy(dtype=np.float32)
    return SegmentData(x=x, y=y, dates=dates, instruments=instruments)


def iterate_minibatches(data: SegmentData, batch_size: int, shuffle: bool):
    idx = np.arange(len(data.y))
    if shuffle:
        np.random.shuffle(idx)
    for start in range(0, len(idx), batch_size):
        batch_idx = idx[start:start + batch_size]
        yield batch_idx


def build_models(config: dict[str, Any]):
    kwargs = config['model']['kwargs']
    hidden_size = int(kwargs['hidden_size'])
    num_layers = int(kwargs['num_layers'])
    num_states = int(kwargs['num_states'])
    d_feat = int(kwargs.get('d_feat', 158))
    backbone = RNN(input_size=d_feat, hidden_size=hidden_size, num_layers=num_layers, rnn_arch='GRU', use_attn=True, dropout=float(kwargs.get('dropout', 0.0)))
    router = TRA(backbone.output_size, num_states=num_states, hidden_size=8, tau=1.0, src_info='LR_TPE')
    device = torch.device('cuda:0' if str(kwargs.get('device', 'cuda')).lower() == 'cuda' and torch.cuda.is_available() else 'cpu')
    return backbone.to(device), router.to(device), device


def compute_daily_ic(scores: pd.Series, labels: pd.Series) -> float:
    aligned = pd.concat([scores.rename('score'), labels.rename('label')], axis=1).dropna()
    if aligned.empty:
        return 0.0
    values: list[float] = []
    for _, cross in aligned.groupby(level=0):
        if len(cross) < 20:
            continue
        corr = cross['score'].corr(cross['label'], method='spearman')
        if pd.notna(corr):
            values.append(float(corr))
    return float(np.mean(values)) if values else 0.0


def run_epoch(backbone, router, optimizer, data: SegmentData, device: torch.device, batch_size: int, train: bool) -> tuple[float, float]:
    backbone.train(train)
    router.train(train)
    losses: list[float] = []
    pred_rows: list[np.ndarray] = []
    label_rows: list[np.ndarray] = []
    date_rows: list[str] = []
    inst_rows: list[str] = []

    for batch_idx in iterate_minibatches(data, batch_size=batch_size, shuffle=train):
        x = torch.from_numpy(data.x[batch_idx]).to(device, non_blocking=True)
        y = torch.from_numpy(data.y[batch_idx]).to(device, non_blocking=True)
        hist = torch.zeros((x.shape[0], x.shape[1], router.num_states), dtype=torch.float32, device=device)
        with torch.set_grad_enabled(train):
            hidden = backbone(x)
            preds, choice, prob = router(hidden, hist)
            pred = preds.squeeze(-1) if choice is None else (preds * prob).sum(dim=1)
            loss = torch.mean((pred - y) ** 2)
            if train:
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                optimizer.step()
        losses.append(float(loss.detach().cpu()))
        pred_rows.append(pred.detach().cpu().numpy())
        label_rows.append(data.y[batch_idx])
        date_rows.extend(data.dates[i] for i in batch_idx)
        inst_rows.extend(data.instruments[i] for i in batch_idx)
    if pred_rows:
        pred_series = pd.Series(np.concatenate(pred_rows), index=pd.MultiIndex.from_arrays([date_rows, inst_rows]))
        label_series = pd.Series(np.concatenate(label_rows), index=pd.MultiIndex.from_arrays([date_rows, inst_rows]))
        daily_ic = compute_daily_ic(pred_series, label_series)
    else:
        daily_ic = 0.0
    return float(np.mean(losses)) if losses else 0.0, daily_ic


def predict(backbone, router, data: SegmentData, device: torch.device, batch_size: int) -> pd.DataFrame:
    backbone.eval()
    router.eval()
    rows: list[dict[str, Any]] = []
    with torch.no_grad():
        for batch_idx in iterate_minibatches(data, batch_size=batch_size, shuffle=False):
            x = torch.from_numpy(data.x[batch_idx]).to(device, non_blocking=True)
            hist = torch.zeros((x.shape[0], x.shape[1], router.num_states), dtype=torch.float32, device=device)
            hidden = backbone(x)
            preds, choice, prob = router(hidden, hist)
            scores = preds.squeeze(-1) if choice is None else (preds * prob).sum(dim=1)
            values = scores.detach().cpu().numpy()
            for offset, score in enumerate(values):
                i = batch_idx[offset]
                rows.append({'datetime': data.dates[i], 'instrument': data.instruments[i], 'score': float(score)})
    return pd.DataFrame(rows)


def build_price_output(test_segment: tuple[str, str]) -> pd.DataFrame:
    frame = D.features(
        D.instruments('all'),
        ['$close'],
        start_time=test_segment[0],
        end_time=test_segment[1],
        freq='day',
    ).reset_index()
    frame = frame.rename(columns={'datetime': 'date', 'instrument': 'instrument', '$close': 'close'})
    frame['instrument'] = frame['instrument'].astype(str).str.upper().map(convert_qlib_instrument)
    return frame[['date', 'instrument', 'close']].copy()


def main() -> int:
    overall_start = time.time()
    config = load_config()
    qlib.init(provider_uri=str(PROVIDER_URI.resolve()), region='cn')

    dump_start = time.time()
    dump_stats = build_tushare_qlib_dump(force=False)
    dump_minutes = round((time.time() - dump_start) / 60.0, 2)

    qlib.init(provider_uri=str(PROVIDER_URI.resolve()), region='cn')

    feature_start = time.time()
    dataset = init_dataset(config)
    train_data = prepare_segment(dataset, 'train')
    valid_data = prepare_segment(dataset, 'valid')
    test_data = prepare_segment(dataset, 'test')
    feature_minutes = round((time.time() - feature_start) / 60.0, 2)

    backbone, router, device = build_models(config)
    lr = float(config['model']['kwargs'].get('lr', 1e-3))
    batch_size = int(config['model']['kwargs']['batch_size'])
    epochs = int(config['model']['kwargs']['n_epochs'])
    optimizer = torch.optim.Adam(list(backbone.parameters()) + list(router.parameters()), lr=lr)

    if torch.cuda.is_available() and device.type == 'cuda':
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()

    history: list[dict[str, Any]] = []
    training_start = time.time()
    best_valid_ic = float('-inf')
    best_epoch = 0
    best_state = None
    patience = 8
    patience_left = patience

    for epoch in range(epochs):
        train_loss, train_ic = run_epoch(backbone, router, optimizer, train_data, device, batch_size=batch_size, train=True)
        valid_loss, valid_ic = run_epoch(backbone, router, optimizer, valid_data, device, batch_size=batch_size, train=False)
        peak_mem = torch.cuda.max_memory_allocated() / 1024**3 if torch.cuda.is_available() and device.type == 'cuda' else 0.0
        history.append({
            'epoch': epoch + 1,
            'train_loss': train_loss,
            'train_ic': train_ic,
            'valid_loss': valid_loss,
            'valid_ic': valid_ic,
            'peak_mem_gb': peak_mem,
        })
        print(
            f"epoch={epoch + 1:02d} train_loss={train_loss:.6f} valid_loss={valid_loss:.6f} valid_ic={valid_ic:.6f} gpu_mem_gb={peak_mem:.3f}",
            flush=True,
        )
        if valid_ic > best_valid_ic:
            best_valid_ic = valid_ic
            best_epoch = epoch + 1
            best_state = {
                'backbone': {k: v.detach().cpu() for k, v in backbone.state_dict().items()},
                'router': {k: v.detach().cpu() for k, v in router.state_dict().items()},
            }
            patience_left = patience
        else:
            patience_left -= 1
            if patience_left <= 0:
                break

    if best_state is not None:
        backbone.load_state_dict(best_state['backbone'])
        router.load_state_dict(best_state['router'])

    predictions = predict(backbone, router, test_data, device, batch_size=batch_size)
    training_minutes = round((time.time() - training_start) / 60.0, 2)
    peak_mem_gb = round(torch.cuda.max_memory_allocated() / 1024**3, 3) if torch.cuda.is_available() and device.type == 'cuda' else 0.0

    output_cfg = config['output']
    model_path = ROOT / output_cfg['model_path']
    factor_path = ROOT / output_cfg['factor_path']
    report_path = ROOT / output_cfg['report_path']
    model_path.parent.mkdir(parents=True, exist_ok=True)
    factor_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        'mode': 'manual_fallback',
        'provider_uri': str(PROVIDER_URI.resolve()),
        'config_path': str(CONFIG_PATH),
        'segments': config['dataset'],
        'dump_stats': dump_stats.__dict__,
        'train_samples': len(train_data.y),
        'valid_samples': len(valid_data.y),
        'test_samples': len(test_data.y),
        'epochs': len(history),
        'best_epoch': best_epoch,
        'best_valid_ic': best_valid_ic,
        'history': history,
        'predictions': predictions.to_dict(orient='records'),
        'peak_mem_gb': peak_mem_gb,
        'dump_minutes': dump_minutes,
        'feature_minutes': feature_minutes,
        'training_minutes': training_minutes,
        'elapsed_minutes': round((time.time() - overall_start) / 60.0, 2),
    }
    with model_path.open('wb') as handle:
        pickle.dump(payload, handle)

    price_frame = build_price_output(tuple(config['dataset']['test']))
    PRICE_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    price_frame.to_parquet(PRICE_OUTPUT_PATH, index=False)

    eval_cmd = [
        str(ROOT / '.venv' / 'Scripts' / 'python.exe'),
        str(IC_EVAL_SCRIPT),
        '--model-path', str(model_path),
        '--config-path', str(CONFIG_PATH),
        '--factor-path', str(factor_path),
        '--prices-path', str(PRICE_OUTPUT_PATH),
        '--output-path', str(report_path),
        '--factor-name', FACTOR_NAME,
        '--factor-id', FACTOR_NAME,
    ]
    subprocess.run(eval_cmd, cwd=str(ROOT), check=True)

    report = json.loads(report_path.read_text(encoding='utf-8'))
    basic = report['basic_metrics']
    summary = TrainStats(
        train_samples=len(train_data.y),
        valid_samples=len(valid_data.y),
        test_samples=len(test_data.y),
        epochs_ran=len(history),
        peak_mem_gb=peak_mem_gb,
        best_valid_ic=float(best_valid_ic),
        ic_mean=float(basic['rank_ic_mean']),
        ic_std=float(basic['rank_ic_mean'] / basic['icir']) if basic['icir'] not in (0, 0.0) else 0.0,
        icir=float(basic['icir']),
        training_minutes=training_minutes,
        dump_minutes=dump_minutes,
        feature_minutes=feature_minutes,
    )
    print(json.dumps({
        'dump_stats': dump_stats.__dict__,
        'train_stats': summary.__dict__,
        'model_path': str(model_path),
        'factor_path': str(factor_path),
        'report_path': str(report_path),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

from __future__ import annotations

import json
import pickle
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import yaml

import qlib
from qlib.data.dataset.handler import DataHandlerLP
from qlib.tests.config import CSI100_MARKET, get_dataset_config
from qlib.utils import init_instance_by_config
from qlib.contrib.model.pytorch_tra import RNN, TRA

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / 'src' / 'alpha_research' / 'qlib_model_configs' / 'tra_config.yaml'
PROVIDER_URI = ROOT / 'runtime' / 'qlib_data' / 'cn_data'


@dataclass
class SegmentData:
    x: np.ndarray
    y: np.ndarray
    dates: list[str]
    instruments: list[str]


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding='utf-8'))


def init_dataset(config: dict):
    train_start, train_end = config['dataset']['train']
    valid_start, valid_end = config['dataset']['valid']
    test_start, test_end = config['dataset']['test']
    calendar_path = ROOT / 'runtime' / 'qlib_data' / 'cn_data' / 'calendars' / 'day.txt'
    max_date = pd.Timestamp(calendar_path.read_text(encoding='utf-8').splitlines()[-1])
    requested_test_end = pd.Timestamp(test_end)
    if requested_test_end > max_date:
        effective_train_end = pd.Timestamp('2018-12-31')
        effective_valid_start = pd.Timestamp('2019-01-01')
        effective_valid_end = pd.Timestamp('2019-12-31')
        effective_test_start = pd.Timestamp('2020-01-01')
        effective_test_end = min(pd.Timestamp('2020-08-31'), max_date)
    else:
        effective_test_end = min(requested_test_end, max_date)
        effective_valid_end = min(pd.Timestamp(valid_end), effective_test_end - pd.Timedelta(days=1))
        effective_valid_start = min(pd.Timestamp(valid_start), effective_valid_end)
        effective_train_end = min(pd.Timestamp(train_end), effective_valid_start - pd.Timedelta(days=1))
        requested_test_start = pd.Timestamp(test_start)
        effective_test_start = requested_test_start if requested_test_start <= effective_test_end else (effective_valid_end + pd.Timedelta(days=1))
    dataset_config = get_dataset_config(
        dataset_class='Alpha158',
        train=(train_start, effective_train_end.strftime('%Y-%m-%d')),
        valid=(effective_valid_start.strftime('%Y-%m-%d'), effective_valid_end.strftime('%Y-%m-%d')),
        test=(effective_test_start.strftime('%Y-%m-%d'), effective_test_end.strftime('%Y-%m-%d')),
        handler_kwargs={
            'start_time': train_start,
            'end_time': effective_test_end.strftime('%Y-%m-%d'),
            'fit_start_time': train_start,
            'fit_end_time': effective_train_end.strftime('%Y-%m-%d'),
            'instruments': CSI100_MARKET,
        },
    )
    return init_instance_by_config(dataset_config), {
        'train': [train_start, effective_train_end.strftime('%Y-%m-%d')],
        'valid': [effective_valid_start.strftime('%Y-%m-%d'), effective_valid_end.strftime('%Y-%m-%d')],
        'test': [effective_test_start.strftime('%Y-%m-%d'), effective_test_end.strftime('%Y-%m-%d')],
        'calendar_max': max_date.strftime('%Y-%m-%d'),
        'requested_test_end': test_end,
    }


def prepare_segment(dataset, segment: str) -> SegmentData:
    frame = dataset.prepare(segment, col_set=['feature', 'label'], data_key=DataHandlerLP.DK_L)
    feature_frame = frame['feature'].astype(float).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    label_frame = frame['label'].astype(float).replace([np.inf, -np.inf], 0.0).fillna(0.0)

    if isinstance(feature_frame.index, pd.MultiIndex):
        level0 = feature_frame.index.get_level_values(0)
        level1 = feature_frame.index.get_level_values(1)
        level0_dt = pd.to_datetime(level0, errors='coerce')
        level1_dt = pd.to_datetime(level1, errors='coerce')
        if level0_dt.notna().sum() >= level1_dt.notna().sum():
            dates = level0_dt.strftime('%Y-%m-%d').tolist()
            instruments = [str(x).upper() for x in level1]
        else:
            dates = level1_dt.strftime('%Y-%m-%d').tolist()
            instruments = [str(x).upper() for x in level0]
    else:
        dates = pd.to_datetime(feature_frame.index).strftime('%Y-%m-%d').tolist()
        instruments = ['UNKNOWN'] * len(feature_frame)

    x = feature_frame.to_numpy(dtype=np.float32).reshape(len(feature_frame), 1, feature_frame.shape[1])
    y = label_frame.iloc[:, 0].to_numpy(dtype=np.float32)
    return SegmentData(x=x, y=y, dates=dates, instruments=instruments)


def iterate_minibatches(data: SegmentData, batch_size: int, shuffle: bool) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    idx = np.arange(len(data.y))
    if shuffle:
        np.random.shuffle(idx)
    for start in range(0, len(idx), batch_size):
        batch_idx = idx[start:start + batch_size]
        yield data.x[batch_idx], data.y[batch_idx], [data.dates[i] for i in batch_idx], [data.instruments[i] for i in batch_idx]


def build_models(config: dict):
    hidden_size = int(config['model']['kwargs']['hidden_size'])
    num_layers = int(config['model']['kwargs']['num_layers'])
    num_states = int(config['model']['kwargs']['num_states'])
    backbone = RNN(input_size=158, hidden_size=hidden_size, num_layers=num_layers, rnn_arch='GRU', use_attn=True, dropout=0.0)
    router = TRA(backbone.output_size, num_states=num_states, hidden_size=8, tau=1.0, src_info='LR_TPE')
    device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    return backbone.to(device), router.to(device), device


def run_epoch(backbone, router, optimizer, data: SegmentData, device, batch_size: int, train: bool) -> float:
    backbone.train(train)
    router.train(train)
    losses = []
    for x_np, y_np, _, _ in iterate_minibatches(data, batch_size=batch_size, shuffle=train):
        x = torch.from_numpy(x_np).to(device)
        y = torch.from_numpy(y_np).to(device)
        hist = torch.zeros((x.shape[0], x.shape[1], router.num_states), dtype=torch.float32, device=device)
        with torch.set_grad_enabled(train):
            hidden = backbone(x)
            preds, choice, prob = router(hidden, hist)
            if choice is None:
                pred = preds.squeeze(-1)
            else:
                pred = (preds * prob).sum(dim=1)
            loss = torch.mean((pred - y) ** 2)
            if train:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
        losses.append(float(loss.detach().cpu()))
    return float(np.mean(losses)) if losses else 0.0


def predict(backbone, router, data: SegmentData, device, batch_size: int) -> pd.DataFrame:
    backbone.eval()
    router.eval()
    rows = []
    offset = 0
    with torch.no_grad():
        for x_np, _, dates, instruments in iterate_minibatches(data, batch_size=batch_size, shuffle=False):
            x = torch.from_numpy(x_np).to(device)
            hist = torch.zeros((x.shape[0], x.shape[1], router.num_states), dtype=torch.float32, device=device)
            hidden = backbone(x)
            preds, choice, prob = router(hidden, hist)
            if choice is None:
                scores = preds.squeeze(-1)
            else:
                scores = (preds * prob).sum(dim=1)
            scores_np = scores.detach().cpu().numpy()
            for i, score in enumerate(scores_np):
                rows.append({'date': dates[i], 'instrument': instruments[i], 'score': float(score)})
            offset += len(scores_np)
    return pd.DataFrame(rows)


def main() -> None:
    started = time.time()
    config = load_config()
    qlib.init(provider_uri=str(PROVIDER_URI.resolve()), region='cn')
    dataset, effective_periods = init_dataset(config)
    train_data = prepare_segment(dataset, 'train')
    valid_data = prepare_segment(dataset, 'valid')
    test_data = prepare_segment(dataset, 'test')

    backbone, router, device = build_models(config)
    optimizer = torch.optim.Adam(list(backbone.parameters()) + list(router.parameters()), lr=1e-3)
    batch_size = 512
    epochs = 5

    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()

    history = []
    for epoch in range(epochs):
        train_loss = run_epoch(backbone, router, optimizer, train_data, device, batch_size, train=True)
        valid_loss = run_epoch(backbone, router, optimizer, valid_data, device, batch_size, train=False)
        history.append({'epoch': epoch, 'train_loss': train_loss, 'valid_loss': valid_loss})

    predictions = predict(backbone, router, test_data, device, batch_size)
    model_path = ROOT / config['output']['model_path']
    model_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'mode': 'manual_fallback',
        'config_path': str(CONFIG_PATH),
        'provider_uri': str(PROVIDER_URI.resolve()),
        'effective_periods': effective_periods,
        'instruments': 'csi100',
        'epochs': epochs,
        'history': history,
        'predictions': predictions.to_dict(orient='records'),
        'peak_mem_gb': round(torch.cuda.max_memory_allocated() / 1024**3, 3) if torch.cuda.is_available() else 0.0,
        'elapsed_minutes': round((time.time() - started) / 60.0, 2),
    }
    with model_path.open('wb') as f:
        pickle.dump(payload, f)
    print(json.dumps({'model_path': str(model_path), 'epochs': epochs, 'peak_mem_gb': payload['peak_mem_gb'], 'elapsed_minutes': payload['elapsed_minutes'], 'rows': len(predictions)}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

from __future__ import annotations

import json
import pickle
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
import yaml

import qlib
from qlib.contrib.model.pytorch_alstm import ALSTM
from qlib.data import D
from qlib.data.dataset.handler import DataHandlerLP
from qlib.tests.config import CSI100_MARKET, get_dataset_config
from qlib.utils import init_instance_by_config

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from alpha_research.qlib_factor_extractor import convert_qlib_instrument

PROVIDER_URI = ROOT / 'runtime' / 'qlib_data' / 'cn_data'
CONFIG_PATH = ROOT / 'src' / 'alpha_research' / 'qlib_model_configs' / 'alstm_config.yaml'
PRICE_OUTPUT_PATH = ROOT / 'runtime' / 'alpha_research' / 'qlib_alstm_test_prices.parquet'
IC_EVAL_SCRIPT = ROOT / 'scripts' / 'run_qlib_factor_ic_eval.py'
FACTOR_NAME = 'qlib_alstm_v1'


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding='utf-8'))


def resolve_segments(config: dict[str, Any]) -> dict[str, tuple[str, str]]:
    calendar = pd.DatetimeIndex(D.calendar())
    available_end = pd.Timestamp(calendar[-1]).normalize()
    requested = {
        'train': tuple(config['dataset']['train']),
        'valid': tuple(config['dataset']['valid']),
        'test': tuple(config['dataset']['test']),
    }
    requested_test_end = pd.Timestamp(requested['test'][1]).normalize()
    if requested_test_end <= available_end:
        return requested
    return {
        'train': ('2016-01-01', '2018-12-31'),
        'valid': ('2019-01-01', '2019-12-31'),
        'test': ('2020-01-01', str(available_end.date())),
    }


def build_dataset(segments: dict[str, tuple[str, str]]):
    dataset_config = get_dataset_config(
        dataset_class='Alpha158',
        train=segments['train'],
        valid=segments['valid'],
        test=segments['test'],
        handler_kwargs={
            'start_time': segments['train'][0],
            'end_time': segments['test'][1],
            'fit_start_time': segments['train'][0],
            'fit_end_time': segments['train'][1],
            'instruments': CSI100_MARKET,
        },
    )
    return init_instance_by_config(dataset_config)


def clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return cleaned


def compute_daily_ic(pred: pd.Series, label_df: pd.DataFrame) -> float:
    label = label_df.squeeze().rename('label').astype(float)
    aligned = pd.concat([pred.rename('score'), label], axis=1).dropna()
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


def predict_frame(model: ALSTM, features: pd.DataFrame) -> pd.Series:
    model.ALSTM_model.eval()
    x_values = features.values
    sample_num = x_values.shape[0]
    preds = []
    for begin in range(0, sample_num, model.batch_size):
        end = min(begin + model.batch_size, sample_num)
        x_batch = torch.from_numpy(x_values[begin:end]).float().to(model.device)
        with torch.no_grad():
            pred = model.ALSTM_model(x_batch).detach().cpu().numpy().reshape(-1)
        preds.append(pred)
    return pd.Series(np.concatenate(preds), index=features.index, name='score')


def build_price_output(test_segment: tuple[str, str]) -> pd.DataFrame:
    frame = D.features(
        D.instruments(CSI100_MARKET),
        ['$close'],
        start_time=test_segment[0],
        end_time=test_segment[1],
        freq='day',
    ).reset_index()
    frame = frame.rename(columns={'datetime': 'date', 'instrument': 'instrument', '$close': 'close'})
    frame['instrument'] = frame['instrument'].astype(str).str.upper().map(convert_qlib_instrument)
    return frame[['date', 'instrument', 'close']].copy()


def main() -> int:
    start_time = time.time()
    qlib.init(provider_uri=str(PROVIDER_URI.resolve()), region='cn')
    config = load_config(CONFIG_PATH)
    segments = resolve_segments(config)
    dataset = build_dataset(segments)

    train_raw, valid_raw, test_raw = dataset.prepare(['train', 'valid', 'test'], col_set=['feature', 'label'], data_key=DataHandlerLP.DK_L)
    x_train = clean_frame(train_raw['feature'])
    y_train = clean_frame(train_raw['label'])
    x_valid = clean_frame(valid_raw['feature'])
    y_valid = clean_frame(valid_raw['label'])
    x_test = clean_frame(test_raw['feature'])
    y_test = clean_frame(test_raw['label'])

    model_kwargs = deepcopy(config['model']['kwargs'])
    epochs = int(model_kwargs.pop('n_epochs', 200))
    device_name = str(model_kwargs.pop('device', 'cuda')).lower()
    model = ALSTM(
        d_feat=int(model_kwargs.get('d_feat', 158)),
        hidden_size=int(model_kwargs.get('hidden_size', 64)),
        num_layers=int(model_kwargs.get('num_layers', 2)),
        dropout=float(model_kwargs.get('dropout', 0.0)),
        n_epochs=epochs,
        lr=float(model_kwargs.get('lr', 1e-3)),
        batch_size=int(model_kwargs.get('batch_size', 800)),
        early_stop=20,
        GPU=0 if device_name == 'cuda' and torch.cuda.is_available() else -1,
    )

    history: list[dict[str, float]] = []
    best_ic = -999.0
    best_epoch = -1
    best_state = None
    patience = 20
    patience_left = patience

    for epoch in range(epochs):
        model.train_epoch(x_train, y_train)
        valid_pred = predict_frame(model, x_valid)
        valid_ic = compute_daily_ic(valid_pred, y_valid)
        history.append({'epoch': float(epoch + 1), 'valid_ic': float(valid_ic)})
        print(f'epoch={epoch + 1} valid_ic={valid_ic:.6f}', flush=True)
        if valid_ic > best_ic:
            best_ic = float(valid_ic)
            best_epoch = epoch + 1
            best_state = {key: value.detach().cpu() for key, value in model.ALSTM_model.state_dict().items()}
            patience_left = patience
        else:
            patience_left -= 1
            if patience_left <= 0:
                break

    if best_state is not None:
        model.ALSTM_model.load_state_dict(best_state)
    model.fitted = True

    test_pred = predict_frame(model, x_test)
    test_output = test_pred.reset_index()
    test_output.columns = ['datetime', 'instrument', 'score']

    output_cfg = config['output']
    model_path = ROOT / output_cfg['model_path']
    factor_path = ROOT / output_cfg['factor_path']
    report_path = ROOT / output_cfg['report_path']
    model_path.parent.mkdir(parents=True, exist_ok=True)
    factor_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with model_path.open('wb') as handle:
        pickle.dump(
            {
                'model_class': 'ALSTM',
                'segments': segments,
                'best_epoch': best_epoch,
                'best_val_ic': best_ic,
                'history': history,
                'state_dict': best_state,
                'predictions': test_output,
            },
            handle,
        )

    price_frame = build_price_output(segments['test'])
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
    payload = {
        'status': 'completed',
        'segments': segments,
        'best_epoch': best_epoch,
        'best_val_ic': best_ic,
        'history': history,
        'factor_path': str(factor_path),
        'report_path': str(report_path),
        'registry_status': report['status'],
        'rank_ic_mean': report['basic_metrics']['rank_ic_mean'],
        'icir': report['basic_metrics']['icir'],
        'elapsed_minutes': round((time.time() - start_time) / 60.0, 2),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

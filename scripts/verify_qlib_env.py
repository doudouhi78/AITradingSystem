from __future__ import annotations

import argparse
import json
from pathlib import Path

import torch

import qlib
from qlib.contrib.model.pytorch_alstm import ALSTM
from qlib.data.dataset.handler import DataHandlerLP
from qlib.tests.config import CSI100_MARKET, get_dataset_config
from qlib.utils import init_instance_by_config

ROOT = Path(__file__).resolve().parents[1]
PROVIDER_URI = ROOT / 'runtime' / 'qlib_data' / 'cn_data'


def build_dataset():
    dataset_config = get_dataset_config(
        dataset_class='Alpha158',
        train=('2018-01-01', '2018-06-30'),
        valid=('2018-07-01', '2018-08-31'),
        test=('2018-09-01', '2018-10-31'),
        handler_kwargs={
            'start_time': '2018-01-01',
            'end_time': '2018-10-31',
            'fit_start_time': '2018-01-01',
            'fit_end_time': '2018-06-30',
            'instruments': CSI100_MARKET,
        },
    )
    return init_instance_by_config(dataset_config)


def summarize(train_result: dict[str, object] | None = None) -> str:
    version = getattr(qlib, '__version__', 'unknown')
    cuda_ok = torch.cuda.is_available()
    gpu_name = torch.cuda.get_device_name(0) if cuda_ok else 'CPU'
    gpu_mem = f"{torch.cuda.get_device_properties(0).total_memory / 1024**3:.0f}GB" if cuda_ok else '0GB'
    alstm_status = 'OK'
    if train_result is None:
        return f'Qlib {version} | CUDA: {cuda_ok} | GPU: {gpu_name} {gpu_mem} | ALSTM: {alstm_status}'
    return (
        f'Qlib {version} | CUDA: {cuda_ok} | GPU: {gpu_name} {gpu_mem} | '
        f"ALSTM: {alstm_status} | Train: {train_result['status']} | PeakMemGB: {train_result['peak_mem_gb']:.3f}"
    )


def run_manual_train(model: ALSTM, train_df, valid_df) -> dict[str, object]:
    x_train = train_df['feature'].astype(float).replace([float('inf'), float('-inf')], 0.0).fillna(0.0)
    y_train = train_df['label'].astype(float).replace([float('inf'), float('-inf')], 0.0).fillna(0.0)
    x_valid = valid_df['feature'].astype(float).replace([float('inf'), float('-inf')], 0.0).fillna(0.0)
    y_valid = valid_df['label'].astype(float).replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

    losses = []
    for epoch in range(5):
        model.train_epoch(x_train, y_train)
        valid_loss, valid_score = model.test_epoch(x_valid, y_valid)
        losses.append({
            'epoch': epoch,
            'valid_loss': float(valid_loss),
            'valid_score': float(valid_score),
        })
    return {
        'status': 'manual_fallback',
        'epochs': 5,
        'metrics': losses,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', action='store_true')
    args = parser.parse_args()

    qlib.init(provider_uri=str(PROVIDER_URI.resolve()), region='cn')
    dataset = build_dataset()
    train_df, valid_df = dataset.prepare(['train', 'valid'], col_set=['feature', 'label'], data_key=DataHandlerLP.DK_L)

    payload: dict[str, object] = {
        'qlib_version': getattr(qlib, '__version__', 'unknown'),
        'provider_uri': str(PROVIDER_URI.resolve()),
        'cuda_available': bool(torch.cuda.is_available()),
        'gpu_name': torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        'gpu_total_mem_gb': round(torch.cuda.get_device_properties(0).total_memory / 1024**3, 2) if torch.cuda.is_available() else 0.0,
        'alstm_import': 'OK',
        'datahandler': type(dataset.handler).__name__,
        'datahandler_ok': isinstance(dataset.handler, DataHandlerLP),
        'train_shape': list(train_df.shape),
        'valid_shape': list(valid_df.shape),
    }

    train_result = None
    if args.train:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats()
        model = ALSTM(
            d_feat=158,
            n_epochs=5,
            GPU=0,
            batch_size=256,
            hidden_size=64,
            num_layers=2,
            dropout=0.0,
            early_stop=10,
            lr=0.001,
        )
        try:
            model.fit(dataset)
            train_result = {
                'status': 'fit_ok',
                'epochs': 5,
            }
        except Exception as exc:
            train_result = run_manual_train(model, train_df, valid_df)
            train_result['fit_exception'] = f'{type(exc).__name__}: {exc}'
        train_result['peak_mem_gb'] = round(torch.cuda.max_memory_allocated() / 1024**3, 3) if torch.cuda.is_available() else 0.0
        payload['train'] = train_result

    payload['summary'] = summarize(train_result)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(payload['summary'])


if __name__ == '__main__':
    main()

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import alphalens as al
import numpy as np
import pandas as pd
from pysr import PySRRegressor


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / 'src'
SCRIPT_ROOT = ROOT / 'scripts'
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from alpha_research.data_loader import load_factor_input, load_prices, select_top_n_by_liquidity  # noqa: E402
from run_feature_factory import compute_factor_series, normalize_with_cupy  # noqa: E402


TOP_FACTORS = ['alpha065', 'alpha006', 'alpha047', 'alpha054', 'momentum_12_1']
FORWARD_DAYS = 5
START = '2016-01-01'
TRAIN_END = '2021-12-31'
EVAL_START = '2022-01-01'
EVAL_END = '2023-12-31'
SEARCH_END = '2023-12-31'
TOP_N = 200
FORMULA_PATH = ROOT / 'runtime' / 'alpha_research' / 'pysr_discovered_formulas.json'
EVAL_PATH = ROOT / 'runtime' / 'alpha_research' / 'pysr_factor_eval.json'
REGISTRY_PATH = ROOT / 'runtime' / 'factor_registry' / 'factor_registry.json'
FACTOR_FILE = ROOT / 'src' / 'alpha_research' / 'factors' / 'pysr_factors.py'


def build_market_inputs() -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    instruments = select_top_n_by_liquidity('stock', START, SEARCH_END, top_n=TOP_N)
    prices = load_prices(instruments, START, SEARCH_END, asset_type='stock')
    factor_input = load_factor_input(instruments, START, SEARCH_END, asset_type='stock').copy()
    factor_input['open'] = factor_input['close']
    return prices, factor_input, instruments


def build_feature_frame(prices: pd.DataFrame, factor_input: pd.DataFrame) -> pd.DataFrame:
    series_list = [compute_factor_series(name, factor_input) for name in TOP_FACTORS]
    factors = pd.concat(series_list, axis=1)
    target = prices.pct_change(FORWARD_DAYS, fill_method=None).shift(-FORWARD_DAYS).stack(future_stack=True).rename('target_return')
    target.index = target.index.set_names(['date', 'asset'])
    frame = factors.join(target, how='inner').reset_index()
    frame['asset'] = frame['asset'].astype(str).str.zfill(6)
    frame['date'] = pd.to_datetime(frame['date'])
    feature_cols = TOP_FACTORS.copy()
    frame = frame.dropna(subset=['target_return'])
    frame = frame.loc[frame[feature_cols].notna().all(axis=1)].copy()
    frame = normalize_with_cupy(frame, feature_cols)
    return frame.sort_values(['date', 'asset']).reset_index(drop=True)


def sample_training_frame(frame: pd.DataFrame) -> pd.DataFrame:
    train = frame.loc[frame['date'] <= pd.Timestamp(TRAIN_END)].copy()
    return train.groupby('date', group_keys=False, sort=True).apply(lambda x: x.sample(n=min(500, len(x)), random_state=42))


def fit_pysr(train: pd.DataFrame) -> PySRRegressor:
    model = PySRRegressor(
        niterations=50,
        populations=20,
        population_size=50,
        maxsize=20,
        binary_operators=['+', '-', '*', '/'],
        unary_operators=['square', 'sqrt', 'log', 'abs'],
        elementwise_loss='loss(x, y) = (x - y)^2',
        progress=False,
        verbosity=0,
        model_selection='best',
        temp_equation_file=False,
        deterministic=True,
        parallelism='serial',
        batching=True,
        batch_size=2048,
        random_state=42,
    )
    model.fit(train[TOP_FACTORS].to_numpy(dtype=float), train['target_return'].to_numpy(dtype=float), variable_names=TOP_FACTORS)
    return model


def rank_formulas(model: PySRRegressor, train: pd.DataFrame) -> list[dict[str, float | int | str]]:
    equations = model.equations_.copy().reset_index(drop=True)
    ranked: list[dict[str, float | int | str]] = []
    y = train['target_return'].to_numpy(dtype=float)
    denom = float(np.sum((y - y.mean()) ** 2)) or 1.0
    for eq_idx, row in equations.head(5).iterrows():
        preds = model.predict(train[TOP_FACTORS].to_numpy(dtype=float), index=eq_idx)
        sse = float(np.sum((y - preds) ** 2))
        r2 = 1.0 - sse / denom
        formula = str(row['equation'])
        ranked.append({
            'rank': int(len(ranked) + 1),
            'formula': formula,
            'r2': float(r2),
            'complexity': int(row['complexity']),
            'equation_index': int(eq_idx),
        })
    FORMULA_PATH.write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding='utf-8')
    return ranked


def _formula_namespace(feature_frame: pd.DataFrame) -> dict[str, object]:
    namespace: dict[str, object] = {name: feature_frame[name] for name in TOP_FACTORS}
    namespace.update({
        'square': lambda x: x * x,
        'sqrt': lambda x: np.sqrt(np.abs(x)),
        'log': lambda x: np.log(np.abs(x) + 1e-12),
        'abs': lambda x: np.abs(x),
    })
    return namespace


def evaluate_formula(feature_frame: pd.DataFrame, formula: str) -> pd.Series:
    values = eval(formula, {'__builtins__': {}}, _formula_namespace(feature_frame))
    if isinstance(values, pd.Series):
        return values.replace([np.inf, -np.inf], np.nan)
    return pd.Series(values, index=feature_frame.index, dtype=float).replace([np.inf, -np.inf], np.nan)


def alphalens_icir(pred_series: pd.Series, prices: pd.DataFrame) -> dict[str, float | int | None]:
    try:
        clean = al.utils.get_clean_factor_and_forward_returns(
            factor=pred_series,
            prices=prices,
            periods=(FORWARD_DAYS,),
            quantiles=5,
            max_loss=1.0,
        )
    except Exception:
        try:
            clean = al.utils.get_clean_factor_and_forward_returns(
                factor=pred_series,
                prices=prices,
                periods=(FORWARD_DAYS,),
                bins=5,
                max_loss=1.0,
            )
        except Exception:
            return {'ic_mean': None, 'ic_std': None, 'icir': None, 'count': 0}
    if clean.empty:
        return {'ic_mean': None, 'ic_std': None, 'icir': None, 'count': 0}
    ic = al.performance.factor_information_coefficient(clean)
    series = ic.iloc[:, 0].dropna()
    if series.empty:
        return {'ic_mean': None, 'ic_std': None, 'icir': None, 'count': 0}
    mean = float(series.mean())
    std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
    return {'ic_mean': mean, 'ic_std': std, 'icir': (mean / std if std else mean), 'count': int(len(series))}


def write_pysr_factor_file(formulas: list[dict[str, float | int | str]]) -> None:
    lines = [
        'from __future__ import annotations',
        '',
        'import numpy as np',
        'import pandas as pd',
        '',
        f"TOP_FACTORS = {TOP_FACTORS!r}",
        f"FORMULAS = {[{'rank': item['rank'], 'formula': item['formula']} for item in formulas[:3]]!r}",
        '',
        'def _namespace(feature_frame: pd.DataFrame) -> dict[str, object]:',
        '    namespace: dict[str, object] = {name: feature_frame[name] for name in TOP_FACTORS}',
        '    namespace.update({',
        "        'square': lambda x: x * x,",
        "        'sqrt': lambda x: np.sqrt(np.abs(x)),",
        "        'log': lambda x: np.log(np.abs(x) + 1e-12),",
        "        'abs': lambda x: np.abs(x),",
        '    })',
        '    return namespace',
        '',
        'def evaluate_formula(feature_frame: pd.DataFrame, formula: str) -> pd.Series:',
        "    values = eval(formula, {'__builtins__': {}}, _namespace(feature_frame))",
        '    if isinstance(values, pd.Series):',
        '        return values.replace([np.inf, -np.inf], np.nan)',
        '    return pd.Series(values, index=feature_frame.index, dtype=float).replace([np.inf, -np.inf], np.nan)',
        '',
    ]
    for item in formulas[:3]:
        lines.extend([
            f"def pysr_factor_{item['rank']}(feature_frame: pd.DataFrame) -> pd.Series:",
            f"    return evaluate_formula(feature_frame, {item['formula']!r})",
            '',
        ])
    FACTOR_FILE.write_text('\n'.join(lines), encoding='utf-8')


def update_registry(results: list[dict[str, object]]) -> int:
    existing = json.loads(REGISTRY_PATH.read_text(encoding='utf-8')) if REGISTRY_PATH.exists() else []
    keep = [item for item in existing if not str(item.get('factor_id', '')).startswith('pysr_formula_')]
    added = []
    for item in results:
        icir = item['metrics']['icir']
        if icir is not None and icir > 0.05:
            added.append({
                'factor_id': f"pysr_formula_{item['rank']}",
                'factor_name': f"pysr_formula_{item['rank']}",
                'category': 'symbolic_regression',
                'formula': item['formula'],
                'ic_mean': item['metrics']['ic_mean'],
                'ic_std': item['metrics']['ic_std'],
                'icir': icir,
                'status': 'pass',
                'source': 'pysr',
            })
    REGISTRY_PATH.write_text(json.dumps(keep + added, ensure_ascii=False, indent=2), encoding='utf-8')
    return len(added)


def main() -> None:
    prices, factor_input, instruments = build_market_inputs()
    feature_frame = build_feature_frame(prices, factor_input)
    train = sample_training_frame(feature_frame)
    eval_frame = feature_frame.loc[(feature_frame['date'] >= pd.Timestamp(EVAL_START)) & (feature_frame['date'] <= pd.Timestamp(EVAL_END))].copy()
    model = fit_pysr(train)
    formulas = rank_formulas(model, train)
    write_pysr_factor_file(formulas)

    wide_eval = eval_frame.set_index(['date', 'asset'])[TOP_FACTORS].sort_index()
    eval_prices = prices.loc[(prices.index >= pd.Timestamp(EVAL_START)) & (prices.index <= pd.Timestamp(EVAL_END))].copy()
    results: list[dict[str, object]] = []
    for item in formulas[:3]:
        values = evaluate_formula(wide_eval, item['formula']).dropna()
        values.index = values.index.set_names(['date', 'asset'])
        metrics = alphalens_icir(values, eval_prices)
        results.append({
            'rank': item['rank'],
            'formula': item['formula'],
            'r2': item['r2'],
            'complexity': item['complexity'],
            'metrics': metrics,
            'status': 'pass' if metrics['icir'] is not None and metrics['icir'] > 0.05 else 'fail',
        })

    added = update_registry(results)
    payload = {
        'top_factors': TOP_FACTORS,
        'train_rows': int(len(train)),
        'formula_count': len(formulas),
        'top3_eval': results,
        'registry_added': added,
    }
    EVAL_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

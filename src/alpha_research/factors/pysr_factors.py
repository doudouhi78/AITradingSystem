from __future__ import annotations

import numpy as np
import pandas as pd

TOP_FACTORS = ['alpha065', 'alpha006', 'alpha047', 'alpha054', 'momentum_12_1']
FORMULAS = [{'rank': 1, 'formula': '0.005347974'}, {'rank': 2, 'formula': 'abs(alpha065 * 0.00519571)'}, {'rank': 3, 'formula': '(momentum_12_1 + 4.241988) * 0.0012860908'}]

def _namespace(feature_frame: pd.DataFrame) -> dict[str, object]:
    namespace: dict[str, object] = {name: feature_frame[name] for name in TOP_FACTORS}
    namespace.update({
        'square': lambda x: x * x,
        'sqrt': lambda x: np.sqrt(np.abs(x)),
        'log': lambda x: np.log(np.abs(x) + 1e-12),
        'abs': lambda x: np.abs(x),
    })
    return namespace

def evaluate_formula(feature_frame: pd.DataFrame, formula: str) -> pd.Series:
    values = eval(formula, {'__builtins__': {}}, _namespace(feature_frame))
    if isinstance(values, pd.Series):
        return values.replace([np.inf, -np.inf], np.nan)
    return pd.Series(values, index=feature_frame.index, dtype=float).replace([np.inf, -np.inf], np.nan)

def pysr_factor_1(feature_frame: pd.DataFrame) -> pd.Series:
    return evaluate_formula(feature_frame, '0.005347974')

def pysr_factor_2(feature_frame: pd.DataFrame) -> pd.Series:
    return evaluate_formula(feature_frame, 'abs(alpha065 * 0.00519571)')

def pysr_factor_3(feature_frame: pd.DataFrame) -> pd.Series:
    return evaluate_formula(feature_frame, '(momentum_12_1 + 4.241988) * 0.0012860908')

from __future__ import annotations

import pandas as pd

from alpha_research.factors import pysr_factors


def test_pysr_factors_import_and_evaluate() -> None:
    frame = pd.DataFrame(
        {
            'alpha065': [0.1, -0.2],
            'alpha006': [0.3, 0.4],
            'alpha047': [0.5, -0.1],
            'alpha054': [0.2, 0.7],
            'momentum_12_1': [0.9, -0.6],
        },
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp('2022-01-03'), '000001'), (pd.Timestamp('2022-01-03'), '000002')],
            names=['date', 'asset'],
        ),
    )

    result = pysr_factors.pysr_factor_3(frame)

    assert isinstance(result, pd.Series)
    assert len(result) == 2
    assert result.notna().all()

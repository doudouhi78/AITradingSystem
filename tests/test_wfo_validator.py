from __future__ import annotations

from alpha_research.wfo_validator import generate_wfo_folds


def test_generate_wfo_folds_produces_multiple_windows() -> None:
    folds = generate_wfo_folds()
    assert len(folds) >= 3
    assert folds[0].fold == 1
    assert folds[0].train_start.strftime('%Y-%m') == '2016-01'

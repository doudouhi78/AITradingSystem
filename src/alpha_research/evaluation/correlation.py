from __future__ import annotations

import pandas as pd
from scipy.stats import spearmanr


def deduplicate_factors(
    factor_dict: dict,
    ic_results: dict,
    corr_threshold: float = 0.7,
) -> list[str]:
    names = list(factor_dict.keys())
    if not names:
        return []

    corr_matrix = pd.DataFrame(0.0, index=names, columns=names)
    for i, left in enumerate(names):
        for j in range(i, len(names)):
            right = names[j]
            common_idx = factor_dict[left].index.intersection(factor_dict[right].index)
            if len(common_idx) < 100:
                corr = 0.0
            else:
                corr, _ = spearmanr(factor_dict[left].loc[common_idx], factor_dict[right].loc[common_idx])
                if pd.isna(corr):
                    corr = 0.0
            corr_matrix.loc[left, right] = abs(float(corr))
            corr_matrix.loc[right, left] = abs(float(corr))

    kept: list[str] = []
    removed: set[str] = set()
    for left in names:
        if left in removed:
            continue
        kept.append(left)
        for right in names:
            if left == right or right in removed:
                continue
            if corr_matrix.loc[left, right] > corr_threshold:
                left_icir = abs(ic_results.get(left, {}).get("icir", {}).get("10", 0.0))
                right_icir = abs(ic_results.get(right, {}).get("icir", {}).get("10", 0.0))
                if left_icir >= right_icir:
                    removed.add(right)
                else:
                    removed.add(left)
                    if kept and kept[-1] == left:
                        kept.pop()
                    break
    return [name for name in kept if name not in removed]

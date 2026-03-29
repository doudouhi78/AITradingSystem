# 当前人工基线分批建仓验证

- baseline_ref: exp-20260328-007-manual-entry25-exit20
- review_id: REV-20260328-007
- validation_ids: VAL-20260328-002, VAL-20260328-003, VAL-20260328-004, VAL-20260328-005, VAL-20260328-006, VAL-20260328-007, VAL-20260328-008
- sample_range: 2024-01-02 -> 2026-03-24
- result:
- 半仓一次建仓: Sharpe=0.603459, total_return=0.102164, max_drawdown=-0.113582
- 半仓两步建仓: Sharpe=0.806323, total_return=0.294137, max_drawdown=-0.206400
- judgement: 当前人工基线在半仓一次建仓与两步建仓下都成立，分批建仓没有直接推翻当前基线结论。

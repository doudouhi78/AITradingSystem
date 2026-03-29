# Commander 任务卡 — Sprint 6 收口
发布时间：2026-03-29

## 验收要点

1. `scripts/validate_limit_constraint.py` 存在，能跑通
2. `coordination/phase4_limit_constraint_result.json` 存在，有完整字段
3. 约束生效：VBT涨跌停日成交6次，约束后1次，constraint_effective=true
4. pytest 49 passed

已知风险（不阻断）：Hikyuu 包在 Builder 环境不可用，用了兜底实现，结论方向一致。

验收通过后：
- `git add -A && git commit -m "Phase 4 partial: limit constraint validation (WI-403)"`
- `git push origin main`
- 在 worker_result.md 写 DONE

## Sprint 43 结果 — PySR 符号回归

- PySR 搜索完成：✅
- 发现表达式数：5 个
- Top3 表达式：
  1. [0.005347974] r2=-0.00 complexity=1
  2. [abs(alpha065 * 0.00519571)] r2=0.00 complexity=4
  3. [(momentum_12_1 + 4.241988) * 0.0012860908] r2=0.00 complexity=5
- 样本外 ICIR（2022-2023）：
  1. [0.005347974] icir=null status=fail
  2. [abs(alpha065 * 0.00519571)] icir=null status=fail
  3. [(momentum_12_1 + 4.241988) * 0.0012860908] icir=0.09 status=pass
- 新增 factor_registry 条目：1 个
- pytest：118 passed, 0 failed
- commit：[待提交]

BUILDER_DONE

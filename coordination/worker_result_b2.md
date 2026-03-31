## Sprint 27 结果
- 问题1修复：将 `src\attribution\strategy_attribution.py` 的 `pyfolio` 改为可选导入；缺失时模块仍可 import，`generate_pyfolio_tearsheet()` 输出降级 HTML，不阻断 pytest collect。
- 问题2修复：在 attribution 序列化中统一将 NaN 转为 JSON `null`，并增加 `note` 字段说明原因；已用真实实验 `exp-20260329-008-parquet-entry25-exit20` 重新生成合规 `strategy_attribution.json`。
- pytest 结果：5 passed, 0 failed
- commit：待提交
- push：待推送

BUILDER_DONE

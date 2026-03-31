## Sprint 30 结果
- 修复项1（WI-504）：已新增 `scripts/attribution/run_trade_diagnostics.py`，脚本会扫描全部 ExperimentRun、合并交易、调用 `run_trade_diagnostics()`，并按 gate/holding/vol/month 打印 top/bottom 分组摘要。已用 `D:\AITradingSystem\.venv\Scripts\python.exe scripts/attribution/run_trade_diagnostics.py` 实跑，无报错。
- 修复项2（WI-502/503）：已在 `src/attribution/trade_diagnostics.py` 的分组统计中补充 `avg_holding` 字段，并补齐 `avg_pnl` 字段；输出文件已对齐蓝图为 `attribution_by_gate.json`、`attribution_by_holding.json`、`attribution_by_vol.json`、`attribution_by_month.json`。同时保留旧文件名一份兼容月报/旧调用方，避免下游读取断裂。已重新生成 JSON，确认新文件包含 `avg_holding`。
- 修复项3（WI-533）：已在 `scripts/signal_daily.py` 增加 `is_month_end()` 与 `maybe_generate_monthly_attribution_report()`，基于交易日序列而不是自然月末判断是否为当月最后一个交易日；若命中月末，会调用 `generate_monthly_report(year, month)` 并打印 `月度归因报告已生成：runtime/attribution/reports/attribution_report_{YYYYMM}.html`。
- 测试验证：`D:\AITradingSystem\.venv\Scripts\python.exe -m pytest tests/test_trade_diagnostics.py tests/test_signal_daily.py -q` 通过（6 passed）。`scripts/attribution/run_trade_diagnostics.py` 实跑成功，`scripts/attribution/run_monthly_report.py --year 2026 --month 3` 也已成功生成 `runtime/attribution/reports/attribution_report_202603.html`。月末判断由 `tests/test_signal_daily.py` 覆盖了正常月末、非月末、短月三种情况。
- commit：[待补]
- push：待补

BUILDER_DONE

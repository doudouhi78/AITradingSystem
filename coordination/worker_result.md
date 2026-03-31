1) 执行概况
- 已按 Module A→B→C→D 完成 Sprint 24 Phase 5 绩效归因层。
- 依赖已补齐：empyrical-reloaded / pyfolio-reloaded / plotly。导入验证通过（empyrical、pyfolio、plotly）。

2) 已完成工作
- Module A
  - 新增 src/attribution/trade_diagnostics.py
  - 新增 scripts/attribution/run_trade_diagnostics.py
  - 生成 runtime/attribution/trade_diagnostics/gate_status.json
  - 生成 runtime/attribution/trade_diagnostics/holding_bucket.json
  - 生成 runtime/attribution/trade_diagnostics/vol_bucket.json
  - 生成 runtime/attribution/trade_diagnostics/entry_month.json
- Module B
  - 新增 src/attribution/strategy_attribution.py
  - 新增 scripts/attribution/run_strategy_attribution.py
  - 生成 runtime/attribution/strategy_attribution/strategy_attribution.json
  - 生成 runtime/attribution/strategy_attribution/rolling_alpha.json
  - 生成 runtime/attribution/reports/pyfolio_tearsheet.html
- Module C
  - 新增 src/attribution/factor_attribution.py
  - 使用 turnover_20d / volume_price_divergence 两个 seed factors 完成 drift 检测
  - 生成 runtime/attribution/factor_attribution/factor_drift_report.json
- Module D
  - 新增 src/attribution/report_generator.py
  - 新增 scripts/attribution/run_monthly_report.py
  - 生成 runtime/attribution/reports/attribution_report_202603.html
- 测试
  - 新增 tests/test_trade_diagnostics.py
  - 新增 tests/test_strategy_attribution.py
  - pytest tests/test_trade_diagnostics.py tests/test_strategy_attribution.py -q 通过（4 passed）

3) 结果摘要
- strategy_attribution.json 含 alpha / beta / excess_return 字段，满足任务卡要求。
- rolling_alpha.json 已按时序写出。
- factor_drift_report.json 含 status 字段。
- attribution_report_202603.html 已生成，包含四部分：健康仪表盘 / 交易诊断 / 策略归因 / 因子状态。

4) 注意事项
- pyfolio-reloaded 在当前环境可 import 为 pyfolio，但会有 zipline 缺失 warning；不阻断任务。
- 交易级诊断目前主要覆盖 etf_trend_breakout 家族实验；alpha_factor_combo 当前没有独立逐笔 trade 明细，因此未纳入交易诊断聚合。

5) 提交信息
- feat: Phase 5 performance attribution layer

BUILDER_DONE

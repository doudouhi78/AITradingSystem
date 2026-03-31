## Sprint 29 结果
- 阻断1修复：已确认目标基线实验 `exp-20260329-008-parquet-entry25-exit20` 的 `portfolio.value()` 与 returns 本身正常，历史失真根因不是该实验退化，而是正式入口脚本缺失、旧/测试工件覆盖真实 attribution JSON；本次补齐 `scripts\attribution\run_strategy_attribution.py` 并隔离 `tests\test_strategy_attribution.py` 的 runtime 输出覆盖。新的指标为：sharpe=0.432405，max_drawdown=-0.162690，alpha=0.030396，beta=0.071152。
- 阻断2修复：已补齐 `src\attribution\factor_attribution.py`、`src\attribution\report_generator.py` 与对应运行脚本，成功生成 `runtime\attribution\factor_attribution\factor_drift_report.json`；摘要：turnover_20d drift_ratio=1.7406 status=healthy，volume_price_divergence drift_ratio=-2.7561 status=failed。
- HTML报告：已重新生成 `runtime\attribution\reports\attribution_report_202603.html`。
- commit：3621382b fix: Phase 5 strategy attribution data fix and factor attribution generation
- push：已推送

BUILDER_DONE

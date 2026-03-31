## Sprint 35 结果
- 修复内容：检查 `scripts/strategy/run_factor_momentum_backtest.py` 后确认问题出在 `max_drawdown = float(abs(drawdown.min()))`，这里把回撤绝对值化了，导致 `strat_factor_momentum_v1` 与其他策略的负值口径不一致。现已改为直接使用 `drawdown.min()`，并同步更新 `runtime/strategy_library/strat_factor_momentum_v1/experiment_run.json` 与 `runtime/strategy_library/strategy_comparison.json` 中对应字段。
- 修复后 max_drawdown 值：`-0.2797143642711561`
- pytest：81 passed
- commit：[待补]
- push：待补
BUILDER_DONE

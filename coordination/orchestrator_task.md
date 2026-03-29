# Commander 任务卡 — Sprint 8
发布时间：2026-03-29
优先级：中

---

## 任务 A：调仓位重跑蒙特卡洛

修改 `scripts/run_monte_carlo.py`：
- 在重建 Portfolio 时把 position_fraction 从 1.0 改为 0.5
- 重跑 1000 次蒙特卡洛模拟
- 更新 `coordination/mc_result.json`
- 打印新的 p(max_dd > 18%)

同时在 `scripts/run_experiment.py` 或 `research_session.py` 里确认 run_experiment 支持传入 position_fraction 参数（如果还没有的话加上）。

目标：p(max_dd > 18%) < 20%。如果仓位 50% 还不够，试 40%，找到刚好通过的最低仓位，记录下来。

## 任务 B：QuantStats 装入 .venv 并生成真实 tearsheet

1. `.venv/Scripts/pip.exe install quantstats`
2. 验证：`.venv/Scripts/python.exe -c "import quantstats; print(quantstats.__version__)"`
3. 重跑 `scripts/generate_tearsheet.py`，生成真实 QuantStats HTML 报告
4. 确认 tearsheet 路径：`runtime/experiments/exp-20260329-008-parquet-entry25-exit20/tearsheet.html`

## 完成后写入 worker_result.md

```
## Sprint 8 结果
### 任务A（仓位调整MC）：position_fraction=X.X，p(max_dd>18%)=X.XXX，是否<20%
### 任务B（QuantStats）：版本=X.X.X，tearsheet路径=...，生成成功/失败
### pytest：XX passed
```

末尾写 `BUILDER_DONE`。

# 2026-03-25 里程碑测试记录

- 时间：2026-03-25
- 实验编号：`exp-20260325-001-trend-following`
- 测试题目：`510300 中期趋势跟随最小验证`
- 为什么这轮重要：首次真实跑通 `草案 -> 规则化 -> 最小回测`，证明项目不再停留在纸面结构
- 规则/变更点：
  - 入场：20MA > 60MA 且收盘上穿 20MA
  - 退出：收盘跌破 60MA
  - 执行：信号收盘生成，次日开盘执行
- 核心结果：
  - annualized_return：0.58%
  - max_drawdown：-22.33%
  - sharpe：0.11
- 复审结论：方向可继续，但首版规则较弱，应进入简单规则迭代
- 晋升判断：`仅记录`
- 原始实验目录：[`runtime/experiments/exp-20260325-001-trend-following`](d:/AITradingSystem/runtime/experiments/exp-20260325-001-trend-following)

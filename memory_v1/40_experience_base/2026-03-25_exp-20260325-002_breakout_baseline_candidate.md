# 2026-03-25 里程碑测试记录

- 时间：2026-03-25
- 实验编号：`exp-20260325-002-breakout-baseline`
- 测试题目：`510300 中期趋势突破最小验证`
- 为什么这轮重要：形成了当前更有希望的临时基线，证明系统已经具备初步筛弱留强能力
- 规则/变更点：
  - 入场：收盘突破前20日最高收盘（不含当天）
  - 退出：收盘跌破前20日最低收盘（不含当天）
  - 执行：信号收盘生成，次日开盘执行
- 核心结果：
  - annualized_return：4.01%
  - max_drawdown：-25.45%
  - sharpe：0.31
- 复审结论：进入首次复审联调，作为当前临时基线继续测试
- 晋升判断：`临时基线`
- 原始实验目录：[`runtime/experiments/exp-20260325-002-breakout-baseline`](d:/AITradingSystem/runtime/experiments/exp-20260325-002-breakout-baseline)

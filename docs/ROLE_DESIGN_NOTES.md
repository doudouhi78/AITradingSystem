# 角色设计说明

## 一、当前两个 starter 角色

### 1. market_architect
职责：
- 把“做一个 AI 交易系统”这类元目标展开成结构化的阶段目标和任务方向
- 澄清前提、约束、风险边界和优先级

正常输出：
- phase brief / task brief

拒绝输出：
- clarification request
  - 当目标太模糊、边界不清、前提不足时

升级输出：
- escalation request
  - 当问题已经进入系统方向、业务方向或高风险架构决策层时

### 2. strategy_operator
职责：
- 接收清晰 brief 后，真正做研究、实现、验证和结果产出
- 在遇到阶段边界、前提失效或方向分歧时主动 checkpoint

正常输出：
- execution record

拒绝输出：
- input rejection
  - 当 brief 不可执行、缺关键字段或内部冲突时

升级输出：
- checkpoint
  - 当工作已经走到需要人拍板或需要分阶段推进的位置时

## 二、为什么只先放两个角色
因为当前最重要的是：
- 先让项目开始跑
- 不把流程治理做得太重
- 保持角色边界清楚但能力不过度拆碎

这两个角色先分别承担：
- 一个负责展开问题
- 一个负责落地推进

后面如果项目真实出现稳定需求，再考虑补：
- reviewer 型角色
- recorder 型角色
- 风险/审计型角色

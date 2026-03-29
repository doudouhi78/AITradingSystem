# Qlib 接入边界 v1

## 目的

只把 Qlib 接成验证工作流引擎，不让它反向定义主对象层。

## 交给 Qlib 的

- 数据研究流程
- 回测工作流
- 记录与分析
- 组合/风险分析报告

## 不交给 Qlib 的

- `ResearchTask`
- `OpportunitySource`
- `FormalReviewRecord`
- `DecisionStatus`
- `StrategyCaseFile`
- 研究题目为什么成立
- 复审为什么通过/不通过

## 当前接法

Qlib 只通过适配层消费这些对象：

- `DatasetSnapshot`
- `RuleExpression`
- 部分 `RiskPositionNote`
- 部分 `ExecutionConstraint`

Qlib 只允许回写这些结果：

- `MetricsSummary`
- 分析工件引用
- 验证证据引用

## 当前不做

- 不让 Qlib 直接写 `DecisionStatus`
- 不让 Qlib 直接写 `FormalReviewRecord`
- 不让前台页面直接读 Qlib 原生对象
- 不让 Qlib 直接成为主对象真相源

## 当前结论

Qlib 可以补：
- 数据研究
- 回测工作流
- 记录与分析

Qlib 不能替代：
- 研究主对象层
- 正式复审链
- 决策链

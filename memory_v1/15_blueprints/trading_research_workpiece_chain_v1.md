# 交易研究工件链 v3

## 一、目的

本文件用于把第二阶段后续开发收敛到“交易研究闭环工件链”本身，而不是继续围绕系统状态建设。

## 二、当前最小研究工件链

当前一条研究应至少形成以下工件：

1. `OpportunitySource`
- 说明研究从哪里来
- 说明市场背景和前人经验
- 说明为什么现在值得研究

2. `ResearchTask`
- 说明这次研究到底要验证什么
- 说明目标、约束、成功标准

3. `RuleExpression`
- 说明规则如何表达
- 说明为什么采用这套表达

4. `DatasetSnapshot`
- 说明验证用了什么数据、什么口径、为什么这么选

5. `DataContractSpec`
- 说明这份数据快照必须满足哪些契约条件
- 说明 Pandera 检查到底在验证什么

6. `ValidationRecord`
- 说明这次验证做了什么
- 说明用了什么方法
- 说明数据契约是否通过
- 说明结果和失败原因是什么

7. `MetricsSummary`
- 说明关键结果是什么
- 说明有哪些关键观察

8. `RiskPositionNote`
- 说明风险与仓位判断
- 说明这种方法承受的代价和边界

9. `ExecutionConstraint`
- 说明现实执行约束
- 说明这个方案对操作者是否友好

10. `FormalReviewRecord`
- 说明一次正式复审到底怎么判断的
- 说明相对基线改了什么、风险是什么、为什么给出这个建议

11. `ReviewOutcome`
- 说明实验当前附带的复审摘要
- 说明缺口、风险和下一步建议

12. `DecisionStatus`
- 说明当前是基线、变体、仅记录还是淘汰

13. `VariantSearchSpec`
- 说明当前基于什么基线、哪些参数空间、什么目标指标在找候选变体

14. `StrategyCaseFile`
- 把同一题材下的研究题目、实验、基线、变体和当前结论串成一条案卷

## 三、当前优先级

当前第一批工件链已经落地到：
- `OpportunitySource`
- `RiskPositionNote` 的解释化扩充
- `ExecutionConstraint`
- `ReviewOutcome` 的方法与理由补全
- `DataContractSpec`
- `ValidationRecord`
- `VariantSearchSpec`

## 四、开发原则

- 先把工件补齐，再谈展示
- 先让每个工件回答“做了什么、为什么这么做、用了什么方法、结果是什么”
- 前台只能围绕这条工件链展开
- 外部组件与未来平台只通过适配层接入，不反向定义工件链

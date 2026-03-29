# 项目对象规范 v4

## 定位

本文件用于说明第二阶段当前主对象层的对象名、字段与关系。

项目对象的源头真相是：
- `src/ai_dev_os/project_objects.py`

本文件只是解释层，不维护第二套平行 schema。

## 当前对象集合

### 研究工件链对象
- `OpportunitySource`
  - 机会来源、市场背景、前人经验引用、为什么现在值得研究
- `ResearchTask`
  - 研究题目、目标、标的池、约束、成功标准
- `RuleExpression`
  - 入场、出场、过滤器、执行假设，以及规则设计理由
- `DatasetSnapshot`
  - 数据版本、来源、区间、口径、成本、缺失值处理，以及为什么选这组数据
- `DataContractSpec`
  - 数据快照的校验契约与验证口径，不让 Pandera 成为真相源
- `MetricsSummary`
  - 核心指标摘要与关键发现
- `ValidationRecord`
  - 一次验证做了什么、为什么这么做、用了什么方法、结果是什么
- `RiskPositionNote`
  - 仓位与风险说明，以及背后的判断
- `ExecutionConstraint`
  - 现实执行约束、流动性、时点与个人适配性说明
- `ReviewOutcome`
  - 实验附带的复审摘要、缺口、下一步建议，以及复审方法/理由
- `FormalReviewRecord`
  - 一次正式复审动作的完整结论，独立于实验摘要对象
- `DecisionStatus`
  - 基线/变体/记录/淘汰等决策状态
- `VariantSearchSpec`
  - 基线、参数空间、搜索约束、目标指标，不让 Optuna 成为变体语义真相源
- `StrategyCaseFile`
  - 把单题材研究串成一条完整案卷的总对象

### 实验对象
- `ExperimentRun`
  - 把以上关键对象挂在同一次实验之下的总对象

## 当前关系约束

- 一个 `ExperimentRun` 必须绑定：
  - 一个 `ResearchTask`
  - 一个 `RuleExpression`
  - 一个 `DatasetSnapshot`
  - 一个 `MetricsSummary`
  - 一个 `RiskPositionNote`
  - 一个 `ReviewOutcome`
  - 一个 `DecisionStatus`
- `OpportunitySource` 与 `ExecutionConstraint` 当前进入 `ExperimentRun` 作为关键补充工件
- `ValidationRecord` 绑定：
  - `DatasetSnapshot`
  - `RuleExpression`
  - `MetricsSummary`
  - `DataContractSpec`
- `FormalReviewRecord` 绑定：
  - 候选实验
  - 基线实验
  - `ValidationRecord`
  - `VariantSearchSpec`
- `VariantSearchSpec` 绑定：
  - 基线实验
  - 参数空间
  - 目标指标
- `StrategyCaseFile` 是更上位的方案案卷对象，用来串联同一研究题材的多轮实验、基线与变体
- 基线/变体关系只通过 `DecisionStatus.is_baseline` 和 `DecisionStatus.baseline_of` 表达

## 当前文件落位

- 实验目录：`runtime/experiments/<experiment_id>/`
- `manifest.json`
  - 顶层索引信息，可包含 `case_file_id`、`search_spec_id`、`validation_record_ids`
- `inputs.json`
  - `research_task` / `opportunity_source` / `rule_expression` / `dataset_snapshot`
- `results.json`
  - `metrics_summary` / `risk_position_note` / `execution_constraint` / `review_outcome` / `decision_status`
- `notes.md`
  - 人工补充说明
- 校验记录：`runtime/validations/<validation_id>.json`

## 当前阶段解释

当前对象层的目标，不是把所有交易主干一次做完，而是先把研究闭环从“只留结果”补到“能解释、能校验、能比较、能搜索候选变体、能正式复审并形成决策状态”。

# 适配层边界定义 v2

## 一、目的

本文件用于固定当前第二阶段的适配层边界，避免主对象层直接被外部组件塑形。

## 二、边界原则

1. 主对象层是真相源，外部组件不是
2. 外部组件只承接能力，不承接业务语义
3. 所有外部结果都必须映射回研究工件链
4. 未来新增平台只能接在适配层后，不得越层直接定义主对象层

## 三、当前四类适配

### 1. 主对象层 -> vectorbt
输入：
- `RuleExpression`
- `DatasetSnapshot`
- 部分 `RiskPositionNote`
- 部分 `ExecutionConstraint`

输出回主对象层：
- `MetricsSummary`
- 关键观察
- 图表与工件路径引用

### 2. 主对象层 -> MLflow
输入：
- `ExperimentRun` 的索引字段
- 基线/变体关系
- 指标摘要
- 决策状态

输出回主对象层：
- 实验比较引用
- 变体筛选证据

### 3. 主对象层 -> Pandera
输入：
- `DatasetSnapshot`
- `DataContractSpec`

输出回主对象层：
- `ValidationRecord`
- 数据契约失败原因
- 数据口径稳定性证据

适配接口：
- `DatasetSnapshot + DataContractSpec -> PanderaResult -> ValidationRecord`

### 4. 主对象层 -> Optuna
输入：
- `VariantSearchSpec`
- 基线实验对象
- 小参数空间约束

输出回主对象层：
- 候选变体集合
- 参数记录
- 目标指标比较结果
- `ExperimentRun + DecisionStatus`

适配接口：
- `VariantSearchSpec + BaselineExperiment -> OptunaTrials -> ExperimentRun + DecisionStatus`

## 四、当前不允许的做法

- 不允许由 vectorbt 结果结构反向定义 `ValidationRecord` 或研究案卷
- 不允许由 MLflow tag/metric 命名反向定义主对象字段
- 不允许由 Pandera schema 直接成为数据契约真相源
- 不允许由 Optuna trial 直接决定什么叫基线/淘汰/继续
- 不允许让前台页面跳过主对象层直接读取底层组件对象
- 不允许未来平台候选直接吞掉机会来源、复审结论、决策状态这类业务语义

## 五、当前阶段解释

当前适配层已经从“预留边界”进入“最小落地边界”：
- Pandera 负责数据契约检查
- Optuna 负责候选变体搜索
- `MLflow` 继续只做实验追踪与比较
- `vectorbt` 继续只做执行与指标产出

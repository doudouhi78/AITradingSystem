# 测试与经验草稿板

## 当前阶段判断

- 第一阶段已完成并正式收口
- 当前处于第二阶段：玻璃盒研究系统建设阶段
- 测试层已分成三部分：
  - 原始实验：`runtime/experiments/`
  - 实验索引：`runtime/system_facts/system_facts.sqlite3`
  - 经验与进展：本目录下的摘要板与里程碑记录

## 当前测试进展

### 2026-03-25 工具层最小接入测试
- 进展：`AkShare` 与 `VectorBT` 已接入项目并完成最小验证
- 当前判断：`VectorBT` 可用；`AkShare` 部分接口受代理影响，需要固定可用路径
- 当前补的主干能力：工具服务层 / 数据入口稳定性
- 相关实验：无独立实验目录，属于工具接入节点

### 2026-03-25 exp-20260325-001-trend-following
- 题目：`510300 中期趋势跟随最小验证`
- 进展：首次跑通 `草案 -> 规则化 -> 最小回测`
- 当前判断：链路跑通，但首版规则较弱
- 当前补的主干能力：最小研究链点火 / 研究表达层可执行性
- 原始留存：[`runtime/experiments/exp-20260325-001-trend-following`](d:/AITradingSystem/runtime/experiments/exp-20260325-001-trend-following)
- 里程碑记录：[`2026-03-25_exp-20260325-001_first_minimal_closed_loop.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-25_exp-20260325-001_first_minimal_closed_loop.md)

### 2026-03-25 exp-20260325-002-breakout-baseline
- 题目：`510300 中期趋势突破最小验证`
- 进展：形成当前更有希望的临时基线
- 当前判断：突破表达明显优于首轮均线确认版
- 当前补的主干能力：研究表达层 / 验证层 / 基线与变体比较机制
- 原始留存：[`runtime/experiments/exp-20260325-002-breakout-baseline`](d:/AITradingSystem/runtime/experiments/exp-20260325-002-breakout-baseline)
- 里程碑记录：[`2026-03-25_exp-20260325-002_breakout_baseline_candidate.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-25_exp-20260325-002_breakout_baseline_candidate.md)

### 2026-03-25 exp-20260325-003-breakout-exit-10d
- 题目：`510300 中期趋势突破 10日退出迭代`
- 进展：完成一次退出敏感性测试
- 当前判断：仅记录，不晋升为新基线
- 当前补的主干能力：验证层 / 变体淘汰机制
- 原始留存：[`runtime/experiments/exp-20260325-003-breakout-exit-10d`](d:/AITradingSystem/runtime/experiments/exp-20260325-003-breakout-exit-10d)


### 2026-03-26 exp-20260326-004-phase2-stack-smoke
- 题目：`第二阶段技术底座联调烟雾测试`
- 进展：完成 Schema-first / 实验留存 / MLflow / MCP / tracing 的一次真实联调
- 当前判断：第二阶段四个技术底座已能协同工作
- 当前补的主干能力：研究操作系统技术底座协同 / 过程与结果双玻璃盒
- 原始留存：[`runtime/experiments/exp-20260326-004-phase2-stack-smoke`](d:/AITradingSystem/runtime/experiments/exp-20260326-004-phase2-stack-smoke)
- 里程碑记录：[`2026-03-26_exp-20260326-004_phase2_stack_smoke.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-26_exp-20260326-004_phase2_stack_smoke.md)

### 2026-03-26 exp-20260326-005-breakout-ma60-filter
- 题目：`510300 突破基线加入单一趋势过滤器联调`
- 进展：在基线突破规则上只增加一个 60 日均线入场过滤器，完成真实变体留存与回读
- 当前判断：过滤器暂不优于当前基线，建议仅记录
- 当前补的主干能力：运行态三工位按新技术底座进行真实变体联调
- 原始留存：[`runtime/experiments/exp-20260326-005-breakout-ma60-filter`](d:/AITradingSystem/runtime/experiments/exp-20260326-005-breakout-ma60-filter)
- 里程碑记录：[`2026-03-26_exp-20260326-005_breakout_ma60_filter.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-26_exp-20260326-005_breakout_ma60_filter.md)


### 2026-03-26 研究驾驶舱 v0 人工浏览验证
- 题目：`本地只读驾驶舱四页人工浏览验证`
- 进展：首页、实验中心、研究链详情、流转与问题页均可访问并正确取数
- 当前判断：驾驶舱 v0 已达到可读可用；已修正首页阶段卡片误读问题
- 当前补的主干能力：研究操作系统观察层 / 人工可读性验证
- 发现问题：实验中心尚缺筛选器与基线/变体关系卡；研究链详情仍偏原始 JSON 展示；流转页尚未提供 trace 详情交互
- 相关接口：`/api/v1/overview`、`/api/v1/experiments`、`/api/v1/experiments/{experiment_id}`、`/api/v1/flow`

## 当前经验结论

- 系统已经能重复跑通小闭环，主干链路可点火
- 第二阶段四个技术底座已完成一次真实联调，后续可以转入“让运行态三工位按新底座工作”的验证
- 当前更值得调的是入场过滤器，不是继续收紧退出
- `fund_etf_hist_sina` 是当前更稳的 ETF 历史数据入口
- 当前人工基线已切换为：`exp-20260328-007-manual-entry25-exit20`
- 当前明确结论：简单、可解释的人工变体已经优于自动搜索候选，后续应优先补风险/仓位与样本外验证，而不是继续无约束搜参
- 当前执行口径已补：该人工基线先按半仓上限（50%）理解，回撤达到12%进入复核，达到18%暂停执行
- 当前样本外结论：后半区间与跨标的验证都已通过，这条人工基线暂时站得住
- 当前下一步：补执行敏感性，确认半仓、分批与成本扰动会不会改变结论


## 过程可观测补链

- `run-20260325-001`：补通首条最小闭环的过程轨迹，证明主链不仅有结果，也可回看步骤。
- `run-20260325-002`：补通基线候选的过程轨迹，说明当前临时基线具备任务->规则->回测->复审的完整玻璃盒过程。
- `run-20260325-003`：补通退出变体的过程轨迹，说明“记录但不晋升”的判断也能落到过程层，而不只停留在结果层。
### 2026-03-28 Pandera / Optuna 共享契约接入
- 题目：`基于 exp-20260325-002-breakout-baseline 的数据契约校验与小参数空间候选搜索`
- 进展：新增 `DataContractSpec / ValidationRecord / VariantSearchSpec`，跑通 1 条真实 Pandera 校验记录与 1 个真实 Optuna 候选实验
- 当前判断：二阶段主对象层已经具备稳定的数据契约、验证记录与候选变体搜索入口
- 当前补的主干能力：数据契约层 / 验证口径稳定化 / 候选变体探索入口
- 校验记录：[`VAL-20260328-001.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-001.json)
- 原始留存：[`exp-20260328-006-optuna-candidate`](d:/AITradingSystem/runtime/experiments/exp-20260328-006-optuna-candidate)
- 里程碑记录：[`2026-03-28_exp-20260328-006_optuna_candidate.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-006_optuna_candidate.md)

- 2026-03-28 补完统一读取层：ValidationRecord 与 VariantSearchSpec 已可经由 project_mcp / tool_bus 回读，说明 Pandera / Optuna 结果不再只是底层文件产物，而开始进入后续复审与前台可消费对象。

### 2026-03-28 exp-20260328-006-optuna-candidate 正式复审
- 题目：`Optuna 候选变体正式复审`
- 进展：基于基线、校验记录和搜索规范完成轻量单审
- 当前判断：该候选改善了 Sharpe 与回撤，但收益略降，先保留为正式候选，不晋升基线
- 当前补的主干能力：正式复审链 / 候选到决策状态闭合
- 正式复审：[`REV-20260328-001.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-001.json)
- 原始留存：[`exp-20260328-006-optuna-candidate`](d:/AITradingSystem/runtime/experiments/exp-20260328-006-optuna-candidate)
- 里程碑记录：[`2026-03-28_exp-20260328-006_formal_review.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-006_formal_review.md)

### 2026-03-28 人工变体对照：新人工基线形成
- 题目：`510300 基线 vs 人工变体 vs 搜索候选正式对照`
- 进展：完成两个人工单变量变体，并与旧基线和 Optuna 候选同口径比较
- 当前判断：`exp-20260328-007-manual-entry25-exit20` 已成为当前最强且最可解释的新基线；`exp-20260328-008-manual-entry20-exit10` 仅作反例记录
- 当前补的主干能力：人工研究对照 / 基线切换机制 / 防止系统滑向纯自动调参
- 原始留存：[`exp-20260328-007-manual-entry25-exit20`](d:/AITradingSystem/runtime/experiments/exp-20260328-007-manual-entry25-exit20)、[`exp-20260328-008-manual-entry20-exit10`](d:/AITradingSystem/runtime/experiments/exp-20260328-008-manual-entry20-exit10)
- 正式复审：[`REV-20260328-002.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-002.json)、[`REV-20260328-003.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-003.json)
- 人工总结：[`2026-03-28_manual_variant_comparison_summary.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_manual_variant_comparison_summary.md)
- 里程碑记录：[`2026-03-28_exp-20260328-007-manual-entry25-exit20_manual_variant.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-007-manual-entry25-exit20_manual_variant.md)、[`2026-03-28_exp-20260328-008-manual-entry20-exit10_manual_variant.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-008-manual-entry20-exit10_manual_variant.md)


### 2026-03-28 exp-20260328-007-manual-entry25-exit20 风险/仓位补链
- 题目：`新人工基线风险/仓位评估`
- 进展：为当前人工基线补上第一版标准化风险/仓位口径
- 当前判断：该基线虽然结果更优，但最大回撤仍深，因此当前只按半仓上限理解，不再按满仓口径解释
- 当前补的主干能力：风险与仓位层 / 从更优结果推进到更可执行候选
- 原始留存：[`exp-20260328-007-manual-entry25-exit20`](d:/AITradingSystem/runtime/experiments/exp-20260328-007-manual-entry25-exit20)
- 相关总结：[`2026-03-28_manual_variant_comparison_summary.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_manual_variant_comparison_summary.md)

### 2026-03-28 exp-20260328-007-manual-entry25-exit20 样本外验证
- 题目：`当前人工基线后半区间样本外验证`
- 进展：固定参数不变，切出 `2024-01-02` 到 `2026-03-24` 作为后半区间独立验证
- 当前判断：该人工基线在样本外区间没有塌掉，Sharpe 提升到 `0.656997`，最大回撤收敛到 `-0.201307`，可继续保留为当前基线
- 当前补的主干能力：样本外验证 / 基线保真度检查
- 校验记录：[`VAL-20260328-002.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-002.json)
- 正式复审：[`REV-20260328-004.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-004.json)
- 里程碑记录：[`2026-03-28_exp-20260328-007_out_of_sample_validation.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-007_out_of_sample_validation.md)

### 2026-03-28 exp-20260328-007-manual-entry25-exit20 跨标的验证
- 题目：`当前人工基线跨标的样本外验证`
- 进展：固定同一组参数，在 `510500` 与 `159915` 的同期样本外区间做独立验证
- 当前判断：这条人工基线不只在 `510300` 上成立，在 `510500` 和 `159915` 上也维持了正收益和较好的 Sharpe，可继续保留为当前基线
- 当前补的主干能力：跨标的迁移性验证 / 基线稳固度检查
- 校验记录：[`VAL-20260328-003.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-003.json)、[`VAL-20260328-004.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-004.json)
- 正式复审：[`REV-20260328-005.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-005.json)
- 里程碑记录：[`2026-03-28_exp-20260328-007_cross_instrument_validation.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-007_cross_instrument_validation.md)

### 2026-03-28 exp-20260328-007-manual-entry25-exit20 执行敏感性验证
- 题目：`当前人工基线执行敏感性验证`
- 进展：固定规则与样本外区间不变，补做半仓同成本与半仓高成本两种执行场景
- 当前判断：该人工基线在半仓和更差成本假设下仍保持正收益与可接受 Sharpe，执行敏感性没有直接推翻当前基线结论
- 当前补的主干能力：执行敏感性验证 / 仓位与成本扰动检查
- 校验记录：[`VAL-20260328-005.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-005.json)、[`VAL-20260328-006.json`](d:/AITradingSystem/runtime/validations/VAL-20260328-006.json)
- 正式复审：[`REV-20260328-006.json`](d:/AITradingSystem/runtime/reviews/REV-20260328-006.json)
- 里程碑记录：[`2026-03-28_exp-20260328-007_execution_sensitivity.md`](d:/AITradingSystem/memory_v1/40_experience_base/2026-03-28_exp-20260328-007_execution_sensitivity.md)

# Builder Handover v2

## 1) 项目概况
AITradingSystem 当前是一个以“标准入 -> 内层验证 -> 标准出”为主线的量化研究工程。核心技术栈是 Python 3.12 + 本地 Parquet 数据层 + Pandera 数据质检 + AlphaLens/Optuna/VectorBT 等研究工具，当前阶段已完成 Alpha Phase 1 收口和 Phase 2 基础设施搭建。

## 2) 已完成工程
- `2ece8894` Initial commit: AITradingSystem Phase 1+2 complete — 建立项目基础骨架与早期 Phase 1/2 工程。
- `ae3dee24` Phase 4 partial: limit constraint validation (WI-403) — 增加涨跌停约束验证能力。
- `9d460a63` Fix: install hikyuu in .venv, re-run cross-validation and limit constraint with real Hikyuu — 修复 Hikyuu 环境并重跑真实脚本。
- `a1f5198e` Phase 5: paper trading infrastructure - signal_daily.py, log_paper_trade.py, trade log format — 建好纸交易信号与日志格式。
- `ab10c73c` Sprint 8: position sizing MC (50% -> p=0.5%), QuantStats tearsheet fixed — 完成仓位调整 Monte Carlo 与 QuantStats 修正。
- `cebfde33` Phase 2: Gate system, R-based position sizing, slippage correction, strategy dashboard — 建立 Gate、仓位、滑点与 dashboard 主体。
- `11504508` Sprint 14: Phase 2 forward simulation — gate_blocked=0, max_dd -2.1%→-1.63%, avg_pos 17.2% — 完成前向模拟一轮收口。
- `4a512e77` Add CLAUDE_MEMORY_BOOTSTRAP.md — cross-account memory wake-up guide — 添加跨账户记忆启动说明。
- `d53fd64e` Phase 1 display completion and Phase 2 closure fixes — 做过展示层收口和 Phase 2 闭环修补。
- `a5052fb1` Sprint 17: Alpha Phase 1 pipeline - momentum IC baseline — 建立 Alpha Phase 1 基线：ETF Top10 + momentum_20d + AlphaLens。
- `c23535a8` Sprint 18: Alpha Phase 1 complete - multi-factor x multi-universe IC — 补完 ETF/股票双 universe，多因子 IC 评估与结果留档。
- `ae0a5fac` Sprint 19: Alpha Phase 2 infrastructure - batch IC pipeline — 建立批量 IC、筛选、去重、registry 基础设施。

## 3) 当前代码结构（关键路径）
### src/alpha_research
- `src/alpha_research/data_loader.py` — 读取 ETF/股票 Parquet，选流动性 TopN，供 Alpha 研究使用。
- `src/alpha_research/factors/price_momentum.py` — 动量/反转因子族（5/10/20/60 日、1 日反转）。
- `src/alpha_research/factors/volume_liquidity.py` — `turnover_20d`，当前用 amount 近似换手活跃度。
- `src/alpha_research/factors/fundamental.py` — PB 接口探测与 `pb_ratio_approx` 近似实现。
- `src/alpha_research/factors/technical.py` — RSI14、ATR14 归一化因子。
- `src/alpha_research/factors/sentiment.py` — 北向/融资占位接口，尚未实现。
- `src/alpha_research/evaluation/ic_pipeline.py` — 单因子 IC/ICIR/半衰期评估与批量评估。
- `src/alpha_research/evaluation/screening.py` — 三关筛选规则（IC 均值、ICIR、半衰期）。
- `src/alpha_research/evaluation/correlation.py` — Spearman 相关去重，保留 ICIR 更高者。
- `src/alpha_research/registry/factor_registry.json` — 有效因子库，当前为空列表。

### src/ai_dev_os
- `src/ai_dev_os/project_objects.py` — 主对象层（ResearchTask / ExperimentRun / ReviewOutcome / DecisionStatus 等）。
- `src/ai_dev_os/experiment_store.py` — 实验留存。
- `src/ai_dev_os/validation_store.py` — 验证记录留存。
- `src/ai_dev_os/review_store.py` — 正式复审记录留存。
- `src/ai_dev_os/project_mcp.py` — 统一读取层。
- `src/ai_dev_os/tool_bus.py` — 工具总线读取接口。
- `src/ai_dev_os/market_data_v1.py` — V1 数据层拉取/标准化/Parquet 落地。
- `src/ai_dev_os/market_data_quality.py` — Pandera 数据质检入口。
- `src/ai_dev_os/qlib_adapter.py` — Qlib 适配骨架。
- `src/ai_dev_os/etf_breakout_runtime.py` — ETF breakout 运行时。
- `src/ai_dev_os/gate/` — breadth/trend/vol/drawdown gate 模块。
- `src/ai_dev_os/risk/` — ATR 止损、仓位 sizing、风险配置。

### scripts/alpha
- `scripts/alpha/run_phase1_ic.py` — Phase 1：ETF Top10 + 股票 Top50 的多因子 IC 评估脚本。
- `scripts/alpha/run_ic_batch.py` — Phase 2：批量因子评估流水线。
- `scripts/alpha/run_screening.py` — 读取 Phase 2 结果并输出筛选摘要。

## 4) 运行环境
- Python 环境：`D:\AITradingSystem\.venv\Scripts\python.exe`
- 核心依赖版本：Python 3.12.0 / pandas 2.3.3 / pandera 0.30.1 / optuna 4.8.0 / yfinance 1.2.0 / alphalens-reloaded 0.4.6 / pandas-ta 0.4.71b0 / akshare 1.18.46
- 中国 ETF Parquet：`runtime/market_data/cn_etf/`
- 中国股票 Parquet：`runtime/market_data/cn_stock/`
- 美股 ETF Parquet：`runtime/market_data/us_etf/`
- 美股股票 Parquet：`runtime/market_data/us_stock/`
- 数据质检结果：`runtime/market_data/quality/`
- Alpha Phase 1 结果：`runtime/alpha_research/phase1/`
- Alpha Phase 2 结果：`runtime/alpha_research/phase2/ic_batch_result.json`

### 标准运行命令
```powershell
$env:PYTHONPATH='D:\AITradingSystem\src'
.\.venv\Scripts\python.exe .\scripts\alpha\run_phase1_ic.py
.\.venv\Scripts\python.exe .\scripts\alpha\run_ic_batch.py
.\.venv\Scripts\python.exe .\scripts\alpha\run_screening.py
.\.venv\Scripts\python.exe .\scripts\validate_market_data.py
.\.venv\Scripts\python.exe -m pytest -q
```

## 5) 当前 Alpha 研究状态
### Phase 1
- Universe：`etf_top10` + `stock_top50`
- 因子：`momentum_20d`、`turnover_20d`、`pb_ratio`（股票近似版，ETF 跳过）
- 结论：
  - ETF `turnover_20d` 在 10D/20D IC 均值较高，但仍未达到 Phase 2 筛选阈值；目前只能算“相对最强”，不能入库。
  - ETF `momentum_20d` 无效。
  - 股票 `momentum_20d` 无效。
  - 股票 `turnover_20d` 无效。
  - 股票 `pb_ratio_approx` 无效。
- 结果文件：`runtime/alpha_research/phase1/factor_log.json`

### Phase 2
- 基础设施已建好：`ic_pipeline / screening / correlation / run_ic_batch / run_screening`
- 已跑现有 7 个因子 × 2 universe，共 14 次评估，0 个通过筛选，0 个运行错误。
- `factor_registry.json` 当前为空，需要下一步继续扩因子并挖掘通过项。

### Phase 3
- 尚未开始。

## 6) 已知问题和注意事项
- `pb_ratio` 个股日频接口不可直接用：
  - `stock_a_all_pb`：只有全市场 PB 时间序列，不是个股横截面 PB。
  - `stock_financial_analysis_indicator`：当前探测报 `ValueError('No tables found')`（部分环境/个股会波动）。
  - `stock_financial_analysis_indicator_em`：当前探测报 `TypeError("'NoneType' object is not subscriptable")`。
- `src/alpha_research/factors/sentiment.py` 只是占位，北向/融资数据接口待接入。
- AlphaLens 运行会打印 warning：
  - `FigureCanvasAgg is non-interactive`
  - `pd.Grouper(freq='M')` 弃用提示
  - `pct_change` 默认 `fill_method='pad'` 弃用提示
  这些目前不影响 JSON 结果生成。
- `.gitignore` 覆盖了 `runtime/`，提交运行产物必须用 `git add -f runtime/...`。
- 当前工作树通常混有协调文件和 Claude 侧文件；提交时需要只挑本轮目标文件，不要 `git add -A`。

## 7) Sprint 20 任务预告
下一步是因子挖掘：继续扩充候选因子，目标让 `factor_registry.json` 达到 `>= 15` 个通过筛选的因子。优先方向：量价背离、52 周高点、ROE 变化、波动率压缩、估值/质量混合因子，以及后续 sentiment 接口落地。

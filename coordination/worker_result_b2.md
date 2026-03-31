## Sprint 23 B2 结果（Module C + Module D）

### Module C：季报财务数据（公告日对齐）
- 新增：`src\data_pipeline\fundamental_loader.py`
- 能力：`build_financial_quarterly()` / `get_latest_financial()`
- 数据结果：已生成 `runtime\fundamental_data\financial_quarterly\` 下 300 个沪深300标的 parquet
- 失败记录：`runtime\fundamental_data\failed_financial.json`，当前 0 失败
- 口径：优先尝试新浪 `stock_financial_analysis_indicator()`，为空时自动回退东方财富 `stock_financial_analysis_indicator_em()`；严格按 `announce_date` 过滤，无前向偏差
- 字段：`report_date / announce_date / roe / roa / gross_margin / net_margin / debt_ratio / eps`

### Module D：另类数据
- 新增：`src\data_pipeline\alternative_loader.py`
- 能力：`build_northbound_flow()` / `build_margin_balance()`
- 产物：
  - `runtime\alternative_data\northbound_flow.parquet`
  - `runtime\alternative_data\margin_balance.parquet`
- 时间范围：
  - northbound 起始 `2014-11-17`
  - margin 起始 `2010-03-31`

### 测试
- 新增：`tests\test_financial_data.py`
- 新增：`tests\test_alternative_data.py`
- 执行结果：
  - `python -m pytest tests\test_financial_data.py -q` → `3 passed`
  - `python -m pytest tests\test_alternative_data.py -q` → `3 passed`

### 风险与说明
- 任务卡指定的 `stock_margin_sz_summary_em/stock_margin_sh_summary_em` 与当前 AkShare 版本不一致，改用真实可用接口 `macro_china_market_margin_sz/sh`
- 任务卡指定的 `stock_hsgt_north_money_em()` 当前 AkShare 不存在，改用 `stock_hsgt_hist_em()` + `stock_hsgt_fund_flow_summary_em()` 组合实现
- 新浪财务接口对部分标的返回空表，因此加入东方财富回退；优先级仍保持“先尝试任务卡指定接口”

### 提交
- commit message：`feat: Phase 4 Module C+D - financial quarterly and alternative data`

BUILDER_DONE

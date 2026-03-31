# 数据工程师结果卡 — Sprint 24-DE

## 调研结论

本次先调研了三类数据源：

1. AkShare 全市场接口
- 当前环境存在 `stock_a_all_pb` / `stock_market_pe_lg` / `stock_market_pb_lg`
- 这些接口只有全市场聚合估值，不提供 `instrument_code` 维度历史 PE/PB/PS
- 不能满足 `valuation_daily.parquet` 目标 schema

2. AkShare 单股历史接口
- 任务卡候选的 `stock_a_lg_indicator` 在当前安装版本中不存在
- 旧版 `build_valuation_daily()` 是用新浪快照反推历史估值，属于临时近似，不适合作为正式数据层方案

3. baostock
- 已成功安装、登录并实测单股查询
- `query_history_k_data_plus` 可直接返回历史 `peTTM / pbMRQ / psTTM`
- 与目标 schema 最匹配，且数据口径比快照反推更正式

### 最终选型
选择 **baostock** 作为正式数据源，实现真实单股历史估值数据拉取。

---

## 本次完成

- 在 `src\data_pipeline\fundamental_loader.py` 中将 `build_valuation_daily()` 重构为 baostock 正式实现
- 为避免单线程全量耗时过长，改为 **分批并行拉取**：每个进程一次登录，一批标的共享连接
- 新增 `tests\test_valuation_data.py`
- 读取 `runtime\classification_data\stock_meta.parquet`，与 `runtime\market_data\cn_stock` 交集生成标的池
- 失败标的写入 `runtime\fundamental_data\failed_valuation.json`
- 生成 `runtime\fundamental_data\valuation_daily.parquet`

---

## 验证结果

- 覆盖标的数：`1138`
- 时间范围：`2015-01-05` 至 `2026-03-30`
- PB 中位数：`2.672726`
- 失败标的数：`528`
- pytest：`tests/test_valuation_data.py` 3/3 通过

---

## 风险与说明

- 失败标的主要是当前 `stock_meta` 与 `market_data` 中存在但 baostock 无可用历史估值返回的股票，已记录到 `failed_valuation.json`
- 当前版本已满足任务卡硬指标：覆盖 > 1000、时间范围覆盖 2015 起、PB 分布合理、测试通过
- 本次未提交大 parquet 文件到 git，数据保留在 `runtime\fundamental_data\` 本地

BUILDER_DONE

# Alpha101 Knowledge Base

本目录维护 Alpha101（Kakushadze 2015《101 Formulaic Alphas》）的结构化知识库，用于后续因子实现、依赖梳理与可计算性评估。

## 文件

- `alpha101_library.json`：101 个因子的标准结构化清单。

## JSON 字段

- `id`：固定格式 `alpha001` 到 `alpha101`。
- `index`：论文中的因子编号。
- `category`：因子主类别。
  - `price_volume`：以价格、成交量、相关性、排名关系为主。
  - `momentum`：以趋势延续、正向价格变化为主。
  - `reversal`：以均值回归、反转逻辑为主。
  - `volatility`：以波动率或标准差特征为主。
  - `fundamental_proxy`：依赖市值或行业中性化等扩展字段。
- `formula_original`：论文 Appendix A 中的原始公式文本，保留原操作符与函数命名。
- `data_fields`：该因子直接依赖的数据字段，枚举自：`open`, `high`, `low`, `close`, `volume`, `amount`, `vwap`, `returns`, `cap`。
- `complexity`：实现复杂度分级。
  - `1`：简单表达式，依赖较少。
  - `2`：中等复杂度，包含时间窗口、排名、相关性等常见嵌套。
  - `3`：复杂表达式，含多层嵌套、条件分支、行业中性化或超长链式算子。
- `expected_direction`：预期方向，当前统一标记为 `unknown`，后续可在实现与回测阶段补充。
- `ashare_notes`：A 股落地提示，用于记录 VWAP、行业分类、市值口径等额外依赖。
- `status`：当前可计算状态。
  - `ready_to_run`：仅依赖 OHLCV 及其直接衍生量（如 returns、adv），当前数据层即可实现。
  - `pending_valuation`：需要市值等估值/规模字段后才能稳定实现。
  - `pending_alternative`：需要 VWAP、成交额推导、行业分类映射或替代实现方案。

## 说明

- 本知识库只做结构化整理，不包含任何因子计算代码。
- `formula_original` 取自论文文本抽取结果，并做了换行归并，便于 JSON 存储。
- `data_fields` 与 `status` 的划分以当前项目的 A 股数据可用性为导向，不等同于论文原始数据接口定义。

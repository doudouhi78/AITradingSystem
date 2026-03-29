## Sprint 5 结果
### 任务A（DraftCardV1）：
- 字段清单：card_id, title, instrument, strategy_family, hypothesis, entry_rule, exit_rule, key_params, created_at
- 是否新增成功：是

### 任务B（草稿卡示例）：
- 策略名称：510300 双均线交叉草稿
- hypothesis内容：当宽基 ETF 进入中期趋势阶段时，短周期均线会先于长周期均线拐头并形成延续。双均线交叉能用简单规则捕捉趋势开始，并在趋势减弱时退出。

### 任务C（验证链）：
- 代码行数：18
- experiment_id：exp-20260329-009-ma10-ma30-cross
- Sharpe：0.156
- Max Drawdown：-0.412
- Trade Count：46
- 能否跑通：能

### pytest：49 passed

BUILDER_DONE

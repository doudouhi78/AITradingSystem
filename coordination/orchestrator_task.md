# Commander 任务卡 — Sprint 7（Phase 5 启动）
发布时间：2026-03-29
优先级：高

---

## 背景

Phase 5 仿真交易正式启动。目标：每日收盘后自动生成信号，记录纸交易日志，连续运行 4 周。
本 Sprint 完成基础设施搭建（WI-501/502），为每日运营做好准备。

基线策略：entry_window=25, exit_window=20，标的 510300。
数据来源：本地 Parquet（`data/market/cn_etf/510300.parquet`）。

**测试原则：全部步骤完成后跑一次 pytest，不要中途跑。**

---

## 任务 A：日信号生成脚本（WI-501）

写 `scripts/signal_daily.py`：

1. 读取本地 510300 Parquet 数据，取截止今日的最新数据
2. 计算 entry_window=25 的最高价通道和 exit_window=20 的最低价通道
3. 判断信号：
   - 今日收盘价 > 过去25日最高价 → **BUY**
   - 今日收盘价 < 过去20日最低价 → **SELL**
   - 否则 → **HOLD**
4. 输出格式：
   ```
   日期: 2026-03-29
   信号: HOLD
   依据: 收盘价=XXXX，25日高点=XXXX，20日低点=XXXX
   建议执行价: 次日开盘价（需人工填入）
   ```
5. 同时把信号写入 `runtime/paper_trading/signals/YYYYMMDD.json`：
   ```json
   {
     "date": "2026-03-29",
     "signal": "HOLD",
     "close": XXXX,
     "entry_threshold": XXXX,
     "exit_threshold": XXXX,
     "rationale": "..."
   }
   ```

**注意**：如果本地 Parquet 数据不含今日数据（数据截止较早），脚本应能正常运行并给出基于最新可用数据的信号，不报错。

---

## 任务 B：纸交易日志格式（WI-502）

写 `scripts/log_paper_trade.py`：

1. 接收参数：日期、信号、假设成交价（次日开盘价，手动输入）
2. 追加记录到 `runtime/paper_trading/trade_log.csv`：

   | date | signal | assumed_price | actual_open | slippage | position | notes |
   |------|--------|---------------|-------------|----------|----------|-------|

3. 写一个 `runtime/paper_trading/trade_log_format.md` 说明每个字段含义

**同时**创建 `runtime/paper_trading/` 目录结构（signals/ 子目录）。

---

## 任务 C：运行验证

1. 运行 `scripts/signal_daily.py`，生成今日（2026-03-29）信号文件
2. 打印输出信号内容
3. 跑 pytest

---

## 完成后写入 worker_result.md

```
## Sprint 7 结果
### 任务A（signal_daily.py）：今日信号=BUY/SELL/HOLD，信号文件路径=...
### 任务B（log_paper_trade.py）：日志格式就绪，目录结构=...
### 任务C（验证）：信号生成成功/失败
### pytest：XX passed
```

末尾写 `BUILDER_DONE`。

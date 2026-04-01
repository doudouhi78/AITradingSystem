## Sprint 35 结果
- 批次1 元数据：stock_basic 5819只（含退市/暂停上市），ST记录34002条，停牌53599条，涨跌停134264条（当前仅拉到 2023-06-21）
- 批次2 日线行情：未执行；受 Tushare 原生接口限频 40203 和 IP 限制 40204 阻断，转接API持续403
- 批次3 财务三表：未执行；保持批次顺序，等待批次2通路恢复
- 批次4 股权结构：未执行；保持批次顺序，等待批次2通路恢复
- 批次5 另类数据：未执行；保持批次顺序，等待批次2通路恢复
- 批次6 指数：未执行；保持批次顺序，等待批次2通路恢复
- 代码交付：新增 src\data_pipeline\tushare_downloader.py、scripts\download_all_data.py、tests\test_tushare_downloader.py；requirements.txt 增加 tushare
- 已验证：pytest tests\test_tushare_downloader.py -q => 2 passed；python -m py_compile 通过；python scripts\download_all_data.py --batch 1 已能启动并写出批次1前四项数据
- 外部通路状态：转接API https://ai-tool.indevs.in/tushare/pro/{api_name} 持续 403 Forbidden；原生 Tushare 可用但对高频长批次触发 40203（每分钟100次）与 40204（IP数量超限）
- 失败汇总：batch1 历史 limit_list 存在大量失败日，详见 runtime\download_log\failed_batch1.json；progress 详见 runtime\download_log\progress.json
- commit：未提交；当前结果为部分执行状态，不适合伪装成 Sprint 35 完整交付
- push：未执行

DATA_ENGINEER_DONE

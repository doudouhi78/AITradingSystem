## Sprint 51 结果
- Tushare 数据规模：5735只股票 / 覆盖 2015-01-05 ~ 2026-04-01（fundamental_data 仅提供元数据与日历，实际 OHLCV 来自 runtime/market_data/cn_stock）
- 训练集样本量：5146834条 / 验证集：1176143条 / 测试集：2547372条
- epochs：30（实际运行10，early stop） / batch_size：2048
- 训练耗时：8.94分钟（数据转换0.54分 + 特征工程2.28分 + 训练6.12分）
- 峰值显存：0.048 GB / 16 GB
- ICIR（测试集2023-2024）：0.7667（对比 cn_data=0.655）
- IC均值：0.0705 / IC_std：0.0919
- pytest：163p/0f/0s
- commit：5e9cbec0
- 结论：模型有信号
BUILDER_DONE

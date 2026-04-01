## Sprint 36b 结果
- neutralization.py：完成
- 分层收益模块：完成
- run_factor_evaluation.py：完成
- 现有6个因子重新评估：factor_margin_balance_change_5d（raw_icir=0.322742，industry_neutral_icir=null，size_neutral_icir=null，is_monotonic=false）；factor_northbound_flow_5d（raw_icir=-0.218685，industry_neutral_icir=null，size_neutral_icir=null，is_monotonic=false）；factor_pb_ratio（raw_icir=0.080409，industry_neutral_icir=0.102852，size_neutral_icir=0.061516，is_monotonic=false）；factor_pe_ttm（raw_icir=0.071551，industry_neutral_icir=0.197859，size_neutral_icir=0.053307，is_monotonic=false）；factor_turnover_20d（raw_icir=0.000000，industry_neutral_icir=0.000000，size_neutral_icir=0.000000，is_monotonic=true）；factor_volume_price_divergence（raw_icir=0.000000，industry_neutral_icir=0.000000，size_neutral_icir=0.000000，is_monotonic=true）
- factor_reports/ 目录：已生成6个报告文件
- pytest：74 passed, 0 failed, 7 skipped
- commit：cca9a2fb feat: Prepare-C - factor evaluation protocol upgrade
- push：已推送
- 遗留问题：stock/valuation 模块原先对 baostock 和 pandera 是硬依赖；本次已改为可选导入/懒加载，避免在当前环境缺包时阻断 pytest collect。行业/市值中性化目前仅对截面因子有效，market-level 因子返回 null 属预期。

BUILDER_DONE

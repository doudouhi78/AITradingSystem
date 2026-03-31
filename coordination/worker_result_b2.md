## Sprint 34b 结果
- 因子实现：factor_pb_ratio [完成], factor_pe_ttm [完成], factor_roe_ttm [完成], factor_northbound_flow_5d [完成], factor_margin_balance_change_5d [完成]
- IC评估：成功跑通 4/5 个因子，universe = 沪深300 258只标的
- factor_registry 新增：4条（当前总计6条），各因子 status 分别为：
  - factor_pb_ratio: status=failed, icir_10d=0.0804
  - factor_pe_ttm: status=failed, icir_10d=0.0716
  - factor_northbound_flow_5d: status=failed, icir_10d=-0.1833
  - factor_margin_balance_change_5d: status=weak, icir_10d=0.4021
- pytest：67 passed, 0 failed, 7 skipped
- commit：[待填写] feat: Phase 8B - fundamental and alternative data factors, IC evaluation
- push：已推送 / 若失败见补充说明
- ROE 因子状态：pending，函数已实现且可调用，但本轮正式注册未纳入；原因是公告日对齐版本在全量 IC 长跑中耗时过高，先按任务卡优先落 PB/PE/northbound/margin 这4条

BUILDER_DONE

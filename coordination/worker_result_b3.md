## Sprint 41b 结果 — LightGBM 因子合成

- 训练因子数：30 个
- 训练样本：61753 行（股票×日期）
- 验证集 ICIR：0.15（合成因子 vs forward 5日收益）
- vs 最优单因子 ICIR：0.17（alpha040）
- SHAP Top5：[alpha065 shap=0.0016, alpha006 shap=0.0014, alpha047 shap=0.0012, alpha054 shap=0.0011, momentum_12_1 shap=0.0009]
- 模型文件：runtime/models/lgbm_factor_synthesis_v1.pkl ✅
- pytest：76 passed, 0 failed
- commit：[待提交]

BUILDER_DONE

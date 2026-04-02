## Sprint 44c 结果 — 因子选股回测

| 因子 | 样本内Sharpe | 样本外Sharpe | 样本外年化 | 样本外最大回撤 |
|------|------------|------------|----------|--------------|
| alpha004 | -0.18 | 1.66 | 10.0% | -6.2% |
| alpha042 | 0.46 | 0.39 | 1.5% | -5.2% |
| alpha027 | -0.60 | 0.34 | 1.4% | -5.4% |
| alpha071 | -0.77 | 0.43 | 2.2% | -5.7% |
| alpha061 | -1.13 | 0.99 | 6.7% | -9.3% |
| lgbm_synthetic | 0.99 | 0.62 | 3.1% | -5.5% |
| pysr_formula_3 | 0.92 | 0.20 | 1.4% | -18.1% |

- 最优因子：[alpha004] 样本外Sharpe=1.66
- factor_backtest_report.json：✅
- pytest：119 passed, 0 failed
- commit：[e43295c6] feat: add factor long-short backtest report

BUILDER_DONE


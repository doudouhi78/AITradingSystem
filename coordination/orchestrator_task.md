# Commander 任务卡 — 修复 Hikyuu 环境
发布时间：2026-03-29

## 背景

Hikyuu 2.7.7 安装在系统 Python（C:\Users\Administrator\AppData\Local\Programs\Python\Python312），
但项目 .venv 里没有，导致脚本调用失败。

## 任务

1. 用 .venv 的 pip 安装 hikyuu：
   `.venv/Scripts/pip.exe install hikyuu`

2. 验证安装成功：
   `.venv/Scripts/python.exe -c "import hikyuu; print(hikyuu.__version__)"`

3. 重跑 `scripts/phase1_hikyuu_cross_validate.py`，确认 Hikyuu 真实调用（非兜底），
   把结果更新到 `coordination/phase1_hikyuu_cross_validate_result.json`

4. 重跑 `scripts/validate_limit_constraint.py`，用真实 Hikyuu 替换兜底实现，
   更新 `coordination/phase4_limit_constraint_result.json`

5. 跑一次 pytest

## 完成后写入 worker_result.md

```
## Hikyuu 环境修复结果
- pip install hikyuu：成功/失败
- hikyuu 版本：X.X.X
- phase1 交叉验证重跑：Sharpe gap = X.XXX
- phase4 涨跌停约束重跑：VBT成交=N，Hikyuu成交=N（真实Hikyuu）
- pytest：XX passed
```

末尾写 `BUILDER_DONE`。

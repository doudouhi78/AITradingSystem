# Commander 任务卡 — GitHub 孤儿分支推送
发布时间：2026-03-29
优先级：高

---

## 背景

本地 git 历史中早期 commit 包含了 .venv/ 和 node_modules/ 大文件，GitHub 拒绝 push。
解决方案：新建孤儿分支，只保留当前干净状态，强推到 GitHub。

remote 已配置：`origin = https://github.com/doudouhi78/AITradingSystem.git`

---

## 任务（顺序执行，不要并行）

1. `git checkout --orphan clean-main`
2. `git add -A`
3. `git commit -m "Initial commit: AITradingSystem Phase 1+2 complete"`
4. `git branch -D main`（删除旧 main）
5. `git branch -m clean-main main`（重命名为 main）
6. `git push -f origin main`

如果 push 提示需要认证，记录错误信息。

---

## 完成后写入 worker_result.md

```
## GitHub 孤儿推送结果
- orphan commit：成功/失败
- git push：成功/失败
- 错误信息（如有）：...
- 仓库地址：https://github.com/doudouhi78/AITradingSystem
```

末尾写 `REVIEWER_DONE`。

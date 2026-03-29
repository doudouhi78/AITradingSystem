# Claude 记忆唤醒引导文件

**用途：** 任何账号、任何机器登录后，Claude 读到此文件即可找到完整记忆并唤醒上下文。

---

## 记忆文件存储路径

Claude 的持久记忆文件存储在**本机文件系统**，路径为：

```
C:\Users\Administrator\.claude\projects\D--AITradingSystem\memory\
```

该目录包含以下文件：

| 文件名 | 内容 |
|--------|------|
| `MEMORY.md` | **记忆索引**（入口，列出所有记忆文件）|
| `project_phase2_state.md` | Phase 2 完成状态、关键数字、下一步行动 |
| `project_root_philosophy.md` | 项目根源思想：总目标、用户角色、三角色结构 |
| `project_three_roles.md` | Builder/Reviewer session ID、调用方式、协作协议 |

---

## 唤醒步骤（换账号后第一步）

1. 读取 `C:\Users\Administrator\.claude\projects\D--AITradingSystem\memory\MEMORY.md`
2. 根据 MEMORY.md 索引，读取相关记忆文件
3. 读取 `memory_v1\80_claude_plan\当前执行状态_20260329.md` 获取最新任务状态
4. 读取 `coordination\orchestrator_task.md` 确认当前 Sprint 任务

完成以上4步，即可恢复完整工作上下文。

---

## 记忆机制说明

- **memory 文件**（`~/.claude/...`）：Commander（Claude）跨对话持久记忆，自动加载
- **memory_v1/**（本 repo）：项目设计文档、蓝图、经验库，git 管理，跨账号共享
- **coordination/**（本 repo）：当前 Sprint 任务卡和执行结果，git 管理
- **对话历史**：账号私有，无法跨账号，依赖以上三层重建

---

## 当前项目状态快照（2026-03-29）

- **阶段**：Phase 2 全部完成并验收
- **最新 commit**：`11504508` — Sprint 14 Phase 2 前向模拟
- **下一步**：修复 gate_scheduler.py layered 非阻断问题 → 规划 Phase 3
- **Builder session**：`019d22ff-5216-75e2-869c-350e84020015`
- **Reviewer session**：`019d37ef-f501-7a51-9fb9-333da32fa50d`

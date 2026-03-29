# MCP 最小暴露面 v1

## 目标

给运行态 agent 一个稳定的只读标准入口，避免继续直接猜文件路径、SQLite 字段和实验目录结构。

## 当前暴露操作

- `list_memory_documents`
- `read_memory_document`
- `list_experiment_runs`
- `get_experiment_run`
- `get_current_baseline`

## 设计边界

- 当前只做项目内只读暴露面，不起外部 MCP server。
- 入口统一挂在 `ToolBus.project_mcp`。
- 结构化对象真相源仍是 Python 类型与实验留存，不是记忆文档。
- 后续主脑 / 执行 / 复审应优先通过该入口读取研究对象与关键记忆。

## 当前价值

- 统一读路径
- 降低 agent 直接碰文件和上下文的自由度
- 为后续 tracing 与更正式的 MCP 能力层预留接口

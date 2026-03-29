# Phoenix-style Tracing v1

## 目标

让研究过程具备最小可观测性，而不只是最终结果可见。

## 当前字段模型

每条 `ResearchTraceEvent` 固定包含：

- `trace_id`
- `span_id`
- `parent_span_id`
- `task_id`
- `run_id`
- `experiment_id`
- `agent_role`
- `step_code`
- `step_label`
- `event_kind`
- `status_code`
- `started_at`
- `finished_at`
- `duration_ms`
- `artifact_refs`
- `memory_refs`
- `metric_refs`
- `tags`
- `notes`

## 当前留存方式

- 原始轨迹：`runtime/traces/<run_id>.jsonl`
- 会话摘要：`runtime/traces/<run_id>.summary.json`
- 标准读取入口：`ToolBus.project_mcp`
  - `list_trace_sessions`
  - `get_trace_session`

## 当前边界

- 当前只做最小字段模型与文件留存，不接 Phoenix 平台本体。
- 当前只回填关键 run 的历史过程，并为后续真实运行提供标准事件模型。
- 后续主脑 / 执行 / 复审应在真实运行中直接写入该结构，不再只留结果摘要。

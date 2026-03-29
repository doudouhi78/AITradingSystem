from pathlib import Path

from ai_dev_os import research_tracing
from ai_dev_os.project_mcp import get_trace_session
from ai_dev_os.project_mcp import list_trace_session_summaries
from ai_dev_os.tool_bus import tool_bus


def test_append_trace_event_and_summary(tmp_path) -> None:
    research_tracing.TRACE_ROOT = tmp_path
    research_tracing.append_trace_event(
        {
            'trace_id': 'trace-test-001',
            'span_id': 'span-001',
            'parent_span_id': '',
            'task_id': 'RTS-TRACE-001',
            'run_id': 'run-trace-001',
            'experiment_id': 'exp-trace-001',
            'agent_role': 'research_executor',
            'step_code': 'backtest_run',
            'step_label': '完成回测',
            'event_kind': 'step',
            'status_code': 'completed',
            'started_at': '2026-03-26T11:00:00+08:00',
            'finished_at': '2026-03-26T11:01:00+08:00',
            'duration_ms': 60000,
            'artifact_refs': [{'artifact_kind': 'reference', 'artifact_path': 'runtime/experiments/exp-trace-001/results.json'}],
            'memory_refs': [],
            'metric_refs': ['metrics_summary'],
            'tags': ['metrics summary'],
            'notes': '',
        }
    )
    events = research_tracing.load_trace_events('run-trace-001')
    summary = research_tracing.summarize_trace_session('run-trace-001')
    assert len(events) == 1
    assert summary['event_count'] == 1
    assert summary['experiment_id'] == 'exp-trace-001'


def test_project_mcp_trace_queries_use_repo_traces() -> None:
    research_tracing.TRACE_ROOT = Path(__file__).resolve().parents[1] / 'runtime' / 'traces'
    summaries = list_trace_session_summaries(limit=20)
    run_ids = {item['run_id'] for item in summaries}
    assert 'run-20260325-001' in run_ids
    assert 'run-20260328-011' in run_ids
    payload = get_trace_session('run-20260325-002')
    assert payload['summary']['experiment_id'] == 'exp-20260325-002-breakout-baseline'
    assert len(payload['events']) >= 1


def test_tool_bus_exposes_trace_queries() -> None:
    result = tool_bus.call_tool('project_mcp', operation='list_trace_sessions', limit=5)
    assert result['success'] is True
    detail = tool_bus.call_tool('project_mcp', operation='get_trace_session', run_id='run-20260325-003')
    assert detail['success'] is True
    assert detail['result']['summary']['run_id'] == 'run-20260325-003'

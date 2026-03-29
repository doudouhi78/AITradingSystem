from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from ai_dev_os.project_mcp import get_current_baseline
from ai_dev_os.project_mcp import get_experiment_run
from ai_dev_os.project_mcp import get_trace_session
from ai_dev_os.project_mcp import list_experiment_runs
from ai_dev_os.project_mcp import list_trace_session_summaries
from ai_dev_os.project_mcp import read_memory_document
from ai_dev_os.system_db import REPO_ROOT


def _parse_iso(value: str) -> datetime:
    text = str(value or '').strip()
    if not text:
        return datetime.min.astimezone()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    return datetime.fromisoformat(text)


def _first_item_after_heading(markdown: str, heading: str) -> str:
    capture = False
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped == heading:
            capture = True
            continue
        if capture and stripped.startswith('## '):
            break
        if not capture or not stripped:
            continue
        if stripped.startswith('- '):
            return stripped[2:].strip()
        if stripped.startswith('> '):
            return stripped[2:].strip()
        if not stripped.endswith('：'):
            return stripped
    return ''


def _first_quote_after_heading(markdown: str, heading: str) -> str:
    capture = False
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped == heading:
            capture = True
            continue
        if capture and stripped.startswith('## '):
            break
        if capture and stripped.startswith('> '):
            return stripped[2:].strip()
    return ''


def _markdown_summary(markdown: str, heading: str) -> list[str]:
    capture = False
    items: list[str] = []
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped == heading:
            capture = True
            continue
        if capture and stripped.startswith('## '):
            break
        if capture and stripped.startswith('- '):
            items.append(stripped[2:].strip())
    return items


def _recent_window_days(days: int) -> datetime:
    return datetime.now().astimezone() - timedelta(days=days)


def _trace_blocked_items(trace_summaries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for item in trace_summaries:
        statuses = [str(code) for code in item.get('status_codes', [])]
        if any(code not in {'completed', 'recorded', 'approved'} for code in statuses):
            blocked.append(
                {
                    'run_id': item['run_id'],
                    'experiment_id': item['experiment_id'],
                    'last_status': statuses[-1] if statuses else '',
                    'last_step': item.get('step_codes', [])[-1] if item.get('step_codes') else '',
                }
            )
    return blocked


def _missing_evidence_items(experiments: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    for item in experiments:
        artifact_root = Path(str(item.get('artifact_root', '') or ''))
        missing_parts = []
        for filename in ('manifest.json', 'inputs.json', 'results.json', 'notes.md'):
            if not (artifact_root / filename).exists():
                missing_parts.append(filename)
        if missing_parts:
            missing.append({'experiment_id': item['experiment_id'], 'missing_files': missing_parts})
    return missing


def build_overview_view() -> dict[str, Any]:
    mainline = read_memory_document('mainline')['content']
    working_draft = read_memory_document('working_draft')['content']
    working_test = read_memory_document('working_test')['content']
    baseline = get_current_baseline()
    experiments = list_experiment_runs(limit=200)
    traces = list_trace_session_summaries(limit=50)

    recent_cutoff = _recent_window_days(7)
    recent_experiment_count = sum(1 for item in experiments if _parse_iso(str(item.get('created_at', ''))) >= recent_cutoff)
    pending_reviews_count = sum(
        1
        for item in experiments
        if str(item.get('decision_status', '') or '').strip() in {'baseline_candidate', 'review_pending'}
    )
    blocked_items = _trace_blocked_items(traces)

    latest_items = [
        {
            'kind': 'experiment',
            'id': item['experiment_id'],
            'title': item['title'],
            'status': item['status_code'],
            'updated_at': item['created_at'],
        }
        for item in experiments[:5]
    ]

    return {
        'current_phase': _first_quote_after_heading(mainline, '## 一、当前阶段位置') or _first_item_after_heading(mainline, '## 一、当前阶段位置'),
        'current_focus': _first_item_after_heading(mainline, '## 二、当前主问题'),
        'current_baseline': {
            'experiment_id': baseline['experiment_id'],
            'title': baseline['title'],
            'variant_name': baseline['variant_name'],
            'instrument': baseline['instrument'],
        },
        'recent_experiment_count_7d': recent_experiment_count,
        'pending_reviews_count': pending_reviews_count,
        'blocked_items_count': len(blocked_items),
        'latest_items': latest_items,
        'blocked_items': blocked_items[:5],
        'current_draft_focus': _markdown_summary(working_draft, '## 三、当前未完成事项')[:5],
        'recent_test_judgements': _markdown_summary(working_test, '## 当前经验结论')[:5],
    }


def build_experiment_list_view(*, status: str = '', strategy_family: str = '', baseline_only: bool = False, limit: int = 50) -> dict[str, Any]:
    rows = list_experiment_runs(limit=200, strategy_family=strategy_family, status_code=status)
    if baseline_only:
        rows = [row for row in rows if int(row.get('is_baseline', 0) or 0) == 1]
    rows = rows[:limit]

    items: list[dict[str, Any]] = []
    for row in rows:
        detail = get_experiment_run(str(row['experiment_id']))
        metrics = detail['index']['metrics_summary']
        items.append(
            {
                'experiment_id': row['experiment_id'],
                'task_id': row['task_id'],
                'baseline_of': row['baseline_of'],
                'variant_label': row['variant_name'],
                'status': row['status_code'],
                'strategy_family': row['strategy_family'],
                'annualized_return': metrics.get('annualized_return', metrics.get('annual_return', 0.0)),
                'max_drawdown': metrics.get('max_drawdown', 0.0),
                'sharpe': metrics.get('sharpe', 0.0),
                'trade_count': metrics.get('trade_count', metrics.get('trades', 0)),
                'review_outcome': detail['artifacts']['results']['review_outcome']['review_outcome'],
                'decision_status': row['decision_status'],
                'updated_at': row['created_at'],
                'is_baseline': bool(row.get('is_baseline', 0)),
            }
        )

    return {
        'items': items,
        'filters': {
            'status': status,
            'strategy_family': strategy_family,
            'baseline_only': baseline_only,
            'limit': limit,
        },
    }


def build_experiment_detail_view(experiment_id: str) -> dict[str, Any]:
    payload = get_experiment_run(experiment_id)
    artifacts = payload['artifacts']
    index = payload['index']
    inputs = artifacts['inputs']
    results = artifacts['results']
    decision_status = results['decision_status']

    stage_progress = [
        {'stage_name': '任务单', 'stage_status': 'completed'},
        {'stage_name': '规则表达', 'stage_status': 'completed'},
        {'stage_name': '数据快照', 'stage_status': 'completed'},
        {'stage_name': '验证摘要', 'stage_status': 'completed'},
        {'stage_name': '风险与仓位', 'stage_status': 'completed'},
        {'stage_name': '复审', 'stage_status': 'completed'},
        {'stage_name': '审批状态', 'stage_status': 'baseline_candidate' if decision_status['is_baseline'] else 'recorded'},
    ]

    return {
        'experiment_id': experiment_id,
        'task_summary': inputs['research_task'],
        'rule_summary': inputs['rule_expression'],
        'data_snapshot_summary': inputs['dataset_snapshot'],
        'validation_summary': results['metrics_summary'],
        'risk_summary': results['risk_position_note'],
        'review_summary': results['review_outcome'],
        'approval_summary': results['decision_status'],
        'artifact_links': {
            'artifact_root': artifacts['artifact_root'],
            'manifest': artifacts['manifest_path'],
            'inputs': artifacts['inputs_path'],
            'results': artifacts['results_path'],
            'notes': artifacts['notes_path'],
            'memory_note_path': index['memory_note_path'],
        },
        'stage_progress': stage_progress,
    }


def build_flow_view() -> dict[str, Any]:
    trace_summaries = list_trace_session_summaries(limit=50)
    experiments = [get_experiment_run(item['experiment_id'])['index'] for item in list_experiment_runs(limit=50)]
    blocked_items = _trace_blocked_items(trace_summaries)
    missing_evidence_items = _missing_evidence_items(experiments)
    status_counts = Counter()
    for item in trace_summaries:
        for status in item.get('status_codes', []):
            status_counts[str(status)] += 1

    recent_returns = []
    for item in trace_summaries[:10]:
        if item.get('status_codes') and any(code not in {'completed', 'approved', 'recorded'} for code in item['status_codes']):
            recent_returns.append(
                {
                    'run_id': item['run_id'],
                    'experiment_id': item['experiment_id'],
                    'return_reason': item['status_codes'][-1],
                    'last_step': item['step_codes'][-1] if item['step_codes'] else '',
                }
            )

    return {
        'recent_traces': trace_summaries[:10],
        'recent_returns': recent_returns[:10],
        'blocked_items': blocked_items[:10],
        'missing_evidence_items': missing_evidence_items[:10],
        'stage_status_counts': dict(status_counts),
    }


def build_trace_detail_view(run_id: str) -> dict[str, Any]:
    payload = get_trace_session(run_id)
    evidence_links = []
    for event in payload['events']:
        for ref in event.get('artifact_refs', []):
            evidence_links.append(
                {
                    'run_id': run_id,
                    'step_code': event['step_code'],
                    'artifact_kind': ref['artifact_kind'],
                    'artifact_path': ref['artifact_path'],
                }
            )
        for path in event.get('memory_refs', []):
            evidence_links.append(
                {
                    'run_id': run_id,
                    'step_code': event['step_code'],
                    'artifact_kind': 'memory_ref',
                    'artifact_path': path,
                }
            )
    return {'trace_summary': payload['summary'], 'events': payload['events'], 'evidence_links': evidence_links}

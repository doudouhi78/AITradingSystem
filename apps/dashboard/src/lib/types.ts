export interface OverviewView {
  current_phase: string;
  current_focus: string;
  current_baseline: {
    experiment_id: string;
    title: string;
    variant_name: string;
    instrument: string;
  };
  recent_experiment_count_7d: number;
  pending_reviews_count: number;
  blocked_items_count: number;
  latest_items: Array<{ kind: string; id: string; title: string; status: string; updated_at: string }>;
  blocked_items: Array<{ run_id: string; experiment_id: string; last_status: string; last_step: string }>;
  current_draft_focus: string[];
  recent_test_judgements: string[];
}

export interface ExperimentListItemView {
  experiment_id: string;
  task_id: string;
  baseline_of: string;
  variant_label: string;
  status: string;
  strategy_family: string;
  annualized_return: number;
  max_drawdown: number;
  sharpe: number;
  trade_count: number;
  review_outcome: string;
  decision_status: string;
  updated_at: string;
  is_baseline: boolean;
}

export interface ExperimentListView {
  items: ExperimentListItemView[];
  filters: {
    status: string;
    strategy_family: string;
    baseline_only: boolean;
    limit: number;
  };
}

export interface ExperimentDetailView {
  experiment_id: string;
  task_summary: Record<string, unknown>;
  rule_summary: Record<string, unknown>;
  data_snapshot_summary: Record<string, unknown>;
  validation_summary: Record<string, unknown>;
  risk_summary: Record<string, unknown>;
  review_summary: Record<string, unknown>;
  approval_summary: Record<string, unknown>;
  artifact_links: Record<string, string>;
  stage_progress: Array<{ stage_name: string; stage_status: string }>;
}

export interface FlowView {
  recent_traces: Array<Record<string, unknown>>;
  recent_returns: Array<Record<string, unknown>>;
  blocked_items: Array<Record<string, unknown>>;
  missing_evidence_items: Array<Record<string, unknown>>;
  stage_status_counts: Record<string, number>;
}

export interface TraceDetailView {
  trace_summary: Record<string, unknown>;
  events: Array<Record<string, unknown>>;
  evidence_links: Array<Record<string, unknown>>;
}

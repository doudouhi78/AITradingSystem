from __future__ import annotations


def build_trigger_protocol(
    *,
    task_kind: str,
    task_profile: str,
    rework_count: int = 0,
    fallback_applied: bool = False,
    fallback_reason: str = "",
    classification_confidence: str = "medium",
) -> dict[str, object]:
    profile_family = {
        "routine": "routine",
        "evidence_sensitive": "evidence",
        "release_sensitive": "release",
        "coordination_sensitive": "coordination",
        "governance_sensitive": "governance",
        "combination_sensitive": "combination",
    }.get(task_profile, "routine")
    low_confidence = classification_confidence == "low"
    heavy_review_required = (
        task_kind == "system_optimization"
        or rework_count > 0
        or low_confidence
        or task_profile in {
            "governance_sensitive",
            "coordination_sensitive",
            "combination_sensitive",
            "release_sensitive",
        }
    )
    reviewer_required = heavy_review_required
    reviewer_required = reviewer_required or (
        task_kind == "system_optimization"
        or rework_count > 0
        or low_confidence
        or task_profile in {
            "governance_sensitive",
            "combination_sensitive",
            "release_sensitive",
        }
    )

    if task_kind == "system_optimization":
        reviewer_reason = "system_optimization_requires_stage_review"
    elif low_confidence:
        reviewer_reason = "low_confidence_classification_requires_guarded_review"
    elif rework_count > 0:
        reviewer_reason = "rework_cycle_requires_quality_recheck"
    elif heavy_review_required:
        reviewer_reason = f"task_profile={task_profile}"
    else:
        reviewer_reason = "reviewer_deferred_to_stage_boundary_or_risk_escalation"

    reviewer_reason = reviewer_reason if reviewer_required else "reviewer_deferred_to_stage_boundary_or_risk_escalation"

    planned_skipped_nodes: list[str] = []
    if not reviewer_required:
        planned_skipped_nodes.append("reviewer_first_pass")

    available_context_layers = ["task_card", "orchestrator_guidance", "governance_contract"]
    missing_context_layers: list[str] = ["mother_memory_context", "project_runtime_context", "scan_result"]
    if not reviewer_required:
        missing_context_layers.append("initial_reviewer_pass")

    path_mode = "full_governance"
    if not reviewer_required:
        path_mode = "dynamic_light"
    elif reviewer_required:
        path_mode = "dynamic_review_heavy"

    human_checkpoint_reasons: list[str] = []
    human_checkpoint_level = "optional"
    if low_confidence:
        human_checkpoint_reasons.append("classification_confidence_is_low")
        human_checkpoint_level = "recommended"
    if task_kind == "system_optimization":
        human_checkpoint_reasons.append("system_optimization_requires_human_visibility")
        human_checkpoint_level = "required"
    if task_profile in {"governance_sensitive", "combination_sensitive"}:
        human_checkpoint_reasons.append(f"high_governance_family={task_profile}")
        human_checkpoint_level = "required"
    elif reviewer_required or rework_count > 0:
        human_checkpoint_reasons.append("non_routine_path_active")
        if human_checkpoint_level == "optional":
            human_checkpoint_level = "recommended"
    if not human_checkpoint_reasons:
        human_checkpoint_reasons.append("routine_task_can_be_monitored_passively")

    review_intensity = "light"
    if heavy_review_required:
        review_intensity = "full" if task_profile in {"governance_sensitive", "combination_sensitive"} or task_kind == "system_optimization" else "focused"
    periodic_cadence = "on_stage_boundary_or_risk_escalation"
    if reviewer_required and (task_profile in {"governance_sensitive", "combination_sensitive"} or task_kind == "system_optimization"):
        periodic_cadence = "after_each_execution_stage_and_before_release_or_approval"
    routine_definition = {
        "goal_shape": "single-module, bounded implementation target",
        "excluded_families": [
            "governance",
            "coordination",
            "release",
            "multi-capability combination",
        ],
        "allowed_light_path": not reviewer_required,
    }

    return {
        "path_mode": path_mode,
        "routing_state": {
            "initial_path_mode": path_mode,
            "current_path_mode": path_mode,
            "last_reassessment_reason": "initial_classification",
            "escalation_count": 0,
            "light_path_exited": False,
            "light_path_exit_reason": "",
        },
        "task_profile_family": profile_family,
        "routine_definition": routine_definition,
        "classification_guard": {
            "fallback_applied": fallback_applied,
            "fallback_reason": fallback_reason or "none",
            "confidence": classification_confidence,
            "policy": "if_classification_is_ambiguous_or_under-classified_choose_heavier_path",
        },
        "soft_trigger": {
            "enabled": heavy_review_required,
            "owner": "reviewer",
            "reason": reviewer_reason,
            "escalate_to": "builder",
            "threshold": "enabled_for_low_confidence_rework_or_non-routine_review_boundaries",
            "review_intensity": review_intensity,
        },
        "hard_trigger": {
            "enabled": task_kind == "system_optimization" or task_profile in {"governance_sensitive", "combination_sensitive"},
            "owner": "approval",
            "reason": "high_risk_or_system_optimization_requires_human_gate",
            "escalate_to": "approval",
            "threshold": "enabled_for_system_optimization_or_high-governance_family",
        },
        "periodic_trigger": {
            "enabled": reviewer_required,
            "owner": "reviewer",
            "reason": reviewer_reason,
            "escalate_to": "builder",
            "threshold": "enabled_for_high_risk_rework_or_stage_boundary_review",
            "cadence": periodic_cadence,
        },
        "reviewer_required": reviewer_required,
        "reviewer_trigger_class": "soft" if heavy_review_required else "deferred",
        "reviewer_reason": reviewer_reason,
        "planned_skipped_nodes": planned_skipped_nodes,
        "available_context_layers": list(dict.fromkeys(available_context_layers)),
        "missing_context_layers": list(dict.fromkeys(missing_context_layers)),
        "escalation_policy": {
            "on_reviewer_changes_requested": "builder",
            "on_reviewer_changes_requested_after_light_path": "builder",
            "on_reviewer_changes_requested_after_reviewed_path": "builder",
            "on_high_risk_or_system_optimization": "approval",
            "on_periodic_inspection_pass": "continue_or_approval",
            "light_path_exit_conditions": [
                "rework_detected",
                "abnormal_duration",
                "high_risk_action",
                "approval_required",
                "major_reviewer_issue",
                "scope_drift_or_permission_issue",
                "evidence_gap_affects_release_judgment",
            ],
        },
        "trigger_thresholds": {
            "classification_confidence": classification_confidence,
            "low_confidence_promotes_review": low_confidence,
            "rework_threshold_for_review": 1,
            "system_optimization_requires_hard_gate": task_kind == "system_optimization",
            "periodic_inspection_always_on": False,
            "periodic_cadence": periodic_cadence,
            "reviewer_positioning": "stage_boundary_or_high_risk_fuse",
        },
        "human_visibility_checkpoint": {
            "enabled": True,
            "level": human_checkpoint_level,
            "reasons": human_checkpoint_reasons,
            "summary": "Human operators should be able to inspect trigger decisions, skipped nodes, recent inspections, and the confidence of any downgraded path.",
        },
    }



def reassess_trigger_protocol(
    trigger_plan: dict[str, object],
    *,
    recent_events: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    updated = dict(trigger_plan or {})
    routing_state = dict(updated.get("routing_state", {}) or {})
    initial_path_mode = str(updated.get("path_mode", routing_state.get("initial_path_mode", "dynamic_light")))
    current_path_mode = str(routing_state.get("current_path_mode", initial_path_mode))
    escalation_count = int(routing_state.get("escalation_count", 0) or 0)
    light_path_exited = bool(routing_state.get("light_path_exited", False))
    light_path_exit_reason = str(routing_state.get("light_path_exit_reason", "") or "")
    last_reassessment_reason = str(routing_state.get("last_reassessment_reason", "initial_classification") or "initial_classification")

    effective_events = [event for event in (recent_events or []) if isinstance(event, dict)]
    reassessment_reason = ""
    for event in reversed(effective_events):
        event_type = str(event.get("event_type", "") or "")
        node = str(event.get("node", "") or "")
        if event_type == "approval_waiting":
            reassessment_reason = "approval_waiting"
            break
        if event_type == "rework_entered":
            reassessment_reason = "rework_entered"
            break
        if event_type == "error" and node in {"execution", "execution_engine", "reviewer"}:
            reassessment_reason = f"error@{node}"
            break

    if reassessment_reason:
        if current_path_mode != "dynamic_review_heavy":
            escalation_count += 1
        current_path_mode = "dynamic_review_heavy"
        updated["path_mode"] = current_path_mode
        updated["reviewer_required"] = True
        updated["reviewer_trigger_class"] = "full"
        updated["reviewer_reason"] = reassessment_reason
        updated["planned_skipped_nodes"] = []
        if reassessment_reason == "rework_entered":
            light_path_exited = True
            light_path_exit_reason = "rework_detected"
        elif reassessment_reason == "approval_waiting":
            light_path_exited = True
            light_path_exit_reason = "approval_required"
        elif reassessment_reason.startswith("error@"):
            light_path_exited = True
            light_path_exit_reason = "error_detected_on_critical_node"
        last_reassessment_reason = reassessment_reason

    updated["routing_state"] = {
        "initial_path_mode": routing_state.get("initial_path_mode", initial_path_mode),
        "current_path_mode": current_path_mode,
        "last_reassessment_reason": last_reassessment_reason,
        "escalation_count": escalation_count,
        "light_path_exited": light_path_exited,
        "light_path_exit_reason": light_path_exit_reason,
    }
    return updated

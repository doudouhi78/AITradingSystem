from __future__ import annotations

from typing import Any
from typing import Literal
from typing import TypedDict


SourceType = Literal["human_idea", "market_scan", "mature_edge"]


class HumanIdeaInput(TypedDict):
    input_id: str
    title: str
    raw_idea: str
    market: str
    asset_scope: list[str]
    timeframe_hint: str
    why_it_matters: str
    created_at: str


class MarketScanInput(TypedDict):
    input_id: str
    scan_source: str
    theme_title: str
    market_snapshot: str
    candidate_assets: list[str]
    signal_summary: str
    timeframe_hint: str
    evidence_refs: list[str]
    created_at: str


class MatureEdgeInput(TypedDict):
    input_id: str
    edge_name: str
    edge_family: str
    source_refs: list[str]
    edge_summary: str
    common_market_conditions: str
    known_failure_modes: list[str]
    created_at: str


class RuleDraftPack(TypedDict):
    draft_id: str
    source_type: SourceType
    source_input_id: str
    theme_statement: str
    loser_definition: str
    profit_source_definition: str
    target_market: str
    target_assets: list[str]
    target_timeframe: str
    entry_rule_prototype: str
    exit_rule_prototype: str
    stop_rule_prototype: str
    failure_conditions: list[str]
    next_validation_focus: str
    created_at: str


def _require_mapping(payload: dict[str, Any] | TypedDict, name: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"{name} must be a mapping")
    return payload


def _require_non_empty_string(payload: dict[str, Any], field: str, name: str) -> str:
    value = str(payload.get(field, "") or "").strip()
    if not value:
        raise ValueError(f"{name}.{field} is required")
    return value


def _require_string_list(payload: dict[str, Any], field: str, name: str) -> list[str]:
    value = payload.get(field)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name}.{field} must be a non-empty list")
    result = [str(item).strip() for item in value if str(item).strip()]
    if not result:
        raise ValueError(f"{name}.{field} must contain non-empty strings")
    return result


def validate_human_idea_input(payload: HumanIdeaInput | dict[str, Any]) -> HumanIdeaInput:
    data = _require_mapping(payload, "human_idea_input")
    for field in ["input_id", "title", "raw_idea", "market", "timeframe_hint", "why_it_matters", "created_at"]:
        _require_non_empty_string(data, field, "human_idea_input")
    data["asset_scope"] = _require_string_list(data, "asset_scope", "human_idea_input")
    return data  # type: ignore[return-value]


def validate_market_scan_input(payload: MarketScanInput | dict[str, Any]) -> MarketScanInput:
    data = _require_mapping(payload, "market_scan_input")
    for field in ["input_id", "scan_source", "theme_title", "market_snapshot", "signal_summary", "timeframe_hint", "created_at"]:
        _require_non_empty_string(data, field, "market_scan_input")
    data["candidate_assets"] = _require_string_list(data, "candidate_assets", "market_scan_input")
    data["evidence_refs"] = _require_string_list(data, "evidence_refs", "market_scan_input")
    return data  # type: ignore[return-value]


def validate_mature_edge_input(payload: MatureEdgeInput | dict[str, Any]) -> MatureEdgeInput:
    data = _require_mapping(payload, "mature_edge_input")
    for field in ["input_id", "edge_name", "edge_family", "edge_summary", "common_market_conditions", "created_at"]:
        _require_non_empty_string(data, field, "mature_edge_input")
    data["source_refs"] = _require_string_list(data, "source_refs", "mature_edge_input")
    data["known_failure_modes"] = _require_string_list(data, "known_failure_modes", "mature_edge_input")
    return data  # type: ignore[return-value]


def validate_rule_draft_pack(payload: RuleDraftPack | dict[str, Any]) -> RuleDraftPack:
    data = _require_mapping(payload, "rule_draft_pack")
    for field in [
        "draft_id",
        "source_type",
        "source_input_id",
        "theme_statement",
        "loser_definition",
        "profit_source_definition",
        "target_market",
        "target_timeframe",
        "entry_rule_prototype",
        "exit_rule_prototype",
        "stop_rule_prototype",
        "next_validation_focus",
        "created_at",
    ]:
        _require_non_empty_string(data, field, "rule_draft_pack")
    if data["source_type"] not in {"human_idea", "market_scan", "mature_edge"}:
        raise ValueError("rule_draft_pack.source_type is invalid")
    data["target_assets"] = _require_string_list(data, "target_assets", "rule_draft_pack")
    data["failure_conditions"] = _require_string_list(data, "failure_conditions", "rule_draft_pack")
    return data  # type: ignore[return-value]


def draft_from_human_idea(
    payload: HumanIdeaInput | dict[str, Any],
    *,
    draft_id: str,
    loser_definition: str,
    profit_source_definition: str,
    entry_rule_prototype: str,
    exit_rule_prototype: str,
    stop_rule_prototype: str,
    failure_conditions: list[str],
    next_validation_focus: str,
) -> RuleDraftPack:
    idea = validate_human_idea_input(payload)
    return validate_rule_draft_pack(
        {
            "draft_id": draft_id,
            "source_type": "human_idea",
            "source_input_id": idea["input_id"],
            "theme_statement": idea["raw_idea"],
            "loser_definition": loser_definition,
            "profit_source_definition": profit_source_definition,
            "target_market": idea["market"],
            "target_assets": idea["asset_scope"],
            "target_timeframe": idea["timeframe_hint"],
            "entry_rule_prototype": entry_rule_prototype,
            "exit_rule_prototype": exit_rule_prototype,
            "stop_rule_prototype": stop_rule_prototype,
            "failure_conditions": failure_conditions,
            "next_validation_focus": next_validation_focus,
            "created_at": idea["created_at"],
        }
    )


def draft_from_market_scan(
    payload: MarketScanInput | dict[str, Any],
    *,
    draft_id: str,
    loser_definition: str,
    profit_source_definition: str,
    target_market: str,
    entry_rule_prototype: str,
    exit_rule_prototype: str,
    stop_rule_prototype: str,
    failure_conditions: list[str],
    next_validation_focus: str,
) -> RuleDraftPack:
    scan = validate_market_scan_input(payload)
    return validate_rule_draft_pack(
        {
            "draft_id": draft_id,
            "source_type": "market_scan",
            "source_input_id": scan["input_id"],
            "theme_statement": scan["theme_title"],
            "loser_definition": loser_definition,
            "profit_source_definition": profit_source_definition,
            "target_market": target_market,
            "target_assets": scan["candidate_assets"],
            "target_timeframe": scan["timeframe_hint"],
            "entry_rule_prototype": entry_rule_prototype,
            "exit_rule_prototype": exit_rule_prototype,
            "stop_rule_prototype": stop_rule_prototype,
            "failure_conditions": failure_conditions,
            "next_validation_focus": next_validation_focus,
            "created_at": scan["created_at"],
        }
    )


def draft_from_mature_edge(
    payload: MatureEdgeInput | dict[str, Any],
    *,
    draft_id: str,
    loser_definition: str,
    profit_source_definition: str,
    target_market: str,
    target_assets: list[str],
    target_timeframe: str,
    entry_rule_prototype: str,
    exit_rule_prototype: str,
    stop_rule_prototype: str,
    next_validation_focus: str,
) -> RuleDraftPack:
    edge = validate_mature_edge_input(payload)
    return validate_rule_draft_pack(
        {
            "draft_id": draft_id,
            "source_type": "mature_edge",
            "source_input_id": edge["input_id"],
            "theme_statement": edge["edge_summary"],
            "loser_definition": loser_definition,
            "profit_source_definition": profit_source_definition,
            "target_market": target_market,
            "target_assets": target_assets,
            "target_timeframe": target_timeframe,
            "entry_rule_prototype": entry_rule_prototype,
            "exit_rule_prototype": exit_rule_prototype,
            "stop_rule_prototype": stop_rule_prototype,
            "failure_conditions": edge["known_failure_modes"],
            "next_validation_focus": next_validation_focus,
            "created_at": edge["created_at"],
        }
    )

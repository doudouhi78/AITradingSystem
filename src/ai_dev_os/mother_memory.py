from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MOTHER_MEMORY_ROOT = ROOT / "mother_memory"
DOCTRINE_ROOT = ROOT / "doctrine"


def _safe_read(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _compact(text: str, limit: int = 1200) -> str:
    normalized = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_./-]+", text.lower())
        if len(token) >= 3
    }


def _mother_memory_candidates() -> list[tuple[str, Path, int]]:
    candidates: list[tuple[str, Path, int]] = [
        ("设计原则", DOCTRINE_ROOT / "AI Dev OS 设计原则 v1.md", 12),
        ("主控链原则", DOCTRINE_ROOT / "主控链与旁路审计原则.md", 10),
        ("能力地图", MOTHER_MEMORY_ROOT / "capabilities" / "system_capability_map.md", 9),
        ("当前阶段", MOTHER_MEMORY_ROOT / "iterations" / "current_state.md", 9),
        ("已知坑点", MOTHER_MEMORY_ROOT / "failures" / "known_pitfalls.md", 10),
        ("下一阶段路线", MOTHER_MEMORY_ROOT / "roadmap" / "next_phase_plan.md", 8),
        ("大型项目路线", MOTHER_MEMORY_ROOT / "roadmap" / "large_project_readiness_roadmap_v1.md", 7),
    ]
    for path in sorted((MOTHER_MEMORY_ROOT / "iterations").glob("*.md")):
        if path.name == "current_state.md":
            continue
        candidates.append((f"迭代记录/{path.stem}", path, 5))
    diagnostics_root = MOTHER_MEMORY_ROOT / "diagnostics"
    if diagnostics_root.exists():
        for path in sorted(diagnostics_root.glob("*.md"))[-12:]:
            candidates.append((f"诊断记录/{path.stem}", path, 6))
        for path in sorted(diagnostics_root.glob("*.json"))[-12:]:
            candidates.append((f"诊断数据/{path.stem}", path, 4))
    return candidates


def _select_relevant_documents(query_text: str, *, limit: int = 4, snippet_limit: int = 950) -> list[dict[str, str]]:
    query_tokens = _tokenize(query_text)
    selected: list[tuple[int, int, str, Path, str]] = []
    for label, path, base_weight in _mother_memory_candidates():
        text = _safe_read(path)
        if not text:
            continue
        lowered = text.lower()
        overlap = len(query_tokens & _tokenize(text))
        substring_hits = sum(1 for token in query_tokens if token in lowered)
        score = base_weight + overlap * 5 + substring_hits * 2
        selected.append((score, overlap, label, path, text))

    selected.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    docs: list[dict[str, str]] = []
    for score, overlap, label, path, text in selected[:limit]:
        docs.append(
            {
                "label": label,
                "path": str(path.relative_to(ROOT)).replace("\\", "/"),
                "score": str(score),
                "snippet": _compact(text, limit=snippet_limit),
            }
        )
    return docs


def get_mother_memory_context(
    query_text: str = "",
    *,
    mode: str = "default",
    combination_sensitive: bool = False,
) -> dict[str, str | list[dict[str, str]]]:
    if mode == "builder" and combination_sensitive:
        doctrine_limit = 420
        capability_limit = 520
        current_state_limit = 520
        pitfalls_limit = 620
        roadmap_limit = 420
        relevant_doc_limit = 2
        relevant_snippet_limit = 620
    else:
        doctrine_limit = 1200
        capability_limit = 1200
        current_state_limit = 1000
        pitfalls_limit = 1000
        roadmap_limit = 1000
        relevant_doc_limit = 4
        relevant_snippet_limit = 950

    doctrine = _compact(_safe_read(DOCTRINE_ROOT / "AI Dev OS 设计原则 v1.md"), limit=doctrine_limit)
    capability_map = _compact(_safe_read(MOTHER_MEMORY_ROOT / "capabilities" / "system_capability_map.md"), limit=capability_limit)
    current_state = _compact(_safe_read(MOTHER_MEMORY_ROOT / "iterations" / "current_state.md"), limit=current_state_limit)
    pitfalls = _compact(_safe_read(MOTHER_MEMORY_ROOT / "failures" / "known_pitfalls.md"), limit=pitfalls_limit)
    roadmap = _compact(_safe_read(MOTHER_MEMORY_ROOT / "roadmap" / "next_phase_plan.md"), limit=roadmap_limit)
    relevant_docs = _select_relevant_documents(
        query_text,
        limit=relevant_doc_limit,
        snippet_limit=relevant_snippet_limit,
    )
    retrieved_context = "\n\n".join(
        f"【{item['label']} | {item['path']} | score={item['score']}】\n{item['snippet']}"
        for item in relevant_docs
    )

    if mode == "builder" and combination_sensitive:
        ordered_sections = [
            "【相关母体记忆】\n" + retrieved_context if retrieved_context else "",
            "【已知坑点】\n" + pitfalls if pitfalls else "",
            "【当前阶段】\n" + current_state if current_state else "",
        ]
        if combination_sensitive and capability_map:
            ordered_sections.append("【能力地图】\n" + capability_map)
        if doctrine:
            ordered_sections.append("【设计原则摘要】\n" + doctrine)
    else:
        ordered_sections = [
            "【设计原则】\n" + doctrine if doctrine else "",
            "【能力地图】\n" + capability_map if capability_map else "",
            "【当前阶段】\n" + current_state if current_state else "",
            "【已知坑点】\n" + pitfalls if pitfalls else "",
            "【下一阶段路线】\n" + roadmap if roadmap else "",
            "【相关母体记忆】\n" + retrieved_context if retrieved_context else "",
        ]

    combined = "\n\n".join(section for section in ordered_sections if section)
    return {
        "doctrine": doctrine,
        "capability_map": capability_map,
        "current_state": current_state,
        "pitfalls": pitfalls,
        "roadmap": roadmap,
        "relevant_docs": relevant_docs,
        "combined_context": combined,
    }

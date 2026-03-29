from __future__ import annotations

import re
from pathlib import Path


def _safe_read(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _tail_text(text: str, limit: int = 1200) -> str:
    normalized = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    if len(normalized) <= limit:
        return normalized
    return "..." + normalized[-(limit - 3) :].lstrip()


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_./-]+", text.lower())
        if len(token) >= 3
    }


def _project_memory_candidates(memory_root: Path) -> list[tuple[str, Path, int]]:
    return [
        ("项目最新摘要", memory_root / "project_state" / "latest_summary.md", 10),
        ("项目事实记录", memory_root / "project_state" / "facts.jsonl", 9),
        ("项目决策记录", memory_root / "journal" / "decision_log" / "log.md", 8),
        ("项目时间线", memory_root / "timeline.md", 7),
    ]


def _select_relevant_documents(
    memory_root: Path,
    query_text: str,
    *,
    limit: int = 3,
    snippet_limit: int = 850,
) -> list[dict[str, str]]:
    query_tokens = _tokenize(query_text)
    selected: list[tuple[int, int, str, Path, str]] = []
    for label, path, base_weight in _project_memory_candidates(memory_root):
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
                "path": str(path),
                "score": str(score),
                "snippet": _tail_text(text, limit=snippet_limit),
            }
        )
    return docs


def get_project_memory_context(
    paths: dict[str, str],
    query_text: str = "",
    *,
    mode: str = "default",
    rework_count: int = 0,
) -> dict[str, str | list[dict[str, str]]]:
    memory_root = Path(paths["memory_root"])
    if mode == "reviewer":
        latest_limit = 700
        facts_limit = 520
        decision_limit = 520 if rework_count > 0 else 680
        timeline_limit = 350 if rework_count > 0 else 520
        relevant_doc_limit = 2 if rework_count > 0 else 3
        relevant_snippet_limit = 520 if rework_count > 0 else 700
    elif mode == "builder":
        latest_limit = 850
        facts_limit = 650
        decision_limit = 700
        timeline_limit = 550
        relevant_doc_limit = 3
        relevant_snippet_limit = 700
    else:
        latest_limit = 1200
        facts_limit = 900
        decision_limit = 900
        timeline_limit = 900
        relevant_doc_limit = 3
        relevant_snippet_limit = 850

    latest_summary = _tail_text(_safe_read(memory_root / "project_state" / "latest_summary.md"), limit=latest_limit)
    facts = _tail_text(_safe_read(memory_root / "project_state" / "facts.jsonl"), limit=facts_limit)
    decision_log = _tail_text(_safe_read(memory_root / "journal" / "decision_log" / "log.md"), limit=decision_limit)
    timeline = _tail_text(_safe_read(memory_root / "timeline.md"), limit=timeline_limit)
    relevant_docs = _select_relevant_documents(
        memory_root,
        query_text,
        limit=relevant_doc_limit,
        snippet_limit=relevant_snippet_limit,
    )
    retrieved_context = "\n\n".join(
        f"【{item['label']} | score={item['score']}】\n{item['snippet']}"
        for item in relevant_docs
    )

    if mode == "reviewer" and rework_count > 0:
        ordered_sections = [
            "【相关项目记忆】\n" + retrieved_context if retrieved_context else "",
            "【项目最新摘要】\n" + latest_summary if latest_summary else "",
            "【项目事实记录】\n" + facts if facts else "",
            "【项目决策记录】\n" + decision_log if decision_log else "",
        ]
    else:
        ordered_sections = [
            "【项目最新摘要】\n" + latest_summary if latest_summary else "",
            "【项目事实记录】\n" + facts if facts else "",
            "【项目决策记录】\n" + decision_log if decision_log else "",
            "【项目时间线】\n" + timeline if timeline else "",
            "【相关项目记忆】\n" + retrieved_context if retrieved_context else "",
        ]

    combined = "\n\n".join(section for section in ordered_sections if section)
    return {
        "latest_summary": latest_summary,
        "facts": facts,
        "decision_log": decision_log,
        "timeline": timeline,
        "relevant_docs": relevant_docs,
        "combined_context": combined,
    }

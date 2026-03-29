from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[2]
CODEX_MEMORY_ROOT = ROOT_DIR / "memory_v3" / "70_runtime_roles" / "codex_zone"

ROLE_DIRS = {
    "orchestrator": CODEX_MEMORY_ROOT / "2_orchestrator",
    "builder": CODEX_MEMORY_ROOT / "3_mainhand",
    "mainhand": CODEX_MEMORY_ROOT / "3_mainhand",
    "reviewer": CODEX_MEMORY_ROOT / "4_reviewer",
    "recorder": CODEX_MEMORY_ROOT / "5_recorder",
}

ROLE_FILE_STEMS = {
    "orchestrator": "orchestrator",
    "builder": "mainhand",
    "mainhand": "mainhand",
    "reviewer": "reviewer",
    "recorder": "recorder",
}


def _normalize_role(role: str) -> str:
    normalized = str(role or "").strip().lower()
    return "builder" if normalized == "mainhand" else normalized


def get_role_memory_dir(role: str) -> Path:
    normalized = _normalize_role(role)
    return ROLE_DIRS[normalized]


def get_role_lessons_path(role: str) -> Path:
    normalized = _normalize_role(role)
    stem = ROLE_FILE_STEMS[normalized]
    return get_role_memory_dir(normalized) / f"{stem}_lessons_v1.md"


def get_role_working_memory_path(role: str) -> Path:
    return get_role_memory_dir(role) / "working_memory.md"


def ensure_role_memory_scaffold(role: str) -> None:
    normalized = _normalize_role(role)
    root = get_role_memory_dir(normalized)
    root.mkdir(parents=True, exist_ok=True)
    (root / "samples").mkdir(parents=True, exist_ok=True)
    (root / "archive").mkdir(parents=True, exist_ok=True)

    wm = get_role_working_memory_path(normalized)
    if not wm.exists():
        wm.write_text("", encoding="utf-8")

    lessons = get_role_lessons_path(normalized)
    if not lessons.exists():
        lessons.write_text(f"# {ROLE_FILE_STEMS[normalized].capitalize()} Lessons v1\n\n", encoding="utf-8")


def _clip_text(text: str, limit: int) -> str:
    value = str(text or '').strip()
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + '...'


def _safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _extract_tagged_lessons(text: str) -> list[str]:
    if not text.strip():
        return []
    blocks = re.split(r"(?=^##\s+L-)", text, flags=re.MULTILINE)
    return [block.strip() for block in blocks if block.strip().startswith("## L-")]


def _filter_lessons_text(lessons_text: str, task_type: str, max_entries: int) -> str:
    entries = _extract_tagged_lessons(lessons_text)
    if not entries:
        return ""
    task_token = f"[{str(task_type or '').strip().lower()}]"
    filtered: list[str] = []
    fallback: list[str] = []
    for entry in entries:
        lowered = entry.lower()
        if "[all]" in lowered or (task_token and task_token in lowered):
            filtered.append(entry)
        else:
            fallback.append(entry)
    selected = (filtered or fallback)[:max_entries]
    return "\n\n".join(selected)


def build_role_memory_context(
    role: str,
    task_type: str = "routine",
    max_chars: int = 1400,
    *,
    include_seed: bool = True,
    include_config: bool = True,
    include_working_memory: bool = True,
    include_lessons: bool = True,
) -> str:
    normalized = _normalize_role(role)
    ensure_role_memory_scaffold(normalized)
    root = get_role_memory_dir(normalized)
    stem = ROLE_FILE_STEMS[normalized]
    sections: list[str] = []

    enabled_count = sum(1 for flag in (include_seed, include_config, include_working_memory, include_lessons) if flag)
    if enabled_count <= 0:
        return ""

    seed_budget = max_chars * 30 // 100 if include_seed else 0
    config_budget = max_chars * 20 // 100 if include_config else 0
    wm_budget = max_chars * 25 // 100 if include_working_memory else 0
    lessons_budget = max_chars * 25 // 100 if include_lessons else 0

    remainder = max_chars - (seed_budget + config_budget + wm_budget + lessons_budget)
    if remainder > 0:
        enabled_sections: list[str] = []
        if include_seed:
            enabled_sections.append('seed')
        if include_config:
            enabled_sections.append('config')
        if include_working_memory:
            enabled_sections.append('working_memory')
        if include_lessons:
            enabled_sections.append('lessons')
        share, extra = divmod(remainder, len(enabled_sections))
        for name in enabled_sections:
            bonus = share + (1 if extra > 0 else 0)
            if name == 'seed':
                seed_budget += bonus
            elif name == 'config':
                config_budget += bonus
            elif name == 'working_memory':
                wm_budget += bonus
            elif name == 'lessons':
                lessons_budget += bonus
            if extra > 0:
                extra -= 1

    if include_seed:
        content = _safe_read(root / f"{stem}_seed_v1.md")
        if content:
            clipped = _clip_text("\n".join(content.splitlines()[:40]).strip(), seed_budget)
            sections.append(f"[{stem}_seed_v1.md]\n{clipped}")

    if include_config:
        content = _safe_read(root / f"{stem}_config_v1.md")
        if content:
            clipped = _clip_text("\n".join(content.splitlines()[:40]).strip(), config_budget)
            sections.append(f"[{stem}_config_v1.md]\n{clipped}")

    if include_working_memory:
        working_memory = _safe_read(root / "working_memory.md")
        if working_memory:
            clipped = _clip_text("\n".join(working_memory.splitlines()[:30]).strip(), wm_budget)
            sections.append(f"[working_memory.md]\n{clipped}")

    if include_lessons:
        lessons = _filter_lessons_text(_safe_read(root / f"{stem}_lessons_v1.md"), task_type=task_type, max_entries=6)
        if lessons:
            sections.append(f"[relevant_lessons]\n{_clip_text(lessons, lessons_budget)}")

    return "\n\n---\n\n".join(section for section in sections if section).strip()

def write_working_memory(
    role: str,
    task_id: str,
    goal: str,
    *,
    status: str = "in_progress",
    facts: Iterable[str] | None = None,
    decisions: Iterable[str] | None = None,
    progress: str = "",
) -> None:
    normalized = _normalize_role(role)
    ensure_role_memory_scaffold(normalized)
    facts_list = [str(item).strip() for item in (facts or []) if str(item).strip()]
    decisions_list = [str(item).strip() for item in (decisions or []) if str(item).strip()]
    lines = [
        "# Working Memory",
        f"role: {normalized}",
        f"task_id: {task_id}",
        f"status: {status}",
        f"updated_at: {datetime.utcnow().isoformat(timespec='seconds')}",
        "",
        "## 当前目标",
        str(goal or "").strip(),
        "",
        "## 已确认现场事实",
    ]
    if facts_list:
        lines.extend(f"- {item}" for item in facts_list)
    else:
        lines.append("- （待补充）")
    lines.extend(["", "## 中间决策"])
    if decisions_list:
        lines.extend(f"- {item}" for item in decisions_list)
    else:
        lines.append("- （待补充）")
    lines.extend(["", "## 当前进度", progress.strip() or "（待补充）"])
    get_role_working_memory_path(normalized).write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def clear_working_memory(role: str) -> None:
    normalized = _normalize_role(role)
    ensure_role_memory_scaffold(normalized)
    get_role_working_memory_path(normalized).write_text("", encoding="utf-8")


def append_lessons(role: str, entries: Iterable[str]) -> None:
    normalized = _normalize_role(role)
    ensure_role_memory_scaffold(normalized)
    path = get_role_lessons_path(normalized)
    existing = _safe_read(path)
    existing_entries = _extract_tagged_lessons(existing)
    existing_bodies = {re.sub(r"\s+", " ", entry).strip(): True for entry in existing_entries}
    additions: list[str] = []
    for raw in entries:
        entry = str(raw or "").strip()
        if not entry:
            continue
        normalized_entry = re.sub(r"\s+", " ", entry).strip()
        if normalized_entry in existing_bodies:
            continue
        additions.append(entry)
        existing_bodies[normalized_entry] = True
    if not additions:
        return
    prefix = existing.strip()
    if prefix and not prefix.endswith("\n"):
        prefix += "\n\n"
    elif not prefix:
        prefix = f"# {ROLE_FILE_STEMS[normalized].capitalize()} Lessons v1\n\n"
    path.write_text(prefix + "\n\n".join(additions).strip() + "\n", encoding="utf-8")


def should_compress_lessons(role: str, threshold: int = 20) -> bool:
    lessons_text = _safe_read(get_role_lessons_path(role))
    return lessons_text.count("## L-") > threshold


def list_promotion_candidates(role: str, min_occurrences: int = 5) -> list[dict[str, str]]:
    """
    扫描 lessons 文件，找出出现次数达到阈值的候选条目（通过统计相同 Tags 行出现频率）。
    返回候选列表，每项含 lesson_id 和内容摘要，供人工确认晋升。
    """
    normalized = _normalize_role(role)
    lessons_text = _safe_read(get_role_lessons_path(normalized))
    entries = _extract_tagged_lessons(lessons_text)
    tag_counts: dict[str, int] = {}
    for entry in entries:
        for line in entry.splitlines():
            if line.strip().startswith("Tags:"):
                tag_key = re.sub(r"\s+", " ", line).strip()
                tag_counts[tag_key] = tag_counts.get(tag_key, 0) + 1

    candidates = []
    for entry in entries:
        lines = entry.splitlines()
        lesson_id = lines[0].replace("## L-", "").strip() if lines else "unknown"
        tag_line = next((l for l in lines if l.strip().startswith("Tags:")), "")
        tag_key = re.sub(r"\s+", " ", tag_line).strip()
        occurrences = tag_counts.get(tag_key, 1)
        if occurrences >= min_occurrences:
            summary = _clip_text(" | ".join(
                l.strip("- ").strip() for l in lines[2:5] if l.strip()
            ), 160)
            candidates.append({
                "role": normalized,
                "lesson_id": lesson_id,
                "occurrences": str(occurrences),
                "summary": summary,
                "suggested_target": "config" if "param" in tag_line.lower() or "setting" in tag_line.lower() else "seed",
            })
    return candidates


def promote_lesson(role: str, lesson_id: str, target: str, append_to_file: Path) -> bool:
    """
    将指定 lesson 的内容追加到目标文件（seed/config/samples）。
    追加后从 lessons_v1.md 中删除该条目。
    target: 'seed' | 'config' | 'samples'
    append_to_file: 目标文件的绝对路径
    返回 True 表示成功。
    """
    normalized = _normalize_role(role)
    lessons_path = get_role_lessons_path(normalized)
    lessons_text = _safe_read(lessons_path)
    entries = _extract_tagged_lessons(lessons_text)

    target_entry = next((e for e in entries if f"L-{lesson_id}" in e.splitlines()[0]), None)
    if not target_entry:
        return False

    # 追加到目标文件
    existing = _safe_read(append_to_file)
    separator = "\n\n---\n\n" if existing.strip() else ""
    promotion_block = f"<!-- promoted from lessons {datetime.utcnow().strftime('%Y-%m-%d')} -->\n{target_entry}"
    append_to_file.write_text(existing.rstrip() + separator + promotion_block + "\n", encoding="utf-8")

    # 从 lessons 中移除
    remaining = [e for e in entries if f"L-{lesson_id}" not in e.splitlines()[0]]
    stem = ROLE_FILE_STEMS[normalized]
    header = f"# {stem.capitalize()} Lessons v1\n\n"
    lessons_path.write_text(header + "\n\n".join(remaining) + "\n" if remaining else header, encoding="utf-8")
    return True


def compress_lessons(role: str, keep: int = 15) -> None:
    """
    超过阈值时执行压缩：归档当前完整 lessons，只保留最新的 keep 条。
    保留策略：文件末尾的条目是最新的，优先保留。
    """
    normalized = _normalize_role(role)
    ensure_role_memory_scaffold(normalized)
    path = get_role_lessons_path(normalized)
    text = _safe_read(path)
    entries = _extract_tagged_lessons(text)
    if len(entries) <= keep:
        return

    # 归档完整原始内容
    archive_dir = get_role_memory_dir(normalized) / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    (archive_dir / f"lessons_archive_{ts}.md").write_text(text, encoding="utf-8")

    # 保留最新的 keep 条（列表末尾 = 最新）
    kept = entries[-keep:]
    stem = ROLE_FILE_STEMS[normalized]
    header = f"# {stem.capitalize()} Lessons v1\n\n"
    path.write_text(header + "\n\n".join(kept) + "\n", encoding="utf-8")

from __future__ import annotations

from pathlib import Path

from ai_dev_os.tool_bus import list_project_directory


def _relative_items(items: list[str], root: Path) -> list[str]:
    rel: list[str] = []
    for item in items:
        try:
            rel.append(str(Path(item).resolve().relative_to(root.resolve())))
        except Exception:
            rel.append(str(item))
    rel.sort()
    return rel


def summarize_project_runtime(paths: dict[str, str]) -> dict[str, str]:
    project_root = Path(paths["project_root"])
    sections: list[str] = []

    for label, target in (
        ("project_root", project_root),
        ("artifacts", project_root / "artifacts"),
        ("memory", project_root / "memory"),
    ):
        result = list_project_directory(path=target, allowed_root=project_root)
        if not result.get("success"):
            sections.append(f"[{label}] unavailable: {result.get('error', 'unknown error')}")
            continue
        items = _relative_items(result.get("result", []), project_root)[:12]
        lines = "\n".join(f"- {item}" for item in items) if items else "- (empty)"
        sections.append(f"[{label}]\n{lines}")

    combined = "\n\n".join(sections)
    return {
        "project_root": str(project_root),
        "directory_summary": combined,
    }

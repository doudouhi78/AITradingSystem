from __future__ import annotations

from typing import Any


def build_feedback_signal(
    *,
    source: str,
    grade: str,
    light: str,
    summary: str,
    recommended_action: str,
    should_interrupt: bool,
    details: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "grade": grade,
        "light": light,
        "summary": summary,
        "recommended_action": recommended_action,
        "should_interrupt": should_interrupt,
        "details": details or [],
    }


def classify_reviewer_feedback(*, decision: str, risk_level: str, issues: list[str], validation_gaps: list[str]) -> dict[str, Any]:
    if decision == "changes_requested":
        return build_feedback_signal(
            source="reviewer",
            grade="B",
            light="yellow",
            summary="Reviewer requested another iteration before continuation.",
            recommended_action="Route to human confirmation first, then return to builder only if rework is confirmed.",
            should_interrupt=False,
            details=[*issues[:3], *validation_gaps[:3]],
        )
    if risk_level == "high":
        return build_feedback_signal(
            source="reviewer",
            grade="A",
            light="red",
            summary="Reviewer marked the task as high risk and governance escalation is required.",
            recommended_action="Route into approval gate before record.",
            should_interrupt=True,
            details=[*issues[:3], *validation_gaps[:3]],
        )
    if issues or validation_gaps:
        return build_feedback_signal(
            source="reviewer",
            grade="B",
            light="yellow",
            summary="Reviewer approved with caution signals that should be tracked.",
            recommended_action="Continue on the mainline, keep caution signals visible, and record evidence debt for later cleanup.",
            should_interrupt=False,
            details=[*issues[:3], *validation_gaps[:3]],
        )
    return build_feedback_signal(
        source="reviewer",
        grade="C",
        light="green",
        summary="Reviewer found no blocking concerns.",
        recommended_action="Continue on the mainline.",
        should_interrupt=False,
        details=[],
    )


def classify_validator_feedback(*, validation_status: str, issue_lines: list[str]) -> dict[str, Any]:
    if validation_status == "changes_requested":
        return build_feedback_signal(
            source="validator",
            grade="A",
            light="red",
            summary="Validator found policy or evidence issues that block progression.",
            recommended_action="Stop progression and remediate before continuing.",
            should_interrupt=True,
            details=issue_lines[:4],
        )
    if issue_lines:
        return build_feedback_signal(
            source="validator",
            grade="B",
            light="yellow",
            summary="Validator approved but still detected non-blocking concerns.",
            recommended_action="Continue with caution and preserve evidence in memory.",
            should_interrupt=False,
            details=issue_lines[:4],
        )
    return build_feedback_signal(
        source="validator",
        grade="C",
        light="green",
        summary="Validator found no policy or evidence problems.",
        recommended_action="Continue on the mainline.",
        should_interrupt=False,
        details=[],
    )


def classify_observer_feedback(*, observations: list[str], improvements: list[dict[str, Any]], approval_pending: bool) -> dict[str, Any]:
    if approval_pending:
        return build_feedback_signal(
            source="observer",
            grade="A",
            light="red",
            summary="Observer sees the run blocked at an approval gate.",
            recommended_action="Keep the task paused until a human decision is made.",
            should_interrupt=True,
            details=observations[:3],
        )
    if improvements:
        return build_feedback_signal(
            source="observer",
            grade="B",
            light="yellow",
            summary="Observer found system-level improvement opportunities.",
            recommended_action="Route findings into the optimization inbox without interrupting the current mainline.",
            should_interrupt=False,
            details=observations[:3],
        )
    return build_feedback_signal(
        source="observer",
        grade="C",
        light="green",
        summary="Observer found no notable system-level drift in this run.",
        recommended_action="Continue normal operation.",
        should_interrupt=False,
        details=observations[:3],
    )


def classify_release_feedback(*, recommendation: str, dirty_count: int, core_change_count: int) -> dict[str, Any]:
    if recommendation == "hold_release":
        return build_feedback_signal(
            source="release",
            grade="A",
            light="red",
            summary="Release advisor recommends holding release actions.",
            recommended_action="Do not tag or release before review and cleanup.",
            should_interrupt=True,
            details=[f"dirty_count={dirty_count}", f"core_change_count={core_change_count}"],
        )
    if recommendation == "review_before_release":
        return build_feedback_signal(
            source="release",
            grade="B",
            light="yellow",
            summary="Release advisor recommends a review before any release action.",
            recommended_action="Validate mainline changes before tagging or release review.",
            should_interrupt=False,
            details=[f"dirty_count={dirty_count}", f"core_change_count={core_change_count}"],
        )
    return build_feedback_signal(
        source="release",
        grade="C",
        light="green",
        summary="Release advisor sees no immediate release blockers.",
        recommended_action="No special release action required right now.",
        should_interrupt=False,
        details=[f"dirty_count={dirty_count}", f"core_change_count={core_change_count}"],
    )

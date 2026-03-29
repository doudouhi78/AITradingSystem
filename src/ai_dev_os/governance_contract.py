from __future__ import annotations


def build_governance_contract(goal: str, task_profile: str) -> dict[str, object]:
    if task_profile != "governance_sensitive":
        return {}

    goal_lower = goal.lower()
    is_export = "export" in goal_lower
    is_read = "read" in goal_lower
    diagnostics_focused = "diagnostic" in goal_lower
    approval_focused = "approval" in goal_lower
    schema_focused = "schema" in goal_lower
    memory_focused = "memory" in goal_lower
    retrieval_focused = "retrieval" in goal_lower or "retrieve" in goal_lower or "semantic" in goal_lower

    if is_export and memory_focused:
        contract_subtype = "policy_export"
        allowed_roots = ["/runtime/projects/*/memory/"]
        exact_denied_roots = ["/mother_memory/"]
        denied_path_tokens = ["doctrine/"]
        positive_tests = [
            "Export of project-local runtime memory snapshot succeeds from an allowed memory root.",
            "Export request fails when the canonical path is outside the allowed project memory root.",
        ]
        negative_tests = [
            "Access to /mother_memory/ root is denied after canonicalization.",
            "Any canonical path containing doctrine/ is denied.",
            "Path traversal using ../ is denied after canonicalization.",
        ]
        canonicalization_expectation = (
            "Normalize to a canonical absolute path before matching exact allowed roots, exact denied roots, and denied path tokens."
        )
        strict_binding_fields = []
        family_semantics = "policy_export defines which project-local runtime memory surfaces may be exported and which protected global memory surfaces must never be exported."
        approved_surface_definition = "Project-local runtime memory export surfaces only."
        disallowed_surface_definition = "Mother memory and doctrine-related global surfaces are always outside export scope."
    elif memory_focused and retrieval_focused:
        contract_subtype = "policy_memory_retrieval"
        allowed_roots = ["/mother_memory/", "/doctrine/"]
        exact_denied_roots = ["/runtime/projects/*/artifacts/"]
        denied_path_tokens = ["../"]
        positive_tests = [
            "Read-only retrieval from relevant mother_memory documents is allowed after canonicalization.",
            "Read-only retrieval from doctrine guidance files is allowed when the task is explicitly about mother-memory retrieval depth.",
        ]
        negative_tests = [
            "Retrieval task does not write to mother_memory or doctrine paths.",
            "Retrieval task does not use runtime artifact roots as its primary retrieval surface.",
            "Path traversal using ../ is denied after canonicalization.",
        ]
        canonicalization_expectation = (
            "Canonicalize retrieval sources to canonical absolute paths, allow only read-only reference access to mother_memory and doctrine surfaces, and keep all write targets project-local."
        )
        strict_binding_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
        ]
        family_semantics = "policy_memory_retrieval defines read-only retrieval from mother_memory and doctrine guidance for retrieval-depth work. It does not permit writing to those surfaces."
        approved_surface_definition = "Read-only reference access to mother_memory and doctrine surfaces is allowed for retrieval-depth tasks. Any write target remains project-local."
        disallowed_surface_definition = "Runtime artifact roots are outside retrieval scope, and mother_memory/doctrine are never writable surfaces."
    elif diagnostics_focused or is_read:
        contract_subtype = "policy_read"
        allowed_roots = ["/runtime/projects/*/diagnostics/"]
        exact_denied_roots = ["/mother_memory/"]
        denied_path_tokens = ["doctrine/"]
        positive_tests = [
            "Read of project-local diagnostics path succeeds from the canonical diagnostics root.",
            "Read of an allowed diagnostics child path succeeds after canonicalization.",
        ]
        negative_tests = [
            "Read of any canonical path outside diagnostics root is denied.",
            "Access to /mother_memory/ root is denied after canonicalization.",
            "Any canonical path containing doctrine/ is denied.",
            "Path traversal using ../ is denied after canonicalization.",
        ]
        canonicalization_expectation = (
            "Resolve the request to a canonical absolute path, then require the canonical path to stay under the diagnostics root before any read is allowed."
        )
        strict_binding_fields = []
        family_semantics = "policy_read defines which project-local diagnostics surfaces may be read after canonicalization."
        approved_surface_definition = "Project-local diagnostics read surfaces only."
        disallowed_surface_definition = "Mother memory, doctrine-related paths, and non-diagnostics runtime paths are outside read scope."
    elif approval_focused:
        contract_subtype = "policy_approval"
        allowed_roots = ["/runtime/projects/*/memory/policies/approval/"]
        exact_denied_roots = ["/mother_memory/", "/runtime/projects/*/artifacts/"]
        denied_path_tokens = ["doctrine/", "design_principles/"]
        positive_tests = [
            "Approval-policy metadata update succeeds only inside the approved project-local approval-policy metadata root.",
            "Approval-policy metadata update does not modify project artifacts or mother memory.",
        ]
        negative_tests = [
            "Approval-policy flow does not mutate project artifact paths.",
            "Approval-policy flow does not modify mother memory paths.",
            "Approval-policy flow does not modify doctrine or design-principles paths.",
        ]
        canonicalization_expectation = (
            "Canonicalize any referenced policy path to a canonical absolute path, then require the final write target to stay under the project-local approval-policy metadata root only."
        )
        strict_binding_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
            "denied_path_tokens",
        ]
        family_semantics = "policy_approval defines project-local approval-policy metadata only. It is a project-memory policy task, not a runtime artifact task and not a mother-memory or doctrine-editing task."
        approved_surface_definition = "Only project-local approval-policy metadata under /runtime/projects/*/memory/policies/approval/ may be updated."
        disallowed_surface_definition = "Mother memory, runtime project artifacts, doctrine paths, and design-principles surfaces are all outside approval-policy scope."
    elif schema_focused:
        contract_subtype = "policy_schema"
        allowed_roots = ["/runtime/projects/*/memory/policies/schema/"]
        exact_denied_roots = ["/mother_memory/", "/runtime/projects/*/artifacts/"]
        denied_path_tokens = ["doctrine/", "mother_memory/"]
        positive_tests = [
            "Schema-policy metadata is written only under the approved project-local schema-policy root.",
            "Schema-policy metadata references only approved configuration schema surfaces: /config/governance.json or /config/agents.json.",
        ]
        negative_tests = [
            "Schema-policy task does not write to project artifact paths.",
            "Schema-policy task does not modify doctrine files.",
            "Schema-policy task does not write to mother_memory paths.",
        ]
        canonicalization_expectation = (
            "Canonicalize any schema-policy metadata path to a canonical absolute path, then require the final write target to stay under the approved project-local schema-policy root."
        )
        strict_binding_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
            "denied_path_tokens",
        ]
        family_semantics = "policy_schema defines project-local schema-policy metadata that may reference approved mother-template config schema surfaces. It is a project-local policy task, not a direct config-editing task."
        approved_surface_definition = "Only project-local schema-policy metadata under /runtime/projects/*/memory/policies/schema/ may be written. The only approved referenced config schema surfaces are /config/governance.json and /config/agents.json."
        disallowed_surface_definition = "Mother memory, runtime project artifacts, doctrine paths, and any unapproved config surfaces are outside schema-policy scope."
        required_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
            "denied_path_tokens",
            "canonicalization_rule",
            "negative_tests",
            "positive_tests",
            "policy_document_write_surface",
            "referenced_read_only_schema_surfaces",
        ]
        policy_document_write_surface_hint = "/runtime/projects/*/memory/policies/schema/"
        referenced_read_only_schema_surfaces_hint = [
            "/config/governance.json",
            "/config/agents.json",
        ]
    else:
        contract_subtype = "policy_general"
        allowed_roots = ["/runtime/projects/*/"]
        exact_denied_roots = ["/mother_memory/"]
        denied_path_tokens = ["doctrine/"]
        positive_tests = [
            "Project-local allowed path succeeds under canonical matching.",
        ]
        negative_tests = [
            "Access to /mother_memory/ root is denied after canonicalization.",
            "Any canonical path containing doctrine/ is denied.",
            "Path traversal using ../ is denied after canonicalization.",
        ]
        canonicalization_expectation = (
            "Normalize to a canonical absolute path before allow/deny matching."
        )
        strict_binding_fields = []
        family_semantics = "policy_general defines bounded project-local governance behavior when no narrower contract family applies."
        approved_surface_definition = "Project-local governed surfaces only."
        disallowed_surface_definition = "Mother memory and doctrine-related global surfaces remain outside scope."
        required_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
            "denied_path_tokens",
            "canonicalization_rule",
            "negative_tests",
            "positive_tests",
        ]
        policy_document_write_surface_hint = ""
        referenced_read_only_schema_surfaces_hint: list[str] = []

    if contract_subtype != "policy_schema":
        required_fields = [
            "exact_allowed_roots",
            "exact_denied_roots",
            "denied_path_tokens",
            "canonicalization_rule",
            "negative_tests",
            "positive_tests",
        ]
        policy_document_write_surface_hint = ""
        referenced_read_only_schema_surfaces_hint = []

    return {
        "contract_type": "governance_policy",
        "contract_subtype": contract_subtype,
        "required_fields": required_fields,
        "exact_allowed_roots_hint": allowed_roots,
        "exact_denied_roots_hint": exact_denied_roots,
        "denied_path_tokens_hint": denied_path_tokens,
        "canonicalization_expectation": canonicalization_expectation,
        "negative_test_expectations": negative_tests,
        "positive_test_expectations": positive_tests,
        "strict_binding_fields": strict_binding_fields,
        "family_semantics": family_semantics,
        "approved_surface_definition": approved_surface_definition,
        "disallowed_surface_definition": disallowed_surface_definition,
        "policy_document_write_surface_hint": policy_document_write_surface_hint,
        "referenced_read_only_schema_surfaces_hint": referenced_read_only_schema_surfaces_hint,
    }

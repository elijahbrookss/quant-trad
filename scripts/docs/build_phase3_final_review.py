#!/usr/bin/env python3
"""Build QT's non-normative Phase 3 final-review assessment.

The frozen Phase 2 registry and remediation records remain immutable inputs.
This module layers a commit-specific forward assessment over those inputs and
optionally binds already-created immutable attestations. It never executes a
proof, changes guarantee activation, or treats remediation progress as closure.

Strict final-gate packets bind proof source commit S plus the clean packet-input
commit/tree P. The generated packet cannot truthfully contain the identity of
its own future Git commit C, so ``check --final-gate`` verifies C externally:
clean branch state, single parent P, only the generated JSON/Markdown pair, and
unchanged frozen remote ``develop`` integration base. Post-commit validation is
likewise reported externally.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, NoReturn, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.docs import guarantees  # noqa: E402


CAMPAIGN_DIR = ROOT / "docs" / "plans" / "documentation-reconciliation"
POLICY_PATH = CAMPAIGN_DIR / "phase-3-final-review-policy.json"
REVIEW_PATH = CAMPAIGN_DIR / "phase-3-final-review.json"
VIEW_PATH = CAMPAIGN_DIR / "phase-3-final-review.md"
SCHEMA_PATH = (
    ROOT
    / "docs"
    / "assurance"
    / "guarantees"
    / "schemas"
    / "phase-3-final-review.v1.schema.json"
)

POLICY_SCHEMA_VERSION = "qt.phase3_final_review_policy.v1"
REVIEW_SCHEMA_VERSION = "qt.phase3_final_review.v1"
VALIDATION_RESULTS_SCHEMA_VERSION = "qt.phase3_validation_results.v1"
HEX40_RE = re.compile(r"[0-9a-f]{40}\Z")
HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
VALIDATION_ID_RE = re.compile(r"[a-z][a-z0-9-]*\Z")
REMEDIATION_ID_RE = re.compile(r"QT-REM-[0-9]{3}\Z")
RISK_ID_RE = re.compile(r"QT-RISK-P3-[0-9]{3}\Z")
SLICE_ID_RE = re.compile(r"QT-P3-SLICE-[A-Z0-9-]+\Z")

ATTESTATION_STATUSES = {
    "PASS",
    "FAIL",
    "NOT_RUN",
    "MANUAL",
    "PARTIAL",
    "UNAVAILABLE",
}
ACTION_OUTCOMES = {"implemented", "planned", "deferred", "blocked", "unavailable"}
ACCEPTANCE_STATES = {"not_assessed", "partial", "met", "contradicted"}
PHASE2_DISPOSITIONS = {
    "retain_phase3",
    "split_resolution_execution",
    "defer_activation_priority",
    "defer_owner_approval",
    "defer_proof_environment",
}
VALIDATION_STATUSES = {
    "PASS",
    "FAIL",
    "PARTIAL",
    "MANUAL",
    "NOT_RUN",
    "UNAVAILABLE",
}
FINAL_GATE_PACKET_FILES = {
    REVIEW_PATH.relative_to(ROOT).as_posix(),
    VIEW_PATH.relative_to(ROOT).as_posix(),
}
DEVELOP_REMOTE = "origin"
DEVELOP_REMOTE_REF = "refs/heads/develop"
DEVELOP_TRACKING_REF = "refs/remotes/origin/develop"
SOURCE_MATERIAL_PATHS = {
    POLICY_PATH.relative_to(ROOT).as_posix(),
    guarantees.REGISTRY_PATH.relative_to(guarantees.ROOT).as_posix(),
    guarantees.PROOF_CATALOG_PATH.relative_to(guarantees.ROOT).as_posix(),
}


class FinalReviewError(ValueError):
    """Raised when the Phase 3 assessment is incomplete or inconsistent."""


def _fail(message: str) -> NoReturn:
    raise FinalReviewError(message)


def _expect_object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{where}:expected_object")
    return value


def _expect_list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"{where}:expected_array")
    return value


def _expect_string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(f"{where}:expected_nonempty_string")
    return value


def _expect_int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(f"{where}:expected_nonnegative_integer")
    return value


def _exact_keys(
    value: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str] | None = None,
    where: str,
) -> None:
    allowed = required | (optional or set())
    missing = sorted(required - set(value))
    extra = sorted(set(value) - allowed)
    if missing:
        _fail(f"{where}:missing_keys:{','.join(missing)}")
    if extra:
        _fail(f"{where}:unknown_keys:{','.join(extra)}")


def _id_sort_key(value: str) -> tuple[str]:
    return (value,)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode("utf-8")


def _schema_value(
    schema: Mapping[str, Any], path: Sequence[str], where: str
) -> Any:
    current: Any = schema
    for key in path:
        if not isinstance(current, dict) or key not in current:
            _fail(f"{where}:missing_schema_path:{'.'.join(path)}")
        current = current[key]
    return current


def _schema_set(
    schema: Mapping[str, Any],
    path: Sequence[str],
    expected: set[str],
    where: str,
) -> None:
    raw = _schema_value(schema, path, where)
    if not isinstance(raw, list) or set(raw) != expected or len(raw) != len(expected):
        _fail(f"{where}:schema_set_mismatch:{'.'.join(path)}")


def _validate_review_schema_contract(root: Path) -> None:
    """Keep the published schema aligned with the executable review model."""

    schema = guarantees.load_json_strict(root / SCHEMA_PATH.relative_to(ROOT))
    where = "schema.phase3_final_review"
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        _fail(f"{where}:expected_draft_2020_12")
    if schema.get("additionalProperties") is not False:
        _fail(f"{where}:top_level_must_be_strict")
    if _schema_value(schema, ("properties", "schema_version", "const"), where) != REVIEW_SCHEMA_VERSION:
        _fail(f"{where}:schema_version_mismatch")
    _schema_set(
        schema,
        ("required",),
        {
            "schema_version",
            "authority",
            "frozen_evidence",
            "assessment_subject",
            "attestation_binding",
            "attestation_results",
            "summary",
            "system_summary",
            "phase3_slices",
            "guarantees",
            "proof_accounting",
            "proofs",
            "remediations",
            "residual_risks",
            "integration_plan",
        },
        where,
    )
    for name, count in (("guarantees", 75), ("proofs", 85), ("remediations", 68)):
        if _schema_value(schema, ("properties", name, "minItems"), where) != count or _schema_value(
            schema, ("properties", name, "maxItems"), where
        ) != count:
            _fail(f"{where}:{name}_cardinality_mismatch")
    _schema_set(
        schema,
        ("$defs", "validationResult", "properties", "status", "enum"),
        VALIDATION_STATUSES,
        where,
    )
    _schema_set(
        schema,
        ("$defs", "finalGate", "properties", "mode", "enum"),
        {"intermediate", "final_gate"},
        where,
    )


def _repo_relative(root: Path, path: Path, where: str) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        _fail(f"{where}:path_outside_repository")


def _verify_git_commit(root: Path, source_commit: str) -> None:
    if not HEX40_RE.fullmatch(source_commit):
        _fail("assessment_subject.source_commit:expected_lowercase_40_hex")
    if not (root / ".git").exists():
        return
    try:
        subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", f"{source_commit}^{{commit}}"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"assessment_subject.source_commit:not_available:{exc}")


def _git_text(root: Path, *args: str, where: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"{where}:git_failed:{exc}")
    return completed.stdout.strip()


def _remote_develop_commit(root: Path, where: str) -> str:
    """Return the authoritative remote develop commit and reject stale tracking data."""

    observed = _git_text(
        root,
        "ls-remote",
        "--exit-code",
        DEVELOP_REMOTE,
        DEVELOP_REMOTE_REF,
        where=f"{where}.origin",
    )
    rows = [line.split() for line in observed.splitlines() if line.strip()]
    if (
        len(rows) != 1
        or len(rows[0]) != 2
        or rows[0][1] != DEVELOP_REMOTE_REF
        or not HEX40_RE.fullmatch(rows[0][0])
    ):
        _fail(f"{where}.origin:unexpected_ref_response")
    remote_commit = rows[0][0]
    tracking_commit = _git_text(
        root,
        "rev-parse",
        DEVELOP_TRACKING_REF,
        where=f"{where}.tracking",
    )
    if tracking_commit != remote_commit:
        _fail(
            f"{where}.tracking:stale:"
            f"remote={remote_commit}:tracking={tracking_commit}"
        )
    return remote_commit


def _git_blob(root: Path, source_commit: str, relative: str, where: str) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "show", f"{source_commit}:{relative}"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"{where}:source_material_unavailable:{relative}:{exc}")
    return completed.stdout


def _source_material_binding(root: Path, source_commit: str) -> list[dict[str, str]]:
    """Bind the assessment semantics to exact material at ``source_commit``."""

    if not (root / ".git").exists():
        _fail("assessment_subject.source_material:git_metadata_required")
    relative_paths = sorted(SOURCE_MATERIAL_PATHS)
    bindings: list[dict[str, str]] = []
    for relative in relative_paths:
        where = f"assessment_subject.source_material.{relative}"
        current = root / relative
        if not current.is_file():
            _fail(f"{where}:current_material_missing")
        current_bytes = current.read_bytes()
        source_bytes = _git_blob(root, source_commit, relative, where)
        current_sha = hashlib.sha256(current_bytes).hexdigest()
        source_sha = hashlib.sha256(source_bytes).hexdigest()
        if current_sha != source_sha:
            _fail(
                f"{where}:exact_source_mismatch:"
                f"source={source_sha}:current={current_sha}"
            )
        bindings.append({"path": relative, "sha256": source_sha})
    return bindings


def _is_ancestor(root: Path, ancestor: str, descendant: str, where: str) -> None:
    try:
        subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"{where}:not_ancestor:{ancestor}:{descendant}:{exc}")


def _capture_packet_input_state(
    root: Path,
    source_commit: str,
    expected_develop_commit: str,
) -> dict[str, Any]:
    """Capture the clean parent/tree from which a self-excluding packet is rendered."""

    branch = _git_text(root, "symbolic-ref", "--short", "HEAD", where="final_gate.branch")
    if branch == "develop":
        _fail("final_gate.branch:develop_is_not_an_authorized_packet_branch")
    head_commit = _git_text(root, "rev-parse", "HEAD", where="final_gate.head")
    tree_oid = _git_text(root, "rev-parse", "HEAD^{tree}", where="final_gate.tree")
    develop_commit = _remote_develop_commit(root, "final_gate.develop_remote")
    if develop_commit != expected_develop_commit:
        _fail(
            "final_gate.develop_remote:not_frozen_baseline:"
            f"expected={expected_develop_commit}:observed={develop_commit}"
        )
    dirty = _git_text(
        root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        where="final_gate.clean",
    )
    if dirty:
        _fail("final_gate.repository_state:packet_input_worktree_not_clean")
    _is_ancestor(
        root,
        source_commit,
        head_commit,
        "final_gate.repository_state.proof_source",
    )
    return {
        "branch": branch,
        "packet_input_commit": head_commit,
        "packet_input_tree": tree_oid,
        "develop_commit": develop_commit,
        "clean": True,
    }


def _validate_recorded_packet_input(
    root: Path,
    source_commit: str,
    state: Mapping[str, Any],
    expected_develop_commit: str,
) -> None:
    _exact_keys(
        state,
        required={
            "branch",
            "packet_input_commit",
            "packet_input_tree",
            "develop_commit",
            "clean",
        },
        where="final_gate.repository_state",
    )
    branch = _expect_string(state["branch"], "final_gate.repository_state.branch")
    if branch == "develop":
        _fail("final_gate.repository_state.branch:develop_forbidden")
    for key in ("packet_input_commit", "packet_input_tree", "develop_commit"):
        if not HEX40_RE.fullmatch(
            _expect_string(state[key], f"final_gate.repository_state.{key}")
        ):
            _fail(f"final_gate.repository_state.{key}:expected_40_hex_oid")
    if state["clean"] is not True:
        _fail("final_gate.repository_state.clean:must_be_true")
    if state["develop_commit"] != expected_develop_commit:
        _fail("final_gate.repository_state.develop_commit:not_frozen_baseline")
    _verify_git_commit(root, state["packet_input_commit"])
    _verify_git_commit(root, state["develop_commit"])
    observed_tree = _git_text(
        root,
        "rev-parse",
        f"{state['packet_input_commit']}^{{tree}}",
        where="final_gate.repository_state.packet_input_tree",
    )
    if observed_tree != state["packet_input_tree"]:
        _fail("final_gate.repository_state.packet_input_tree:mismatch")
    _is_ancestor(
        root,
        source_commit,
        state["packet_input_commit"],
        "final_gate.repository_state.proof_source",
    )


def _verify_external_approval_repository_state(
    root: Path,
    state: Mapping[str, Any],
) -> dict[str, str | bool]:
    """Verify the clean packet commit externally, avoiding commit self-reference."""

    current_branch = _git_text(
        root, "symbolic-ref", "--short", "HEAD", where="approval_gate.branch"
    )
    if current_branch != state["branch"]:
        _fail("approval_gate.branch:recorded_branch_mismatch")
    current_head = _git_text(root, "rev-parse", "HEAD", where="approval_gate.head")
    current_tree = _git_text(
        root, "rev-parse", "HEAD^{tree}", where="approval_gate.tree"
    )
    dirty = _git_text(
        root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        where="approval_gate.clean",
    )
    if dirty:
        _fail("approval_gate.clean:worktree_not_clean")
    if _remote_develop_commit(
        root, "approval_gate.develop_remote"
    ) != state["develop_commit"]:
        _fail("approval_gate.develop_remote:changed_since_packet_input")
    parents = _git_text(
        root, "show", "-s", "--format=%P", current_head, where="approval_gate.parent"
    ).split()
    if parents != [state["packet_input_commit"]]:
        _fail("approval_gate.parent:head_must_be_single_packet_commit")
    changed = _git_text(
        root,
        "diff-tree",
        "--no-commit-id",
        "--name-only",
        "-r",
        current_head,
        where="approval_gate.packet_files",
    ).splitlines()
    if set(changed) != FINAL_GATE_PACKET_FILES:
        _fail("approval_gate.packet_files:expected_only_generated_review_pair")
    return {
        "branch": current_branch,
        "head_commit": current_head,
        "head_tree": current_tree,
        "clean": True,
        "packet_parent_commit": state["packet_input_commit"],
        "develop_commit": state["develop_commit"],
    }


def _parse_utc_timestamp(value: Any, where: str) -> datetime:
    text = _expect_string(value, where)
    if not text.endswith("Z"):
        _fail(f"{where}:timestamp_must_be_utc_Z")
    try:
        return datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        _fail(f"{where}:invalid_timestamp:{exc}")


def _load_validation_results(
    *,
    root: Path,
    source_commit: str,
    path: Path,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    resolved = path.resolve()
    relative = _repo_relative(root, resolved, "final_gate.validation_results_source")
    prefix = "docs/assurance/guarantees/validation-results/"
    if not relative.startswith(prefix) or not relative.endswith(".json"):
        _fail("final_gate.validation_results_source:path_outside_immutable_layout")
    raw = guarantees.load_json_strict(resolved)
    _exact_keys(
        raw,
        required={"schema_version", "source_commit", "results"},
        where="final_gate.validation_results_document",
    )
    if raw["schema_version"] != VALIDATION_RESULTS_SCHEMA_VERSION:
        _fail("final_gate.validation_results_document.schema_version:invalid")
    if raw["source_commit"] != source_commit:
        _fail("final_gate.validation_results_document.source_commit:mismatch")
    validated = _validate_embedded_validation_results(raw["results"])
    for index, row in enumerate(validated):
        for evidence_index, evidence_row in enumerate(row["evidence_refs"]):
            evidence_where = (
                f"final_gate.validation_results[{index}]."
                f"evidence_refs[{evidence_index}]"
            )
            evidence_path = evidence_row["path"]
            resolved_evidence = (root / evidence_path).resolve()
            if _repo_relative(root, resolved_evidence, evidence_where) != evidence_path:
                _fail(f"{evidence_where}.path:not_canonical")
            if (
                not resolved_evidence.is_file()
                or _sha256(resolved_evidence) != evidence_row["sha256"]
            ):
                _fail(f"{evidence_where}.sha256:mismatch")
    return validated, {"path": relative, "sha256": _sha256(resolved)}


def _verify_slice_evidence(root: Path, source_commit: str, slices: Sequence[Mapping[str, Any]]) -> None:
    if not (root / ".git").exists():
        return
    for slice_row in slices:
        slice_id = slice_row["id"]
        for commit in slice_row["commits"]:
            try:
                subprocess.run(
                    [
                        "git",
                        "-C",
                        str(root),
                        "merge-base",
                        "--is-ancestor",
                        commit,
                        source_commit,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except (OSError, subprocess.CalledProcessError) as exc:
                _fail(f"{slice_id}:commit_not_in_source_lineage:{commit}:{exc}")
        for repo_path in slice_row["files"]:
            try:
                subprocess.run(
                    [
                        "git",
                        "-C",
                        str(root),
                        "cat-file",
                        "-e",
                        f"{source_commit}:{repo_path}",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except (OSError, subprocess.CalledProcessError) as exc:
                _fail(f"{slice_id}:file_missing_at_source:{repo_path}:{exc}")


def _read_remediation_frontmatter(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        _fail(f"{path.as_posix()}:missing_frontmatter")
    result: dict[str, str] = {}
    for line in lines[1:]:
        if line == "---":
            return result
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition(":")
        if not separator or not key.strip() or not value.strip():
            _fail(f"{path.as_posix()}:invalid_frontmatter_line:{line}")
        key = key.strip()
        if key in result:
            _fail(f"{path.as_posix()}:duplicate_frontmatter_key:{key}")
        result[key] = value.strip()
    _fail(f"{path.as_posix()}:unterminated_frontmatter")


def _validate_partition(
    raw: Mapping[str, Any],
    *,
    expected_ids: set[str],
    allowed_groups: set[str],
    where: str,
) -> dict[str, str]:
    _exact_keys(raw, required=allowed_groups, where=where)
    assignment: dict[str, str] = {}
    for group in sorted(allowed_groups):
        ids = _expect_list(raw[group], f"{where}.{group}")
        normalized = [_expect_string(item, f"{where}.{group}[]") for item in ids]
        if normalized != sorted(normalized, key=_id_sort_key):
            _fail(f"{where}.{group}:must_be_sorted")
        if len(normalized) != len(set(normalized)):
            _fail(f"{where}.{group}:duplicate_id")
        for identifier in normalized:
            if identifier in assignment:
                _fail(f"{where}:id_in_multiple_groups:{identifier}")
            assignment[identifier] = group
    if set(assignment) != expected_ids:
        missing = sorted(expected_ids - set(assignment))
        extra = sorted(set(assignment) - expected_ids)
        _fail(
            f"{where}:partition_mismatch:missing={','.join(missing)}:"
            f"extra={','.join(extra)}"
        )
    return assignment


def _validate_policy(
    policy: Mapping[str, Any],
    *,
    guarantee_ids: set[str],
    remediation_ids: set[str],
    proof_ids: set[str],
) -> None:
    _exact_keys(
        policy,
        required={
            "schema_version",
            "frozen_evidence",
            "expected_summary",
            "forward_overrides",
            "phase2_dispositions",
            "remediation_outcomes",
            "acceptance_overrides",
            "deferred_terms",
            "phase3_slices",
            "residual_risks",
            "system_summary",
            "integration_plan",
        },
        where="policy",
    )
    if policy["schema_version"] != POLICY_SCHEMA_VERSION:
        _fail(f"policy.schema_version:expected:{POLICY_SCHEMA_VERSION}")

    frozen = _expect_object(policy["frozen_evidence"], "policy.frozen_evidence")
    _exact_keys(
        frozen,
        required={
            "audit_baseline_commit",
            "classification_commit",
            "decision_packet_commit",
            "registry_path",
            "registry_sha256",
        },
        where="policy.frozen_evidence",
    )
    for key in ("audit_baseline_commit", "classification_commit", "decision_packet_commit"):
        if not HEX40_RE.fullmatch(_expect_string(frozen[key], f"policy.frozen_evidence.{key}")):
            _fail(f"policy.frozen_evidence.{key}:expected_lowercase_40_hex")
    if not HEX64_RE.fullmatch(
        _expect_string(frozen["registry_sha256"], "policy.frozen_evidence.registry_sha256")
    ):
        _fail("policy.frozen_evidence.registry_sha256:expected_lowercase_64_hex")

    expected = _expect_object(policy["expected_summary"], "policy.expected_summary")
    _exact_keys(
        expected,
        required={
            "guarantees",
            "remediations",
            "proof_definitions",
            "forward_dispositions",
            "forward_conformance",
            "remediation_outcomes",
        },
        where="policy.expected_summary",
    )

    overrides = _expect_object(policy["forward_overrides"], "policy.forward_overrides")
    _exact_keys(
        overrides,
        required={
            "registry_disposition",
            "conformance",
            "current_claim_overlay_required",
        },
        where="policy.forward_overrides",
    )
    for field in ("registry_disposition", "conformance"):
        mapping = _expect_object(overrides[field], f"policy.forward_overrides.{field}")
        unknown = sorted(set(mapping) - guarantee_ids)
        if unknown:
            _fail(f"policy.forward_overrides.{field}:unknown_guarantees:{','.join(unknown)}")
    overlays = _expect_list(
        overrides["current_claim_overlay_required"],
        "policy.forward_overrides.current_claim_overlay_required",
    )
    if overlays != sorted(overlays, key=_id_sort_key) or not set(overlays) <= guarantee_ids:
        _fail("policy.forward_overrides.current_claim_overlay_required:invalid_set")

    _validate_partition(
        _expect_object(policy["phase2_dispositions"], "policy.phase2_dispositions"),
        expected_ids=remediation_ids,
        allowed_groups=PHASE2_DISPOSITIONS,
        where="policy.phase2_dispositions",
    )
    _validate_partition(
        _expect_object(policy["remediation_outcomes"], "policy.remediation_outcomes"),
        expected_ids=remediation_ids,
        allowed_groups=ACTION_OUTCOMES,
        where="policy.remediation_outcomes",
    )
    acceptance = _expect_object(policy["acceptance_overrides"], "policy.acceptance_overrides")
    if not set(acceptance) <= remediation_ids:
        _fail("policy.acceptance_overrides:unknown_remediation")
    for remediation_id, state in acceptance.items():
        if state not in ACCEPTANCE_STATES:
            _fail(f"policy.acceptance_overrides.{remediation_id}:invalid:{state}")

    deferred_term_ids: set[str] = set()
    for index, raw in enumerate(_expect_list(policy["deferred_terms"], "policy.deferred_terms")):
        where = f"policy.deferred_terms[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={"term_id", "conflict_id", "guarantee_ids"},
            where=where,
        )
        term_id = _expect_string(row["term_id"], f"{where}.term_id")
        if term_id in deferred_term_ids:
            _fail(f"{where}.term_id:duplicate:{term_id}")
        deferred_term_ids.add(term_id)
        affected = _expect_list(row["guarantee_ids"], f"{where}.guarantee_ids")
        if affected != sorted(affected, key=_id_sort_key) or not set(affected) <= guarantee_ids:
            _fail(f"{where}.guarantee_ids:invalid_set")

    slice_ids: set[str] = set()
    for index, raw in enumerate(_expect_list(policy["phase3_slices"], "policy.phase3_slices")):
        where = f"policy.phase3_slices[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "id",
                "title",
                "commits",
                "files",
                "guarantee_ids",
                "remediation_ids",
                "effect",
            },
            optional={"guarantee_selector", "remediation_selector"},
            where=where,
        )
        slice_id = _expect_string(row["id"], f"{where}.id")
        if not SLICE_ID_RE.fullmatch(slice_id) or slice_id in slice_ids:
            _fail(f"{where}.id:invalid_or_duplicate:{slice_id}")
        slice_ids.add(slice_id)
        commits = _expect_list(row["commits"], f"{where}.commits")
        if not commits or any(
            not HEX40_RE.fullmatch(_expect_string(commit, f"{where}.commits[]"))
            for commit in commits
        ):
            _fail(f"{where}.commits:invalid")
        files = _expect_list(row["files"], f"{where}.files")
        if not files or files != sorted(files):
            _fail(f"{where}.files:must_be_nonempty_sorted")
        guarantee_selector = row.get("guarantee_selector", "listed")
        remediation_selector = row.get("remediation_selector", "listed")
        if guarantee_selector not in {"listed", "all"}:
            _fail(f"{where}.guarantee_selector:invalid")
        if remediation_selector not in {"listed", "all"}:
            _fail(f"{where}.remediation_selector:invalid")
        if guarantee_selector == "all" and row["guarantee_ids"]:
            _fail(f"{where}.guarantee_ids:all_selector_requires_empty_list")
        if remediation_selector == "all" and row["remediation_ids"]:
            _fail(f"{where}.remediation_ids:all_selector_requires_empty_list")
        if not set(row["guarantee_ids"]) <= guarantee_ids:
            _fail(f"{where}.guarantee_ids:unknown_id")
        if not set(row["remediation_ids"]) <= remediation_ids:
            _fail(f"{where}.remediation_ids:unknown_id")

    risk_ids: set[str] = set()
    for index, raw in enumerate(_expect_list(policy["residual_risks"], "policy.residual_risks")):
        where = f"policy.residual_risks[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "id",
                "category",
                "severity",
                "summary",
                "guarantee_selector",
                "guarantee_ids",
                "remediation_selector",
                "remediation_ids",
                "proof_ids",
                "exit_criteria",
            },
            where=where,
        )
        risk_id = _expect_string(row["id"], f"{where}.id")
        if not RISK_ID_RE.fullmatch(risk_id) or risk_id in risk_ids:
            _fail(f"{where}.id:invalid_or_duplicate:{risk_id}")
        risk_ids.add(risk_id)
        if not set(row["guarantee_ids"]) <= guarantee_ids:
            _fail(f"{where}.guarantee_ids:unknown_id")
        if not set(row["remediation_ids"]) <= remediation_ids:
            _fail(f"{where}.remediation_ids:unknown_id")
        if not set(row["proof_ids"]) <= proof_ids:
            _fail(f"{where}.proof_ids:unknown_id")
        if row["guarantee_selector"] not in {"listed", "all", "proof_maturity_partial"}:
            _fail(f"{where}.guarantee_selector:invalid")
        if row["remediation_selector"] not in {"listed", "all", "open"}:
            _fail(f"{where}.remediation_selector:invalid")

    for key in ("system_summary", "integration_plan"):
        values = _expect_list(policy[key], f"policy.{key}")
        if not values or any(not isinstance(value, str) or not value for value in values):
            _fail(f"policy.{key}:expected_nonempty_string_array")


def _expand_risk_ids(
    row: Mapping[str, Any],
    *,
    registry: Sequence[Mapping[str, Any]],
    remediation_ids: set[str],
) -> tuple[list[str], list[str]]:
    guarantee_selector = row["guarantee_selector"]
    if guarantee_selector == "all":
        guarantee_ids = [item["id"] for item in registry]
    elif guarantee_selector == "proof_maturity_partial":
        guarantee_ids = [
            item["id"] for item in registry if item["proof_maturity"] == "partial"
        ]
    else:
        guarantee_ids = list(row["guarantee_ids"])

    remediation_selector = row["remediation_selector"]
    if remediation_selector in {"all", "open"}:
        affected_remediations = sorted(remediation_ids, key=_id_sort_key)
    else:
        affected_remediations = list(row["remediation_ids"])
    return (
        sorted(guarantee_ids, key=_id_sort_key),
        sorted(affected_remediations, key=_id_sort_key),
    )


def _load_attestations(
    *,
    root: Path,
    bundle: guarantees.ValidationBundle,
    source_commit: str,
    attestation_paths: Sequence[Path] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not attestation_paths:
        return [], {
            "state": "not_attested",
            "attestations": [],
        }

    expected_prefix = "docs/assurance/guarantees/attestations/"
    candidates: list[tuple[str, Path]] = []
    for index, attestation_path in enumerate(attestation_paths):
        resolved = attestation_path.resolve()
        relative = _repo_relative(root, resolved, f"attestations[{index}]")
        if not relative.startswith(expected_prefix) or not relative.endswith(".json"):
            _fail(f"attestations[{index}]:path_must_be_immutable_attestation_json")
        candidates.append((relative, resolved))
    candidates.sort(key=lambda item: item[0])
    relative_paths = [relative for relative, _ in candidates]
    if len(relative_paths) != len(set(relative_paths)):
        _fail("attestations:duplicate_path")

    loaded: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    attestation_ids: set[str] = set()
    for index, (relative, resolved) in enumerate(candidates):
        raw = guarantees.load_json_strict(resolved)
        attestation_source = _expect_object(
            raw.get("source"), f"attestations[{index}].source"
        )
        if attestation_source.get("git_commit") != source_commit:
            _fail(
                f"attestations[{index}].source.git_commit:"
                "assessment_subject_mismatch"
            )
        validated = guarantees.validate_attestation_file_historically(
            resolved,
            bundle,
            evidence_root=root,
        )
        attestation_id = validated["attestation_id"]
        if attestation_id in attestation_ids:
            _fail(f"attestations:duplicate_attestation_id:{attestation_id}")
        attestation_ids.add(attestation_id)
        loaded.append(validated)
        bindings.append(
            {
                "attestation_id": attestation_id,
                "path": relative,
                "sha256": _sha256(resolved),
                "environment_profile_ids": [
                    row["profile_id"] for row in validated["environments"]
                ],
            }
        )
    return loaded, {
        "state": "bound",
        "attestations": bindings,
    }


def _aggregate_status(statuses: Sequence[str]) -> str:
    """Mirror the attestation v1 guarantee-result aggregation semantics."""

    if not statuses:
        return "NOT_RUN"
    if "FAIL" in statuses:
        return "FAIL"
    if all(status == "PASS" for status in statuses):
        return "PASS"
    for status in ("NOT_RUN", "UNAVAILABLE", "MANUAL"):
        if all(item == status for item in statuses):
            return status
    return "PARTIAL"


def _effective_proof_results(
    *,
    catalog: Sequence[Mapping[str, Any]],
    attestations: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Resolve one effective status per proof without conflating run profiles.

    Every immutable raw result remains available in ``raw_refs`` and in the
    top-level attestation result sets. NOT_RUN rows from non-owning executions
    are intentionally ignored. PASS, FAIL, PARTIAL, or MANUAL can affect the
    effective status only when the attestation binds the proof's declared
    environment profile. UNAVAILABLE can honestly record that the profile could
    not be realized, but it is used only when no attempted profile result exists.
    Conflicting attempted/profile results are rejected.
    """

    bindings_by_id = {
        row["attestation_id"]: row for row in binding["attestations"]
    }
    proof_by_id = {row["id"]: row for row in catalog}
    raw_refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    attempted_candidates: dict[str, list[tuple[str, str]]] = defaultdict(list)
    unavailable_candidates: dict[str, list[str]] = defaultdict(list)

    for attestation in attestations:
        attestation_id = attestation["attestation_id"]
        bound_profiles = set(bindings_by_id[attestation_id]["environment_profile_ids"])
        for result in attestation["proof_results"]:
            proof_id = result["proof_id"]
            definition = proof_by_id[proof_id]
            profile_bound = definition["environment_profile_id"] in bound_profiles
            raw_refs[proof_id].append(
                {
                    "attestation_id": attestation_id,
                    "status": result["status"],
                    "profile_bound": profile_bound,
                }
            )
            if result["status"] in {"PASS", "FAIL", "PARTIAL", "MANUAL"}:
                if profile_bound:
                    attempted_candidates[proof_id].append(
                        (attestation_id, result["status"])
                    )
            elif result["status"] == "UNAVAILABLE":
                unavailable_candidates[proof_id].append(attestation_id)

    effective: dict[str, dict[str, Any]] = {}
    for proof in catalog:
        proof_id = proof["id"]
        if proof["lifecycle"] != "active":
            effective[proof_id] = {
                "state": "definition_only",
                "status": None,
                "attestation_ids": [],
            }
            continue
        attempted = attempted_candidates.get(proof_id, [])
        statuses = {status for _, status in attempted}
        if len(statuses) > 1:
            detail = ",".join(
                f"{attestation_id}={status}"
                for attestation_id, status in sorted(attempted)
            )
            _fail(f"proofs.{proof_id}:conflicting_attempted_results:{detail}")
        if attempted:
            effective[proof_id] = {
                "state": "profile_result",
                "status": attempted[0][1],
                "attestation_ids": sorted(
                    {attestation_id for attestation_id, _ in attempted}
                ),
            }
        elif unavailable_candidates.get(proof_id):
            effective[proof_id] = {
                "state": "unavailable",
                "status": "UNAVAILABLE",
                "attestation_ids": sorted(set(unavailable_candidates[proof_id])),
            }
        else:
            effective[proof_id] = {
                "state": "not_run",
                "status": "NOT_RUN",
                "attestation_ids": [],
            }
    return effective, raw_refs


def build_review(
    *,
    root: Path = ROOT,
    source_commit: str,
    attestation_paths: Sequence[Path] | None = None,
    final_gate: bool = False,
    validation_results_path: Path | None = None,
    repository_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the exact forward assessment without executing any proof."""

    root = root.resolve()
    _validate_review_schema_contract(root)
    _verify_git_commit(root, source_commit)
    source_material = _source_material_binding(root, source_commit)
    bundle = guarantees.validate_repository(root=root)
    policy = guarantees.load_json_strict(root / POLICY_PATH.relative_to(ROOT))
    registry = bundle.registry["guarantees"]
    catalog = bundle.proof_catalog["proofs"]
    guarantee_ids = {row["id"] for row in registry}

    remediation_to_guarantees: dict[str, set[str]] = defaultdict(set)
    remediation_paths: dict[str, str] = {}
    for guarantee in registry:
        for ref in guarantee["remediation_refs"]:
            remediation_to_guarantees[ref["id"]].add(guarantee["id"])
            remediation_paths[ref["id"]] = ref["path"]
    remediation_ids = set(remediation_to_guarantees)
    proof_ids = {row["id"] for row in catalog}
    _validate_policy(
        policy,
        guarantee_ids=guarantee_ids,
        remediation_ids=remediation_ids,
        proof_ids=proof_ids,
    )

    frozen = policy["frozen_evidence"]
    registry_path = root / frozen["registry_path"]
    observed_registry_hash = _sha256(registry_path)
    if observed_registry_hash != frozen["registry_sha256"]:
        _fail(
            "frozen_registry_sha256_mismatch:"
            f"expected={frozen['registry_sha256']}:observed={observed_registry_hash}"
        )
    if bundle.registry["audit_baseline_commit"] != frozen["audit_baseline_commit"]:
        _fail("frozen_audit_baseline_commit_mismatch")

    _verify_slice_evidence(root, source_commit, policy["phase3_slices"])
    attestations, attestation_binding = _load_attestations(
        root=root,
        bundle=bundle,
        source_commit=source_commit,
        attestation_paths=attestation_paths,
    )
    if final_gate:
        if not attestations:
            _fail("final_gate:requires_nonempty_attestation_set")
        if validation_results_path is None:
            _fail("final_gate:requires_validation_results_document")
        validation_results, validation_results_source = _load_validation_results(
            root=root,
            source_commit=source_commit,
            path=validation_results_path,
        )
        if repository_state is None:
            captured_repository_state = _capture_packet_input_state(
                root,
                source_commit,
                frozen["audit_baseline_commit"],
            )
        else:
            captured_repository_state = dict(repository_state)
            _validate_recorded_packet_input(
                root,
                source_commit,
                captured_repository_state,
                frozen["audit_baseline_commit"],
            )
        final_gate_state: dict[str, Any] = {
            "mode": "final_gate",
            "repository_state": captured_repository_state,
            "validation_results_source": validation_results_source,
            "validation_results": validation_results,
            "integration_approval_request": {
                "status": "requested",
                "requested_action": "integrate_feature_branch_into_develop",
                "target_branch": "develop",
                "guarantee_activation_included": False,
            },
        }
    else:
        if validation_results_path is not None or repository_state is not None:
            _fail("intermediate_review:forbids_final_gate_evidence")
        final_gate_state = {
            "mode": "intermediate",
            "repository_state": None,
            "validation_results_source": None,
            "validation_results": [],
            "integration_approval_request": None,
        }

    phase2_disposition_by_remediation = _validate_partition(
        policy["phase2_dispositions"],
        expected_ids=remediation_ids,
        allowed_groups=PHASE2_DISPOSITIONS,
        where="policy.phase2_dispositions",
    )
    outcome_by_remediation = _validate_partition(
        policy["remediation_outcomes"],
        expected_ids=remediation_ids,
        allowed_groups=ACTION_OUTCOMES,
        where="policy.remediation_outcomes",
    )

    active_required: dict[str, list[str]] = defaultdict(list)
    proposed_required: dict[str, list[str]] = defaultdict(list)
    proof_guarantees: dict[str, list[str]] = defaultdict(list)
    for proof in catalog:
        for coverage in proof["coverage"]:
            guarantee_id = coverage["guarantee_id"]
            proof_guarantees[proof["id"]].append(guarantee_id)
            if coverage["required_for_full_attestation"]:
                target = active_required if proof["lifecycle"] == "active" else proposed_required
                target[guarantee_id].append(proof["id"])
    for mapping in (active_required, proposed_required, proof_guarantees):
        for values in mapping.values():
            values.sort(key=_id_sort_key)

    effective_proof_by_id: dict[str, dict[str, Any]] = {}
    raw_result_refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if attestations:
        effective_proof_by_id, raw_result_refs = _effective_proof_results(
            catalog=catalog,
            attestations=attestations,
            binding=attestation_binding,
        )
    binding_by_id = {
        row["attestation_id"]: row for row in attestation_binding["attestations"]
    }
    attestation_results = [
        {
            "attestation_id": attestation["attestation_id"],
            "path": binding_by_id[attestation["attestation_id"]]["path"],
            "proof_results": [dict(row) for row in attestation["proof_results"]],
            "guarantee_results": [
                dict(row) for row in attestation["guarantee_results"]
            ],
        }
        for attestation in attestations
    ]

    slices_by_guarantee: dict[str, list[str]] = defaultdict(list)
    slices_by_remediation: dict[str, list[str]] = defaultdict(list)
    phase3_slices: list[dict[str, Any]] = []
    for raw in policy["phase3_slices"]:
        row = dict(raw)
        guarantee_selector = row.pop("guarantee_selector", "listed")
        remediation_selector = row.pop("remediation_selector", "listed")
        row["guarantee_ids"] = sorted(
            guarantee_ids if guarantee_selector == "all" else row["guarantee_ids"],
            key=_id_sort_key,
        )
        row["remediation_ids"] = sorted(
            remediation_ids
            if remediation_selector == "all"
            else row["remediation_ids"],
            key=_id_sort_key,
        )
        phase3_slices.append(row)
        for guarantee_id in row["guarantee_ids"]:
            slices_by_guarantee[guarantee_id].append(row["id"])
        for remediation_id in row["remediation_ids"]:
            slices_by_remediation[remediation_id].append(row["id"])

    term_blockers: dict[str, list[str]] = defaultdict(list)
    for row in policy["deferred_terms"]:
        for guarantee_id in row["guarantee_ids"]:
            term_blockers[guarantee_id].append(row["term_id"])

    residual_risks: list[dict[str, Any]] = []
    risks_by_guarantee: dict[str, list[str]] = defaultdict(list)
    risks_by_remediation: dict[str, list[str]] = defaultdict(list)
    for raw in policy["residual_risks"]:
        guarantee_scope, remediation_scope = _expand_risk_ids(
            raw,
            registry=registry,
            remediation_ids=remediation_ids,
        )
        row = {
            "id": raw["id"],
            "category": raw["category"],
            "severity": raw["severity"],
            "summary": raw["summary"],
            "guarantee_ids": guarantee_scope,
            "remediation_ids": remediation_scope,
            "proof_ids": sorted(raw["proof_ids"], key=_id_sort_key),
            "exit_criteria": raw["exit_criteria"],
        }
        residual_risks.append(row)
        for guarantee_id in guarantee_scope:
            risks_by_guarantee[guarantee_id].append(row["id"])
        for remediation_id in remediation_scope:
            risks_by_remediation[remediation_id].append(row["id"])

    disposition_overrides = policy["forward_overrides"]["registry_disposition"]
    conformance_overrides = policy["forward_overrides"]["conformance"]
    claim_overlay_ids = set(
        policy["forward_overrides"]["current_claim_overlay_required"]
    )
    guarantee_rows: list[dict[str, Any]] = []
    for frozen_row in registry:
        guarantee_id = frozen_row["id"]
        frozen_state = {
            "claim_lifecycle": frozen_row["claim_lifecycle"],
            "registry_disposition": frozen_row["registry_disposition"],
            "conformance": frozen_row["conformance"],
            "enforcement_maturity": frozen_row["enforcement_maturity"],
            "proof_maturity": frozen_row["proof_maturity"],
            "activation_status": frozen_row["activation_status"],
        }
        forward_state = {
            "claim_lifecycle": "current",
            "registry_disposition": disposition_overrides.get(
                guarantee_id, frozen_row["registry_disposition"]
            ),
            "conformance": conformance_overrides.get(
                guarantee_id, frozen_row["conformance"]
            ),
            "enforcement_maturity": frozen_row["enforcement_maturity"],
            "proof_maturity": frozen_row["proof_maturity"],
            "activation_status": "unactivated",
        }
        changed_fields = [
            field for field in frozen_state if frozen_state[field] != forward_state[field]
        ]
        forward_reasons = (
            ["approved_phase3_forward_reassessment"]
            if changed_fields
            else ["frozen_state_carried_forward"]
        )
        if guarantee_id in claim_overlay_ids:
            forward_reasons.append("current_claim_wording_overlay_required")

        active_proofs = sorted(active_required.get(guarantee_id, []), key=_id_sort_key)
        proposed_proofs = sorted(
            proposed_required.get(guarantee_id, []), key=_id_sort_key
        )
        if not attestations:
            proof_assessment = {
                "state": "not_attested",
                "attested_status": None,
                "reported_status": None,
                "proof_ids": active_proofs,
                "reason_codes": ["no_attestation"],
            }
        else:
            if not active_proofs:
                reason_codes = ["no_active_required_proof"]
                if proposed_proofs:
                    reason_codes.append("proposed_required_proof")
                proof_assessment = {
                    "state": "unavailable",
                    "attested_status": None,
                    "reported_status": None,
                    "proof_ids": active_proofs,
                    "reason_codes": reason_codes,
                }
            else:
                aggregate_status = _aggregate_status(
                    [
                        effective_proof_by_id[proof_id]["status"]
                        for proof_id in active_proofs
                    ]
                )
                reason_codes = ["effective_profile_result_aggregation"]
                attested_status = aggregate_status
                if aggregate_status == "PASS" and proposed_proofs:
                    attested_status = "PARTIAL"
                    reason_codes.append("proposed_required_proof")
                reported_status = attested_status
                if aggregate_status == "NOT_RUN":
                    reason_codes.append("no_matching_profile_attempt")
                if attested_status == "PASS" and forward_state["proof_maturity"] != "adequate":
                    reported_status = "PARTIAL"
                    reason_codes.append(
                        f"registry_proof_maturity_{forward_state['proof_maturity']}"
                    )
                proof_assessment = {
                    "state": "attested",
                    "attested_status": attested_status,
                    "reported_status": reported_status,
                    "proof_ids": active_proofs,
                    "reason_codes": reason_codes,
                }

        guarantee_rows.append(
            {
                "id": guarantee_id,
                "title": frozen_row["title"],
                "candidate_ids": [ref["id"] for ref in frozen_row["candidate_refs"]],
                "frozen_state": frozen_state,
                "forward_state": forward_state,
                "changed_fields": changed_fields,
                "forward_reason_codes": forward_reasons,
                "active_required_proof_ids": active_proofs,
                "proposed_required_proof_ids": proposed_proofs,
                "proof_assessment": proof_assessment,
                "remediation_ids": sorted(
                    [ref["id"] for ref in frozen_row["remediation_refs"]],
                    key=_id_sort_key,
                ),
                "deferred_term_ids": sorted(
                    term_blockers.get(guarantee_id, []), key=_id_sort_key
                ),
                "phase3_slice_ids": sorted(
                    slices_by_guarantee.get(guarantee_id, [])
                ),
                "residual_risk_ids": sorted(
                    risks_by_guarantee.get(guarantee_id, []), key=_id_sort_key
                ),
            }
        )

    proof_rows: list[dict[str, Any]] = []
    for proof in catalog:
        result = effective_proof_by_id.get(proof["id"])
        if result is None:
            result_state = (
                "definition_only"
                if proof["lifecycle"] != "active"
                else "not_attested"
            )
            attested_status = None
            effective_attestation_ids: list[str] = []
        else:
            result_state = result["state"]
            attested_status = result["status"]
            effective_attestation_ids = list(result["attestation_ids"])
        proof_rows.append(
            {
                "id": proof["id"],
                "lifecycle": proof["lifecycle"],
                "environment_profile_id": proof["environment_profile_id"],
                "guarantee_ids": sorted(
                    proof_guarantees.get(proof["id"], []), key=_id_sort_key
                ),
                "result_state": result_state,
                "attested_status": attested_status,
                "effective_attestation_ids": effective_attestation_ids,
                "raw_result_refs": raw_result_refs.get(proof["id"], []),
            }
        )

    remediation_rows: list[dict[str, Any]] = []
    for remediation_id in sorted(remediation_ids, key=_id_sort_key):
        path = root / remediation_paths[remediation_id]
        frontmatter = _read_remediation_frontmatter(path)
        if frontmatter.get("remediation_id") != remediation_id:
            _fail(f"{remediation_id}:frontmatter_id_mismatch")
        if frontmatter.get("lifecycle") != "proposed":
            _fail(f"{remediation_id}:source_lifecycle_changed")
        if frontmatter.get("review_status") != "pending":
            _fail(f"{remediation_id}:source_review_status_changed")
        outcome = outcome_by_remediation[remediation_id]
        default_acceptance = "partial" if outcome == "implemented" else "not_assessed"
        acceptance_state = policy["acceptance_overrides"].get(
            remediation_id, default_acceptance
        )
        guarantee_scope = sorted(
            remediation_to_guarantees[remediation_id], key=_id_sort_key
        )
        remediation_proofs = sorted(
            {
                proof_id
                for guarantee_id in guarantee_scope
                for proof_id in (
                    active_required.get(guarantee_id, [])
                    + proposed_required.get(guarantee_id, [])
                )
            },
            key=_id_sort_key,
        )
        remediation_rows.append(
            {
                "id": remediation_id,
                "guarantee_ids": guarantee_scope,
                "source_path": remediation_paths[remediation_id],
                "source_lifecycle": frontmatter["lifecycle"],
                "source_review_status": frontmatter["review_status"],
                "approved_phase2_disposition": phase2_disposition_by_remediation[
                    remediation_id
                ],
                "phase3_action_outcome": outcome,
                "acceptance_state": acceptance_state,
                "closure_state": "open",
                "proof_ids": remediation_proofs,
                "phase3_slice_ids": sorted(
                    slices_by_remediation.get(remediation_id, [])
                ),
                "residual_risk_ids": sorted(
                    risks_by_remediation.get(remediation_id, []), key=_id_sort_key
                ),
            }
        )

    disposition_counts = Counter(
        row["forward_state"]["registry_disposition"] for row in guarantee_rows
    )
    conformance_counts = Counter(
        row["forward_state"]["conformance"] for row in guarantee_rows
    )
    enforcement_counts = Counter(
        row["forward_state"]["enforcement_maturity"] for row in guarantee_rows
    )
    proof_maturity_counts = Counter(
        row["forward_state"]["proof_maturity"] for row in guarantee_rows
    )
    remediation_outcome_counts = Counter(
        row["phase3_action_outcome"] for row in remediation_rows
    )
    proof_lifecycle_counts = Counter(row["lifecycle"] for row in proof_rows)
    proof_environment_counts = Counter(
        row["environment_profile_id"] for row in proof_rows
    )
    effective_result_counts = (
        {
            status: sum(row["attested_status"] == status for row in proof_rows)
            for status in sorted(ATTESTATION_STATUSES)
        }
        if attestations
        else None
    )
    raw_result_counts = (
        dict(
            sorted(
                Counter(
                    result["status"]
                    for attestation in attestations
                    for result in attestation["proof_results"]
                ).items()
            )
        )
        if attestations
        else None
    )

    summary = {
        "guarantees": len(guarantee_rows),
        "remediations": len(remediation_rows),
        "proof_definitions": len(proof_rows),
        "claim_lifecycle": {"current": len(guarantee_rows)},
        "registry_disposition": dict(sorted(disposition_counts.items())),
        "conformance": dict(sorted(conformance_counts.items())),
        "enforcement_maturity": dict(sorted(enforcement_counts.items())),
        "proof_maturity": dict(sorted(proof_maturity_counts.items())),
        "activation_status": {"unactivated": len(guarantee_rows)},
        "remediation_outcomes": dict(sorted(remediation_outcome_counts.items())),
    }

    review = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "authority": "non_normative_final_review",
        "frozen_evidence": dict(frozen),
        "assessment_subject": {
            "source_commit": source_commit,
            "source_tree_kind": "committed_snapshot",
            "source_material": source_material,
        },
        "attestation_binding": attestation_binding,
        "attestation_results": attestation_results,
        "summary": summary,
        "system_summary": list(policy["system_summary"]),
        "phase3_slices": phase3_slices,
        "guarantees": guarantee_rows,
        "proof_accounting": {
            "catalog_path": guarantees.PROOF_CATALOG_PATH.relative_to(
                guarantees.ROOT
            ).as_posix(),
            "catalog_sha256": _sha256(
                root / guarantees.PROOF_CATALOG_PATH.relative_to(guarantees.ROOT)
            ),
            "definition_count": len(proof_rows),
            "lifecycle_counts": dict(sorted(proof_lifecycle_counts.items())),
            "environment_counts": dict(sorted(proof_environment_counts.items())),
            "attestation_count": len(attestations),
            "raw_result_counts": raw_result_counts,
            "effective_result_counts": effective_result_counts,
        },
        "proofs": proof_rows,
        "remediations": remediation_rows,
        "residual_risks": residual_risks,
        "final_gate": final_gate_state,
        "integration_plan": list(policy["integration_plan"]),
    }
    validate_review_data(review, final_gate_required=final_gate)
    _validate_expected_summary(policy["expected_summary"], review)
    return review


def _validate_expected_summary(expected: Mapping[str, Any], review: Mapping[str, Any]) -> None:
    summary = review["summary"]
    scalar_pairs = {
        "guarantees": summary["guarantees"],
        "remediations": summary["remediations"],
        "proof_definitions": summary["proof_definitions"],
    }
    for key, observed in scalar_pairs.items():
        if expected[key] != observed:
            _fail(f"expected_summary.{key}:expected={expected[key]}:observed={observed}")
    comparisons = {
        "forward_dispositions": summary["registry_disposition"],
        "forward_conformance": summary["conformance"],
        "remediation_outcomes": summary["remediation_outcomes"],
    }
    for key, observed in comparisons.items():
        if expected[key] != observed:
            _fail(f"expected_summary.{key}:mismatch")


def _validate_embedded_validation_results(results: Any) -> list[dict[str, Any]]:
    rows = _expect_list(results, "review.final_gate.validation_results")
    if not rows:
        _fail("review.final_gate.validation_results:must_be_nonempty")
    ids: list[str] = []
    validated: list[dict[str, Any]] = []
    for index, raw in enumerate(rows):
        where = f"review.final_gate.validation_results[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "id",
                "command_argv",
                "status",
                "exit_code",
                "started_at",
                "finished_at",
                "reason_code",
                "evidence_refs",
            },
            where=where,
        )
        result_id = _expect_string(row["id"], f"{where}.id")
        if not VALIDATION_ID_RE.fullmatch(result_id):
            _fail(f"{where}.id:invalid")
        ids.append(result_id)
        argv = _expect_list(row["command_argv"], f"{where}.command_argv")
        if not argv or any(
            not isinstance(argument, str)
            or not argument
            or any(character in argument for character in ("\0", "\r", "\n"))
            for argument in argv
        ):
            _fail(f"{where}.command_argv:invalid")
        status = row["status"]
        if status not in VALIDATION_STATUSES:
            _fail(f"{where}.status:invalid")
        evidence_rows = _expect_list(row["evidence_refs"], f"{where}.evidence_refs")
        evidence_paths: list[str] = []
        for evidence_index, evidence_raw in enumerate(evidence_rows):
            evidence_where = f"{where}.evidence_refs[{evidence_index}]"
            evidence = _expect_object(evidence_raw, evidence_where)
            _exact_keys(
                evidence,
                required={"path", "sha256"},
                where=evidence_where,
            )
            evidence_paths.append(
                _expect_string(evidence["path"], f"{evidence_where}.path")
            )
            if not HEX64_RE.fullmatch(
                _expect_string(evidence["sha256"], f"{evidence_where}.sha256")
            ):
                _fail(f"{evidence_where}.sha256:invalid")
        if evidence_paths != sorted(set(evidence_paths)):
            _fail(f"{where}.evidence_refs:must_be_sorted_unique")

        executed = status in {"PASS", "FAIL", "PARTIAL", "MANUAL"}
        if executed:
            started = _parse_utc_timestamp(row["started_at"], f"{where}.started_at")
            finished = _parse_utc_timestamp(row["finished_at"], f"{where}.finished_at")
            if finished < started:
                _fail(f"{where}:finished_before_started")
            if not evidence_rows:
                _fail(f"{where}:executed_result_requires_evidence")
            if status == "MANUAL":
                if row["exit_code"] is not None:
                    _fail(f"{where}.exit_code:manual_requires_null")
            else:
                exit_code = _expect_int(row["exit_code"], f"{where}.exit_code")
                if status in {"PASS", "PARTIAL"} and exit_code != 0:
                    _fail(f"{where}.exit_code:{status}_requires_zero")
                if status == "FAIL" and exit_code == 0:
                    _fail(f"{where}.exit_code:FAIL_requires_nonzero")
            if status == "PASS":
                if row["reason_code"] is not None:
                    _fail(f"{where}.reason_code:PASS_requires_null")
            else:
                _expect_string(row["reason_code"], f"{where}.reason_code")
        else:
            if any(
                row[key] is not None
                for key in ("exit_code", "started_at", "finished_at")
            ) or evidence_rows:
                _fail(f"{where}:{status}_forbids_execution_result")
            _expect_string(row["reason_code"], f"{where}.reason_code")
        validated.append(dict(row))
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        _fail("review.final_gate.validation_results:ids_must_be_sorted_unique")
    return validated


def _validate_source_material(
    source: Mapping[str, Any], *, required: bool
) -> None:
    raw = source.get("source_material")
    if raw is None:
        if required:
            _fail("review.assessment_subject.source_material:required_at_final_gate")
        return
    rows = _expect_list(raw, "review.assessment_subject.source_material")
    paths: list[str] = []
    for index, item in enumerate(rows):
        where = f"review.assessment_subject.source_material[{index}]"
        row = _expect_object(item, where)
        _exact_keys(row, required={"path", "sha256"}, where=where)
        paths.append(_expect_string(row["path"], f"{where}.path"))
        if not HEX64_RE.fullmatch(
            _expect_string(row["sha256"], f"{where}.sha256")
        ):
            _fail(f"{where}.sha256:invalid")
    if paths != sorted(SOURCE_MATERIAL_PATHS):
        _fail("review.assessment_subject.source_material:expected_exact_bound_set")


def _validate_final_gate_data(
    review: Mapping[str, Any],
    *,
    binding: Mapping[str, Any],
    guarantee_rows: Sequence[Mapping[str, Any]],
    proof_rows: Sequence[Mapping[str, Any]],
    final_gate_required: bool,
) -> None:
    raw = review.get("final_gate")
    if raw is None:
        if final_gate_required:
            _fail("review.final_gate:required")
        return
    gate = _expect_object(raw, "review.final_gate")
    _exact_keys(
        gate,
        required={
            "mode",
            "repository_state",
            "validation_results_source",
            "validation_results",
            "integration_approval_request",
        },
        where="review.final_gate",
    )
    mode = gate["mode"]
    if mode not in {"intermediate", "final_gate"}:
        _fail("review.final_gate.mode:invalid")
    if mode == "intermediate":
        if final_gate_required:
            _fail("review.final_gate.mode:final_gate_required")
        if (
            gate["repository_state"] is not None
            or gate["validation_results_source"] is not None
            or gate["validation_results"] != []
            or gate["integration_approval_request"] is not None
        ):
            _fail("review.final_gate.intermediate:forbids_final_evidence")
        return

    if binding["state"] != "bound" or not binding["attestations"]:
        _fail("review.final_gate:requires_nonempty_bound_attestations")
    _validate_source_material(review["assessment_subject"], required=True)

    state = _expect_object(
        gate["repository_state"], "review.final_gate.repository_state"
    )
    _exact_keys(
        state,
        required={
            "branch",
            "packet_input_commit",
            "packet_input_tree",
            "develop_commit",
            "clean",
        },
        where="review.final_gate.repository_state",
    )
    if _expect_string(state["branch"], "review.final_gate.repository_state.branch") == "develop":
        _fail("review.final_gate.repository_state.branch:develop_forbidden")
    for key in ("packet_input_commit", "packet_input_tree", "develop_commit"):
        if not HEX40_RE.fullmatch(
            _expect_string(state[key], f"review.final_gate.repository_state.{key}")
        ):
            _fail(f"review.final_gate.repository_state.{key}:invalid")
    if state["clean"] is not True:
        _fail("review.final_gate.repository_state.clean:must_be_true")

    source = _expect_object(
        gate["validation_results_source"],
        "review.final_gate.validation_results_source",
    )
    _exact_keys(source, required={"path", "sha256"}, where="review.final_gate.validation_results_source")
    validation_path = _expect_string(
        source["path"], "review.final_gate.validation_results_source.path"
    )
    if not validation_path.startswith(
        "docs/assurance/guarantees/validation-results/"
    ) or not validation_path.endswith(".json"):
        _fail("review.final_gate.validation_results_source.path:invalid")
    if not HEX64_RE.fullmatch(
        _expect_string(
            source["sha256"], "review.final_gate.validation_results_source.sha256"
        )
    ):
        _fail("review.final_gate.validation_results_source.sha256:invalid")
    _validate_embedded_validation_results(gate["validation_results"])

    approval = _expect_object(
        gate["integration_approval_request"],
        "review.final_gate.integration_approval_request",
    )
    _exact_keys(
        approval,
        required={
            "status",
            "requested_action",
            "target_branch",
            "guarantee_activation_included",
        },
        where="review.final_gate.integration_approval_request",
    )
    if approval != {
        "status": "requested",
        "requested_action": "integrate_feature_branch_into_develop",
        "target_branch": "develop",
        "guarantee_activation_included": False,
    }:
        _fail("review.final_gate.integration_approval_request:invalid")

    if len(proof_rows) != 85 or any(row["lifecycle"] != "active" for row in proof_rows):
        _fail("review.final_gate.proofs:expected_all_85_active")
    for row in proof_rows:
        if row["result_state"] not in {"profile_result", "unavailable", "not_run"}:
            _fail(f"review.final_gate.proofs.{row['id']}:explicit_result_required")
        if row["attested_status"] is None or not row["raw_result_refs"]:
            _fail(f"review.final_gate.proofs.{row['id']}:raw_result_coverage_required")
    for row in guarantee_rows:
        proof = row["proof_assessment"]
        if proof["state"] == "not_attested":
            _fail(f"review.final_gate.guarantees.{row['id']}:assessment_required")
        if row["active_required_proof_ids"] and (
            proof["attested_status"] is None or proof["reported_status"] is None
        ):
            _fail(f"review.final_gate.guarantees.{row['id']}:explicit_result_required")


def validate_review_data(
    review: Mapping[str, Any], *, final_gate_required: bool = False
) -> None:
    """Validate the generated v1 final-review structure and safety invariants."""

    _exact_keys(
        review,
        required={
            "schema_version",
            "authority",
            "frozen_evidence",
            "assessment_subject",
            "attestation_binding",
            "attestation_results",
            "summary",
            "system_summary",
            "phase3_slices",
            "guarantees",
            "proof_accounting",
            "proofs",
            "remediations",
            "residual_risks",
            "integration_plan",
        },
        optional={"final_gate"},
        where="review",
    )
    if review["schema_version"] != REVIEW_SCHEMA_VERSION:
        _fail(f"review.schema_version:expected:{REVIEW_SCHEMA_VERSION}")
    if review["authority"] != "non_normative_final_review":
        _fail("review.authority:must_remain_non_normative")
    source = _expect_object(review["assessment_subject"], "review.assessment_subject")
    _exact_keys(
        source,
        required={"source_commit", "source_tree_kind"},
        optional={"source_material"},
        where="review.assessment_subject",
    )
    if not HEX40_RE.fullmatch(source["source_commit"]):
        _fail("review.assessment_subject.source_commit:invalid")
    if source["source_tree_kind"] != "committed_snapshot":
        _fail("review.assessment_subject.source_tree_kind:invalid")
    _validate_source_material(source, required=False)

    binding = _expect_object(review["attestation_binding"], "review.attestation_binding")
    _exact_keys(
        binding,
        required={"state", "attestations"},
        where="review.attestation_binding",
    )
    if binding["state"] not in {"not_attested", "bound"}:
        _fail("review.attestation_binding.state:invalid")
    binding_rows = _expect_list(
        binding["attestations"], "review.attestation_binding.attestations"
    )
    if binding["state"] == "not_attested" and binding_rows:
        _fail("review.attestation_binding:not_attested_forbids_reference")
    if binding["state"] == "bound" and not binding_rows:
        _fail("review.attestation_binding:bound_requires_reference")
    binding_paths: list[str] = []
    binding_ids: list[str] = []
    for index, raw in enumerate(binding_rows):
        where = f"review.attestation_binding.attestations[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "attestation_id",
                "path",
                "sha256",
                "environment_profile_ids",
            },
            where=where,
        )
        binding_paths.append(_expect_string(row["path"], f"{where}.path"))
        binding_ids.append(
            _expect_string(row["attestation_id"], f"{where}.attestation_id")
        )
        if not HEX64_RE.fullmatch(row["sha256"]):
            _fail(f"{where}.sha256:invalid")
        profiles = _expect_list(
            row["environment_profile_ids"], f"{where}.environment_profile_ids"
        )
        if not profiles or profiles != sorted(set(profiles)):
            _fail(f"{where}.environment_profile_ids:must_be_sorted_unique_nonempty")
    if binding_paths != sorted(binding_paths) or len(binding_paths) != len(
        set(binding_paths)
    ):
        _fail("review.attestation_binding.attestations:paths_must_be_sorted_unique")
    if len(binding_ids) != len(set(binding_ids)):
        _fail("review.attestation_binding.attestations:duplicate_attestation_id")

    result_sets = _expect_list(
        review["attestation_results"], "review.attestation_results"
    )
    if binding["state"] == "not_attested" and result_sets:
        _fail("review.attestation_results:not_attested_forbids_results")
    result_ids: list[str] = []
    result_paths: list[str] = []
    for index, raw in enumerate(result_sets):
        where = f"review.attestation_results[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "attestation_id",
                "path",
                "proof_results",
                "guarantee_results",
            },
            where=where,
        )
        result_ids.append(row["attestation_id"])
        result_paths.append(row["path"])
        _expect_list(row["proof_results"], f"{where}.proof_results")
        _expect_list(row["guarantee_results"], f"{where}.guarantee_results")
    if result_ids != binding_ids or result_paths != binding_paths:
        _fail("review.attestation_results:binding_identity_mismatch")

    guarantee_rows = _expect_list(review["guarantees"], "review.guarantees")
    if len(guarantee_rows) != 75:
        _fail("review.guarantees:expected_exactly_75")
    guarantee_ids: list[str] = []
    for index, raw in enumerate(guarantee_rows):
        where = f"review.guarantees[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "id",
                "title",
                "candidate_ids",
                "frozen_state",
                "forward_state",
                "changed_fields",
                "forward_reason_codes",
                "active_required_proof_ids",
                "proposed_required_proof_ids",
                "proof_assessment",
                "remediation_ids",
                "deferred_term_ids",
                "phase3_slice_ids",
                "residual_risk_ids",
            },
            where=where,
        )
        guarantee_ids.append(row["id"])
        forward = _expect_object(row["forward_state"], f"{where}.forward_state")
        if forward.get("claim_lifecycle") != "current":
            _fail(f"{where}.forward_state.claim_lifecycle:must_be_current")
        if forward.get("activation_status") != "unactivated":
            _fail(f"{where}.forward_state.activation_status:must_be_unactivated")
        proof = _expect_object(row["proof_assessment"], f"{where}.proof_assessment")
        _exact_keys(
            proof,
            required={
                "state",
                "attested_status",
                "reported_status",
                "proof_ids",
                "reason_codes",
            },
            where=f"{where}.proof_assessment",
        )
        if proof["state"] == "not_attested" and (
            proof["attested_status"] is not None or proof["reported_status"] is not None
        ):
            _fail(f"{where}.proof_assessment:not_attested_invented_result")
        for key in ("attested_status", "reported_status"):
            if proof[key] is not None and proof[key] not in ATTESTATION_STATUSES:
                _fail(f"{where}.proof_assessment.{key}:invalid")
    if len(guarantee_ids) != len(set(guarantee_ids)):
        _fail("review.guarantees:duplicate_ids")

    proof_rows = _expect_list(review["proofs"], "review.proofs")
    if len(proof_rows) != 85:
        _fail("review.proofs:expected_exactly_85")
    if len({row["id"] for row in proof_rows}) != len(proof_rows):
        _fail("review.proofs:duplicate_ids")
    for index, raw in enumerate(proof_rows):
        where = f"review.proofs[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(
            row,
            required={
                "id",
                "lifecycle",
                "environment_profile_id",
                "guarantee_ids",
                "result_state",
                "attested_status",
                "effective_attestation_ids",
                "raw_result_refs",
            },
            where=where,
        )
        effective_ids = _expect_list(
            row["effective_attestation_ids"],
            f"{where}.effective_attestation_ids",
        )
        if any(attestation_id not in binding_ids for attestation_id in effective_ids):
            _fail(f"{where}.effective_attestation_ids:unknown")
        raw_refs = _expect_list(row["raw_result_refs"], f"{where}.raw_result_refs")
        for ref_index, raw_ref in enumerate(raw_refs):
            ref_where = f"{where}.raw_result_refs[{ref_index}]"
            ref = _expect_object(raw_ref, ref_where)
            _exact_keys(
                ref,
                required={"attestation_id", "status", "profile_bound"},
                where=ref_where,
            )
            if ref["attestation_id"] not in binding_ids:
                _fail(f"{ref_where}.attestation_id:unknown")
            if ref["status"] not in ATTESTATION_STATUSES:
                _fail(f"{ref_where}.status:invalid")
            if not isinstance(ref["profile_bound"], bool):
                _fail(f"{ref_where}.profile_bound:expected_boolean")
        state = row["result_state"]
        status = row["attested_status"]
        if state in {"not_attested", "definition_only"} and (
            status is not None or effective_ids
        ):
            _fail(f"{where}:{state}_forbids_effective_result")
        if state == "not_run" and (status != "NOT_RUN" or effective_ids):
            _fail(f"{where}.not_run:invalid_effective_result")
        if state == "profile_result" and (
            status not in {"PASS", "FAIL", "PARTIAL", "MANUAL"}
            or not effective_ids
        ):
            _fail(f"{where}.profile_result:invalid_effective_result")
        if state == "unavailable" and (
            status != "UNAVAILABLE" or not effective_ids
        ):
            _fail(f"{where}.unavailable:invalid_effective_result")
        if state not in {
            "not_attested",
            "definition_only",
            "not_run",
            "profile_result",
            "unavailable",
        }:
            _fail(f"{where}.result_state:invalid")
    if binding["state"] == "not_attested" and any(
        row["attested_status"] is not None or row["raw_result_refs"]
        for row in proof_rows
    ):
        _fail("review.proofs:not_attested_invented_result")

    remediation_rows = _expect_list(review["remediations"], "review.remediations")
    if len(remediation_rows) != 68:
        _fail("review.remediations:expected_exactly_68")
    remediation_ids: list[str] = []
    for index, raw in enumerate(remediation_rows):
        where = f"review.remediations[{index}]"
        row = _expect_object(raw, where)
        remediation_ids.append(row["id"])
        if row["source_lifecycle"] != "proposed":
            _fail(f"{where}.source_lifecycle:must_remain_proposed")
        if row["source_review_status"] != "pending":
            _fail(f"{where}.source_review_status:must_remain_pending")
        if row["closure_state"] != "open":
            _fail(f"{where}.closure_state:must_remain_open")
        if row["phase3_action_outcome"] not in ACTION_OUTCOMES:
            _fail(f"{where}.phase3_action_outcome:invalid")
        if row["acceptance_state"] not in ACCEPTANCE_STATES:
            _fail(f"{where}.acceptance_state:invalid")
    if len(remediation_ids) != len(set(remediation_ids)):
        _fail("review.remediations:duplicate_ids")

    accounting = _expect_object(review["proof_accounting"], "review.proof_accounting")
    if binding["state"] == "not_attested" and any(
        accounting[key] is not None
        for key in ("raw_result_counts", "effective_result_counts")
    ):
        _fail("review.proof_accounting:not_attested_invented_counts")
    if accounting["attestation_count"] != len(binding_rows):
        _fail("review.proof_accounting.attestation_count:mismatch")
    _validate_final_gate_data(
        review,
        binding=binding,
        guarantee_rows=guarantee_rows,
        proof_rows=proof_rows,
        final_gate_required=final_gate_required,
    )


def _join_ids(values: Sequence[str]) -> str:
    return ", ".join(f"`{value}`" for value in values) if values else "—"


def render_markdown(review: Mapping[str, Any]) -> str:
    """Render the deterministic owner-facing review and complete appendices."""

    summary = review["summary"]
    binding = review["attestation_binding"]
    lines = [
        "# Phase 3 Final Review",
        "",
        "Generated by `python scripts/docs/build_phase3_final_review.py render`. Do not edit by hand.",
        "",
        "> **Non-normative review boundary.** This document does not rewrite the frozen",
        "> audit, activate a guarantee, close a remediation, or turn a proof definition",
        "> into a proof result. The final gate requested here is integration approval only.",
        "",
        "## Decision Requested",
        "",
        "Review the forward assessment, proof/attestation accounting, and residual risks.",
        "Merging into `develop` and guarantee activation remain separate decisions.",
        "",
        "## Bound Evidence",
        "",
        "| Evidence | Value |",
        "| --- | --- |",
        f"| Frozen audit subject | `{review['frozen_evidence']['audit_baseline_commit']}` |",
        f"| Phase 2 classification | `{review['frozen_evidence']['classification_commit']}` |",
        f"| Frozen registry SHA-256 | `{review['frozen_evidence']['registry_sha256']}` |",
        f"| Approved decision packet | `{review['frozen_evidence']['decision_packet_commit']}` |",
        f"| Phase 3 source commit | `{review['assessment_subject']['source_commit']}` |",
        f"| Attestation state | `{binding['state']}` |",
        f"| Bound attestations | `{len(binding['attestations'])}` |",
    ]
    if binding["state"] == "bound":
        lines.extend(
            [
                "",
                "### Bound Immutable Attestations",
                "",
                "| Attestation | Path | SHA-256 | Environments |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in binding["attestations"]:
            lines.append(
                f"| `{row['attestation_id']}` | `{row['path']}` | "
                f"`{row['sha256']}` | {_join_ids(row['environment_profile_ids'])} |"
            )

    gate = review.get("final_gate")
    if gate is not None:
        lines.extend(
            [
                "",
                "## Review Mode",
                "",
                f"This packet is in `{gate['mode']}` mode.",
            ]
        )
    if gate is not None and gate["mode"] == "final_gate":
        repository_state = gate["repository_state"]
        validation_source = gate["validation_results_source"]
        approval = gate["integration_approval_request"]
        lines.extend(
            [
                "",
                "## Final Gate Evidence",
                "",
                "The packet binds the clean repository input from which it was rendered. "
                "It deliberately does not claim the Git identity of its own containing "
                "commit. The strict external check verifies that final clean commit, its "
                "single parent, its two generated files, unchanged remote `develop`, and any "
                "post-commit validation reported at the approval gate.",
                "",
                "| Repository evidence | Value |",
                "| --- | --- |",
                f"| Feature branch | `{repository_state['branch']}` |",
                f"| Clean packet-input commit | `{repository_state['packet_input_commit']}` |",
                f"| Packet-input tree | `{repository_state['packet_input_tree']}` |",
                "| Integration base ref | "
                f"`{DEVELOP_REMOTE} {DEVELOP_REMOTE_REF}` |",
                "| Unchanged frozen remote develop commit | "
                f"`{repository_state['develop_commit']}` |",
                f"| Input worktree clean | `{repository_state['clean']}` |",
                "",
                "### Recorded Validation Results",
                "",
                f"Source: `{validation_source['path']}` "
                f"(`{validation_source['sha256']}`).",
                "",
                "| Validation | Command argv | Status | Exit | Reason | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for result in gate["validation_results"]:
            command = " ".join(result["command_argv"])
            evidence = _join_ids([row["path"] for row in result["evidence_refs"]])
            lines.append(
                f"| `{result['id']}` | `{command}` | `{result['status']}` | "
                f"`{result['exit_code'] if result['exit_code'] is not None else '—'}` | "
                f"`{result['reason_code'] or '—'}` | {evidence} |"
            )
        lines.extend(
            [
                "",
                "### Explicit Approval Request",
                "",
                f"Status: `{approval['status']}`. Requested action: "
                f"`{approval['requested_action']}` into `{approval['target_branch']}`. "
                f"Guarantee activation included: "
                f"`{approval['guarantee_activation_included']}`.",
            ]
        )

    lines.extend(["", "## How QT Works After Phase 3", ""])
    lines.extend(f"- {item}" for item in review["system_summary"])
    lines.extend(
        [
            "",
            "## Resulting Guarantee State",
            "",
            "| Axis | Accounting |",
            "| --- | --- |",
            f"| Guarantees | `{summary['guarantees']}` |",
            f"| Claim lifecycle | `{summary['claim_lifecycle']}` |",
            f"| Registry disposition | `{summary['registry_disposition']}` |",
            f"| Conformance | `{summary['conformance']}` |",
            f"| Enforcement maturity | `{summary['enforcement_maturity']}` |",
            f"| Proof maturity | `{summary['proof_maturity']}` |",
            f"| Activation | `{summary['activation_status']}` |",
            "",
            "Only the following rows change relative to the frozen registry:",
            "",
            "| Guarantee | Changed fields | Forward disposition | Conformance |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in review["guarantees"]:
        if not row["changed_fields"]:
            continue
        lines.append(
            f"| `{row['id']}` | {', '.join(row['changed_fields'])} | "
            f"`{row['forward_state']['registry_disposition']}` | "
            f"`{row['forward_state']['conformance']}` |"
        )

    lines.extend(
        [
            "",
            "## Proof And Attestation Accounting",
            "",
            f"The catalog contains **{review['proof_accounting']['definition_count']}** definitions: "
            f"`{review['proof_accounting']['lifecycle_counts']}`.",
            "",
        ]
    )
    if binding["state"] == "not_attested":
        lines.append(
            "No immutable attestation is bound, so this review reports no invented PASS, "
            "FAIL, NOT_RUN, or UNAVAILABLE result."
        )
    else:
        lines.append(
            "Every raw proof and guarantee result is retained in the machine view with its "
            "immutable attestation identity. Executed results use only an attestation that "
            "binds the proof's required profile; UNAVAILABLE may record that a profile could "
            "not be realized, and duplicate NOT_RUN rows do not dilute a real result. "
            "Reported guarantee results then apply "
            "the conservative proof-maturity/proposed-proof cap without changing activation."
        )

    lines.extend(
        [
            "",
            "## Remediation Outcomes",
            "",
            f"All **{summary['remediations']}** source records remain proposed, pending review, and open.",
            "",
            f"Outcome accounting: `{summary['remediation_outcomes']}`.",
        ]
    )
    for outcome in ("implemented", "planned", "deferred", "blocked", "unavailable"):
        ids = [
            row["id"]
            for row in review["remediations"]
            if row["phase3_action_outcome"] == outcome
        ]
        lines.extend(["", f"### {outcome.replace('_', ' ').title()}", "", _join_ids(ids)])

    lines.extend(
        [
            "",
            "## Residual Risks",
            "",
            "| ID | Category | Severity | Summary | Exit criterion |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in review["residual_risks"]:
        lines.append(
            f"| `{row['id']}` | `{row['category']}` | `{row['severity']}` | "
            f"{row['summary']} | {row['exit_criteria']} |"
        )

    lines.extend(["", "## Proposed Integration Plan", ""])
    lines.extend(
        f"{index}. {item}" for index, item in enumerate(review["integration_plan"], 1)
    )

    lines.extend(
        [
            "",
            "## Appendix A — All 75 Guarantees",
            "",
            "| Guarantee | Disposition | Conformance | Enforcement | Proof maturity | Proof result | Activation |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in review["guarantees"]:
        state = row["forward_state"]
        proof_status = row["proof_assessment"]["reported_status"] or row["proof_assessment"]["state"]
        lines.append(
            f"| `{row['id']}` | `{state['registry_disposition']}` | "
            f"`{state['conformance']}` | `{state['enforcement_maturity']}` | "
            f"`{state['proof_maturity']}` | `{proof_status}` | "
            f"`{state['activation_status']}` |"
        )

    lines.extend(
        [
            "",
            "## Appendix B — All 85 Proof Definitions",
            "",
            "| Proof | Lifecycle | Environment | Result state | Effective status | Attestations | Guarantees |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in review["proofs"]:
        lines.append(
            f"| `{row['id']}` | `{row['lifecycle']}` | "
            f"`{row['environment_profile_id']}` | `{row['result_state']}` | "
            f"`{row['attested_status'] or '—'}` | "
            f"{_join_ids(row['effective_attestation_ids'])} | "
            f"{_join_ids(row['guarantee_ids'])} |"
        )

    lines.extend(
        [
            "",
            "## Appendix C — All 68 Remediations",
            "",
            "| Remediation | Approved Phase 2 disposition | Phase 3 outcome | Acceptance | Closure | Guarantees |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in review["remediations"]:
        lines.append(
            f"| `{row['id']}` | `{row['approved_phase2_disposition']}` | "
            f"`{row['phase3_action_outcome']}` | `{row['acceptance_state']}` | "
            f"`{row['closure_state']}` | {_join_ids(row['guarantee_ids'])} |"
        )
    return "\n".join(lines) + "\n"


def _write_outputs(root: Path, review: Mapping[str, Any]) -> None:
    review_path = root / REVIEW_PATH.relative_to(ROOT)
    view_path = root / VIEW_PATH.relative_to(ROOT)
    review_path.write_bytes(_canonical_json_bytes(review))
    view_path.write_bytes(render_markdown(review).encode("utf-8"))
    print(f"wrote {review_path.relative_to(root).as_posix()}")
    print(f"wrote {view_path.relative_to(root).as_posix()}")


def _check_outputs(root: Path, *, final_gate_required: bool = False) -> None:
    _validate_review_schema_contract(root)
    review_path = root / REVIEW_PATH.relative_to(ROOT)
    checked = guarantees.load_json_strict(review_path)
    validate_review_data(checked, final_gate_required=final_gate_required)
    binding = checked["attestation_binding"]
    gate = checked.get("final_gate")
    if gate is None:
        # The checked 8029 packet is an intentionally retained historical,
        # pre-attestation fixture. Validate its own canonical bytes and view,
        # but never rebuild it against later catalog/policy material.
        rebuilt = checked
        review_mode = "legacy_historical_intermediate"
    else:
        attestation_paths = [root / row["path"] for row in binding["attestations"]]
        is_final_gate = gate["mode"] == "final_gate"
        validation_results_path = (
            root / gate["validation_results_source"]["path"]
            if is_final_gate
            else None
        )
        rebuilt = build_review(
            root=root,
            source_commit=checked["assessment_subject"]["source_commit"],
            attestation_paths=attestation_paths,
            final_gate=is_final_gate,
            validation_results_path=validation_results_path,
            repository_state=gate["repository_state"] if is_final_gate else None,
        )
        review_mode = gate["mode"]
    if review_path.read_bytes() != _canonical_json_bytes(rebuilt):
        _fail(
            "generated_phase3_final_review_json_stale:run "
            "python scripts/docs/build_phase3_final_review.py render --source-commit <commit>"
        )
    view_path = root / VIEW_PATH.relative_to(ROOT)
    if not view_path.exists() or view_path.read_bytes() != render_markdown(rebuilt).encode("utf-8"):
        _fail(
            "generated_phase3_final_review_markdown_stale:run "
            "python scripts/docs/build_phase3_final_review.py render --source-commit <commit>"
        )
    external_state: dict[str, str | bool] | None = None
    if final_gate_required:
        external_state = _verify_external_approval_repository_state(
            root, rebuilt["final_gate"]["repository_state"]
        )
    print(
        "phase 3 final review valid: "
        f"{len(rebuilt['guarantees'])} guarantees, "
        f"{len(rebuilt['proofs'])} proofs, "
        f"{len(rebuilt['remediations'])} remediations, "
        f"attestation={binding['state']}, mode={review_mode}"
    )
    if external_state is not None:
        print(
            "phase 3 external approval-gate repository evidence: "
            + json.dumps(external_state, sort_keys=True, separators=(",", ":"))
        )


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    render_parser = subparsers.add_parser("render", help="write deterministic JSON and Markdown")
    render_parser.add_argument("--source-commit", required=True)
    render_parser.add_argument(
        "--attestation",
        type=Path,
        action="append",
        default=[],
        help="immutable attestation JSON; repeat once per isolated execution",
    )
    render_parser.add_argument(
        "--final-gate",
        action="store_true",
        help="require the strict final-gate packet inputs",
    )
    render_parser.add_argument(
        "--validation-results",
        type=Path,
        help="immutable, sorted validation-result document for strict final-gate mode",
    )
    check_parser = subparsers.add_parser(
        "check", help="validate checked-in data and generated Markdown"
    )
    check_parser.add_argument(
        "--final-gate",
        action="store_true",
        help="also verify strict final-gate completeness and the external packet commit",
    )
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.command == "render":
            review = build_review(
                root=root,
                source_commit=args.source_commit,
                attestation_paths=args.attestation,
                final_gate=args.final_gate,
                validation_results_path=args.validation_results,
            )
            _write_outputs(root, review)
        else:
            _check_outputs(root, final_gate_required=args.final_gate)
    except (FinalReviewError, guarantees.GuaranteeValidationError) as exc:
        print(f"phase3_final_review_failed:{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())

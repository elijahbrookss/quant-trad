#!/usr/bin/env python3
"""Validate and render QT's guarantee assurance registry.

The registry and proof catalog are durable, commit-independent assurance
metadata. Execution outcomes belong only in commit/environment attestations.
This module intentionally uses only the Python standard library so docs
validation does not depend on the application import graph.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import os
import posixpath
import re
import subprocess
import sys
import tarfile
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterator, Mapping, NoReturn, Sequence


ROOT = Path(__file__).resolve().parents[2]
ASSURANCE_DIR = ROOT / "docs" / "assurance" / "guarantees"
REGISTRY_PATH = ASSURANCE_DIR / "registry.json"
PROOF_CATALOG_PATH = ASSURANCE_DIR / "proof-catalog.json"
VIEW_PATH = ASSURANCE_DIR / "GUARANTEES.md"
GLOSSARY_PATH = (
    ROOT
    / "docs"
    / "plans"
    / "documentation-reconciliation"
    / "proposed-glossary.md"
)
ADOPTED_GLOSSARY_PATH = ROOT / "docs" / "contracts" / "platform" / "04_glossary.md"
CONTRACT_INDEX_PATH = ROOT / "docs" / "contracts" / "README.md"
CANDIDATE_INVENTORY_PATH = (
    ROOT
    / "docs"
    / "plans"
    / "documentation-reconciliation"
    / "guarantee-candidates.md"
)
CAMPAIGN_DIR = ROOT / "docs" / "plans" / "documentation-reconciliation"
PHASE_1_SURFACE_INVENTORY_PATH = CAMPAIGN_DIR / "implementation-surface-inventory.json"
SCHEMA_DIR = ASSURANCE_DIR / "schemas"
SCHEMA_PATHS = {
    "registry": SCHEMA_DIR / "registry.v1.schema.json",
    "proof-catalog": SCHEMA_DIR / "proof-catalog.v1.schema.json",
    "attestation": SCHEMA_DIR / "attestation.v1.schema.json",
    "execution-admission": SCHEMA_DIR / "execution-admission.v1.schema.json",
    "python-wheel-manifest": SCHEMA_DIR / "python-wheel-manifest.v1.schema.json",
    "runner-build-profile": SCHEMA_DIR / "runner-build-profile.v1.schema.json",
    "runner-materialization": SCHEMA_DIR / "runner-materialization.v1.schema.json",
    "runner-build-record": SCHEMA_DIR / "runner-build-record.v1.schema.json",
    "execution-draft": SCHEMA_DIR / "execution-draft.v1.schema.json",
    "execution-manifest": SCHEMA_DIR / "execution-manifest.v1.schema.json",
    "cleanup-manifest": SCHEMA_DIR / "cleanup-manifest.v1.schema.json",
    "cleanup-recovery-intent": SCHEMA_DIR
    / "cleanup-recovery-intent.v1.schema.json",
    "cleanup-recovery-report": SCHEMA_DIR / "cleanup-recovery-report.v1.schema.json",
}

REGISTRY_SCHEMA_VERSION = "qt.guarantee_registry.v1"
PROOF_CATALOG_SCHEMA_VERSION = "qt.guarantee_proof_catalog.v1"
ATTESTATION_SCHEMA_VERSION = "qt.guarantee_attestation.v1"
NODE_TEST_EVENT_SCHEMA_VERSION = "qt.node_test_events.v1"
NODE_TEST_RESULT_SCHEMA_VERSION = "qt.node_test_result.v1"
EXECUTION_ADMISSION_SCHEMA_VERSION = "qt.assurance_execution_admission.v1"
EXECUTION_ADMISSION_ARCHIVE_SCHEMA_VERSION = (
    "qt.assurance_execution_admission_archive.v1"
)
EXECUTION_DRAFT_SCHEMA_VERSION = "qt.assurance_execution_draft.v1"
EXECUTION_MANIFEST_SCHEMA_VERSION = "qt.assurance_execution_manifest.v1"
CLEANUP_MANIFEST_SCHEMA_VERSION = "qt.assurance_cleanup_manifest.v1"
CLEANUP_RECOVERY_INTENT_SCHEMA_VERSION = "qt.assurance_cleanup_recovery_intent.v1"
CLEANUP_RECOVERY_REPORT_SCHEMA_VERSION = "qt.assurance_cleanup_recovery_report.v1"
PYTHON_WHEEL_MANIFEST_SCHEMA_VERSION = "qt.assurance_python_wheel_manifest.v1"
RUNNER_BUILD_PROFILE_SCHEMA_VERSION = "qt.assurance_runner_build_profile.v1"
RUNNER_MATERIALIZATION_SCHEMA_VERSION = "qt.assurance_runner_materialization.v1"
RUNNER_BUILD_RECORD_SCHEMA_VERSION = "qt.assurance_runner_build_record.v1"
PYTHON_WHEEL_MANIFEST_PATH = ROOT / "docker/assurance/python-wheel-manifest.lock.json"
RUNNER_BUILD_PROFILE_PATH = ROOT / "docker/assurance/runner-build.profile.json"

GUARANTEE_ID_RE = re.compile(r"QT-GUAR-[A-Z0-9]+(?:-[A-Z0-9]+)*\Z")
CANDIDATE_ID_RE = re.compile(r"QT-GC-(\d{3})\Z")
PROOF_ID_RE = re.compile(r"QT-PROOF-(\d{3})\Z")
ENFORCEMENT_ID_RE = re.compile(r"QT-ENF-(\d{3})\Z")
TERM_ID_RE = re.compile(r"QT-TERM-(\d{3})\Z")
ACTIVATION_DECISION_ID_RE = re.compile(r"QT-ACT-DEC-(\d{3})\Z")
REMEDIATION_ID_RE = re.compile(r"QT-REM-(\d{3})\Z")
HEX40_RE = re.compile(r"[0-9a-f]{40}\Z")
HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
PROFILE_ID_RE = re.compile(r"[a-z][a-z0-9-]*\Z")
ADMISSION_ID_RE = re.compile(r"[a-z][a-z0-9-]{2,127}\Z")
IMAGE_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SECRET_FACT_KEY_RE = re.compile(
    r"(?:^|_)(?:credential|dsn|password|secret|token)s?(?:_|$)", re.IGNORECASE
)
OWNER_SLUG_RE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
MAKE_TARGET_RE = re.compile(r"[a-zA-Z0-9_.-]+\Z")
MAKE_VARIABLE_RE = re.compile(r"[A-Z][A-Z0-9_]*\Z")
ATTESTATION_ID_RE = re.compile(
    r"QT-ATT-(?P<timestamp>[0-9]{8}T[0-9]{6}Z)-"
    r"(?P<commit>[0-9a-f]{8,40})-(?P<profile>[a-z0-9-]+)\Z"
)
VERSION_CLAUSE_RE = re.compile(r"(>=|<=|==|>|<)\s*([0-9]+(?:\.[0-9]+)*)\Z")
RFC2119_STRONG_RE = re.compile(r"\b(?:MUST(?: NOT)?|SHALL(?: NOT)?|REQUIRED)\b")


def _git_env() -> dict[str, str]:
    """Return a closed Git environment for historical object resolution."""

    env = dict(os.environ)
    for name in list(env):
        if name.startswith("GIT_"):
            env.pop(name, None)
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["LC_ALL"] = "C"
    return env

CLAIM_KINDS = {
    "behavioral_invariant",
    "safety_invariant",
    "authority_boundary",
    "capability_ceiling",
    "operational_policy",
    "governance_policy",
    "documentation_invariant",
}
CLAIM_LIFECYCLES = {"current", "proposed", "superseded", "historical", "unclear"}
REGISTRY_DISPOSITIONS = {
    "enforced",
    "candidate",
    "partially_enforced",
    "implementation_property",
    "superseded",
    "contradicted",
    "unclear",
}
ACTIVATION_STATUSES = {"unactivated", "active"}
REMEDIATION_STATUSES = {"not_required", "pending", "recorded", "resolved"}
REMEDIATION_LIFECYCLES = {"proposed", "active", "resolved", "superseded"}
REGISTRY_PHASES = {"phase_2a_calibration", "whole_system_classification"}
CONFORMANCE_VALUES = {
    "static_aligned",
    "partial",
    "contradicted",
    "unclear",
    "not_assessed",
}
ENFORCEMENT_MATURITIES = {
    "none",
    "named",
    "partial",
    "adequate",
    "defense_in_depth",
    "unclear",
}
PROOF_MATURITIES = {"none", "named", "partial", "adequate", "unclear"}
PROOF_MODES = {"none", "automated", "manual", "mixed"}
CANDIDATE_RELATIONS = {"equivalent", "split_part", "merged_source"}
AUTHORITY_KINDS = {
    "normative_platform_contract",
    "source_module_contract",
    "normative_agent_governance",
    "operational_canonical",
    "decision_record",
    "explanatory_architecture",
    "operational_guidance",
    "historical_evidence",
}
SOURCE_LIFECYCLES = {
    "active",
    "accepted",
    "proposed",
    "draft",
    "superseded",
    "historical",
    "unclear",
}
AUTHORITY_ROLES = {"primary", "supporting", "conflicting", "context"}
AUTHORITY_ROLE_ORDER = {"primary": 0, "supporting": 1, "conflicting": 2, "context": 3}
ENFORCEMENT_KINDS = {
    "runtime_guard",
    "schema_validation",
    "database_constraint",
    "workflow_gate",
    "process_control",
    "structural_boundary",
}
COVERAGE_VALUES = {"complete", "partial", "supporting"}
PROOF_LIFECYCLES = {"active", "proposed", "superseded", "retired"}
PROOF_KINDS = {
    "automated_test",
    "database_integration",
    "static_validation",
    "manual_procedure",
}
EXECUTION_CLASSES = {
    "isolated_container",
    "isolated_database",
    "isolated_recovery",
}
RUNNER_KINDS = {
    "pytest",
    "node_test",
    "vitest",
    "python_script",
    "make_target",
    "manual",
}
ATTESTATION_RESULTS = {
    "PASS",
    "FAIL",
    "NOT_RUN",
    "MANUAL",
    "PARTIAL",
    "UNAVAILABLE",
}
EVIDENCE_ARTIFACT_KINDS = {
    "stdout",
    "stderr",
    "result_summary",
    "manual_evidence",
}
ENVIRONMENT_EVIDENCE_ARTIFACT_KINDS = {
    "base_image_digests",
    "bootstrap_log",
    "cleanup_manifest",
    "cleanup_log",
    "container_identity",
    "database_identity",
    "execution_draft",
    "execution_admission_archive",
    "execution_manifest",
    "extension_versions",
    "image_digest",
    "network_mode",
    "published_endpoint",
    "recovery_manifest",
    "runtime_probe",
    "server_version",
    "source_mount",
}
ISOLATED_ENVIRONMENT_CLASSES = {"isolated_test", "ephemeral_ci"}
ISOLATION_MODES = {"disposable", "session_scoped"}
LIFECYCLE_BINDING_FACTS = {
    "attestation_id",
    "cleanup_manifest_sha256",
    "control_plane_identity_sha256",
    "environment_instance_id",
    "execution_admission_sha256",
    "execution_admission_archive_sha256",
    "execution_draft_sha256",
    "execution_manifest_sha256",
    "proof_results_sha256",
    "runner_build_record_sha256",
    "source_snapshot_sha256",
}
NORMATIVE_AUTHORITY_KINDS = {
    "normative_platform_contract",
}
FORBIDDEN_DURABLE_KEYS = {
    "status",
    "result",
    "results",
    "last_result",
    "last_verified",
    "verified_at",
    "attested_at",
    "started_at",
    "finished_at",
    "exit_code",
    "duration_ms",
    "passed",
    "failed",
    "proof_status",
    "verification_status",
    "attestation_status",
    "generated_at",
}


class GuaranteeValidationError(ValueError):
    """Raised when assurance metadata is structurally or semantically invalid."""


@dataclass(frozen=True)
class ValidationBundle:
    registry: dict[str, Any]
    proof_catalog: dict[str, Any]
    root: Path
    git_object_root: Path | None = None


def _fail(message: str) -> NoReturn:
    raise GuaranteeValidationError(message)


def _reject_constant(value: str) -> None:
    _fail(f"strict_json_non_finite_number:{value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"strict_json_duplicate_key:{key}")
        result[key] = value
    return result


def load_json_strict(path: Path) -> dict[str, Any]:
    """Load a JSON object while rejecting duplicate keys and non-finite numbers."""

    try:
        raw = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except GuaranteeValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail(f"invalid_json:{path}:{exc}")
    if not isinstance(raw, dict):
        _fail(f"json_root_must_be_object:{path}")
    return raw


def _expect_object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{where}:expected_object")
    return value


def _expect_list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"{where}:expected_array")
    return value


def _expect_string(value: Any, where: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str):
        _fail(f"{where}:expected_string")
    if nonempty and not value.strip():
        _fail(f"{where}:empty_string")
    return value


def _expect_bool(value: Any, where: str) -> bool:
    if type(value) is not bool:
        _fail(f"{where}:expected_boolean")
    return value


def _expect_int(value: Any, where: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        _fail(f"{where}:expected_integer")
    if minimum is not None and value < minimum:
        _fail(f"{where}:must_be_at_least:{minimum}")
    return value


def _exact_keys(
    obj: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str] | None = None,
    where: str,
) -> None:
    optional = optional or set()
    keys = set(obj)
    missing = sorted(required - keys)
    extra = sorted(keys - required - optional)
    if missing:
        _fail(f"{where}:missing_keys:{','.join(missing)}")
    if extra:
        _fail(f"{where}:unknown_keys:{','.join(extra)}")


def _enum(value: Any, allowed: set[str], where: str) -> str:
    text = _expect_string(value, where)
    if text not in allowed:
        _fail(f"{where}:invalid_value:{text}")
    return text


def _string_list(
    value: Any,
    where: str,
    *,
    nonempty: bool = False,
    sorted_values: bool = True,
) -> list[str]:
    values = _expect_list(value, where)
    result = [_expect_string(item, f"{where}[{index}]") for index, item in enumerate(values)]
    if nonempty and not result:
        _fail(f"{where}:must_not_be_empty")
    if len(result) != len(set(result)):
        _fail(f"{where}:duplicate_values")
    if sorted_values and result != sorted(result):
        _fail(f"{where}:must_be_sorted")
    return result


def _indexed_summary(value: Any, where: str) -> str:
    """Validate non-normative index prose without creating hidden requirements."""

    text = _expect_string(value, where)
    if text != text.strip() or "\n" in text or "\r" in text:
        _fail(f"{where}:summary_must_be_trimmed_single_line")
    if RFC2119_STRONG_RE.search(text):
        _fail(f"{where}:normative_keyword_forbidden")
    return text


def _indexed_summary_list(
    value: Any,
    where: str,
    *,
    nonempty: bool = False,
) -> list[str]:
    values = _string_list(value, where, nonempty=nonempty)
    for index, item in enumerate(values):
        _indexed_summary(item, f"{where}[{index}]")
    return values


def _id_sort_key(value: str) -> tuple[str]:
    # All numeric IDs are zero padded, so lexical ordering is stable for both
    # numeric catalog IDs and semantic QT-GUAR-* IDs.
    return (value,)


def _require_sorted_ids(values: Sequence[str], where: str) -> None:
    if list(values) != sorted(values, key=_id_sort_key):
        _fail(f"{where}:must_be_sorted_by_id")


def _scan_for_result_state(value: Any, where: str = "durable_input") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key.lower() in FORBIDDEN_DURABLE_KEYS:
                _fail(f"{where}:transient_result_key_forbidden:{key}")
            _scan_for_result_state(nested, f"{where}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _scan_for_result_state(nested, f"{where}[{index}]")
    elif isinstance(value, str) and value in ATTESTATION_RESULTS:
        _fail(f"{where}:transient_result_value_forbidden:{value}")


def _repo_path(root: Path, raw: Any, where: str, *, must_exist: bool = True) -> tuple[str, Path]:
    value = _expect_string(raw, where)
    if "\\" in value:
        _fail(f"{where}:path_must_use_posix_separators:{value}")
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        _fail(f"{where}:path_must_be_repo_relative:{value}")
    candidate = root.joinpath(*pure.parts)
    try:
        candidate.resolve(strict=False).relative_to(root.resolve())
    except ValueError:
        _fail(f"{where}:path_escapes_repository:{value}")
    if must_exist and not candidate.exists():
        _fail(f"{where}:path_missing:{value}")
    if must_exist:
        current = root
        for part in pure.parts:
            try:
                actual = {child.name for child in current.iterdir()}
            except OSError as exc:
                _fail(f"{where}:path_unreadable:{value}:{exc}")
            if part not in actual:
                _fail(f"{where}:path_case_mismatch:{value}")
            current /= part
    return value, candidate


def _validate_locator(
    root: Path,
    path: Path,
    raw: Any,
    where: str,
    *,
    content: str | None = None,
) -> dict[str, Any]:
    locator = _expect_object(raw, where)
    kind = _expect_string(locator.get("kind"), f"{where}.kind")
    if kind == "line_range":
        _exact_keys(locator, required={"kind", "start", "end"}, where=where)
        start = _expect_int(locator["start"], f"{where}.start", minimum=1)
        end = _expect_int(locator["end"], f"{where}.end", minimum=1)
        if end < start:
            _fail(f"{where}:end_before_start")
        try:
            line_count = len(
                (content if content is not None else path.read_text(encoding="utf-8")).splitlines()
            )
        except (OSError, UnicodeError) as exc:
            _fail(f"{where}:referenced_file_unreadable:{exc}")
        if end > line_count:
            _fail(f"{where}:line_range_out_of_bounds:{end}>{line_count}")
    elif kind == "heading":
        _exact_keys(locator, required={"kind", "value"}, where=where)
        expected = _expect_string(locator["value"], f"{where}.value").strip()
        try:
            headings = [
                match.group(1).strip().rstrip("#").rstrip()
                for line in (
                    content if content is not None else path.read_text(encoding="utf-8")
                ).splitlines()
                if (match := re.match(r"^#{1,6}\s+(.+?)\s*$", line))
            ]
        except (OSError, UnicodeError) as exc:
            _fail(f"{where}:referenced_file_unreadable:{exc}")
        count = headings.count(expected)
        if count != 1:
            _fail(f"{where}:heading_must_resolve_once:{expected}:{count}")
    else:
        _fail(f"{where}:invalid_locator_kind:{kind}")
    return locator


def _baseline_reference_text(
    root: Path,
    git_object_root: Path,
    baseline_commit: str,
    path: str,
    resolved: Path,
    where: str,
) -> str:
    """Read a referenced file at the frozen audit baseline when Git is available."""

    if (git_object_root / ".git").exists():
        try:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(git_object_root),
                    "cat-file",
                    "-e",
                    f"{baseline_commit}^{{commit}}",
                ],
                check=True,
                capture_output=True,
                env=_git_env(),
            )
            object_type = subprocess.run(
                [
                    "git",
                    "-C",
                    str(git_object_root),
                    "cat-file",
                    "-t",
                    f"{baseline_commit}:{path}",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=_git_env(),
            ).stdout.strip()
            if object_type != "blob":
                _fail(f"{where}:baseline_reference_must_be_file:{path}")
            content = subprocess.run(
                ["git", "-C", str(git_object_root), "show", f"{baseline_commit}:{path}"],
                check=True,
                capture_output=True,
                env=_git_env(),
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            _fail(f"{where}:baseline_git_reference_unavailable:{path}:{exc}")
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as exc:
            _fail(f"{where}:baseline_reference_not_utf8:{path}:{exc}")
    try:
        return resolved.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        _fail(f"{where}:referenced_file_unreadable:{path}:{exc}")


def _extract_ids(path: Path, pattern: re.Pattern[str]) -> set[str]:
    if not path.exists():
        return set()
    text = path.read_text(encoding="utf-8")
    unanchored = re.compile(pattern.pattern.removesuffix("\\Z"))
    return set(unanchored.findall(text)) if pattern.groups == 0 else {
        match.group(0) for match in unanchored.finditer(text)
    }


def _table_definition_ids(path: Path, pattern: str) -> set[str]:
    if not path.exists():
        return set()
    definition = re.compile(rf"^\|\s*`?({pattern})`?\s*\|")
    return {
        match.group(1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if (match := definition.match(line))
    }


def _finding_ids(root: Path) -> set[str]:
    phase_1 = root / CAMPAIGN_DIR.relative_to(ROOT) / "phase-1-findings.md"
    terminology = root / CAMPAIGN_DIR.relative_to(ROOT) / "terminology-inventory.md"
    return _table_definition_ids(phase_1, r"[A-Z][A-Z0-9-]+-\d{3}") | _table_definition_ids(
        terminology, r"QT-CONFLICT-\d{3}"
    )


def _term_inventory_ids(root: Path) -> set[str]:
    path = root / CAMPAIGN_DIR.relative_to(ROOT) / "terminology-inventory.md"
    if not path.exists():
        _fail(f"term_inventory_missing:{path.relative_to(root).as_posix()}")
    term_ids = _table_definition_ids(path, r"QT-TERM-\d{3}")
    if not term_ids:
        _fail("term_inventory:must_define_at_least_one_term")
    return term_ids


def _candidate_ids(root: Path) -> set[str]:
    path = root / CANDIDATE_INVENTORY_PATH.relative_to(ROOT)
    if not path.exists():
        _fail(f"candidate_inventory_missing:{path.relative_to(root).as_posix()}")
    return _table_definition_ids(path, r"QT-GC-\d{3}")


def _term_entries(root: Path) -> dict[str, dict[str, Any]]:
    path = root / GLOSSARY_PATH.relative_to(ROOT)
    if not path.exists():
        return {}
    allowed_statuses = {"proposed", "blocked", "deferred"}
    inventory_ids = _term_inventory_ids(root)
    entries: dict[str, dict[str, Any]] = {}
    sections: dict[str, list[str]] = {}
    current_id: str | None = None
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if re.match(r"^#{1,6}\s+.*QT-TERM-", line):
            heading = re.fullmatch(
                r"#{2,6}\s+`?(QT-TERM-\d{3})`?(?:\s+[—-]\s+.+)?\s*",
                line,
            )
            if not heading:
                _fail(f"glossary:noncanonical_term_heading:line={line_number}")
            current_id = heading.group(1)
            if current_id in sections:
                _fail(f"glossary:duplicate_term_heading:{current_id}")
            sections[current_id] = []
            continue
        if current_id is not None:
            sections[current_id].append(line)
    for term_id, lines in sections.items():
        if term_id not in inventory_ids:
            _fail(f"glossary:{term_id}:not_in_phase_1_terminology_inventory")
        statuses = [
            match.group(1)
            for line in lines
            if (
                match := re.fullmatch(
                    r"- Proposal status:\s*`?([a-z_]+)`?\s*",
                    line,
                )
            )
        ]
        if len(statuses) != 1:
            _fail(f"glossary:{term_id}:requires_exactly_one_proposal_status")
        status = statuses[0]
        if status not in allowed_statuses:
            _fail(f"glossary:{term_id}:invalid_proposal_status:{status}")
        if status == "blocked":
            section_text = "\n".join(lines)
            if not re.search(r"(?m)^- Conflict handling:", section_text) or not re.search(
                r"\bQT-CONFLICT-\d{3}\b", section_text
            ):
                _fail(f"glossary:{term_id}:blocked_requires_conflict_handling_reference")
        entries[term_id] = {
            "status": status,
            "conflict_ids": sorted(
                set(re.findall(r"\bQT-CONFLICT-\d{3}\b", "\n".join(lines))),
                key=_id_sort_key,
            ),
        }
    return entries


def _adopted_term_entries(root: Path) -> dict[str, str]:
    path = root / ADOPTED_GLOSSARY_PATH.relative_to(ROOT)
    if not path.exists():
        return {}
    inventory_ids = _term_inventory_ids(root)
    contract_index = root / CONTRACT_INDEX_PATH.relative_to(ROOT)
    if not contract_index.exists():
        _fail("adopted_glossary:contracts_index_missing")
    index_text = contract_index.read_text(encoding="utf-8")
    read_order_match = re.search(
        r"(?ms)^## Read Order\s*$\n(?P<body>.*?)(?=^## |\Z)", index_text
    )
    if not read_order_match or len(
        re.findall(
            r"(?m)^\d+\.\s+`platform/04_glossary\.md`\s*$",
            read_order_match.group("body") if read_order_match else "",
        )
    ) != 1:
        _fail("adopted_glossary:not_in_normative_contract_read_order")

    sections: dict[str, list[str]] = {}
    current_id: str | None = None
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if re.match(r"^#{1,6}\s+.*QT-TERM-", line):
            heading = re.fullmatch(
                r"#{2,6}\s+`?(QT-TERM-\d{3})`?(?:\s+[—-]\s+.+)?\s*",
                line,
            )
            if not heading:
                _fail(f"adopted_glossary:noncanonical_term_heading:line={line_number}")
            current_id = heading.group(1)
            if current_id in sections:
                _fail(f"adopted_glossary:duplicate_term_heading:{current_id}")
            sections[current_id] = []
            continue
        if current_id is not None:
            sections[current_id].append(line)
    entries: dict[str, str] = {}
    for term_id, lines in sections.items():
        if term_id not in inventory_ids:
            _fail(
                f"adopted_glossary:{term_id}:not_in_phase_1_terminology_inventory"
            )
        statuses = [
            match.group(1)
            for line in lines
            if (
                match := re.fullmatch(
                    r"- Adoption status:\s*`?([a-z_]+)`?\s*",
                    line,
                )
            )
        ]
        if statuses != ["adopted"]:
            _fail(f"adopted_glossary:{term_id}:requires_explicit_adopted_status")
        entries[term_id] = "adopted"
    return entries


def _frontmatter_status(path: Path, where: str, *, content: str | None = None) -> str:
    try:
        lines = (
            content if content is not None else path.read_text(encoding="utf-8")
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        _fail(f"{where}:authority_unreadable:{exc}")
    if not lines or lines[0].strip() != "---":
        _fail(f"{where}:architecture_authority_requires_status_frontmatter")
    try:
        end = lines.index("---", 1)
    except ValueError:
        _fail(f"{where}:architecture_authority_has_unclosed_frontmatter")
    for line in lines[1:end]:
        if match := re.match(r"^status:\s*([^\s#]+)\s*$", line):
            return match.group(1)
    _fail(f"{where}:architecture_authority_requires_status_frontmatter")


def _strict_scalar_frontmatter(
    lines: Sequence[str],
    end: int,
    *,
    expected_keys: set[str],
    optional_keys: set[str] | None = None,
    where: str,
) -> dict[str, str]:
    allowed_keys = expected_keys | (optional_keys or set())
    frontmatter: dict[str, str] = {}
    for index, line in enumerate(lines[1:end], start=2):
        if not line.strip():
            continue
        match = re.fullmatch(r"([a-z0-9_]+):\s*([^\s#]+)\s*", line)
        if not match:
            _fail(f"{where}:frontmatter_malformed_line:{index}")
        key, value = match.groups()
        if key in frontmatter:
            _fail(f"{where}:frontmatter_duplicate_key:{key}")
        frontmatter[key] = value
    unknown = sorted(set(frontmatter) - allowed_keys)
    if unknown:
        _fail(f"{where}:frontmatter_unknown_keys:{','.join(unknown)}")
    missing = sorted(expected_keys - set(frontmatter))
    if missing:
        _fail(f"{where}:frontmatter_missing_keys:{','.join(missing)}")
    return frontmatter


def _validate_authority_path(
    kind: str,
    path: str,
    resolved: Path,
    source_lifecycle: str,
    where: str,
    *,
    content: str,
) -> None:
    if kind == "normative_platform_contract" and not path.startswith("docs/contracts/platform/"):
        _fail(f"{where}:normative_platform_contract_outside_hierarchy:{path}")
    if kind == "source_module_contract":
        pure = PurePosixPath(path)
        if not (
            (path.startswith("src/") or path.startswith("portal/"))
            and pure.suffix == ".md"
            and "docs" in pure.parts
        ):
            _fail(f"{where}:source_module_contract_requires_module_documentation:{path}")
    if kind == "normative_agent_governance" and path != "AGENTS.md":
        _fail(f"{where}:agent_governance_must_reference_AGENTS.md")
    if kind == "decision_record" and not path.startswith("docs/architecture/decisions/"):
        _fail(f"{where}:decision_record_outside_adr_directory:{path}")
    if kind == "explanatory_architecture" and not path.startswith("docs/architecture/"):
        _fail(f"{where}:architecture_reference_outside_architecture_tree:{path}")
    if kind == "operational_canonical" and not path.startswith("docs/"):
        _fail(f"{where}:operational_canonical_outside_docs_tree:{path}")
    if path.startswith("docs/architecture/"):
        actual_status = _frontmatter_status(resolved, where, content=content)
        if source_lifecycle != actual_status:
            _fail(
                f"{where}:source_lifecycle_frontmatter_mismatch:"
                f"declared={source_lifecycle}:actual={actual_status}"
            )


def _validate_activation_decision(
    path: Path,
    guarantee_id: str,
    decision: Mapping[str, Any],
    where: str,
) -> None:
    """Check decision structure/integrity, not the external reviewer's authority."""

    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        _fail(f"{where}:activation_decision_unreadable:{exc}")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        _fail(f"{where}:activation_decision_requires_frontmatter")
    try:
        frontmatter_end = lines.index("---", 1)
    except ValueError:
        _fail(f"{where}:activation_decision_has_unclosed_frontmatter")
    expected_frontmatter_keys = {
        "status",
        "decision_id",
        "guarantee_id",
        "decision_type",
        "gate_id",
        "outcome",
        "attestation_id",
        "attestation_sha256",
        "reviewed_by",
        "reviewed_at",
        "external_review_system",
        "external_review_id",
        "external_review_sha256",
    }
    frontmatter = _strict_scalar_frontmatter(
        lines,
        frontmatter_end,
        expected_keys=expected_frontmatter_keys,
        where=where,
    )
    if frontmatter.get("status") != "reviewed":
        _fail(f"{where}:activation_decision_must_be_reviewed")
    external = decision["external_review_ref"]
    expected_frontmatter = {
        "decision_id": decision["decision_id"],
        "guarantee_id": guarantee_id,
        "decision_type": decision["decision_type"],
        "gate_id": decision["gate_id"],
        "outcome": decision["outcome"],
        "attestation_id": decision["attestation_id"],
        "attestation_sha256": decision["attestation_sha256"],
        "reviewed_by": decision["reviewed_by"],
        "reviewed_at": decision["reviewed_at"],
        "external_review_system": external["system"],
        "external_review_id": external["reference_id"],
        "external_review_sha256": external["sha256"],
    }
    for key, expected in expected_frontmatter.items():
        if frontmatter.get(key) != expected:
            _fail(f"{where}:activation_decision_frontmatter_mismatch:{key}")


def _validate_remediation_record(
    path: Path,
    guarantee_id: str,
    remediation_id: str,
    lifecycle: str,
    where: str,
) -> None:
    """Bind a concrete remediation record to its ID, guarantee, and lifecycle."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        _fail(f"{where}:remediation_record_unreadable:{exc}")
    if not lines or lines[0].strip() != "---":
        _fail(f"{where}:remediation_record_requires_frontmatter")
    try:
        frontmatter_end = lines.index("---", 1)
    except ValueError:
        _fail(f"{where}:remediation_record_has_unclosed_frontmatter")
    frontmatter = _strict_scalar_frontmatter(
        lines,
        frontmatter_end,
        expected_keys={
            "remediation_id",
            "guarantee_ids",
            "lifecycle",
            "owner",
            "required_reviewers",
            "required_review",
            "review_status",
        },
        optional_keys={"reviewed_by", "reviewed_at"},
        where=where,
    )
    expected = {"remediation_id": remediation_id, "lifecycle": lifecycle}
    for key, value in expected.items():
        if frontmatter.get(key) != value:
            _fail(f"{where}:remediation_record_frontmatter_mismatch:{key}")
    guarantee_ids = [
        item.strip() for item in frontmatter.get("guarantee_ids", "").split(",")
        if item.strip()
    ]
    if (
        not guarantee_ids
        or guarantee_ids != sorted(guarantee_ids, key=_id_sort_key)
        or len(guarantee_ids) != len(set(guarantee_ids))
        or any(not GUARANTEE_ID_RE.fullmatch(item) for item in guarantee_ids)
    ):
        _fail(f"{where}:remediation_record_invalid_guarantee_ids")
    if guarantee_id not in guarantee_ids:
        _fail(f"{where}:remediation_record_does_not_bind_guarantee:{guarantee_id}")
    owner = frontmatter["owner"]
    if not PROFILE_ID_RE.fullmatch(owner):
        _fail(f"{where}:remediation_record_invalid_owner")
    required_reviewers = [
        item.strip()
        for item in frontmatter["required_reviewers"].split(",")
        if item.strip()
    ]
    if (
        not required_reviewers
        or required_reviewers != sorted(required_reviewers)
        or len(required_reviewers) != len(set(required_reviewers))
        or any(not PROFILE_ID_RE.fullmatch(item) for item in required_reviewers)
    ):
        _fail(f"{where}:remediation_record_invalid_required_reviewers")
    if frontmatter["required_review"] != "true":
        _fail(f"{where}:remediation_record_requires_review")
    review_status = frontmatter["review_status"]
    if review_status not in {"pending", "reviewed", "approved"}:
        _fail(f"{where}:remediation_record_invalid_review_status")
    reviewer_fields = {"reviewed_by", "reviewed_at"} & set(frontmatter)
    if review_status == "pending" and reviewer_fields:
        _fail(f"{where}:pending_remediation_forbids_reviewer_fields")
    if review_status in {"reviewed", "approved"}:
        if reviewer_fields != {"reviewed_by", "reviewed_at"}:
            _fail(f"{where}:reviewed_remediation_requires_reviewer_and_timestamp")
        _expect_string(frontmatter["reviewed_by"], f"{where}.reviewed_by")
        _parse_timestamp(frontmatter["reviewed_at"], f"{where}.reviewed_at")
    allowed_review_statuses = {
        "proposed": {"pending", "reviewed"},
        "active": {"reviewed", "approved"},
        "resolved": {"approved"},
        "superseded": {"approved"},
    }
    if review_status not in allowed_review_statuses[lifecycle]:
        _fail(f"{where}:remediation_lifecycle_review_status_mismatch")
    required_headings = [
        "## Gap",
        "## Action",
        "## Acceptance criteria",
        "## Proof plan",
    ]
    heading_positions: list[int] = []
    for heading in required_headings:
        positions = [
            index
            for index, line in enumerate(lines[frontmatter_end + 1 :], start=frontmatter_end + 1)
            if line.strip() == heading
        ]
        if len(positions) != 1:
            _fail(f"{where}:remediation_record_requires_unique_section:{heading[3:]}")
        heading_positions.append(positions[0])
    if heading_positions != sorted(heading_positions):
        _fail(f"{where}:remediation_record_sections_out_of_order")
    for heading, start in zip(required_headings, heading_positions):
        next_heading = next(
            (
                index
                for index in range(start + 1, len(lines))
                if lines[index].startswith("## ")
            ),
            len(lines),
        )
        if not any(
            line.strip() and not line.lstrip().startswith("#")
            for line in lines[start + 1 : next_heading]
        ):
            _fail(f"{where}:remediation_record_empty_section:{heading[3:]}")


def validate_registry_data(
    registry: Mapping[str, Any],
    *,
    root: Path = ROOT,
    git_object_root: Path | None = None,
) -> dict[str, Any]:
    """Validate durable claim metadata without consulting proof execution state."""

    _scan_for_result_state(registry)
    _exact_keys(
        registry,
        required={"schema_version", "audit_baseline_commit", "scope", "guarantees"},
        where="registry",
    )
    if registry["schema_version"] != REGISTRY_SCHEMA_VERSION:
        _fail(f"registry.schema_version:expected:{REGISTRY_SCHEMA_VERSION}")
    baseline = _expect_string(registry["audit_baseline_commit"], "registry.audit_baseline_commit")
    if not HEX40_RE.fullmatch(baseline):
        _fail("registry.audit_baseline_commit:expected_lowercase_40_hex")
    phase_1_inventory_path = root / PHASE_1_SURFACE_INVENTORY_PATH.relative_to(ROOT)
    phase_1_inventory = load_json_strict(phase_1_inventory_path)
    phase_1_baseline = _expect_string(
        phase_1_inventory.get("baseline_commit"),
        "phase_1_surface_inventory.baseline_commit",
    )
    if baseline != phase_1_baseline:
        _fail("registry.audit_baseline_commit:phase_1_baseline_mismatch")
    reference_git_root = git_object_root or root

    scope = _expect_object(registry["scope"], "registry.scope")
    _exact_keys(
        scope,
        required={
            "phase",
            "gate",
            "source_candidate_count",
            "included_candidate_ids",
            "whole_system_classification_complete",
        },
        where="registry.scope",
    )
    phase = _enum(scope["phase"], REGISTRY_PHASES, "registry.scope.phase")
    gate = _enum(scope["gate"], {"gate_2_pending", "gate_2_approved", "complete"}, "registry.scope.gate")
    source_count = _expect_int(scope["source_candidate_count"], "registry.scope.source_candidate_count", minimum=1)
    included = _string_list(
        scope["included_candidate_ids"],
        "registry.scope.included_candidate_ids",
        nonempty=True,
        sorted_values=False,
    )
    for value in included:
        if not CANDIDATE_ID_RE.fullmatch(value):
            _fail(f"registry.scope.included_candidate_ids:invalid_id:{value}")
    _require_sorted_ids(included, "registry.scope.included_candidate_ids")
    complete = _expect_bool(
        scope["whole_system_classification_complete"],
        "registry.scope.whole_system_classification_complete",
    )
    if phase == "phase_2a_calibration":
        if not 10 <= len(included) <= 15:
            _fail("registry.scope:phase_2a_calibration_requires_10_to_15_candidates")
        if complete:
            _fail("registry.scope:calibration_cannot_claim_whole_system_complete")
    if source_count < len(included):
        _fail("registry.scope:source_candidate_count_less_than_included_count")

    inventory_ids = _candidate_ids(root)
    if source_count != len(inventory_ids):
        _fail(
            "registry.scope.source_candidate_count:inventory_mismatch:"
            f"declared={source_count}:actual={len(inventory_ids)}"
        )
    missing_candidates = sorted(set(included) - inventory_ids, key=_id_sort_key)
    if missing_candidates:
        _fail(f"registry.scope:unknown_candidate_ids:{','.join(missing_candidates)}")
    if complete:
        if phase != "whole_system_classification":
            _fail("registry.scope:complete_requires_whole_system_classification_phase")
        if len(inventory_ids) != 75:
            _fail(
                "registry.scope:complete_requires_frozen_75_candidate_inventory:"
                f"actual={len(inventory_ids)}"
            )
        if set(included) != inventory_ids:
            missing = sorted(inventory_ids - set(included), key=_id_sort_key)
            extra = sorted(set(included) - inventory_ids, key=_id_sort_key)
            _fail(
                "registry.scope:complete_candidate_set_mismatch:"
                f"missing={','.join(missing)}:extra={','.join(extra)}"
            )
    term_entries = _term_entries(root)
    adopted_term_entries = _adopted_term_entries(root)
    if gate == "gate_2_pending" and adopted_term_entries:
        _fail("registry.scope.gate_2_pending_forbids_adopted_glossary_entries")
    known_findings = _finding_ids(root)

    rows = _expect_list(registry["guarantees"], "registry.guarantees")
    if not rows:
        _fail("registry.guarantees:must_not_be_empty")
    if phase == "phase_2a_calibration" and not 10 <= len(rows) <= 15:
        _fail("registry.guarantees:phase_2a_calibration_requires_10_to_15_guarantees")
    guarantee_ids: list[str] = []
    guarantee_order_keys: list[tuple[str, str]] = []
    enforcement_ids: set[str] = set()
    remediation_definitions: dict[str, tuple[str, str, str]] = {}
    candidate_usage: dict[str, list[str]] = defaultdict(list)
    normalized: list[dict[str, Any]] = []

    required_entry_keys = {
        "id",
        "candidate_refs",
        "title",
        "claim_summary",
        "claim_scope",
        "failure_semantics",
        "wording_constraints",
        "owners",
        "claim_kind",
        "claim_lifecycle",
        "registry_disposition",
        "activation_status",
        "activation_decision_refs",
        "activation_attestation_refs",
        "conformance",
        "enforcement_maturity",
        "proof_maturity",
        "proof_mode",
        "term_refs",
        "authority_refs",
        "enforcement_refs",
        "finding_refs",
        "remediation_status",
        "remediation_refs",
        "replaced_by_ids",
    }
    for index, raw in enumerate(rows):
        where = f"registry.guarantees[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(row, required=required_entry_keys, where=where)
        guarantee_id = _expect_string(row["id"], f"{where}.id")
        if not GUARANTEE_ID_RE.fullmatch(guarantee_id):
            _fail(f"{where}.id:invalid_guarantee_id:{guarantee_id}")
        guarantee_ids.append(guarantee_id)
        _expect_string(row["title"], f"{where}.title")
        _indexed_summary(row["claim_summary"], f"{where}.claim_summary")
        _indexed_summary_list(row["claim_scope"], f"{where}.claim_scope", nonempty=True)
        _indexed_summary_list(
            row["failure_semantics"], f"{where}.failure_semantics", nonempty=True
        )
        wording_constraints = _indexed_summary_list(
            row["wording_constraints"], f"{where}.wording_constraints"
        )

        candidate_refs = _expect_list(row["candidate_refs"], f"{where}.candidate_refs")
        if not candidate_refs:
            _fail(f"{where}.candidate_refs:must_not_be_empty")
        candidate_ref_keys: list[tuple[str, str]] = []
        for candidate_index, candidate_raw in enumerate(candidate_refs):
            candidate_where = f"{where}.candidate_refs[{candidate_index}]"
            candidate = _expect_object(candidate_raw, candidate_where)
            _exact_keys(candidate, required={"id", "relation"}, where=candidate_where)
            candidate_id = _expect_string(candidate["id"], f"{candidate_where}.id")
            if not CANDIDATE_ID_RE.fullmatch(candidate_id):
                _fail(f"{candidate_where}.id:invalid_candidate_id:{candidate_id}")
            if candidate_id not in inventory_ids:
                _fail(f"{candidate_where}.id:not_in_phase_1_inventory:{candidate_id}")
            relation = _enum(candidate["relation"], CANDIDATE_RELATIONS, f"{candidate_where}.relation")
            candidate_ref_keys.append((candidate_id, relation))
            candidate_usage[candidate_id].append(relation)
        if candidate_ref_keys != sorted(candidate_ref_keys, key=lambda item: (_id_sort_key(item[0]), item[1])):
            _fail(f"{where}.candidate_refs:must_be_sorted")
        if len(candidate_ref_keys) != len(set(candidate_ref_keys)):
            _fail(f"{where}.candidate_refs:duplicate_reference")
        relations = {relation for _, relation in candidate_ref_keys}
        if len(candidate_ref_keys) > 1 and relations != {"merged_source"}:
            _fail(f"{where}.candidate_refs:multi_candidate_claim_requires_merged_source")
        if len(candidate_ref_keys) == 1 and relations == {"merged_source"}:
            _fail(f"{where}.candidate_refs:merged_source_requires_multiple_candidates")
        guarantee_order_keys.append((candidate_ref_keys[0][0], guarantee_id))

        row_owners = _string_list(row["owners"], f"{where}.owners", nonempty=True)
        for owner in row_owners:
            if not OWNER_SLUG_RE.fullmatch(owner):
                _fail(f"{where}.owners:invalid_owner_slug:{owner}")
        _enum(row["claim_kind"], CLAIM_KINDS, f"{where}.claim_kind")
        claim_lifecycle = _enum(row["claim_lifecycle"], CLAIM_LIFECYCLES, f"{where}.claim_lifecycle")
        disposition = _enum(row["registry_disposition"], REGISTRY_DISPOSITIONS, f"{where}.registry_disposition")
        activation_status = _enum(
            row["activation_status"], ACTIVATION_STATUSES, f"{where}.activation_status"
        )
        if gate == "gate_2_pending" and activation_status != "unactivated":
            _fail(f"{where}:gate_2_pending_requires_unactivated_guarantees")
        conformance = _enum(row["conformance"], CONFORMANCE_VALUES, f"{where}.conformance")
        enforcement_maturity = _enum(
            row["enforcement_maturity"], ENFORCEMENT_MATURITIES, f"{where}.enforcement_maturity"
        )
        proof_maturity = _enum(row["proof_maturity"], PROOF_MATURITIES, f"{where}.proof_maturity")
        proof_mode = _enum(row["proof_mode"], PROOF_MODES, f"{where}.proof_mode")

        term_refs = _string_list(row["term_refs"], f"{where}.term_refs", sorted_values=False)
        _require_sorted_ids(term_refs, f"{where}.term_refs")
        blocked_term_conflicts: set[str] = set()
        for term_id in term_refs:
            if not TERM_ID_RE.fullmatch(term_id):
                _fail(f"{where}.term_refs:invalid_term_id:{term_id}")
            if activation_status != "active" and term_id not in term_entries:
                _fail(f"{where}.term_refs:not_in_proposed_glossary:{term_id}")
            if (
                term_id in term_entries
                and term_entries[term_id]["status"] == "blocked"
            ):
                blocked_term_conflicts.update(term_entries[term_id]["conflict_ids"])

        authority_refs = _expect_list(row["authority_refs"], f"{where}.authority_refs")
        if not authority_refs:
            _fail(f"{where}.authority_refs:must_not_be_empty")
        authority_sort_keys: list[tuple[int, str, str]] = []
        primary_authorities: list[tuple[str, str]] = []
        has_conflicting_authority = False
        conflicting_authority_count = 0
        for authority_index, authority_raw in enumerate(authority_refs):
            authority_where = f"{where}.authority_refs[{authority_index}]"
            authority = _expect_object(authority_raw, authority_where)
            _exact_keys(
                authority,
                required={"path", "locator", "authority_kind", "source_lifecycle", "role"},
                where=authority_where,
            )
            authority_path, resolved = _repo_path(
                root,
                authority["path"],
                f"{authority_where}.path",
                must_exist=False,
            )
            authority_content = _baseline_reference_text(
                root,
                reference_git_root,
                baseline,
                authority_path,
                resolved,
                authority_where,
            )
            locator = _validate_locator(
                root,
                resolved,
                authority["locator"],
                f"{authority_where}.locator",
                content=authority_content,
            )
            authority_kind = _enum(
                authority["authority_kind"], AUTHORITY_KINDS, f"{authority_where}.authority_kind"
            )
            source_lifecycle = _enum(
                authority["source_lifecycle"],
                SOURCE_LIFECYCLES,
                f"{authority_where}.source_lifecycle",
            )
            _validate_authority_path(
                authority_kind,
                authority_path,
                resolved,
                source_lifecycle,
                authority_where,
                content=authority_content,
            )
            role = _enum(authority["role"], AUTHORITY_ROLES, f"{authority_where}.role")
            if role == "primary":
                primary_authorities.append((authority_kind, source_lifecycle))
            if role == "conflicting":
                has_conflicting_authority = True
                conflicting_authority_count += 1
            authority_sort_keys.append(
                (AUTHORITY_ROLE_ORDER[role], authority_path, json.dumps(locator, sort_keys=True))
            )
        if not primary_authorities and not (
            disposition == "contradicted" and conflicting_authority_count >= 2
        ):
            _fail(f"{where}.authority_refs:requires_primary_or_conflicting_pair")
        if authority_sort_keys != sorted(authority_sort_keys):
            _fail(f"{where}.authority_refs:must_be_sorted")

        enforcement_refs = _expect_list(row["enforcement_refs"], f"{where}.enforcement_refs")
        enforcement_sort_keys: list[str] = []
        complete_enforcement = False
        for enforcement_index, enforcement_raw in enumerate(enforcement_refs):
            enforcement_where = f"{where}.enforcement_refs[{enforcement_index}]"
            enforcement = _expect_object(enforcement_raw, enforcement_where)
            _exact_keys(
                enforcement,
                required={"id", "kind", "path", "locator", "coverage"},
                where=enforcement_where,
            )
            enforcement_id = _expect_string(enforcement["id"], f"{enforcement_where}.id")
            if not ENFORCEMENT_ID_RE.fullmatch(enforcement_id):
                _fail(f"{enforcement_where}.id:invalid_enforcement_id:{enforcement_id}")
            if enforcement_id in enforcement_ids:
                _fail(f"{enforcement_where}.id:duplicate:{enforcement_id}")
            enforcement_ids.add(enforcement_id)
            enforcement_sort_keys.append(enforcement_id)
            _enum(enforcement["kind"], ENFORCEMENT_KINDS, f"{enforcement_where}.kind")
            enforcement_path, resolved = _repo_path(
                root,
                enforcement["path"],
                f"{enforcement_where}.path",
                must_exist=False,
            )
            enforcement_content = _baseline_reference_text(
                root,
                reference_git_root,
                baseline,
                enforcement_path,
                resolved,
                enforcement_where,
            )
            _validate_locator(
                root,
                resolved,
                enforcement["locator"],
                f"{enforcement_where}.locator",
                content=enforcement_content,
            )
            coverage = _enum(enforcement["coverage"], COVERAGE_VALUES, f"{enforcement_where}.coverage")
            complete_enforcement |= coverage == "complete"
        _require_sorted_ids(enforcement_sort_keys, f"{where}.enforcement_refs")
        if enforcement_maturity == "none" and enforcement_refs:
            _fail(f"{where}:enforcement_maturity_none_with_refs")
        if enforcement_maturity not in {"none", "unclear"} and not enforcement_refs:
            _fail(f"{where}:enforcement_maturity_requires_refs")

        finding_refs = _string_list(row["finding_refs"], f"{where}.finding_refs")
        remediation_status = _enum(
            row["remediation_status"],
            REMEDIATION_STATUSES,
            f"{where}.remediation_status",
        )
        remediation_refs = _expect_list(
            row["remediation_refs"], f"{where}.remediation_refs"
        )
        remediation_ref_ids: list[str] = []
        remediation_lifecycles: list[str] = []
        for remediation_index, remediation_raw in enumerate(remediation_refs):
            remediation_where = f"{where}.remediation_refs[{remediation_index}]"
            remediation = _expect_object(remediation_raw, remediation_where)
            _exact_keys(
                remediation,
                required={"id", "path", "locator", "lifecycle"},
                where=remediation_where,
            )
            remediation_id = _expect_string(
                remediation["id"], f"{remediation_where}.id"
            )
            if not REMEDIATION_ID_RE.fullmatch(remediation_id):
                _fail(f"{remediation_where}.id:invalid_remediation_id:{remediation_id}")
            remediation_ref_ids.append(remediation_id)
            remediation_path_text, remediation_path = _repo_path(
                root, remediation["path"], f"{remediation_where}.path"
            )
            expected_remediation_path = (
                "docs/assurance/guarantees/remediations/"
                f"{remediation_id}.md"
            )
            if remediation_path_text != expected_remediation_path:
                _fail(
                    f"{remediation_where}.path:expected_remediation_record_path:"
                    f"{expected_remediation_path}"
                )
            _validate_locator(
                root,
                remediation_path,
                remediation["locator"],
                f"{remediation_where}.locator",
            )
            remediation_lifecycle = _enum(
                remediation["lifecycle"],
                REMEDIATION_LIFECYCLES,
                f"{remediation_where}.lifecycle",
            )
            remediation_lifecycles.append(remediation_lifecycle)
            remediation_signature = (
                remediation_path_text,
                json.dumps(remediation["locator"], sort_keys=True),
                remediation_lifecycle,
            )
            previous_signature = remediation_definitions.setdefault(
                remediation_id, remediation_signature
            )
            if previous_signature != remediation_signature:
                _fail(f"{remediation_where}.id:conflicting_definition:{remediation_id}")
            _validate_remediation_record(
                remediation_path,
                guarantee_id,
                remediation_id,
                remediation_lifecycle,
                remediation_where,
            )
        _require_sorted_ids(remediation_ref_ids, f"{where}.remediation_refs")
        for ref in finding_refs:
            if ref not in known_findings:
                _fail(f"{where}.finding_refs:unknown_definition:{ref}")
        if blocked_term_conflicts:
            if activation_status != "unactivated":
                _fail(f"{where}:blocked_term_requires_unactivated_guarantee")
            indexed_wording = "\n".join(wording_constraints)
            missing_blocked_conflicts = sorted(
                (
                    conflict_id
                    for conflict_id in blocked_term_conflicts
                    if conflict_id not in finding_refs
                    and not re.search(
                        rf"\b{re.escape(conflict_id)}\b", indexed_wording
                    )
                ),
                key=_id_sort_key,
            )
            if missing_blocked_conflicts:
                _fail(
                    f"{where}:blocked_term_requires_conflict_acknowledgement:"
                    + ",".join(missing_blocked_conflicts)
                )
        if remediation_status in {"not_required", "pending"} and remediation_refs:
            _fail(f"{where}:{remediation_status}_remediation_requires_empty_refs")
        if remediation_status in {"recorded", "resolved"} and not remediation_refs:
            _fail(f"{where}:{remediation_status}_remediation_requires_refs")
        if remediation_status == "resolved" and any(
            lifecycle != "resolved" for lifecycle in remediation_lifecycles
        ):
            _fail(f"{where}:resolved_remediation_requires_resolved_refs")
        replaced_by_ids = _string_list(
            row["replaced_by_ids"], f"{where}.replaced_by_ids", sorted_values=False
        )
        _require_sorted_ids(replaced_by_ids, f"{where}.replaced_by_ids")
        for replacement in replaced_by_ids:
            if not GUARANTEE_ID_RE.fullmatch(replacement):
                _fail(f"{where}.replaced_by_ids:invalid_guarantee_id:{replacement}")

        activation_decision_refs = _expect_list(
            row["activation_decision_refs"], f"{where}.activation_decision_refs"
        )
        activation_decision_ids: list[str] = []
        decision_attestation_bindings: dict[str, tuple[str, datetime]] = {}
        for decision_index, decision_raw in enumerate(activation_decision_refs):
            decision_where = f"{where}.activation_decision_refs[{decision_index}]"
            decision = _expect_object(decision_raw, decision_where)
            _exact_keys(
                decision,
                required={
                    "decision_id",
                    "decision_type",
                    "gate_id",
                    "outcome",
                    "attestation_id",
                    "attestation_sha256",
                    "reviewed_by",
                    "reviewed_at",
                    "external_review_ref",
                    "path",
                    "locator",
                    "sha256",
                },
                where=decision_where,
            )
            decision_id = _expect_string(decision["decision_id"], f"{decision_where}.decision_id")
            if not ACTIVATION_DECISION_ID_RE.fullmatch(decision_id):
                _fail(f"{decision_where}.decision_id:invalid:{decision_id}")
            if decision["decision_type"] != "guarantee_activation":
                _fail(f"{decision_where}.decision_type:expected_guarantee_activation")
            if decision["gate_id"] != "activation_review":
                _fail(f"{decision_where}.gate_id:expected_activation_review")
            if decision["outcome"] != "approved":
                _fail(f"{decision_where}.outcome:activation_requires_approved")
            bound_attestation_id = _expect_string(
                decision["attestation_id"], f"{decision_where}.attestation_id"
            )
            if not ATTESTATION_ID_RE.fullmatch(bound_attestation_id):
                _fail(f"{decision_where}.attestation_id:invalid")
            bound_attestation_sha256 = _expect_string(
                decision["attestation_sha256"],
                f"{decision_where}.attestation_sha256",
            )
            if not HEX64_RE.fullmatch(bound_attestation_sha256):
                _fail(f"{decision_where}.attestation_sha256:invalid")
            reviewed_by = _expect_string(
                decision["reviewed_by"], f"{decision_where}.reviewed_by"
            )
            if reviewed_by != reviewed_by.strip() or any(
                character in reviewed_by for character in "\r\n"
            ):
                _fail(f"{decision_where}.reviewed_by:invalid_identity")
            reviewed_at = _expect_string(
                decision["reviewed_at"], f"{decision_where}.reviewed_at"
            )
            reviewed_at_timestamp = _parse_timestamp(
                reviewed_at, f"{decision_where}.reviewed_at"
            )
            external_review = _expect_object(
                decision["external_review_ref"], f"{decision_where}.external_review_ref"
            )
            _exact_keys(
                external_review,
                required={"system", "reference_id", "sha256"},
                where=f"{decision_where}.external_review_ref",
            )
            external_system = _expect_string(
                external_review["system"],
                f"{decision_where}.external_review_ref.system",
            )
            if not PROFILE_ID_RE.fullmatch(external_system):
                _fail(f"{decision_where}.external_review_ref.system:invalid_slug")
            external_reference_id = _expect_string(
                external_review["reference_id"],
                f"{decision_where}.external_review_ref.reference_id",
            )
            if external_reference_id != external_reference_id.strip() or any(
                character in external_reference_id for character in "\r\n"
            ):
                _fail(f"{decision_where}.external_review_ref.reference_id:invalid")
            external_hash = _expect_string(
                external_review["sha256"],
                f"{decision_where}.external_review_ref.sha256",
            )
            if not HEX64_RE.fullmatch(external_hash):
                _fail(f"{decision_where}.external_review_ref.sha256:invalid")
            decision_path, decision_file = _repo_path(
                root, decision["path"], f"{decision_where}.path"
            )
            expected_prefix = "docs/assurance/guarantees/activation-decisions/"
            if not decision_path.startswith(expected_prefix) or not decision_path.endswith(".md"):
                _fail(f"{decision_where}.path:outside_activation_decisions:{decision_path}")
            _validate_locator(root, decision_file, decision["locator"], f"{decision_where}.locator")
            observed_hash = _expect_string(decision["sha256"], f"{decision_where}.sha256")
            if not HEX64_RE.fullmatch(observed_hash) or observed_hash != _sha256_file(decision_file):
                _fail(f"{decision_where}.sha256:mismatch")
            _validate_activation_decision(
                decision_file, guarantee_id, decision, decision_where
            )
            activation_decision_ids.append(decision_id)
            if bound_attestation_id in decision_attestation_bindings:
                _fail(
                    f"{decision_where}.attestation_id:duplicate_binding:"
                    f"{bound_attestation_id}"
                )
            decision_attestation_bindings[bound_attestation_id] = (
                bound_attestation_sha256,
                reviewed_at_timestamp,
            )
        _require_sorted_ids(activation_decision_ids, f"{where}.activation_decision_refs")
        if len(activation_decision_ids) != len(set(activation_decision_ids)):
            _fail(f"{where}.activation_decision_refs:duplicate_ids")

        activation_attestation_refs = _expect_list(
            row["activation_attestation_refs"], f"{where}.activation_attestation_refs"
        )
        activation_attestation_ids: list[str] = []
        activation_attestation_metadata: dict[str, tuple[str, datetime]] = {}
        for attestation_index, attestation_raw in enumerate(activation_attestation_refs):
            attestation_where = (
                f"{where}.activation_attestation_refs[{attestation_index}]"
            )
            attestation_ref = _expect_object(attestation_raw, attestation_where)
            _exact_keys(
                attestation_ref,
                required={"attestation_id", "path", "sha256"},
                where=attestation_where,
            )
            referenced_attestation_id = _expect_string(
                attestation_ref["attestation_id"], f"{attestation_where}.attestation_id"
            )
            match = ATTESTATION_ID_RE.fullmatch(referenced_attestation_id)
            if not match:
                _fail(
                    f"{attestation_where}.attestation_id:invalid:{referenced_attestation_id}"
                )
            attestation_path, attestation_file = _repo_path(
                root, attestation_ref["path"], f"{attestation_where}.path"
            )
            attestation_parts = PurePosixPath(attestation_path).parts
            expected_prefix_parts = (
                "docs",
                "assurance",
                "guarantees",
                "attestations",
            )
            if not (
                len(attestation_parts) == 6
                and attestation_parts[:4] == expected_prefix_parts
                and HEX40_RE.fullmatch(attestation_parts[4])
                and attestation_parts[4].startswith(match.group("commit"))
                and attestation_parts[5] == f"{referenced_attestation_id}.json"
            ):
                _fail(
                    f"{attestation_where}.path:invalid_attestation_layout:{attestation_path}"
                )
            observed_hash = _expect_string(
                attestation_ref["sha256"], f"{attestation_where}.sha256"
            )
            if not HEX64_RE.fullmatch(observed_hash) or observed_hash != _sha256_file(attestation_file):
                _fail(f"{attestation_where}.sha256:mismatch")
            referenced_attestation = load_json_strict(attestation_file)
            if referenced_attestation.get("attestation_id") != referenced_attestation_id:
                _fail(f"{attestation_where}:attestation_id_content_mismatch")
            attestation_finished = _parse_timestamp(
                referenced_attestation.get("finished_at"),
                f"{attestation_where}.finished_at",
            )
            activation_attestation_metadata[referenced_attestation_id] = (
                observed_hash,
                attestation_finished,
            )
            activation_attestation_ids.append(referenced_attestation_id)
        if activation_attestation_ids != sorted(activation_attestation_ids):
            _fail(f"{where}.activation_attestation_refs:must_be_sorted_by_id")
        if len(activation_attestation_ids) != len(set(activation_attestation_ids)):
            _fail(f"{where}.activation_attestation_refs:duplicate_ids")
        if set(decision_attestation_bindings) != set(activation_attestation_metadata):
            _fail(f"{where}:activation_decisions_must_bind_exact_attestation_set")
        for attestation_id, (attestation_hash, finished_at) in (
            activation_attestation_metadata.items()
        ):
            decision_hash, reviewed_at = decision_attestation_bindings[attestation_id]
            if decision_hash != attestation_hash:
                _fail(
                    f"{where}:activation_decision_attestation_hash_mismatch:"
                    f"{attestation_id}"
                )
            if reviewed_at < finished_at:
                _fail(
                    f"{where}:activation_decision_precedes_attestation_completion:"
                    f"{attestation_id}"
                )

        if proof_maturity == "none" and proof_mode != "none":
            _fail(f"{where}:proof_maturity_none_requires_proof_mode_none")
        if proof_maturity not in {"none", "unclear"} and proof_mode == "none":
            _fail(f"{where}:proof_maturity_requires_non_none_mode")
        has_active_normative_primary = any(
            kind in NORMATIVE_AUTHORITY_KINDS and lifecycle == "active"
            for kind, lifecycle in primary_authorities
        )
        has_clear_intended_primary = any(
            lifecycle in {"active", "accepted"}
            for _, lifecycle in primary_authorities
        )
        enforced_thresholds_met = (
            conformance == "static_aligned"
            and enforcement_maturity in {"adequate", "defense_in_depth"}
            and complete_enforcement
            and proof_maturity == "adequate"
            and proof_mode != "none"
        )
        if disposition == "enforced":
            if claim_lifecycle != "current":
                _fail(f"{where}:enforced_requires_current_claim")
            if not has_active_normative_primary:
                _fail(f"{where}:enforced_requires_primary_normative_active_authority")
            if has_conflicting_authority or remediation_status in {"pending", "recorded"}:
                _fail(f"{where}:enforced_forbidden_with_conflict_or_remediation")
            if not enforced_thresholds_met:
                _fail(
                    f"{where}:enforced_requires_static_alignment_and_complete_adequate_enforcement_proof"
                )
        if disposition == "partially_enforced":
            if claim_lifecycle != "current":
                _fail(f"{where}:partially_enforced_requires_current_claim")
            if not has_clear_intended_primary:
                _fail(f"{where}:partially_enforced_requires_clear_intended_primary_source")
            if has_conflicting_authority:
                _fail(f"{where}:partially_enforced_forbidden_with_conflicting_authority")
            if not enforcement_refs or proof_maturity in {"none", "unclear"}:
                _fail(
                    f"{where}:partially_enforced_requires_enforcement_and_proof_model"
                )
            if enforced_thresholds_met:
                _fail(f"{where}:partially_enforced_cannot_meet_all_enforced_thresholds")
        if disposition == "superseded":
            if claim_lifecycle != "superseded" or not replaced_by_ids:
                _fail(f"{where}:superseded_requires_lifecycle_and_replacement")
        elif replaced_by_ids:
            _fail(f"{where}:only_superseded_entries_may_have_replaced_by_ids")
        if disposition == "contradicted" and not (
            conformance == "contradicted" or has_conflicting_authority or finding_refs
        ):
            _fail(f"{where}:contradicted_requires_conformance_or_conflict_reference")
        if not wording_constraints and not (
            disposition == "contradicted"
            and conformance == "contradicted"
            and conflicting_authority_count >= 2
            and finding_refs
        ):
            _fail(
                f"{where}.wording_constraints:empty_requires_explicit_contradicted_semantics"
            )
        if activation_status == "unactivated":
            if activation_decision_refs or activation_attestation_refs:
                _fail(f"{where}:unactivated_forbids_activation_references")
        if activation_status == "active":
            if disposition != "enforced":
                _fail(f"{where}:active_requires_enforced_disposition")
            if claim_lifecycle != "current":
                _fail(f"{where}:active_requires_current_claim")
            if conformance != "static_aligned":
                _fail(f"{where}:active_requires_static_aligned_conformance")
            if enforcement_maturity not in {"adequate", "defense_in_depth"} or not complete_enforcement:
                _fail(f"{where}:active_requires_complete_adequate_enforcement")
            if proof_maturity != "adequate" or proof_mode == "none":
                _fail(f"{where}:active_requires_adequate_proof_model")
            if not has_active_normative_primary:
                _fail(f"{where}:active_requires_primary_normative_authority")
            if (
                has_conflicting_authority
                or finding_refs
                or remediation_status in {"pending", "recorded"}
            ):
                _fail(f"{where}:active_forbidden_with_open_conflict_or_remediation")
            unadopted_terms = sorted(
                term_id for term_id in term_refs if term_id not in adopted_term_entries
            )
            if unadopted_terms:
                _fail(
                    f"{where}:active_requires_adopted_terms:"
                    + ",".join(unadopted_terms)
                )
            if not activation_decision_refs:
                _fail(f"{where}:active_requires_reviewed_activation_decision")
            if not activation_attestation_refs:
                _fail(f"{where}:active_requires_activation_attestation")
        normalized.append(dict(row))

        if complete and disposition in {"partially_enforced", "contradicted"}:
            if remediation_status not in {"recorded", "resolved"} or not remediation_refs:
                _fail(
                    f"{where}:whole_system_complete_requires_concrete_remediation"
                )

    if len(guarantee_ids) != len(set(guarantee_ids)):
        _fail("registry.guarantees:duplicate_ids")
    if guarantee_order_keys != sorted(
        guarantee_order_keys, key=lambda item: (_id_sort_key(item[0]), item[1])
    ):
        _fail("registry.guarantees:must_be_sorted_by_primary_candidate_id_then_guarantee_id")
    guarantee_id_set = set(guarantee_ids)
    for index, row in enumerate(normalized):
        for replacement in row["replaced_by_ids"]:
            if replacement not in guarantee_id_set:
                _fail(f"registry.guarantees[{index}].replaced_by_ids:unknown:{replacement}")
            if replacement == row["id"]:
                _fail(f"registry.guarantees[{index}].replaced_by_ids:self_reference")

    graph = {row["id"]: list(row["replaced_by_ids"]) for row in normalized}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            _fail(f"registry.guarantees:replacement_cycle:{node}")
        if node in visited:
            return
        visiting.add(node)
        for child in graph[node]:
            visit(child)
        visiting.remove(node)
        visited.add(node)

    for guarantee_id in guarantee_ids:
        visit(guarantee_id)

    used_candidates = set(candidate_usage)
    if used_candidates != set(included):
        missing = sorted(set(included) - used_candidates, key=_id_sort_key)
        extra = sorted(used_candidates - set(included), key=_id_sort_key)
        _fail(
            "registry.scope:included_candidate_mismatch:"
            f"missing={','.join(missing)}:extra={','.join(extra)}"
        )
    for candidate_id, relations in candidate_usage.items():
        if len(relations) > 1 and set(relations) != {"split_part"}:
            _fail(f"registry.guarantees:candidate_reused_without_split_part:{candidate_id}")
        if len(relations) == 1 and relations[0] == "split_part":
            _fail(f"registry.guarantees:orphan_split_part:{candidate_id}")
    return dict(registry)


def _validate_pytest_selector(root: Path, selector: Any, where: str) -> None:
    text = _expect_string(selector, where)
    if "::" not in text:
        _fail(f"{where}:pytest_selector_requires_node:{text}")
    raw_path, *nodes = text.split("::")
    path_text, path = _repo_path(root, raw_path, f"{where}.path")
    if not path_text.endswith(".py") or not path.is_file():
        _fail(f"{where}:pytest_selector_requires_python_file:{path_text}")
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path_text)
    except (OSError, UnicodeError, SyntaxError) as exc:
        _fail(f"{where}:pytest_selector_unparseable:{exc}")
    current: list[ast.stmt] = tree.body
    for raw_node in nodes:
        node_name = raw_node.split("[", 1)[0]
        match = next(
            (
                item
                for item in current
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                and item.name == node_name
            ),
            None,
        )
        if match is None:
            _fail(f"{where}:pytest_node_missing:{raw_node}")
        current = match.body if isinstance(match, ast.ClassDef) else []


def _validate_runner(root: Path, raw: Any, where: str) -> str:
    runner = _expect_object(raw, where)
    kind = _enum(runner.get("kind"), RUNNER_KINDS, f"{where}.kind")
    if kind == "pytest":
        _exact_keys(
            runner,
            required={"kind", "selectors"},
            optional={"required_executables"},
            where=where,
        )
        selectors = _string_list(runner["selectors"], f"{where}.selectors", nonempty=True)
        for index, selector in enumerate(selectors):
            _validate_pytest_selector(root, selector, f"{where}.selectors[{index}]")
        if "required_executables" in runner:
            requirements = _expect_object(
                runner["required_executables"], f"{where}.required_executables"
            )
            if not requirements:
                _fail(f"{where}.required_executables:must_not_be_empty")
            if list(requirements) != sorted(requirements):
                _fail(f"{where}.required_executables:must_be_sorted")
            for executable, constraint in requirements.items():
                if not re.fullmatch(r"[a-zA-Z0-9_.+-]+", executable):
                    _fail(f"{where}.required_executables.key:invalid:{executable}")
                _parse_version_constraint(
                    _expect_string(
                        constraint, f"{where}.required_executables.{executable}"
                    ),
                    f"{where}.required_executables.{executable}",
                )
    elif kind == "node_test":
        _exact_keys(
            runner,
            required={
                "kind",
                "files",
                "event_transport",
                "expected_test_names",
                "expected_excluded_nonmatch_count",
            },
            optional={"name_patterns"},
            where=where,
        )
        files = _string_list(runner["files"], f"{where}.files", nonempty=True)
        for index, file_name in enumerate(files):
            _, path = _repo_path(root, file_name, f"{where}.files[{index}]")
            if path.suffix not in {".js", ".mjs", ".cjs"} or not path.is_file():
                _fail(f"{where}.files[{index}]:node_test_requires_javascript_file")
        expected_names = _string_list(
            runner["expected_test_names"],
            f"{where}.expected_test_names",
            nonempty=True,
        )
        expected_excluded_count = _expect_int(
            runner["expected_excluded_nonmatch_count"],
            f"{where}.expected_excluded_nonmatch_count",
            minimum=0,
        )
        if "name_patterns" in runner:
            patterns = _string_list(
                runner["name_patterns"], f"{where}.name_patterns", nonempty=True
            )
            try:
                compiled_patterns = [re.compile(pattern) for pattern in patterns]
            except re.error as exc:
                _fail(f"{where}.name_patterns:invalid_regular_expression:{exc}")
            for expected_name in expected_names:
                if not any(pattern.fullmatch(expected_name) for pattern in compiled_patterns):
                    _fail(
                        f"{where}.expected_test_names:not_selected_by_pattern:"
                        f"{expected_name}"
                    )
        elif expected_excluded_count:
            _fail(
                f"{where}.expected_excluded_nonmatch_count:"
                "requires_name_patterns"
            )
        transport = _expect_object(runner["event_transport"], f"{where}.event_transport")
        _exact_keys(
            transport,
            required={"path", "schema_version"},
            where=f"{where}.event_transport",
        )
        transport_path, resolved_transport = _repo_path(
            root, transport["path"], f"{where}.event_transport.path"
        )
        if resolved_transport.suffix != ".mjs" or not resolved_transport.is_file():
            _fail(
                f"{where}.event_transport.path:requires_mjs_file:{transport_path}"
            )
        if transport["schema_version"] != NODE_TEST_EVENT_SCHEMA_VERSION:
            _fail(
                f"{where}.event_transport.schema_version:expected:"
                f"{NODE_TEST_EVENT_SCHEMA_VERSION}"
            )
    elif kind == "vitest":
        _exact_keys(runner, required={"kind", "files"}, optional={"name_patterns"}, where=where)
        files = _string_list(runner["files"], f"{where}.files", nonempty=True)
        for index, file_name in enumerate(files):
            _, path = _repo_path(root, file_name, f"{where}.files[{index}]")
            if path.suffix not in {
                ".js",
                ".jsx",
                ".mjs",
                ".cjs",
                ".ts",
                ".tsx",
                ".mts",
                ".cts",
            }:
                _fail(f"{where}.files[{index}]:unsupported_test_file")
        if "name_patterns" in runner:
            _string_list(runner["name_patterns"], f"{where}.name_patterns", nonempty=True)
    elif kind == "python_script":
        _exact_keys(runner, required={"kind", "path", "args"}, where=where)
        path_text, path = _repo_path(root, runner["path"], f"{where}.path")
        if path.suffix != ".py" or not path.is_file():
            _fail(f"{where}.path:python_script_requires_py_file:{path_text}")
        _string_list(runner["args"], f"{where}.args", sorted_values=False)
    elif kind == "make_target":
        _exact_keys(runner, required={"kind", "target"}, optional={"variables"}, where=where)
        target = _expect_string(runner["target"], f"{where}.target")
        if not MAKE_TARGET_RE.fullmatch(target):
            _fail(f"{where}.target:invalid_make_target:{target}")
        makefile = root / "Makefile"
        if not makefile.exists() or not re.search(
            rf"(?m)^{re.escape(target)}(?:\s+[^:]*)?:", makefile.read_text(encoding="utf-8")
        ):
            _fail(f"{where}.target:make_target_missing:{target}")
        variables = _expect_object(runner.get("variables", {}), f"{where}.variables")
        for key, value in variables.items():
            if not MAKE_VARIABLE_RE.fullmatch(key):
                _fail(f"{where}.variables:invalid_name:{key}")
            _expect_string(value, f"{where}.variables.{key}", nonempty=False)
    else:
        _exact_keys(runner, required={"kind", "procedure_ref"}, where=where)
        procedure = _expect_object(runner["procedure_ref"], f"{where}.procedure_ref")
        _exact_keys(procedure, required={"path", "locator"}, where=f"{where}.procedure_ref")
        _, path = _repo_path(root, procedure["path"], f"{where}.procedure_ref.path")
        _validate_locator(root, path, procedure["locator"], f"{where}.procedure_ref.locator")
    return kind


def _canonical_runner_argv(runner: Mapping[str, Any]) -> list[str] | None:
    """Translate a typed runner to its exact, shell-free attestation argv."""

    kind = runner["kind"]
    if kind == "pytest":
        argv = ["python", "-m", "pytest", *runner["selectors"]]
    elif kind == "node_test":
        reporter_path = runner["event_transport"]["path"]
        argv = ["node", "--test", f"--test-reporter=./{reporter_path}"]
        for pattern in runner.get("name_patterns", []):
            argv.extend(["--test-name-pattern", pattern])
        argv.extend(runner["files"])
    elif kind == "vitest":
        argv = ["npx", "vitest", "run", *runner["files"]]
        for pattern in runner.get("name_patterns", []):
            argv.extend(["--testNamePattern", pattern])
    elif kind == "python_script":
        argv = ["python", runner["path"], *runner["args"]]
    elif kind == "make_target":
        argv = ["make", runner["target"]]
        argv.extend(
            f"{key}={value}" for key, value in sorted(runner.get("variables", {}).items())
        )
    else:
        return None
    if any(not item or "\0" in item or "\n" in item or "\r" in item for item in argv):
        _fail("proof_catalog.runner:canonical_argv_contains_invalid_argument")
    return argv


def _minimum_collected_count(runner: Mapping[str, Any]) -> int:
    if runner["kind"] == "pytest":
        return len(runner["selectors"])
    if runner["kind"] == "node_test":
        return len(runner["expected_test_names"])
    if runner["kind"] == "vitest":
        return len(runner["files"])
    return 1


def validate_proof_catalog_data(
    catalog: Mapping[str, Any], registry: Mapping[str, Any], *, root: Path = ROOT
) -> dict[str, Any]:
    """Validate proof definitions and their many-to-many claim coverage."""

    _scan_for_result_state(catalog)
    _exact_keys(
        catalog,
        required={"schema_version", "environment_profiles", "proofs"},
        where="proof_catalog",
    )
    if catalog["schema_version"] != PROOF_CATALOG_SCHEMA_VERSION:
        _fail(f"proof_catalog.schema_version:expected:{PROOF_CATALOG_SCHEMA_VERSION}")
    guarantee_rows = _expect_list(registry["guarantees"], "registry.guarantees")
    guarantee_by_id = {row["id"]: row for row in guarantee_rows}

    profiles = _expect_list(catalog["environment_profiles"], "proof_catalog.environment_profiles")
    if not profiles:
        _fail("proof_catalog.environment_profiles:must_not_be_empty")
    profile_ids: list[str] = []
    for index, raw in enumerate(profiles):
        where = f"proof_catalog.environment_profiles[{index}]"
        profile = _expect_object(raw, where)
        _exact_keys(
            profile,
            required={
                "id",
                "execution_class",
                "runtime_definition",
                "python",
                "lockfiles",
                "required_services",
            },
            optional={"node", "runner_build_profile"},
            where=where,
        )
        profile_id = _expect_string(profile["id"], f"{where}.id")
        if not PROFILE_ID_RE.fullmatch(profile_id):
            _fail(f"{where}.id:invalid_profile_id:{profile_id}")
        profile_ids.append(profile_id)
        execution_class = _enum(
            profile["execution_class"], EXECUTION_CLASSES, f"{where}.execution_class"
        )
        runtime_definition, runtime_path = _repo_path(
            root, profile["runtime_definition"], f"{where}.runtime_definition"
        )
        if not runtime_path.is_file():
            _fail(f"{where}.runtime_definition:must_be_file:{runtime_definition}")
        if execution_class in {"isolated_container", "isolated_database"}:
            if "runner_build_profile" not in profile:
                _fail(f"{where}.runner_build_profile:required_for_automated_profile")
            build_profile, build_profile_path = _repo_path(
                root,
                profile["runner_build_profile"],
                f"{where}.runner_build_profile",
            )
            if not build_profile_path.is_file():
                _fail(
                    f"{where}.runner_build_profile:must_be_file:{build_profile}"
                )
        elif "runner_build_profile" in profile:
            _fail(f"{where}.runner_build_profile:forbidden_for_nonautomated_profile")
        python_constraint = _expect_string(profile["python"], f"{where}.python")
        _parse_version_constraint(python_constraint, f"{where}.python")
        if "node" in profile:
            node_constraint = _expect_string(profile["node"], f"{where}.node")
            _parse_version_constraint(node_constraint, f"{where}.node")
        lockfiles = _string_list(profile["lockfiles"], f"{where}.lockfiles", nonempty=True)
        for lock_index, lockfile in enumerate(lockfiles):
            _, path = _repo_path(root, lockfile, f"{where}.lockfiles[{lock_index}]")
            if not path.is_file():
                _fail(f"{where}.lockfiles[{lock_index}]:must_be_file")
        _string_list(profile["required_services"], f"{where}.required_services")
    if len(profile_ids) != len(set(profile_ids)):
        _fail("proof_catalog.environment_profiles:duplicate_ids")
    if profile_ids != sorted(profile_ids):
        _fail("proof_catalog.environment_profiles:must_be_sorted")
    profile_id_set = set(profile_ids)

    proofs = _expect_list(catalog["proofs"], "proof_catalog.proofs")
    proof_ids: list[str] = []
    coverage_by_guarantee: dict[
        str, list[tuple[str, str, bool, str, str, str, str]]
    ] = defaultdict(list)
    for index, raw in enumerate(proofs):
        where = f"proof_catalog.proofs[{index}]"
        proof = _expect_object(raw, where)
        _exact_keys(
            proof,
            required={
                "id",
                "title",
                "lifecycle",
                "proof_kind",
                "environment_profile_id",
                "runner",
                "timeout_seconds",
                "coverage",
            },
            where=where,
        )
        proof_id = _expect_string(proof["id"], f"{where}.id")
        if not PROOF_ID_RE.fullmatch(proof_id):
            _fail(f"{where}.id:invalid_proof_id:{proof_id}")
        proof_ids.append(proof_id)
        _expect_string(proof["title"], f"{where}.title")
        proof_lifecycle = _enum(
            proof["lifecycle"], PROOF_LIFECYCLES, f"{where}.lifecycle"
        )
        proof_kind = _enum(proof["proof_kind"], PROOF_KINDS, f"{where}.proof_kind")
        profile_id = _expect_string(proof["environment_profile_id"], f"{where}.environment_profile_id")
        if profile_id not in profile_id_set:
            _fail(f"{where}.environment_profile_id:unknown:{profile_id}")
        runner_kind = _validate_runner(root, proof["runner"], f"{where}.runner")
        if proof_kind == "manual_procedure" and runner_kind != "manual":
            _fail(f"{where}:manual_procedure_requires_manual_runner")
        if proof_kind != "manual_procedure" and runner_kind == "manual":
            _fail(f"{where}:manual_runner_requires_manual_procedure_kind")
        if proof_kind == "database_integration" and runner_kind not in {
            "pytest",
            "python_script",
            "make_target",
        }:
            _fail(f"{where}:database_integration_has_incompatible_runner")
        _expect_int(proof["timeout_seconds"], f"{where}.timeout_seconds", minimum=1)
        coverage = _expect_list(proof["coverage"], f"{where}.coverage")
        if not coverage:
            _fail(f"{where}.coverage:must_not_be_empty")
        coverage_keys: list[str] = []
        for coverage_index, coverage_raw in enumerate(coverage):
            coverage_where = f"{where}.coverage[{coverage_index}]"
            link = _expect_object(coverage_raw, coverage_where)
            _exact_keys(
                link,
                required={"guarantee_id", "strength", "required_for_full_attestation"},
                where=coverage_where,
            )
            guarantee_id = _expect_string(link["guarantee_id"], f"{coverage_where}.guarantee_id")
            if guarantee_id not in guarantee_by_id:
                _fail(f"{coverage_where}.guarantee_id:unknown:{guarantee_id}")
            strength = _enum(link["strength"], COVERAGE_VALUES, f"{coverage_where}.strength")
            required = _expect_bool(
                link["required_for_full_attestation"],
                f"{coverage_where}.required_for_full_attestation",
            )
            if required and proof_lifecycle not in {"active", "proposed"}:
                _fail(f"{coverage_where}:required_proof_must_be_active_or_proposed")
            coverage_keys.append(guarantee_id)
            coverage_by_guarantee[guarantee_id].append(
                (
                    proof_id,
                    strength,
                    required,
                    runner_kind,
                    proof_kind,
                    profile_id,
                    proof_lifecycle,
                )
            )
        if coverage_keys != sorted(coverage_keys, key=_id_sort_key):
            _fail(f"{where}.coverage:must_be_sorted")
        if len(coverage_keys) != len(set(coverage_keys)):
            _fail(f"{where}.coverage:duplicate_guarantee_link")
    if len(proof_ids) != len(set(proof_ids)):
        _fail("proof_catalog.proofs:duplicate_ids")
    _require_sorted_ids(proof_ids, "proof_catalog.proofs")

    for guarantee_id, row in guarantee_by_id.items():
        links = coverage_by_guarantee.get(guarantee_id, [])
        active_links = [link for link in links if link[6] == "active"]
        model_links = [link for link in links if link[6] in {"active", "proposed"}]
        maturity = row["proof_maturity"]
        mode = row["proof_mode"]
        required_links = [link for link in active_links if link[2]]
        required_model_links = [link for link in model_links if link[2]]
        if maturity == "none":
            if links:
                _fail(f"{guarantee_id}:proof_maturity_none_with_catalog_links")
            continue
        if maturity == "unclear" and not links:
            if mode != "none":
                _fail(f"{guarantee_id}:unclear_without_links_requires_proof_mode_none")
            continue
        if not links:
            _fail(f"{guarantee_id}:proof_maturity_requires_catalog_link")
        if maturity == "adequate":
            if any(link[6] != "active" for link in required_model_links):
                _fail(f"{guarantee_id}:adequate_required_proof_must_be_active")
            if not any(
                strength == "complete" and required
                for _, strength, required, _, _, _, _ in active_links
            ):
                _fail(f"{guarantee_id}:adequate_proof_requires_complete_required_link")
        if maturity == "partial" and any(
            strength == "complete" and required
            for _, strength, required, _, _, _, _ in active_links
        ):
            _fail(f"{guarantee_id}:partial_proof_cannot_have_complete_required_link")
        if not required_model_links and maturity in {"partial", "adequate"}:
            _fail(f"{guarantee_id}:proof_maturity_requires_required_link")
        if row["registry_disposition"] == "partially_enforced" and not required_model_links:
            _fail(f"{guarantee_id}:partially_enforced_requires_required_proof_link")
        runner_kinds = {link[3] for link in model_links}
        if mode == "none" and links:
            _fail(f"{guarantee_id}:proof_mode_none_with_catalog_links")
        if mode == "automated" and "manual" in runner_kinds:
            _fail(f"{guarantee_id}:automated_mode_has_manual_proof")
        if mode == "manual" and runner_kinds != {"manual"}:
            _fail(f"{guarantee_id}:manual_mode_requires_only_manual_proofs")
        proof_signatures = {
            (link[3], link[4], link[5]) for link in required_model_links
        }
        if mode == "mixed":
            required_runner_kinds = {link[3] for link in required_model_links}
            if "manual" not in required_runner_kinds or not (
                required_runner_kinds - {"manual"}
            ):
                _fail(
                    f"{guarantee_id}:mixed_mode_requires_required_automated_and_manual_proofs"
                )
            if len(proof_signatures) < 2:
                _fail(f"{guarantee_id}:mixed_mode_requires_multiple_proof_signatures")
        if row["activation_status"] == "active" and not any(
            strength == "complete" and required
            for _, strength, required, _, _, _, _ in active_links
        ):
            _fail(f"{guarantee_id}:active_guarantee_missing_complete_required_proof")
    return dict(catalog)


def _schema_value(schema: Mapping[str, Any], path: Sequence[str], where: str) -> Any:
    current: Any = schema
    for key in path:
        if not isinstance(current, dict) or key not in current:
            _fail(f"{where}:missing_schema_path:{'.'.join(path)}")
        current = current[key]
    return current


def _schema_set(
    schema: Mapping[str, Any], path: Sequence[str], expected: set[str], where: str
) -> None:
    raw = _schema_value(schema, path, where)
    if not isinstance(raw, list) or set(raw) != expected or len(raw) != len(expected):
        _fail(f"{where}:schema_set_mismatch:{'.'.join(path)}")


def validate_schema_contracts(*, root: Path = ROOT) -> None:
    """Keep the published interchange schemas aligned with executable v1 shapes."""

    schemas = {
        name: load_json_strict(root / path.relative_to(ROOT))
        for name, path in SCHEMA_PATHS.items()
    }
    for name, schema in schemas.items():
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            _fail(f"schema.{name}:expected_draft_2020_12")
        if schema.get("additionalProperties") is not False:
            _fail(f"schema.{name}:top_level_must_be_strict")

    lifecycle_schema_versions = {
        "execution-admission": (
            ("properties", "schema_version", "const"),
            EXECUTION_ADMISSION_SCHEMA_VERSION,
        ),
        "execution-draft": (
            ("$defs", "facts", "properties", "record_schema_version", "const"),
            EXECUTION_DRAFT_SCHEMA_VERSION,
        ),
        "execution-manifest": (
            ("$defs", "facts", "properties", "record_schema_version", "const"),
            EXECUTION_MANIFEST_SCHEMA_VERSION,
        ),
        "cleanup-manifest": (
            ("$defs", "facts", "properties", "record_schema_version", "const"),
            CLEANUP_MANIFEST_SCHEMA_VERSION,
        ),
        "cleanup-recovery-report": (
            ("properties", "schema_version", "const"),
            CLEANUP_RECOVERY_REPORT_SCHEMA_VERSION,
        ),
        "cleanup-recovery-intent": (
            ("properties", "schema_version", "const"),
            CLEANUP_RECOVERY_INTENT_SCHEMA_VERSION,
        ),
        "python-wheel-manifest": (
            ("properties", "schema_version", "const"),
            PYTHON_WHEEL_MANIFEST_SCHEMA_VERSION,
        ),
        "runner-build-profile": (
            ("properties", "schema_version", "const"),
            RUNNER_BUILD_PROFILE_SCHEMA_VERSION,
        ),
        "runner-materialization": (
            ("properties", "schema_version", "const"),
            RUNNER_MATERIALIZATION_SCHEMA_VERSION,
        ),
        "runner-build-record": (
            ("properties", "schema_version", "const"),
            RUNNER_BUILD_RECORD_SCHEMA_VERSION,
        ),
    }
    for schema_name, (path, expected) in lifecycle_schema_versions.items():
        if _schema_value(
            schemas[schema_name], path, f"schema.{schema_name}"
        ) != expected:
            _fail(f"schema.{schema_name}:schema_version_mismatch")

    registry = schemas["registry"]
    if _schema_value(registry, ("properties", "schema_version", "const"), "schema.registry") != REGISTRY_SCHEMA_VERSION:
        _fail("schema.registry:schema_version_mismatch")
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "claim_kind", "enum"),
        CLAIM_KINDS,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "claim_lifecycle", "enum"),
        CLAIM_LIFECYCLES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "registry_disposition", "enum"),
        REGISTRY_DISPOSITIONS,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "activation_status", "enum"),
        ACTIVATION_STATUSES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "remediation_status", "enum"),
        REMEDIATION_STATUSES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "remediationRef", "properties", "lifecycle", "enum"),
        REMEDIATION_LIFECYCLES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "scope", "properties", "phase", "enum"),
        REGISTRY_PHASES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "conformance", "enum"),
        CONFORMANCE_VALUES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "enforcement_maturity", "enum"),
        ENFORCEMENT_MATURITIES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "proof_maturity", "enum"),
        PROOF_MATURITIES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "properties", "proof_mode", "enum"),
        PROOF_MODES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "candidateRef", "properties", "relation", "enum"),
        CANDIDATE_RELATIONS,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "authorityRef", "properties", "authority_kind", "enum"),
        AUTHORITY_KINDS,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "authorityRef", "properties", "source_lifecycle", "enum"),
        SOURCE_LIFECYCLES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "authorityRef", "properties", "role", "enum"),
        AUTHORITY_ROLES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "enforcementRef", "properties", "kind", "enum"),
        ENFORCEMENT_KINDS,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "enforcementRef", "properties", "coverage", "enum"),
        COVERAGE_VALUES,
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "guarantee", "required"),
        {
            "id",
            "candidate_refs",
            "title",
            "claim_summary",
            "claim_scope",
            "failure_semantics",
            "wording_constraints",
            "owners",
            "claim_kind",
            "claim_lifecycle",
            "registry_disposition",
            "activation_status",
            "activation_decision_refs",
            "activation_attestation_refs",
            "conformance",
            "enforcement_maturity",
            "proof_maturity",
            "proof_mode",
            "term_refs",
            "authority_refs",
            "enforcement_refs",
            "finding_refs",
            "remediation_status",
            "remediation_refs",
            "replaced_by_ids",
        },
        "schema.registry",
    )
    _schema_set(
        registry,
        ("$defs", "activationDecisionRef", "required"),
        {
            "decision_id",
            "decision_type",
            "gate_id",
            "outcome",
            "attestation_id",
            "attestation_sha256",
            "reviewed_by",
            "reviewed_at",
            "external_review_ref",
            "path",
            "locator",
            "sha256",
        },
        "schema.registry",
    )

    proof = schemas["proof-catalog"]
    if _schema_value(proof, ("properties", "schema_version", "const"), "schema.proof") != PROOF_CATALOG_SCHEMA_VERSION:
        _fail("schema.proof:schema_version_mismatch")
    _schema_set(
        proof,
        ("$defs", "proof", "properties", "lifecycle", "enum"),
        PROOF_LIFECYCLES,
        "schema.proof",
    )
    _schema_set(
        proof,
        ("$defs", "proof", "properties", "proof_kind", "enum"),
        PROOF_KINDS,
        "schema.proof",
    )
    _schema_set(
        proof,
        ("$defs", "environmentProfile", "properties", "execution_class", "enum"),
        EXECUTION_CLASSES,
        "schema.proof",
    )
    if _schema_value(
        proof,
        ("$defs", "environmentProfile", "properties", "runner_build_profile", "type"),
        "schema.proof",
    ) != "string":
        _fail("schema.proof:runner_build_profile_shape_mismatch")
    runner_kinds = {
        _schema_value(proof, ("$defs", "pytestRunner", "properties", "kind", "const"), "schema.proof"),
        _schema_value(proof, ("$defs", "nodeRunner", "properties", "kind", "const"), "schema.proof"),
        _schema_value(proof, ("$defs", "vitestRunner", "properties", "kind", "const"), "schema.proof"),
        _schema_value(proof, ("$defs", "pythonRunner", "properties", "kind", "const"), "schema.proof"),
        _schema_value(proof, ("$defs", "makeRunner", "properties", "kind", "const"), "schema.proof"),
        _schema_value(proof, ("$defs", "manualRunner", "properties", "kind", "const"), "schema.proof"),
    }
    if runner_kinds != RUNNER_KINDS:
        _fail("schema.proof:runner_kind_mismatch")
    if _schema_value(
        proof,
        ("$defs", "eventTransport", "properties", "schema_version", "const"),
        "schema.proof",
    ) != NODE_TEST_EVENT_SCHEMA_VERSION:
        _fail("schema.proof:node_event_transport_schema_version_mismatch")

    attestation = schemas["attestation"]
    if _schema_value(attestation, ("properties", "schema_version", "const"), "schema.attestation") != ATTESTATION_SCHEMA_VERSION:
        _fail("schema.attestation:schema_version_mismatch")
    _schema_set(
        attestation,
        ("$defs", "resultStatus", "enum"),
        ATTESTATION_RESULTS,
        "schema.attestation",
    )
    _schema_set(
        attestation,
        ("$defs", "evidenceRef", "properties", "artifact_kind", "enum"),
        EVIDENCE_ARTIFACT_KINDS,
        "schema.attestation",
    )
    _schema_set(
        attestation,
        ("required",),
        {
            "schema_version",
            "attestation_id",
            "source",
            "inputs",
            "environments",
            "started_at",
            "finished_at",
            "proof_results",
            "guarantee_results",
        },
        "schema.attestation",
    )
    proof_result_required = {
        "proof_id",
        "environment_profile_id",
        "status",
        "evidence_refs",
    }
    _schema_set(
        attestation,
        ("$defs", "proofResult", "required"),
        proof_result_required,
        "schema.attestation",
    )
    _schema_set(
        attestation,
        ("$defs", "inputs", "required"),
        {
            "registry_semantics_sha256",
            "proof_catalog_sha256",
            "guarantee_material_sha256",
            "required_proof_material_sha256",
            "glossary_inputs",
        },
        "schema.attestation",
    )
    if _schema_value(
        attestation,
        ("$defs", "proofResult", "properties", "executed_argv", "type"),
        "schema.attestation",
    ) != "array":
        _fail("schema.attestation:executed_argv_shape_mismatch")
    if _schema_value(
        attestation,
        ("$defs", "nodeTestResult", "properties", "schema_version", "const"),
        "schema.attestation",
    ) != NODE_TEST_RESULT_SCHEMA_VERSION:
        _fail("schema.attestation:node_test_result_schema_version_mismatch")
    if _schema_value(
        attestation,
        (
            "$defs",
            "nodeTestResult",
            "properties",
            "transport_schema_version",
            "const",
        ),
        "schema.attestation",
    ) != NODE_TEST_EVENT_SCHEMA_VERSION:
        _fail("schema.attestation:node_event_transport_schema_version_mismatch")


def _validate_runner_build_sources(
    *, root: Path, proof_catalog: Mapping[str, Any]
) -> None:
    """Validate the source-owned portion of the offline runner closure.

    Byte-level wheel and Docker environment checks stay in build_runner.py;
    this standard-library check keeps routine docs validation tied to the exact
    lock/profile model without performing cache discovery or a build.
    """

    manifest_path = root / PYTHON_WHEEL_MANIFEST_PATH.relative_to(ROOT)
    profile_path = root / RUNNER_BUILD_PROFILE_PATH.relative_to(ROOT)
    manifest_bytes = manifest_path.read_bytes()
    profile_bytes = profile_path.read_bytes()
    manifest = load_json_strict(manifest_path)
    profile = load_json_strict(profile_path)
    if manifest_bytes != _canonical_json_bytes(manifest):
        _fail("runner_wheel_manifest:not_canonical_json")
    if profile_bytes != _canonical_json_bytes(profile):
        _fail("runner_build_profile:not_canonical_json")
    if manifest.get("schema_version") != PYTHON_WHEEL_MANIFEST_SCHEMA_VERSION:
        _fail("runner_wheel_manifest:schema_version_mismatch")
    if profile.get("schema_version") != RUNNER_BUILD_PROFILE_SCHEMA_VERSION:
        _fail("runner_build_profile:schema_version_mismatch")
    if manifest.get("requirements_lock_path") != "requirements.lock":
        _fail("runner_wheel_manifest:requirements_lock_path_mismatch")
    entries = _expect_list(manifest.get("entries"), "runner_wheel_manifest.entries")
    names: list[str] = []
    filenames: list[str] = []
    rows: list[str] = []
    selected_bytes = 0
    pins: list[tuple[str, str]] = []
    for line_number, raw_line in enumerate(
        (root / "requirements.lock").read_text(encoding="utf-8").splitlines(), 1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(
            r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]*)",
            line,
        )
        if match is None:
            _fail(f"runner_requirements_lock:{line_number}:exact_pin_required")
        pins.append((re.sub(r"[-_.]+", "-", match.group(1)).lower(), match.group(2)))
    for index, raw in enumerate(entries):
        where = f"runner_wheel_manifest.entries[{index}]"
        entry = _expect_object(raw, where)
        name = _expect_string(entry.get("name"), f"{where}.name")
        version = _expect_string(entry.get("version"), f"{where}.version")
        filename = _expect_string(entry.get("filename"), f"{where}.filename")
        digest = _expect_string(entry.get("sha256"), f"{where}.sha256")
        if not HEX64_RE.fullmatch(digest):
            _fail(f"{where}.sha256:invalid")
        size = entry.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            _fail(f"{where}.size:positive_integer_required")
        names.append(name)
        filenames.append(filename)
        selected_bytes += size
        rows.append(f"{name}=={version}\t{digest}\t{size}\t{filename}\n")
    if names != sorted(names) or len(names) != len(set(names)):
        _fail("runner_wheel_manifest.entries:names_not_unique_sorted")
    folded_filenames = [item.casefold() for item in filenames]
    if len(folded_filenames) != len(set(folded_filenames)):
        _fail("runner_wheel_manifest.entries:filename_collision")
    if [(item["name"], item["version"]) for item in entries] != pins:
        _fail("runner_wheel_manifest.entries:requirements_closure_mismatch")
    aggregate = _expect_object(
        manifest.get("aggregate"), "runner_wheel_manifest.aggregate"
    )
    expected_aggregate = {
        "entry_count": len(entries),
        "selected_bytes": selected_bytes,
        "entry_manifest_sha256": hashlib.sha256(
            "".join(sorted(rows)).encode("utf-8")
        ).hexdigest(),
    }
    if aggregate != expected_aggregate:
        _fail("runner_wheel_manifest.aggregate:mismatch")
    if expected_aggregate != {
        "entry_count": 91,
        "selected_bytes": 223452511,
        "entry_manifest_sha256": (
            "dbf015ad7ff4211cdb4a6fa311a66abc0b540026b502ab9c3eca2b7d487b6b86"
        ),
    }:
        _fail("runner_wheel_manifest:reviewed_calibration_mismatch")
    source_materials = _expect_object(
        profile.get("source_materials"), "runner_build_profile.source_materials"
    )
    if source_materials != {
        "dockerfile": "docker/assurance/frontend-node.Dockerfile",
        "materializer": "scripts/assurance/build_runner.py",
        "requirements_lock": "requirements.lock",
        "wheel_manifest": "docker/assurance/python-wheel-manifest.lock.json",
    }:
        _fail("runner_build_profile.source_materials:mismatch")
    if profile.get("external_order_submission_enabled") is not False:
        _fail("runner_build_profile.external_order_submission_enabled:must_be_false")
    expected_profile_path = RUNNER_BUILD_PROFILE_PATH.relative_to(ROOT).as_posix()
    automated = {
        "isolated_container",
        "isolated_database",
    }
    for environment in proof_catalog["environment_profiles"]:
        if environment["execution_class"] in automated:
            if environment.get("runner_build_profile") != expected_profile_path:
                _fail(
                    f"proof_catalog.environment_profiles.{environment['id']}:"
                    "runner_build_profile_mismatch"
                )


def validate_repository(
    *,
    root: Path = ROOT,
    registry_path: Path | None = None,
    proof_catalog_path: Path | None = None,
) -> ValidationBundle:
    validate_schema_contracts(root=root)
    registry_file = registry_path or root / REGISTRY_PATH.relative_to(ROOT)
    proof_file = proof_catalog_path or root / PROOF_CATALOG_PATH.relative_to(ROOT)
    registry = validate_registry_data(load_json_strict(registry_file), root=root)
    catalog = validate_proof_catalog_data(load_json_strict(proof_file), registry, root=root)
    _validate_runner_build_sources(root=root, proof_catalog=catalog)
    bundle = ValidationBundle(registry=registry, proof_catalog=catalog, root=root)
    validate_activation_evidence(bundle)
    return bundle


def _ref_label(ref: Mapping[str, Any], baseline_commit: str | None = None) -> str:
    locator = ref["locator"]
    baseline_label = f"@{baseline_commit[:8]}" if baseline_commit else ""
    if locator["kind"] == "line_range":
        return (
            f"{ref['path']}{baseline_label}:"
            f"{locator['start']}-{locator['end']}"
        )
    return f"{ref['path']}{baseline_label}#{locator['value']}"


def _ref_link(
    ref: Mapping[str, Any], baseline_commit: str | None = None
) -> str:
    relative = posixpath.relpath(ref["path"], "docs/assurance/guarantees")
    locator = ref["locator"]
    fragment = ""
    if baseline_commit is None:
        fragment = (
            f"#L{locator['start']}-L{locator['end']}"
            if locator["kind"] == "line_range"
            else "#" + re.sub(r"[^a-z0-9]+", "-", locator["value"].lower()).strip("-")
        )
    return f"[{_ref_label(ref, baseline_commit)}]({relative}{fragment})"


def _escape_table(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _anchor_id(kind: str, identifier: str) -> str:
    return f"{kind}-{identifier.lower()}"


def _runner_target_label(runner: Mapping[str, Any]) -> str:
    kind = runner["kind"]
    if kind == "pytest":
        return "; ".join(f"`{selector}`" for selector in runner["selectors"])
    if kind == "node_test":
        files = "; ".join(f"`{path}`" for path in runner["files"])
        names = "; ".join(runner["expected_test_names"])
        patterns = "; ".join(runner.get("name_patterns", []))
        reporter = runner["event_transport"]["path"]
        details = [
            f"expected names: {names}",
            f"excluded nonmatches: {runner['expected_excluded_nonmatch_count']}",
            f"reporter: `{reporter}`",
        ]
        if patterns:
            details.insert(0, f"patterns: {patterns}")
        return files + "; " + "; ".join(details)
    if kind == "vitest":
        files = "; ".join(f"`{path}`" for path in runner["files"])
        patterns = "; ".join(runner.get("name_patterns", []))
        return files + (f"; names: {patterns}" if patterns else "")
    if kind == "python_script":
        argv = [runner["path"], *runner["args"]]
        return " ".join(f"`{part}`" for part in argv)
    if kind == "make_target":
        variables = " ".join(
            f"{key}={value}" for key, value in sorted(runner.get("variables", {}).items())
        )
        return f"`{runner['target']}`" + (f" ({variables})" if variables else "")
    return _ref_link(runner["procedure_ref"])


def render_markdown(registry: Mapping[str, Any], catalog: Mapping[str, Any]) -> str:
    """Return the deterministic, outcome-free human view."""

    coverage_by_guarantee: dict[str, list[str]] = defaultdict(list)
    for proof in catalog["proofs"]:
        for link in proof["coverage"]:
            coverage_by_guarantee[link["guarantee_id"]].append(proof["id"])
    for proof_ids in coverage_by_guarantee.values():
        proof_ids.sort(key=_id_sort_key)

    scope = registry["scope"]
    guarantees = registry["guarantees"]
    baseline = registry["audit_baseline_commit"]
    active_count = sum(row["activation_status"] == "active" for row in guarantees)
    lines = [
        "# QT Guarantee Registry",
        "",
        "Generated by `python scripts/docs/guarantees.py render`. Do not edit by hand.",
        "",
        "This is a non-normative assurance index. Referenced sources retain their",
        "existing authority; this view records classification and traceability only.",
        "Execution outcomes live only in commit/environment attestations.",
        "Artifact validation proves structure, integrity, and internal consistency, not",
        "execution authenticity; external activation review verifies provenance and binds",
        "its decision to the exact attestation digest.",
        "Automated PASS is admitted only for pytest and the catalog-bound native Node",
        "runner in v1; other runner kinds require reviewed typed result semantics.",
        "",
        "## Scope",
        "",
        f"- Audit baseline: `{registry['audit_baseline_commit']}`",
        f"- Phase: `{scope['phase']}`",
        f"- Gate: `{scope['gate']}`",
        f"- Classified candidates: **{len(scope['included_candidate_ids'])} of {scope['source_candidate_count']}**",
        f"- Whole-system classification complete: **{'yes' if scope['whole_system_classification_complete'] else 'no'}**",
        f"- Active guarantees: **{active_count}**",
        "",
        "## Claims",
        "",
        "| ID | Candidate source | Title | Owners | Kind | Disposition | Activation | Claim lifecycle | Conformance | Enforcement | Proof model |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in guarantees:
        candidates = ", ".join(
            f"{ref['id']} ({ref['relation']})" for ref in row["candidate_refs"]
        )
        lines.append(
            "| "
            + " | ".join(
                _escape_table(value)
                for value in (
                    f"[`{row['id']}`](#{_anchor_id('guarantee', row['id'])})",
                    candidates,
                    row["title"],
                    ", ".join(row["owners"]),
                    row["claim_kind"],
                    row["registry_disposition"],
                    row["activation_status"],
                    row["claim_lifecycle"],
                    row["conformance"],
                    row["enforcement_maturity"],
                    f"{row['proof_maturity']} / {row['proof_mode']}",
                )
            )
            + " |"
        )
    for row in guarantees:
        lines.extend(
            [
                "",
                f"<a id=\"{_anchor_id('guarantee', row['id'])}\"></a>",
                f"### {row['id']} — {row['title']}",
                "",
            ]
        )
        lines.append(f"- Indexed claim summary: {row['claim_summary']}")
        lines.append("- Indexed claim scope: " + "; ".join(row["claim_scope"]))
        lines.append(
            "- Indexed failure semantics: " + "; ".join(row["failure_semantics"])
        )
        wording = "; ".join(row["wording_constraints"]) or "none"
        lines.append(f"- Indexed wording constraints: {wording}")
        lines.append(
            "- Authority references: "
            + "; ".join(
                f"{_ref_link(ref, baseline)} (`{ref['role']}`, `{ref['authority_kind']}`, `{ref['source_lifecycle']}`)"
                for ref in row["authority_refs"]
            )
        )
        enforcement = "; ".join(
            f"`{ref['id']}` {_ref_link(ref, baseline)} (`{ref['kind']}`, `{ref['coverage']}`)"
            for ref in row["enforcement_refs"]
        ) or "none"
        lines.append(f"- Enforcement references: {enforcement}")
        proofs = ", ".join(
            f"[`{item}`](#{_anchor_id('proof', item)})"
            for item in coverage_by_guarantee.get(row["id"], [])
        ) or "none"
        lines.append(f"- Proof definitions: {proofs}")
        terms = ", ".join(f"`{item}`" for item in row["term_refs"]) or "none"
        glossary_source = (
            "adopted platform contract"
            if row["activation_status"] == "active"
            else "Gate 2 proposal"
        )
        lines.append(f"- Glossary references ({glossary_source}): {terms}")
        findings = ", ".join(f"`{item}`" for item in row["finding_refs"]) or "none"
        remediations = "; ".join(
            f"`{item['id']}` {_ref_link(item)} (`{item['lifecycle']}`)"
            for item in row["remediation_refs"]
        ) or "none"
        replacements = ", ".join(f"`{item}`" for item in row["replaced_by_ids"]) or "none"
        lines.append(f"- Open finding references: {findings}")
        lines.append(
            f"- Remediation: `{row['remediation_status']}`; references: {remediations}"
        )
        lines.append(f"- Replaced by: {replacements}")
        decisions = "; ".join(
            f"`{ref['decision_id']}` {_ref_link(ref)}" for ref in row["activation_decision_refs"]
        ) or "none"
        attestations = "; ".join(
            f"`{ref['attestation_id']}` (`{ref['path']}`)"
            for ref in row["activation_attestation_refs"]
        ) or "none"
        lines.append(f"- Activation decisions: {decisions}")
        lines.append(f"- Activation attestations: {attestations}")

    lines.extend(
        [
            "",
            "## Proof Definitions",
            "",
            "| ID | Title | Lifecycle | Kind | Runner | Runner target | Environment | Coverage |",
            "|---|---|---|---|---|---|---|---|",
        ]
    )
    for proof in catalog["proofs"]:
        covers = ", ".join(
            f"[`{link['guarantee_id']}`](#{_anchor_id('guarantee', link['guarantee_id'])}) ({link['strength']}; "
            f"{'required' if link['required_for_full_attestation'] else 'supporting'})"
            for link in proof["coverage"]
        )
        lines.append(
            "| "
            + " | ".join(
                _escape_table(value)
                for value in (
                    f"<a id=\"{_anchor_id('proof', proof['id'])}\"></a>[`{proof['id']}`](#{_anchor_id('proof', proof['id'])})",
                    proof["title"],
                    proof["lifecycle"],
                    proof["proof_kind"],
                    proof["runner"]["kind"],
                    _runner_target_label(proof["runner"]),
                    proof["environment_profile_id"],
                    covers,
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _sha256_file(path: Path) -> str:
    """Hash binary bytes, but canonicalize text newlines across Git checkouts."""

    content = path.read_bytes()
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        canonical = content
    else:
        canonical = text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def registry_semantics_bytes(registry: Mapping[str, Any]) -> bytes:
    """Canonicalize durable claim semantics while excluding activation-only links."""

    projected = json.loads(json.dumps(registry))
    for row in projected["guarantees"]:
        row.pop("activation_status", None)
        row.pop("activation_decision_refs", None)
        row.pop("activation_attestation_refs", None)
    return (
        json.dumps(projected, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def registry_semantics_sha256(registry: Mapping[str, Any]) -> str:
    return hashlib.sha256(registry_semantics_bytes(registry)).hexdigest()


def _bound_material_bytes(
    root: Path, relative: str, *, git_commit: str | None = None
) -> bytes:
    """Read Git blob bytes when possible so clean CRLF checkouts hash portably."""

    path = root.joinpath(*PurePosixPath(relative).parts)
    git_available = (root / ".git").exists()
    if git_available and git_commit is not None:
        try:
            return subprocess.run(
                ["git", "-C", str(root), "show", f"{git_commit}:{relative}"],
                check=True,
                capture_output=True,
                env=_git_env(),
            ).stdout
        except (OSError, subprocess.CalledProcessError) as exc:
            _fail(f"bound_material:git_blob_unavailable:{git_commit}:{relative}:{exc}")
    if git_available:
        try:
            status = subprocess.run(
                ["git", "-C", str(root), "status", "--porcelain", "--", relative],
                check=True,
                capture_output=True,
                text=True,
                env=_git_env(),
            ).stdout
            if not status.strip():
                head = subprocess.run(
                    ["git", "-C", str(root), "rev-parse", "HEAD"],
                    check=True,
                    capture_output=True,
                    text=True,
                    env=_git_env(),
                ).stdout.strip()
                return subprocess.run(
                    ["git", "-C", str(root), "show", f"{head}:{relative}"],
                    check=True,
                    capture_output=True,
                    env=_git_env(),
                ).stdout
        except (OSError, subprocess.CalledProcessError):
            # An untracked current file has no blob; its actual bytes remain binding.
            pass
    try:
        return path.read_bytes()
    except OSError as exc:
        _fail(f"bound_material:file_unreadable:{relative}:{exc}")


def _bound_material_sha256(
    root: Path, relative: str, *, git_commit: str | None = None
) -> str:
    return hashlib.sha256(
        _bound_material_bytes(root, relative, git_commit=git_commit)
    ).hexdigest()


def _bound_source_tree(root: Path, git_commit: str) -> str:
    """Resolve the exact Git tree object named by a bound source commit."""

    try:
        tree = subprocess.run(
            ["git", "-C", str(root), "rev-parse", f"{git_commit}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
            env=_git_env(),
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"bound_source_tree:unavailable:{git_commit}:{exc}")
    if not HEX40_RE.fullmatch(tree):
        _fail(f"bound_source_tree:invalid:{git_commit}")
    return tree


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def proof_results_sha256(value: Any) -> str:
    """Hash the complete final proof-result set in canonical proof-id order."""

    rows = _expect_list(value, "attestation.proof_results")
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(rows):
        row = _expect_object(raw, f"attestation.proof_results[{index}]")
        proof_id = _expect_string(
            row.get("proof_id"), f"attestation.proof_results[{index}].proof_id"
        )
        if not PROOF_ID_RE.fullmatch(proof_id):
            _fail(f"attestation.proof_results[{index}].proof_id:invalid")
        normalized.append(dict(row))
    normalized.sort(key=lambda item: _id_sort_key(item["proof_id"]))
    if len({item["proof_id"] for item in normalized}) != len(normalized):
        _fail("attestation.proof_results:duplicate_proof_id")
    return hashlib.sha256(_canonical_json_bytes(normalized)).hexdigest()


def source_snapshot_sha256(root: Path, git_commit: str) -> str:
    """Hash exact Git-archive file paths, executable bits, and bytes."""

    try:
        archive = subprocess.run(
            ["git", "-C", str(root), "archive", "--format=tar", git_commit],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_git_env(),
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"source_snapshot:git_archive_failed:{exc}")
    rows: list[tuple[str, bool, bytes]] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
            for member in handle.getmembers():
                pure = PurePosixPath(member.name)
                if (
                    pure.is_absolute()
                    or not pure.parts
                    or any(part in {"", ".", ".."} for part in pure.parts)
                    or member.issym()
                    or member.islnk()
                ):
                    _fail("source_snapshot:unsafe_archive_member")
                if member.isdir():
                    continue
                if not member.isfile():
                    _fail("source_snapshot:unsupported_archive_member")
                stream = handle.extractfile(member)
                if stream is None:
                    _fail("source_snapshot:archive_member_unreadable")
                rows.append(
                    (pure.as_posix(), bool(member.mode & 0o111), stream.read())
                )
    except (OSError, tarfile.TarError) as exc:
        _fail(f"source_snapshot:archive_invalid:{exc}")
    digest = hashlib.sha256()
    for relative, executable, content in sorted(rows, key=lambda item: item[0]):
        path_bytes = relative.encode("utf-8")
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(b"x" if executable else b"-")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _framed_sha256(components: Sequence[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for label, content in sorted(components, key=lambda item: item[0]):
        encoded_label = label.encode("utf-8")
        digest.update(len(encoded_label).to_bytes(8, "big"))
        digest.update(encoded_label)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _make_target_material(makefile: bytes, target: str) -> bytes:
    try:
        lines = makefile.decode("utf-8").splitlines(keepends=True)
    except UnicodeDecodeError as exc:
        _fail(f"proof_material:Makefile_not_utf8:{exc}")
    blocks: list[str] = []
    for index, line in enumerate(lines):
        if not re.match(rf"^{re.escape(target)}(?:\s+[^:]*)?:", line):
            continue
        block = [line]
        cursor = index + 1
        while cursor < len(lines) and lines[cursor].startswith("\t"):
            block.append(lines[cursor])
            cursor += 1
        blocks.extend(block)
    if not blocks:
        _fail(f"proof_material:make_target_missing:{target}")
    return "".join(blocks).encode("utf-8")


def _proof_runner_material(
    root: Path, runner: Mapping[str, Any], *, git_commit: str | None
) -> list[tuple[str, bytes]]:
    kind = runner["kind"]
    paths: set[str] = set()
    if kind == "pytest":
        paths.update(selector.split("::", 1)[0] for selector in runner["selectors"])
    elif kind in {"node_test", "vitest"}:
        paths.update(runner["files"])
        if kind == "node_test":
            paths.add(runner["event_transport"]["path"])
    elif kind == "python_script":
        paths.add(runner["path"])
    elif kind == "manual":
        paths.add(runner["procedure_ref"]["path"])
    if kind != "make_target":
        return [
            (
                f"runner:{path}",
                _bound_material_bytes(root, path, git_commit=git_commit),
            )
            for path in sorted(paths)
        ]
    makefile = _bound_material_bytes(root, "Makefile", git_commit=git_commit)
    return [
        (
            f"make-target:{runner['target']}",
            _make_target_material(makefile, runner["target"]),
        )
    ]


def required_proof_material_hashes(
    bundle: ValidationBundle, *, git_commit: str | None = None
) -> dict[str, str]:
    """Hash each active required proof without pulling in unrelated repo files."""

    profiles = {
        profile["id"]: profile for profile in bundle.proof_catalog["environment_profiles"]
    }
    result: dict[str, str] = {}
    for proof in bundle.proof_catalog["proofs"]:
        if proof["lifecycle"] != "active" or not any(
            link["required_for_full_attestation"] for link in proof["coverage"]
        ):
            continue
        profile = profiles[proof["environment_profile_id"]]
        components: list[tuple[str, bytes]] = [
            ("proof-definition", _canonical_json_bytes(proof)),
            ("environment-profile", _canonical_json_bytes(profile)),
            (
                f"runtime-definition:{profile['runtime_definition']}",
                _bound_material_bytes(
                    bundle.root,
                    profile["runtime_definition"],
                    git_commit=git_commit,
                ),
            ),
        ]
        if "runner_build_profile" in profile:
            runner_build_profile_bytes = _bound_material_bytes(
                bundle.root,
                profile["runner_build_profile"],
                git_commit=git_commit,
            )
            components.append(
                (
                    f"runner-build-profile:{profile['runner_build_profile']}",
                    runner_build_profile_bytes,
                )
            )
            try:
                runner_build_profile = json.loads(
                    runner_build_profile_bytes.decode("utf-8"),
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_constant,
                )
            except (UnicodeError, json.JSONDecodeError) as exc:
                _fail(f"runner_build_profile:invalid_json:{exc}")
            source_materials = _expect_object(
                _expect_object(
                    runner_build_profile, "runner_build_profile"
                ).get("source_materials"),
                "runner_build_profile.source_materials",
            )
            for material_name, material_path in sorted(source_materials.items()):
                material_relative = _expect_string(
                    material_path,
                    f"runner_build_profile.source_materials.{material_name}",
                )
                components.append(
                    (
                        f"runner-build-material:{material_name}:{material_relative}",
                        _bound_material_bytes(
                            bundle.root,
                            material_relative,
                            git_commit=git_commit,
                        ),
                    )
                )
        components.extend(
            (
                f"lockfile:{path}",
                _bound_material_bytes(bundle.root, path, git_commit=git_commit),
            )
            for path in profile["lockfiles"]
        )
        components.extend(
            _proof_runner_material(
                bundle.root, proof["runner"], git_commit=git_commit
            )
        )
        result[proof["id"]] = _framed_sha256(components)
    return dict(sorted(result.items(), key=lambda item: _id_sort_key(item[0])))


def glossary_inputs(
    bundle: ValidationBundle, *, git_commit: str | None = None
) -> list[dict[str, str]]:
    """Bind every proposal/adopted glossary that supplies a referenced term."""

    referenced_terms = {
        term_id
        for row in bundle.registry["guarantees"]
        for term_id in row["term_refs"]
    }
    if not referenced_terms:
        return []
    sources: list[tuple[str, str, set[str]]] = [
        (
            "proposal",
            GLOSSARY_PATH.relative_to(ROOT).as_posix(),
            set(_term_entries(bundle.root)),
        )
    ]
    adopted_path = bundle.root / ADOPTED_GLOSSARY_PATH.relative_to(ROOT)
    if adopted_path.exists():
        sources.append(
            (
                "adopted_normative",
                ADOPTED_GLOSSARY_PATH.relative_to(ROOT).as_posix(),
                set(_adopted_term_entries(bundle.root)),
            )
        )
    return [
        {
            "source_kind": source_kind,
            "path": path,
            "sha256": _bound_material_sha256(
                bundle.root, path, git_commit=git_commit
            ),
        }
        for source_kind, path, term_ids in sorted(sources)
        if referenced_terms & term_ids
    ]


def guarantee_material_hashes(
    bundle: ValidationBundle, *, git_commit: str | None = None
) -> dict[str, str]:
    """Bind each attestable claim to its referenced source material at a commit."""

    required_guarantee_ids = {
        link["guarantee_id"]
        for proof in bundle.proof_catalog["proofs"]
        if proof["lifecycle"] == "active"
        for link in proof["coverage"]
        if link["required_for_full_attestation"]
    }
    all_glossary_inputs = glossary_inputs(bundle, git_commit=git_commit)
    result: dict[str, str] = {}
    for row in bundle.registry["guarantees"]:
        guarantee_id = row["id"]
        if guarantee_id not in required_guarantee_ids:
            continue
        projected_row = json.loads(json.dumps(row))
        projected_row.pop("activation_status", None)
        projected_row.pop("activation_decision_refs", None)
        projected_row.pop("activation_attestation_refs", None)
        components: list[tuple[str, bytes]] = [
            ("guarantee-semantics", _canonical_json_bytes(projected_row))
        ]
        paths = {
            ref["path"]
            for field in (
                "authority_refs",
                "enforcement_refs",
                "remediation_refs",
            )
            for ref in row[field]
        }
        if row["term_refs"]:
            paths.update(item["path"] for item in all_glossary_inputs)
            if any(
                item["source_kind"] == "adopted_normative"
                for item in all_glossary_inputs
            ):
                paths.add(CONTRACT_INDEX_PATH.relative_to(ROOT).as_posix())
        components.extend(
            (
                f"referenced-source:{path}",
                _bound_material_bytes(bundle.root, path, git_commit=git_commit),
            )
            for path in sorted(paths)
        )
        result[guarantee_id] = _framed_sha256(components)
    return dict(sorted(result.items(), key=lambda item: _id_sort_key(item[0])))


def assurance_material_sha256(
    bundle: ValidationBundle, *, git_commit: str | None = None
) -> str:
    """Hash all material that gives the current assurance mapping meaning."""

    paths: set[str] = {
        PROOF_CATALOG_PATH.relative_to(ROOT).as_posix(),
        "scripts/docs/guarantees.py",
        "scripts/assurance/build_runner.py",
        "scripts/assurance/docker_lifecycle.py",
        "scripts/assurance/verify_guarantees.py",
        "docker/assurance/python-wheel-manifest.lock.json",
        "docker/assurance/runner-build.profile.json",
    }
    for schema_path in SCHEMA_PATHS.values():
        relative = schema_path.relative_to(ROOT)
        if (bundle.root / relative).exists():
            paths.add(relative.as_posix())
    campaign_dir = bundle.root / CAMPAIGN_DIR.relative_to(ROOT)
    if campaign_dir.exists():
        paths.update(
            path.relative_to(bundle.root).as_posix()
            for path in campaign_dir.iterdir()
            if path.is_file() and path.suffix in {".md", ".json"}
        )
    glossary = bundle.root / GLOSSARY_PATH.relative_to(ROOT)
    if glossary.exists():
        paths.add(GLOSSARY_PATH.relative_to(ROOT).as_posix())
    adopted_glossary = bundle.root / ADOPTED_GLOSSARY_PATH.relative_to(ROOT)
    if adopted_glossary.exists():
        paths.add(ADOPTED_GLOSSARY_PATH.relative_to(ROOT).as_posix())
        paths.add(CONTRACT_INDEX_PATH.relative_to(ROOT).as_posix())
    for row in bundle.registry["guarantees"]:
        paths.update(ref["path"] for ref in row["authority_refs"])
        paths.update(ref["path"] for ref in row["enforcement_refs"])
        paths.update(ref["path"] for ref in row["remediation_refs"])
    for profile in bundle.proof_catalog["environment_profiles"]:
        paths.update(profile["lockfiles"])
        paths.add(profile["runtime_definition"])
        if "runner_build_profile" in profile:
            paths.add(profile["runner_build_profile"])
    for proof in bundle.proof_catalog["proofs"]:
        runner = proof["runner"]
        if runner["kind"] == "pytest":
            paths.update(selector.split("::", 1)[0] for selector in runner["selectors"])
        elif runner["kind"] in {"node_test", "vitest"}:
            paths.update(runner["files"])
            if runner["kind"] == "node_test":
                paths.add(runner["event_transport"]["path"])
        elif runner["kind"] == "python_script":
            paths.add(runner["path"])
        elif runner["kind"] == "make_target":
            paths.add("Makefile")
        elif runner["kind"] == "manual":
            paths.add(runner["procedure_ref"]["path"])
    digest = hashlib.sha256()
    registry_relative = REGISTRY_PATH.relative_to(ROOT).as_posix()
    paths.add(registry_relative)
    for relative in sorted(paths):
        content = (
            registry_semantics_bytes(bundle.registry)
            if relative == registry_relative
            else _bound_material_bytes(
                bundle.root, relative, git_commit=git_commit
            )
        )
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _parse_timestamp(value: Any, where: str) -> datetime:
    text = _expect_string(value, where)
    if not text.endswith("Z"):
        _fail(f"{where}:timestamp_must_be_utc_Z")
    try:
        return datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        _fail(f"{where}:invalid_timestamp:{exc}")


def _aggregate_status(statuses: Sequence[str]) -> str:
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


def _aggregate_guarantee_status(
    statuses: Sequence[str],
    *,
    proof_maturity: str,
    required_strengths: Sequence[str],
    has_proposed_required_proof: bool,
) -> str:
    """Derive a guarantee result without promoting an incomplete proof model."""

    status = _aggregate_status(statuses)
    if status != "PASS":
        return status
    if (
        proof_maturity != "adequate"
        or any(strength != "complete" for strength in required_strengths)
        or has_proposed_required_proof
    ):
        return "PARTIAL"
    return "PASS"


def _version_tuple(value: str, where: str) -> tuple[int, ...]:
    match = re.search(r"[0-9]+(?:\.[0-9]+)*", value)
    if not match:
        _fail(f"{where}:version_number_missing:{value}")
    return tuple(int(part) for part in match.group(0).split("."))


def _parse_version_constraint(
    constraint: str, where: str
) -> list[tuple[str, tuple[int, ...]]]:
    clauses = [part.strip() for part in constraint.split(",")]
    if not clauses or any(not part for part in clauses):
        _fail(f"{where}:invalid_version_constraint:{constraint}")
    result: list[tuple[str, tuple[int, ...]]] = []
    for clause in clauses:
        match = VERSION_CLAUSE_RE.fullmatch(clause)
        if not match:
            _fail(f"{where}:unsupported_version_constraint:{clause}")
        operator, expected_text = match.groups()
        result.append((operator, tuple(int(part) for part in expected_text.split("."))))
    return result


def _version_satisfies(observed: str, constraint: str, where: str) -> None:
    observed_parts = _version_tuple(observed, where)
    for operator, expected_parts in _parse_version_constraint(constraint, where):
        width = max(len(observed_parts), len(expected_parts))
        left = observed_parts + (0,) * (width - len(observed_parts))
        right = expected_parts + (0,) * (width - len(expected_parts))
        comparisons = {
            ">=": left >= right,
            "<=": left <= right,
            "==": left == right,
            ">": left > right,
            "<": left < right,
        }
        if not comparisons[operator]:
            _fail(
                f"{where}:version_constraint_not_satisfied:"
                f"observed={observed}:required={constraint}"
            )


def _validate_admission_fact_value(value: Any, where: str) -> None:
    if value is None or type(value) in {bool, int}:
        return
    if isinstance(value, str):
        if not value or any(character in value for character in "\x00\r\n"):
            _fail(f"{where}:invalid_string_fact")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_admission_fact_value(item, f"{where}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            fact_key = _expect_string(key, f"{where}.key")
            if SECRET_FACT_KEY_RE.search(fact_key) and not fact_key.endswith(
                ("_sha256", "_scope", "_mode")
            ):
                if not (
                    fact_key == "credentials" and item == "synthetic_session_only"
                ):
                    _fail(f"{where}.{fact_key}:secret_fact_forbidden")
            _validate_admission_fact_value(item, f"{where}.{fact_key}")
        return
    _fail(f"{where}:unsupported_fact_type")


def _validate_admission_facts(value: Any, where: str) -> dict[str, Any]:
    facts = _expect_object(value, where)
    if not facts:
        _fail(f"{where}:must_not_be_empty")
    _validate_admission_fact_value(facts, where)
    return facts


def _environment_evidence_payload(path: Path, where: str) -> dict[str, Any]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except GuaranteeValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail(f"{where}:invalid_environment_evidence_json:{exc}")
    return _expect_object(payload, where)


_LIFECYCLE_RESOURCE_KINDS = {
    "container",
    "database",
    "network",
    "published_endpoint",
    "source_snapshot",
    "temporary_secret_file",
    "volume",
}


def _lifecycle_hash(value: Any, where: str) -> str:
    digest = _expect_string(value, where)
    if not HEX64_RE.fullmatch(digest):
        _fail(f"{where}:invalid_hash")
    return digest


def _lifecycle_identity(value: Any, where: str) -> str:
    identity = _expect_string(value, where)
    if not re.fullmatch(r"[a-z][a-z0-9-]{7,127}", identity):
        _fail(f"{where}:invalid_identity")
    return identity


def _lifecycle_resources(
    value: Any,
    where: str,
    *,
    runtime_identity: bool,
    disposition: bool = False,
) -> list[dict[str, Any]]:
    rows = _expect_list(value, where)
    if not rows:
        _fail(f"{where}:must_not_be_empty")
    normalized: list[dict[str, Any]] = []
    required = {"kind", "logical_name"}
    if runtime_identity:
        required.add("runtime_identity")
    if disposition:
        required.add("absent")
    for index, raw in enumerate(rows):
        item_where = f"{where}[{index}]"
        item = _expect_object(raw, item_where)
        _exact_keys(item, required=required, where=item_where)
        normalized_item: dict[str, Any] = {
            "kind": _enum(item["kind"], _LIFECYCLE_RESOURCE_KINDS, f"{item_where}.kind"),
            "logical_name": _expect_string(
                item["logical_name"], f"{item_where}.logical_name"
            ),
        }
        if runtime_identity:
            normalized_item["runtime_identity"] = _expect_string(
                item["runtime_identity"], f"{item_where}.runtime_identity"
            )
        if disposition:
            normalized_item["absent"] = _expect_bool(
                item["absent"], f"{item_where}.absent"
            )
        normalized.append(normalized_item)
    keys = [(item["kind"], item["logical_name"]) for item in normalized]
    if keys != sorted(keys):
        _fail(f"{where}:must_be_sorted_by_kind_and_logical_name")
    if len(keys) != len(set(keys)):
        _fail(f"{where}:duplicate_resource_key")
    return normalized


def _validate_execution_admission_archive_facts(
    value: Any, where: str
) -> dict[str, Any]:
    facts = _expect_object(value, where)
    _exact_keys(
        facts,
        required={
            "record_schema_version",
            "source_admission_schema_version",
            "source_commit",
            "execution_admission_sha256",
            "profile",
        },
        where=where,
    )
    if facts["record_schema_version"] != EXECUTION_ADMISSION_ARCHIVE_SCHEMA_VERSION:
        _fail(f"{where}.record_schema_version:unsupported")
    if facts["source_admission_schema_version"] != EXECUTION_ADMISSION_SCHEMA_VERSION:
        _fail(f"{where}.source_admission_schema_version:unsupported")
    source_commit = _expect_string(facts["source_commit"], f"{where}.source_commit")
    if not HEX40_RE.fullmatch(source_commit):
        _fail(f"{where}.source_commit:invalid")
    _lifecycle_hash(
        facts["execution_admission_sha256"],
        f"{where}.execution_admission_sha256",
    )
    profile = _expect_object(facts["profile"], f"{where}.profile")
    _exact_keys(
        profile,
        required={
            "profile_id",
            "admission_id",
            "environment_class",
            "isolation",
            "external_order_submission_enabled",
            "runtime_definition",
            "docker_tool",
            "runner_image",
            "runner_build_record",
            "service_images",
        },
        where=f"{where}.profile",
    )
    profile_id = _expect_string(profile["profile_id"], f"{where}.profile.profile_id")
    if not PROFILE_ID_RE.fullmatch(profile_id):
        _fail(f"{where}.profile.profile_id:invalid")
    admission_id = _expect_string(
        profile["admission_id"], f"{where}.profile.admission_id"
    )
    if not ADMISSION_ID_RE.fullmatch(admission_id):
        _fail(f"{where}.profile.admission_id:invalid")
    _enum(
        profile["environment_class"],
        ISOLATED_ENVIRONMENT_CLASSES,
        f"{where}.profile.environment_class",
    )
    _enum(profile["isolation"], ISOLATION_MODES, f"{where}.profile.isolation")
    if profile["external_order_submission_enabled"] is not False:
        _fail(f"{where}.profile.external_order_submission_enabled:must_be_false")

    def definition(raw: Any, item_where: str) -> None:
        item = _expect_object(raw, item_where)
        _exact_keys(item, required={"path", "sha256"}, where=item_where)
        relative = _expect_string(item["path"], f"{item_where}.path")
        pure = PurePosixPath(relative)
        if (
            "\\" in relative
            or pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            _fail(f"{item_where}.path:relative_safe_path_required")
        _lifecycle_hash(item["sha256"], f"{item_where}.sha256")

    definition(profile["runtime_definition"], f"{where}.profile.runtime_definition")
    docker_tool = _expect_object(profile["docker_tool"], f"{where}.profile.docker_tool")
    _exact_keys(
        docker_tool,
        required={
            "executable_basename",
            "executable_sha256",
            "resolved_path_sha256",
            "daemon_identity_sha256",
            "version",
        },
        where=f"{where}.profile.docker_tool",
    )
    basename = _expect_string(
        docker_tool["executable_basename"],
        f"{where}.profile.docker_tool.executable_basename",
    )
    if Path(basename).name != basename or "/" in basename or "\\" in basename:
        _fail(f"{where}.profile.docker_tool.executable_basename:invalid")
    _expect_string(docker_tool["version"], f"{where}.profile.docker_tool.version")
    for key in (
        "executable_sha256",
        "resolved_path_sha256",
        "daemon_identity_sha256",
    ):
        _lifecycle_hash(docker_tool[key], f"{where}.profile.docker_tool.{key}")
    runner = _expect_object(profile["runner_image"], f"{where}.profile.runner_image")
    _exact_keys(
        runner,
        required={"image_id", "platform", "base_image_digests", "build_definition"},
        where=f"{where}.profile.runner_image",
    )
    if not IMAGE_DIGEST_RE.fullmatch(
        _expect_string(runner["image_id"], f"{where}.profile.runner_image.image_id")
    ):
        _fail(f"{where}.profile.runner_image.image_id:invalid")
    if runner["platform"] != "linux/amd64":
        _fail(f"{where}.profile.runner_image.platform:unsupported")
    base_digests = _string_list(
        runner["base_image_digests"],
        f"{where}.profile.runner_image.base_image_digests",
        sorted_values=True,
    )
    if any(not IMAGE_DIGEST_RE.fullmatch(item) for item in base_digests):
        _fail(f"{where}.profile.runner_image.base_image_digests:invalid")
    definition(runner["build_definition"], f"{where}.profile.runner_image.build_definition")
    build_record = _expect_object(
        profile["runner_build_record"], f"{where}.profile.runner_build_record"
    )
    _exact_keys(
        build_record,
        required={
            "sha256",
            "record_basename",
            "resolved_path_sha256",
            "record",
        },
        where=f"{where}.profile.runner_build_record",
    )
    _lifecycle_hash(
        build_record["sha256"], f"{where}.profile.runner_build_record.sha256"
    )
    _lifecycle_hash(
        build_record["resolved_path_sha256"],
        f"{where}.profile.runner_build_record.resolved_path_sha256",
    )
    record_basename = _expect_string(
        build_record["record_basename"],
        f"{where}.profile.runner_build_record.record_basename",
    )
    if (
        Path(record_basename).name != record_basename
        or "/" in record_basename
        or "\\" in record_basename
    ):
        _fail(f"{where}.profile.runner_build_record.record_basename:invalid")
    archived_record = _expect_object(
        build_record["record"], f"{where}.profile.runner_build_record.record"
    )
    def reject_host_paths(item: Any, item_where: str) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                reject_host_paths(child, f"{item_where}.{key}")
        elif isinstance(item, list):
            for index, child in enumerate(item):
                reject_host_paths(child, f"{item_where}[{index}]")
        elif isinstance(item, str) and (
            PurePosixPath(item).is_absolute()
            or PureWindowsPath(item).is_absolute()
        ):
            _fail(f"{item_where}:absolute_host_path_forbidden")

    reject_host_paths(
        archived_record, f"{where}.profile.runner_build_record.record"
    )
    services = _expect_object(profile["service_images"], f"{where}.profile.service_images")
    if list(services) != sorted(services):
        _fail(f"{where}.profile.service_images:must_be_sorted")
    for service_id, raw_service in services.items():
        if not PROFILE_ID_RE.fullmatch(service_id):
            _fail(f"{where}.profile.service_images.key:invalid")
        service = _expect_object(raw_service, f"{where}.profile.service_images.{service_id}")
        _exact_keys(
            service,
            required={"reference", "image_id", "image_digest"},
            where=f"{where}.profile.service_images.{service_id}",
        )
        _expect_string(service["reference"], f"{where}.profile.service_images.{service_id}.reference")
        for key in ("image_id", "image_digest"):
            if not IMAGE_DIGEST_RE.fullmatch(
                _expect_string(service[key], f"{where}.profile.service_images.{service_id}.{key}")
            ):
                _fail(f"{where}.profile.service_images.{service_id}.{key}:invalid")
    return facts


def _validate_lifecycle_evidence_facts(
    artifact_kind: str, value: Any, where: str
) -> dict[str, Any]:
    if artifact_kind == "execution_admission_archive":
        return _validate_execution_admission_archive_facts(value, where)
    facts = _expect_object(value, where)
    common = {
        "attestation_id",
        "control_plane_identity_sha256",
        "environment_instance_id",
        "source_commit",
        "source_snapshot_sha256",
    }
    if artifact_kind == "execution_draft":
        required = common | {
            "record_schema_version",
            "requested_profile_ids",
            "started_at",
            "admission_id",
            "runtime_definition_sha256",
            "execution_admission_sha256",
            "execution_admission_archive_sha256",
            "runner_build_record_sha256",
            "external_order_submission_enabled",
            "planned_resources",
        }
        _exact_keys(facts, required=required, where=where)
        if facts["record_schema_version"] != EXECUTION_DRAFT_SCHEMA_VERSION:
            _fail(f"{where}.record_schema_version:unsupported")
        requested = _string_list(
            facts["requested_profile_ids"],
            f"{where}.requested_profile_ids",
            nonempty=True,
        )
        if any(not PROFILE_ID_RE.fullmatch(item) for item in requested):
            _fail(f"{where}.requested_profile_ids:invalid_profile_id")
        admission_id = _expect_string(facts["admission_id"], f"{where}.admission_id")
        if not ADMISSION_ID_RE.fullmatch(admission_id):
            _fail(f"{where}.admission_id:invalid")
        _parse_timestamp(facts["started_at"], f"{where}.started_at")
        _lifecycle_hash(
            facts["runtime_definition_sha256"],
            f"{where}.runtime_definition_sha256",
        )
        _lifecycle_hash(
            facts["execution_admission_sha256"],
            f"{where}.execution_admission_sha256",
        )
        _lifecycle_hash(
            facts["execution_admission_archive_sha256"],
            f"{where}.execution_admission_archive_sha256",
        )
        _lifecycle_hash(
            facts["runner_build_record_sha256"],
            f"{where}.runner_build_record_sha256",
        )
        if _expect_bool(
            facts["external_order_submission_enabled"],
            f"{where}.external_order_submission_enabled",
        ):
            _fail(f"{where}.external_order_submission_enabled:must_be_false")
        _lifecycle_resources(
            facts["planned_resources"],
            f"{where}.planned_resources",
            runtime_identity=False,
        )
    elif artifact_kind == "execution_manifest":
        required = common | {
            "record_schema_version",
            "execution_draft_sha256",
            "execution_admission_archive_sha256",
            "execution_state",
            "execution_started_at",
            "execution_finished_at",
            "executed_proof_ids",
            "proof_results_sha256",
            "resource_identities",
        }
        _exact_keys(facts, required=required, where=where)
        if facts["record_schema_version"] != EXECUTION_MANIFEST_SCHEMA_VERSION:
            _fail(f"{where}.record_schema_version:unsupported")
        _lifecycle_hash(
            facts["execution_draft_sha256"], f"{where}.execution_draft_sha256"
        )
        _lifecycle_hash(
            facts["execution_admission_archive_sha256"],
            f"{where}.execution_admission_archive_sha256",
        )
        _lifecycle_hash(
            facts["proof_results_sha256"], f"{where}.proof_results_sha256"
        )
        _enum(
            facts["execution_state"],
            {"complete", "executor_error", "interrupted"},
            f"{where}.execution_state",
        )
        execution_started = _parse_timestamp(
            facts["execution_started_at"], f"{where}.execution_started_at"
        )
        execution_finished = _parse_timestamp(
            facts["execution_finished_at"], f"{where}.execution_finished_at"
        )
        if execution_finished < execution_started:
            _fail(f"{where}:execution_finished_before_started")
        proof_ids = _string_list(
            facts["executed_proof_ids"], f"{where}.executed_proof_ids"
        )
        if any(not PROOF_ID_RE.fullmatch(item) for item in proof_ids):
            _fail(f"{where}.executed_proof_ids:invalid_proof_id")
        resources = _expect_list(
            facts["resource_identities"], f"{where}.resource_identities"
        )
        if facts["execution_state"] == "complete" and not resources:
            _fail(f"{where}.resource_identities:complete_requires_resources")
        if resources:
            _lifecycle_resources(
                resources,
                f"{where}.resource_identities",
                runtime_identity=True,
            )
    elif artifact_kind == "cleanup_manifest":
        required = common | {
            "record_schema_version",
            "execution_draft_sha256",
            "execution_manifest_sha256",
            "execution_admission_archive_sha256",
            "cleanup_started_at",
            "cleanup_finished_at",
            "attempt_number",
            "cleanup_state",
            "exit_code",
            "stdout",
            "stdout_sha256",
            "stderr",
            "stderr_sha256",
            "cleanup_completed",
            "resources",
            "label_query_remaining",
        }
        _exact_keys(facts, required=required, where=where)
        if facts["record_schema_version"] != CLEANUP_MANIFEST_SCHEMA_VERSION:
            _fail(f"{where}.record_schema_version:unsupported")
        for key in (
            "execution_admission_archive_sha256",
            "execution_draft_sha256",
            "execution_manifest_sha256",
        ):
            _lifecycle_hash(facts[key], f"{where}.{key}")
        cleanup_started = _parse_timestamp(
            facts["cleanup_started_at"], f"{where}.cleanup_started_at"
        )
        cleanup_finished = _parse_timestamp(
            facts["cleanup_finished_at"], f"{where}.cleanup_finished_at"
        )
        if cleanup_finished < cleanup_started:
            _fail(f"{where}:cleanup_finished_before_started")
        _expect_int(facts["attempt_number"], f"{where}.attempt_number", minimum=1)
        _enum(
            facts["cleanup_state"],
            {"failed", "interrupted", "passed"},
            f"{where}.cleanup_state",
        )
        _expect_int(facts["exit_code"], f"{where}.exit_code", minimum=0)
        stdout = _expect_string(facts["stdout"], f"{where}.stdout", nonempty=False)
        stderr = _expect_string(facts["stderr"], f"{where}.stderr", nonempty=False)
        for key, raw in (("stdout_sha256", stdout), ("stderr_sha256", stderr)):
            digest = _lifecycle_hash(facts[key], f"{where}.{key}")
            if digest != hashlib.sha256(raw.encode("utf-8")).hexdigest():
                _fail(f"{where}.{key}:content_mismatch")
        _expect_bool(facts["cleanup_completed"], f"{where}.cleanup_completed")
        _lifecycle_resources(
            facts["resources"],
            f"{where}.resources",
            runtime_identity=True,
            disposition=True,
        )
        _string_list(
            facts["label_query_remaining"], f"{where}.label_query_remaining"
        )
    else:  # pragma: no cover - caller has a closed lifecycle-kind set
        _fail(f"{where}:unsupported_lifecycle_artifact_kind:{artifact_kind}")

    if not ATTESTATION_ID_RE.fullmatch(
        _expect_string(facts["attestation_id"], f"{where}.attestation_id")
    ):
        _fail(f"{where}.attestation_id:invalid")
    source_commit = _expect_string(facts["source_commit"], f"{where}.source_commit")
    if not HEX40_RE.fullmatch(source_commit):
        _fail(f"{where}.source_commit:invalid")
    _lifecycle_identity(
        facts["environment_instance_id"], f"{where}.environment_instance_id"
    )
    _lifecycle_hash(
        facts["control_plane_identity_sha256"],
        f"{where}.control_plane_identity_sha256",
    )
    _lifecycle_hash(
        facts["source_snapshot_sha256"], f"{where}.source_snapshot_sha256"
    )
    return facts


def _validate_environment_evidence_refs(
    value: Any,
    *,
    where: str,
    bundle: ValidationBundle,
    evidence_root: Path | None,
    attestation_id: str,
    profile_id: str,
    binding_facts: Mapping[str, Any],
    service_id: str | None,
    used_paths: set[str],
) -> dict[str, list[dict[str, Any]]]:
    refs = _expect_list(value, where)
    if not refs:
        _fail(f"{where}:must_not_be_empty")
    paths: list[str] = []
    evidence_by_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, raw in enumerate(refs):
        ref_where = f"{where}[{index}]"
        ref = _expect_object(raw, ref_where)
        _exact_keys(
            ref,
            required={"artifact_kind", "path", "sha256"},
            where=ref_where,
        )
        artifact_kind = _enum(
            ref["artifact_kind"],
            ENVIRONMENT_EVIDENCE_ARTIFACT_KINDS,
            f"{ref_where}.artifact_kind",
        )
        resolved_root = evidence_root or bundle.root
        relative, resolved = _repo_path(
            resolved_root, ref["path"], f"{ref_where}.path"
        )
        parts = PurePosixPath(relative).parts
        expected_prefix = (
            "docs",
            "assurance",
            "guarantees",
            "evidence",
            attestation_id,
            "_environments",
            profile_id,
        )
        if len(parts) != 8 or parts[:7] != expected_prefix:
            _fail(f"{ref_where}.path:outside_attestation_environment_evidence_layout")
        filename = parts[-1]
        if not filename.startswith(f"{artifact_kind}-") or not filename.endswith(
            ".json"
        ):
            _fail(f"{ref_where}.path:artifact_kind_filename_mismatch")
        observed_hash = _expect_string(ref["sha256"], f"{ref_where}.sha256")
        if not HEX64_RE.fullmatch(observed_hash) or observed_hash != _sha256_file(
            resolved
        ):
            _fail(f"{ref_where}.sha256:mismatch")
        if relative in used_paths:
            _fail(f"{where}:duplicate_environment_evidence_path:{relative}")
        used_paths.add(relative)
        paths.append(relative)

        payload_where = f"{ref_where}.payload"
        payload = _environment_evidence_payload(resolved, payload_where)
        _exact_keys(
            payload,
            required={"schema_version", "profile_id", "artifact_kind", "facts"},
            optional={"service_id"},
            where=payload_where,
        )
        if payload["schema_version"] != "qt.assurance_environment_evidence.v1":
            _fail(f"{payload_where}.schema_version:unsupported")
        if payload["profile_id"] != profile_id:
            _fail(f"{payload_where}.profile_id:mismatch")
        if payload["artifact_kind"] != artifact_kind:
            _fail(f"{payload_where}.artifact_kind:mismatch")
        observed_service_id = payload.get("service_id")
        if observed_service_id != service_id:
            _fail(f"{payload_where}.service_id:mismatch")
        if artifact_kind in {
            "execution_admission_archive",
            "execution_draft",
            "execution_manifest",
            "cleanup_manifest",
        }:
            payload_facts = _validate_lifecycle_evidence_facts(
                artifact_kind, payload["facts"], f"{payload_where}.facts"
            )
        else:
            payload_facts = _validate_admission_facts(
                payload["facts"], f"{payload_where}.facts"
            )
        for fact_key in set(payload_facts) & set(binding_facts):
            if payload_facts[fact_key] != binding_facts[fact_key]:
                _fail(f"{payload_where}.facts.{fact_key}:admission_mismatch")
        evidence_by_kind[artifact_kind].append(
            {
                **payload_facts,
                "__artifact_path": relative,
                "__artifact_sha256": observed_hash,
            }
        )
    if paths != sorted(paths):
        _fail(f"{where}:must_be_sorted_by_path")
    if len(paths) != len(set(paths)):
        _fail(f"{where}:duplicate_path")
    return evidence_by_kind


def _require_evidence_kinds(
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    required: set[str],
    where: str,
) -> None:
    if missing := sorted(required - set(evidence)):
        _fail(f"{where}:missing_required_kinds:{','.join(missing)}")


def _require_evidence_fact(
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    artifact_kind: str,
    fact_key: str,
    expected: Any,
    where: str,
) -> None:
    rows = evidence.get(artifact_kind, ())
    if not rows or not any(row.get(fact_key) == expected for row in rows):
        _fail(
            f"{where}.{artifact_kind}:missing_matching_fact:{fact_key}"
        )


def _validate_archived_runner_build_record(
    value: Any,
    *,
    bundle: ValidationBundle,
    git_commit: str,
    build_profile_path: str,
    dockerfile_binding: Mapping[str, Any],
    where: str,
) -> dict[str, Any]:
    """Re-derive the complete v1 runner-build record from source-owned bytes."""

    record = _expect_object(value, where)
    _exact_keys(
        record,
        required={
            "schema_version",
            "build_id",
            "status",
            "source",
            "source_materials",
            "wheel_artifacts",
            "build_context",
            "docker_tool",
            "base_images",
            "invocation",
            "started_at",
            "finished_at",
            "exit_code",
            "log",
            "output_image",
            "external_order_submission_enabled",
        },
        where=where,
    )
    if record["schema_version"] != RUNNER_BUILD_RECORD_SCHEMA_VERSION:
        _fail(f"{where}.schema_version:unsupported")
    if record["status"] != "succeeded" or record["exit_code"] != 0:
        _fail(f"{where}:successful_zero_exit_build_required")
    if record["external_order_submission_enabled"] is not False:
        _fail(f"{where}.external_order_submission_enabled:must_be_false")

    source = _expect_object(record["source"], f"{where}.source")
    expected_tree = _bound_source_tree(bundle.git_object_root or bundle.root, git_commit)
    if source != {"commit": git_commit, "tree": expected_tree}:
        _fail(f"{where}.source:mismatch")

    profile_bytes = _bound_material_bytes(
        bundle.root, build_profile_path, git_commit=git_commit
    )
    profile = _bound_json_object(
        bundle.root,
        build_profile_path,
        git_commit=git_commit,
        where=f"{where}.build_profile",
    )
    if profile_bytes != _canonical_json_bytes(profile):
        _fail(f"{where}.build_profile:not_canonical_json")
    required_labels = [
        "com.quant-trad.assurance.source",
        "com.quant-trad.assurance.source-tree",
        "com.quant-trad.assurance.build-profile-sha256",
        "com.quant-trad.assurance.build-definition-sha256",
        "com.quant-trad.assurance.wheel-manifest-sha256",
        "com.quant-trad.assurance.wheel-artifact-sha256",
        "com.quant-trad.assurance.build-context-sha256",
    ]
    expected_profile_sources = {
        "dockerfile": "docker/assurance/frontend-node.Dockerfile",
        "requirements_lock": "requirements.lock",
        "wheel_manifest": "docker/assurance/python-wheel-manifest.lock.json",
        "materializer": "scripts/assurance/build_runner.py",
    }
    expected_bases = [
        {
            "argument": "NODE_IMAGE",
            "stage": "node_runtime",
            "reference": "docker.io/library/node@sha256:"
            "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0",
            "digest": "sha256:"
            "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0",
        },
        {
            "argument": "PYTHON_IMAGE",
            "stage": "runtime",
            "reference": "docker.io/library/python@sha256:"
            "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134",
            "digest": "sha256:"
            "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134",
        },
    ]
    expected_installation = {
        "argv": [
            "python",
            "-m",
            "pip",
            "install",
            "--no-cache-dir",
            "--no-index",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "--find-links=/opt/qt-assurance/wheelhouse",
            "-r",
            "/opt/qt-assurance/requirements.hashed.txt",
        ],
        "pip_check_argv": ["python", "-m", "pip", "check"],
        "intentionally_unavailable_executables": ["psql"],
    }
    expected_profile = {
        "schema_version": RUNNER_BUILD_PROFILE_SCHEMA_VERSION,
        "id": "phase3-python312-node20-offline",
        "platform": {
            "os": "linux",
            "architecture": "amd64",
            "python": "3.12",
            "glibc_max": "2.36",
        },
        "external_order_submission_enabled": False,
        "source_materials": expected_profile_sources,
        "wheelhouse": {
            "context_prefix": ".qt-assurance-wheelhouse",
            "hashed_requirements_name": "requirements.hashed.txt",
            "manifest_name": "python-wheel-manifest.lock.json",
        },
        "docker": {
            "network_mode": "none",
            "pull": False,
            "no_cache": True,
            "shell": False,
            "context_transport": "verified_tar_stdin",
        },
        "base_images": expected_bases,
        "installation": expected_installation,
        "required_image_labels": required_labels,
    }
    if profile != expected_profile:
        _fail(f"{where}.build_profile:reviewed_profile_mismatch")

    material_paths = {"build_profile": build_profile_path, **expected_profile_sources}
    expected_materials = {
        name: {
            "path": path,
            "sha256": _bound_material_sha256(
                bundle.root, path, git_commit=git_commit
            ),
        }
        for name, path in sorted(material_paths.items())
    }
    materials = _expect_object(record["source_materials"], f"{where}.source_materials")
    if materials != expected_materials:
        _fail(f"{where}.source_materials:mismatch")
    if dockerfile_binding != expected_materials["dockerfile"]:
        _fail(f"{where}.source_materials.dockerfile:admission_mismatch")

    manifest_path = expected_profile_sources["wheel_manifest"]
    manifest_bytes = _bound_material_bytes(
        bundle.root, manifest_path, git_commit=git_commit
    )
    manifest = _bound_json_object(
        bundle.root,
        manifest_path,
        git_commit=git_commit,
        where=f"{where}.wheel_manifest",
    )
    if manifest_bytes != _canonical_json_bytes(manifest):
        _fail(f"{where}.wheel_manifest:not_canonical_json")
    entries = _expect_list(manifest.get("entries"), f"{where}.wheel_manifest.entries")
    aggregate = _expect_object(
        manifest.get("aggregate"), f"{where}.wheel_manifest.aggregate"
    )
    wheel_artifacts = _expect_object(
        record["wheel_artifacts"], f"{where}.wheel_artifacts"
    )
    _exact_keys(
        wheel_artifacts,
        required={"entries", "aggregate", "tar"},
        where=f"{where}.wheel_artifacts",
    )
    if wheel_artifacts["entries"] != entries or wheel_artifacts["aggregate"] != aggregate:
        _fail(f"{where}.wheel_artifacts:manifest_mismatch")
    wheel_tar = _expect_object(
        wheel_artifacts["tar"], f"{where}.wheel_artifacts.tar"
    )
    _exact_keys(
        wheel_tar,
        required={"basename", "sha256", "size"},
        where=f"{where}.wheel_artifacts.tar",
    )
    if wheel_tar["basename"] != "wheelhouse.tar":
        _fail(f"{where}.wheel_artifacts.tar.basename:mismatch")
    _lifecycle_hash(wheel_tar["sha256"], f"{where}.wheel_artifacts.tar.sha256")
    _expect_int(wheel_tar["size"], f"{where}.wheel_artifacts.tar.size", minimum=1)

    context = _expect_object(record["build_context"], f"{where}.build_context")
    _exact_keys(
        context,
        required={"tar", "inventory"},
        where=f"{where}.build_context",
    )
    context_tar = _expect_object(context["tar"], f"{where}.build_context.tar")
    _exact_keys(
        context_tar,
        required={"basename", "sha256", "size"},
        where=f"{where}.build_context.tar",
    )
    if context_tar["basename"] != "build-context.tar":
        _fail(f"{where}.build_context.tar.basename:mismatch")
    _lifecycle_hash(context_tar["sha256"], f"{where}.build_context.tar.sha256")
    _expect_int(context_tar["size"], f"{where}.build_context.tar.size", minimum=1)

    hashed_requirements = "".join(
        f"{entry['name']}=={entry['version']} --hash=sha256:{entry['sha256']}\n"
        for entry in entries
    ).encode("utf-8")
    prefix = profile["wheelhouse"]["context_prefix"]
    dockerfile_bytes = _bound_material_bytes(
        bundle.root, expected_profile_sources["dockerfile"], git_commit=git_commit
    )
    expected_inventory = [
        {
            "path": expected_profile_sources["dockerfile"],
            "sha256": expected_materials["dockerfile"]["sha256"],
            "size": len(dockerfile_bytes),
        },
        {
            "path": f"{prefix}/requirements.hashed.txt",
            "sha256": hashlib.sha256(hashed_requirements).hexdigest(),
            "size": len(hashed_requirements),
        },
        {
            "path": f"{prefix}/python-wheel-manifest.lock.json",
            "sha256": expected_materials["wheel_manifest"]["sha256"],
            "size": len(manifest_bytes),
        },
        *(
            {
                "path": f"{prefix}/wheelhouse/{entry['filename']}",
                "sha256": entry["sha256"],
                "size": entry["size"],
            }
            for entry in entries
        ),
    ]
    expected_inventory.sort(key=lambda item: item["path"])
    if context["inventory"] != expected_inventory:
        _fail(f"{where}.build_context.inventory:mismatch")

    tool = _expect_object(record["docker_tool"], f"{where}.docker_tool")
    _exact_keys(
        tool,
        required={
            "executable_basename",
            "executable_sha256",
            "resolved_path_sha256",
            "version",
            "daemon_identity_sha256",
        },
        where=f"{where}.docker_tool",
    )
    basename = _expect_string(
        tool["executable_basename"], f"{where}.docker_tool.executable_basename"
    )
    if Path(basename).name != basename or "/" in basename or "\\" in basename:
        _fail(f"{where}.docker_tool.executable_basename:invalid")
    for key in ("executable_sha256", "resolved_path_sha256", "daemon_identity_sha256"):
        _lifecycle_hash(tool[key], f"{where}.docker_tool.{key}")
    _expect_string(tool["version"], f"{where}.docker_tool.version")

    base_images = _expect_list(record["base_images"], f"{where}.base_images")
    if len(base_images) != len(expected_bases):
        _fail(f"{where}.base_images:exact_pair_required")
    for index, (raw_base, expected_base) in enumerate(zip(base_images, expected_bases)):
        base = _expect_object(raw_base, f"{where}.base_images[{index}]")
        _exact_keys(
            base,
            required={"argument", "stage", "reference", "digest", "image_id", "platform"},
            where=f"{where}.base_images[{index}]",
        )
        if any(base.get(key) != expected_base[key] for key in expected_base):
            _fail(f"{where}.base_images[{index}]:profile_mismatch")
        if not IMAGE_DIGEST_RE.fullmatch(
            _expect_string(base["image_id"], f"{where}.base_images[{index}].image_id")
        ):
            _fail(f"{where}.base_images[{index}].image_id:invalid")
        if base["platform"] != "linux/amd64":
            _fail(f"{where}.base_images[{index}].platform:mismatch")

    expected_labels = {
        required_labels[0]: git_commit,
        required_labels[1]: expected_tree,
        required_labels[2]: expected_materials["build_profile"]["sha256"],
        required_labels[3]: expected_materials["dockerfile"]["sha256"],
        required_labels[4]: expected_materials["wheel_manifest"]["sha256"],
        required_labels[5]: wheel_tar["sha256"],
        required_labels[6]: context_tar["sha256"],
    }
    output = _expect_object(record["output_image"], f"{where}.output_image")
    _exact_keys(
        output,
        required={"image_id", "platform", "labels"},
        where=f"{where}.output_image",
    )
    if not IMAGE_DIGEST_RE.fullmatch(
        _expect_string(output["image_id"], f"{where}.output_image.image_id")
    ) or output["platform"] != "linux/amd64":
        _fail(f"{where}.output_image:identity_mismatch")
    labels = _expect_object(output["labels"], f"{where}.output_image.labels")
    if any(labels.get(key) != expected for key, expected in expected_labels.items()):
        _fail(f"{where}.output_image.labels:derived_binding_mismatch")
    if any(not isinstance(key, str) or not isinstance(item, str) for key, item in labels.items()):
        _fail(f"{where}.output_image.labels:string_map_required")

    invocation = _expect_object(record["invocation"], f"{where}.invocation")
    _exact_keys(
        invocation,
        required={
            "argv",
            "argv_sha256",
            "context_stdin_sha256",
            "output_tag",
            "timeout_seconds",
        },
        where=f"{where}.invocation",
    )
    output_tag = _expect_string(invocation["output_tag"], f"{where}.invocation.output_tag")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+:-]{0,255}", output_tag) or "@" in output_tag:
        _fail(f"{where}.invocation.output_tag:invalid")
    expected_argv = [
        basename,
        "build",
        "--platform=linux/amd64",
        "--network=none",
        "--pull=false",
        "--no-cache",
        "--progress=plain",
    ]
    for base in expected_bases:
        expected_argv.extend(["--build-arg", f"{base['argument']}={base['reference']}"])
    for label in required_labels:
        expected_argv.extend(["--label", f"{label}={expected_labels[label]}"])
    expected_argv.extend(
        [
            "--tag",
            output_tag,
            "--file",
            expected_profile_sources["dockerfile"],
            "-",
        ]
    )
    if invocation["argv"] != expected_argv:
        _fail(f"{where}.invocation.argv:mismatch")
    expected_argv_sha256 = hashlib.sha256(
        _canonical_json_bytes(expected_argv)
    ).hexdigest()
    if invocation["argv_sha256"] != expected_argv_sha256:
        _fail(f"{where}.invocation.argv_sha256:mismatch")
    if invocation["context_stdin_sha256"] != context_tar["sha256"]:
        _fail(f"{where}.invocation.context_stdin_sha256:mismatch")
    if invocation["timeout_seconds"] != 7200:
        _fail(f"{where}.invocation.timeout_seconds:mismatch")

    log = _expect_object(record["log"], f"{where}.log")
    _exact_keys(
        log,
        required={"basename", "sha256", "size"},
        where=f"{where}.log",
    )
    if log["basename"] != "runner-build.log":
        _fail(f"{where}.log.basename:mismatch")
    _lifecycle_hash(log["sha256"], f"{where}.log.sha256")
    _expect_int(log["size"], f"{where}.log.size", minimum=0)
    started = _parse_timestamp(record["started_at"], f"{where}.started_at")
    finished = _parse_timestamp(record["finished_at"], f"{where}.finished_at")
    if finished < started:
        _fail(f"{where}:timestamp_order_invalid")
    build_id = _expect_string(record["build_id"], f"{where}.build_id")
    if not re.fullmatch(
        r"QT-RUNNER-BUILD-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{16}", build_id
    ):
        _fail(f"{where}.build_id:invalid")
    return record


def _validate_lifecycle_binding(
    *,
    admission: Mapping[str, Any],
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    profile_id: str,
    attestation_id: str,
    git_commit: str,
    proof_results: Sequence[Mapping[str, Any]],
    expected_source_snapshot_sha256: str | None,
    profile: Mapping[str, Any],
    bundle: ValidationBundle,
    services: Mapping[str, Mapping[str, Any]],
    service_evidence: Mapping[
        str, Mapping[str, Sequence[Mapping[str, Any]]]
    ],
    where: str,
) -> datetime:
    """Require one cleanup-gated, cross-hashed execution chain for a profile."""

    required_kinds = {
        "execution_admission_archive",
        "execution_draft",
        "execution_manifest",
        "cleanup_manifest",
    }
    _require_evidence_kinds(evidence, required_kinds, f"{where}.evidence_refs")
    for artifact_kind in required_kinds:
        if len(evidence[artifact_kind]) != 1:
            _fail(f"{where}.{artifact_kind}:exactly_one_required")
    archived_admission = evidence["execution_admission_archive"][0]
    draft = evidence["execution_draft"][0]
    execution = evidence["execution_manifest"][0]
    cleanup = evidence["cleanup_manifest"][0]
    draft_hash = draft["__artifact_sha256"]
    execution_hash = execution["__artifact_sha256"]
    cleanup_hash = cleanup["__artifact_sha256"]
    result_hash = proof_results_sha256(proof_results)
    if (
        expected_source_snapshot_sha256 is not None
        and draft["source_snapshot_sha256"] != expected_source_snapshot_sha256
    ):
        _fail(f"{where}.execution_draft.source_snapshot_sha256:source_commit_mismatch")

    binding_values = {
        "attestation_id": attestation_id,
        "control_plane_identity_sha256": draft["control_plane_identity_sha256"],
        "environment_instance_id": draft["environment_instance_id"],
        "execution_admission_sha256": draft["execution_admission_sha256"],
        "execution_admission_archive_sha256": archived_admission[
            "__artifact_sha256"
        ],
        "execution_draft_sha256": draft_hash,
        "execution_manifest_sha256": execution_hash,
        "cleanup_manifest_sha256": cleanup_hash,
        "proof_results_sha256": result_hash,
        "runner_build_record_sha256": draft["runner_build_record_sha256"],
        "source_snapshot_sha256": draft["source_snapshot_sha256"],
    }
    facts = admission["facts"]
    for key, expected in binding_values.items():
        observed = facts.get(key)
        if observed != expected:
            _fail(f"{where}.facts.{key}:lifecycle_mismatch")
    if facts.get("source_commit") != git_commit:
        _fail(f"{where}.facts.source_commit:lifecycle_mismatch")
    if archived_admission["source_commit"] != git_commit:
        _fail(f"{where}.execution_admission_archive.source_commit:mismatch")
    if (
        archived_admission["execution_admission_sha256"]
        != draft["execution_admission_sha256"]
    ):
        _fail(
            f"{where}.execution_admission_archive.execution_admission_sha256:mismatch"
        )
    if (
        draft["execution_admission_archive_sha256"]
        != archived_admission["__artifact_sha256"]
    ):
        _fail(
            f"{where}.execution_draft.execution_admission_archive_sha256:mismatch"
        )
    archived_profile = archived_admission["profile"]
    archive_pairs = {
        "profile_id": profile_id,
        "admission_id": admission["admission_id"],
        "environment_class": admission["environment_class"],
        "isolation": admission["isolation"],
        "external_order_submission_enabled": False,
        "runtime_definition": admission["runtime_definition"],
    }
    for key, expected in archive_pairs.items():
        if archived_profile[key] != expected:
            _fail(f"{where}.execution_admission_archive.profile.{key}:mismatch")
    archived_runtime = archived_profile["runtime_definition"]
    if archived_runtime["path"] != profile["runtime_definition"] or archived_runtime[
        "sha256"
    ] != _bound_material_sha256(
        bundle.root, profile["runtime_definition"], git_commit=git_commit
    ):
        _fail(
            f"{where}.execution_admission_archive.profile.runtime_definition:"
            "source_binding_mismatch"
        )
    archived_runner = archived_profile["runner_image"]
    archived_build_binding = archived_profile["runner_build_record"]
    if archived_build_binding["sha256"] != draft["runner_build_record_sha256"]:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "sha256:draft_mismatch"
        )
    build_definition = archived_runner["build_definition"]
    if build_definition["sha256"] != _bound_material_sha256(
        bundle.root, build_definition["path"], git_commit=git_commit
    ):
        _fail(
            f"{where}.execution_admission_archive.profile.runner_image."
            "build_definition:source_binding_mismatch"
        )
    build_bytes = _bound_material_bytes(
        bundle.root, build_definition["path"], git_commit=git_commit
    )
    expected_base_digests = sorted(
        {
            f"sha256:{digest.decode('ascii')}"
            for digest in re.findall(rb"@sha256:([0-9a-f]{64})", build_bytes)
        }
    )
    if archived_runner["base_image_digests"] != expected_base_digests:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_image."
            "base_image_digests:source_binding_mismatch"
        )
    archived_build_record = _expect_object(
        archived_build_binding["record"],
        f"{where}.execution_admission_archive.profile.runner_build_record.record",
    )
    if hashlib.sha256(_canonical_json_bytes(archived_build_record)).hexdigest() != (
        archived_build_binding["sha256"]
    ):
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record:canonical_sha256_mismatch"
        )
    expected_build_profile_path = profile.get("runner_build_profile")
    if not isinstance(expected_build_profile_path, str):
        _fail(f"{where}.runner_build_profile:missing")
    archived_build_record = _validate_archived_runner_build_record(
        archived_build_record,
        bundle=bundle,
        git_commit=git_commit,
        build_profile_path=expected_build_profile_path,
        dockerfile_binding=build_definition,
        where=(
            f"{where}.execution_admission_archive.profile.runner_build_record.record"
        ),
    )
    if archived_build_record.get("status") != "succeeded":
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.status:not_success"
        )
    archived_build_source = _expect_object(
        archived_build_record.get("source"),
        f"{where}.execution_admission_archive.profile.runner_build_record.record.source",
    )
    if archived_build_source.get("commit") != git_commit:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.source.commit:mismatch"
        )
    if archived_build_source.get("tree") != _bound_source_tree(
        bundle.git_object_root or bundle.root, git_commit
    ):
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.source.tree:mismatch"
        )
    archived_materials = _expect_object(
        archived_build_record.get("source_materials"),
        f"{where}.execution_admission_archive.profile.runner_build_record."
        "record.source_materials",
    )
    archived_build_profile = _expect_object(
        archived_materials.get("build_profile"),
        f"{where}.execution_admission_archive.profile.runner_build_record."
        "record.source_materials.build_profile",
    )
    if (
        archived_build_profile.get("path") != expected_build_profile_path
        or archived_build_profile.get("sha256")
        != _bound_material_sha256(
            bundle.root, expected_build_profile_path, git_commit=git_commit
        )
    ):
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.source_materials.build_profile:mismatch"
        )
    if archived_materials.get("dockerfile") != build_definition:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.source_materials.dockerfile:mismatch"
        )
    archived_bases = _expect_list(
        archived_build_record.get("base_images"),
        f"{where}.execution_admission_archive.profile.runner_build_record."
        "record.base_images",
    )
    archived_base_digests = sorted(
        _expect_string(
            _expect_object(
                item,
                f"{where}.execution_admission_archive.profile.runner_build_record."
                f"record.base_images[{index}]",
            ).get("digest"),
            f"{where}.execution_admission_archive.profile.runner_build_record."
            f"record.base_images[{index}].digest",
        )
        for index, item in enumerate(archived_bases)
    )
    if archived_base_digests != expected_base_digests:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.base_images:mismatch"
        )
    if archived_build_record.get("external_order_submission_enabled") is not False:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.external_order_submission_enabled:must_be_false"
        )
    if set(archived_profile["service_images"]) != set(profile["required_services"]):
        _fail(
            f"{where}.execution_admission_archive.profile.service_images:"
            "profile_mismatch"
        )
    if archived_profile["docker_tool"]["daemon_identity_sha256"] != draft[
        "control_plane_identity_sha256"
    ]:
        _fail(
            f"{where}.execution_admission_archive.profile.docker_tool."
            "daemon_identity_sha256:mismatch"
        )
    archived_build_tool = _expect_object(
        archived_build_record.get("docker_tool"),
        f"{where}.execution_admission_archive.profile.runner_build_record."
        "record.docker_tool",
    )
    if archived_build_tool != archived_profile["docker_tool"]:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.docker_tool:admission_mismatch"
        )
    runner_image_id = archived_runner["image_id"]
    final_runner_image_id = (
        facts.get("image_digest")
        if profile["execution_class"] == "isolated_container"
        else facts.get("runner_image_digest")
    )
    if runner_image_id != final_runner_image_id:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_image.image_id:"
            "final_environment_mismatch"
        )
    runtime_rows = evidence.get("runtime_probe", ())
    if len(runtime_rows) != 1:
        _fail(f"{where}.runtime_probe:exactly_one_required")
    observed_configuration = _expect_object(
        runtime_rows[0].get("observed_configuration"),
        f"{where}.runtime_probe.observed_configuration",
    )
    observed_runner = _expect_object(
        observed_configuration.get("runner"),
        f"{where}.runtime_probe.observed_configuration.runner",
    )
    observed_runner_required = {
            "assurance_mode",
            "container_identity",
            "external_order_submission_enabled",
            "image_id",
            "network_identity",
            "network_mode",
            "proof_child_environment_keys",
            "root_filesystem_mode",
            "source_mount_mode",
            "writable_tmpfs",
        }
    if profile["execution_class"] == "isolated_database":
        observed_runner_required.add("pg_dsn_sha256")
    _exact_keys(
        observed_runner,
        required=observed_runner_required,
        where=f"{where}.runtime_probe.observed_configuration.runner",
    )
    if observed_runner.get("image_id") != runner_image_id:
        _fail(
            f"{where}.runtime_probe.observed_configuration.runner.image_id:mismatch"
        )
    if (
        observed_runner["assurance_mode"] != "1"
        or observed_runner["external_order_submission_enabled"] != "0"
    ):
        _fail(
            f"{where}.runtime_probe.observed_configuration.runner:"
            "assurance_flags_mismatch"
        )
    expected_runner = {
        "container_identity": facts.get(
            "container_identity", facts.get("runner_container_identity")
        ),
        "network_mode": facts.get("network_mode", facts.get("runner_network_mode")),
        "root_filesystem_mode": facts.get(
            "runner_root_filesystem_mode", "read_only"
        ),
        "source_mount_mode": facts.get("source_mount_mode"),
        "writable_tmpfs": "/tmp",
    }
    for key, expected in expected_runner.items():
        if observed_runner.get(key) != expected:
            _fail(
                f"{where}.runtime_probe.observed_configuration.runner.{key}:mismatch"
            )
    child_environment_keys = _string_list(
        observed_runner["proof_child_environment_keys"],
        f"{where}.runtime_probe.observed_configuration.runner."
        "proof_child_environment_keys",
        sorted_values=True,
    )
    expected_child_keys = {
        "HOME",
        "LANG",
        "NO_COLOR",
        "PATH",
        "PYTHONHASHSEED",
        "PYTHONDONTWRITEBYTECODE",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTHONPATH",
        "QT_ASSURANCE_MODE",
        "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED",
        "TMPDIR",
        "TZ",
    }
    if profile["execution_class"] == "isolated_database":
        expected_child_keys |= {"PG_DSN", "QT_DB_TEST_ISOLATED", "RUN_DB_TESTS"}
        if observed_runner["pg_dsn_sha256"] != services[
            next(iter(services))
        ]["facts"].get("pg_dsn_sha256"):
            _fail(
                f"{where}.runtime_probe.observed_configuration.runner."
                "pg_dsn_sha256:service_mismatch"
            )
    if set(child_environment_keys) != expected_child_keys:
        _fail(
            f"{where}.runtime_probe.observed_configuration.runner."
            "proof_child_environment_keys:mismatch"
        )
    image_rows = evidence.get("image_digest", ())
    if len(image_rows) != 1:
        _fail(f"{where}.image_digest:exactly_one_required")
    if image_rows[0].get("build_definition_sha256") != build_definition["sha256"]:
        _fail(f"{where}.image_digest.build_definition_sha256:mismatch")
    if (
        image_rows[0].get("runner_build_record_sha256")
        != archived_build_binding["sha256"]
    ):
        _fail(f"{where}.image_digest.runner_build_record_sha256:mismatch")
    archived_build_record = _expect_object(
        archived_build_binding["record"],
        f"{where}.execution_admission_archive.profile.runner_build_record.record",
    )
    archived_build_output = _expect_object(
        archived_build_record.get("output_image"),
        f"{where}.execution_admission_archive.profile.runner_build_record."
        "record.output_image",
    )
    if image_rows[0].get("runner_build_labels") != archived_build_output.get(
        "labels"
    ):
        _fail(f"{where}.image_digest.runner_build_labels:mismatch")
    if archived_build_output.get("image_id") != runner_image_id:
        _fail(
            f"{where}.execution_admission_archive.profile.runner_build_record."
            "record.output_image.image_id:mismatch"
        )
    if profile["execution_class"] == "isolated_database":
        definition_payload = _bound_json_object(
            bundle.root,
            profile["runtime_definition"],
            git_commit=git_commit,
            where=f"{where}.runtime_definition_payload",
        )
        service_contract = _expect_object(
            definition_payload.get("service"),
            f"{where}.runtime_definition_payload.service",
        )
        service_id = _expect_string(
            service_contract.get("id"),
            f"{where}.runtime_definition_payload.service.id",
        )
        archived_service = archived_profile["service_images"][service_id]
        expected_reference = _expect_string(
            service_contract.get("image"),
            f"{where}.runtime_definition_payload.service.image",
        )
        expected_digest = "sha256:" + expected_reference.rsplit("@sha256:", 1)[-1]
        if (
            archived_service["reference"] != expected_reference
            or archived_service["image_digest"] != expected_digest
        ):
            _fail(
                f"{where}.execution_admission_archive.profile.service_images."
                f"{service_id}:runtime_definition_mismatch"
            )
        if services[service_id]["facts"].get("image_digest") != expected_digest:
            _fail(f"{where}.services.{service_id}.facts.image_digest:archive_mismatch")
        observed_database = _expect_object(
            observed_configuration.get("database_service"),
            f"{where}.runtime_probe.observed_configuration.database_service",
        )
        _exact_keys(
            observed_database,
            required={
                "container_identity",
                "image_id",
                "network_identity",
                "network_internal",
                "publish_host",
                "published_port",
                "volume_identity",
            },
            where=f"{where}.runtime_probe.observed_configuration.database_service",
        )
        if observed_database.get("image_id") != archived_service["image_id"]:
            _fail(
                f"{where}.runtime_probe.observed_configuration.database_service."
                "image_id:mismatch"
            )
        service_facts = services[service_id]["facts"]
        if (
            observed_database.get("container_identity")
            != service_facts.get("container_identity")
            or observed_database.get("publish_host") != "127.0.0.1"
            or observed_database.get("published_port")
            != service_facts.get("published_port")
            or observed_database.get("network_internal") is not True
        ):
            _fail(
                f"{where}.runtime_probe.observed_configuration.database_service:"
                "final_environment_mismatch"
            )
    if draft["attestation_id"] != attestation_id:
        _fail(f"{where}.execution_draft.attestation_id:mismatch")
    if profile_id not in draft["requested_profile_ids"]:
        _fail(f"{where}.execution_draft.requested_profile_ids:profile_missing")
    if draft["admission_id"] != admission["admission_id"]:
        _fail(f"{where}.execution_draft.admission_id:mismatch")
    if draft["runtime_definition_sha256"] != admission["runtime_definition"]["sha256"]:
        _fail(f"{where}.execution_draft.runtime_definition_sha256:mismatch")

    identity_keys = {
        "attestation_id",
        "source_commit",
        "source_snapshot_sha256",
        "environment_instance_id",
        "control_plane_identity_sha256",
    }
    for record_name, record in (("execution_manifest", execution), ("cleanup_manifest", cleanup)):
        for key in identity_keys:
            if record[key] != draft[key]:
                _fail(f"{where}.{record_name}.{key}:draft_mismatch")
        if (
            record["execution_admission_archive_sha256"]
            != archived_admission["__artifact_sha256"]
        ):
            _fail(
                f"{where}.{record_name}.execution_admission_archive_sha256:mismatch"
            )
    if execution["execution_draft_sha256"] != draft_hash:
        _fail(f"{where}.execution_manifest.execution_draft_sha256:mismatch")
    if execution["proof_results_sha256"] != result_hash:
        _fail(f"{where}.execution_manifest.proof_results_sha256:mismatch")
    if execution["execution_state"] != "complete":
        _fail(f"{where}.execution_manifest.execution_state:not_complete")
    if cleanup["execution_draft_sha256"] != draft_hash:
        _fail(f"{where}.cleanup_manifest.execution_draft_sha256:mismatch")
    if cleanup["execution_manifest_sha256"] != execution_hash:
        _fail(f"{where}.cleanup_manifest.execution_manifest_sha256:mismatch")
    if cleanup["attempt_number"] != 1:
        _fail(f"{where}.cleanup_manifest:only_first_attempt_finalizable")
    if (
        cleanup["cleanup_state"] != "passed"
        or cleanup["cleanup_completed"] is not True
        or cleanup["exit_code"] != 0
    ):
        _fail(f"{where}.cleanup_manifest:not_successful")
    if cleanup["label_query_remaining"]:
        _fail(f"{where}.cleanup_manifest.label_query_remaining:not_empty")

    planned = {
        (item["kind"], item["logical_name"])
        for item in draft["planned_resources"]
    }
    observed = {
        (item["kind"], item["logical_name"]): item["runtime_identity"]
        for item in execution["resource_identities"]
    }
    cleaned = {
        (item["kind"], item["logical_name"]): item
        for item in cleanup["resources"]
    }
    if planned != set(observed):
        _fail(f"{where}.execution_manifest.resource_identities:planned_set_mismatch")
    if planned != set(cleaned):
        _fail(f"{where}.cleanup_manifest.resources:planned_set_mismatch")
    for key in sorted(planned):
        if cleaned[key]["runtime_identity"] != observed[key]:
            _fail(f"{where}.cleanup_manifest.resources:runtime_identity_mismatch")
        if cleaned[key]["absent"] is not True:
            _fail(f"{where}.cleanup_manifest.resources:not_absent")
    if observed.get(("container", "proof-runner")) != observed_runner[
        "container_identity"
    ]:
        _fail(
            f"{where}.runtime_probe.observed_configuration.runner."
            "container_identity:inventory_mismatch"
        )
    if observed.get(("source_snapshot", "exact-source-snapshot")) != (
        "git-archive:" + draft["source_snapshot_sha256"]
    ):
        _fail(f"{where}.source_snapshot:inventory_mismatch")
    if profile["execution_class"] == "isolated_container":
        if observed_runner["network_identity"] != "none":
            _fail(
                f"{where}.runtime_probe.observed_configuration.runner."
                "network_identity:mismatch"
            )
    else:
        inventory_network = observed.get(("network", "session-network"))
        if (
            observed_runner["network_identity"] != inventory_network
            or observed_database["network_identity"] != inventory_network
        ):
            _fail(f"{where}.runtime_probe.observed_configuration:network_inventory_mismatch")
        if observed_database["container_identity"] != observed.get(
            ("container", "database-service")
        ):
            _fail(
                f"{where}.runtime_probe.observed_configuration.database_service."
                "container_identity:inventory_mismatch"
            )
        if observed_database["volume_identity"] != observed.get(
            ("volume", "database-data")
        ):
            _fail(
                f"{where}.runtime_probe.observed_configuration.database_service."
                "volume_identity:inventory_mismatch"
            )
        if service_facts["database_identity"] != observed.get(
            ("database", "session-database")
        ):
            _fail(f"{where}.services.database_identity:inventory_mismatch")
        endpoint_identity = (
            f"{observed_database['publish_host']}:"
            f"{observed_database['published_port']}"
        )
        if endpoint_identity != observed.get(
            ("published_endpoint", "database-loopback-endpoint")
        ):
            _fail(f"{where}.services.published_endpoint:inventory_mismatch")

    attempted_ids = sorted(
        (
            item["proof_id"]
            for item in proof_results
            if item.get("environment_profile_id") == profile_id
            and item.get("status") in {"PASS", "FAIL", "PARTIAL"}
        ),
        key=_id_sort_key,
    )
    if execution["executed_proof_ids"] != attempted_ids:
        _fail(f"{where}.execution_manifest.executed_proof_ids:mismatch")

    draft_started = _parse_timestamp(draft["started_at"], f"{where}.execution_draft.started_at")
    execution_started = _parse_timestamp(
        execution["execution_started_at"],
        f"{where}.execution_manifest.execution_started_at",
    )
    execution_finished = _parse_timestamp(
        execution["execution_finished_at"],
        f"{where}.execution_manifest.execution_finished_at",
    )
    cleanup_started = _parse_timestamp(
        cleanup["cleanup_started_at"], f"{where}.cleanup_manifest.cleanup_started_at"
    )
    cleanup_finished = _parse_timestamp(
        cleanup["cleanup_finished_at"], f"{where}.cleanup_manifest.cleanup_finished_at"
    )
    if not (
        draft_started <= execution_started <= execution_finished <= cleanup_started <= cleanup_finished
    ):
        _fail(f"{where}:lifecycle_timestamps_out_of_order")
    return cleanup_finished


def _normalized_architecture(value: str) -> str:
    normalized = value.strip().lower()
    return {"x86_64": "amd64", "aarch64": "arm64"}.get(normalized, normalized)


def _bound_json_object(
    root: Path, relative: str, *, git_commit: str | None, where: str
) -> dict[str, Any]:
    try:
        value = json.loads(
            _bound_material_bytes(root, relative, git_commit=git_commit).decode(
                "utf-8"
            ),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except GuaranteeValidationError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        _fail(f"{where}:invalid_json:{exc}")
    return _expect_object(value, where)


def _validate_container_profile_admission(
    *,
    admission: Mapping[str, Any],
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    environment: Mapping[str, Any],
    profile: Mapping[str, Any],
    bundle: ValidationBundle,
    git_commit: str,
    lifecycle_required: bool,
    where: str,
) -> None:
    facts = admission["facts"]
    required_facts = {
        "base_image_digests",
        "cleanup_completed",
        "container_identity",
        "docker_version",
        "image_digest",
        "network_mode",
        "platform",
        "source_commit",
        "source_mount_mode",
        "writable_temp_outside_source",
    } | (LIFECYCLE_BINDING_FACTS if lifecycle_required else set())
    _exact_keys(facts, required=required_facts, where=f"{where}.facts")
    for key in ("container_identity", "docker_version"):
        _expect_string(facts[key], f"{where}.facts.{key}")
    if not IMAGE_DIGEST_RE.fullmatch(
        _expect_string(facts["image_digest"], f"{where}.facts.image_digest")
    ):
        _fail(f"{where}.facts.image_digest:invalid")
    if facts["network_mode"] != "none":
        _fail(f"{where}.facts.network_mode:must_be_none")
    if facts["source_mount_mode"] != "read_only":
        _fail(f"{where}.facts.source_mount_mode:must_be_read_only")
    if facts["platform"] != "linux/amd64":
        _fail(f"{where}.facts.platform:must_be_linux_amd64")
    if facts["source_commit"] != git_commit:
        _fail(f"{where}.facts.source_commit:mismatch")
    for key in ("cleanup_completed", "writable_temp_outside_source"):
        if facts[key] is not True:
            _fail(f"{where}.facts.{key}:must_be_true")
    base_digests = _string_list(
        facts["base_image_digests"],
        f"{where}.facts.base_image_digests",
        sorted_values=True,
    )
    if any(not IMAGE_DIGEST_RE.fullmatch(item) for item in base_digests):
        _fail(f"{where}.facts.base_image_digests:invalid_digest")
    runtime_bytes = _bound_material_bytes(
        bundle.root, profile["runtime_definition"], git_commit=git_commit
    )
    expected_base_digests = sorted(
        {
            f"sha256:{digest.decode('ascii')}"
            for digest in re.findall(rb"@sha256:([0-9a-f]{64})", runtime_bytes)
        }
    )
    if expected_base_digests and base_digests != expected_base_digests:
        _fail(f"{where}.facts.base_image_digests:runtime_definition_mismatch")
    if not str(environment["os"]).lower().startswith("linux"):
        _fail(f"{where}:os_must_be_linux")
    if _normalized_architecture(str(environment["architecture"])) != "amd64":
        _fail(f"{where}:architecture_must_be_amd64")
    required_evidence = {
        "bootstrap_log",
        "cleanup_log",
        "container_identity",
        "image_digest",
        "network_mode",
        "runtime_probe",
        "source_mount",
    }
    if lifecycle_required:
        required_evidence |= {
            "execution_admission_archive",
            "execution_draft",
            "execution_manifest",
            "cleanup_manifest",
        }
    if expected_base_digests:
        required_evidence.add("base_image_digests")
    _require_evidence_kinds(evidence, required_evidence, f"{where}.evidence_refs")
    _require_evidence_fact(
        evidence, "bootstrap_log", "bootstrap_completed", True, where
    )
    for artifact_kind, fact_key in (
        ("cleanup_log", "cleanup_completed"),
        ("container_identity", "container_identity"),
        ("image_digest", "image_digest"),
        ("network_mode", "network_mode"),
        ("source_mount", "source_mount_mode"),
    ):
        _require_evidence_fact(
            evidence, artifact_kind, fact_key, facts[fact_key], where
        )
    _require_evidence_fact(
        evidence,
        "bootstrap_log",
        "container_identity",
        facts["container_identity"],
        where,
    )
    _require_evidence_fact(
        evidence,
        "cleanup_log",
        "container_identity",
        facts["container_identity"],
        where,
    )
    if expected_base_digests:
        _require_evidence_fact(
            evidence,
            "base_image_digests",
            "base_image_digests",
            base_digests,
            where,
        )


def _validate_database_profile_admission(
    *,
    admission: Mapping[str, Any],
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    environment: Mapping[str, Any],
    services: Mapping[str, Mapping[str, Any]],
    service_evidence: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    profile: Mapping[str, Any],
    bundle: ValidationBundle,
    git_commit: str,
    lifecycle_required: bool,
    where: str,
) -> None:
    facts = admission["facts"]
    required_profile_facts = {
        "environment_identity",
        "source_commit",
        "source_mount_mode",
    }
    if lifecycle_required:
        required_profile_facts |= {
            "runner_container_identity",
            "runner_image_digest",
            "runner_network_mode",
            "runner_root_filesystem_mode",
            "writable_temp_outside_source",
        } | LIFECYCLE_BINDING_FACTS
    _exact_keys(
        facts,
        required=required_profile_facts,
        where=f"{where}.facts",
    )
    _expect_string(facts["environment_identity"], f"{where}.facts.environment_identity")
    if lifecycle_required:
        _expect_string(
            facts["runner_container_identity"],
            f"{where}.facts.runner_container_identity",
        )
        if not IMAGE_DIGEST_RE.fullmatch(
            _expect_string(
                facts["runner_image_digest"], f"{where}.facts.runner_image_digest"
            )
        ):
            _fail(f"{where}.facts.runner_image_digest:invalid")
        if facts["runner_network_mode"] != "isolated_internal_bridge":
            _fail(f"{where}.facts.runner_network_mode:must_be_isolated_internal_bridge")
        if facts["runner_root_filesystem_mode"] != "read_only":
            _fail(f"{where}.facts.runner_root_filesystem_mode:must_be_read_only")
        if facts["writable_temp_outside_source"] is not True:
            _fail(f"{where}.facts.writable_temp_outside_source:must_be_true")
    if facts["source_commit"] != git_commit:
        _fail(f"{where}.facts.source_commit:mismatch")
    if facts["source_mount_mode"] != "read_only":
        _fail(f"{where}.facts.source_mount_mode:must_be_read_only")
    required_profile_evidence = {"runtime_probe", "source_mount"}
    if lifecycle_required:
        required_profile_evidence |= {
            "container_identity",
            "image_digest",
            "network_mode",
            "execution_admission_archive",
            "execution_draft",
            "execution_manifest",
            "cleanup_manifest",
        }
    _require_evidence_kinds(
        evidence,
        required_profile_evidence,
        f"{where}.evidence_refs",
    )
    _require_evidence_fact(
        evidence, "source_mount", "source_mount_mode", "read_only", where
    )
    if lifecycle_required:
        _require_evidence_fact(
            evidence,
            "container_identity",
            "runner_container_identity",
            facts["runner_container_identity"],
            where,
        )
        _require_evidence_fact(
            evidence,
            "image_digest",
            "runner_image_digest",
            facts["runner_image_digest"],
            where,
        )
        _require_evidence_fact(
            evidence,
            "network_mode",
            "runner_network_mode",
            "isolated_internal_bridge",
            where,
        )

    definition = _bound_json_object(
        bundle.root,
        profile["runtime_definition"],
        git_commit=git_commit,
        where=f"{where}.runtime_definition_payload",
    )
    service_contract = _expect_object(
        definition.get("service"), f"{where}.runtime_definition_payload.service"
    )
    isolation_contract = _expect_object(
        definition.get("isolation"), f"{where}.runtime_definition_payload.isolation"
    )
    service_id = _expect_string(
        service_contract.get("id"), f"{where}.runtime_definition_payload.service.id"
    )
    if set(services) != {service_id}:
        _fail(f"{where}.services:runtime_definition_mismatch")
    service = services[service_id]
    service_where = f"{where}.services.{service_id}"
    service_facts = service["facts"]
    required_service_facts = {
        "cleanup_completed",
        "container_identity",
        "credentials",
        "database_identity",
        "database_identity_scope",
        "extension_versions",
        "image_digest",
        "live_database",
        "network_mode",
        "pg_dsn_sha256",
        "postgresql_version",
        "production_database",
        "publish_host",
        "publish_port_mode",
        "published_port",
        "session_isolation_key_sha256",
        "shared_development_database",
        "timescaledb_version",
    }
    _exact_keys(
        service_facts,
        required=required_service_facts,
        where=f"{service_where}.facts",
    )
    expected_image = "sha256:" + _expect_string(
        service_contract.get("image"),
        f"{where}.runtime_definition_payload.service.image",
    ).rsplit("@sha256:", 1)[-1]
    if service_facts["image_digest"] != expected_image:
        _fail(f"{service_where}.facts.image_digest:runtime_definition_mismatch")
    _version_satisfies(
        _expect_string(
            service_facts["postgresql_version"],
            f"{service_where}.facts.postgresql_version",
        ),
        _expect_string(
            service_contract.get("postgresql"),
            f"{where}.runtime_definition_payload.service.postgresql",
        ),
        f"{service_where}.facts.postgresql_version",
    )
    _version_satisfies(
        _expect_string(
            service_facts["timescaledb_version"],
            f"{service_where}.facts.timescaledb_version",
        ),
        _expect_string(
            service_contract.get("timescaledb"),
            f"{where}.runtime_definition_payload.service.timescaledb",
        ),
        f"{service_where}.facts.timescaledb_version",
    )
    required_extensions = set(
        _string_list(
            service_contract.get("required_extensions"),
            f"{where}.runtime_definition_payload.service.required_extensions",
            nonempty=True,
        )
    )
    extension_versions = _expect_object(
        service_facts["extension_versions"],
        f"{service_where}.facts.extension_versions",
    )
    if set(extension_versions) != required_extensions:
        _fail(f"{service_where}.facts.extension_versions:required_set_mismatch")
    for extension, version in extension_versions.items():
        _expect_string(version, f"{service_where}.facts.extension_versions.{extension}")
    if extension_versions.get("timescaledb") != service_facts["timescaledb_version"]:
        _fail(f"{service_where}.facts.extension_versions.timescaledb:mismatch")
    expected_scalars = {
        "network_mode": service_contract.get("network_mode"),
        "publish_host": service_contract.get("publish_host"),
        "publish_port_mode": service_contract.get("publish_port"),
        "database_identity_scope": isolation_contract.get("database_identity"),
        "credentials": isolation_contract.get("credentials"),
    }
    for key, expected in expected_scalars.items():
        if service_facts[key] != expected:
            _fail(f"{service_where}.facts.{key}:runtime_definition_mismatch")
    _expect_int(
        service_facts["published_port"],
        f"{service_where}.facts.published_port",
        minimum=1,
    )
    for key in ("container_identity", "database_identity"):
        _expect_string(service_facts[key], f"{service_where}.facts.{key}")
    for key in ("pg_dsn_sha256", "session_isolation_key_sha256"):
        value = _expect_string(service_facts[key], f"{service_where}.facts.{key}")
        if not HEX64_RE.fullmatch(value):
            _fail(f"{service_where}.facts.{key}:invalid_hash")
    for key in (
        "shared_development_database",
        "live_database",
        "production_database",
    ):
        if service_facts[key] is not False:
            _fail(f"{service_where}.facts.{key}:must_be_false")
    if service_facts["cleanup_completed"] is not True:
        _fail(f"{service_where}.facts.cleanup_completed:must_be_true")
    if not str(environment["os"]).lower().startswith("linux"):
        _fail(f"{where}:os_must_be_linux")
    expected_arch = _normalized_architecture(
        _expect_string(
            _expect_object(
                definition.get("platform"),
                f"{where}.runtime_definition_payload.platform",
            ).get("architecture"),
            f"{where}.runtime_definition_payload.platform.architecture",
        )
    )
    if _normalized_architecture(str(environment["architecture"])) != expected_arch:
        _fail(f"{where}:architecture_runtime_definition_mismatch")
    expected_evidence = set(
        _string_list(
            definition.get("required_environment_evidence"),
            f"{where}.runtime_definition_payload.required_environment_evidence",
            nonempty=True,
        )
    )
    bound_evidence = service_evidence[service_id]
    _require_evidence_kinds(
        bound_evidence, expected_evidence, f"{service_where}.evidence_refs"
    )
    for artifact_kind, fact_key in (
        ("container_identity", "container_identity"),
        ("database_identity", "database_identity"),
        ("extension_versions", "extension_versions"),
        ("image_digest", "image_digest"),
        ("server_version", "postgresql_version"),
    ):
        _require_evidence_fact(
            bound_evidence,
            artifact_kind,
            fact_key,
            service_facts[fact_key],
            service_where,
        )
    _require_evidence_fact(
        bound_evidence,
        "published_endpoint",
        "publish_host",
        service_facts["publish_host"],
        service_where,
    )
    for artifact_kind, completed_key in (
        ("bootstrap_log", "bootstrap_completed"),
        ("cleanup_log", "cleanup_completed"),
    ):
        _require_evidence_fact(
            bound_evidence, artifact_kind, completed_key, True, service_where
        )
        for identity_key in ("container_identity", "database_identity"):
            _require_evidence_fact(
                bound_evidence,
                artifact_kind,
                identity_key,
                service_facts[identity_key],
                service_where,
            )


def _validate_recovery_profile_admission(
    *,
    admission: Mapping[str, Any],
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    services: Mapping[str, Mapping[str, Any]],
    service_evidence: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    git_commit: str,
    where: str,
) -> None:
    facts = admission["facts"]
    _exact_keys(
        facts,
        required={"environment_identity", "non_production", "source_commit"},
        where=f"{where}.facts",
    )
    _expect_string(facts["environment_identity"], f"{where}.facts.environment_identity")
    if facts["non_production"] is not True:
        _fail(f"{where}.facts.non_production:must_be_true")
    if facts["source_commit"] != git_commit:
        _fail(f"{where}.facts.source_commit:mismatch")
    _require_evidence_kinds(
        evidence, {"recovery_manifest"}, f"{where}.evidence_refs"
    )
    for service_id, service in services.items():
        service_where = f"{where}.services.{service_id}"
        service_facts = service["facts"]
        _exact_keys(
            service_facts,
            required={"cleanup_completed", "non_production", "resource_identity"},
            where=f"{service_where}.facts",
        )
        _expect_string(
            service_facts["resource_identity"],
            f"{service_where}.facts.resource_identity",
        )
        if service_facts["non_production"] is not True:
            _fail(f"{service_where}.facts.non_production:must_be_true")
        if service_facts["cleanup_completed"] is not True:
            _fail(f"{service_where}.facts.cleanup_completed:must_be_true")
        bound_evidence = service_evidence[service_id]
        _require_evidence_kinds(
            bound_evidence, {"cleanup_log"}, f"{service_where}.evidence_refs"
        )
        _require_evidence_fact(
            bound_evidence,
            "cleanup_log",
            "resource_identity",
            service_facts["resource_identity"],
            service_where,
        )
        _require_evidence_fact(
            bound_evidence,
            "cleanup_log",
            "cleanup_completed",
            True,
            service_where,
        )


def _validate_profile_admission(
    environment: Mapping[str, Any],
    profile: Mapping[str, Any],
    *,
    bundle: ValidationBundle,
    evidence_root: Path | None,
    attestation_id: str,
    git_commit: str,
    where: str,
    proof_results: Sequence[Mapping[str, Any]] = (),
    expected_source_snapshot_sha256: str | None = None,
) -> datetime | None:
    profile_id = profile["id"]
    admission_where = f"{where}.profile_admission"
    admission = _expect_object(environment["profile_admission"], admission_where)
    _exact_keys(
        admission,
        required={
            "admission_id",
            "environment_class",
            "isolation",
            "external_order_submission_enabled",
            "runtime_definition",
            "facts",
            "evidence_refs",
        },
        where=admission_where,
    )
    admission_id = _expect_string(
        admission["admission_id"], f"{admission_where}.admission_id"
    )
    if not ADMISSION_ID_RE.fullmatch(admission_id):
        _fail(f"{admission_where}.admission_id:invalid")
    _enum(
        admission["environment_class"],
        ISOLATED_ENVIRONMENT_CLASSES,
        f"{admission_where}.environment_class",
    )
    _enum(
        admission["isolation"], ISOLATION_MODES, f"{admission_where}.isolation"
    )
    if _expect_bool(
        admission["external_order_submission_enabled"],
        f"{admission_where}.external_order_submission_enabled",
    ):
        _fail(f"{admission_where}.external_order_submission_enabled:must_be_false")
    runtime = _expect_object(
        admission["runtime_definition"], f"{admission_where}.runtime_definition"
    )
    _exact_keys(
        runtime,
        required={"path", "sha256"},
        where=f"{admission_where}.runtime_definition",
    )
    if runtime["path"] != profile["runtime_definition"]:
        _fail(f"{admission_where}.runtime_definition.path:profile_mismatch")
    expected_runtime_hash = _bound_material_sha256(
        bundle.root, profile["runtime_definition"], git_commit=git_commit
    )
    if runtime["sha256"] != expected_runtime_hash:
        _fail(f"{admission_where}.runtime_definition.sha256:mismatch")
    facts = _validate_admission_facts(admission["facts"], f"{admission_where}.facts")
    used_paths: set[str] = set()
    profile_evidence = _validate_environment_evidence_refs(
        admission["evidence_refs"],
        where=f"{admission_where}.evidence_refs",
        bundle=bundle,
        evidence_root=evidence_root,
        attestation_id=attestation_id,
        profile_id=profile_id,
        binding_facts=facts,
        service_id=None,
        used_paths=used_paths,
    )
    services = _expect_object(environment["services"], f"{where}.services")
    expected_services = set(profile["required_services"])
    if set(services) != expected_services:
        _fail(f"{where}.services:profile_mismatch")
    normalized_services: dict[str, Mapping[str, Any]] = {}
    service_evidence: dict[
        str, Mapping[str, Sequence[Mapping[str, Any]]]
    ] = {}
    for service_id in sorted(services):
        if not PROFILE_ID_RE.fullmatch(service_id):
            _fail(f"{where}.services.key:invalid:{service_id}")
        service_where = f"{where}.services.{service_id}"
        service = _expect_object(services[service_id], service_where)
        _exact_keys(
            service,
            required={
                "environment_class",
                "isolation",
                "external_order_submission_enabled",
                "facts",
                "evidence_refs",
            },
            where=service_where,
        )
        _enum(
            service["environment_class"],
            ISOLATED_ENVIRONMENT_CLASSES,
            f"{service_where}.environment_class",
        )
        _enum(
            service["isolation"], ISOLATION_MODES, f"{service_where}.isolation"
        )
        if _expect_bool(
            service["external_order_submission_enabled"],
            f"{service_where}.external_order_submission_enabled",
        ):
            _fail(f"{service_where}.external_order_submission_enabled:must_be_false")
        service_facts = _validate_admission_facts(
            service["facts"], f"{service_where}.facts"
        )
        normalized_services[service_id] = {**service, "facts": service_facts}
        service_evidence[service_id] = _validate_environment_evidence_refs(
            service["evidence_refs"],
            where=f"{service_where}.evidence_refs",
            bundle=bundle,
            evidence_root=evidence_root,
            attestation_id=attestation_id,
            profile_id=profile_id,
            binding_facts=service_facts,
            service_id=service_id,
            used_paths=used_paths,
        )
    normalized_admission = {**admission, "facts": facts}
    execution_class = profile["execution_class"]
    lifecycle_required = bool(set(facts) & LIFECYCLE_BINDING_FACTS)
    if execution_class == "isolated_container":
        _validate_container_profile_admission(
            admission=normalized_admission,
            evidence=profile_evidence,
            environment=environment,
            profile=profile,
            bundle=bundle,
            git_commit=git_commit,
            lifecycle_required=lifecycle_required,
            where=admission_where,
        )
        if not lifecycle_required:
            return None
        return _validate_lifecycle_binding(
            admission=normalized_admission,
            evidence=profile_evidence,
            profile_id=profile_id,
            attestation_id=attestation_id,
            git_commit=git_commit,
            proof_results=proof_results,
            expected_source_snapshot_sha256=expected_source_snapshot_sha256,
            profile=profile,
            bundle=bundle,
            services=normalized_services,
            service_evidence=service_evidence,
            where=admission_where,
        )
    elif execution_class == "isolated_database":
        _validate_database_profile_admission(
            admission=normalized_admission,
            evidence=profile_evidence,
            environment=environment,
            services=normalized_services,
            service_evidence=service_evidence,
            profile=profile,
            bundle=bundle,
            git_commit=git_commit,
            lifecycle_required=lifecycle_required,
            where=admission_where,
        )
        if not lifecycle_required:
            return None
        return _validate_lifecycle_binding(
            admission=normalized_admission,
            evidence=profile_evidence,
            profile_id=profile_id,
            attestation_id=attestation_id,
            git_commit=git_commit,
            proof_results=proof_results,
            expected_source_snapshot_sha256=expected_source_snapshot_sha256,
            profile=profile,
            bundle=bundle,
            services=normalized_services,
            service_evidence=service_evidence,
            where=admission_where,
        )
    elif execution_class == "isolated_recovery":
        _validate_recovery_profile_admission(
            admission=normalized_admission,
            evidence=profile_evidence,
            services=normalized_services,
            service_evidence=service_evidence,
            git_commit=git_commit,
            where=admission_where,
        )
        return None
    else:
        _fail(f"{admission_where}:unsupported_execution_class:{execution_class}")


def _publication_untracked_path_in_closed_namespace(
    relative: str, git_commit: str
) -> bool:
    """Limit the validator exception to publisher-owned path layouts."""

    if "\\" in relative:
        return False
    pure = PurePosixPath(relative)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        return False
    parts = pure.parts
    evidence_prefix = ("docs", "assurance", "guarantees", "evidence")
    attestation_prefix = (
        "docs",
        "assurance",
        "guarantees",
        "attestations",
        git_commit,
    )
    if len(parts) >= 6 and parts[:4] == evidence_prefix:
        match = ATTESTATION_ID_RE.fullmatch(parts[4])
        return bool(match and git_commit.startswith(match.group("commit")))
    if len(parts) == 6 and parts[:5] == attestation_prefix:
        filename = parts[5]
        if not filename.endswith(".json"):
            return False
        attestation_id = filename[:-5]
        match = ATTESTATION_ID_RE.fullmatch(attestation_id)
        return bool(match and git_commit.startswith(match.group("commit")))
    if len(parts) == 3 and parts[0] == ".qt-assurance-publication":
        return bool(
            HEX64_RE.fullmatch(parts[1])
            and re.fullmatch(r"[0-9a-f]{64}\.pending", parts[2])
        )
    return False


def _verify_local_git_source(
    root: Path,
    git_commit: str,
    clean: bool,
    *,
    publication_allowed_untracked_paths: frozenset[str] | None = None,
) -> None:
    """Verify source assertions when the attestation is checked in a Git worktree."""

    if not (root / ".git").exists():
        return
    if publication_allowed_untracked_paths is not None and any(
        not _publication_untracked_path_in_closed_namespace(relative, git_commit)
        for relative in publication_allowed_untracked_paths
    ):
        _fail("attestation.source.clean:publication_allowlist_path_invalid")
    try:
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            env=_git_env(),
        ).stdout.strip()
        subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", f"{git_commit}^{{commit}}"],
            check=True,
            capture_output=True,
            env=_git_env(),
        )
        subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", git_commit, head],
            check=True,
            capture_output=True,
            env=_git_env(),
        )
        if publication_allowed_untracked_paths is None:
            status = subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "status",
                    "--porcelain",
                    "--untracked-files=normal",
                    "--",
                    ".",
                    ":(exclude)docs/assurance/guarantees/attestations/**",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=_git_env(),
            ).stdout
            observed_clean = not status.strip()
        else:
            raw_status = subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=all",
                ],
                check=True,
                capture_output=True,
                env=_git_env(),
            ).stdout
            unexpected: list[bytes] = []
            for entry in raw_status.split(b"\0"):
                if not entry:
                    continue
                if len(entry) < 4 or entry[2:3] != b" ":
                    unexpected.append(entry)
                    continue
                status_code = entry[:2]
                try:
                    relative = entry[3:].decode("utf-8")
                except UnicodeDecodeError:
                    unexpected.append(entry)
                    continue
                if (
                    status_code != b"??"
                    or relative not in publication_allowed_untracked_paths
                ):
                    unexpected.append(entry)
            observed_clean = not unexpected
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"attestation.source:git_verification_failed:{exc}")
    if head == git_commit and clean != observed_clean:
        _fail(
            "attestation.source.clean:worktree_mismatch:"
            f"declared={str(clean).lower()}:observed={str(observed_clean).lower()}"
        )


def _validate_node_test_result(
    result: Mapping[str, Any],
    runner: Mapping[str, Any],
    status: str,
    where: str,
) -> None:
    """Validate the admitted native Node result without parsing console prose."""

    node_where = f"{where}.node_test_result"
    node_result = _expect_object(result.get("node_test_result"), node_where)
    _exact_keys(
        node_result,
        required={
            "schema_version",
            "transport_schema_version",
            "reporter_path",
            "collected_files",
            "selected_test_names",
            "excluded_nonmatch_test_names",
            "cancelled_count",
            "todo_count",
            "explicitly_skipped_count",
        },
        where=node_where,
    )
    if node_result["schema_version"] != NODE_TEST_RESULT_SCHEMA_VERSION:
        _fail(
            f"{node_where}.schema_version:expected:{NODE_TEST_RESULT_SCHEMA_VERSION}"
        )
    transport = runner["event_transport"]
    if node_result["transport_schema_version"] != transport["schema_version"]:
        _fail(f"{node_where}.transport_schema_version:runner_definition_mismatch")
    if node_result["reporter_path"] != transport["path"]:
        _fail(f"{node_where}.reporter_path:runner_definition_mismatch")

    collected_files = _string_list(
        node_result["collected_files"], f"{node_where}.collected_files"
    )
    expected_files = runner["files"]
    if not set(collected_files) <= set(expected_files):
        _fail(f"{node_where}.collected_files:outside_runner_targets")
    selected_names = _string_list(
        node_result["selected_test_names"], f"{node_where}.selected_test_names"
    )
    excluded_names = _string_list(
        node_result["excluded_nonmatch_test_names"],
        f"{node_where}.excluded_nonmatch_test_names",
    )
    if set(selected_names) & set(excluded_names):
        _fail(f"{node_where}:selected_and_excluded_names_overlap")

    for count_key in (
        "cancelled_count",
        "todo_count",
        "explicitly_skipped_count",
    ):
        _expect_int(node_result[count_key], f"{node_where}.{count_key}", minimum=0)
    required_result_counts = {
        "collected_count",
        "passed_count",
        "failed_count",
        "skipped_count",
        "exit_code",
    }
    if missing_counts := sorted(required_result_counts - set(result)):
        _fail(f"{where}:node_test_result_missing_fields:{','.join(missing_counts)}")
    if {"xfailed_count", "xpassed_count"} & set(result):
        _fail(f"{where}:node_test_result_forbids_pytest_expected_outcomes")

    collected_count = _expect_int(
        result["collected_count"], f"{where}.collected_count", minimum=0
    )
    passed_count = _expect_int(
        result["passed_count"], f"{where}.passed_count", minimum=0
    )
    failed_count = _expect_int(
        result["failed_count"], f"{where}.failed_count", minimum=0
    )
    skipped_count = _expect_int(
        result["skipped_count"], f"{where}.skipped_count", minimum=0
    )
    cancelled_count = node_result["cancelled_count"]
    todo_count = node_result["todo_count"]
    if node_result["explicitly_skipped_count"] != skipped_count:
        _fail(f"{node_where}.explicitly_skipped_count:skipped_count_mismatch")
    if len(selected_names) != collected_count:
        _fail(f"{node_where}.selected_test_names:collected_count_mismatch")
    if (
        passed_count
        + failed_count
        + skipped_count
        + cancelled_count
        + todo_count
        != collected_count
    ):
        _fail(f"{where}:node_test_counts_do_not_equal_collected_count")

    exit_code = _expect_int(result["exit_code"], f"{where}.exit_code", minimum=0)
    pass_eligible = (
        exit_code == 0
        and collected_files == expected_files
        and selected_names == runner["expected_test_names"]
        and len(excluded_names) == runner["expected_excluded_nonmatch_count"]
        and passed_count == collected_count == len(runner["expected_test_names"])
        and failed_count == 0
        and skipped_count == 0
        and cancelled_count == 0
        and todo_count == 0
    )
    if exit_code == 124 and result.get("reason_code") == "runner_timeout_unattributed":
        expected_status = "PARTIAL"
    else:
        expected_status = "FAIL" if exit_code else ("PASS" if pass_eligible else "PARTIAL")
    if status != expected_status:
        _fail(f"{where}.status:expected_node_derived:{expected_status}")


def validate_attestation_data(
    attestation: Mapping[str, Any],
    bundle: ValidationBundle,
    *,
    registry_path: Path | None = None,
    proof_catalog_path: Path | None = None,
    evidence_root: Path | None = None,
    expected_source_snapshot_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate a result-bearing attestation against durable definitions."""

    _exact_keys(
        attestation,
        required={
            "schema_version",
            "attestation_id",
            "source",
            "inputs",
            "environments",
            "started_at",
            "finished_at",
            "proof_results",
            "guarantee_results",
        },
        where="attestation",
    )
    if attestation["schema_version"] != ATTESTATION_SCHEMA_VERSION:
        _fail(f"attestation.schema_version:expected:{ATTESTATION_SCHEMA_VERSION}")
    attestation_id = _expect_string(attestation["attestation_id"], "attestation.attestation_id")
    attestation_id_match = ATTESTATION_ID_RE.fullmatch(attestation_id)
    if not attestation_id_match:
        _fail("attestation.attestation_id:invalid")
    source = _expect_object(attestation["source"], "attestation.source")
    _exact_keys(
        source,
        required={"git_commit", "clean", "assurance_material_sha256"},
        where="attestation.source",
    )
    git_commit = _expect_string(source["git_commit"], "attestation.source.git_commit")
    if not HEX40_RE.fullmatch(git_commit):
        _fail("attestation.source.git_commit:expected_lowercase_40_hex")
    if not git_commit.startswith(attestation_id_match.group("commit")):
        _fail("attestation.attestation_id:commit_prefix_mismatch")
    clean = _expect_bool(source["clean"], "attestation.source.clean")
    _verify_local_git_source(bundle.root, git_commit, clean)
    material_hash = _expect_string(
        source["assurance_material_sha256"], "attestation.source.assurance_material_sha256"
    )
    if not HEX64_RE.fullmatch(material_hash):
        _fail("attestation.source.assurance_material_sha256:expected_lowercase_64_hex")
    observed_material_hash = assurance_material_sha256(bundle, git_commit=git_commit)
    if material_hash != observed_material_hash:
        _fail(
            "attestation.source.assurance_material_sha256:mismatch:"
            f"expected={material_hash}:observed={observed_material_hash}"
        )
    inputs = _expect_object(attestation["inputs"], "attestation.inputs")
    _exact_keys(
        inputs,
        required={
            "registry_semantics_sha256",
            "proof_catalog_sha256",
            "guarantee_material_sha256",
            "required_proof_material_sha256",
            "glossary_inputs",
        },
        where="attestation.inputs",
    )
    proof_file = proof_catalog_path or bundle.root / PROOF_CATALOG_PATH.relative_to(ROOT)
    try:
        proof_relative = proof_file.relative_to(bundle.root).as_posix()
    except ValueError:
        _fail("attestation.inputs.proof_catalog_sha256:path_outside_repository")
    expected_hashes = {
        "registry_semantics_sha256": registry_semantics_sha256(bundle.registry),
        "proof_catalog_sha256": _bound_material_sha256(
            bundle.root, proof_relative, git_commit=git_commit
        ),
    }
    for key, expected in expected_hashes.items():
        observed = _expect_string(inputs[key], f"attestation.inputs.{key}")
        if not HEX64_RE.fullmatch(observed) or observed != expected:
            _fail(f"attestation.inputs.{key}:mismatch")
    observed_glossary_inputs = _expect_list(
        inputs["glossary_inputs"], "attestation.inputs.glossary_inputs"
    )
    normalized_glossary_inputs: list[dict[str, str]] = []
    for index, raw_glossary_input in enumerate(observed_glossary_inputs):
        glossary_where = f"attestation.inputs.glossary_inputs[{index}]"
        glossary_input = _expect_object(raw_glossary_input, glossary_where)
        _exact_keys(
            glossary_input,
            required={"source_kind", "path", "sha256"},
            where=glossary_where,
        )
        source_kind = _enum(
            glossary_input["source_kind"],
            {"proposal", "adopted_normative"},
            f"{glossary_where}.source_kind",
        )
        path = _expect_string(glossary_input["path"], f"{glossary_where}.path")
        sha256 = _expect_string(
            glossary_input["sha256"], f"{glossary_where}.sha256"
        )
        if not HEX64_RE.fullmatch(sha256):
            _fail(f"{glossary_where}.sha256:invalid")
        normalized_glossary_inputs.append(
            {"source_kind": source_kind, "path": path, "sha256": sha256}
        )
    if normalized_glossary_inputs != glossary_inputs(bundle, git_commit=git_commit):
        _fail("attestation.inputs.glossary_inputs:mismatch")
    guarantee_material = _expect_object(
        inputs["guarantee_material_sha256"],
        "attestation.inputs.guarantee_material_sha256",
    )
    expected_guarantee_material = guarantee_material_hashes(
        bundle, git_commit=git_commit
    )
    if list(guarantee_material) != sorted(guarantee_material, key=_id_sort_key):
        _fail("attestation.inputs.guarantee_material_sha256:must_be_sorted_by_id")
    if set(guarantee_material) != set(expected_guarantee_material):
        _fail("attestation.inputs.guarantee_material_sha256:guarantee_set_mismatch")
    for guarantee_id, expected in expected_guarantee_material.items():
        observed = _expect_string(
            guarantee_material[guarantee_id],
            f"attestation.inputs.guarantee_material_sha256.{guarantee_id}",
        )
        if not HEX64_RE.fullmatch(observed) or observed != expected:
            _fail(
                "attestation.inputs.guarantee_material_sha256:"
                f"material_mismatch:{guarantee_id}"
            )
    proof_material = _expect_object(
        inputs["required_proof_material_sha256"],
        "attestation.inputs.required_proof_material_sha256",
    )
    expected_proof_material = required_proof_material_hashes(
        bundle, git_commit=git_commit
    )
    if list(proof_material) != sorted(proof_material, key=_id_sort_key):
        _fail("attestation.inputs.required_proof_material_sha256:must_be_sorted_by_id")
    if set(proof_material) != set(expected_proof_material):
        _fail("attestation.inputs.required_proof_material_sha256:proof_set_mismatch")
    for proof_id, expected in expected_proof_material.items():
        observed = _expect_string(
            proof_material[proof_id],
            f"attestation.inputs.required_proof_material_sha256.{proof_id}",
        )
        if not HEX64_RE.fullmatch(observed) or observed != expected:
            _fail(
                "attestation.inputs.required_proof_material_sha256:"
                f"material_mismatch:{proof_id}"
            )

    proof_results = _expect_list(attestation["proof_results"], "attestation.proof_results")
    # Compute this before environment validation so every profile lifecycle can
    # bind the complete final result set, not a transitive or profile-local view.
    proof_results_sha256(proof_results)
    profiles = {profile["id"]: profile for profile in bundle.proof_catalog["environment_profiles"]}
    environments = _expect_list(attestation["environments"], "attestation.environments")
    if not environments:
        _fail("attestation.environments:must_not_be_empty")
    bound_environment_ids: list[str] = []
    cleanup_finished_times: list[datetime] = []
    for index, raw_environment in enumerate(environments):
        environment_where = f"attestation.environments[{index}]"
        environment = _expect_object(raw_environment, environment_where)
        _exact_keys(
            environment,
            required={
                "profile_id",
                "os",
                "architecture",
                "tool_versions",
                "lockfile_hashes",
                "profile_admission",
                "services",
            },
            where=environment_where,
        )
        profile_id = _expect_string(
            environment["profile_id"], f"{environment_where}.profile_id"
        )
        if profile_id not in profiles:
            _fail(f"{environment_where}.profile_id:unknown:{profile_id}")
        bound_environment_ids.append(profile_id)
        _expect_string(environment["os"], f"{environment_where}.os")
        _expect_string(environment["architecture"], f"{environment_where}.architecture")
        tool_versions = _expect_object(
            environment["tool_versions"], f"{environment_where}.tool_versions"
        )
        for key, value in tool_versions.items():
            _expect_string(key, f"{environment_where}.tool_versions.key")
            _expect_string(value, f"{environment_where}.tool_versions.{key}")
        required_tools = {"python"}
        if "node" in profiles[profile_id]:
            required_tools.add("node")
        if missing_tools := sorted(required_tools - set(tool_versions)):
            _fail(
                f"{environment_where}.tool_versions:missing_required:"
                + ",".join(missing_tools)
            )
        _version_satisfies(
            tool_versions["python"],
            profiles[profile_id]["python"],
            f"{environment_where}.tool_versions.python",
        )
        if "node" in profiles[profile_id]:
            _version_satisfies(
                tool_versions["node"],
                profiles[profile_id]["node"],
                f"{environment_where}.tool_versions.node",
            )
        lockfile_hashes = _expect_object(
            environment["lockfile_hashes"], f"{environment_where}.lockfile_hashes"
        )
        expected_lockfiles = set(profiles[profile_id]["lockfiles"])
        if set(lockfile_hashes) != expected_lockfiles:
            _fail(f"{environment_where}.lockfile_hashes:profile_mismatch")
        for relative, value in lockfile_hashes.items():
            expected = _bound_material_sha256(
                bundle.root, relative, git_commit=git_commit
            )
            if _expect_string(value, f"{environment_where}.lockfile_hashes.{relative}") != expected:
                _fail(f"{environment_where}.lockfile_hashes:{relative}:mismatch")
        cleanup_finished = _validate_profile_admission(
            environment,
            profiles[profile_id],
            bundle=bundle,
            evidence_root=evidence_root,
            attestation_id=attestation_id,
            git_commit=git_commit,
            where=environment_where,
            proof_results=proof_results,
            expected_source_snapshot_sha256=expected_source_snapshot_sha256,
        )
        if cleanup_finished is not None:
            cleanup_finished_times.append(cleanup_finished)
    if len(bound_environment_ids) != len(set(bound_environment_ids)):
        _fail("attestation.environments:duplicate_profile_ids")
    if bound_environment_ids != sorted(bound_environment_ids):
        _fail("attestation.environments:must_be_sorted_by_profile_id")
    expected_id_profile = bound_environment_ids[0] if len(bound_environment_ids) == 1 else "multi"
    if attestation_id_match.group("profile") != expected_id_profile:
        _fail("attestation.attestation_id:environment_profile_mismatch")
    bound_environment_id_set = set(bound_environment_ids)

    started = _parse_timestamp(attestation["started_at"], "attestation.started_at")
    finished = _parse_timestamp(attestation["finished_at"], "attestation.finished_at")
    if finished < started:
        _fail("attestation:finished_before_started")
    if started.strftime("%Y%m%dT%H%M%SZ") != attestation_id_match.group("timestamp"):
        _fail("attestation.attestation_id:started_at_mismatch")
    if cleanup_finished_times and max(cleanup_finished_times) != finished:
        _fail("attestation.finished_at:must_equal_last_cleanup_verification")

    proof_by_id = {proof["id"]: proof for proof in bundle.proof_catalog["proofs"]}
    expected_active_proof_ids = sorted(
        (proof["id"] for proof in bundle.proof_catalog["proofs"] if proof["lifecycle"] == "active"),
        key=_id_sort_key,
    )
    if not expected_active_proof_ids:
        _fail("attestation:no_active_proofs_to_attest")
    proof_statuses: dict[str, str] = {}
    for index, raw in enumerate(proof_results):
        where = f"attestation.proof_results[{index}]"
        result = _expect_object(raw, where)
        _exact_keys(
            result,
            required={"proof_id", "environment_profile_id", "status", "evidence_refs"},
            optional={
                "started_at",
                "finished_at",
                "exit_code",
                "collected_count",
                "passed_count",
                "failed_count",
                "skipped_count",
                "xfailed_count",
                "xpassed_count",
                "stdout_sha256",
                "stderr_sha256",
                "result_summary_sha256",
                "reason_code",
                "operator_identity",
                "reviewer_identity",
                "executed_argv",
                "node_test_result",
            },
            where=where,
        )
        proof_id = _expect_string(result["proof_id"], f"{where}.proof_id")
        if proof_id not in proof_by_id:
            _fail(f"{where}.proof_id:unknown:{proof_id}")
        if proof_id in proof_statuses:
            _fail(f"{where}.proof_id:duplicate:{proof_id}")
        status = _enum(result["status"], ATTESTATION_RESULTS, f"{where}.status")
        proof_statuses[proof_id] = status
        proof_definition = proof_by_id[proof_id]
        runner_kind = proof_definition["runner"]["kind"]
        if runner_kind != "node_test" and "node_test_result" in result:
            _fail(f"{where}:node_test_result_requires_node_test_runner")
        result_profile_id = _expect_string(
            result["environment_profile_id"], f"{where}.environment_profile_id"
        )
        if result_profile_id != proof_definition["environment_profile_id"]:
            _fail(f"{where}.environment_profile_id:proof_definition_mismatch")
        if (
            proof_definition["lifecycle"] != "active"
            and status not in {"NOT_RUN", "UNAVAILABLE"}
        ):
            _fail(f"{where}:attempted_result_requires_active_proof")
        if (
            result_profile_id not in bound_environment_id_set
            and status not in {"NOT_RUN", "UNAVAILABLE"}
        ):
            _fail(f"{where}:attempted_result_requires_matching_environment_profile")
        if status == "MANUAL" and runner_kind != "manual":
            _fail(f"{where}:MANUAL_requires_manual_runner")
        if status == "PASS" and runner_kind not in {"pytest", "node_test", "manual"}:
            _fail(f"{where}:automated_PASS_requires_admitted_runner_in_v1")
        if runner_kind == "manual" and status not in {
            "PASS",
            "FAIL",
            "MANUAL",
            "NOT_RUN",
            "UNAVAILABLE",
        }:
            _fail(f"{where}:manual_runner_requires_manual_or_unattempted_status")
        attempted_automated = (
            runner_kind != "manual" and status in {"PASS", "FAIL", "PARTIAL"}
        )
        if attempted_automated:
            if "executed_argv" not in result:
                _fail(f"{where}:{status}_requires_executed_argv")
            raw_argv = _expect_list(result["executed_argv"], f"{where}.executed_argv")
            if not raw_argv:
                _fail(f"{where}.executed_argv:must_not_be_empty")
            executed_argv = [
                _expect_string(arg, f"{where}.executed_argv[{arg_index}]")
                for arg_index, arg in enumerate(raw_argv)
            ]
            if any("\0" in arg or "\n" in arg or "\r" in arg for arg in executed_argv):
                _fail(f"{where}.executed_argv:invalid_argument")
            expected_argv = _canonical_runner_argv(proof_definition["runner"])
            if executed_argv != expected_argv:
                _fail(f"{where}.executed_argv:runner_definition_mismatch")
        elif "executed_argv" in result:
            _fail(f"{where}:{status}_forbids_executed_argv")
        evidence_refs = _expect_list(result["evidence_refs"], f"{where}.evidence_refs")
        evidence_paths: list[str] = []
        evidence_hashes_by_kind: dict[str, list[str]] = defaultdict(list)
        evidence_files_by_kind: dict[str, list[Path]] = defaultdict(list)
        for evidence_index, evidence_raw in enumerate(evidence_refs):
            evidence_where = f"{where}.evidence_refs[{evidence_index}]"
            evidence = _expect_object(evidence_raw, evidence_where)
            _exact_keys(
                evidence,
                required={"artifact_kind", "path", "sha256"},
                where=evidence_where,
            )
            artifact_kind = _enum(
                evidence["artifact_kind"],
                EVIDENCE_ARTIFACT_KINDS,
                f"{evidence_where}.artifact_kind",
            )
            resolved_evidence_root = evidence_root or bundle.root
            evidence_path, resolved = _repo_path(
                resolved_evidence_root, evidence["path"], f"{evidence_where}.path"
            )
            evidence_parts = PurePosixPath(evidence_path).parts
            expected_evidence_prefix = (
                "docs",
                "assurance",
                "guarantees",
                "evidence",
                attestation_id,
                proof_id,
            )
            if len(evidence_parts) != 7 or evidence_parts[:6] != expected_evidence_prefix:
                _fail(f"{evidence_where}.path:outside_attestation_proof_evidence_layout")
            filename = evidence_parts[-1]
            valid_filename = (
                filename == "result_summary.json"
                if artifact_kind == "result_summary"
                else filename.startswith(f"{artifact_kind}-")
            )
            if not valid_filename:
                _fail(f"{evidence_where}.path:artifact_kind_filename_mismatch")
            evidence_paths.append(evidence_path)
            observed_hash = _expect_string(evidence["sha256"], f"{evidence_where}.sha256")
            if not HEX64_RE.fullmatch(observed_hash) or observed_hash != _sha256_file(resolved):
                _fail(f"{evidence_where}.sha256:mismatch")
            evidence_hashes_by_kind[artifact_kind].append(observed_hash)
            evidence_files_by_kind[artifact_kind].append(resolved)
        if evidence_paths != sorted(evidence_paths):
            _fail(f"{where}.evidence_refs:must_be_sorted_by_path")
        if len(evidence_paths) != len(set(evidence_paths)):
            _fail(f"{where}.evidence_refs:duplicate_path")

        result_started = (
            _parse_timestamp(result["started_at"], f"{where}.started_at")
            if "started_at" in result
            else None
        )
        result_finished = (
            _parse_timestamp(result["finished_at"], f"{where}.finished_at")
            if "finished_at" in result
            else None
        )
        if (result_started is None) != (result_finished is None):
            _fail(f"{where}:result_timestamps_must_be_paired")
        if result_started is not None and result_finished is not None:
            if result_finished < result_started:
                _fail(f"{where}:finished_before_started")
            if result_started < started or result_finished > finished:
                _fail(f"{where}:result_timestamps_outside_attestation_window")
            if (
                runner_kind != "manual"
                and status in {"PASS", "FAIL", "PARTIAL"}
                and (result_finished - result_started).total_seconds()
                > proof_definition["timeout_seconds"]
            ):
                _fail(f"{where}:proof_timeout_exceeded")
        if "exit_code" in result:
            _expect_int(result["exit_code"], f"{where}.exit_code", minimum=0)
        if "collected_count" in result:
            collected_count = _expect_int(
                result["collected_count"], f"{where}.collected_count", minimum=0
            )
            if status == "PASS" and collected_count < _minimum_collected_count(
                proof_definition["runner"]
            ):
                _fail(f"{where}.collected_count:PASS_undercollected_runner_targets")
        count_keys = {
            "passed_count",
            "failed_count",
            "skipped_count",
            "xfailed_count",
            "xpassed_count",
        }
        for count_key in count_keys & set(result):
            _expect_int(result[count_key], f"{where}.{count_key}", minimum=0)
        if runner_kind == "pytest" and attempted_automated:
            required_count_keys = count_keys
            if missing_counts := sorted(required_count_keys - set(result)):
                _fail(f"{where}:pytest_result_missing_counts:{','.join(missing_counts)}")
            if "collected_count" not in result:
                _fail(f"{where}:pytest_result_requires_collected_count")
            accounted_count = sum(
                _expect_int(result.get(key, 0), f"{where}.{key}", minimum=0)
                for key in count_keys
            )
            collected_count = _expect_int(
                result["collected_count"],
                f"{where}.collected_count",
                minimum=(
                    0
                    if result.get("reason_code") == "runner_timeout_unattributed"
                    else 1
                ),
            )
            if status in {"PASS", "FAIL"} and accounted_count != collected_count:
                _fail(f"{where}:pytest_counts_do_not_equal_collected_count")
            if status == "PARTIAL" and accounted_count > collected_count:
                _fail(f"{where}:pytest_counts_exceed_collected_count")
            if status == "PASS":
                if result["passed_count"] < 1:
                    _fail(f"{where}:pytest_PASS_requires_passed_test")
                if any(
                    result[key] != 0
                    for key in (
                        "failed_count",
                        "skipped_count",
                        "xfailed_count",
                        "xpassed_count",
                    )
                ):
                    _fail(
                        f"{where}:pytest_PASS_forbids_failed_skipped_or_expected_outcomes"
                    )
            if status == "FAIL" and result["failed_count"] < 1:
                _fail(f"{where}:pytest_FAIL_requires_failed_test")
            if status == "PARTIAL" and (
                result["failed_count"] != 0 or result["xpassed_count"] != 0
            ):
                _fail(f"{where}:pytest_PARTIAL_forbids_failed_or_xpassed_tests")
        elif runner_kind == "node_test" and attempted_automated:
            _validate_node_test_result(result, proof_definition["runner"], status, where)
        elif runner_kind != "pytest" and attempted_automated and status == "PARTIAL":
            if "collected_count" not in result:
                _fail(f"{where}:automated_PARTIAL_requires_executed_step_count")
        elif not attempted_automated and count_keys & set(result):
            _fail(f"{where}:{status}_forbids_execution_counts")
        artifact_hash_fields = {
            "stdout_sha256": "stdout",
            "stderr_sha256": "stderr",
            "result_summary_sha256": "result_summary",
        }
        for hash_key, artifact_kind in artifact_hash_fields.items():
            if hash_key in result and not HEX64_RE.fullmatch(
                _expect_string(result[hash_key], f"{where}.{hash_key}")
            ):
                _fail(f"{where}.{hash_key}:expected_lowercase_64_hex")
            if hash_key in result and evidence_hashes_by_kind.get(artifact_kind) != [
                result[hash_key]
            ]:
                _fail(f"{where}.{hash_key}:runner_artifact_mismatch")
        if runner_kind == "node_test" and attempted_automated:
            for hash_key, artifact_kind in artifact_hash_fields.items():
                if hash_key not in result:
                    _fail(f"{where}:node_test_result_requires_{hash_key}")
                if len(evidence_hashes_by_kind.get(artifact_kind, [])) != 1:
                    _fail(
                        f"{where}:node_test_result_requires_unique_{artifact_kind}_artifact"
                    )
        if attempted_automated and (
            status in {"PASS", "FAIL"} or runner_kind == "node_test"
        ):
            summary_files = evidence_files_by_kind.get("result_summary", [])
            if len(summary_files) != 1 or len(
                evidence_hashes_by_kind.get("result_summary", [])
            ) != 1:
                _fail(f"{where}:{status}_requires_unique_result_summary_artifact")
            summary = load_json_strict(summary_files[0])
            summary_keys = {
                "proof_id",
                "environment_profile_id",
                "status",
                "started_at",
                "finished_at",
                "executed_argv",
                "exit_code",
                "collected_count",
                "passed_count",
                "failed_count",
                "skipped_count",
                "xfailed_count",
                "xpassed_count",
                "stdout_sha256",
                "stderr_sha256",
                "reason_code",
                "node_test_result",
            }
            expected_summary = {
                key: result[key] for key in summary_keys if key in result
            }
            if summary != expected_summary:
                _fail(f"{where}:result_summary_mismatch")
        if "reason_code" in result:
            _expect_string(result["reason_code"], f"{where}.reason_code")
        if status == "PARTIAL" and "reason_code" not in result:
            _fail(f"{where}:PARTIAL_requires_reason_code")
        if "operator_identity" in result:
            _expect_string(result["operator_identity"], f"{where}.operator_identity")
        if "reviewer_identity" in result:
            _expect_string(result["reviewer_identity"], f"{where}.reviewer_identity")

        if status in {"PASS", "FAIL"} and runner_kind != "manual":
            for key in ("started_at", "finished_at", "exit_code", "collected_count"):
                if key not in result:
                    _fail(f"{where}:{status}_requires_{key}")
            if not any(
                evidence_hashes_by_kind.get(kind)
                for kind in {"stdout", "stderr", "result_summary"}
            ):
                _fail(f"{where}:{status}_requires_hashed_runner_artifact")
            exit_code = _expect_int(result["exit_code"], f"{where}.exit_code", minimum=0)
            if status == "PASS" and exit_code != 0:
                _fail(f"{where}:PASS_requires_zero_exit_code")
            if status == "FAIL" and exit_code == 0:
                _fail(f"{where}:FAIL_requires_nonzero_exit_code")
        elif status == "PARTIAL" and runner_kind != "manual":
            if "exit_code" in result and result["exit_code"] != 0:
                if not (
                    result["exit_code"] == 124
                    and result.get("reason_code") == "runner_timeout_unattributed"
                ):
                    _fail(f"{where}:automated_PARTIAL_forbids_nonzero_exit_code")
        elif status in {"PASS", "FAIL"}:
            for key in ("started_at", "finished_at", "operator_identity", "reviewer_identity"):
                if key not in result:
                    _fail(f"{where}:manual_{status}_requires_{key}")
            if not evidence_refs:
                _fail(f"{where}:manual_{status}_requires_hashed_evidence")
            if set(evidence_hashes_by_kind) != {"manual_evidence"}:
                _fail(f"{where}:manual_{status}_requires_manual_evidence_artifacts")
            if result["operator_identity"] == result["reviewer_identity"]:
                _fail(f"{where}:manual_{status}_requires_independent_reviewer")
            forbidden_manual_execution = {
                "executed_argv",
                "exit_code",
                "collected_count",
                "passed_count",
                "failed_count",
                "skipped_count",
                "xfailed_count",
                "xpassed_count",
                "stdout_sha256",
                "stderr_sha256",
                "result_summary_sha256",
                "node_test_result",
            }
            if forbidden_manual_execution & set(result):
                _fail(f"{where}:manual_{status}_forbids_automated_execution_fields")
        else:
            _expect_string(result.get("reason_code"), f"{where}.reason_code")
        if status in {"NOT_RUN", "UNAVAILABLE"} and set(result) & {
            "started_at",
            "finished_at",
            "exit_code",
            "collected_count",
            "passed_count",
            "failed_count",
            "skipped_count",
            "xfailed_count",
            "xpassed_count",
            "stdout_sha256",
            "stderr_sha256",
            "result_summary_sha256",
            "executed_argv",
            "node_test_result",
        }:
            _fail(f"{where}:{status}_forbids_execution_evidence")
        if status in {"MANUAL", "PARTIAL"} and result_started is None:
            _fail(f"{where}:{status}_requires_timestamps")
        if status == "MANUAL" and not evidence_refs:
            _fail(f"{where}:MANUAL_requires_evidence_refs")
        if status == "MANUAL" and "operator_identity" not in result:
            _fail(f"{where}:MANUAL_requires_operator_identity")
        if status == "PARTIAL" and not evidence_refs:
            _fail(f"{where}:PARTIAL_requires_evidence")
        if status == "PASS" and not clean:
            _fail(f"{where}:PASS_forbidden_for_dirty_source")
    if list(proof_statuses) != sorted(proof_statuses, key=_id_sort_key):
        _fail("attestation.proof_results:must_be_sorted_by_id")
    if list(proof_statuses) != expected_active_proof_ids:
        _fail("attestation.proof_results:active_proof_set_mismatch")

    required_by_guarantee: dict[str, list[str]] = defaultdict(list)
    required_strengths_by_guarantee: dict[str, list[str]] = defaultdict(list)
    proposed_required_by_guarantee: dict[str, list[str]] = defaultdict(list)
    for proof in bundle.proof_catalog["proofs"]:
        for link in proof["coverage"]:
            if link["required_for_full_attestation"]:
                if proof["lifecycle"] == "active":
                    required_by_guarantee[link["guarantee_id"]].append(proof["id"])
                    required_strengths_by_guarantee[link["guarantee_id"]].append(
                        link["strength"]
                    )
                elif proof["lifecycle"] == "proposed":
                    proposed_required_by_guarantee[link["guarantee_id"]].append(
                        proof["id"]
                    )
    guarantee_results = _expect_list(attestation["guarantee_results"], "attestation.guarantee_results")
    guarantee_by_id = {
        row["id"]: row for row in bundle.registry["guarantees"]
    }
    guarantee_ids: list[str] = []
    for index, raw in enumerate(guarantee_results):
        where = f"attestation.guarantee_results[{index}]"
        result = _expect_object(raw, where)
        _exact_keys(result, required={"guarantee_id", "status", "proof_ids"}, where=where)
        guarantee_id = _expect_string(result["guarantee_id"], f"{where}.guarantee_id")
        if guarantee_id not in required_by_guarantee:
            _fail(f"{where}.guarantee_id:unknown_or_has_no_required_proofs:{guarantee_id}")
        guarantee_ids.append(guarantee_id)
        proof_ids = _string_list(result["proof_ids"], f"{where}.proof_ids", nonempty=True, sorted_values=False)
        _require_sorted_ids(proof_ids, f"{where}.proof_ids")
        expected_proofs = sorted(required_by_guarantee[guarantee_id], key=_id_sort_key)
        if proof_ids != expected_proofs:
            _fail(f"{where}.proof_ids:required_proof_set_mismatch")
        missing_results = [proof_id for proof_id in proof_ids if proof_id not in proof_statuses]
        if missing_results:
            _fail(f"{where}:missing_proof_results:{','.join(missing_results)}")
        expected_status = _aggregate_guarantee_status(
            [proof_statuses[proof_id] for proof_id in proof_ids],
            proof_maturity=guarantee_by_id[guarantee_id]["proof_maturity"],
            required_strengths=required_strengths_by_guarantee[guarantee_id],
            has_proposed_required_proof=bool(
                proposed_required_by_guarantee.get(guarantee_id)
            ),
        )
        observed_status = _enum(result["status"], ATTESTATION_RESULTS, f"{where}.status")
        if observed_status != expected_status:
            _fail(f"{where}.status:expected_derived:{expected_status}")
        if observed_status == "PASS" and not clean:
            _fail(f"{where}:PASS_forbidden_for_dirty_source")
    if len(guarantee_ids) != len(set(guarantee_ids)):
        _fail("attestation.guarantee_results:duplicate_ids")
    _require_sorted_ids(guarantee_ids, "attestation.guarantee_results")
    if guarantee_ids != sorted(required_by_guarantee, key=_id_sort_key):
        _fail("attestation.guarantee_results:required_guarantee_set_mismatch")
    return dict(attestation)


@contextmanager
def _historical_repository(root: Path, git_commit: str) -> Iterator[Path]:
    """Expose a read-only commit snapshot for historical attestation validation."""

    if not (root / ".git").exists():
        _fail("historical_attestation:git_metadata_required")
    try:
        archive = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "core.autocrlf=false",
                "-c",
                "core.eol=lf",
                "archive",
                "--format=tar",
                git_commit,
            ],
            check=True,
            capture_output=True,
            env=_git_env(),
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        _fail(f"activation_attestation:git_archive_failed:{exc}")
    with tempfile.TemporaryDirectory(prefix="qt-guarantee-attestation-") as directory:
        snapshot_root = Path(directory)
        try:
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
                for member in tar.getmembers():
                    pure = PurePosixPath(member.name)
                    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
                        _fail(
                            "activation_attestation:unsafe_git_archive_member:"
                            f"{member.name}"
                        )
                tar.extractall(snapshot_root, filter="data")
        except (OSError, tarfile.TarError) as exc:
            _fail(f"activation_attestation:git_archive_extract_failed:{exc}")
        yield snapshot_root


def validate_attestation_historically(
    attestation: Mapping[str, Any],
    current_bundle: ValidationBundle,
    *,
    evidence_root: Path | None = None,
    publication_allowed_untracked_paths: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Validate an immutable attestation against definitions at its source commit."""

    source = _expect_object(attestation.get("source"), "attestation.source")
    git_commit = _expect_string(source.get("git_commit"), "attestation.source.git_commit")
    if not HEX40_RE.fullmatch(git_commit):
        _fail("attestation.source.git_commit:expected_lowercase_40_hex")
    clean = _expect_bool(source.get("clean"), "attestation.source.clean")
    _verify_local_git_source(
        current_bundle.root,
        git_commit,
        clean,
        publication_allowed_untracked_paths=publication_allowed_untracked_paths,
    )
    with _historical_repository(current_bundle.root, git_commit) as historical_root:
        expected_source_snapshot = source_snapshot_sha256(
            current_bundle.root, git_commit
        )
        historical_bundle = current_bundle
        if historical_root != current_bundle.root:
            validate_schema_contracts(root=historical_root)
            historical_registry = validate_registry_data(
                load_json_strict(historical_root / REGISTRY_PATH.relative_to(ROOT)),
                root=historical_root,
                git_object_root=current_bundle.root,
            )
            historical_catalog = validate_proof_catalog_data(
                load_json_strict(
                    historical_root / PROOF_CATALOG_PATH.relative_to(ROOT)
                ),
                historical_registry,
                root=historical_root,
            )
            _validate_runner_build_sources(
                root=historical_root,
                proof_catalog=historical_catalog,
            )
            historical_bundle = ValidationBundle(
                registry=historical_registry,
                proof_catalog=historical_catalog,
                root=historical_root,
                git_object_root=current_bundle.root,
            )
        return validate_attestation_data(
            attestation,
            historical_bundle,
            registry_path=historical_root / REGISTRY_PATH.relative_to(ROOT),
            proof_catalog_path=historical_root / PROOF_CATALOG_PATH.relative_to(ROOT),
            evidence_root=evidence_root or current_bundle.root,
            expected_source_snapshot_sha256=expected_source_snapshot,
        )


def validate_attestation_file_historically(
    attestation_path: Path,
    current_bundle: ValidationBundle,
    *,
    evidence_root: Path | None = None,
    publication_allowed_untracked_paths: frozenset[str] | None = None,
) -> dict[str, Any]:
    return validate_attestation_historically(
        load_json_strict(attestation_path),
        current_bundle,
        evidence_root=evidence_root,
        publication_allowed_untracked_paths=publication_allowed_untracked_paths,
    )


def validate_activation_evidence(bundle: ValidationBundle) -> None:
    """Validate immutable, historical PASS evidence for every active guarantee."""

    for row_index, row in enumerate(bundle.registry["guarantees"]):
        if row["activation_status"] != "active":
            continue
        guarantee_id = row["id"]
        pass_found = False
        for ref_index, ref in enumerate(row["activation_attestation_refs"]):
            where = (
                f"registry.guarantees[{row_index}].activation_attestation_refs[{ref_index}]"
            )
            attestation_path = bundle.root.joinpath(*PurePosixPath(ref["path"]).parts)
            attestation = load_json_strict(attestation_path)
            if attestation.get("attestation_id") != ref["attestation_id"]:
                _fail(f"{where}:attestation_id_content_mismatch")
            inputs = _expect_object(attestation.get("inputs"), f"{where}.inputs")
            current_registry_hash = registry_semantics_sha256(bundle.registry)
            if inputs.get("registry_semantics_sha256") != current_registry_hash:
                _fail(f"{where}:current_registry_semantics_mismatch")
            current_proof_catalog = (
                bundle.root / PROOF_CATALOG_PATH.relative_to(ROOT)
            )
            current_proof_relative = current_proof_catalog.relative_to(
                bundle.root
            ).as_posix()
            if inputs.get("proof_catalog_sha256") != _bound_material_sha256(
                bundle.root, current_proof_relative
            ):
                _fail(f"{where}:current_proof_catalog_mismatch")
            if inputs.get("glossary_inputs") != glossary_inputs(bundle):
                _fail(f"{where}:current_glossary_mismatch")
            attested_guarantee_material = _expect_object(
                inputs.get("guarantee_material_sha256"),
                f"{where}.inputs.guarantee_material_sha256",
            )
            current_guarantee_material = guarantee_material_hashes(bundle)
            if attested_guarantee_material.get(
                guarantee_id
            ) != current_guarantee_material.get(guarantee_id):
                _fail(f"{where}:current_guarantee_material_mismatch:{guarantee_id}")
            attested_proof_material = _expect_object(
                inputs.get("required_proof_material_sha256"),
                f"{where}.inputs.required_proof_material_sha256",
            )
            current_proof_material = required_proof_material_hashes(bundle)
            required_proof_ids = {
                proof["id"]
                for proof in bundle.proof_catalog["proofs"]
                if proof["lifecycle"] == "active"
                and any(
                    link["guarantee_id"] == guarantee_id
                    and link["required_for_full_attestation"]
                    for link in proof["coverage"]
                )
            }
            for proof_id in sorted(required_proof_ids, key=_id_sort_key):
                if attested_proof_material.get(proof_id) != current_proof_material.get(
                    proof_id
                ):
                    _fail(
                        f"{where}:current_required_proof_material_mismatch:{proof_id}"
                    )
            source = _expect_object(attestation.get("source"), f"{where}.source")
            git_commit = _expect_string(source.get("git_commit"), f"{where}.source.git_commit")
            if not HEX40_RE.fullmatch(git_commit):
                _fail(f"{where}.source.git_commit:expected_lowercase_40_hex")
            commit_directory = PurePosixPath(ref["path"]).parts[-2]
            if commit_directory != git_commit:
                _fail(f"{where}.path:source_commit_directory_mismatch")
            validated = validate_attestation_historically(
                attestation, bundle, evidence_root=bundle.root
            )
            matching_results = [
                result
                for result in validated["guarantee_results"]
                if result["guarantee_id"] == guarantee_id
            ]
            if len(matching_results) != 1:
                _fail(f"{where}:attestation_missing_unique_guarantee_result:{guarantee_id}")
            pass_found |= matching_results[0]["status"] == "PASS"
        if not pass_found:
            _fail(f"{guarantee_id}:active_requires_validated_PASS_attestation")


def _check_generated(bundle: ValidationBundle, view_path: Path) -> None:
    expected = render_markdown(bundle.registry, bundle.proof_catalog).encode("utf-8")
    if not view_path.exists() or view_path.read_bytes() != expected:
        _fail(
            "generated_guarantee_view_stale:run "
            "python scripts/docs/guarantees.py render"
        )


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate", help="validate durable assurance inputs")
    validate_parser.add_argument("--attestation", type=Path, action="append", default=[])
    subparsers.add_parser("render", help="write deterministic GUARANTEES.md")
    subparsers.add_parser("check", help="validate inputs and check generated view")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        bundle = validate_repository(root=root)
        if args.command == "render":
            view = root / VIEW_PATH.relative_to(ROOT)
            view.parent.mkdir(parents=True, exist_ok=True)
            view.write_bytes(
                render_markdown(bundle.registry, bundle.proof_catalog).encode("utf-8")
            )
            print(f"wrote {view.relative_to(root).as_posix()}")
        elif args.command == "check":
            _check_generated(bundle, root / VIEW_PATH.relative_to(ROOT))
            print(
                "guarantee registry valid: "
                f"{len(bundle.registry['guarantees'])} claims, "
                f"{len(bundle.proof_catalog['proofs'])} proofs"
            )
        else:
            for attestation_path in args.attestation:
                validate_attestation_file_historically(
                    attestation_path,
                    bundle,
                    evidence_root=root,
                )
            print(
                "guarantee registry valid: "
                f"{len(bundle.registry['guarantees'])} claims, "
                f"{len(bundle.proof_catalog['proofs'])} proofs"
            )
    except GuaranteeValidationError as exc:
        print(f"guarantee_validation_failed:{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())

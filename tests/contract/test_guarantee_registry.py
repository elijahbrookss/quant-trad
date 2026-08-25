from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.docs import guarantees


def _write(root: Path, relative: str, text: str) -> Path:
    path = root.joinpath(*relative.split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _base_repository(tmp_path: Path) -> tuple[dict, dict]:
    _write(tmp_path, "AGENTS.md", "# Agent governance\n")
    _write(
        tmp_path,
        "docs/contracts/platform/00_system_contract.md",
        "# System Contract\n\n## Known-at causality\nDerived facts remain causal.\n",
    )
    _write(tmp_path, "src/guard.py", "def enforce_known_at():\n    return True\n")
    _write(tmp_path, "tests/test_known_at.py", "def test_known_at():\n    assert True\n")
    _write(tmp_path, "requirements.lock", "pytest==8.4.2\n")
    _write(tmp_path, "Makefile", "validate-docs:\n\t@true\n")
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/guarantee-candidates.md",
        "# Candidates\n\n| ID | Claim |\n| --- | --- |\n| `QT-GC-001` | Fixture |\n",
    )
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/implementation-surface-inventory.json",
        json.dumps({"baseline_commit": "d" * 40}) + "\n",
    )
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/proposed-glossary.md",
        "# Proposed glossary\n\n## QT-TERM-001 — Known-at\n\n- Proposal status: `proposed`\n",
    )
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/terminology-inventory.md",
        "# Phase 1 Terminology Inventory\n\n"
        "| ID | Preferred term |\n"
        "| --- | --- |\n"
        "| `QT-TERM-001` | Known-at |\n",
    )
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/phase-1-findings.md",
        "# Findings\n\n| ID | Finding |\n| --- | --- |\n| `DOC-VALIDATION-001` | Fixture |\n",
    )
    registry = {
        "schema_version": guarantees.REGISTRY_SCHEMA_VERSION,
        "audit_baseline_commit": "d" * 40,
        "scope": {
            "phase": "whole_system_classification",
            "gate": "gate_2_pending",
            "source_candidate_count": 1,
            "included_candidate_ids": ["QT-GC-001"],
            "whole_system_classification_complete": False,
        },
        "guarantees": [
            {
                "id": "QT-GUAR-KNOWN-AT",
                "candidate_refs": [{"id": "QT-GC-001", "relation": "equivalent"}],
                "title": "Known-at causality",
                "claim_summary": "Known-at selection preserves causal outputs.",
                "claim_scope": ["The named causal selection path."],
                "failure_semantics": ["Later-known data changes an earlier output."],
                "wording_constraints": ["The index is limited to the named path."],
                "owners": ["platform"],
                "claim_kind": "behavioral_invariant",
                "claim_lifecycle": "current",
                "registry_disposition": "candidate",
                "activation_status": "unactivated",
                "activation_decision_refs": [],
                "activation_attestation_refs": [],
                "conformance": "static_aligned",
                "enforcement_maturity": "adequate",
                "proof_maturity": "adequate",
                "proof_mode": "automated",
                "term_refs": ["QT-TERM-001"],
                "authority_refs": [
                    {
                        "path": "docs/contracts/platform/00_system_contract.md",
                        "locator": {"kind": "heading", "value": "Known-at causality"},
                        "authority_kind": "normative_platform_contract",
                        "source_lifecycle": "active",
                        "role": "primary",
                    }
                ],
                "enforcement_refs": [
                    {
                        "id": "QT-ENF-001",
                        "kind": "runtime_guard",
                        "path": "src/guard.py",
                        "locator": {"kind": "line_range", "start": 1, "end": 2},
                        "coverage": "complete",
                    }
                ],
                "finding_refs": [],
                "remediation_status": "not_required",
                "remediation_refs": [],
                "replaced_by_ids": [],
            }
        ],
    }
    catalog = {
        "schema_version": guarantees.PROOF_CATALOG_SCHEMA_VERSION,
        "environment_profiles": [
            {
                "id": "python-nondb",
                "python": ">=3.12",
                "lockfiles": ["requirements.lock"],
                "required_services": [],
            }
        ],
        "proofs": [
            {
                "id": "QT-PROOF-001",
                "title": "Known-at unit proof",
                "lifecycle": "active",
                "proof_kind": "automated_test",
                "environment_profile_id": "python-nondb",
                "runner": {
                    "kind": "pytest",
                    "selectors": ["tests/test_known_at.py::test_known_at"],
                },
                "timeout_seconds": 60,
                "coverage": [
                    {
                        "guarantee_id": "QT-GUAR-KNOWN-AT",
                        "strength": "complete",
                        "required_for_full_attestation": True,
                    }
                ],
            }
        ],
    }
    return registry, catalog


def _validate(tmp_path: Path, registry: dict, catalog: dict) -> guarantees.ValidationBundle:
    validated_registry = guarantees.validate_registry_data(registry, root=tmp_path)
    validated_catalog = guarantees.validate_proof_catalog_data(
        catalog, validated_registry, root=tmp_path
    )
    return guarantees.ValidationBundle(validated_registry, validated_catalog, tmp_path)


def _attestation_inputs(
    bundle: guarantees.ValidationBundle,
    catalog_path: Path,
    *,
    git_commit: str | None = None,
) -> dict:
    proof_relative = catalog_path.relative_to(bundle.root).as_posix()
    return {
        "registry_semantics_sha256": guarantees.registry_semantics_sha256(
            bundle.registry
        ),
        "proof_catalog_sha256": guarantees._bound_material_sha256(
            bundle.root, proof_relative, git_commit=git_commit
        ),
        "guarantee_material_sha256": guarantees.guarantee_material_hashes(
            bundle, git_commit=git_commit
        ),
        "required_proof_material_sha256": guarantees.required_proof_material_hashes(
            bundle, git_commit=git_commit
        ),
        "glossary_inputs": guarantees.glossary_inputs(
            bundle, git_commit=git_commit
        ),
    }


def _runner_artifact(
    root: Path,
    name: str,
    text: str,
    *,
    artifact_kind: str = "stdout",
    attestation_id: str = "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb",
    proof_id: str = "QT-PROOF-001",
) -> tuple[dict, str]:
    filename = (
        "result_summary.json"
        if artifact_kind == "result_summary"
        else f"{artifact_kind}-{name}"
    )
    path = _write(
        root,
        "docs/assurance/guarantees/evidence/"
        f"{attestation_id}/{proof_id}/{filename}",
        text,
    )
    digest = guarantees._sha256_file(path)
    return (
        {
            "artifact_kind": artifact_kind,
            "path": path.relative_to(root).as_posix(),
            "sha256": digest,
        },
        digest,
    )


def _attach_result_summary(
    root: Path,
    attestation_id: str,
    result: dict,
) -> None:
    result["evidence_refs"] = [
        ref
        for ref in result["evidence_refs"]
        if ref["artifact_kind"] != "result_summary"
    ]
    result.pop("result_summary_sha256", None)
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
    }
    summary = {key: result[key] for key in summary_keys if key in result}
    ref, digest = _runner_artifact(
        root,
        "result-summary",
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        artifact_kind="result_summary",
        attestation_id=attestation_id,
        proof_id=result["proof_id"],
    )
    result["result_summary_sha256"] = digest
    result["evidence_refs"].append(ref)
    result["evidence_refs"].sort(key=lambda item: item["path"])


def test_valid_registry_and_catalog_render_deterministically(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    bundle = _validate(tmp_path, registry, catalog)

    first = guarantees.render_markdown(bundle.registry, bundle.proof_catalog)
    second = guarantees.render_markdown(bundle.registry, bundle.proof_catalog)

    assert first == second
    assert "QT-GUAR-KNOWN-AT" in first
    assert "Active guarantees: **0**" in first
    assert '<a id="guarantee-qt-guar-known-at"></a>' in first
    assert '<a id="proof-qt-proof-001"></a>' in first
    assert "tests/test_known_at.py::test_known_at" in first
    assert "(#proof-qt-proof-001)" in first
    assert "(#guarantee-qt-guar-known-at)" in first
    assert not any(token in first for token in ("NOT_RUN", "UNAVAILABLE", "PARTIAL"))


def test_generated_view_check_is_byte_exact_lf(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    bundle = _validate(tmp_path, registry, catalog)
    expected = guarantees.render_markdown(bundle.registry, bundle.proof_catalog).encode(
        "utf-8"
    )
    view = _write(tmp_path, "GUARANTEES.md", expected.decode("utf-8"))
    guarantees._check_generated(bundle, view)

    view.write_bytes(expected.replace(b"\n", b"\r\n"))
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="generated_guarantee_view_stale"
    ):
        guarantees._check_generated(bundle, view)


def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers(tmp_path: Path) -> None:
    duplicate = _write(tmp_path, "duplicate.json", '{"a": 1, "a": 2}\n')
    nonfinite = _write(tmp_path, "nonfinite.json", '{"a": NaN}\n')

    with pytest.raises(guarantees.GuaranteeValidationError, match="duplicate_key"):
        guarantees.load_json_strict(duplicate)
    with pytest.raises(guarantees.GuaranteeValidationError, match="non_finite"):
        guarantees.load_json_strict(nonfinite)


def test_durable_inputs_reject_execution_result_state(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["status"] = "PASS"

    with pytest.raises(guarantees.GuaranteeValidationError, match="result_key_forbidden"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_gate_two_pending_rejects_active_guarantee(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["registry_disposition"] = "enforced"
    registry["guarantees"][0]["activation_status"] = "active"

    with pytest.raises(guarantees.GuaranteeValidationError, match="gate_2_pending"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_unmodeled_retired_activation_status_is_rejected(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["activation_status"] = "retired"

    with pytest.raises(guarantees.GuaranteeValidationError, match="invalid_value:retired"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_calibration_batch_and_inventory_count_are_exact(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["scope"]["source_candidate_count"] = 2
    with pytest.raises(guarantees.GuaranteeValidationError, match="inventory_mismatch"):
        guarantees.validate_registry_data(registry, root=tmp_path)

    candidate_ids = [f"QT-GC-{index:03d}" for index in range(1, 11)]
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/guarantee-candidates.md",
        "| ID | Claim |\n| --- | --- |\n"
        + "".join(f"| `{candidate_id}` | Fixture |\n" for candidate_id in candidate_ids),
    )
    registry["scope"].update(
        phase="phase_2a_calibration",
        source_candidate_count=10,
        included_candidate_ids=candidate_ids,
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="10_to_15_guarantees"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_whole_system_complete_requires_all_frozen_candidates(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    all_candidate_ids = [f"QT-GC-{index:03d}" for index in range(1, 76)]
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/guarantee-candidates.md",
        "| ID | Claim |\n| --- | --- |\n"
        + "".join(f"| `{candidate_id}` | Fixture |\n" for candidate_id in all_candidate_ids),
    )
    registry["scope"].update(
        source_candidate_count=75,
        included_candidate_ids=all_candidate_ids[:-1],
        whole_system_classification_complete=True,
    )

    with pytest.raises(
        guarantees.GuaranteeValidationError, match="complete_candidate_set_mismatch"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)

    registry["scope"]["phase"] = "phase_2a_calibration"
    registry["scope"]["included_candidate_ids"] = all_candidate_ids[:12]
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="calibration_cannot_claim_whole_system"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_disposition_thresholds_match_declared_assurance_meaning(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    row = registry["guarantees"][0]
    row.update(
        registry_disposition="partially_enforced",
        conformance="partial",
        enforcement_maturity="partial",
        proof_maturity="partial",
        remediation_status="pending",
    )
    catalog["proofs"][0]["coverage"][0]["strength"] = "partial"
    _validate(tmp_path, registry, catalog)

    no_required_proof = copy.deepcopy(catalog)
    no_required_proof["proofs"][0]["coverage"][0][
        "required_for_full_attestation"
    ] = False
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="proof_maturity_requires_required_link",
    ):
        _validate(tmp_path, registry, no_required_proof)

    fully_adequate_partial = copy.deepcopy(registry)
    fully_adequate_partial["guarantees"][0].update(
        conformance="static_aligned",
        enforcement_maturity="adequate",
        proof_maturity="adequate",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="partially_enforced_cannot_meet_all_enforced_thresholds",
    ):
        guarantees.validate_registry_data(fully_adequate_partial, root=tmp_path)

    falsely_enforced = copy.deepcopy(registry)
    falsely_enforced["guarantees"][0].update(
        registry_disposition="enforced",
        remediation_status="not_required",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="enforced_requires_static_alignment_and_complete_adequate_enforcement_proof",
    ):
        guarantees.validate_registry_data(falsely_enforced, root=tmp_path)


def test_remediation_records_are_structurally_bound_and_completion_requires_them(
    tmp_path: Path,
) -> None:
    registry, _ = _base_repository(tmp_path)
    row = registry["guarantees"][0]
    row.update(
        registry_disposition="partially_enforced",
        conformance="partial",
        enforcement_maturity="partial",
        proof_maturity="partial",
        remediation_status="recorded",
    )
    remediation_path = _write(
        tmp_path,
        "docs/assurance/guarantees/remediations/QT-REM-001.md",
        "---\n"
        "remediation_id: QT-REM-001\n"
        "guarantee_ids: QT-GUAR-KNOWN-AT\n"
        "lifecycle: proposed\n"
        "owner: execution-runtime\n"
        "required_reviewers: execution-runtime-owner,platform-contract-reviewer\n"
        "required_review: true\n"
        "review_status: pending\n"
        "---\n\n"
        "# Remediation QT-REM-001\n\n"
        "## Gap\n\nThe named enforcement remains incomplete.\n\n"
        "## Action\n\nAdd the scoped enforcement after review.\n\n"
        "## Acceptance criteria\n\nThe frozen claim examples are rejected correctly.\n\n"
        "## Proof plan\n\nRun the mapped isolated proof and retain its attestation.\n",
    )
    row["remediation_refs"] = [
        {
            "id": "QT-REM-001",
            "path": remediation_path.relative_to(tmp_path).as_posix(),
            "locator": {"kind": "heading", "value": "Remediation QT-REM-001"},
            "lifecycle": "proposed",
        }
    ]
    guarantees.validate_registry_data(registry, root=tmp_path)

    unrelated = copy.deepcopy(registry)
    unrelated["guarantees"][0]["remediation_refs"][0]["path"] = (
        "docs/contracts/platform/00_system_contract.md"
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="expected_remediation_record_path",
    ):
        guarantees.validate_registry_data(unrelated, root=tmp_path)

    original_remediation = remediation_path.read_text(encoding="utf-8")
    remediation_path.write_text(
        original_remediation.replace(
            "lifecycle: proposed\n", "lifecycle: proposed\nunknown_key: value\n"
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="frontmatter_unknown_keys:unknown_key",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)
    remediation_path.write_text(original_remediation, encoding="utf-8")

    missing_reviewers = original_remediation.replace(
        "required_reviewers: execution-runtime-owner,platform-contract-reviewer\n", ""
    )
    remediation_path.write_text(missing_reviewers, encoding="utf-8")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="frontmatter_missing_keys:required_reviewers",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)
    remediation_path.write_text(original_remediation, encoding="utf-8")

    unsorted_reviewers = original_remediation.replace(
        "execution-runtime-owner,platform-contract-reviewer",
        "platform-contract-reviewer,execution-runtime-owner",
    )
    remediation_path.write_text(unsorted_reviewers, encoding="utf-8")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="remediation_record_invalid_required_reviewers",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)
    remediation_path.write_text(original_remediation, encoding="utf-8")

    empty_remediation = original_remediation.replace(
        "The named enforcement remains incomplete.", ""
    )
    remediation_path.write_text(empty_remediation, encoding="utf-8")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="remediation_record_empty_section:Gap",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)
    remediation_path.write_text(original_remediation, encoding="utf-8")

    all_candidate_ids = [f"QT-GC-{index:03d}" for index in range(1, 76)]
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/guarantee-candidates.md",
        "| ID | Claim |\n| --- | --- |\n"
        + "".join(f"| `{candidate_id}` | Fixture |\n" for candidate_id in all_candidate_ids),
    )
    template = copy.deepcopy(registry["guarantees"][0])
    template["remediation_status"] = "pending"
    template["remediation_refs"] = []
    registry["guarantees"] = []
    for index, candidate_id in enumerate(all_candidate_ids, 1):
        generated = copy.deepcopy(template)
        generated["id"] = f"QT-GUAR-FIXTURE-{index:03d}"
        generated["candidate_refs"] = [{"id": candidate_id, "relation": "equivalent"}]
        generated["title"] = f"Fixture {index:03d}"
        generated["enforcement_refs"][0]["id"] = f"QT-ENF-{index:03d}"
        registry["guarantees"].append(generated)
    registry["scope"].update(
        phase="whole_system_classification",
        gate="complete",
        source_candidate_count=75,
        included_candidate_ids=all_candidate_ids,
        whole_system_classification_complete=True,
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="whole_system_complete_requires_concrete_remediation",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_active_guarantee_requires_normative_primary_authority(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    _write(
        tmp_path,
        "docs/architecture/decisions/0001-example.md",
        "---\nstatus: accepted\n---\n\n# ADR 0001\n\n## Decision\nExample.\n",
    )
    row = registry["guarantees"][0]
    registry["scope"]["gate"] = "gate_2_approved"
    row["registry_disposition"] = "enforced"
    row["activation_status"] = "active"
    row["authority_refs"] = [
        {
            "path": "docs/architecture/decisions/0001-example.md",
            "locator": {"kind": "heading", "value": "Decision"},
            "authority_kind": "decision_record",
            "source_lifecycle": "accepted",
            "role": "primary",
        }
    ]

    with pytest.raises(guarantees.GuaranteeValidationError, match="primary_normative"):
        guarantees.validate_registry_data(registry, root=tmp_path)

    row["authority_refs"] = [
        {
            "path": "src/guard.py",
            "locator": {"kind": "line_range", "start": 1, "end": 2},
            "authority_kind": "source_module_contract",
            "source_lifecycle": "active",
            "role": "primary",
        }
    ]
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="requires_module_documentation"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)

    row["authority_refs"] = [
        {
            "path": "docs/contracts/platform/00_system_contract.md",
            "locator": {"kind": "heading", "value": "Known-at causality"},
            "authority_kind": "normative_platform_contract",
            "source_lifecycle": "active",
            "role": "primary",
        }
    ]
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    glossary.write_text(
        glossary.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="active_requires_adopted_terms"):
        guarantees.validate_registry_data(registry, root=tmp_path)

    glossary.write_text(
        glossary.read_text(encoding="utf-8").replace("`proposed`", "`adopted`"),
        encoding="utf-8",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="invalid_proposal_status:adopted"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_gate_two_glossary_guards_status_heading_and_blocked_conflict(
    tmp_path: Path,
) -> None:
    registry, _ = _base_repository(tmp_path)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    glossary.write_text(
        glossary.read_text(encoding="utf-8").replace("`proposed`", "`adopted`"),
        encoding="utf-8",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="invalid_proposal_status:adopted"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)

    registry, _ = _base_repository(tmp_path)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    glossary.write_text(
        glossary.read_text(encoding="utf-8")
        + "\n## QT-TERM-001 — Duplicate\n\n- Proposal status: `proposed`\n",
        encoding="utf-8",
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="duplicate_term_heading"):
        guarantees.validate_registry_data(registry, root=tmp_path)

    registry, _ = _base_repository(tmp_path)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    glossary.write_text(
        glossary.read_text(encoding="utf-8").replace("`proposed`", "`blocked`"),
        encoding="utf-8",
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="blocked_requires_conflict_handling_reference",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_gate_two_pending_rejects_adopted_normative_glossary_entries(
    tmp_path: Path,
) -> None:
    registry, _ = _base_repository(tmp_path)
    _write(
        tmp_path,
        "docs/contracts/README.md",
        "# Contracts\n\n## Read Order\n\n"
        "1. `platform/00_system_contract.md`\n"
        "2. `platform/04_glossary.md`\n",
    )
    _write(
        tmp_path,
        "docs/contracts/platform/04_glossary.md",
        "# Platform glossary\n\n## QT-TERM-001 — Known-at\n\n"
        "- Adoption status: `adopted`\n",
    )

    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="gate_2_pending_forbids_adopted_glossary_entries",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_glossary_term_ids_must_come_from_phase_one_inventory(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["term_refs"] = ["QT-TERM-999"]
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    glossary.write_text(
        glossary.read_text(encoding="utf-8").replace("QT-TERM-001", "QT-TERM-999"),
        encoding="utf-8",
    )

    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="QT-TERM-999:not_in_phase_1_terminology_inventory",
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_indexed_claim_prose_rejects_normative_keywords(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["claim_summary"] = "The runtime MUST preserve causality."

    with pytest.raises(guarantees.GuaranteeValidationError, match="normative_keyword"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_reference_path_and_locator_must_resolve(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["authority_refs"][0]["locator"] = {
        "kind": "heading",
        "value": "Missing heading",
    }

    with pytest.raises(guarantees.GuaranteeValidationError, match="heading_must_resolve_once"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_authority_and_enforcement_locators_bind_frozen_baseline(
    tmp_path: Path,
) -> None:
    registry, catalog = _base_repository(tmp_path)

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(tmp_path), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    git("config", "user.email", "baseline-test@example.invalid")
    git("config", "user.name", "Baseline Test")
    git("add", ".")
    git("commit", "-m", "frozen audit baseline")
    baseline = git("rev-parse", "HEAD")
    registry["audit_baseline_commit"] = baseline
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/implementation-surface-inventory.json",
        json.dumps({"baseline_commit": baseline}) + "\n",
    )
    _write(
        tmp_path,
        "docs/contracts/platform/00_system_contract.md",
        "# System Contract\n\n## Current-only heading\nDrifted after the audit.\n",
    )
    git("add", ".")
    git("commit", "-m", "post-baseline documentation drift")

    bundle = _validate(tmp_path, registry, catalog)
    rendered = guarantees.render_markdown(bundle.registry, bundle.proof_catalog)
    assert f"@{baseline[:8]}" in rendered
    assert "#known-at-causality" not in rendered

    registry["guarantees"][0]["authority_refs"][0]["locator"] = {
        "kind": "heading",
        "value": "Current-only heading",
    }
    with pytest.raises(guarantees.GuaranteeValidationError, match="heading_must_resolve_once"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_guarantees_are_ordered_by_primary_candidate_id(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    candidate_inventory = tmp_path / "docs/plans/documentation-reconciliation/guarantee-candidates.md"
    candidate_inventory.write_text(
        "| ID | Claim |\n| --- | --- |\n| `QT-GC-001` | First |\n"
        "| `QT-GC-002` | Second |\n",
        encoding="utf-8",
    )
    second = copy.deepcopy(registry["guarantees"][0])
    second["id"] = "QT-GUAR-SECOND"
    second["candidate_refs"] = [{"id": "QT-GC-002", "relation": "equivalent"}]
    second["enforcement_refs"][0]["id"] = "QT-ENF-002"
    registry["guarantees"] = [second, registry["guarantees"][0]]
    registry["scope"]["included_candidate_ids"] = ["QT-GC-001", "QT-GC-002"]
    registry["scope"]["source_candidate_count"] = 2

    with pytest.raises(guarantees.GuaranteeValidationError, match="primary_candidate_id"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_owner_is_a_provisional_slug_not_a_claimed_subsystem_registry(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    registry["guarantees"][0]["owners"] = ["market-data"]

    guarantees.validate_registry_data(registry, root=tmp_path)

    registry["guarantees"][0]["owners"] = ["Market Data"]
    with pytest.raises(guarantees.GuaranteeValidationError, match="invalid_owner_slug"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_contradicted_claim_preserves_unresolved_authority_pair(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    _write(
        tmp_path,
        "docs/architecture/decisions/0001-first.md",
        "---\nstatus: accepted\n---\n\n# First decision\n",
    )
    _write(
        tmp_path,
        "docs/architecture/decisions/0002-second.md",
        "---\nstatus: accepted\n---\n\n# Second decision\n",
    )
    row = registry["guarantees"][0]
    row["claim_lifecycle"] = "unclear"
    row["registry_disposition"] = "contradicted"
    row["conformance"] = "contradicted"
    row["authority_refs"] = [
        {
            "path": f"docs/architecture/decisions/000{number}-{name}.md",
            "locator": {"kind": "heading", "value": f"{name.title()} decision"},
            "authority_kind": "decision_record",
            "source_lifecycle": "accepted",
            "role": "conflicting",
        }
        for number, name in ((1, "first"), (2, "second"))
    ]

    guarantees.validate_registry_data(registry, root=tmp_path)

    row["authority_refs"].pop()
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="requires_primary_or_conflicting_pair"
    ):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_catalog_rejects_dangling_claim_and_missing_test_node(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    validated = guarantees.validate_registry_data(registry, root=tmp_path)
    catalog["proofs"][0]["coverage"][0]["guarantee_id"] = "QT-GUAR-MISSING"

    with pytest.raises(guarantees.GuaranteeValidationError, match="unknown:QT-GUAR-MISSING"):
        guarantees.validate_proof_catalog_data(catalog, validated, root=tmp_path)

    _, catalog = _base_repository(tmp_path)
    catalog["proofs"][0]["runner"]["selectors"] = [
        "tests/test_known_at.py::test_does_not_exist"
    ]
    with pytest.raises(guarantees.GuaranteeValidationError, match="pytest_node_missing"):
        guarantees.validate_proof_catalog_data(catalog, validated, root=tmp_path)


def test_adequate_proof_requires_complete_required_mapping(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    catalog["proofs"][0]["coverage"][0].update(
        strength="partial", required_for_full_attestation=False
    )

    with pytest.raises(guarantees.GuaranteeValidationError, match="complete_required"):
        _validate(tmp_path, registry, catalog)

    registry, catalog = _base_repository(tmp_path)
    catalog["proofs"][0]["lifecycle"] = "proposed"
    with pytest.raises(guarantees.GuaranteeValidationError, match="required_proof_must_be_active"):
        _validate(tmp_path, registry, catalog)


def test_unclear_maturity_can_honestly_have_no_refs_or_links(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    unclear = registry["guarantees"][0]
    unclear["enforcement_maturity"] = "unclear"
    unclear["enforcement_refs"] = []
    unclear["proof_maturity"] = "unclear"
    unclear["proof_mode"] = "none"
    catalog["proofs"] = []

    _validate(tmp_path, registry, catalog)


def test_adequate_mixed_mode_requires_distinct_required_proofs(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    registry["guarantees"][0]["proof_mode"] = "mixed"
    supporting = copy.deepcopy(catalog["proofs"][0])
    supporting.update(
        id="QT-PROOF-002",
        proof_kind="manual_procedure",
    )
    supporting["runner"] = {
        "kind": "manual",
        "procedure_ref": {
            "path": "docs/contracts/platform/00_system_contract.md",
            "locator": {"kind": "heading", "value": "Known-at causality"},
        },
    }
    supporting["coverage"][0].update(
        strength="supporting",
        required_for_full_attestation=False,
    )
    catalog["proofs"].append(supporting)

    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="mixed_mode_requires_required_automated_and_manual_proofs",
    ):
        _validate(tmp_path, registry, catalog)

    supporting["coverage"][0]["required_for_full_attestation"] = True
    _validate(tmp_path, registry, catalog)


def test_proposed_required_proof_caps_attestation_and_cannot_be_attempted(
    tmp_path: Path,
) -> None:
    registry, catalog = _base_repository(tmp_path)
    row = registry["guarantees"][0]
    row.update(
        registry_disposition="partially_enforced",
        conformance="partial",
        enforcement_maturity="partial",
        proof_maturity="partial",
        proof_mode="mixed",
        remediation_status="pending",
    )
    catalog["proofs"][0]["coverage"][0]["strength"] = "partial"
    catalog["proofs"].append(
        {
            "id": "QT-PROOF-002",
            "title": "Proposed reviewed procedure",
            "lifecycle": "proposed",
            "proof_kind": "manual_procedure",
            "environment_profile_id": "python-nondb",
            "runner": {
                "kind": "manual",
                "procedure_ref": {
                    "path": "docs/contracts/platform/00_system_contract.md",
                    "locator": {"kind": "heading", "value": "Known-at causality"},
                },
            },
            "timeout_seconds": 60,
            "coverage": [
                {
                    "guarantee_id": "QT-GUAR-KNOWN-AT",
                    "strength": "partial",
                    "required_for_full_attestation": True,
                }
            ],
        }
    )
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2) + "\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    stdout_ref, stdout_hash = _runner_artifact(
        tmp_path, "proposed-model-active-proof.stdout", "1 passed\n"
    )
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb",
        "source": {
            "git_commit": "a" * 40,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(bundle),
        },
        "inputs": _attestation_inputs(bundle, catalog_path),
        "environments": [
            {
                "profile_id": "python-nondb",
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12.4"},
                "lockfile_hashes": {
                    "requirements.lock": hashlib.sha256(
                        (tmp_path / "requirements.lock").read_bytes()
                    ).hexdigest()
                },
                "services": {},
            }
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:01Z",
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "environment_profile_id": "python-nondb",
                "status": "PASS",
                "started_at": "2026-08-23T12:00:00Z",
                "finished_at": "2026-08-23T12:00:01Z",
                "exit_code": 0,
                "collected_count": 1,
                "passed_count": 1,
                "failed_count": 0,
                "skipped_count": 0,
                "xfailed_count": 0,
                "xpassed_count": 0,
                "stdout_sha256": stdout_hash,
                "executed_argv": [
                    "python",
                    "-m",
                    "pytest",
                    "tests/test_known_at.py::test_known_at",
                ],
                "evidence_refs": [stdout_ref],
            }
        ],
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "PARTIAL",
                "proof_ids": ["QT-PROOF-001"],
            }
        ],
    }
    _attach_result_summary(
        tmp_path, attestation["attestation_id"], attestation["proof_results"][0]
    )
    guarantees.validate_attestation_data(
        attestation,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )

    attempted_proposed = copy.deepcopy(attestation)
    manual_ref, _ = _runner_artifact(
        tmp_path,
        "proposed-model-manual.txt",
        "reviewed manual evidence\n",
        artifact_kind="manual_evidence",
        attestation_id=attestation["attestation_id"],
        proof_id="QT-PROOF-002",
    )
    attempted_proposed["proof_results"].append(
        {
            "proof_id": "QT-PROOF-002",
            "environment_profile_id": "python-nondb",
            "status": "PASS",
            "started_at": "2026-08-23T12:00:00Z",
            "finished_at": "2026-08-23T12:00:01Z",
            "operator_identity": "operator-a",
            "reviewer_identity": "reviewer-b",
            "evidence_refs": [manual_ref],
        }
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="attempted_result_requires_active_proof",
    ):
        guarantees.validate_attestation_data(
            attempted_proposed,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )


def test_manual_runner_cannot_be_disguised_as_automated(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    catalog["proofs"][0]["proof_kind"] = "automated_test"
    catalog["proofs"][0]["runner"] = {
        "kind": "manual",
        "procedure_ref": {
            "path": "docs/contracts/platform/00_system_contract.md",
            "locator": {"kind": "heading", "value": "Known-at causality"},
        },
    }

    with pytest.raises(guarantees.GuaranteeValidationError, match="manual_runner_requires"):
        _validate(tmp_path, registry, catalog)


def test_replacement_graph_must_be_acyclic(tmp_path: Path) -> None:
    registry, _ = _base_repository(tmp_path)
    candidate_inventory = tmp_path / "docs/plans/documentation-reconciliation/guarantee-candidates.md"
    candidate_inventory.write_text(
        "| ID | Claim |\n| --- | --- |\n| `QT-GC-001` | First |\n"
        "| `QT-GC-002` | Second |\n",
        encoding="utf-8",
    )
    first = registry["guarantees"][0]
    second = copy.deepcopy(first)
    first.update(
        claim_lifecycle="superseded",
        registry_disposition="superseded",
        replaced_by_ids=["QT-GUAR-SECOND"],
    )
    second["id"] = "QT-GUAR-SECOND"
    second["candidate_refs"] = [{"id": "QT-GC-002", "relation": "equivalent"}]
    second["enforcement_refs"][0]["id"] = "QT-ENF-002"
    second.update(
        claim_lifecycle="superseded",
        registry_disposition="superseded",
        replaced_by_ids=["QT-GUAR-KNOWN-AT"],
    )
    registry["guarantees"] = [first, second]
    registry["scope"]["included_candidate_ids"] = ["QT-GC-001", "QT-GC-002"]
    registry["scope"]["source_candidate_count"] = 2

    with pytest.raises(guarantees.GuaranteeValidationError, match="replacement_cycle"):
        guarantees.validate_registry_data(registry, root=tmp_path)


def test_assurance_material_hash_covers_authority_and_proof_files(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2, sort_keys=True) + "\n",
    )
    _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    before = guarantees.assurance_material_sha256(bundle)

    authority = tmp_path / "docs/contracts/platform/00_system_contract.md"
    authority.write_text(authority.read_text(encoding="utf-8") + "More detail.\n", encoding="utf-8")

    assert guarantees.assurance_material_sha256(bundle) != before


def test_attestation_derives_not_run_and_rejects_false_result_state(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    registry["scope"]["gate"] = "gate_2_approved"
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2, sort_keys=True) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb",
        "source": {
            "git_commit": "a" * 40,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(bundle),
        },
        "inputs": _attestation_inputs(bundle, catalog_path),
        "environments": [
            {
                "profile_id": "python-nondb",
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12"},
                "lockfile_hashes": {
                    "requirements.lock": hashlib.sha256(
                        (tmp_path / "requirements.lock").read_bytes()
                    ).hexdigest()
                },
                "services": {},
            },
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:01Z",
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "environment_profile_id": "python-nondb",
                "status": "NOT_RUN",
                "reason_code": "model_only",
                "evidence_refs": [],
            }
        ],
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "NOT_RUN",
                "proof_ids": ["QT-PROOF-001"],
            }
        ],
    }

    guarantees.validate_attestation_data(
        attestation,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )
    broken = copy.deepcopy(attestation)
    broken["guarantee_results"][0]["status"] = "PASS"
    with pytest.raises(guarantees.GuaranteeValidationError, match="expected_derived"):
        guarantees.validate_attestation_data(
            broken,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    wrong_identity = copy.deepcopy(attestation)
    wrong_identity["attestation_id"] = "QT-ATT-20260823T120000Z-bbbbbbbb-python-nondb"
    with pytest.raises(guarantees.GuaranteeValidationError, match="commit_prefix_mismatch"):
        guarantees.validate_attestation_data(
            wrong_identity,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    false_not_run = copy.deepcopy(attestation)
    false_not_run["proof_results"][0]["exit_code"] = 0
    with pytest.raises(guarantees.GuaranteeValidationError, match="NOT_RUN_forbids"):
        guarantees.validate_attestation_data(
            false_not_run,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    wrong_services = copy.deepcopy(attestation)
    wrong_services["environments"][0]["services"] = {"unbound-service": "present"}
    with pytest.raises(guarantees.GuaranteeValidationError, match="services:profile_mismatch"):
        guarantees.validate_attestation_data(
            wrong_services,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    mutable_evidence = copy.deepcopy(attestation)
    mutable_evidence["proof_results"][0]["evidence_refs"] = [
        {"artifact_kind": "stdout", "path": "requirements.lock", "sha256": "0" * 64}
    ]
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="outside_attestation_proof_evidence_layout",
    ):
        guarantees.validate_attestation_data(
            mutable_evidence,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    decorative_source_hash = copy.deepcopy(attestation)
    decorative_source_hash["source"]["runtime_source_tree_sha256"] = "0" * 64
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="unknown_keys:runtime_source_tree_sha256",
    ):
        guarantees.validate_attestation_data(
            decorative_source_hash,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )


def test_historical_attestation_validation_requires_git_metadata(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    bundle = _validate(tmp_path, registry, catalog)

    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="historical_attestation:git_metadata_required",
    ):
        guarantees.validate_attestation_historically(
            {"source": {"git_commit": "a" * 40, "clean": True}}, bundle
        )


def test_attestation_composes_required_proofs_across_environment_profiles(
    tmp_path: Path,
) -> None:
    registry, catalog = _base_repository(tmp_path)
    _write(
        tmp_path,
        "tests/test_known_at.py",
        "def test_known_at():\n    assert True\n\n"
        "def test_known_at_second():\n    assert True\n",
    )
    catalog["proofs"][0]["runner"]["selectors"].append(
        "tests/test_known_at.py::test_known_at_second"
    )
    registry["guarantees"][0]["proof_mode"] = "automated"
    catalog["environment_profiles"].insert(
        0,
        {
            "id": "python-db-isolated",
            "python": ">=3.12,<3.13",
            "lockfiles": ["requirements.lock"],
            "required_services": [],
        },
    )
    second_proof = copy.deepcopy(catalog["proofs"][0])
    second_proof.update(
        id="QT-PROOF-002",
        title="Known-at database proof",
        environment_profile_id="python-db-isolated",
    )
    catalog["proofs"].append(second_proof)
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2, sort_keys=True) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    lock_hash = hashlib.sha256((tmp_path / "requirements.lock").read_bytes()).hexdigest()
    multi_attestation_id = "QT-ATT-20260823T120000Z-aaaaaaaa-multi"
    proof_results = []
    for proof_id, profile_id in (
        ("QT-PROOF-001", "python-nondb"),
        ("QT-PROOF-002", "python-db-isolated"),
    ):
        runner_artifact, runner_hash = _runner_artifact(
            tmp_path,
            "pytest.stdout",
            "2 passed in 0.01s\n",
            attestation_id=multi_attestation_id,
            proof_id=proof_id,
        )
        result = {
            "proof_id": proof_id,
            "environment_profile_id": profile_id,
            "status": "PASS",
            "started_at": "2026-08-23T12:00:00Z",
            "finished_at": "2026-08-23T12:00:01Z",
            "exit_code": 0,
            "collected_count": 2,
            "passed_count": 2,
            "failed_count": 0,
            "skipped_count": 0,
            "xfailed_count": 0,
            "xpassed_count": 0,
            "stdout_sha256": runner_hash,
            "executed_argv": [
                "python",
                "-m",
                "pytest",
                "tests/test_known_at.py::test_known_at",
                "tests/test_known_at.py::test_known_at_second",
            ],
            "evidence_refs": [runner_artifact],
        }
        _attach_result_summary(tmp_path, multi_attestation_id, result)
        proof_results.append(result)
    proof_results.sort(key=lambda result: result["proof_id"])
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": multi_attestation_id,
        "source": {
            "git_commit": "a" * 40,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(bundle),
        },
        "inputs": _attestation_inputs(bundle, catalog_path),
        "environments": [
            {
                "profile_id": profile_id,
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12.4"},
                "lockfile_hashes": {"requirements.lock": lock_hash},
                "services": {},
            }
            for profile_id in ("python-db-isolated", "python-nondb")
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:02Z",
        "proof_results": proof_results,
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "PASS",
                "proof_ids": ["QT-PROOF-001", "QT-PROOF-002"],
            }
        ],
    }

    guarantees.validate_attestation_data(
        attestation,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )

    wrong_argv = copy.deepcopy(attestation)
    wrong_argv["proof_results"][0]["executed_argv"].pop()
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="runner_definition_mismatch"
    ):
        guarantees.validate_attestation_data(
            wrong_argv,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    undercollected = copy.deepcopy(attestation)
    undercollected["proof_results"][0].update(
        collected_count=1,
        passed_count=1,
    )
    _attach_result_summary(
        tmp_path, multi_attestation_id, undercollected["proof_results"][0]
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="undercollected"):
        guarantees.validate_attestation_data(
            undercollected,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])

    all_skipped = copy.deepcopy(attestation)
    all_skipped["proof_results"][0].update(
        passed_count=0,
        skipped_count=2,
    )
    _attach_result_summary(
        tmp_path, multi_attestation_id, all_skipped["proof_results"][0]
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="requires_passed_test"
    ):
        guarantees.validate_attestation_data(
            all_skipped,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])

    one_pass_one_xfail = copy.deepcopy(attestation)
    one_pass_one_xfail["proof_results"][0].update(
        passed_count=1,
        xfailed_count=1,
    )
    _attach_result_summary(
        tmp_path, multi_attestation_id, one_pass_one_xfail["proof_results"][0]
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="expected_outcomes"
    ):
        guarantees.validate_attestation_data(
            one_pass_one_xfail,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])

    synthetic_stdout = copy.deepcopy(attestation)
    synthetic_stdout["proof_results"][0]["stdout_sha256"] = "f" * 64
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="runner_artifact_mismatch"
    ):
        guarantees.validate_attestation_data(
            synthetic_stdout,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    valid_partial = copy.deepcopy(attestation)
    valid_partial["proof_results"][0].update(
        status="PARTIAL",
        reason_code="incomplete_collection",
        exit_code=0,
        passed_count=1,
        failed_count=0,
    )
    valid_partial["guarantee_results"][0]["status"] = "PARTIAL"
    _attach_result_summary(
        tmp_path, multi_attestation_id, valid_partial["proof_results"][0]
    )
    guarantees.validate_attestation_data(
        valid_partial,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )
    missing_partial_reason = copy.deepcopy(valid_partial)
    missing_partial_reason["proof_results"][0].pop("reason_code")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="PARTIAL_requires_reason_code",
    ):
        guarantees.validate_attestation_data(
            missing_partial_reason,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])

    masked_failure = copy.deepcopy(attestation)
    masked_failure["proof_results"][0].update(
        status="PARTIAL",
        reason_code="incomplete",
        exit_code=1,
        passed_count=1,
        failed_count=1,
    )
    _attach_result_summary(
        tmp_path, multi_attestation_id, masked_failure["proof_results"][0]
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="pytest_PARTIAL_forbids_failed_or_xpassed_tests",
    ):
        guarantees.validate_attestation_data(
            masked_failure,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])

    wrong_runtime = copy.deepcopy(attestation)
    wrong_runtime["environments"][0]["tool_versions"]["python"] = "3.11.9"
    with pytest.raises(guarantees.GuaranteeValidationError, match="not_satisfied"):
        guarantees.validate_attestation_data(
            wrong_runtime,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    overtime = copy.deepcopy(attestation)
    overtime["finished_at"] = "2026-08-23T12:02:00Z"
    overtime["proof_results"][0]["finished_at"] = "2026-08-23T12:01:01Z"
    _attach_result_summary(
        tmp_path, multi_attestation_id, overtime["proof_results"][0]
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="proof_timeout_exceeded"):
        guarantees.validate_attestation_data(
            overtime,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )
    _attach_result_summary(tmp_path, multi_attestation_id, attestation["proof_results"][0])


def test_reviewed_manual_pass_can_satisfy_required_proof(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    registry["guarantees"][0]["proof_mode"] = "manual"
    catalog["proofs"][0]["proof_kind"] = "manual_procedure"
    catalog["proofs"][0]["runner"] = {
        "kind": "manual",
        "procedure_ref": {
            "path": "docs/contracts/platform/00_system_contract.md",
            "locator": {"kind": "heading", "value": "Known-at causality"},
        },
    }
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2) + "\n",
    )
    evidence_path = _write(
        tmp_path,
        "docs/assurance/guarantees/evidence/"
        "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb/"
        "QT-PROOF-001/manual_evidence-review.txt",
        "independently reviewed evidence\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb",
        "source": {
            "git_commit": "a" * 40,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(bundle),
        },
        "inputs": _attestation_inputs(bundle, catalog_path),
        "environments": [
            {
                "profile_id": "python-nondb",
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12.4"},
                "lockfile_hashes": {
                    "requirements.lock": hashlib.sha256(
                        (tmp_path / "requirements.lock").read_bytes()
                    ).hexdigest()
                },
                "services": {},
            }
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:01Z",
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "environment_profile_id": "python-nondb",
                "status": "PASS",
                "started_at": "2026-08-23T12:00:00Z",
                "finished_at": "2026-08-23T12:00:01Z",
                "operator_identity": "operator-a",
                "reviewer_identity": "reviewer-b",
                "evidence_refs": [
                    {
                        "artifact_kind": "manual_evidence",
                        "path": "docs/assurance/guarantees/evidence/"
                        "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb/"
                        "QT-PROOF-001/manual_evidence-review.txt",
                        "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
                    }
                ],
            }
        ],
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "PASS",
                "proof_ids": ["QT-PROOF-001"],
            }
        ],
    }

    guarantees.validate_attestation_data(
        attestation,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )

    self_reviewed = copy.deepcopy(attestation)
    self_reviewed["proof_results"][0]["reviewer_identity"] = "operator-a"
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="independent_reviewer"
    ):
        guarantees.validate_attestation_data(
            self_reviewed,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    recorded_only = copy.deepcopy(attestation)
    recorded_only["proof_results"][0]["status"] = "MANUAL"
    recorded_only["proof_results"][0]["reason_code"] = "awaiting_independent_acceptance"
    recorded_only["proof_results"][0].pop("reviewer_identity")
    recorded_only["guarantee_results"][0]["status"] = "MANUAL"
    guarantees.validate_attestation_data(
        recorded_only,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )


def test_non_pytest_partial_requires_zero_exit_and_reason(tmp_path: Path) -> None:
    registry, catalog = _base_repository(tmp_path)
    script_path = _write(
        tmp_path, "scripts/check_known_at.py", "raise SystemExit(0)\n"
    )
    proof = catalog["proofs"][0]
    proof["proof_kind"] = "static_validation"
    proof["runner"] = {
        "kind": "python_script",
        "path": script_path.relative_to(tmp_path).as_posix(),
        "args": [],
    }
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2) + "\n",
    )
    bundle = _validate(tmp_path, registry, catalog)
    attestation_id = "QT-ATT-20260823T120000Z-aaaaaaaa-python-nondb"
    stdout_ref, stdout_hash = _runner_artifact(
        tmp_path,
        "python-script.stdout",
        "one step completed\n",
        attestation_id=attestation_id,
        proof_id="QT-PROOF-001",
    )
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": attestation_id,
        "source": {
            "git_commit": "a" * 40,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(bundle),
        },
        "inputs": _attestation_inputs(bundle, catalog_path),
        "environments": [
            {
                "profile_id": "python-nondb",
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12.4"},
                "lockfile_hashes": {
                    "requirements.lock": guarantees._sha256_file(
                        tmp_path / "requirements.lock"
                    )
                },
                "services": {},
            }
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:01Z",
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "environment_profile_id": "python-nondb",
                "status": "PARTIAL",
                "started_at": "2026-08-23T12:00:00Z",
                "finished_at": "2026-08-23T12:00:01Z",
                "exit_code": 0,
                "collected_count": 1,
                "reason_code": "incomplete_coverage",
                "stdout_sha256": stdout_hash,
                "executed_argv": ["python", "scripts/check_known_at.py"],
                "evidence_refs": [stdout_ref],
            }
        ],
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "PARTIAL",
                "proof_ids": ["QT-PROOF-001"],
            }
        ],
    }
    guarantees.validate_attestation_data(
        attestation,
        bundle,
        registry_path=registry_path,
        proof_catalog_path=catalog_path,
    )

    false_pass = copy.deepcopy(attestation)
    false_pass["proof_results"][0]["status"] = "PASS"
    false_pass["guarantee_results"][0]["status"] = "PASS"
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="automated_PASS_requires_pytest_runner_in_v1",
    ):
        guarantees.validate_attestation_data(
            false_pass,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

    failed_as_partial = copy.deepcopy(attestation)
    failed_as_partial["proof_results"][0]["exit_code"] = 1
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="automated_PARTIAL_forbids_nonzero_exit_code",
    ):
        guarantees.validate_attestation_data(
            failed_as_partial,
            bundle,
            registry_path=registry_path,
            proof_catalog_path=catalog_path,
        )

def test_active_reference_validates_historical_attestation_after_head_advances(
    tmp_path: Path,
) -> None:
    registry, catalog = _base_repository(tmp_path)
    registry["scope"]["gate"] = "gate_2_approved"
    row = registry["guarantees"][0]
    row["registry_disposition"] = "enforced"
    glossary = tmp_path / "docs/plans/documentation-reconciliation/proposed-glossary.md"
    _write(
        tmp_path,
        "docs/contracts/README.md",
        "# Contracts\n\n## Read Order\n\n"
        "1. `platform/00_system_contract.md`\n"
        "2. `platform/04_glossary.md`\n",
    )
    _write(
        tmp_path,
        "docs/contracts/platform/04_glossary.md",
        "# Platform glossary\n\n## QT-TERM-001 — Known-at\n\n"
        "- Adoption status: `adopted`\n",
    )
    _write(tmp_path, "scripts/docs/guarantees.py", "# validator v1\n")
    shutil.copytree(
        guarantees.SCHEMA_DIR,
        tmp_path / "docs/assurance/guarantees/schemas",
        dirs_exist_ok=True,
    )
    registry_path = _write(
        tmp_path,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2) + "\n",
    )
    catalog_path = _write(
        tmp_path,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2) + "\n",
    )

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(tmp_path), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    git("config", "user.email", "guarantee-test@example.invalid")
    git("config", "user.name", "Guarantee Test")
    git("add", ".")
    git("commit", "-m", "audit baseline")
    baseline_commit = git("rev-parse", "HEAD")
    registry["audit_baseline_commit"] = baseline_commit
    _write(
        tmp_path,
        "docs/plans/documentation-reconciliation/implementation-surface-inventory.json",
        json.dumps({"baseline_commit": baseline_commit}) + "\n",
    )
    registry_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "attested source")
    source_commit = git("rev-parse", "HEAD")
    source_bundle = _validate(tmp_path, registry, catalog)
    attestation_id = (
        f"QT-ATT-20260823T120000Z-{source_commit[:8]}-python-nondb"
    )
    runner_artifact, runner_hash = _runner_artifact(
        tmp_path,
        "activation-pytest.stdout",
        "1 passed in 0.01s\n",
        attestation_id=attestation_id,
        proof_id="QT-PROOF-001",
    )
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": attestation_id,
        "source": {
            "git_commit": source_commit,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(
                source_bundle, git_commit=source_commit
            ),
        },
        "inputs": _attestation_inputs(
            source_bundle, catalog_path, git_commit=source_commit
        ),
        "environments": [
            {
                "profile_id": "python-nondb",
                "os": "test",
                "architecture": "test",
                "tool_versions": {"python": "3.12.4"},
                "lockfile_hashes": {
                    "requirements.lock": guarantees._bound_material_sha256(
                        tmp_path, "requirements.lock", git_commit=source_commit
                    )
                },
                "services": {},
            }
        ],
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:00:01Z",
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "environment_profile_id": "python-nondb",
                "status": "PASS",
                "started_at": "2026-08-23T12:00:00Z",
                "finished_at": "2026-08-23T12:00:01Z",
                "exit_code": 0,
                "collected_count": 1,
                "passed_count": 1,
                "failed_count": 0,
                "skipped_count": 0,
                "xfailed_count": 0,
                "xpassed_count": 0,
            "stdout_sha256": runner_hash,
                "executed_argv": [
                    "python",
                    "-m",
                    "pytest",
                    "tests/test_known_at.py::test_known_at",
                ],
                "evidence_refs": [runner_artifact],
            }
        ],
        "guarantee_results": [
            {
                "guarantee_id": "QT-GUAR-KNOWN-AT",
                "status": "PASS",
                "proof_ids": ["QT-PROOF-001"],
            }
        ],
    }
    _attach_result_summary(tmp_path, attestation_id, attestation["proof_results"][0])
    attestation_text = json.dumps(attestation, indent=2) + "\n"
    attestation_sha256 = hashlib.sha256(attestation_text.encode("utf-8")).hexdigest()
    decision_path = _write(
        tmp_path,
        "docs/assurance/guarantees/activation-decisions/QT-ACT-DEC-001.md",
        "---\n"
        "status: reviewed\n"
        "decision_id: QT-ACT-DEC-001\n"
        "guarantee_id: QT-GUAR-KNOWN-AT\n"
        "decision_type: guarantee_activation\n"
        "gate_id: activation_review\n"
        "outcome: approved\n"
        f"attestation_id: {attestation_id}\n"
        f"attestation_sha256: {attestation_sha256}\n"
        "reviewed_by: user-elijah\n"
        "reviewed_at: 2026-08-23T12:30:00Z\n"
        "external_review_system: codex-thread\n"
        "external_review_id: gate-2-message-001\n"
        f"external_review_sha256: {'f' * 64}\n"
        "---\n\n"
        "# Activation decision QT-ACT-DEC-001\n\n"
        "Reviewed activation for QT-GUAR-KNOWN-AT.\n",
    )
    attestation_relative = (
        "docs/assurance/guarantees/attestations/"
        f"{source_commit}/{attestation_id}.json"
    )
    attestation_path = _write(
        tmp_path,
        attestation_relative,
        attestation_text,
    )
    row["activation_status"] = "active"
    row["activation_decision_refs"] = [
        {
            "decision_id": "QT-ACT-DEC-001",
            "decision_type": "guarantee_activation",
            "gate_id": "activation_review",
            "outcome": "approved",
            "attestation_id": attestation_id,
            "attestation_sha256": attestation_sha256,
            "reviewed_by": "user-elijah",
            "reviewed_at": "2026-08-23T12:30:00Z",
            "external_review_ref": {
                "system": "codex-thread",
                "reference_id": "gate-2-message-001",
                "sha256": "f" * 64,
            },
            "path": "docs/assurance/guarantees/activation-decisions/QT-ACT-DEC-001.md",
            "locator": {
                "kind": "heading",
                "value": "Activation decision QT-ACT-DEC-001",
            },
            "sha256": hashlib.sha256(decision_path.read_bytes()).hexdigest(),
        }
    ]
    row["activation_attestation_refs"] = [
        {
            "attestation_id": attestation_id,
            "path": attestation_relative,
            "sha256": attestation_sha256,
        }
    ]
    registry_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "activate guarantee")
    _write(tmp_path, "docs/unrelated-head-advance.md", "# Unrelated head advance\n")
    git("add", "docs/unrelated-head-advance.md")
    git("commit", "-m", "advance unrelated repository state")

    validated = guarantees.validate_repository(root=tmp_path)
    assert validated.registry["guarantees"][0]["activation_status"] == "active"
    guarantees.validate_attestation_file_historically(attestation_path, validated)

    git("config", "core.autocrlf", "true")
    summary_path = tmp_path.joinpath(
        *Path(
            attestation["proof_results"][0]["evidence_refs"][0]["path"]
        ).parts
    )
    stdout_path = tmp_path.joinpath(*Path(runner_artifact["path"]).parts)
    for text_path in (decision_path, attestation_path, summary_path, stdout_path):
        text_path.write_bytes(
            text_path.read_text(encoding="utf-8").replace("\n", "\r\n").encode("utf-8")
        )
    guarantees.validate_repository(root=tmp_path)
    git("config", "core.autocrlf", "false")
    for text_path in (decision_path, attestation_path, summary_path, stdout_path):
        text_path.write_bytes(
            text_path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")
        )

    original_decision_text = decision_path.read_text(encoding="utf-8")
    duplicate_decision = copy.deepcopy(registry)
    decision_path.write_text(
        original_decision_text.replace(
            "status: reviewed\n", "status: reviewed\nstatus: reviewed\n"
        ),
        encoding="utf-8",
    )
    duplicate_decision["guarantees"][0]["activation_decision_refs"][0][
        "sha256"
    ] = guarantees._sha256_file(decision_path)
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="frontmatter_duplicate_key:status",
    ):
        guarantees.validate_registry_data(duplicate_decision, root=tmp_path)

    unknown_decision = copy.deepcopy(registry)
    decision_path.write_text(
        original_decision_text.replace(
            "status: reviewed\n", "status: reviewed\nunknown_key: value\n"
        ),
        encoding="utf-8",
    )
    unknown_decision["guarantees"][0]["activation_decision_refs"][0][
        "sha256"
    ] = guarantees._sha256_file(decision_path)
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="frontmatter_unknown_keys:unknown_key",
    ):
        guarantees.validate_registry_data(unknown_decision, root=tmp_path)
    decision_path.write_text(original_decision_text, encoding="utf-8")

    missing_reviewer = copy.deepcopy(registry)
    missing_reviewer["guarantees"][0]["activation_decision_refs"][0].pop(
        "reviewed_by"
    )
    with pytest.raises(guarantees.GuaranteeValidationError, match="missing_keys:reviewed_by"):
        guarantees.validate_registry_data(missing_reviewer, root=tmp_path)

    rejected = copy.deepcopy(registry)
    rejected["guarantees"][0]["activation_decision_refs"][0]["outcome"] = "rejected"
    with pytest.raises(guarantees.GuaranteeValidationError, match="requires_approved"):
        guarantees.validate_registry_data(rejected, root=tmp_path)

    gate_two_is_not_activation = copy.deepcopy(registry)
    gate_two_is_not_activation["guarantees"][0]["activation_decision_refs"][0][
        "gate_id"
    ] = "gate_2"
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="expected_activation_review"
    ):
        guarantees.validate_registry_data(gate_two_is_not_activation, root=tmp_path)

    unbound_attestation = copy.deepcopy(attestation)
    unbound_attestation_id = (
        f"QT-ATT-20260823T120001Z-{source_commit[:8]}-python-nondb"
    )
    unbound_attestation["attestation_id"] = unbound_attestation_id
    unbound_relative = (
        "docs/assurance/guarantees/attestations/"
        f"{source_commit}/{unbound_attestation_id}.json"
    )
    unbound_path = _write(
        tmp_path, unbound_relative, json.dumps(unbound_attestation, indent=2) + "\n"
    )
    unbound_registry = copy.deepcopy(registry)
    unbound_registry["guarantees"][0]["activation_attestation_refs"].append(
        {
            "attestation_id": unbound_attestation_id,
            "path": unbound_relative,
            "sha256": guarantees._sha256_file(unbound_path),
        }
    )
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="activation_decisions_must_bind_exact_attestation_set",
    ):
        guarantees.validate_registry_data(unbound_registry, root=tmp_path)

    original_summary = row["claim_summary"]
    row["claim_summary"] = "Changed semantics under the same guarantee identifier."
    registry_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "change active claim semantics")
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="current_registry_semantics_mismatch"
    ):
        guarantees.validate_repository(root=tmp_path)

    row["claim_summary"] = original_summary
    original_proof_title = catalog["proofs"][0]["title"]
    catalog["proofs"][0]["title"] = "Changed proof semantics under the same proof ID"
    registry_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    catalog_path.write_text(json.dumps(catalog, indent=2) + "\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "change active proof semantics")
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="current_proof_catalog_mismatch"
    ):
        guarantees.validate_repository(root=tmp_path)

    catalog["proofs"][0]["title"] = original_proof_title
    catalog_path.write_text(json.dumps(catalog, indent=2) + "\n", encoding="utf-8")
    test_target = tmp_path / "tests/test_known_at.py"
    original_test_target = test_target.read_text(encoding="utf-8")
    test_target.write_text(original_test_target + "# semantic drift\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "change required proof target")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="current_required_proof_material_mismatch:QT-PROOF-001",
    ):
        guarantees.validate_repository(root=tmp_path)

    test_target.write_text(original_test_target, encoding="utf-8")
    enforcement_target = tmp_path / "src/guard.py"
    original_enforcement_target = enforcement_target.read_text(encoding="utf-8")
    enforcement_target.write_text(
        original_enforcement_target + "# implementation drift\n", encoding="utf-8"
    )
    git("add", ".")
    git("commit", "-m", "change guarantee enforcement source")
    with pytest.raises(
        guarantees.GuaranteeValidationError,
        match="current_guarantee_material_mismatch:QT-GUAR-KNOWN-AT",
    ):
        guarantees.validate_repository(root=tmp_path)

    enforcement_target.write_text(original_enforcement_target, encoding="utf-8")
    adopted_glossary = tmp_path / "docs/contracts/platform/04_glossary.md"
    adopted_glossary.write_text(
        adopted_glossary.read_text(encoding="utf-8") + "\nDefinition detail drift.\n",
        encoding="utf-8",
    )
    git("add", ".")
    git("commit", "-m", "change adopted glossary semantics")
    with pytest.raises(
        guarantees.GuaranteeValidationError, match="current_glossary_mismatch"
    ):
        guarantees.validate_repository(root=tmp_path)


def test_checked_in_phase_2b_snapshot_preserves_authorization_ceiling() -> None:
    bundle = guarantees.validate_repository()
    registry = bundle.registry
    rows = registry["guarantees"]
    scope = registry["scope"]

    assert scope["phase"] == "whole_system_classification"
    assert scope["gate"] == "gate_2_approved"
    assert scope["whole_system_classification_complete"] is True
    assert scope["source_candidate_count"] == 75
    assert len(scope["included_candidate_ids"]) == 75
    assert len(rows) == 75

    disposition_counts = {
        disposition: sum(
            row["registry_disposition"] == disposition for row in rows
        )
        for disposition in {
            "candidate",
            "contradicted",
            "implementation_property",
            "partially_enforced",
        }
    }
    assert disposition_counts == {
        "candidate": 6,
        "contradicted": 3,
        "implementation_property": 1,
        "partially_enforced": 65,
    }
    assert all(row["activation_status"] == "unactivated" for row in rows)
    assert all(not row["activation_decision_refs"] for row in rows)
    assert all(not row["activation_attestation_refs"] for row in rows)

    nonconforming = [
        row
        for row in rows
        if row["registry_disposition"] in {"partially_enforced", "contradicted"}
    ]
    assert len(nonconforming) == 68
    assert all(row["remediation_status"] == "recorded" for row in nonconforming)
    assert all(len(row["remediation_refs"]) == 1 for row in nonconforming)
    assert len(bundle.proof_catalog["proofs"]) == 85

    term_entries = guarantees._term_entries(guarantees.ROOT)
    assert len(term_entries) == 55
    assert sum(entry["status"] == "proposed" for entry in term_entries.values()) == 34
    assert sum(entry["status"] == "blocked" for entry in term_entries.values()) == 2
    assert sum(entry["status"] == "deferred" for entry in term_entries.values()) == 19
    adopted_term_entries = guarantees._adopted_term_entries(guarantees.ROOT)
    assert len(adopted_term_entries) == 53
    assert set(term_entries) - set(adopted_term_entries) == {"QT-TERM-035", "QT-TERM-055"}


def test_checked_in_phase_2b_review_map_is_complete_and_nonactivating() -> None:
    bundle = guarantees.validate_repository()
    review_map = guarantees.load_json_strict(
        Path("docs/plans/documentation-reconciliation/phase-2b-review-map.json")
    )
    metadata = review_map["map"]
    accounting = review_map["source_accounting"]

    assert metadata["status"] == "complete_for_required_review"
    assert metadata["classification_coverage_complete"] is True
    assert metadata["classification_only"] is True
    assert metadata["nonnormative"] is True
    assert metadata["terms_adopted"] is False
    assert metadata["guarantees_activated"] is False
    assert metadata["proof_results_asserted"] is False
    assert metadata["source_candidate_count"] == 75
    assert metadata["active_guarantee_count"] == 0

    assert accounting["phase_1_finding_count"] == 26
    assert accounting["phase_2b_new_finding_ids"] == [
        "DOC-CANDIDATE-LOCATOR-001"
    ]
    assert accounting["consolidated_finding_count"] == 27
    assert accounting["phase_2a_calibration_candidate_count"] == 12
    assert accounting["completed_batch_candidate_count"] == 63
    assert accounting["coverage_candidate_count"] == 75
    assert accounting["coverage_partition"]["complete"] is True
    assert accounting["final_registry_guarantee_count"] == 75
    assert accounting["final_proof_definition_count"] == 85
    assert accounting["final_concrete_remediation_count"] == 68
    assert accounting["completed_batch_decision_review_count"] == 21
    assert accounting["final_registry_activation_counts"] == {
        "unactivated": 75,
        "active": 0,
    }

    index = accounting["candidate_guarantee_index"]
    assert len(index) == 75
    assert {row["candidate_id"] for row in index} == set(
        bundle.registry["scope"]["included_candidate_ids"]
    )
    assert {row["guarantee_id"] for row in index} == {
        row["id"] for row in bundle.registry["guarantees"]
    }

    decisions = [
        decision
        for group in review_map["review_groups"]
        for decision in group["decisions"]
    ]
    assert len(decisions) == 40
    assert len({decision["id"] for decision in decisions}) == 40
    for decision in decisions:
        reviewers = decision["required_reviewers"]
        assert reviewers == sorted(set(reviewers))
        assert reviewers
        assert decision["decision_needed"].strip()
        assert decision["why_classification_cannot_settle"].strip()
        assert decision["forbidden_before_approval"].strip()

    assert len(review_map["authority_model_decisions"]) == 3
    assert len(review_map["proof_environment_ceilings"]) == 9
    assert review_map["terminology_accounting"]["total"] == 55
    assert review_map["terminology_accounting"]["proposed"] == 34
    assert review_map["terminology_accounting"]["blocked"] == 2
    assert review_map["terminology_accounting"]["deferred"] == 19
    assert review_map["terminology_accounting"]["adopted"] == 0

    conflict_reviews = {
        row["conflict_id"]: row
        for row in review_map["terminology_accounting"]["conflict_reviews"]
    }
    assert len(conflict_reviews) == 26
    for row in bundle.registry["guarantees"]:
        candidate_ids = {ref["id"] for ref in row["candidate_refs"]}
        for conflict_id in {
            ref for ref in row["finding_refs"] if ref.startswith("QT-CONFLICT-")
        }:
            review = conflict_reviews[conflict_id]
            assert candidate_ids <= set(review["candidate_ids"])
            assert row["id"] in review["guarantee_ids"]


def test_checked_in_schemas_match_executable_versions_and_enums() -> None:
    guarantees.validate_schema_contracts()
    schema_dir = Path("docs/assurance/guarantees/schemas")
    registry_schema = json.loads((schema_dir / "registry.v1.schema.json").read_text())
    proof_schema = json.loads((schema_dir / "proof-catalog.v1.schema.json").read_text())
    attestation_schema = json.loads((schema_dir / "attestation.v1.schema.json").read_text())

    assert registry_schema["properties"]["schema_version"]["const"] == guarantees.REGISTRY_SCHEMA_VERSION
    assert set(registry_schema["$defs"]["guarantee"]["properties"]["registry_disposition"]["enum"]) == guarantees.REGISTRY_DISPOSITIONS
    assert set(registry_schema["$defs"]["guarantee"]["properties"]["activation_status"]["enum"]) == guarantees.ACTIVATION_STATUSES
    assert set(proof_schema["$defs"]["proof"]["properties"]["proof_kind"]["enum"]) == guarantees.PROOF_KINDS
    assert set(attestation_schema["$defs"]["resultStatus"]["enum"]) == guarantees.ATTESTATION_RESULTS
    assert registry_schema["additionalProperties"] is False
    assert proof_schema["additionalProperties"] is False
    assert attestation_schema["additionalProperties"] is False
    assert "environments" in attestation_schema["required"]
    assert "environment" not in attestation_schema["properties"]
    assert "environment_profile_id" in attestation_schema["$defs"]["proofResult"]["required"]
    assert "runtime_source_tree_sha256" not in attestation_schema["$defs"]["source"]["properties"]
    assert "guarantee_material_sha256" in attestation_schema["$defs"]["inputs"]["required"]
    assert "pytest" in attestation_schema["$defs"]["proofResult"]["properties"]["status"][
        "description"
    ]
    assert "executed_argv" in attestation_schema["$defs"]["proofResult"]["properties"]
    assert "passed_count" in attestation_schema["$defs"]["proofResult"]["properties"]
    assert (
        attestation_schema["$defs"]["proofResult"]["properties"]["evidence_refs"]["items"][
            "$ref"
        ]
        == "#/$defs/evidenceRef"
    )
    assert proof_schema["$defs"]["proof"]["properties"]["coverage"]["uniqueItems"] is True


def test_checked_in_registry_catalog_schemas_and_view_are_consistent() -> None:
    bundle = guarantees.validate_repository()
    expected = guarantees.render_markdown(bundle.registry, bundle.proof_catalog).encode(
        "utf-8"
    )

    assert guarantees.VIEW_PATH.read_bytes() == expected

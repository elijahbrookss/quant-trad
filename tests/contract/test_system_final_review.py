from __future__ import annotations

import copy
import hashlib
import json

import pytest

from scripts.docs import build_system_final_review as final_review


HISTORICAL_SOURCE_COMMIT = "1672f22fc269cb3e4b11d68505057932b088e9ed"
HISTORICAL_REVIEW_SHA256 = (
    "f3a828ebf3b1ad1a7366f159fc463ffb68255bc4da5c66ddc3a6b986b80d913a"
)
HISTORICAL_VIEW_SHA256 = (
    "c49e7048c0cda43fedfa0db1448fe38cb8ebdd4102eed7f8ea5a8e8a4e3fa6c7"
)
HISTORICAL_SOURCE_MATERIAL_PATHS = {
    "docs/assurance/guarantees/proof-catalog.json",
    "docs/assurance/guarantees/registry.json",
    "docs/plans/documentation-reconciliation/phase-3-final-review-policy.json",
}
IMPLEMENTED = {
    "QT-REM-004",
    "QT-REM-207",
    "QT-REM-308",
    "QT-REM-313",
    "QT-REM-400",
}
UNAVAILABLE = {"QT-REM-009", "QT-REM-311"}
DEFERRED = {
    "QT-REM-003",
    "QT-REM-121",
    "QT-REM-203",
    "QT-REM-205",
    "QT-REM-208",
    "QT-REM-210",
    "QT-REM-211",
    "QT-REM-213",
    "QT-REM-307",
    "QT-REM-310",
    "QT-REM-312",
}


@pytest.fixture(scope="module")
def review() -> dict:
    source_commit = final_review._git_text(
        final_review.ROOT,
        "rev-parse",
        "HEAD",
        where="tests.current_source",
    )
    return final_review.build_review(
        root=final_review.ROOT,
        source_commit=source_commit,
    )


@pytest.fixture(scope="module")
def historical_review() -> dict:
    return final_review.guarantees.load_json_strict(
        final_review.HISTORICAL_REVIEW_PATH
    )


def test_pre_attestation_review_accounts_for_every_record(review: dict) -> None:
    assert len(review["guarantees"]) == 75
    assert len(review["proofs"]) == 85
    assert len(review["remediations"]) == 68
    assert review["attestation_binding"] == {
        "state": "not_attested",
        "attestations": [],
    }
    assert review["attestation_results"] == []
    assert review["proof_accounting"]["attestation_count"] == 0
    assert review["proof_accounting"]["raw_result_counts"] is None
    assert review["proof_accounting"]["effective_result_counts"] is None
    assert all(
        row["forward_state"]["activation_status"] == "unactivated"
        for row in review["guarantees"]
    )
    assert all(
        row["proof_assessment"]["attested_status"] is None
        and row["proof_assessment"]["reported_status"] is None
        for row in review["guarantees"]
    )
    assert all(row["attested_status"] is None for row in review["proofs"])
    proof_ids = {row["id"] for row in review["proofs"]}
    assert len(proof_ids) == 85
    assert "QT-PROOF-014" in proof_ids
    proof_014 = next(row for row in review["proofs"] if row["id"] == "QT-PROOF-014")
    assert proof_014["lifecycle"] == "active"
    assert review["final_gate"] == {
        "mode": "intermediate",
        "repository_state": None,
        "validation_results_source": None,
        "validation_results": [],
        "integration_approval_request": None,
    }
    assert {
        row["path"] for row in review["assessment_subject"]["source_material"]
    } == final_review.SOURCE_MATERIAL_PATHS


def test_forward_state_and_remediation_outcomes_are_exact(review: dict) -> None:
    assert review["summary"]["registry_disposition"] == {
        "candidate": 6,
        "implementation_property": 1,
        "partially_enforced": 68,
    }
    assert review["summary"]["conformance"] == {
        "partial": 12,
        "static_aligned": 63,
    }
    assert review["summary"]["remediation_outcomes"] == {
        "deferred": 11,
        "implemented": 5,
        "planned": 50,
        "unavailable": 2,
    }
    rows = {row["id"]: row for row in review["remediations"]}
    assert {
        item
        for item, row in rows.items()
        if row["system_action_outcome"] == "implemented"
    } == IMPLEMENTED
    assert {
        item
        for item, row in rows.items()
        if row["system_action_outcome"] == "unavailable"
    } == UNAVAILABLE
    assert {
        item
        for item, row in rows.items()
        if row["system_action_outcome"] == "deferred"
    } == DEFERRED
    assert all(
        row["source_lifecycle"] == "proposed"
        and row["source_review_status"] == "pending"
        and row["closure_state"] == "open"
        for row in rows.values()
    )


def test_checked_intermediate_review_fixture_is_deterministic(
    historical_review: dict,
) -> None:
    checked = json.loads(
        final_review.HISTORICAL_REVIEW_PATH.read_text(encoding="utf-8")
    )
    assert checked == historical_review
    assert checked["assessment_subject"]["source_commit"] == HISTORICAL_SOURCE_COMMIT
    assert {
        row["path"] for row in checked["assessment_subject"]["source_material"]
    } == HISTORICAL_SOURCE_MATERIAL_PATHS
    assert checked["final_gate"] == {
        "mode": "intermediate",
        "repository_state": None,
        "validation_results_source": None,
        "validation_results": [],
        "integration_approval_request": None,
    }
    assert checked["attestation_binding"]["state"] == "not_attested"
    proof_014 = next(row for row in checked["proofs"] if row["id"] == "QT-PROOF-014")
    assert proof_014["lifecycle"] == "active"
    assert (
        hashlib.sha256(final_review.HISTORICAL_REVIEW_PATH.read_bytes()).hexdigest()
        == HISTORICAL_REVIEW_SHA256
    )
    assert final_review.HISTORICAL_REVIEW_PATH.read_bytes() == (
        final_review._canonical_json_bytes(checked)
    )
    assert (
        hashlib.sha256(final_review.HISTORICAL_VIEW_PATH.read_bytes()).hexdigest()
        == HISTORICAL_VIEW_SHA256
    )


def test_published_schema_is_programmatically_bound_to_executable_model() -> None:
    final_review._validate_review_schema_contract(final_review.ROOT)
    schema = final_review.guarantees.load_json_strict(final_review.SCHEMA_PATH)
    assert schema["properties"]["final_gate"] == {"$ref": "#/$defs/finalGate"}
    assert schema["$defs"]["finalGate"]["properties"]["mode"]["enum"] == [
        "intermediate",
        "final_gate",
    ]


def test_historical_source_cannot_be_rebuilt_with_current_material() -> None:
    with pytest.raises(final_review.FinalReviewError, match="exact_source_mismatch"):
        final_review._source_material_binding(
            final_review.ROOT,
            HISTORICAL_SOURCE_COMMIT,
        )


def _synthetic_attestation(
    attestation_id: str,
    results: list[tuple[str, str]],
) -> dict:
    return {
        "attestation_id": attestation_id,
        "proof_results": [
            {"proof_id": proof_id, "status": status}
            for proof_id, status in results
        ],
    }


def test_attestation_loader_sorts_and_binds_every_immutable_input(
    tmp_path, monkeypatch
) -> None:
    attestation_dir = (
        tmp_path / "docs" / "assurance" / "guarantees" / "attestations"
    )
    attestation_dir.mkdir(parents=True)
    earlier = attestation_dir / "a.json"
    later = attestation_dir / "b.json"
    earlier.write_text("{}\n", encoding="utf-8")
    later.write_text("{}\n", encoding="utf-8")
    ids = {
        "a.json": "QT-ATT-20260824T120000Z-aaaaaaaa-python-nondb",
        "b.json": "QT-ATT-20260824T120100Z-aaaaaaaa-frontend-node",
    }

    monkeypatch.setattr(
        final_review.guarantees,
        "load_json_strict",
        lambda _: {"source": {"git_commit": HISTORICAL_SOURCE_COMMIT}},
    )

    def validated(path, _bundle, *, evidence_root):
        assert evidence_root == tmp_path
        profile = "python-nondb" if path.name == "a.json" else "frontend-node"
        return {
            "attestation_id": ids[path.name],
            "environments": [{"profile_id": profile}],
            "proof_results": [],
            "guarantee_results": [],
        }

    monkeypatch.setattr(
        final_review.guarantees,
        "validate_attestation_file_historically",
        validated,
    )
    loaded, binding = final_review._load_attestations(
        root=tmp_path,
        bundle=object(),
        source_commit=HISTORICAL_SOURCE_COMMIT,
        attestation_paths=[later, earlier],
    )

    assert [row["attestation_id"] for row in loaded] == [
        ids["a.json"],
        ids["b.json"],
    ]
    assert [row["path"] for row in binding["attestations"]] == [
        "docs/assurance/guarantees/attestations/a.json",
        "docs/assurance/guarantees/attestations/b.json",
    ]


def test_validation_results_are_immutable_sorted_and_source_bound(tmp_path) -> None:
    source_commit = "a" * 40
    evidence_path = "logs/system-validation/catalog-check.txt"
    evidence = tmp_path / evidence_path
    evidence.parent.mkdir(parents=True)
    evidence.write_text("catalog valid\n", encoding="utf-8")
    evidence_sha = hashlib.sha256(evidence.read_bytes()).hexdigest()
    results = [
        {
            "id": "catalog-check",
            "command_argv": ["python", "scripts/docs/guarantees.py", "check"],
            "status": "PASS",
            "exit_code": 0,
            "started_at": "2026-08-25T12:00:00Z",
            "finished_at": "2026-08-25T12:00:01Z",
            "reason_code": None,
            "evidence_refs": [{"path": evidence_path, "sha256": evidence_sha}],
        },
        {
            "id": "recovery-environment",
            "command_argv": ["manual", "isolated-recovery-rehearsal"],
            "status": "UNAVAILABLE",
            "exit_code": None,
            "started_at": None,
            "finished_at": None,
            "reason_code": "approved_environment_not_provisioned",
            "evidence_refs": [],
        },
    ]
    document = {
        "schema_version": final_review.VALIDATION_RESULTS_SCHEMA_VERSION,
        "source_commit": source_commit,
        "results": results,
    }
    result_path = (
        tmp_path
        / "docs"
        / "assurance"
        / "guarantees"
        / "validation-results"
        / "system.json"
    )
    result_path.parent.mkdir(parents=True)
    result_path.write_text(json.dumps(document) + "\n", encoding="utf-8")

    loaded, binding = final_review._load_validation_results(
        root=tmp_path,
        source_commit=source_commit,
        path=result_path,
    )

    assert loaded == results
    assert binding == {
        "path": "docs/assurance/guarantees/validation-results/system.json",
        "sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
    }
    with pytest.raises(final_review.FinalReviewError, match="ids_must_be_sorted_unique"):
        final_review._validate_embedded_validation_results(list(reversed(results)))


def _strict_final_gate_review(intermediate: dict) -> dict:
    review = copy.deepcopy(intermediate)
    attestation_id = "QT-ATT-20260825T120000Z-aaaaaaaa-final-gate"
    attestation_path = (
        "docs/assurance/guarantees/attestations/"
        "QT-ATT-20260825T120000Z-aaaaaaaa-final-gate.json"
    )
    review["attestation_binding"] = {
        "state": "bound",
        "attestations": [
            {
                "attestation_id": attestation_id,
                "path": attestation_path,
                "sha256": "a" * 64,
                "environment_profile_ids": ["python-nondb"],
            }
        ],
    }
    review["attestation_results"] = [
        {
            "attestation_id": attestation_id,
            "path": attestation_path,
            "proof_results": [
                {"proof_id": row["id"], "status": "NOT_RUN"}
                for row in review["proofs"]
            ],
            "guarantee_results": [],
        }
    ]
    for row in review["proofs"]:
        row["result_state"] = "not_run"
        row["attested_status"] = "NOT_RUN"
        row["effective_attestation_ids"] = []
        row["raw_result_refs"] = [
            {
                "attestation_id": attestation_id,
                "status": "NOT_RUN",
                "profile_bound": False,
            }
        ]
    for row in review["guarantees"]:
        proof = row["proof_assessment"]
        if row["active_required_proof_ids"]:
            proof.update(
                {
                    "state": "attested",
                    "attested_status": "NOT_RUN",
                    "reported_status": "NOT_RUN",
                    "reason_codes": ["explicit_not_run_results"],
                }
            )
        else:
            proof.update(
                {
                    "state": "unavailable",
                    "attested_status": None,
                    "reported_status": None,
                    "reason_codes": ["no_active_required_proof"],
                }
            )
    review["proof_accounting"].update(
        {
            "attestation_count": 1,
            "raw_result_counts": {"NOT_RUN": 85},
            "effective_result_counts": {"NOT_RUN": 85},
        }
    )
    review["final_gate"] = {
        "mode": "final_gate",
        "repository_state": {
            "branch": "feat/docs-guarantee-reconciliation",
            "packet_input_commit": "b" * 40,
            "packet_input_tree": "c" * 40,
            "develop_commit": "d" * 40,
            "clean": True,
        },
        "validation_results_source": {
            "path": "docs/assurance/guarantees/validation-results/system.json",
            "sha256": "e" * 64,
        },
        "validation_results": [
            {
                "id": "recovery-environment",
                "command_argv": ["manual", "isolated-recovery-rehearsal"],
                "status": "UNAVAILABLE",
                "exit_code": None,
                "started_at": None,
                "finished_at": None,
                "reason_code": "approved_environment_not_provisioned",
                "evidence_refs": [],
            }
        ],
        "integration_approval_request": {
            "status": "requested",
            "requested_action": "integrate_feature_branch_into_develop",
            "target_branch": "develop",
            "guarantee_activation_included": False,
        },
    }
    return review


def test_strict_final_gate_requires_explicit_all_active_proof_results(
    review: dict,
) -> None:
    strict = _strict_final_gate_review(review)
    final_review.validate_review_data(strict, final_gate_required=True)

    proof_014 = next(row for row in strict["proofs"] if row["id"] == "QT-PROOF-014")
    proof_014["raw_result_refs"] = []
    with pytest.raises(final_review.FinalReviewError, match="raw_result_coverage_required"):
        final_review.validate_review_data(strict, final_gate_required=True)


def test_final_packet_records_input_identity_not_its_own_future_head(
    review: dict,
) -> None:
    strict = _strict_final_gate_review(review)
    strict["final_gate"]["repository_state"]["head_commit"] = "f" * 40
    with pytest.raises(final_review.FinalReviewError, match="unknown_keys:head_commit"):
        final_review.validate_review_data(strict, final_gate_required=True)


def test_external_gate_verifies_clean_single_packet_commit(monkeypatch) -> None:
    packet_input = "a" * 40
    packet_head = "b" * 40
    packet_tree = "c" * 40
    develop = "d" * 40
    state = {
        "branch": "feat/docs-guarantee-reconciliation",
        "packet_input_commit": packet_input,
        "packet_input_tree": "e" * 40,
        "develop_commit": develop,
        "clean": True,
    }

    def git_text(_root, *args, where):
        responses = {
            ("symbolic-ref", "--short", "HEAD"): state["branch"],
            ("rev-parse", "HEAD"): packet_head,
            ("rev-parse", "HEAD^{tree}"): packet_tree,
            ("status", "--porcelain=v1", "--untracked-files=all"): "",
            (
                "ls-remote",
                "--exit-code",
                final_review.DEVELOP_REMOTE,
                final_review.DEVELOP_REMOTE_REF,
            ): f"{develop}\t{final_review.DEVELOP_REMOTE_REF}",
            ("rev-parse", final_review.DEVELOP_TRACKING_REF): develop,
            ("show", "-s", "--format=%P", packet_head): packet_input,
            (
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                packet_head,
            ): "\n".join(sorted(final_review.FINAL_GATE_PACKET_FILES)),
        }
        assert args in responses, where
        return responses[args]

    monkeypatch.setattr(final_review, "_git_text", git_text)
    observed = final_review._verify_external_approval_repository_state(
        final_review.ROOT, state
    )
    assert observed == {
        "branch": state["branch"],
        "head_commit": packet_head,
        "head_tree": packet_tree,
        "clean": True,
        "packet_parent_commit": packet_input,
        "develop_commit": develop,
    }


def test_external_gate_rejects_remote_develop_drift(monkeypatch) -> None:
    state = {
        "branch": "feat/docs-guarantee-reconciliation",
        "packet_input_commit": "a" * 40,
        "packet_input_tree": "b" * 40,
        "develop_commit": "c" * 40,
        "clean": True,
    }

    def git_text(_root, *args, where):
        responses = {
            ("symbolic-ref", "--short", "HEAD"): state["branch"],
            ("rev-parse", "HEAD"): "d" * 40,
            ("rev-parse", "HEAD^{tree}"): "e" * 40,
            ("status", "--porcelain=v1", "--untracked-files=all"): "",
            (
                "ls-remote",
                "--exit-code",
                final_review.DEVELOP_REMOTE,
                final_review.DEVELOP_REMOTE_REF,
            ): f"{'f' * 40}\t{final_review.DEVELOP_REMOTE_REF}",
            ("rev-parse", final_review.DEVELOP_TRACKING_REF): "f" * 40,
        }
        assert args in responses, where
        return responses[args]

    monkeypatch.setattr(final_review, "_git_text", git_text)
    with pytest.raises(
        final_review.FinalReviewError,
        match="develop_remote:changed_since_packet_input",
    ):
        final_review._verify_external_approval_repository_state(
            final_review.ROOT, state
        )


def test_remote_develop_ref_rejects_stale_tracking_state(monkeypatch) -> None:
    remote_commit = "a" * 40

    def git_text(_root, *args, where):
        responses = {
            (
                "ls-remote",
                "--exit-code",
                final_review.DEVELOP_REMOTE,
                final_review.DEVELOP_REMOTE_REF,
            ): f"{remote_commit}\t{final_review.DEVELOP_REMOTE_REF}",
            ("rev-parse", final_review.DEVELOP_TRACKING_REF): "b" * 40,
        }
        assert args in responses, where
        return responses[args]

    monkeypatch.setattr(final_review, "_git_text", git_text)
    with pytest.raises(final_review.FinalReviewError, match="tracking:stale"):
        final_review._remote_develop_commit(
            final_review.ROOT, "approval_gate.develop_remote"
        )


def test_slice_evidence_requires_complete_named_commit_footprint(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        final_review.subprocess,
        "run",
        lambda *args, **kwargs: object(),
    )

    def git_text(_root, *args, where):
        assert args[:6] == (
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--name-only",
            "-r",
            "b" * 40,
        ), where
        return "docs/one.md\ndocs/two.md"

    monkeypatch.setattr(final_review, "_git_text", git_text)
    with pytest.raises(
        final_review.FinalReviewError,
        match="missing_commit_footprint:docs/two.md",
    ):
        final_review._verify_system_slice_evidence(
            final_review.ROOT,
            "a" * 40,
            [
                {
                    "id": "QT-SYSTEM-SLICE-TEST",
                    "commits": ["b" * 40],
                    "files": ["docs/one.md"],
                }
            ],
        )


def test_multiple_attestations_resolve_only_the_owning_profile_result() -> None:
    catalog = [
        {
            "id": "QT-PROOF-001",
            "lifecycle": "active",
            "environment_profile_id": "python-nondb",
        },
        {
            "id": "QT-PROOF-002",
            "lifecycle": "active",
            "environment_profile_id": "frontend-node",
        },
        {
            "id": "QT-PROOF-003",
            "lifecycle": "active",
            "environment_profile_id": "manual-recovery",
        },
    ]
    python_id = "QT-ATT-20260824T120000Z-aaaaaaaa-python-nondb"
    frontend_id = "QT-ATT-20260824T120100Z-aaaaaaaa-frontend-node"
    attestations = [
        _synthetic_attestation(
            python_id,
            [
                ("QT-PROOF-001", "PASS"),
                ("QT-PROOF-002", "NOT_RUN"),
                ("QT-PROOF-003", "UNAVAILABLE"),
            ],
        ),
        _synthetic_attestation(
            frontend_id,
            [
                ("QT-PROOF-001", "UNAVAILABLE"),
                ("QT-PROOF-002", "UNAVAILABLE"),
                ("QT-PROOF-003", "NOT_RUN"),
            ],
        ),
    ]
    binding = {
        "attestations": [
            {
                "attestation_id": python_id,
                "environment_profile_ids": ["python-nondb"],
            },
            {
                "attestation_id": frontend_id,
                "environment_profile_ids": ["frontend-node"],
            },
        ]
    }

    effective, raw = final_review._effective_proof_results(
        catalog=catalog,
        attestations=attestations,
        binding=binding,
    )

    assert effective["QT-PROOF-001"] == {
        "state": "profile_result",
        "status": "PASS",
        "attestation_ids": [python_id],
    }
    assert effective["QT-PROOF-002"] == {
        "state": "unavailable",
        "status": "UNAVAILABLE",
        "attestation_ids": [frontend_id],
    }
    assert effective["QT-PROOF-003"] == {
        "state": "unavailable",
        "status": "UNAVAILABLE",
        "attestation_ids": [python_id],
    }
    assert [row["status"] for row in raw["QT-PROOF-001"]] == [
        "PASS",
        "UNAVAILABLE",
    ]


def test_conflicting_profile_results_are_rejected() -> None:
    proof = {
        "id": "QT-PROOF-001",
        "lifecycle": "active",
        "environment_profile_id": "python-nondb",
    }
    first_id = "QT-ATT-20260824T120000Z-aaaaaaaa-python-nondb"
    second_id = "QT-ATT-20260824T120100Z-aaaaaaaa-python-nondb"
    attestations = [
        _synthetic_attestation(first_id, [("QT-PROOF-001", "PASS")]),
        _synthetic_attestation(second_id, [("QT-PROOF-001", "FAIL")]),
    ]
    binding = {
        "attestations": [
            {
                "attestation_id": first_id,
                "environment_profile_ids": ["python-nondb"],
            },
            {
                "attestation_id": second_id,
                "environment_profile_ids": ["python-nondb"],
            },
        ]
    }
    with pytest.raises(
        final_review.FinalReviewError,
        match="conflicting_attempted_results",
    ):
        final_review._effective_proof_results(
            catalog=[proof],
            attestations=attestations,
            binding=binding,
        )


def test_status_aggregation_matches_attestation_v1() -> None:
    assert final_review._aggregate_status(["PASS", "PASS"]) == "PASS"
    assert final_review._aggregate_status(["PASS", "FAIL"]) == "FAIL"
    assert final_review._aggregate_status(["NOT_RUN", "NOT_RUN"]) == "NOT_RUN"
    assert (
        final_review._aggregate_status(["UNAVAILABLE", "UNAVAILABLE"])
        == "UNAVAILABLE"
    )
    assert final_review._aggregate_status(["PASS", "NOT_RUN"]) == "PARTIAL"

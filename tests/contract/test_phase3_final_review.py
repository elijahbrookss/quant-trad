from __future__ import annotations

import json

import pytest

from scripts.docs import build_phase3_final_review as final_review


SOURCE_COMMIT = "8029d9ab20ad559c4860a2ca1aed8d0328c99292"
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
    return final_review.build_review(
        root=final_review.ROOT,
        source_commit=SOURCE_COMMIT,
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
        if row["phase3_action_outcome"] == "implemented"
    } == IMPLEMENTED
    assert {
        item
        for item, row in rows.items()
        if row["phase3_action_outcome"] == "unavailable"
    } == UNAVAILABLE
    assert {
        item
        for item, row in rows.items()
        if row["phase3_action_outcome"] == "deferred"
    } == DEFERRED
    assert all(
        row["source_lifecycle"] == "proposed"
        and row["source_review_status"] == "pending"
        and row["closure_state"] == "open"
        for row in rows.values()
    )


def test_checked_machine_and_human_views_are_deterministic(review: dict) -> None:
    checked = json.loads(final_review.REVIEW_PATH.read_text(encoding="utf-8"))
    assert checked == review
    assert final_review.REVIEW_PATH.read_bytes() == final_review._canonical_json_bytes(
        review
    )
    assert final_review.VIEW_PATH.read_text(encoding="utf-8") == (
        final_review.render_markdown(review)
    )
    schema = json.loads(final_review.SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["properties"]["guarantees"]["minItems"] == 75
    assert schema["properties"]["proofs"]["minItems"] == 85
    assert schema["properties"]["remediations"]["minItems"] == 68


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
        lambda _: {"source": {"git_commit": SOURCE_COMMIT}},
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
        source_commit=SOURCE_COMMIT,
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

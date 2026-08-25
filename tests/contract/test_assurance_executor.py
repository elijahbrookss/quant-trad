from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.assurance import pytest_result_plugin
from scripts.assurance import verify_guarantees as verifier
from scripts.docs import guarantees


ROOT = Path(__file__).resolve().parents[2]


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _evidence(
    kind: str = "runtime_probe",
    *,
    path: str = "profile/runtime-probe.json",
    digest: str = "a" * 64,
) -> dict[str, str]:
    return {"kind": kind, "path": path, "sha256": digest}


def _profile_admission(*, services: list[object] | None = None) -> dict:
    return {
        "profile_id": "python-nondb",
        "admission_id": "phase3-python-nondb",
        "admitted": True,
        "environment_class": "local_test",
        "isolation": "process_local",
        "external_order_submission_enabled": False,
        "tools": {},
        "services": services or [],
        "admission_evidence": [_evidence()],
    }


def _pytest_session_stdout(
    node_ids: list[str], outcomes: list[str], *, exit_code: int = 0
) -> bytes:
    counts = {name: 0 for name in ("passed", "failed", "skipped", "xfailed", "xpassed")}
    results = []
    for node_id, outcome in zip(node_ids, outcomes, strict=True):
        counts[outcome] += 1
        results.append({"node_id": node_id, "outcome": outcome})
    event = {
        "schema_version": pytest_result_plugin.SCHEMA_VERSION,
        "event": "session_result",
        "collection_errors": [],
        "counts": counts,
        "exit_code": exit_code,
        "node_ids": node_ids,
        "results": results,
    }
    return (
        pytest_result_plugin.LINE_PREFIX
        + json.dumps(event, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode()


def test_exact_source_binding_requires_clean_head_and_external_stage(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    (root / "bound.txt").write_text("bound\n", encoding="utf-8")
    _git(root, "add", "bound.txt")
    _git(
        root,
        "-c",
        "user.name=QT Assurance Test",
        "-c",
        "user.email=assurance@example.invalid",
        "commit",
        "-m",
        "fixture",
    )
    commit = _git(root, "rev-parse", "HEAD")
    verifier.require_exact_clean_source(root, commit, tmp_path / "stage")

    with pytest.raises(
        verifier.AssuranceExecutionError, match="stage_root_must_be_outside_source_tree"
    ):
        verifier.require_exact_clean_source(root, commit, root / "stage")

    (root / "bound.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(
        verifier.AssuranceExecutionError, match="source_worktree_must_be_clean"
    ):
        verifier.require_exact_clean_source(root, commit, tmp_path / "stage")


def test_profile_admission_rejects_free_form_or_secret_service_data() -> None:
    free_form = _profile_admission(services=["isolated database"])
    with pytest.raises(verifier.AssuranceExecutionError, match="object_required"):
        verifier._validate_profile_admission(free_form, "profile")

    service = {
        "service_id": "isolated-postgresql-timescaledb",
        "environment_class": "isolated_test",
        "isolation": "session_scoped",
        "external_order_submission_enabled": False,
        "facts": {"dsn": "postgresql://should-not-be-recorded"},
        "evidence": [_evidence()],
    }
    with pytest.raises(verifier.AssuranceExecutionError, match="secret_forbidden"):
        verifier._validate_profile_admission(
            _profile_admission(services=[service]), "profile"
        )


def test_admission_evidence_is_hash_verified_and_archived_immutably(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "admission-evidence"
    source = evidence_root / "profile" / "runtime-probe.json"
    source.parent.mkdir(parents=True)
    source.write_text('{"isolated":true}\n', encoding="utf-8")
    digest = guarantees._sha256_file(source)
    raw = _profile_admission()
    raw_tool_path = "/home/qt-private/runtime/venv/bin/python"
    raw["tools"] = {
        "python": {
            "resolved_path": raw_tool_path,
            "version": "Python 3.12.3",
            "executable_sha256": "c" * 64,
        }
    }
    raw["admission_evidence"] = [_evidence(digest=digest)]
    admission = verifier._validate_profile_admission(raw, "profile")

    artifacts = verifier.resolve_admission_artifacts(admission, evidence_root)
    stage_root = tmp_path / "stage"
    proof_dir = verifier._proof_directory(
        stage_root,
        "QT-ATT-20260824T120000Z-aaaaaaaa-python-nondb",
        "QT-PROOF-001",
    )
    payload = verifier.archive_admission_payload(admission, "a" * 40)
    refs = verifier._admission_refs(stage_root, proof_dir, payload, artifacts)

    assert len(refs) == 2
    copied_ref = next(
        item
        for item in refs
        if item["path"].endswith("manual_evidence-001-profile-runtime-probe.json")
    )
    copied = stage_root.joinpath(*copied_ref["path"].split("/"))
    assert copied.read_bytes() == source.read_bytes()
    assert copied_ref["sha256"] == digest
    manifest_ref = next(
        item for item in refs if item["path"].endswith("profile-admission.json")
    )
    manifest_text = stage_root.joinpath(*manifest_ref["path"].split("/")).read_text(
        encoding="utf-8"
    )
    manifest = json.loads(manifest_text)
    assert manifest["profile"]["admission_evidence"] == [
        {
            "kind": "runtime_probe",
            "path": "profile/runtime-probe.json",
            "sha256": digest,
        }
    ]
    assert manifest["profile"]["tools"]["python"] == {
        "executable_basename": "python",
        "version": "Python 3.12.3",
        "executable_sha256": "c" * 64,
        "resolved_path_sha256": hashlib.sha256(raw_tool_path.encode()).hexdigest(),
        "resolved_path_class": "posix_absolute",
    }
    assert (
        manifest["schema_version"]
        == verifier.ARCHIVED_ADMISSION_SCHEMA_VERSION
    )
    assert raw_tool_path not in manifest_text
    assert str(evidence_root) not in manifest_text


def test_admission_evidence_rejects_traversal_and_hash_mismatch(tmp_path: Path) -> None:
    traversal = _profile_admission()
    traversal["admission_evidence"] = [_evidence(path="../escape.json")]
    with pytest.raises(
        verifier.AssuranceExecutionError, match="relative_safe_path_required"
    ):
        verifier._validate_profile_admission(traversal, "profile")

    evidence_root = tmp_path / "admission-evidence"
    source = evidence_root / "profile" / "runtime-probe.json"
    source.parent.mkdir(parents=True)
    source.write_text("observed\n", encoding="utf-8")
    mismatched = _profile_admission()
    mismatched["admission_evidence"] = [_evidence(digest="b" * 64)]
    admission = verifier._validate_profile_admission(mismatched, "profile")
    with pytest.raises(
        verifier.AssuranceExecutionError, match="admission_evidence_hash_mismatch"
    ):
        verifier.resolve_admission_artifacts(admission, evidence_root)


def test_database_admission_requires_structured_exact_facts_and_bound_dsn() -> None:
    dsn = "postgresql://qt@127.0.0.1:6543/qt_assurance_session"
    service = {
        "facts": {
            "postgresql_major": 15,
            "timescaledb_version": "2.14.2",
            "extensions": ["pgcrypto", "timescaledb"],
            "pg_dsn_sha256": hashlib.sha256(dsn.encode()).hexdigest(),
            "session_isolation_key_sha256": "b" * 64,
        }
    }
    with pytest.raises(
        verifier._PrerequisiteUnavailable, match="database_dsn_unavailable"
    ):
        verifier._validate_database_admission(service, {})

    verifier._validate_database_admission(service, {"PG_DSN": dsn})
    with pytest.raises(
        verifier._PrerequisiteUnavailable, match="database_dsn_not_admitted"

    ):
        verifier._validate_database_admission(
            service, {"PG_DSN": dsn + "_different"}
        )


def test_service_profiles_fail_closed_until_cleanup_lifecycle_is_integrated() -> None:
    profile = {
        "id": "python-db",
        "required_services": ["isolated-postgresql-timescaledb"],
    }
    admission = _profile_admission()
    admission["profile_id"] = "python-db"
    with pytest.raises(
        verifier._PrerequisiteUnavailable,
        match="service_profile_cleanup_not_integrated",
    ):
        verifier.prepare_profile(
            profile,
            admission,
            source_commit="a" * 40,
            root=ROOT,
        )


def test_profile_process_environment_does_not_inherit_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "provider-secret")
    monkeypatch.setenv("BINANCE_SECRET", "provider-secret")
    monkeypatch.setenv("PG_DSN", "postgresql://isolated")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--unexpected-host-option")
    admission = _profile_admission()

    nondb = verifier._profile_process_env(admission, include_database=False)
    assert "ALPACA_API_KEY" not in nondb
    assert "BINANCE_SECRET" not in nondb
    assert "PG_DSN" not in nondb
    assert "PYTEST_ADDOPTS" not in nondb
    assert "RUN_DB_TESTS" not in nondb
    assert "QT_DB_TEST_ISOLATED" not in nondb

    database = verifier._profile_process_env(admission, include_database=True)
    assert database["PG_DSN"] == "postgresql://isolated"
    assert database["RUN_DB_TESTS"] == "1"
    assert database["QT_DB_TEST_ISOLATED"] == "1"
    assert "ALPACA_API_KEY" not in database
    assert "BINANCE_SECRET" not in database


def test_executor_refuses_pre_pdr02_node_model_before_execution() -> None:
    catalog = {
        "proofs": [
            {
                "id": "QT-PROOF-001",
                "lifecycle": "active",
                "runner": {"kind": "node_test", "files": ["tests/example.js"]},
            }
        ]
    }
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="assurance_model_requires_pdr02_node_integration",
    ):
        verifier.require_execution_model_ready(catalog)

    catalog["proofs"][0]["runner"].update(
        {
            "event_transport": {
                "path": "scripts/assurance/node_test_reporter.mjs",
                "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            },
            "expected_test_names": ["example"],
            "expected_excluded_nonmatch_count": 0,
        }
    )
    verifier.require_execution_model_ready(catalog)


def test_pytest_plugin_emits_typed_node_ids_and_outcome_counts(tmp_path: Path) -> None:
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(
        "import pytest\n\n"
        "def test_pass():\n    assert True\n\n"
        "@pytest.mark.skip(reason='fixture')\n"
        "def test_skip():\n    assert False\n\n"
        "@pytest.mark.xfail(reason='fixture')\n"
        "def test_xfail():\n    assert False\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        item for item in (str(ROOT), env.get("PYTHONPATH", "")) if item
    )
    env["PYTEST_PLUGINS"] = "scripts.assurance.pytest_result_plugin"
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", test_file.name],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    process = verifier.ProcessResult(
        completed.stdout, completed.stderr, completed.returncode, False
    )
    selectors = [
        "test_sample.py::test_pass",
        "test_sample.py::test_skip",
        "test_sample.py::test_xfail",
    ]
    counts, reason = verifier.parse_pytest_result(
        process, {"kind": "pytest", "selectors": selectors}
    )
    assert reason is None
    assert counts == {
        "collected_count": 3,
        "passed_count": 1,
        "failed_count": 0,
        "skipped_count": 1,
        "xfailed_count": 1,
        "xpassed_count": 0,
    }
    status, status_reason = verifier._classify_attempt(process, counts, reason)
    assert (status, status_reason) == ("PARTIAL", "selected_outcome_incomplete")
    assert b"test_sample.py::test_pass" in completed.stdout


def test_attempt_result_hashes_exact_argv_counts_and_external_artifacts(
    tmp_path: Path,
) -> None:
    proof = {
        "id": "QT-PROOF-001",
        "environment_profile_id": "python-nondb",
        "runner": {
            "kind": "pytest",
            "selectors": ["tests/test_example.py::test_example"],
        },
    }
    process = verifier.ProcessResult(
        _pytest_session_stdout(
            ["tests/test_example.py::test_example"], ["passed"]
        ),
        b"",
        0,
        False,
    )
    profile = verifier.PreparedProfile(
        "python-nondb",
        {},
        {},
        {
            "schema_version": verifier.ADMISSION_SCHEMA_VERSION,
            "source_commit": "a" * 40,
            "profile": _profile_admission(),
        },
    )
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    result = verifier._attempt_result(
        stage_root=tmp_path,
        attestation_id="QT-ATT-20260824T120000Z-aaaaaaaa-python-nondb",
        proof=proof,
        profile=profile,
        root=ROOT,
        started_at=now,
        finished_at=now,
        process=process,
    )
    assert result["status"] == "PASS"
    assert result["executed_argv"] == [
        "python",
        "-m",
        "pytest",
        "tests/test_example.py::test_example",
    ]
    assert result["collected_count"] == result["passed_count"] == 1
    assert [item["path"] for item in result["evidence_refs"]] == sorted(
        item["path"] for item in result["evidence_refs"]
    )
    for ref in result["evidence_refs"]:
        artifact = tmp_path.joinpath(*ref["path"].split("/"))
        assert guarantees._sha256_file(artifact) == ref["sha256"]
    summary_path = next(
        tmp_path.joinpath(*ref["path"].split("/"))
        for ref in result["evidence_refs"]
        if ref["artifact_kind"] == "result_summary"
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert "evidence_refs" not in summary
    assert summary["stdout_sha256"] == result["stdout_sha256"]


def test_node20_plan_transport_separates_pattern_nonmatches_from_selected_skips(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    file_path = source / "tests" / "sample.test.js"
    events = [
        {
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            "sequence": 0,
            "event_type": "test:pass",
            "data": {
                "name": "selected test",
                "file": file_path.as_uri(),
                "nesting": 0,
            },
        },
        {
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            "sequence": 1,
            "event_type": "test:pass",
            "data": {
                "name": "nonmatching test",
                "file": file_path.as_uri(),
                "nesting": 0,
                "skip": "test name does not match pattern",
            },
        },
        {
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            "sequence": 2,
            "event_type": "test:plan",
            "data": {
                "count": 2,
                "nesting": 0,
            },
        },
    ]
    stdout = ("\n".join(json.dumps(item, sort_keys=True) for item in events) + "\n").encode()
    runner = {
        "kind": "node_test",
        "files": ["tests/sample.test.js"],
        "event_transport": {
            "path": "scripts/assurance/node_test_reporter.mjs",
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
        },
        "expected_test_names": ["selected test"],
        "expected_excluded_nonmatch_count": 1,
    }
    result, reason = verifier.parse_node_result(
        verifier.ProcessResult(stdout, b"", 0, False), runner, root=source
    )
    assert reason is None
    assert result["collected_count"] == result["passed_count"] == 1
    assert result["skipped_count"] == 0
    assert result["node_test_result"]["selected_test_names"] == ["selected test"]
    assert result["node_test_result"]["excluded_nonmatch_test_names"] == [
        "nonmatching test"
    ]


def test_node_transport_accounts_cancelled_todo_and_explicit_skip_separately(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    file_path = source / "tests" / "sample.test.js"
    event_rows = [
        ("test:fail", "cancelled test", {"cancelled": True}),
        ("test:pass", "explicit skip", {"skip": True}),
        ("test:pass", "todo test", {"todo": True}),
    ]
    events = [
        {
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            "sequence": sequence,
            "event_type": event_type,
            "data": {
                "name": name,
                "file": file_path.as_uri(),
                "nesting": 0,
                **extra,
            },
        }
        for sequence, (event_type, name, extra) in enumerate(event_rows)
    ]
    events.append(
        {
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
            "sequence": len(events),
            "event_type": "test:summary",
            "data": {
                "counts": {
                    "tests": 3,
                    "passed": 0,
                    "failed": 0,
                    "cancelled": 1,
                    "skipped": 1,
                    "todo": 1,
                },
                "success": False,
            },
        }
    )
    runner = {
        "kind": "node_test",
        "files": ["tests/sample.test.js"],
        "event_transport": {
            "path": "scripts/assurance/node_test_reporter.mjs",
            "schema_version": verifier.NODE_TRANSPORT_SCHEMA_VERSION,
        },
        "expected_test_names": [
            "cancelled test",
            "explicit skip",
            "todo test",
        ],
        "expected_excluded_nonmatch_count": 0,
    }
    process = verifier.ProcessResult(
        ("\n".join(json.dumps(item, sort_keys=True) for item in events) + "\n").encode(),
        b"",
        1,
        False,
    )
    result, reason = verifier.parse_node_result(process, runner, root=source)
    assert reason is None
    assert result["collected_count"] == 3
    assert result["passed_count"] == 0
    assert result["failed_count"] == 0
    assert result["skipped_count"] == 1
    assert "xfailed_count" not in result
    assert "xpassed_count" not in result
    assert result["node_test_result"]["cancelled_count"] == 1
    assert result["node_test_result"]["todo_count"] == 1
    assert result["node_test_result"]["explicitly_skipped_count"] == 1
    assert verifier._classify_attempt(process, result, reason) == ("FAIL", None)


def test_guarantee_derivation_caps_pass_by_maturity_and_proposed_proof() -> None:
    registry = {
        "guarantees": [
            {"id": "QT-GUAR-A", "proof_maturity": "partial"},
            {"id": "QT-GUAR-B", "proof_maturity": "adequate"},
            {"id": "QT-GUAR-C", "proof_maturity": "adequate"},
            {"id": "QT-GUAR-D", "proof_maturity": "adequate"},
        ]
    }
    coverage = lambda guarantee_id: [  # noqa: E731 - compact fixture builder
        {
            "guarantee_id": guarantee_id,
            "strength": "complete",
            "required_for_full_attestation": True,
        }
    ]
    catalog = {
        "proofs": [
            {"id": "QT-PROOF-001", "lifecycle": "active", "coverage": coverage("QT-GUAR-A")},
            {"id": "QT-PROOF-002", "lifecycle": "active", "coverage": coverage("QT-GUAR-B")},
            {"id": "QT-PROOF-003", "lifecycle": "active", "coverage": coverage("QT-GUAR-C")},
            {"id": "QT-PROOF-004", "lifecycle": "proposed", "coverage": coverage("QT-GUAR-C")},
            {
                "id": "QT-PROOF-005",
                "lifecycle": "active",
                "coverage": [
                    {
                        "guarantee_id": "QT-GUAR-D",
                        "strength": "partial",
                        "required_for_full_attestation": True,
                    }
                ],
            },
        ]
    }
    proof_results = [
        {"proof_id": "QT-PROOF-001", "status": "PASS"},
        {"proof_id": "QT-PROOF-002", "status": "PASS"},
        {"proof_id": "QT-PROOF-003", "status": "PASS"},
        {"proof_id": "QT-PROOF-005", "status": "PASS"},
    ]
    assert verifier.derive_guarantee_results(registry, catalog, proof_results) == [
        {"guarantee_id": "QT-GUAR-A", "status": "PARTIAL", "proof_ids": ["QT-PROOF-001"]},
        {"guarantee_id": "QT-GUAR-B", "status": "PASS", "proof_ids": ["QT-PROOF-002"]},
        {"guarantee_id": "QT-GUAR-C", "status": "PARTIAL", "proof_ids": ["QT-PROOF-003"]},
        {"guarantee_id": "QT-GUAR-D", "status": "PARTIAL", "proof_ids": ["QT-PROOF-005"]},
    ]

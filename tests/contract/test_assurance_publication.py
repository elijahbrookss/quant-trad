from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.assurance import verify_guarantees as verifier
from tests.contract import test_guarantee_registry as registry_fixture


SOURCE_COMMIT = "a" * 40
PROFILES = ("frontend-node", "python-db-isolated", "python-nondb")


def _stage(
    root: Path,
    *,
    profile_id: str,
    sequence: int,
    extra: bool = False,
    source_commit: str = SOURCE_COMMIT,
) -> tuple[Path, Path]:
    attestation_id = (
        f"QT-ATT-20260825T1200{sequence:02d}Z-{source_commit[:12]}-{profile_id}"
    )
    proof_relative = (
        f"docs/assurance/guarantees/evidence/{attestation_id}/"
        "QT-PROOF-001/stdout-output.txt"
    )
    environment_relative = (
        f"docs/assurance/guarantees/evidence/{attestation_id}/"
        f"_environments/{profile_id}/runtime_probe-001-profile.json"
    )
    proof_bytes = f"proof:{profile_id}\n".encode()
    environment_bytes = f'{{"profile":"{profile_id}"}}\n'.encode()
    proof_path = root / proof_relative
    environment_path = root / environment_relative
    proof_path.parent.mkdir(parents=True)
    environment_path.parent.mkdir(parents=True)
    proof_path.write_bytes(proof_bytes)
    environment_path.write_bytes(environment_bytes)
    if extra:
        (proof_path.parent / "unreferenced.txt").write_text(
            "must not publish\n", encoding="utf-8"
        )
    attestation = {
        "schema_version": "qt.guarantee_attestation.v1",
        "attestation_id": attestation_id,
        "source": {"git_commit": source_commit, "clean": True},
        "proof_results": [
            {
                "proof_id": "QT-PROOF-001",
                "evidence_refs": [
                    {
                        "artifact_kind": "stdout",
                        "path": proof_relative,
                        "sha256": verifier._sha256_bytes(proof_bytes),
                    }
                ],
            }
        ],
        "environments": [
            {
                "profile_id": profile_id,
                "profile_admission": {
                    "evidence_refs": [
                        {
                            "artifact_kind": "runtime_probe",
                            "path": environment_relative,
                            "sha256": verifier._sha256_bytes(environment_bytes),
                        }
                    ]
                },
                "services": {},
            }
        ],
    }
    attestation_path = (
        root
        / "docs"
        / "assurance"
        / "guarantees"
        / "attestations"
        / source_commit
        / f"{attestation_id}.json"
    )
    attestation_path.parent.mkdir(parents=True)
    attestation_path.write_text(json.dumps(attestation) + "\n", encoding="utf-8")
    return attestation_path, root


def _install_fakes(monkeypatch: pytest.MonkeyPatch, destination: Path) -> None:
    catalog = {
        "environment_profiles": [
            {"id": profile_id, "execution_class": "isolated_container"}
            for profile_id in PROFILES
        ]
    }
    monkeypatch.setattr(
        verifier.guarantees,
        "validate_repository",
        lambda root: SimpleNamespace(proof_catalog=catalog),
    )
    monkeypatch.setattr(
        verifier,
        "validate_staged",
        lambda root,
        attestation_path,
        evidence_root,
        publication_allowed_untracked_paths=None: json.loads(
            attestation_path.read_text(encoding="utf-8")
        ),
    )
    monkeypatch.setattr(
        verifier,
        "_publication_head_is_source",
        lambda root, source_commit: None,
    )
    monkeypatch.setattr(
        verifier,
        "_publication_lock_path",
        lambda root: destination.parent / f".{destination.name}-publication.lock",
    )
    monkeypatch.setattr(
        verifier,
        "_assert_publication_scratch_untracked",
        lambda root, source_commit, relative: None,
    )

    def status(root: Path) -> list[tuple[str, str]]:
        return [
            ("??", path.relative_to(root).as_posix())
            for path in sorted(root.rglob("*"))
            if path.is_file()
        ]

    monkeypatch.setattr(verifier, "_publication_git_status", status)
    destination.mkdir()


def _attestation_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_attestation_payload(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _planned_destination_paths(pairs: list[tuple[Path, Path]]) -> set[str]:
    result: set[str] = set()
    for attestation_path, evidence_root in pairs:
        payload = _attestation_payload(attestation_path)
        result.add(attestation_path.relative_to(evidence_root).as_posix())
        result.update(
            ref["path"]
            for proof in payload["proof_results"]
            for ref in proof["evidence_refs"]
        )
        result.update(
            ref["path"]
            for environment in payload["environments"]
            for ref in environment["profile_admission"]["evidence_refs"]
        )
    return result


def _full_validator_publication_fixture(
    tmp_path: Path,
) -> tuple[Path, str, list[tuple[Path, Path]]]:
    """Create a small real Git source with three fully valid attestations."""

    destination = tmp_path / "full-validator-destination"
    destination.mkdir()
    registry, catalog = registry_fixture._base_repository(destination)
    profiles: list[dict] = []
    proofs: list[dict] = []
    for index, profile_id in enumerate(PROFILES, start=1):
        runtime_relative = f"docker/assurance/{profile_id}.profile.json"
        registry_fixture._write(
            destination,
            runtime_relative,
            '{"schema_version":"qt.assurance_environment_profile.v1"}\n',
        )
        profile = copy.deepcopy(catalog["environment_profiles"][0])
        profile.update(
            id=profile_id,
            execution_class="isolated_container",
            runtime_definition=runtime_relative,
        )
        profiles.append(profile)
        proof = copy.deepcopy(catalog["proofs"][0])
        proof.update(
            id=f"QT-PROOF-{index:03d}",
            title=f"Fixture proof for {profile_id}",
            environment_profile_id=profile_id,
        )
        proofs.append(proof)
    catalog["environment_profiles"] = profiles
    catalog["proofs"] = proofs
    registry_fixture._write(destination, "scripts/docs/guarantees.py", "# validator v1\n")
    shutil.copytree(
        verifier.guarantees.SCHEMA_DIR,
        destination / "docs/assurance/guarantees/schemas",
        dirs_exist_ok=True,
    )
    registry_path = registry_fixture._write(
        destination,
        "docs/assurance/guarantees/registry.json",
        json.dumps(registry, indent=2, sort_keys=True) + "\n",
    )
    catalog_path = registry_fixture._write(
        destination,
        "docs/assurance/guarantees/proof-catalog.json",
        json.dumps(catalog, indent=2, sort_keys=True) + "\n",
    )

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(destination), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "--quiet")
    git("config", "user.email", "qt@example.invalid")
    git("config", "user.name", "QT publication contract")
    git("add", ".")
    git("commit", "--quiet", "-m", "audit baseline")
    baseline_commit = git("rev-parse", "HEAD")
    registry["audit_baseline_commit"] = baseline_commit
    registry_fixture._write(
        destination,
        "docs/plans/documentation-reconciliation/implementation-surface-inventory.json",
        json.dumps({"baseline_commit": baseline_commit}) + "\n",
    )
    registry_path.write_text(
        json.dumps(registry, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    git("add", ".")
    git("commit", "--quiet", "-m", "source S")
    source_commit = git("rev-parse", "HEAD")
    bundle = registry_fixture._validate(destination, registry, catalog)
    source_snapshot_sha256 = verifier.guarantees.source_snapshot_sha256(
        destination, source_commit
    )

    def environment_evidence(
        stage_root: Path,
        *,
        attestation_id: str,
        profile_id: str,
        artifact_kind: str,
        facts: dict,
    ) -> dict[str, str]:
        payload = {
            "schema_version": "qt.assurance_environment_evidence.v1",
            "profile_id": profile_id,
            "artifact_kind": artifact_kind,
            "facts": facts,
        }
        relative = (
            "docs/assurance/guarantees/evidence/"
            f"{attestation_id}/_environments/{profile_id}/"
            f"{artifact_kind}-profile.json"
        )
        path = registry_fixture._write(
            stage_root,
            relative,
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
        )
        return {
            "artifact_kind": artifact_kind,
            "path": relative,
            "sha256": verifier.guarantees._sha256_file(path),
        }

    pairs: list[tuple[Path, Path]] = []
    proof_ids = [proof["id"] for proof in proofs]
    for sequence, profile in enumerate(profiles, start=1):
        profile_id = profile["id"]
        stage_root = tmp_path / f"full-validator-stage-{sequence}"
        timestamp = f"20260825T1300{sequence:02d}Z"
        attestation_id = f"QT-ATT-{timestamp}-{source_commit[:12]}-{profile_id}"
        container_identity = f"container-{profile_id}"
        image_digest = "sha256:" + (str(sequence) * 64)
        admission_facts = {
            "base_image_digests": [],
            "cleanup_completed": True,
            "container_identity": container_identity,
            "docker_version": "28.1.1",
            "image_digest": image_digest,
            "network_mode": "none",
            "platform": "linux/amd64",
            "source_commit": source_commit,
            "source_mount_mode": "read_only",
            "writable_temp_outside_source": True,
        }
        evidence_facts = {
            "bootstrap_log": {
                "bootstrap_completed": True,
                "container_identity": container_identity,
            },
            "cleanup_log": {
                "cleanup_completed": True,
                "container_identity": container_identity,
            },
            "container_identity": {"container_identity": container_identity},
            "image_digest": {"image_digest": image_digest},
            "network_mode": {"network_mode": "none"},
            "runtime_probe": {
                "docker_version": "28.1.1",
                "platform": "linux/amd64",
                "source_commit": source_commit,
            },
            "source_mount": {"source_mount_mode": "read_only"},
        }
        evidence_refs = sorted(
            (
                environment_evidence(
                    stage_root,
                    attestation_id=attestation_id,
                    profile_id=profile_id,
                    artifact_kind=kind,
                    facts=facts,
                )
                for kind, facts in evidence_facts.items()
            ),
            key=lambda item: item["path"],
        )
        runtime_relative = profile["runtime_definition"]
        environment = {
            "profile_id": profile_id,
            "os": "Linux",
            "architecture": "x86_64",
            "tool_versions": {"python": "3.12.4"},
            "lockfile_hashes": {
                "requirements.lock": verifier.guarantees._bound_material_sha256(
                    destination, "requirements.lock", git_commit=source_commit
                )
            },
            "profile_admission": {
                "admission_id": f"fixture-{profile_id}",
                "environment_class": "isolated_test",
                "isolation": "disposable",
                "external_order_submission_enabled": False,
                "runtime_definition": {
                    "path": runtime_relative,
                    "sha256": verifier.guarantees._bound_material_sha256(
                        destination, runtime_relative, git_commit=source_commit
                    ),
                },
                "facts": admission_facts,
                "evidence_refs": evidence_refs,
            },
            "services": {},
        }
        proof_results = [
            {
                "proof_id": proof["id"],
                "environment_profile_id": proof["environment_profile_id"],
                "status": "NOT_RUN",
                "reason_code": "profile_not_selected",
                "evidence_refs": [],
            }
            for proof in proofs
        ]
        attestation = {
            "schema_version": verifier.guarantees.ATTESTATION_SCHEMA_VERSION,
            "attestation_id": attestation_id,
            "source": {
                "git_commit": source_commit,
                "clean": True,
                "assurance_material_sha256": (
                    verifier.guarantees.assurance_material_sha256(
                        bundle, git_commit=source_commit
                    )
                ),
            },
            "inputs": registry_fixture._attestation_inputs(
                bundle, catalog_path, git_commit=source_commit
            ),
            "environments": [environment],
            "started_at": (
                f"2026-08-25T13:00:{sequence:02d}Z"
            ),
            "finished_at": (
                f"2026-08-25T13:01:{sequence:02d}Z"
            ),
            "proof_results": proof_results,
            "guarantee_results": [
                {
                    "guarantee_id": "QT-GUAR-KNOWN-AT",
                    "status": "NOT_RUN",
                    "proof_ids": proof_ids,
                }
            ],
        }
        # This pre-publication check proves the fixture itself exercises the
        # full historical validator, including source snapshot material.
        attestation_path = registry_fixture._write(
            stage_root,
            "docs/assurance/guarantees/attestations/"
            f"{source_commit}/{attestation_id}.json",
            json.dumps(attestation, indent=2, sort_keys=True) + "\n",
        )
        validated = verifier.validate_staged(
            root=destination,
            attestation_path=attestation_path,
            evidence_root=stage_root,
        )
        assert validated["attestation_id"] == attestation_id
        assert source_snapshot_sha256 == verifier.guarantees.source_snapshot_sha256(
            destination, source_commit
        )
        pairs.append((attestation_path, stage_root))
    assert git("status", "--porcelain", "--untracked-files=all") == ""
    return destination, source_commit, pairs


def test_publish_staged_validates_all_then_publishes_evidence_before_attestations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    receipt = tmp_path / "publication-receipt.json"
    result = verifier.publish_staged(
        root=destination,
        source_commit=SOURCE_COMMIT,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    assert result == receipt
    record = json.loads(receipt.read_text(encoding="utf-8"))
    assert record["publication_state"] == "verified"
    assert record["multi_file_atomicity"] is False
    assert record["crash_resume_supported"] is True
    assert record["evidence_published_before_attestations"] is True
    assert record["published_file_count"] == 9
    assert len(list((destination / "docs").rglob("*.json"))) == 6
    assert {
        path.relative_to(destination).as_posix()
        for path in (destination / "docs").rglob("*")
        if path.is_file()
    } == _planned_destination_paths(pairs)

    # The same batch is a create-only, byte-identical resume, not an overwrite.
    before = receipt.read_bytes()
    verifier.publish_staged(
        root=destination,
        source_commit=SOURCE_COMMIT,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    assert receipt.read_bytes() == before


def test_publish_staged_rejects_unreferenced_session_file_before_destination_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(
            tmp_path / f"stage-{index}",
            profile_id=profile,
            sequence=index,
            extra=index == 3,
        )
        for index, profile in enumerate(PROFILES, start=1)
    ]
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="session_inventory_not_exact",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert not (destination / "docs").exists()


def test_publish_staged_third_validation_failure_precedes_any_destination_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    calls = 0

    def fail_third(
        root: Path,
        attestation_path: Path,
        evidence_root: Path,
        publication_allowed_untracked_paths: frozenset[str] | None = None,
    ) -> dict:
        nonlocal calls
        del root, evidence_root, publication_allowed_untracked_paths
        calls += 1
        if calls == 3:
            raise verifier.AssuranceExecutionError("third-invalid")
        return json.loads(attestation_path.read_text(encoding="utf-8"))

    monkeypatch.setattr(verifier, "validate_staged", fail_third)
    with pytest.raises(verifier.AssuranceExecutionError, match="third-invalid"):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert calls == 3
    assert not (destination / "docs").exists()
    assert not (
        destination.parent / f".{destination.name}-publication.lock"
    ).exists()


def test_publish_staged_refuses_one_byte_destination_conflict_without_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    payload = _attestation_payload(pairs[0][0])
    relative = payload["proof_results"][0]["evidence_refs"][0]["path"]
    conflict = destination / relative
    conflict.parent.mkdir(parents=True)
    conflict.write_bytes(b"proof:frontend-node\r")
    before = conflict.read_bytes()

    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_destination_differs",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert conflict.read_bytes() == before
    assert {
        path.relative_to(destination).as_posix()
        for path in (destination / "docs").rglob("*")
        if path.is_file()
    } == {relative}


def test_publish_staged_refuses_moved_head_before_destination_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]

    def moved(root: Path, source_commit: str) -> None:
        del root, source_commit
        raise verifier.AssuranceExecutionError(
            "publication_source_commit_must_equal_head"
        )

    monkeypatch.setattr(verifier, "_publication_head_is_source", moved)
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_source_commit_must_equal_head",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert not (destination / "docs").exists()


def test_publish_staged_refuses_unrelated_dirty_destination_before_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    monkeypatch.setattr(
        verifier,
        "_publication_git_status",
        lambda root: [("??", "unrelated.txt")],
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="destination_not_clean_and_no_matching_pending_batch",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert not (destination / "docs").exists()


@pytest.mark.parametrize("variant", ["mixed-source", "duplicate-profile"])
def test_publish_staged_refuses_mixed_source_or_duplicate_profile_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    profiles = list(PROFILES)
    if variant == "duplicate-profile":
        profiles[-1] = profiles[-2]
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(profiles, start=1)
    ]
    if variant == "mixed-source":
        payload = _attestation_payload(pairs[-1][0])
        payload["source"]["git_commit"] = "b" * 40
        _write_attestation_payload(pairs[-1][0], payload)
        expected = "publication_attestation_source_mismatch"
    else:
        expected = "publication_profile_set_mismatch"

    with pytest.raises(verifier.AssuranceExecutionError, match=expected):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert not (destination / "docs").exists()


@pytest.mark.parametrize(
    ("variant", "expected"),
    [
        ("symlink", "publication_staged_symlink_forbidden"),
        ("special", "publication_staged_special_file_forbidden"),
        ("traversal", "safe_relative_path_required"),
        ("casefold", "publication_staged_casefold_collision"),
        ("unicode-normalization", "publication_staged_casefold_collision"),
    ],
)
def test_publish_staged_refuses_unsafe_staged_paths_before_destination_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
    expected: str,
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    payload = _attestation_payload(pairs[0][0])
    relative = payload["proof_results"][0]["evidence_refs"][0]["path"]
    proof_path = pairs[0][1] / relative
    if variant == "symlink":
        target = proof_path.with_name("outside.txt")
        target.write_bytes(proof_path.read_bytes())
        proof_path.unlink()
        proof_path.symlink_to(target)
    elif variant == "special":
        os.mkfifo(proof_path.with_name("unexpected.fifo"))
    elif variant == "traversal":
        payload["proof_results"][0]["evidence_refs"][0]["path"] = (
            relative.rsplit("/", 1)[0] + "/../escape.txt"
        )
        _write_attestation_payload(pairs[0][0], payload)
    elif variant == "casefold":
        proof_path.with_name(proof_path.name.swapcase()).write_bytes(
            proof_path.read_bytes()
        )
    else:
        proof_path.with_name("caf\u00e9.txt").write_bytes(b"composed\n")
        proof_path.with_name("cafe\u0301.txt").write_bytes(b"decomposed\n")

    with pytest.raises(verifier.AssuranceExecutionError, match=expected):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=tmp_path / "publication-receipt.json",
        )
    assert not (destination / "docs").exists()


def test_publish_staged_uses_immutable_snapshot_if_source_mutates_after_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    payload = _attestation_payload(pairs[0][0])
    relative = payload["proof_results"][0]["evidence_refs"][0]["path"]
    original = pairs[0][1] / relative
    original_bytes = original.read_bytes()
    calls = 0

    def mutate_after_snapshots_begin(
        root: Path,
        attestation_path: Path,
        evidence_root: Path,
        publication_allowed_untracked_paths: frozenset[str] | None = None,
    ) -> dict:
        nonlocal calls
        del root, evidence_root, publication_allowed_untracked_paths
        calls += 1
        if calls == 3:
            original.write_bytes(b"mutated after immutable snapshot\n")
        return json.loads(attestation_path.read_text(encoding="utf-8"))

    monkeypatch.setattr(verifier, "validate_staged", mutate_after_snapshots_begin)
    verifier.publish_staged(
        root=destination,
        source_commit=SOURCE_COMMIT,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=tmp_path / "publication-receipt.json",
    )
    assert original.read_bytes() != original_bytes
    assert (destination / relative).read_bytes() == original_bytes


def test_publish_staged_resumes_after_interruption_between_evidence_and_attestations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    receipt = tmp_path / "publication-receipt.json"
    original_write = verifier._write_publication_destination
    interrupted = False

    def interrupt_before_first_attestation(
        *, root: Path, destination: Path, scratch: Path, content: bytes
    ) -> bool:
        nonlocal interrupted
        if (
            not interrupted
            and "/attestations/" in destination.as_posix()
        ):
            interrupted = True
            raise RuntimeError("injected publication interruption")
        return original_write(
            root=root,
            destination=destination,
            scratch=scratch,
            content=content,
        )

    monkeypatch.setattr(
        verifier,
        "_write_publication_destination",
        interrupt_before_first_attestation,
    )
    with pytest.raises(RuntimeError, match="injected publication interruption"):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert receipt.with_name(receipt.name + ".pending").is_file()
    assert not list(
        (destination / "docs" / "assurance" / "guarantees" / "attestations").rglob(
            "*.json"
        )
    )
    assert len(
        [
            path
            for path in (destination / "docs").rglob("*")
            if path.is_file()
        ]
    ) == 6

    monkeypatch.setattr(verifier, "_write_publication_destination", original_write)
    verifier.publish_staged(
        root=destination,
        source_commit=SOURCE_COMMIT,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    assert receipt.is_file()
    assert {
        path.relative_to(destination).as_posix()
        for path in (destination / "docs").rglob("*")
        if path.is_file()
    } == _planned_destination_paths(pairs)


def test_publish_staged_refuses_unrelated_final_git_dirt_and_writes_no_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    receipt = tmp_path / "publication-receipt.json"
    calls = 0

    def status(root: Path) -> list[tuple[str, str]]:
        nonlocal calls
        calls += 1
        if calls <= 2:
            return []
        rows = [
            ("??", path.relative_to(root).as_posix())
            for path in sorted((root / "docs").rglob("*"))
            if path.is_file()
        ]
        return [*rows, ("??", "unrelated.txt")]

    monkeypatch.setattr(verifier, "_publication_git_status", status)
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_final_worktree_contains_unrelated_change",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert calls == 3
    assert not receipt.exists()
    assert receipt.with_name(receipt.name + ".pending").is_file()


def test_publish_staged_resumes_from_partial_deterministic_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    receipt = tmp_path / "publication-receipt.json"
    original_write = verifier._write_publication_destination
    crashed = False
    stranded_scratch: Path | None = None

    def crash_mid_scratch(
        *, root: Path, destination: Path, scratch: Path, content: bytes
    ) -> bool:
        nonlocal crashed, stranded_scratch
        if not crashed:
            crashed = True
            stranded_scratch = scratch
            scratch.parent.mkdir(parents=True)
            scratch.write_bytes(content[:1])
            raise RuntimeError("injected mid-scratch crash")
        return original_write(
            root=root,
            destination=destination,
            scratch=scratch,
            content=content,
        )

    monkeypatch.setattr(
        verifier, "_write_publication_destination", crash_mid_scratch
    )
    with pytest.raises(RuntimeError, match="injected mid-scratch crash"):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert stranded_scratch is not None and stranded_scratch.is_file()
    assert not (destination / "docs").exists()
    assert receipt.with_name(receipt.name + ".pending").is_file()

    monkeypatch.setattr(verifier, "_write_publication_destination", original_write)
    verifier.publish_staged(
        root=destination,
        source_commit=SOURCE_COMMIT,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    assert receipt.is_file()
    assert not (destination / ".qt-assurance-publication").exists()
    assert {
        path.relative_to(destination).as_posix()
        for path in (destination / "docs").rglob("*")
        if path.is_file()
    } == _planned_destination_paths(pairs)


def test_publish_staged_real_git_resume_authenticates_exact_dirt_before_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    destination.mkdir()
    subprocess.run(["git", "init", "--quiet", str(destination)], check=True)
    subprocess.run(
        ["git", "-C", str(destination), "config", "user.email", "qt@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(destination), "config", "user.name", "QT contract"],
        check=True,
    )
    (destination / "source.txt").write_text("source S\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(destination), "add", "source.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(destination), "commit", "--quiet", "-m", "source S"],
        check=True,
    )
    source_commit = subprocess.run(
        ["git", "-C", str(destination), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    pairs = [
        _stage(
            tmp_path / f"stage-real-{index}",
            profile_id=profile,
            sequence=index,
            source_commit=source_commit,
        )
        for index, profile in enumerate(PROFILES, start=1)
    ]
    catalog = {
        "environment_profiles": [
            {"id": profile_id, "execution_class": "isolated_container"}
            for profile_id in PROFILES
        ]
    }
    monkeypatch.setattr(
        verifier.guarantees,
        "validate_repository",
        lambda root: SimpleNamespace(proof_catalog=catalog),
    )

    validation_calls = 0

    def dirty_sensitive_validation(
        root: Path,
        attestation_path: Path,
        evidence_root: Path,
        publication_allowed_untracked_paths: frozenset[str] | None = None,
    ) -> dict:
        nonlocal validation_calls
        del evidence_root
        payload = json.loads(attestation_path.read_text(encoding="utf-8"))
        verifier.guarantees._verify_local_git_source(
            root,
            payload["source"]["git_commit"],
            payload["source"]["clean"],
            publication_allowed_untracked_paths=publication_allowed_untracked_paths,
        )
        validation_calls += 1
        return payload

    monkeypatch.setattr(verifier, "validate_staged", dirty_sensitive_validation)
    receipt = tmp_path / "real-git-publication-receipt.json"
    original_write = verifier._write_publication_destination
    crashed = False
    stranded_scratch: Path | None = None

    def crash_with_evidence_and_attestation_scratch(
        *, root: Path, destination: Path, scratch: Path, content: bytes
    ) -> bool:
        nonlocal crashed, stranded_scratch
        if not crashed and "/attestations/" in destination.as_posix():
            crashed = True
            stranded_scratch = scratch
            scratch.parent.mkdir(parents=True, exist_ok=True)
            scratch.write_bytes(content[:1])
            raise RuntimeError("injected real-git publication interruption")
        return original_write(
            root=root,
            destination=destination,
            scratch=scratch,
            content=content,
        )

    monkeypatch.setattr(
        verifier,
        "_write_publication_destination",
        crash_with_evidence_and_attestation_scratch,
    )
    with pytest.raises(RuntimeError, match="injected real-git"):
        verifier.publish_staged(
            root=destination,
            source_commit=source_commit,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert validation_calls == 3
    assert stranded_scratch is not None and stranded_scratch.is_file()
    assert receipt.with_name(receipt.name + ".pending").is_file()

    extra_attestation_id = (
        f"QT-ATT-20260825T125959Z-{source_commit[:12]}-python-nondb"
    )
    extra_evidence = (
        destination
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / extra_attestation_id
        / "extra.txt"
    )
    extra_evidence.parent.mkdir(parents=True)
    extra_evidence.write_text("unplanned\n", encoding="utf-8")
    extra_scratch = (
        destination
        / ".qt-assurance-publication"
        / ("f" * 64)
        / (("e" * 64) + ".pending")
    )
    extra_scratch.parent.mkdir(parents=True)
    extra_scratch.write_text("unplanned\n", encoding="utf-8")
    monkeypatch.setattr(verifier, "_write_publication_destination", original_write)
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_resume_has_unrelated_worktree_change",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=source_commit,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert validation_calls == 3
    assert extra_evidence.is_file() and extra_scratch.is_file()
    extra_evidence.unlink()
    extra_evidence.parent.rmdir()
    extra_scratch.unlink()
    extra_scratch.parent.rmdir()

    verifier.publish_staged(
        root=destination,
        source_commit=source_commit,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    assert validation_calls == 6
    assert receipt.is_file()
    assert not (destination / ".qt-assurance-publication").exists()
    assert {
        path.relative_to(destination).as_posix()
        for path in (destination / "docs").rglob("*")
        if path.is_file()
    } == _planned_destination_paths(pairs)


def test_publish_staged_full_validator_resumes_only_exact_pending_bound_dirt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination, source_commit, pairs = _full_validator_publication_fixture(tmp_path)
    receipt = tmp_path / "full-validator-publication-receipt.json"
    original_write = verifier._write_publication_destination
    crashed = False
    stranded_scratch: Path | None = None

    def crash_after_evidence_with_attestation_scratch(
        *, root: Path, destination: Path, scratch: Path, content: bytes
    ) -> bool:
        nonlocal crashed, stranded_scratch
        if not crashed and "/attestations/" in destination.as_posix():
            crashed = True
            stranded_scratch = scratch
            scratch.parent.mkdir(parents=True, exist_ok=True)
            scratch.write_bytes(content[:1])
            raise RuntimeError("injected full-validator interruption")
        return original_write(
            root=root,
            destination=destination,
            scratch=scratch,
            content=content,
        )

    monkeypatch.setattr(
        verifier,
        "_write_publication_destination",
        crash_after_evidence_with_attestation_scratch,
    )
    with pytest.raises(RuntimeError, match="full-validator interruption"):
        verifier.publish_staged(
            root=destination,
            source_commit=source_commit,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert stranded_scratch is not None and stranded_scratch.is_file()
    pending = receipt.with_name(receipt.name + ".pending")
    assert pending.is_file()

    extra_attestation_id = (
        f"QT-ATT-20260825T135959Z-{source_commit[:12]}-python-nondb"
    )
    extra_evidence = (
        destination
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / extra_attestation_id
        / "extra.txt"
    )
    extra_evidence.parent.mkdir(parents=True)
    extra_evidence.write_text("unplanned\n", encoding="utf-8")
    extra_scratch = (
        destination
        / ".qt-assurance-publication"
        / ("f" * 64)
        / (("e" * 64) + ".pending")
    )
    extra_scratch.parent.mkdir(parents=True)
    extra_scratch.write_text("unplanned\n", encoding="utf-8")
    before_rejected_resume = {
        path.relative_to(destination).as_posix(): path.read_bytes()
        for path in destination.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(destination).parts
    }
    monkeypatch.setattr(verifier, "_write_publication_destination", original_write)
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_resume_has_unrelated_worktree_change",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=source_commit,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    after_rejected_resume = {
        path.relative_to(destination).as_posix(): path.read_bytes()
        for path in destination.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(destination).parts
    }
    assert after_rejected_resume == before_rejected_resume
    assert not receipt.exists()
    extra_evidence.unlink()
    extra_evidence.parent.rmdir()
    extra_scratch.unlink()
    extra_scratch.parent.rmdir()

    verifier.publish_staged(
        root=destination,
        source_commit=source_commit,
        attestation_paths=[item[0] for item in pairs],
        evidence_roots=[item[1] for item in pairs],
        receipt_path=receipt,
    )
    planned = _planned_destination_paths(pairs)
    published = {
        path.relative_to(destination).as_posix()
        for path in destination.rglob("*")
        if path.is_file()
        and (
            "docs/assurance/guarantees/evidence/"
            in path.relative_to(destination).as_posix()
            or path.relative_to(destination).as_posix().startswith(
                f"docs/assurance/guarantees/attestations/{source_commit}/"
            )
        )
    }
    assert published == planned
    assert receipt.is_file()
    assert not (destination / ".qt-assurance-publication").exists()


def test_publication_validator_allowlist_is_closed_to_publisher_namespaces(
    tmp_path: Path,
) -> None:
    (tmp_path / ".git").mkdir()
    with pytest.raises(
        verifier.guarantees.GuaranteeValidationError,
        match="publication_allowlist_path_invalid",
    ):
        verifier.guarantees._verify_local_git_source(
            tmp_path,
            SOURCE_COMMIT,
            True,
            publication_allowed_untracked_paths=frozenset({"pyproject.toml"}),
        )


def test_publish_staged_preflights_conflicting_receipt_before_repository_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    receipt = tmp_path / "publication-receipt.json"
    receipt.write_bytes(b"{\"conflict\":true}\n")
    before = receipt.read_bytes()
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_receipt_batch_mismatch",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=[item[1] for item in pairs],
            receipt_path=receipt,
        )
    assert receipt.read_bytes() == before
    assert not (destination / "docs").exists()
    assert not receipt.with_name(receipt.name + ".pending").exists()


def test_publication_root_lock_is_nonblocking_across_receipt_choices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = tmp_path / "git-metadata-publication.lock"
    monkeypatch.setattr(verifier, "_publication_lock_path", lambda root: shared)
    first = verifier._PublicationRootLock(tmp_path / "destination")
    second = verifier._PublicationRootLock(tmp_path / "destination")
    first.acquire()
    try:
        with pytest.raises(
            verifier.AssuranceExecutionError,
            match="publication_root_lock_busy",
        ):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_publish_staged_rejects_symlinked_input_root_and_c0_path_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "destination"
    _install_fakes(monkeypatch, destination)
    pairs = [
        _stage(tmp_path / f"stage-{index}", profile_id=profile, sequence=index)
        for index, profile in enumerate(PROFILES, start=1)
    ]
    linked_root = tmp_path / "linked-stage"
    linked_root.symlink_to(pairs[0][1], target_is_directory=True)
    evidence_roots = [linked_root, pairs[1][1], pairs[2][1]]
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="path_symlink_forbidden",
    ):
        verifier.publish_staged(
            root=destination,
            source_commit=SOURCE_COMMIT,
            attestation_paths=[item[0] for item in pairs],
            evidence_roots=evidence_roots,
            receipt_path=tmp_path / "publication-receipt.json",
        )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="nonportable_path_component",
    ):
        verifier._portable_component("control-\x01-name", "test.path")


def test_publication_scratch_namespace_must_be_absent_and_unignored_at_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        verifier,
        "_git",
        lambda root, *args: (
            ".qt-assurance-publication"
            if args[:3] == ("ls-tree", "--name-only", SOURCE_COMMIT)
            else ""
        ),
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_scratch_tracked_at_source",
    ):
        verifier._assert_publication_scratch_untracked(
            tmp_path, SOURCE_COMMIT, ".qt-assurance-publication"
        )

    monkeypatch.setattr(verifier, "_git", lambda root, *args: "")
    monkeypatch.setattr(
        verifier.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0),
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="publication_scratch_path_ignored",
    ):
        verifier._assert_publication_scratch_untracked(
            tmp_path, SOURCE_COMMIT, ".qt-assurance-publication"
        )

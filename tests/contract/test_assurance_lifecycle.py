from __future__ import annotations

import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.assurance import docker_lifecycle
from scripts.assurance import process_guard
from scripts.assurance import verify_guarantees as verifier


def _controller(
    tmp_path: Path,
    command_runner: docker_lifecycle.CommandRunner,
) -> docker_lifecycle.DockerController:
    docker = tmp_path / "docker"
    docker.write_bytes(b"fake docker executable\n")
    docker.chmod(0o700)
    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o700)
    source = private / "source"
    admission = {
        "docker_tool": {"resolved_path": str(docker)},
        "runner_image": {"image_id": "sha256:" + "1" * 64},
        "service_images": {},
    }
    return docker_lifecycle.DockerController(
        admission=admission,
        root=source,
        private_root=private,
        source_commit="a" * 40,
        attestation_id="QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb",
        profile_id="python-nondb",
        environment_instance_id="qt-12345678",
        command_runner=command_runner,
    )


def _runner_image_controller(tmp_path: Path) -> tuple[
    docker_lifecycle.DockerController,
    dict[str, dict[str, object]],
]:
    docker = tmp_path / "docker"
    docker.write_bytes(b"fake docker executable\n")
    docker.chmod(0o700)
    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o700)
    runner_id = "sha256:" + "1" * 64
    base_rows = [
        {
            "reference": "docker.io/library/node@sha256:" + "2" * 64,
            "digest": "sha256:" + "2" * 64,
            "image_id": "sha256:" + "3" * 64,
        },
        {
            "reference": "docker.io/library/python@sha256:" + "4" * 64,
            "digest": "sha256:" + "4" * 64,
            "image_id": "sha256:" + "5" * 64,
        },
    ]
    labels = {
        docker_lifecycle.BUILD_SOURCE_LABEL: "a" * 40,
        docker_lifecycle.BUILD_SOURCE_TREE_LABEL: "b" * 40,
        docker_lifecycle.BUILD_PROFILE_LABEL: "c" * 64,
        docker_lifecycle.BUILD_DEFINITION_LABEL: "d" * 64,
        docker_lifecycle.WHEEL_MANIFEST_LABEL: "e" * 64,
        docker_lifecycle.WHEEL_ARTIFACT_LABEL: "f" * 64,
        docker_lifecycle.BUILD_CONTEXT_LABEL: "0" * 64,
    }
    admission = {
        "docker_tool": {"resolved_path": str(docker)},
        "runner_image": {
            "image_id": runner_id,
            "platform": "linux/amd64",
            "build_definition": {
                "path": "docker/assurance/frontend-node.Dockerfile",
                "sha256": labels[docker_lifecycle.BUILD_DEFINITION_LABEL],
            },
        },
        "runner_build_record": {
            "validated_record": {
                "output_image": {
                    "image_id": runner_id,
                    "platform": "linux/amd64",
                    "labels": labels,
                },
                "base_images": base_rows,
            }
        },
        "service_images": {},
    }
    controller = docker_lifecycle.DockerController(
        admission=admission,
        root=tmp_path / "source",
        private_root=private,
        source_commit="a" * 40,
        attestation_id="QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb",
        profile_id="python-nondb",
        environment_instance_id="qt-12345678",
        command_runner=lambda argv, env, timeout: docker_lifecycle.CommandResult(
            b"", b"", 0
        ),
    )
    images: dict[str, dict[str, object]] = {
        runner_id: {
            "Id": runner_id,
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {"Labels": dict(labels)},
        }
    }
    for base in base_rows:
        observed = {
            "Id": base["image_id"],
            "Os": "linux",
            "Architecture": "amd64",
            "RepoDigests": [base["reference"]],
        }
        images[str(base["reference"])] = observed
        images[str(base["image_id"])] = observed
    controller._image_inspect = lambda token: images[token]  # type: ignore[method-assign]
    return controller, images


def test_runner_image_requires_validated_record_labels_and_exact_local_bases(
    tmp_path: Path,
) -> None:
    controller, images = _runner_image_controller(tmp_path)
    runner_id = controller.admission["runner_image"]["image_id"]
    assert controller.verify_runner_image() == runner_id

    labels = images[runner_id]["Config"]["Labels"]
    assert isinstance(labels, dict)
    labels.pop(docker_lifecycle.WHEEL_ARTIFACT_LABEL)
    with pytest.raises(
        docker_lifecycle.DockerLifecycleError,
        match="runner_build_labels_mismatch",
    ):
        controller.verify_runner_image()


def test_execution_admission_archive_forwards_exact_path_free_build_record(
    tmp_path: Path,
) -> None:
    record = {
        "schema_version": "qt.assurance_runner_build_record.v1",
        "status": "succeeded",
        "source": {"commit": "a" * 40, "tree": "b" * 40},
    }
    record_path = tmp_path / "runner-build-record.json"
    docker_path = tmp_path / "docker"
    admission = {
        "docker_tool": {
            "resolved_path": str(docker_path),
            "version": "client=1;server=1",
            "executable_sha256": "c" * 64,
            "daemon_identity_sha256": "d" * 64,
        },
        "runner_build_record": {
            "resolved_path": str(record_path),
            "sha256": "e" * 64,
            "validated_record": record,
        },
    }
    archived = verifier.archive_execution_admission_profile(
        admission,
        "a" * 40,
        "f" * 64,
    )
    profile = archived["profile"]
    assert profile["runner_build_record"] == {
        "sha256": "e" * 64,
        "record_basename": record_path.name,
        "resolved_path_sha256": hashlib.sha256(
            str(record_path).encode("utf-8")
        ).hexdigest(),
        "record": record,
    }
    assert profile["docker_tool"]["executable_basename"] == docker_path.name
    assert "resolved_path" not in profile["docker_tool"]
    assert str(tmp_path) not in json.dumps(archived, sort_keys=True)


def test_process_guard_strips_ambient_credentials_and_kills_timeout_group(
    tmp_path: Path,
) -> None:
    env = {
        "PATH": os.environ["PATH"],
        "QT_ASSURANCE_MODE": "1",
        "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED": "0",
        "ALPACA_SECRET_TOKEN": "must-not-reach-child",
    }
    observed = process_guard.child_environment(env)
    assert observed == {
        "PATH": os.environ["PATH"],
        "QT_ASSURANCE_MODE": "1",
        "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED": "0",
    }

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(process_guard.__file__).resolve()),
            "--timeout",
            "1",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(5)",
        ],
        check=False,
        capture_output=True,
        env=env,
        timeout=5,
    )
    assert completed.returncode == 124
    assert process_guard.TIMEOUT_MARKER in completed.stderr
    assert b"must-not-reach-child" not in completed.stdout + completed.stderr


def test_first_signal_enters_noninterruptible_cleanup_before_propagation() -> None:
    with verifier._installed_interrupt_handlers() as state:
        with pytest.raises(verifier._ExecutionInterrupted):
            os.kill(os.getpid(), signal.SIGINT)
        assert state.cleanup_in_progress is True
        # A repeated signal is durable state, not a second escape from cleanup.
        os.kill(os.getpid(), signal.SIGTERM)
        assert state.signals == [signal.SIGINT, signal.SIGTERM]


def test_signal_during_stuck_docker_control_call_unwinds_promptly(
    tmp_path: Path,
) -> None:
    child_pid_path = tmp_path / "child.pid"
    fake_control = tmp_path / "sleeping_control.py"
    fake_control.write_text(
        "import os, pathlib, sys, time\n"
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding='utf-8')\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )

    def interrupt_when_child_started() -> None:
        deadline = time.monotonic() + 5
        while not child_pid_path.exists():
            if time.monotonic() >= deadline:
                return
            time.sleep(0.01)
        os.kill(os.getpid(), signal.SIGTERM)

    sender = threading.Thread(target=interrupt_when_child_started, daemon=True)
    sender.start()
    started = time.monotonic()
    with verifier._installed_interrupt_handlers() as state:
        with pytest.raises(verifier._ExecutionInterrupted):
            docker_lifecycle._default_command_runner(
                [sys.executable, str(fake_control), str(child_pid_path)],
                os.environ,
                30,
            )
        assert state.cleanup_in_progress is True
    sender.join(timeout=1)
    assert not sender.is_alive()
    assert time.monotonic() - started < 3

    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


def test_private_secret_target_is_reserved_before_first_write(tmp_path: Path) -> None:
    private = tmp_path / "private"
    reserved: list[Path] = []

    def reserve(path: Path) -> None:
        assert not path.exists()
        reserved.append(path)

    path = docker_lifecycle._private_env_file(
        private,
        "proof-runner-env",
        {"PG_DSN": "postgresql://synthetic"},
        reserve=reserve,
    )
    assert reserved == [path]
    assert path.stat().st_mode & 0o077 == 0


def test_generated_secret_and_dsn_are_redacted_before_durable_output(
    tmp_path: Path,
) -> None:
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: docker_lifecycle.CommandResult(b"", b"", 0),
    )
    password = b"high-entropy-password"
    dsn = b"postgresql://user:high-entropy-password@db/session"
    controller._secret_tokens.extend([password, dsn])
    safe, redacted = controller.redact_durable_output(
        b"failure " + dsn + b" and " + password
    )
    assert redacted is True
    assert password not in safe
    assert dsn not in safe
    assert safe.count(b"[QT-ASSURANCE-REDACTED]") == 2


def test_cleanup_attempts_all_resources_after_one_removal_failure(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def command(
        argv: list[str] | tuple[str, ...], env: object, timeout: int
    ) -> docker_lifecycle.CommandResult:
        del env, timeout
        args = list(argv)[1:]
        calls.append(args)
        if args[:3] == ["rm", "--force", "first"]:
            return docker_lifecycle.CommandResult(b"", b"removal failed", 1)
        if args[:2] in (["container", "inspect"], ["network", "inspect"], ["volume", "inspect"]):
            return docker_lifecycle.CommandResult(b"", b"no such object", 1)
        return docker_lifecycle.CommandResult(b"", b"", 0)

    controller = _controller(tmp_path, command)
    controller.verify_admission = lambda: ("a" * 64, "docker-test")  # type: ignore[method-assign]
    controller._docker_cleanup_target_present = lambda resource, target: True  # type: ignore[method-assign]
    resources = [
        docker_lifecycle.ResourceIdentity("container", "a-fails", "first"),
        docker_lifecycle.ResourceIdentity("container", "z-still-attempted", "second"),
    ]
    prepared = docker_lifecycle.ProvisionedProfile(
        profile_id="python-nondb",
        execution_class="isolated_container",
        runner_container_id="first",
        runner_image_id="sha256:" + "1" * 64,
        resources=resources,
        resource_targets={
            ("container", "a-fails"): "first",
            ("container", "z-still-attempted"): "second",
        },
        tool_versions={},
        bootstrap_stdout="",
        bootstrap_stderr="",
    )
    report = controller.cleanup(prepared)
    assert ["rm", "--force", "first"] in calls
    assert ["rm", "--force", "second"] in calls
    assert report.completed is False
    assert all(item.absent for item in report.resources)


def test_cleanup_rechecks_admitted_daemon_immediately_before_docker_rm(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    inspected = False

    def command(
        argv: list[str] | tuple[str, ...], env: object, timeout: int
    ) -> docker_lifecycle.CommandResult:
        del env, timeout
        args = list(argv)[1:]
        calls.append(args)
        return docker_lifecycle.CommandResult(b"", b"", 0)

    controller = _controller(tmp_path, command)

    def verify() -> tuple[str, str]:
        if inspected:
            raise docker_lifecycle.DockerLifecycleError("daemon changed after inspect")
        return "a" * 64, "docker-test"

    def inspect_target(resource: object, target: object) -> bool:
        nonlocal inspected
        del resource, target
        inspected = True
        return True

    controller.verify_admission = verify  # type: ignore[method-assign]
    controller.discover_labeled_resources = lambda: []  # type: ignore[method-assign]
    controller._docker_cleanup_target_present = inspect_target  # type: ignore[method-assign]
    resource = docker_lifecycle.ResourceIdentity(
        "container", "proof-runner", "runner-id"
    )
    prepared = docker_lifecycle.ProvisionedProfile(
        profile_id="python-nondb",
        execution_class="isolated_container",
        runner_container_id="runner-id",
        runner_image_id="sha256:" + "1" * 64,
        resources=[resource],
        resource_targets={("container", "proof-runner"): "runner-id"},
        tool_versions={},
        bootstrap_stdout="",
        bootstrap_stderr="",
    )

    report = controller.cleanup(prepared)

    assert inspected is True
    assert ["rm", "--force", "runner-id"] not in calls
    assert report.completed is False
    assert report.resources[0].absent is False
    assert "control-plane-identity-unverified" in report.label_query_remaining


def test_control_plane_mismatch_cannot_prove_replacement_daemon_absence(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: (
            calls.append(list(argv))
            or docker_lifecycle.CommandResult(b"", b"", 0)
        ),
    )

    def mismatch() -> tuple[str, str]:
        raise docker_lifecycle.DockerLifecycleError("daemon changed")

    controller.verify_admission = mismatch  # type: ignore[method-assign]
    source = controller.root
    source.mkdir()
    (source / "bound.txt").write_text("bound\n", encoding="utf-8")
    source.chmod(0o555)
    resources = [
        docker_lifecycle.ResourceIdentity(
            "source_snapshot", "exact-source-snapshot", "git-archive:" + "b" * 64
        ),
        docker_lifecycle.ResourceIdentity("container", "proof-runner", "runner-id"),
    ]
    prepared = docker_lifecycle.ProvisionedProfile(
        profile_id="python-nondb",
        execution_class="isolated_container",
        runner_container_id="runner-id",
        runner_image_id="sha256:" + "1" * 64,
        resources=resources,
        resource_targets={
            ("source_snapshot", "exact-source-snapshot"): source,
            ("container", "proof-runner"): "runner-id",
        },
        tool_versions={},
        bootstrap_stdout="",
        bootstrap_stderr="",
    )
    report = controller.cleanup(prepared)
    assert calls == []
    assert not source.exists()
    assert report.completed is False
    assert report.label_query_remaining == ("control-plane-identity-unverified",)
    assert next(item for item in report.resources if item.kind == "container").absent is False


def test_label_discovery_uses_full_session_profile_instance_identity(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def command(argv: object, env: object, timeout: int) -> docker_lifecycle.CommandResult:
        del env, timeout
        calls.append(list(argv))
        return docker_lifecycle.CommandResult(b"", b"", 0)

    controller = _controller(tmp_path, command)
    controller.verify_admission = lambda: ("a" * 64, "docker-test")  # type: ignore[method-assign]
    assert controller.discover_labeled_resources() == []
    text = "\n".join(" ".join(item) for item in calls)
    assert f"label={docker_lifecycle.SESSION_LABEL}={controller.attestation_id}" in text
    assert f"label={docker_lifecycle.PROFILE_LABEL}={controller.profile_id}" in text
    assert (
        f"label={docker_lifecycle.INSTANCE_LABEL}={controller.environment_instance_id}"
        in text
    )
    assert f"label={docker_lifecycle.SOURCE_LABEL}={controller.source_commit}" in text
    assert any(
        item[1:4] == ["ps", "-aq", "--no-trunc"] for item in calls
    )
    assert any(
        item[1:5] == ["network", "ls", "-q", "--no-trunc"]
        for item in calls
    )


def test_discovery_rejects_resource_missing_exact_source_label(tmp_path: Path) -> None:
    def command(argv: object, env: object, timeout: int) -> docker_lifecycle.CommandResult:
        del env, timeout
        args = list(argv)[1:]
        if args[:2] == ["ps", "-aq"]:
            return docker_lifecycle.CommandResult(b"container-id\n", b"", 0)
        if args[:3] == ["container", "inspect", "container-id"]:
            payload = [
                {
                    "Id": "container-id",
                    "Name": "/unexpected",
                    "Config": {
                        "Labels": {
                            docker_lifecycle.SESSION_LABEL: (
                                "QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb"
                            ),
                            docker_lifecycle.PROFILE_LABEL: "python-nondb",
                            docker_lifecycle.INSTANCE_LABEL: "qt-12345678",
                        }
                    },
                }
            ]
            return docker_lifecycle.CommandResult(json.dumps(payload).encode(), b"", 0)
        return docker_lifecycle.CommandResult(b"", b"", 0)

    controller = _controller(tmp_path, command)
    controller.verify_admission = lambda: ("a" * 64, "docker-test")  # type: ignore[method-assign]
    with pytest.raises(
        docker_lifecycle.DockerLifecycleError,
        match="identity_labels_mismatch",
    ):
        controller.discover_labeled_resources()


def test_recovery_registers_only_exact_private_secret_patterns_and_redaction(
    tmp_path: Path,
) -> None:
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: docker_lifecycle.CommandResult(b"", b"", 0),
    )
    secret = controller.private_root / "proof-runner-env-0123456789abcdef.env"
    secret.write_text(
        "PG_DSN=postgresql://synthetic:private-value@db/session\n",
        encoding="utf-8",
    )
    secret.chmod(0o600)
    unrelated = controller.private_root / "unrelated.env"
    unrelated.write_text("TOKEN=must-remain\n", encoding="utf-8")
    unrelated.chmod(0o600)
    controller.register_recovery_local_resources(
        source_snapshot_sha256="b" * 64,
        planned_resources=[
            {"kind": "source_snapshot", "logical_name": "exact-source-snapshot"},
            {"kind": "temporary_secret_file", "logical_name": "proof-runner-env"},
        ],
    )
    prepared = controller.partial_profile("isolated_database")
    registered = {
        (item.kind, item.logical_name) for item in prepared.resources
    }
    assert ("temporary_secret_file", "proof-runner-env") in registered
    assert all("unrelated" not in logical_name for _, logical_name in registered)
    redacted, changed = controller.redact_durable_output(
        b"postgresql://synthetic:private-value@db/session"
    )
    assert changed is True
    assert b"private-value" not in redacted
    controller.verify_admission = lambda: ("a" * 64, "docker-test")  # type: ignore[method-assign]
    prepared = controller.partial_profile("isolated_database")
    report = controller.cleanup(prepared)
    assert secret.exists() is False
    assert unrelated.read_text(encoding="utf-8") == "TOKEN=must-remain\n"
    assert report.completed is False
    assert "local-residue:unrecognized" in report.label_query_remaining
    residue = [item for item in report.resources if item.kind == "private_residue"]
    assert len(residue) == 1
    assert residue[0].logical_name == "unrecognized-private-residue-001"
    assert residue[0].runtime_identity.startswith("path-name-sha256:")
    assert "unrelated" not in residue[0].runtime_identity
    assert residue[0].absent is False


def test_recovery_source_symlink_cannot_delete_private_sibling(
    tmp_path: Path,
) -> None:
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: docker_lifecycle.CommandResult(b"", b"", 0),
    )
    sibling = controller.private_root / "must-remain"
    sibling.mkdir()
    marker = sibling / "marker.txt"
    marker.write_text("preserve\n", encoding="utf-8")
    controller.root.symlink_to(sibling, target_is_directory=True)
    planned = [
        {"kind": "source_snapshot", "logical_name": "exact-source-snapshot"}
    ]
    with pytest.raises(
        docker_lifecycle.DockerLifecycleError,
        match="source_snapshot_invalid",
    ):
        controller.register_recovery_local_resources(
            source_snapshot_sha256="b" * 64,
            planned_resources=planned,
        )
    assert marker.read_text(encoding="utf-8") == "preserve\n"

    prepared = docker_lifecycle.ProvisionedProfile(
        profile_id="python-nondb",
        execution_class="isolated_container",
        runner_container_id="absent",
        runner_image_id="sha256:" + "1" * 64,
        resources=[
            docker_lifecycle.ResourceIdentity(
                "source_snapshot",
                "exact-source-snapshot",
                "git-archive:" + "b" * 64,
            )
        ],
        resource_targets={
            ("source_snapshot", "exact-source-snapshot"): controller.root
        },
        tool_versions={},
        bootstrap_stdout="",
        bootstrap_stderr="",
    )
    controller.verify_admission = lambda: ("a" * 64, "docker-test")  # type: ignore[method-assign]
    report = controller.cleanup(prepared)
    assert report.completed is False
    assert report.resources[0].absent is False
    assert marker.read_text(encoding="utf-8") == "preserve\n"


def test_session_profile_lock_is_nonblocking_and_crash_reusable(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir()
    private.chmod(0o700)
    first = verifier._SessionProfileLock(
        private_root=private,
        attestation_id="QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb",
        profile_id="python-nondb",
    )
    second = verifier._SessionProfileLock(
        private_root=private,
        attestation_id="QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb",
        profile_id="python-nondb",
    )
    first.acquire()
    try:
        with pytest.raises(verifier.AssuranceExecutionError, match="lock_busy"):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_recovery_draft_rejects_unbound_admission_archive_hash(
    tmp_path: Path,
) -> None:
    source_commit = "a" * 40
    attestation_id = "QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb"
    profile_id = "python-nondb"
    environment_dir = (
        tmp_path
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / attestation_id
        / "_environments"
        / profile_id
    )
    environment_dir.mkdir(parents=True)
    archive = environment_dir / "execution_admission_archive-001-profile.json"
    archive.write_text("{}\n", encoding="utf-8")
    draft = environment_dir / "execution_draft-001-profile.json"
    draft.write_text(
        json.dumps(
            {
                "schema_version": "qt.assurance_environment_evidence.v1",
                "profile_id": profile_id,
                "artifact_kind": "execution_draft",
                "facts": {
                    "record_schema_version": "qt.assurance_execution_draft.v1",
                    "attestation_id": attestation_id,
                    "source_commit": source_commit,
                    "source_snapshot_sha256": "b" * 64,
                    "requested_profile_ids": [profile_id],
                    "started_at": "2026-08-25T12:00:00Z",
                    "admission_id": "owner-reviewed-admission",
                    "environment_instance_id": "qt-12345678",
                    "control_plane_identity_sha256": "c" * 64,
                    "runtime_definition_sha256": "d" * 64,
                        "execution_admission_sha256": "e" * 64,
                        "execution_admission_archive_sha256": "f" * 64,
                        "runner_build_record_sha256": "9" * 64,
                        "external_order_submission_enabled": False,
                    "planned_resources": [
                        {
                            "kind": "source_snapshot",
                            "logical_name": "exact-source-snapshot",
                        }
                    ],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="admission_archive_hash_mismatch",
    ):
        verifier._load_recovery_draft(
            stage_root=tmp_path,
            draft_path=draft,
            source_commit=source_commit,
        )


def _install_recovery_fakes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    binding_tamper: bool,
    cleanup_raises: bool = False,
    cleanup_completed: bool = True,
) -> tuple[dict[str, Path], list[str]]:
    source_commit = "a" * 40
    profile_id = "python-nondb"
    attestation_id = (
        "QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb"
    )
    paths = {
        "root": tmp_path / "source-repository",
        "stage": tmp_path / "external-stage",
        "private": tmp_path / "private-runtime",
        "admission": tmp_path / "owner-input" / "admission.json",
        "report": tmp_path / "operator-records" / "recovery-report.json",
    }
    for key in ("root", "stage", "private"):
        paths[key].mkdir()
    paths["private"].chmod(0o700)
    paths["admission"].parent.mkdir()
    paths["admission"].write_text("{}\n", encoding="utf-8")
    paths["report"].parent.mkdir()
    draft_path = (
        paths["stage"]
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / attestation_id
        / "_environments"
        / profile_id
        / "execution_draft-001-profile.json"
    )
    draft_path.parent.mkdir(parents=True)
    draft_path.write_text("{}\n", encoding="utf-8")
    paths["draft"] = draft_path

    source_snapshot_sha256 = "b" * 64
    control_identity_sha256 = "c" * 64
    runtime_definition_sha256 = "d" * 64
    execution_admission_sha256 = "e" * 64
    archive_sha256 = "f" * 64
    runner_build_record_sha256 = "9" * 64
    planned_resources = [
        {"kind": "source_snapshot", "logical_name": "exact-source-snapshot"},
        {"kind": "temporary_secret_file", "logical_name": "proof-runner-env"},
        {"kind": "container", "logical_name": "proof-runner"},
    ]
    facts = {
        "attestation_id": attestation_id,
        "source_commit": source_commit,
        "source_snapshot_sha256": source_snapshot_sha256,
        "requested_profile_ids": [profile_id],
        "admission_id": "owner-reviewed-admission",
        "environment_instance_id": "qt-12345678",
        "control_plane_identity_sha256": control_identity_sha256,
        "runtime_definition_sha256": runtime_definition_sha256,
        "execution_admission_sha256": (
            "0" * 64 if binding_tamper else execution_admission_sha256
        ),
        "execution_admission_archive_sha256": archive_sha256,
        "runner_build_record_sha256": runner_build_record_sha256,
        "planned_resources": planned_resources,
    }
    admission = {
        "admission_id": "owner-reviewed-admission",
        "runtime_definition": {"sha256": runtime_definition_sha256},
        "docker_tool": {
            "daemon_identity_sha256": control_identity_sha256,
        },
        "runner_build_record": {
            "resolved_path": str(
                paths["admission"].parent / "runner-build-record.json"
            ),
            "sha256": runner_build_record_sha256,
        },
    }
    archive_facts = {
        "normalized": "exact-owner-reviewed-admission",
    }
    mutations: list[str] = []

    class FakeController:
        def __init__(self, **kwargs: object) -> None:
            del kwargs
            mutations.append("controller-created")

        def planned_resources(self, execution_class: str) -> list[dict[str, str]]:
            assert execution_class == "isolated_container"
            return planned_resources

        def register_recovery_local_resources(self, **kwargs: object) -> None:
            del kwargs
            mutations.append("recovery-targets-registered")

        def partial_profile(self, execution_class: str) -> object:
            assert execution_class == "isolated_container"
            return object()

        def cleanup(self, prepared: object) -> docker_lifecycle.CleanupReport:
            del prepared
            assert paths["report"].with_name(
                paths["report"].name + ".pending"
            ).is_file()
            mutations.append("cleanup-called")
            if cleanup_raises:
                raise RuntimeError("injected recovery crash")
            return docker_lifecycle.CleanupReport(
                stdout="private cleanup token",
                stderr="",
                exit_code=0 if cleanup_completed else 1,
                resources=tuple(
                    docker_lifecycle.CleanupResource(
                        kind=item["kind"],
                        logical_name=item["logical_name"],
                        runtime_identity=f"recovered:{item['logical_name']}",
                        absent=cleanup_completed,
                    )
                    for item in planned_resources
                ),
                label_query_remaining=(
                    () if cleanup_completed else ("container:proof-runner",)
                ),
            )

        def redact_durable_output(self, content: bytes) -> tuple[bytes, bool]:
            if b"private cleanup token" in content:
                return b"[QT-ASSURANCE-REDACTED]", True
            return content, False

    bundle = SimpleNamespace(
        proof_catalog={
            "environment_profiles": [
                {"id": profile_id, "execution_class": "isolated_container"}
            ]
        }
    )
    monkeypatch.setattr(verifier, "require_exact_clean_source", lambda *args: None)
    monkeypatch.setattr(
        verifier.guarantees, "validate_repository", lambda root: bundle
    )
    monkeypatch.setattr(
        verifier,
        "_load_recovery_draft",
        lambda **kwargs: (facts, "1" * 64, archive_facts, archive_sha256),
    )
    monkeypatch.setattr(
        verifier,
        "load_execution_admission",
        lambda path, commit: ({profile_id: admission}, execution_admission_sha256),
    )
    monkeypatch.setattr(verifier, "_align_execution_admission", lambda **kwargs: None)
    monkeypatch.setattr(
        verifier,
        "archive_execution_admission_profile",
        lambda item, commit, digest: archive_facts,
    )
    monkeypatch.setattr(verifier, "_git_archive_bytes", lambda root, commit: b"archive")
    monkeypatch.setattr(
        verifier,
        "_archive_tree_sha256",
        lambda archive: source_snapshot_sha256,
    )
    monkeypatch.setattr(
        verifier.docker_lifecycle, "DockerController", FakeController
    )
    return paths, mutations


def test_recovery_binding_tamper_causes_no_cleanup_mutation_or_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, mutations = _install_recovery_fakes(
        tmp_path, monkeypatch, binding_tamper=True
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="recovery_execution_admission_sha256_mismatch",
    ):
        verifier.recover_cleanup(
            root=paths["root"],
            source_commit="a" * 40,
            stage_root=paths["stage"],
            private_root=paths["private"],
            execution_admission=paths["admission"],
            draft_path=paths["draft"],
            output_path=paths["report"],
        )
    assert mutations == []
    assert not paths["report"].exists()
    assert not paths["report"].with_name(
        paths["report"].name + ".pending"
    ).exists()


def test_recovery_success_is_immutable_redacted_and_permanently_nonfinalizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, mutations = _install_recovery_fakes(
        tmp_path, monkeypatch, binding_tamper=False
    )
    result = verifier.recover_cleanup(
        root=paths["root"],
        source_commit="a" * 40,
        stage_root=paths["stage"],
        private_root=paths["private"],
        execution_admission=paths["admission"],
        draft_path=paths["draft"],
        output_path=paths["report"],
    )
    assert result == paths["report"]
    report = json.loads(result.read_text(encoding="utf-8"))
    assert report["schema_version"] == verifier.RECOVERY_REPORT_SCHEMA_VERSION
    assert report["recovery_state"] == "cleanup_verified"
    assert report["finalizable"] is False
    assert report["nonfinalizable"] is True
    assert report["attestation_emitted"] is False
    assert report["cleanup"]["cleanup_completed"] is True
    assert report["cleanup"]["redaction_applied"] is True
    assert "private cleanup token" not in result.read_text(encoding="utf-8")
    assert report["cleanup"]["stdout"] == "[QT-ASSURANCE-REDACTED]"
    immutable_bytes = result.read_bytes()
    intent_path = result.with_name(result.name + ".pending")
    intent = json.loads(intent_path.read_text(encoding="utf-8"))
    assert intent["schema_version"] == verifier.RECOVERY_INTENT_SCHEMA_VERSION
    assert intent["recovery_state"] == "cleanup_pending"
    assert intent["finalizable"] is False
    assert intent["attestation_emitted"] is False
    assert intent["recovery_attempt_id"] == report["recovery_attempt_id"]
    assert report["recovery_intent"] == {
        "file_name": intent_path.name,
        "sha256": verifier._sha256_file_binary(intent_path),
    }
    final_attestation = (
        paths["stage"]
        / "docs"
        / "assurance"
        / "guarantees"
        / "attestations"
        / ("a" * 40)
        / (
            "QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb.json"
        )
    )
    assert not final_attestation.exists()
    assert mutations == [
        "controller-created",
        "controller-created",
        "recovery-targets-registered",
        "cleanup-called",
    ]
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="recovery_report:already_exists",
    ):
        verifier.recover_cleanup(
            root=paths["root"],
            source_commit="a" * 40,
            stage_root=paths["stage"],
            private_root=paths["private"],
            execution_admission=paths["admission"],
            draft_path=paths["draft"],
            output_path=paths["report"],
        )
    assert result.read_bytes() == immutable_bytes
    assert mutations == [
        "controller-created",
        "controller-created",
        "recovery-targets-registered",
        "cleanup-called",
    ]


def test_recovery_crash_retains_intent_and_requires_a_new_attempt_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, mutations = _install_recovery_fakes(
        tmp_path,
        monkeypatch,
        binding_tamper=False,
        cleanup_raises=True,
    )
    arguments = {
        "root": paths["root"],
        "source_commit": "a" * 40,
        "stage_root": paths["stage"],
        "private_root": paths["private"],
        "execution_admission": paths["admission"],
        "draft_path": paths["draft"],
    }
    with pytest.raises(RuntimeError, match="injected recovery crash"):
        verifier.recover_cleanup(
            **arguments,
            output_path=paths["report"],
        )
    first_intent = paths["report"].with_name(paths["report"].name + ".pending")
    first_bytes = first_intent.read_bytes()
    assert not paths["report"].exists()
    assert json.loads(first_bytes)["recovery_state"] == "cleanup_pending"

    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="recovery_intent:already_exists",
    ):
        verifier.recover_cleanup(
            **arguments,
            output_path=paths["report"],
        )
    assert first_intent.read_bytes() == first_bytes

    second_report = paths["report"].with_name("recovery-report-attempt-2.json")
    paths["report"] = second_report
    with pytest.raises(RuntimeError, match="injected recovery crash"):
        verifier.recover_cleanup(
            **arguments,
            output_path=second_report,
        )
    second_intent = second_report.with_name(second_report.name + ".pending")
    assert second_intent.is_file()
    assert second_intent.read_bytes() != first_bytes
    assert mutations.count("cleanup-called") == 2


def test_recovery_incomplete_writes_nonfinalizable_report_then_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, mutations = _install_recovery_fakes(
        tmp_path,
        monkeypatch,
        binding_tamper=False,
        cleanup_completed=False,
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="recovery_cleanup_incomplete_report",
    ):
        verifier.recover_cleanup(
            root=paths["root"],
            source_commit="a" * 40,
            stage_root=paths["stage"],
            private_root=paths["private"],
            execution_admission=paths["admission"],
            draft_path=paths["draft"],
            output_path=paths["report"],
        )
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    assert report["recovery_state"] == "cleanup_incomplete"
    assert report["finalizable"] is False
    assert report["nonfinalizable"] is True
    assert report["attestation_emitted"] is False
    assert report["cleanup"]["cleanup_completed"] is False
    assert report["cleanup"]["label_query_remaining"] == [
        "container:proof-runner"
    ]
    assert mutations[-1] == "cleanup-called"


def test_recovery_rechecks_final_attestation_after_acquiring_session_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, mutations = _install_recovery_fakes(
        tmp_path,
        monkeypatch,
        binding_tamper=False,
    )
    final_attestation = (
        paths["stage"]
        / "docs"
        / "assurance"
        / "guarantees"
        / "attestations"
        / ("a" * 40)
        / "QT-ATT-20260825T120000Z-aaaaaaaaaaaa-python-nondb.json"
    )

    class FinalizingRaceLock:
        def __enter__(self) -> object:
            final_attestation.parent.mkdir(parents=True)
            final_attestation.write_text("{}\n", encoding="utf-8")
            return self

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            del exc_type, exc, traceback

    monkeypatch.setattr(
        verifier,
        "_SessionProfileLock",
        lambda **kwargs: FinalizingRaceLock(),
    )
    with pytest.raises(
        verifier.AssuranceExecutionError,
        match="recovery_finalized_attestation_already_exists",
    ):
        verifier.recover_cleanup(
            root=paths["root"],
            source_commit="a" * 40,
            stage_root=paths["stage"],
            private_root=paths["private"],
            execution_admission=paths["admission"],
            draft_path=paths["draft"],
            output_path=paths["report"],
        )
    assert mutations == ["controller-created", "controller-created"]
    assert not paths["report"].exists()
    assert not paths["report"].with_name(
        paths["report"].name + ".pending"
    ).exists()


def test_exact_archive_snapshot_detects_post_extract_mutation(tmp_path: Path) -> None:
    archive_buffer = io.BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
        info = tarfile.TarInfo("scripts/proof.py")
        content = b"print('bound')\n"
        info.size = len(content)
        info.mode = 0o755
        archive.addfile(info, io.BytesIO(content))
    archive_bytes = archive_buffer.getvalue()
    expected = verifier._archive_tree_sha256(archive_bytes)
    snapshot = tmp_path / "snapshot"
    verifier._extract_source_snapshot(archive_bytes, snapshot)
    assert verifier._snapshot_tree_sha256(snapshot) == expected
    proof_file = snapshot / "scripts" / "proof.py"
    proof_file.chmod(0o700)
    proof_file.write_text("print('mutated')\n", encoding="utf-8")
    assert verifier._snapshot_tree_sha256(snapshot) != expected


def test_runner_observation_rejects_unadmitted_image_environment(
    tmp_path: Path,
) -> None:
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: docker_lifecycle.CommandResult(b"", b"", 0),
    )
    controller.root.mkdir()
    expected_env = controller._runner_environment(database=False)
    labels = {
        docker_lifecycle.SESSION_LABEL: controller.attestation_id,
        docker_lifecycle.PROFILE_LABEL: controller.profile_id,
        docker_lifecycle.SOURCE_LABEL: controller.source_commit,
        docker_lifecycle.INSTANCE_LABEL: controller.environment_instance_id,
    }
    observed = {
        "Id": "runner",
        "Image": "sha256:" + "1" * 64,
        "Config": {
            "Labels": labels,
            "WorkingDir": "/workspace",
            "Env": [
                *(f"{key}={value}" for key, value in expected_env.items()),
                "ALPACA_SECRET_TOKEN=forbidden",
            ],
        },
        "HostConfig": {
            "ReadonlyRootfs": True,
            "NetworkMode": "none",
            "Tmpfs": {"/tmp": "rw"},
        },
        "Mounts": [
            {
                "Destination": "/workspace",
                "Type": "bind",
                "RW": False,
                "Source": str(controller.root),
            }
        ],
        "NetworkSettings": {"Networks": {"none": {"NetworkID": ""}}},
    }
    controller._resource_inspect = lambda kind, token: observed  # type: ignore[method-assign]
    with pytest.raises(
        docker_lifecycle.DockerLifecycleError,
        match="runner_environment_unadmitted_keys",
    ):
        controller._verify_runner_configuration(
            "runner", "sha256:" + "1" * 64, "none"
        )


def test_database_runner_observation_retains_only_dsn_hash(tmp_path: Path) -> None:
    controller = _controller(
        tmp_path,
        lambda argv, env, timeout: docker_lifecycle.CommandResult(b"", b"", 0),
    )
    controller.root.mkdir()
    expected_env = controller._runner_environment(database=True)
    expected_env["PYTHON_SHA256"] = "f" * 64
    dsn = "postgresql://user:private-password@db/session"
    expected_env["PG_DSN"] = dsn
    observed = {
        "Id": "runner",
        "Image": "sha256:" + "1" * 64,
        "Config": {
            "Labels": {
                docker_lifecycle.SESSION_LABEL: controller.attestation_id,
                docker_lifecycle.PROFILE_LABEL: controller.profile_id,
                docker_lifecycle.SOURCE_LABEL: controller.source_commit,
                docker_lifecycle.INSTANCE_LABEL: controller.environment_instance_id,
            },
            "WorkingDir": "/workspace",
            "Env": [f"{key}={value}" for key, value in expected_env.items()],
        },
        "HostConfig": {
            "ReadonlyRootfs": True,
            "NetworkMode": "network-id",
            "Tmpfs": {"/tmp": "rw"},
        },
        "Mounts": [
            {
                "Destination": "/workspace",
                "Type": "bind",
                "RW": False,
                "Source": str(controller.root),
            }
        ],
        "NetworkSettings": {
            "Networks": {"session": {"NetworkID": "network-id"}}
        },
    }
    controller._resource_inspect = lambda kind, token: observed  # type: ignore[method-assign]
    facts = controller._verify_runner_configuration(
        "runner", "sha256:" + "1" * 64, "network-id"
    )
    assert facts["pg_dsn_sha256"] == hashlib.sha256(dsn.encode()).hexdigest()
    assert dsn not in repr(facts)

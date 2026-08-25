from __future__ import annotations

import hashlib
import io
import os
import signal
import subprocess
import sys
import tarfile
from pathlib import Path

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

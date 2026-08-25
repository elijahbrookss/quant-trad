"""Shell-free disposable Docker resources for assurance proof execution.

This module owns only the control-plane mechanics.  The caller owns immutable
draft/execution/cleanup records and must write the draft before calling
``provision``.  No method pulls or builds an image.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


HEX_IMAGE_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SESSION_LABEL = "com.quant-trad.assurance.session"
PROFILE_LABEL = "com.quant-trad.assurance.profile"
SOURCE_LABEL = "com.quant-trad.assurance.source"
INSTANCE_LABEL = "com.quant-trad.assurance.instance"
BUILD_DEFINITION_LABEL = "com.quant-trad.assurance.build-definition-sha256"
RUNNER_CONTAINER_ENV_ALLOWLIST = {
    "GPG_KEY",
    "HOME",
    "LANG",
    "NO_COLOR",
    "PATH",
    "PG_DSN",
    "PIP_DISABLE_PIP_VERSION_CHECK",
    "PYTHON_GET_PIP_SHA256",
    "PYTHON_GET_PIP_URL",
    "PYTHON_PIP_VERSION",
    "PYTHON_SHA256",
    "PYTHON_VERSION",
    "PYTHONHASHSEED",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONUNBUFFERED",
    "PYTEST_ADDOPTS",
    "PYTEST_PLUGINS",
    "PYTHONPATH",
    "QT_ASSURANCE_MODE",
    "QT_DB_TEST_ISOLATED",
    "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED",
    "RUN_DB_TESTS",
    "TMPDIR",
    "TZ",
}
SECRET_ENV_NAME_RE = re.compile(
    r"(?:PASSWORD|SECRET|TOKEN|CREDENTIAL|PRIVATE_KEY|API_KEY|ACCESS_KEY|DSN)", re.I
)


class DockerLifecycleError(RuntimeError):
    """A Docker lifecycle boundary could not be established honestly."""


@dataclass(frozen=True)
class CommandResult:
    stdout: bytes
    stderr: bytes
    exit_code: int
    timed_out: bool = False


@dataclass(frozen=True)
class ResourceIdentity:
    kind: str
    logical_name: str
    runtime_identity: str

    def as_record(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "logical_name": self.logical_name,
            "runtime_identity": self.runtime_identity,
        }


@dataclass(frozen=True)
class CleanupResource:
    kind: str
    logical_name: str
    runtime_identity: str
    absent: bool

    def as_record(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "logical_name": self.logical_name,
            "runtime_identity": self.runtime_identity,
            "absent": self.absent,
        }


@dataclass(frozen=True)
class CleanupReport:
    stdout: str
    stderr: str
    exit_code: int
    resources: tuple[CleanupResource, ...]
    label_query_remaining: tuple[str, ...]

    @property
    def completed(self) -> bool:
        return (
            self.exit_code == 0
            and not self.label_query_remaining
            and all(item.absent for item in self.resources)
        )


@dataclass
class ProvisionedProfile:
    profile_id: str
    execution_class: str
    runner_container_id: str
    runner_image_id: str
    resources: list[ResourceIdentity]
    resource_targets: dict[tuple[str, str], Any]
    tool_versions: dict[str, str]
    bootstrap_stdout: str
    bootstrap_stderr: str
    network_id: str | None = None
    database_container_id: str | None = None
    database_name: str | None = None
    published_port: int | None = None
    service_facts: dict[str, Any] = field(default_factory=dict)
    observed_configuration: dict[str, Any] = field(default_factory=dict)

    def sorted_resources(self) -> list[dict[str, str]]:
        return [
            item.as_record()
            for item in sorted(
                self.resources, key=lambda value: (value.kind, value.logical_name)
            )
        ]


CommandRunner = Callable[[Sequence[str], Mapping[str, str], int], CommandResult]
CONTROL_REAP_TIMEOUT_SECONDS = 5


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _default_command_runner(
    argv: Sequence[str], env: Mapping[str, str], timeout_seconds: int
) -> CommandResult:
    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=dict(env),
            shell=False,
            start_new_session=True,
        )
    except OSError as exc:
        raise DockerLifecycleError(
            f"docker_control_start_failed:{type(exc).__name__}"
        ) from exc
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        stdout, stderr, reaped = _terminate_command_process(process)
        if not stdout:
            stdout = exc.stdout or b""
        if not stderr:
            stderr = exc.stderr or b""
        reap_marker = (
            b""
            if reaped
            else b"qt_assurance_docker_control:reap_incomplete\n"
        )
        return CommandResult(
            stdout,
            stderr + b"\nqt_assurance_docker_control:timeout\n" + reap_marker,
            124,
            True,
        )
    except BaseException:
        # The lifecycle signal handler deliberately raises a BaseException.  A
        # plain ``subprocess.run``/``Popen.__exit__`` can then wait forever for
        # a stuck Docker CLI, preventing the outer cleanup gate from running.
        # Kill the isolated control-process group and bound the reap before
        # propagating the original interruption into that cleanup gate.
        _terminate_command_process(process)
        raise
    return CommandResult(stdout, stderr, int(process.returncode), False)


def _terminate_command_process(
    process: subprocess.Popen[bytes],
) -> tuple[bytes, bytes, bool]:
    try:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (AttributeError, OSError):
                process.kill()
    except OSError:
        # A concurrent exit is equivalent to the requested termination.  The
        # bounded reap below remains the authority for whether it completed.
        pass
    try:
        stdout, stderr = process.communicate(timeout=CONTROL_REAP_TIMEOUT_SECONDS)
        return stdout, stderr, True
    except subprocess.TimeoutExpired as exc:
        try:
            process.kill()
        except OSError:
            pass
        try:
            process.wait(timeout=CONTROL_REAP_TIMEOUT_SECONDS)
            reaped = True
        except subprocess.TimeoutExpired:
            reaped = False
        return exc.stdout or b"", exc.stderr or b"", reaped


def _safe_name(value: str, *, limit: int = 63) -> str:
    result = re.sub(r"[^a-z0-9_.-]+", "-", value.lower()).strip("-.")
    if not result:
        raise DockerLifecycleError("docker_resource_name_empty")
    return result[:limit].rstrip("-.")


def _new_database_credentials() -> tuple[str, str, str]:
    suffix = secrets.token_hex(10)
    return (
        f"qt_assurance_{suffix}",
        f"qt_assurance_{suffix}",
        secrets.token_urlsafe(32),
    )


def _private_env_file(
    directory: Path,
    logical_name: str,
    values: Mapping[str, str],
    *,
    reserve: Callable[[Path], None],
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{_safe_name(logical_name)}-{secrets.token_hex(8)}.env"
    reserve(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            for key, value in sorted(values.items()):
                if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
                    raise DockerLifecycleError("private_env_key_invalid")
                if any(character in value for character in "\x00\r\n"):
                    raise DockerLifecycleError("private_env_value_invalid")
                handle.write(f"{key}={value}\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    if path.stat().st_mode & 0o077:
        path.unlink(missing_ok=True)
        raise DockerLifecycleError("private_env_permissions_not_private")
    return path


class DockerController:
    """An admitted Docker control plane bound to one attestation profile."""

    def __init__(
        self,
        *,
        admission: Mapping[str, Any],
        root: Path,
        private_root: Path,
        source_commit: str,
        attestation_id: str,
        profile_id: str,
        environment_instance_id: str,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.admission = admission
        self.root = root.resolve()
        self.private_root = private_root.resolve()
        self.source_commit = source_commit
        self.attestation_id = attestation_id
        self.profile_id = profile_id
        self.environment_instance_id = environment_instance_id
        self.docker_path = str(Path(admission["docker_tool"]["resolved_path"]).resolve())
        self._command_runner = command_runner or _default_command_runner
        self._control_env = self._docker_control_env()
        self._last_provisioned: ProvisionedProfile | None = None
        self._partial_resources: list[ResourceIdentity] = []
        self._partial_targets: dict[tuple[str, str], Any] = {}
        self._secret_tokens: list[bytes] = []
        self._recovery_local_inventory_required = False

    def _docker_control_env(self) -> dict[str, str]:
        env = {
            "PATH": os.pathsep.join(
                [str(Path(self.docker_path).parent), "/usr/bin", "/bin"]
            ),
            "LANG": "C.UTF-8",
            "TZ": "UTC",
        }
        # These select the Docker control plane.  Their effective result is
        # hashed and rechecked; they are never inherited by proof children.
        for name in (
            "DOCKER_CONTEXT",
            "DOCKER_HOST",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
            "HOME",
        ):
            value = os.environ.get(name, "").strip()
            if value and not any(character in value for character in "\x00\r\n"):
                env[name] = value
        return env

    def _call(
        self, args: Sequence[str], *, timeout_seconds: int = 30, check: bool = False
    ) -> CommandResult:
        result = self._command_runner(
            [self.docker_path, *args], self._control_env, timeout_seconds
        )
        if check and result.exit_code != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise DockerLifecycleError(
                f"docker_command_failed:{args[0] if args else 'unknown'}:{detail[:240]}"
            )
        return result

    def process_env(self) -> dict[str, str]:
        """Return only the admitted Docker-control environment for host exec."""

        return dict(self._control_env)

    def redact_durable_output(self, value: bytes) -> tuple[bytes, bool]:
        """Remove every generated secret before bytes can enter evidence."""

        sanitized = value
        redacted = False
        for token in sorted(self._secret_tokens, key=len, reverse=True):
            if token and token in sanitized:
                sanitized = sanitized.replace(token, b"[QT-ASSURANCE-REDACTED]")
                redacted = True
        return sanitized, redacted

    @staticmethod
    def _json(stdout: bytes, where: str) -> Any:
        try:
            return json.loads(stdout.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise DockerLifecycleError(f"{where}:invalid_json") from exc

    def control_plane(self) -> tuple[dict[str, Any], str, str]:
        version_raw = self._call(
            ["version", "--format", "{{json .}}"], check=True
        )
        context_raw = self._call(["context", "show"], check=True)
        info_raw = self._call(["info", "--format", "{{json .}}"], check=True)
        version = self._json(version_raw.stdout, "docker_version")
        info = self._json(info_raw.stdout, "docker_info")
        if not isinstance(version, dict) or not isinstance(info, dict):
            raise DockerLifecycleError("docker_control_plane_shape_invalid")
        client = version.get("Client") or {}
        server = version.get("Server") or {}
        context_name = context_raw.stdout.decode("utf-8", errors="strict").strip()
        snapshot = {
            "architecture": info.get("Architecture"),
            "context": context_name,
            "daemon_id": info.get("ID"),
            "docker_root_dir_sha256": _sha256_bytes(
                str(info.get("DockerRootDir", "")).encode("utf-8")
            ),
            "os_type": info.get("OSType"),
            "server_api_version": server.get("ApiVersion"),
            "server_version": server.get("Version"),
        }
        if any(value in {None, ""} for value in snapshot.values()):
            raise DockerLifecycleError("docker_control_plane_identity_incomplete")
        version_label = f"client={client.get('Version')};server={server.get('Version')}"
        identity = _sha256_bytes(_canonical_json_bytes(snapshot))
        return snapshot, identity, version_label

    def verify_admission(self) -> tuple[str, str]:
        expected = self.admission["docker_tool"]
        path = Path(self.docker_path)
        if not path.is_absolute() or not path.is_file():
            raise DockerLifecycleError("admitted_docker_executable_unavailable")
        if _sha256_file(path) != expected["executable_sha256"]:
            raise DockerLifecycleError("admitted_docker_executable_hash_mismatch")
        _, identity, version = self.control_plane()
        if identity != expected["daemon_identity_sha256"]:
            raise DockerLifecycleError("admitted_docker_control_plane_mismatch")
        if version != expected["version"]:
            raise DockerLifecycleError("admitted_docker_version_mismatch")
        return identity, version

    def _image_inspect(self, token: str) -> dict[str, Any]:
        result = self._call(["image", "inspect", token], check=True)
        payload = self._json(result.stdout, "docker_image_inspect")
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            raise DockerLifecycleError("docker_image_inspect_shape_invalid")
        return payload[0]

    def _resource_inspect(self, kind: str, token: str) -> dict[str, Any]:
        result = self._call([kind, "inspect", token], check=True)
        payload = self._json(result.stdout, f"docker_{kind}_inspect")
        if (
            not isinstance(payload, list)
            or len(payload) != 1
            or not isinstance(payload[0], dict)
        ):
            raise DockerLifecycleError(f"docker_{kind}_inspect_shape_invalid")
        return payload[0]

    def _verify_labels(self, observed: Mapping[str, Any], where: str) -> None:
        labels = (observed.get("Config") or {}).get("Labels") or observed.get(
            "Labels"
        ) or {}
        expected = {
            SESSION_LABEL: self.attestation_id,
            PROFILE_LABEL: self.profile_id,
            SOURCE_LABEL: self.source_commit,
            INSTANCE_LABEL: self.environment_instance_id,
        }
        if not isinstance(labels, dict) or any(
            labels.get(key) != value for key, value in expected.items()
        ):
            raise DockerLifecycleError(f"{where}:identity_labels_mismatch")

    def _verify_runner_configuration(
        self, container_id: str, image_id: str, network_id: str
    ) -> dict[str, Any]:
        observed = self._resource_inspect("container", container_id)
        self._verify_labels(observed, "runner_container")
        if observed.get("Id") != container_id or observed.get("Image") != image_id:
            raise DockerLifecycleError("runner_container_identity_mismatch")
        host = observed.get("HostConfig") or {}
        config = observed.get("Config") or {}
        if host.get("ReadonlyRootfs") is not True:
            raise DockerLifecycleError("runner_root_filesystem_not_read_only")
        if config.get("WorkingDir") != "/workspace":
            raise DockerLifecycleError("runner_workdir_mismatch")
        raw_env = config.get("Env") or []
        if not isinstance(raw_env, list):
            raise DockerLifecycleError("runner_environment_shape_invalid")
        effective_env: dict[str, str] = {}
        for row in raw_env:
            if not isinstance(row, str) or "=" not in row:
                raise DockerLifecycleError("runner_environment_entry_invalid")
            key, value = row.split("=", 1)
            if key in effective_env:
                raise DockerLifecycleError("runner_environment_duplicate_key")
            effective_env[key] = value
        if extra := sorted(set(effective_env) - RUNNER_CONTAINER_ENV_ALLOWLIST):
            raise DockerLifecycleError(
                "runner_environment_unadmitted_keys:" + ",".join(extra)
            )
        forbidden = sorted(
            key
            for key in effective_env
            if SECRET_ENV_NAME_RE.search(key) and key != "PG_DSN"
        )
        if forbidden:
            raise DockerLifecycleError(
                "runner_environment_credential_keys_forbidden:" + ",".join(forbidden)
            )
        expected_env = self._runner_environment(database=network_id != "none")
        if any(effective_env.get(key) != value for key, value in expected_env.items()):
            raise DockerLifecycleError("runner_environment_required_value_mismatch")
        if (network_id == "none") != ("PG_DSN" not in effective_env):
            raise DockerLifecycleError("runner_environment_database_dsn_scope_mismatch")
        tmpfs = host.get("Tmpfs") or {}
        if not isinstance(tmpfs, dict) or "/tmp" not in tmpfs:
            raise DockerLifecycleError("runner_tmpfs_missing")
        mounts = [
            item
            for item in (observed.get("Mounts") or [])
            if isinstance(item, dict) and item.get("Destination") == "/workspace"
        ]
        if (
            len(mounts) != 1
            or mounts[0].get("Type") != "bind"
            or mounts[0].get("RW") is not False
            or Path(str(mounts[0].get("Source", ""))).resolve() != self.root
        ):
            raise DockerLifecycleError("runner_source_mount_not_exact_read_only")
        networks = (observed.get("NetworkSettings") or {}).get("Networks") or {}
        if not isinstance(networks, dict):
            raise DockerLifecycleError("runner_network_shape_invalid")
        if network_id == "none":
            if host.get("NetworkMode") != "none" or any(
                (item or {}).get("NetworkID")
                for item in networks.values()
                if isinstance(item, dict)
            ):
                raise DockerLifecycleError("runner_network_not_none")
            network_mode = "none"
        else:
            attached_ids = {
                item.get("NetworkID")
                for item in networks.values()
                if isinstance(item, dict) and item.get("NetworkID")
            }
            if attached_ids != {network_id}:
                raise DockerLifecycleError("runner_network_not_exact_internal_bridge")
            network_mode = "isolated_internal_bridge"
        result = {
            "assurance_mode": effective_env["QT_ASSURANCE_MODE"],
            "container_identity": container_id,
            "external_order_submission_enabled": effective_env[
                "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED"
            ],
            "image_id": image_id,
            "network_identity": network_id,
            "network_mode": network_mode,
            "proof_child_environment_keys": sorted(
                set(expected_env) | ({"PG_DSN"} if network_id != "none" else set())
            ),
            "root_filesystem_mode": "read_only",
            "source_mount_mode": "read_only",
            "writable_tmpfs": "/tmp",
        }
        if network_id != "none":
            result["pg_dsn_sha256"] = _sha256_bytes(
                effective_env["PG_DSN"].encode("utf-8")
            )
        return result

    def _verify_database_configuration(
        self,
        *,
        container_id: str,
        image_id: str,
        network_id: str,
        volume_identity: str,
        published_port: int,
    ) -> dict[str, Any]:
        network = self._resource_inspect("network", network_id)
        self._verify_labels(network, "database_network")
        if network.get("Id") != network_id or network.get("Internal") is not True:
            raise DockerLifecycleError("database_network_not_exact_internal_bridge")
        observed = self._resource_inspect("container", container_id)
        self._verify_labels(observed, "database_container")
        if observed.get("Id") != container_id or observed.get("Image") != image_id:
            raise DockerLifecycleError("database_container_identity_mismatch")
        networks = (observed.get("NetworkSettings") or {}).get("Networks") or {}
        attached_ids = {
            item.get("NetworkID")
            for item in networks.values()
            if isinstance(item, dict) and item.get("NetworkID")
        }
        if attached_ids != {network_id}:
            raise DockerLifecycleError("database_service_network_mismatch")
        bindings = (observed.get("HostConfig") or {}).get("PortBindings") or {}
        port_rows = bindings.get("5432/tcp") if isinstance(bindings, dict) else None
        if (
            not isinstance(port_rows, list)
            or len(port_rows) != 1
            or port_rows[0].get("HostIp") != "127.0.0.1"
            or str(port_rows[0].get("HostPort")) != str(published_port)
        ):
            raise DockerLifecycleError("database_endpoint_not_exact_loopback_publish")
        mounts = [
            item
            for item in (observed.get("Mounts") or [])
            if isinstance(item, dict)
            and item.get("Destination") == "/var/lib/postgresql/data"
        ]
        if (
            len(mounts) != 1
            or mounts[0].get("Type") != "volume"
            or mounts[0].get("Name") != volume_identity
        ):
            raise DockerLifecycleError("database_volume_mount_mismatch")
        return {
            "container_identity": container_id,
            "image_id": image_id,
            "network_identity": network_id,
            "network_internal": True,
            "publish_host": "127.0.0.1",
            "published_port": published_port,
            "volume_identity": volume_identity,
        }

    def verify_observed_configuration(
        self, prepared: ProvisionedProfile
    ) -> dict[str, Any]:
        """Re-inspect the runtime; successful finalization uses observed facts only."""

        self.verify_admission()
        runner = self._verify_runner_configuration(
            prepared.runner_container_id,
            prepared.runner_image_id,
            "none" if prepared.execution_class == "isolated_container" else str(prepared.network_id),
        )
        facts: dict[str, Any] = {"runner": runner}
        if prepared.execution_class == "isolated_database":
            if (
                prepared.database_container_id is None
                or prepared.network_id is None
                or prepared.published_port is None
            ):
                raise DockerLifecycleError("database_observed_configuration_incomplete")
            volume_identity = next(
                item.runtime_identity
                for item in prepared.resources
                if item.kind == "volume" and item.logical_name == "database-data"
            )
            service_id = next(iter(self.admission["service_images"]))
            facts["database_service"] = self._verify_database_configuration(
                container_id=prepared.database_container_id,
                image_id=self.admission["service_images"][service_id]["image_id"],
                network_id=prepared.network_id,
                volume_identity=volume_identity,
                published_port=prepared.published_port,
            )
        prepared.observed_configuration = facts
        return facts

    def verify_runner_image(self) -> str:
        expected = self.admission["runner_image"]
        image_id = expected["image_id"]
        if not HEX_IMAGE_RE.fullmatch(image_id):
            raise DockerLifecycleError("admitted_runner_image_id_invalid")
        observed = self._image_inspect(image_id)
        if observed.get("Id") != image_id:
            raise DockerLifecycleError("admitted_runner_image_id_mismatch")
        platform = f"{observed.get('Os')}/{observed.get('Architecture')}"
        if platform != expected["platform"]:
            raise DockerLifecycleError("admitted_runner_image_platform_mismatch")
        config = observed.get("Config") or {}
        labels = config.get("Labels") or {}
        if not isinstance(labels, dict) or labels.get(BUILD_DEFINITION_LABEL) != expected[
            "build_definition"
        ]["sha256"]:
            raise DockerLifecycleError("admitted_runner_build_definition_label_mismatch")
        return image_id

    def verify_service_image(self, service_id: str) -> str:
        expected = self.admission["service_images"][service_id]
        reference = expected["reference"]
        image_id = expected["image_id"]
        digest = expected["image_digest"]
        if not HEX_IMAGE_RE.fullmatch(image_id) or not HEX_IMAGE_RE.fullmatch(digest):
            raise DockerLifecycleError("admitted_service_image_identity_invalid")
        if not reference.endswith("@" + digest):
            raise DockerLifecycleError("admitted_service_reference_digest_mismatch")
        observed = self._image_inspect(reference)
        if observed.get("Id") != image_id:
            raise DockerLifecycleError("admitted_service_image_id_mismatch")
        repo_digests = observed.get("RepoDigests") or []
        if not isinstance(repo_digests, list) or not any(
            isinstance(item, str) and item.endswith("@" + digest)
            for item in repo_digests
        ):
            raise DockerLifecycleError("admitted_service_repo_digest_missing")
        if self._image_inspect(image_id).get("Id") != image_id:
            raise DockerLifecycleError("admitted_service_image_id_recheck_failed")
        return image_id

    def _assert_control_before_mutation(self) -> None:
        self.verify_admission()

    def _labels(self) -> list[str]:
        values = {
            SESSION_LABEL: self.attestation_id,
            PROFILE_LABEL: self.profile_id,
            SOURCE_LABEL: self.source_commit,
            INSTANCE_LABEL: self.environment_instance_id,
        }
        result: list[str] = []
        for key, value in sorted(values.items()):
            result.extend(["--label", f"{key}={value}"])
        return result

    def _label_filters(self) -> list[str]:
        values = {
            SESSION_LABEL: self.attestation_id,
            PROFILE_LABEL: self.profile_id,
            SOURCE_LABEL: self.source_commit,
            INSTANCE_LABEL: self.environment_instance_id,
        }
        result: list[str] = []
        for key, value in sorted(values.items()):
            result.extend(["--filter", f"label={key}={value}"])
        return result

    def planned_resources(self, execution_class: str) -> list[dict[str, str]]:
        if execution_class == "isolated_container":
            rows = [
                ("container", "proof-runner"),
                ("source_snapshot", "exact-source-snapshot"),
            ]
        elif execution_class == "isolated_database":
            rows = [
                ("container", "database-service"),
                ("container", "proof-runner"),
                ("database", "session-database"),
                ("network", "session-network"),
                ("published_endpoint", "database-loopback-endpoint"),
                ("source_snapshot", "exact-source-snapshot"),
                ("temporary_secret_file", "database-service-env"),
                ("temporary_secret_file", "proof-runner-env"),
                ("volume", "database-data"),
            ]
        else:
            raise DockerLifecycleError(f"unsupported_execution_class:{execution_class}")
        return [
            {"kind": kind, "logical_name": name}
            for kind, name in sorted(rows)
        ]

    def register_source_snapshot(self, snapshot_sha256: str) -> None:
        """Track the extracted exact Git archive before Docker mutation."""

        try:
            relative = self.root.absolute().relative_to(self.private_root.absolute())
        except ValueError as exc:
            raise DockerLifecycleError("source_snapshot_must_be_inside_private_root") from exc
        if relative != Path("source"):
            raise DockerLifecycleError("source_snapshot_path_not_exact")
        try:
            observed = os.lstat(self.root)
        except FileNotFoundError:
            observed = None
        if observed is not None:
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise DockerLifecycleError("source_snapshot_invalid")
            if (self.root / ".git").exists() or (self.root / ".git").is_symlink():
                raise DockerLifecycleError("source_snapshot_invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha256):
            raise DockerLifecycleError("source_snapshot_hash_invalid")
        resource = ResourceIdentity(
            "source_snapshot", "exact-source-snapshot", f"git-archive:{snapshot_sha256}"
        )
        self._partial_resources.append(resource)
        self._partial_targets[(resource.kind, resource.logical_name)] = self.root

    def register_recovery_local_resources(
        self,
        *,
        source_snapshot_sha256: str,
        planned_resources: Sequence[Mapping[str, str]],
    ) -> None:
        """Register only executor-created local targets for cleanup recovery.

        The recovery caller derives ``private_root`` and ``root`` from the
        immutable draft identities.  This method never recursively scans a
        caller path: it considers only the exact source child and the two
        private env-file patterns created by this controller.  Secret values
        are loaded into the in-memory redaction set before cleanup output can
        be made durable.
        """

        planned = {
            (str(item.get("kind", "")), str(item.get("logical_name", "")))
            for item in planned_resources
        }
        source_key = ("source_snapshot", "exact-source-snapshot")
        if source_key not in planned:
            raise DockerLifecycleError("recovery_source_snapshot_not_planned")
        expected_root = self.private_root / "source"
        if self.root != expected_root:
            raise DockerLifecycleError("recovery_source_snapshot_path_mismatch")
        self.register_source_snapshot(source_snapshot_sha256)

        secret_names = sorted(
            logical_name
            for kind, logical_name in planned
            if kind == "temporary_secret_file"
        )
        unexpected = set(secret_names) - {
            "database-service-env",
            "proof-runner-env",
        }
        if unexpected:
            raise DockerLifecycleError(
                "recovery_secret_resource_unknown:" + ",".join(sorted(unexpected))
            )
        self._recovery_local_inventory_required = True
        if not self.private_root.exists():
            return
        root_stat = os.lstat(self.private_root)
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise DockerLifecycleError("recovery_private_profile_not_directory")
        entries = list(self.private_root.iterdir())
        expected_keys = {
            "database-service-env": {
                b"POSTGRES_DB",
                b"POSTGRES_PASSWORD",
                b"POSTGRES_USER",
            },
            "proof-runner-env": {b"PG_DSN"},
        }
        for logical_name in secret_names:
            prefix = _safe_name(logical_name)
            pattern = re.compile(rf"{re.escape(prefix)}-[0-9a-f]{{16}}\.env\Z")
            candidates = sorted(path for path in entries if pattern.fullmatch(path.name))
            for index, path in enumerate(candidates, start=1):
                observed = os.lstat(path)
                if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
                    raise DockerLifecycleError("recovery_secret_target_not_regular_file")
                if observed.st_size > 64 * 1024:
                    raise DockerLifecycleError("recovery_secret_target_too_large")
                if observed.st_mode & 0o077:
                    raise DockerLifecycleError("recovery_secret_permissions_not_private")
                raw = path.read_bytes()
                values: dict[bytes, bytes] = {}
                for line in raw.splitlines():
                    if b"=" not in line:
                        raise DockerLifecycleError("recovery_secret_file_invalid")
                    key, value = line.split(b"=", 1)
                    if (
                        not re.fullmatch(rb"[A-Z][A-Z0-9_]*", key)
                        or not value
                        or key in values
                    ):
                        raise DockerLifecycleError("recovery_secret_file_invalid")
                    values[key] = value
                if set(values) != expected_keys[logical_name]:
                    raise DockerLifecycleError("recovery_secret_file_keys_mismatch")
                self._secret_tokens.extend(values.values())
                recovered_name = (
                    logical_name
                    if len(candidates) == 1
                    else f"{logical_name}-recovered-{index:03d}"
                )
                self._reserve_secret_file(recovered_name, path)

    def _reserve_secret_file(self, logical_name: str, path: Path) -> None:
        """Track the cleanup target before the first possible file-system write."""

        try:
            path.resolve().relative_to(self.private_root)
        except ValueError as exc:
            raise DockerLifecycleError("private_secret_must_be_inside_private_root") from exc
        resource = ResourceIdentity(
            "temporary_secret_file",
            logical_name,
            "secret-file:" + _sha256_bytes(str(path).encode("utf-8")),
        )
        self._partial_resources.append(resource)
        self._partial_targets[(resource.kind, resource.logical_name)] = path

    def _runner_environment(self, *, database: bool) -> dict[str, str]:
        env = {
            "HOME": "/tmp",
            "LANG": "C.UTF-8",
            "NO_COLOR": "1",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONHASHSEED": "0",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTEST_ADDOPTS": "-p no:cacheprovider",
            "PYTEST_PLUGINS": "scripts.assurance.pytest_result_plugin",
            "PYTHONPATH": "/workspace",
            "QT_ASSURANCE_MODE": "1",
            "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED": "0",
            "TMPDIR": "/tmp",
            "TZ": "UTC",
        }
        if database:
            env.update(
                {
                    "QT_DB_TEST_ISOLATED": "1",
                    "RUN_DB_TESTS": "1",
                }
            )
        return env

    def _create_runner(
        self,
        *,
        image_id: str,
        network: str,
        name: str,
        logical_name: str,
        env: Mapping[str, str],
        env_file: Path | None,
    ) -> str:
        self._assert_control_before_mutation()
        if self.verify_runner_image() != image_id:
            raise DockerLifecycleError("runner_image_changed_before_create")
        args = [
            "create",
            "--name",
            name,
            *self._labels(),
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=1g",
            "--mount",
            f"type=bind,src={self.root},dst=/workspace,readonly",
            "--workdir",
            "/workspace",
            "--network",
            network,
        ]
        for key, value in sorted(env.items()):
            args.extend(["--env", f"{key}={value}"])
        if env_file is not None:
            args.extend(["--env-file", str(env_file)])
        args.extend(
            [
                image_id,
                "python",
                "-c",
                "import signal; signal.pause()",
            ]
        )
        result = self._call(args, check=True)
        container_id = result.stdout.decode("utf-8", errors="strict").strip()
        if not container_id:
            raise DockerLifecycleError("runner_container_identity_missing")
        resource = ResourceIdentity("container", logical_name, container_id)
        self._partial_resources.append(resource)
        self._partial_targets[(resource.kind, resource.logical_name)] = container_id
        self._call(["start", container_id], check=True)
        return container_id

    def _probe_runner_tools(
        self, container_id: str, *, require_node: bool
    ) -> tuple[dict[str, str], list[str], list[str]]:
        tools = ["python"] + (["node"] if require_node else [])
        versions: dict[str, str] = {}
        stdout_rows: list[str] = []
        stderr_rows: list[str] = []
        for tool in tools:
            result = self._call(["exec", container_id, tool, "--version"], check=True)
            stdout_rows.append(result.stdout.decode("utf-8", errors="replace"))
            stderr_rows.append(result.stderr.decode("utf-8", errors="replace"))
            value = (result.stdout + result.stderr).decode(
                "utf-8", errors="replace"
            ).strip()
            if not value:
                raise DockerLifecycleError(f"runner_tool_version_missing:{tool}")
            versions[tool] = value
        return versions, stdout_rows, stderr_rows

    def provision(
        self,
        *,
        execution_class: str,
        require_node: bool,
        service_definition: Mapping[str, Any] | None = None,
    ) -> ProvisionedProfile:
        if self._last_provisioned is not None:
            raise DockerLifecycleError("profile_already_provisioned")
        runner_image_id = self.verify_runner_image()
        prefix = _safe_name(
            f"qt-assurance-{self.environment_instance_id}-{self.profile_id}", limit=55
        )
        resources = self._partial_resources
        targets = self._partial_targets
        stdout_rows: list[str] = []
        stderr_rows: list[str] = []
        if execution_class == "isolated_container":
            runner_id = self._create_runner(
                image_id=runner_image_id,
                network="none",
                name=f"{prefix}-runner",
                logical_name="proof-runner",
                env=self._runner_environment(database=False),
                env_file=None,
            )
            versions, out, err = self._probe_runner_tools(
                runner_id, require_node=require_node
            )
            stdout_rows.extend(out)
            stderr_rows.extend(err)
            prepared = ProvisionedProfile(
                profile_id=self.profile_id,
                execution_class=execution_class,
                runner_container_id=runner_id,
                runner_image_id=runner_image_id,
                resources=resources,
                resource_targets=targets,
                tool_versions=versions,
                bootstrap_stdout="".join(stdout_rows),
                bootstrap_stderr="".join(stderr_rows),
            )
            self._last_provisioned = prepared
            self.verify_observed_configuration(prepared)
            return prepared

        if execution_class != "isolated_database" or service_definition is None:
            raise DockerLifecycleError(f"unsupported_execution_class:{execution_class}")
        service_id = str(service_definition["id"])
        service_image_id = self.verify_service_image(service_id)
        username, database, password = _new_database_credentials()
        self._secret_tokens.append(password.encode("utf-8"))
        service_env = _private_env_file(
            self.private_root,
            "database-service-env",
            {
                "POSTGRES_DB": database,
                "POSTGRES_PASSWORD": password,
                "POSTGRES_USER": username,
            },
            reserve=lambda path: self._reserve_secret_file(
                "database-service-env", path
            ),
        )

        self._assert_control_before_mutation()
        volume_name = f"{prefix}-data"
        volume_result = self._call(
            ["volume", "create", *self._labels(), volume_name], check=True
        )
        volume_identity = volume_result.stdout.decode("utf-8", errors="strict").strip()
        resource = ResourceIdentity("volume", "database-data", volume_identity)
        resources.append(resource)
        targets[(resource.kind, resource.logical_name)] = volume_identity

        self._assert_control_before_mutation()
        network_name = f"{prefix}-network"
        network_result = self._call(
            ["network", "create", "--internal", *self._labels(), network_name],
            check=True,
        )
        network_id = network_result.stdout.decode("utf-8", errors="strict").strip()
        resource = ResourceIdentity("network", "session-network", network_id)
        resources.append(resource)
        targets[(resource.kind, resource.logical_name)] = network_id

        self._assert_control_before_mutation()
        if self.verify_service_image(service_id) != service_image_id:
            raise DockerLifecycleError("service_image_changed_before_create")
        database_name = f"{prefix}-db"
        database_create = self._call(
            [
                "create",
                "--name",
                database_name,
                *self._labels(),
                "--network",
                network_id,
                "--network-alias",
                "qt-assurance-db",
                "--publish",
                "127.0.0.1::5432",
                "--mount",
                f"type=volume,src={volume_identity},dst=/var/lib/postgresql/data",
                "--env-file",
                str(service_env),
                service_image_id,
            ],
            check=True,
        )
        database_container_id = database_create.stdout.decode(
            "utf-8", errors="strict"
        ).strip()
        resource = ResourceIdentity(
            "container", "database-service", database_container_id
        )
        resources.append(resource)
        targets[(resource.kind, resource.logical_name)] = database_container_id
        self._call(["start", database_container_id], check=True)

        readiness: CommandResult | None = None
        readiness_deadline = time.monotonic() + 30.0
        while time.monotonic() < readiness_deadline:
            readiness = self._call(
                [
                    "exec",
                    database_container_id,
                    "pg_isready",
                    "-U",
                    username,
                    "-d",
                    database,
                ],
                timeout_seconds=5,
            )
            if readiness.exit_code == 0:
                break
            time.sleep(0.5)
        if readiness is None or readiness.exit_code != 0:
            raise DockerLifecycleError("database_readiness_failed")
        stdout_rows.append(readiness.stdout.decode("utf-8", errors="replace"))
        stderr_rows.append(readiness.stderr.decode("utf-8", errors="replace"))
        for extension in sorted(service_definition["required_extensions"]):
            result = self._call(
                [
                    "exec",
                    database_container_id,
                    "psql",
                    "-U",
                    username,
                    "-d",
                    database,
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    f'CREATE EXTENSION IF NOT EXISTS "{extension}"',
                ],
                check=True,
            )
            stdout_rows.append(result.stdout.decode("utf-8", errors="replace"))
            stderr_rows.append(result.stderr.decode("utf-8", errors="replace"))
        port_result = self._call(
            ["port", database_container_id, "5432/tcp"], check=True
        )
        port_text = port_result.stdout.decode("utf-8", errors="strict").strip()
        match = re.fullmatch(r"127\.0\.0\.1:([0-9]{1,5})", port_text)
        if not match:
            raise DockerLifecycleError("database_published_endpoint_not_loopback_ephemeral")
        published_port = int(match.group(1))
        if not 1 <= published_port <= 65535:
            raise DockerLifecycleError("database_published_port_invalid")

        runner_dsn = (
            f"postgresql://{username}:{password}@qt-assurance-db:5432/{database}"
        )
        self._secret_tokens.append(runner_dsn.encode("utf-8"))
        runner_env = _private_env_file(
            self.private_root,
            "proof-runner-env",
            {"PG_DSN": runner_dsn},
            reserve=lambda path: self._reserve_secret_file(
                "proof-runner-env", path
            ),
        )
        runner_id = self._create_runner(
            image_id=runner_image_id,
            network=network_id,
            name=f"{prefix}-runner",
            logical_name="proof-runner",
            env=self._runner_environment(database=True),
            env_file=runner_env,
        )

        database_identity = ResourceIdentity(
            "database", "session-database", database
        )
        endpoint_identity = ResourceIdentity(
            "published_endpoint",
            "database-loopback-endpoint",
            f"127.0.0.1:{published_port}",
        )
        resources.extend([database_identity, endpoint_identity])
        targets[(database_identity.kind, database_identity.logical_name)] = (
            database_container_id,
            database,
        )
        targets[(endpoint_identity.kind, endpoint_identity.logical_name)] = (
            "127.0.0.1",
            published_port,
            database_container_id,
        )

        versions, out, err = self._probe_runner_tools(
            runner_id, require_node=require_node
        )
        stdout_rows.extend(out)
        stderr_rows.extend(err)
        query = self._call(
            [
                "exec",
                database_container_id,
                "psql",
                "-U",
                username,
                "-d",
                database,
                "-At",
                "-c",
                "SHOW server_version",
            ],
            check=True,
        )
        postgresql_version = query.stdout.decode("utf-8", errors="strict").strip()
        extension_versions: dict[str, str] = {}
        for extension in sorted(service_definition["required_extensions"]):
            extension_result = self._call(
                [
                    "exec",
                    database_container_id,
                    "psql",
                    "-U",
                    username,
                    "-d",
                    database,
                    "-At",
                    "-c",
                    f"SELECT extversion FROM pg_extension WHERE extname = '{extension}'",
                ],
                check=True,
            )
            extension_versions[extension] = extension_result.stdout.decode(
                "utf-8", errors="strict"
            ).strip()
        service_facts = {
            "container_identity": database_container_id,
            "database_identity": database,
            "extension_versions": extension_versions,
            "image_digest": self.admission["service_images"][service_id][
                "image_digest"
            ],
            "pg_dsn_sha256": _sha256_bytes(runner_dsn.encode("utf-8")),
            "postgresql_version": postgresql_version,
            "published_port": published_port,
            "session_isolation_key_sha256": _sha256_bytes(
                f"{self.attestation_id}:{self.environment_instance_id}:{database}".encode(
                    "utf-8"
                )
            ),
            "timescaledb_version": extension_versions.get("timescaledb", ""),
        }
        prepared = ProvisionedProfile(
            profile_id=self.profile_id,
            execution_class=execution_class,
            runner_container_id=runner_id,
            runner_image_id=runner_image_id,
            resources=resources,
            resource_targets=targets,
            tool_versions=versions,
            bootstrap_stdout="".join(stdout_rows),
            bootstrap_stderr="".join(stderr_rows),
            network_id=network_id,
            database_container_id=database_container_id,
            database_name=database,
            published_port=published_port,
            service_facts=service_facts,
        )
        self._last_provisioned = prepared
        self.verify_observed_configuration(prepared)
        return prepared

    def partial_profile(self, execution_class: str) -> ProvisionedProfile:
        """Expose only tracked identities so an incomplete provision can clean up."""

        runner_id = next(
            (
                item.runtime_identity
                for item in self._partial_resources
                if item.kind == "container" and item.logical_name == "proof-runner"
            ),
            "unprovisioned",
        )
        return ProvisionedProfile(
            profile_id=self.profile_id,
            execution_class=execution_class,
            runner_container_id=runner_id,
            runner_image_id=self.admission["runner_image"]["image_id"],
            resources=list(self._partial_resources),
            resource_targets=dict(self._partial_targets),
            tool_versions={},
            bootstrap_stdout="",
            bootstrap_stderr="",
        )

    def proof_argv(
        self, inner_argv: Sequence[str], *, timeout_seconds: int
    ) -> list[str]:
        if self._last_provisioned is None:
            raise DockerLifecycleError("profile_not_provisioned")
        return [
            self.docker_path,
            "exec",
            self._last_provisioned.runner_container_id,
            "python",
            "/workspace/scripts/assurance/process_guard.py",
            "--timeout",
            str(timeout_seconds),
            "--",
            *inner_argv,
        ]

    def terminate_runner(self) -> None:
        """Invalidate a runner after a host fail-safe; no later proof may use it."""

        if self._last_provisioned is None:
            return
        container_id = self._last_provisioned.runner_container_id
        result = self._call(["kill", container_id])
        if result.exit_code != 0 and not self._not_found(result):
            raise DockerLifecycleError("runner_kill_failed_after_host_timeout")
        inspected = self._call(["container", "inspect", container_id])
        if inspected.exit_code == 0:
            payload = self._json(inspected.stdout, "runner_post_kill_inspect")
            if (
                not isinstance(payload, list)
                or len(payload) != 1
                or (payload[0].get("State") or {}).get("Running") is not False
            ):
                raise DockerLifecycleError("runner_still_running_after_host_timeout")

    def probe_required_executable(self, executable: str) -> str | None:
        if self._last_provisioned is None:
            raise DockerLifecycleError("profile_not_provisioned")
        if not re.fullmatch(r"[a-zA-Z0-9_.+-]+", executable):
            raise DockerLifecycleError("required_executable_name_invalid")
        result = self._call(
            [
                "exec",
                self._last_provisioned.runner_container_id,
                executable,
                "--version",
            ],
            timeout_seconds=10,
        )
        if result.exit_code != 0:
            return None
        version = (result.stdout + result.stderr).decode(
            "utf-8", errors="replace"
        ).strip()
        return version or None

    def discover_labeled_resources(self) -> list[ResourceIdentity]:
        """Add label-visible partial resources before manifesting an error."""

        self.verify_admission()
        known = {
            (item.kind, item.runtime_identity) for item in self._partial_resources
        }
        label_filters = self._label_filters()
        discovered: list[ResourceIdentity] = []
        for kind, args in (
            ("container", ["ps", "-aq", "--no-trunc", *label_filters]),
            ("network", ["network", "ls", "-q", "--no-trunc", *label_filters]),
            ("volume", ["volume", "ls", "-q", *label_filters]),
        ):
            result = self._call(args)
            if result.exit_code != 0:
                raise DockerLifecycleError(f"docker_{kind}_label_query_failed")
            for raw_identity in result.stdout.decode(
                "utf-8", errors="strict"
            ).splitlines():
                identity = raw_identity.strip()
                if not identity or (kind, identity) in known:
                    continue
                observed = self._resource_inspect(kind, identity)
                self._verify_labels(observed, f"discovered_{kind}")
                logical_name = self._verified_resource_logical_name(
                    kind, identity, observed
                )
                existing = next(
                    (
                        item
                        for item in self._partial_resources
                        if item.kind == kind and item.logical_name == logical_name
                    ),
                    None,
                )
                if existing is not None and existing.runtime_identity != identity:
                    raise DockerLifecycleError(
                        f"discovered_{kind}:logical_identity_collision"
                    )
                item = ResourceIdentity(kind, logical_name, identity)
                self._partial_resources.append(item)
                self._partial_targets[(kind, logical_name)] = identity
                known.add((kind, identity))
                discovered.append(item)
        return discovered

    def _verified_resource_logical_name(
        self, kind: str, identity: str, observed: Mapping[str, Any]
    ) -> str:
        prefix = _safe_name(
            f"qt-assurance-{self.environment_instance_id}-{self.profile_id}",
            limit=55,
        )
        expected_names = {
            "container": {
                f"{prefix}-runner": "proof-runner",
                f"{prefix}-db": "database-service",
            },
            "network": {f"{prefix}-network": "session-network"},
            "volume": {f"{prefix}-data": "database-data"},
        }
        if kind == "container":
            observed_identity = observed.get("Id")
            observed_name = str(observed.get("Name", "")).removeprefix("/")
        elif kind == "network":
            observed_identity = observed.get("Id")
            observed_name = observed.get("Name")
        elif kind == "volume":
            observed_identity = observed.get("Name")
            observed_name = observed.get("Name")
        else:  # pragma: no cover - caller owns the closed Docker-kind set
            raise DockerLifecycleError(f"discovered_resource_kind_unsupported:{kind}")
        if observed_identity != identity:
            raise DockerLifecycleError(f"discovered_{kind}:identity_mismatch")
        logical_name = expected_names[kind].get(str(observed_name))
        if logical_name is None:
            raise DockerLifecycleError(f"discovered_{kind}:name_mismatch")
        return logical_name

    def _docker_cleanup_target_present(
        self, resource: ResourceIdentity, target: Any
    ) -> bool:
        self._assert_control_before_mutation()
        result = self._call([resource.kind, "inspect", str(target)])
        if self._not_found(result):
            return False
        if result.exit_code != 0:
            raise DockerLifecycleError(
                f"cleanup_{resource.kind}_inspect_failed:{resource.logical_name}"
            )
        payload = self._json(result.stdout, f"cleanup_{resource.kind}_inspect")
        if (
            not isinstance(payload, list)
            or len(payload) != 1
            or not isinstance(payload[0], dict)
        ):
            raise DockerLifecycleError(
                f"cleanup_{resource.kind}_inspect_shape_invalid"
            )
        observed = payload[0]
        self._verify_labels(observed, f"cleanup_{resource.kind}")
        logical_name = self._verified_resource_logical_name(
            resource.kind, str(target), observed
        )
        if logical_name != resource.logical_name:
            raise DockerLifecycleError(
                f"cleanup_{resource.kind}:logical_name_mismatch"
            )
        return True

    @staticmethod
    def _not_found(result: CommandResult) -> bool:
        if result.exit_code == 0:
            return False
        text = (result.stdout + result.stderr).decode(
            "utf-8", errors="replace"
        ).lower()
        return "no such" in text or "not found" in text

    def _resource_absent(
        self,
        resource: ResourceIdentity,
        target: Any,
        container_absence: Mapping[str, bool],
    ) -> bool:
        if resource.kind == "container":
            result = self._call(["container", "inspect", str(target)])
            return self._not_found(result)
        if resource.kind == "network":
            result = self._call(["network", "inspect", str(target)])
            return self._not_found(result)
        if resource.kind == "volume":
            result = self._call(["volume", "inspect", str(target)])
            return self._not_found(result)
        if resource.kind in {"temporary_secret_file", "source_snapshot"}:
            try:
                os.lstat(Path(target))
            except FileNotFoundError:
                return True
            return False
        if resource.kind == "database":
            container_id, _ = target
            return bool(container_absence.get(container_id))
        if resource.kind == "published_endpoint":
            host, port, container_id = target
            if not container_absence.get(container_id):
                return False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.2)
                return probe.connect_ex((host, int(port))) != 0
        return False

    def _recovery_local_residue_dispositions(self) -> list[CleanupResource]:
        """Describe, but never traverse or remove, unknown private-root children."""

        if not self._recovery_local_inventory_required:
            return []
        try:
            observed = os.lstat(self.private_root)
        except FileNotFoundError:
            return []
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise DockerLifecycleError("recovery_private_profile_not_directory")
        dispositions: list[CleanupResource] = []
        for index, child in enumerate(
            sorted(self.private_root.iterdir(), key=lambda path: os.fsencode(path.name)),
            start=1,
        ):
            # This is deliberately a top-level lstat only.  Unknown content is
            # never opened, traversed, or deleted, and its raw name is not
            # emitted into the durable recovery report.
            os.lstat(child)
            name_hash = _sha256_bytes(os.fsencode(child.name))
            dispositions.append(
                CleanupResource(
                    kind="private_residue",
                    logical_name=f"unrecognized-private-residue-{index:03d}",
                    runtime_identity=f"path-name-sha256:{name_hash}",
                    absent=False,
                )
            )
        return dispositions

    def cleanup(self, prepared: ProvisionedProfile) -> CleanupReport:
        stdout_rows: list[str] = []
        stderr_rows: list[str] = []
        removal_failed = False
        control_plane_valid = True
        try:
            self.verify_admission()
        except DockerLifecycleError as exc:
            control_plane_valid = False
            removal_failed = True
            stderr_rows.append(
                f"admitted control plane unavailable before cleanup:{type(exc).__name__}\n"
            )
        if control_plane_valid:
            try:
                self.discover_labeled_resources()
            except DockerLifecycleError as exc:
                removal_failed = True
                stderr_rows.append(
                    f"labeled resource discovery failed:{type(exc).__name__}\n"
                )
        if prepared.resources is not self._partial_resources:
            known = {
                (item.kind, item.runtime_identity) for item in prepared.resources
            }
            for item in self._partial_resources:
                if (item.kind, item.runtime_identity) not in known:
                    prepared.resources.append(item)
                    prepared.resource_targets[(item.kind, item.logical_name)] = (
                        self._partial_targets[(item.kind, item.logical_name)]
                    )
        resources = sorted(
            prepared.resources, key=lambda item: (item.kind, item.logical_name)
        )

        # Every removal is attempted even after an earlier failure.
        removal_order = [
            item
            for kind in (
                "container",
                "temporary_secret_file",
                "source_snapshot",
                "volume",
                "network",
            )
            for item in resources
            if item.kind == kind
        ]
        for resource in removal_order:
            try:
                target = prepared.resource_targets[
                    (resource.kind, resource.logical_name)
                ]
            except (KeyError, TypeError) as exc:
                removal_failed = True
                stderr_rows.append(
                    f"remove target missing {resource.kind} {resource.logical_name}:"
                    f"{type(exc).__name__}\n"
                )
                continue
            if resource.kind in {"temporary_secret_file", "source_snapshot"}:
                try:
                    if resource.kind == "source_snapshot":
                        exact_target = self.private_root / "source"
                        target_path = Path(target)
                        if target_path.absolute() != exact_target.absolute():
                            raise DockerLifecycleError(
                                "cleanup_source_snapshot_path_not_exact"
                            )
                        try:
                            observed = os.lstat(target_path)
                        except FileNotFoundError:
                            observed = None
                        if observed is not None:
                            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(
                                observed.st_mode
                            ):
                                raise DockerLifecycleError(
                                    "cleanup_source_snapshot_target_unsafe"
                                )
                            for child in target_path.rglob("*"):
                                child_stat = os.lstat(child)
                                if stat.S_ISLNK(child_stat.st_mode):
                                    continue
                                if stat.S_ISDIR(child_stat.st_mode):
                                    child.chmod(0o700)
                                elif stat.S_ISREG(child_stat.st_mode):
                                    child.chmod(0o600)
                            target_path.chmod(0o700)
                            shutil.rmtree(target_path)
                    else:
                        Path(target).unlink(missing_ok=True)
                    stdout_rows.append(
                        f"removed {resource.kind} {resource.logical_name}\n"
                    )
                except (OSError, ValueError, DockerLifecycleError) as exc:
                    removal_failed = True
                    stderr_rows.append(
                        f"remove failed {resource.kind} {resource.logical_name}:"
                        f"{type(exc).__name__}\n"
                    )
                continue
            if not control_plane_valid:
                stderr_rows.append(
                    f"remove not attempted {resource.kind} {resource.logical_name}:"
                    "admitted-control-plane-unavailable\n"
                )
                continue
            try:
                if not self._docker_cleanup_target_present(resource, target):
                    stdout_rows.append(
                        f"already absent {resource.kind} {resource.logical_name}\n"
                    )
                    continue
            except DockerLifecycleError as exc:
                removal_failed = True
                stderr_rows.append(
                    f"cleanup target verification failed {resource.kind} "
                    f"{resource.logical_name}:{type(exc).__name__}\n"
                )
                continue
            command = {
                "container": ["rm", "--force", str(target)],
                "volume": ["volume", "rm", "--force", str(target)],
                "network": ["network", "rm", str(target)],
            }[resource.kind]
            try:
                # The admitted daemon may change after target inspection.  Bind
                # the control plane again at the final boundary before every rm.
                self.verify_admission()
            except DockerLifecycleError as exc:
                control_plane_valid = False
                removal_failed = True
                stderr_rows.append(
                    f"remove control-plane recheck failed {resource.kind} "
                    f"{resource.logical_name}:"
                    f"{type(exc).__name__}\n"
                )
                continue
            try:
                result = self._call(command)
            except DockerLifecycleError as exc:
                removal_failed = True
                stderr_rows.append(
                    f"remove command failed {resource.kind} {resource.logical_name}:"
                    f"{type(exc).__name__}\n"
                )
                continue
            stdout_rows.append(result.stdout.decode("utf-8", errors="replace"))
            stderr_rows.append(result.stderr.decode("utf-8", errors="replace"))
            if result.exit_code != 0 and not self._not_found(result):
                removal_failed = True

        container_absence: dict[str, bool] = {}
        for resource in resources if control_plane_valid else ():
            if resource.kind != "container":
                continue
            try:
                target = prepared.resource_targets[
                    (resource.kind, resource.logical_name)
                ]
                result = self._call(["container", "inspect", str(target)])
                container_absence[str(target)] = self._not_found(result)
            except (KeyError, TypeError, DockerLifecycleError) as exc:
                removal_failed = True
                stderr_rows.append(
                    f"absence query failed {resource.kind} {resource.logical_name}:"
                    f"{type(exc).__name__}\n"
                )

        dispositions_list: list[CleanupResource] = []
        for resource in resources:
            try:
                target = prepared.resource_targets[
                    (resource.kind, resource.logical_name)
                ]
                if not control_plane_valid and resource.kind not in {
                    "temporary_secret_file",
                    "source_snapshot",
                }:
                    absent = False
                else:
                    absent = self._resource_absent(
                        resource, target, container_absence
                    )
            except (KeyError, TypeError, ValueError, OSError, DockerLifecycleError) as exc:
                absent = False
                removal_failed = True
                stderr_rows.append(
                    f"absence evaluation failed {resource.kind} {resource.logical_name}:"
                    f"{type(exc).__name__}\n"
                )
            dispositions_list.append(
                CleanupResource(
                    resource.kind,
                    resource.logical_name,
                    resource.runtime_identity,
                    absent,
                )
            )
        remaining: list[str] = []
        label_filters = self._label_filters()
        if control_plane_valid:
            try:
                self.verify_admission()
            except DockerLifecycleError as exc:
                control_plane_valid = False
                removal_failed = True
                stderr_rows.append(
                    "admitted control plane unavailable before final absence query:"
                    f"{type(exc).__name__}\n"
                )
        if control_plane_valid:
            for kind, args in (
                ("container", ["ps", "-aq", *label_filters]),
                ("network", ["network", "ls", "-q", *label_filters]),
                ("volume", ["volume", "ls", "-q", *label_filters]),
            ):
                try:
                    result = self._call(args)
                except DockerLifecycleError as exc:
                    removal_failed = True
                    remaining.append(f"query-error:{kind}")
                    stderr_rows.append(
                        f"label query failed {kind}:{type(exc).__name__}\n"
                    )
                    continue
                stdout_rows.append(result.stdout.decode("utf-8", errors="replace"))
                stderr_rows.append(result.stderr.decode("utf-8", errors="replace"))
                if result.exit_code != 0:
                    removal_failed = True
                    remaining.append(f"query-error:{kind}")
                    continue
                for identity in result.stdout.decode("utf-8", errors="strict").splitlines():
                    if identity.strip():
                        remaining.append(f"{kind}:{identity.strip()}")
        else:
            remaining.append("control-plane-identity-unverified")
        if self._recovery_local_inventory_required:
            try:
                residue_dispositions = self._recovery_local_residue_dispositions()
                if residue_dispositions:
                    dispositions_list.extend(residue_dispositions)
                    remaining.append("local-residue:unrecognized")
                    removal_failed = True
            except (OSError, DockerLifecycleError) as exc:
                removal_failed = True
                remaining.append("query-error:local-residue")
                stderr_rows.append(
                    "private recovery residue query failed:"
                    f"{type(exc).__name__}\n"
                )
        if control_plane_valid:
            try:
                self.verify_admission()
            except DockerLifecycleError as exc:
                removal_failed = True
                remaining.append("control-plane-identity-unverified-after-absence")
                stderr_rows.append(
                    "admitted control plane unavailable after absence query:"
                    f"{type(exc).__name__}\n"
                )
        remaining.sort()
        dispositions = tuple(dispositions_list)
        all_absent = all(item.absent for item in dispositions)
        exit_code = 0 if all_absent and not remaining and not removal_failed else 1
        return CleanupReport(
            stdout="".join(stdout_rows),
            stderr="".join(stderr_rows),
            exit_code=exit_code,
            resources=dispositions,
            label_query_remaining=tuple(remaining),
        )

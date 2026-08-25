#!/usr/bin/env python3
"""Run cataloged guarantee proofs without weakening their trust boundaries.

A run is allowed only from an exact clean Git commit. Results are staged in an
external directory using the repository evidence layout; this keeps execution
artifacts from making the bound source dirty. The staged attestation can later
be checked in without changing the source commit recorded inside it.

This program never changes registry activation state, runs a manual procedure,
or infers that a remediation or passing proof activates a guarantee.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tarfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.assurance import pytest_result_plugin  # noqa: E402
from scripts.assurance import docker_lifecycle  # noqa: E402
from scripts.docs import guarantees  # noqa: E402


ADMISSION_SCHEMA_VERSION = "qt.assurance_profile_admission.v1"
EXECUTION_ADMISSION_SCHEMA_VERSION = "qt.assurance_execution_admission.v1"
ARCHIVED_ADMISSION_SCHEMA_VERSION = "qt.assurance_profile_admission_archive.v1"
EXECUTION_ADMISSION_ARCHIVE_SCHEMA_VERSION = (
    "qt.assurance_execution_admission_archive.v1"
)
UNAVAILABILITY_SCHEMA_VERSION = "qt.assurance_unavailability.v1"
NODE_RESULT_SCHEMA_VERSION = "qt.node_test_result.v1"
NODE_TRANSPORT_SCHEMA_VERSION = "qt.node_test_events.v1"
RESULT_STATUSES = {"PASS", "FAIL", "NOT_RUN", "MANUAL", "PARTIAL", "UNAVAILABLE"}
SAFE_ENVIRONMENT_CLASSES = {"isolated_test", "ephemeral_ci", "local_test"}
SAFE_SERVICE_ENVIRONMENT_CLASSES = {"isolated_test", "ephemeral_ci"}
SAFE_PROFILE_ISOLATION = {"process_local", "disposable", "session_scoped"}
SAFE_SERVICE_ISOLATION = {"disposable", "session_scoped"}
HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
ADMISSION_ID_RE = re.compile(r"[a-z][a-z0-9-]{2,127}\Z")
SECRET_KEY_RE = re.compile(r"(?:^|_)(?:password|secret|token|dsn|credential)(?:_|$)", re.I)
NONMATCH_REASON_RE = re.compile(
    r"(?:test name does not match pattern|does not match (?:the )?test name pattern)",
    re.I,
)


class AssuranceExecutionError(RuntimeError):
    """Raised when an assurance boundary cannot be represented honestly."""


class _PrerequisiteUnavailable(AssuranceExecutionError):
    def __init__(self, reason_code: str, details: Sequence[str]):
        super().__init__(f"{reason_code}:{','.join(details)}")
        self.reason_code = reason_code
        self.details = tuple(details)


@dataclass(frozen=True)
class AdmissionArtifact:
    scope: str
    index: int
    relative_path: str
    source_path: Path
    sha256: str


@dataclass(frozen=True)
class PreparedProfile:
    profile_id: str
    environment: dict[str, Any]
    process_env: dict[str, str]
    admission_payload: dict[str, Any]

    admission_artifacts: tuple[AdmissionArtifact, ...] = ()
    container_source_root: PurePosixPath | None = None

@dataclass(frozen=True)
class UnavailableProfile:
    profile_id: str
    reason_code: str
    details: tuple[str, ...]

    admission_payload: dict[str, Any] | None = None
    admission_artifacts: tuple[AdmissionArtifact, ...] = ()

@dataclass(frozen=True)
class ProcessResult:
    stdout: bytes
    stderr: bytes
    exit_code: int
    timed_out: bool


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _git(root: Path, *args: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AssuranceExecutionError(f"git_check_failed:{':'.join(args)}:{exc}") from exc
    return completed.stdout.strip()


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def require_exact_clean_source(root: Path, source_commit: str, stage_root: Path) -> None:
    """Require the requested full commit at clean HEAD and external staging."""

    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise AssuranceExecutionError("source_commit_must_be_full_lowercase_sha1")
    if not (root / ".git").exists():
        raise AssuranceExecutionError("source_git_metadata_required")
    if _git(root, "rev-parse", "HEAD") != source_commit:
        raise AssuranceExecutionError("source_commit_must_equal_head")
    if _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise AssuranceExecutionError("source_worktree_must_be_clean")
    if _is_within(stage_root, root):
        raise AssuranceExecutionError("stage_root_must_be_outside_source_tree")
    try:
        _git(root, "cat-file", "-e", f"{source_commit}^{{commit}}")
    except AssuranceExecutionError as exc:
        raise AssuranceExecutionError("source_commit_not_available_locally") from exc


def _git_archive_bytes(root: Path, source_commit: str) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "archive", "--format=tar", source_commit],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AssuranceExecutionError("exact_source_archive_failed") from exc
    if not completed.stdout:
        raise AssuranceExecutionError("exact_source_archive_empty")
    return completed.stdout


def _source_tree_digest(rows: Sequence[tuple[str, bool, bytes]]) -> str:
    digest = hashlib.sha256()
    for relative, executable, content in sorted(rows, key=lambda item: item[0]):
        path_bytes = relative.encode("utf-8")
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(b"x" if executable else b"-")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _archive_tree_sha256(archive: bytes) -> str:
    rows: list[tuple[str, bool, bytes]] = []
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
                raise AssuranceExecutionError("source_snapshot_unsafe_member")
            if member.isdir():
                continue
            if not member.isfile():
                raise AssuranceExecutionError("source_snapshot_unsupported_member")
            extracted = handle.extractfile(member)
            if extracted is None:
                raise AssuranceExecutionError("source_snapshot_member_unreadable")
            rows.append((pure.as_posix(), bool(member.mode & 0o111), extracted.read()))
    if not rows:
        raise AssuranceExecutionError("source_snapshot_has_no_files")
    return _source_tree_digest(rows)


def _snapshot_tree_sha256(root: Path) -> str:
    rows: list[tuple[str, bool, bytes]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise AssuranceExecutionError("source_snapshot_symlink_forbidden")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        rows.append((relative, bool(path.stat().st_mode & 0o111), path.read_bytes()))
    if not rows:
        raise AssuranceExecutionError("source_snapshot_has_no_files")
    return _source_tree_digest(rows)


def _make_snapshot_host_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            raise AssuranceExecutionError("source_snapshot_symlink_forbidden")
        if path.is_file():
            path.chmod(0o555 if path.stat().st_mode & 0o111 else 0o444)
        elif path.is_dir():
            path.chmod(0o555)
    root.chmod(0o555)


def _extract_source_snapshot(archive: bytes, destination: Path) -> None:
    if destination.exists():
        raise AssuranceExecutionError("source_snapshot_destination_exists")
    destination.mkdir(parents=True, exist_ok=False)
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
                    raise AssuranceExecutionError("source_snapshot_unsafe_member")
            handle.extractall(destination, filter="data")
    except BaseException:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    if (destination / ".git").exists():
        shutil.rmtree(destination, ignore_errors=True)
        raise AssuranceExecutionError("source_snapshot_git_metadata_forbidden")
    expected = _archive_tree_sha256(archive)
    if _snapshot_tree_sha256(destination) != expected:
        shutil.rmtree(destination, ignore_errors=True)
        raise AssuranceExecutionError("source_snapshot_tree_hash_mismatch")
    _make_snapshot_host_read_only(destination)
    if _snapshot_tree_sha256(destination) != expected:
        raise AssuranceExecutionError("source_snapshot_changed_during_hardening")


def _write_immutable_bytes(path: Path, content: bytes) -> str:
    """Publish bytes durably with no overwrite, including concurrent writers."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{secrets.token_hex(8)}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise AssuranceExecutionError(f"immutable_artifact_already_exists:{path}") from exc
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return _sha256_file_binary(path)


class _ExecutionInterrupted(BaseException):
    def __init__(self, signum: int):
        super().__init__(f"signal:{signum}")
        self.signum = signum


@dataclass
class _InterruptState:
    signals: list[int]
    cleanup_in_progress: bool = False


@contextmanager
def _installed_interrupt_handlers() -> Any:
    previous: dict[int, Any] = {}
    state = _InterruptState([])

    def interrupt(signum: int, frame: Any) -> None:
        del frame
        state.signals.append(signum)
        if state.cleanup_in_progress:
            return
        # Set the non-interruptible cleanup phase before propagating the first
        # signal. This closes the transition race into the outer cleanup
        # ``finally`` and makes repeated signals record-only.
        state.cleanup_in_progress = True
        raise _ExecutionInterrupted(signum)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupt)
        yield state
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def _exact_keys(
    value: Mapping[str, Any],
    required: set[str],
    where: str,
    *,
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    missing = sorted(required - set(value))
    extra = sorted(set(value) - required - optional)
    if missing:
        raise AssuranceExecutionError(f"{where}:missing:{','.join(missing)}")
    if extra:
        raise AssuranceExecutionError(f"{where}:unknown:{','.join(extra)}")


def _string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AssuranceExecutionError(f"{where}:nonempty_string_required")
    if "\x00" in value or "\r" in value or "\n" in value:
        raise AssuranceExecutionError(f"{where}:control_character_forbidden")
    return value


def _evidence_list(value: Any, where: str) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise AssuranceExecutionError(f"{where}:nonempty_array_required")
    result: list[dict[str, str]] = []
    for index, raw in enumerate(value):
        item_where = f"{where}[{index}]"
        if not isinstance(raw, dict):
            raise AssuranceExecutionError(f"{item_where}:object_required")
        _exact_keys(raw, {"kind", "path", "sha256"}, item_where)
        kind = _string(raw["kind"], f"{item_where}.kind")
        relative = _string(raw["path"], f"{item_where}.path")
        pure = PurePosixPath(relative)
        if (
            "\\" in relative
            or pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise AssuranceExecutionError(f"{item_where}.path:relative_safe_path_required")
        digest = _string(raw["sha256"], f"{item_where}.sha256")
        if not HEX64_RE.fullmatch(digest):
            raise AssuranceExecutionError(f"{item_where}.sha256:invalid")
        result.append({"kind": kind, "path": relative, "sha256": digest})
    if result != sorted(result, key=lambda item: (item["kind"], item["path"], item["sha256"])):
        raise AssuranceExecutionError(f"{where}:must_be_sorted")
    if len({(item['kind'], item['path'], item['sha256']) for item in result}) != len(result):
        raise AssuranceExecutionError(f"{where}:duplicate")
    return result


def _validate_fact_value(value: Any, where: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        if isinstance(value, str) and ("\x00" in value or "\r" in value or "\n" in value):
            raise AssuranceExecutionError(f"{where}:control_character_forbidden")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_fact_value(item, f"{where}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _string(key, f"{where}.key")
            _validate_fact_value(item, f"{where}.{key}")
        return
    raise AssuranceExecutionError(f"{where}:unsupported_fact_type")


def _validate_profile_admission(raw: Any, where: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise AssuranceExecutionError(f"{where}:object_required")
    _exact_keys(
        raw,
        {
            "profile_id",
            "admission_id",
            "admitted",
            "environment_class",
            "isolation",
            "external_order_submission_enabled",
            "tools",
            "services",
            "admission_evidence",
        },
        where,
    )
    profile_id = _string(raw["profile_id"], f"{where}.profile_id")
    admission_id = _string(raw["admission_id"], f"{where}.admission_id")
    if not ADMISSION_ID_RE.fullmatch(admission_id):
        raise AssuranceExecutionError(f"{where}.admission_id:invalid")
    if raw["admitted"] is not True:
        raise AssuranceExecutionError(f"{where}.admitted:must_be_true_or_omit_entry")
    environment_class = _string(raw["environment_class"], f"{where}.environment_class")
    if environment_class not in SAFE_ENVIRONMENT_CLASSES:
        raise AssuranceExecutionError(f"{where}.environment_class:not_isolated_test")
    isolation = _string(raw["isolation"], f"{where}.isolation")
    if isolation not in SAFE_PROFILE_ISOLATION:
        raise AssuranceExecutionError(f"{where}.isolation:invalid")
    if raw["external_order_submission_enabled"] is not False:
        raise AssuranceExecutionError(
            f"{where}.external_order_submission_enabled:must_be_false"
        )
    tools = raw["tools"]
    if not isinstance(tools, dict):
        raise AssuranceExecutionError(f"{where}.tools:object_required")
    normalized_tools: dict[str, dict[str, str]] = {}
    for name, tool_raw in sorted(tools.items()):
        tool_where = f"{where}.tools.{name}"
        _string(name, f"{where}.tools.key")
        if not isinstance(tool_raw, dict):
            raise AssuranceExecutionError(f"{tool_where}:object_required")
        _exact_keys(tool_raw, {"resolved_path", "version", "executable_sha256"}, tool_where)
        resolved_path = _string(tool_raw["resolved_path"], f"{tool_where}.resolved_path")
        if not Path(resolved_path).is_absolute():
            raise AssuranceExecutionError(f"{tool_where}.resolved_path:absolute_required")
        digest = _string(tool_raw["executable_sha256"], f"{tool_where}.executable_sha256")
        if not HEX64_RE.fullmatch(digest):
            raise AssuranceExecutionError(f"{tool_where}.executable_sha256:invalid")
        normalized_tools[name] = {
            "resolved_path": resolved_path,
            "version": _string(tool_raw["version"], f"{tool_where}.version"),
            "executable_sha256": digest,
        }
    services = raw["services"]
    if not isinstance(services, list):
        raise AssuranceExecutionError(f"{where}.services:array_required")
    normalized_services: list[dict[str, Any]] = []
    for index, service_raw in enumerate(services):
        service_where = f"{where}.services[{index}]"
        if not isinstance(service_raw, dict):
            raise AssuranceExecutionError(f"{service_where}:object_required")
        _exact_keys(
            service_raw,
            {
                "service_id",
                "environment_class",
                "isolation",
                "external_order_submission_enabled",
                "facts",
                "evidence",
            },
            service_where,
        )
        service_id = _string(service_raw["service_id"], f"{service_where}.service_id")
        service_class = _string(
            service_raw["environment_class"], f"{service_where}.environment_class"
        )
        if service_class not in SAFE_SERVICE_ENVIRONMENT_CLASSES:
            raise AssuranceExecutionError(f"{service_where}.environment_class:not_isolated")
        service_isolation = _string(service_raw["isolation"], f"{service_where}.isolation")
        if service_isolation not in SAFE_SERVICE_ISOLATION:
            raise AssuranceExecutionError(f"{service_where}.isolation:not_disposable")
        if service_raw["external_order_submission_enabled"] is not False:
            raise AssuranceExecutionError(
                f"{service_where}.external_order_submission_enabled:must_be_false"
            )
        facts = service_raw["facts"]
        if not isinstance(facts, dict) or not facts:
            raise AssuranceExecutionError(f"{service_where}.facts:nonempty_object_required")
        for key, value in facts.items():
            _string(key, f"{service_where}.facts.key")
            if SECRET_KEY_RE.search(key) and not key.endswith("_sha256"):
                raise AssuranceExecutionError(f"{service_where}.facts.{key}:secret_forbidden")
            _validate_fact_value(value, f"{service_where}.facts.{key}")
        normalized_services.append(
            {
                "service_id": service_id,
                "environment_class": service_class,
                "isolation": service_isolation,
                "external_order_submission_enabled": False,
                "facts": facts,
                "evidence": _evidence_list(service_raw["evidence"], f"{service_where}.evidence"),
            }
        )
    service_ids = [item["service_id"] for item in normalized_services]
    if service_ids != sorted(service_ids) or len(service_ids) != len(set(service_ids)):
        raise AssuranceExecutionError(f"{where}.services:must_be_unique_and_sorted")
    return {
        "profile_id": profile_id,
        "admission_id": admission_id,
        "admitted": True,
        "environment_class": environment_class,
        "isolation": isolation,
        "external_order_submission_enabled": False,
        "tools": normalized_tools,
        "services": normalized_services,
        "admission_evidence": _evidence_list(
            raw["admission_evidence"], f"{where}.admission_evidence"
        ),
    }


def load_admission_manifest(path: Path, source_commit: str) -> dict[str, dict[str, Any]]:
    raw = guarantees.load_json_strict(path)
    _exact_keys(raw, {"schema_version", "source_commit", "profiles"}, "admission")
    if raw["schema_version"] != ADMISSION_SCHEMA_VERSION:
        raise AssuranceExecutionError("admission.schema_version:unsupported")
    if raw["source_commit"] != source_commit:
        raise AssuranceExecutionError("admission.source_commit:mismatch")
    if not isinstance(raw["profiles"], list):
        raise AssuranceExecutionError("admission.profiles:array_required")
    profiles = [
        _validate_profile_admission(item, f"admission.profiles[{index}]")
        for index, item in enumerate(raw["profiles"])
    ]
    profile_ids = [item["profile_id"] for item in profiles]
    if profile_ids != sorted(profile_ids) or len(profile_ids) != len(set(profile_ids)):
        raise AssuranceExecutionError("admission.profiles:must_be_unique_and_sorted")
    return {item["profile_id"]: item for item in profiles}


def _relative_definition(value: Any, where: str) -> dict[str, str]:
    raw = value
    if not isinstance(raw, dict):
        raise AssuranceExecutionError(f"{where}:object_required")
    _exact_keys(raw, {"path", "sha256"}, where)
    relative = _string(raw["path"], f"{where}.path")
    pure = PurePosixPath(relative)
    if (
        "\\" in relative
        or pure.is_absolute()
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise AssuranceExecutionError(f"{where}.path:relative_safe_path_required")
    digest = _string(raw["sha256"], f"{where}.sha256")
    if not HEX64_RE.fullmatch(digest):
        raise AssuranceExecutionError(f"{where}.sha256:invalid")
    return {"path": relative, "sha256": digest}


def _validate_execution_admission_profile(raw: Any, where: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise AssuranceExecutionError(f"{where}:object_required")
    _exact_keys(
        raw,
        {
            "profile_id",
            "admission_id",
            "environment_class",
            "isolation",
            "external_order_submission_enabled",
            "runtime_definition",
            "docker_tool",
            "runner_image",
            "service_images",
        },
        where,
    )
    profile_id = _string(raw["profile_id"], f"{where}.profile_id")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", profile_id):
        raise AssuranceExecutionError(f"{where}.profile_id:invalid")
    admission_id = _string(raw["admission_id"], f"{where}.admission_id")
    if not ADMISSION_ID_RE.fullmatch(admission_id):
        raise AssuranceExecutionError(f"{where}.admission_id:invalid")
    if raw["environment_class"] not in {"isolated_test", "ephemeral_ci"}:
        raise AssuranceExecutionError(f"{where}.environment_class:not_isolated")
    if raw["isolation"] not in {"disposable", "session_scoped"}:
        raise AssuranceExecutionError(f"{where}.isolation:not_disposable")
    if raw["external_order_submission_enabled"] is not False:
        raise AssuranceExecutionError(
            f"{where}.external_order_submission_enabled:must_be_false"
        )
    docker_tool = raw["docker_tool"]
    if not isinstance(docker_tool, dict):
        raise AssuranceExecutionError(f"{where}.docker_tool:object_required")
    _exact_keys(
        docker_tool,
        {"resolved_path", "version", "executable_sha256", "daemon_identity_sha256"},
        f"{where}.docker_tool",
    )
    resolved_path = _string(
        docker_tool["resolved_path"], f"{where}.docker_tool.resolved_path"
    )
    if not Path(resolved_path).is_absolute():
        raise AssuranceExecutionError(f"{where}.docker_tool.resolved_path:absolute_required")
    normalized_tool = {
        "resolved_path": resolved_path,
        "version": _string(docker_tool["version"], f"{where}.docker_tool.version"),
    }
    for key in ("executable_sha256", "daemon_identity_sha256"):
        digest = _string(docker_tool[key], f"{where}.docker_tool.{key}")
        if not HEX64_RE.fullmatch(digest):
            raise AssuranceExecutionError(f"{where}.docker_tool.{key}:invalid")
        normalized_tool[key] = digest
    runner = raw["runner_image"]
    if not isinstance(runner, dict):
        raise AssuranceExecutionError(f"{where}.runner_image:object_required")
    _exact_keys(
        runner,
        {"image_id", "platform", "base_image_digests", "build_definition"},
        f"{where}.runner_image",
    )
    image_id = _string(runner["image_id"], f"{where}.runner_image.image_id")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise AssuranceExecutionError(f"{where}.runner_image.image_id:invalid")
    if runner["platform"] != "linux/amd64":
        raise AssuranceExecutionError(f"{where}.runner_image.platform:unsupported")
    base_digests = runner["base_image_digests"]
    if (
        not isinstance(base_digests, list)
        or base_digests != sorted(set(base_digests))
        or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", item or "") for item in base_digests)
    ):
        raise AssuranceExecutionError(f"{where}.runner_image.base_image_digests:invalid")
    service_images = raw["service_images"]
    if not isinstance(service_images, dict):
        raise AssuranceExecutionError(f"{where}.service_images:object_required")
    normalized_services: dict[str, dict[str, str]] = {}
    for service_id, service in sorted(service_images.items()):
        if not re.fullmatch(r"[a-z][a-z0-9-]*", service_id):
            raise AssuranceExecutionError(f"{where}.service_images.key:invalid")
        if not isinstance(service, dict):
            raise AssuranceExecutionError(f"{where}.service_images.{service_id}:object_required")
        _exact_keys(
            service,
            {"reference", "image_id", "image_digest"},
            f"{where}.service_images.{service_id}",
        )
        normalized = {
            "reference": _string(
                service["reference"], f"{where}.service_images.{service_id}.reference"
            )
        }
        for key in ("image_id", "image_digest"):
            digest = _string(
                service[key], f"{where}.service_images.{service_id}.{key}"
            )
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise AssuranceExecutionError(
                    f"{where}.service_images.{service_id}.{key}:invalid"
                )
            normalized[key] = digest
        normalized_services[service_id] = normalized
    return {
        "profile_id": profile_id,
        "admission_id": admission_id,
        "environment_class": raw["environment_class"],
        "isolation": raw["isolation"],
        "external_order_submission_enabled": False,
        "runtime_definition": _relative_definition(
            raw["runtime_definition"], f"{where}.runtime_definition"
        ),
        "docker_tool": normalized_tool,
        "runner_image": {
            "image_id": image_id,
            "platform": "linux/amd64",
            "base_image_digests": base_digests,
            "build_definition": _relative_definition(
                runner["build_definition"], f"{where}.runner_image.build_definition"
            ),
        },
        "service_images": normalized_services,
    }


def load_execution_admission(
    path: Path, source_commit: str
) -> tuple[dict[str, dict[str, Any]], str]:
    try:
        source_bytes = path.read_bytes()
        raw = json.loads(
            source_bytes.decode("utf-8"),
            object_pairs_hook=guarantees._unique_object,
            parse_constant=guarantees._reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AssuranceExecutionError(
            f"execution_admission:invalid_json:{type(exc).__name__}"
        ) from exc
    if not isinstance(raw, dict):
        raise AssuranceExecutionError("execution_admission:object_required")
    _exact_keys(raw, {"schema_version", "source_commit", "profiles"}, "execution_admission")
    if raw["schema_version"] != EXECUTION_ADMISSION_SCHEMA_VERSION:
        if raw["schema_version"] == ADMISSION_SCHEMA_VERSION:
            raise AssuranceExecutionError("execution_admission:legacy_profile_admission_rejected")
        raise AssuranceExecutionError("execution_admission.schema_version:unsupported")
    if raw["source_commit"] != source_commit:
        raise AssuranceExecutionError("execution_admission.source_commit:mismatch")
    if not isinstance(raw["profiles"], list) or not raw["profiles"]:
        raise AssuranceExecutionError("execution_admission.profiles:nonempty_array_required")
    profiles = [
        _validate_execution_admission_profile(item, f"execution_admission.profiles[{index}]")
        for index, item in enumerate(raw["profiles"])
    ]
    profile_ids = [item["profile_id"] for item in profiles]
    if profile_ids != sorted(profile_ids) or len(profile_ids) != len(set(profile_ids)):
        raise AssuranceExecutionError(
            "execution_admission.profiles:must_be_unique_and_sorted"
        )
    return (
        {item["profile_id"]: item for item in profiles},
        _sha256_bytes(source_bytes),
    )


def archive_execution_admission_profile(
    admission: Mapping[str, Any],
    source_commit: str,
    execution_admission_sha256: str,
) -> dict[str, Any]:
    profile = json.loads(json.dumps(admission))
    resolved_path = profile["docker_tool"].pop("resolved_path")
    profile["docker_tool"]["executable_basename"] = Path(resolved_path).name
    profile["docker_tool"]["resolved_path_sha256"] = _sha256_bytes(
        resolved_path.encode("utf-8")
    )
    payload = {
        "record_schema_version": EXECUTION_ADMISSION_ARCHIVE_SCHEMA_VERSION,
        "source_admission_schema_version": EXECUTION_ADMISSION_SCHEMA_VERSION,
        "source_commit": source_commit,
        "execution_admission_sha256": execution_admission_sha256,
        "profile": profile,
    }
    _assert_archive_has_no_absolute_paths(payload)
    return payload


def _tool_path_identity(resolved_path: str) -> tuple[str, str]:
    windows_path = PureWindowsPath(resolved_path)
    posix_path = PurePosixPath(resolved_path)
    if windows_path.is_absolute():
        return windows_path.name, "windows_absolute"
    if posix_path.is_absolute():
        return posix_path.name, "posix_absolute"
    raise AssuranceExecutionError("admission_tool_resolved_path_not_absolute")


def _assert_archive_has_no_absolute_paths(value: Any, where: str = "archive") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_archive_has_no_absolute_paths(item, f"{where}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_archive_has_no_absolute_paths(item, f"{where}[{index}]")
        return
    if isinstance(value, str) and (
        PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute()
    ):
        raise AssuranceExecutionError(f"{where}:absolute_host_path_forbidden")


def archive_admission_payload(
    admission: Mapping[str, Any], source_commit: str
) -> dict[str, Any]:
    """Build a durable admission record without leaking a host-specific path."""

    profile = json.loads(json.dumps(admission))
    archived_tools: dict[str, dict[str, str]] = {}
    for name, tool in sorted(admission["tools"].items()):
        resolved_path = tool["resolved_path"]
        basename, path_class = _tool_path_identity(resolved_path)
        archived_tools[name] = {
            "executable_basename": basename,
            "version": tool["version"],
            "executable_sha256": tool["executable_sha256"],
            "resolved_path_sha256": _sha256_bytes(resolved_path.encode("utf-8")),
            "resolved_path_class": path_class,
        }
    profile["tools"] = archived_tools
    payload = {
        "schema_version": ARCHIVED_ADMISSION_SCHEMA_VERSION,
        "source_admission_schema_version": ADMISSION_SCHEMA_VERSION,
        "source_commit": source_commit,
        "profile": profile,
    }
    _assert_archive_has_no_absolute_paths(payload)
    return payload


def _resolve_evidence_file(
    evidence_root: Path,
    relative: str,
    expected_sha256: str,
) -> Path:
    source = evidence_root.joinpath(*PurePosixPath(relative).parts)
    try:
        resolved = source.resolve(strict=True)
        resolved.relative_to(evidence_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise AssuranceExecutionError(
            f"admission_evidence_path_invalid:{relative}"
        ) from exc
    if not resolved.is_file():
        raise AssuranceExecutionError(f"admission_evidence_not_file:{relative}")
    observed = guarantees._sha256_file(resolved)
    if observed != expected_sha256:
        raise AssuranceExecutionError(f"admission_evidence_hash_mismatch:{relative}")
    return resolved


def resolve_admission_artifacts(
    admission: Mapping[str, Any], evidence_root: Path
) -> tuple[AdmissionArtifact, ...]:
    entries: list[tuple[str, dict[str, str]]] = [
        ("profile", item) for item in admission["admission_evidence"]
    ]
    for service in admission["services"]:
        scope = "service-" + re.sub(r"[^a-z0-9]+", "-", service["service_id"].lower()).strip("-")
        entries.extend((scope, item) for item in service["evidence"])
    artifacts: list[AdmissionArtifact] = []
    for index, (scope, item) in enumerate(entries, start=1):
        artifacts.append(
            AdmissionArtifact(
                scope=scope,
                index=index,
                relative_path=item["path"],
                source_path=_resolve_evidence_file(
                    evidence_root, item["path"], item["sha256"]
                ),
                sha256=item["sha256"],
            )
        )
    return tuple(artifacts)


def _required_service_ids(profile: Mapping[str, Any]) -> list[str]:
    result: list[str] = []
    for index, raw in enumerate(profile["required_services"]):
        if isinstance(raw, str):
            result.append(raw)
        elif isinstance(raw, dict):
            service_id = raw.get("id", raw.get("service_id"))
            result.append(_string(service_id, f"profile.required_services[{index}].id"))
        else:
            raise AssuranceExecutionError(
                f"profile.required_services[{index}]:unsupported_shape"
            )
    return result


def _sha256_file_binary(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profile_process_env(
    admission: Mapping[str, Any], *, include_database: bool
) -> dict[str, str]:
    tool_dirs: list[str] = []
    for tool in admission["tools"].values():
        parent = str(Path(tool["resolved_path"]).resolve().parent)
        if parent not in tool_dirs:
            tool_dirs.append(parent)
    env = {
        "PATH": os.pathsep.join(
            [
                *tool_dirs,
                "/usr/local/sbin",
                "/usr/local/bin",
                "/usr/sbin",
                "/usr/bin",
                "/sbin",
                "/bin",
            ]
        ),
        "HOME": "/tmp",
        "TMPDIR": "/tmp",
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "TZ": "UTC",
        "PYTHONHASHSEED": "0",
        "NO_COLOR": "1",
        "QT_ASSURANCE_MODE": "1",
    }
    for name in ("LC_ALL", "SSL_CERT_DIR", "SSL_CERT_FILE"):
        value = os.environ.get(name, "").strip()
        if value and not any(character in value for character in "\x00\r\n"):
            env[name] = value
    if include_database:
        dsn = os.environ.get("PG_DSN", "").strip()
        if dsn:
            env["PG_DSN"] = dsn
        env["RUN_DB_TESTS"] = "1"
        env["QT_DB_TEST_ISOLATED"] = "1"
    return env


def _probe_tool(name: str, expected: Mapping[str, str], env: Mapping[str, str]) -> str:
    observed = shutil.which(name, path=env.get("PATH"))
    if observed is None:
        raise _PrerequisiteUnavailable("tool_unavailable", [name])
    observed_path = Path(observed).resolve()
    expected_path = Path(expected["resolved_path"]).resolve()
    if observed_path != expected_path:
        raise _PrerequisiteUnavailable("tool_path_mismatch", [name])
    if _sha256_file_binary(observed_path) != expected["executable_sha256"]:
        raise _PrerequisiteUnavailable("tool_hash_mismatch", [name])
    try:
        completed = subprocess.run(
            [name, "--version"],
            check=False,
            capture_output=True,
            env=dict(env),
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise _PrerequisiteUnavailable("tool_probe_failed", [name, type(exc).__name__]) from exc
    version = (completed.stdout + completed.stderr).decode("utf-8", errors="replace").strip()
    if completed.returncode != 0 or not version:
        raise _PrerequisiteUnavailable("tool_probe_failed", [name])
    if version != expected["version"]:
        raise _PrerequisiteUnavailable("tool_version_mismatch", [name])
    return version


def _validate_database_admission(service: Mapping[str, Any], env: Mapping[str, str]) -> None:
    facts = service["facts"]
    required = {
        "postgresql_major",
        "timescaledb_version",
        "extensions",
        "pg_dsn_sha256",
        "session_isolation_key_sha256",
    }
    missing = sorted(required - set(facts))
    if missing:
        raise _PrerequisiteUnavailable("database_admission_facts_missing", missing)
    if facts["postgresql_major"] != 15 or facts["timescaledb_version"] != "2.14.2":
        raise _PrerequisiteUnavailable("database_version_not_admitted", [])
    extensions = facts["extensions"]
    if not isinstance(extensions, list) or sorted(extensions) != extensions:
        raise _PrerequisiteUnavailable("database_extensions_invalid", [])
    if not {"pgcrypto", "timescaledb"}.issubset(set(extensions)):
        raise _PrerequisiteUnavailable("database_extensions_missing", [])
    for key in ("pg_dsn_sha256", "session_isolation_key_sha256"):
        if not isinstance(facts[key], str) or not HEX64_RE.fullmatch(facts[key]):
            raise _PrerequisiteUnavailable("database_admission_hash_invalid", [key])
    dsn = env.get("PG_DSN", "").strip()
    if not dsn:
        raise _PrerequisiteUnavailable("database_dsn_unavailable", ["PG_DSN"])
    if _sha256_bytes(dsn.encode("utf-8")) != facts["pg_dsn_sha256"]:
        raise _PrerequisiteUnavailable("database_dsn_not_admitted", ["PG_DSN"])


def prepare_profile(
    profile: Mapping[str, Any],
    admission: Mapping[str, Any],
    *,
    source_commit: str,
    root: Path,
    admission_artifacts: Sequence[AdmissionArtifact] = (),
) -> PreparedProfile:
    profile_id = profile["id"]
    if admission["profile_id"] != profile_id:
        raise AssuranceExecutionError(f"profile_admission_id_mismatch:{profile_id}")
    required_services = _required_service_ids(profile)
    if required_services:
        # Service-backed execution remains fail-closed until the profile model
        # supplies a single environment-scoped post-run cleanup/finalize hook.
        raise _PrerequisiteUnavailable(
            "service_profile_cleanup_not_integrated", required_services
        )
    if required_services and admission["environment_class"] == "local_test":
        raise _PrerequisiteUnavailable("service_profile_requires_isolated_environment", [])
    service_by_id = {item["service_id"]: item for item in admission["services"]}
    missing_services = sorted(set(required_services) - set(service_by_id))
    extra_services = sorted(set(service_by_id) - set(required_services))
    if missing_services:
        raise _PrerequisiteUnavailable("required_service_admission_missing", missing_services)
    if extra_services:
        raise AssuranceExecutionError(
            f"profile_service_admission_not_cataloged:{profile_id}:{','.join(extra_services)}"
        )
    database_service_ids = [
        service_id for service_id in required_services
        if "postgresql" in service_id or "timescaledb" in service_id
    ]
    process_env = _profile_process_env(admission, include_database=bool(database_service_ids))
    required_tools = {"python"}
    if "node" in profile:
        required_tools.add("node")
    missing_tools = sorted(required_tools - set(admission["tools"]))
    if missing_tools:
        raise _PrerequisiteUnavailable("tool_admission_missing", missing_tools)
    observed_versions: dict[str, str] = {}
    for tool_name in sorted(required_tools):
        observed_versions[tool_name] = _probe_tool(
            tool_name, admission["tools"][tool_name], process_env
        )
        constraint = profile[tool_name]
        try:
            guarantees._version_satisfies(
                observed_versions[tool_name], constraint, f"profile.{profile_id}.{tool_name}"
            )
        except guarantees.GuaranteeValidationError as exc:
            raise _PrerequisiteUnavailable("tool_version_outside_profile", [tool_name]) from exc
    for service_id in required_services:
        service = service_by_id[service_id]
        if "postgresql" in service_id or "timescaledb" in service_id:
            _validate_database_admission(service, process_env)
    if not required_services:
        process_env.pop("PG_DSN", None)
    lockfiles = {
        relative: guarantees._bound_material_sha256(
            root, relative, git_commit=source_commit
        )
        for relative in profile["lockfiles"]
    }
    services = {
        service_id: "structured-admission-sha256:"
        + _sha256_bytes(_canonical_json_bytes(service_by_id[service_id]))
        for service_id in required_services
    }
    payload = archive_admission_payload(admission, source_commit)
    return PreparedProfile(
        profile_id=profile_id,
        environment={
            "profile_id": profile_id,
            "os": f"{platform.system()} {platform.release()}",
            "architecture": platform.machine(),
            "tool_versions": observed_versions,
            "lockfile_hashes": lockfiles,
            "services": services,
        },
        process_env=process_env,
        admission_payload=payload,
        admission_artifacts=tuple(admission_artifacts),
    )


def _runner_supported(runner: Mapping[str, Any]) -> tuple[bool, str]:
    kind = runner["kind"]
    if kind == "pytest":
        return True, ""
    if kind == "node_test":
        required = {
            "event_transport",
            "expected_test_names",
            "expected_excluded_nonmatch_count",
        }
        if required.issubset(runner):
            return True, ""
        return False, "node_typed_transport_not_cataloged"
    if kind == "manual":
        return False, "manual_proof_requires_separate_operator_review"
    return False, f"runner_kind_not_executable:{kind}"


def require_execution_model_ready(catalog: Mapping[str, Any]) -> None:
    """Fail before execution when the approved Node result model is absent.

    PDR-02 adds the Node transport fields in the same model change that caps
    guarantee PASS by proof maturity. Requiring those fields prevents an older
    validator from rejecting a conservative attestation only after proofs ran.
    """

    for proof in catalog["proofs"]:
        if proof["lifecycle"] != "active" or proof["runner"]["kind"] != "node_test":
            continue
        supported, _ = _runner_supported(proof["runner"])
        if not supported:
            raise AssuranceExecutionError(
                "assurance_model_requires_pdr02_node_integration"
            )


def _run_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout_seconds: int,
) -> ProcessResult:
    try:
        process = subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            start_new_session=True,
        )
    except OSError as exc:
        raise AssuranceExecutionError(f"runner_start_failed:{type(exc).__name__}") from exc
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return ProcessResult(stdout, stderr, int(process.returncode), False)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (AttributeError, OSError):
            process.kill()
        stdout, stderr = process.communicate()
        marker = f"\nqt_assurance_executor:timeout_after:{timeout_seconds}s\n".encode()
        return ProcessResult(stdout, stderr + marker, 124, True)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (AttributeError, OSError):
            process.kill()
        process.communicate()
        raise


def _parse_json_lines(stdout: bytes, *, prefix: str | None = None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw_line in stdout.decode("utf-8", errors="replace").splitlines():
        line = raw_line
        if prefix is not None:
            prefix_at = line.find(prefix)
            if prefix_at < 0:
                continue
            line = line[prefix_at + len(prefix) :]
        else:
            line = line.strip()
            if not line.startswith("{"):
                continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def _selector_matches_node_id(selector: str, node_id: str) -> bool:
    return node_id == selector or node_id.startswith(selector + "[")


def parse_pytest_result(
    process: ProcessResult, runner: Mapping[str, Any]
) -> tuple[dict[str, Any], str | None]:
    events = _parse_json_lines(process.stdout, prefix=pytest_result_plugin.LINE_PREFIX)
    sessions = [
        event
        for event in events
        if event.get("schema_version") == pytest_result_plugin.SCHEMA_VERSION
        and event.get("event") == "session_result"
    ]
    if len(sessions) != 1:
        if process.timed_out:
            return {
                "collected_count": 0,
                "passed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "xfailed_count": 0,
                "xpassed_count": 0,
            }, "runner_timeout_unattributed"
        if process.exit_code == 0:
            counts = {
                "collected_count": len(runner["selectors"]),
                "passed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "xfailed_count": 0,
                "xpassed_count": 0,
            }
            return counts, "typed_pytest_result_missing"
        count = max(1, len(runner["selectors"]))
        counts = {
            "collected_count": count,
            "passed_count": 0,
            "failed_count": count,
            "skipped_count": 0,
            "xfailed_count": 0,
            "xpassed_count": 0,
        }
        return counts, "pytest_runner_failed_before_typed_result"
    session = sessions[0]
    if session.get("exit_code") != (1 if process.exit_code < 0 else process.exit_code):
        if not process.timed_out:
            raise AssuranceExecutionError("pytest_transport_exit_code_mismatch")
    node_ids = session.get("node_ids")
    results = session.get("results")
    collection_errors = session.get("collection_errors")
    counts = session.get("counts")
    if (
        not isinstance(node_ids, list)
        or any(not isinstance(item, str) for item in node_ids)
        or node_ids != sorted(set(node_ids))
        or not isinstance(results, list)
        or not isinstance(collection_errors, list)
        or not isinstance(counts, dict)
    ):
        raise AssuranceExecutionError("pytest_transport_shape_invalid")
    expected_count_keys = {"passed", "failed", "skipped", "xfailed", "xpassed"}
    if set(counts) != expected_count_keys or any(
        type(counts[key]) is not int or counts[key] < 0 for key in expected_count_keys
    ):
        raise AssuranceExecutionError("pytest_transport_counts_invalid")
    result_node_ids = [item.get("node_id") for item in results if isinstance(item, dict)]
    if result_node_ids != node_ids:
        raise AssuranceExecutionError("pytest_transport_result_node_ids_mismatch")
    selector_complete = all(
        any(_selector_matches_node_id(selector, node_id) for node_id in node_ids)
        for selector in runner["selectors"]
    )
    unexpected = [
        node_id
        for node_id in node_ids
        if not any(
            _selector_matches_node_id(selector, node_id) for selector in runner["selectors"]
        )
    ]
    normalized = {
        "collected_count": len(node_ids) + len(collection_errors),
        "passed_count": counts["passed"],
        "failed_count": counts["failed"],
        "skipped_count": counts["skipped"],
        "xfailed_count": counts["xfailed"],
        "xpassed_count": counts["xpassed"],
    }
    if normalized["collected_count"] < 1:
        return {
            **normalized,
            "collected_count": max(1, len(runner["selectors"])),
        }, "pytest_collected_no_tests"
    if unexpected or not selector_complete:
        return normalized, "pytest_selector_coverage_incomplete"
    return normalized, None


def _node_file(
    value: Any,
    root: Path,
    *,
    container_source_root: PurePosixPath | None = None,
) -> str:
    raw = _string(value, "node_event.data.file")
    parsed = urlparse(raw)
    decoded = unquote(parsed.path) if parsed.scheme == "file" else raw
    if container_source_root is not None:
        pure = PurePosixPath(decoded)
        try:
            relative = pure.relative_to(container_source_root)
        except ValueError as exc:
            raise AssuranceExecutionError("node_event_file_outside_container_source") from exc
        if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            raise AssuranceExecutionError("node_event_file_outside_container_source")
        return relative.as_posix()
    path = Path(decoded)
    if not path.is_absolute():
        path = root / path
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise AssuranceExecutionError("node_event_file_outside_source") from exc


def _is_nonmatch_skip(value: Any) -> bool:
    return isinstance(value, str) and bool(NONMATCH_REASON_RE.search(value))


def parse_node_result(
    process: ProcessResult,
    runner: Mapping[str, Any],
    *,
    root: Path,
    container_source_root: PurePosixPath | None = None,
) -> tuple[dict[str, Any], str | None]:
    events = _parse_json_lines(process.stdout)
    transport = [
        event
        for event in events
        if event.get("schema_version") == NODE_TRANSPORT_SCHEMA_VERSION
    ]
    if not transport:
        if process.timed_out:
            return {
                "collected_count": 0,
                "passed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "node_test_result": {
                    "schema_version": NODE_RESULT_SCHEMA_VERSION,
                    "transport_schema_version": runner["event_transport"]["schema_version"],
                    "reporter_path": runner["event_transport"]["path"],
                    "collected_files": [],
                    "selected_test_names": [],
                    "excluded_nonmatch_test_names": [],
                    "explicitly_skipped_count": 0,
                    "cancelled_count": 0,
                    "todo_count": 0,
                },
            }, "runner_timeout_unattributed"
        raise AssuranceExecutionError("node_typed_transport_missing")
    sequences = [event.get("sequence") for event in transport]
    if sequences != list(range(len(transport))):
        raise AssuranceExecutionError("node_transport_sequence_invalid")
    summaries = [event for event in transport if event.get("event_type") == "test:summary"]
    if len(summaries) > 1:
        raise AssuranceExecutionError("node_transport_terminal_summary_duplicate")
    top_level_plans = []
    for event in transport:
        if event.get("event_type") != "test:plan":
            continue
        data = event.get("data")
        if isinstance(data, dict) and data.get("nesting") == 0:
            top_level_plans.append(event)
    if len(top_level_plans) > 1:
        raise AssuranceExecutionError("node_transport_top_level_plan_duplicate")
    if not summaries and not top_level_plans and not process.timed_out:
        raise AssuranceExecutionError("node_transport_terminal_accounting_missing")
    plan_count: int | None = None
    if top_level_plans:
        plan_data = top_level_plans[0]["data"]
        if type(plan_data.get("count")) is not int or plan_data["count"] < 0:
            raise AssuranceExecutionError("node_transport_top_level_plan_invalid")
        plan_count = plan_data["count"]
    selected: list[tuple[str, str, str]] = []
    excluded: list[str] = []
    all_files: set[str] = set()
    for event in transport:
        if event.get("event_type") not in {"test:pass", "test:fail"}:
            continue
        data = event.get("data")
        if not isinstance(data, dict):
            raise AssuranceExecutionError("node_transport_event_data_invalid")
        name = _string(data.get("name"), "node_event.data.name")
        file_name = _node_file(
            data.get("file"), root, container_source_root=container_source_root
        )
        all_files.add(file_name)
        if _is_nonmatch_skip(data.get("skip")):
            excluded.append(name)
            continue
        if data.get("todo"):
            outcome = "todo"
        elif data.get("skip"):
            outcome = "skipped"
        elif event["event_type"] == "test:fail":
            outcome = "cancelled" if data.get("cancelled") else "failed"
        else:
            outcome = "passed"
        selected.append((name, file_name, outcome))
    selected.sort(key=lambda item: (item[0], item[1], item[2]))
    excluded.sort()
    selected_names = [item[0] for item in selected]
    expected_names = list(runner["expected_test_names"])
    expected_files = list(runner["files"])
    counts = {
        name: sum(1 for _, _, outcome in selected if outcome == name)
        for name in ("passed", "failed", "skipped", "cancelled", "todo")
    }
    terminal_count = len(selected) + len(excluded)
    if plan_count is not None and plan_count != terminal_count:
        raise AssuranceExecutionError("node_transport_top_level_plan_count_mismatch")
    if summaries:
        summary_data = summaries[0].get("data")
        if not isinstance(summary_data, dict) or not isinstance(
            summary_data.get("counts"), dict
        ):
            raise AssuranceExecutionError("node_transport_summary_shape_invalid")
        native_counts = summary_data["counts"]
        expected_native_counts = {
            "tests": terminal_count,
            "passed": counts["passed"],
            "failed": counts["failed"],
            "cancelled": counts["cancelled"],
            "skipped": counts["skipped"] + len(excluded),
            "todo": counts["todo"],
        }
        if any(
            type(native_counts.get(key)) is not int or native_counts[key] < 0
            for key in expected_native_counts
        ):
            raise AssuranceExecutionError("node_transport_summary_counts_invalid")
        if any(
            native_counts[key] != value for key, value in expected_native_counts.items()
        ):
            raise AssuranceExecutionError("node_transport_summary_counts_mismatch")
        if type(summary_data.get("success")) is not bool:
            raise AssuranceExecutionError("node_transport_summary_success_invalid")
    typed_result = {
        "schema_version": NODE_RESULT_SCHEMA_VERSION,
        "transport_schema_version": runner["event_transport"]["schema_version"],
        "reporter_path": runner["event_transport"]["path"],
        "collected_files": sorted(all_files),
        "selected_test_names": selected_names,
        "excluded_nonmatch_test_names": excluded,
        "explicitly_skipped_count": counts["skipped"],
        "cancelled_count": counts["cancelled"],
        "todo_count": counts["todo"],
    }
    generic = {
        "collected_count": len(selected),
        "passed_count": counts["passed"],
        "failed_count": counts["failed"],
        "skipped_count": counts["skipped"],
        "node_test_result": typed_result,
    }
    mismatch = (
        selected_names != expected_names
        or typed_result["collected_files"] != expected_files
        or len(excluded) != runner["expected_excluded_nonmatch_count"]
    )
    if process.timed_out:
        return generic, "runner_timeout_unattributed"
    if mismatch:
        return generic, "node_expected_selection_mismatch"
    if process.exit_code == 0 and any(counts[name] for name in ("skipped", "cancelled", "todo")):
        return generic, "node_selected_outcome_incomplete"
    return generic, None


def _classify_attempt(
    process: ProcessResult,
    counts: Mapping[str, Any],
    incomplete_reason: str | None,
) -> tuple[str, str | None]:
    if process.timed_out:
        return "PARTIAL", "runner_timeout_unattributed"
    if process.exit_code != 0:
        return "FAIL", None
    if counts.get("failed_count", 0) or counts.get("xpassed_count", 0):
        raise AssuranceExecutionError("zero_exit_with_failure_outcome_not_representable")
    if incomplete_reason is not None:
        return "PARTIAL", incomplete_reason
    if any(
        counts.get(key, 0)
        for key in ("skipped_count", "xfailed_count", "xpassed_count")
    ):
        return "PARTIAL", "selected_outcome_incomplete"
    return "PASS", None


def _proof_directory(stage_root: Path, attestation_id: str, proof_id: str) -> Path:
    return (
        stage_root
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / attestation_id
        / proof_id
    )


def _write_artifact(
    stage_root: Path,
    proof_dir: Path,
    filename: str,
    content: bytes,
    artifact_kind: str,
) -> tuple[dict[str, str], str]:
    proof_dir.mkdir(parents=True, exist_ok=True)
    path = proof_dir / filename
    digest = _write_immutable_bytes(path, content)
    return (
        {
            "artifact_kind": artifact_kind,
            "path": path.relative_to(stage_root).as_posix(),
            "sha256": digest,
        },
        digest,
    )


def _admission_refs(
    stage_root: Path,
    proof_dir: Path,
    payload: Mapping[str, Any],
    artifacts: Sequence[AdmissionArtifact],
) -> list[dict[str, str]]:
    manifest_ref, _ = _write_artifact(
        stage_root,
        proof_dir,
        "manual_evidence-profile-admission.json",
        _canonical_json_bytes(payload),
        "manual_evidence",
    )
    refs = [manifest_ref]
    for artifact in artifacts:
        observed = guarantees._sha256_file(artifact.source_path)
        if observed != artifact.sha256:
            raise AssuranceExecutionError(
                f"admission_evidence_changed_before_copy:{artifact.relative_path}"
            )
        basename = re.sub(
            r"[^a-z0-9._-]+",
            "-",
            PurePosixPath(artifact.relative_path).name.lower(),
        ).strip("-.")
        if not basename:
            basename = "artifact"
        ref, copied_hash = _write_artifact(
            stage_root,
            proof_dir,
            f"manual_evidence-{artifact.index:03d}-{artifact.scope}-{basename}",
            artifact.source_path.read_bytes(),
            "manual_evidence",
        )
        if copied_hash != artifact.sha256:
            raise AssuranceExecutionError(
                f"admission_evidence_copy_hash_mismatch:{artifact.relative_path}"
            )
        refs.append(ref)
    return refs


def _unavailable_result(
    *,
    stage_root: Path,
    attestation_id: str,
    source_commit: str,
    proof: Mapping[str, Any],
    reason_code: str,
    details: Sequence[str],
    admission_payload: Mapping[str, Any] | None = None,
    admission_artifacts: Sequence[AdmissionArtifact] = (),
) -> dict[str, Any]:
    proof_dir = _proof_directory(stage_root, attestation_id, proof["id"])
    payload = {
        "schema_version": UNAVAILABILITY_SCHEMA_VERSION,
        "source_commit": source_commit,
        "proof_id": proof["id"],
        "environment_profile_id": proof["environment_profile_id"],
        "reason_code": reason_code,
        "missing_prerequisites": sorted(set(details)),
    }
    ref, _ = _write_artifact(
        stage_root,
        proof_dir,
        "manual_evidence-unavailability.json",
        _canonical_json_bytes(payload),
        "manual_evidence",
    )
    evidence_refs = [ref]
    if admission_payload is not None:
        evidence_refs.extend(
            _admission_refs(stage_root, proof_dir, admission_payload, admission_artifacts)
        )
    evidence_refs.sort(key=lambda item: item["path"])
    return {
        "proof_id": proof["id"],
        "environment_profile_id": proof["environment_profile_id"],
        "status": "UNAVAILABLE",
        "evidence_refs": evidence_refs,
        "reason_code": reason_code,
    }


def _attempt_result(
    *,
    stage_root: Path,
    attestation_id: str,
    proof: Mapping[str, Any],
    profile: PreparedProfile,
    root: Path,
    started_at: datetime,
    finished_at: datetime,
    process: ProcessResult,
) -> dict[str, Any]:
    runner = proof["runner"]
    if runner["kind"] == "pytest":
        counts, incomplete_reason = parse_pytest_result(process, runner)
    elif runner["kind"] == "node_test":
        counts, incomplete_reason = parse_node_result(
            process,
            runner,
            root=root,
            container_source_root=profile.container_source_root,
        )
    else:
        raise AssuranceExecutionError(f"unsupported_attempted_runner:{runner['kind']}")
    if (
        runner["kind"] == "pytest"
        and process.exit_code != 0
        and not process.timed_out
        and counts.get("failed_count", 0) == 0
    ):
        counts = dict(counts)
        counts["failed_count"] = 1
        counts["collected_count"] = max(
            1,
            int(counts.get("collected_count", 0)) + 1,
        )
    status, reason_code = _classify_attempt(process, counts, incomplete_reason)
    proof_dir = _proof_directory(stage_root, attestation_id, proof["id"])
    stdout_ref, stdout_hash = _write_artifact(
        stage_root, proof_dir, "stdout-output.txt", process.stdout, "stdout"
    )
    stderr_ref, stderr_hash = _write_artifact(
        stage_root, proof_dir, "stderr-output.txt", process.stderr, "stderr"
    )
    evidence_refs = [
        *_admission_refs(
            stage_root, proof_dir, profile.admission_payload, profile.admission_artifacts
        ),
        stderr_ref,
        stdout_ref,
    ]
    result: dict[str, Any] = {
        "proof_id": proof["id"],
        "environment_profile_id": proof["environment_profile_id"],
        "status": status,
        "evidence_refs": evidence_refs,
        "started_at": _timestamp(started_at),
        "finished_at": _timestamp(finished_at),
        "executed_argv": guarantees._canonical_runner_argv(runner),
        "exit_code": process.exit_code,
        **counts,
        "stdout_sha256": stdout_hash,
        "stderr_sha256": stderr_hash,
    }
    if reason_code is not None:
        result["reason_code"] = reason_code
    summary = {
        key: value
        for key, value in result.items()
        if key not in {"evidence_refs", "result_summary_sha256"}
    }
    summary_ref, summary_hash = _write_artifact(
        stage_root,
        proof_dir,
        "result_summary.json",
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True).encode("utf-8") + b"\n",
        "result_summary",
    )
    result["result_summary_sha256"] = summary_hash
    result["evidence_refs"].append(summary_ref)
    result["evidence_refs"].sort(key=lambda item: item["path"])
    return result


def derive_guarantee_results(
    registry: Mapping[str, Any],
    catalog: Mapping[str, Any],
    proof_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    statuses = {item["proof_id"]: item["status"] for item in proof_results}
    required: dict[str, list[str]] = defaultdict(list)
    required_strengths: dict[str, list[str]] = defaultdict(list)
    proposed: dict[str, list[str]] = defaultdict(list)
    for proof in catalog["proofs"]:
        for coverage in proof["coverage"]:
            if not coverage["required_for_full_attestation"]:
                continue
            target = required if proof["lifecycle"] == "active" else proposed
            if proof["lifecycle"] in {"active", "proposed"}:
                target[coverage["guarantee_id"]].append(proof["id"])
                if proof["lifecycle"] == "active":
                    required_strengths[coverage["guarantee_id"]].append(
                        coverage["strength"]
                    )
    maturity = {row["id"]: row["proof_maturity"] for row in registry["guarantees"]}
    results: list[dict[str, Any]] = []
    for guarantee_id in sorted(required):
        proof_ids = sorted(required[guarantee_id])
        status = guarantees._aggregate_guarantee_status(
            [statuses[item] for item in proof_ids],
            proof_maturity=maturity[guarantee_id],
            required_strengths=required_strengths[guarantee_id],
            has_proposed_required_proof=bool(proposed.get(guarantee_id)),
        )
        results.append(
            {
                "guarantee_id": guarantee_id,
                "status": status,
                "proof_ids": proof_ids,
            }
        )
    return results


def _attestation_inputs(
    bundle: guarantees.ValidationBundle, source_commit: str
) -> dict[str, Any]:
    proof_path = bundle.root / guarantees.PROOF_CATALOG_PATH.relative_to(guarantees.ROOT)
    proof_relative = proof_path.relative_to(bundle.root).as_posix()
    return {
        "registry_semantics_sha256": guarantees.registry_semantics_sha256(bundle.registry),
        "proof_catalog_sha256": guarantees._bound_material_sha256(
            bundle.root, proof_relative, git_commit=source_commit
        ),
        "guarantee_material_sha256": guarantees.guarantee_material_hashes(
            bundle, git_commit=source_commit
        ),
        "required_proof_material_sha256": guarantees.required_proof_material_hashes(
            bundle, git_commit=source_commit
        ),
        "glossary_inputs": guarantees.glossary_inputs(
            bundle, git_commit=source_commit
        ),
    }


def _environment_evidence_ref(
    *,
    stage_root: Path,
    attestation_id: str,
    profile_id: str,
    artifact_kind: str,
    facts: Mapping[str, Any],
    service_id: str | None = None,
    suffix: str = "001",
) -> tuple[dict[str, str], str]:
    payload = _environment_evidence_payload(
        profile_id=profile_id,
        artifact_kind=artifact_kind,
        facts=facts,
        service_id=service_id,
    )
    scope = "profile" if service_id is None else f"service-{service_id}"
    filename = f"{artifact_kind}-{suffix}-{scope}.json"
    path = (
        stage_root
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / attestation_id
        / "_environments"
        / profile_id
        / filename
    )
    digest = _write_immutable_bytes(path, _canonical_json_bytes(payload))
    return (
        {
            "artifact_kind": artifact_kind,
            "path": path.relative_to(stage_root).as_posix(),
            "sha256": digest,
        },
        digest,
    )


def _environment_evidence_payload(
    *,
    profile_id: str,
    artifact_kind: str,
    facts: Mapping[str, Any],
    service_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "qt.assurance_environment_evidence.v1",
        "profile_id": profile_id,
        "artifact_kind": artifact_kind,
        "facts": dict(facts),
    }
    if service_id is not None:
        payload["service_id"] = service_id
    return payload


def _align_execution_admission(
    *,
    admission: Mapping[str, Any],
    profile: Mapping[str, Any],
    root: Path,
    source_commit: str,
) -> None:
    profile_id = profile["id"]
    if admission["profile_id"] != profile_id:
        raise AssuranceExecutionError(f"execution_admission_profile_mismatch:{profile_id}")
    runtime = admission["runtime_definition"]
    if runtime["path"] != profile["runtime_definition"]:
        raise AssuranceExecutionError(
            f"execution_admission_runtime_definition_path_mismatch:{profile_id}"
        )
    expected_runtime_hash = guarantees._bound_material_sha256(
        root, profile["runtime_definition"], git_commit=source_commit
    )
    if runtime["sha256"] != expected_runtime_hash:
        raise AssuranceExecutionError(
            f"execution_admission_runtime_definition_hash_mismatch:{profile_id}"
        )
    build = admission["runner_image"]["build_definition"]
    expected_build_hash = guarantees._bound_material_sha256(
        root, build["path"], git_commit=source_commit
    )
    if build["sha256"] != expected_build_hash:
        raise AssuranceExecutionError(
            f"execution_admission_build_definition_hash_mismatch:{profile_id}"
        )
    build_bytes = guarantees._bound_material_bytes(
        root, build["path"], git_commit=source_commit
    )
    expected_base_digests = sorted(
        {
            f"sha256:{digest.decode('ascii')}"
            for digest in re.findall(rb"@sha256:([0-9a-f]{64})", build_bytes)
        }
    )
    if admission["runner_image"]["base_image_digests"] != expected_base_digests:
        raise AssuranceExecutionError(
            f"execution_admission_runner_base_digest_mismatch:{profile_id}"
        )
    required_services = set(_required_service_ids(profile))
    if set(admission["service_images"]) != required_services:
        raise AssuranceExecutionError(
            f"execution_admission_service_image_set_mismatch:{profile_id}"
        )
    if profile["execution_class"] == "isolated_database":
        definition = json.loads(
            guarantees._bound_material_bytes(
                root, profile["runtime_definition"], git_commit=source_commit
            )
        )
        service = definition["service"]
        admitted_service = admission["service_images"][service["id"]]
        expected_reference = service["image"]
        expected_digest = "sha256:" + expected_reference.rsplit("@sha256:", 1)[-1]
        if admitted_service["reference"] != expected_reference:
            raise AssuranceExecutionError(
                f"execution_admission_service_reference_mismatch:{profile_id}"
            )
        if admitted_service["image_digest"] != expected_digest:
            raise AssuranceExecutionError(
                f"execution_admission_service_digest_mismatch:{profile_id}"
            )


def _profile_lockfile_hashes(
    root: Path, profile: Mapping[str, Any], source_commit: str
) -> dict[str, str]:
    return {
        relative: guarantees._bound_material_sha256(
            root, relative, git_commit=source_commit
        )
        for relative in profile["lockfiles"]
    }


def _resource_identity(
    prepared: docker_lifecycle.ProvisionedProfile, kind: str, logical_name: str
) -> str:
    for item in prepared.resources:
        if item.kind == kind and item.logical_name == logical_name:
            return item.runtime_identity
    raise AssuranceExecutionError(f"lifecycle_resource_identity_missing:{kind}:{logical_name}")


def _build_final_environment(
    *,
    root: Path,
    source_commit: str,
    stage_root: Path,
    attestation_id: str,
    profile: Mapping[str, Any],
    admission: Mapping[str, Any],
    prepared: docker_lifecycle.ProvisionedProfile,
    docker_version: str,
    source_snapshot_sha256: str,
    execution_admission_sha256: str,
    execution_admission_archive_ref: Mapping[str, str],
    execution_admission_archive_hash: str,
    draft_ref: Mapping[str, str],
    draft_hash: str,
    execution_ref: Mapping[str, str],
    execution_hash: str,
    cleanup_ref: Mapping[str, str],
    cleanup_hash: str,
    proof_results_hash: str,
    environment_instance_id: str,
    control_plane_identity_sha256: str,
) -> dict[str, Any]:
    profile_id = profile["id"]
    common_binding = {
        "attestation_id": attestation_id,
        "cleanup_manifest_sha256": cleanup_hash,
        "control_plane_identity_sha256": control_plane_identity_sha256,
        "environment_instance_id": environment_instance_id,
        "execution_admission_sha256": execution_admission_sha256,
        "execution_admission_archive_sha256": execution_admission_archive_hash,
        "execution_draft_sha256": draft_hash,
        "execution_manifest_sha256": execution_hash,
        "proof_results_sha256": proof_results_hash,
        "source_snapshot_sha256": source_snapshot_sha256,
    }
    evidence_refs: list[dict[str, str]] = [
        dict(execution_admission_archive_ref),
        dict(draft_ref),
        dict(execution_ref),
        dict(cleanup_ref),
    ]
    runner_identity = _resource_identity(prepared, "container", "proof-runner")
    execution_class = profile["execution_class"]
    if execution_class == "isolated_container":
        facts = {
            **common_binding,
            "base_image_digests": admission["runner_image"]["base_image_digests"],
            "cleanup_completed": True,
            "container_identity": runner_identity,
            "docker_version": docker_version,
            "image_digest": prepared.runner_image_id,
            "network_mode": "none",
            "platform": "linux/amd64",
            "source_commit": source_commit,
            "source_mount_mode": "read_only",
            "writable_temp_outside_source": True,
        }
        evidence_facts = {
            "base_image_digests": {
                "base_image_digests": facts["base_image_digests"]
            },
            "bootstrap_log": {
                "bootstrap_completed": True,
                "container_identity": runner_identity,
            },
            "cleanup_log": {
                "cleanup_completed": True,
                "container_identity": runner_identity,
            },
            "container_identity": {"container_identity": runner_identity},
            "image_digest": {
                "build_definition_sha256": admission["runner_image"][
                    "build_definition"
                ]["sha256"],
                "image_digest": prepared.runner_image_id,
            },
            "network_mode": {"network_mode": "none"},
            "runtime_probe": {
                "docker_version": docker_version,
                "observed_configuration": prepared.observed_configuration,
                "platform": "linux/amd64",
                "source_commit": source_commit,
            },
            "source_mount": {
                "source_mount_mode": "read_only",
                "source_snapshot_sha256": source_snapshot_sha256,
            },
        }
        for kind, evidence_fact in evidence_facts.items():
            ref, _ = _environment_evidence_ref(
                stage_root=stage_root,
                attestation_id=attestation_id,
                profile_id=profile_id,
                artifact_kind=kind,
                facts=evidence_fact,
            )
            evidence_refs.append(ref)
        services: dict[str, Any] = {}
    else:
        facts = {
            **common_binding,
            "environment_identity": environment_instance_id,
            "runner_container_identity": runner_identity,
            "runner_image_digest": prepared.runner_image_id,
            "runner_network_mode": "isolated_internal_bridge",
            "runner_root_filesystem_mode": "read_only",
            "source_commit": source_commit,
            "source_mount_mode": "read_only",
            "writable_temp_outside_source": True,
        }
        for kind, evidence_fact in {
            "container_identity": {"runner_container_identity": runner_identity},
            "image_digest": {
                "build_definition_sha256": admission["runner_image"][
                    "build_definition"
                ]["sha256"],
                "runner_image_digest": prepared.runner_image_id,
            },
            "network_mode": {"runner_network_mode": "isolated_internal_bridge"},
            "runtime_probe": {
                "docker_version": docker_version,
                "environment_identity": environment_instance_id,
                "observed_configuration": prepared.observed_configuration,
                "source_commit": source_commit,
            },
            "source_mount": {
                "source_mount_mode": "read_only",
                "source_snapshot_sha256": source_snapshot_sha256,
            },
        }.items():
            ref, _ = _environment_evidence_ref(
                stage_root=stage_root,
                attestation_id=attestation_id,
                profile_id=profile_id,
                artifact_kind=kind,
                facts=evidence_fact,
            )
            evidence_refs.append(ref)
        definition = json.loads(
            guarantees._bound_material_bytes(
                root, profile["runtime_definition"], git_commit=source_commit
            )
        )
        service_contract = definition["service"]
        service_id = service_contract["id"]
        observed = prepared.service_facts
        service_facts = {
            "cleanup_completed": True,
            "container_identity": observed["container_identity"],
            "credentials": definition["isolation"]["credentials"],
            "database_identity": observed["database_identity"],
            "database_identity_scope": definition["isolation"]["database_identity"],
            "extension_versions": observed["extension_versions"],
            "image_digest": observed["image_digest"],
            "live_database": False,
            "network_mode": service_contract["network_mode"],
            "pg_dsn_sha256": observed["pg_dsn_sha256"],
            "postgresql_version": observed["postgresql_version"],
            "production_database": False,
            "publish_host": service_contract["publish_host"],
            "publish_port_mode": service_contract["publish_port"],
            "published_port": observed["published_port"],
            "session_isolation_key_sha256": observed[
                "session_isolation_key_sha256"
            ],
            "shared_development_database": False,
            "timescaledb_version": observed["timescaledb_version"],
        }
        service_refs: list[dict[str, str]] = []
        service_evidence = {
            "bootstrap_log": {
                "bootstrap_completed": True,
                "container_identity": service_facts["container_identity"],
                "database_identity": service_facts["database_identity"],
            },
            "cleanup_log": {
                "cleanup_completed": True,
                "container_identity": service_facts["container_identity"],
                "database_identity": service_facts["database_identity"],
            },
            "container_identity": {
                "container_identity": service_facts["container_identity"]
            },
            "database_identity": {
                "database_identity": service_facts["database_identity"]
            },
            "extension_versions": {
                "extension_versions": service_facts["extension_versions"]
            },
            "image_digest": {"image_digest": service_facts["image_digest"]},
            "published_endpoint": {
                "publish_host": service_facts["publish_host"],
                "published_port": service_facts["published_port"],
            },
            "server_version": {
                "postgresql_version": service_facts["postgresql_version"]
            },
        }
        for kind, evidence_fact in service_evidence.items():
            ref, _ = _environment_evidence_ref(
                stage_root=stage_root,
                attestation_id=attestation_id,
                profile_id=profile_id,
                artifact_kind=kind,
                facts=evidence_fact,
                service_id=service_id,
            )
            service_refs.append(ref)
        services = {
            service_id: {
                "environment_class": admission["environment_class"],
                "isolation": "disposable",
                "external_order_submission_enabled": False,
                "facts": service_facts,
                "evidence_refs": sorted(service_refs, key=lambda item: item["path"]),
            }
        }
    return {
        "profile_id": profile_id,
        "os": "Linux",
        "architecture": "amd64",
        "tool_versions": prepared.tool_versions,
        "lockfile_hashes": _profile_lockfile_hashes(root, profile, source_commit),
        "profile_admission": {
            "admission_id": admission["admission_id"],
            "environment_class": admission["environment_class"],
            "isolation": admission["isolation"],
            "external_order_submission_enabled": False,
            "runtime_definition": admission["runtime_definition"],
            "facts": facts,
            "evidence_refs": sorted(evidence_refs, key=lambda item: item["path"]),
        },
        "services": services,
    }


def run_attestation(
    *,
    root: Path,
    source_commit: str,
    stage_root: Path,
    private_root: Path,
    execution_admission: Path,
    selected_profile_ids: Sequence[str],
) -> Path:
    """Execute admitted Docker profiles and publish only after exact cleanup."""

    root = root.resolve()
    stage_root = stage_root.resolve()
    private_root = private_root.resolve()
    execution_admission = execution_admission.resolve()
    require_exact_clean_source(root, source_commit, stage_root)
    if _is_within(execution_admission, root):
        raise AssuranceExecutionError("execution_admission_must_be_outside_source_tree")
    if _is_within(execution_admission, stage_root):
        raise AssuranceExecutionError("execution_admission_must_be_outside_stage_tree")
    if not private_root.is_dir():
        raise AssuranceExecutionError("private_root_must_be_existing_directory")
    if private_root.stat().st_mode & 0o077:
        raise AssuranceExecutionError("private_root_must_be_owner_private_0700")
    disjoint_pairs = (
        (private_root, root),
        (private_root, stage_root),
    )
    if any(_is_within(left, right) or _is_within(right, left) for left, right in disjoint_pairs):
        raise AssuranceExecutionError(
            "private_root_must_be_disjoint_from_source_stage_and_admission"
        )
    if _is_within(execution_admission, private_root):
        raise AssuranceExecutionError("execution_admission_must_be_outside_private_root")
    bundle = guarantees.validate_repository(root=root)
    require_execution_model_ready(bundle.proof_catalog)
    profile_by_id = {
        item["id"]: item for item in bundle.proof_catalog["environment_profiles"]
    }
    selected = sorted(set(selected_profile_ids))
    if not selected:
        raise AssuranceExecutionError("at_least_one_profile_must_be_selected")
    unknown = sorted(set(selected) - set(profile_by_id))
    if unknown:
        raise AssuranceExecutionError(f"selected_profile_unknown:{','.join(unknown)}")
    automated_profile_ids = [
        profile_id
        for profile_id in selected
        if profile_by_id[profile_id]["execution_class"]
        in {"isolated_container", "isolated_database"}
    ]
    if not automated_profile_ids:
        raise AssuranceExecutionError(
            "manual_recovery_only:unavailable_without_finalizable_automated_environment"
        )
    if len(automated_profile_ids) != 1:
        raise AssuranceExecutionError(
            "one_automated_profile_per_attestation_required:"
            "final_review_accepts_multiple_independent_attestations"
        )
    admissions, admission_hash = load_execution_admission(
        execution_admission, source_commit
    )
    missing_admissions = sorted(set(automated_profile_ids) - set(admissions))
    if missing_admissions:
        raise AssuranceExecutionError(
            "execution_admission_profile_missing:" + ",".join(missing_admissions)
        )
    extra_admissions = sorted(set(admissions) - set(automated_profile_ids))
    if extra_admissions:
        raise AssuranceExecutionError(
            "execution_admission_unselected_profile:" + ",".join(extra_admissions)
        )
    for profile_id in automated_profile_ids:
        _align_execution_admission(
            admission=admissions[profile_id],
            profile=profile_by_id[profile_id],
            root=root,
            source_commit=source_commit,
        )

    source_archive = _git_archive_bytes(root, source_commit)
    source_snapshot_sha256 = _archive_tree_sha256(source_archive)
    session_started = _utc_now()
    suffix = (
        automated_profile_ids[0]
        if len(automated_profile_ids) == 1
        else "multi"
    )
    attestation_id = (
        f"QT-ATT-{session_started.strftime('%Y%m%dT%H%M%SZ')}-"
        f"{source_commit[:12]}-{suffix}"
    )
    session_evidence = (
        stage_root
        / "docs"
        / "assurance"
        / "guarantees"
        / "evidence"
        / attestation_id
    )
    if session_evidence.exists():
        raise AssuranceExecutionError(f"staged_session_already_exists:{attestation_id}")
    private_session_root = private_root / attestation_id
    if private_session_root.exists():
        raise AssuranceExecutionError("private_session_already_exists")

    states: dict[str, dict[str, Any]] = {}
    requested_profile_ids = list(selected)
    # Read-only daemon/image checks happen before the immutable draft. No
    # Docker create/network/volume command is permitted before every draft.
    for profile_id in automated_profile_ids:
        profile = profile_by_id[profile_id]
        admission = admissions[profile_id]
        environment_instance_id = f"qt-{secrets.token_hex(16)}"
        private_profile_root = private_session_root / profile_id
        snapshot_root = private_profile_root / "source"
        controller = docker_lifecycle.DockerController(
            admission=admission,
            root=snapshot_root,
            private_root=private_profile_root,
            source_commit=source_commit,
            attestation_id=attestation_id,
            profile_id=profile_id,
            environment_instance_id=environment_instance_id,
        )
        try:
            control_identity, docker_version = controller.verify_admission()
            controller.verify_runner_image()
            for service_id in sorted(admission["service_images"]):
                controller.verify_service_image(service_id)
        except docker_lifecycle.DockerLifecycleError as exc:
            raise AssuranceExecutionError(f"profile_preflight_unavailable:{profile_id}:{exc}") from exc
        admission_archive_payload = archive_execution_admission_profile(
            admission, source_commit, admission_hash
        )
        admission_archive_ref, admission_archive_hash = _environment_evidence_ref(
            stage_root=stage_root,
            attestation_id=attestation_id,
            profile_id=profile_id,
            artifact_kind="execution_admission_archive",
            facts=admission_archive_payload,
        )
        draft_facts = {
            "record_schema_version": guarantees.EXECUTION_DRAFT_SCHEMA_VERSION,
            "attestation_id": attestation_id,
            "source_commit": source_commit,
            "source_snapshot_sha256": source_snapshot_sha256,
            "requested_profile_ids": requested_profile_ids,
            "started_at": _timestamp(session_started),
            "admission_id": admission["admission_id"],
            "environment_instance_id": environment_instance_id,
            "control_plane_identity_sha256": control_identity,
            "runtime_definition_sha256": admission["runtime_definition"]["sha256"],
            "execution_admission_sha256": admission_hash,
            "execution_admission_archive_sha256": admission_archive_hash,
            "external_order_submission_enabled": False,
            "planned_resources": controller.planned_resources(
                profile["execution_class"]
            ),
        }
        draft_ref, draft_hash = _environment_evidence_ref(
            stage_root=stage_root,
            attestation_id=attestation_id,
            profile_id=profile_id,
            artifact_kind="execution_draft",
            facts=draft_facts,
        )
        states[profile_id] = {
            "profile": profile,
            "admission": admission,
            "controller": controller,
            "environment_instance_id": environment_instance_id,
            "control_identity": control_identity,
            "docker_version": docker_version,
            "draft_ref": draft_ref,
            "draft_hash": draft_hash,
            "admission_archive_ref": admission_archive_ref,
            "admission_archive_hash": admission_archive_hash,
            "snapshot_root": snapshot_root,
            "prepared": None,
        }

    return _run_cleanup_gated_session(
        root=root,
        source_commit=source_commit,
        stage_root=stage_root,
        private_session_root=private_session_root,
        session_started=session_started,
        attestation_id=attestation_id,
        source_archive=source_archive,
        source_snapshot_sha256=source_snapshot_sha256,
        selected_profile_ids=selected,
        automated_profile_ids=automated_profile_ids,
        states=states,
        admissions=admissions,
        admission_hash=admission_hash,
        bundle=bundle,
    )


def _run_cleanup_gated_session(
    *,
    root: Path,
    source_commit: str,
    stage_root: Path,
    private_session_root: Path,
    session_started: datetime,
    attestation_id: str,
    source_archive: bytes,
    source_snapshot_sha256: str,
    selected_profile_ids: Sequence[str],
    automated_profile_ids: Sequence[str],
    states: Mapping[str, dict[str, Any]],
    admissions: Mapping[str, Mapping[str, Any]],
    admission_hash: str,
    bundle: guarantees.ValidationBundle,
) -> Path:
    """Run one admitted profile and make cleanup the outermost side-effect guard."""

    if len(automated_profile_ids) != 1:
        raise AssuranceExecutionError("cleanup_gated_session_requires_one_profile")
    profile_id = automated_profile_ids[0]
    state = states[profile_id]
    profile = state["profile"]
    controller: docker_lifecycle.DockerController = state["controller"]
    active_proofs = sorted(
        (
            proof
            for proof in bundle.proof_catalog["proofs"]
            if proof["lifecycle"] == "active"
        ),
        key=lambda item: item["id"],
    )
    selected = set(selected_profile_ids)
    proof_results: list[dict[str, Any]] = []
    execution_state = "complete"
    execution_error: BaseException | None = None
    execution_started = _utc_now()
    execution_finished = execution_started
    results_hash: str | None = None
    execution_ref: dict[str, str] | None = None
    execution_hash: str | None = None
    intended_execution_hash: str | None = None
    cleanup_ref: dict[str, str] | None = None
    cleanup_hash: str | None = None
    cleanup: docker_lifecycle.CleanupReport | None = None
    cleanup_finished = execution_started
    manifest_error: BaseException | None = None
    cleanup_record_error: BaseException | None = None
    draft_only_manifest = {
        "record_schema_version": guarantees.EXECUTION_MANIFEST_SCHEMA_VERSION,
        "attestation_id": attestation_id,
        "source_commit": source_commit,
        "source_snapshot_sha256": source_snapshot_sha256,
        "execution_admission_archive_sha256": state["admission_archive_hash"],
        "execution_draft_sha256": state["draft_hash"],
        "environment_instance_id": state["environment_instance_id"],
        "control_plane_identity_sha256": state["control_identity"],
        "execution_state": "executor_error",
        "execution_started_at": _timestamp(execution_started),
        "execution_finished_at": _timestamp(execution_started),
        "executed_proof_ids": [],
        "proof_results_sha256": _sha256_bytes(_canonical_json_bytes([])),
        "resource_identities": [],
    }
    intended_execution_hash = _sha256_bytes(
        _canonical_json_bytes(
            _environment_evidence_payload(
                profile_id=profile_id,
                artifact_kind="execution_manifest",
                facts=draft_only_manifest,
            )
        )
    )

    with _installed_interrupt_handlers() as interrupt_state:
        try:
            try:
                controller.register_source_snapshot(source_snapshot_sha256)
                _extract_source_snapshot(source_archive, state["snapshot_root"])
                service_definition = None
                if profile["execution_class"] == "isolated_database":
                    service_definition = json.loads(
                        guarantees._bound_material_bytes(
                            root,
                            profile["runtime_definition"],
                            git_commit=source_commit,
                        )
                    )["service"]
                require_node = any(
                    proof["environment_profile_id"] == profile_id
                    and proof["runner"]["kind"] == "node_test"
                    for proof in active_proofs
                )
                state["prepared"] = controller.provision(
                    execution_class=profile["execution_class"],
                    require_node=require_node,
                    service_definition=service_definition,
                )

                for proof in active_proofs:
                    proof_profile_id = proof["environment_profile_id"]
                    if proof_profile_id not in selected:
                        proof_results.append(
                            {
                                "proof_id": proof["id"],
                                "environment_profile_id": proof_profile_id,
                                "status": "NOT_RUN",
                                "evidence_refs": [],
                                "reason_code": "profile_not_selected",
                            }
                        )
                        continue
                    supported, unsupported_reason = _runner_supported(proof["runner"])
                    if proof_profile_id != profile_id:
                        proof_results.append(
                            _unavailable_result(
                                stage_root=stage_root,
                                attestation_id=attestation_id,
                                source_commit=source_commit,
                                proof=proof,
                                reason_code=(
                                    unsupported_reason
                                    if not supported
                                    else "automated_profile_not_admitted"
                                ),
                                details=[
                                    proof["runner"]["kind"]
                                    if not supported
                                    else proof_profile_id
                                ],
                            )
                        )
                        continue
                    if not supported:
                        proof_results.append(
                            _unavailable_result(
                                stage_root=stage_root,
                                attestation_id=attestation_id,
                                source_commit=source_commit,
                                proof=proof,
                                reason_code=unsupported_reason,
                                details=[proof["runner"]["kind"]],
                                admission_payload=archive_execution_admission_profile(
                                    admissions[profile_id],
                                    source_commit,
                                    admission_hash,
                                ),
                            )
                        )
                        continue
                    if _snapshot_tree_sha256(state["snapshot_root"]) != source_snapshot_sha256:
                        raise AssuranceExecutionError(
                            f"source_snapshot_changed_before_proof:{proof['id']}"
                        )
                    executable_failure: tuple[str, str] | None = None
                    for executable, constraint in proof["runner"].get(
                        "required_executables", {}
                    ).items():
                        observed_version = controller.probe_required_executable(executable)
                        if observed_version is None:
                            executable_failure = (
                                "required_executable_unavailable",
                                executable,
                            )
                            break
                        try:
                            guarantees._version_satisfies(
                                observed_version,
                                constraint,
                                f"proof.{proof['id']}.required_executables.{executable}",
                            )
                        except guarantees.GuaranteeValidationError:
                            executable_failure = (
                                "required_executable_version_mismatch",
                                executable,
                            )
                            break
                    if executable_failure is not None:
                        proof_results.append(
                            _unavailable_result(
                                stage_root=stage_root,
                                attestation_id=attestation_id,
                                source_commit=source_commit,
                                proof=proof,
                                reason_code=executable_failure[0],
                                details=[executable_failure[1]],
                                admission_payload=archive_execution_admission_profile(
                                    admissions[profile_id],
                                    source_commit,
                                    admission_hash,
                                ),
                            )
                        )
                        continue
                    inner_argv = guarantees._canonical_runner_argv(proof["runner"])
                    if inner_argv is None:
                        raise AssuranceExecutionError(
                            f"runner_has_no_canonical_argv:{proof['id']}"
                        )
                    attempt_profile = PreparedProfile(
                        profile_id=profile_id,
                        environment={},
                        process_env=controller.process_env(),
                        admission_payload=archive_execution_admission_profile(
                            admissions[profile_id], source_commit, admission_hash
                        ),
                        container_source_root=PurePosixPath("/workspace"),
                    )
                    proof_started = _utc_now()
                    process = _run_process(
                        controller.proof_argv(
                            inner_argv, timeout_seconds=proof["timeout_seconds"]
                        ),
                        cwd=root,
                        env=controller.process_env(),
                        timeout_seconds=proof["timeout_seconds"] + 15,
                    )
                    safe_stdout, _ = controller.redact_durable_output(process.stdout)
                    safe_stderr, _ = controller.redact_durable_output(process.stderr)
                    process = ProcessResult(
                        safe_stdout,
                        safe_stderr,
                        process.exit_code,
                        process.timed_out,
                    )
                    proof_finished = _utc_now()
                    guard_marker = (
                        b"qt_assurance_process_guard:timeout_child_group_terminated\n"
                    )
                    if process.timed_out:
                        controller.terminate_runner()
                        raise AssuranceExecutionError(
                            f"docker_exec_host_failsafe_timeout:{proof['id']}"
                        )
                    if process.exit_code == 124 and guard_marker in process.stderr:
                        process = ProcessResult(
                            process.stdout,
                            process.stderr,
                            process.exit_code,
                            True,
                        )
                    proof_results.append(
                        _attempt_result(
                            stage_root=stage_root,
                            attestation_id=attestation_id,
                            proof=proof,
                            profile=attempt_profile,
                            root=root,
                            started_at=proof_started,
                            finished_at=proof_finished,
                            process=process,
                        )
                    )
                if _snapshot_tree_sha256(state["snapshot_root"]) != source_snapshot_sha256:
                    raise AssuranceExecutionError(
                        f"source_snapshot_changed_after_execution:{profile_id}"
                    )
                controller.verify_observed_configuration(state["prepared"])
            except _ExecutionInterrupted as exc:
                execution_state = "interrupted"
                execution_error = exc
            except BaseException as exc:
                execution_state = "executor_error"
                execution_error = exc
            execution_finished = _utc_now()

            # From this point until every cleanup attempt completes, signals are
            # recorded but never allowed to bypass cleanup or later evidence writes.
            interrupt_state.cleanup_in_progress = True
            try:
                completed_proof_ids = {item["proof_id"] for item in proof_results}
                for proof in active_proofs:
                    if proof["id"] in completed_proof_ids:
                        continue
                    proof_results.append(
                        {
                            "proof_id": proof["id"],
                            "environment_profile_id": proof["environment_profile_id"],
                            "status": "NOT_RUN",
                            "evidence_refs": [],
                            "reason_code": (
                                "execution_interrupted"
                                if execution_state == "interrupted"
                                else "executor_error_before_attempt"
                            ),
                        }
                    )
                proof_results.sort(key=lambda item: item["proof_id"])
                results_hash = guarantees.proof_results_sha256(proof_results)
                try:
                    controller.discover_labeled_resources()
                except BaseException as exc:
                    if execution_error is None:
                        execution_error = exc
                    execution_state = "executor_error"
                prepared = state["prepared"] or controller.partial_profile(
                    profile["execution_class"]
                )
                state["prepared"] = prepared
                executed_ids = sorted(
                    item["proof_id"]
                    for item in proof_results
                    if item["environment_profile_id"] == profile_id
                    and item["status"] in {"PASS", "FAIL", "PARTIAL"}
                )
                manifest_facts = {
                    "record_schema_version": guarantees.EXECUTION_MANIFEST_SCHEMA_VERSION,
                    "attestation_id": attestation_id,
                    "source_commit": source_commit,
                    "source_snapshot_sha256": source_snapshot_sha256,
                    "execution_admission_archive_sha256": state[
                        "admission_archive_hash"
                    ],
                    "execution_draft_sha256": state["draft_hash"],
                    "environment_instance_id": state["environment_instance_id"],
                    "control_plane_identity_sha256": state["control_identity"],
                    "execution_state": execution_state,
                    "execution_started_at": _timestamp(execution_started),
                    "execution_finished_at": _timestamp(execution_finished),
                    "executed_proof_ids": executed_ids,
                    "proof_results_sha256": results_hash,
                    "resource_identities": prepared.sorted_resources(),
                }
                intended_execution_hash = _sha256_bytes(
                    _canonical_json_bytes(
                        _environment_evidence_payload(
                            profile_id=profile_id,
                            artifact_kind="execution_manifest",
                            facts=manifest_facts,
                        )
                    )
                )
                execution_ref, execution_hash = _environment_evidence_ref(
                    stage_root=stage_root,
                    attestation_id=attestation_id,
                    profile_id=profile_id,
                    artifact_kind="execution_manifest",
                    facts=manifest_facts,
                )
            except BaseException as exc:
                manifest_error = exc
                if execution_error is None:
                    execution_error = exc
                execution_state = "executor_error"
                prepared = state["prepared"] or controller.partial_profile(
                    profile["execution_class"]
                )
                state["prepared"] = prepared
                if results_hash is None:
                    results_hash = _sha256_bytes(
                        _canonical_json_bytes(
                            sorted(
                                proof_results,
                                key=lambda item: str(item.get("proof_id", "")),
                            )
                        )
                    )
                fallback_manifest_facts = {
                    "record_schema_version": guarantees.EXECUTION_MANIFEST_SCHEMA_VERSION,
                    "attestation_id": attestation_id,
                    "source_commit": source_commit,
                    "source_snapshot_sha256": source_snapshot_sha256,
                    "execution_admission_archive_sha256": state[
                        "admission_archive_hash"
                    ],
                    "execution_draft_sha256": state["draft_hash"],
                    "environment_instance_id": state["environment_instance_id"],
                    "control_plane_identity_sha256": state["control_identity"],
                    "execution_state": "executor_error",
                    "execution_started_at": _timestamp(execution_started),
                    "execution_finished_at": _timestamp(_utc_now()),
                    "executed_proof_ids": sorted(
                        item["proof_id"]
                        for item in proof_results
                        if item.get("environment_profile_id") == profile_id
                        and item.get("status") in {"PASS", "FAIL", "PARTIAL"}
                    ),
                    "proof_results_sha256": results_hash,
                    "resource_identities": prepared.sorted_resources(),
                }
                intended_execution_hash = _sha256_bytes(
                    _canonical_json_bytes(
                        _environment_evidence_payload(
                            profile_id=profile_id,
                            artifact_kind="execution_manifest",
                            facts=fallback_manifest_facts,
                        )
                    )
                )
                if execution_hash is None:
                    try:
                        execution_ref, execution_hash = _environment_evidence_ref(
                            stage_root=stage_root,
                            attestation_id=attestation_id,
                            profile_id=profile_id,
                            artifact_kind="execution_manifest",
                            facts=fallback_manifest_facts,
                        )
                    except BaseException:
                        # Cleanup still binds the intended immutable manifest hash.
                        pass
        finally:
            interrupt_state.cleanup_in_progress = True
            prepared = state["prepared"] or controller.partial_profile(
                profile["execution_class"]
            )
            state["prepared"] = prepared
            cleanup_started = _utc_now()
            try:
                cleanup = controller.cleanup(prepared)
            except BaseException as exc:
                cleanup = docker_lifecycle.CleanupReport(
                    stdout="",
                    stderr=f"cleanup controller error:{type(exc).__name__}\n",
                    exit_code=1,
                    resources=tuple(
                        docker_lifecycle.CleanupResource(
                            item.kind,
                            item.logical_name,
                            item.runtime_identity,
                            False,
                        )
                        for item in sorted(
                            prepared.resources,
                            key=lambda value: (value.kind, value.logical_name),
                        )
                    ),
                    label_query_remaining=("cleanup-controller-error",),
                )
                if execution_error is None:
                    execution_error = exc
            cleanup_finished = _utc_now()
            cleanup_stdout, _ = controller.redact_durable_output(
                cleanup.stdout.encode("utf-8")
            )
            cleanup_stderr, _ = controller.redact_durable_output(
                cleanup.stderr.encode("utf-8")
            )
            cleanup_execution_hash = execution_hash or intended_execution_hash
            if cleanup_execution_hash is not None:
                cleanup_facts = {
                    "record_schema_version": guarantees.CLEANUP_MANIFEST_SCHEMA_VERSION,
                    "attestation_id": attestation_id,
                    "source_commit": source_commit,
                    "source_snapshot_sha256": source_snapshot_sha256,
                    "execution_admission_archive_sha256": state[
                        "admission_archive_hash"
                    ],
                    "execution_draft_sha256": state["draft_hash"],
                    "execution_manifest_sha256": cleanup_execution_hash,
                    "environment_instance_id": state["environment_instance_id"],
                    "control_plane_identity_sha256": state["control_identity"],
                    "cleanup_started_at": _timestamp(cleanup_started),
                    "cleanup_finished_at": _timestamp(cleanup_finished),
                    "attempt_number": 1,
                    "cleanup_state": (
                        "interrupted"
                        if interrupt_state.signals
                        else ("passed" if cleanup.completed else "failed")
                    ),
                    "exit_code": cleanup.exit_code,
                    "stdout": cleanup_stdout.decode("utf-8", errors="replace"),
                    "stdout_sha256": _sha256_bytes(cleanup_stdout),
                    "stderr": cleanup_stderr.decode("utf-8", errors="replace"),
                    "stderr_sha256": _sha256_bytes(cleanup_stderr),
                    "cleanup_completed": cleanup.completed,
                    "resources": [item.as_record() for item in cleanup.resources],
                    "label_query_remaining": list(cleanup.label_query_remaining),
                }
                try:
                    cleanup_ref, cleanup_hash = _environment_evidence_ref(
                        stage_root=stage_root,
                        attestation_id=attestation_id,
                        profile_id=profile_id,
                        artifact_kind="cleanup_manifest",
                        facts=cleanup_facts,
                        suffix="attempt-001",
                    )
                except BaseException as exc:
                    cleanup_record_error = exc
                    if execution_error is None:
                        execution_error = exc

    if interrupt_state.signals and execution_state == "complete":
        execution_state = "interrupted"
        execution_error = _ExecutionInterrupted(interrupt_state.signals[0])
    finalizable = (
        execution_state == "complete"
        and manifest_error is None
        and cleanup_record_error is None
        and execution_ref is not None
        and execution_hash is not None
        and cleanup_ref is not None
        and cleanup_hash is not None
        and cleanup is not None
        and cleanup.completed
        and not interrupt_state.signals
    )
    if not finalizable:
        reason = (
            f"{type(execution_error).__name__}:{execution_error}"
            if execution_error is not None
            else "cleanup_or_manifest_not_proven"
        )
        reason_bytes = reason.encode("utf-8", errors="replace")
        reason_bytes, _ = controller.redact_durable_output(reason_bytes)
        raise AssuranceExecutionError(
            f"session_not_finalizable:{execution_state}:"
            f"{reason_bytes.decode('utf-8', errors='replace')}"
        )

    assert results_hash is not None
    assert execution_ref is not None and execution_hash is not None
    assert cleanup_ref is not None and cleanup_hash is not None
    environments = [
        _build_final_environment(
            root=root,
            source_commit=source_commit,
            stage_root=stage_root,
            attestation_id=attestation_id,
            profile=profile,
            admission=state["admission"],
            prepared=state["prepared"],
            docker_version=state["docker_version"],
            source_snapshot_sha256=source_snapshot_sha256,
            execution_admission_sha256=admission_hash,
            execution_admission_archive_ref=state["admission_archive_ref"],
            execution_admission_archive_hash=state["admission_archive_hash"],
            draft_ref=state["draft_ref"],
            draft_hash=state["draft_hash"],
            execution_ref=execution_ref,
            execution_hash=execution_hash,
            cleanup_ref=cleanup_ref,
            cleanup_hash=cleanup_hash,
            proof_results_hash=results_hash,
            environment_instance_id=state["environment_instance_id"],
            control_plane_identity_sha256=state["control_identity"],
        )
    ]
    attestation = {
        "schema_version": guarantees.ATTESTATION_SCHEMA_VERSION,
        "attestation_id": attestation_id,
        "source": {
            "git_commit": source_commit,
            "clean": True,
            "assurance_material_sha256": guarantees.assurance_material_sha256(
                bundle, git_commit=source_commit
            ),
        },
        "inputs": _attestation_inputs(bundle, source_commit),
        "environments": environments,
        "started_at": _timestamp(session_started),
        "finished_at": _timestamp(cleanup_finished),
        "proof_results": proof_results,
        "guarantee_results": derive_guarantee_results(
            bundle.registry, bundle.proof_catalog, proof_results
        ),
    }
    require_exact_clean_source(root, source_commit, stage_root)
    guarantees.validate_attestation_historically(
        attestation, bundle, evidence_root=stage_root
    )
    attestation_path = (
        stage_root
        / "docs"
        / "assurance"
        / "guarantees"
        / "attestations"
        / source_commit
        / f"{attestation_id}.json"
    )
    _write_immutable_bytes(
        attestation_path,
        json.dumps(
            attestation, indent=2, ensure_ascii=False, sort_keys=False
        ).encode("utf-8")
        + b"\n",
    )
    for directory in (private_session_root / profile_id, private_session_root):
        try:
            directory.rmdir()
        except OSError:
            pass
    return attestation_path


def validate_staged(
    *, root: Path, attestation_path: Path, evidence_root: Path
) -> dict[str, Any]:
    bundle = guarantees.validate_repository(root=root.resolve())
    return guarantees.validate_attestation_file_historically(
        attestation_path.resolve(), bundle, evidence_root=evidence_root.resolve()
    )


def inspect_execution_admission(
    *,
    root: Path,
    source_commit: str,
    docker_path: Path,
    runner_image: str,
    runner_build_definition: str,
    selected_profile_ids: Sequence[str],
    output_path: Path,
) -> Path:
    """Emit a read-only, deliberately non-approvable admission review packet."""

    root = root.resolve()
    output_path = output_path.resolve()
    if _is_within(output_path, root):
        raise AssuranceExecutionError("admission_inspection_output_must_be_outside_source")
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise AssuranceExecutionError("admission_inspection_source_commit_invalid")
    require_exact_clean_source(root, source_commit, output_path.parent)
    bundle = guarantees.validate_repository(root=root)
    profile_by_id = {
        item["id"]: item for item in bundle.proof_catalog["environment_profiles"]
    }
    selected = sorted(set(selected_profile_ids))
    if not selected:
        raise AssuranceExecutionError("admission_inspection_profile_required")
    unknown = sorted(set(selected) - set(profile_by_id))
    if unknown:
        raise AssuranceExecutionError(
            "admission_inspection_profile_unknown:" + ",".join(unknown)
        )
    if any(
        profile_by_id[item]["execution_class"]
        not in {"isolated_container", "isolated_database"}
        for item in selected
    ):
        raise AssuranceExecutionError(
            "admission_inspection_automated_profiles_only"
        )
    resolved_docker = docker_path.resolve()
    if not resolved_docker.is_file():
        raise AssuranceExecutionError("admission_inspection_docker_unavailable")
    build_hash = guarantees._bound_material_sha256(
        root, runner_build_definition, git_commit=source_commit
    )
    build_bytes = guarantees._bound_material_bytes(
        root, runner_build_definition, git_commit=source_commit
    )
    base_digests = sorted(
        {
            f"sha256:{digest.decode('ascii')}"
            for digest in re.findall(rb"@sha256:([0-9a-f]{64})", build_bytes)
        }
    )
    probe = docker_lifecycle.DockerController(
        admission={"docker_tool": {"resolved_path": str(resolved_docker)}},
        root=root,
        private_root=output_path.parent,
        source_commit=source_commit,
        attestation_id="QT-ATT-19700101T000000Z-000000000000-inspect",
        profile_id=selected[0],
        environment_instance_id="qt-admission-inspection",
    )
    _, daemon_identity, docker_version = probe.control_plane()
    runner = probe._image_inspect(runner_image)
    runner_image_id = runner.get("Id")
    runner_platform = f"{runner.get('Os')}/{runner.get('Architecture')}"
    if not isinstance(runner_image_id, str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", runner_image_id
    ):
        raise AssuranceExecutionError("admission_inspection_runner_image_id_invalid")
    if runner_platform != "linux/amd64":
        raise AssuranceExecutionError("admission_inspection_runner_platform_unsupported")
    labels = (runner.get("Config") or {}).get("Labels") or {}
    if labels.get(docker_lifecycle.BUILD_DEFINITION_LABEL) != build_hash:
        raise AssuranceExecutionError(
            "admission_inspection_runner_build_definition_label_mismatch"
        )
    profiles: list[dict[str, Any]] = []
    for profile_id in selected:
        profile = profile_by_id[profile_id]
        service_images: dict[str, dict[str, str]] = {}
        if profile["execution_class"] == "isolated_database":
            definition = json.loads(
                guarantees._bound_material_bytes(
                    root,
                    profile["runtime_definition"],
                    git_commit=source_commit,
                )
            )
            service = definition["service"]
            reference = service["image"]
            image_digest = "sha256:" + reference.rsplit("@sha256:", 1)[-1]
            observed = probe._image_inspect(reference)
            observed_id = observed.get("Id")
            repo_digests = observed.get("RepoDigests") or []
            if (
                not isinstance(observed_id, str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", observed_id)
                or not any(
                    isinstance(item, str) and item.endswith("@" + image_digest)
                    for item in repo_digests
                )
            ):
                raise AssuranceExecutionError(
                    f"admission_inspection_service_image_unbound:{service['id']}"
                )
            service_images[service["id"]] = {
                "reference": reference,
                "image_id": observed_id,
                "image_digest": image_digest,
            }
        profiles.append(
            {
                "profile_id": profile_id,
                "admission_id": "<OWNER-REVIEW-REQUIRED>",
                "environment_class": "isolated_test",
                "isolation": "session_scoped",
                "external_order_submission_enabled": False,
                "runtime_definition": {
                    "path": profile["runtime_definition"],
                    "sha256": guarantees._bound_material_sha256(
                        root,
                        profile["runtime_definition"],
                        git_commit=source_commit,
                    ),
                },
                "docker_tool": {
                    "resolved_path": str(resolved_docker),
                    "version": docker_version,
                    "executable_sha256": _sha256_file_binary(resolved_docker),
                    "daemon_identity_sha256": daemon_identity,
                },
                "runner_image": {
                    "image_id": runner_image_id,
                    "platform": runner_platform,
                    "base_image_digests": base_digests,
                    "build_definition": {
                        "path": runner_build_definition,
                        "sha256": build_hash,
                    },
                },
                "service_images": service_images,
            }
        )
    packet = {
        "schema_version": "qt.assurance_execution_admission_inspection.v1",
        "review_required": True,
        "generated_at": _timestamp(_utc_now()),
        "candidate_execution_admission": {
            "schema_version": EXECUTION_ADMISSION_SCHEMA_VERSION,
            "source_commit": source_commit,
            "profiles": profiles,
        },
    }
    _write_immutable_bytes(output_path, _canonical_json_bytes(packet))
    return output_path


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="execute selected admitted profiles")
    run_parser.add_argument("--source-commit", required=True)
    run_parser.add_argument("--stage-root", type=Path, required=True)
    run_parser.add_argument("--private-root", type=Path, required=True)
    run_parser.add_argument("--execution-admission", type=Path, required=True)
    run_parser.add_argument("--profile", action="append", required=True)
    inspect_parser = subparsers.add_parser(
        "inspect-admission",
        help="write a read-only, owner-review-required execution-admission skeleton",
    )
    inspect_parser.add_argument("--source-commit", required=True)
    inspect_parser.add_argument("--docker", type=Path, required=True)
    inspect_parser.add_argument("--runner-image", required=True)
    inspect_parser.add_argument("--runner-build-definition", required=True)
    inspect_parser.add_argument("--profile", action="append", required=True)
    inspect_parser.add_argument("--output", type=Path, required=True)
    validate_parser = subparsers.add_parser(
        "validate-staged", help="historically validate an externally staged attestation"
    )
    validate_parser.add_argument("--attestation", type=Path, required=True)
    validate_parser.add_argument("--evidence-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "run":
            path = run_attestation(
                root=args.root,
                source_commit=args.source_commit,
                stage_root=args.stage_root,
                private_root=args.private_root,
                execution_admission=args.execution_admission,
                selected_profile_ids=args.profile,
            )
            print(path)
        elif args.command == "inspect-admission":
            path = inspect_execution_admission(
                root=args.root,
                source_commit=args.source_commit,
                docker_path=args.docker,
                runner_image=args.runner_image,
                runner_build_definition=args.runner_build_definition,
                selected_profile_ids=args.profile,
                output_path=args.output,
            )
            print(path)
        else:
            validated = validate_staged(
                root=args.root,
                attestation_path=args.attestation,
                evidence_root=args.evidence_root,
            )
            print(
                "staged attestation valid: "
                f"{validated['attestation_id']} at {validated['source']['git_commit']}"
            )
    except (
        AssuranceExecutionError,
        docker_lifecycle.DockerLifecycleError,
        guarantees.GuaranteeValidationError,
    ) as exc:
        print(f"assurance_execution_failed:{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())

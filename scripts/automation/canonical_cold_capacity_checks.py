"""Enforce the reviewed cutover's physical headroom on the serving host."""

from dataclasses import asdict
from datetime import UTC, datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
from time import monotonic

from core.storage_mounts import inspect_filesystem
from market_data.fact_archive import archive_evidence_hash
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields
from scripts.reporting.server_filesystem_capacity import _authority


VERSION = "qt.canonical_cold_capacity_plan.v1"
_DEVICE_PLAN = {"path", "filesystem_uuid", "reserve_bytes", "temporary_bytes"}
_DOCKER_INFO = '{"DockerRootDir":{{json .DockerRootDir}},"OperatingSystem":{{json .OperatingSystem}},' \
               '"KernelVersion":{{json .KernelVersion}}}'


def _absolute_path(value):
    if not isinstance(value, str):
        raise ValueError("canonical_cold_capacity_invalid_path")
    path = PurePosixPath(value)
    if path.root != "/" or path == PurePosixPath("/") or ".." in path.parts or str(path) != value:
        raise ValueError("canonical_cold_capacity_invalid_path")
    return path


def _device_plan(plan):
    if not isinstance(plan, dict) or plan.keys() != _DEVICE_PLAN:
        raise ValueError("canonical_cold_capacity_invalid_device_plan")
    path, uuid = plan["path"], plan["filesystem_uuid"]
    _absolute_path(path)
    if (not isinstance(uuid, str) or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", uuid)
            or any(type(plan[key]) is not int or not 1 <= plan[key] < 2**63
                   for key in ("reserve_bytes", "temporary_bytes"))):
        raise ValueError("canonical_cold_capacity_invalid_device_plan")
    return plan


def _mounted_path(mounts, path):
    path = _absolute_path(path)
    matches = []
    for mount in mounts:
        target = _absolute_path(mount.get("Destination"))
        if path.is_relative_to(target):
            matches.append((target, mount))
    if not matches:
        raise RuntimeError("canonical_cold_capacity_postgresql_path_unmounted")
    target, mount = max(matches, key=lambda row: len(row[0].parts))
    if (sum(candidate == target for candidate, _ in matches) != 1
            or mount.get("Type") not in ("bind", "volume") or mount.get("RW") is not True):
        raise RuntimeError("canonical_cold_capacity_postgresql_mount_unqualified")
    return _absolute_path(mount.get("Source")) / path.relative_to(target)


def bind_approved_capacity_checks(*, package_path, package, archive_root, database_container):
    """Bind the approved peaks and return a fresh, read-only physical check."""
    artifact = package["artifacts"]["physical_capacity"]
    digest, raw = _hash_file(Path(package_path).parent / artifact["filename"], json_limit=65_536)
    if digest != artifact["sha256"]:
        raise RuntimeError("canonical_cold_capacity_approved_plan_changed")
    plan = json.loads(raw, object_pairs_hook=_unique_fields)
    if (not isinstance(plan, dict) or plan.keys() != {"schema", "rehearsal_sha256", "ssd", "hdd"}
            or plan["schema"] != VERSION or not _pin(plan["rehearsal_sha256"])):
        raise ValueError("canonical_cold_capacity_invalid_plan")
    ssd, hdd = _device_plan(plan["ssd"]), _device_plan(plan["hdd"])
    archive = _absolute_path(str(archive_root))
    max_copy = package["scope"]["raw_mapping_index"]["max_copy_bytes"]
    if (ssd["filesystem_uuid"] == hdd["filesystem_uuid"] or ssd["path"] == hdd["path"]
            or not archive.is_relative_to(hdd["path"]) or hdd["temporary_bytes"] < max_copy or not _pin(database_container)):
        raise ValueError("canonical_cold_capacity_plan_scope_changed")

    def check(*, history_filesystem_uuid, raw_index_bytes, database_directory, history_directory, timeout_seconds):
        if history_filesystem_uuid != hdd["filesystem_uuid"]:
            raise RuntimeError("canonical_cold_capacity_history_registration_changed")
        if type(raw_index_bytes) is not int or not 0 < raw_index_bytes <= max_copy:
            raise RuntimeError("canonical_cold_capacity_raw_index_copy_budget_exceeded")
        deadline = monotonic() + timeout_seconds

        def remaining():
            seconds = deadline - monotonic()
            if seconds <= 0:
                raise TimeoutError("canonical_cold_capacity_deadline")
            return min(5, seconds)

        # The host owner must see the same mount namespace as its local Docker
        # engine. A non-local Docker endpoint could report a coincidental path.
        docker_host = os.environ.get("DOCKER_HOST", "")
        if os.environ.get("DOCKER_CONTEXT") or not docker_host:
            endpoint = json.loads(host.docker("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}",
                                              timeout=remaining()))
        else:
            endpoint = docker_host
        if not isinstance(endpoint, str) or not endpoint.startswith("unix:///"):
            raise RuntimeError("canonical_cold_capacity_requires_local_docker")
        info = json.loads(host.docker("info", "--format", _DOCKER_INFO, timeout=remaining()))
        runtime, physical = _authority(info)
        if runtime != "native_linux" or not physical or info.get("DockerRootDir") != ssd["path"]:
            raise RuntimeError("canonical_cold_capacity_docker_root_changed")
        observed = {}
        for name, approved in (("ssd", ssd), ("hdd", hdd)):
            evidence = inspect_filesystem(Path(approved["path"]), expected_uuid=approved["filesystem_uuid"],
                                          require_writable=False)
            if evidence.path != approved["path"] or evidence.read_only:
                raise RuntimeError(f"canonical_cold_capacity_device_path_changed: tier={name}")
            required = approved["reserve_bytes"] + approved["temporary_bytes"]
            if evidence.available_bytes < required:
                raise RuntimeError(f"canonical_cold_capacity_insufficient: tier={name} "
                                   f"available={evidence.available_bytes} required={required}")
            observed[name] = {**asdict(evidence), "required_free_bytes": required}
        actual_archive = inspect_filesystem(Path(str(archive)), expected_uuid=hdd["filesystem_uuid"])
        if (observed["ssd"]["device_id"] == observed["hdd"]["device_id"]
                or actual_archive.path != str(archive) or actual_archive.device_id != observed["hdd"]["device_id"]):
            raise RuntimeError("canonical_cold_capacity_requires_distinct_bound_devices")
        mounts = json.loads(host.docker("inspect", "--format", "{{json .Mounts}}", database_container, timeout=remaining()))
        if not isinstance(mounts, list) or not 1 <= len(mounts) <= 64 or not all(isinstance(m, dict) for m in mounts):
            raise RuntimeError("canonical_cold_capacity_invalid_postgresql_mounts")
        postgresql = {}
        for name, directory, tier in (("database", database_directory, "ssd"),
                                      ("wal", str(_absolute_path(database_directory) / "pg_wal"), "ssd"),
                                      ("history", history_directory, "hdd")):
            path = _mounted_path(mounts, directory)
            device = inspect_filesystem(Path(str(path)), expected_uuid=observed[tier]["filesystem_uuid"],
                                        require_writable=False)
            if device.device_id != observed[tier]["device_id"] or device.read_only:
                raise RuntimeError(f"canonical_cold_capacity_postgresql_device_changed: member={name}")
            postgresql[name] = device.path
        remaining()
        receipt = {"plan_sha256": digest, "observed_at": datetime.now(UTC).isoformat(),
                   "devices": observed, "archive_path": actual_archive.path, "raw_index_bytes": raw_index_bytes,
                   "postgresql": postgresql}
        return receipt | {"receipt_hash": archive_evidence_hash(receipt)}

    return check

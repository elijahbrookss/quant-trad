"""One read-only NVMe/archive capacity sample for the native server sidecar.

Prints bounded JSON to normal container stdout (Alloy owns delivery to Loki).
No recursive size scans, database writes, direct log shipping, or cleanup.
"""

from __future__ import annotations

import json
import os
import posixpath
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from core.storage_mounts import StorageMountError, inspect_filesystem


RESOURCES = (
    ("docker-engine-storage", "docker_engine_storage"),
    ("market-archive-storage", "market_archive_storage"),
)


def _superblock_read_only(device_id: str, mountinfo: str) -> bool:
    """Ignore our observer's read-only bind; inspect the underlying filesystem."""

    modes = set()
    for line in mountinfo.splitlines():
        left, separator, right = line.partition(" - ")
        fields = left.split()
        if len(fields) < 6 or fields[2] != device_id:
            continue
        details = right.split()
        if not separator or len(details) < 3:
            raise StorageMountError("storage_mountinfo_invalid: malformed device entry")
        mode = set(details[2].split(",")) & {"ro", "rw"}
        if len(mode) != 1:
            raise StorageMountError("storage_mountinfo_invalid: missing superblock mode")
        modes.update(mode)
    if len(modes) != 1:
        raise StorageMountError(
            f"storage_mountinfo_unavailable: device={device_id} modes={sorted(modes)}"
        )
    return modes == {"ro"}


def _authority(info: Mapping[str, Any]) -> tuple[str, bool]:
    operating_system = str(info.get("OperatingSystem") or "")
    kernel = str(info.get("KernelVersion") or "")
    if not operating_system or not kernel:
        raise StorageMountError("storage_authority_unavailable: Docker host identity missing")
    probe = f"{operating_system} {kernel}".lower()
    if any(value in probe for value in ("docker desktop", "microsoft", "wsl")):
        return "virtualized_docker_desktop", False
    return "native_linux", True


def collect_samples(
    *,
    engine_info: Mapping[str, Any],
    docker_root: Path,
    archive_root: Path,
    docker_host_root: str,
    expected_archive_uuid: str,
    udev_root: Path,
    mountinfo: str,
    observed_at: str,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for (resource_id, scope), path in zip(RESOURCES, (docker_root, archive_root), strict=True):
        common: dict[str, Any] = {
            "observed_at": observed_at,
            "resource_id": resource_id,
            "capacity_scope": scope,
            "capacity_authority": "unknown",
            "runtime_kind": "unknown",
            "physical_host_visible": False,
        }
        try:
            runtime_kind, physical = _authority(engine_info)
            common.update(
                runtime_kind=runtime_kind,
                physical_host_visible=physical,
                capacity_authority=(
                    "engine_storage_filesystem" if resource_id == "docker-engine-storage"
                    else "archive_filesystem"
                ) if physical else "virtual_guest_storage",
            )
            if resource_id == "docker-engine-storage":
                actual_root = str(engine_info.get("DockerRootDir") or "")
                if (
                    not actual_root.startswith("/")
                    or not docker_host_root.startswith("/")
                    or posixpath.normpath(actual_root) != posixpath.normpath(docker_host_root)
                ):
                    raise StorageMountError(
                        "storage_engine_root_mismatch: "
                        f"configured={docker_host_root} actual={actual_root}"
                    )
            evidence = inspect_filesystem(
                path,
                expected_uuid=(expected_archive_uuid if resource_id == "market-archive-storage" else ""),
                udev_root=udev_root,
                # Telemetry intentionally has a read-only bind to both disks.
                require_writable=False,
            )
            if _superblock_read_only(evidence.device_id, mountinfo):
                raise StorageMountError(
                    f"storage_filesystem_read_only: resource={resource_id} device={evidence.device_id}"
                )
            allocatable = evidence.used_bytes + evidence.available_bytes
            if allocatable <= 0:
                raise StorageMountError("storage_capacity_unavailable: no allocatable capacity")
            samples.append({
                **common, "sample_kind": "filesystem",
                "path": evidence.path, "device_id": evidence.device_id,
                "filesystem_uuid": evidence.filesystem_uuid,
                "total_bytes": evidence.total_bytes,
                "used_bytes": evidence.used_bytes,
                "available_bytes": evidence.available_bytes,
                # Matches df: filesystem-reserved space is not application headroom.
                "used_percent": 100.0 * evidence.used_bytes / allocatable,
                "observer_read_only": evidence.read_only,
            })
            samples.append({**common, "sample_kind": "storage_health", "available": 1})
        except (StorageMountError, OSError, ValueError) as exc:
            samples.append({**common, "sample_kind": "capacity_unavailable", "reason": str(exc)[:512]})
            samples.append({**common, "sample_kind": "storage_health", "available": 0})
    return samples


def main() -> int:
    observed_at = datetime.now(UTC).isoformat()
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{json .}}"],
            check=True, capture_output=True, text=True, timeout=8,
        )
        info = json.loads(result.stdout)
        if not isinstance(info, dict):
            raise ValueError("Docker info must return an object")
        samples = collect_samples(
            engine_info=info,
            docker_root=Path(os.environ.get("QT_DOCKER_STORAGE_MOUNT", "/host-docker")),
            archive_root=Path(os.environ.get("QT_ARCHIVE_STORAGE_MOUNT", "/host-archive")),
            docker_host_root=os.environ.get("QT_DOCKER_HOST_STORAGE_ROOT", "/var/lib/docker"),
            expected_archive_uuid=os.environ.get("QT_MARKET_DATA_EXPECTED_UUID", ""),
            udev_root=Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/qt-host-udev/data")),
            mountinfo=Path("/proc/self/mountinfo").read_text(encoding="utf-8"),
            observed_at=observed_at,
        )
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        # Failure is evidence of unavailability, never a fabricated zero sample.
        samples = []
        for resource_id, scope in RESOURCES:
            common = {
                "observed_at": observed_at, "resource_id": resource_id,
                "capacity_scope": scope, "physical_host_visible": False,
                "runtime_kind": "unknown", "capacity_authority": "unknown",
            }
            samples.extend([
                {**common, "sample_kind": "capacity_unavailable", "reason": f"{type(exc).__name__}: {exc}"[:512]},
                {**common, "sample_kind": "storage_health", "available": 0},
            ])
    for sample in samples:
        print(json.dumps(sample, sort_keys=True, separators=(",", ":")), flush=True)
    return 1 if any(s["sample_kind"] == "capacity_unavailable" for s in samples) else 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Fail-closed Linux filesystem admission for the single-node archive tier.

The kernel device number identifies the filesystem actually serving a path.
The host udev database binds that number to its filesystem UUID. Reading this
metadata needs neither block-device access nor a privileged container. An
on-disk marker is deliberately not accepted as filesystem identity.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path


class StorageMountError(RuntimeError):
    """Configured storage is unavailable; never create a fallback directory."""


@dataclass(frozen=True)
class FilesystemEvidence:
    path: str
    device_id: str
    filesystem_uuid: str | None
    total_bytes: int
    used_bytes: int
    available_bytes: int
    read_only: bool


def inspect_filesystem(
    path: Path,
    *,
    expected_uuid: str = "",
    udev_root: Path = Path("/run/udev/data"),
    require_writable: bool = True,
) -> FilesystemEvidence:
    """Read mount identity and capacity without writing or creating paths."""

    expected_uuid = expected_uuid.strip()
    if expected_uuid and not re.fullmatch(r"[A-Za-z0-9-]{4,128}", expected_uuid):
        raise StorageMountError("storage_mount_invalid: expected UUID is malformed")
    root = Path(path)
    if not root.is_absolute():
        raise StorageMountError(
            f"storage_mount_invalid: absolute path required path={root}"
        )
    try:
        root = root.resolve(strict=True)
        if not root.is_dir():
            raise StorageMountError(
                f"storage_mount_missing: not a directory path={root}"
            )
        device = root.stat().st_dev
        capacity = os.statvfs(root)
        device_id = f"{os.major(device)}:{os.minor(device)}"
        actual_uuid = None
        if expected_uuid:
            properties = (udev_root / f"b{device_id}").read_text(encoding="utf-8")
            uuids = [
                line.removeprefix("E:ID_FS_UUID=")
                for line in properties.splitlines()
                if line.startswith("E:ID_FS_UUID=")
            ]
            if len(uuids) != 1 or uuids[0] != expected_uuid:
                raise StorageMountError(
                    "storage_mount_identity_mismatch: "
                    f"path={root} device={device_id} expected_uuid={expected_uuid} "
                    f"actual_uuid={uuids}"
                )
            actual_uuid = uuids[0]
        read_only = bool(capacity.f_flag & os.ST_RDONLY)
        if require_writable and (
            read_only or not os.access(root, os.W_OK | os.X_OK)
        ):
            raise StorageMountError(
                f"storage_mount_read_only: path={root} device={device_id}"
            )
        if capacity.f_blocks <= 0 or capacity.f_frsize <= 0:
            raise StorageMountError(f"storage_capacity_unavailable: path={root}")
        if root.stat().st_dev != device:
            raise StorageMountError(f"storage_mount_changed: path={root}")
        return FilesystemEvidence(
            path=str(root),
            device_id=device_id,
            filesystem_uuid=actual_uuid,
            total_bytes=capacity.f_blocks * capacity.f_frsize,
            used_bytes=(capacity.f_blocks - capacity.f_bfree) * capacity.f_frsize,
            available_bytes=capacity.f_bavail * capacity.f_frsize,
            read_only=read_only,
        )
    except OSError as exc:
        raise StorageMountError(
            f"storage_mount_unavailable: path={root} expected_uuid={expected_uuid} "
            f"error={type(exc).__name__}:{exc}"
        ) from exc


def configured_archive_root() -> Path:
    """One configuration owner for the local archive placement (without mkdir)."""
    return Path(os.environ.get("MARKET_STRUCTURE_STORAGE_ROOT", "logs/market-structure"))


def require_configured_archive_mount(
    path: Path | None = None, *, require_writable: bool = True
) -> FilesystemEvidence | None:
    """Enforce the dedicated-filesystem contract at archive boundaries.

    An empty UUID selects the existing directory-backed development mode. A
    configured UUID requires the root to exist and paths to stay inside it and
    on the same filesystem, including through symlinks and nested mounts.
    """

    expected_uuid = os.environ.get("QT_MARKET_DATA_EXPECTED_UUID", "").strip()
    if not expected_uuid:
        return None
    configured_root = configured_archive_root()
    if not configured_root.is_absolute() or configured_root == Path("/"):
        raise StorageMountError(
            "storage_mount_invalid: dedicated archive root must be absolute and not /"
        )
    evidence = inspect_filesystem(
        configured_root,
        expected_uuid=expected_uuid,
        udev_root=Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data")),
        require_writable=require_writable,
    )
    if Path(evidence.path) == Path("/"):
        raise StorageMountError("storage_mount_invalid: archive root resolves to /")
    if path is not None:
        try:
            root = Path(evidence.path)
            candidate = Path(path).resolve()
            if candidate != root and root not in candidate.parents:
                raise StorageMountError(
                    f"storage_path_outside_archive: path={candidate} root={root}"
                )
            while not candidate.exists():
                candidate = candidate.parent
            if candidate.stat().st_dev != root.stat().st_dev:
                raise StorageMountError(
                    f"storage_path_wrong_filesystem: path={candidate} root={root}"
                )
        except OSError as exc:
            raise StorageMountError(
                f"storage_path_unavailable: path={path} error={exc}"
            ) from exc
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--expected-uuid", default="")
    parser.add_argument("--udev-root", type=Path, default=Path("/run/udev/data"))
    args = parser.parse_args()
    try:
        evidence = inspect_filesystem(
            args.path, expected_uuid=args.expected_uuid, udev_root=args.udev_root
        )
    except StorageMountError as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps({"event": "storage_mount_verified", **asdict(evidence)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

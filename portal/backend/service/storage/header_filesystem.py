"""Verify catalog file locations inside the PostgreSQL server's namespaces.

The eventual worker must share PostgreSQL's PID namespace and storage paths.
Nothing here is an API action, a mount operation, a reservation, or a mover.
"""
from __future__ import annotations

import os
import re
import stat
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

from core.storage_header_placement import HeaderPlacementSnapshot
from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageTarget
from .header_catalog import HeaderCatalogInventory

_PROC_ROOT = Path("/proc")


@dataclass(frozen=True)
class VerifiedHeaderPlacement:
    snapshot: HeaderPlacementSnapshot
    capacity: dict[str, FilesystemEvidence]
    bindings: tuple[dict, ...]
    verified_at: datetime


def _failure(reason):
    raise StorageMountError("header_filesystem_unverified: " + reason)


def _device_id(device):
    return f"{os.major(device)}:{os.minor(device)}"


def _read_small(path, maximum):
    with path.open("rb") as stream:
        value = stream.read(maximum + 1)
    if len(value) > maximum:
        _failure("identity file exceeds budget")
    return value.decode("utf-8")


def _cluster_identity(inventory, binary, deadline):
    root = Path(inventory.server_data_directory)
    if not root.is_absolute() or root == Path("/") or ".." in root.parts:
        _failure("absolute database directory required")
    server_directory = root
    root = root.resolve(strict=True)
    if _read_small(root / "PG_VERSION", 64).strip() != "15":
        _failure("database major version is not 15")
    fields = _read_small(root / "postmaster.pid", 4096).splitlines()
    if len(fields) < 3 or not fields[0].isdigit() or not fields[2].isdigit():
        _failure("running postmaster identity missing")
    pid = int(fields[0])
    if pid < 1 or not Path(fields[1]).is_absolute() or Path(fields[1]).resolve(strict=True) != root:
        _failure("postmaster directory mismatch")
    started = inventory.postmaster_started_at
    if started.tzinfo is None or started.utcoffset() is None or int(started.timestamp()) != int(fields[2]):
        _failure("postmaster start time mismatch")
    # A backup's copied PID file and system ID do not establish active storage.
    executable = (_PROC_ROOT / str(pid) / "exe").readlink()
    if not executable.is_absolute() or executable.name != "postgres":
        _failure("postmaster executable mismatch")
    # Use a kernel stat through proc's magic root link, not Path.resolve:
    # a sidecar can share data/PID namespaces without sharing binary paths.
    server_control = (_PROC_ROOT / str(pid) / "root" / server_directory.relative_to("/")
                      / "global" / "pg_control").stat()
    local_control = (root / "global" / "pg_control").stat()
    if (server_control.st_dev, server_control.st_ino) != (local_control.st_dev, local_control.st_ino):
        _failure("process namespace points to different database files")
    process_fields = _read_small(_PROC_ROOT / str(pid) / "cmdline", 65536).split("\x00")
    if not process_fields or Path(process_fields[0]).name != "postgres":
        _failure("postmaster process mismatch")
    remaining = deadline - monotonic()
    if remaining <= 0:
        _failure("time budget exceeded")
    result = subprocess.run(
        [str(binary), "-D", str(root)], check=True, capture_output=True, text=True,
        timeout=remaining, env={"PATH": os.defpath, "LC_ALL": "C", "LANG": "C", "PG_COLOR": "never"},
    )
    if result.stderr.strip():
        _failure("pg_controldata reported diagnostics")
    identities = re.findall(r"^Database system identifier:\s*([0-9]+)\s*$", result.stdout, re.MULTILINE)
    if identities != [inventory.snapshot.database_identity.split("/")[0]]:
        _failure("database cluster identity mismatch")
    return root, pid, int(fields[2])


def verify_header_filesystem(inventory, targets, *, pg_controldata: Path, timeout_seconds=30):
    """Bind all observed header/index files to verified registered roots.

    pg_controldata is a trusted, absolute worker configuration path, never a UI
    input. Run with the database server's PID namespace and identical storage paths. A new
    catalog observation and verification are required immediately before moves.
    """
    if not isinstance(inventory, HeaderCatalogInventory):
        raise ValueError("header_filesystem_invalid: catalog inventory")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("header_filesystem_invalid: time budget")
    if not 1 <= len(targets) <= 32 or any(not isinstance(item, StorageTarget) for item in targets):
        raise ValueError("header_filesystem_invalid: target inventory")
    identity = re.fullmatch(r"([0-9]{1,20})/([0-9]{1,10})", inventory.snapshot.database_identity)
    if identity is None or not 1 <= int(identity[2]) <= 2**32 - 1:
        raise ValueError("header_filesystem_invalid: database identity")
    database_oid = int(identity[2])
    relations = {item.oid: item for group in inventory.snapshot.partitions for item in group.relations}
    relation_count = sum(len(group.relations) for group in inventory.snapshot.partitions)
    if relation_count != len(relations) or len(inventory.physical_locations) != relation_count:
        raise ValueError("header_filesystem_invalid: relation inventory")
    if not inventory.snapshot.inventory_complete or inventory.filesystem_bindings_verified:
        raise ValueError("header_filesystem_invalid: unbound complete catalog required")
    if len({item.target_id for item in targets}) != len(targets) or len({item.filesystem_uuid for item in targets}) != len(targets):
        raise ValueError("header_filesystem_invalid: duplicate target identity")
    binary = Path(pg_controldata)
    if not binary.is_absolute():
        raise ValueError("header_filesystem_invalid: absolute pg_controldata path required")
    deadline = monotonic() + timeout_seconds
    try:
        binary = binary.resolve(strict=True)
        # Reject an incompatible binary before interpreting cluster control bytes.
        version = subprocess.run(
            [str(binary), "--version"], check=True, capture_output=True, text=True,
            timeout=max(0.001, deadline - monotonic()),
            env={"PATH": os.defpath, "LC_ALL": "C", "LANG": "C", "PG_COLOR": "never"},
        )
        if version.stderr.strip() or not re.fullmatch(r"pg_controldata \(PostgreSQL\) 15\.[^\n]+\s*", version.stdout):
            _failure("pg_controldata major version is not 15")
        cluster = _cluster_identity(inventory, binary, deadline)
        root = cluster[0]
        capacity, roots, devices = {}, {}, set()
        for target in targets:
            evidence = target.inspect(require_writable=True)
            resolved = Path(target.root).resolve(strict=True)
            if (resolved == Path("/") or Path(evidence.path).resolve(strict=True) != resolved
                    or evidence.filesystem_uuid != target.filesystem_uuid or evidence.read_only
                    or _device_id(resolved.stat().st_dev) != evidence.device_id):
                _failure("target filesystem identity mismatch: " + target.target_id)
            if evidence.device_id in devices:
                _failure("duplicate target filesystem device")
            devices.add(evidence.device_id)
            roots[target.target_id] = resolved
            capacity[target.target_id] = evidence
        bindings, seen, observed = [], set(), {}
        for location in inventory.physical_locations:
            if monotonic() >= deadline:
                _failure("time budget exceeded")
            oid = location["relation_oid"]
            if oid not in relations or oid in seen or location["database_oid"] != database_oid:
                _failure("catalog relation identity mismatch")
            seen.add(oid)
            relation = relations[oid]
            tablespace = location["tablespace_oid"]
            relative = location["relative_path"]
            if type(tablespace) is not int or not 1 <= tablespace <= 2**32 - 1 or not isinstance(relative, str):
                _failure("catalog path malformed")
            if tablespace == 1663:  # PostgreSQL's built-in pg_default OID
                expected = f"base/{database_oid}/{relation.relfilenode}"
                if location["tablespace_location"] != "" or relative != expected:
                    _failure("database default path mismatch")
            else:
                pattern = rf"pg_tblspc/{tablespace}/PG_15_[0-9]+/{database_oid}/{relation.relfilenode}"
                if not re.fullmatch(pattern, relative):
                    _failure("tablespace relation path mismatch")
                declared = Path(location["tablespace_location"])
                if not declared.is_absolute() or declared.resolve(strict=True) != (root / "pg_tblspc" / str(tablespace)).resolve(strict=True):
                    _failure("tablespace directory mismatch")
            candidate = root / relative
            if candidate.is_symlink():
                _failure("individual relation file cannot be a symlink")
            actual = candidate.resolve(strict=True)
            info = actual.stat()
            if not stat.S_ISREG(info.st_mode):
                _failure("relation file is not regular")
            matching = [key for key, directory in roots.items()
                        if actual.is_relative_to(directory) and _device_id(info.st_dev) == capacity[key].device_id]
            if len(matching) != 1:
                _failure("relation is outside a unique registered filesystem")
            target_id = matching[0]
            observed[oid] = (candidate, actual, info.st_dev, info.st_ino, target_id)
            bindings.append({"relation_oid": oid, "target_id": target_id,
                             "filesystem_uuid": capacity[target_id].filesystem_uuid,
                             "device_id": capacity[target_id].device_id, "path": str(actual)})
        # Refuse a changed mount, process or file instead of returning mixed evidence.
        if _cluster_identity(inventory, binary, deadline) != cluster:
            _failure("database process changed during verification")
        for target in targets:
            current = target.inspect(require_writable=True)
            if (current.device_id != capacity[target.target_id].device_id
                    or current.filesystem_uuid != capacity[target.target_id].filesystem_uuid
                    or current.read_only
                    or Path(target.root).resolve(strict=True) != roots[target.target_id]):
                _failure("target changed during verification")
            capacity[target.target_id] = current
        for candidate, actual, device, inode, _ in observed.values():
            info = candidate.stat()
            if candidate.is_symlink() or candidate.resolve(strict=True) != actual or (info.st_dev, info.st_ino) != (device, inode):
                _failure("relation changed during verification")
        if monotonic() >= deadline:
            _failure("time budget exceeded")
        def bind(relation):
            return replace(relation, target_id=observed[relation.oid][4])
        groups = tuple(replace(group, heap=bind(group.heap), indexes=tuple(bind(item) for item in group.indexes))
                       for group in inventory.snapshot.partitions)
        return VerifiedHeaderPlacement(replace(inventory.snapshot, partitions=groups),
                                       capacity, tuple(sorted(bindings, key=lambda item: item["relation_oid"])),
                                       datetime.now(UTC))
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise StorageMountError("header_filesystem_unavailable: " + type(exc).__name__) from exc

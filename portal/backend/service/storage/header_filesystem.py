"""Verify catalog file locations inside the PostgreSQL server's namespaces.

The eventual worker must share PostgreSQL's PID namespace and storage paths.
Nothing here is an API action, a mount operation, a reservation, or a mover.
"""
from __future__ import annotations

import os
import re
import stat
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic

from core.storage_header_placement import HeaderPlacementSnapshot
from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageTarget
from .header_catalog import HeaderCatalogInventory, HeaderTablespaceObservation

_PROC_ROOT = Path("/proc")


@dataclass(frozen=True)
class VerifiedTablespaceDestination:
    database_identity: str
    target_id: str
    filesystem_uuid: str
    device_id: str
    tablespace_oid: int
    tablespace_name: str
    tablespace_location: str
    directory: str
    server_directory: str
    directory_inode: int
    catalog_version: int
    target_root: str


@dataclass(frozen=True)
class VerifiedHeaderPlacement:
    snapshot: HeaderPlacementSnapshot
    capacity: dict[str, FilesystemEvidence]
    bindings: tuple[dict, ...]
    verified_at: datetime
    destinations: tuple[VerifiedTablespaceDestination, ...] = ()


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


def _process_path_fd(pid, path):
    """Walk an absolute server path from the postmaster root without links.

    Following an absolute symlink after /proc/PID/root can escape back into the
    caller's root. Directory-relative O_NOFOLLOW opens prevent that false proof.
    The initial proc magic link is intentional; all subsequent components must
    be real directories/files. O_PATH avoids reading database file contents.
    """
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts or len(path.parts) > 256:
        _failure("invalid process namespace path")
    handle = os.open(_PROC_ROOT / str(pid) / "root", os.O_PATH | os.O_DIRECTORY)
    try:
        parts = path.parts[1:]
        for index, part in enumerate(parts):
            flags = os.O_PATH | os.O_NOFOLLOW
            if index < len(parts) - 1:
                flags |= os.O_DIRECTORY
            next_handle = os.open(part, flags, dir_fd=handle)
            os.close(handle)
            handle = next_handle
        if stat.S_ISLNK(os.fstat(handle).st_mode):
            _failure("process namespace path contains a symlink")
        result, handle = handle, None
        return result
    finally:
        if handle is not None:
            os.close(handle)


def _process_stat(pid, path):
    handle = _process_path_fd(pid, path)
    try:
        return os.fstat(handle)
    finally:
        os.close(handle)


def _process_readlink(pid, path):
    path = Path(path)
    handle = _process_path_fd(pid, path.parent)
    try:
        return os.readlink(path.name, dir_fd=handle)
    finally:
        os.close(handle)


def _same_process_file(pid, actual, local_info, *, require_writable=False):
    handle = _process_path_fd(pid, actual)
    try:
        server_info = os.fstat(handle)
        if (server_info.st_dev, server_info.st_ino) != (local_info.st_dev, local_info.st_ino):
            _failure("worker and database see different storage files")
        if require_writable:
            server_user = (_PROC_ROOT / str(pid)).stat().st_uid
            if (not stat.S_ISDIR(server_info.st_mode) or server_info.st_uid != server_user
                    or server_info.st_mode & 0o700 != 0o700
                    or server_info.st_mode & 0o022
                    or os.fstatvfs(handle).f_flag & os.ST_RDONLY):
                _failure("destination directory is not privately writable by database owner")
        return server_info
    finally:
        os.close(handle)


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
    # Walk from the proc magic root without following later symlinks:
    # a sidecar can share data/PID namespaces without sharing binary paths.
    server_control = _process_stat(pid, server_directory / "global" / "pg_control")
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


def verify_header_filesystem(inventory, targets, *, pg_controldata: Path, timeout_seconds=30,
                             destination_assignments=None):
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
    if inventory.filesystem_bindings_verified:
        raise ValueError("header_filesystem_invalid: unbound catalog required")
    if inventory.group_storage_day is None:
        if not inventory.snapshot.inventory_complete:
            raise ValueError("header_filesystem_invalid: unbound complete catalog required")
    elif (type(inventory.group_storage_day) is not date
            or inventory.snapshot.inventory_complete
            or len(inventory.snapshot.partitions) != 1
            or inventory.snapshot.partitions[0].storage_day != inventory.group_storage_day):
        raise ValueError("header_filesystem_invalid: explicit partial group required")
    if len({item.target_id for item in targets}) != len(targets) or len({item.filesystem_uuid for item in targets}) != len(targets):
        raise ValueError("header_filesystem_invalid: duplicate target identity")
    binary = Path(pg_controldata)
    if not binary.is_absolute():
        raise ValueError("header_filesystem_invalid: absolute pg_controldata path required")
    if destination_assignments is not None and (
            not isinstance(destination_assignments, Mapping) or len(destination_assignments) > 32):
        raise ValueError("header_filesystem_invalid: destination assignments")
    assignments = dict(destination_assignments or {})
    if (len(assignments) > 32 or set(assignments) - {target.target_id for target in targets}
            or any(type(oid) is not int or not 1 <= oid <= 2**32 - 1 or oid == 1664
                   for oid in assignments.values())
            or len(set(assignments.values())) != len(assignments)):
        raise ValueError("header_filesystem_invalid: destination assignments")
    destinations = inventory.destination_tablespaces
    if (not isinstance(destinations, tuple) or len(destinations) > 32
            or any(not isinstance(item, HeaderTablespaceObservation) for item in destinations)
            or len({item.oid for item in destinations}) != len(destinations)
            or {item.oid for item in destinations} != set(assignments.values())):
        raise ValueError("header_filesystem_invalid: destination catalog mismatch")
    if destinations and (type(inventory.catalog_version) is not int
                         or not 1 <= inventory.catalog_version <= 2**31 - 1):
        raise ValueError("header_filesystem_invalid: catalog version")
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
                server_candidate = root / relative
            else:
                pattern = rf"pg_tblspc/{tablespace}/PG_15_[0-9]+/{database_oid}/{relation.relfilenode}"
                if not re.fullmatch(pattern, relative):
                    _failure("tablespace relation path mismatch")
                declared = Path(location["tablespace_location"])
                if not declared.is_absolute() or declared.resolve(strict=True) != (root / "pg_tblspc" / str(tablespace)).resolve(strict=True):
                    _failure("tablespace directory mismatch")
                if _process_readlink(cluster[1], root / "pg_tblspc" / str(tablespace)) != str(declared):
                    _failure("server tablespace link changed")
                _same_process_file(cluster[1], declared, declared.stat())
                server_candidate = declared.joinpath(*Path(relative).parts[2:])
            candidate = root / relative
            if candidate.is_symlink():
                _failure("individual relation file cannot be a symlink")
            actual = candidate.resolve(strict=True)
            info = actual.stat()
            if not stat.S_ISREG(info.st_mode):
                _failure("relation file is not regular")
            _same_process_file(cluster[1], server_candidate, info)
            matching = [key for key, directory in roots.items()
                        if actual.is_relative_to(directory) and _device_id(info.st_dev) == capacity[key].device_id]
            if len(matching) != 1:
                _failure("relation is outside a unique registered filesystem")
            target_id = matching[0]
            observed[oid] = (candidate, actual, info.st_dev, info.st_ino, target_id, server_candidate)
            bindings.append({"relation_oid": oid, "target_id": target_id,
                             "filesystem_uuid": capacity[target_id].filesystem_uuid,
                             "device_id": capacity[target_id].device_id, "path": str(actual),
                             "server_path": str(server_candidate)})
        destination_results, destination_paths = [], []
        targets_by_id = {target.target_id: target for target in targets}
        assigned = {oid: target_id for target_id, oid in assignments.items()}
        for destination in destinations:
            if monotonic() >= deadline:
                _failure("time budget exceeded")
            target_id = assigned[destination.oid]
            target = targets_by_id[target_id]
            if target.state != "active" or "history" not in target.roles or not destination.can_create:
                _failure("destination lacks history role or database CREATE privilege")
            if destination.oid == 1663:
                if destination.location != "" or destination.name != "pg_default":
                    _failure("default destination identity mismatch")
                directory = root / "base" / str(database_oid)
                server_directory = directory
                link = None
            else:
                declared = Path(destination.location)
                if not declared.is_absolute() or ".." in declared.parts:
                    _failure("absolute destination directory required")
                link = root / "pg_tblspc" / str(destination.oid)
                if (not link.is_symlink() or link.readlink() != declared
                        or _process_readlink(cluster[1], link) != str(declared)):
                    _failure("destination tablespace link mismatch")
                _same_process_file(cluster[1], declared, declared.stat())
                directory = link / f"PG_15_{inventory.catalog_version}"
                server_directory = declared / f"PG_15_{inventory.catalog_version}"
            if directory.is_symlink():
                _failure("destination version directory cannot be a symlink")
            actual = directory.resolve(strict=True)
            info = actual.stat()
            if (not stat.S_ISDIR(info.st_mode)
                    or not actual.is_relative_to(roots[target_id])
                    or _device_id(info.st_dev) != capacity[target_id].device_id):
                _failure("destination is outside its registered filesystem")
            _same_process_file(cluster[1], server_directory, info, require_writable=True)
            destination_paths.append((directory, actual, info.st_dev, info.st_ino, link,
                                      destination.location, server_directory))
            destination_results.append(VerifiedTablespaceDestination(
                inventory.snapshot.database_identity, target_id, target.filesystem_uuid,
                capacity[target_id].device_id, destination.oid, destination.name,
                destination.location, str(actual), str(server_directory), info.st_ino, inventory.catalog_version, target.root,
            ))
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
        for candidate, actual, device, inode, _, server_candidate in observed.values():
            info = candidate.stat()
            if candidate.is_symlink() or candidate.resolve(strict=True) != actual or (info.st_dev, info.st_ino) != (device, inode):
                _failure("relation changed during verification")
            _same_process_file(cluster[1], server_candidate, info)
        for directory, actual, device, inode, link, location, server_directory in destination_paths:
            info = directory.stat()
            if (directory.is_symlink() or directory.resolve(strict=True) != actual
                    or (info.st_dev, info.st_ino) != (device, inode)):
                _failure("destination changed during verification")
            _same_process_file(cluster[1], server_directory, info, require_writable=True)
            if link is not None and (str(link.readlink()) != location
                                    or _process_readlink(cluster[1], link) != location):
                _failure("destination tablespace link changed")
        if monotonic() >= deadline:
            _failure("time budget exceeded")
        def bind(relation):
            return replace(relation, target_id=observed[relation.oid][4])
        groups = tuple(replace(group, heap=bind(group.heap), indexes=tuple(bind(item) for item in group.indexes))
                       for group in inventory.snapshot.partitions)
        return VerifiedHeaderPlacement(replace(inventory.snapshot, partitions=groups),
                                       capacity, tuple(sorted(bindings, key=lambda item: item["relation_oid"])),
                                       datetime.now(UTC),
                                       tuple(sorted(destination_results, key=lambda item: item.target_id)))
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise StorageMountError("header_filesystem_unavailable: " + type(exc).__name__) from exc

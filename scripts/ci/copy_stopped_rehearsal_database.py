"""Duplicate one stopped private PostgreSQL 15 rehearsal on the same disk.

Keep the donor stopped throughout. Bind PGDATA and history read-only, with
their original internal tablespace path, and bind a separate private writable
destination. The completed clone uses destination/pgdata and destination/history.
Only the destination is written. The launcher owns PostgreSQL boot and replay.
"""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from time import monotonic

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _stamp
from scripts.automation.verify_canonical_recovery_archive import _sha_file, _sha256
from scripts.ci.measure_same_hdd_rehearsal import _require_isolated_mounts


_SCHEMA = "qt.stopped_rehearsal_database_copy.v1"
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
_EMPTY_CHUNK = bytes(1024 * 1024)


def _canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _owned(info, device):
    return (info.st_dev == device and info.st_uid == os.getuid()
            and not info.st_mode & 0o7000)


def _member(path, device):
    info = path.lstat()
    if not _owned(info, device):
        raise RuntimeError(f"stopped_clone_member_owner_or_device_changed: {path.name}")
    return info


def _regular(path, device, *, links=1):
    info = _member(path, device)
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_nlink <= links:
        raise RuntimeError(f"stopped_clone_unsafe_file: {path.name}")
    return info


def _directory(path, device):
    info = _member(path, device)
    if not stat.S_ISDIR(info.st_mode) or path.resolve(strict=True) != path:
        raise RuntimeError(f"stopped_clone_unsafe_directory: {path.name}")
    return info


def _control(args, device, check):
    path = args.pgdata / "global" / "pg_control"
    _directory(path.parent, device)
    info = _regular(path, device)
    version = args.pgdata / "PG_VERSION"
    if _regular(version, device).st_size != 3:
        raise RuntimeError("stopped_clone_postgres_version_changed")
    with os.fdopen(os.open(version, _READ_FLAGS), "rb") as stream:
        if stream.read(4) != b"15\n":
            raise RuntimeError("stopped_clone_postgres_version_changed")
    if (info.st_size != 8192 or os.path.lexists(args.pgdata / "postmaster.pid")
            or os.path.lexists(args.pgdata / "standby.signal")):
        raise RuntimeError("stopped_clone_requires_stopped_recovery_donor")
    _regular(args.pgdata / "recovery.signal", device)
    digest, size = _sha_file(path, check=check)
    if size != 8192 or _stamp(path.lstat()) != _stamp(info):
        raise RuntimeError("stopped_clone_control_changed")
    evidence = {"sha256": digest, "method": "pinned_native_observer"}
    if args.control_sha256 is not None:
        if digest != args.control_sha256:
            raise RuntimeError("stopped_clone_control_sha256_changed")
    else:
        binary = args.pg_controldata
        if not binary.is_absolute() or binary.resolve(strict=True) != binary or not os.access(binary, os.X_OK):
            raise ValueError("stopped_clone_requires_exact_pg_controldata")
        result = subprocess.run([binary, args.pgdata], check=True, capture_output=True,
                                timeout=max(0.001, min(10, check())), env={"LC_ALL": "C", "PATH": os.defpath})
        fields = dict(line.split(":", 1) for line in result.stdout.decode("ascii").splitlines() if ":" in line)
        fields = {key: value.strip() for key, value in fields.items()}
        if (fields.get("Database cluster state"), fields.get("Database system identifier"),
                fields.get("Latest checkpoint's TimeLineID")) != (
                "shut down in recovery", args.system_identifier, str(args.timeline)):
            raise RuntimeError("stopped_clone_native_donor_scope_changed")
        evidence.update(method="pg_controldata", redo_lsn=fields["Latest checkpoint's REDO location"],
                        minimum_recovery_lsn=fields["Minimum recovery ending location"])
    if _stamp(path.lstat()) != _stamp(info):
        raise RuntimeError("stopped_clone_control_changed")
    return path, _stamp(info), evidence


def _source_inventory(args, device, check):
    entries = {"pgdata": {}, "history": {}}
    logical_bytes = count = 0
    link = ("pg_tblspc", str(args.tablespace_oid))
    digest = hashlib.sha256()

    def visit(path, parts, role):
        nonlocal logical_bytes, count
        check()
        info = _member(path, device)
        mode = stat.S_IMODE(info.st_mode)
        target = None
        if role == "pgdata" and parts[:1] == ("pg_tblspc",) and len(parts) > 1:
            if (parts != link or not stat.S_ISLNK(info.st_mode) or os.readlink(path) != str(args.history)
                    or _identity(path.stat()) != _identity(args.history.lstat())):
                raise RuntimeError("stopped_clone_unknown_tablespace_link")
            kind, target = "link", str(args.clone_history_path)
        elif stat.S_ISDIR(info.st_mode):
            _directory(path, device)
            kind = "directory"
        elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and parts != ("pg_tblspc",):
            kind = "file"
            logical_bytes += info.st_size
            if logical_bytes > args.max_bytes:
                raise RuntimeError("stopped_clone_logical_byte_budget_exceeded")
        else:
            raise RuntimeError(f"stopped_clone_unsafe_source_member: {path.name}")
        metadata = (kind, _stamp(info), mode, target)
        entries[role][parts] = metadata
        digest.update(_canonical((role, parts, metadata)))
        if kind == "directory":
            children = []
            with os.scandir(path) as listing:
                for item in listing:
                    check()
                    count += 1
                    name = item.name
                    if (count > args.max_entries or len(parts) >= 64 or "\\" in name
                            or not name.isascii() or not name.isprintable()):
                        raise RuntimeError("stopped_clone_entry_or_path_budget_exceeded")
                    children.append(name)
            for name in sorted(children):
                visit(path / name, (*parts, name), role)
        if _stamp(path.lstat()) != _stamp(info):
            raise RuntimeError(f"stopped_clone_source_member_changed: {path.name}")

    for role in entries:
        root = getattr(args, role)
        visit(root, (), role)
    if link not in entries["pgdata"] or entries["pgdata"].get(("pg_wal",), (None,))[0] != "directory":
        raise RuntimeError("stopped_clone_missing_tablespace_or_resident_wal")
    return entries, logical_bytes, count, digest.hexdigest()


def _workspace(root, scope, device):
    work, marker = root / ".copy-work", root / "scope.json"
    encoded = _canonical(scope)
    has_marker = os.path.lexists(marker)
    allowed = {work.name, marker.name, "pgdata", "history"} if has_marker else {work.name}
    if any(entry.name not in allowed for entry in root.iterdir()):
        raise RuntimeError("stopped_clone_unexpected_destination_member")
    if has_marker:
        _regular(marker, device, links=2)
        with os.fdopen(os.open(marker, _READ_FLAGS), "rb") as stream:
            if stream.read(len(encoded) + 1) != encoded:
                raise RuntimeError("stopped_clone_scope_changed")
    work.mkdir(mode=0o700, exist_ok=True)
    _private_root(work)
    _directory(work, device)
    for name in work.iterdir():
        if name.name not in ({"file", "scope"} if has_marker else {"scope"}):
            raise RuntimeError("stopped_clone_unknown_staging_member")
        _regular(name, device, links=2)
        if not has_marker:
            with os.fdopen(os.open(name, _READ_FLAGS), "rb") as stream:
                if not encoded.startswith(stream.read(len(encoded) + 1)):
                    raise RuntimeError("stopped_clone_unbound_staged_scope_changed")
        name.unlink()
    if not has_marker:
        staged = work / "scope"
        with os.fdopen(os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(staged, marker)
        _sync(root)
        staged.unlink()
    _regular(marker, device)
    _sync(work)
    return work


def _existing_tree(root, entries, device, check):
    """Refuse stale, booted or unrelated names in a partially copied destination."""
    if not os.path.lexists(root):
        return

    def inspect(path, parts):
        _directory(path, device)
        with os.scandir(path) as listing:
            for item in listing:
                check()
                child = (*parts, item.name)
                if child not in entries:
                    raise RuntimeError(f"stopped_clone_unexpected_destination_member: {item.name}")
                kind, stamp, mode, target = entries[child]
                member = Path(item.path)
                if kind == "directory":
                    inspect(member, child)
                elif kind == "link":
                    info = _member(member, device)
                    if not stat.S_ISLNK(info.st_mode) or os.readlink(member) != target:
                        raise RuntimeError("stopped_clone_destination_link_changed")
                else:
                    info = _regular(member, device)
                    if info.st_size != stamp[2] or stat.S_IMODE(info.st_mode) != mode:
                        raise RuntimeError(f"stopped_clone_destination_file_changed: {item.name}")

    inspect(root, ())


def _copy_file(source, destination, work, *, size, stamp, mode, device, check, reserve):
    if source.resolve(strict=True) != source or _stamp(_regular(source, device)) != stamp:
        raise RuntimeError(f"stopped_clone_source_file_changed: {source.name}")
    if os.path.lexists(destination):
        digest, source_bytes = _sha_file(source, check=check)
        if (source_bytes != size or _sha_file(destination, check=check) != (digest, size)
                or _stamp(source.lstat()) != stamp):
            raise RuntimeError(f"stopped_clone_checkpoint_bytes_changed: {destination.name}")
        return digest, False
    reserve(size)
    staged = work / "file"
    digest = hashlib.sha256()
    with os.fdopen(os.open(source, _READ_FLAGS), "rb") as incoming:
        if _stamp(os.fstat(incoming.fileno())) != stamp:
            raise RuntimeError(f"stopped_clone_source_file_changed: {source.name}")
        copied = 0
        with os.fdopen(os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
            while chunk := incoming.read(len(_EMPTY_CHUNK)):
                check()
                copied += len(chunk)
                if copied > size:
                    raise RuntimeError(f"stopped_clone_source_file_changed: {source.name}")
                digest.update(chunk)
                if chunk == _EMPTY_CHUNK[:len(chunk)]:
                    output.seek(len(chunk), os.SEEK_CUR)
                else:
                    output.write(chunk)
            output.truncate(copied)
            output.flush()
            os.fchmod(output.fileno(), mode)
            os.fsync(output.fileno())
        if copied != size or _stamp(os.fstat(incoming.fileno())) != stamp or _stamp(source.lstat()) != stamp:
            raise RuntimeError(f"stopped_clone_source_file_changed: {source.name}")
    checksum = digest.hexdigest()
    if _sha_file(staged, check=check) != (checksum, size):
        raise RuntimeError(f"stopped_clone_destination_readback_changed: {destination.name}")
    os.link(staged, destination)
    _sync(destination.parent)
    staged.unlink()
    _sync(work)
    return checksum, True


def copy_database(args):
    if (not re.fullmatch(r"[0-9]+:[0-9]+", args.device_id)
            or not re.fullmatch(r"[1-9][0-9]{0,19}", args.system_identifier)
            or int(args.system_identifier) >= 2**64
            or not _sha256(args.binding_sha256) or args.control_sha256 is not None and not _sha256(args.control_sha256)
            or (args.control_sha256 is None) == (args.pg_controldata is None)
            or not all(0 < oid < 2**32 for oid in (args.tablespace_oid, args.timeline))
            or not args.clone_history_path.is_absolute() or ".." in args.clone_history_path.parts
            or args.clone_history_path == Path("/") or min(args.pgdata_inode, args.history_inode) <= 0
            or not 1 <= args.max_entries <= 1000000 or not 1 <= args.timeout_seconds <= 604800
            or not 0 < args.max_bytes < 2**63
            or not all(0 <= value < 2**63 for value in (args.reserve_bytes, args.reserve_inodes))):
        raise ValueError("stopped_clone_invalid_scope_or_budget")
    started = monotonic()
    deadline = started + args.timeout_seconds
    roots = (args.pgdata, args.history, args.destination)
    identities = tuple(_private_root(path)[1] for path in roots)
    device = identities[0][0]
    if (len(set(identities)) != 3 or {identity[0] for identity in identities} != {device}
            or (identities[0][1], identities[1][1]) != (args.pgdata_inode, args.history_inode)
            or any(left.is_relative_to(right) or right.is_relative_to(left)
                   for index, left in enumerate(roots) for right in roots[index + 1:])):
        raise RuntimeError("stopped_clone_root_identity_or_overlap_changed")
    for source in roots[:2]:
        _require_isolated_mounts(source, args.destination, args.device_id)

    def reserve(additional=0):
        space = os.statvfs(args.destination)
        block = space.f_frsize
        allocation = (additional + block - 1) // block * block
        if (space.f_bavail * block < args.reserve_bytes + allocation
                or space.f_favail <= args.reserve_inodes):
            raise RuntimeError("stopped_clone_filesystem_reserve_exhausted")

    control_pin = None
    next_check = started

    def check():
        nonlocal next_check
        now = monotonic()
        if now >= deadline:
            raise TimeoutError("stopped_clone_deadline")
        if now >= next_check:
            next_check = now + 0.25
            if (tuple(_private_root(path)[1] for path in roots) != identities
                    or any(not os.statvfs(path).f_flag & os.ST_RDONLY for path in roots[:2])
                    or os.statvfs(args.destination).f_flag & os.ST_RDONLY
                    or os.path.lexists(args.pgdata / "postmaster.pid")
                    or control_pin is not None and _stamp(control_pin[0].lstat()) != control_pin[1]):
                raise RuntimeError("stopped_clone_mount_or_donor_changed")
            reserve()
        return deadline - now

    descriptor = os.open(args.destination, _READ_FLAGS | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if _identity(os.fstat(descriptor)) != identities[2]:
            raise RuntimeError("stopped_clone_destination_root_changed")
        check()
        control_pin = _control(args, device, check)
        inventory, logical_bytes, count, metadata_sha = _source_inventory(args, device, check)
        scope = {"schema": _SCHEMA, "binding_sha256": args.binding_sha256,
                 "system_identifier": args.system_identifier, "timeline": args.timeline, "device_id": args.device_id,
                 "source": {"pgdata": str(args.pgdata), "history": str(args.history),
                            "pgdata_inode": args.pgdata_inode, "history_inode": args.history_inode,
                            "metadata_sha256": metadata_sha, "control": control_pin[2]},
                 "destination_inode": identities[2][1], "tablespace_oid": args.tablespace_oid,
                 "clone_history_path": str(args.clone_history_path)}
        work = _workspace(args.destination, scope, device)
        digest = hashlib.sha256()
        files = copied = 0
        for role, members in inventory.items():
            target_root = args.destination / role
            _existing_tree(target_root, members, device, check)
            for parts, (kind, stamp, mode, target) in members.items():
                check()
                destination = target_root.joinpath(*parts)
                checksum = None
                if kind == "directory":
                    if not os.path.lexists(destination):
                        destination.mkdir(mode=0o700)
                        os.chmod(destination, mode)
                        _sync(destination)
                        _sync(destination.parent)
                    if stat.S_IMODE(_directory(destination, device).st_mode) != mode:
                        raise RuntimeError(f"stopped_clone_destination_directory_mode_changed: {destination.name}")
                elif kind == "link":
                    if not os.path.lexists(destination):
                        destination.symlink_to(target)
                        _sync(destination.parent)
                    if not stat.S_ISLNK(_member(destination, device).st_mode) or os.readlink(destination) != target:
                        raise RuntimeError("stopped_clone_destination_link_changed")
                else:
                    source = getattr(args, role).joinpath(*parts)
                    checksum, new = _copy_file(source, destination, work, size=stamp[2], stamp=stamp, mode=mode,
                                               device=device, check=check, reserve=reserve)
                    if role == "pgdata" and parts == ("global", "pg_control") and checksum != control_pin[2]["sha256"]:
                        raise RuntimeError("stopped_clone_copied_control_changed")
                    files += 1
                    copied += new
                digest.update(_canonical((role, parts, kind, mode, target, checksum)))
            _existing_tree(target_root, members, device, check)
        del inventory
        _, final_bytes, final_count, final_sha = _source_inventory(args, device, check)
        if ((final_bytes, final_count, final_sha) != (logical_bytes, count, metadata_sha)
                or _control(args, device, check) != control_pin):
            raise RuntimeError("stopped_clone_final_donor_changed")
        for source in roots[:2]:
            _require_isolated_mounts(source, args.destination, args.device_id)
        check()
        report = scope | {"logical_bytes": logical_bytes, "entries": count, "files": files, "copied_files": copied,
                          "reused_files": files - copied, "file_inventory_sha256": digest.hexdigest(),
                          "elapsed_seconds": round(monotonic() - started, 3)}
        return report | {"verification_sha256": archive_evidence_hash(report)}
    finally:
        os.close(descriptor)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("pgdata", "history", "destination", "clone-history-path"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    for name in ("device-id", "system-identifier", "binding-sha256"):
        parser.add_argument(f"--{name}", required=True)
    control = parser.add_mutually_exclusive_group(required=True)
    control.add_argument("--pg-controldata", type=Path, help="Exact local PostgreSQL 15 native control reader.")
    control.add_argument("--control-sha256", help="Pinned control file hash from the launcher's stopped native observer.")
    for name in ("pgdata-inode", "history-inode", "tablespace-oid", "timeline", "max-bytes", "max-entries",
                 "reserve-bytes", "reserve-inodes", "timeout-seconds"):
        parser.add_argument(f"--{name}", required=True, type=int)
    try:
        receipt = copy_database(parser.parse_args(argv))
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(receipt, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

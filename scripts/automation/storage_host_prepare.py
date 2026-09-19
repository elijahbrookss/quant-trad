#!/usr/bin/env python3
"""Prepare an approved HDD or its fixed runtime directories; never move existing data."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import subprocess
import tempfile
import time
from uuid import UUID

if __package__:
    from .storage_device_audit import audit
else:
    from storage_device_audit import audit

PLAN_KEYS = {"device", "expected_serial", "expected_size_bytes", "filesystem_uuid", "mountpoint", "owner"}
OPTIONS = "defaults,nofail,noatime,x-systemd.device-timeout=30s"


def validate_plan(plan: dict) -> dict:
    if not isinstance(plan, dict) or set(plan) != PLAN_KEYS:
        raise ValueError("storage_prepare_invalid: unexpected plan fields")
    if type(plan["expected_size_bytes"]) is not int or plan["expected_size_bytes"] <= 0:
        raise ValueError("storage_prepare_invalid: expected_size_bytes must be positive")
    if str(UUID(plan["filesystem_uuid"])) != plan["filesystem_uuid"]:
        raise ValueError("storage_prepare_invalid: canonical filesystem UUID required")
    path = Path(plan["mountpoint"])
    base = Path("/srv/quanttrad/storage")
    if not path.is_absolute() or path.parent != base or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", path.name) or ".." in path.parts:
        raise ValueError("storage_prepare_invalid: dedicated QT storage mountpoint required")
    if not isinstance(plan["owner"], str) or not plan["owner"]:
        raise ValueError("storage_prepare_invalid: owner required")
    return plan


def admit_device(plan: dict, evidence: dict) -> bool:
    """Return True only when this plan still needs initial formatting."""
    devices = evidence["topology"]["blockdevices"]
    if len(devices) != 1:
        raise ValueError("storage_prepare_invalid: one whole disk required")
    device = devices[0]
    if device.get("type") != "disk" or device.get("serial", "").strip() != plan["expected_serial"]:
        raise ValueError("storage_prepare_identity_mismatch")
    if device.get("size") != plan["expected_size_bytes"]:
        raise ValueError("storage_prepare_size_mismatch")
    if device.get("children"):
        raise ValueError("storage_prepare_refused: partitioned disk")
    mounts = [value for value in device.get("mountpoints", []) if value]
    if any(value != plan["mountpoint"] for value in mounts):
        raise ValueError("storage_prepare_refused: disk mounted elsewhere")
    signatures = evidence["signatures"].get("signatures", [])
    if device.get("fstype") == "ext4" and device.get("uuid") == plan["filesystem_uuid"]:
        if any(item.get("type") != "ext4" for item in signatures):
            raise ValueError("storage_prepare_refused: unexpected additional signature")
        return False
    if mounts or device.get("fstype") or device.get("uuid") or signatures:
        raise ValueError("storage_prepare_refused: existing data signatures or filesystem")
    return True


def fstab_update(original: str, plan: dict) -> str:
    wanted = f"UUID={plan['filesystem_uuid']} {plan['mountpoint']} ext4 {OPTIONS} 0 2"
    for raw in original.splitlines():
        fields = raw.strip().split()
        if not fields or fields[0].startswith("#"):
            continue
        if len(fields) >= 2 and (fields[0] == f"UUID={plan['filesystem_uuid']}" or fields[1] == plan["mountpoint"]):
            if " ".join(fields) != wanted:
                raise ValueError("storage_prepare_refused: conflicting fstab entry")
            return original
    return original.rstrip("\n") + "\n# QT registered historical storage\n" + wanted + "\n"


def run(command: list[str]) -> str:
    result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE)
    return result.stdout.strip()


def atomic_text(path: Path, value: str, mode: int) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.qt-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def prepare(plan: dict, *, initialize_empty_device: bool) -> dict:
    validate_plan(plan)
    for name in ("lsblk", "wipefs", "mkfs.ext4", "blkid", "mount", "findmnt", "udevadm", "smartctl"):
        if shutil.which(name) is None:
            raise ValueError(f"storage_prepare_prerequisite_missing: {name}")
    account = pwd.getpwnam(plan["owner"])
    mountpoint = Path(plan["mountpoint"])
    for parent in (mountpoint, *mountpoint.parents):
        if parent.is_symlink():
            raise ValueError("storage_prepare_refused: symlink mount path")
    original_fstab = Path("/etc/fstab").read_text()
    updated_fstab = fstab_update(original_fstab, plan)
    evidence = audit(Path(plan["device"]), plan["expected_serial"])
    needs_format = admit_device(plan, evidence)
    health = json.loads(run(["smartctl", "--json", "--health", plan["device"]]))
    if health.get("smart_status", {}).get("passed") is not True:
        raise ValueError("storage_prepare_refused: drive health not confirmed")
    if needs_format and not initialize_empty_device:
        raise ValueError("storage_prepare_confirmation_required: use --initialize-empty-device only for the approved disk")
    mounted_here = plan["mountpoint"] in (evidence["topology"]["blockdevices"][0].get("mountpoints") or [])
    if mountpoint.exists() and not mounted_here and (not mountpoint.is_dir() or any(mountpoint.iterdir())):
        raise ValueError("storage_prepare_refused: mountpoint is not an empty directory")
    if needs_format:
        # Fixed UUID makes an interrupted run distinguishable from an unrelated
        # pre-existing filesystem. No force flag is ever used.
        print("Creating ext4 on the approved HDD; inode initialization may take several minutes.", flush=True)
        run(["mkfs.ext4", "-U", plan["filesystem_uuid"], "-L", "qt-history-01",
             "-m", "0", "-i", "65536", "-E", "lazy_itable_init=0,lazy_journal_init=0", plan["device"]])
        run(["udevadm", "trigger", "--action=change", f"/sys/class/block/{Path(evidence['resolved_device']).name}"])
        run(["udevadm", "settle", "--timeout=30"])
    actual_uuid = run(["blkid", "-s", "UUID", "-o", "value", plan["device"]])
    if actual_uuid != plan["filesystem_uuid"]:
        raise ValueError("storage_prepare_refused: filesystem UUID changed")
    mountpoint.mkdir(parents=True, exist_ok=True)
    if not mounted_here:
        run(["mount", "-t", "ext4", "-o", "noatime", plan["device"], str(mountpoint)])
    actual_mount_uuid = run(["findmnt", "--mountpoint", str(mountpoint), "--noheadings", "--output", "UUID"])
    if actual_mount_uuid != plan["filesystem_uuid"]:
        raise ValueError("storage_prepare_refused: wrong filesystem mounted")
    benchmarks = mountpoint / "benchmarks"
    if benchmarks.is_symlink():
        raise ValueError("storage_prepare_refused: benchmark directory is a symlink")
    benchmarks.mkdir(mode=0o700, exist_ok=True)
    os.chown(benchmarks, account.pw_uid, account.pw_gid)
    os.chmod(benchmarks, 0o700)
    # Write only after the direct mount has been verified. Preserve the original
    # once, and never overwrite a concurrent administrator edit of fstab.
    fstab = Path("/etc/fstab")
    if updated_fstab != original_fstab:
        if fstab.read_text() != original_fstab:
            raise ValueError("storage_prepare_refused: fstab changed concurrently; rerun after review")
        backup = Path(f"/etc/fstab.qt-before-{plan['filesystem_uuid']}")
        if not backup.exists():
            with backup.open("x") as stream:
                os.chmod(backup, 0o600)
                stream.write(original_fstab)
                stream.flush()
                os.fsync(stream.fileno())
        atomic_text(fstab, updated_fstab, fstab.stat().st_mode & 0o777)
    capacity = os.statvfs(mountpoint)
    return {"schema_version": "qt.storage_host_prepared.v1", "device": plan["device"],
            "serial": plan["expected_serial"], "filesystem_uuid": actual_uuid,
            "mountpoint": str(mountpoint), "benchmark_root": str(benchmarks),
            "total_bytes": capacity.f_blocks * capacity.f_frsize,
            "available_bytes": capacity.f_bavail * capacity.f_frsize,
            "created_filesystem": needs_format, "database_moved": False}



def _runtime_directory(parent_fd: int, name: str, operator_gid: int) -> int:
    """Open one fixed child; finish only empty interrupted preparation."""
    try:
        os.mkdir(name,mode=0o700,dir_fd=parent_fd)
    except FileExistsError:
        pass
    descriptor = os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
    try:
        info = os.fstat(descriptor)
        if info.st_dev != os.fstat(parent_fd).st_dev:
            raise ValueError("storage_runtime_directory_wrong_filesystem: "+name)
        expected = (70,operator_gid,0o770)
        current = (info.st_uid,info.st_gid,stat.S_IMODE(info.st_mode))
        if current != expected:
            # New root-owned directories, or an empty directory interrupted
            # after chown, are the only incomplete states we can finish.
            pending = (info.st_uid == 0 or (info.st_uid,info.st_gid) == (70,operator_gid))
            with os.scandir(descriptor) as entries:
                empty = next(entries,None) is None
            if not pending or current[2] != 0o700 or not empty:
                raise ValueError("storage_runtime_directory_requires_review: "+name)
            os.fchown(descriptor,70,operator_gid)
            os.fchmod(descriptor,0o770)
            os.fsync(descriptor)
            os.fsync(parent_fd)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def prepare_runtime_directories(plan: dict) -> dict:
    """Prepare the fixed runtime roots on an already mounted audited HDD.

    Never format, mount, alter fstab, traverse/chown existing contents or move
    database files. UID 70 is the pinned PostgreSQL/application identity. The
    named operator's group retains root-directory access for host admission.
    Private tablespace/archive files keep their own stricter permissions.
    """
    validate_plan(plan)
    if os.geteuid() != 0:
        raise ValueError("storage_runtime_directories_administrator_required")
    account = pwd.getpwnam(plan["owner"])
    mountpoint = Path(plan["mountpoint"])
    for parent in (mountpoint,*mountpoint.parents):
        if parent.is_symlink():
            raise ValueError("storage_prepare_refused: symlink mount path")
    evidence = audit(Path(plan["device"]),plan["expected_serial"])
    if (admit_device(plan,evidence)
            or plan["mountpoint"] not in (evidence["topology"]["blockdevices"][0].get("mountpoints") or [])):
        raise ValueError("storage_runtime_directories_existing_verified_mount_required")
    def mounted():
        if run(["findmnt","--mountpoint",str(mountpoint),"--noheadings","--output","UUID"]) != plan["filesystem_uuid"]:
            raise ValueError("storage_runtime_directories_mount_changed")
    mounted()
    root_fd = os.open(mountpoint,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        identity = os.fstat(root_fd)
        if os.fstatvfs(root_fd).f_flag & os.ST_RDONLY:
            raise ValueError("storage_runtime_directories_read_only")
        if identity.st_dev == os.stat(mountpoint.parent).st_dev:
            raise ValueError("storage_runtime_directories_distinct_hdd_filesystem_required")
        data_fd = _runtime_directory(root_fd,"data",account.pw_gid)
        try:
            archive_fd = _runtime_directory(data_fd,"archives",account.pw_gid)
            os.close(archive_fd)
            mounted()
            if os.fstatvfs(root_fd).f_flag & os.ST_RDONLY:
                raise ValueError("storage_runtime_directories_read_only")
            actual = mountpoint.stat()
            if (actual.st_dev,actual.st_ino) != (identity.st_dev,identity.st_ino):
                raise ValueError("storage_runtime_directories_mount_changed")
        finally:
            os.close(data_fd)
    finally:
        os.close(root_fd)
    return {"schema_version":"qt.storage_runtime_directories.v1",
            "history_root":str(mountpoint/"data"),"archive_root":str(mountpoint/"data"/"archives"),
            "filesystem_uuid":plan["filesystem_uuid"],"runtime_uid":70,"operator_gid":account.pw_gid,
            "existing_files_changed":False,"database_moved":False}


def prepare_legacy_working_ownership(root: Path, *, expected_device: int,
                                     expected_inode: int, max_duration_seconds: int,
                                     max_entries: int = 1000000) -> dict:
    """Transfer the paused legacy working tree to UID 70, preserving its contents.

    Internal cutover step: the caller owns the deployment lock, has stopped all
    writers, and binds only the exact existing collector root into this helper.
    Preflight the entire tree before changing anything. Interrupted ownership
    changes are repeatable; any failure keeps the caller's deployment hold.
    """
    root = Path(root)
    if os.geteuid() != 0:
        raise ValueError("storage_working_ownership_administrator_required")
    if (type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 86400
            or type(max_entries) is not int or not 1 <= max_entries <= 1000000
            or type(expected_device) is not int or type(expected_inode) is not int):
        raise ValueError("storage_working_ownership_invalid_limits")
    if (not root.is_absolute() or root == Path("/") or root.resolve(strict=True) != root
            or any(parent.is_symlink() for parent in (root,*root.parents))):
        raise ValueError("storage_working_ownership_canonical_root_required")
    deadline = time.monotonic()+max_duration_seconds
    descriptor = os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        initial = os.fstat(descriptor)
        if (initial.st_dev,initial.st_ino) != (expected_device,expected_inode):
            raise ValueError("storage_working_ownership_root_changed")
        if os.fstatvfs(descriptor).f_flag & os.ST_RDONLY:
            raise ValueError("storage_working_ownership_read_only")
        def mount_id(fd):
            values = [line.split(":",1)[1].strip() for line in
                      Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
                      if line.startswith("mnt_id:")]
            if len(values)!=1:
                raise ValueError("storage_working_ownership_mount_identity_unavailable")
            return values[0]
        initial_mount = mount_id(descriptor)
        owners = {0,70,initial.st_uid}
        inventory = {}
        changed = 0
        def stamp(info):
            return (info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid,
                    info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        def check(fd,parts):
            if time.monotonic() >= deadline:
                raise TimeoutError("storage_working_ownership_deadline_expired")
            info = os.fstat(fd)
            if (info.st_dev != initial.st_dev or mount_id(fd)!=initial_mount or info.st_uid not in owners
                    or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
                    or info.st_mode & (stat.S_ISUID|stat.S_ISGID)
                    or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1)):
                raise ValueError("storage_working_ownership_unsupported_entry")
            return info
        def walk(fd,parts,apply):
            nonlocal changed
            info = check(fd,parts)
            key = tuple(parts)
            if apply:
                if inventory.pop(key,None) != stamp(info):
                    raise ValueError("storage_working_ownership_entry_changed")
            else:
                if len(inventory) >= max_entries or len(parts)>64:
                    raise ValueError("storage_working_ownership_inventory_limit")
                inventory[key] = stamp(info)
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(fd) as entries:
                    for entry in entries:
                        child_info = entry.stat(follow_symlinks=False)
                        if not (stat.S_ISDIR(child_info.st_mode) or stat.S_ISREG(child_info.st_mode)):
                            raise ValueError("storage_working_ownership_unsupported_entry")
                        child = os.open(entry.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,
                                        dir_fd=fd)
                        try:
                            if (os.fstat(child).st_dev,os.fstat(child).st_ino)!=(child_info.st_dev,child_info.st_ino):
                                raise ValueError("storage_working_ownership_entry_changed")
                            walk(child,(*parts,entry.name),apply)
                        finally:
                            os.close(child)
            if apply and info.st_uid != 70:
                current = check(fd,parts)
                if stamp(current) != stamp(info):
                    raise ValueError("storage_working_ownership_entry_changed")
                os.fchown(fd,70,-1)
                os.fsync(fd)
                after = os.fstat(fd)
                if (after.st_uid!=70 or (after.st_dev,after.st_ino,after.st_mode,after.st_gid,
                        after.st_nlink,after.st_size,after.st_mtime_ns) !=
                        (info.st_dev,info.st_ino,info.st_mode,info.st_gid,
                         info.st_nlink,info.st_size,info.st_mtime_ns)):
                    raise ValueError("storage_working_ownership_preservation_failed")
                changed += 1
        walk(descriptor,(),False)
        entries = len(inventory)
        walk(descriptor,(),True)
        if inventory or (root.stat().st_dev,root.stat().st_ino)!=(initial.st_dev,initial.st_ino):
            raise ValueError("storage_working_ownership_tree_changed")
        return dict(schema_version="qt.storage_working_ownership.v1",runtime_uid=70,
                    entries=entries,changed=changed,contents_preserved=True)
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--initialize-empty-device", action="store_true",
                        help="Authorize initial formatting of the exact signature-free disk in the plan.")
    action.add_argument("--prepare-runtime-directories", action="store_true",
                        help="Prepare fixed data/archive directories on the already verified mounted HDD; never format or mount.")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run in the administrator's terminal")
    try:
        plan = validate_plan(json.loads(args.plan.read_text()))
        with Path("/run/lock/qt-storage-prepare.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = (prepare_runtime_directories(plan) if args.prepare_runtime_directories
                      else prepare(plan, initialize_empty_device=args.initialize_empty_device))
        print(json.dumps(result, indent=2))
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"storage_prepare_failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

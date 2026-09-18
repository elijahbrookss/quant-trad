#!/usr/bin/env python3
"""Prepare one explicitly approved, signature-free HDD; never move live data."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import subprocess
import tempfile
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--initialize-empty-device", action="store_true",
                        help="Authorize initial formatting of the exact signature-free disk in the plan.")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run in the administrator's terminal")
    try:
        plan = validate_plan(json.loads(args.plan.read_text()))
        with Path("/run/lock/qt-storage-prepare.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = prepare(plan, initialize_empty_device=args.initialize_empty_device)
        print(json.dumps(result, indent=2))
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"storage_prepare_failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

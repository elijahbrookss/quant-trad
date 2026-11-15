"""Measure an approved recovery pair and pinned receiver workspace before the outage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import partial
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from time import monotonic
from types import SimpleNamespace

from core.storage_mounts import inspect_filesystem
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _keys, _run_native
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from scripts.automation import canonical_cold_named_recovery as named
from scripts.automation.canonical_cold_named_recovery import RecoverySitePolicy, _private_credential, _sign, _signer
from scripts.automation.stage_encrypted_recovery_handoff import (
    TransferLimits, _append_only_wal, _identity, _inventory, _manifest_entry, _private_root, _source_generation, _source_hold,
)
from scripts.automation.verify_encrypted_recovery_handoff import _selected_backup, _snapshot_owner
from scripts.db.canonical_cold_recovery_wal_transfer import _baseline


@dataclass(frozen=True)
class ReceiverPath:
    path: Path
    filesystem_uuid: str


def _deadline(seconds):
    end = monotonic() + seconds

    def check():
        if monotonic() >= end:
            raise TimeoutError("canonical_recovery_preflight_deadline")

    return end, check


def measure_source_prestage(*, source, generation, receipt_sha256, limits):
    """Inspect the exact encrypted source inventory using metadata and the existing writer hold."""
    if not isinstance(limits, TransferLimits):
        raise ValueError("canonical_recovery_preflight_invalid_source_limits")
    source, root_id = _private_root(source)
    _, deadline = _deadline(limits.seconds)

    def check():
        deadline()
        if _identity(source.lstat()) != root_id:
            raise RuntimeError("canonical_recovery_preflight_source_mount_changed")

    with _source_hold(source):
        check()
        _, receipt_info, pair = _source_generation(source, root_id[0], generation, receipt_sha256)
        before = _inventory(source, root_id[0], limits, check, source=True)
        directories, files, total = before
        manifest_bytes = 0
        for path, stamp in files.items():
            check()
            manifest_bytes += len(_manifest_entry(path, {"bytes": stamp[2], "sha256": "0" * 64}))
        _append_only_wal(before, _inventory(source, root_id[0], limits, check, source=True))
        check()
    evidence = {"schema": "qt.recovery_source_prestage_preflight.v1", "database_identity": pair["database_identity"],
                "source_filesystem_uuid": pair["filesystem_uuid"], "generation": generation, "pair_receipt_sha256": receipt_sha256,
                "encrypted_bytes": total, "encrypted_files": len(files), "repository_directories": len(directories),
                "manifest_bytes": manifest_bytes, "pair_receipt_bytes": receipt_info.st_size}
    return evidence | {"measurement_sha256": archive_evidence_hash(evidence)}


def _observe(descriptor, *, writable, udev):
    if (not isinstance(descriptor.filesystem_uuid, str)
            or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", descriptor.filesystem_uuid)):
        raise ValueError("canonical_recovery_preflight_filesystem_uuid_required")
    root, identity = _private_root(descriptor.path)
    data = inspect_filesystem(root, expected_uuid=descriptor.filesystem_uuid, udev_root=udev, require_writable=writable)
    capacity = os.statvfs(root)
    if _identity(root.lstat()) != identity or capacity.f_favail < 0 or capacity.f_files <= 0:
        raise RuntimeError("canonical_recovery_preflight_filesystem_changed_or_unobservable")
    return asdict(data) | {"available_entries": capacity.f_favail, "block_size": capacity.f_frsize, "inode": identity[1]}


def _manifest_requirements(run, pgbackrest, pair, tablespaces, check, *, max_entries):
    """Project logical file sizes and entry count per PostgreSQL data target."""
    current = b""
    section = None
    seen = 0
    sizes = {"pgdata": [0, 0], **{name: [0, 0] for name in tablespaces.values()}}

    def handle(line):
        nonlocal section, seen
        if line.startswith(b"[") and line.rstrip().endswith(b"]"):
            section = line.strip()
            return
        if section not in (b"[target:file]", b"[target:path]", b"[target:link]") or not line.strip():
            return
        name, delimiter, payload = line.rstrip(b"\r\n").partition(b"=")
        if not delimiter or len(name) > 4096:
            raise ValueError("canonical_recovery_preflight_native_manifest_invalid")
        path = name.decode("utf-8").split("/")
        if path[0] == "pg_data" or path == ["pg_tblspc"]:
            role = "pgdata"
        elif len(path) >= 2 and path[0] == "pg_tblspc" and path[1] in tablespaces:
            role = tablespaces[path[1]]
        else:
            raise ValueError("canonical_recovery_preflight_native_manifest_target_unknown")
        if section == b"[target:file]":
            size = json.loads(payload, object_pairs_hook=_unique_fields).get("size")
            if type(size) is not int or size < 0:
                raise ValueError("canonical_recovery_preflight_native_manifest_size_invalid")
            sizes[role][0] += size
        seen += 1
        sizes[role][1] += 1
        if seen > max_entries:
            raise ValueError("canonical_recovery_preflight_native_manifest_entry_budget")

    def consume(chunk):
        nonlocal current
        check()
        *lines, current = (current + chunk).split(b"\n")
        if len(current) > 65536:
            raise ValueError("canonical_recovery_preflight_native_manifest_line_budget")
        for line in lines:
            if len(line) > 65536:
                raise ValueError("canonical_recovery_preflight_native_manifest_line_budget")
            handle(line)

    run([*pgbackrest, "repo-get", f"backup/qt/{pair['database_label']}/backup.manifest"], stdout_consumer=consume)
    if current or not seen or not sizes["pgdata"][0]:
        raise ValueError("canonical_recovery_preflight_native_manifest_incomplete")
    return {role: {"logical_bytes": total, "entries": entries} for role, (total, entries) in sizes.items()}


def measure_receiver_preflight(*, site, receiver_signing_key, expected_host_sha256, source_host_sha256,
                               source_filesystem_uuids, receiving, workspace, pgdata, tablespaces,
                               pgbackrest, restic, database_key, archive_key, pending_expected,
                               runtime_bytes, runtime_entries, max_manifest_entries, timeout_seconds):
    """Return signed native size and filesystem evidence from the prospective independent receiver."""
    if (not isinstance(site, RecoverySitePolicy) or type(pending_expected) is not bool
            or any(type(value) is not int or value < 0 for value in (runtime_bytes, runtime_entries))
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600
            or type(max_manifest_entries) is not int or not 1 <= max_manifest_entries <= 10_000_000
            or not isinstance(source_filesystem_uuids, (tuple, list, set)) or not source_filesystem_uuids
            or any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", value) for value in source_filesystem_uuids)
            or not isinstance(tablespaces, dict) or any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", key) for key in tablespaces)
            or not all(isinstance(path, ReceiverPath) for path in (receiving, workspace, pgdata, *tablespaces.values()))):
        raise ValueError("canonical_recovery_preflight_invalid_limits_or_roots")
    host = named.receiver_host_sha256()
    if (host != expected_host_sha256 or host == source_host_sha256
            or not re.fullmatch(r"[0-9a-f]{64}", str(source_host_sha256))):
        raise ValueError("canonical_recovery_preflight_receiver_host_not_independent")
    root = Path(site.receiving)
    if Path(receiving.path) != root or Path(pgdata.path) == Path(workspace.path):
        raise ValueError("canonical_recovery_preflight_receiver_path_changed")
    udev = Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
    mounts = {"exchange": receiving, "workspace": workspace, "pgdata": pgdata,
              **{f"tablespace:{name}": path for name, path in tablespaces.items()}}
    observed = {name: _observe(descriptor, writable=True, udev=udev) for name, descriptor in mounts.items()}
    if any(mount["filesystem_uuid"] in source_filesystem_uuids for mount in observed.values()):
        raise ValueError("canonical_recovery_preflight_receiver_filesystem_is_source")
    work, data = Path(workspace.path), Path(pgdata.path)
    spaces = tuple(Path(path.path) for path in tablespaces.values())
    if (len({(mount["device_id"], mount["inode"]) for mount in observed.values()}) != len(observed)
            or not data.is_relative_to(work) or root.is_relative_to(work) or work.is_relative_to(root)
            or any(data.iterdir()) or any(any(path.iterdir()) or path.is_relative_to(root)
                or root.is_relative_to(path) or path.is_relative_to(data) or data.is_relative_to(path) for path in spaces)
            or any(left.is_relative_to(right) or right.is_relative_to(left)
                   for index, left in enumerate(spaces) for right in spaces[index + 1:])):
        raise ValueError("canonical_recovery_preflight_receiver_workspace_invalid")
    deadline, elapsed = _deadline(timeout_seconds)
    pinned = {role: (os.makedev(*map(int, shape["device_id"].split(":"))), shape["inode"])
              for role, shape in observed.items()}

    def check():
        elapsed()
        if any(_identity(Path(mounts[role].path).lstat()) != identity for role, identity in pinned.items()):
            raise RuntimeError("canonical_recovery_preflight_receiver_mount_changed")

    pair = _baseline(root, site.base_transfer_sha256, site.baseline_verification, site.baseline_verification["verification_sha256"])
    if pair["filesystem_uuid"] not in source_filesystem_uuids:
        raise ValueError("canonical_recovery_preflight_source_filesystem_unpinned")
    receiver_signing_key, database_key, archive_key = (
        _private_credential(path, receiving=root, workspace=work) for path in (receiver_signing_key, database_key, archive_key)
    )
    signer = _signer(receiver_signing_key, site.receiver_public_key, root)
    br, rs = map(_binary, (pgbackrest, restic))
    pg_secret, rs_secret = _keys(SimpleNamespace(database_key_path=Path(database_key), archive_key_path=Path(archive_key)), root)
    with TemporaryDirectory(prefix="qt-named-preflight-", dir=work) as scratch:
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": scratch, "TMPDIR": scratch,
               "PGBACKREST_REPO1_CIPHER_PASS": pg_secret, "RESTIC_PASSWORD": rs_secret}
        run = partial(_run_native, cwd=scratch, env=env, deadline=deadline, check=check)
        br_cmd = [str(br), "--no-config", "--stanza=qt", "--repo1-cipher-type=aes-256-cbc", f"--repo1-path={root/'database'}",
                  "--log-level-file=off", "--log-level-console=off"]
        if run([str(br), "version"]).strip() != b"pgBackRest 2.59.1" or not run([str(rs), "version"]).startswith(b"restic 0.19.1 "):
            raise ValueError("canonical_recovery_preflight_native_tools_unqualified")
        info = run([*br_cmd, f"--lock-path={scratch}", f"--set={pair['database_label']}", "--output=json", "info"])
        selected, _, _ = _selected_backup(info, pair)
        selected_spaces = selected.get("tablespace") or []
        if (selected.get("link") or len(selected_spaces) != len(tablespaces)
                or {space["name"] for space in selected_spaces} != set(tablespaces)):
            raise ValueError("canonical_recovery_preflight_native_tablespace_allowlist_changed")
        by_oid = {str(space["oid"]): f"tablespace:{space['name']}" for space in selected_spaces}
        if (len(by_oid) != len(selected_spaces) or int(pair["database_identity"].split("/")[1]) not in
                {item["oid"] for item in selected["database-ref"]}):
            raise ValueError("canonical_recovery_preflight_native_manifest_identity_changed")
        database = _manifest_requirements(run, br_cmd, pair, by_oid, check, max_entries=max_manifest_entries)
        database_total = sum(item["logical_bytes"] for item in database.values())
        database_rounded = sum(item["logical_bytes"] + item["entries"] * observed[role]["block_size"]
                               for role, item in database.items())
        if database_total != selected["info"]["size"] or database_rounded > site.database.bytes:
            raise ValueError("canonical_recovery_preflight_database_native_size_changed_or_exceeds_budget")
        rs_cmd = [str(rs), "--repo", str(root / "archives"), "--no-lock", "--no-cache", "--json", "--quiet"]
        _snapshot_owner(run, rs_cmd, pair)
        restic_json = run([*rs_cmd, "stats", pair["archive_snapshot"], "--mode=restore-size"])
        base = json.loads(restic_json, object_pairs_hook=_unique_fields)
        if (base.get("snapshots_count") != 1 or type(base.get("total_size")) is not int
                or not pair["archive_bytes"] <= base["total_size"] <= site.archive.bytes
                or type(base.get("total_file_count")) is not int or base["total_file_count"] < pair["archive_objects"]
                or base["total_size"] + base["total_file_count"] * observed["workspace"]["block_size"] > site.archive.bytes):
            raise ValueError("canonical_recovery_preflight_base_archive_capacity_invalid")
    if _baseline(root, site.base_transfer_sha256, site.baseline_verification, site.baseline_verification["verification_sha256"]) != pair:
        raise RuntimeError("canonical_recovery_preflight_pair_changed")
    final = [site.archive, site.wal] + ([site.pending] if pending_expected else [])
    requirements = {name: {"checkpoint_bytes": item["logical_bytes"], "entries": item["entries"], "reserve_bytes": site.database.reserve_bytes}
                    for name, item in database.items()}
    requirements["pgdata"]["checkpoint_bytes"] += site.wal.files * site.baseline_verification["wal_segment_bytes"]
    requirements["pgdata"]["entries"] += site.wal.files
    # Each new Restic repository can create up to 300 extra directory entries.
    requirements["exchange"] = {"checkpoint_bytes": sum(limit.bytes for limit in final),
                                "entries": sum(limit.files for limit in final) + 64 + 300 * (1 + pending_expected),
                                "reserve_bytes": max(limit.reserve_bytes for limit in final)}
    # Archive objects admit at most 64 path components. Native archive-get uses
    # the workspace to verify one decrypted WAL segment at a time.
    requirements["workspace"] = {"checkpoint_bytes": base["total_size"] + site.archive.bytes + runtime_bytes
                                  + site.baseline_verification["wal_segment_bytes"] + (site.pending.bytes if pending_expected else 0),
                                  "entries": base["total_file_count"] + site.max_archive_objects * 64 + runtime_entries
                                  + (site.max_pending_entries if pending_expected else 0) + 64,
                                  "reserve_bytes": max(site.archive.reserve_bytes, site.pending.reserve_bytes if pending_expected else 0)}
    by_device = named._preflight_needs(observed, requirements)
    for role in requirements:
        old = observed[role]
        fresh = _observe(mounts[role], writable=True, udev=udev)
        if any(old[key] != fresh[key] for key in ("device_id", "filesystem_uuid", "inode", "block_size")):
            raise RuntimeError("canonical_recovery_preflight_receiver_mount_changed")
        device = by_device[fresh["device_id"]]
        if device.setdefault("filesystem_uuid", fresh["filesystem_uuid"]) != fresh["filesystem_uuid"]:
            raise RuntimeError("canonical_recovery_preflight_receiver_mount_changed")
        for key in ("available_bytes", "available_entries"):
            device[key] = min(device.get(key, fresh[key]), fresh[key])
    if any(item["available_bytes"] < item["required_bytes"] + item["reserve_bytes"] or item["available_entries"] < item["required_entries"]
           for item in by_device.values()):
        raise RuntimeError("canonical_recovery_preflight_receiver_capacity_insufficient")
    check()
    report = {"schema": named.PREFLIGHT_VERSION, "receiver_host_sha256": host,
              "source_host_sha256": source_host_sha256, "site_policy": site.preflight_scope(), "pending_expected": pending_expected,
              "source_filesystem_uuids": sorted(set(source_filesystem_uuids)),
              "receiver_mounts": observed,
              "database_targets": database, "archive_restore_bytes": base["total_size"], "archive_restore_files": base["total_file_count"],
              "requirements": requirements, "capacity_by_device": by_device}
    return _sign(report | {"measurement_sha256": archive_evidence_hash(report)}, signer)

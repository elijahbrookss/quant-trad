"""Measure a same-HDD QT recovery rehearsal without restoring or replaying it.

Supply a private, explicitly read-only repository mount and an empty scratch
directory on the same filesystem. The only writes are temporary native-tool
workspace files under scratch. Measurements describe the paired backup and
its Restic snapshot. They confer no recovery or cutover approval.

The PostgreSQL manifest entry limit applies during parsing. The same limit
checks Restic's entry count after stats returns. The timeout bounds the native
Restic metadata traversal; it does not count entries as Restic visits them.
"""

from __future__ import annotations

import argparse
from functools import partial
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from time import monotonic
from types import SimpleNamespace

from portal.backend.service.storage.incremental_recovery import _binary, _keys, _run_native
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from scripts.automation.canonical_cold_named_recovery import _TABLESPACE_NAME
from scripts.automation.canonical_cold_recovery_preflight import ReceiverPath, _manifest_requirements, _observe
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _source_generation
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _directory, _selected_backup_manifest, _snapshot_owner

def _require_isolated_mounts(repository, scratch, device_id):
    # Compare backing roots as well as container paths. A writable bind can
    # otherwise alias an empty subdirectory of the read-only repository.
    with Path("/proc/self/mountinfo").open("rb") as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise RuntimeError("same_hdd_rehearsal_mountinfo_budget_exceeded")
    mounts = {repository: [], scratch: []}
    for line in os.fsdecode(data).splitlines():
        fields = line.split(" ")
        root, mount = (Path(re.sub(r"\\([0-7]{3})", lambda value: chr(int(value[1], 8)), field))
                       for field in fields[3:5])
        if mount in mounts:
            mounts[mount].append((fields[2], root, "ro" in fields[5].split(",")))
        elif any(mount.is_relative_to(path) for path in mounts):
            raise ValueError("same_hdd_rehearsal_nested_mount")
    if any(len(matches) != 1 for matches in mounts.values()):
        raise ValueError("same_hdd_rehearsal_explicit_mounts_required")
    (repo_device, repo_root, repo_read_only), = mounts[repository]
    (scratch_device, scratch_root, scratch_read_only), = mounts[scratch]
    if (repo_read_only is not True or scratch_read_only or {repo_device, scratch_device} != {device_id}
            or scratch_root.is_relative_to(repo_root) or repo_root.is_relative_to(scratch_root)):
        raise ValueError("same_hdd_rehearsal_requires_readonly_repository_and_disjoint_scratch")


def _check_limits(args):
    for name, low, high in (
        ("restore_copies", 1, 2), ("timeout_seconds", 1, 3600), ("reserve_bytes", 0, 2**63 - 1),
        ("wal_bytes", 0, 2**63 - 1), ("scratch_bytes", 0, 2**63 - 1),
        ("wal_entries", 0, 2**63 - 1), ("scratch_entries", 0, 2**63 - 1),
        ("max_metadata_entries", 1, 10_000_000), ("max_stream_bytes", 1, 2**31),
    ):
        if type(value := getattr(args, name)) is not int or not low <= value <= high:
            raise ValueError(f"same_hdd_rehearsal_limit_invalid: {name}")
    if (not _sha256(args.pair_sha256) or not re.fullmatch(r"[0-9]+:[0-9]+", args.device_id)
            or len(args.tablespace) != len(set(args.tablespace))
            or not all(_TABLESPACE_NAME.fullmatch(name) for name in args.tablespace)):
        raise ValueError("same_hdd_rehearsal_scope_invalid")


def _measure(args):
    _check_limits(args)
    started = monotonic()
    deadline = started + args.timeout_seconds
    repository, scratch = map(_directory, (args.repository, args.scratch))
    if repository.is_relative_to(scratch) or scratch.is_relative_to(repository) or any(scratch.iterdir()):
        raise ValueError("same_hdd_rehearsal_requires_separate_empty_scratch")
    _require_isolated_mounts(repository, scratch, args.device_id)
    observe = partial(_observe, udev=Path("/run/udev/data"))
    source = observe(ReceiverPath(repository, args.filesystem_uuid), writable=False)
    target = observe(ReceiverPath(scratch, args.filesystem_uuid), writable=True)
    if (not source["read_only"] or {source["device_id"], target["device_id"]} != {args.device_id}
            or source["inode"] == target["inode"] or source["block_size"] != target["block_size"]):
        raise ValueError("same_hdd_rehearsal_device_or_mount_scope_changed")
    database_repo, archive_repo = map(_directory, (repository / "database", repository / "archives"))
    roots = {path: _identity(path.lstat()) for path in (repository, scratch, database_repo, archive_repo)}
    device = roots[repository][0]
    if any(dev != device for dev, _ in roots.values()):
        raise ValueError("same_hdd_rehearsal_repository_device_changed")

    def check():
        if monotonic() >= deadline:
            raise TimeoutError("same_hdd_rehearsal_measurement_deadline")
        for path, identity in roots.items():
            if _identity(path.lstat()) != identity:
                raise RuntimeError("same_hdd_rehearsal_mount_changed")
        if not all(os.statvfs(path).f_flag & os.ST_RDONLY for path in (repository, database_repo, archive_repo)):
            raise RuntimeError("same_hdd_rehearsal_repository_became_writable")

    _, _, pair = _source_generation(repository, device, args.generation, args.pair_sha256)
    if (pair["filesystem_uuid"] != args.filesystem_uuid or pair.get("restore_endpoint") != "immediate"
            or not re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", pair.get("database_identity", ""))
            or not _sha256(pair.get("archive_snapshot")) or not _sha256(pair.get("inventory_sha256"))
            or type(pair.get("archive_objects")) is not int
            or not 0 <= pair["archive_objects"] <= args.max_metadata_entries
            or type(pair.get("archive_bytes")) is not int or pair["archive_bytes"] < 0):
        raise ValueError("same_hdd_rehearsal_pair_scope_invalid")

    paths = (args.pgbackrest, args.restic, args.database_key, args.archive_key)
    if any(path.resolve(strict=True).is_relative_to(root) for path in paths for root in (repository, scratch)):
        raise ValueError("same_hdd_rehearsal_tools_and_keys_must_be_separate")
    pgbackrest, restic = map(_binary, paths[:2])
    pg_secret, rs_secret = _keys(
        SimpleNamespace(database_key_path=args.database_key, archive_key_path=args.archive_key), repository,
    )
    streamed_bytes = 0
    with TemporaryDirectory(prefix="qt-rehearsal-measure-", dir=scratch) as work:
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": work, "TMPDIR": work,
               "PGBACKREST_REPO1_CIPHER_PASS": pg_secret, "RESTIC_PASSWORD": rs_secret}
        native = partial(_run_native, cwd=work, env=env, deadline=deadline, check=check)

        def run(command, *, stdout_consumer=None):
            if stdout_consumer is None:
                return native(command)

            def consume(chunk):
                nonlocal streamed_bytes
                streamed_bytes += len(chunk)
                if streamed_bytes > args.max_stream_bytes:
                    raise RuntimeError("same_hdd_rehearsal_native_metadata_stdout_budget_exceeded")
                stdout_consumer(chunk)

            return native(command, stdout_consumer=consume)

        tools = {name: run([str(binary), "version"]).decode("ascii").strip()
                 for name, binary in (("pgbackrest", pgbackrest), ("restic", restic))}
        if (tools != pair["tools"] or tools["pgbackrest"] != "pgBackRest 2.59.1"
                or not tools["restic"].startswith("restic 0.19.1 ")):
            raise ValueError("same_hdd_rehearsal_tools_changed")
        br = [str(pgbackrest), "--no-config", "--stanza=qt", "--repo1-cipher-type=aes-256-cbc",
              f"--repo1-path={database_repo}", "--log-level-file=off", "--log-level-console=off"]
        br_info = [*br, f"--lock-path={work}"]
        rs = [str(restic), "--repo", str(archive_repo), "--no-lock", "--no-cache", "--json", "--quiet"]
        selected, database_id, chain = _selected_backup_manifest(run, br_info, pair)
        spaces = selected.get("tablespace") or []
        if (selected.get("link") or len(spaces) != len(args.tablespace)
                or {space["name"] for space in spaces} != set(args.tablespace)
                or any(type(space["oid"]) is not int or not 0 < space["oid"] < 2**32 for space in spaces)
                or int(pair["database_identity"].split("/")[1]) not in {item["oid"] for item in selected["database-ref"]}):
            raise ValueError("same_hdd_rehearsal_native_manifest_scope_changed")
        by_oid = {str(space["oid"]): f"tablespace:{space['name']}" for space in spaces}
        if len(by_oid) != len(spaces):
            raise ValueError("same_hdd_rehearsal_duplicate_tablespace_oid")
        database = _manifest_requirements(run, br, pair, by_oid, check, max_entries=args.max_metadata_entries)
        database_bytes = sum(item["logical_bytes"] for item in database.values())
        database_entries = sum(item["entries"] for item in database.values())
        if database_bytes != selected["info"]["size"]:
            raise RuntimeError("same_hdd_rehearsal_native_manifest_size_changed")

        _snapshot_owner(run, rs, pair)
        snapshot = json.loads(run([*rs, "stats", pair["archive_snapshot"], "--mode=restore-size"]),
                              object_pairs_hook=_unique_fields)
        archive_bytes = snapshot.get("total_size")
        archive_entries = snapshot.get("total_file_count")
        if (snapshot.get("snapshots_count") != 1 or type(archive_bytes) is not int or archive_bytes < pair["archive_bytes"]
                or type(archive_entries) is not int or not pair["archive_objects"] <= archive_entries <= args.max_metadata_entries):
            raise RuntimeError("same_hdd_rehearsal_archive_snapshot_stat_invalid")
        if _selected_backup_manifest(run, br_info, pair) != (selected, database_id, chain):
            raise RuntimeError("same_hdd_rehearsal_native_backup_changed")

    check()
    _require_isolated_mounts(repository, scratch, args.device_id)
    if any(scratch.iterdir()) or _source_generation(repository, device, args.generation, args.pair_sha256)[2] != pair:
        raise RuntimeError("same_hdd_rehearsal_scope_changed")
    fresh = observe(ReceiverPath(scratch, args.filesystem_uuid), writable=True)
    if any(fresh[key] != target[key] for key in ("inode", "device_id", "block_size")):
        raise RuntimeError("same_hdd_rehearsal_scratch_changed")
    check()

    # Restic's restore-size count includes visited directories. Also admit its
    # destination root. Both database copies share that archive materialization.
    archive_restore_entries = archive_entries + 1
    planned_entries = (args.restore_copies * database_entries + archive_restore_entries
                       + args.wal_entries + args.scratch_entries)
    rounding = (args.restore_copies * database_entries + archive_restore_entries) * target["block_size"]
    planned_bytes = args.restore_copies * database_bytes + archive_bytes + rounding + args.wal_bytes + args.scratch_bytes
    available_bytes = min(target["available_bytes"], fresh["available_bytes"])
    available_entries = min(target["available_entries"], fresh["available_entries"])
    bytes_remaining = available_bytes - planned_bytes - args.reserve_bytes
    entries_remaining = available_entries - planned_entries
    return {
        "schema": "qt.same_hdd_rehearsal_measurement.v1", "fits_sizing_plan": bytes_remaining >= 0 and entries_remaining >= 0,
        "repository": source["path"], "scratch": target["path"], "filesystem_uuid": args.filesystem_uuid,
        "device_id": args.device_id, "block_size": target["block_size"], "repository_read_only": True,
        "repository_inode": source["inode"], "scratch_inode": target["inode"],
        "generation": args.generation, "pair_sha256": args.pair_sha256, "database_identity": pair["database_identity"],
        "database_label": pair["database_label"], "database_chain": chain, "database_archive_id": f"15-{database_id}",
        "database_backup_wal": selected["archive"], "tablespaces": by_oid, "database_targets": database,
        "database_logical_bytes_per_copy": database_bytes, "database_entries_per_copy": database_entries,
        "archive_snapshot": pair["archive_snapshot"], "archive_inventory_sha256": pair["inventory_sha256"],
        "archive_objects_in_pair": pair["archive_objects"], "archive_logical_bytes": archive_bytes,
        "archive_snapshot_entries": archive_entries, "archive_restore_entries": archive_restore_entries,
        "database_restore_copies": args.restore_copies, "archive_restore_copies": 1, "allocation_rounding_bytes": rounding,
        "wal_extra_bytes": args.wal_bytes, "wal_extra_entries": args.wal_entries,
        "scratch_extra_bytes": args.scratch_bytes, "scratch_extra_entries": args.scratch_entries,
        "planned_bytes": planned_bytes, "planned_entries": planned_entries, "reserve_bytes": args.reserve_bytes,
        "available_bytes": available_bytes, "available_entries": available_entries,
        "remaining_bytes_after_reserve": bytes_remaining, "remaining_entries": entries_remaining,
        "byte_shortfall": max(0, -bytes_remaining), "entry_shortfall": max(0, -entries_remaining),
        "native_tools": tools, "streamed_metadata_bytes": streamed_bytes, "max_stream_bytes": args.max_stream_bytes,
        "max_metadata_entries": args.max_metadata_entries, "elapsed_seconds": round(monotonic() - started, 3),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repository", "scratch", "pgbackrest", "restic", "database-key", "archive-key"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    for name in ("generation", "pair-sha256", "filesystem-uuid", "device-id"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--tablespace", action="append", default=[], help="Repeat for each approved native tablespace name.")
    parser.add_argument("--restore-copies", required=True, type=int, choices=(1, 2),
                        help="Number of database materializations sharing one read-only archive baseline.")
    parser.add_argument("--reserve-bytes", required=True, type=int, help="Absolute bytes to leave available on the shared HDD.")
    for name in ("wal-bytes", "wal-entries", "scratch-bytes", "scratch-entries"):
        parser.add_argument(f"--{name}", required=True, type=int, help="Extra peak allowance across all copies.")
    parser.add_argument("--max-metadata-entries", type=int, default=2_000_000,
                        help="Stop PostgreSQL manifest parsing at this limit and check Restic's returned count.")
    parser.add_argument("--max-stream-bytes", type=int, default=512 * 1024**2,
                        help="Total stdout allowed for streamed PostgreSQL manifest and Restic snapshot metadata.")
    parser.add_argument("--timeout-seconds", type=int, default=600, help="Whole measurement deadline.")
    args = parser.parse_args(argv)
    try:
        report = _measure(args)
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0 if report["fits_sizing_plan"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

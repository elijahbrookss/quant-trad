"""Copy exact archive objects required by a private PostgreSQL pause at an LSN.

Run with no network. Mount the clone and verified baseline read-only under the
attempt root, the pair receipt read-only, and the archive input and writable
private suffix as separate binds. A matching object already in the suffix is
verified and reused even if its source has since expired. Optionally place the
required baseline objects in the same private archive for application reads.
Materializing the baseline also publishes the paused catalog as canonical JSONL.
The archive-pair preflight budgets that complete output and its candidate copy
before publishing any files. It can also run on fully read-only mounts.
Current serving SQL, WAL replay, backup keys, and archive deletion are outside
this tool's scope.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import stat
from time import monotonic

from sqlalchemy.exc import SQLAlchemyError

from core.storage_targets import StorageLocation
from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _archive_rows, _json_write, _sync
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root
from scripts.automation.verify_canonical_recovery_archive import _path, _sha256, _sha_file
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json, _directory
from scripts.ci.measure_same_hdd_rehearsal import _require_isolated_mounts
from scripts.ci.verify_paused_recovery_pair import (
    _add_scope_arguments, _bound_statement, _check_scope, _lsn, _mount_paths, _probe, _read_pair, _readonly_engine,
)
from scripts.db.canonical_cold_recovery_suffix import _copy_object, _encode, prepare_archive_baseline


_CATALOG_INVENTORY = "catalog.jsonl"


def _roots(args):
    source = _directory(args.source_archive)
    suffix, suffix_id = _private_root(args.suffix)
    paths = (args.attempt_root, source, suffix)
    for index, left in enumerate(paths):
        if any(left.is_relative_to(right) or right.is_relative_to(left) for right in paths[index + 1:]):
            raise ValueError("paused_archive_suffix_overlapping_paths")
    _require_isolated_mounts(source, suffix, args.device_id, scratch_read_only=args.preflight_only)
    _require_isolated_mounts(args.attempt_root, suffix, args.device_id, scratch_read_only=args.preflight_only)
    return _identity(source.lstat()), suffix_id


def _scope_data(scope):
    return (json.dumps(scope, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _bound_scope(suffix, scope):
    marker = suffix / "scope.json"
    if not os.path.lexists(marker):
        return False
    if _bound_json(marker, hashlib.sha256(_scope_data(scope)).hexdigest()) != scope:
        raise RuntimeError("paused_archive_suffix_scope_changed")
    return True


def _scope_directory(suffix, scope):
    marker = suffix / "scope.json"
    work = suffix / ".suffix-work"
    data = _scope_data(scope)
    existing = _bound_scope(suffix, scope)
    if not existing and any(path != work for path in suffix.iterdir()):
        raise RuntimeError("paused_archive_suffix_unbound_destination_not_empty")
    work.mkdir(mode=0o700, exist_ok=True)
    device = suffix.stat().st_dev
    if _private_root(work)[1][0] != device:
        raise RuntimeError("paused_archive_suffix_work_device_changed")
    # The exclusive root lock excludes another copier. Refuse unknown files
    # and keep final names even if a crash left an extra staging hard link.
    allowed_leftovers = {"object", marker.name}
    if scope.get("materialize_baseline"):
        allowed_leftovers.add(_CATALOG_INVENTORY)
    leftovers = []
    for entry in work.iterdir():
        info = entry.lstat()
        if (entry.name not in allowed_leftovers or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid() or info.st_dev != device):
            raise RuntimeError("paused_archive_suffix_unknown_work_file")
        if not existing:
            if entry.name != marker.name:
                raise RuntimeError("paused_archive_suffix_unbound_work_file")
            with os.fdopen(os.open(entry, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
                if not data.startswith(stream.read(len(data) + 1)):
                    raise RuntimeError("paused_archive_suffix_staged_scope_changed")
        leftovers.append(entry)
    for entry in leftovers:
        entry.unlink()
    if leftovers:
        _sync(work)
    if not existing:
        staged = work / marker.name
        _json_write(staged, scope)
        os.link(staged, marker)
        _sync(suffix)
        staged.unlink()
        _sync(work)
    objects = suffix / "objects"
    objects.mkdir(mode=0o700, exist_ok=True)
    if _private_root(objects)[1][0] != device:
        raise RuntimeError("paused_archive_suffix_object_device_changed")
    _sync(suffix)
    return objects, work


def _place_object(key, descriptor, *, source, objects, work, reserve_bytes, check, reserve_entries=0):
    """Verify a durable checkpoint or publish a newly verified temporary file."""
    destination = objects / key
    if destination.exists() or destination.is_symlink():
        actual = _sha_file(_path(objects, key, absolute=False), check=check)
        if actual != descriptor:
            raise RuntimeError(f"paused_archive_suffix_checkpoint_bytes_changed: {key}")
        _sync(destination.parent)
        return False
    if destination.resolve() != destination:
        raise RuntimeError(f"paused_archive_suffix_destination_alias: {key}")
    try:
        original = _path(source, key, absolute=False)
    except FileNotFoundError as error:
        raise RuntimeError(f"paused_archive_suffix_source_missing: {key}") from error
    capacity = os.statvfs(objects)
    block = capacity.f_frsize
    allocation = (descriptor[1] + block - 1) // block * block + len(Path(key).parts) * block
    if (capacity.f_bavail * block < reserve_bytes + allocation
            or capacity.f_favail < reserve_entries + len(Path(key).parts)):
        raise RuntimeError(f"paused_archive_suffix_object_exceeds_free_space: {key}")
    directory = objects
    for part in Path(key).parts[:-1]:
        directory /= part
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        else:
            _sync(directory.parent)
        if _private_root(directory)[1][0] != objects.stat().st_dev:
            raise RuntimeError("paused_archive_suffix_object_device_changed")
        check()
    staged = work / "object"
    try:
        _copy_object(original, staged, descriptor, check)
    except (OSError, ValueError, RuntimeError) as error:
        raise RuntimeError(f"paused_archive_suffix_source_copy_failed: {key}: {error}") from error
    os.link(staged, destination)
    _sync(destination.parent)
    staged.unlink()
    check()
    return True


def _catalog_descriptors(conn, args, baseline, check, bound):
    seen = set()
    for row in _archive_rows(conn, max_objects=args.max_objects, check=bound):
        check()
        key, digest, size = row["object_key"], row["object_sha256"], row["byte_count"]
        StorageLocation("archive", key)
        if (len(key) > 2048 or len(Path(key).parts) > 64 or not _sha256(digest)
                or type(size) is not int or size <= 0 or key in seen):
            raise RuntimeError("paused_archive_suffix_catalog_descriptor_invalid_or_duplicate")
        descriptor = digest, size
        seen.add(key)
        known = baseline.objects.get(key)
        if known is not None and known != descriptor:
            raise RuntimeError(f"paused_archive_suffix_baseline_key_changed: {key}")
        yield key, descriptor, _encode(key, descriptor), known is not None


def _copy_catalog(conn, args, baseline, objects, work, check, bound, *, baseline_bytes, baseline_source=None,
                  catalog_inventory=None, reserve_bytes=None, reserve_entries=0):
    catalog_hash, suffix_hash = hashlib.sha256(), hashlib.sha256()
    total_bytes = suffix_bytes = copied = reused = baseline_copied = baseline_reused = 0
    inventory_bytes = count = 0
    reserve = args.reserve_bytes if reserve_bytes is None else reserve_bytes
    for key, descriptor, encoded, uses_baseline in _catalog_descriptors(conn, args, baseline, check, bound):
        count += 1
        size = descriptor[1]
        total_bytes += size
        catalog_hash.update(encoded)
        if not uses_baseline:
            suffix_bytes += size
        if catalog_inventory is not None:
            inventory_bytes += len(encoded)
        if baseline_bytes + suffix_bytes + inventory_bytes > args.max_work_bytes:
            raise RuntimeError("paused_archive_suffix_work_byte_budget_exceeded")
        if catalog_inventory is not None:
            catalog_inventory.write(encoded)
        if uses_baseline:
            if baseline_source is None:
                continue
            added = _place_object(key, descriptor, source=baseline_source, objects=objects, work=work,
                                  reserve_bytes=reserve, reserve_entries=reserve_entries, check=check)
            baseline_copied += added
            baseline_reused += not added
            continue
        added = _place_object(key, descriptor, source=args.source_archive, objects=objects, work=work,
                              reserve_bytes=reserve, reserve_entries=reserve_entries, check=check)
        copied += added
        reused += not added
        suffix_hash.update(encoded)
    result = {
        "catalog_inventory_sha256": catalog_hash.hexdigest(), "catalog_objects": count, "catalog_bytes": total_bytes,
        "suffix_inventory_sha256": suffix_hash.hexdigest(), "suffix_objects": copied + reused, "suffix_bytes": suffix_bytes,
        "copied_objects": copied, "reused_objects": reused,
        "baseline_objects_used": count - copied - reused, "baseline_bytes_used": total_bytes - suffix_bytes,
    }
    if baseline_source is not None:
        result.update(archive_root=str(objects.parent), materialized_inventory_sha256=catalog_hash.hexdigest(),
                      materialized_objects=count, materialized_bytes=total_bytes,
                      copied_baseline_objects=baseline_copied, reused_baseline_objects=baseline_reused,
                      catalog_inventory_bytes=inventory_bytes)
    return result


def _filesystem_space(path):
    fs = os.statvfs(path)
    if min(fs.f_frsize, fs.f_blocks, fs.f_files) <= 0:
        raise RuntimeError("paused_archive_suffix_filesystem_counters_missing")
    return fs.f_frsize, fs.f_bavail * fs.f_frsize, fs.f_favail


def _existing_object_credit(path, *, device, size, rounded):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return 0, 0
    if (not stat.S_ISREG(info.st_mode) or info.st_dev != device or info.st_size != size
            or info.st_uid != os.getuid() or path.resolve(strict=True) != path):
        raise RuntimeError(f"paused_archive_suffix_invalid_checkpoint: {path.name}")
    # Count only file blocks already allocated in this private scope. The copy
    # pass verifies content and keeps conservative metadata headroom on retries.
    return (min(rounded, info.st_blocks * 512), 1) if info.st_nlink == 1 else (0, 0)


def _measure_catalog(conn, args, baseline, scope, check, bound, *, baseline_bytes):
    block, before_bytes, before_entries = _filesystem_space(args.suffix)
    existing = _bound_scope(args.suffix, scope)
    device = args.suffix.stat().st_dev
    digest = hashlib.sha256()
    count = logical = suffix_bytes = inventory = rounded = parent_occurrences = credited = credited_entries = 0

    def round_blocks(size):
        return (size + block - 1) // block * block

    for key, (_, size), encoded, uses_baseline in _catalog_descriptors(conn, args, baseline, check, bound):
        count += 1
        logical += size
        inventory += len(encoded)
        if not uses_baseline:
            suffix_bytes += size
        object_blocks = round_blocks(size)
        rounded += object_blocks
        parent_occurrences += len(Path(key).parts) - 1
        if existing:
            allocated, entries = _existing_object_credit(args.suffix / "objects" / key, device=device,
                                                          size=size, rounded=object_blocks)
            credited += allocated
            credited_entries += entries
        digest.update(encoded)

    after_block, after_bytes, after_entries = _filesystem_space(args.suffix)
    if block != after_block:
        raise RuntimeError("paused_archive_suffix_allocation_unit_changed")
    # Data blocks are exact rounds of accepted object lengths. Metadata uses one
    # block for each potential directory and object entry, so reused parents do
    # not make the admission depend on an optimistic directory topology.
    candidate_bytes = rounded + (8 + parent_occurrences + count) * block
    source_bytes = candidate_bytes + round_blocks(inventory)
    candidate_entries = 4 + count + parent_occurrences
    source_entries = candidate_entries + 1
    combined_bytes = source_bytes + candidate_bytes
    required_bytes = combined_bytes - credited + args.reserve_bytes + args.other_planned_bytes
    required_entries = source_entries + candidate_entries - credited_entries + args.other_planned_entries
    available_bytes = min(before_bytes, after_bytes)
    available_entries = min(before_entries, after_entries)
    fits = (baseline_bytes + suffix_bytes + inventory <= args.max_work_bytes
            and combined_bytes <= args.max_pair_allocated_bytes
            and required_bytes <= available_bytes and required_entries <= available_entries)
    return {"admission": "fits_inputs" if fits else "insufficient", "catalog_objects": count,
            "catalog_logical_bytes": logical, "catalog_inventory_bytes": inventory,
            "catalog_inventory_sha256": digest.hexdigest(), "rounded_object_data_bytes": rounded,
            "allocation_unit_bytes": block, "source_allocation_bound_bytes": source_bytes,
            "candidate_allocation_bound_bytes": candidate_bytes, "combined_allocation_bound_bytes": combined_bytes,
            "source_entry_bound": source_entries, "candidate_entry_bound": candidate_entries,
            "previously_allocated_object_bytes": credited, "previously_allocated_object_entries": credited_entries,
            "other_planned_bytes": args.other_planned_bytes, "other_planned_entries": args.other_planned_entries,
            "reserve_bytes": args.reserve_bytes, "max_pair_allocated_bytes": args.max_pair_allocated_bytes,
            "work_logical_bytes": baseline_bytes + suffix_bytes + inventory, "max_work_bytes": args.max_work_bytes,
            "required_free_bytes": required_bytes, "required_free_entries": required_entries,
            "available_bytes": available_bytes, "available_entries": available_entries}


def _publish_catalog_inventory(suffix, work, digest, check):
    staged = work / _CATALOG_INVENTORY
    published = suffix / _CATALOG_INVENTORY
    if published.exists() or published.is_symlink():
        existing, _ = _sha_file(_path(suffix, _CATALOG_INVENTORY, absolute=False), check=check)
        if existing != digest:
            raise RuntimeError("paused_archive_suffix_catalog_inventory_changed")
    else:
        os.link(staged, published)
    _sync(suffix)
    staged.unlink()
    _sync(work)
    return published


def materialize(args):
    target, replay = _lsn(args.target_lsn), _lsn(args.expected_replay_lsn)
    paired = args.max_pair_allocated_bytes is not None
    if (not (0, 0) < target <= replay or not 0 < args.max_work_bytes < 2**63
            or not 0 <= args.reserve_bytes < 2**63 or not 0 <= args.other_planned_bytes < 2**63
            or not 0 <= args.other_planned_entries < 2**63
            or (paired and (not args.materialize_baseline or not 0 < args.max_pair_allocated_bytes < 2**63))
            or (not paired and (args.preflight_only or args.other_planned_bytes or args.other_planned_entries))):
        raise ValueError("paused_archive_suffix_scope_or_limits_invalid")
    args.minimum_replay_lsn = "%X/%X" % replay
    _check_scope(args)
    started = monotonic()
    deadline = started + args.timeout_seconds
    initial_mounts = _mount_paths(args)
    source_id, suffix_id = _roots(args)
    pair = _read_pair(args)
    baseline_bytes = pair["archive_bytes"]
    if type(baseline_bytes) is not int or not 0 <= baseline_bytes <= args.max_work_bytes:
        raise RuntimeError("paused_archive_suffix_baseline_byte_budget_exceeded")

    next_fs_check = started
    copy_reserve_bytes, copy_reserve_entries = args.reserve_bytes, 0

    def check():
        nonlocal next_fs_check
        now = monotonic()
        if now >= deadline:
            raise TimeoutError("paused_archive_suffix_deadline")
        if now >= next_fs_check:
            next_fs_check = now + 0.25
            free = os.statvfs(args.suffix)
            if (_identity(args.source_archive.lstat()) != source_id or _identity(args.suffix.lstat()) != suffix_id
                    or not os.statvfs(args.source_archive).f_flag & os.ST_RDONLY
                    or bool(free.f_flag & os.ST_RDONLY) != args.preflight_only):
                raise RuntimeError("paused_archive_suffix_mount_changed")
            if free.f_bavail * free.f_frsize < copy_reserve_bytes or free.f_favail < copy_reserve_entries:
                raise RuntimeError("paused_archive_suffix_reserve_exhausted")
        return deadline - now

    def bound(conn):
        _bound_statement(conn, check, args.statement_timeout_seconds)

    def probe(conn):
        state = _probe(conn, args, pair, target_lsn=args.target_lsn, replay_lsn=args.expected_replay_lsn)
        bound(conn)
        watermark = PostgresMarketDataRepository._current_commit_seq_with_session(conn)
        return state | {"accepted_commit_seq": watermark}

    engine = _readonly_engine(args, check)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            bound(conn)
            before = probe(conn)
        with archive_namespace(args.suffix, exclusive=True) as held:
            baseline = prepare_archive_baseline(args.receipt, args.archive_root,
                expected_receipt_sha256=args.receipt_sha256, max_objects=args.max_objects, check=check)
            baseline_source = None
            if args.materialize_baseline:
                baseline_source = _path(args.archive_root, pair["archive_source_root"],
                                        absolute=True) if baseline.objects else args.archive_root
            if _mount_paths(args) != initial_mounts or _roots(args) != (source_id, suffix_id):
                raise RuntimeError("paused_archive_suffix_mount_changed")
            scope = {
                "schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": args.receipt_sha256,
                "database_identity": baseline.database_identity, "storage_layout": baseline.layout.copy(),
                "timeline": args.timeline, "target_lsn": "%X/%X" % target, "replay_lsn": before["replay_lsn"],
                "accepted_commit_seq": before["accepted_commit_seq"],
                "pgdata": str(args.pgdata), "pgdata_inode": args.pgdata_inode,
                "source_archive": str(args.source_archive), "source_inode": source_id[1], "suffix_inode": suffix_id[1],
                "device_id": args.device_id,
            }
            if baseline_source is not None:
                scope["materialize_baseline"] = True
            with engine.begin() as conn:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                bound(conn)
                if probe(conn) != before:
                    raise RuntimeError("paused_archive_suffix_endpoint_changed")
                held()
                admission = None
                if paired:
                    admission = _measure_catalog(conn, args, baseline, scope, check, lambda: bound(conn),
                                                 baseline_bytes=baseline_bytes)
                    if not args.preflight_only and admission["admission"] != "fits_inputs":
                        raise RuntimeError("paused_archive_suffix_pair_budget_exceeded: " + json.dumps(admission))
                if not args.preflight_only:
                    if admission is not None:
                        copy_reserve_bytes += admission["candidate_allocation_bound_bytes"] + args.other_planned_bytes
                        copy_reserve_entries = admission["candidate_entry_bound"] + args.other_planned_entries
                    copied, objects, work = _materialize_catalog(conn, args, baseline, scope, check, lambda: bound(conn),
                        baseline_bytes=baseline_bytes, baseline_source=baseline_source, admission=admission,
                        reserve_bytes=copy_reserve_bytes, reserve_entries=copy_reserve_entries)
                bound(conn)
                if probe(conn) != before:
                    raise RuntimeError("paused_archive_suffix_endpoint_changed")
            held()
            if (_mount_paths(args) != initial_mounts or _roots(args) != (source_id, suffix_id)
                    or _read_pair(args) != pair):
                raise RuntimeError("paused_archive_suffix_final_scope_changed")
            check()
            if args.materialize_baseline and not args.preflight_only:
                copied["catalog_inventory_path"] = str(_publish_catalog_inventory(
                    args.suffix, work, copied["catalog_inventory_sha256"], check))
    finally:
        engine.dispose()
    receipt = scope | {
        "archive_snapshot": pair["archive_snapshot"], "baseline_inventory_sha256": pair["inventory_sha256"],
        "elapsed_seconds": round(monotonic() - started, 3),
    }
    if args.preflight_only:
        receipt.update(admission, schema="qt.paused_archive_pair_preflight.v1")
    else:
        receipt.update(copied, suffix_root=str(objects),
                       work_logical_bytes=baseline_bytes + copied["suffix_bytes"] + copied.get("catalog_inventory_bytes", 0))
        if admission is not None:
            receipt["archive_pair_admission"] = admission
    return receipt | {"verification_sha256": archive_evidence_hash(receipt)}


def _materialize_catalog(conn, args, baseline, scope, check, bound, *, baseline_bytes, baseline_source, admission,
                         reserve_bytes, reserve_entries):
    objects, work = _scope_directory(args.suffix, scope)
    inventory = nullcontext()
    if args.materialize_baseline:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        inventory = os.fdopen(os.open(work / _CATALOG_INVENTORY, flags, 0o600), "wb")
    with inventory as catalog:
        copied = _copy_catalog(conn, args, baseline, objects, work, check, bound, baseline_bytes=baseline_bytes,
                               baseline_source=baseline_source, catalog_inventory=catalog,
                               reserve_bytes=reserve_bytes, reserve_entries=reserve_entries)
        if admission is not None and (copied["catalog_inventory_sha256"], copied["catalog_objects"],
                                      copied["catalog_bytes"], copied["catalog_inventory_bytes"]) != (
                admission["catalog_inventory_sha256"], admission["catalog_objects"],
                admission["catalog_logical_bytes"], admission["catalog_inventory_bytes"]):
            raise RuntimeError("paused_archive_suffix_catalog_changed_after_admission")
        if catalog is not None:
            catalog.flush()
            os.fsync(catalog.fileno())
    return copied, objects, work


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    _add_scope_arguments(parser)
    parser.add_argument("--target-lsn", required=True, help="The configured PostgreSQL recovery_target_lsn.")
    parser.add_argument("--expected-replay-lsn", required=True, help="Exact expected replay record end at pause.")
    parser.add_argument("--source-archive", type=Path, required=True, help="Read-only bind of the archive object root.")
    parser.add_argument("--suffix", type=Path, required=True, help="Private bind for this endpoint's suffix.")
    parser.add_argument("--materialize-baseline", action="store_true",
                        help="Also copy referenced baseline objects into the same private application archive.")
    parser.add_argument("--max-work-bytes", type=int, required=True,
                        help="Logical bytes across the baseline, suffix objects and any materialized catalog.")
    parser.add_argument("--reserve-bytes", type=int, required=True, help="Free HDD bytes to preserve.")
    parser.add_argument("--max-pair-allocated-bytes", type=int,
                        help="Combined allocation ceiling for the late archive and a separate candidate baseline.")
    parser.add_argument("--other-planned-bytes", type=int, default=0,
                        help="Additional device capacity reserved for still-unallocated rehearsal work.")
    parser.add_argument("--other-planned-entries", type=int, default=0,
                        help="Device entries reserved for still-unallocated rehearsal work.")
    parser.add_argument("--preflight-only", action="store_true",
                        help="Measure the archive pair without copying. Requires a read-only suffix bind and pair cap.")
    try:
        receipt = materialize(parser.parse_args(argv))
    except SQLAlchemyError as error:
        parser.exit(1, f"paused_archive_suffix_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False))
    return 3 if receipt.get("admission") == "insufficient" else 0


if __name__ == "__main__":
    raise SystemExit(main())

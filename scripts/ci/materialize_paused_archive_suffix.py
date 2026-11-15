"""Copy exact archive objects required by a private PostgreSQL pause at an LSN.

Run with no network. Mount the clone and verified baseline read-only under the
attempt root, the pair receipt read-only, and the archive input and writable
private suffix as separate binds. A matching object already in the suffix is
verified and reused even if its source has since expired. Optionally place the
required baseline objects in the same private archive for application reads.
Current serving SQL, WAL replay, backup keys, and archive deletion are outside
this tool's scope.
"""

from __future__ import annotations

import argparse
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


def _roots(args):
    source = _directory(args.source_archive)
    suffix, suffix_id = _private_root(args.suffix)
    paths = (args.attempt_root, source, suffix)
    for index, left in enumerate(paths):
        if any(left.is_relative_to(right) or right.is_relative_to(left) for right in paths[index + 1:]):
            raise ValueError("paused_archive_suffix_overlapping_paths")
    _require_isolated_mounts(source, suffix, args.device_id)
    _require_isolated_mounts(args.attempt_root, suffix, args.device_id)
    return _identity(source.lstat()), suffix_id


def _scope_directory(suffix, scope):
    marker = suffix / "scope.json"
    work = suffix / ".suffix-work"
    data = (json.dumps(scope, sort_keys=True, separators=(",", ":")) + "\n").encode()
    existing = marker.exists() or marker.is_symlink()
    if existing and _bound_json(marker, hashlib.sha256(data).hexdigest()) != scope:
        raise RuntimeError("paused_archive_suffix_scope_changed")
    if not existing and any(path != work for path in suffix.iterdir()):
        raise RuntimeError("paused_archive_suffix_unbound_destination_not_empty")
    work.mkdir(mode=0o700, exist_ok=True)
    device = suffix.stat().st_dev
    if _private_root(work)[1][0] != device:
        raise RuntimeError("paused_archive_suffix_work_device_changed")
    # The exclusive root lock excludes another copier. Refuse unknown files
    # and keep final names even if a crash left an extra staging hard link.
    leftovers = []
    for entry in work.iterdir():
        info = entry.lstat()
        if (entry.name not in {"object", marker.name} or not stat.S_ISREG(info.st_mode)
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


def _place_object(key, descriptor, *, source, objects, work, reserve_bytes, check):
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
    if capacity.f_bavail * block < reserve_bytes + allocation:
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


def _copy_catalog(conn, args, baseline, objects, work, check, bound, *, baseline_bytes, baseline_source=None):
    catalog_hash, suffix_hash = hashlib.sha256(), hashlib.sha256()
    seen = set()
    total_bytes = suffix_bytes = copied = reused = baseline_copied = baseline_reused = 0
    for row in _archive_rows(conn, max_objects=args.max_objects, check=bound):
        check()
        key, digest, size = row["object_key"], row["object_sha256"], row["byte_count"]
        StorageLocation("archive", key)
        if (len(key) > 2048 or len(Path(key).parts) > 64 or not _sha256(digest)
                or type(size) is not int or size <= 0 or key in seen):
            raise RuntimeError("paused_archive_suffix_catalog_descriptor_invalid_or_duplicate")
        descriptor = digest, size
        seen.add(key)
        total_bytes += size
        encoded = _encode(key, descriptor)
        catalog_hash.update(encoded)
        known = baseline.objects.get(key)
        if known is not None:
            if known != descriptor:
                raise RuntimeError(f"paused_archive_suffix_baseline_key_changed: {key}")
            if baseline_source is None:
                continue
            added = _place_object(key, descriptor, source=baseline_source, objects=objects, work=work,
                                  reserve_bytes=args.reserve_bytes, check=check)
            baseline_copied += added
            baseline_reused += not added
            continue
        suffix_bytes += size
        if baseline_bytes + suffix_bytes > args.max_work_bytes:
            raise RuntimeError("paused_archive_suffix_work_byte_budget_exceeded")
        added = _place_object(key, descriptor, source=args.source_archive, objects=objects, work=work,
                              reserve_bytes=args.reserve_bytes, check=check)
        copied += added
        reused += not added
        suffix_hash.update(encoded)
    result = {
        "catalog_inventory_sha256": catalog_hash.hexdigest(), "catalog_objects": len(seen), "catalog_bytes": total_bytes,
        "suffix_inventory_sha256": suffix_hash.hexdigest(), "suffix_objects": copied + reused, "suffix_bytes": suffix_bytes,
        "copied_objects": copied, "reused_objects": reused,
        "baseline_objects_used": len(seen) - copied - reused, "baseline_bytes_used": total_bytes - suffix_bytes,
    }
    if baseline_source is not None:
        result.update(archive_root=str(objects.parent), materialized_inventory_sha256=catalog_hash.hexdigest(),
                      materialized_objects=len(seen), materialized_bytes=total_bytes,
                      copied_baseline_objects=baseline_copied, reused_baseline_objects=baseline_reused)
    return result


def materialize(args):
    target, replay = _lsn(args.target_lsn), _lsn(args.expected_replay_lsn)
    if not (0, 0) < target <= replay or not 0 < args.max_work_bytes < 2**63 or not 0 <= args.reserve_bytes < 2**63:
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

    def check():
        nonlocal next_fs_check
        now = monotonic()
        if now >= deadline:
            raise TimeoutError("paused_archive_suffix_deadline")
        if now >= next_fs_check:
            next_fs_check = now + 0.25
            free = os.statvfs(args.suffix)
            if (_identity(args.source_archive.lstat()) != source_id or _identity(args.suffix.lstat()) != suffix_id
                    or not os.statvfs(args.source_archive).f_flag & os.ST_RDONLY or free.f_flag & os.ST_RDONLY):
                raise RuntimeError("paused_archive_suffix_mount_changed")
            if free.f_bavail * free.f_frsize < args.reserve_bytes:
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
                objects, work = _scope_directory(args.suffix, scope)
                copied = _copy_catalog(conn, args, baseline, objects, work, check, lambda: bound(conn),
                                       baseline_bytes=baseline_bytes, baseline_source=baseline_source)
                bound(conn)
                if probe(conn) != before:
                    raise RuntimeError("paused_archive_suffix_endpoint_changed")
            held()
            if (_mount_paths(args) != initial_mounts or _roots(args) != (source_id, suffix_id)
                    or _read_pair(args) != pair):
                raise RuntimeError("paused_archive_suffix_final_scope_changed")
            check()
    finally:
        engine.dispose()
    receipt = scope | copied | {
        "archive_snapshot": pair["archive_snapshot"], "baseline_inventory_sha256": pair["inventory_sha256"],
        "suffix_root": str(objects), "work_logical_bytes": baseline_bytes + copied["suffix_bytes"],
        "elapsed_seconds": round(monotonic() - started, 3),
    }
    return receipt | {"verification_sha256": archive_evidence_hash(receipt)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    _add_scope_arguments(parser)
    parser.add_argument("--target-lsn", required=True, help="The configured PostgreSQL recovery_target_lsn.")
    parser.add_argument("--expected-replay-lsn", required=True, help="Exact expected replay record end at pause.")
    parser.add_argument("--source-archive", type=Path, required=True, help="Read-only bind of the archive object root.")
    parser.add_argument("--suffix", type=Path, required=True, help="Private writable bind for this endpoint's suffix.")
    parser.add_argument("--materialize-baseline", action="store_true",
                        help="Also copy referenced baseline objects into the same private application archive.")
    parser.add_argument("--max-work-bytes", type=int, required=True, help="Logical bytes verified across baseline and suffix.")
    parser.add_argument("--reserve-bytes", type=int, required=True, help="Free HDD bytes to preserve.")
    try:
        receipt = materialize(parser.parse_args(argv))
    except SQLAlchemyError as error:
        parser.exit(1, f"paused_archive_suffix_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()

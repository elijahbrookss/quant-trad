"""Verify a copied PostgreSQL rehearsal before promoting the private copy.

Mount the copy parent, PostgreSQL paths, socket, unified archive, and receipts
read-only. The native postmaster stays paused. This check binds the copy and
the archive to the same accepted commit without booting or promoting anything.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from time import monotonic

from sqlalchemy.exc import SQLAlchemyError

from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _archive_rows
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root
from scripts.automation.verify_canonical_recovery_archive import _path, _sha256, _sha_file
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json
from scripts.ci.verify_paused_recovery_pair import (
    _add_scope_arguments, _bound_statement, _check_scope, _lsn, _probe, _read_pair, _readonly_engine,
)
from scripts.db.canonical_cold_recovery_suffix import _encode


_SCHEMA = "qt.benchmark_clone_boundary.v1"
_COPY_SCOPE = ("schema", "binding_sha256", "system_identifier", "timeline", "device_id", "source",
               "destination_inode", "tablespace_oid", "clone_history_path")


def _read_receipt(path, expected_hash):
    if not stat.S_ISREG(path.lstat().st_mode) or not os.statvfs(path).f_flag & os.ST_RDONLY:
        raise ValueError("benchmark_clone_requires_readonly_receipts")
    receipt = _bound_json(path, expected_hash)
    if not isinstance(receipt, dict):
        raise ValueError("benchmark_clone_receipt_invalid")
    body = {key: value for key, value in receipt.items() if key != "verification_sha256"}
    if receipt.get("verification_sha256") != archive_evidence_hash(body):
        raise ValueError("benchmark_clone_receipt_body_hash_changed")
    return receipt


def _require_mounts(args):
    parents = (args.attempt_root, args.archive_root)
    guarded = (*parents, args.pgdata, args.tablespace)
    with Path("/proc/self/mountinfo").open("rb") as stream:
        contents = stream.read(4 * 1024**2 + 1)
    if len(contents) > 4 * 1024**2:
        raise RuntimeError("benchmark_clone_mountinfo_budget_exceeded")
    entries = {parent: [] for parent in parents}
    for line in os.fsdecode(contents).splitlines():
        fields = line.split(" ")
        mount = Path(re.sub(r"\\([0-7]{3})", lambda value: chr(int(value[1], 8)), fields[4]))
        if mount in entries:
            entries[mount].append((fields[2], "ro" in fields[5].split(",")))
        elif any(mount != parent and mount.is_relative_to(parent) for parent in guarded):
            raise ValueError("benchmark_clone_unexpected_nested_mount")
    if any(matches != [(args.device_id, True)] for matches in entries.values()):
        raise ValueError("benchmark_clone_requires_exact_readonly_copy_and_archive_mounts")


def _mount_identities(args):
    paths = {"copy": args.attempt_root, "pgdata": args.pgdata, "tablespace": args.tablespace,
             "socket_directory": args.socket, "archive": args.archive_root,
             "objects": args.archive_root / "objects"}
    roots = {role: _private_root(path)[1] for role, path in paths.items()}
    device = roots["copy"][0]
    if (f"{os.major(device)}:{os.minor(device)}" != args.device_id
            or {identity[0] for identity in roots.values()} != {device}
            or len(set(roots.values())) != len(roots)
            or any(not os.statvfs(path).f_flag & os.ST_RDONLY for path in paths.values())
            or (roots["pgdata"][1], roots["tablespace"][1]) != (args.pgdata_inode, args.tablespace_inode)
            or any(_private_root(args.attempt_root / name)[1] != roots[role]
                   for name, role in (("pgdata", "pgdata"), ("history", "tablespace")))):
        raise RuntimeError("benchmark_clone_readonly_mount_identity_changed")
    link = args.pgdata / "pg_tblspc" / str(args.tablespace_oid)
    if (not link.is_symlink() or os.readlink(link) != str(args.tablespace)
            or _identity(link.stat()) != roots["tablespace"]):
        raise RuntimeError("benchmark_clone_tablespace_link_changed")
    socket_path = args.socket / f".s.PGSQL.{args.port}"
    info = socket_path.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_dev != device:
        raise RuntimeError("benchmark_clone_private_socket_changed")
    return roots | {"socket": _identity(info)}


def _binding(args, pair, mounts):
    copy = _read_receipt(args.copy_receipt, args.copy_receipt_sha256)
    archive = _read_receipt(args.suffix_receipt, args.suffix_receipt_sha256)
    system_id, _database_oid = pair["database_identity"].split("/", 1)
    scope = {key: copy[key] for key in _COPY_SCOPE}
    source = scope["source"]
    expected_copy = {"schema": "qt.stopped_rehearsal_database_copy.v1", "system_identifier": system_id,
                     "timeline": args.timeline, "device_id": args.device_id,
                     "binding_sha256": args.expected_copy_binding_sha256, "destination_inode": mounts["copy"][1],
                     "tablespace_oid": args.tablespace_oid, "clone_history_path": str(args.tablespace)}
    if (any(scope[key] != value for key, value in expected_copy.items())
            or not _sha256(copy["file_inventory_sha256"]) or not _sha256(source["control"]["sha256"])
            or not _sha256(source["metadata_sha256"])
            or {args.pgdata_inode, args.tablespace_inode} & {source["pgdata_inode"], source["history_inode"]}):
        raise RuntimeError("benchmark_clone_copy_boundary_changed")
    scope_bytes = (json.dumps(scope, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
    if _bound_json(args.attempt_root / "scope.json", hashlib.sha256(scope_bytes).hexdigest()) != scope:
        raise RuntimeError("benchmark_clone_copy_scope_changed")
    expected_archive = {"schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": args.receipt_sha256,
                        "database_identity": pair["database_identity"], "storage_layout": pair["storage_layout"],
                        "timeline": args.timeline, "device_id": args.device_id, "target_lsn": args.target_lsn,
                        "replay_lsn": args.expected_replay_lsn, "pgdata": source["pgdata"],
                        "pgdata_inode": source["pgdata_inode"], "suffix_inode": mounts["archive"][1],
                        "materialize_baseline": True, "archive_snapshot": pair["archive_snapshot"],
                        "baseline_inventory_sha256": pair["inventory_sha256"]}
    if (any(archive.get(key) != value for key, value in expected_archive.items())
            or not _sha256(archive["catalog_inventory_sha256"])
            or (archive["catalog_inventory_sha256"], archive["catalog_objects"], archive["catalog_bytes"]) != (
                archive["materialized_inventory_sha256"], archive["materialized_objects"], archive["materialized_bytes"])
            or type(archive["catalog_objects"]) is not int or not 1 <= archive["catalog_objects"] <= args.max_objects
            or type(archive["catalog_bytes"]) is not int or not 1 <= archive["catalog_bytes"] <= args.max_archive_bytes):
        raise RuntimeError("benchmark_clone_archive_boundary_changed_or_exceeds_budget")
    return copy, archive


def _verify_archive(conn, objects, *, expected, max_objects, max_bytes, check, bound):
    seen = set()
    digest = hashlib.sha256()
    logical_bytes = 0
    for row in _archive_rows(conn, max_objects=max_objects, check=bound):
        check()
        key, sha256, size = row["object_key"], row["object_sha256"], row["byte_count"]
        if (not isinstance(key, str) or len(key) > 2048 or key in seen
                or not _sha256(sha256) or type(size) is not int or size <= 0):
            raise RuntimeError("benchmark_clone_archive_descriptor_invalid")
        logical_bytes += size
        if logical_bytes > min(max_bytes, expected["catalog_bytes"]) or len(seen) >= expected["catalog_objects"]:
            raise RuntimeError("benchmark_clone_archive_catalog_exceeds_bound_receipt")
        descriptor = sha256, size
        path = _path(objects, key, absolute=False)
        if path.lstat().st_size != size or _sha_file(path, check=check) != descriptor:
            raise RuntimeError(f"benchmark_clone_archive_object_changed: {key}")
        seen.add(key)
        digest.update(_encode(key, descriptor))
    actual = (digest.hexdigest(), len(seen), logical_bytes)
    if actual != (expected["catalog_inventory_sha256"], expected["catalog_objects"], expected["catalog_bytes"]):
        raise RuntimeError("benchmark_clone_archive_catalog_digest_changed")
    return {"inventory_sha256": actual[0], "objects": actual[1], "bytes": actual[2]}


def verify_boundary(args):
    target, replay = _lsn(args.target_lsn), _lsn(args.expected_replay_lsn)
    if (not (0, 0) < target <= replay or not _sha256(args.expected_copy_binding_sha256)
            or not 0 < args.expected_commit_seq < 2**63 or not 0 < args.max_archive_bytes < 2**63):
        raise ValueError("benchmark_clone_invalid_scope_or_limits")
    args.minimum_replay_lsn = args.expected_replay_lsn
    _check_scope(args)
    start = monotonic()
    deadline = start + args.timeout_seconds

    def time_left():
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("benchmark_clone_verification_deadline")
        return remaining

    _require_mounts(args)
    mounts = _mount_identities(args)
    pair = _read_pair(args)
    copy, suffix = _binding(args, pair, mounts)
    objects = args.archive_root / "objects"
    engine = _readonly_engine(args, time_left)
    try:
        # The materializer locks the archive parent, and object-store writers
        # lock the objects directory itself. Exclude both during verification.
        with archive_namespace(args.archive_root, exclusive=True) as parent_check, \
                archive_namespace(objects, exclusive=True) as objects_check:
            next_check = start

            def check():
                nonlocal next_check
                remaining = time_left()
                if monotonic() >= next_check:
                    next_check = monotonic() + 0.5
                    parent_check()
                    objects_check()
                    if _mount_identities(args) != mounts:
                        raise RuntimeError("benchmark_clone_mount_changed_during_archive_verification")
                return remaining

            def bound(conn):
                _bound_statement(conn, check, args.statement_timeout_seconds)

            def endpoint(conn):
                bound(conn)
                state = _probe(conn, args, pair, target_lsn=args.target_lsn, replay_lsn=args.expected_replay_lsn)
                bound(conn)
                epoch = conn.exec_driver_sql("SELECT extract(epoch FROM pg_postmaster_start_time())::text").scalar_one()
                if not re.fullmatch(r"[1-9][0-9]*\.[0-9]{6}", epoch):
                    raise RuntimeError("benchmark_clone_postmaster_epoch_invalid")
                if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != args.expected_commit_seq:
                    raise RuntimeError("benchmark_clone_accepted_commit_changed")
                return state | {"postmaster_epoch": epoch}

            with engine.begin() as conn:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                before = endpoint(conn)
                closure = _verify_archive(conn, objects, expected=suffix, max_objects=args.max_objects,
                    max_bytes=args.max_archive_bytes, check=check, bound=lambda: bound(conn))
                if endpoint(conn) != before:
                    raise RuntimeError("benchmark_clone_endpoint_changed_during_archive_verification")
            # A fresh SQL snapshot also catches an endpoint that advanced
            # while the original transaction kept its MVCC catalog view.
            with engine.begin() as conn:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                if endpoint(conn) != before:
                    raise RuntimeError("benchmark_clone_endpoint_changed_after_archive_verification")
            _require_mounts(args)
            if (_mount_identities(args) != mounts or _read_pair(args) != pair
                    or _binding(args, pair, mounts) != (copy, suffix)):
                raise RuntimeError("benchmark_clone_final_boundary_changed")
            check()
    finally:
        engine.dispose()
    receipt = {"schema": _SCHEMA, "database_identity": pair["database_identity"], "database": args.database,
               "role": args.role, "role_oid": args.role_oid, "timeline": args.timeline, "storage_layout": pair["storage_layout"],
               "target_lsn": args.target_lsn, "replay_lsn": before["replay_lsn"], "accepted_commit_seq": args.expected_commit_seq,
               "postmaster_started_at": before["postmaster_started_at"], "postmaster_epoch": before["postmaster_epoch"],
               "device_id": args.device_id,
               "pgdata_inode": args.pgdata_inode, "tablespace_inode": args.tablespace_inode,
               "tablespace_oid": args.tablespace_oid, "socket_inode": mounts["socket"][1], "archive_inode": mounts["archive"][1],
               "archive_objects_inode": mounts["objects"][1], "archive": closure,
               "pair_receipt_sha256": args.receipt_sha256, "copy_receipt_sha256": args.copy_receipt_sha256,
               "suffix_receipt_sha256": args.suffix_receipt_sha256, "copy_binding_sha256": args.expected_copy_binding_sha256,
               "elapsed_seconds": round(monotonic() - start, 3)}
    return receipt | {"verification_sha256": archive_evidence_hash(receipt)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    _add_scope_arguments(parser)
    for name in ("copy-receipt", "suffix-receipt"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("copy-receipt-sha256", "suffix-receipt-sha256", "expected-copy-binding-sha256", "target-lsn",
                 "expected-replay-lsn"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("expected-commit-seq", "max-archive-bytes"):
        parser.add_argument(f"--{name}", type=int, required=True)
    try:
        receipt = verify_boundary(parser.parse_args(argv))
    except SQLAlchemyError as error:
        parser.exit(1, f"benchmark_clone_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(receipt, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

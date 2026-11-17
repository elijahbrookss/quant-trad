"""Verify the promoted rehearsal candidate's complete accepted archive.

Mount the private candidate archive, socket, and original clone proof read-only
in a network-disabled capsule. Set PG_DSN to the existing qt_bench_* socket
and configure MARKET_STRUCTURE_STORAGE_ROOT and QT_MARKET_DATA_EXPECTED_UUID
for the mounted archive. Stdout is the B1 candidate archive mount scope. Stderr
records what was checked. This same-device rehearsal checks no backup copy.
"""

import argparse
from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import socket
import sys
from time import monotonic

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import configured_archive_root, require_configured_archive_mount
from core.storage_targets import StorageLocation
from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import MAX_RECOVERY_OBJECTS, _archive_rows
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.automation.verify_archive_custody_subset import _object_file
from scripts.automation.verify_canonical_recovery_archive import _sha256, _sha_file
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt
from scripts.ci.verify_paused_recovery_pair import _bound_statement
from scripts.db.canonical_cold_b1_export import (
    ARCHIVE_SCOPE_VERSION, assert_candidate, validate_archive_scope, verify_private_mounts,
)
from scripts.db.canonical_cold_benchmark import _db_url
from scripts.db.canonical_cold_recovery_suffix import _encode


def _require_archive_mount(root, device_id):
    with Path("/proc/self/mountinfo").open("rb") as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise RuntimeError("candidate_archive_mountinfo_limit")
    observed = []
    for line in os.fsdecode(data).splitlines():
        fields = line.split(" ")
        path = Path(re.sub(r"\\([0-7]{3})", lambda value: chr(int(value[1], 8)), fields[4]))
        if path == root:
            observed.append((fields[2], "ro" in fields[5].split(",")))
        elif path.is_relative_to(root):
            raise ValueError("candidate_archive_unexpected_nested_mount")
    if observed != [(device_id, True)]:
        raise ValueError("candidate_archive_requires_exact_readonly_mount")


def _validate_clone_proof(proof):
    layout = proof.get("storage_layout")
    if (proof.get("schema") != "qt.benchmark_clone_boundary.v1" or proof.get("timeline") != 1
            or not isinstance(layout, dict) or layout.get("layout_version") != "market.fact_storage_tiers.v2"
            or type(proof.get("accepted_commit_seq")) is not int or not 1 <= proof["accepted_commit_seq"] < 2**63):
        raise ValueError("candidate_archive_requires_verified_clone_boundary")


def _verify_catalog(conn, objects, *, max_objects, max_bytes, check, bound):
    """Verify every accepted descriptor once. An unreferenced object is harmless."""
    seen = set()
    digest = sha256()
    logical_bytes = 0
    device = objects.stat().st_dev
    for row in _archive_rows(conn, max_objects=max_objects, check=bound):
        check()
        key, checksum, size = row["object_key"], row["object_sha256"], row["byte_count"]
        if (not isinstance(key, str) or len(key) > 2048 or len(Path(key).parts) > 64
                or key in seen or not _sha256(checksum) or type(size) is not int or size <= 0):
            raise RuntimeError("candidate_archive_invalid_or_duplicate_descriptor")
        StorageLocation("archive", key)
        seen.add(key)
        logical_bytes += size
        if len(seen) > max_objects or logical_bytes > max_bytes:
            raise RuntimeError("candidate_archive_catalog_exceeds_budget")
        found = _object_file(objects, device, key)
        if found is None:
            raise RuntimeError(f"candidate_archive_missing_selected_object: {key}")
        expected = checksum, size
        if found[1].st_size != size or _sha_file(found[0], check=check) != expected:
            raise RuntimeError(f"candidate_archive_selected_object_mismatch: {key}")
        digest.update(_encode(key, expected))
    if not seen:
        raise RuntimeError("candidate_archive_catalog_empty")
    return {"catalog_inventory_sha256": digest.hexdigest(), "objects": len(seen), "bytes": logical_bytes}


def verify(args):
    if ({name for _, name in socket.if_nameindex()} != {"lo"}
            or not 1 <= args.max_objects <= MAX_RECOVERY_OBJECTS or not 1 <= args.max_archive_bytes < 2**63
            or not 1 <= args.timeout_seconds <= 86400 or not 1 <= args.statement_timeout_seconds <= 600):
        raise ValueError("candidate_archive_invalid_limits_or_network_namespace")
    start = monotonic()
    deadline = start + args.timeout_seconds

    def remaining():
        seconds = deadline - monotonic()
        if seconds <= 0:
            raise TimeoutError("candidate_archive_verification_deadline")
        return seconds

    proof = _read_receipt(args.proof, args.proof_sha256)
    _validate_clone_proof(proof)
    url = _db_url(os.environ.get("PG_DSN"))
    if url.username != proof["role"] or url.database == proof["database"]:
        raise ValueError("candidate_archive_requires_verified_clone_role_and_database")
    root = args.archive_root
    objects = root / "objects"
    if configured_archive_root() != root:
        raise ValueError("candidate_archive_different_configured_root")
    scope = {"schema": ARCHIVE_SCOPE_VERSION, "clone_proof_sha256": args.proof_sha256,
             "database_identity": proof["database_identity"], "accepted_commit_seq": proof["accepted_commit_seq"],
             "cold_before_day": args.retire_before, "execution_hash": args.execution_hash,
             "loaded_inventory_hash": args.loaded_inventory_hash, "device_id": proof["device_id"],
             "filesystem_uuid": args.filesystem_uuid, "archive_inode": args.archive_inode,
             "archive_objects_inode": args.archive_objects_inode}
    scope["verification_sha256"] = archive_evidence_hash(scope)
    validate_archive_scope(scope, proof=proof, proof_sha256=args.proof_sha256, retire_before=args.retire_before)
    if ({args.archive_inode, args.archive_objects_inode} & {proof["archive_inode"], proof["archive_objects_inode"]}
            or args.archive_inode == args.archive_objects_inode):
        raise ValueError("candidate_archive_aliases_source_archive")

    def check_mounts():
        _require_archive_mount(root, proof["device_id"])
        _private_root(root)
        _private_root(objects)
        mount = require_configured_archive_mount(root, require_writable=False)
        if mount is None or (mount.device_id, mount.filesystem_uuid, mount.read_only) != (
                scope["device_id"], scope["filesystem_uuid"], True):
            raise ValueError("candidate_archive_filesystem_binding_changed")
        verify_private_mounts(url, root, proof, scope)

    check_mounts()
    engine = create_engine(url, poolclass=NullPool, hide_parameters=True, isolation_level="REPEATABLE READ",
        connect_args={"application_name": "qt_verify_private_candidate_archive", "passfile": "/dev/null",
                      "connect_timeout": max(2, min(5, int(remaining()))),
                      "options": "-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000"})
    try:
        with archive_namespace(root, exclusive=True) as root_held, \
                archive_namespace(objects, exclusive=True) as objects_held:
            next_mount_check = start

            def check():
                nonlocal next_mount_check
                if monotonic() >= next_mount_check:
                    root_held()
                    objects_held()
                    verify_private_mounts(url, root, proof, scope)
                    next_mount_check = monotonic() + 0.5
                return remaining()

            def bound(conn):
                _bound_statement(conn, check, args.statement_timeout_seconds)

            def verify_endpoint(conn):
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                bound(conn)
                assert_candidate(conn, url=url, proof=proof, archive_scope=scope, retire_before=args.retire_before)

            with engine.begin() as conn:
                verify_endpoint(conn)
                result = _verify_catalog(conn, objects, max_objects=args.max_objects, max_bytes=args.max_archive_bytes,
                    check=check, bound=lambda: bound(conn))
            with engine.begin() as conn:
                verify_endpoint(conn)
            check_mounts()
            if _read_receipt(args.proof, args.proof_sha256) != proof:
                raise RuntimeError("candidate_archive_clone_proof_changed")
            check()
    finally:
        engine.dispose()
    evidence = {"schema": "qt.private_candidate_archive_verification.v1", **result,
                "candidate_archive_scope_body_sha256": scope["verification_sha256"],
                "elapsed_seconds": round(monotonic() - start, 3)}
    return scope, evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "archive-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("proof-sha256", "filesystem-uuid", "execution-hash", "loaded-inventory-hash", "retire-before"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("archive-inode", "archive-objects-inode", "max-objects", "max-archive-bytes", "timeout-seconds"):
        parser.add_argument(f"--{name}", type=int, required=True)
    parser.add_argument("--statement-timeout-seconds", type=int, default=5)
    args = parser.parse_args(argv)
    try:
        if date.fromisoformat(args.retire_before).isoformat() != args.retire_before:
            raise ValueError("candidate_archive_expected_iso_retirement_day")
        scope, evidence = verify(args)
    except SQLAlchemyError as error:
        parser.exit(1, f"candidate_archive_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"candidate_archive_refused: {error}\n")
    print(json.dumps(evidence, sort_keys=True), file=sys.stderr)
    print(json.dumps(scope, sort_keys=True))


if __name__ == "__main__":
    main()

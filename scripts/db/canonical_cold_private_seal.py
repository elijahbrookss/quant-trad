"""Seal one reviewed open storage day on a verified, promoted rehearsal copy.

Use the existing private Unix-socket PG_DSN and the reviewed SHA-256 of
benchmark-clone-verified. Mount the corresponding archive objects read-only.
Run before benchmark ingest, while the copy still has the verified watermark.
This operation counts headers and payloads and updates only the day marker.
"""

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path
import stat
from time import monotonic
from types import SimpleNamespace

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_identity_schema import _partition_bound, fact_header_partition_name
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.history_policy import _read as read_saved_history_policy
from portal.backend.service.storage.recovery_copies import _snapshot_layout
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_retention import require_hot_window_elapsed
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_benchmark import _db_url


@dataclass(frozen=True)
class PrivateClone:
    url: URL
    proof: dict
    socket: Path


def _matches_inode(path, proof, key, *, expected_type):
    entry = path.lstat()
    return (expected_type(entry.st_mode) and entry.st_ino == proof[key]
            and f"{os.major(entry.st_dev)}:{os.minor(entry.st_dev)}" == proof["device_id"])


def load_private_clone(proof_path, proof_sha256, dsn):
    from scripts.automation.verify_encrypted_recovery_handoff import _bound_json

    proof = _bound_json(proof_path, proof_sha256)
    if not isinstance(proof, dict):
        raise ValueError("private_seal_invalid_clone_proof")
    body = {key: value for key, value in proof.items() if key != "verification_sha256"}
    layout = proof.get("storage_layout")
    if (proof.get("schema") != "qt.benchmark_clone_boundary.v1" or proof.get("timeline") != 1
            or not isinstance(layout, dict) or layout.get("layout_version") != "market.fact_storage_tiers.v2"
            or proof.get("verification_sha256") != archive_evidence_hash(body)):
        raise ValueError("private_seal_invalid_clone_proof")
    url = _db_url(dsn)
    endpoint = Path(url.query["host"]) / f".s.PGSQL.{url.query['port']}"
    if (url.database == proof["database"] or url.username != proof["role"]
            or not _matches_inode(endpoint, proof, "socket_inode", expected_type=stat.S_ISSOCK)):
        raise ValueError("private_seal_clone_socket_or_role_changed")
    return PrivateClone(url, proof, endpoint)


def assert_promoted_postmaster(session, clone):
    """Recheck the physical identity of the promoted private database."""
    proof = clone.proof
    server = session.execute(text("""
        SELECT system_identifier::text || '/' ||
                   (SELECT oid FROM pg_database WHERE datname=current_database()) AS identity,
               current_database() AS database, current_user AS role,
               (SELECT oid::bigint FROM pg_roles WHERE rolname=current_user) AS role_oid,
               extract(epoch FROM pg_postmaster_start_time())::text AS started,
               (SELECT timeline_id FROM pg_control_checkpoint()) AS timeline,
               pg_last_wal_replay_lsn()::text AS replay_lsn,
               current_setting('unix_socket_directories') AS sockets,
               current_setting('data_directory') AS pgdata,
               pg_tablespace_location(CAST(:tablespace_oid AS oid)) AS tablespace,
               NOT pg_is_in_recovery() AND current_user=session_user AND inet_server_addr() IS NULL
                   AND current_setting('listen_addresses')='' AND current_setting('archive_mode')='off'
                   AND current_setting('primary_conninfo')='' AND NOT EXISTS (SELECT 1 FROM pg_stat_wal_receiver)
                   AS isolated
        FROM pg_control_system()
    """), {"tablespace_oid": proof["tablespace_oid"]}).mappings().one()
    root = clone.socket.parents[2]
    expected = {"identity": proof["database_identity"], "database": clone.url.database,
                "role": proof["role"], "role_oid": proof["role_oid"], "started": proof["postmaster_epoch"],
                "timeline": 2, "replay_lsn": proof["replay_lsn"], "sockets": str(clone.socket.parent),
                "pgdata": str(root / "pgdata"), "tablespace": str(root / "history"), "isolated": True}
    if server != expected or not _matches_inode(clone.socket, proof, "socket_inode", expected_type=stat.S_ISSOCK):
        raise RuntimeError("private_seal_promoted_clone_changed")


def assert_promoted_clone(session, clone):
    """Recheck the promoted copy still contains its original layout and commit."""
    assert_promoted_postmaster(session, clone)
    proof = clone.proof
    if _snapshot_layout(session) != proof["storage_layout"]:
        raise RuntimeError("private_seal_promoted_clone_changed")
    if PostgresMarketDataRepository._current_commit_seq_with_session(session) != proof["accepted_commit_seq"]:
        raise RuntimeError("private_seal_clone_watermark_changed")


def _locked_children(session, day, header_oid, payload_oid):
    children = ((fact_header_partition_name(day), "market.fact_versions", header_oid),
                (fact_partition_name(day), "market.fact_hot_payloads", payload_oid))
    for name, parent, oid in children:
        relation = f"market.{name}"
        # The date-generated identifier cannot contain SQL syntax. This lock
        # also excludes concurrent detach until the marker commits.
        session.execute(text(f"LOCK TABLE ONLY {relation} IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"))
        observed = session.execute(text("""
            SELECT c.oid::bigint, pg_get_expr(c.relpartbound,c.oid)
            FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
            WHERE c.oid=to_regclass(:relation) AND i.inhparent=to_regclass(:parent)
                AND c.relkind='r' AND NOT i.inhdetachpending
        """), {"relation": relation, "parent": parent}).one_or_none()
        if observed != (oid, _partition_bound(day)):
            raise RuntimeError(f"private_seal_partition_identity_changed: {relation}")


def seal_one(clone, *, proof_sha256, day, expected_state, header_oid, payload_oid,
             archive_object_root, max_seconds, count_statement_timeout_ms):
    if (type(day) is not date or expected_state != "open"
            or type(max_seconds) is not int or not 1 <= max_seconds <= 3600
            or type(count_statement_timeout_ms) is not int or not 1 <= count_statement_timeout_ms <= 60_000
            or any(type(oid) is not int or not 0 < oid < 2**32 for oid in (header_oid, payload_oid))):
        raise ValueError("private_seal_invalid_day_or_limits")
    obj_root = Path(archive_object_root)
    if (obj_root.resolve(strict=True) != obj_root or not os.statvfs(obj_root).f_flag & os.ST_RDONLY
            or not _matches_inode(obj_root, clone.proof, "archive_objects_inode", expected_type=stat.S_ISDIR)):
        raise ValueError("private_seal_requires_verified_readonly_archive")
    object_store = FilesystemRawArchiveObjectStore(obj_root, writable=False)
    deadline = monotonic() + max_seconds
    engine = create_engine(clone.url, poolclass=NullPool, hide_parameters=True, isolation_level="READ COMMITTED",
        connect_args={"connect_timeout": max(2, min(max_seconds, 5)), "passfile": "/dev/null",
                      "application_name": "qt_private_cohort_seal",
                      "options": "-c default_transaction_read_only=off -c idle_in_transaction_session_timeout=15000"})
    sessions = sessionmaker(bind=engine)
    policy_evidence = None

    def remaining():
        seconds = deadline - monotonic()
        if seconds <= 0:
            raise TimeoutError("private_seal_deadline")
        return seconds

    @contextmanager
    def own_session():
        # Do not run application startup DDL. The verified clone has the
        # original schema and this invocation owns exactly one transaction.
        with sessions.begin() as session:
            session.execute(text("SELECT set_config('statement_timeout', :ms, true)"),
                            {"ms": str(max(1, min(5000, int(1000 * remaining()))))})
            if not session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                                  {"name": _LIFECYCLE_LOCK_NAME}):
                raise RuntimeError("private_seal_publication_busy")
            assert_promoted_clone(session, clone)
            yield session
            remaining()

    def guard(session, partition):
        nonlocal policy_evidence
        if (partition["state"] != expected_state or partition["expected_rows"] is not None
                or partition["sealed_at"] is not None):
            raise RuntimeError("private_seal_partition_state_changed")
        _locked_children(session, day, header_oid, payload_oid)
        saved, policy_evidence = read_saved_history_policy(session)
        policy = CanonicalFactRetentionPolicy() if saved is None else CanonicalFactRetentionPolicy(hot_days=saved.recent_days)
        require_hot_window_elapsed(session, partition, policy=policy)

    try:
        archive = PostgresCanonicalFactArchiveRepository(database=SimpleNamespace(session=own_session),
            object_store=object_store, temporary_directory=obj_root.parent, partition_guard=guard,
            statement_timeout_ms=max(1, min(5000, int(1000 * remaining()))), check_budget=remaining)
        result = archive.seal_partition(day, count_statement_timeout_ms=count_statement_timeout_ms,
                                        remaining_run_seconds=remaining)
    finally:
        engine.dispose()
    return {"schema": "qt.private_rehearsal_cohort_seal.v1", "proof_sha256": proof_sha256,
            "database": clone.url.database, "database_identity": clone.proof["database_identity"],
            "accepted_commit_seq": clone.proof["accepted_commit_seq"], "policy": policy_evidence,
            "storage_day": day.isoformat(), "previous_state": expected_state, "state": result["state"],
            "header_oid": header_oid, "payload_oid": payload_oid, "expected_rows": result["expected_rows"],
            "sealed_at": result["sealed_at"].isoformat(), "source_payload_bytes": result["source_bytes"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--proof-sha256", required=True)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--expected-state", choices=("open",), required=True)
    parser.add_argument("--archive-object-root", type=Path, required=True)
    for name in ("header-oid", "payload-oid", "max-seconds", "count-statement-timeout-ms"):
        parser.add_argument(f"--{name}", type=int, required=True)
    fields = vars(parser.parse_args())
    proof_path = fields.pop("proof")
    try:
        clone = load_private_clone(proof_path, fields["proof_sha256"], os.environ.get("PG_DSN"))
        output = seal_one(clone, **fields)
    except SQLAlchemyError as error:
        parser.exit(1, f"private_seal_sql_failed: {getattr(error.orig, 'pgcode', None) or type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"private_seal_refused: {error}\n")
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()

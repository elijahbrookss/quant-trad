"""Retain a verified, private original-layout PostgreSQL pause for rollback reads."""
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt
from scripts.ci.verify_paused_recovery_pair import (
    _bound_statement, _check_scope, _lsn, _mount_paths, _probe, _readonly_engine,
)
from scripts.db.canonical_cold_migration import PostgresMigrationSource


@dataclass(frozen=True)
class PausedRollbackDonor:
    connection: object
    identity: dict
    accepted_commit_seq: int
    archive_objects: Path
    _transaction: object
    _args: SimpleNamespace
    _proof: dict
    _mounts: dict
    _paused: dict
    _deadline: float

    def assert_snapshot(self, conn, expected):
        if (conn is not self.connection or not conn.in_transaction() or conn.get_transaction() is not self._transaction
                or expected != self.identity):
            raise ValueError("paused_rollback_donor_connection_or_identity_changed")
        args, proof = self._args, self._proof
        url = conn.engine.url
        if (url.host is not None or url.database != args.database or url.username != args.role
                or url.query.get("host") != str(args.socket) or url.query.get("port") != str(args.port)):
            raise ValueError("paused_rollback_donor_connection_or_identity_changed")
        remaining = max(1, int(1000 * min(_remaining(self._deadline), args.statement_timeout_seconds)))
        conn.execute(text("""SELECT set_config('statement_timeout',
            LEAST(:milliseconds, COALESCE(NULLIF(setting::integer,0), :milliseconds))::text, true)
            FROM pg_settings WHERE name='statement_timeout'"""), {"milliseconds": remaining})
        _archive_identity(self.archive_objects.parent, proof)
        if (_mount_paths(args) != self._mounts
                or _probe(conn, args, proof, target_lsn=proof["target_lsn"], replay_lsn=proof["replay_lsn"]) != self._paused):
            raise RuntimeError("paused_rollback_donor_snapshot_changed")


def _remaining(deadline):
    seconds = deadline - monotonic()
    if seconds <= 0:
        raise TimeoutError("paused_rollback_donor_deadline")
    return seconds


def _archive_identity(root, proof):
    paths = root, root / "objects"
    identities = tuple(_private_root(path)[1] for path in paths)
    if (tuple(inode for _, inode in identities) != (proof["archive_inode"], proof["archive_objects_inode"])
            or any(f"{os.major(device)}:{os.minor(device)}" != proof["device_id"] for device, _ in identities)
            or any(not os.statvfs(path).f_flag & os.ST_RDONLY for path in paths)):
        raise ValueError("paused_rollback_donor_archive_mount_changed")
    return identities


def _binding(clone, copy, suffix, *, copy_hash, suffix_hash):
    fields = ("database_identity", "storage_layout", "device_id", "timeline", "target_lsn", "replay_lsn",
              "accepted_commit_seq", "pair_receipt_sha256")
    if (clone["schema"] != "qt.benchmark_clone_boundary.v1"
            or copy["schema"] != "qt.stopped_rehearsal_database_copy.v1"
            or suffix["schema"] != "qt.paused_archive_suffix.v1"
            or clone["copy_receipt_sha256"] != copy_hash or clone["suffix_receipt_sha256"] != suffix_hash
            or clone["copy_binding_sha256"] != copy["binding_sha256"]
            or any(clone[key] != suffix[key] for key in fields)
            or clone["timeline"] != 1 or copy["timeline"] != 1 or copy["device_id"] != clone["device_id"]
            or clone["storage_layout"]["layout_version"] != "market.fact_storage_tiers.v2"
            or not (0, 0) < _lsn(clone["target_lsn"]) <= _lsn(clone["replay_lsn"])
            or type(clone["accepted_commit_seq"]) is not int or clone["accepted_commit_seq"] < 1
            or clone["database_identity"].split("/", 1)[0] != copy["system_identifier"]
            or clone["tablespace_oid"] != copy["tablespace_oid"]
            or suffix["materialize_baseline"] is not True
            or clone["archive_inode"] != suffix["suffix_inode"]):
        raise ValueError("paused_rollback_donor_reviewed_boundary_changed")
    original = copy["source"]
    if (not _sha256(original["control"]["sha256"]) or not _sha256(original["metadata_sha256"])
            or (original["pgdata"], original["pgdata_inode"]) != (suffix["pgdata"], suffix["pgdata_inode"])
            or {original["pgdata_inode"], original["history_inode"]} & {clone["pgdata_inode"], clone["tablespace_inode"]}):
        raise ValueError("paused_rollback_donor_original_and_candidate_overlap")
    catalog = clone["archive"]
    observed = catalog["inventory_sha256"], catalog["objects"], catalog["bytes"]
    if (observed != (suffix["materialized_inventory_sha256"], suffix["materialized_objects"], suffix["materialized_bytes"])
            or observed != (suffix["catalog_inventory_sha256"], suffix["catalog_objects"], suffix["catalog_bytes"])):
        raise ValueError("paused_rollback_donor_archive_boundary_changed")
    return original


@contextmanager
def open_paused_donor(*, clone_proof, clone_proof_sha256, copy_receipt, copy_receipt_sha256,
                      suffix_receipt, suffix_receipt_sha256, archive_root, tablespace_name,
                      port, timeout_seconds, statement_timeout_seconds=120):
    """Open the original-name donor only through its private, mounted Unix socket.

    The stopped original must already be restarted into the exact reviewed
    recovery pause. This client never starts, resumes, promotes, or writes it.
    """
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS:
        raise ValueError("paused_rollback_donor_invalid_deadline")
    clone = _read_receipt(Path(clone_proof), clone_proof_sha256)
    copy = _read_receipt(Path(copy_receipt), copy_receipt_sha256)
    suffix = _read_receipt(Path(suffix_receipt), suffix_receipt_sha256)
    original = _binding(clone, copy, suffix, copy_hash=copy_receipt_sha256, suffix_hash=suffix_receipt_sha256)
    pgdata, history = Path(original["pgdata"]), Path(original["history"])
    root = pgdata.parent
    args = SimpleNamespace(attempt_root=root, pgdata=pgdata, tablespace=history, socket=root / "control" / "socket",
        archive_root=root / "archive", minimum_replay_lsn=clone["replay_lsn"],
        database=clone["database"], role=clone["role"], role_oid=clone["role_oid"], timeline=clone["timeline"],
        pgdata_inode=original["pgdata_inode"], tablespace_inode=original["history_inode"], device_id=clone["device_id"],
        tablespace_oid=clone["tablespace_oid"], tablespace_name=tablespace_name, port=port, max_objects=1,
        timeout_seconds=min(timeout_seconds, 86400), statement_timeout_seconds=statement_timeout_seconds)
    _check_scope(args)
    archive_root = Path(archive_root)
    archive_objects = archive_root / "objects"
    if archive_root.is_relative_to(root) or root.is_relative_to(archive_root):
        raise ValueError("paused_rollback_donor_archive_overlaps_workspace")

    mounts = _mount_paths(args)
    _archive_identity(archive_root, clone)
    deadline = monotonic() + timeout_seconds
    def check():
        return _remaining(deadline)
    engine = _readonly_engine(args, check)
    try:
        with archive_namespace(archive_root, exclusive=True), archive_namespace(archive_objects, exclusive=True), \
                engine.connect() as conn, conn.begin() as transaction:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            source = PostgresMigrationSource(engine, expected_system_identifier=copy["system_identifier"])
            _bound_statement(conn, check, statement_timeout_seconds)
            paused = _probe(conn, args, clone, target_lsn=clone["target_lsn"], replay_lsn=clone["replay_lsn"])
            identity = source.identity(conn)
            accepted = PostgresMarketDataRepository._current_commit_seq_with_session(conn)
            if accepted != clone["accepted_commit_seq"]:
                raise RuntimeError("paused_rollback_donor_accepted_boundary_changed")
            donor = PausedRollbackDonor(conn, identity, accepted, archive_objects, transaction, args, clone, mounts, paused, deadline)
            donor.assert_snapshot(conn, identity)
            yield donor
            donor.assert_snapshot(conn, identity)
            transaction.rollback()
    finally:
        engine.dispose()

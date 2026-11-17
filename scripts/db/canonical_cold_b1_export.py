"""Export the exact saved B1 selection from the private canonical candidate."""

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
import socket
import sys

from core.execution_control import ExecutionControl, controlled_execution
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt

if __package__:
    from . import canonical_cold_benchmark as bench
    from .canonical_cold_workload_plan import B1, B1_HASH
else:
    import canonical_cold_benchmark as bench
    from canonical_cold_workload_plan import B1, B1_HASH


VERSION = "qt.b1_candidate_input_export.v1"
ARCHIVE_SCOPE_VERSION = "qt.benchmark_candidate_archive_scope.v1"
SEALED_EXPORT_SHA256 = "662767e03844bc58602b192ac9f64f32a32ff975662ef522716cdb240950ed14"
MAX_BYTES = 32 * 1024**2


@dataclass(frozen=True)
class Selection:
    dataset_id: str
    dataset_hash: str
    commit: int
    series: int
    source: str
    start: datetime
    end: datetime
    rows: int
    manifest_sha256: str
    records_sha256: str


B1_SELECTION = Selection(
    dataset_id=B1, dataset_hash=B1_HASH, commit=207402044, series=2361,
    source="10fd0310823dc4e9526c3f12d3baf7a5f499ee17c5cc1c489bd37dcacf115293",
    start=bench._timestamp("2021-12-30T23:45:00Z"), end=bench._timestamp("2022-02-01T00:00:00Z"), rows=3073,
    manifest_sha256="618eccf880567c5d8e043253457e5e55acea2249bd7ab521dcdd7336d52527df",
    records_sha256="42f61cfcf5881fe441a53801fd5795532ebdb863da859b3225bfc0e85ef06f84",
)


def _material(value):
    # Match the original export's datetime string representation in the manifest.
    return json.loads(json.dumps(value, default=str, allow_nan=False))


def capture(repo, selection=B1_SELECTION):
    """Read the selected frozen candle API with its normal storage lease."""
    control = ExecutionControl()
    control.limit(seconds=30, input_rows=3500, input_bytes=MAX_BYTES, archive_read_bytes=MAX_BYTES)
    with controlled_execution(control):
        dataset = repo.get_dataset(selection.dataset_id)
        if (dataset.dataset_id, dataset.dataset_hash, dataset.max_commit_seq) != (
            selection.dataset_id, selection.dataset_hash, selection.commit,
        ):
            raise ValueError("B1 candidate frozen dataset identity changed")
        members = [row for row in dataset.series if row["series_id"] == selection.series]
        if len(members) != 1:
            raise ValueError("B1 candidate frozen series manifest changed")
        manifest = _material(dict(members[0]))
        if bench._digest(manifest) != selection.manifest_sha256:
            raise ValueError("B1 candidate frozen series manifest changed")
        rows = repo.read_dataset_series(dataset_id=selection.dataset_id, series_id=selection.series,
            start=selection.start, end=selection.end, known_at_lte=selection.end,
            source_identity_keys=(selection.source,), causal_at_interval_close=True)
        if len(rows) != selection.rows:
            raise ValueError("B1 candidate frozen selected candle count changed")
        records = []
        for ordinal, row in enumerate(rows):
            opened = selection.start + timedelta(minutes=15 * ordinal)
            closing = opened + timedelta(minutes=15)
            if (row.series_id != selection.series or row.source_identity_key != selection.source
                    or not 0 < row.market_commit_seq <= selection.commit or row.fact.open_time != opened
                    or row.fact.close_time != closing or closing > selection.end
                    or row.fact.known_at is None or row.fact.known_at > closing):
                raise ValueError(f"B1 candidate candle selection changed at ordinal {ordinal}")
            records.append({"series_id": row.series_id, "revision": row.revision,
                "market_commit_seq": row.market_commit_seq, "source_identity_key": row.source_identity_key,
                "canonical_material_hash": row.canonical_material_hash, "fact": row.fact.to_dict()})
        records = _material(records)
        if bench._digest(records) != selection.records_sha256:
            raise ValueError("B1 candidate ordered candle material or clock changed")
    return {"dataset_id": dataset.dataset_id, "dataset_hash": dataset.dataset_hash,
        "max_commit_seq": dataset.max_commit_seq, "series_manifest": manifest,
        "start_inclusive": selection.start.isoformat(), "end_exclusive": selection.end.isoformat(),
        "records": records, "resource_metrics": control.snapshot(), "provider_access": False,
        "read_only": True, "new_check": False}


def validate_archive_scope(scope, *, proof, proof_sha256, retire_before):
    if (scope.keys() != {"schema", "clone_proof_sha256", "database_identity", "accepted_commit_seq", "cold_before_day",
                        "execution_hash", "loaded_inventory_hash", "device_id", "filesystem_uuid", "archive_inode",
                        "archive_objects_inode", "verification_sha256"}
            or (scope["schema"], scope["clone_proof_sha256"], scope["database_identity"], scope["accepted_commit_seq"],
                scope["cold_before_day"]) != (ARCHIVE_SCOPE_VERSION, proof_sha256, proof["database_identity"],
                                              proof["accepted_commit_seq"], retire_before)
            or not all(bench._matches(scope[field], r"[0-9a-f]{64}") for field in ("execution_hash", "loaded_inventory_hash"))
            or not bench._matches(scope["device_id"], r"[0-9]+:[0-9]+")
            or not bench._matches(scope["filesystem_uuid"], r"[a-zA-Z0-9-]{4,128}")
            or not all(bench._bounded_int(scope[field], 1, 2**64 - 1) for field in ("archive_inode", "archive_objects_inode"))):
        raise ValueError("B1 candidate archive scope does not match the verified clone boundary")


def verify_private_mounts(url, archive, proof, archive_scope):
    """Match the verified clone socket and the separate candidate archive mount."""
    directory = Path(url.query["host"])
    endpoint = directory / f".s.PGSQL.{url.query['port']}"
    objects = archive / "objects"
    for path in (directory, archive, objects):
        if path != path.resolve(strict=True) or not path.is_dir() or not os.statvfs(path).f_flag & os.ST_RDONLY:
            raise ValueError("B1 export needs exact read-only socket and archive mounts")
    for path, field, scope in ((endpoint, "socket_inode", proof), (archive, "archive_inode", archive_scope),
                               (objects, "archive_objects_inode", archive_scope)):
        info = path.lstat()
        if info.st_ino != scope[field] or f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}" != scope["device_id"]:
            raise ValueError(f"B1 verified clone {field} or device changed")


def verify_candidate(engine, *, url, proof, archive_scope, retire_before):
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        assert_candidate(conn, url=url, proof=proof, archive_scope=archive_scope, retire_before=retire_before)


def assert_candidate(conn, *, url, proof, archive_scope, retire_before):
    """Check the candidate in the caller's read-only snapshot."""
    from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository

    observed = conn.exec_driver_sql("""
            SELECT system_identifier::text || '/' || (SELECT oid FROM pg_database WHERE datname=current_database()),
                   current_database(), session_user::text, (SELECT oid FROM pg_roles WHERE rolname=session_user),
                   extract(epoch FROM pg_postmaster_start_time())::text, pg_last_wal_replay_lsn()::text,
                   (SELECT timeline_id FROM pg_control_checkpoint()), current_setting('server_version_num'),
                   (SELECT extversion FROM pg_extension WHERE extname='timescaledb'),
                   current_setting('transaction_read_only')='on' AND inet_server_addr() IS NULL
                       AND inet_client_addr() IS NULL AND current_setting('listen_addresses')=''
                       AND current_setting('timescaledb.max_background_workers')='0'
                       AND current_setting('timescaledb.restoring')='off' AND NOT pg_is_in_recovery()
                       AND NOT EXISTS (SELECT 1 FROM pg_stat_activity
                           WHERE starts_with(backend_type, 'TimescaleDB Background Worker '))
            FROM pg_control_system()
    """).one()
    if observed != (proof["database_identity"], url.database, proof["role"], proof["role_oid"],
                     proof["postmaster_epoch"], proof["replay_lsn"], 2, "150006", "2.14.2", True):
        raise ValueError("B1 native promoted clone identity or idle worker profile changed")
    state = conn.exec_driver_sql("""
            SELECT layout_version, evidence->>'cold_before_day', evidence->>'execution_hash',
                   evidence->>'loaded_inventory_hash', evidence->>'source_commit_seq'
            FROM market.fact_storage_state
            WHERE state='ready' AND completed_at IS NOT NULL
    """).one()
    if state != (bench.LAYOUTS["candidate"], retire_before, archive_scope["execution_hash"],
                 archive_scope["loaded_inventory_hash"], str(proof["accepted_commit_seq"])):
        raise ValueError("B1 candidate activation certificate differs from mounted archive evidence")
    if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != proof["accepted_commit_seq"]:
        raise ValueError("B1 candidate accepted watermark changed")


def run(args):
    from core.settings import get_settings
    from core.storage_mounts import configured_archive_root, require_configured_archive_mount
    from portal.backend.db.session import db
    from portal.backend.service.storage.repos import market_data
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    if {name for _, name in socket.if_nameindex()} != {"lo"}:
        raise ValueError("B1 export requires a network-disabled namespace")
    if not bench._matches(args.revision, r"[0-9a-f]{40}") or not bench._matches(args.runtime_digest, r"sha256:[0-9a-f]{64}"):
        raise ValueError("B1 export requires exact application revision and runtime digest")
    url = bench._db_url(os.environ.get("PG_DSN"))
    proof = _read_receipt(args.proof, args.proof_sha256)
    if (proof.get("schema") != "qt.benchmark_clone_boundary.v1" or proof.get("timeline") != 1
            or not bench._bounded_int(proof.get("accepted_commit_seq"), B1_SELECTION.commit, 2**63 - 1)):
        raise ValueError("B1 export requires the verified pre-promotion clone boundary")
    archive_scope = _read_receipt(args.candidate_archive_proof, args.candidate_archive_proof_sha256)
    validate_archive_scope(archive_scope, proof=proof, proof_sha256=args.proof_sha256, retire_before=args.retire_before)
    if (db._engine is not None or get_settings().storage.history_cache_bytes
            or configured_archive_root() != args.archive_root):
        raise ValueError("B1 export requires an uninitialized standalone runtime and exact archive root with no writable cache")
    mount = require_configured_archive_mount(args.archive_root, require_writable=False)
    if (mount is None or not mount.read_only or (mount.device_id, mount.filesystem_uuid)
            != (archive_scope["device_id"], archive_scope["filesystem_uuid"])):
        raise ValueError("B1 export requires the pinned read-only archive filesystem UUID")
    revision = bench._code_revision(market_data, args.revision)
    verify_private_mounts(url, args.archive_root, proof, archive_scope)
    engine = create_engine(url, connect_args={"application_name": "qt_offline_b1_candidate_export", "connect_timeout": 5,
        "options": "-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000 -c timezone=UTC"})
    endpoint = {"url": url, "proof": proof, "archive_scope": archive_scope, "retire_before": args.retire_before}
    try:
        verify_candidate(engine, **endpoint)
        db._engine = engine
        db._session_factory = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
        db._available = True
        export = capture(market_data.market_data_repo)
        verify_candidate(engine, **endpoint)
        verify_private_mounts(url, args.archive_root, proof, archive_scope)
        if (_read_receipt(args.proof, args.proof_sha256) != proof
                or _read_receipt(args.candidate_archive_proof, args.candidate_archive_proof_sha256) != archive_scope):
            raise ValueError("B1 verified clone proof changed during export")
    finally:
        if db._engine is engine:
            db._engine, db._session_factory, db._available = None, None, False
        engine.dispose()
    encoded = (bench._json(export) + "\n").encode()
    if len(encoded) > 8 * 1024**2:
        raise ValueError("B1 candidate export exceeds its input envelope bound")
    return encoded, {"schema": VERSION, "candidate_revision": revision, "runtime_digest": args.runtime_digest,
        "proof_sha256": args.proof_sha256, "database_identity": proof["database_identity"],
        "candidate_archive_proof_sha256": args.candidate_archive_proof_sha256,
        "activation_execution_hash": archive_scope["execution_hash"], "loaded_inventory_hash": archive_scope["loaded_inventory_hash"],
        "accepted_commit_seq": proof["accepted_commit_seq"], "retire_before": args.retire_before,
        "archive_device": mount.device_id, "archive_uuid": mount.filesystem_uuid, "records": B1_SELECTION.rows,
        "sealed_input_sha256": SEALED_EXPORT_SHA256, "candidate_export_sha256": sha256(encoded).hexdigest(),
        "series_manifest_sha256": B1_SELECTION.manifest_sha256, "ordered_records_sha256": B1_SELECTION.records_sha256,
        "resource_metrics": export["resource_metrics"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--candidate-archive-proof", type=Path, required=True)
    for name in ("proof-sha256", "candidate-archive-proof-sha256", "revision", "runtime-digest", "retire-before"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args()
    if date.fromisoformat(args.retire_before).isoformat() != args.retire_before:
        parser.error("--retire-before requires a canonical YYYY-MM-DD date")
    os.environ["QT_DISABLE_DOTENV"] = "1"
    encoded, receipt = run(args)
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()
    print(bench._json(receipt), file=sys.stderr)


if __name__ == "__main__":
    main()

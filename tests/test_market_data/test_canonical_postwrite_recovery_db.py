"""Restore the activated cold layout at a real committed provider-write boundary."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
from time import monotonic, sleep

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from core.storage_targets import StorageTarget
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, read_raw_archive_parquet
from market_data.archive_namespace import archive_namespace
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from market_data.structure import RawStreamRecord
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.db.session import Database
from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, IncrementalRecoveryConfig, prepare_encrypted_repository,
)
from portal.backend.service.storage.recovery_copies import _identity, _snapshot_layout
from portal.backend.service.storage.repos import market_data, market_lifecycle, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.automation.canonical_cold_collector_checks import RECEIPT_VERSION as COLLECTOR_RECEIPT_VERSION
from scripts.automation.canonical_cold_recovery_checks import PLAN_VERSION, bind_approved_recovery_checks
from scripts.automation.canonical_cold_named_recovery import (
    RecoverySitePolicy, NamedRecoverySite, _opened, bind_named_recovery_source, prepare_named_receiver, recover_named_site,
)
from scripts.automation import canonical_cold_recovery_preflight as preflight
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, stage_handoff, verify_staged_handoff
from scripts.automation.storage_host_boundary import load_receipt, save_receipt
from scripts.automation.verify_encrypted_recovery_handoff import _wal_range, verify_handoff
from scripts.db.canonical_cold_database_fence import _database, close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_identity_rebuild import PREVIOUS
from scripts.db.canonical_cold_raw_index import inspect_raw_mapping_index
from scripts.db.canonical_cold_recovery_anchor import create_original_recovery_anchor
from scripts.db.canonical_cold_recovery_archive_transport import stage_encrypted_archive_suffix, verify_named_archive_closure
from scripts.db.canonical_cold_recovery_pending_spool import (
    PendingSpoolScope, measure_pending_spool, measure_pending_spool_before_anchor, stage_pending_spool, verify_pending_spool,
)
from scripts.db.canonical_cold_recovery_wal_transfer import stage_final_wal, verify_final_wal
from scripts.db.canonical_cold_recovery_suffix import (
    capture_final_archive_suffix, prepare_archive_baseline, verify_final_archive_suffix,
)
from tests.test_market_data import test_canonical_cold_activate_db as activation_fixture
from tests.test_market_data.exact_deployed_reader import check_deployed_source_reader
from tests.test_market_data.test_canonical_recovery_spool_db import (
    _admitted_rows, _define_trade_stream, _native_recover, _seal_trade,
)
from tests.test_market_data.test_canonical_cold_provider_boundary_db import _definition, _messages
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement, storage


pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_INCREMENTAL_APPLICATION_TEST") != "1",
                       reason="requires owned incremental recovery topology"),
]


def test_postwrite_pair_restores_exact_canonical_row_and_raw_archive(storage, tmp_path, monkeypatch):
    _exercise_postwrite_pair(storage, tmp_path, monkeypatch)


def _exercise_postwrite_pair(storage, tmp_path, monkeypatch, *, raw_scope=None, restore_database=None):
    assert os.getuid() == 70 and os.getenv("QT_STORAGE_DEMO") == "1"
    engine = storage.database._engine
    assert engine.url.host == "timescaledb" and engine.url.database.startswith("qt_migration_")
    activation_fixture._exercise_exact_activation(
        storage, tmp_path, monkeypatch, owner_path=raw_scope is not None, native_raw_index=raw_scope,
    )
    monkeypatch.setattr(market_data, "db", storage.database)
    root = tmp_path
    objects = FilesystemRawArchiveObjectStore(root / "objects")
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository",
                        market_data.canonical_fact_storage_repository)
    structures, claim, series = _define_trade_stream(storage, monkeypatch)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(root))
    segment, record = _seal_trade(root, claim, trade_id="post-activation-recovery")
    _native_recover(structures, root)
    assert segment.ack_path.is_file() and not segment.sealed_path.exists()
    expected = _admitted_rows(storage.database, series)
    assert len(expected[0]) == len(expected[1]) == 1
    start = record.received_at - timedelta(seconds=10)
    end = record.received_at + timedelta(seconds=10)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series, start, end)])
    current = storage.repo.read_facts(series_id=series, start=start, end=end)
    expected_frozen = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
    assert len(current) == len(expected_frozen) == 1
    with engine.connect() as conn:
        identity = _identity(conn)[0]
        assert _snapshot_layout(conn)["layout_version"] == COLD_HISTORY_LAYOUT_VERSION
        assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
        archive_keys = set(conn.execute(text("SELECT object_key FROM market.cold_fact_pages")).scalars())
        assert archive_keys
        older_datasets = conn.execute(text("""
            SELECT dataset_id,series_id FROM market.dataset_series WHERE series_id<>:series
        """), {"series": series}).all()
        raw_index = (inspect_raw_mapping_index(conn, expected_database=identity, target_id=raw_scope["target_id"])
                     if raw_scope is not None else None)
        if raw_index is not None:
            assert raw_index["member"]["tablespace_oid"] == raw_scope["tablespace_oid"]
            assert conn.scalar(text("SELECT to_regclass(:previous) IS NULL"), {"previous": PREVIOUS})
    assert older_datasets
    older_frozen = {
        (dataset, older_series): storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=older_series)
        for dataset, older_series in older_datasets
    }
    assert all(older_frozen.values())

    worker = _prepare_native_repository(engine, identity, tmp_path, monkeypatch)
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        selected = worker.create(snapshot, objects=objects, keep_copies=1)
    assert [point for _, _, point in worker.completed()] == [selected]
    assert selected["storage_layout"]["layout_version"] == COLD_HISTORY_LAYOUT_VERSION
    assert selected["archive_objects"] >= len(archive_keys) + 1
    # This committed write is after the pinned database backup endpoint.
    _later_trade(structures, root, claim, record)
    assert len(_admitted_rows(storage.database, series)[0]) == 2

    restore_root, copied, receipt, _ = _copy_repository(worker, selected)
    # Hold the original plaintext archive objects out of the runtime reader's path.
    objects.root.rename(root / "original-objects")
    (_restore_database if restore_database is None else restore_database)(worker, copied, selected, "--type=immediate")
    restored_archives = tmp_path / "restored-verified-archive"
    restored_archives.mkdir(mode=0o700)
    handoff = verify_handoff(repository=copied, receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),
        archive_restore=restored_archives, pgbackrest=worker.pgbackrest, restic=worker.restic,
        database_key=worker.config.database_key_path, archive_key=worker.config.archive_key_path,
        timeout_seconds=60, max_archive_bytes=64 * 1024**2, max_archive_objects=100,
        wal_segment_bytes=16 * 1024**2, max_wal_segments=32)
    assert handoff["verified_archive_objects"] == selected["archive_objects"]
    assert handoff["archive_snapshot"] == selected["archive_snapshot"]
    restored_store = FilesystemRawArchiveObjectStore(restored_archives / str(objects.root).lstrip("/"), writable=False)
    for key in archive_keys:
        assert restored_store.local_path(key).is_file()
    (restore_root / "start").write_text("committed cold recovery point\n")
    url = engine.url.set(host="restored")
    _wait_for_restored_cluster(url, expected_identity=identity)

    restored = Database(url.render_as_string(hide_password=False))
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        for module in (market_data, market_structure, market_lifecycle):
            monkeypatch.setattr(module, "db", restored)
        reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: restored_store,
                                                         read_cache_factory=lambda: None)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
        monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", reader)
        monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(restored_store.root.parent))
        repository = market_data.PostgresMarketDataRepository()
        assert _admitted_rows(restored, series) == expected
        assert repository.read_facts(series_id=series, start=start, end=end) == current
        assert repository.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series) == expected_frozen
        for (dataset, older_series), records in older_frozen.items():
            assert repository.read_dataset_fact_revisions(dataset_id=dataset, series_id=older_series) == records
        with restored.session() as session:
            assert _snapshot_layout(session) == selected["storage_layout"]
            assert session.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
            if raw_index is not None:
                assert inspect_raw_mapping_index(session, expected_database=identity,
                                                 target_id=raw_scope["target_id"]) == raw_index
                assert session.scalar(text("SELECT to_regclass(:previous) IS NULL"), {"previous": PREVIOUS})
        raw_id, _, key, digest = expected[1][0]
        path = restored_store.local_path(key)
        assert raw_id == record.raw_record_id
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        assert read_raw_archive_parquet(path) == [record]
    finally:
        restored._reset_engine()


def test_named_wal_anchor_restores_original_layout_at_fenced_endpoint(storage, tmp_path, monkeypatch):
    assert os.getuid() == 70 and os.getenv("QT_STORAGE_DEMO") == "1"
    engine = storage.database._engine
    assert engine.url.host == "timescaledb" and engine.url.database.startswith("qt_migration_")
    legacy_day, legacy_end = storage.today - timedelta(days=2), storage.today - timedelta(days=1)
    _placement(monkeypatch, legacy_day)
    _ingest(storage)
    with engine.begin() as conn:
        identity = _identity(conn)[0]
        system_identifier, database_oid = identity.split("/")
        activation_fixture._attach_legacy_child(conn, legacy_day, legacy_end)
    objects = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    structures, claim, series = _define_trade_stream(storage, monkeypatch)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    worker = _prepare_native_repository(engine, identity, tmp_path, monkeypatch)
    _, baseline_raw = _seal_trade(tmp_path, claim, trade_id="original-before-baseline")
    _native_recover(structures, tmp_path)
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        baseline = worker.create(snapshot, objects=objects, keep_copies=2)
    assert baseline["storage_layout"]["layout_version"] == FACT_STORAGE_LAYOUT_VERSION
    assert baseline["archive_objects"] == 1
    _, baseline_refs = _admitted_rows(storage.database, series)
    assert baseline_refs[0].raw_record_id == baseline_raw.raw_record_id
    original_receipt = worker.root / baseline["name"] / "complete.json"
    receipt_hash = hashlib.sha256(original_receipt.read_bytes()).hexdigest()
    restore_root, copied, receipt, transfer = _copy_repository(worker, baseline)
    prestaged = tmp_path / "baseline-restic"
    prestaged.mkdir(mode=0o700)
    handoff_base = dict(repository=copied, receipt_sha256=receipt_hash, pgbackrest=worker.pgbackrest,
        restic=worker.restic, database_key=worker.config.database_key_path, archive_key=worker.config.archive_key_path,
        timeout_seconds=60, max_archive_bytes=64 * 1024**2, max_archive_objects=100,
        wal_segment_bytes=16 * 1024**2, max_wal_segments=32)
    baseline_verification = verify_handoff(**handoff_base, archive_restore=prestaged)
    baseline_inventory = prepare_archive_baseline(
        receipt, prestaged, expected_receipt_sha256=receipt_hash, max_objects=100,
    )
    suffix_key = worker.config.archive_key_path.with_name("suffix-key")
    suffix_key.touch(mode=0o600)
    suffix_key.write_text(secrets.token_hex(32))
    pending_key = worker.config.archive_key_path.with_name("pending-key")
    pending_key.touch(mode=0o600)
    pending_key.write_text(secrets.token_hex(32))

    # Provider intake and its archive are newer than the prestaged baseline.
    record = _later_trade(structures, tmp_path, claim, baseline_raw, trade_id="original-at-anchor")
    expected = _admitted_rows(storage.database, series)
    assert len(expected[0]) == len(expected[1]) == 2
    raw_id, _, key, _ = next(row for row in expected[1] if row.raw_record_id == record.raw_record_id)
    assert raw_id != baseline_raw.raw_record_id
    start, end = baseline_raw.received_at - timedelta(seconds=10), record.received_at + timedelta(seconds=10)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series, start, end)])
    original_reader = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
    assert len(original_reader) == 2
    pending_root = tmp_path / "pending-level2"
    pending_root.mkdir(mode=0o700)
    pending_scratch = tmp_path / "pending-scratch"
    pending_scratch.mkdir(mode=0o700)
    pending_scope, pending_records, failed_event = _pending_level2(storage, structures, pending_root)
    pending_limits = TransferLimits(seconds=25, bytes=16 * 1024**2, files=100, reserve_bytes=16 * 1024**2)
    source_commit = expected[0][-1].market_commit_seq
    target = {"system_identifier": system_identifier, "database_oid": int(database_oid),
              "database": engine.url.database}
    anchor_receipt = tmp_path / "anchor.json"

    with activation_fixture._fence_connections(storage) as (retained, control):
        events = []

        def record_event(phase, evidence):
            events.append(phase)

        fence = close_database_fence(retained, control, expected_database=target, timeout_seconds=60,
                                      record=record_event)
        try:
            with retained.begin():
                fence.assert_quiet(retained)
                assert _snapshot_layout(retained) == baseline["storage_layout"]
                assert retained.scalar(text("SELECT market.fact_header_legacy_end_day()")) == legacy_end
                pending_inventory = dict(source=pending_root, scope=pending_scope, timeout_seconds=15, max_entries=64,
                                         check_held=lambda: fence.assert_quiet(retained))
                pending_before = measure_pending_spool_before_anchor(
                    **pending_inventory, database_identity=identity, source_commit_seq=source_commit,
                )
                with pytest.raises(RuntimeError, match="original_authority_changed"):
                    create_original_recovery_anchor(
                        fence, control, expected_source_commit_seq=expected[0][-1].market_commit_seq + 1,
                    )
                anchor = create_original_recovery_anchor(
                    fence, control, expected_source_commit_seq=expected[0][-1].market_commit_seq,
                )
                assert anchor["database"]["allow_connections"] is False
                assert anchor["storage_layout"] == baseline["storage_layout"]
                anchor_receipt.write_text(json.dumps(anchor, sort_keys=True))
                anchor_hash = hashlib.sha256(anchor_receipt.read_bytes()).hexdigest()
                pending_boundary = {"database_identity": identity, "source_commit_seq": source_commit, "anchor_sha256": anchor_hash}
                pending_source = measure_pending_spool(**pending_inventory, boundary=pending_boundary)
                wal_options = dict(receiving=copied, base_transfer_sha256=transfer["transfer_receipt_sha256"],
                    baseline=baseline_verification, baseline_verification_sha256=baseline_verification["verification_sha256"],
                    anchor_path=anchor_receipt, anchor_sha256=anchor_hash)
                transport = dict(fence=fence, control=control, source=worker.root, **wal_options)
                with pytest.raises(ValueError, match="ciphertext_budget_exceeded"):
                    stage_final_wal(**transport, limits=TransferLimits(seconds=25, bytes=1, files=32))
                wal_limits = TransferLimits(seconds=25, bytes=64 * 1024**2, files=32, reserve_bytes=16 * 1024**2)
                wal_transfer = stage_final_wal(**transport, limits=wal_limits)
                assert stage_final_wal(**transport, limits=wal_limits) == wal_transfer
                assert wal_transfer["wal_transferred_through"] == anchor["wal_segment"]
                assert wal_transfer["encrypted_bytes"] > 0
                stage = tmp_path / "archive-suffix"
                stage.mkdir(mode=0o700)
                archive_limits = TransferLimits(seconds=55, bytes=16 * 1024**2, files=100, reserve_bytes=16 * 1024**2)
                archive_wal_options = {key: value for key, value in wal_options.items() if key != "receiving"}
                archive_wal_options["limits"] = wal_limits
                with archive_namespace(objects.root, exclusive=True) as check_archive:
                    with pytest.raises(RuntimeError, match="suffix_budget_exceeded"):
                        capture_final_archive_suffix(
                            fence, anchor, baseline_inventory, source_root=objects.root, stage=stage,
                            max_objects=100, max_suffix_bytes=0, check_archive=check_archive,
                        )
                    archive_transfer = stage_encrypted_archive_suffix(
                        fence=fence, baseline=baseline_inventory, source_root=objects.root, plaintext_stage=stage,
                        receiving=copied, wal_options=archive_wal_options, expected_wal_sha256=wal_transfer["wal_transfer_sha256"],
                        restic=worker.restic, encryption_key=suffix_key, max_objects=100,
                        max_plaintext_bytes=16 * 1024**2, limits=archive_limits, check_archive=check_archive,
                    )
                    pending_transfer = stage_pending_spool(
                        source=pending_root, receiving=copied, private_scratch=pending_scratch,
                        scope=pending_scope, boundary=pending_boundary, restic=worker.restic, encryption_key=pending_key,
                        limits=pending_limits, max_entries=64, check_held=lambda: (fence.check_clock(), check_archive()),
                        pre_anchor=pending_before, expected_pre_anchor_sha256=pending_before["source_sha256"],
                    )
                assert pending_transfer["segments"] == pending_source["segments"] == pending_before["segments"]
                evidence = archive_transfer["archive"]
                assert evidence["archive_objects"] == 1
                suffix_snapshot = evidence["archive_snapshot"]
                stage.rename(tmp_path / "unavailable-suffix")
                fence.assert_quiet(retained)
        finally:
            reopen_database_fence(fence, control, expected_outcome="original",
                read_outcome=lambda conn: (
                    "original" if _snapshot_layout(conn) == baseline["storage_layout"] else "changed"
                ), record=record_event)
        assert events == ["login_closing", "jobs_stopping", "database_fenced",
                          "login_opening", "jobs_restarting", "database_reopened"]

    _later_trade(structures, tmp_path, claim, record)
    assert len(_admitted_rows(storage.database, series)[0]) == 3
    wal_name = anchor["wal_segment"]
    deadline = monotonic() + 25
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # Also archive transactions beyond the anchor to test exact stopping.
        conn.execute(text("SELECT pg_switch_wal()"))
        while not conn.scalar(text("""
            SELECT left(last_archived_wal,8)=left(:wal,8) AND last_archived_wal>:wal
            FROM pg_stat_archiver
        """), {"wal": wal_name}):
            assert monotonic() < deadline, "anchor and post-anchor WAL must be archived"
            sleep(0.1)
        beyond = conn.scalar(text("SELECT last_archived_wal FROM pg_stat_archiver"))
    assert not list((copied / "database" / "archive" / "qt" / "15-1" / beyond[:16]).glob(beyond + "-*"))
    assert verify_final_wal(**wal_options, expected_hash=wal_transfer["wal_transfer_sha256"], limits=wal_limits) == wal_transfer
    with (copied / wal_transfer["files"][-1]["path"]).open("r+b") as cipher:
        first_byte = cipher.read(1)
        try:
            cipher.seek(0)
            cipher.write(bytes([first_byte[0] ^ 0xff]))
            cipher.flush()
            with pytest.raises(RuntimeError, match="receiving_ciphertext_changed"):
                verify_final_wal(**wal_options, expected_hash=wal_transfer["wal_transfer_sha256"], limits=wal_limits)
        finally:
            cipher.seek(0)
            cipher.write(first_byte)
    print(f"QT_FINAL_WAL files={len(wal_transfer['files'])} bytes={wal_transfer['encrypted_bytes']}")

    _restore_database(worker, copied, baseline, "--type=name", "--target=" + anchor["restore_name"],
                      "--target-timeline=" + str(anchor["timeline"]))
    baseline_restore = tmp_path / "baseline-verified-restore"
    baseline_restore.mkdir(mode=0o700)
    handoff_options = dict(**handoff_base, archive_restore=baseline_restore,
        anchor=anchor_receipt, anchor_sha256=anchor_hash,
        isolated_pgdata=restore_root / "pgdata")
    with pytest.raises(ValueError, match="evidence_hash_mismatch"):
        verify_handoff(**{**handoff_options, "receipt_sha256": "a" * 64})
    segment_dir = copied / "database" / "archive" / "qt" / "15-1" / wal_name[:16]
    (archived_anchor,) = segment_dir.glob(wal_name + "-*")
    withheld_anchor = archived_anchor.with_name("withheld-" + archived_anchor.name)
    archived_anchor.rename(withheld_anchor)
    try:
        with pytest.raises(RuntimeError, match="wal_segment_missing_or_ambiguous"):
            verify_handoff(**handoff_options)
    finally:
        withheld_anchor.rename(archived_anchor)
    handoff = verify_handoff(**handoff_options)
    assert handoff["wal_verified_through"] == wal_name
    assert handoff["verified_archive_objects"] == baseline["archive_objects"]
    restored_baseline = prepare_archive_baseline(
        receipt, baseline_restore, expected_receipt_sha256=receipt_hash, max_objects=100,
    )
    suffix_restore = restore_root / "suffix-restored"
    suffix_restore.mkdir(mode=0o700)
    objects.root.rename(tmp_path / "original-objects")
    (restore_root / "start").write_text("old layout exact archive anchor\n")
    restored_url = engine.url.set(host="restored")
    control_url = restored_url.set(database="postgres")
    _wait_for_restored_cluster(control_url)
    probe = create_engine(restored_url, connect_args={"connect_timeout": 2}, poolclass=NullPool)
    try:
        with pytest.raises(OperationalError, match="not currently accepting connections"):
            probe.connect()
    finally:
        probe.dispose()
    control_engine = create_engine(control_url, poolclass=NullPool)
    try:
        with control_engine.begin() as control:
            saved = control.execute(text("""
                SELECT d.oid::bigint,d.datallowconn,c.system_identifier::text
                FROM pg_database d CROSS JOIN pg_control_system() c WHERE d.datname=:name
            """), {"name": target["database"]}).one()
            assert tuple(saved) == (target["database_oid"], False, system_identifier)
            quoted = control.dialect.identifier_preparer.quote_identifier(target["database"])
            control.exec_driver_sql(f"ALTER DATABASE {quoted} ALLOW_CONNECTIONS true")
    finally:
        control_engine.dispose()
    restored = Database(restored_url.render_as_string(hide_password=False))
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        pending_root.rename(tmp_path / "original-pending-unavailable")
        pending_restore = restore_root / "inactive-pending-restore"
        pending_restore.mkdir(mode=0o700)
        assert pending_restore.stat().st_dev != (tmp_path / "original-pending-unavailable").stat().st_dev
        pending_verified = verify_pending_spool(receiving=copied, expected_transfer_sha256=pending_transfer["transfer_sha256"],
            scope=pending_scope, boundary=pending_boundary, inactive_restore=pending_restore, restic=worker.restic,
            decryption_key=pending_key, limits=pending_limits, max_entries=64,
            expected_pre_anchor_sha256=pending_before["source_sha256"])
        assert pending_verified["segments"] == pending_source["segments"]
        forged_anchor = pending_boundary | {"anchor_sha256": "a" * 64}
        with pytest.raises(RuntimeError, match="receipt_scope_changed"):
            verify_pending_spool(receiving=copied, expected_transfer_sha256=pending_transfer["transfer_sha256"],
                scope=pending_scope, boundary=forged_anchor, inactive_restore=pending_restore, restic=worker.restic,
                decryption_key=pending_key, limits=pending_limits, max_entries=64)
        closure_args = dict(receiving=copied, baseline=restored_baseline, wal_options=archive_wal_options,
            expected_wal_sha256=wal_transfer["wal_transfer_sha256"], native_named=handoff,
            native_named_sha256=handoff["verification_sha256"], archive_transfer_sha256=archive_transfer["archive_transfer_sha256"],
            archive_restore=suffix_restore, restic=worker.restic, decryption_key=suffix_key,
            max_objects=100, limits=archive_limits)
        with restored._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            with pytest.raises(RuntimeError, match="native_named_mismatch"):
                verify_named_archive_closure(conn, **{**closure_args, "native_named_sha256": "a" * 64})
            config = copied / "final-archive" / "restic" / "config"
            hidden = copied / "unavailable-restic-config"
            config.rename(hidden)
            damaged = hidden.read_bytes()
            try:
                config.write_bytes(bytes([damaged[0] ^ 0xff]) + damaged[1:])
                with pytest.raises(RuntimeError, match="ciphertext_changed"):
                    verify_named_archive_closure(conn, **closure_args)
            finally:
                hidden.replace(config)
            closure = verify_named_archive_closure(conn, **closure_args)
        assert closure["coverage"]["final_objects"] == 2
        assert closure["coverage"]["verified_suffix_objects"] == 1
        assert pending_verified["boundary"]["anchor_sha256"] == closure["anchor_sha256"] == anchor_hash
        assert pending_verified["boundary"]["source_commit_seq"] == closure["source_commit_seq"]
        assert pending_verified["pre_anchor_sha256"] == pending_before["source_sha256"]
        recovered_records = [record for descriptor in pending_verified["segments"]
                             for record in DurableRawSpoolSegment.from_path(pending_restore / "spool" / descriptor["path"]).records()]
        assert recovered_records == pending_records
        print(f"QT_NAMED_PENDING_SPOOL files={pending_scope.sealed_files} bytes={pending_scope.sealed_bytes} frames={len(recovered_records)}")
        print(f"QT_FINAL_ARCHIVE files={evidence['encrypted_files']} bytes={evidence['encrypted_bytes']} objects={evidence['archive_objects']}")
        with restored.session() as session:
            assert _snapshot_layout(session) == baseline["storage_layout"]
            assert session.scalar(text("SELECT market.fact_header_legacy_end_day()")) == legacy_end
            assert session.execute(text("""
                SELECT definition_id,session_id,connection_epoch,event_type
                FROM market.stream_session_events WHERE id=:event
            """), {"event": failed_event}).one() == (pending_scope.definition_id, pending_scope.session_id, 1, "failed")
            assert not session.scalar(text("""
                SELECT EXISTS (SELECT 1 FROM market.raw_archive_record_mappings WHERE raw_record_id=ANY(:ids))
            """), {"ids": [item.raw_record_id for item in recovered_records]})
            options = dict(max_objects=100, suffix_snapshot_id=suffix_snapshot)
            with pytest.raises(RuntimeError, match="suffix_binding_mismatch"):
                verify_final_archive_suffix(
                    session, anchor, restored_baseline, evidence, suffix_restore,
                    max_objects=100, suffix_snapshot_id="a" * 64,
                )
            archive = suffix_restore / evidence["archive_source_root"].lstrip("/")
            relocated = archive / key
            payload = relocated.read_bytes()
            relocated.unlink()
            with pytest.raises(FileNotFoundError):
                verify_final_archive_suffix(session, anchor, restored_baseline, evidence, suffix_restore, **options)
            relocated.write_bytes(b"x" * len(payload))
            with pytest.raises(ValueError, match="archive object failed size or hash"):
                verify_final_archive_suffix(session, anchor, restored_baseline, evidence, suffix_restore, **options)
            relocated.write_bytes(payload)
            coverage = verify_final_archive_suffix(session, anchor, restored_baseline, evidence, suffix_restore, **options)
            assert coverage["final_objects"] == 2
            assert coverage["baseline_objects_used"] == coverage["verified_suffix_objects"] == 1
        assert _admitted_rows(restored, series) == expected
        monkeypatch.setattr(market_data, "db", restored)
        merged = tmp_path / "restored-merged-objects"
        shutil.copytree(baseline_restore / str(objects.root).lstrip("/"), merged)
        shutil.copytree(suffix_restore / evidence["archive_source_root"].lstrip("/"), merged, dirs_exist_ok=True)
        restored_store = FilesystemRawArchiveObjectStore(merged, writable=False)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
            object_store_factory=lambda: restored_store, read_cache_factory=lambda: None,
        ))
        repository = market_data.PostgresMarketDataRepository()
        assert repository.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series) == original_reader
        assert read_raw_archive_parquet(restored_store.local_path(key)) == [record]
        check_deployed_source_reader(restored_url, restored_store, frozen.dataset_id, series, original_reader)
    finally:
        restored._reset_engine()


@pytest.fixture
def signed_named_tablespace(storage):
    engine = storage.database._engine
    assert engine.url.host == "timescaledb" and engine.url.database.startswith("qt_migration_")
    source_root = Path("/qt-history/signed-source-tablespace")
    source_root.mkdir(mode=0o700)
    source_path = source_root / "space"
    source_path.mkdir(mode=0o700)
    (source_root / "source-volume").write_text("signed source history\n")
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin.connect() as conn:
            conn.execute(text('CREATE TABLESPACE "qt_history_hdd-01" LOCATION :path'), {"path": str(source_path)})
        with engine.begin() as conn:
            conn.exec_driver_sql('ALTER TABLE market.raw_archive_record_mappings SET TABLESPACE "qt_history_hdd-01"')
            conn.exec_driver_sql('ALTER INDEX market.ix_market_raw_archive_mapping_segment SET TABLESPACE "qt_history_hdd-01"')
        yield source_path
    finally:
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("ALTER INDEX market.ix_market_raw_archive_mapping_segment SET TABLESPACE pg_default")
                conn.exec_driver_sql("ALTER TABLE market.raw_archive_record_mappings SET TABLESPACE pg_default")
            engine.dispose()
            with admin.connect() as conn:
                conn.exec_driver_sql('DROP TABLESPACE IF EXISTS "qt_history_hdd-01"')
        finally:
            admin.dispose()


def _signed_tablespace_member(conn):
    return conn.execute(text("""
        SELECT s.oid::bigint,pg_tablespace_location(s.oid),heap.oid::bigint,idx.oid::bigint,
               pg_relation_filepath(heap.oid),pg_relation_filepath(idx.oid)
        FROM pg_tablespace s
        JOIN pg_class heap ON heap.reltablespace=s.oid AND heap.oid='market.raw_archive_record_mappings'::regclass
        JOIN pg_class idx ON idx.reltablespace=s.oid AND idx.oid='market.ix_market_raw_archive_mapping_segment'::regclass
        JOIN pg_index def ON def.indexrelid=idx.oid AND def.indrelid=heap.oid AND def.indisvalid
        WHERE s.spcname='qt_history_hdd-01'
    """)).one()


_SOURCE_POSTMASTER = "SELECT pg_postmaster_start_time(), timeline_id FROM pg_control_checkpoint()"


def _emit_fixture_wal_prefix(engine, *, replayed=False):
    generated = []
    boundary = None
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as archiver:
        archiver.exec_driver_sql("SET statement_timeout='5s'")
        for record in range(1, 6):
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='5s'")
                writer.exec_driver_sql("SET LOCAL synchronous_commit=on")
                writer.execute(text("INSERT INTO public.qt_named_wal_prefix_probe VALUES (:record)"), {"record": record})
            if replayed and record == 5:
                archiver.exec_driver_sql("CHECKPOINT").close()
                redo = archiver.exec_driver_sql("SELECT redo_wal_file FROM pg_control_checkpoint()").scalar_one()
                target, replay = archiver.exec_driver_sql("""
                    SELECT pg_current_wal_insert_lsn()::text,pg_create_restore_point('qt_fixture_signed_prefix')::text
                """).one()
                boundary = (target, replay, redo)
            segment = archiver.exec_driver_sql("SELECT pg_walfile_name(pg_current_wal_insert_lsn())").scalar_one()
            generated.append(segment)
            archiver.exec_driver_sql("SELECT pg_switch_wal()").close()
        deadline = monotonic() + 25
        while not archiver.scalar(text("""
            SELECT left(last_archived_wal,8)=left(:segment,8) AND last_archived_wal>=:segment
            FROM pg_stat_archiver
        """), {"segment": generated[-1]}):
            assert monotonic() < deadline, "all fixture prefix WAL must be archived"
            sleep(0.1)
    return generated, boundary


def _stage_fixture_wal_prefix(engine, prepared, worker, receiving, transfer, baseline, *, generated=None,
                              authenticate_prefix=None):
    from scripts.automation.canonical_cold_named_recovery import (
        _sign, verify_prepared_wal_prefix, verify_signed_recovery, verify_wal_prefix_chain,
    )
    from scripts.db.canonical_cold_recovery_wal_transfer import (
        check_source_wal_capacity, stage_wal_prefix, verify_wal_prefix,
    )

    if generated is None:
        generated, _ = _emit_fixture_wal_prefix(engine)

    segment_bytes = baseline["wal_segment_bytes"]
    assert _wal_range(generated[0], generated[-1], segment_bytes, 5) == generated
    start = baseline["wal_backup_stop"]
    first_through, last_through = generated[1], generated[-1]
    prefix_limits = TransferLimits(25, 64 * 1024**2, 3 if authenticate_prefix is not None else 16)
    options = dict(receiving=receiving, base_transfer_sha256=transfer["transfer_receipt_sha256"],
                   baseline=baseline, baseline_verification_sha256=baseline["verification_sha256"],
                   limits=prefix_limits)
    control_engine = create_engine(engine.url.set(database="postgres"), poolclass=NullPool)
    try:
        with control_engine.connect() as control:
            with control.begin():
                expected_database = _database(control, engine.url.database)
            with pytest.raises(ValueError, match="recovery_handoff_wal_range_outside_budget"):
                check_source_wal_capacity(control, expected_database=expected_database, backup_stop=start,
                                          segment_bytes=segment_bytes, max_segments=3, timeout_seconds=5)
            stage_options = options | dict(control=control, expected_database=expected_database, source=worker.root)
            first = stage_wal_prefix(**stage_options, after=start, through=first_through)
            first_hash = first["wal_prefix_sha256"]
            assert verify_wal_prefix(**options, after=start, through=first_through, expected_hash=first_hash) == first
            with pytest.raises(ValueError, match="recovery_handoff_evidence_hash_mismatch"):
                stage_wal_prefix(**stage_options, after=first_through, through=last_through,
                                 previous_prefix_sha256="f" * 64)
            second_scope = dict(after=first_through, through=last_through, previous_prefix_sha256=first_hash)
            second = stage_wal_prefix(**stage_options, **second_scope)
            second_hash = second["wal_prefix_sha256"]
            assert verify_wal_prefix(**options, **second_scope, expected_hash=second_hash) == second
            with pytest.raises(ValueError, match="recovery_handoff_evidence_hash_mismatch"):
                verify_wal_prefix(**options, **second_scope, expected_hash=first_hash)
            receipt = receiving / f"encrypted-wal-prefix-through-{last_through}.json"
            before = receipt.stat()
            assert stage_wal_prefix(**stage_options, **second_scope) == second
            after = receipt.stat()
            assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
            assert not control.in_transaction()
    finally:
        control_engine.dispose()

    assert [item["segment"] for item in first["files"] + second["files"]] == (
        _wal_range(start, last_through, segment_bytes, 2 * prefix_limits.files + 1)[1:]
    )
    if authenticate_prefix is not None:
        for batch in (first, second):
            authenticate_prefix(after=batch["wal_after"], through=batch["wal_transferred_through"],
                                prefix_sha256=batch["wal_prefix_sha256"],
                                previous_prefix_sha256=batch["previous_prefix_sha256"], limits=prefix_limits)
        assert not (receiving / "encrypted-wal-transfer.json").exists()
        print(f"QT_SIGNED_REPLAYED_PREFIX files={len(first['files'])}+{len(second['files'])} through={last_through}")
        return
    envelopes = []
    for batch in (first, second):
        envelope = verify_prepared_wal_prefix(prepared=prepared, after=batch["wal_after"],
            through=batch["wal_transferred_through"], prefix_sha256=batch["wal_prefix_sha256"], timeout_seconds=25,
            previous_prefix_sha256=batch["previous_prefix_sha256"], limits=prefix_limits)
        envelopes.append(envelope)
        verified = verify_signed_recovery(envelope, prepared.site.receiver_public_key)
        entries = [[row["segment"], row["wal_checksum"]] for row in batch["files"]]
        assert verified == {
            "schema": "qt.named_recovery_wal_prefix_verification.v1",
            "prepared_receiver_sha256": prepared.digest,
            "pair_receipt_sha256": baseline["pair_receipt_sha256"],
            "baseline_verification_sha256": baseline["verification_sha256"],
            "wal_prefix_sha256": batch["wal_prefix_sha256"],
            "previous_prefix_sha256": batch["previous_prefix_sha256"],
            "after": batch["wal_after"],
            "through": batch["wal_transferred_through"],
            "verified_wal_sha256": archive_evidence_hash(entries),
            "segments": len(entries),
        }
        print(f"QT_NATIVE_WAL_PREFIX after={verified['after']} through={verified['through']} segments={len(entries)} "
              f"native_bytes={segment_bytes * len(entries)} native_sha256={verified['verified_wal_sha256']}")
    chain_options = {"site": prepared.site, "prepared_sha256": prepared.digest}
    chain = verify_wal_prefix_chain(**chain_options, prefixes=envelopes)
    assert (chain["wal_first"], chain["wal_verified_through"], chain["wal_segment_bytes"],
            chain["prefix_batches"], chain["prefix_segments"]) == (baseline["wal_start"], last_through, segment_bytes, 2, 5)
    for invalid in ([envelopes[1]], envelopes[::-1]):
        with pytest.raises(ValueError, match="prefix_chain_changed"):
            verify_wal_prefix_chain(**chain_options, prefixes=invalid)
    tampered_body = envelopes[1]["body"] | {"segments": 2}
    with pytest.raises(ValueError, match="signature_mismatch"):
        verify_wal_prefix_chain(**chain_options, prefixes=[envelopes[0], envelopes[1] | {"body": tampered_body}])
    with pytest.raises(ValueError, match="segment_count_changed"):
        verify_wal_prefix_chain(**chain_options, prefixes=[envelopes[0], _sign(tampered_body, prepared.signer)])
    print(f"QT_NATIVE_WAL_PREFIX_CHAIN batches={chain['prefix_batches']} verified_through={chain['wal_verified_through']} "
          f"chain_sha256={chain['wal_verification_sha256']}")
    assert not (receiving / "encrypted-wal-transfer.json").exists()
    assert not prepared.armed
    print(f"QT_ROLLING_WAL_PREFIX old_cap=3 first_files={len(first['files'])} second_files={len(second['files'])} "
          f"first_bytes={first['encrypted_bytes']} second_bytes={second['encrypted_bytes']} "
          f"through={last_through} first_receipt={first_hash} second_receipt={second_hash}")


def test_signed_named_receiver_coordinates_fenced_recovery(storage, tmp_path, monkeypatch, signed_named_tablespace):
    assert os.getenv("QT_STORAGE_DEMO") == "1" and os.getuid() == 70
    engine = storage.database._engine
    assert engine.url.host == "timescaledb" and engine.url.database.startswith("qt_migration_")
    replayed = os.getenv("QT_NAMED_REPLAYED_FIXTURE") == "1"
    runtime_fixture = os.getenv("QT_NAMED_RUNTIME_FIXTURE") == "1"
    rolling_prefix = replayed or os.getenv("QT_TEST_ROLLING_WAL_PREFIX") == "1"
    with engine.begin() as conn:
        identity = _identity(conn)[0]
        cluster, db_oid = identity.split("/")
        target = {"system_identifier": cluster, "database_oid": int(db_oid), "database": engine.url.database}
        member = _signed_tablespace_member(conn)
        assert member[1] == str(signed_named_tablespace)
        role, role_oid = conn.exec_driver_sql("SELECT rolname,oid FROM pg_roles WHERE rolname=session_user").one()
        if rolling_prefix:
            source_postmaster = conn.exec_driver_sql(_SOURCE_POSTMASTER).one()
            conn.exec_driver_sql("CREATE TABLE public.qt_named_wal_prefix_probe (record integer PRIMARY KEY)")
    objects = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    structures, claim, series = _define_trade_stream(storage, monkeypatch)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    worker = _prepare_native_repository(engine, identity, tmp_path, monkeypatch)
    _, first_trade = _seal_trade(tmp_path, claim, trade_id="coordinator-before-backup")
    _native_recover(structures, tmp_path)
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        full = worker.create(snapshot, objects=objects, keep_copies=2)
    assert full["database_type"] == "full"
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        pair = worker.create(snapshot, objects=objects, keep_copies=2)
    assert pair["database_type"] == "incr"
    assert pair["database_label"].startswith(full["database_label"] + "_")
    restore_root, receiving, pair_receipt, transfer = _copy_repository(worker, pair)
    base = restore_root / "prestage-native-base"
    base.mkdir(mode=0o700)
    base_native = verify_handoff(repository=receiving, receipt_sha256=transfer["pair_receipt_sha256"], archive_restore=base,
        pgbackrest=worker.pgbackrest, restic=worker.restic, database_key=worker.config.database_key_path,
        archive_key=worker.config.archive_key_path, timeout_seconds=30, max_archive_bytes=64 * 1024**2,
        max_archive_objects=100, wal_segment_bytes=16 * 1024**2, max_wal_segments=16)
    baseline = prepare_archive_baseline(pair_receipt, base, expected_receipt_sha256=transfer["pair_receipt_sha256"], max_objects=100)

    def private_key(name, secret):
        path = worker.config.archive_key_path.with_name(name)
        path.touch(mode=0o600)
        path.write_text(secret)
        return path

    source = Ed25519PrivateKey.generate()
    receiver = Ed25519PrivateKey.generate()
    source_sign = private_key("source-sign", source.private_bytes_raw().hex())
    receiver_sign = private_key("receiver-sign", receiver.private_bytes_raw().hex())
    suffix_key = private_key("named-suffix", secrets.token_hex(32))
    pending_key = private_key("named-pending", secrets.token_hex(32))
    policy = RecoverySitePolicy(receiving=receiving,
        base_transfer_sha256=transfer["transfer_receipt_sha256"], baseline_verification=base_native,
        source_public_key=source.public_key().public_bytes_raw().hex(), receiver_public_key=receiver.public_key().public_bytes_raw().hex(),
        wal=TransferLimits(35, 64 * 1024**2, 3 if replayed else 16), archive=TransferLimits(40, 64 * 1024**2, 100),
        pending=TransferLimits(40, 16 * 1024**2, 100), database=TransferLimits(40, 256 * 1024**2, 1, 32 * 1024**2),
        max_archive_objects=100, max_pending_entries=64)
    scratch = tmp_path / "source-scratch"
    pending_root = tmp_path / "source-pending"
    receipts = tmp_path / "site-receipts"
    workspace = restore_root / "receiver-workspace"
    pgdata = workspace / "pgdata"
    history = restore_root / "receiver-history"
    history_path = history / "space"
    negative = restore_root / "receiver-negative"
    negative_data = negative / "pgdata"
    for path in (scratch, pending_root, receipts, workspace, pgdata, history, history_path, negative, negative_data):
        path.mkdir(mode=0o700)
    (history / "receiver-volume").write_text("signed receiver history\n")
    assert history.stat().st_dev != signed_named_tablespace.stat().st_dev
    alias = history / "aliased-space"
    alias.symlink_to(history_path)
    device = receiving.stat().st_dev
    receiver_uuid = "postwrite-independent-fixture"
    udev = Path(os.environ["QT_STORAGE_UDEV_ROOT"])
    (udev / f"b{os.major(device)}:{os.minor(device)}").write_text(f"E:ID_FS_UUID={receiver_uuid}\n")
    # This disposable server pair shares a single kernel. The independent host
    # pins are fixture inputs. Capacity and native metadata are measured for real.
    monkeypatch.setattr(preflight.named, "receiver_host_sha256", lambda: "c" * 64)
    mount = lambda path: preflight.ReceiverPath(path, receiver_uuid)
    preflight_options = dict(site=policy, receiver_signing_key=receiver_sign, expected_host_sha256="c" * 64,
        source_host_sha256="d" * 64, source_filesystem_uuids=(pair["filesystem_uuid"],), receiving=mount(receiving),
        workspace=mount(workspace), pgdata=mount(pgdata), tablespaces={"qt_history_hdd-01": mount(history_path)},
        pgbackrest=worker.pgbackrest, restic=worker.restic, database_key=worker.config.database_key_path,
        archive_key=worker.config.archive_key_path, pending_expected=True, runtime_bytes=16 * 1024**2,
        runtime_entries=128, max_manifest_entries=10_000, timeout_seconds=25)
    with pytest.raises(ValueError, match="host_not_independent"):
        preflight.measure_receiver_preflight(**(preflight_options | {"source_host_sha256": "c" * 64}))
    with pytest.raises(RuntimeError, match="mount_identity_mismatch"):
        preflight.measure_receiver_preflight(**(preflight_options | {"receiving": preflight.ReceiverPath(receiving, "wrong-uuid")}))
    with pytest.raises(ValueError, match="tablespace_allowlist_changed"):
        preflight.measure_receiver_preflight(**(preflight_options | {"tablespaces": {}}))
    with pytest.raises(RuntimeError, match="capacity_insufficient"):
        preflight.measure_receiver_preflight(**(preflight_options | {"runtime_bytes": 2 * 1024**3}))
    measured = preflight.measure_receiver_preflight(**preflight_options)
    sizing = measured["body"]
    assert sizing["database_targets"]["tablespace:qt_history_hdd-01"]["logical_bytes"] > 0
    assert sizing["archive_restore_files"] >= pair["archive_objects"]
    assert len(sizing["capacity_by_device"]) == 1
    capacity, = sizing["capacity_by_device"].values()
    assert capacity["required_bytes"] > sum(item["logical_bytes"] for item in sizing["database_targets"].values())
    receiver.public_key().verify(bytes.fromhex(measured["signature"]),
        json.dumps(sizing, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
    assert not any(pgdata.iterdir()) and not any(history_path.iterdir())
    print(f"QT_RECEIVER_PREFLIGHT database={sizing['database_targets']} archive={sizing['archive_restore_bytes']} "
          f"workspace_required={capacity['required_bytes']} entries={capacity['required_entries']}")
    raw_ids = [first_trade.raw_record_id]
    if replayed:
        prefix_trade = _later_trade(structures, tmp_path, claim, first_trade, trade_id="coordinator-prefix")
        raw_ids.append(prefix_trade.raw_record_id)
        prefix_facts = _admitted_rows(storage.database, series)[0]
        prefix_commit = prefix_facts[-1].market_commit_seq
        generated, prefix_boundary = _emit_fixture_wal_prefix(engine, replayed=True)
        assert prefix_boundary[2] == generated[-1]
    later_trade = _later_trade(structures, tmp_path, claim, first_trade, trade_id="coordinator-at-anchor")
    raw_ids.append(later_trade.raw_record_id)
    scope, records, _ = _pending_level2(storage, structures, pending_root)
    start = first_trade.received_at - timedelta(seconds=10)
    end = later_trade.received_at + timedelta(seconds=10)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series, start, end)])
    expected = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
    assert len(expected) == len(raw_ids)
    source_commit = _admitted_rows(storage.database, series)[0][-1].market_commit_seq
    scope_hash = archive_evidence_hash(asdict(scope))
    package_path = receipts / "package.json"
    verify_runtime = None

    def runtime_artifact():
        from scripts.automation.canonical_cold_receiver_runtime import (
            WITNESS_SCHEMA, fingerprint_frozen_records, fingerprint_raw_records,
        )

        def raw_case(conn, materialized):
            manifest = conn.execute(text("""
                SELECT id,content_fingerprint,record_count FROM market.raw_archive_manifests WHERE id=:id
            """), {"id": materialized[1]}).one()
            archived = read_raw_archive_parquet(objects.local_path(materialized[2]))
            return {"manifest_id": manifest.id, "rows": manifest.record_count,
                    "content_fingerprint": manifest.content_fingerprint, "sha256": fingerprint_raw_records(archived)}

        assert replayed
        accepted = {row.raw_record_id: row for row in _admitted_rows(storage.database, series)[1]}
        with engine.connect() as conn:
            cases = [raw_case(conn, accepted[raw_id]) for raw_id in raw_ids[1:]]
        witness = {"schema": WITNESS_SCHEMA, "database_identity": identity, "max_rows": 100,
                   "max_archive_bytes": 16 * 1024**2, "raw": cases,
                   "frozen": [{"dataset_id": frozen.dataset_id, "dataset_hash": frozen.dataset_hash, "series_id": series,
                               "mode": "frozen_revisions", "rows": len(expected), "sha256": fingerprint_frozen_records(
                                   expected, mode="frozen_revisions", series_id=series, commit_seq=frozen.max_commit_seq)}]}
        path = receipts / "replay-equivalence.json"
        save_receipt(path, witness, initial=True)
        return {"filename": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def approve_fixture(staged=None):
        nonlocal verify_runtime
        pins = {}
        inventory = None
        schema = PLAN_VERSION
        if staged is not None:
            from scripts.automation.canonical_cold_recovery_checks import REPLAYED_PLAN_VERSION

            schema = REPLAYED_PLAN_VERSION
            pins = {"replayed_checkpoint_sha256": staged.checkpoint["verification_sha256"], "max_restart_segments": 3}
            inventory = scratch / "received-prefix-catalog.jsonl"
            shutil.copyfile(staged.inventory, inventory)
        plan = {"schema": schema, "pending_spool_scope_sha256": scope_hash,
                "receiver_public_key": policy.receiver_public_key, "receiver_preflight": measured, **pins,
                **{key: sizing["site_policy"][key] for key in
                   ("base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256")}}
        plan_path = receipts / "recovery-plan.json"
        save_receipt(plan_path, plan, initial=True)
        plan_hash = hashlib.sha256(plan_path.read_bytes()).hexdigest()
        artifacts = {"independent_recovery": {"filename": plan_path.name, "sha256": plan_hash}}
        if runtime_fixture:
            artifacts["replay_equivalence"] = runtime_artifact()
        package = {"source_commit_floor": source_commit, "scope": {"target_database": target}, "artifacts": artifacts}
        save_receipt(package_path, package, initial=True)
        package_hash = hashlib.sha256(package_path.read_bytes()).hexdigest()
        site = NamedRecoverySite(**vars(policy), approved_package_sha256=package_hash, receiver_preflight=measured, **pins)
        with pytest.raises(ValueError, match="preflight_policy_changed"):
            replace(site, wal=replace(site.wal, bytes=site.wal.bytes + 1))
        source_options = dict(site=site, provider_receipt_root=receipts, database_repository=worker.root,
            archive_objects=objects.root, scratch=scratch, baseline=baseline, restic=worker.restic, archive_key=suffix_key,
            pending_key=pending_key, pending_spool=pending_root, replayed_inventory=inventory)
        with pytest.raises(ValueError, match="signing_key_unapproved"):
            bind_named_recovery_source(**source_options, signing_key=receiver_sign)
        if runtime_fixture:
            from scripts.automation.canonical_cold_receiver_runtime import bind_receiver_runtime

            runtime = artifacts["replay_equivalence"]
            verify_runtime = bind_receiver_runtime(witness_path=receipts / runtime["filename"], witness_sha256=runtime["sha256"],
                source_checkout=os.environ["QT_EXACT_OLD_SOURCE_ROOT"], expected_revision="afafda39b4347adf8c88d01df6ae4b849796935a",
                expected_source_tree_sha256=os.environ["QT_EXACT_OLD_SOURCE_HASH"], receiving=receiving, timeout_seconds=60)
        return site, package, package_hash, bind_named_recovery_source(**source_options, signing_key=source_sign)

    @contextmanager
    def boot_isolated(restored_path, check):
        assert restored_path == pgdata
        (restore_root / "pgdata").symlink_to(pgdata)
        (restore_root / "start").write_text("signed named receiver\n")
        check()
        url = engine.url.set(host="restored")
        _wait_for_restored_cluster(url.set(database="postgres"))
        check()
        control_engine = create_engine(url.set(database="postgres"), poolclass=NullPool)
        target_engine = create_engine(url, poolclass=NullPool)
        try:
            yield control_engine, target_engine
        finally:
            target_engine.dispose()
            control_engine.dispose()

    def verify_original(target_engine, base_restore, suffix_restore, pending, check):
        check()
        if verify_runtime is not None:
            digest = verify_runtime(target_engine, base_restore, suffix_restore, pending, check)
            print(f"QT_NAMED_RECEIVER_ORIGINAL_RUNTIME receipt_sha256={digest} frozen_rows={len(expected)} "
                  f"raw_manifests=2 pending_ids={len(records)}")
            return digest
        with target_engine.begin() as conn:
            assert _snapshot_layout(conn) == pair["storage_layout"]
            assert conn.scalar(text("SELECT current_setting('server_version_num')::int / 10000")) == 15
            restored = _signed_tablespace_member(conn)
            assert tuple(restored) == (member[0], str(history_path), *member[2:])
            assert conn.execute(text("""
                SELECT raw_record_id FROM market.raw_archive_record_mappings
                WHERE raw_record_id=ANY(:ids) ORDER BY raw_record_id
            """), {"ids": raw_ids}).scalars().all() == sorted(raw_ids)
            if rolling_prefix:
                probe_rows = conn.exec_driver_sql(
                    "SELECT record FROM public.qt_named_wal_prefix_probe ORDER BY record"
                ).scalars().all()
                assert probe_rows == [1, 2, 3, 4, 5]
        assert (history / "receiver-volume").read_text() == "signed receiver history\n"
        assert (signed_named_tablespace.parent / "source-volume").read_text() == "signed source history\n"
        assert not (history / "source-volume").exists()
        assert not (signed_named_tablespace.parent / "receiver-volume").exists()
        source_prefix = Path(str(objects.root).lstrip("/"))
        final = receiving / "final-archive" / "complete.json"
        suffix_prefix = Path(json.loads(final.read_text())["archive"]["archive_source_root"].lstrip("/"))
        class ReceiverArchiveView(FilesystemRawArchiveObjectStore):
            def local_path(self, object_key):
                suffix = suffix_store.local_path(object_key)
                return suffix if suffix.is_file() else super().local_path(object_key)

        suffix_store = FilesystemRawArchiveObjectStore(suffix_restore / suffix_prefix, writable=False)
        recovered_store = ReceiverArchiveView(base_restore if replayed else base_restore / source_prefix, writable=False)
        recovered_db = Database(target_engine.url.render_as_string(hide_password=False))
        try:
            assert recovered_db.ensure_schema(), str(recovered_db.last_error)
            with monkeypatch.context() as patch:
                patch.setattr(market_data, "db", recovered_db)
                patch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
                    object_store_factory=lambda: recovered_store, read_cache_factory=lambda: None))
                repo = market_data.PostgresMarketDataRepository()
                assert repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series) == expected
            check_deployed_source_reader(target_engine.url, recovered_store, frozen.dataset_id, series, expected)
        finally:
            recovered_db._reset_engine()
        restored_pending = workspace / "inactive-pending" / "spool"
        exact = [frame for segment in pending["segments"]
                 for frame in DurableRawSpoolSegment.from_path(restored_pending / segment["path"]).records()]
        assert exact == records
        return archive_evidence_hash({"layout": pair["storage_layout"], "frozen": frozen.dataset_id,
                                      "pending": pending["verification_sha256"], "tablespace": list(restored)})

    prepare_options = dict(signing_key=receiver_sign, pgbackrest=worker.pgbackrest,
        restic=worker.restic, database_key=worker.config.database_key_path, base_archive_key=worker.config.archive_key_path,
        workspace=workspace, pgdata=pgdata, tablespaces={"qt_history_hdd-01": history_path},
        timeout_seconds=100, max_custody_entries=10_000)
    if replayed:
        from tests.test_market_data.local_named_replayed_fixture import NativeReplayedFixture

        native_receiver = NativeReplayedFixture(database=engine.url.database, role=role, pgdata=pgdata)
    receiver_options = dict(suffix_key=suffix_key, pending_key=pending_key,
        boot_isolated=native_receiver.boot if replayed else boot_isolated,
        verify_original_runtime=verify_original, timeout_seconds=100)

    def qualify_receiver(prepared):
        pg_control = pgdata / "global" / "pg_control"
        before = pg_control.stat()
        expected_workspace = set(workspace.iterdir())
        deadline = monotonic() + 60
        while not (receiving / "named-recovery-request.json").is_file():
            if monotonic() >= deadline:
                raise TimeoutError("fixture signed recovery request did not arrive")
            sleep(0.05)
        pack = next((receiving / "final-archive" / "restic" / "data").glob("*/*"))
        original = pack.read_bytes()
        assert len(original) > 1
        intact = receiving / "fixture-intact-restic-pack"
        for damaged in (original[:-1], bytes([original[0] ^ 1]) + original[1:]):
            pack.rename(intact)
            try:
                pack.write_bytes(damaged)
                with pytest.raises(RuntimeError, match="archive_transport_ciphertext_changed"):
                    recover_named_site(prepared=prepared, **receiver_options)
                assert pg_control.stat().st_ctime_ns == before.st_ctime_ns
                assert set(workspace.iterdir()) == expected_workspace
                assert not (restore_root / "start").exists()
                assert not (receiving / "named-recovery-response.json").exists()
            finally:
                intact.replace(pack)
        return recover_named_site(prepared=prepared, **receiver_options)

    def replay_before_approval(*, pgdata, base_restore, authenticate_prefix, check):
        from scripts.automation.canonical_cold_receiver_replay import replay_private_receiver
        from tests.test_market_data.local_named_replayed_fixture import PORT, SOCKET, materialize_fixture_archive

        _stage_fixture_wal_prefix(engine, None, worker, receiving, transfer, base_native,
                                  generated=generated, authenticate_prefix=authenticate_prefix)
        prefix_target, prefix_replay, _ = prefix_boundary
        archive_paths = None

        @contextmanager
        def boot_paused(path, *, socket, port, timeline, target_lsn, check):
            assert (socket, port, timeline, target_lsn) == (SOCKET, PORT, 1, prefix_target)
            with native_receiver.boot(path, check, target=target_lsn, expected_replay=prefix_replay) as engines:
                yield engines

        def verify_archive(conn, check_archive):
            nonlocal archive_paths
            paused, receipt, inventory, prefix_objects = materialize_fixture_archive(conn, pgdata=pgdata,
                archive_root=workspace / "replayed-archive", source_objects=objects.root, base_restore=base_restore,
                pair=pair, pair_receipt=pair_receipt, pair_receipt_sha256=transfer["pair_receipt_sha256"],
                tablespace=history_path, tablespace_oid=member[0], role_oid=role_oid, target=prefix_target,
                replay=prefix_replay, check=check_archive)
            assert paused["accepted_commit_seq"] == prefix_commit and receipt["catalog_objects"] == 2
            assert set(conn.exec_driver_sql("SELECT raw_record_id FROM market.raw_archive_record_mappings").scalars()) == set(raw_ids[:2])
            decoded = [record.raw_record_id for path in prefix_objects.rglob("*.parquet") for record in read_raw_archive_parquet(path)]
            assert sorted(decoded) == sorted(raw_ids[:2])
            archive_paths = {"archive_inventory": inventory, "archive_objects": prefix_objects}
            return receipt

        def read_control(path, check_control):
            assert path == pgdata
            check_control()
            control, expected_hash = native_receiver.stopped_control()
            assert hashlib.sha256((pgdata / "global/pg_control").read_bytes()).hexdigest() == expected_hash
            return control

        result = replay_private_receiver(pgdata=pgdata, workspace=workspace, socket=SOCKET, port=PORT,
            expected_roots=[(path, native_receiver._identity(path)) for path in (pgdata, workspace, history_path)],
            expected_control_sha256=hashlib.sha256((pgdata / "global/pg_control").read_bytes()).hexdigest(),
            database_identity=identity, database=engine.url.database, role=role, role_oid=role_oid, timeline=1,
            target_lsn=prefix_target, replay_lsn=prefix_replay,
            restore_command=f'{worker.pgbackrest} --config={workspace / "pgbackrest.conf"} --stanza=qt archive-get %f "%p"',
            boot_paused=boot_paused, verify_archive=verify_archive, timeout_seconds=max(1, min(check(), 75)),
            read_stopped_control=read_control, tablespaces={"qt_history_hdd-01": (history_path, member[0])},
            request_restartpoint=True)
        assert archive_paths is not None
        return result | archive_paths

    @contextmanager
    def prepare_before_fence():
        if replayed:
            from scripts.automation.canonical_cold_named_recovery import prepare_replayed_receiver, seal_replayed_receiver

            with prepare_replayed_receiver(**prepare_options, site=policy, receiver_preflight=measured,
                                            replay_before_approval=replay_before_approval) as staged:
                assert not package_path.exists()
                assert staged.checkpoint["accepted_commit_seq"] == prefix_commit < source_commit
                assert staged.checkpoint["redo_wal_segment"] == prefix_boundary[2] > base_native["wal_backup_stop"]
                assert not (pgdata / "postmaster.pid").exists()
                site, package, package_hash, recover_source = approve_fixture(staged)
                with seal_replayed_receiver(staged=staged, site=site, ready_seconds=120) as prepared:
                    print(f"QT_SIGNED_REPLAYED_APPROVAL checkpoint={staged.checkpoint['verification_sha256']} "
                          f"prefix_commit={prefix_commit} final_commit={source_commit} package_sha256={package_hash}")
                    yield prepared, site, package, package_hash, recover_source
            return
        site, package, package_hash, recover_source = approve_fixture()
        for supplied, failure in (({"qt_history_hdd-01": alias}, "directory_invalid"), ({}, "preflight_receiver_changed"),
                                  ({"qt_history_hdd-01": history_path}, "preflight_mount_changed")):
            with pytest.raises(ValueError, match=failure), prepare_named_receiver(**(prepare_options | {
                    "site": site, "ready_seconds": 120, "workspace": negative,
                    "pgdata": negative_data, "tablespaces": supplied})):
                pytest.fail("unapproved preparation was admitted")
        assert not any(negative_data.iterdir())
        with prepare_named_receiver(**prepare_options, site=site, ready_seconds=120) as prepared:
            assert not (restore_root / "start").exists()
            assert (pgdata / "backup_label").is_file() and (pgdata / "recovery.signal").is_file()
            if rolling_prefix:
                _stage_fixture_wal_prefix(engine, prepared, worker, receiving, transfer, base_native)
                assert not (restore_root / "start").exists()
            yield prepared, site, package, package_hash, recover_source

    with prepare_before_fence() as approved, activation_fixture._fence_connections(storage) as (retained, control):
        prepared, site, package, approved_package_hash, recover_source = approved
        ready = recover_source.assert_receiver_ready()
        assert ready == prepared.ready
        admission = bind_approved_recovery_checks(package_path=package_path, package=package,
            approved_package_hash=approved_package_hash, receipt_root=receipts,
            verify_site_restore=recover_source, expected_pending_scope_sha256=scope_hash)
        assert admission.assert_receiver_ready() == prepared.digest
        with retained.begin():
            expected_database = _database(retained, target["database"])
        segments = admission.assert_source_wal_capacity(control, expected_database=expected_database, timeout_seconds=5)
        assert 0 <= segments <= site.wal.files
        assert not control.in_transaction()

        def bulk_operation_during_fence(*args, **kwargs):
            pytest.fail("bulk materialization repeated during the source fence")
        for name in ("_native_restore", "verify_handoff", "prepare_archive_baseline"):
            monkeypatch.setattr(preflight.named, name, bulk_operation_during_fence)
        fence = close_database_fence(retained, control, expected_database=target, timeout_seconds=110, record=lambda *_: None)
        try:
            with retained.begin():
                before = measure_pending_spool_before_anchor(source=pending_root, scope=scope, database_identity=identity,
                    source_commit_seq=source_commit, timeout_seconds=10, max_entries=64, check_held=lambda: fence.assert_quiet(retained))
            provider = {"schema": COLLECTOR_RECEIPT_VERSION, "phase": "drained",
                        "evidence": {"source_commit_seq": source_commit, "pending_spool": before}}
            provider_hash = archive_evidence_hash(provider)
            save_receipt(receipts / f"collector-drained-{provider_hash}.json", provider | {"sha256": provider_hash}, initial=True)
            with ThreadPoolExecutor(max_workers=1) as threads, archive_namespace(objects.root, exclusive=True) as check_archive:
                result = threads.submit(qualify_receiver, prepared)
                def remaining():
                    if result.done():
                        result.result()
                    return fence.deadline - monotonic()

                report = recover_source(fence, control, source_commit, provider_hash, remaining, check_archive)
                reply = result.result(timeout=10)
            assert reply["archive_closure"] == report["archive_closure"]
            assert report["archive_closure"]["receiver_runtime_sha256"] == reply["runtime_sha256"]
            coverage = report["archive_closure"]["coverage"]
            assert coverage["final_objects"] == len(raw_ids)
            if replayed:
                wal = load_receipt(receiving / "encrypted-wal-transfer.json", max_bytes=65536)
                assert wal["schema"] == "qt.encrypted_recovery_wal_transfer.v2"
                assert wal["checkpoint_sha256"] == site.replayed_checkpoint_sha256 == report["archive_closure"]["checkpoint_sha256"]
                assert wal["wal_after"] == generated[-1] and 0 < len(wal["files"]) <= site.wal.files
                assert coverage["baseline_objects_used"] == 2 and coverage["verified_suffix_objects"] == 1
                print(f"QT_SIGNED_REPLAYED_FINAL archive_baseline=2 archive_suffix=1 wal_final_files={len(wal['files'])} "
                      f"wal_after={wal['wal_after']} final_wal={wal['wal_transferred_through']} closure={report['archive_closure']['closure_sha256']}")
            assert report["pending_spool_verified"]["pre_anchor_sha256"] == before["source_sha256"]
            response = load_receipt(receiving / "named-recovery-response.json", max_bytes=1024**2)
            response["body"]["request_sha256"] = "f" * 64
            damaged = receipts / "forged-response.json"
            save_receipt(damaged, response, initial=True)
            with pytest.raises(ValueError, match="signature_mismatch"):
                _opened(damaged, site.receiver_public_key)
            def exact_reply(*_):
                return report
            exact_reply.assert_receiver_ready = lambda: ready
            if replayed:
                exact_reply.wal_baseline = base_native
                exact_reply.restart_budget = site.max_restart_segments
            binding = bind_approved_recovery_checks(package_path=package_path, package=package,
                approved_package_hash=approved_package_hash, receipt_root=receipts,
                verify_site_restore=exact_reply, expected_pending_scope_sha256=scope_hash)
            assert binding.assert_receiver_ready() == prepared.digest
            digest = binding(None, None, source_commit, provider_hash, lambda: 10, lambda: None)
            published = load_receipt(receipts / f"independent-recovery-{digest}.json", max_bytes=4 * 1024**2)
            assert published["receiver_preflight_sha256"] == sizing["measurement_sha256"]
            assert published["receiver_response"] == report["receiver_response"]
            assert reply["prepared_receiver_sha256"] == prepared.digest
            with pytest.raises(ValueError, match="live_prepared_receiver_required"):
                recover_named_site(prepared=prepared, **receiver_options)
        finally:
            reopen_database_fence(fence, control, expected_outcome="original", read_outcome=lambda _: "original", record=lambda *_: None)
    if rolling_prefix:
        with engine.connect() as conn:
            assert conn.exec_driver_sql(_SOURCE_POSTMASTER).one() == source_postmaster
        print("QT_ROLLING_WAL_SOURCE_STABLE exact_restored_probe_rows=5 source_postmaster_and_timeline=unchanged")


def _prepare_native_repository(engine, identity, tmp_path, monkeypatch):
    hdd = Path("/qt-history")
    udev = tmp_path / "backup-udev"
    udev.mkdir()
    device = hdd.stat().st_dev
    (udev / f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=postwrite-backup-fixture\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    secrets_root = Path("/qt-incremental-secrets")
    access = secrets_root / "postwrite-access"
    access.mkdir(mode=0o700)
    for name in ("database-key", "archive-key"):
        key = access / name
        key.touch(mode=0o600, exist_ok=False)
        key.write_text(secrets.token_hex(32))
    config = IncrementalRecoveryConfig(
        Path("/usr/local/bin/pgbackrest"), Path("/usr/local/bin/restic"),
        Path("/qt-source/pgdata"), Path("/var/run/postgresql"),
        access / "database-key", access / "archive-key", max_chain_backups=2,
    )
    limits = dict(
        target=StorageTarget("hdd", "Disposable", "postwrite-backup-fixture", str(hdd), "hdd"),
        database_identity=identity, max_bytes=128*1024**2, reserve_bytes=8*1024**2,
        max_objects=100, timeout_seconds=120,
    )
    archiver = access / "pgbackrest.conf"
    prepare_encrypted_repository(incremental=config, connection_url=engine.url,
                                 archiver_config=archiver, **limits)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        from psycopg2 import sql
        archive_command = f"pgbackrest --config={archiver} --stanza=qt archive-push %p"
        with conn.connection.driver_connection.cursor() as cursor:
            cursor.execute(sql.SQL("ALTER SYSTEM SET archive_command = {}").format(sql.Literal(archive_command)))
        conn.exec_driver_sql("SELECT pg_reload_conf()")
    return EncryptedRecoveryCopies(incremental=config, connection_url=engine.url, **limits)


def _pending_level2(storage, structures, root):
    product = "BIP-FIXTURE"
    definition = _definition(storage, structures, "level2", product)
    claim = structures.claim_stream(definition_id=definition, owner_id="named-pending-fixture", lease_seconds=60, bounded=True)
    structures.append_session_event(claim, event_ordinal=0, connection_epoch=1, event_type="session_started",
                                    occurred_at=datetime.now(UTC))
    messages = list(_messages(claim, "level2", resumed=False, inject_gap=False))
    records, total = [], 0
    for ordinal, frames in enumerate((messages[:2], messages[2:])):
        segment = DurableRawSpoolSegment(root=root, definition_id=definition, session_id=claim.session_id,
                                         connection_epoch=1, segment_ordinal=ordinal)
        for frame in frames:
            message = ProviderRawMessage.build(provider=frame.provider, venue=frame.venue,
                stream_session_id=claim.session_id, connection_epoch=1, receive_ordinal=frame.receive_ordinal,
                received_at=frame.received_at, raw_frame=frame.raw_frame)
            channel = json.loads(message.raw_frame)["channel"]
            record = RawStreamRecord.from_provider_message(message, definition_id=definition,
                spool_segment_id=segment.spool_segment_id, provider_product_id=product,
                requested_channel="level2", observed_channel="level2" if channel == "l2_data" else channel)
            segment.append(record)
            records.append(record)
        total += segment.seal().stat().st_size
    failed = structures.append_session_event(claim, event_ordinal=1, connection_epoch=1, event_type="failed",
        occurred_at=datetime.now(UTC), reason="fixture unavailable before sealed tail admission")
    structures.release(claim)
    scope = PendingSpoolScope(definition, claim.session_id, 1, claim.provider, claim.venue, product, "level2", 2, total)
    return scope, records, failed


def _later_trade(structures, root, claim, record, *, trade_id="after-physical-endpoint"):
    late = structures.claim_stream(definition_id=claim.definition_id, owner_id="post-point-fixture",
                                   lease_seconds=60, bounded=True)
    structures.append_session_event(late, event_ordinal=0, connection_epoch=0, event_type="session_started",
                                    occurred_at=record.received_at)
    structures.release(late)
    _, accepted = _seal_trade(root, late, trade_id=trade_id)
    _native_recover(structures, root)
    return accepted


def _copy_repository(worker, selected):
    restore_root = Path("/qt-restore")
    assert not list(restore_root.iterdir())
    copied = restore_root / "repository"
    copied.mkdir(mode=0o700)
    receipt = worker.root / selected["name"] / "complete.json"
    limits = TransferLimits(seconds=40, bytes=128 * 1024**2, files=10_000, reserve_bytes=16 * 1024**2)
    options = dict(source=worker.root, receiving=copied, generation=selected["name"],
                   receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(), limits=limits)
    with worker.lock():
        with pytest.raises(RuntimeError, match="source_writer_busy"):
            stage_handoff(**options)
        with pytest.raises(RuntimeError, match="source_writer_busy"):
            preflight.measure_source_prestage(**{key: value for key, value in options.items() if key != "receiving"})
    measured = preflight.measure_source_prestage(**{key: value for key, value in options.items() if key != "receiving"})
    transfer = stage_handoff(**options)
    assert transfer["encrypted_files"] > 0 and transfer["encrypted_bytes"] > 0
    assert transfer["encrypted_bytes"] >= measured["encrypted_bytes"]
    assert (copied / "encrypted-copy.jsonl").stat().st_size >= measured["manifest_bytes"]
    assert verify_staged_handoff(receiving=copied, transfer_receipt_sha256=transfer["transfer_receipt_sha256"],
                                 limits=limits) == transfer
    metadata = copied / "database" / "backup" / "qt" / "backup.info"
    with metadata.open("r+b") as handle:
        first = handle.read(1)
        assert first
        try:
            handle.seek(0)
            handle.write(bytes([first[0] ^ 0xff]))
            handle.flush()
            with pytest.raises(RuntimeError, match="receiving_ciphertext_changed"):
                verify_staged_handoff(receiving=copied, transfer_receipt_sha256=transfer["transfer_receipt_sha256"],
                                      limits=limits)
        finally:
            handle.seek(0)
            handle.write(first)
    print(f"QT_PAIRED_TRANSFER files={transfer['encrypted_files']} bytes={transfer['encrypted_bytes']}")
    return restore_root, copied, copied / "complete.json", transfer


def _restored_access(worker, copied):
    # The postmaster runs in a separate container without the client's
    # pgBackRest environment. Supply the restored repository's key separately.
    access = copied.parent / "recovered-access"
    access.mkdir(mode=0o700)
    settings = access / "pgbackrest.conf"
    settings.touch(mode=0o600, exist_ok=False)
    key = worker.config.database_key_path.read_text()
    settings.write_text(
        f"[global]\nrepo1-path={copied / 'database'}\nrepo1-cipher-type=aes-256-cbc\n"
        f"repo1-cipher-pass={key}\nlock-path={access}\nlog-level-file=off\n"
        "[qt]\npg1-path=/qt-restore/pgdata\n"
    )
    return settings


def _restore_database(worker, copied, selected, *recovery_target):
    settings = _restored_access(worker, copied)
    worker._run(worker._br("--repo1-path=" + str(copied / "database"), "--pg1-path=/qt-restore/pgdata",
                "--tablespace-map-all=/qt-restore/history", "--set=" + selected["database_label"],
                f'--recovery-option=restore_command=/usr/local/bin/pgbackrest --config={settings} --stanza=qt archive-get %f "%p"',
                *recovery_target, "--target-action=promote", "--archive-mode=off", "restore"))


def _wait_for_restored_cluster(url, *, expected_identity=None):
    probe = create_engine(url, connect_args={"connect_timeout": 2})
    deadline = monotonic() + 45
    try:
        while True:
            try:
                with probe.connect() as conn:
                    if not conn.scalar(text("SELECT pg_is_in_recovery()")):
                        if expected_identity is not None:
                            assert _identity(conn)[0] == expected_identity
                        return
            except OperationalError:
                if monotonic() >= deadline:
                    raise
            assert monotonic() < deadline, "restored database did not promote by deadline"
            sleep(0.1)
    finally:
        probe.dispose()

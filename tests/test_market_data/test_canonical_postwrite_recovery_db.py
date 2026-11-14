"""Restore the activated cold layout at a real committed provider-write boundary."""

from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
from time import monotonic, sleep

import pytest
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
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, stage_handoff, verify_staged_handoff
from scripts.automation.storage_host_boundary import load_receipt, save_receipt
from scripts.automation.verify_encrypted_recovery_handoff import verify_handoff
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
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
        _check_named_receipt_package(tmp_path, target, pending_before, pending_source, pending_verified, closure)
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


def _check_named_receipt_package(tmp_path, target, before, source, received, closure):
    """Validate the real receiver receipts offline, using the fixture's measured provider input."""
    root = tmp_path / "independent-approval-fixture"
    root.mkdir(mode=0o700)
    scope_hash = archive_evidence_hash(before["scope"])
    plan = {"schema": PLAN_VERSION, "pending_spool_scope_sha256": scope_hash,
            **{key: closure[key] for key in ("base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256")}}
    plan_path = root / "recovery-plan.json"
    save_receipt(plan_path, plan, initial=True)
    plan_hash = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    package = {"source_commit_floor": closure["source_commit_seq"], "scope": {"target_database": target},
               "artifacts": {"independent_recovery": {"filename": plan_path.name, "sha256": plan_hash}}}
    drained = {"schema": COLLECTOR_RECEIPT_VERSION, "phase": "drained",
               "evidence": {"source_commit_seq": closure["source_commit_seq"], "pending_spool": before}}
    provider_hash = archive_evidence_hash(drained)
    save_receipt(root / f"collector-drained-{provider_hash}.json", drained | {"sha256": provider_hash}, initial=True)
    report = {"archive_closure": closure, "pending_spool_source": source, "pending_spool_verified": received}
    binding = bind_approved_recovery_checks(package_path=root / "package.json", package=package, receipt_root=root,
        verify_site_restore=lambda *_: report, expected_pending_scope_sha256=scope_hash)
    digest = binding(None, None, closure["source_commit_seq"], provider_hash, lambda: 10, lambda: None)
    published = load_receipt(root / f"independent-recovery-{digest}.json", max_bytes=4 * 1024**2)
    assert published["sha256"] == digest
    assert published["pending_spool_before_anchor_sha256"] == before["source_sha256"]
    assert {key: published[key] for key in report} == report


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
    transfer = stage_handoff(**options)
    assert transfer["encrypted_files"] > 0 and transfer["encrypted_bytes"] > 0
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

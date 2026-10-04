"""Preserving archive-file copy on the two owned disposable filesystems."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore
from data_providers.streams.contracts import ProviderRawMessage
from market_data.structure import RawStreamRecord
from portal.backend.service.market.continuous_stream_collector import (
    ContinuousMarketStructureCollector, CoinbaseMarketTradeProjectionAdapter,
)
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db import archive_root_v2_copy as archives
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db.fact_header_v2_capture import SCHEMA
from scripts.db import archive_reference_v2_placement as references
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_book_retention_db import _cold_book_handoff
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement, _assert_disk
from tests.test_market_data.test_fact_header_copy_db import _insert
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_archive_reference_placement_db import _options, _seed_retained_source
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned SSD/HDD storage-demo topology"),
]


def _hashes(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*") if path.is_file()}


def _pending_trade_spool(storage, book, source):
    """Leave one genuinely unpublished trade on the preserved working drive."""
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
        contract_version="market.trade.v1", timeframe_seconds=None)
    structures = book.structures
    structures.upsert_stream_definition(definition_id="handoff-pending-spool",
        source_id=book.source_id, series_id=series, provider=book.source.provider,
        venue=book.source.venue, provider_product_id="BTC-USD", channels=("market_trades",),
        auth_mode="public", contract_version="market.trade.v1",
        max_spool_bytes=1024**3, max_segment_bytes=128*1024**2,
        config={"product_definition_version_id": book.claim.config["product_definition_version_id"]})
    claim = structures.claim_stream(definition_id="handoff-pending-spool",
        owner_id="before-handoff", lease_seconds=600, bounded=True)
    structures.append_session_event(claim, event_ordinal=0, connection_epoch=0,
        event_type="connected", occurred_at=BASE)
    segment = DurableRawSpoolSegment(root=source/"spool", definition_id=claim.definition_id,
        session_id=claim.session_id, connection_epoch=0, segment_ordinal=0)
    timestamp = (BASE+timedelta(seconds=10)).isoformat()
    message = ProviderRawMessage.build(provider=book.source.provider, venue=book.source.venue,
        stream_session_id=claim.session_id, connection_epoch=0, receive_ordinal=1,
        received_at=timestamp, raw_frame=json.dumps({
            "channel": "market_trades", "timestamp": timestamp, "sequence_num": 1,
            "events": [{"type": "update", "trades": [{
                "product_id": "BTC-USD", "trade_id": "handoff-pending-trade",
                "price": "100", "size": "0.01", "side": "BUY", "time": timestamp}]}]}))
    record = RawStreamRecord.from_provider_message(message, definition_id=claim.definition_id,
        spool_segment_id=segment.spool_segment_id, provider_product_id="BTC-USD",
        requested_channel="market_trades", observed_channel="market_trades")
    segment.append(record)
    segment.close()
    with segment.open_path.open("ab") as handle:
        handle.write(b'{"partial"')
        handle.flush()
        os.fsync(handle.fileno())
    structures.release(claim)
    return claim, segment, record


def test_archive_copy_resumes_and_serves_frozen_history_from_hdd_only(storage, tmp_path, monkeypatch):
    assert os.getuid() == 70 and os.getenv("QT_DB_TEST_ISOLATED") == "1"
    source = Path("/qt-source/pgdata") / ("archive-copy-"+uuid4().hex)
    source.mkdir()
    book = _cold_book_handoff(storage, source, monkeypatch, split_sources=False)
    pending_claim, pending_spool, pending_record = _pending_trade_spool(storage, book, source)
    pending_bytes = pending_spool.open_path.read_bytes()
    service = MarketStructureService(repository=book.structures)
    replay_args = dict(definition_id=book.claim.definition_id, session_id=book.claim.session_id)
    replay = service.replay_book_session(**replay_args, storage_root=source)
    with storage.database.session() as session:
        pairs = session.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
        ranges = session.execute(text("""
            SELECT series_id,min(observation_time),max(observation_time)
            FROM market.fact_versions GROUP BY series_id
        """)).all()
    frozen = {tuple(pair): storage.repo.read_dataset_fact_revisions(
        dataset_id=pair[0], series_id=pair[1]) for pair in pairs}
    reads = {(series,start,end): storage.repo.read_facts(series_id=series,
        start=start-timedelta(seconds=1), end=end+timedelta(seconds=1))
        for series,start,end in ranges}
    source_objects = source / "objects"
    before = _hashes(source_objects)
    assert before
    storage.open_day = storage.today
    _placement(monkeypatch, storage.open_day)
    restore_tiered_v1_fixture(storage)
    _configure_placement(storage, tmp_path, monkeypatch)
    storage.copy_plan = replace(storage.copy_plan, history_before=storage.today-timedelta(days=30))
    engine = storage.database._engine
    destination = Path("/qt-history") / ("archive-copy-"+uuid4().hex)
    destination.mkdir()
    objects = destination / "objects"
    objects.mkdir()
    # This is the isolated migration process's intended destination. Existing
    # source records and configuration in the database are never rewritten.
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(destination))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", storage.copy_plan.history.filesystem_uuid)
    options = dict(source_root=source_objects, destination_root=objects,
                   max_page_bytes=32*1024**2, **_options(storage))
    options["policy"] = replace(options["policy"], movement_enabled=True, backup_enabled=True)
    staging = dict(placement=storage.copy_plan, page_rows=2, **options)
    finishing = dict(staging, max_objects=100, max_bytes=32*1024**2)
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        handoff.finish_database_handoff(engine, cancelled=lambda: True, **finishing)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": headers.STATE}) is None
    prepared_reply_lost = [False]
    original_prepare = raw.prepare_copy
    original_prepare_commit = Connection._commit_impl
    def mark_prepared(conn, **kwargs):
        result = original_prepare(conn, **kwargs)
        conn.info["fixture_staging_preparation"] = True
        return result
    def lose_preparation_reply(conn):
        prepared = conn.info.pop("fixture_staging_preparation", False)
        original_prepare_commit(conn)
        if prepared and not prepared_reply_lost[0]:
            prepared_reply_lost[0] = True
            raise RuntimeError("injected_staging_preparation_reply_lost")
    with monkeypatch.context() as uncertainty:
        uncertainty.setattr(raw, "prepare_copy", mark_prepared)
        uncertainty.setattr(Connection, "_commit_impl", lose_preparation_reply)
        with pytest.raises(RuntimeError, match="injected_staging_preparation_reply_lost"):
            handoff.finish_database_handoff(engine, **finishing)
    assert prepared_reply_lost[0]
    with engine.connect() as conn:
        original_attempt = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture WHERE id=1"))
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE} WHERE id=1")) == 0
        assert conn.scalar(text(f"SELECT verified_rows FROM {raw.STATE} WHERE id=1")) == 0
    family = "raw_archive_manifests"

    with pytest.raises(RuntimeError, match="root_wrong_filesystem"):
        archives.copy_archive_page(engine, family=family,
            **(options | {"source_root": objects}))
    with engine.begin() as blocker:
        blocker.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                        {"name": _LIFECYCLE_LOCK_NAME})
        with pytest.raises(RuntimeError, match="archive_copy_expiry_busy"):
            archives.copy_archive_page(engine, family=family, **options)
    with pytest.raises(RuntimeError, match="page_byte_budget_exceeded"):
        archives.copy_archive_page(engine, family=family,
                                   **(options | {"max_page_bytes": 1}))
    # Exceed this fixture filesystem's capacity without allocating it. The
    # history drive may be the small CI tmpfs or an isolated physical HDD path.
    filesystem = os.statvfs(objects)
    constrained = {**options["resource_limits"],
        "maintenance_bytes": {"ssd": 1024**2,
                              "hdd": filesystem.f_blocks * filesystem.f_frsize}}
    with pytest.raises(RuntimeError, match="capacity_blocked"):
        archives.copy_archive_page(engine, family=family,
                                   **(options | {"resource_limits": constrained}))
    assert not _hashes(objects)
    assert _hashes(source_objects) == before

    # Corrupt source bytes must never get a valid destination acknowledgement.
    with engine.connect() as conn:
        key = conn.scalar(text("SELECT object_key FROM market.raw_archive_manifests ORDER BY id LIMIT 1"))
    candidate = source_objects / key
    original = candidate.read_bytes()
    candidate.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    try:
        with pytest.raises(ValueError, match="source checksum mismatch"):
            archives.copy_archive_page(engine, family=family, **options)
        assert not (objects / key).exists()
    finally:
        candidate.write_bytes(original)

    # Interrupt after a durable object is published. Source collection remains
    # writable under the archive-expiry and storage-job fences.
    published = []
    cancel = [False]
    original_put = FilesystemRawArchiveObjectStore.put_verified

    def interrupt(self, **kwargs):
        result = original_put(self, **kwargs)
        published.append((result.object_key, self.local_path(result.object_key).stat().st_ino))
        with engine.begin() as writer:
            writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
            _insert(writer, storage, "during-archive-copy")
        cancel[0] = True
        return result

    with monkeypatch.context() as interruption:
        interruption.setattr(FilesystemRawArchiveObjectStore, "put_verified", interrupt)
        with pytest.raises(RuntimeError, match="storage_move_cancelled"):
            archives.copy_archive_page(engine, family=family, cancelled=lambda: cancel[0], **options)
    assert len(published) == 1
    assert _hashes(source_objects) == before
    assert not list(objects.rglob("*.partial"))
    resumed = archives.copy_archive_page(engine, family=family, **options)
    assert resumed["reused_objects"] >= 1 and resumed["copied_objects"] >= 1
    assert (objects / published[0][0]).stat().st_ino == published[0][1]
    assert resumed["source_preserved"] and not resumed["migration_ready"]

    for family in archives.FAMILIES:
        cursor = ""
        while True:
            result = archives.copy_archive_page(engine, family=family, after_id=cursor,
                                                page_rows=2, **options)
            assert result["final_fenced_inventory_verification_required"]
            if result["page_objects"] < 2:
                break
            assert result["next_after_id"] > cursor
            cursor = result["next_after_id"]
    copied = _hashes(objects)
    assert copied and copied == {key: before[key] for key in copied}
    assert _hashes(source_objects) == before

    verification = {key: value for key, value in options.items() if key != "max_page_bytes"}
    verification.update(max_objects=100, max_bytes=32*1024**2, page_rows=2)
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, **verification) as baseline_inventory:
            assert baseline_inventory["verified_catalog_objects"] >= len(copied)
            assert not baseline_inventory["root_activation_authorized"]
            # Ordinary readers remain usable. A new catalog publisher cannot
            # pass the final transaction fence.
            with engine.connect() as reader:
                assert reader.scalar(text("SELECT count(*) FROM market.raw_archive_manifests")) > 0
            with pytest.raises(DBAPIError) as busy, engine.begin() as writer:
                writer.exec_driver_sql("LOCK TABLE market.raw_archive_manifests IN ROW EXCLUSIVE MODE NOWAIT")
            assert getattr(busy.value.orig, "pgcode", None) == "55P03"

    # Genuine raw publication after the baseline page pass. The final verifier
    # must rescan the entire catalog, regardless of the earlier copy cursors.
    with monkeypatch.context() as publisher:
        publisher.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(source))
        publisher.setenv("QT_MARKET_DATA_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
        late = _raw_book_fixture(storage, source, publisher, definition_id="archive-copy-late",
                                 provider_product_id="BTC-USD-LATE",
                                 event_start=BASE+timedelta(hours=1))
    with engine.connect() as conn:
        late_keys = conn.execute(text("""
            SELECT object_key FROM market.raw_archive_manifests WHERE id=ANY(:ids)
        """), {"ids": late.manifests}).scalars().all()
    assert late_keys and all(not (objects/key).exists() for key in late_keys)
    with engine.begin() as conn:
        # A failed verification cannot poison unrelated caller work or keep
        # this context's publisher fence held until the outer transaction ends.
        conn.exec_driver_sql("CREATE TEMP TABLE archive_inventory_probe(value integer)")
        conn.exec_driver_sql("INSERT INTO archive_inventory_probe VALUES (1)")
        with pytest.raises(RuntimeError, match="archive_inventory_object_missing"):
            with archives.verified_archive_inventory(conn, **verification):
                pytest.fail("incomplete archive inventory admitted")
        assert conn.scalar(text("SELECT value FROM archive_inventory_probe")) == 1
        with engine.begin() as writer:
            writer.exec_driver_sql("LOCK TABLE market.raw_archive_manifests IN ROW EXCLUSIVE MODE NOWAIT")
    recopy = archives.copy_archive_page(engine, family="raw_archive_manifests", **options)
    assert recopy["copied_objects"] == len(late_keys)
    after_late = _hashes(source_objects)
    assert {key: after_late[key] for key in before} == before
    before = after_late
    copied = _hashes(objects)

    # A live catalog writer refuses final admission without waiting. No fence
    # survives the failed savepoint; retry after that writer ends is possible.
    with engine.begin() as writer:
        writer.exec_driver_sql("LOCK TABLE market.raw_archive_manifests IN ROW EXCLUSIVE MODE")
        with pytest.raises(DBAPIError) as busy, engine.begin() as conn:
            with archives.verified_archive_inventory(conn, **verification):
                pytest.fail("busy publisher admitted")
        assert getattr(busy.value.orig, "pgcode", None) == "55P03"
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="verification_budget_exceeded"):
            with archives.verified_archive_inventory(conn, **(verification | {"max_objects": 1})):
                pytest.fail("inventory budget ignored")
    damaged = objects/late_keys[0]
    valid_bytes = damaged.read_bytes()
    damaged.write_bytes(bytes([valid_bytes[0] ^ 1])+valid_bytes[1:])
    try:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="archive_inventory_object_mismatch"):
                with archives.verified_archive_inventory(conn, **verification):
                    pytest.fail("corrupt destination admitted")
    finally:
        damaged.write_bytes(valid_bytes)

    # The fixed commit boundary owns complete verification and its transaction.
    # Publisher drain/root activation and full-volume qualification stay separate.
    _seed_retained_source(engine)
    from scripts.db import archive_reference_v2_placement as retained_move
    stage_pages = [0]
    def interrupt_staging(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO "+SCHEMA+".fact_versions "):
            _assert_disk(conn, retained_move.RETAINED_LEGACY, Path("/qt-history"))
            stage_pages[0] += 1
            if stage_pages[0] == 2:
                with engine.begin() as killer:
                    assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                        {"pid": conn.connection.driver_connection.get_backend_pid()})
                conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", interrupt_staging)
    try:
        with pytest.raises(DBAPIError):
            handoff.finish_database_handoff(engine, **finishing)
    finally:
        event.remove(engine, "after_cursor_execute", interrupt_staging)
    assert stage_pages[0] == 2
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE} WHERE id=1")) > 0
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions")) > 0
        _insert(conn, storage, "collection-between-staging-attempts")
    # Raw staging must not overlap the full identity copy on SSD. Interrupt
    # after the committed relocation, then admit a new source record and resume.
    original_raw_copy = raw.copy_page
    observed_history = []
    def interrupt_before_raw_staging(conn, **kwargs):
        _assert_disk(conn, SCHEMA+".fact_identities", Path("/qt-history"))
        observed_history.append(headers._inspect_progress(conn)["identity_history_ready"])
        raise RuntimeError("fixture_interrupt_after_identity_relocation")
    with monkeypatch.context() as interrupted:
        interrupted.setattr(raw, "copy_page", interrupt_before_raw_staging)
        with pytest.raises(RuntimeError, match="fixture_interrupt_after_identity_relocation"):
            handoff.stage_handoff(engine, **staging)
    assert observed_history == [True]
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture WHERE id=1")) == original_attempt
        _assert_disk(conn, SCHEMA+".fact_identities", Path("/qt-history"))
        _insert(conn, storage, "collection-after-identity-relocation")
    def raw_copy_without_identity_overlap(conn, **kwargs):
        _assert_disk(conn, SCHEMA+".fact_identities", Path("/qt-history"))
        return original_raw_copy(conn, **kwargs)
    with monkeypatch.context() as resumed:
        resumed.setattr(raw, "copy_page", raw_copy_without_identity_overlap)
        staged = handoff.stage_handoff(engine, **staging)
    assert staged["staging_pass_complete"] and not staged["migration_ready"]
    assert not staged["database_handoff_committed"] and not staged["collection_resume_authorized"]
    assert handoff.stage_handoff(engine, **staging) == staged
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture WHERE id=1")) == original_attempt
    inspect_options = dict(policy=options["policy"], source_root=source_objects,
                           destination_root=objects)
    activation_options = {**inspect_options, "resource_limits": options["resource_limits"]}
    with pytest.raises(RuntimeError, match="policy_database_handoff_required"):
        handoff.activate_handoff_policy(engine, **activation_options)
    with engine.connect() as conn:
        original_oids = [handoff._oid(conn, relation) for relation in (headers.SOURCE, raw.SOURCE)]

    # Kill the actual PG backend after the first source rename. All schema and
    # certificate changes must roll back; completed archive files stay reusable.
    killed = [False]
    def kill_mid_switch(conn, cursor, statement, parameters, context, executemany):
        if not killed[0] and statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
            killed[0] = True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", kill_mid_switch)
    try:
        with pytest.raises(DBAPIError):
            handoff.finish_database_handoff(engine, **finishing)
    finally:
        event.remove(engine, "after_cursor_execute", kill_mid_switch)
    assert killed[0]
    with engine.begin() as conn:
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        assert not handoff.inspect_handoff(conn, **inspect_options)["database_handoff_committed"]
        assert [handoff._oid(conn, relation) for relation in (headers.SOURCE, raw.SOURCE)] == original_oids
        conn.exec_driver_sql("LOCK TABLE market.fact_versions IN ROW EXCLUSIVE MODE NOWAIT")

    # A real commit followed by a lost response must be resolved by inspection,
    # never by replaying DDL or restarting the old version.
    original_commit = Connection._commit_impl
    lost = [False]
    pending = [False]
    def mark_switch(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
            conn.info["fixture_handoff_switch"] = True
    def committed_then_lost(conn):
        if not conn.info.pop("fixture_handoff_switch", False):
            return original_commit(conn)
        if not lost[0]:
            # The switch is fully staged but not yet committed. A second
            # connection cannot interpret its invisible certificate as rollback.
            # Roll back this observer explicitly to avoid recursively committing
            # through the fault hook itself.
            with engine.connect() as observer:
                transaction = observer.begin()
                try:
                    observer.exec_driver_sql("SET TRANSACTION READ ONLY")
                    observer.exec_driver_sql("SET LOCAL statement_timeout='5s'")
                    with pytest.raises(RuntimeError, match="handoff_outcome_pending"):
                        handoff.inspect_handoff(observer, **inspect_options)
                    pending[0] = True
                finally:
                    transaction.rollback()
        original_commit(conn)
        if not lost[0]:
            lost[0] = True
            raise RuntimeError("injected_handoff_commit_reply_lost")
    event.listen(engine, "after_cursor_execute", mark_switch)
    try:
        with monkeypatch.context() as fault:
            fault.setattr(Connection, "_commit_impl", committed_then_lost)
            with pytest.raises(RuntimeError, match="injected_handoff_commit_reply_lost"):
                handoff.finish_database_handoff(engine, **finishing)
    finally:
        event.remove(engine, "after_cursor_execute", mark_switch)
    assert lost[0] and pending[0]
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        committed = handoff.inspect_handoff(conn, **inspect_options)
        assert committed["database_handoff_committed"] and not committed["collection_resume_authorized"]
        receipt = committed["receipt"]
        assert receipt["verified_archive_objects"] > baseline_inventory["verified_catalog_objects"]
        assert receipt["archive_inventory_sha256"] != baseline_inventory["inventory_sha256"]
    original_stage_handoff = handoff.stage_handoff
    def no_replayed_schema(*args, **kwargs):
        pytest.fail("committed database staging/switch replayed")
    monkeypatch.setattr(handoff, "stage_handoff", no_replayed_schema)
    monkeypatch.setattr(handoff, "commit_handoff", no_replayed_schema)
    with pytest.raises(RuntimeError, match="sequence_placement_changed"):
        handoff.finish_database_handoff(engine, **(finishing | {
            "placement": replace(storage.copy_plan,
                                 history_before=storage.copy_plan.history_before-timedelta(days=1))}))
    # First policy is a separate, supervised atomic transaction after the
    # database commit. Services remain paused; no host/runtime resume is implied.
    with engine.begin() as conn:
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        assert not handoff.inspect_handoff_policy(conn, **inspect_options)["policy_activated"]
    with pytest.raises(ValueError, match="automatic_history_and_recovery_required"):
        handoff.activate_handoff_policy(engine, **(activation_options | {
            "policy": replace(options["policy"], backup_enabled=False)}))
    with pytest.raises(RuntimeError, match="receipt_mismatch"):
        handoff.activate_handoff_policy(engine, **(activation_options | {
            "policy": replace(options["policy"], recent_days=31)}))

    # The intended cutoff alone is insufficient if actual placement drifted.
    # Move a recent heap to the owned HDD and require refusal, then put it back.
    recent_relation = "market.fact_versions_" + storage.today.strftime("%Y%m%d")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"ALTER TABLE {recent_relation} SET TABLESPACE {storage.copy_history_name}")
    try:
        with pytest.raises(RuntimeError, match="policy_recent_files_not_on_ssd"):
            handoff.activate_handoff_policy(engine, **activation_options)
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"ALTER TABLE {recent_relation} SET TABLESPACE pg_default")

    # Kill a real PG backend after policy insertion. Targets, registration,
    # policy and completed plan all roll back, retaining the committed database.
    policy_killed = [False]
    def kill_policy(conn, cursor, statement, parameters, context, executemany):
        if not policy_killed[0] and statement.startswith("INSERT INTO portal_storage_policy"):
            policy_killed[0] = True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", kill_policy)
    try:
        with pytest.raises(DBAPIError):
            handoff.finish_database_handoff(engine, **finishing)
    finally:
        event.remove(engine, "after_cursor_execute", kill_policy)
    assert policy_killed[0]
    with engine.begin() as conn:
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        assert handoff.inspect_handoff(conn, **inspect_options)["database_handoff_committed"]
        assert not handoff.inspect_handoff_policy(conn, **inspect_options)["policy_activated"]
        for table in ("portal_storage_targets", "portal_storage_header_tablespaces", "portal_storage_policy", "portal_storage_plans"):
            assert conn.scalar(text("SELECT count(*) FROM public."+table)) == 0

    policy_lost = [False]
    policy_pending = [False]
    def mark_policy_transaction(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO portal_storage_policy"):
            conn.info["qt_test_initial_policy_transaction"] = True
    def policy_committed_then_lost(conn):
        if not conn.info.pop("qt_test_initial_policy_transaction", False):
            return original_commit(conn)
        if not policy_lost[0]:
            with engine.connect() as observer:
                transaction = observer.begin()
                try:
                    observer.exec_driver_sql("SET TRANSACTION READ ONLY")
                    observer.exec_driver_sql("SET LOCAL statement_timeout='5s'")
                    with pytest.raises(RuntimeError, match="outcome_pending"):
                        handoff.inspect_handoff_policy(observer, **inspect_options)
                    policy_pending[0] = True
                finally:
                    transaction.rollback()
        original_commit(conn)
        if not policy_lost[0]:
            policy_lost[0] = True
            raise RuntimeError("injected_policy_commit_reply_lost")
    event.listen(engine, "after_cursor_execute", mark_policy_transaction)
    try:
        with monkeypatch.context() as fault:
            fault.setattr(Connection, "_commit_impl", policy_committed_then_lost)
            with pytest.raises(RuntimeError, match="injected_policy_commit_reply_lost"):
                handoff.finish_database_handoff(engine, **finishing)
    finally:
        event.remove(engine, "after_cursor_execute", mark_policy_transaction)
    assert policy_lost[0] and policy_pending[0]
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        policy_result = handoff.inspect_handoff_policy(conn, **inspect_options)
        assert policy_result["policy_activated"] and policy_result["policy_current"]
        assert policy_result["policy_revision"] == 1 and not policy_result["collection_resume_authorized"]
        assert conn.scalar(text("SELECT count(*) FROM public.portal_storage_targets")) == 2
        assert conn.scalar(text("SELECT count(*) FROM public.portal_storage_header_tablespaces")) == 1
    monkeypatch.setattr(handoff, "activate_handoff_policy", no_replayed_schema)
    retry = handoff.finish_database_handoff(engine, **finishing)
    assert retry["database_sequence_complete"] and not retry["collection_resume_authorized"]
    assert retry["policy_revision"] == 1 and retry["plan_id"] == policy_result["plan_id"]
    with engine.connect() as conn, conn.begin() as transaction:
        conn.exec_driver_sql("UPDATE public.portal_storage_policy SET revision=2")
        changed = handoff.inspect_handoff_policy(conn, **inspect_options)
        assert changed["policy_activated"] and not changed["policy_current"]
        transaction.rollback()
    # Exercise the real automatic-history runner against the operator-installed
    # registry/policy, without seeding either through a test-only shortcut.
    from portal.backend.service.storage.history_maintenance import run_history_maintenance
    maintained = run_history_maintenance(storage.database,
        pg_controldata=storage.copy_plan.pg_controldata, resource_limits=options["resource_limits"])
    assert maintained["state"] in ("idle", "completed"), maintained

    # Inspection remains possible after the attempt clock expires.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"UPDATE {SCHEMA}.capture SET prepared_at=clock_timestamp()-interval '25 hours'")
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        assert handoff.inspect_handoff(conn, **inspect_options)["receipt"] == receipt
        assert handoff.inspect_handoff_policy(conn, **inspect_options)["policy_current"]
    assert handoff.finish_database_handoff(engine, **finishing) == retry
    with pytest.raises(RuntimeError, match="migration_attempt_expired"):
        original_stage_handoff(engine, **staging)
    with engine.connect() as conn, conn.begin() as transaction:
        conn.exec_driver_sql("""
            UPDATE market.fact_storage_state SET evidence=jsonb_set(evidence,
                '{handoff,active_relation_oids,fact_versions}', '0'::jsonb)
            WHERE layout_version='market.fact_storage_tiers.v2'
        """)
        with pytest.raises(RuntimeError, match="relation_identity_changed"):
            handoff.inspect_handoff(conn, **inspect_options)
        transaction.rollback()

    retained = source_objects.with_name("objects-retained")
    source_objects.rename(retained)
    assert not source_objects.exists()  # Archive readers cannot fall back to SSD.
    assert pending_spool.open_path.read_bytes() == pending_bytes
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(source))
    monkeypatch.setenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
    reader = FilesystemRawArchiveObjectStore(objects, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    for (dataset,series), expected in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=series) == expected
    for (series,start,end), expected in reads.items():
        assert storage.repo.read_facts(series_id=series, start=start-timedelta(seconds=1),
                                      end=end+timedelta(seconds=1)) == expected
    assert service.replay_book_session(**replay_args, storage_root=destination) == replay
    assert _hashes(retained) == before
    _placement(monkeypatch, storage.open_day)
    # The old archive root is unavailable, but the unchanged SSD working path
    # still contains a trade received before cutover and never published.
    recovery = ContinuousMarketStructureCollector(repository=book.structures)
    recovery_options = dict(definition={"id": pending_claim.definition_id},
        owner_id="after-handoff", lease_seconds=90, spool_root=source/"spool",
        object_store=FilesystemRawArchiveObjectStore(objects),
        temporary_root=source/"raw-staging", projection=CoinbaseMarketTradeProjectionAdapter())
    def before_canonical_ack(*args, **kwargs):
        raise RuntimeError("injected_pending_spool_publication_interruption")
    with monkeypatch.context() as interrupted:
        interrupted.setattr(book.structures, "ingest_trades", before_canonical_ack)
        with pytest.raises(RuntimeError, match="injected_pending_spool_publication_interruption"):
            recovery._recover_orphaned_spools_sync(**recovery_options)
    assert pending_spool.sealed_path.exists()  # Durable input survives failure.
    with engine.connect() as conn:
        key = conn.scalar(text("SELECT object_key FROM market.raw_archive_manifests WHERE session_id=:id"),
                          {"id": pending_claim.session_id})
        assert key and (objects/key).is_file()
        assert not (source_objects/key).exists()
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                           {"id": pending_claim.series_id}) == 0
    recovery._recover_orphaned_spools_sync(**recovery_options)
    assert not pending_spool.open_path.exists() and not pending_spool.sealed_path.exists()
    recovered = storage.repo.read_facts(series_id=pending_claim.series_id,
        start=BASE, end=BASE+timedelta(minutes=1))
    assert len(recovered) == 1
    recovery._recover_orphaned_spools_sync(**recovery_options)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT count(*) FROM market.raw_archive_record_mappings WHERE raw_record_id=:id"),
                           {"id": pending_record.raw_record_id}) == 1
        assert conn.scalar(text("SELECT count(*) FROM market.raw_archive_manifests WHERE session_id=:id"),
                           {"id": pending_claim.session_id}) == 1
    assert storage.repo.read_facts(series_id=pending_claim.series_id,
        start=BASE, end=BASE+timedelta(minutes=1)) == recovered
    for (dataset,series), expected in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=series) == expected
    assert _hashes(retained) == before
    recent = replace(storage.fact, observation_key="after-archive-root-handoff",
                     observation_time=BASE+timedelta(days=3))
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                     facts=[recent]).inserted_count == 1
    print("QT_ARCHIVE_ROOT_COPY_RESULT="+json.dumps({
        "source_preserved": True, "interrupted_copy_reused_verified_objects": True,
        "fixed_staging_fresh_prepare_lost_reply_reused_without_resetting_clock": prepared_reply_lost[0],
        "fixed_staging_resumed_after_real_backend_death_with_prior_pages_retained": True,
        "collection_between_staging_attempts_preserved": True,
        "repeated_staging_did_not_switch_or_authorize_resume": True,
        "pre_cutover_ssd_spool_recovered_to_hdd_and_canonical_v2": True,
        "recovery_interruption_retained_spool_and_retry_did_not_duplicate": True,
        "frozen_results_unchanged_after_pending_spool_recovery": True,
        "collection_during_copy_and_after_handoff": True,
        "recent_history_frozen_and_book_replay_with_original_root_unavailable": True,
        "wrong_filesystem_expiry_lock_byte_budget_low_capacity_and_corruption_refused": True,
        "late_publication_missing_and_corrupt_destination_refused": True,
        "final_catalog_fence_allows_reads_blocks_writes_and_releases_on_failure": True,
        "final_inventory_verified_objects": receipt["verified_archive_objects"],
        "mid_switch_backend_death_preserved_source": killed[0],
        "lost_commit_inspected_without_repeating_switch": lost[0],
        "inflight_commit_reports_pending": pending[0],
        "inspection_after_deadline_and_wrong_relation_refusal": True,
        "initial_policy_and_tablespace_registry_activated_atomically": True,
        "recent_files_on_wrong_drive_refused_before_activation": True,
        "policy_process_death_rolled_back_without_undoing_database_handoff": policy_killed[0],
        "policy_lost_commit_reconciled_without_revision_increment": policy_lost[0],
        "policy_inflight_outcome_pending": policy_pending[0],
        "automatic_history_consumed_operator_policy": maintained["state"],
        "copied_objects": len(copied), "copied_bytes": sum((objects/key).stat().st_size for key in copied),
        "limits": ["tiny disposable data", "page progress is not final inventory readiness; publisher drain/root activation remain separate",
                   "no production root switch, migration-duration or hardware qualification"]
    }, sort_keys=True))


def test_fixed_database_sequence_completes_and_refuses_later_policy_change(storage,tmp_path,monkeypatch):
    migration_window=7200
    from market_data.contracts import DatasetSeriesRequest
    facts = [replace(storage.fact,observation_key="sequence-"+str(i),
                     observation_time=BASE+timedelta(seconds=i)) for i in range(3)]
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=facts)
    request = DatasetSeriesRequest(storage.series_id,BASE-timedelta(seconds=1),BASE+timedelta(seconds=4))
    frozen = storage.repo.freeze_dataset([request])
    expected = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
    source = Path("/qt-working")/("sequence-"+uuid4().hex)/"objects"
    source.mkdir(parents=True)
    destination = Path("/qt-history")/("sequence-"+uuid4().hex)/"archives"/"objects"
    destination.mkdir(parents=True)
    storage.open_day = storage.today
    restore_tiered_v1_fixture(storage)
    _configure_placement(storage,tmp_path,monkeypatch)
    storage.copy_plan = replace(storage.copy_plan,history_before=storage.today-timedelta(days=30),
        history=replace(storage.copy_plan.history,root=str(destination.parent.parent)))
    assert not source.is_relative_to(Path(storage.copy_plan.recent.root))
    assert source.stat().st_dev == Path(storage.copy_plan.recent.root).stat().st_dev
    options = _options(storage)
    options["resource_limits"]["movement_timeout_seconds"]=migration_window
    options["policy"] = replace(options["policy"],movement_enabled=True,backup_enabled=True)
    options.update(placement=storage.copy_plan,source_root=source,destination_root=destination,
                   max_page_bytes=32*1024**2,max_objects=100,max_bytes=32*1024**2,page_rows=2)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT",str(destination.parent))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID",storage.copy_plan.history.filesystem_uuid)
    engine = storage.database._engine
    # Exercise the same packaged process boundary the host will invoke, with
    # no schema bootstrap and no process-local patching of migration functions.
    from dataclasses import asdict
    import subprocess
    import sys
    inventory = tmp_path/"operator-inventory.json"
    inventory.write_text(json.dumps({"schema_version":"qt.storage_inventory.v1",
        "targets":[asdict(storage.copy_plan.recent),asdict(storage.copy_plan.history)]}))
    with engine.connect() as conn:
        identity = conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text "
            "FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
    operator = dict(schema_version="qt.storage_database_operator.v1",
        source_revision=os.environ["QT_IMAGE_SOURCE_REVISION"],
        source_tree_hash=os.environ["QT_IMAGE_SOURCE_TREE_HASH"],
        database_identity=identity,inventory_path=str(inventory),
        policy=options["policy"].to_dict(),resource_limits=options["resource_limits"],
        history_before=storage.copy_plan.history_before.isoformat(),
        source_root=str(source),destination_root=str(destination),
        max_page_bytes=options["max_page_bytes"],max_objects=options["max_objects"],
        max_bytes=options["max_bytes"],page_rows=4096,max_duration_seconds=migration_window)
    child_env = {**os.environ,"PG_DSN":storage.dsn,
        "MARKET_STRUCTURE_WORKING_ROOT":str(source.parent),
        "QT_MARKET_DATA_WORKING_EXPECTED_UUID":storage.copy_plan.recent.filesystem_uuid}
    def invoke(payload):
        return subprocess.run([sys.executable,"-m","scripts.db.fact_header_v2_handoff"],
            input=json.dumps(payload),env=child_env,text=True,capture_output=True,timeout=650)
    oversized = invoke({**operator,"page_rows":4097})
    assert oversized.returncode and "storage_database_operator_budget_invalid" in oversized.stderr
    assert not (Path(storage.copy_plan.history.root)/"postgres").exists()
    # Wrong-cluster admission must not even create the destination tablespace.
    wrong = invoke({**operator,"database_identity":"1/1"})
    assert wrong.returncode and "storage_database_operator_database_changed" in wrong.stderr
    assert not (Path(storage.copy_plan.history.root)/"postgres").exists()
    first = invoke(operator)
    assert first.returncode == 0, first.stderr[-5000:]
    completed = json.loads(first.stdout)
    assert completed["database_sequence_complete"] and completed["policy_current"]
    assert completed["source_preserved"] and not completed["collection_resume_authorized"]
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id,series_id=storage.series_id) == expected
    again = invoke(operator)
    assert again.returncode == 0, again.stderr[-5000:]
    assert json.loads(again.stdout) == completed
    # Runtime admission must independently inspect the committed database and a
    # real completed copy, not accept the host's earlier operator result alone.
    from portal.backend.service.storage.recovery_maintenance import run_due_local_recovery
    from portal.backend.service.storage.recovery_copies import _snapshot_layout, _identity
    worker={"alive":True,"worker_id":"disposable-candidate",
        "context":{"storage_lifecycle":{"state":"running",
            "maintenance":{"history_movement":{"configured":True},"local_recovery":{"configured":True}},
            "last_run":{"local_recovery":{"state":"starting"}}}}}
    inspect=lambda:handoff.inspect_runtime_handoff(operator,engine=engine,worker=worker)
    assert inspect()=={"ready":False,"reason":"current_layout_recovery_pending"}
    recovery_root=Path(storage.copy_plan.history.root)/"recovery"
    assert not recovery_root.exists(),"inspection must not create recovery directories"
    with engine.connect() as conn:
        layout=_snapshot_layout(conn)
        _,namespace=_identity(conn)
    worker["context"]["storage_lifecycle"]["last_run"]["local_recovery"]={
        "state":"completed","storage_layout":layout,"policy_revision":1,
        "policy_hash":options["policy"].fingerprint,"generation":"copy_"+"1"*32}
    assert inspect()=={"ready":False,"reason":"current_layout_recovery_pending"}
    assert not recovery_root.exists(),"a heartbeat alone is not a published recovery copy"
    copied=run_due_local_recovery(storage.database,storage_root=destination.parent,
        pg_dump=Path("/usr/lib/postgresql/15/bin/pg_dump"),
        pg_controldata=Path("/usr/lib/postgresql/15/bin/pg_controldata"),max_bytes=16*1024**2,
        timeout_seconds=120,headroom_bytes={"ssd":1024**2,"hdd":1024**2},max_objects=1000)
    assert copied["state"]=="completed"
    worker["context"]["storage_lifecycle"]["last_run"]["local_recovery"]=copied
    ready=inspect()
    assert ready["ready"] and ready["plan_id"]==completed["plan_id"]
    assert ready["recovery_generation"]==copied["generation"] and ready["storage_layout"]==layout
    with engine.begin() as owner:
        assert owner.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))
        assert inspect()=={"ready":False,"reason":"storage_operation_running"}
    certificate=recovery_root/namespace/copied["generation"]/"complete.json"
    original=certificate.read_text()
    stale=json.loads(original);stale["storage_layout"]={"layout_version":"market.fact_storage_tiers.v1","certificate_sha256":"a"*64}
    try:
        certificate.write_text(json.dumps(stale))
        assert inspect()=={"ready":False,"reason":"current_layout_recovery_pending"}
    finally:
        certificate.write_text(original)
    assert inspect()==ready
    with engine.begin() as conn:
        conn.exec_driver_sql("UPDATE public.portal_storage_policy SET revision=2")
    with pytest.raises(RuntimeError,match="storage_runtime_handoff_policy_changed"):
        inspect()
    changed = invoke(operator)
    assert changed.returncode and "fact_header_policy_changed_after_activation" in changed.stderr
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT revision FROM public.portal_storage_policy WHERE id=1")) == 2

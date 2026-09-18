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

from market_data.archive import FilesystemRawArchiveObjectStore
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
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement
from tests.test_market_data.test_fact_header_copy_db import _finish as finish_headers, _insert
from tests.test_market_data.test_raw_mapping_copy_db import _finish as finish_raw
from tests.test_market_data.test_fact_header_references_db import _stage_all
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_archive_reference_placement_db import _options
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned SSD/HDD storage-demo topology"),
]


def _hashes(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*") if path.is_file()}


def test_archive_copy_resumes_and_serves_frozen_history_from_hdd_only(storage, tmp_path, monkeypatch):
    assert os.getuid() == 70 and os.getenv("QT_DB_TEST_ISOLATED") == "1"
    source = Path("/qt-source/pgdata") / ("archive-copy-"+uuid4().hex)
    source.mkdir()
    book = _cold_book_handoff(storage, source, monkeypatch, split_sources=False)
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
    with engine.begin() as conn:
        headers.prepare_copy(conn, placement=storage.copy_plan)
        raw.prepare_copy(conn)
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
    constrained = {**options["resource_limits"],
        "maintenance_bytes": {"ssd": 1024**2, "hdd": 256*1024**2}}
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
    finish_headers(engine)
    finish_raw(engine)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    _stage_all(engine)
    for relation in references.RELATIONS:
        references.move_reference_catalog(engine, relation=relation,
            policy=options["policy"], resource_limits=options["resource_limits"])
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
            handoff.commit_handoff(engine, **verification)
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
    def committed_then_lost(conn):
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
    with monkeypatch.context() as fault:
        fault.setattr(Connection, "_commit_impl", committed_then_lost)
        with pytest.raises(RuntimeError, match="injected_handoff_commit_reply_lost"):
            handoff.commit_handoff(engine, **verification)
    assert lost[0] and pending[0]
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='30s'")
        committed = handoff.inspect_handoff(conn, **inspect_options)
        assert committed["database_handoff_committed"] and not committed["collection_resume_authorized"]
        receipt = committed["receipt"]
        assert receipt["verified_archive_objects"] > baseline_inventory["verified_catalog_objects"]
        assert receipt["archive_inventory_sha256"] != baseline_inventory["inventory_sha256"]
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
            handoff.activate_handoff_policy(engine, **activation_options)
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
                handoff.activate_handoff_policy(engine, **activation_options)
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
    retry = handoff.activate_handoff_policy(engine, **activation_options)
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
    with engine.connect() as conn, conn.begin() as transaction:
        conn.exec_driver_sql("""
            UPDATE market.fact_storage_state SET evidence=jsonb_set(evidence,
                '{handoff,active_relation_oids,fact_versions}', '0'::jsonb)
            WHERE layout_version='market.fact_storage_tiers.v2'
        """)
        with pytest.raises(RuntimeError, match="relation_identity_changed"):
            handoff.inspect_handoff(conn, **inspect_options)
        transaction.rollback()

    retained = source.with_name(source.name+"-retained")
    source.rename(retained)
    assert not source.exists()  # Readers cannot fall back to the original root.
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
    assert _hashes(retained/"objects") == before
    _placement(monkeypatch, storage.open_day)
    recent = replace(storage.fact, observation_key="after-archive-root-handoff",
                     observation_time=BASE+timedelta(days=3))
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                     facts=[recent]).inserted_count == 1
    print("QT_ARCHIVE_ROOT_COPY_RESULT="+json.dumps({
        "source_preserved": True, "interrupted_copy_reused_verified_objects": True,
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

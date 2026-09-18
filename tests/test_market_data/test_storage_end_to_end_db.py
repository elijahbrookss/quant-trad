"""Actual application collection/archive/query paths across two disposable filesystems.

Run with scripts/ci/run_test_suite.sh storage-demo. UUID labels are synthetic;
PostgreSQL, schema, file placement, archive bytes and application reads are real.
This measures local concurrency, not physical HDD throughput or production sizing.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
from threading import Event
from time import monotonic

import pytest
from sqlalchemy import event, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from core.storage_targets import StoragePolicy, StorageTarget
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.storage_target_models import (
    StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord, StorageHeaderMoveRecord,
)
from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_destinations import register_header_tablespaces, review_header_moves
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from portal.backend.service.storage.header_journal import reserve_header_batch
from portal.backend.service.storage.header_movement import stage_header_move
from portal.backend.service.storage.header_resource_claims import reserve_header_resources
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                       reason="requires the supported storage-demo disposable topology"),
]
CONTROL = Path("/usr/lib/postgresql/15/bin/pg_controldata")


def _summary(samples):
    ordered = sorted(samples)
    return {"samples": len(ordered), "p95_ms": round(ordered[max(0, math.ceil(.95*len(ordered))-1)]*1000, 3),
            "max_ms": round(ordered[-1]*1000, 3)} if ordered else {"samples": 0}


def test_collect_move_freeze_interrupt_recover_and_measure(storage, tmp_path, monkeypatch):
    assert os.getenv("QT_DB_TEST_ISOLATED") == "1" and os.getuid() == 70
    source_root, history_root = Path("/qt-source/pgdata"), Path("/qt-history")
    assert source_root.stat().st_dev != history_root.stat().st_dev
    udev = tmp_path / "udev"
    udev.mkdir()
    targets = (
        StorageTarget("ssd", "Disposable recent", "uuid-demo-ssd", str(source_root), "ssd"),
        StorageTarget("hdd", "Disposable history", "uuid-demo-hdd", str(history_root), "hdd"),
    )
    for target in targets:
        device = Path(target.root).stat().st_dev
        (udev / f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID="+target.filesystem_uuid+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    engine = storage.database._engine
    old_day = storage.today - timedelta(days=45)
    _placement(monkeypatch, old_day)
    history_facts = [replace(storage.fact, observation_key=f"history-{i}",
        observation_time=BASE+timedelta(seconds=i)) for i in range(256)]
    accepted = storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=history_facts)
    assert accepted.inserted_count == len(history_facts)
    request = DatasetSeriesRequest(storage.series_id, BASE-timedelta(hours=1), BASE+timedelta(hours=1))
    frozen = storage.repo.freeze_dataset([request])
    frozen_before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    assert len(frozen_before) == len(history_facts)

    _placement(monkeypatch, storage.today)
    def recent_fact(number):
        stamp = BASE+timedelta(days=2, seconds=number)
        return replace(storage.fact, observation_key=f"recent-{number}", observation_time=stamp,
                       known_at=stamp, accepted_at=stamp)
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                             facts=[recent_fact(i) for i in range(32)])
    correction = replace(history_facts[0], known_at=BASE+timedelta(days=2),
        accepted_at=BASE+timedelta(days=2), payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"})
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                    facts=[correction]).corrected_count == 1

    archive_root = history_root / "archives"
    archive_root.mkdir()
    archive = CanonicalFactRetentionExecutor(
        repository=PostgresCanonicalFactRetentionRepository(database=storage.database))
    archive_policy = CanonicalFactRetentionPolicy(execution_enabled=True, hot_days=30,
        max_steps_per_run=16, max_page_rows=128, archive_min_free_bytes=0,
        max_page_logical_bytes=1024**2)
    actions = []
    for _ in range(4):
        result = archive.run(policy=archive_policy, storage_root=archive_root, execute=True)
        assert result["failure_count"] == 0, result
        actions.extend(item["action"] for item in result["outcomes"])
        if "reclaim_partition" in actions:
            break
    assert "reclaim_partition" in actions
    reader = FilesystemRawArchiveObjectStore(archive_root / "objects", writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,
                                                   series_id=storage.series_id) == frozen_before

    tablespace = history_root / "tablespace"
    tablespace.mkdir()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql("CREATE TABLESPACE qt_demo_history LOCATION '/qt-history/tablespace'")
        destination = conn.scalar(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname='qt_demo_history'"))
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",),
                           backups=("hdd",), recent_days=30, movement_enabled=True)
    with storage.database.session() as session:
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id,label=target.label,
                filesystem_uuid=target.filesystem_uuid,root=target.root,medium=target.medium,roles=list(target.roles)))
        session.add(StoragePolicyRecord(id=1,revision=0,policy=policy.to_dict()))
        session.add(StoragePlanRecord(id="demo",request_id="demo",base_revision=0,policy=policy.to_dict(),
            policy_hash=policy.fingerprint,state="queued",impact={},progress={}))
    catalog = read_header_catalog(engine, destination_tablespace_oids=(destination,))
    verified = verify_header_filesystem(catalog, targets, pg_controldata=CONTROL,
                                       destination_assignments={"hdd": destination})
    review = review_header_moves(verified=verified, policy=policy, targets=targets,
                                 reserved_bytes={"ssd": 0, "hdd": 0})
    with storage.database.session() as session:
        register_header_tablespaces(session, verified=verified)
        reserved = reserve_header_batch(session, plan_id="demo", review_hash=review["plan_hash"], verified=verified)
    assert len(reserved["moves"]) == 1, reserved
    move_id = reserved["moves"][0]["id"]
    with Session(engine) as session, session.begin():
        reserve_header_resources(session, move_id=move_id, review_hash=review["plan_hash"],
            pg_controldata=CONTROL,wal_bytes=4*1024**2,
            temporary_bytes={"ssd":1024**2,"hdd":1024**2},
            growth_bytes_per_second={"ssd":1024**2,"hdd":0},
            maintenance_bytes={"ssd":0,"hdd":0},movement_timeout_seconds=30,cancellation_grace_seconds=2)

    def move():
        with Session(engine) as session, session.begin():
            return stage_header_move(session,move_id=move_id,review_hash=review["plan_hash"],
                                     pg_controldata=CONTROL,timeout_seconds=30)
    killed = [False]
    def interrupt(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("ALTER TABLE"):
            killed[0] = True
            pid = conn.connection.driver_connection.get_backend_pid()
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid":pid})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",interrupt)
    try:
        with pytest.raises(DBAPIError):
            move()
    finally:
        event.remove(engine,"after_cursor_execute",interrupt)
    assert killed[0]
    with storage.database.session() as session:
        assert session.get(StorageHeaderMoveRecord,move_id).state == "reserved"
        assert session.get(StorageTargetRecord,"ssd").auxiliary_reserved_bytes > 0
    after_abort = verify_header_filesystem(read_header_catalog(engine), targets, pg_controldata=CONTROL)
    old = next(group for group in after_abort.snapshot.partitions if group.storage_day == old_day)
    assert all(item.target_id == "ssd" for item in old.relations)
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,
                                                   series_id=storage.series_id) == frozen_before

    recent_start, recent_end = BASE+timedelta(days=1), BASE+timedelta(days=3)
    def query_recent():
        return storage.repo.read_facts(series_id=storage.series_id,start=recent_start,end=recent_end)
    expected_history = {fact.observation_key:fact.row_hash for fact in history_facts}
    expected_history[correction.observation_key] = correction.row_hash
    def query_history():
        rows = storage.repo.read_facts(series_id=storage.series_id,start=request.start,end=request.end)
        assert {row.fact.observation_key:row.row_hash for row in rows} == expected_history
        assert len(rows) == len(expected_history)
        return rows
    def query_across_drives():
        rows = storage.repo.read_facts(series_id=storage.series_id,start=request.start,end=recent_end)
        history = [row for row in rows if row.fact.observation_time < recent_start]
        assert {row.fact.observation_key:row.row_hash for row in history} == expected_history
        assert len(history) == len(expected_history)
        assert len({row.fact_version_id for row in rows}) == len(rows)
        return rows
    def query_frozen():
        rows = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
        assert rows == frozen_before
        return rows
    operations = {"recent_query":query_recent,"history_query":query_history,"frozen_query":query_frozen,"cross_drive_query":query_across_drives}
    samples = {phase:{name:[] for name in (*operations,"collection")} for phase in ("baseline","concurrent")}
    collection_number = [100]
    def collect():
        number = collection_number[0]
        collection_number[0] += 1
        result = storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,
                                          facts=[recent_fact(number)])
        assert result.inserted_count == 1
    operation_intervals = {name:[] for name in (*operations,"collection")}
    ddl_started = []
    def observe_move(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith(("ALTER TABLE", "ALTER INDEX")):
            ddl_started.append(monotonic())
            begin.set()
    # Match concurrency and operation counts; a serial baseline would confound
    # the cost of movement with the cost of the concurrent application callers.
    for phase in ("baseline","concurrent"):
        begin = Event()
        def workload(name,operation):
            assert begin.wait(45)
            for _ in range(12):
                began=monotonic();operation();ended=monotonic()
                samples[phase][name].append(ended-began)
                if phase == "concurrent":
                    operation_intervals[name].append((began,ended))
        with ThreadPoolExecutor(max_workers=len(operations)+1) as pool:
            tasks=[pool.submit(workload,name,operation)
                   for name,operation in {**operations,"collection":collect}.items()]
            if phase == "concurrent":
                event.listen(engine,"before_cursor_execute",observe_move)
                try:
                    move_started=monotonic()
                    completed=move()
                    move_ended=monotonic()
                finally:
                    begin.set()
                    event.remove(engine,"before_cursor_execute",observe_move)
            else:
                begin.set()
            for task in tasks:
                task.result(timeout=30)
    assert completed["state"] == "completed"
    after = verify_header_filesystem(read_header_catalog(engine), targets, pg_controldata=CONTROL)
    old = next(group for group in after.snapshot.partitions if group.storage_day == old_day)
    recent = next(group for group in after.snapshot.partitions if group.storage_day == storage.today)
    assert all(item.target_id == "hdd" for item in old.relations)
    assert all(item.target_id == "ssd" for item in recent.relations)
    assert query_frozen() == frozen_before
    assert next(row for row in query_history() if row.fact.observation_key=="history-0").revision == 2
    assert len(query_recent()) == 32+12+12
    assert [row.fact_version_id for row in query_across_drives()] == [
        row.fact_version_id for row in query_history()+query_recent()]
    with storage.database.session() as session:
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))
    overlapping = {
        name:_summary([end-start for start,end in intervals if start < move_ended and end > move_started])
        for name,intervals in operation_intervals.items()}
    assert ddl_started
    overlapping_copy = {
        name:_summary([end-start for start,end in intervals if start < move_ended and end > ddl_started[0]])
        for name,intervals in operation_intervals.items()}
    assert all(values["samples"] > 0 for values in overlapping_copy.values()), overlapping_copy
    report = {
        "schema_version":"qt.storage_end_to_end_demo.v1",
        "recorded_at":datetime.now(UTC).isoformat(),
        "source_revision":os.environ["SOURCE_REVISION"],
        "source_tree_hash":os.environ["SOURCE_TREE_HASH"],
        "historical_rows":len(history_facts),
        "historical_header_and_index_bytes":sum(item.byte_count for item in old.relations),
        "recent_rows":len(query_recent()),
        "historical_payload_archived":True,"historical_headers_and_indexes_on_history":True,
        "recent_headers_and_indexes_on_source":True,"frozen_results_unchanged":True,
        "late_correction_visible":True,"cross_drive_results_complete_and_ordered":True,
        "interrupted_backend_recovered":True,
        "copy_and_auxiliary_reservations_released":True,
        "movement_seconds":round(move_ended-move_started,4),
        "first_copy_through_commit_seconds":round(move_ended-ddl_started[0],4),
        "operations_overlapping_copy_and_commit":overlapping_copy,
        "baseline_concurrency_matches_movement":True,
        "baseline":{name:_summary(values) for name,values in samples["baseline"].items()},
        "operations_overlapping_movement":overlapping,
        "concurrent":{name:_summary(values) for name,values in samples["concurrent"].items()},
        "limitations":["synthetic collection inputs through real ingestion repository",
                       "local source filesystem and tmpfs history; not physical HDD performance",
                       "small dataset; not two-year capacity or full-volume migration evidence",
                       "internal movement primitive; automatic scheduling not yet qualified",
                       "global identity/raw mappings still require measured SSD growth resolution"],
    }
    print("QT_STORAGE_DEMO_REPORT="+json.dumps(report,sort_keys=True))

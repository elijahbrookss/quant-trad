"""Preserving archive-file copy on the two owned disposable filesystems."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db import archive_root_v2_copy as archives
from scripts.db import archive_reference_v2_placement as references
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_book_retention_db import _cold_book_handoff
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement
from tests.test_market_data.test_fact_header_copy_db import _finish as finish_headers, _insert
from tests.test_market_data.test_raw_mapping_copy_db import _finish as finish_raw
from tests.test_market_data.test_fact_header_references_db import _stage_all
from tests.test_market_data.test_archive_reference_placement_db import _options
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture, stage_shadow_handoff_fixture

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

    # Reuse the guarded tiny database handoff; this does not turn a page copy
    # into a production migration or an archive-root activation certificate.
    finish_headers(engine)
    finish_raw(engine)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    _stage_all(engine)
    for relation in references.RELATIONS:
        references.move_reference_catalog(engine, relation=relation, **_options(storage))
    with engine.begin() as conn:
        stage_shadow_handoff_fixture(conn, storage, prevalidated=True, raw_mapping=True)

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
        "copied_objects": len(copied), "copied_bytes": sum((objects/key).stat().st_size for key in copied),
        "limits": ["tiny disposable data", "page progress is not final inventory readiness",
                   "no production root switch, migration-duration or hardware qualification"]
    }, sort_keys=True))

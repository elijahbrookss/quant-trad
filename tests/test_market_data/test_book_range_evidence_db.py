"""Persistent producer proof, archive pinning and immutable research binding."""
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import derive_book_features, MarketStateValuationContract
from market_data.range_evidence import is_complete_range_evidence
from portal.backend.service.market.frozen_dataset_service import resolve_frozen_dataset_read_binding
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result

pytestmark = pytest.mark.db


@pytest.fixture
def book(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch, trailing_heartbeat=True,
                                replay_features=True, continuous_sequence=True,
                                provider_product_id="EXAMPLE-USD")
    for index in range(len(fixture.results)):
        _publish_book_result(fixture, index)
    config = fixture.claim.config
    bbo, depth = derive_book_features((item.state for item in fixture.results),
        contract=MarketStateValuationContract(product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=BASE + timedelta(minutes=1))
    fixture.structures.ingest_market_state_features(bbo_facts=bbo, depth_facts=depth)
    reader = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    fixture.start = BASE.replace(microsecond=0) + timedelta(seconds=1)
    fixture.end = fixture.start + timedelta(seconds=3)
    fixture.watermark = storage.repo.current_commit_seq()
    return fixture


def receipts(book, *, omit=None, watermark=None):
    for index, manifest in enumerate(book.manifests):
        if index == omit:
            continue
        book.structures.append_session_event(book.claim, event_ordinal=100 + index,
            connection_epoch=0, event_type="book_segment_canonicalized",
            occurred_at=BASE + timedelta(minutes=2), evidence={"manifest_id": manifest,
                "record_count": 1, "book_ingest_commit_seq": watermark or book.watermark})


def quality(storage, book, key="bbo_series_id", **kwargs):
    return storage.repo.list_gap_evidence(series_id=book.claim.config[key],
        start=book.start, end=book.end, **kwargs)


def test_quiet_proof_requires_acknowledged_objects_and_finished_processing(storage, book):
    assert quality(storage, book) == []
    receipts(book, omit=1)  # The heartbeat-only archive also requires a receipt.
    assert quality(storage, book) == []
    book.structures.append_session_event(book.claim, event_ordinal=101, connection_epoch=0,
        event_type="book_segment_canonicalized", occurred_at=BASE + timedelta(minutes=2),
        evidence={"manifest_id": book.manifests[1], "record_count": 1,
                  "book_ingest_commit_seq": book.watermark})
    proof = quality(storage, book)
    assert len(proof) == 1 and is_complete_range_evidence(proof[0])
    assert proof[0]["start"] == (book.start + timedelta(seconds=1)).isoformat(timespec="microseconds").replace("+00:00", "Z")
    assert len(quality(storage, book, "depth_series_id")) == 1
    assert quality(storage, book, known_at_lte=BASE + timedelta(seconds=90)) == []
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                              {"id": book.manifests[1]}).scalar_one()
    path = book.store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    with pytest.raises(RuntimeError, match="archive_hash_mismatch"):
        quality(storage, book)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        quality(storage, book)


def test_future_processing_receipt_does_not_prove_past_watermark(storage, book):
    receipts(book, watermark=book.watermark + 1)
    assert quality(storage, book, as_of_commit_seq=book.watermark) == []


def test_frozen_quality_retains_quiet_proof_and_heartbeat_archive_without_new_samples(storage, book, monkeypatch):
    receipts(book)
    series_id = book.claim.config["bbo_series_id"]
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series_id, book.start, book.end)])
    entry = frozen.series[0]
    assert entry["row_count"] == 2  # No artificial observation for the quiet second.
    assert len(entry["quality_evidence"]) == 1
    assert is_complete_range_evidence(entry["quality_evidence"][0])
    with storage.database.session() as session:
        pinned = set(session.execute(text("SELECT raw_archive_manifest_id FROM market.dataset_archive_refs WHERE dataset_id=:id"),
                                     {"id": frozen.dataset_id}).scalars())
    assert set(book.manifests[:3]) <= pinned
    requirement = {"alias": "book", "instrument_id": "storage-fixture", "fact_type": "market.bbo",
        "contract_version": "market.bbo.v1", "timeframe_seconds": 1, "dimensions": {},
        "required_start": book.start, "required_end": book.end,
        "source_policy": {"mode": "exact", "source_identity_key": next(iter(entry["source_summary"]["counts"]))}}
    def resolve():
        return resolve_frozen_dataset_read_binding(dataset_id=frozen.dataset_id, requirements=[requirement],
            store=storage.repo, instrument_loader=lambda key: {"id": key, "symbol": "EXAMPLE-USD"})
    before = resolve()
    assert before["recorded_gaps"] == []
    def mutable_forbidden(*args, **kwargs):
        raise AssertionError("frozen replay must not reevaluate mutable range quality")
    monkeypatch.setattr(storage.repo, "list_gap_evidence", mutable_forbidden)
    assert resolve() == before



def compact(book, tmp_path, *, sources, records, name):
    from market_data.archive import encode_raw_records_to_parquet
    encoded = encode_raw_records_to_parquet(records, archive_segment_id=name,
                                            temporary_directory=tmp_path / "compact")
    ack = book.store.put_verified(object_key=f"{name}.parquet", source_path=encoded.path,
                                  expected_sha256=encoded.sha256)
    return book.structures.commit_compacted_archive(definition_id=book.claim.definition_id,
        encoded=encoded, acknowledgement=ack, records=records, source_manifest_ids=sources).manifest_id


def expire_fixture_objects(storage, book, identities):
    # Model completed expiry in this disposable database; no lifecycle worker or
    # production archive is touched. Metadata and original receipts are retained.
    with storage.database.session() as session:
        for identity in identities:
            session.execute(text("""
                INSERT INTO market.storage_lifecycle_events
                    (id,operation_id,event_ordinal,policy_version,action,event_type,
                     target_kind,target_id,occurred_at,known_at,evidence_hash,evidence)
                VALUES (:id,:id,0,'fixture','archive_expire','completed','raw_manifest',
                        :manifest,clock_timestamp(),clock_timestamp(),:hash,'{}')
            """), {"id": f"expired-{identity}", "manifest": identity, "hash": "e" * 64})
            key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                                  {"id": identity}).scalar_one()
            book.store.local_path(key).unlink()


def test_compaction_preserves_quiet_proof_and_frozen_custody_through_multiple_hops(storage, book, tmp_path, monkeypatch):
    from portal.backend.service.storage.repos import book_range_evidence
    receipts(book)
    original = quality(storage, book)[0]
    first = compact(book, tmp_path, sources=book.manifests[:3], records=book.raws[:3], name="first-compaction")
    # Tiny fixtures can compress into fewer bytes than a single source file.
    # Selection may use originals, replacements, or both, but must not read the
    # entire overlapping set after the requested raw positions are covered.
    retained = quality(storage, book)[0]
    assert retained["evidence"]["transport"] == original["evidence"]["transport"]
    selected = {row["manifest_id"] for row in retained["evidence"]["archives"]}
    candidates = set(book.manifests[:3]) | {first}
    unselected = sorted(candidates - selected)[0]
    with storage.database.session() as session:
        ordinal = session.execute(text("SELECT first_receive_ordinal FROM market.raw_archive_manifests WHERE id=:id"),
                                  {"id": unselected}).scalar_one()
        session.execute(text("ALTER TABLE market.raw_archive_record_mappings DISABLE TRIGGER USER"))
        session.execute(text("UPDATE market.raw_archive_record_mappings SET raw_frame_sha256=:hash WHERE manifest_id=:id AND receive_ordinal=:ordinal"),
                        {"id": unselected, "ordinal": ordinal, "hash": "a" * 64})
        session.execute(text("ALTER TABLE market.raw_archive_record_mappings ENABLE TRIGGER USER"))
    with pytest.raises(RuntimeError, match="identity_conflict"):
        quality(storage, book)  # An unselected copy cannot hide a scoped conflict.
    with storage.database.session() as session:
        session.execute(text("ALTER TABLE market.raw_archive_record_mappings DISABLE TRIGGER USER"))
        session.execute(text("UPDATE market.raw_archive_record_mappings SET raw_frame_sha256=:hash WHERE manifest_id=:id AND receive_ordinal=:ordinal"),
                        {"id": unselected, "ordinal": ordinal, "hash": book.raws[ordinal - 1].raw_frame_sha256})
        session.execute(text("ALTER TABLE market.raw_archive_record_mappings ENABLE TRIGGER USER"))
    expire_fixture_objects(storage, book, book.manifests[:3])
    one_hop = quality(storage, book)[0]
    assert one_hop["start"] == original["start"] and one_hop["end"] == original["end"]
    archives = one_hop["evidence"]["archives"]
    assert [row["manifest_id"] for row in archives] == [first]
    assert {row["manifest_id"] for row in archives[0]["source_processing_receipts"]} == set(book.manifests[:3])
    second = compact(book, tmp_path, sources=[first, book.manifests[3]], records=book.raws, name="second-compaction")
    expire_fixture_objects(storage, book, [first, book.manifests[3]])
    multi_hop = quality(storage, book)[0]
    assert is_complete_range_evidence(multi_hop)
    assert multi_hop["evidence"]["transport"] == original["evidence"]["transport"]
    assert len(multi_hop["evidence"]["archives"][0]["compaction_sources"]) == 5
    assert multi_hop["known_at"] >= one_hop["known_at"]
    with monkeypatch.context() as bounded:
        bounded.setattr(book_range_evidence, "MAX_RECORDS", 3)
        with pytest.raises(RuntimeError, match="budget_exceeded"):
            quality(storage, book)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(book.claim.config["bbo_series_id"], book.start, book.end)])
    assert frozen.series[0]["row_count"] == 2
    assert frozen.series[0]["quality_evidence"][0]["evidence"]["archives"][0]["manifest_id"] == second
    with storage.database.session() as session:
        pinned = set(session.execute(text("SELECT raw_archive_manifest_id FROM market.dataset_archive_refs WHERE dataset_id=:id"),
                                     {"id": frozen.dataset_id}).scalars())
    assert second in pinned and not (set(book.manifests) | {first}) & pinned


def test_compacted_proof_rejects_incomplete_conflicting_and_late_lineage(storage, book, tmp_path):
    from portal.backend.service.storage.repos.book_range_evidence import BookRangeWitnesses
    receipts(book)
    replacement = compact(book, tmp_path, sources=book.manifests[:3], records=book.raws[:3], name="lineage-faults")
    expire_fixture_objects(storage, book, book.manifests[:3])
    assert len(quality(storage, book)) == 1
    # Roll back each injected metadata fault. The reader uses this same session,
    # so there is no corrupted committed fixture or cross-test cleanup dependency.
    cases = [
        ("UPDATE market.raw_archive_compaction_sources SET replacement_content_fingerprint=:bad WHERE replacement_manifest_id=:id", "lineage_conflict"),
        ("DELETE FROM market.raw_archive_compaction_sources WHERE replacement_manifest_id=:id AND source_ordinal=2", "coverage_conflict"),
        ("UPDATE market.raw_archive_compaction_sources SET source_manifest_id=:id WHERE replacement_manifest_id=:id AND source_ordinal=0", "compaction_cycle"),
        ("UPDATE market.raw_archive_manifests SET first_receive_ordinal=2,last_receive_ordinal=2 WHERE id=:source", "lineage_conflict"),
        ("UPDATE market.raw_archive_manifests SET content_fingerprint=:bad WHERE id=:source", "fingerprint_mismatch"),
        ("UPDATE market.raw_archive_compaction_sources SET known_at=clock_timestamp()+interval '1 day' WHERE replacement_manifest_id=:id", None),
        ("DELETE FROM market.stream_session_events WHERE evidence->>'manifest_id'=:source", None),
        ("UPDATE market.stream_session_events SET known_at=clock_timestamp()+interval '1 day' WHERE evidence->>'manifest_id'=:source", None),
        ("UPDATE market.stream_session_events SET evidence=jsonb_set(evidence,'{book_ingest_commit_seq}',to_jsonb(CAST(:future AS bigint))) WHERE evidence->>'manifest_id'=:source", None),
    ]
    for sql, error in cases:
        with storage.database.session() as session:
            savepoint = session.begin_nested()
            for table in ("raw_archive_compaction_sources", "raw_archive_manifests", "stream_session_events"):
                session.execute(text(f"ALTER TABLE market.{table} DISABLE TRIGGER USER"))
            session.execute(text(sql), {"id": replacement, "source": book.manifests[0], "bad": "0" * 64,
                                        "future": book.watermark + 1})
            witnesses = BookRangeWitnesses(session, object_store=book.store, watermark=book.watermark, known_at_lte=None)
            manifest = witnesses._manifest(replacement)
            if error:
                with pytest.raises(RuntimeError, match=error):
                    proof = witnesses._processing(manifest)
                    witnesses._read(manifest, proof)
            else:
                assert witnesses._processing(manifest) is None
            savepoint.rollback()
    assert len(quality(storage, book)) == 1


@pytest.fixture
def stopped_book(storage, book, tmp_path, monkeypatch):
    from datetime import UTC, datetime
    from dataclasses import replace
    from market_data.order_book import BookLifecycle

    position = replace(book.results[-1].state.source_position,
        receive_ordinal=book.raws[-1].receive_ordinal, event_ordinal=0, provider_sequence_num=3)
    closure = book.reducer.close_valid_at(position=position, effective_at=book.raws[-1].received_at,
        known_at=datetime.now(UTC), reason="continuous collector stopped at a proven transport boundary")
    book.structures.ingest_book_facts(book.claim, snapshots=[], batches=[],
        validity_versions=closure.validity_versions, lifecycle=BookLifecycle.AWAITING_SNAPSHOT,
        final_validity_interval_id=None, checkpoint_id=None, final_state_hash=None,
        final_connection_epoch=0, final_receive_ordinal=position.receive_ordinal,
        final_event_ordinal=position.event_ordinal, final_sequence_num=position.provider_sequence_num)
    book.stop_id = book.structures.append_session_event(book.claim, event_ordinal=200, connection_epoch=0,
        event_type="continuous_capture_stopped", occurred_at=datetime.now(UTC),
        reason="supervisor_stop_requested", evidence={"raw_record_count": 4, "invalid_stream_state": False})
    book.terminal_time = datetime.now(UTC)
    book.terminal_id = book.structures.append_session_event(book.claim, event_ordinal=201, connection_epoch=0,
        event_type="book_segment_canonicalized", occurred_at=book.terminal_time,
        evidence={"manifest_id": book.manifests[-1], "record_count": 1,
                  "book_ingest_commit_seq": book.watermark, "terminal": True,
                  "lifecycle": "awaiting_snapshot", "state_hash": None})
    book.closed = closure.validity_versions[0]
    book.structures.release(book.claim)
    second = _raw_book_fixture(storage, tmp_path / "second", monkeypatch, trailing_heartbeat=True,
        replay_features=True, continuous_sequence=True, provider_product_id="EXAMPLE-USD",
        event_start=BASE + timedelta(seconds=10), object_store=book.store)
    for index in range(len(second.results)):
        _publish_book_result(second, index)
    config = second.claim.config
    bbo, depth = derive_book_features((item.state for item in second.results),
        contract=MarketStateValuationContract(product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=BASE + timedelta(minutes=1))
    second.structures.ingest_market_state_features(bbo_facts=bbo, depth_facts=depth)
    book.second = second
    book.start = BASE.replace(microsecond=0) + timedelta(seconds=3)
    book.end = BASE.replace(microsecond=0) + timedelta(seconds=12)
    book.watermark = storage.repo.current_commit_seq()
    return book


def test_clean_stop_retains_interruption_and_frozen_replay(storage, stopped_book, monkeypatch):
    book = stopped_book
    from market_data.range_evidence import utc
    from portal.backend.service.research.execution import _quality
    from types import SimpleNamespace

    evidence = quality(storage, book, as_of_commit_seq=book.watermark)
    assert len(evidence) == 1
    row = evidence[0]
    assert row["classification"] == "source_interrupted" and not is_complete_range_evidence(row)
    assert row["evidence"]["capture_stop"]["reason"] == "collector_capture_stopped"
    assert utc(row["start"]) == BASE + timedelta(seconds=4)
    assert utc(row["end"]) == BASE + timedelta(seconds=11)
    assert utc(row["known_at"]) >= book.terminal_time
    assert evidence == quality(storage, book, as_of_commit_seq=book.watermark)
    assert _quality(SimpleNamespace(quality_evidence=evidence, gap_policy="continue_degraded",
                                   missing_coverage=[]))["status"] == "degraded"
    assert _quality(SimpleNamespace(quality_evidence=evidence, gap_policy="reject",
                                   missing_coverage=[]))["recorded_gap_count"] == 1
    assert quality(storage, book, known_at_lte=book.terminal_time - timedelta(microseconds=1)) == []
    series_id = book.claim.config["bbo_series_id"]
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series_id, book.start, book.end)])
    entry = frozen.series[0]
    assert entry["row_count"] == 2 and entry["quality_evidence"] == evidence
    requirement = {"alias": "book", "instrument_id": "storage-fixture", "fact_type": "market.bbo",
        "contract_version": "market.bbo.v1", "timeframe_seconds": 1, "dimensions": {},
        "required_start": book.start, "required_end": book.end,
        "source_policy": {"mode": "exact", "source_identity_key": next(iter(entry["source_summary"]["counts"]))}}
    def resolve():
        return resolve_frozen_dataset_read_binding(dataset_id=frozen.dataset_id, requirements=[requirement],
            store=storage.repo, instrument_loader=lambda key: {"id": key, "symbol": "EXAMPLE-USD"})
    pinned = resolve()
    assert pinned["recorded_gaps"]
    def forbidden(*args, **kwargs):
        raise AssertionError("frozen quality must not be reprojected")
    monkeypatch.setattr(storage.repo, "list_gap_evidence", forbidden)
    assert resolve() == pinned


def test_clean_stop_missing_conflicting_and_late_provenance_stays_unresolved(storage, stopped_book):
    book = stopped_book
    assert len(quality(storage, book)) == 1
    # Deliberate corruption is confined to the disposable test database. Roll
    # back each mutation, exercising the real SQL reader and receipt guards.
    from portal.backend.service.storage.repos.book_range_evidence import BookRangeWitnesses
    def proof(session):
        w = BookRangeWitnesses(session, object_store=book.store, watermark=book.watermark, known_at_lte=None)
        before = w.validity(book.closed.interval_id)
        after = w.validity(book.second.results[0].state.validity_interval_id)
        return w.capture_stop(before, after, book.results[-1].state.source_position.material(),
                              book.second.results[0].state.source_position.material())
    changes = [
        ("DELETE FROM market.stream_session_events WHERE id=:id", {"id": book.stop_id}),
        ("DELETE FROM market.stream_session_events WHERE id=:id", {"id": book.terminal_id}),
        ("UPDATE market.stream_session_events SET evidence=jsonb_set(evidence,'{book_ingest_commit_seq}',to_jsonb(CAST(:wm AS bigint))) WHERE id=:id",
         {"id": book.terminal_id, "wm": book.watermark + 1}),
        ("UPDATE market.stream_session_events SET evidence=jsonb_set(evidence,'{terminal}','false') WHERE id=:id", {"id": book.terminal_id}),
        ("UPDATE market.raw_archive_ranges SET provider_product_id='WRONG' WHERE manifest_id=:id", {"id": book.manifests[-1]}),
        ("UPDATE market.book_validity_interval_versions SET reason='bounded capture ended cleanly' WHERE id=:id", {"id": book.closed.version_id}),
        ("UPDATE market.book_validity_interval_versions SET closing_receive_ordinal=999 WHERE id=:id", {"id": book.closed.version_id}),
        ("UPDATE market.book_validity_interval_versions SET opening_snapshot_id='missing' WHERE id=:id", {"id": book.closed.version_id}),
        ("UPDATE market.stream_session_events SET definition_id=:definition WHERE id=:id", {"id": book.stop_id, "definition": book.claim.definition_id}),
    ]
    # The last no-op is a positive control after every rollback.
    for index, (sql, args) in enumerate(changes):
        with storage.database.session() as session:
            nested = session.begin_nested()
            # This fixture owns an isolated generated database. Disable only
            # its immutable-table guard inside the rollback-only fault scope;
            # production never mutates these records.
            for table in ("stream_session_events", "raw_archive_ranges", "book_validity_interval_versions"):
                session.execute(text(f"ALTER TABLE market.{table} DISABLE TRIGGER USER"))
            session.execute(text(sql), args)
            assert (proof(session) is not None) == (index == len(changes) - 1)
            nested.rollback()
    # A retained opening between the two selected feature rows cannot be skipped.
    with storage.database.session() as session:
        nested = session.begin_nested()
        session.execute(text("""INSERT INTO market.book_validity_interval_versions
            SELECT (jsonb_populate_record(NULL::market.book_validity_interval_versions,
                    to_jsonb(v) || jsonb_build_object('id','intervening','interval_id','intervening',
                    'opening_effective_at',CAST(:at AS timestamptz)))).*
            FROM market.book_validity_interval_versions v WHERE id=:id"""),
            {"id": book.closed.version_id, "at": BASE + timedelta(seconds=8)})
        assert proof(session) is None
        nested.rollback()
    with storage.database.session() as session:
        nested = session.begin_nested()
        session.execute(text("""INSERT INTO market.stream_session_events
            SELECT (jsonb_populate_record(NULL::market.stream_session_events,
                    to_jsonb(e) || jsonb_build_object('id','duplicate-stop','event_ordinal',202))).*
            FROM market.stream_session_events e WHERE id=:id"""), {"id": book.stop_id})
        with pytest.raises(RuntimeError, match="multiple stop receipts"):
            proof(session)
        nested.rollback()

"""The real collector resumes a bounded raw proof before admitting its first book."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment, RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.structure import RawStreamRecord
from portal.backend.service.market import continuous_stream_collector as collector_module
from portal.backend.service.storage.repos import fact_references
from scripts.db.canonical_cold_collector_drain import CollectorDrainLimits, InactiveLevel2, capture_collector_tail, verify_collector_drain
from scripts.db.canonical_cold_collector_restart import CollectorRestartPending, verify_recovered_pending_spool
from scripts.db.canonical_cold_recovery_pending_spool import PendingSpoolScope
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE, storage


pytestmark = pytest.mark.db


def _segment(fixture, *, claim, root, segment_ordinal, entries):
    segment = DurableRawSpoolSegment(
        root=root, definition_id=claim.definition_id, session_id=claim.session_id,
        connection_epoch=1, segment_ordinal=segment_ordinal,
    )
    analyzer = collector_module._CaptureAnalyzer(claim, primary_channel="level2")
    parser = CoinbaseMessageParser(symbol_by_product_id={claim.provider_product_id: claim.provider_product_id})
    records = []
    for ordinal, channel, sequence_num, event_type, levels in entries:
        timestamp = (BASE + timedelta(seconds=ordinal)).isoformat()
        event = ({"current_time": timestamp, "heartbeat_counter": str(sequence_num + 1)}
                 if channel == "heartbeats" else
                 {"type": event_type, "product_id": claim.provider_product_id,
                  "updates": [{"side": side, "price_level": price, "new_quantity": quantity,
                               "event_time": timestamp} for side, price, quantity in levels]})
        frame = json.dumps({"channel": channel, "timestamp": timestamp,
                            "sequence_num": sequence_num, "events": [event]})
        raw = RawStreamRecord.from_provider_message(
            ProviderRawMessage.build(
                provider=fixture.source.provider, venue=fixture.source.venue,
                stream_session_id=claim.session_id, connection_epoch=1, receive_ordinal=ordinal,
                received_at=timestamp, raw_frame=frame,
            ),
            definition_id=claim.definition_id, spool_segment_id=segment.spool_segment_id,
            provider_product_id=claim.provider_product_id, requested_channel="level2",
            observed_channel="heartbeats" if channel == "heartbeats" else "level2",
        )
        segment.append(raw)
        analyzer.observe(raw, parser.parse_raw(frame, received_at=timestamp, raw_ref={"raw_record_id": raw.raw_record_id}))
        records.append(raw)
    segment.seal()
    return segment, analyzer.finalize(), records


def test_long_invalid_epoch_resumes_exact_prefix_before_acknowledging_spool(storage, tmp_path, monkeypatch):
    fixture = _raw_book_fixture(storage, tmp_path / "enrollment", monkeypatch, replay_features=True)
    fixture.structures.release(fixture.claim)
    with storage.database._engine.begin() as conn:
        conn.execute(text("""
            UPDATE market.stream_definitions SET channels='["level2","heartbeats"]'::jsonb,
                   enabled=true,desired_state='running' WHERE id=:id
        """), {"id": fixture.claim.definition_id})
    claim = fixture.structures.claim_stream(
        definition_id=fixture.claim.definition_id, owner_id="initial-worker", lease_seconds=600, bounded=True,
    )
    projection = collector_module.CoinbaseLevel2BookProjectionAdapter()
    collector = collector_module.ContinuousMarketStructureCollector(repository=fixture.structures)
    spool_root = tmp_path / "worker-spool"
    temporary_root = tmp_path / "worker-staging"
    initial, analysis, initial_records = _segment(
        fixture, claim=claim, root=spool_root, segment_ordinal=0,
        entries=[
            (1, "l2_data", 0, "snapshot", []),
            (2, "heartbeats", 0, None, []),
            (3, "heartbeats", 1, None, []),
        ],
    )
    event_writer = collector_module._SessionEventWriter(fixture.structures, claim)
    collector._finalize_level2_segment(
        claim=claim, checkpoint=collector_module._SegmentCheckpoint(
            initial, analysis, terminal=False, terminal_reason=None, closing_session_event_id=None,
        ),
        object_store=fixture.store, temporary_root=temporary_root,
        event_writer=event_writer,
        states={}, counters={"manifests": 0, "quality_events": 0, **projection.counters()},
    )
    assert not initial.sealed_path.exists()
    previous = fixture.structures.get_book_reconstruction_state(
        definition_id=claim.definition_id, session_id=claim.session_id, connection_epoch=1,
    )
    assert previous["lifecycle"] == "invalid" and previous["checkpoint_id"] is None
    initial_manifest, = fixture.structures.list_session_manifests(
        definition_id=claim.definition_id, session_id=claim.session_id,
    )
    assert initial_manifest["first_receive_ordinal"] == 1
    assert initial_manifest["last_receive_ordinal"] == initial_records[-1].receive_ordinal
    tail, _, tail_records = _segment(
        fixture, claim=claim, root=spool_root, segment_ordinal=1,
        entries=[
            (4, "l2_data", 1, "snapshot", [("bid", "99", "10"), ("offer", "101", "11")]),
            (5, "l2_data", 2, "update", [("bid", "99", "12")]),
        ],
    )
    failed_id = event_writer.append(
        connection_epoch=1, event_type="failed", occurred_at=datetime.now(UTC),
        reason="fixture interrupted before spool admission", evidence={"pending_segments": 1},
    )
    fixture.structures.release(claim)
    pending_scope = PendingSpoolScope(claim.definition_id, claim.session_id, 1, claim.provider, claim.venue,
                                      claim.provider_product_id, "level2", 1, tail.sealed_path.stat().st_size)
    archive_limits = ArchiveVerificationLimits(max_objects=16, max_bytes=16 * 1024**2)
    limits = CollectorDrainLimits(archive_limits, RawArchiveReadLimits(max_rows=64), max_events=64,
                                  max_raw_rows=128, max_spool_entries=64)
    with storage.database._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        events = conn.execute(text("""
            SELECT id,event_type,evidence_hash FROM market.stream_session_events
            WHERE session_id=:session AND connection_epoch=1
        """), {"session": claim.session_id}).mappings().all()
        published, = [event for event in events if event["event_type"] == "book_segment_canonicalized"]
        failure, = [event for event in events if event["id"] == failed_id]
        approved = InactiveLevel2(pending_scope, failed_id, failure["evidence_hash"], published["id"], published["evidence_hash"])
        identity = conn.scalar(text("""
            SELECT system_identifier::text || '/' || oid::text
            FROM pg_control_system() CROSS JOIN pg_database WHERE datname=current_database()
        """))
        with pytest.raises(RuntimeError, match="tail_stream_not_ready"):
            capture_collector_tail(conn, stream_ids=(claim.definition_id,), poll_ids=(), check=lambda: 30)
        with pytest.raises(RuntimeError, match="inactive_boundary_changed"):
            capture_collector_tail(conn, stream_ids=(claim.definition_id,), poll_ids=(), check=lambda: 30,
                                   inactive_level2=replace(approved, failed_event_sha256="f" * 64))
        before = capture_collector_tail(conn, stream_ids=(claim.definition_id,), poll_ids=(), check=lambda: 30,
                                      inactive_level2=approved)
    with storage.database._engine.begin() as conn:
        arguments = dict(before=before, source_commit_seq=before["source_commit_seq"], spool_root=spool_root,
                         store=fixture.store, limits=limits, check=lambda: 30, database_identity=identity)
        with pytest.raises(RuntimeError, match="inactive_plan_changed"):
            verify_collector_drain(conn, **arguments)
        unexpected = spool_root / "unapproved.open"
        unexpected.write_bytes(b"unexpected fixture data")
        try:
            with pytest.raises(RuntimeError, match="pending_spool_unexpected_file"):
                verify_collector_drain(conn, **arguments, inactive_level2=approved)
        finally:
            unexpected.unlink()
        drained = verify_collector_drain(conn, **arguments, inactive_level2=approved)
    pending = drained["pending_spool"]
    assert pending["boundary"] == {"database_identity": identity, "source_commit_seq": before["source_commit_seq"]}
    assert pending["segments"][0]["segment_id"] == tail.spool_segment_id
    assert drained["streams"][0]["interruption"]["classification"] == "pre_existing_provider_unavailable"
    started = datetime.now(UTC)

    def verify_replayed(evidence=pending):
        with storage.database._engine.begin() as conn:
            files = ArchiveVerificationBatch(fixture.store, limits=archive_limits)
            receipt = verify_recovered_pending_spool(
                conn, pending=evidence, started_at=started, spool_root=spool_root, files=files, limits=limits,
                budget={"events": 64, "rows": 128}, check=lambda: 30,
            )
            files.assert_unchanged()
            return receipt

    with pytest.raises(CollectorRestartPending, match="spool_not_released"):
        verify_replayed()

    original = fact_references.lock_canonical_raw_references
    monkeypatch.setattr(fact_references, "lock_canonical_raw_references",
                        lambda session, facts: original(session, facts, max_mapping_rows=3))
    monkeypatch.setattr(fact_references, "RAW_REFERENCE_MAPPING_LIMIT", 3)
    monkeypatch.setattr(collector_module, "_BOOK_PREFIX_CHUNKS_PER_ATTEMPT", 1)

    def resume():
        collector._recover_orphaned_spools_sync(
            definition={"id": claim.definition_id}, owner_id="restart-worker", lease_seconds=600,
            spool_root=spool_root, object_store=fixture.store,
            temporary_root=temporary_root, projection=projection,
        )

    path = fixture.store.local_path(initial_manifest["object_key"])
    previous_bytes = path.read_bytes()
    path.write_bytes(bytes([previous_bytes[0] ^ 1]) + previous_bytes[1:])
    with pytest.raises(RuntimeError, match="archive_verification_checksum_mismatch"):
        resume()
    assert tail.sealed_path.exists()
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_book_prefix_chunks")) == 0
    path.write_bytes(previous_bytes)

    for end in (1, 2):
        with pytest.raises(RuntimeError, match="continuous_l2_raw_prefix_pending") as interrupted:
            resume()
        assert isinstance(interrupted.value.__cause__, fact_references.CanonicalBookPrefixBudgetExceeded)
        assert tail.sealed_path.exists()
        with storage.database.session() as session:
            tips = session.execute(text("""
                SELECT first_receive_ordinal,last_receive_ordinal
                FROM market.fact_book_prefix_chunks WHERE session_id=:id AND connection_epoch=1
                ORDER BY first_receive_ordinal
            """), {"id": claim.session_id}).all()
            assert tips == [(ordinal, ordinal) for ordinal in range(1, end + 1)]
            assert not session.scalar(text("SELECT EXISTS (SELECT 1 FROM market.fact_versions WHERE series_id=:id)"),
                                      {"id": fixture.series_id})

    raw = tail_records[0]
    reference = {field: getattr(raw, field) for field in (
        "definition_id", "session_id", "connection_epoch", "provider_product_id",
        "receive_ordinal", "raw_record_id",
    )}
    for wrong in ({"connection_epoch": 2}, {"provider_product_id": "OTHER"}):
        unrelated = SimpleNamespace(fact_type="market.l2_book", observation_key="other",
                                    provenance={"_qt_l2_evidence": {**reference, **wrong}})
        with storage.database.session() as session:
            with pytest.raises(fact_references.CanonicalBookPrefixBudgetExceeded):
                original(session, [unrelated], max_mapping_rows=3)
    resume()

    assert not tail.sealed_path.exists()
    recovery = verify_replayed()
    assert recovery["source_sha256"] == pending["source_sha256"]
    recovered, = recovery["segments"]
    assert recovered["segment_id"] == tail.spool_segment_id and recovered["mappings"]["count"] == len(tail_records)
    wrong = {**pending, "segments": [{**pending["segments"][0], "records_sha256": "0" * 64}]}
    with pytest.raises(RuntimeError, match="pending_spool_records_changed"):
        verify_replayed(wrong)
    with storage.database.session() as session:
        roots = session.execute(text("""
            SELECT payload->>'event_type',provenance->'_qt_l2_evidence'->>'raw_record_id'
            FROM market.fact_hot_payloads WHERE series_id=:id
            ORDER BY observation_time
        """), {"id": fixture.series_id}).all()
        assert roots == [("snapshot", tail_records[0].raw_record_id), ("update", tail_records[1].raw_record_id)]
        assert session.scalar(text("""
            SELECT count(*) FROM market.stream_session_events
            WHERE session_id=:id AND event_type='collector_restart_recovery_completed'
        """), {"id": claim.session_id}) == 1

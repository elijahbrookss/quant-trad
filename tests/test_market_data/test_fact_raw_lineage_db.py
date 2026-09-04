from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import json

import pytest
from sqlalchemy import text

from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, publish_spool_archive
from market_data.canonical_adapters import canonicalize_market_trade
from market_data.contracts import SourceIdentity
from market_data.structure import MarketSide
from portal.backend.service.storage.repos import fact_archival, market_structure
from portal.backend.service.storage.repos.market_lifecycle import market_storage_lifecycle_repository
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement
from tests.test_market_data.test_market_structure_archive import _record
from tests.test_market_data.test_market_state_phase3 import _trade

pytestmark = pytest.mark.db


def _raw_trade_fixture(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(market_structure, "db", storage.database)
    structures = market_structure.market_structure_repository
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="exact-lineage.fixture.v1")
    source_id = storage.repo.register_source(source, lineage={"fixture": "exact-lineage"})
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
                                           contract_version="market.trade.v1", timeframe_seconds=None)
    structures.upsert_stream_definition(
        definition_id="exact-lineage", source_id=source_id, series_id=series_id, provider=source.provider,
        venue=source.venue, provider_product_id="BTC-USD", channels=("market_trades",), auth_mode="public",
        contract_version="market.trade.v1", max_spool_bytes=1024**3, max_segment_bytes=128 * 1024**2,
        config={"fixture": "exact-lineage"},
    )
    claim = structures.claim_stream(definition_id="exact-lineage", owner_id="exact-lineage-test", lease_seconds=600, bounded=True)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    raws, manifests = [], []
    for ordinal in (1, 2):
        segment = DurableRawSpoolSegment(root=tmp_path / "spool", definition_id=claim.definition_id,
                                        session_id=claim.session_id, connection_epoch=0, segment_ordinal=ordinal - 1)
        raw = _record(segment, ordinal)
        segment.append(raw)
        segment.seal()
        encoded, ack, records = publish_spool_archive(segment, object_store=store, temporary_directory=tmp_path / "raw-staging")
        manifests.append(structures.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=records).manifest_id)
        raws.append(raw)
    return SimpleNamespace(day=day, source=source, source_id=source_id, series_id=series_id,
                           store=store, raws=raws, manifests=manifests, structures=structures, claim=claim)


def test_trade_prefix_progress_holds_raw_evidence_and_resumes_after_interruption(storage, tmp_path, monkeypatch):
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from portal.backend.service.storage.repos import fact_book_prefix, fact_lineage
    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    last = fixture.raws[-1]
    prefix = {name: getattr(last, name) for name in fact_lineage.BOOK_SCOPE_FIELDS} | {
        "first_receive_ordinal": 1, "receive_ordinal": last.receive_ordinal, "root_fact_version_id": "coverage-root",
    }
    witnesses = [{**prefix, "receive_ordinal": raw.receive_ordinal, "raw_record_id": raw.raw_record_id,
                  "root_fact_version_id": f"coverage-root:{raw.receive_ordinal}"} for raw in fixture.raws]
    def verifier():
        return ArchiveVerificationBatch(fixture.store, limits=ArchiveVerificationLimits())
    def advance():
        with storage.database.session() as session:
            market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
            return fact_book_prefix.prepare_next_trade_prefix(session, prefixes=[prefix], object_store=fixture.store,
                byte_verifier=verifier(), limits=RawArchiveReadLimits(), max_mapping_rows=4, max_objects=100)
    def verify():
        with storage.database.session() as session:
            market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
            refs, bindings = fact_book_prefix.resolve_verified_trade_prefixes(session, prefixes=[prefix], witnesses=witnesses,
                byte_verifier=verifier(), max_objects=100)
            exact = fact_lineage.resolve_canonical_raw_archive_refs(session, rows=[], object_store=fixture.store,
                byte_verifier=verifier(), book_prefix_ranges=[{**item, "first_receive_ordinal": item["receive_ordinal"],
                    "requested_channel": "market_trades"} for item in witnesses], witness_manifest_ids=bindings)
            assert exact == refs
            return refs
    first = advance()
    assert first["status"] == "trade_prefix_verified" and first["last_receive_ordinal"] == 1
    with pytest.raises(RuntimeError, match="not_ready"):
        verify()
    original_dependencies = fact_book_prefix._dependencies
    def interrupt(*args, **kwargs):
        original_dependencies(*args, **kwargs)
        raise RuntimeError("injected trade-prefix interruption after flush")
    with monkeypatch.context() as interrupted:
        interrupted.setattr(fact_book_prefix, "_dependencies", interrupt)
        with pytest.raises(RuntimeError, match="injected trade-prefix interruption"):
            advance()
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 1
        assert market_storage_lifecycle_repository.canonical_dependency_count(session,
            target_kind="raw_manifest", target_id=fixture.manifests[0]) > 0
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                              {"id": fixture.manifests[1]}).scalar_one()
    path = fixture.store.local_path(key)
    original = path.read_bytes()
    path.unlink()
    with pytest.raises(FileNotFoundError):
        advance()
    path.write_bytes(original)
    assert advance()["last_receive_ordinal"] == 2
    assert advance() is None
    assert set(verify()) == set(fixture.manifests)
    with storage.database.session() as session:
        versions = session.execute(text("SELECT DISTINCT verifier_version FROM market.fact_book_prefix_chunks")).scalars().all()
        assert versions == [fact_book_prefix.TRADE_PREFIX_VERIFIER_VERSION]
    path.write_bytes(b"corrupted verified trade prefix")
    with pytest.raises(RuntimeError, match="archive_verification"):
        verify()
    path.write_bytes(original)
    assert set(verify()) == set(fixture.manifests)


def _canonical_trade(fixture, raw, *, trade_id="same-trade"):
    trade = replace(_trade(trade_id, offset="0", side=MarketSide.BUY, price="100", receive_ordinal=1),
                    provider_product_id="BTC-USD", provider_event_time=fixture.raws[0].received_at - timedelta(seconds=1),
                    provider_message_time=raw.received_at, received_at=raw.received_at,
                    accepted_at=raw.received_at, known_at=raw.received_at, connection_epoch=raw.connection_epoch,
                    receive_ordinal=raw.receive_ordinal, raw_record_id=raw.raw_record_id,
                    coverage_interval_id=None)
    return canonicalize_market_trade(trade, source=fixture.source)


def _raw_book_fixture(storage, tmp_path, monkeypatch, *, trailing_heartbeat=False, replay_features=False,
                      definition_id="book-prefix", instrument_id="storage-fixture", provider_product_id="BTC-USD"):
    from data_providers.streams.coinbase import CoinbaseMessageParser
    from data_providers.streams.contracts import ProviderRawMessage
    from market_data.canonical_adapters import canonicalize_l2_snapshot, canonicalize_l2_mutation_batch
    from market_data.order_book import L2ProductContract, Level2BookReconstructor, translate_coinbase_l2_event
    from market_data.structure import RawStreamRecord
    from tests.test_market_data.test_fact_storage_tiers_db import BASE

    monkeypatch.setattr(market_structure, "db", storage.database)
    structures = market_structure.market_structure_repository
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="book-prefix.fixture.v1")
    source_id = storage.repo.register_source(source)
    series_id = storage.repo.register_series(instrument_id=instrument_id, fact_type="market.l2_book",
                                           contract_version="market.l2_book.v1", timeframe_seconds=None)
    contract = L2ProductContract(provider_product_id=provider_product_id, provider_size_unit="base",
                                 product_definition_version_id=f"{definition_id}.product.v1")
    structures.register_product_definition(definition_version_id=contract.product_definition_version_id,
        source_id=source_id, instrument_id=instrument_id, provider_product_id=provider_product_id, product_type="spot",
        venue=source.venue, status="fixture", base_currency="BTC", quote_currency="USD", provider_size_unit="base",
        contract_size=None, price_increment=None, base_increment=None, effective_at=BASE, received_at=BASE,
        provenance={"fixture": "book-retention"})
    config = {"product_definition_version_id": contract.product_definition_version_id, "provider_size_unit": "base"}
    if replay_features:
        from market_data.market_state import BBO_FACT_TYPE, BBO_FACT_VERSION, DEPTH_FACT_TYPE, DEPTH_FACT_VERSION
        config.update(base_currency="BTC", quote_currency="USD")
        config["bbo_series_id"] = storage.repo.register_series(instrument_id=instrument_id,
            fact_type=BBO_FACT_TYPE, contract_version=BBO_FACT_VERSION, timeframe_seconds=1)
        config["depth_series_id"] = storage.repo.register_series(instrument_id=instrument_id,
            fact_type=DEPTH_FACT_TYPE, contract_version=DEPTH_FACT_VERSION, timeframe_seconds=1)
    structures.upsert_stream_definition(definition_id=definition_id, source_id=source_id, series_id=series_id,
        provider=source.provider, venue=source.venue, provider_product_id=provider_product_id, channels=("level2",), auth_mode="public",
        contract_version="market.l2_book.v1", max_spool_bytes=1024**3, max_segment_bytes=128 * 1024**2,
        config=config)
    claim = structures.claim_stream(definition_id=definition_id, owner_id="book-prefix-test", lease_seconds=600, bounded=True)
    parser = CoinbaseMessageParser(symbol_by_product_id={provider_product_id: provider_product_id})
    reducer = Level2BookReconstructor(series_id=series_id, contract=contract)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    raws, manifests, facts, results = [], [], [], []
    for ordinal in ((1, 2, 3, 4) if trailing_heartbeat else (1, 2, 3)):
        timestamp = (BASE + timedelta(seconds=ordinal)).isoformat()
        if ordinal in (2, 4):
            payload = {"channel": "heartbeats", "timestamp": timestamp, "sequence_num": 0,
                       "events": [{"current_time": timestamp, "heartbeat_counter": "1"}]}
        else:
            levels = [("bid", "99", "10"), ("offer", "101", "11")] if ordinal == 1 else [("bid", "99", "12")]
            payload = {"channel": "l2_data", "timestamp": timestamp, "sequence_num": 0 if ordinal == 1 else 1,
                "events": [{"type": "snapshot" if ordinal == 1 else "update", "product_id": provider_product_id,
                    "updates": [{"side": side, "price_level": price, "new_quantity": quantity, "event_time": timestamp}
                                for side, price, quantity in levels]}]}
        segment = DurableRawSpoolSegment(root=tmp_path / "spool", definition_id=claim.definition_id,
            session_id=claim.session_id, connection_epoch=0, segment_ordinal=ordinal - 1)
        message = ProviderRawMessage.build(provider=source.provider, venue=source.venue,
            stream_session_id=claim.session_id, connection_epoch=0, receive_ordinal=ordinal,
            received_at=timestamp, raw_frame=json.dumps(payload))
        raw = RawStreamRecord.from_provider_message(message, definition_id=claim.definition_id,
            spool_segment_id=segment.spool_segment_id, provider_product_id=provider_product_id, requested_channel="level2",
            observed_channel="heartbeats" if ordinal in (2, 4) else "level2")
        segment.append(raw)
        segment.seal()
        encoded, ack, records = publish_spool_archive(segment, object_store=store, temporary_directory=tmp_path / "raw-staging")
        manifests.append(structures.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=records).manifest_id)
        raws.append(raw)
        for parsed in parser.parse_raw(raw.raw_frame, received_at=timestamp, raw_ref={"raw_record_id": raw.raw_record_id}):
            if parsed.event_kind not in {"market_l2_snapshot", "market_l2_update"}:
                continue
            event = translate_coinbase_l2_event(parsed, raw_record=raw, contract=contract, accepted_at=raw.received_at)
            result = reducer.process(event)
            assert result.accepted
            results.append(result)
            facts.append(canonicalize_l2_snapshot(result.snapshot, source=source) if result.snapshot is not None
                         else canonicalize_l2_mutation_batch(result.batch, source=source))
    assert len(facts) == 2 and len(raws) == (4 if trailing_heartbeat else 3)
    return SimpleNamespace(day=day, source=source, source_id=source_id, series_id=series_id, store=store,
        raws=raws, manifests=manifests, facts=facts, results=results, structures=structures, claim=claim)


def _publish_book_result(fixture, index):
    from market_data.order_book import BookLifecycle
    result = fixture.results[index]
    position = result.state.source_position
    return fixture.structures.ingest_book_facts(fixture.claim,
        snapshots=[result.snapshot] if result.snapshot is not None else [],
        batches=[result.batch] if result.batch is not None else [],
        validity_versions=result.validity_versions, lifecycle=BookLifecycle.VALID,
        final_validity_interval_id=result.state.validity_interval_id, checkpoint_id=None,
        final_state_hash=result.state.state_hash, final_connection_epoch=position.connection_epoch,
        final_receive_ordinal=position.receive_ordinal, final_event_ordinal=position.event_ordinal,
        final_sequence_num=position.provider_sequence_num)


def test_archive_keeps_each_trade_revisions_exact_raw_delivery(storage, tmp_path, monkeypatch):
    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    day, store, raws, manifests = fixture.day, fixture.store, fixture.raws, fixture.manifests
    structures, series_id, source_id = fixture.structures, fixture.series_id, fixture.source_id
    canonical = []
    for raw in raws:
        fact = _canonical_trade(fixture, raw)
        canonical.append(fact)
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id, facts=[fact])
    assert canonical[0].material_hash == canonical[1].material_hash
    assert canonical[0].row_hash != canonical[1].row_hash
    for identity in manifests:
        status = structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
        assert status["canonical_backlog_present"] is True and status["pinned"] is True
        assert status["canonical_dependency_count"] == 0
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=fact_archival.FactArchiveLimits(max_rows=1, row_group_size=1),
    )
    archive.seal_partition(day)
    for expected in manifests:
        page = archive.stage_next_page(day)
        with storage.database.session() as session:
            held = session.execute(text("SELECT target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id"),
                                   {"id": page["manifest_id"]}).scalars().all()
        assert held == [expected], "archival must not replace an older revision's raw evidence with the latest delivery"
        if page["page_ordinal"] == 0:
            with monkeypatch.context() as old_verifier:
                old_verifier.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v1")
                assert archive.verify_next_page(day)["page_ordinal"] == 0
            with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
                archive.verify_partition(day)
        assert archive.verify_next_page(day)["status"] == "page_verified"
    assert archive.verify_partition(day)["row_count"] == 2

    from portal.backend.service.storage.repos import market_data
    from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
    from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    request = {"series_id": series_id, "start": canonical[0].observation_time - timedelta(seconds=1),
               "end": canonical[0].observation_time + timedelta(seconds=1)}
    before = storage.repo.read_fact_revisions(**request)
    assert len(before) == 2
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    # Once admitted, a missing exact raw dependency must still stop hot
    # deletion; a canonical page checksum by itself is not sufficient.
    with storage.database.session() as session:
        raw_key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                                  {"id": manifests[0]}).scalar_one()
    path = store.local_path(raw_key)
    original = path.read_bytes()
    path.unlink()
    with pytest.raises(FileNotFoundError):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert archive.inspect_partition(day)["state"] == "verified"
    path.write_bytes(original)
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    assert storage.repo.read_fact_revisions(**request) == before
    for identity in manifests:
        status = structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
        assert status["canonical_dependency_count"] == 1 and status["pinned"] is True
        assert status["canonical_backlog_present"] is False


def test_older_verified_partitions_restart_without_reusing_deletion_authority(storage, tmp_path, monkeypatch):
    from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
    from portal.backend.db.fact_storage_schema import ensure_fact_payload_partition
    from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor
    from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
    from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
    from tests.test_market_data.test_fact_reclamation_db import _physical

    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    day = fixture.day
    for raw in fixture.raws:
        storage.repo.ingest_facts(series_id=fixture.series_id, source_id=fixture.source_id,
                                 facts=[_canonical_trade(fixture, raw)])
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=fixture.store, temporary_directory=tmp_path / "staging",
        limits=fact_archival.FactArchiveLimits(max_rows=1, row_group_size=1))
    archive.seal_partition(day)
    pages = [archive.stage_next_page(day), archive.stage_next_page(day)]
    legacy_version = "market.canonical_archive_verification.v2"
    with monkeypatch.context() as legacy:
        legacy.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", legacy_version)
        archive.verify_next_page(day)
        archive.verify_next_page(day)
        legacy_proof = archive.verify_partition(day)
    with storage.database.session() as session:
        receipts_before = session.execute(text("SELECT * FROM market.fact_archive_verifications ORDER BY manifest_id")).mappings().all()
        raw_key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                                  {"id": fixture.manifests[0]}).scalar_one()
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["hot_rows"] == 2
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, hot_days=1, max_steps_per_run=1,
                                          max_page_rows=1, archive_min_free_bytes=0)
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)
    def restarted_worker():
        return CanonicalFactRetentionExecutor(repository=repository).run(policy=policy, storage_root=tmp_path, execute=True)

    plan = repository.plan(policy=policy, storage_root=tmp_path)
    action = next(row for row in plan["actions"] if row["storage_day"] == day.isoformat())
    assert action["action"] == "restart_verification" and action["eligible"]
    assert archive.inspect_partition(day)["state"] == "verified", "planning must not withdraw an admission"
    restarted = restarted_worker()
    assert restarted["failure_count"] == 0
    assert restarted["outcomes"][0]["status"] == "partition_verification_restarted"
    assert restarted["outcomes"][0]["prior_manifest_set_hash"] == legacy_proof["manifest_set_hash"]
    partition = archive.inspect_partition(day)
    assert partition["state"] == "sealed" and partition["manifest_set_hash"] is None and partition["verified_at"] is None
    assert archive.restart_partition_verification(day)["status"] == "verification_already_restarted"
    with pytest.raises(RuntimeError, match="canonical_reclaim_not_verified"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)

    # The new verifier must really reread raw evidence; resuming cannot merely
    # copy old receipts into the new namespace, even with canonical bytes intact.
    path = fixture.store.local_path(raw_key)
    original = path.read_bytes()
    path.write_bytes(b"x" * len(original))
    failed = restarted_worker()
    assert failed["failure_count"] == 1 and "checksum_mismatch" in failed["outcomes"][0]["error"]
    assert archive.inspect_partition(day)["state"] == "sealed" and _physical(storage, day)["hot_rows"] == 2
    path.write_bytes(original)
    for page in pages:
        outcome = restarted_worker()["outcomes"][0]
        assert outcome["action"] == "verify_page" and outcome["manifest_id"] == page["manifest_id"]
    with pytest.raises(RuntimeError, match="canonical_reclaim_not_verified"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    proof = restarted_worker()["outcomes"][0]
    assert proof["status"] == "partition_verified" and proof["manifest_set_hash"] != legacy_proof["manifest_set_hash"]
    with pytest.raises(RuntimeError, match="verification_not_stale"):
        archive.restart_partition_verification(day)
    with storage.database.session() as session:
        old_receipts = session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE verifier_version=:version ORDER BY manifest_id"),
                                       {"version": legacy_version}).mappings().all()
        assert old_receipts == receipts_before, "older receipts are historical evidence, never overwritten or deleted"
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 4
    assert restarted_worker()["outcomes"][0]["status"] == "partition_reclaimed"
    assert _physical(storage, day)["relation"] is None
    with pytest.raises(RuntimeError, match="partition_not_verified"):
        archive.restart_partition_verification(day)

    # Empty physical partitions also have verifier-versioned admission hashes.
    empty_day = storage.today - timedelta(days=3)
    with storage.database.session() as session:
        ensure_fact_payload_partition(session.connection(), empty_day)
    archive.seal_partition(empty_day)
    with monkeypatch.context() as legacy:
        legacy.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", legacy_version)
        archive.verify_partition(empty_day)
    assert restarted_worker()["outcomes"][0]["status"] == "partition_verification_restarted"
    assert restarted_worker()["outcomes"][0]["status"] == "partition_verified"
    assert restarted_worker()["outcomes"][0]["status"] == "partition_reclaimed"


def test_book_archival_holds_intermediate_control_frames_not_only_canonical_mutations(storage, tmp_path, monkeypatch):
    from sqlalchemy.exc import DBAPIError
    from portal.backend.db.fact_storage_schema import fact_partition_name
    from portal.backend.service.storage.repos.fact_references import lock_canonical_raw_references
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch)
    for index, fact in enumerate(fixture.facts):
        _publish_book_result(fixture, index)
        if index == 0:
            with storage.database.session() as writer:
                # Three prefix positions exceed this two-row mapping budget:
                # success proves the real hot-holder fast path, not a rescan.
                lock_canonical_raw_references(writer, [fixture.facts[-1]], max_mapping_rows=2)
                with pytest.raises(DBAPIError) as busy:
                    with storage.database.session() as reclaimer:
                        reclaimer.execute(text("LOCK TABLE market." + fact_partition_name(fixture.day) + " IN ACCESS EXCLUSIVE MODE NOWAIT"))
                assert busy.value.orig.pgcode == "55P03", "the holder cannot be dropped before its successor commits"
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database,
        object_store=fixture.store, temporary_directory=tmp_path / "staging", max_raw_mapping_rows=4)
    archive.seal_partition(fixture.day)
    first = archive.stage_next_page(fixture.day)
    assert first["status"] == "book_prefix_verified" and first["last_receive_ordinal"] == 1
    from market_data.fact_archive import archive_evidence_hash
    from portal.backend.service.storage.repos.fact_book_prefix import BOOK_PREFIX_VERIFIER_VERSION
    from portal.backend.service.storage.repos.market_lifecycle import MarketStorageLifecycleBusyError
    with storage.database.session() as blocker:
        descriptor = blocker.execute(text("SELECT descriptor FROM market.fact_book_prefix_chunks WHERE id=:id"),
                                     {"id": first["chunk_id"]}).scalar_one()
        scope = {name: descriptor[name] for name in ("definition_id", "session_id", "connection_epoch", "provider_product_id")}
        scope["verifier_version"] = BOOK_PREFIX_VERIFIER_VERSION
        blocker.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                        {"name": "quant-trad:book-prefix:" + archive_evidence_hash(scope)})
        with pytest.raises(MarketStorageLifecycleBusyError, match="book_prefix_busy"):
            archive.stage_next_page(fixture.day)
    for table in ("fact_book_prefix_chunks", "fact_book_prefix_dependencies"):
        with pytest.raises(DBAPIError, match="immutable"):
            with storage.database.session() as attempt:
                attempt.execute(text(f"DELETE FROM market.{table}"))
    # The receipt and its hold commit even though the canonical page is not
    # ready. A restarted worker must continue at two, not decode one again.
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database,
        object_store=fixture.store, temporary_directory=tmp_path / "staging", max_raw_mapping_rows=4)
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                              {"id": fixture.manifests[1]}).scalar_one()
    path = fixture.store.local_path(key)
    original = path.read_bytes()
    path.unlink()
    with pytest.raises(FileNotFoundError):
        archive.stage_next_page(fixture.day)
    progress = archive.inspect_partition(fixture.day)
    assert progress["state"] == "sealed" and progress["page_count"] == 0
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 1
        assert market_storage_lifecycle_repository.canonical_dependency_count(session,
            target_kind="raw_manifest", target_id=fixture.manifests[0]) == 1
    path.write_bytes(original)
    from portal.backend.service.storage.repos import fact_book_prefix
    original_dependencies = fact_book_prefix._dependencies
    def interrupted_after_flush(*args, **kwargs):
        original_dependencies(*args, **kwargs)
        raise RuntimeError("injected interruption after chunk and hold flush")
    with monkeypatch.context() as interrupted:
        interrupted.setattr(fact_book_prefix, "_dependencies", interrupted_after_flush)
        with pytest.raises(RuntimeError, match="injected interruption"):
            archive.stage_next_page(fixture.day)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 1
    assert archive.stage_next_page(fixture.day)["last_receive_ordinal"] == 2
    assert archive.stage_next_page(fixture.day)["last_receive_ordinal"] == 3
    page = archive.stage_next_page(fixture.day)
    assert archive.verify_next_page(fixture.day)["status"] == "page_verified"
    assert archive.verify_partition(fixture.day)["row_count"] == 2
    with storage.database.session() as session:
        held = session.execute(text("SELECT target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id ORDER BY target_id"),
                               {"id": page["manifest_id"]}).scalars().all()
        assert held == sorted(fixture.manifests), "the heartbeat has no canonical row but remains replay evidence"
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                               {"day": fixture.day}).scalar_one() == 2


def test_older_book_page_reverification_builds_resumable_prefixes_without_rewriting_catalogs(storage, tmp_path, monkeypatch):
    from portal.backend.service.storage.repos import fact_book_prefix, fact_lineage
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch)
    for index in range(len(fixture.facts)):
        _publish_book_result(fixture, index)
    def worker():
        return fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database,
            object_store=fixture.store, temporary_directory=tmp_path / "staging", max_raw_mapping_rows=4)
    archive = worker()
    archive.seal_partition(fixture.day)
    def previous_prefix_admission(session, *, rows, byte_verifier, bound_manifest_ids=None, **kwargs):
        # Reproduce v4's one-shot deep prefix proof and complete raw holds,
        # without installing any new shared interval receipts.
        return fact_lineage.resolve_canonical_raw_archive_refs(session, rows=rows,
            object_store=fixture.store, byte_verifier=byte_verifier, preserve_book_prefixes=True,
            bound_manifest_ids=bound_manifest_ids), {}
    with monkeypatch.context() as legacy:
        legacy.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v4")
        legacy.setattr(archive, "_prepare_book_prefix", lambda *args, **kwargs: None)
        legacy.setattr(fact_book_prefix, "resolve_verified_book_prefixes", previous_prefix_admission)
        page = archive.stage_next_page(fixture.day)
        archive.verify_next_page(fixture.day)
        archive.verify_partition(fixture.day)
    with storage.database.session() as session:
        catalogs_before = archive._page_catalogs(session, page["manifest_id"])
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 0
    assert archive.restart_partition_verification(fixture.day)["status"] == "partition_verification_restarted"
    for ordinal in (1, 2, 3):
        progress = worker().verify_next_page(fixture.day)
        assert progress["status"] == "book_prefix_verified" and progress["last_receive_ordinal"] == ordinal
    assert worker().verify_next_page(fixture.day)["status"] == "page_verified"
    assert worker().verify_partition(fixture.day)["row_count"] == 2
    with storage.database.session() as session:
        assert archive._page_catalogs(session, page["manifest_id"]) == catalogs_before
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 2

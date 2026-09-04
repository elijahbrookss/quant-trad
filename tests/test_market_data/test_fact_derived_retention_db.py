"""Physical basis retention over hot/cold causal book-source revisions."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_basis_feature, canonicalize_bbo_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import MarketStateValuationContract, derive_basis_features, derive_book_features
from portal.backend.db import InstrumentRecord
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, fact_reclamation, market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos.fact_derived_admission import resolve_material_source_revisions
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_reclamation_db import _physical

pytestmark = pytest.mark.db


def _archive_day(archive, day):
    archive.seal_partition(day)
    for _ in range(32):
        if archive.stage_next_page(day)["status"] == "source_exhausted":
            break
    else:
        pytest.fail("fixture archive exceeded its bounded phase count")
    for _ in range(32):
        if archive.verify_next_page(day)["status"] == "no_unverified_pages":
            break
    else:
        pytest.fail("fixture verification exceeded its bounded phase count")
    return archive.verify_partition(day)


@pytest.mark.parametrize("source_cold", [False, True, "legacy"])
def test_basis_reclaims_with_every_causal_source_and_preserves_frozen_research(storage, tmp_path, monkeypatch, source_cold):
    legacy = source_cold == "legacy"
    if legacy:
        source_cold = False
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    with storage.database.session() as session:
        session.add(InstrumentRecord(id="basis-future", datasource="TEST", exchange="ISOLATED", symbol="BTC-FUT",
            instrument_type="futures", can_short=True, short_requires_borrow=False, has_funding=False, extra_metadata={}))
    futures = _raw_book_fixture(storage, tmp_path, monkeypatch, replay_features=True,
        definition_id="basis-future", instrument_id="basis-future", provider_product_id="BTC-FUT")
    spot = _raw_book_fixture(storage, tmp_path, monkeypatch, replay_features=True)
    features = []
    for fixture in (futures, spot):
        for index in range(len(fixture.results)):
            _publish_book_result(fixture, index)
        config = fixture.claim.config
        bbo, _ = derive_book_features((item.state for item in fixture.results),
            contract=MarketStateValuationContract(product_definition_version_id=config["product_definition_version_id"],
                provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
            bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
            computed_at=BASE + timedelta(minutes=1))
        fixture.structures.ingest_market_state_features(bbo_facts=bbo)
        features.append(bbo)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    # Three same-content revisions: the original and second delivery are causal;
    # a revision known tomorrow must not enter today's basis dependency closure.
    duplicate = canonicalize_bbo_feature(features[0][0], source=DERIVED_MARKET_STATE_SOURCE,
                                         provenance={"fixture_delivery": "second"})
    storage.repo.ingest_facts(series_id=features[0][0].series_id, source_id=source_id, facts=[duplicate])
    future_known = replace(duplicate, known_at=BASE + timedelta(days=1), accepted_at=BASE + timedelta(days=1),
                           provenance={**duplicate.provenance, "fixture_delivery": "future-known"})
    future_known_outcome = storage.repo.ingest_facts(series_id=features[0][0].series_id, source_id=source_id, facts=[future_known])
    mapping_id = futures.structures.register_instrument_mapping(primary_instrument_id="basis-future",
        related_instrument_id="storage-fixture", role="spot_reference", effective_from=BASE,
        mapping_reason="isolated basis retention fixture", mapping_source="fixture.v1")
    series_id = storage.repo.register_series(instrument_id="basis-future", fact_type="market.futures_spot_relationship",
        contract_version="market.futures_spot_basis.v1", timeframe_seconds=1)
    basis = derive_basis_features(features[0], features[1], mapping_id=mapping_id,
                                   computed_at=BASE + timedelta(minutes=1), series_id=series_id)
    assert len(basis) == 2
    day = futures.day + timedelta(days=1)
    _placement(monkeypatch, day)
    basis_outcome = futures.structures.ingest_market_state_features(basis_facts=basis)
    root = canonicalize_basis_feature(basis[0])
    revised = replace(root, known_at=root.known_at + timedelta(milliseconds=100),
        accepted_at=root.known_at + timedelta(milliseconds=100), provenance={**root.provenance, "fixture_revision": 2})
    invalidated = replace(revised, state="invalidated", known_at=root.known_at + timedelta(milliseconds=200),
        accepted_at=root.known_at + timedelta(milliseconds=200), provenance={**root.provenance, "fixture_revision": 3})
    for revision in (revised, invalidated):
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id, facts=[revision])
    # A later commit with an old known-at is not an input to an earlier output.
    _placement(monkeypatch, storage.today)
    late = replace(duplicate, provenance={**duplicate.provenance, "fixture_delivery": "later-commit"})
    late_outcome = storage.repo.ingest_facts(series_id=features[0][0].series_id, source_id=source_id, facts=[late])
    assert late_outcome.max_commit_seq > basis_outcome.max_commit_seq > future_known_outcome.max_commit_seq
    reader = FilesystemRawArchiveObjectStore(futures.store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    request = DatasetSeriesRequest(series_id, BASE - timedelta(seconds=1), BASE + timedelta(minutes=1))
    with monkeypatch.context() as prior_selection:
        prior_selection.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset([request])
    with pytest.raises(RuntimeError, match="market_dataset_revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=series_id)
    frozen = storage.repo.freeze_dataset([request])
    assert frozen.dataset_hash != older_dataset.dataset_hash
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
    assert len(before) == 4 and any(record.fact.state.value == "invalidated" for record in before)
    binding_args = dict(store=storage.repo, entry={**frozen.series[0], "dataset_id": frozen.dataset_id})
    binding_before = validate_frozen_dataset_series(**binding_args)
    typed_args = dict(series_id=series_id, start=request.start, end=request.end, known_at=BASE + timedelta(minutes=1))
    typed_before = futures.structures.read_basis_features(**typed_args)
    known_args = dict(series_id=series_id, start=request.start, end=request.end, known_at_lte=basis[0].known_at)
    known_before = storage.repo.read_facts(**known_args)
    assert len(known_before) == 1
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database,
        object_store=futures.store, temporary_directory=tmp_path / "staging")
    reclaimer = fact_reclamation.PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        _archive_day(archive, futures.day)
        assert reclaimer.reclaim_partition(futures.day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
        assert _physical(storage, futures.day)["relation"] is None
    # When the inputs are hot, this must prepare both full book prefixes itself.
    # When cold, it must find old causal revisions beside a newer hot delivery.
    with monkeypatch.context() as prior:
        if legacy:
            original_dependencies = archive._dependencies
            prior.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v6")
            prior.setattr(archive, "_dependencies", lambda *args, **kwargs: (original_dependencies(*args, **kwargs)[0], []))
        _archive_day(archive, day)
    if legacy:
        with storage.database.session() as session:
            old_page = dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                           {"day": day}).mappings().one())
            old_receipt = dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id"),
                                               {"id": old_page["id"]}).mappings().one())
            assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies WHERE manifest_id=:id"),
                                   {"id": old_page["id"]}).scalar_one() == 0
        with pytest.raises(RuntimeError, match="canonical_archive_verification_missing_or_stale"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
        assert _physical(storage, day)["relation"] is not None
        assert archive.restart_partition_verification(day)["status"] == "partition_verification_restarted"
        assert archive.verify_next_page(day)["status"] == "page_verified"
        archive.verify_partition(day)
        with storage.database.session() as session:
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE id=:id"),
                                        {"id": old_page["id"]}).mappings().one()) == old_page
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_verifications "
                "WHERE manifest_id=:id AND verifier_version=:version"),
                {"id": old_page["id"], "version": old_receipt["verifier_version"]}).mappings().one()) == old_receipt
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT source.id,source.fact_type,source.market_commit_seq,source.known_at
            FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_archive_manifests AS manifest ON manifest.id=edge.manifest_id
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id
            WHERE manifest.storage_day=:day ORDER BY source.id
        """), {"day": day}).mappings().all()
        assert len(edges) == 9, "four L2 states and five causal BBO revisions must all be bound"
        assert sum(item["fact_type"] == "market.bbo" for item in edges) == 5
        assert all(item["market_commit_seq"] not in {future_known_outcome.max_commit_seq, late_outcome.max_commit_seq} for item in edges)
        receipts = session.execute(text("SELECT verifier_version FROM market.fact_archive_verifications AS proof "
            "JOIN market.fact_archive_manifests AS manifest ON manifest.id=proof.manifest_id WHERE manifest.storage_day=:day"),
            {"day": day}).scalars().all()
        assert set(receipts) == ({"market.canonical_archive_verification.v6", "market.canonical_archive_verification.v7"}
                                 if legacy else {"market.canonical_archive_verification.v7"})
    if not source_cold:
        # Legitimate source movement after receipt must not invalidate immutable
        # basis evidence. Final destructive admission still rereads current bytes.
        _archive_day(archive, futures.day)
        reclaimer.reclaim_partition(futures.day, eligible_before=storage.today, execute=True)
    with storage.database.session() as session:
        source_page = session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                      {"day": futures.day}).mappings().one()
    path = futures.store.local_path(source_page["object_key"])
    original = path.read_bytes()
    path.write_bytes(b"corrupt transitive source after basis verification")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, day)
    result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert result["status"] == "partition_reclaimed" and result["reclaimed_bytes"] == physical["bytes"] > 0
    assert _physical(storage, day)["relation"] is None
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == before
    assert validate_frozen_dataset_series(**binding_args) == binding_before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    assert storage.repo.read_facts(**known_args) == known_before
    assert futures.structures.read_basis_features(**typed_args) == typed_before
    # Families with only canonical material hashes have no optional alias key.
    # The material resolver must not construct an invalid JSON null-key probe.
    with storage.database.session() as session:
        selected, _ = resolve_material_source_revisions(session, requests=[{
            "root_id": "canonical-material-only", "role": "input", "series_id": futures.series_id,
            "fact_type": "market.l2_book", "material_hash": futures.facts[0].material_hash,
            "commit_seq": basis_outcome.max_commit_seq, "known_at": BASE + timedelta(minutes=1),
        }], reader=tiered, max_rows=10, max_logical_bytes=64 * 1024**2)
        assert len(selected) == 1 and next(iter(selected.values()))["material_hash"] == futures.facts[0].material_hash
    for fixture in (futures, spot):
        for identity in fixture.manifests:
            status = fixture.structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
            assert status["canonical_dependency_count"] > 0 and status["pinned"] is True

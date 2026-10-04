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

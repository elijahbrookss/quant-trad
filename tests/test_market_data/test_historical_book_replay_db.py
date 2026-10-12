"""Historical book reconciliation must not depend on mutable current state."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.market_state import derive_book_features, MarketStateValuationContract
from market_data.order_book import L2ProductContract, Level2BookReconstructor
from portal.backend.service.market.market_structure_service import MarketStructureService
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_book_retention_db import (
    _cold_book_handoff as _cold_book,
)

pytestmark = pytest.mark.db


def _publish_complete_book(book):
    for index in range(len(book.results)):
        _publish_book_result(book, index)
    config = book.claim.config
    bbo, depth = derive_book_features((result.state for result in book.results),
        contract=MarketStateValuationContract(
            product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit=config["provider_size_unit"],
            base_currency=config["base_currency"], quote_currency=config["quote_currency"]),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=book.raws[-1].received_at+timedelta(minutes=1))
    book.structures.ingest_market_state_features(bbo_facts=bbo, depth_facts=depth)


def test_cold_book_replay_survives_same_series_session_rollover(storage, tmp_path, monkeypatch):
    old = _cold_book(storage, tmp_path, monkeypatch, split_sources=False)
    service = MarketStructureService(repository=old.structures)
    old_args = dict(definition_id=old.claim.definition_id, session_id=old.claim.session_id,
                    storage_root=tmp_path)
    expected = service.replay_book_session(**old_args)
    with storage.database.session() as session:
        pairs = session.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
    frozen = {tuple(pair): storage.repo.read_dataset_fact_revisions(
        dataset_id=pair[0], series_id=pair[1]) for pair in pairs}

    # Real ownership handoff, not overwriting/deleting the old projection as
    # setup. Later event times preserve the original feature-query windows.
    old.structures.release(old.claim)
    new = _raw_book_fixture(storage, tmp_path, monkeypatch, trailing_heartbeat=True,
                            replay_features=True, event_start=BASE+timedelta(hours=1))
    assert new.series_id == old.series_id and new.claim.session_id != old.claim.session_id
    _placement(monkeypatch, storage.today)
    _publish_complete_book(new)
    with storage.database.session() as session:
        current = session.execute(text("""
            SELECT definition_id,session_id FROM market.book_reconstruction_state
            WHERE series_id=:series
        """), {"series": old.series_id}).one()
        assert tuple(current) == (new.claim.definition_id, new.claim.session_id)
    assert service.replay_book_session(**old_args) == expected
    new_args = dict(definition_id=new.claim.definition_id, session_id=new.claim.session_id,
                    storage_root=tmp_path)
    new_expected = service.replay_book_session(**new_args)
    assert new_expected["snapshot_count"] == 1 and new_expected["mutation_batch_count"] == 1
    for (dataset, series), records in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=series) == records

    # Losing a disposable projection cannot lose retained session history.
    with storage.database.session() as session:
        session.execute(text("DELETE FROM market.book_reconstruction_state WHERE series_id=:series"),
                        {"series": old.series_id})
    assert service.replay_book_session(**old_args) == expected
    assert service.replay_book_session(**new_args) == new_expected
    args = dict(definition_id=old.claim.definition_id, session_id=old.claim.session_id,
                snapshot_ids=[old.results[0].snapshot.snapshot_id],
                batch_ids=[old.results[-1].batch.batch_id],
                final_state_hash=old.results[-1].state.state_hash)
    for change in ({"final_state_hash": None}, {"final_state_hash": "0"*64},
                   {"snapshot_ids": []}, {"batch_ids": []}):
        with pytest.raises(RuntimeError, match="market_book_replay_reconciliation_failed"):
            old.structures.reconcile_book_replay(**(args | change))


@pytest.mark.parametrize("invalidated", [False, True], ids=["clean-close", "invalidated"])
def test_terminal_validity_preserves_hash_checks_without_current_state(storage, tmp_path, monkeypatch, invalidated):
    book = _raw_book_fixture(storage, tmp_path, monkeypatch, trailing_heartbeat=True)
    for index in range(len(book.results)):
        _publish_book_result(book, index)
    contract = L2ProductContract(provider_product_id="BTC-USD", provider_size_unit="base",
                                product_definition_version_id=book.claim.config["product_definition_version_id"])
    reducer = Level2BookReconstructor.from_checkpoint(book.results[0].checkpoints[0],
        contract=contract, validity=book.results[0].validity_versions[0])
    last = reducer.process(book.results[-1].batch.event)
    assert last.state.state_hash == book.results[-1].state.state_hash
    position = replace(last.state.source_position, receive_ordinal=book.raws[-1].receive_ordinal)
    boundary = dict(position=position, effective_at=book.raws[-1].received_at,
                    known_at=book.raws[-1].received_at, reason="disposable terminal boundary")
    if invalidated:
        closed = reducer.invalidate_transport(**boundary, raw_record_id=book.raws[-1].raw_record_id,
                                             classification="disconnect")
    else:
        closed = reducer.close_valid_at(**boundary)
    book.structures.ingest_book_facts(book.claim, snapshots=[], batches=[],
        validity_versions=closed.validity_versions, lifecycle=reducer.lifecycle,
        final_validity_interval_id=None, checkpoint_id=None, final_state_hash=None,
        final_connection_epoch=position.connection_epoch, final_receive_ordinal=position.receive_ordinal,
        final_event_ordinal=position.event_ordinal, final_sequence_num=position.provider_sequence_num)
    args = dict(definition_id=book.claim.definition_id, session_id=book.claim.session_id,
                snapshot_ids=[book.results[0].snapshot.snapshot_id],
                batch_ids=[book.results[-1].batch.batch_id],
                final_state_hash=None if invalidated else last.state.state_hash)
    # A graceful close clears live state, but raw replay still proves the last
    # accepted hash. Invalidation instead proves that no valid terminal book exists.
    assert book.structures.reconcile_book_replay(**args)["equal"]
    with storage.database.session() as session:
        session.execute(text("DELETE FROM market.book_reconstruction_state WHERE series_id=:series"),
                        {"series": book.series_id})
    assert book.structures.reconcile_book_replay(**args)["equal"]
    wrong = last.state.state_hash if invalidated else None
    with pytest.raises(RuntimeError, match="market_book_replay_reconciliation_failed"):
        book.structures.reconcile_book_replay(**(args | {"final_state_hash": wrong}))

    # Retained terminal evidence must agree with the immutable accepted event.
    corrupt = replace(closed.validity_versions[0], version_id="corrupt-terminal-fixture",
                      revision=closed.validity_versions[0].revision+1, last_state_hash="f"*64)
    book.structures.ingest_book_facts(book.claim, snapshots=[], batches=[], validity_versions=[corrupt],
        lifecycle=reducer.lifecycle, final_validity_interval_id=None, checkpoint_id=None,
        final_state_hash=None, final_connection_epoch=position.connection_epoch,
        final_receive_ordinal=position.receive_ordinal, final_event_ordinal=position.event_ordinal,
        final_sequence_num=position.provider_sequence_num)
    with pytest.raises(RuntimeError, match="terminal validity mismatch"):
        book.structures.reconcile_book_replay(**args)

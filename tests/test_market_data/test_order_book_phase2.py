from __future__ import annotations

import gzip
import json
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.book_archive import (
    encode_book_checkpoint_parquet,
    publish_book_checkpoint,
    read_book_checkpoint_parquet,
)
from market_data.order_book import (
    BookLifecycle,
    BookSide,
    BookSourcePosition,
    BookValidityStatus,
    L2EventFact,
    L2EventType,
    L2Mutation,
    L2ProductContract,
    Level2BookReconstructor,
    checkpoint_canonical_rows,
    translate_coinbase_l2_event,
)
from market_data.structure import (
    OrderingAssurance,
    PHASE1_COINBASE_TRADE_CONTRACTS,
    ProviderSizeUnit,
    RawStreamRecord,
    build_spool_segment_id,
)


FIXTURE_PATH = (
    Path(__file__).parents[1]
    / "fixtures/providers/coinbase/market_structure_phase0/raw_frames.json.gz"
)
BASE_TIME = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)


def _contract(product_id: str = "BIP-20DEC30-CDE") -> L2ProductContract:
    trade = PHASE1_COINBASE_TRADE_CONTRACTS[product_id]
    return L2ProductContract(
        provider_product_id=product_id,
        product_definition_version_id=trade.product_definition_version_id,
        provider_size_unit=trade.provider_size_unit,
        price_increment=Decimal("1") if trade.provider_size_unit is ProviderSizeUnit.CONTRACTS else None,
        quantity_increment=Decimal("1") if trade.provider_size_unit is ProviderSizeUnit.CONTRACTS else None,
    )


def _event(
    *,
    event_type: L2EventType,
    receive_ordinal: int,
    sequence_num: int,
    updates: list[tuple[BookSide, str, str]],
    event_ordinal: int = 0,
) -> L2EventFact:
    effective = BASE_TIME + timedelta(seconds=receive_ordinal)
    return L2EventFact(
        event_type=event_type,
        position=BookSourcePosition(
            definition_id="ms-l2-bip",
            session_id="session-a",
            connection_epoch=0,
            provider_product_id="BIP-20DEC30-CDE",
            provider_sequence_num=sequence_num,
            receive_ordinal=receive_ordinal,
            event_ordinal=event_ordinal,
        ),
        product_definition_version_id="coinbase.BIP-20DEC30-CDE.phase0.v1",
        mutations=tuple(
            L2Mutation(
                mutation_ordinal=ordinal,
                side=side,
                price=price,
                new_quantity=quantity,
                provider_event_time=effective,
                provider_size_unit=ProviderSizeUnit.CONTRACTS,
            )
            for ordinal, (side, price, quantity) in enumerate(updates)
        ),
        provider_message_time=effective,
        received_at=effective + timedelta(milliseconds=1),
        accepted_at=effective + timedelta(milliseconds=2),
        known_at=effective + timedelta(milliseconds=2),
        raw_record_id=f"raw-{receive_ordinal}-{event_ordinal}",
    )


def _snapshot(*, receive_ordinal: int = 1, sequence_num: int = 1) -> L2EventFact:
    return _event(
        event_type=L2EventType.SNAPSHOT,
        receive_ordinal=receive_ordinal,
        sequence_num=sequence_num,
        updates=[
            (BookSide.BID, "99", "10"),
            (BookSide.BID, "98", "20"),
            (BookSide.ASK, "101", "11"),
            (BookSide.ASK, "102", "21"),
        ],
    )


def test_snapshot_and_absolute_update_are_atomic() -> None:
    reducer = Level2BookReconstructor(series_id=1, contract=_contract())
    snapshot_result = reducer.process(_snapshot())
    assert snapshot_result.accepted is True
    assert reducer.lifecycle is BookLifecycle.VALID
    assert snapshot_result.snapshot is not None
    assert snapshot_result.checkpoints[0].provider_size_unit is ProviderSizeUnit.CONTRACTS

    update = _event(
        event_type=L2EventType.UPDATE,
        receive_ordinal=2,
        sequence_num=2,
        updates=[
            (BookSide.BID, "99", "12"),
            (BookSide.ASK, "102", "0"),
            (BookSide.ASK, "103", "5"),
            (BookSide.BID, "95", "0"),
        ],
    )
    result = reducer.process(update)
    assert result.accepted is True
    assert reducer.bids[Decimal("99")] == Decimal("12")
    assert Decimal("102") not in reducer.asks
    assert reducer.asks[Decimal("103")] == Decimal("5")
    assert result.batch is not None
    assert result.batch.unknown_zero_delete_count == 1
    assert [item.classification for item in result.quality] == ["unknown_zero_delete"]
    assert result.validity_versions == ()


def test_update_before_snapshot_is_suppressed_without_claiming_validity() -> None:
    reducer = Level2BookReconstructor(series_id=1, contract=_contract())
    result = reducer.process(
        _event(
            event_type=L2EventType.UPDATE,
            receive_ordinal=1,
            sequence_num=1,
            updates=[(BookSide.BID, "99", "12")],
        )
    )
    assert result.accepted is False
    assert reducer.lifecycle is BookLifecycle.AWAITING_SNAPSHOT
    assert result.quality[0].classification == "update_before_snapshot"


def test_crossed_batch_invalidates_and_fresh_snapshot_resynchronizes() -> None:
    reducer = Level2BookReconstructor(series_id=1, contract=_contract())
    opened = reducer.process(_snapshot())
    opening_interval = opened.validity_versions[-1]
    crossed = _event(
        event_type=L2EventType.UPDATE,
        receive_ordinal=2,
        sequence_num=2,
        updates=[(BookSide.BID, "101", "1")],
    )
    invalid = reducer.process(crossed)
    assert invalid.accepted is False
    assert reducer.lifecycle is BookLifecycle.INVALID
    assert invalid.quality[0].classification == "book_invalid"
    assert invalid.validity_versions[0].interval_id == opening_interval.interval_id
    assert invalid.validity_versions[0].status is BookValidityStatus.CLOSED_INVALIDATED
    assert reducer.bids == {} and reducer.asks == {}

    healed = reducer.process(_snapshot(receive_ordinal=3, sequence_num=3))
    assert healed.accepted is True
    assert reducer.lifecycle is BookLifecycle.VALID
    assert healed.validity_versions[-1].interval_id != opening_interval.interval_id


def test_exact_duplicate_is_noop_and_divergent_duplicate_invalidates() -> None:
    reducer = Level2BookReconstructor(series_id=1, contract=_contract())
    snapshot = _snapshot()
    first = reducer.process(snapshot)
    state_hash = reducer.current_state_hash
    duplicate_delivery = replace(
        snapshot,
        position=replace(snapshot.position, receive_ordinal=2),
        received_at=snapshot.received_at + timedelta(seconds=1),
        accepted_at=snapshot.accepted_at + timedelta(seconds=1),
        known_at=snapshot.known_at + timedelta(seconds=1),
        raw_record_id="raw-redelivery",
    )
    duplicate = reducer.process(duplicate_delivery)
    assert first.accepted is True
    assert duplicate.accepted is False
    assert duplicate.quality[0].classification == "duplicate"
    assert reducer.current_state_hash == state_hash

    changed_mutations = list(snapshot.mutations)
    changed_mutations[0] = replace(changed_mutations[0], new_quantity=Decimal("99"))
    divergent = reducer.process(
        replace(duplicate_delivery, mutations=tuple(changed_mutations))
    )
    assert divergent.quality[0].classification == "divergent_duplicate"
    assert reducer.lifecycle is BookLifecycle.INVALID


def test_full_replay_and_checkpoint_plus_delta_are_equal() -> None:
    events = [_snapshot()]
    rng = random.Random(20260802)
    bid_prices = [Decimal("96"), Decimal("97"), Decimal("98"), Decimal("99")]
    ask_prices = [Decimal("101"), Decimal("102"), Decimal("103"), Decimal("104")]
    for ordinal in range(2, 202):
        side = BookSide.BID if rng.randrange(2) == 0 else BookSide.ASK
        price = rng.choice(bid_prices if side is BookSide.BID else ask_prices)
        quantity = Decimal(rng.randrange(1, 50))
        events.append(
            _event(
                event_type=L2EventType.UPDATE,
                receive_ordinal=ordinal,
                sequence_num=ordinal,
                updates=[(side, str(price), str(quantity))],
            )
        )

    full = Level2BookReconstructor(series_id=1, contract=_contract())
    full_results = [full.process(event) for event in events]
    full_hash = full.current_state_hash

    repeated = Level2BookReconstructor(series_id=1, contract=_contract())
    repeated_hashes = []
    for event in events:
        repeated.process(event)
        repeated_hashes.append(repeated.current_state_hash)
    assert repeated.current_state_hash == full_hash
    assert repeated_hashes[-1] == full_hash

    initial_checkpoint = full_results[0].checkpoints[0]
    opening_validity = full_results[0].validity_versions[-1]
    resumed = Level2BookReconstructor.from_checkpoint(
        initial_checkpoint,
        contract=_contract(),
        validity=opening_validity,
    )
    for event in events[1:]:
        assert resumed.process(event).accepted is True
    assert resumed.current_state_hash == full_hash
    assert resumed.bids == full.bids
    assert resumed.asks == full.asks


def test_randomized_batch_partition_replay_is_stable() -> None:
    rng = random.Random(44)
    for case in range(50):
        reducer_a = Level2BookReconstructor(series_id=case + 1, contract=_contract())
        reducer_b = Level2BookReconstructor(series_id=case + 1, contract=_contract())
        events = [_snapshot()]
        for ordinal in range(2, 30):
            updates = []
            for _ in range(rng.randrange(1, 6)):
                side = BookSide.BID if rng.randrange(2) == 0 else BookSide.ASK
                prices = range(90, 100) if side is BookSide.BID else range(101, 111)
                updates.append((side, str(rng.choice(tuple(prices))), str(rng.randrange(1, 100))))
            events.append(
                _event(
                    event_type=L2EventType.UPDATE,
                    receive_ordinal=ordinal,
                    sequence_num=ordinal,
                    updates=updates,
                )
            )
        hashes_a = [reducer_a.process(event).accepted for event in events]
        hashes_b = [reducer_b.process(event).accepted for event in events]
        assert hashes_a == hashes_b
        assert reducer_a.current_state_hash == reducer_b.current_state_hash
        assert reducer_a.bids == reducer_b.bids
        assert reducer_a.asks == reducer_b.asks


@pytest.mark.parametrize("product_id", ["BIP-20DEC30-CDE", "BTC-USD"])
def test_captured_coinbase_snapshot_and_update_replay_exactly(product_id: str) -> None:
    with gzip.open(FIXTURE_PATH, "rt", encoding="utf-8") as handle:
        rows = json.load(handle)["frames"]
    parser = CoinbaseMessageParser()
    contract = _contract(product_id)
    reducer = Level2BookReconstructor(series_id=1, contract=contract)
    accepted = []
    receive_ordinal = 0
    for row in rows:
        raw_frame = row["raw_frame"].encode("utf-8")
        payload = json.loads(raw_frame)
        if payload.get("channel") != "l2_data":
            continue
        if not any(event.get("product_id") == product_id for event in payload.get("events", [])):
            continue
        receive_ordinal += 1
        received_at = row.get("received_at") or "2026-08-02T08:00:00Z"
        message = ProviderRawMessage.build(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            stream_session_id="fixture-session",
            connection_epoch=0,
            receive_ordinal=receive_ordinal,
            received_at=received_at,
            raw_frame=raw_frame,
        )
        segment = build_spool_segment_id(
            definition_id=f"fixture-{product_id}",
            session_id="fixture-session",
            connection_epoch=0,
            segment_ordinal=0,
        )
        raw = RawStreamRecord.from_provider_message(
            message,
            definition_id=f"fixture-{product_id}",
            spool_segment_id=segment,
            provider_product_id=product_id,
            requested_channel="level2",
            observed_channel="l2_data",
        )
        for event in parser.parse_raw(raw_frame, received_at=received_at):
            if event.product_id != product_id or not event.event_kind.startswith("market_l2_"):
                continue
            fact = translate_coinbase_l2_event(
                event,
                raw_record=raw,
                contract=contract,
                accepted_at=raw.received_at,
            )
            accepted.append(reducer.process(fact).accepted)
    assert accepted == [True, True]
    assert reducer.lifecycle is BookLifecycle.VALID
    assert reducer.current_state_hash is not None
    assert reducer.bids and reducer.asks


def test_checkpoint_rows_are_sorted_typed_and_fingerprinted() -> None:
    result = Level2BookReconstructor(series_id=1, contract=_contract()).process(_snapshot())
    checkpoint = result.checkpoints[0]
    rows = checkpoint_canonical_rows(checkpoint)
    assert rows[0]["side"] == "bid"
    assert [Decimal(row["price"]) for row in rows[:2]] == [Decimal("98"), Decimal("99")]
    assert all(row["provider_size_unit"] == "contracts" for row in rows)
    assert len(checkpoint.content_fingerprint) == 64


def test_checkpoint_object_is_deterministic_verified_and_reusable(tmp_path: Path) -> None:
    checkpoint = Level2BookReconstructor(
        series_id=1, contract=_contract()
    ).process(_snapshot()).checkpoints[0]
    first = encode_book_checkpoint_parquet(
        checkpoint, temporary_directory=tmp_path / "tmp"
    )
    second = encode_book_checkpoint_parquet(
        checkpoint, temporary_directory=tmp_path / "tmp"
    )
    try:
        assert first.sha256 == second.sha256
        assert first.content_fingerprint == second.content_fingerprint
        assert read_book_checkpoint_parquet(first.path) == checkpoint_canonical_rows(
            checkpoint
        )
    finally:
        first.path.unlink(missing_ok=True)
        second.path.unlink(missing_ok=True)

    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    encoded, acknowledgement = publish_book_checkpoint(
        checkpoint,
        object_store=store,
        temporary_directory=tmp_path / "tmp",
    )
    assert acknowledgement.sha256 == encoded.sha256
    assert acknowledgement.reused_existing is False
    _encoded_again, acknowledgement_again = publish_book_checkpoint(
        checkpoint,
        object_store=store,
        temporary_directory=tmp_path / "tmp",
    )
    assert acknowledgement_again.reused_existing is True


def test_external_sequence_gap_closes_validity_and_suppresses_updates() -> None:
    reducer = Level2BookReconstructor(
        series_id=1,
        contract=_contract(),
        ordering_assurance=OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
    )
    reducer.process(_snapshot())
    gap_event = _event(
        event_type=L2EventType.UPDATE,
        receive_ordinal=2,
        sequence_num=4,
        updates=[(BookSide.BID, "99", "12")],
    )
    result = reducer.invalidate(
        gap_event,
        classification="sequence_gap",
        reason="connection-wide sequence advanced from 1 to 4",
        evidence={"sequence_before": 1, "sequence_after": 4},
    )
    assert result.validity_versions[0].status is BookValidityStatus.CLOSED_INVALIDATED
    assert reducer.process(gap_event).quality[0].classification == "update_before_snapshot"
    reconnect_snapshot = _snapshot(receive_ordinal=3, sequence_num=0)
    reconnect_snapshot = replace(
        reconnect_snapshot,
        position=replace(
            reconnect_snapshot.position,
            connection_epoch=1,
            receive_ordinal=1,
        ),
    )
    resynchronized = reducer.process(reconnect_snapshot)
    assert resynchronized.accepted is True
    assert resynchronized.snapshot is not None
    assert reducer.lifecycle is BookLifecycle.VALID
    assert (
        resynchronized.validity_versions[-1].interval_id
        != result.validity_versions[0].interval_id
    )

from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    MarketSide,
    OrderingAssurance,
    ProductContract,
    ProviderSizeUnit,
    RawStreamRecord,
    TradeCoverageIntervalVersion,
    aggregate_trade_bucket,
    bucket_start_for,
    build_raw_record_id,
    build_spool_segment_id,
    translate_coinbase_market_trade,
)
from market_data.stream_enrollment import load_stream_enrollment_manifest


FIXTURE_PATH = (
    Path(__file__).parents[1]
    / "fixtures/providers/coinbase/market_structure_phase0/raw_frames.json.gz"
)


def _product_contract(product_id: str) -> ProductContract:
    manifest = load_stream_enrollment_manifest(
        "config/market_data/coinbase_perpetual_trade_fleet.v1.json"
    )
    for enrollment in manifest.enrollments:
        if enrollment.product_contract.provider_product_id == product_id:
            return enrollment.product_contract
    base, quote = product_id.split("-", maxsplit=1)
    return ProductContract(
        provider_product_id=product_id,
        provider_size_unit="base",
        base_currency=base,
        quote_currency=quote,
        product_definition_version_id=f"coinbase.{product_id}.product_contract.v1",
    )


def _captured_trade_event(product_id: str, *, delivery_kind: str = "update"):
    with gzip.open(FIXTURE_PATH, "rt", encoding="utf-8") as handle:
        rows = json.load(handle)["frames"]
    for row in rows:
        raw = row["raw_frame"]
        payload = json.loads(raw)
        if payload.get("channel") != "market_trades":
            continue
        events = payload.get("events") or []
        if not events or events[0].get("type") != delivery_kind:
            continue
        trades = events[0].get("trades") or []
        if not trades or trades[0].get("product_id") != product_id:
            continue
        received_at = "2026-08-02T07:20:00Z"
        parser = CoinbaseMessageParser()
        parsed = parser.parse_raw(raw, received_at=received_at)
        return next(event for event in parsed if event.event_kind == "market_trade"), raw
    raise AssertionError(f"captured frame missing for {product_id}/{delivery_kind}")


def _fact(product_id: str = "BTC-USD"):
    event, raw = _captured_trade_event(product_id)
    raw_message = ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id="session-a",
        connection_epoch=0,
        receive_ordinal=7,
        received_at=event.received_at,
        raw_frame=raw,
    )
    segment_id = build_spool_segment_id(
        definition_id=f"definition-{product_id}",
        session_id="session-a",
        connection_epoch=0,
        segment_ordinal=0,
    )
    record = RawStreamRecord.from_provider_message(
        raw_message,
        definition_id=f"definition-{product_id}",
        spool_segment_id=segment_id,
        provider_product_id=product_id,
        requested_channel="market_trades",
        observed_channel="market_trades",
    )
    accepted_at = datetime(2026, 8, 2, 7, 20, 1, tzinfo=UTC)
    fact = translate_coinbase_market_trade(
        event,
        contract=_product_contract(product_id),
        raw_record_id=record.raw_record_id,
        connection_epoch=0,
        receive_ordinal=7,
        accepted_at=accepted_at,
        coverage_interval_id="coverage-a",
    )
    return fact, record


def _coverage(
    fact,
    *,
    archive_status: ArchiveStatus = ArchiveStatus.COMPLETE,
    status: CoverageStatus = CoverageStatus.CLOSED_VALID,
    assurance: OrderingAssurance = OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
    gaps: tuple[str, ...] = (),
):
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    return TradeCoverageIntervalVersion(
        interval_id="coverage-a",
        revision=2,
        definition_id="definition-a",
        session_id="session-a",
        connection_epoch=0,
        provider_product_id=fact.provider_product_id,
        channel="market_trades",
        status=status,
        ordering_assurance=assurance,
        archive_status=archive_status,
        opening_raw_record_id="raw-opening",
        opening_receive_ordinal=1,
        opening_effective_at=bucket - timedelta(seconds=1),
        last_raw_record_id="raw-closing",
        last_receive_ordinal=10,
        last_effective_at=bucket + timedelta(seconds=2),
        closing_raw_record_id="raw-closing",
        closing_receive_ordinal=10,
        closing_effective_at=bucket + timedelta(seconds=2),
        canonicalization_watermark_ordinal=10,
        archive_complete_through_ordinal=10,
        known_at=bucket + timedelta(seconds=3),
        first_provider_sequence_num=1,
        last_provider_sequence_num=10,
        gap_quality_event_ids=gaps,
    )


def test_raw_record_identity_is_stable_before_archive_upload() -> None:
    fact, raw = _fact()
    expected = build_raw_record_id(
        definition_id=raw.definition_id,
        session_id=raw.session_id,
        connection_epoch=raw.connection_epoch,
        receive_ordinal=raw.receive_ordinal,
        raw_frame_sha256=raw.raw_frame_sha256,
    )
    assert raw.raw_record_id == expected
    assert fact.raw_record_id == expected


def test_captured_bip_trade_preserves_maker_side_and_exact_contract_units() -> None:
    fact, _raw = _fact("BIP-20DEC30-CDE")
    assert fact.provider_size_unit is ProviderSizeUnit.CONTRACTS
    assert fact.contract_quantity == fact.provider_size
    assert fact.base_quantity == fact.contract_quantity * Decimal("0.01")
    assert fact.quote_notional == fact.price * fact.base_quantity
    assert fact.aggressor_side is (
        MarketSide.SELL if fact.maker_side is MarketSide.BUY else MarketSide.BUY
    )


def test_captured_spot_trade_uses_provider_size_as_base_quantity() -> None:
    fact, _raw = _fact("BTC-USD")
    assert fact.provider_size_unit is ProviderSizeUnit.BASE
    assert fact.contract_quantity is None
    assert fact.base_quantity == fact.provider_size
    assert fact.quote_notional == fact.price * fact.base_quantity


def test_trade_material_identity_ignores_duplicate_delivery_provenance() -> None:
    fact, _raw = _fact()
    duplicate = replace(
        fact,
        connection_epoch=1,
        receive_ordinal=99,
        raw_record_id="raw-second-delivery",
        received_at=fact.received_at + timedelta(minutes=1),
        accepted_at=fact.accepted_at + timedelta(minutes=1),
        known_at=fact.known_at + timedelta(minutes=1),
    )
    assert duplicate.material_hash == fact.material_hash
    assert duplicate.row_hash != fact.row_hash


def test_aggregate_is_duplicate_delivery_invariant_and_deterministic() -> None:
    fact, _raw = _fact()
    duplicate = replace(
        fact,
        connection_epoch=1,
        receive_ordinal=99,
        raw_record_id="raw-second-delivery",
        received_at=fact.received_at + timedelta(minutes=1),
        accepted_at=fact.accepted_at + timedelta(minutes=1),
        known_at=fact.known_at + timedelta(minutes=1),
    )
    coverage = _coverage(fact)
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    first = aggregate_trade_bucket(
        [duplicate, fact],
        interval_seconds=1,
        bucket_start=bucket,
        coverage=coverage,
        computed_at=coverage.known_at,
    )
    second = aggregate_trade_bucket(
        [fact],
        interval_seconds=1,
        bucket_start=bucket,
        coverage=coverage,
        computed_at=coverage.known_at,
    )
    assert first.trade_count == 1
    assert first.material_hash == second.material_hash
    assert first.aggregate_complete is True


def test_aggregate_rejects_divergent_same_trade_identity() -> None:
    fact, _raw = _fact()
    conflict = replace(fact, price=fact.price + Decimal("1"))
    coverage = _coverage(fact)
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    with pytest.raises(ValueError, match="conflicting provider trade identity"):
        aggregate_trade_bucket(
            [fact, conflict],
            interval_seconds=1,
            bucket_start=bucket,
            coverage=coverage,
            computed_at=coverage.known_at,
        )


@pytest.mark.parametrize(
    ("coverage_mutation", "expected"),
    [
        ({}, True),
        ({"archive_status": ArchiveStatus.PENDING}, False),
        ({"assurance": OrderingAssurance.RECEIPT_CONTIGUOUS}, False),
        ({"gaps": ("quality-gap",)}, False),
        ({"status": CoverageStatus.INVALID}, False),
    ],
)
def test_trade_coverage_distinguishes_complete_from_pending_gap_or_invalid(
    coverage_mutation, expected
) -> None:
    fact, _raw = _fact()
    coverage = _coverage(fact, **coverage_mutation)
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    assert coverage.complete_for_bucket(
        bucket_start=bucket, bucket_end=bucket + timedelta(seconds=1)
    ) is expected


def test_zero_trade_bucket_requires_proven_complete_coverage() -> None:
    fact, _raw = _fact()
    coverage = _coverage(fact)
    populated_bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    empty_bucket = populated_bucket + timedelta(seconds=1)
    complete_zero = aggregate_trade_bucket(
        [],
        interval_seconds=1,
        bucket_start=empty_bucket,
        coverage=coverage,
        computed_at=coverage.known_at,
    )
    assert complete_zero.trade_count == 0
    assert complete_zero.aggregate_complete is True
    assert complete_zero.contract_volume is None

    with pytest.raises(ValueError, match="zero rows require proven complete coverage"):
        aggregate_trade_bucket(
            [],
            interval_seconds=1,
            bucket_start=empty_bucket,
            coverage=_coverage(fact, archive_status=ArchiveStatus.PENDING),
            computed_at=coverage.known_at,
        )


def test_complete_aggregate_contract_cannot_hide_incomplete_evidence() -> None:
    fact, _raw = _fact()
    coverage = _coverage(fact)
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    aggregate = aggregate_trade_bucket(
        [fact],
        interval_seconds=1,
        bucket_start=bucket,
        coverage=coverage,
        computed_at=coverage.known_at,
    )
    with pytest.raises(ValueError, match="complete aggregate requires complete archive"):
        replace(aggregate, archive_complete=False)


def test_bucket_emission_is_causal_and_late_trade_is_explicit() -> None:
    fact, _raw = _fact()
    coverage = _coverage(fact)
    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    with pytest.raises(ValueError, match="cannot emit before bucket end"):
        aggregate_trade_bucket(
            [fact],
            interval_seconds=1,
            bucket_start=bucket,
            coverage=coverage,
            computed_at=bucket + timedelta(milliseconds=999),
        )
    aggregate = aggregate_trade_bucket(
        [fact],
        interval_seconds=1,
        bucket_start=bucket,
        coverage=coverage,
        computed_at=coverage.known_at,
        late_known_after=fact.known_at - timedelta(microseconds=1),
    )
    assert aggregate.late_trade_count == 1
    assert aggregate.known_at >= fact.known_at

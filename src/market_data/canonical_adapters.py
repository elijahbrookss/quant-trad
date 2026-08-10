"""Provider-boundary adapters for structured canonical market Facts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .canonical import CanonicalFact, CanonicalFactRecord
from .contracts import SourceIdentity
from .order_book import (
    BOOK_RECONSTRUCTION_VERSION,
    L2MutationBatchFact,
    L2SnapshotFact,
)
from .structure import (
    MarketTradeFact,
    MarketTradeRecord,
    TradeFlowAggregateFact,
    TradeFlowAggregateRecord,
)


def _evidence_mapping(
    value: Mapping[str, Any], *, key: str, fact_version_id: str
) -> dict[str, Any]:
    evidence = value.get(key)
    if not isinstance(evidence, Mapping):
        raise RuntimeError(
            "market_canonical_fact_corrupt: structured evidence is missing "
            f"key={key} fact_version_id={fact_version_id}"
        )
    return dict(evidence)


def _legacy_identity(
    fact: CanonicalFact,
    *,
    version_id: str,
    provenance_hash: str,
) -> tuple[str, str]:
    migration = fact.provenance.get("_qt_migration")
    if not isinstance(migration, Mapping):
        return version_id, provenance_hash
    return (
        str(migration.get("legacy_version_id") or version_id),
        str(migration.get("legacy_provenance_hash") or provenance_hash),
    )


def canonicalize_market_trade(
    fact: MarketTradeFact,
    *,
    source: SourceIdentity,
    transformation_id: str = "market.trade.canonicalization.v1",
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    """Translate one typed provider trade into its provider-neutral payload."""

    canonical_provenance = dict(provenance or {})
    if "_qt_trade_evidence" in canonical_provenance:
        raise ValueError(
            "market_trade_canonicalization_invalid: reserved provenance key"
        )
    canonical_provenance["_qt_trade_evidence"] = {
        "provider_product_id": fact.provider_product_id,
        "provider_trade_id": fact.provider_trade_id,
        "delivery_kind": fact.delivery_kind.value,
        "aggressor_transform_version": fact.aggressor_transform_version,
        "product_definition_version_id": fact.product_definition_version_id,
        "provider_message_time": fact.provider_message_time,
        "provider_sequence_num": fact.provider_sequence_num,
        "connection_epoch": fact.connection_epoch,
        "receive_ordinal": fact.receive_ordinal,
        "event_ordinal": fact.event_ordinal,
        "trade_ordinal": fact.trade_ordinal,
        "raw_record_id": fact.raw_record_id,
        "coverage_interval_id": fact.coverage_interval_id,
    }
    canonical = CanonicalFact(
        fact_type="market.trade",
        payload_schema_id="market.trade.v1",
        observation_key=f"{fact.provider_product_id}:{fact.provider_trade_id}",
        observation_time=fact.provider_event_time,
        observation_time_method="provider_event_time",
        source_published_at=fact.provider_message_time,
        received_at=fact.received_at,
        accepted_at=fact.accepted_at,
        known_at=fact.known_at,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id=transformation_id,
        external_event_key=fact.provider_trade_id,
        external_event_group_key=fact.provider_product_id,
        payload={
            "price": fact.price,
            "reported_quantity": fact.provider_size,
            "reported_quantity_unit": fact.provider_size_unit.value,
            "contract_quantity": fact.contract_quantity,
            "base_quantity": fact.base_quantity,
            "quote_notional": fact.quote_notional,
            "base_currency": fact.base_currency,
            "quote_currency": fact.quote_currency,
            "maker_side": fact.maker_side.value,
            "aggressor_side": (
                fact.aggressor_side.value if fact.aggressor_side else None
            ),
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )
    if canonical.material_hash != fact.material_hash or canonical.row_hash != fact.row_hash:
        raise RuntimeError(
            "market_trade_canonicalization_corrupt: retained v1 hashes disagree "
            f"provider_product_id={fact.provider_product_id} "
            f"provider_trade_id={fact.provider_trade_id}"
        )
    return canonical


def canonicalize_trade_flow(
    fact: TradeFlowAggregateFact,
    *,
    source: SourceIdentity,
    aggregation_version: str = "market.trade_flow.v1",
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    """Translate one causal aggregate without flattening its atomic state."""

    canonical_provenance = dict(provenance or {})
    canonical_quality = dict(quality or {})
    if "_qt_trade_flow_evidence" in canonical_provenance:
        raise ValueError(
            "market_trade_flow_canonicalization_invalid: reserved provenance key"
        )
    if "_qt_trade_flow_quality" in canonical_quality:
        raise ValueError(
            "market_trade_flow_canonicalization_invalid: reserved quality key"
        )
    canonical_provenance["_qt_trade_flow_evidence"] = {
        "interval_seconds": fact.interval_seconds,
        "first_trade_id": fact.first_trade_id,
        "last_trade_id": fact.last_trade_id,
        "first_receive_ordinal": fact.first_receive_ordinal,
        "last_receive_ordinal": fact.last_receive_ordinal,
        "coverage_interval_id": fact.coverage_interval_id,
        "coverage_revision": fact.coverage_revision,
        "input_fingerprint": fact.input_fingerprint,
    }
    canonical_quality["_qt_trade_flow_quality"] = {
        "aggregate_complete": fact.aggregate_complete,
        "archive_complete": fact.archive_complete,
        "canonicalization_complete": fact.canonicalization_complete,
        "late_trade_count": fact.late_trade_count,
    }
    canonical = CanonicalFact(
        fact_type="market.trade_flow",
        payload_schema_id="market.trade_flow.v1",
        observation_key=fact.bucket_start.isoformat(),
        observation_time=fact.bucket_start,
        observation_time_method="bucket_start",
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_materialization",
        source=source,
        transformation_id=str(aggregation_version),
        payload={
            "bucket_end": fact.bucket_end,
            "trade_count": fact.trade_count,
            "maker_buy_count": fact.maker_buy_count,
            "maker_sell_count": fact.maker_sell_count,
            "aggressor_buy_count": fact.aggressor_buy_count,
            "aggressor_sell_count": fact.aggressor_sell_count,
            "contract_volume": fact.contract_volume,
            "base_volume": fact.base_volume,
            "quote_notional": fact.quote_notional,
            "maker_buy_base_volume": fact.maker_buy_base_volume,
            "maker_sell_base_volume": fact.maker_sell_base_volume,
            "aggressor_buy_base_volume": fact.aggressor_buy_base_volume,
            "aggressor_sell_base_volume": fact.aggressor_sell_base_volume,
            "cvd_delta": fact.cvd_delta,
            "cvd_unit": fact.cvd_unit,
            "open_price": fact.open_price,
            "high_price": fact.high_price,
            "low_price": fact.low_price,
            "close_price": fact.close_price,
        },
        provenance=canonical_provenance,
        quality=canonical_quality,
    )
    if canonical.material_hash != fact.material_hash:
        raise RuntimeError(
            "market_trade_flow_canonicalization_corrupt: retained material hash "
            f"disagrees bucket_start={fact.bucket_start.isoformat()}"
        )
    return canonical


def _l2_observation_key(fact: L2SnapshotFact | L2MutationBatchFact) -> str:
    position = fact.event.position
    return (
        f"{position.definition_id}:{position.session_id}:"
        f"{position.connection_epoch}:{position.receive_ordinal}:"
        f"{position.event_ordinal}"
    )


def _l2_provenance(
    fact: L2SnapshotFact | L2MutationBatchFact,
    *,
    provenance: Mapping[str, Any] | None,
) -> dict[str, Any]:
    canonical_provenance = dict(provenance or {})
    if "_qt_l2_evidence" in canonical_provenance:
        raise ValueError("market_l2_canonicalization_invalid: reserved provenance key")
    position = fact.event.position
    canonical_provenance["_qt_l2_evidence"] = {
        "definition_id": position.definition_id,
        "session_id": position.session_id,
        "connection_epoch": position.connection_epoch,
        "provider_product_id": position.provider_product_id,
        "provider_sequence_num": position.provider_sequence_num,
        "receive_ordinal": position.receive_ordinal,
        "event_ordinal": position.event_ordinal,
        "raw_record_id": fact.event.raw_record_id,
    }
    return canonical_provenance


def _l2_entries(
    fact: L2SnapshotFact | L2MutationBatchFact,
) -> list[dict[str, Any]]:
    return [
        {
            "ordinal": mutation.mutation_ordinal,
            "side": mutation.side.value,
            "price": mutation.price,
            "quantity": mutation.new_quantity,
            "provider_size_unit": mutation.provider_size_unit.value,
            "provider_event_time": mutation.provider_event_time,
        }
        for mutation in fact.event.mutations
    ]


def _l2_snapshot_entries(fact: L2SnapshotFact) -> list[dict[str, Any]]:
    mutation_by_level = {
        (mutation.side.value, mutation.price): mutation
        for mutation in fact.event.mutations
    }
    entries: list[dict[str, Any]] = []
    for side, levels in (("bid", fact.bids), ("ask", fact.asks)):
        for price, quantity in levels:
            mutation = mutation_by_level[(side, price)]
            entries.append(
                {
                    "ordinal": len(entries),
                    "side": side,
                    "price": price,
                    "quantity": quantity,
                    "provider_size_unit": mutation.provider_size_unit.value,
                    "provider_event_time": mutation.provider_event_time,
                }
            )
    return entries


def _retained_hash(value: str, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(
            f"market_l2_canonicalization_invalid: {field} must be sha256"
        )
    return normalized


def canonicalize_l2_snapshot(
    fact: L2SnapshotFact,
    *,
    source: SourceIdentity,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
    retained_event_material_hash: str | None = None,
) -> CanonicalFact:
    """Keep one complete Level 2 snapshot as one strict atomic Fact."""

    if fact.event.event_type.value != "snapshot":
        raise ValueError(
            "market_l2_snapshot_canonicalization_invalid: event type mismatch"
        )
    expected_levels = {
        (side, price): quantity
        for side, levels in (("bid", fact.bids), ("ask", fact.asks))
        for price, quantity in levels
    }
    mutation_levels = {
        (mutation.side.value, mutation.price): mutation.new_quantity
        for mutation in fact.event.mutations
    }
    if (
        expected_levels != mutation_levels
        or len(mutation_levels) != len(fact.event.mutations)
        or any(quantity <= 0 for quantity in mutation_levels.values())
    ):
        raise RuntimeError(
            "market_l2_snapshot_canonicalization_corrupt: snapshot entries "
            f"disagree snapshot_id={fact.snapshot_id}"
        )
    event_material_hash = (
        _retained_hash(
            retained_event_material_hash,
            field="retained_event_material_hash",
        )
        if retained_event_material_hash is not None
        else fact.event.material_hash
    )
    entries = _l2_snapshot_entries(fact)
    return CanonicalFact(
        fact_type="market.l2_book",
        payload_schema_id="market.l2_book.v1",
        observation_key=_l2_observation_key(fact),
        observation_time=fact.event.effective_at,
        observation_time_method="provider_event_time_max",
        source_published_at=fact.event.provider_message_time,
        received_at=fact.event.received_at,
        accepted_at=fact.event.accepted_at,
        known_at=fact.event.known_at,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="market.l2_snapshot.canonicalization.v1",
        external_event_key=(
            str(fact.event.position.provider_sequence_num)
            if fact.event.position.provider_sequence_num is not None
            else fact.event.raw_record_id
        ),
        external_event_group_key=fact.event.position.provider_product_id,
        external_event_component_key=fact.snapshot_id,
        payload={
            "event_type": "snapshot",
            "product_definition_version_id": (
                fact.event.product_definition_version_id
            ),
            "validity_interval_id": fact.validity_interval_id,
            "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
            "before_state_hash": None,
            "after_state_hash": fact.state_hash,
            "event_material_hash": event_material_hash,
            "entry_count": len(entries),
            "unknown_zero_delete_count": 0,
            "entries": entries,
        },
        provenance=_l2_provenance(fact, provenance=provenance),
        quality=dict(quality or {}),
    )


def canonicalize_l2_mutation_batch(
    fact: L2MutationBatchFact,
    *,
    source: SourceIdentity,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    """Keep one ordered absolute-update event as one strict atomic Fact."""

    if fact.event.event_type.value != "update":
        raise ValueError(
            "market_l2_mutation_canonicalization_invalid: event type mismatch"
        )
    return CanonicalFact(
        fact_type="market.l2_book",
        payload_schema_id="market.l2_book.v1",
        observation_key=_l2_observation_key(fact),
        observation_time=fact.event.effective_at,
        observation_time_method="provider_event_time_max",
        source_published_at=fact.event.provider_message_time,
        received_at=fact.event.received_at,
        accepted_at=fact.event.accepted_at,
        known_at=fact.event.known_at,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="market.l2_mutation.canonicalization.v1",
        external_event_key=(
            str(fact.event.position.provider_sequence_num)
            if fact.event.position.provider_sequence_num is not None
            else fact.event.raw_record_id
        ),
        external_event_group_key=fact.event.position.provider_product_id,
        external_event_component_key=fact.batch_id,
        payload={
            "event_type": "update",
            "product_definition_version_id": (
                fact.event.product_definition_version_id
            ),
            "validity_interval_id": fact.validity_interval_id,
            "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
            "before_state_hash": fact.before_state_hash,
            "after_state_hash": fact.after_state_hash,
            "event_material_hash": fact.event.material_hash,
            "entry_count": len(fact.event.mutations),
            "unknown_zero_delete_count": fact.unknown_zero_delete_count,
            "entries": _l2_entries(fact),
        },
        provenance=_l2_provenance(fact, provenance=provenance),
        quality=dict(quality or {}),
    )


def decode_market_trade_record(record: CanonicalFactRecord) -> MarketTradeRecord:
    if record.fact.payload_schema_id != "market.trade.v1":
        raise ValueError(
            "market_trade_decode_invalid: expected market.trade.v1 "
            f"actual={record.fact.payload_schema_id}"
        )
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_trade_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = MarketTradeFact(
        provider_product_id=evidence["provider_product_id"],
        provider_trade_id=evidence["provider_trade_id"],
        delivery_kind=evidence["delivery_kind"],
        price=payload["price"],
        provider_size=payload["reported_quantity"],
        provider_size_unit=payload["reported_quantity_unit"],
        maker_side=payload["maker_side"],
        aggressor_side=payload["aggressor_side"],
        aggressor_transform_version=evidence["aggressor_transform_version"],
        contract_quantity=payload["contract_quantity"],
        base_quantity=payload["base_quantity"],
        quote_notional=payload["quote_notional"],
        base_currency=payload["base_currency"],
        quote_currency=payload["quote_currency"],
        product_definition_version_id=evidence[
            "product_definition_version_id"
        ],
        provider_event_time=record.fact.observation_time,
        provider_message_time=evidence["provider_message_time"],
        received_at=record.fact.received_at,
        accepted_at=record.fact.accepted_at,
        known_at=record.fact.known_at,
        provider_sequence_num=evidence["provider_sequence_num"],
        connection_epoch=evidence["connection_epoch"],
        receive_ordinal=evidence["receive_ordinal"],
        event_ordinal=evidence["event_ordinal"],
        trade_ordinal=evidence["trade_ordinal"],
        raw_record_id=evidence["raw_record_id"],
        coverage_interval_id=evidence["coverage_interval_id"],
    )
    if fact.material_hash != record.fact.material_hash or fact.row_hash != record.row_hash:
        raise RuntimeError(
            "market_trade_decode_corrupt: canonical and typed hashes disagree "
            f"fact_version_id={record.fact_version_id}"
        )
    version_id, provenance_hash = _legacy_identity(
        record.fact,
        version_id=str(record.fact_version_id),
        provenance_hash=record.fact.provenance_hash,
    )
    return MarketTradeRecord(
        version_id=version_id,
        series_id=record.series_id,
        source_id=record.source_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        provenance_hash=provenance_hash,
        quality=dict(record.fact.quality),
        fact=fact,
    )


def decode_trade_flow_record(
    record: CanonicalFactRecord,
) -> TradeFlowAggregateRecord:
    if record.fact.payload_schema_id != "market.trade_flow.v1":
        raise ValueError(
            "market_trade_flow_decode_invalid: expected market.trade_flow.v1 "
            f"actual={record.fact.payload_schema_id}"
        )
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_trade_flow_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    flow_quality = _evidence_mapping(
        record.fact.quality,
        key="_qt_trade_flow_quality",
        fact_version_id=str(record.fact_version_id),
    )
    fact = TradeFlowAggregateFact(
        interval_seconds=evidence["interval_seconds"],
        bucket_start=record.fact.observation_time,
        bucket_end=payload["bucket_end"],
        trade_count=payload["trade_count"],
        maker_buy_count=payload["maker_buy_count"],
        maker_sell_count=payload["maker_sell_count"],
        aggressor_buy_count=payload["aggressor_buy_count"],
        aggressor_sell_count=payload["aggressor_sell_count"],
        contract_volume=payload["contract_volume"],
        base_volume=payload["base_volume"],
        quote_notional=payload["quote_notional"],
        maker_buy_base_volume=payload["maker_buy_base_volume"],
        maker_sell_base_volume=payload["maker_sell_base_volume"],
        aggressor_buy_base_volume=payload["aggressor_buy_base_volume"],
        aggressor_sell_base_volume=payload["aggressor_sell_base_volume"],
        cvd_delta=payload["cvd_delta"],
        cvd_unit=payload["cvd_unit"],
        open_price=payload["open_price"],
        high_price=payload["high_price"],
        low_price=payload["low_price"],
        close_price=payload["close_price"],
        first_trade_id=evidence["first_trade_id"],
        last_trade_id=evidence["last_trade_id"],
        first_receive_ordinal=evidence["first_receive_ordinal"],
        last_receive_ordinal=evidence["last_receive_ordinal"],
        coverage_interval_id=evidence["coverage_interval_id"],
        coverage_revision=evidence["coverage_revision"],
        aggregate_complete=flow_quality["aggregate_complete"],
        archive_complete=flow_quality["archive_complete"],
        canonicalization_complete=flow_quality["canonicalization_complete"],
        late_trade_count=flow_quality["late_trade_count"],
        known_at=record.fact.known_at,
        input_fingerprint=evidence["input_fingerprint"],
    )
    if fact.material_hash != record.fact.material_hash:
        raise RuntimeError(
            "market_trade_flow_decode_corrupt: canonical and typed hashes disagree "
            f"fact_version_id={record.fact_version_id}"
        )
    version_id, provenance_hash = _legacy_identity(
        record.fact,
        version_id=str(record.fact_version_id),
        provenance_hash=record.fact.provenance_hash,
    )
    quality = dict(record.fact.quality)
    quality.pop("_qt_trade_flow_quality", None)
    return TradeFlowAggregateRecord(
        version_id=version_id,
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        aggregation_version=record.fact.transformation_id,
        provenance_hash=provenance_hash,
        quality=quality,
        fact=fact,
    )


__all__ = [
    "canonicalize_l2_mutation_batch",
    "canonicalize_l2_snapshot",
    "canonicalize_market_trade",
    "canonicalize_trade_flow",
    "decode_market_trade_record",
    "decode_trade_flow_record",
]

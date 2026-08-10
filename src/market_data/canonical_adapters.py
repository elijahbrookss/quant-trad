"""Provider-boundary adapters for structured canonical market Facts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .canonical import CanonicalFact, CanonicalFactRecord
from .contracts import SourceIdentity, TypedFeatureRecord
from .market_state import (
    BasisFeatureFact,
    BboFeatureFact,
    DepthFeatureFact,
    DerivativeStateFeatureFact,
    ResponseFeatureFact,
    TradeFlowFeatureFact,
)
from .order_book import (
    BOOK_RECONSTRUCTION_VERSION,
    BookSourcePosition,
    L2MutationBatchFact,
    L2SnapshotFact,
)
from .structure import (
    MarketTradeFact,
    MarketTradeRecord,
    TradeFlowAggregateFact,
    TradeFlowAggregateRecord,
)


DERIVED_MARKET_STATE_SOURCE = SourceIdentity(
    provider="QT",
    venue="",
    source_kind="deterministic_derivation",
    adapter_version="market_state.canonical.v1",
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


def _timestamp(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"market_canonical_fact_corrupt: field={field} is not a timestamp"
        ) from exc


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


def _derived_provenance(
    provenance: Mapping[str, Any] | None,
    *,
    evidence_key: str,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    canonical_provenance = dict(provenance or {})
    if evidence_key in canonical_provenance:
        raise ValueError(
            "market_state_canonicalization_invalid: reserved provenance key "
            f"key={evidence_key}"
        )
    canonical_provenance[evidence_key] = dict(evidence)
    return canonical_provenance


def canonicalize_bbo_feature(
    fact: BboFeatureFact,
    *,
    source: SourceIdentity,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    """Translate one deterministic BBO state into its canonical payload."""

    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_bbo_evidence",
        evidence={
            "source_l2_series_id": fact.source_l2_series_id,
            "source_effective_at": fact.source_effective_at,
            "source_position": fact.source_position.material(),
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.bbo",
        payload_schema_id="market.bbo.v1",
        observation_key=fact.bucket_start.isoformat(),
        observation_time=fact.bucket_start,
        observation_time_method="derived_bucket_start",
        source_published_at=fact.source_effective_at,
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.bbo.canonicalization.v1",
        external_event_group_key=fact.validity_interval_id,
        external_event_component_key=fact.source_state_hash,
        payload={
            "bucket_end": fact.bucket_end,
            "product_definition_version_id": fact.product_definition_version_id,
            "validity_interval_id": fact.validity_interval_id,
            "provider_size_unit": fact.provider_size_unit.value,
            "source_state_hash": fact.source_state_hash,
            "bid_price": fact.bid_price,
            "bid_quantity": fact.bid_quantity,
            "bid_base_quantity": fact.bid_base_quantity,
            "ask_price": fact.ask_price,
            "ask_quantity": fact.ask_quantity,
            "ask_base_quantity": fact.ask_base_quantity,
            "mid_price": fact.mid_price,
            "spread": fact.spread,
            "spread_bps": fact.spread_bps,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def canonicalize_depth_feature(
    fact: DepthFeatureFact,
    *,
    source: SourceIdentity,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    """Translate one fixed-band depth state into its canonical payload."""

    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_depth_evidence",
        evidence={
            "source_l2_series_id": fact.source_l2_series_id,
            "source_effective_at": fact.source_effective_at,
            "source_position": fact.source_position.material(),
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.depth_observation",
        payload_schema_id="market.depth_band.v1",
        observation_key=f"{fact.bucket_start.isoformat()}:{fact.band_bps}",
        observation_time=fact.bucket_start,
        observation_time_method="derived_bucket_start",
        source_published_at=fact.source_effective_at,
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.depth_band.canonicalization.v1",
        external_event_group_key=fact.validity_interval_id,
        external_event_component_key=fact.source_state_hash,
        payload={
            "bucket_end": fact.bucket_end,
            "validity_interval_id": fact.validity_interval_id,
            "source_state_hash": fact.source_state_hash,
            "bbo_input_fingerprint": fact.bbo_input_fingerprint,
            "provider_size_unit": fact.provider_size_unit.value,
            "band_bps": fact.band_bps,
            "mid_price": fact.mid_price,
            "bid_quantity": fact.bid_quantity,
            "ask_quantity": fact.ask_quantity,
            "bid_base_quantity": fact.bid_base_quantity,
            "ask_base_quantity": fact.ask_base_quantity,
            "bid_notional": fact.bid_notional,
            "ask_notional": fact.ask_notional,
            "imbalance": fact.imbalance,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def _typed_feature_record(
    record: CanonicalFactRecord,
    *,
    fact: Any,
    evidence: Mapping[str, Any],
) -> TypedFeatureRecord:
    if fact.material_hash != str(evidence["legacy_material_hash"]):
        raise RuntimeError(
            "market_state_decode_corrupt: canonical evidence and typed material "
            f"disagree fact_version_id={record.fact_version_id}"
        )
    version_id, provenance_hash = _legacy_identity(
        record.fact,
        version_id=str(record.fact_version_id),
        provenance_hash=record.fact.provenance_hash,
    )
    return TypedFeatureRecord(
        version_id=version_id,
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        provenance_hash=provenance_hash,
        quality=dict(record.fact.quality),
        fact=fact,
    )


def decode_bbo_feature_record(record: CanonicalFactRecord) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.bbo.v1":
        raise ValueError(
            "market_bbo_decode_invalid: expected market.bbo.v1 "
            f"actual={record.fact.payload_schema_id}"
        )
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_bbo_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = BboFeatureFact(
        series_id=record.series_id,
        source_l2_series_id=evidence["source_l2_series_id"],
        bucket_start=record.fact.observation_time,
        bucket_end=_timestamp(payload["bucket_end"], field="bucket_end"),
        source_effective_at=_timestamp(
            evidence["source_effective_at"], field="source_effective_at"
        ),
        known_at=record.fact.known_at,
        source_position=BookSourcePosition(**evidence["source_position"]),
        validity_interval_id=payload["validity_interval_id"],
        product_definition_version_id=payload[
            "product_definition_version_id"
        ],
        provider_size_unit=payload["provider_size_unit"],
        source_state_hash=payload["source_state_hash"],
        bid_price=payload["bid_price"],
        bid_quantity=payload["bid_quantity"],
        bid_base_quantity=payload["bid_base_quantity"],
        ask_price=payload["ask_price"],
        ask_quantity=payload["ask_quantity"],
        ask_base_quantity=payload["ask_base_quantity"],
        mid_price=payload["mid_price"],
        spread=payload["spread"],
        spread_bps=payload["spread_bps"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


def decode_depth_feature_record(record: CanonicalFactRecord) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.depth_band.v1":
        raise ValueError(
            "market_depth_decode_invalid: expected market.depth_band.v1 "
            f"actual={record.fact.payload_schema_id}"
        )
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_depth_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = DepthFeatureFact(
        series_id=record.series_id,
        source_l2_series_id=evidence["source_l2_series_id"],
        bucket_start=record.fact.observation_time,
        bucket_end=_timestamp(payload["bucket_end"], field="bucket_end"),
        source_effective_at=_timestamp(
            evidence["source_effective_at"], field="source_effective_at"
        ),
        known_at=record.fact.known_at,
        source_position=BookSourcePosition(**evidence["source_position"]),
        validity_interval_id=payload["validity_interval_id"],
        source_state_hash=payload["source_state_hash"],
        bbo_input_fingerprint=payload["bbo_input_fingerprint"],
        provider_size_unit=payload["provider_size_unit"],
        band_bps=payload["band_bps"],
        mid_price=payload["mid_price"],
        bid_quantity=payload["bid_quantity"],
        ask_quantity=payload["ask_quantity"],
        bid_base_quantity=payload["bid_base_quantity"],
        ask_base_quantity=payload["ask_base_quantity"],
        bid_notional=payload["bid_notional"],
        ask_notional=payload["ask_notional"],
        imbalance=payload["imbalance"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


def canonicalize_trade_flow_feature(
    fact: TradeFlowFeatureFact,
    *,
    source: SourceIdentity = DERIVED_MARKET_STATE_SOURCE,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_trade_flow_feature_evidence",
        evidence={
            "source_trade_flow_series_id": fact.source_trade_flow_series_id,
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.trade_flow_feature",
        payload_schema_id="market.trade_flow_feature.v1",
        observation_key=(
            f"{fact.bucket_start.isoformat()}:{fact.interval_seconds}"
        ),
        observation_time=fact.bucket_start,
        observation_time_method="derived_bucket_start",
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.trade_flow_feature.canonicalization.v1",
        external_event_key=fact.aggregate_material_hash,
        payload={
            "bucket_end": fact.bucket_end,
            "interval_seconds": fact.interval_seconds,
            "aggregate_material_hash": fact.aggregate_material_hash,
            "aggregate_input_fingerprint": fact.aggregate_input_fingerprint,
            "trade_count": fact.trade_count,
            "quote_notional": fact.quote_notional,
            "aggressor_buy_base_volume": fact.aggressor_buy_base_volume,
            "aggressor_sell_base_volume": fact.aggressor_sell_base_volume,
            "aggressor_buy_notional": fact.aggressor_buy_notional,
            "aggressor_sell_notional": fact.aggressor_sell_notional,
            "cvd_base": fact.cvd_base,
            "cvd_notional": fact.cvd_notional,
            "cvd_volume_share": fact.cvd_volume_share,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def canonicalize_basis_feature(
    fact: BasisFeatureFact,
    *,
    source: SourceIdentity = DERIVED_MARKET_STATE_SOURCE,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_basis_evidence",
        evidence={
            "futures_series_id": fact.futures_series_id,
            "spot_series_id": fact.spot_series_id,
            "futures_bbo_material_hash": fact.futures_bbo_material_hash,
            "spot_bbo_material_hash": fact.spot_bbo_material_hash,
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.futures_spot_relationship",
        payload_schema_id="market.futures_spot_basis.v1",
        observation_key=fact.effective_at.isoformat(),
        observation_time=fact.effective_at,
        observation_time_method="derived_effective_time",
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.futures_spot_basis.canonicalization.v1",
        external_event_key=fact.mapping_id,
        payload={
            "mapping_id": fact.mapping_id,
            "futures_mid": fact.futures_mid,
            "spot_mid": fact.spot_mid,
            "futures_staleness_seconds": fact.futures_staleness_seconds,
            "spot_staleness_seconds": fact.spot_staleness_seconds,
            "basis": fact.basis,
            "basis_bps": fact.basis_bps,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def canonicalize_derivative_state_feature(
    fact: DerivativeStateFeatureFact,
    *,
    source: SourceIdentity = DERIVED_MARKET_STATE_SOURCE,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_derivative_state_evidence",
        evidence={
            "oi_series_id": fact.oi_series_id,
            "funding_series_id": fact.funding_series_id,
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.derivative_state",
        payload_schema_id="market.derivative_state.v1",
        observation_key=fact.effective_at.isoformat(),
        observation_time=fact.effective_at,
        observation_time_method="derived_effective_time",
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.derivative_state.canonicalization.v1",
        external_event_group_key=fact.instrument_id,
        payload={
            "instrument_id": fact.instrument_id,
            "oi_sample_time": fact.oi_sample_time,
            "oi_market_commit_seq": fact.oi_market_commit_seq,
            "oi_value": fact.oi_value,
            "oi_previous_value": fact.oi_previous_value,
            "oi_log_change": fact.oi_log_change,
            "funding_sample_time": fact.funding_sample_time,
            "funding_market_commit_seq": fact.funding_market_commit_seq,
            "funding_rate": fact.funding_rate,
            "funding_time": fact.funding_time,
            "funding_interval_seconds": fact.funding_interval_seconds,
            "funding_semantics": fact.funding_semantics,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def canonicalize_response_feature(
    fact: ResponseFeatureFact,
    *,
    source: SourceIdentity = DERIVED_MARKET_STATE_SOURCE,
    provenance: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
) -> CanonicalFact:
    canonical_provenance = _derived_provenance(
        provenance,
        evidence_key="_qt_response_evidence",
        evidence={
            "source_flow_feature_series_id": fact.source_flow_feature_series_id,
            "source_l2_series_id": fact.source_l2_series_id,
            "source_flow_material_hash": fact.source_flow_material_hash,
            "pre_state_hash": fact.pre_state_hash,
            "trough_state_hash": fact.trough_state_hash,
            "post_state_hash": fact.post_state_hash,
            "first_trade_id": fact.first_trade_id,
            "last_trade_id": fact.last_trade_id,
            "first_trade_source_position": dict(fact.first_trade_source_position),
            "last_trade_source_position": dict(fact.last_trade_source_position),
            "pre_book_source_position": fact.pre_book_source_position.material(),
            "trough_book_source_position": fact.trough_book_source_position.material(),
            "post_book_source_position": fact.post_book_source_position.material(),
            "legacy_material_hash": fact.material_hash,
        },
    )
    return CanonicalFact(
        fact_type="market.market_response",
        payload_schema_id="market.market_response.v1",
        observation_key=(
            f"{fact.bucket_start.isoformat()}:{fact.direction.value}"
        ),
        observation_time=fact.effective_at,
        observation_time_method="derived_response_horizon",
        received_at=fact.known_at,
        accepted_at=fact.known_at,
        known_at=fact.known_at,
        known_at_method="derived_input_watermark",
        source=source,
        transformation_id="market.market_response.canonicalization.v1",
        external_event_key=fact.source_flow_material_hash,
        external_event_group_key=fact.validity_interval_id,
        payload={
            "bucket_start": fact.bucket_start,
            "bucket_end": fact.bucket_end,
            "direction": fact.direction.value,
            "validity_interval_id": fact.validity_interval_id,
            "aggressive_notional": fact.aggressive_notional,
            "signed_aggressive_notional": fact.signed_aggressive_notional,
            "response_bps": fact.response_bps,
            "pre_depth_notional": fact.pre_depth_notional,
            "consumed_depth_notional": fact.consumed_depth_notional,
            "replenished_depth_notional": fact.replenished_depth_notional,
            "depth_replenishment": fact.depth_replenishment,
            "liquidity_adjusted_impact": fact.liquidity_adjusted_impact,
            "price_response_per_flow": fact.price_response_per_flow,
            "input_fingerprint": fact.input_fingerprint,
        },
        provenance=canonical_provenance,
        quality=dict(quality or {}),
    )


def decode_trade_flow_feature_record(
    record: CanonicalFactRecord,
) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.trade_flow_feature.v1":
        raise ValueError("market_trade_flow_feature_decode_invalid: schema mismatch")
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_trade_flow_feature_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = TradeFlowFeatureFact(
        series_id=record.series_id,
        source_trade_flow_series_id=evidence["source_trade_flow_series_id"],
        interval_seconds=payload["interval_seconds"],
        bucket_start=record.fact.observation_time,
        bucket_end=_timestamp(payload["bucket_end"], field="bucket_end"),
        known_at=record.fact.known_at,
        aggregate_material_hash=payload["aggregate_material_hash"],
        aggregate_input_fingerprint=payload["aggregate_input_fingerprint"],
        trade_count=payload["trade_count"],
        quote_notional=payload["quote_notional"],
        aggressor_buy_base_volume=payload["aggressor_buy_base_volume"],
        aggressor_sell_base_volume=payload["aggressor_sell_base_volume"],
        aggressor_buy_notional=payload["aggressor_buy_notional"],
        aggressor_sell_notional=payload["aggressor_sell_notional"],
        cvd_base=payload["cvd_base"],
        cvd_notional=payload["cvd_notional"],
        cvd_volume_share=payload["cvd_volume_share"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


def decode_basis_feature_record(record: CanonicalFactRecord) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.futures_spot_basis.v1":
        raise ValueError("market_basis_decode_invalid: schema mismatch")
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_basis_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = BasisFeatureFact(
        mapping_id=payload["mapping_id"],
        futures_series_id=evidence["futures_series_id"],
        series_id=record.series_id,
        spot_series_id=evidence["spot_series_id"],
        effective_at=record.fact.observation_time,
        known_at=record.fact.known_at,
        futures_bbo_material_hash=evidence["futures_bbo_material_hash"],
        spot_bbo_material_hash=evidence["spot_bbo_material_hash"],
        futures_mid=payload["futures_mid"],
        spot_mid=payload["spot_mid"],
        futures_staleness_seconds=payload["futures_staleness_seconds"],
        spot_staleness_seconds=payload["spot_staleness_seconds"],
        basis=payload["basis"],
        basis_bps=payload["basis_bps"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


def _optional_timestamp(value: Any, *, field: str) -> datetime | None:
    return None if value is None else _timestamp(value, field=field)


def decode_derivative_state_feature_record(
    record: CanonicalFactRecord,
) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.derivative_state.v1":
        raise ValueError("market_derivative_state_decode_invalid: schema mismatch")
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_derivative_state_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = DerivativeStateFeatureFact(
        instrument_id=payload["instrument_id"],
        effective_at=record.fact.observation_time,
        series_id=record.series_id,
        known_at=record.fact.known_at,
        oi_series_id=evidence["oi_series_id"],
        oi_sample_time=_optional_timestamp(
            payload["oi_sample_time"], field="oi_sample_time"
        ),
        oi_market_commit_seq=payload["oi_market_commit_seq"],
        oi_value=payload["oi_value"],
        oi_previous_value=payload["oi_previous_value"],
        oi_log_change=payload["oi_log_change"],
        funding_series_id=evidence["funding_series_id"],
        funding_sample_time=_optional_timestamp(
            payload["funding_sample_time"], field="funding_sample_time"
        ),
        funding_market_commit_seq=payload["funding_market_commit_seq"],
        funding_rate=payload["funding_rate"],
        funding_time=_optional_timestamp(
            payload["funding_time"], field="funding_time"
        ),
        funding_interval_seconds=payload["funding_interval_seconds"],
        funding_semantics=payload["funding_semantics"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


def decode_response_feature_record(
    record: CanonicalFactRecord,
) -> TypedFeatureRecord:
    if record.fact.payload_schema_id != "market.market_response.v1":
        raise ValueError("market_response_decode_invalid: schema mismatch")
    payload = record.fact.payload
    evidence = _evidence_mapping(
        record.fact.provenance,
        key="_qt_response_evidence",
        fact_version_id=str(record.fact_version_id),
    )
    fact = ResponseFeatureFact(
        series_id=record.series_id,
        bucket_start=_timestamp(payload["bucket_start"], field="bucket_start"),
        source_flow_feature_series_id=evidence[
            "source_flow_feature_series_id"
        ],
        source_l2_series_id=evidence["source_l2_series_id"],
        source_flow_material_hash=evidence["source_flow_material_hash"],
        pre_state_hash=evidence["pre_state_hash"],
        trough_state_hash=evidence["trough_state_hash"],
        post_state_hash=evidence["post_state_hash"],
        bucket_end=_timestamp(payload["bucket_end"], field="bucket_end"),
        effective_at=record.fact.observation_time,
        known_at=record.fact.known_at,
        direction=payload["direction"],
        first_trade_id=evidence["first_trade_id"],
        last_trade_id=evidence["last_trade_id"],
        first_trade_source_position=evidence["first_trade_source_position"],
        last_trade_source_position=evidence["last_trade_source_position"],
        pre_book_source_position=BookSourcePosition(
            **evidence["pre_book_source_position"]
        ),
        trough_book_source_position=BookSourcePosition(
            **evidence["trough_book_source_position"]
        ),
        post_book_source_position=BookSourcePosition(
            **evidence["post_book_source_position"]
        ),
        validity_interval_id=payload["validity_interval_id"],
        aggressive_notional=payload["aggressive_notional"],
        signed_aggressive_notional=payload["signed_aggressive_notional"],
        response_bps=payload["response_bps"],
        pre_depth_notional=payload["pre_depth_notional"],
        consumed_depth_notional=payload["consumed_depth_notional"],
        replenished_depth_notional=payload["replenished_depth_notional"],
        depth_replenishment=payload["depth_replenishment"],
        liquidity_adjusted_impact=payload["liquidity_adjusted_impact"],
        price_response_per_flow=payload["price_response_per_flow"],
        input_fingerprint=payload["input_fingerprint"],
    )
    return _typed_feature_record(record, fact=fact, evidence=evidence)


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
    "DERIVED_MARKET_STATE_SOURCE",
    "canonicalize_basis_feature",
    "canonicalize_bbo_feature",
    "canonicalize_depth_feature",
    "canonicalize_derivative_state_feature",
    "canonicalize_l2_mutation_batch",
    "canonicalize_l2_snapshot",
    "canonicalize_market_trade",
    "canonicalize_response_feature",
    "canonicalize_trade_flow",
    "canonicalize_trade_flow_feature",
    "decode_basis_feature_record",
    "decode_bbo_feature_record",
    "decode_depth_feature_record",
    "decode_derivative_state_feature_record",
    "decode_market_trade_record",
    "decode_response_feature_record",
    "decode_trade_flow_record",
    "decode_trade_flow_feature_record",
]

#!/usr/bin/env python3
"""Offline migration of legacy core market facts into ``market.fact_versions``.

This script is migration lineage, not a runtime compatibility path. Apply the
canonical Fact store schema first, stop every writer, set ``PG_DSN``, then run
with ``--execute``. Without ``--execute`` the complete source set is decoded
and validated but no rows are written.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
from typing import Any

from sqlalchemy import create_engine, text

from market_data.canonical import CanonicalFact, build_fact_version_id
from market_data.canonical_adapters import (
    DERIVED_MARKET_STATE_SOURCE,
    canonicalize_basis_feature,
    canonicalize_bbo_feature,
    canonicalize_depth_feature,
    canonicalize_derivative_state_feature,
    canonicalize_l2_mutation_batch,
    canonicalize_l2_snapshot,
    canonicalize_market_trade,
    canonicalize_response_feature,
    canonicalize_trade_flow,
    canonicalize_trade_flow_feature,
)
from market_data.contracts import (
    CandleFact,
    FundingRateFact,
    NumericFact,
    NumericFactState,
    OpenInterestFact,
    SourceIdentity,
)
from market_data.structure import MarketTradeFact, TradeFlowAggregateFact
from market_data.market_state import (
    BasisFeatureFact,
    BboFeatureFact,
    DepthFeatureFact,
    DerivativeStateFeatureFact,
    ResponseFeatureFact,
    TradeFlowFeatureFact,
)
from market_data.order_book import (
    BookSourcePosition,
    L2EventFact,
    L2Mutation,
    L2MutationBatchFact,
    L2SnapshotFact,
)


_ADVISORY_LOCK_ID = 9_021_011
_BATCH_SIZE = 2_000
_DERIVED_SOURCE = DERIVED_MARKET_STATE_SOURCE


@dataclass(frozen=True)
class MigrationFamily:
    name: str
    source_table: str
    select_sql: str
    transform: Callable[[Mapping[str, Any]], "MigrationRow"]


@dataclass(frozen=True)
class MigrationRow:
    values: Mapping[str, Any]
    source_row_hash: str | None


def _source(row: Mapping[str, Any]) -> SourceIdentity:
    source = SourceIdentity(
        provider=str(row["source_provider"]),
        venue=str(row["source_venue"]),
        source_kind=str(row["source_kind"]),
        adapter_version=str(row["source_adapter_version"]),
    )
    if source.identity_key != str(row["source_identity_key"]):
        raise RuntimeError(
            "canonical_fact_migration_source_identity_mismatch: "
            f"source_id={row['source_id']}"
        )
    return source


def _migration_provenance(
    row: Mapping[str, Any], *, source_table: str, extra: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    provenance = dict(row.get("provenance") or {})
    if "_qt_migration" in provenance:
        raise RuntimeError(
            "canonical_fact_migration_reserved_provenance_key: "
            f"source_table={source_table} series_id={row['series_id']}"
        )
    evidence = {"source_table": source_table}
    evidence.update(dict(extra or {}))
    provenance["_qt_migration"] = evidence
    return provenance


def _canonical_values(
    *,
    row: Mapping[str, Any],
    fact: CanonicalFact,
    source_row_hash: str | None = None,
    source_material_hash: str | None = None,
) -> MigrationRow:
    if source_row_hash is not None and fact.row_hash != source_row_hash:
        raise RuntimeError(
            "canonical_fact_migration_schema_hash_mismatch: "
            f"schema_id={fact.payload_schema_id} series_id={row['series_id']}"
        )
    if source_material_hash is not None and fact.material_hash != source_material_hash:
        raise RuntimeError(
            "canonical_fact_migration_material_hash_mismatch: "
            f"schema_id={fact.payload_schema_id} series_id={row['series_id']}"
        )
    canonical_row_hash = source_row_hash or fact.row_hash
    version_id = build_fact_version_id(
        series_id=int(row["series_id"]),
        observation_key=fact.observation_key,
        revision=int(row["revision"]),
        row_hash=canonical_row_hash,
    )
    return MigrationRow(
        source_row_hash=source_row_hash,
        values={
            "id": version_id,
            "series_id": int(row["series_id"]),
            "observation_key": fact.observation_key,
            "revision": int(row["revision"]),
            "market_commit_seq": int(row["market_commit_seq"]),
            "source_id": int(row["source_id"]),
            "ingestion_run_id": (
                str(row["ingestion_run_id"])
                if row.get("ingestion_run_id") is not None
                else None
            ),
            "fact_type": fact.fact_type,
            "payload_schema_id": fact.payload_schema_id,
            "payload_contract_hash": fact.payload_contract_hash,
            "observation_time": fact.observation_time,
            "observation_time_method": fact.observation_time_method,
            "source_published_at": fact.source_published_at,
            "received_at": fact.received_at,
            "accepted_at": fact.accepted_at,
            "known_at": fact.known_at,
            "known_at_method": fact.known_at_method,
            "transformation_id": fact.transformation_id,
            "external_event_key": fact.external_event_key,
            "external_event_group_key": fact.external_event_group_key,
            "external_event_component_key": fact.external_event_component_key,
            "state": fact.state.value,
            "payload": json.dumps(dict(fact.payload), sort_keys=True),
            "payload_hash": fact.payload_hash,
            "material_hash": fact.material_hash,
            "provenance_schema_id": fact.provenance_schema_id,
            "provenance": json.dumps(dict(fact.provenance), sort_keys=True),
            "provenance_hash": fact.provenance_hash,
            "quality_schema_id": fact.quality_schema_id,
            "quality": json.dumps(dict(fact.quality), sort_keys=True),
            "quality_hash": fact.quality_hash,
            # Historical v1 schemas own their original row-hash algorithms.
            "row_hash": canonical_row_hash,
        },
    )


def _candle(row: Mapping[str, Any]) -> MigrationRow:
    legacy = CandleFact(
        open_time=row["candle_open_time"],
        close_time=row["candle_close_time"],
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        trade_count=row["trade_count"],
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    if legacy.row_hash != str(row["row_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=candle series_id={row['series_id']} "
            f"observation_time={legacy.open_time.isoformat()}"
        )
    canonical = CanonicalFact(
        fact_type="candle.ohlcv",
        payload_schema_id="candle.ohlcv.v1",
        observation_key=legacy.open_time.isoformat(),
        observation_time=legacy.open_time,
        observation_time_method="interval_open",
        source_published_at=legacy.source_published_at,
        received_at=legacy.received_at,
        accepted_at=legacy.accepted_at,
        known_at=legacy.known_at,
        known_at_method=legacy.known_at_method,
        source=_source(row),
        transformation_id="migration.candle_versions.v1",
        payload={
            "close_time": legacy.close_time,
            "open": legacy.open,
            "high": legacy.high,
            "low": legacy.low,
            "close": legacy.close,
            "volume": legacy.volume,
            "trade_count": legacy.trade_count,
        },
        provenance=_migration_provenance(
            row, source_table="market.candle_versions"
        ),
    )
    return _canonical_values(
        row=row, fact=canonical, source_row_hash=legacy.row_hash
    )


def _open_interest(row: Mapping[str, Any]) -> MigrationRow:
    legacy = OpenInterestFact(
        sample_time=row["sample_time"],
        value=row["open_interest"],
        unit=row["unit"],
        sample_time_method=row["sample_time_method"],
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    if legacy.row_hash != str(row["row_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=open_interest series_id={row['series_id']} "
            f"observation_time={legacy.sample_time.isoformat()}"
        )
    canonical = CanonicalFact(
        fact_type="derivatives.open_interest",
        payload_schema_id="derivatives.open_interest.v1",
        observation_key=legacy.sample_time.isoformat(),
        observation_time=legacy.sample_time,
        observation_time_method=legacy.sample_time_method,
        source_published_at=legacy.source_published_at,
        received_at=legacy.received_at,
        accepted_at=legacy.accepted_at,
        known_at=legacy.known_at,
        known_at_method=legacy.known_at_method,
        source=_source(row),
        transformation_id="migration.open_interest_versions.v1",
        payload={"value": legacy.value, "unit": legacy.unit},
        provenance=_migration_provenance(
            row, source_table="market.open_interest_versions"
        ),
    )
    return _canonical_values(
        row=row, fact=canonical, source_row_hash=legacy.row_hash
    )


def _funding(row: Mapping[str, Any]) -> MigrationRow:
    legacy = FundingRateFact(
        sample_time=row["sample_time"],
        rate=row["funding_rate"],
        funding_time=row["funding_time"],
        interval_seconds=row["funding_interval_seconds"],
        unit=row["unit"],
        sample_time_method=row["sample_time_method"],
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    if legacy.row_hash != str(row["row_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=funding_rate series_id={row['series_id']} "
            f"observation_time={legacy.sample_time.isoformat()}"
        )
    canonical = CanonicalFact(
        fact_type="derivatives.funding_rate",
        payload_schema_id="derivatives.funding_rate.v1",
        observation_key=legacy.sample_time.isoformat(),
        observation_time=legacy.sample_time,
        observation_time_method=legacy.sample_time_method,
        source_published_at=legacy.source_published_at,
        received_at=legacy.received_at,
        accepted_at=legacy.accepted_at,
        known_at=legacy.known_at,
        known_at_method=legacy.known_at_method,
        source=_source(row),
        transformation_id="migration.funding_rate_versions.v1",
        payload={
            "rate": legacy.rate,
            "funding_time": legacy.funding_time,
            "interval_seconds": legacy.interval_seconds,
            "unit": legacy.unit,
        },
        provenance=_migration_provenance(
            row, source_table="market.funding_rate_versions"
        ),
    )
    return _canonical_values(
        row=row, fact=canonical, source_row_hash=legacy.row_hash
    )


def _numeric(row: Mapping[str, Any]) -> MigrationRow:
    legacy = NumericFact(
        fact_type=row["fact_type"],
        contract_version=row["contract_version"],
        value=row["numeric_value"],
        raw_value=row["raw_value"],
        unit=row["unit"],
        dimensions=dict(row["dimensions"] or {}),
        effective_at=row["effective_at"],
        effective_at_method=row["effective_at_method"],
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
        source_event_key=row["source_event_key"],
        source_event_group_key=row["source_event_group_key"],
        source_event_component_key=row["source_event_component_key"],
        source_event_material_hash=row["source_event_material_hash"],
        state=NumericFactState(str(row["state"])),
    )
    if legacy.row_hash != str(row["row_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=numeric series_id={row['series_id']} "
            f"observation_key={legacy.source_event_key}"
        )
    canonical = CanonicalFact(
        fact_type=legacy.fact_type,
        payload_schema_id=legacy.contract_version,
        observation_key=legacy.source_event_key,
        observation_time=legacy.effective_at,
        observation_time_method=legacy.effective_at_method,
        source_published_at=legacy.source_published_at,
        received_at=legacy.received_at,
        accepted_at=legacy.accepted_at,
        known_at=legacy.known_at,
        known_at_method=legacy.known_at_method,
        source=_source(row),
        transformation_id="migration.numeric_fact_versions.v1",
        external_event_key=legacy.source_event_key,
        external_event_group_key=legacy.source_event_group_key,
        external_event_component_key=legacy.source_event_component_key,
        state=legacy.state.value,
        payload={
            "value": legacy.value,
            "raw_value": legacy.raw_value,
            "unit": legacy.unit,
        },
        provenance=_migration_provenance(
            row,
            source_table="market.numeric_fact_versions",
            extra={
                "source_event_material_hash": legacy.source_event_material_hash,
                "series_dimensions": dict(legacy.dimensions),
            },
        ),
    )
    return _canonical_values(
        row=row, fact=canonical, source_row_hash=legacy.row_hash
    )


def _trade(row: Mapping[str, Any]) -> MigrationRow:
    legacy = MarketTradeFact(
        provider_product_id=row["provider_product_id"],
        provider_trade_id=row["provider_trade_id"],
        delivery_kind=row["delivery_kind"],
        price=row["price"],
        provider_size=row["provider_size"],
        provider_size_unit=row["provider_size_unit"],
        maker_side=row["maker_side"],
        aggressor_side=row["aggressor_side"],
        aggressor_transform_version=row["aggressor_transform_version"],
        contract_quantity=row["contract_quantity"],
        base_quantity=row["base_quantity"],
        quote_notional=row["quote_notional"],
        base_currency=row["base_currency"],
        quote_currency=row["quote_currency"],
        product_definition_version_id=row["product_definition_version_id"],
        provider_event_time=row["provider_event_time"],
        provider_message_time=row["provider_message_time"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        provider_sequence_num=row["provider_sequence_num"],
        connection_epoch=row["connection_epoch"],
        receive_ordinal=row["receive_ordinal"],
        event_ordinal=row["event_ordinal"],
        trade_ordinal=row["trade_ordinal"],
        raw_record_id=row["raw_record_id"],
        coverage_interval_id=row["coverage_interval_id"],
    )
    if legacy.material_hash != str(row["material_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=trade kind=material series_id={row['series_id']} "
            f"provider_trade_id={legacy.provider_trade_id}"
        )
    if legacy.row_hash != str(row["row_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=trade kind=row series_id={row['series_id']} "
            f"provider_trade_id={legacy.provider_trade_id}"
        )
    canonical = canonicalize_market_trade(
        legacy,
        source=_source(row),
        transformation_id="migration.market_trade_versions.v1",
        provenance=_migration_provenance(
            row,
            source_table="market.market_trade_versions",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(
        row=row,
        fact=canonical,
        source_row_hash=legacy.row_hash,
        source_material_hash=legacy.material_hash,
    )


def _trade_flow(row: Mapping[str, Any]) -> MigrationRow:
    if int(row["source_match_count"]) != 1:
        raise RuntimeError(
            "canonical_fact_migration_derived_source_ambiguous: "
            f"family=trade_flow series_id={row['series_id']} "
            f"source_match_count={row['source_match_count']}"
        )
    legacy = TradeFlowAggregateFact(
        interval_seconds=row["interval_seconds"],
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        trade_count=row["trade_count"],
        maker_buy_count=row["maker_buy_count"],
        maker_sell_count=row["maker_sell_count"],
        aggressor_buy_count=row["aggressor_buy_count"],
        aggressor_sell_count=row["aggressor_sell_count"],
        contract_volume=row["contract_volume"],
        base_volume=row["base_volume"],
        quote_notional=row["quote_notional"],
        maker_buy_base_volume=row["maker_buy_base_volume"],
        maker_sell_base_volume=row["maker_sell_base_volume"],
        aggressor_buy_base_volume=row["aggressor_buy_base_volume"],
        aggressor_sell_base_volume=row["aggressor_sell_base_volume"],
        cvd_delta=row["cvd_delta"],
        cvd_unit=row["cvd_unit"],
        open_price=row["open_price"],
        high_price=row["high_price"],
        low_price=row["low_price"],
        close_price=row["close_price"],
        first_trade_id=row["first_trade_id"],
        last_trade_id=row["last_trade_id"],
        first_receive_ordinal=row["first_receive_ordinal"],
        last_receive_ordinal=row["last_receive_ordinal"],
        coverage_interval_id=row["coverage_interval_id"],
        coverage_revision=row["coverage_revision"],
        aggregate_complete=row["aggregate_complete"],
        archive_complete=row["archive_complete"],
        canonicalization_complete=row["canonicalization_complete"],
        late_trade_count=row["late_trade_count"],
        known_at=row["known_at"],
        input_fingerprint=row["input_fingerprint"],
    )
    if legacy.material_hash != str(row["material_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=trade_flow series_id={row['series_id']} "
            f"bucket_start={legacy.bucket_start.isoformat()}"
        )
    canonical = canonicalize_trade_flow(
        legacy,
        source=_source(row),
        aggregation_version=str(row["aggregation_version"]),
        provenance=_migration_provenance(
            row,
            source_table="market.trade_flow_aggregate_versions",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(
        row=row,
        fact=canonical,
        source_material_hash=legacy.material_hash,
    )


def _l2_event(
    row: Mapping[str, Any],
    *,
    event_type: str,
) -> L2EventFact:
    entries = tuple(
        L2Mutation(
            mutation_ordinal=int(entry["ordinal"]),
            side=str(entry["side"]),
            price=str(entry["price"]),
            new_quantity=str(entry["quantity"]),
            provider_event_time=entry["provider_event_time"],
            provider_size_unit=str(entry["provider_size_unit"]),
        )
        for entry in list(row["entries"] or [])
    )
    event = L2EventFact(
        event_type=event_type,
        position=BookSourcePosition(
            definition_id=str(row["definition_id"]),
            session_id=str(row["session_id"]),
            connection_epoch=int(row["connection_epoch"]),
            provider_product_id=str(row["provider_product_id"]),
            provider_sequence_num=row["provider_sequence_num"],
            receive_ordinal=int(row["receive_ordinal"]),
            event_ordinal=int(row["event_ordinal"]),
        ),
        product_definition_version_id=str(
            row["product_definition_version_id"]
        ),
        mutations=entries,
        provider_message_time=row["provider_message_time"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        raw_record_id=str(row["raw_record_id"]),
    )
    expected_count = int(
        row["level_count"]
        if event_type == "snapshot"
        else row["mutation_count"]
    )
    if (
        len(entries) != expected_count
        or event.effective_at != row["effective_at"]
        or (
            event_type == "update"
            and event.material_hash != str(row["event_material_hash"])
        )
    ):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=l2_{event_type} series_id={row['series_id']} "
            f"event_id={row['id']}"
        )
    return event


def _l2_snapshot(row: Mapping[str, Any]) -> MigrationRow:
    event = _l2_event(row, event_type="snapshot")
    bids = tuple(
        (mutation.price, mutation.new_quantity)
        for mutation in event.mutations
        if mutation.side.value == "bid"
    )
    asks = tuple(
        (mutation.price, mutation.new_quantity)
        for mutation in event.mutations
        if mutation.side.value == "ask"
    )
    legacy = L2SnapshotFact(
        snapshot_id=str(row["id"]),
        series_id=int(row["series_id"]),
        event=event,
        validity_interval_id=str(row["validity_interval_id"]),
        state_hash=str(row["state_hash"]),
        bids=bids,
        asks=asks,
    )
    canonical = canonicalize_l2_snapshot(
        legacy,
        source=_source(row),
        provenance=_migration_provenance(
            row,
            source_table="market.l2_snapshot_versions",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
        retained_event_material_hash=str(row["event_material_hash"]),
    )
    return _canonical_values(row=row, fact=canonical)


def _l2_mutation(row: Mapping[str, Any]) -> MigrationRow:
    event = _l2_event(row, event_type="update")
    legacy = L2MutationBatchFact(
        batch_id=str(row["id"]),
        series_id=int(row["series_id"]),
        event=event,
        validity_interval_id=str(row["validity_interval_id"]),
        before_state_hash=str(row["before_state_hash"]),
        after_state_hash=str(row["after_state_hash"]),
        unknown_zero_delete_count=int(row["unknown_zero_delete_count"]),
    )
    canonical = canonicalize_l2_mutation_batch(
        legacy,
        source=_source(row),
        provenance=_migration_provenance(
            row,
            source_table="market.l2_mutation_batches",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _book_position(value: Mapping[str, Any]) -> BookSourcePosition:
    return BookSourcePosition(
        definition_id=str(value["definition_id"]),
        session_id=str(value["session_id"]),
        connection_epoch=int(value["connection_epoch"]),
        provider_product_id=str(value["provider_product_id"]),
        provider_sequence_num=(
            int(value["provider_sequence_num"])
            if value.get("provider_sequence_num") is not None
            else None
        ),
        receive_ordinal=int(value["receive_ordinal"]),
        event_ordinal=int(value["event_ordinal"]),
    )


def _bbo_feature(row: Mapping[str, Any]) -> MigrationRow:
    legacy = BboFeatureFact(
        series_id=int(row["series_id"]),
        source_l2_series_id=int(row["source_l2_series_id"]),
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        source_effective_at=row["source_effective_at"],
        known_at=row["known_at"],
        source_position=_book_position(dict(row["source_position"])),
        validity_interval_id=str(row["validity_interval_id"]),
        product_definition_version_id=str(
            row["product_definition_version_id"]
        ),
        provider_size_unit=str(row["provider_size_unit"]),
        source_state_hash=str(row["source_state_hash"]),
        bid_price=row["bid_price"],
        bid_quantity=row["bid_quantity"],
        bid_base_quantity=row["bid_base_quantity"],
        ask_price=row["ask_price"],
        ask_quantity=row["ask_quantity"],
        ask_base_quantity=row["ask_base_quantity"],
        mid_price=row["mid_price"],
        spread=row["spread"],
        spread_bps=row["spread_bps"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    if legacy.material_hash != str(row["material_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=bbo_feature series_id={row['series_id']} id={row['id']}"
        )
    canonical = canonicalize_bbo_feature(
        legacy,
        source=_source(row),
        provenance=_migration_provenance(
            row,
            source_table="market.bbo_feature_versions",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _depth_feature(row: Mapping[str, Any]) -> MigrationRow:
    legacy = DepthFeatureFact(
        series_id=int(row["series_id"]),
        source_l2_series_id=int(row["source_l2_series_id"]),
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        source_effective_at=row["source_effective_at"],
        known_at=row["known_at"],
        source_position=_book_position(dict(row["source_position"])),
        validity_interval_id=str(row["validity_interval_id"]),
        source_state_hash=str(row["source_state_hash"]),
        bbo_input_fingerprint=str(row["bbo_input_fingerprint"]),
        provider_size_unit=str(row["provider_size_unit"]),
        band_bps=int(row["band_bps"]),
        mid_price=row["mid_price"],
        bid_quantity=row["bid_quantity"],
        ask_quantity=row["ask_quantity"],
        bid_base_quantity=row["bid_base_quantity"],
        ask_base_quantity=row["ask_base_quantity"],
        bid_notional=row["bid_notional"],
        ask_notional=row["ask_notional"],
        imbalance=row["imbalance"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    if legacy.material_hash != str(row["material_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family=depth_feature series_id={row['series_id']} id={row['id']}"
        )
    canonical = canonicalize_depth_feature(
        legacy,
        source=_source(row),
        provenance=_migration_provenance(
            row,
            source_table="market.depth_feature_versions",
            extra={
                "legacy_version_id": str(row["id"]),
                "legacy_provenance_hash": str(row["provenance_hash"]),
            },
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _derived_provenance(
    row: Mapping[str, Any], *, source_table: str
) -> dict[str, Any]:
    return _migration_provenance(
        row,
        source_table=source_table,
        extra={
            "legacy_version_id": str(row["id"]),
            "legacy_provenance_hash": str(row["provenance_hash"]),
        },
    )


def _assert_derived_material(
    row: Mapping[str, Any], *, family: str, material_hash: str
) -> None:
    if material_hash != str(row["material_hash"]):
        raise RuntimeError(
            "canonical_fact_migration_source_hash_mismatch: "
            f"family={family} series_id={row['series_id']} id={row['id']}"
        )


def _trade_flow_feature(row: Mapping[str, Any]) -> MigrationRow:
    legacy = TradeFlowFeatureFact(
        series_id=int(row["series_id"]),
        source_trade_flow_series_id=int(row["source_trade_flow_series_id"]),
        interval_seconds=int(row["interval_seconds"]),
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        known_at=row["known_at"],
        aggregate_material_hash=str(row["aggregate_material_hash"]),
        aggregate_input_fingerprint=str(row["aggregate_input_fingerprint"]),
        trade_count=int(row["trade_count"]),
        quote_notional=row["quote_notional"],
        aggressor_buy_base_volume=row["aggressor_buy_base_volume"],
        aggressor_sell_base_volume=row["aggressor_sell_base_volume"],
        aggressor_buy_notional=row["aggressor_buy_notional"],
        aggressor_sell_notional=row["aggressor_sell_notional"],
        cvd_base=row["cvd_base"],
        cvd_notional=row["cvd_notional"],
        cvd_volume_share=row["cvd_volume_share"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    _assert_derived_material(
        row, family="trade_flow_feature", material_hash=legacy.material_hash
    )
    canonical = canonicalize_trade_flow_feature(
        legacy,
        source=_source(row),
        provenance=_derived_provenance(
            row, source_table="market.trade_flow_feature_versions"
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _basis_feature(row: Mapping[str, Any]) -> MigrationRow:
    legacy = BasisFeatureFact(
        mapping_id=str(row["mapping_id"]),
        futures_series_id=int(row["futures_series_id"]),
        series_id=int(row["series_id"]),
        spot_series_id=int(row["spot_series_id"]),
        effective_at=row["effective_at"],
        known_at=row["known_at"],
        futures_bbo_material_hash=str(row["futures_bbo_material_hash"]),
        spot_bbo_material_hash=str(row["spot_bbo_material_hash"]),
        futures_mid=row["futures_mid"],
        spot_mid=row["spot_mid"],
        futures_staleness_seconds=row["futures_staleness_seconds"],
        spot_staleness_seconds=row["spot_staleness_seconds"],
        basis=row["basis"],
        basis_bps=row["basis_bps"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    _assert_derived_material(
        row, family="basis_feature", material_hash=legacy.material_hash
    )
    canonical = canonicalize_basis_feature(
        legacy,
        source=_source(row),
        provenance=_derived_provenance(
            row, source_table="market.futures_spot_relationship_versions"
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _derivative_state_feature(row: Mapping[str, Any]) -> MigrationRow:
    legacy = DerivativeStateFeatureFact(
        instrument_id=str(row["instrument_id"]),
        effective_at=row["effective_at"],
        series_id=int(row["series_id"]),
        known_at=row["known_at"],
        oi_series_id=(
            int(row["oi_series_id"])
            if row["oi_series_id"] is not None
            else None
        ),
        oi_sample_time=row["oi_sample_time"],
        oi_market_commit_seq=(
            int(row["oi_market_commit_seq"])
            if row["oi_market_commit_seq"] is not None
            else None
        ),
        oi_value=row["oi_value"],
        oi_previous_value=row["oi_previous_value"],
        oi_log_change=row["oi_log_change"],
        funding_series_id=(
            int(row["funding_series_id"])
            if row["funding_series_id"] is not None
            else None
        ),
        funding_sample_time=row["funding_sample_time"],
        funding_market_commit_seq=(
            int(row["funding_market_commit_seq"])
            if row["funding_market_commit_seq"] is not None
            else None
        ),
        funding_rate=row["funding_rate"],
        funding_time=row["funding_time"],
        funding_interval_seconds=(
            int(row["funding_interval_seconds"])
            if row["funding_interval_seconds"] is not None
            else None
        ),
        funding_semantics=row["funding_semantics"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    _assert_derived_material(
        row, family="derivative_state", material_hash=legacy.material_hash
    )
    canonical = canonicalize_derivative_state_feature(
        legacy,
        source=_source(row),
        provenance=_derived_provenance(
            row, source_table="market.derivative_state_versions"
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


def _response_feature(row: Mapping[str, Any]) -> MigrationRow:
    positions = dict(row["source_positions"])
    legacy = ResponseFeatureFact(
        series_id=int(row["series_id"]),
        bucket_start=row["bucket_start"],
        source_flow_feature_series_id=int(
            row["source_flow_feature_series_id"]
        ),
        source_l2_series_id=int(row["source_l2_series_id"]),
        source_flow_material_hash=str(row["source_flow_material_hash"]),
        pre_state_hash=str(row["pre_state_hash"]),
        trough_state_hash=str(row["trough_state_hash"]),
        post_state_hash=str(row["post_state_hash"]),
        bucket_end=row["bucket_end"],
        effective_at=row["effective_at"],
        known_at=row["known_at"],
        direction=str(row["direction"]),
        first_trade_id=str(row["first_trade_id"]),
        last_trade_id=str(row["last_trade_id"]),
        first_trade_source_position=dict(positions["first_trade"]),
        last_trade_source_position=dict(positions["last_trade"]),
        pre_book_source_position=_book_position(dict(positions["pre_book"])),
        trough_book_source_position=_book_position(dict(positions["trough_book"])),
        post_book_source_position=_book_position(dict(positions["post_book"])),
        validity_interval_id=str(row["validity_interval_id"]),
        aggressive_notional=row["aggressive_notional"],
        signed_aggressive_notional=row["signed_aggressive_notional"],
        response_bps=row["response_bps"],
        pre_depth_notional=row["pre_depth_notional"],
        consumed_depth_notional=row["consumed_depth_notional"],
        replenished_depth_notional=row["replenished_depth_notional"],
        depth_replenishment=row["depth_replenishment"],
        liquidity_adjusted_impact=row["liquidity_adjusted_impact"],
        price_response_per_flow=row["price_response_per_flow"],
        input_fingerprint=str(row["input_fingerprint"]),
    )
    _assert_derived_material(
        row, family="response_feature", material_hash=legacy.material_hash
    )
    canonical = canonicalize_response_feature(
        legacy,
        source=_source(row),
        provenance=_derived_provenance(
            row, source_table="market.market_response_feature_versions"
        ),
        quality=dict(row["quality"] or {}),
    )
    return _canonical_values(row=row, fact=canonical)


_SOURCE_JOIN = """
    JOIN market.ingestion_runs AS ingestion
      ON ingestion.id = fact.ingestion_run_id
    JOIN market.sources AS source
      ON source.id = ingestion.source_id
"""
_SOURCE_COLUMNS = """
    ingestion.source_id,
    source.identity_key AS source_identity_key,
    source.provider AS source_provider,
    source.venue AS source_venue,
    source.source_kind,
    source.adapter_version AS source_adapter_version
"""
_SOURCE_IDENTITY_COLUMNS = """
    source.identity_key AS source_identity_key,
    source.provider AS source_provider,
    source.venue AS source_venue,
    source.source_kind,
    source.adapter_version AS source_adapter_version
"""
_DERIVED_SOURCE_COLUMNS = f"""
    CAST(:derived_source_id AS bigint) AS source_id,
    '{_DERIVED_SOURCE.identity_key}'::text AS source_identity_key,
    '{_DERIVED_SOURCE.provider}'::text AS source_provider,
    '{_DERIVED_SOURCE.venue}'::text AS source_venue,
    '{_DERIVED_SOURCE.source_kind}'::text AS source_kind,
    '{_DERIVED_SOURCE.adapter_version}'::text AS source_adapter_version,
    NULL::text AS ingestion_run_id
"""

_FAMILIES = (
    MigrationFamily(
        "candle",
        "market.candle_versions",
        f"SELECT fact.*, {_SOURCE_COLUMNS} FROM market.candle_versions AS fact "
        f"{_SOURCE_JOIN} ORDER BY fact.series_id, fact.candle_open_time, fact.revision",
        _candle,
    ),
    MigrationFamily(
        "open_interest",
        "market.open_interest_versions",
        f"SELECT fact.*, {_SOURCE_COLUMNS} FROM market.open_interest_versions AS fact "
        f"{_SOURCE_JOIN} ORDER BY fact.series_id, fact.sample_time, fact.revision",
        _open_interest,
    ),
    MigrationFamily(
        "funding_rate",
        "market.funding_rate_versions",
        f"SELECT fact.*, {_SOURCE_COLUMNS} FROM market.funding_rate_versions AS fact "
        f"{_SOURCE_JOIN} ORDER BY fact.series_id, fact.sample_time, fact.revision",
        _funding,
    ),
    MigrationFamily(
        "numeric",
        "market.numeric_fact_versions",
        f"SELECT fact.*, {_SOURCE_COLUMNS} FROM market.numeric_fact_versions AS fact "
        f"{_SOURCE_JOIN} ORDER BY fact.series_id, fact.source_event_key, fact.revision",
        _numeric,
    ),
    MigrationFamily(
        "trade",
        "market.market_trade_versions",
        f"SELECT fact.*, {_SOURCE_IDENTITY_COLUMNS}, NULL::text AS ingestion_run_id "
        "FROM market.market_trade_versions AS fact "
        "JOIN market.sources AS source ON source.id = fact.source_id "
        "ORDER BY fact.series_id, fact.provider_event_time, fact.revision",
        _trade,
    ),
    MigrationFamily(
        "trade_flow",
        "market.trade_flow_aggregate_versions",
        f"SELECT fact.*, origin.source_id, {_SOURCE_IDENTITY_COLUMNS}, "
        "origin.source_match_count, NULL::text AS ingestion_run_id "
        "FROM market.trade_flow_aggregate_versions AS fact "
        "JOIN market.series AS flow_series ON flow_series.id = fact.series_id "
        "JOIN LATERAL ("
        "    SELECT min(trade.source_id) AS source_id, "
        "           count(DISTINCT trade.source_id) AS source_match_count "
        "    FROM market.series AS trade_series "
        "    JOIN market.market_trade_versions AS trade "
        "      ON trade.series_id = trade_series.id "
        "    WHERE trade_series.instrument_id = flow_series.instrument_id"
        ") AS origin ON origin.source_id IS NOT NULL "
        "JOIN market.sources AS source ON source.id = origin.source_id "
        "ORDER BY fact.series_id, fact.bucket_start, fact.revision",
        _trade_flow,
    ),
    MigrationFamily(
        "l2_snapshot",
        "market.l2_snapshot_versions",
        f"SELECT fact.*, 1 AS revision, stream.source_id, "
        f"{_SOURCE_IDENTITY_COLUMNS}, NULL::text AS ingestion_run_id, "
        "children.entries "
        "FROM market.l2_snapshot_versions AS fact "
        "JOIN market.stream_definitions AS stream "
        "  ON stream.id = fact.definition_id "
        "JOIN market.sources AS source ON source.id = stream.source_id "
        "JOIN LATERAL ("
        "    SELECT jsonb_agg("
        "        jsonb_build_object("
        "            'ordinal', level.level_ordinal, "
        "            'side', level.side, "
        "            'price', level.price::text, "
        "            'quantity', level.quantity::text, "
        "            'provider_size_unit', level.provider_size_unit, "
        "            'provider_event_time', level.provider_event_time"
        "        ) ORDER BY level.level_ordinal"
        "    ) AS entries "
        "    FROM market.l2_snapshot_levels AS level "
        "    WHERE level.snapshot_version_id = fact.id "
        "      AND level.snapshot_effective_at = fact.effective_at"
        ") AS children ON children.entries IS NOT NULL "
        "ORDER BY fact.series_id, fact.effective_at, fact.id",
        _l2_snapshot,
    ),
    MigrationFamily(
        "l2_mutation",
        "market.l2_mutation_batches",
        f"SELECT fact.*, 1 AS revision, stream.source_id, "
        f"{_SOURCE_IDENTITY_COLUMNS}, NULL::text AS ingestion_run_id, "
        "children.entries "
        "FROM market.l2_mutation_batches AS fact "
        "JOIN market.stream_definitions AS stream "
        "  ON stream.id = fact.definition_id "
        "JOIN market.sources AS source ON source.id = stream.source_id "
        "JOIN LATERAL ("
        "    SELECT jsonb_agg("
        "        jsonb_build_object("
        "            'ordinal', mutation.mutation_ordinal, "
        "            'side', mutation.side, "
        "            'price', mutation.price::text, "
        "            'quantity', mutation.new_quantity::text, "
        "            'provider_size_unit', mutation.provider_size_unit, "
        "            'provider_event_time', mutation.provider_event_time"
        "        ) ORDER BY mutation.mutation_ordinal"
        "    ) AS entries "
        "    FROM market.l2_mutations AS mutation "
        "    WHERE mutation.batch_id = fact.id "
        "      AND mutation.batch_effective_at = fact.effective_at"
        ") AS children ON children.entries IS NOT NULL "
        "ORDER BY fact.series_id, fact.effective_at, fact.id",
        _l2_mutation,
    ),
    MigrationFamily(
        "bbo_feature",
        "market.bbo_feature_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.bbo_feature_versions AS fact "
        "ORDER BY fact.series_id, fact.bucket_start, fact.revision",
        _bbo_feature,
    ),
    MigrationFamily(
        "depth_feature",
        "market.depth_feature_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.depth_feature_versions AS fact "
        "ORDER BY fact.series_id, fact.bucket_start, fact.band_bps, fact.revision",
        _depth_feature,
    ),
    MigrationFamily(
        "trade_flow_feature",
        "market.trade_flow_feature_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.trade_flow_feature_versions AS fact "
        "ORDER BY fact.series_id, fact.bucket_start, fact.revision",
        _trade_flow_feature,
    ),
    MigrationFamily(
        "basis_feature",
        "market.futures_spot_relationship_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.futures_spot_relationship_versions AS fact "
        "ORDER BY fact.series_id, fact.effective_at, fact.revision",
        _basis_feature,
    ),
    MigrationFamily(
        "derivative_state",
        "market.derivative_state_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.derivative_state_versions AS fact "
        "ORDER BY fact.series_id, fact.effective_at, fact.revision",
        _derivative_state_feature,
    ),
    MigrationFamily(
        "response_feature",
        "market.market_response_feature_versions",
        f"SELECT fact.*, {_DERIVED_SOURCE_COLUMNS} "
        "FROM market.market_response_feature_versions AS fact "
        "ORDER BY fact.series_id, fact.bucket_start, fact.direction, fact.revision",
        _response_feature,
    ),
)

_INSERT = text(
    """
    INSERT INTO market.fact_versions (
        id, series_id, observation_key, revision, market_commit_seq, source_id,
        ingestion_run_id, fact_type, payload_schema_id, payload_contract_hash,
        observation_time, observation_time_method, source_published_at,
        received_at, accepted_at, known_at, known_at_method, transformation_id,
        external_event_key, external_event_group_key,
        external_event_component_key, state, payload, payload_hash,
        material_hash, provenance_schema_id, provenance, provenance_hash,
        quality_schema_id, quality, quality_hash, row_hash
    ) VALUES (
        :id, :series_id, :observation_key, :revision, :market_commit_seq,
        :source_id, :ingestion_run_id, :fact_type, :payload_schema_id,
        :payload_contract_hash, :observation_time, :observation_time_method,
        :source_published_at, :received_at, :accepted_at, :known_at,
        :known_at_method, :transformation_id, :external_event_key,
        :external_event_group_key, :external_event_component_key, :state,
        CAST(:payload AS jsonb), :payload_hash, :material_hash,
        :provenance_schema_id, CAST(:provenance AS jsonb), :provenance_hash,
        :quality_schema_id, CAST(:quality AS jsonb), :quality_hash, :row_hash
    )
    ON CONFLICT (id) DO NOTHING
    """
)

_VALIDATION_COLUMNS = (
    "id",
    "series_id",
    "observation_key",
    "revision",
    "market_commit_seq",
    "source_id",
    "ingestion_run_id",
    "fact_type",
    "payload_schema_id",
    "payload_contract_hash",
    "observation_time",
    "observation_time_method",
    "source_published_at",
    "received_at",
    "accepted_at",
    "known_at",
    "known_at_method",
    "transformation_id",
    "external_event_key",
    "external_event_group_key",
    "external_event_component_key",
    "state",
    "payload",
    "payload_hash",
    "material_hash",
    "provenance_schema_id",
    "provenance",
    "provenance_hash",
    "quality_schema_id",
    "quality",
    "quality_hash",
    "row_hash",
)


def _resolve_derived_source_id(conn, *, execute: bool) -> int:
    row = conn.execute(
        text(
            """
            SELECT id, provider, venue, source_kind, adapter_version
            FROM market.sources
            WHERE identity_key = :identity_key
            """
        ),
        {"identity_key": _DERIVED_SOURCE.identity_key},
    ).mappings().first()
    if row is not None:
        actual = SourceIdentity(
            provider=str(row["provider"]),
            venue=str(row["venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["adapter_version"]),
        )
        if actual != _DERIVED_SOURCE:
            raise RuntimeError(
                "canonical_fact_migration_derived_source_conflict: "
                f"source_id={row['id']}"
            )
        return int(row["id"])
    if not execute:
        return 0
    return int(
        conn.execute(
            text(
                """
                INSERT INTO market.sources (
                    identity_key, provider, venue, source_kind,
                    adapter_version, lineage
                ) VALUES (
                    :identity_key, :provider, :venue, :source_kind,
                    :adapter_version, CAST(:lineage AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "identity_key": _DERIVED_SOURCE.identity_key,
                "provider": _DERIVED_SOURCE.provider,
                "venue": _DERIVED_SOURCE.venue,
                "source_kind": _DERIVED_SOURCE.source_kind,
                "adapter_version": _DERIVED_SOURCE.adapter_version,
                "lineage": json.dumps(
                    {
                        "schema_version": "market.derived_source_lineage.v1",
                        "authority": "QT deterministic market-state transforms",
                    },
                    sort_keys=True,
                ),
            },
        ).scalar_one()
    )


def _assert_boundary(conn, *, execute: bool) -> None:
    required = [
        "market.fact_schemas",
        "market.fact_versions",
        *(family.source_table for family in _FAMILIES),
    ]
    missing = [
        relation
        for relation in required
        if conn.execute(text("SELECT to_regclass(:name)"), {"name": relation}).scalar()
        is None
    ]
    if missing:
        raise RuntimeError(
            "canonical_fact_migration_boundary_missing: " + ",".join(missing)
        )
    if not execute:
        return
    other_clients = int(
        conn.execute(
            text(
                """
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
                """
            )
        ).scalar_one()
    )
    if other_clients:
        raise RuntimeError(
            "canonical_fact_migration_requires_exclusive_database: "
            f"other_client_backends={other_clients}"
        )
    conn.execute(text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": _ADVISORY_LOCK_ID})


def _migrate_family(
    conn,
    family: MigrationFamily,
    *,
    execute: bool,
    derived_source_id: int,
) -> dict[str, Any]:
    source_count = int(
        conn.execute(text(f"SELECT count(*) FROM {family.source_table}")).scalar_one()
    )
    source_rows = conn.execute(
        text(family.select_sql),
        {"derived_source_id": int(derived_source_id)},
    ).mappings().all()
    if len(source_rows) != source_count:
        raise RuntimeError(
            "canonical_fact_migration_orphaned_source_rows: "
            f"family={family.name} source_count={source_count} joined={len(source_rows)}"
        )
    migrated = [family.transform(row) for row in source_rows]
    ids = [str(row.values["id"]) for row in migrated]
    if len(ids) != len(set(ids)):
        raise RuntimeError(
            f"canonical_fact_migration_duplicate_ids: family={family.name}"
        )
    inserted_count = 0
    if execute:
        existing_ids = set(
            conn.execute(
                text(
                    "SELECT id FROM market.fact_versions "
                    "WHERE id = ANY(CAST(:ids AS varchar[]))"
                ),
                {"ids": ids},
            ).scalars()
        )
        pending = [
            item
            for item in migrated
            if str(item.values["id"]) not in existing_ids
        ]
        inserted_count = len(pending)
        for offset in range(0, len(pending), _BATCH_SIZE):
            conn.execute(
                _INSERT,
                [
                    dict(item.values)
                    for item in pending[offset : offset + _BATCH_SIZE]
                ],
            )
        stored = {
            str(row["id"]): tuple(row[column] for column in _VALIDATION_COLUMNS)
            for row in conn.execute(
                text(
                    f"""
                    SELECT {', '.join(_VALIDATION_COLUMNS)}
                    FROM market.fact_versions
                    WHERE transformation_id = :transformation_id
                    """
                ),
                {
                    "transformation_id": str(
                        migrated[0].values["transformation_id"]
                    )
                    if migrated
                    else f"migration.{family.name}.empty.v1"
                },
            ).mappings()
        }
        expected: dict[str, tuple[Any, ...]] = {}
        for item in migrated:
            values = dict(item.values)
            for name in ("payload", "provenance", "quality"):
                values[name] = json.loads(str(values[name]))
            expected[str(values["id"])] = tuple(
                values[column] for column in _VALIDATION_COLUMNS
            )
        if stored != expected:
            raise RuntimeError(
                "canonical_fact_migration_validation_failed: "
                f"family={family.name} expected={len(expected)} stored={len(stored)}"
            )
    return {
        "family": family.name,
        "source_table": family.source_table,
        "source_rows": source_count,
        "validated_rows": len(migrated),
        "inserted_rows": inserted_count,
        "written": bool(execute),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="write and commit canonical rows; default is validation-only",
    )
    parser.add_argument(
        "--family",
        action="append",
        choices=tuple(family.name for family in _FAMILIES),
        help="validate or migrate only the named family; repeat as needed",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("canonical_fact_migration_requires_PG_DSN")
    engine = create_engine(dsn, future=True)
    reports: list[dict[str, Any]] = []
    try:
        with engine.begin() as conn:
            _assert_boundary(conn, execute=bool(args.execute))
            derived_source_id = _resolve_derived_source_id(
                conn, execute=bool(args.execute)
            )
            selected = set(args.family or ())
            families = tuple(
                family
                for family in _FAMILIES
                if not selected or family.name in selected
            )
            for family in families:
                report = _migrate_family(
                    conn,
                    family,
                    execute=bool(args.execute),
                    derived_source_id=derived_source_id,
                )
                reports.append(report)
                print(
                    "canonical_fact_migration_family "
                    + " ".join(f"{key}={value}" for key, value in report.items()),
                    flush=True,
                )
        print(
            "canonical_fact_migration_complete "
            f"mode={'execute' if args.execute else 'validate'} "
            f"source_rows={sum(item['source_rows'] for item in reports)} "
            f"validated_rows={sum(item['validated_rows'] for item in reports)}",
            flush=True,
        )
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

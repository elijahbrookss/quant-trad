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
from market_data.contracts import (
    CandleFact,
    FundingRateFact,
    NumericFact,
    NumericFactState,
    OpenInterestFact,
    SourceIdentity,
)
from market_data.structure import MarketTradeFact, TradeFlowAggregateFact


_ADVISORY_LOCK_ID = 9_021_011
_BATCH_SIZE = 2_000


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
    provenance = _migration_provenance(
        row,
        source_table="market.market_trade_versions",
        extra={
            "legacy_version_id": str(row["id"]),
            "legacy_provenance_hash": str(row["provenance_hash"]),
        },
    )
    provenance["_qt_trade_evidence"] = {
        "provider_product_id": legacy.provider_product_id,
        "provider_trade_id": legacy.provider_trade_id,
        "delivery_kind": legacy.delivery_kind.value,
        "aggressor_transform_version": legacy.aggressor_transform_version,
        "product_definition_version_id": legacy.product_definition_version_id,
        "provider_message_time": legacy.provider_message_time,
        "provider_sequence_num": legacy.provider_sequence_num,
        "connection_epoch": legacy.connection_epoch,
        "receive_ordinal": legacy.receive_ordinal,
        "event_ordinal": legacy.event_ordinal,
        "trade_ordinal": legacy.trade_ordinal,
        "raw_record_id": legacy.raw_record_id,
        "coverage_interval_id": legacy.coverage_interval_id,
    }
    canonical = CanonicalFact(
        fact_type="market.trade",
        payload_schema_id="market.trade.v1",
        observation_key=(
            f"{legacy.provider_product_id}:{legacy.provider_trade_id}"
        ),
        observation_time=legacy.provider_event_time,
        observation_time_method="provider_event_time",
        source_published_at=legacy.provider_message_time,
        received_at=legacy.received_at,
        accepted_at=legacy.accepted_at,
        known_at=legacy.known_at,
        known_at_method="platform_acceptance",
        source=_source(row),
        transformation_id="migration.market_trade_versions.v1",
        external_event_key=legacy.provider_trade_id,
        external_event_group_key=legacy.provider_product_id,
        payload={
            "price": legacy.price,
            "reported_quantity": legacy.provider_size,
            "reported_quantity_unit": legacy.provider_size_unit.value,
            "contract_quantity": legacy.contract_quantity,
            "base_quantity": legacy.base_quantity,
            "quote_notional": legacy.quote_notional,
            "base_currency": legacy.base_currency,
            "quote_currency": legacy.quote_currency,
            "maker_side": legacy.maker_side.value,
            "aggressor_side": (
                legacy.aggressor_side.value if legacy.aggressor_side else None
            ),
        },
        provenance=provenance,
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
    quality = dict(row["quality"] or {})
    if "_qt_trade_flow_quality" in quality:
        raise RuntimeError(
            "canonical_fact_migration_reserved_quality_key: "
            f"series_id={row['series_id']} bucket_start={legacy.bucket_start.isoformat()}"
        )
    quality["_qt_trade_flow_quality"] = {
        "aggregate_complete": legacy.aggregate_complete,
        "archive_complete": legacy.archive_complete,
        "canonicalization_complete": legacy.canonicalization_complete,
        "late_trade_count": legacy.late_trade_count,
    }
    provenance = _migration_provenance(
        row,
        source_table="market.trade_flow_aggregate_versions",
        extra={
            "legacy_version_id": str(row["id"]),
            "legacy_provenance_hash": str(row["provenance_hash"]),
        },
    )
    provenance["_qt_trade_flow_evidence"] = {
        "interval_seconds": legacy.interval_seconds,
        "first_trade_id": legacy.first_trade_id,
        "last_trade_id": legacy.last_trade_id,
        "first_receive_ordinal": legacy.first_receive_ordinal,
        "last_receive_ordinal": legacy.last_receive_ordinal,
        "coverage_interval_id": legacy.coverage_interval_id,
        "coverage_revision": legacy.coverage_revision,
        "input_fingerprint": legacy.input_fingerprint,
    }
    canonical = CanonicalFact(
        fact_type="market.trade_flow",
        payload_schema_id="market.trade_flow.v1",
        observation_key=legacy.bucket_start.isoformat(),
        observation_time=legacy.bucket_start,
        observation_time_method="bucket_start",
        accepted_at=legacy.known_at,
        known_at=legacy.known_at,
        known_at_method="derived_materialization",
        source=_source(row),
        transformation_id=str(row["aggregation_version"]),
        payload={
            "bucket_end": legacy.bucket_end,
            "trade_count": legacy.trade_count,
            "maker_buy_count": legacy.maker_buy_count,
            "maker_sell_count": legacy.maker_sell_count,
            "aggressor_buy_count": legacy.aggressor_buy_count,
            "aggressor_sell_count": legacy.aggressor_sell_count,
            "contract_volume": legacy.contract_volume,
            "base_volume": legacy.base_volume,
            "quote_notional": legacy.quote_notional,
            "maker_buy_base_volume": legacy.maker_buy_base_volume,
            "maker_sell_base_volume": legacy.maker_sell_base_volume,
            "aggressor_buy_base_volume": legacy.aggressor_buy_base_volume,
            "aggressor_sell_base_volume": legacy.aggressor_sell_base_volume,
            "cvd_delta": legacy.cvd_delta,
            "cvd_unit": legacy.cvd_unit,
            "open_price": legacy.open_price,
            "high_price": legacy.high_price,
            "low_price": legacy.low_price,
            "close_price": legacy.close_price,
        },
        provenance=provenance,
        quality=quality,
    )
    return _canonical_values(
        row=row,
        fact=canonical,
        source_material_hash=legacy.material_hash,
    )


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


def _migrate_family(conn, family: MigrationFamily, *, execute: bool) -> dict[str, Any]:
    source_count = int(
        conn.execute(text(f"SELECT count(*) FROM {family.source_table}")).scalar_one()
    )
    source_rows = conn.execute(text(family.select_sql)).mappings().all()
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
            for family in _FAMILIES:
                report = _migrate_family(conn, family, execute=bool(args.execute))
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

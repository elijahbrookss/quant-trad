"""Canonical PostgreSQL repository for market-data facts and datasets."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
import os
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Optional

from market_data.canonical import (
    CanonicalFact,
    CanonicalFactRecord,
    build_fact_version_id,
)
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    CandleFact,
    CandleRecord,
    DatasetSeriesRequest,
    FundingRateFact,
    FundingRateRecord,
    MarketDataRecord,
    NumericFact,
    NumericFactRecord,
    TypedFeatureRecord,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
    build_candle_material_hash,
    build_dataset_identity_hash,
    dataset_series_identity_payload,
    build_funding_rate_material_hash,
    build_open_interest_material_hash,
    build_numeric_fact_material_hash,
    build_provenance_hash,
    build_quality_hash,
    build_typed_feature_material_hash,
    record_effective_time,
)
from market_data.fact_registry import get_fact_contract, get_fact_payload_schema
from market_data.store import FrozenDataset, IngestionOutcome
from market_data.structure import (
    MARKET_TRADE_FACT_TYPE,
    TRADE_FLOW_FACT_TYPE,
    build_market_trade_material_hash,
    build_trade_flow_material_hash,
)
from sqlalchemy import text

from ....db import db

from .market_lifecycle import market_storage_lifecycle_repository


_SERIES_IDENTITY_VERSION = "market_series.v1"
_DIMENSIONAL_SERIES_IDENTITY_VERSION = "market_series.v2"


def _json_text(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"), default=str)


def _json_mapping_array_text(
    value: Sequence[Mapping[str, Any]] | None,
) -> str:
    return json.dumps(
        [dict(item) for item in value or ()],
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_text(value).encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _observation_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _canonical_source(row: Mapping[str, Any]) -> SourceIdentity:
    source = SourceIdentity(
        provider=str(row["source_provider"]),
        venue=str(row["source_venue"]),
        source_kind=str(row["source_kind"]),
        adapter_version=str(row["source_adapter_version"]),
    )
    if source.identity_key != str(row["source_identity_key"]):
        raise RuntimeError(
            "market_data_corrupt: canonical source identity mismatch "
            f"source_id={row.get('source_id')}"
        )
    return source


def _canonical_row_to_record(row: Mapping[str, Any]) -> CanonicalFactRecord:
    fact = CanonicalFact(
        fact_type=str(row["fact_type"]),
        payload_schema_id=str(row["payload_schema_id"]),
        observation_key=str(row["observation_key"]),
        observation_time=row["observation_time"],
        observation_time_method=str(row["observation_time_method"]),
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=str(row["known_at_method"]),
        source=_canonical_source(row),
        transformation_id=str(row["transformation_id"]),
        external_event_key=row.get("external_event_key"),
        external_event_group_key=row.get("external_event_group_key"),
        external_event_component_key=row.get("external_event_component_key"),
        state=str(row["state"]),
        payload=dict(row.get("payload") or {}),
        provenance_schema_id=str(row["provenance_schema_id"]),
        provenance=dict(row.get("provenance") or {}),
        quality_schema_id=str(row["quality_schema_id"]),
        quality=dict(row.get("quality") or {}),
    )
    expected = {
        "payload_contract_hash": fact.payload_contract_hash,
        "payload_hash": fact.payload_hash,
        "material_hash": fact.material_hash,
        "provenance_hash": fact.provenance_hash,
        "quality_hash": fact.quality_hash,
        "row_hash": fact.row_hash,
    }
    for field_name, expected_hash in expected.items():
        if str(row.get(field_name) or "") != expected_hash:
            raise RuntimeError(
                "market_data_corrupt: canonical Fact hash mismatch "
                f"field={field_name} fact_version_id={row.get('id')}"
            )
    return CanonicalFactRecord(
        series_id=int(row["series_id"]),
        source_id=int(row["source_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=row.get("ingestion_run_id"),
        fact_version_id=str(row["id"]),
        row_hash=str(row["row_hash"]),
        fact=fact,
    )


def _source_provenance(fact: CanonicalFact) -> dict[str, Any]:
    provenance = dict(fact.provenance)
    provenance.pop("_qt_migration", None)
    provenance.pop("_qt_numeric_evidence", None)
    return provenance


def _canonical_to_candle_record(row: Mapping[str, Any]) -> CandleRecord:
    record = _canonical_row_to_record(row)
    payload = record.fact.payload
    fact = CandleFact(
        open_time=record.fact.observation_time,
        close_time=payload["close_time"],
        open=payload["open"],
        high=payload["high"],
        low=payload["low"],
        close=payload["close"],
        volume=payload.get("volume"),
        trade_count=payload.get("trade_count"),
        source_published_at=record.fact.source_published_at,
        received_at=record.fact.received_at,
        accepted_at=record.fact.accepted_at,
        known_at=record.fact.known_at,
        known_at_method=record.fact.known_at_method,
    )
    return CandleRecord(
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        ingestion_run_id=str(record.ingestion_run_id or ""),
        source_identity_key=record.fact.source.identity_key,
        source=record.fact.source,
        provenance=_source_provenance(record.fact),
        fact=fact,
    )


def _canonical_to_open_interest_record(
    row: Mapping[str, Any],
) -> OpenInterestRecord:
    record = _canonical_row_to_record(row)
    payload = record.fact.payload
    fact = OpenInterestFact(
        sample_time=record.fact.observation_time,
        sample_time_method=record.fact.observation_time_method,
        value=payload["value"],
        unit=payload["unit"],
        source_published_at=record.fact.source_published_at,
        received_at=record.fact.received_at,
        accepted_at=record.fact.accepted_at,
        known_at=record.fact.known_at,
        known_at_method=record.fact.known_at_method,
    )
    return OpenInterestRecord(
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        ingestion_run_id=str(record.ingestion_run_id or ""),
        source_identity_key=record.fact.source.identity_key,
        source=record.fact.source,
        provenance=_source_provenance(record.fact),
        fact=fact,
    )


def _canonical_to_funding_rate_record(
    row: Mapping[str, Any],
) -> FundingRateRecord:
    record = _canonical_row_to_record(row)
    payload = record.fact.payload
    fact = FundingRateFact(
        sample_time=record.fact.observation_time,
        sample_time_method=record.fact.observation_time_method,
        rate=payload["rate"],
        funding_time=payload["funding_time"],
        interval_seconds=payload["interval_seconds"],
        unit=payload["unit"],
        source_published_at=record.fact.source_published_at,
        received_at=record.fact.received_at,
        accepted_at=record.fact.accepted_at,
        known_at=record.fact.known_at,
        known_at_method=record.fact.known_at_method,
    )
    return FundingRateRecord(
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        ingestion_run_id=str(record.ingestion_run_id or ""),
        source_identity_key=record.fact.source.identity_key,
        source=record.fact.source,
        provenance=_source_provenance(record.fact),
        fact=fact,
    )


def _canonical_to_numeric_record(row: Mapping[str, Any]) -> NumericFactRecord:
    record = _canonical_row_to_record(row)
    payload = record.fact.payload
    evidence = record.fact.provenance.get("_qt_numeric_evidence")
    if not isinstance(evidence, Mapping):
        migration = record.fact.provenance.get("_qt_migration")
        evidence = migration if isinstance(migration, Mapping) else None
    if evidence is None or not evidence.get("source_event_material_hash"):
        raise RuntimeError(
            "market_data_corrupt: numeric canonical Fact lacks source-event evidence "
            f"fact_version_id={record.fact_version_id}"
        )
    dimensions = dict(row.get("series_dimensions") or {})
    if dict(evidence.get("series_dimensions") or {}) != dimensions:
        raise RuntimeError(
            "market_data_corrupt: numeric canonical Fact dimensions disagree with series "
            f"fact_version_id={record.fact_version_id}"
        )
    fact = NumericFact(
        fact_type=record.fact.fact_type,
        contract_version=record.fact.payload_schema_id,
        value=payload["value"],
        raw_value=str(payload["raw_value"]),
        unit=str(payload["unit"]),
        dimensions=dimensions,
        effective_at=record.fact.observation_time,
        effective_at_method=record.fact.observation_time_method,
        source_published_at=record.fact.source_published_at,
        received_at=record.fact.received_at,
        accepted_at=record.fact.accepted_at,
        known_at=record.fact.known_at,
        known_at_method=record.fact.known_at_method,
        source_event_key=record.fact.external_event_key
        or record.fact.observation_key,
        source_event_group_key=record.fact.external_event_group_key,
        source_event_component_key=record.fact.external_event_component_key,
        source_event_material_hash=str(evidence["source_event_material_hash"]),
        state=record.fact.state.value,
    )
    return NumericFactRecord(
        series_id=record.series_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        ingestion_run_id=str(record.ingestion_run_id or ""),
        source_identity_key=record.fact.source.identity_key,
        source=record.fact.source,
        provenance=_source_provenance(record.fact),
        fact=fact,
    )


def _candle_to_canonical(
    fact: CandleFact, *, source: SourceIdentity, provenance: Mapping[str, Any]
) -> CanonicalFact:
    return CanonicalFact(
        fact_type=CANDLE_FACT_TYPE,
        payload_schema_id="candle.ohlcv.v1",
        observation_key=_observation_key(fact.open_time),
        observation_time=fact.open_time,
        observation_time_method="interval_open",
        source_published_at=fact.source_published_at,
        received_at=fact.received_at,
        accepted_at=fact.accepted_at,
        known_at=fact.known_at,
        known_at_method=fact.known_at_method,
        source=source,
        transformation_id="candle_fact_to_canonical.v1",
        payload={
            "close_time": fact.close_time,
            "open": fact.open,
            "high": fact.high,
            "low": fact.low,
            "close": fact.close,
            "volume": fact.volume,
            "trade_count": fact.trade_count,
        },
        provenance=dict(provenance),
    )


def _open_interest_to_canonical(
    fact: OpenInterestFact, *, source: SourceIdentity, provenance: Mapping[str, Any]
) -> CanonicalFact:
    return CanonicalFact(
        fact_type=OPEN_INTEREST_FACT_TYPE,
        payload_schema_id=OPEN_INTEREST_FACT_VERSION,
        observation_key=_observation_key(fact.sample_time),
        observation_time=fact.sample_time,
        observation_time_method=fact.sample_time_method,
        source_published_at=fact.source_published_at,
        received_at=fact.received_at,
        accepted_at=fact.accepted_at,
        known_at=fact.known_at,
        known_at_method=fact.known_at_method,
        source=source,
        transformation_id="open_interest_fact_to_canonical.v1",
        payload={"value": fact.value, "unit": fact.unit},
        provenance=dict(provenance),
    )


def _funding_rate_to_canonical(
    fact: FundingRateFact, *, source: SourceIdentity, provenance: Mapping[str, Any]
) -> CanonicalFact:
    return CanonicalFact(
        fact_type=FUNDING_RATE_FACT_TYPE,
        payload_schema_id=FUNDING_RATE_FACT_VERSION,
        observation_key=_observation_key(fact.sample_time),
        observation_time=fact.sample_time,
        observation_time_method=fact.sample_time_method,
        source_published_at=fact.source_published_at,
        received_at=fact.received_at,
        accepted_at=fact.accepted_at,
        known_at=fact.known_at,
        known_at_method=fact.known_at_method,
        source=source,
        transformation_id="funding_rate_fact_to_canonical.v1",
        payload={
            "rate": fact.rate,
            "funding_time": fact.funding_time,
            "interval_seconds": fact.interval_seconds,
            "unit": fact.unit,
        },
        provenance=dict(provenance),
    )


def _numeric_to_canonical(
    fact: NumericFact, *, source: SourceIdentity, provenance: Mapping[str, Any]
) -> CanonicalFact:
    canonical_provenance = dict(provenance)
    if "_qt_numeric_evidence" in canonical_provenance:
        raise ValueError(
            "market_data_ingest_invalid: provenance uses reserved _qt_numeric_evidence"
        )
    canonical_provenance["_qt_numeric_evidence"] = {
        "source_event_material_hash": fact.source_event_material_hash,
        "series_dimensions": dict(fact.dimensions),
    }
    return CanonicalFact(
        fact_type=fact.fact_type,
        payload_schema_id=fact.contract_version,
        observation_key=fact.source_event_key,
        observation_time=fact.effective_at,
        observation_time_method=fact.effective_at_method,
        source_published_at=fact.source_published_at,
        received_at=fact.received_at,
        accepted_at=fact.accepted_at,
        known_at=fact.known_at,
        known_at_method=fact.known_at_method,
        source=source,
        transformation_id="numeric_fact_to_canonical.v1",
        external_event_key=fact.source_event_key,
        external_event_group_key=fact.source_event_group_key,
        external_event_component_key=fact.source_event_component_key,
        state=fact.state.value,
        payload={
            "value": fact.value,
            "raw_value": fact.raw_value,
            "unit": fact.unit,
        },
        provenance=canonical_provenance,
    )


def _decode_core_canonical_rows(
    fact_type: str, rows: Sequence[Mapping[str, Any]]
) -> list[MarketDataRecord]:
    if fact_type == CANDLE_FACT_TYPE:
        return [_canonical_to_candle_record(row) for row in rows]
    if fact_type == OPEN_INTEREST_FACT_TYPE:
        return [_canonical_to_open_interest_record(row) for row in rows]
    if fact_type == FUNDING_RATE_FACT_TYPE:
        return [_canonical_to_funding_rate_record(row) for row in rows]
    if get_fact_contract(fact_type).uses_exact_numeric_storage:
        return [_canonical_to_numeric_record(row) for row in rows]
    raise RuntimeError(
        f"market_dataset_unsupported_canonical_fact: fact_type={fact_type}"
    )


def _row_to_record(row: Mapping[str, Any]) -> CandleRecord:
    fact = CandleFact(
        open_time=row["candle_open_time"],
        close_time=row["candle_close_time"],
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row.get("volume"),
        trade_count=row.get("trade_count"),
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    stored_hash = str(row.get("row_hash") or "")
    if fact.row_hash != stored_hash:
        raise RuntimeError(
            "market_data_corrupt: candle row hash mismatch "
            f"series_id={row.get('series_id')} open_time={_iso(fact.open_time)}"
        )
    return CandleRecord(
        series_id=int(row["series_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=str(row["ingestion_run_id"]),
        source_identity_key=str(row["source_identity_key"]),
        source=SourceIdentity(
            provider=str(row["source_provider"]),
            venue=str(row["source_venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["source_adapter_version"]),
        ),
        provenance=dict(row.get("provenance") or {}),
        fact=fact,
    )


def _row_to_open_interest_record(row: Mapping[str, Any]) -> OpenInterestRecord:
    fact = OpenInterestFact(
        sample_time=row["sample_time"],
        sample_time_method=row["sample_time_method"],
        value=row["open_interest"],
        unit=row["unit"],
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    stored_hash = str(row.get("row_hash") or "")
    if fact.row_hash != stored_hash:
        raise RuntimeError(
            "market_data_corrupt: open-interest row hash mismatch "
            f"series_id={row.get('series_id')} sample_time={_iso(fact.sample_time)}"
        )
    return OpenInterestRecord(
        series_id=int(row["series_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=str(row["ingestion_run_id"]),
        source_identity_key=str(row["source_identity_key"]),
        source=SourceIdentity(
            provider=str(row["source_provider"]),
            venue=str(row["source_venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["source_adapter_version"]),
        ),
        provenance=dict(row.get("provenance") or {}),
        fact=fact,
    )


def _row_to_funding_rate_record(row: Mapping[str, Any]) -> FundingRateRecord:
    fact = FundingRateFact(
        sample_time=row["sample_time"],
        sample_time_method=row["sample_time_method"],
        rate=row["funding_rate"],
        funding_time=row["funding_time"],
        interval_seconds=row["funding_interval_seconds"],
        unit=row["unit"],
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    stored_hash = str(row.get("row_hash") or "")
    if fact.row_hash != stored_hash:
        raise RuntimeError(
            "market_data_corrupt: funding-rate row hash mismatch "
            f"series_id={row.get('series_id')} sample_time={_iso(fact.sample_time)}"
        )
    return FundingRateRecord(
        series_id=int(row["series_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=str(row["ingestion_run_id"]),
        source_identity_key=str(row["source_identity_key"]),
        source=SourceIdentity(
            provider=str(row["source_provider"]),
            venue=str(row["source_venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["source_adapter_version"]),
        ),
        provenance=dict(row.get("provenance") or {}),
        fact=fact,
    )


def _row_to_numeric_fact_record(row: Mapping[str, Any]) -> NumericFactRecord:
    fact = NumericFact(
        fact_type=row["fact_type"],
        contract_version=row["contract_version"],
        value=row["numeric_value"],
        raw_value=row["raw_value"],
        unit=row["unit"],
        dimensions=dict(row.get("dimensions") or {}),
        effective_at=row["effective_at"],
        effective_at_method=row["effective_at_method"],
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
        source_event_key=row["source_event_key"],
        source_event_group_key=row.get("source_event_group_key"),
        source_event_component_key=row.get("source_event_component_key"),
        state=row["state"],
        source_event_material_hash=row["source_event_material_hash"],
    )
    if fact.row_hash != str(row.get("row_hash") or ""):
        raise RuntimeError(
            "market_data_corrupt: numeric fact row hash mismatch "
            f"series_id={row.get('series_id')} "
            f"source_event_key={fact.source_event_key}"
        )
    return NumericFactRecord(
        series_id=int(row["series_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=str(row["ingestion_run_id"]),
        source_identity_key=str(row["source_identity_key"]),
        source=SourceIdentity(
            provider=str(row["source_provider"]),
            venue=str(row["source_venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["source_adapter_version"]),
        ),
        provenance=dict(row.get("provenance") or {}),
        fact=fact,
    )


def _build_material_hash(
    *,
    fact_type: str,
    series_identity: Mapping[str, Any],
    records: Sequence[Any],
) -> str:
    if records and all(isinstance(record, CanonicalFactRecord) for record in records):
        canonical_rows = sorted(
            records,
            key=lambda record: (
                record.fact.observation_time,
                record.fact.observation_key,
                record.revision,
            ),
        )
        return _stable_hash(
            {
                "schema_version": "market.canonical_fact_series_material.v1",
                "series": dict(series_identity),
                "rows": [
                    {
                        "observation_time": _iso(record.fact.observation_time),
                        "observation_key": record.fact.observation_key,
                        "revision": record.revision,
                        "payload_schema_id": record.fact.payload_schema_id,
                        "payload_contract_hash": record.fact.payload_contract_hash,
                        "row_hash": record.row_hash,
                    }
                    for record in canonical_rows
                ],
            }
        )
    if fact_type == CANDLE_FACT_TYPE:
        return build_candle_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if fact_type == OPEN_INTEREST_FACT_TYPE:
        return build_open_interest_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if fact_type == FUNDING_RATE_FACT_TYPE:
        return build_funding_rate_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if get_fact_contract(fact_type).uses_exact_numeric_storage:
        return build_numeric_fact_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if fact_type == MARKET_TRADE_FACT_TYPE:
        return build_market_trade_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if fact_type == TRADE_FLOW_FACT_TYPE:
        return build_trade_flow_material_hash(
            series_identity=series_identity,
            records=records,
        )
    if records and all(
        isinstance(record, TypedFeatureRecord) for record in records
    ):
        return build_typed_feature_material_hash(
            series_identity=series_identity,
            records=records,
        )
    raise RuntimeError(
        f"market_dataset_unsupported_fact: fact_type={fact_type}"
    )


def _build_canonical_provenance_hash(
    records: Sequence[CanonicalFactRecord],
) -> str:
    rows = sorted(
        records,
        key=lambda record: (
            record.fact.observation_time,
            record.fact.observation_key,
            record.revision,
        ),
    )
    return _stable_hash(
        {
            "schema_version": "market.canonical_fact_provenance.v1",
            "records": [
                {
                    "fact_version_id": record.fact_version_id,
                    "source_identity_key": record.source_identity_key,
                    "ingestion_run_id": record.ingestion_run_id,
                    "provenance_hash": record.fact.provenance_hash,
                }
                for record in rows
            ],
        }
    )


_TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS = frozenset(
    {
        "candle.ohlcv.v1",
        "derivatives.open_interest.v1",
        "derivatives.funding_rate.v1",
        "market.reference_price.v1",
        "market.reserve_balance.v1",
        "market.trade.v1",
        "market.trade_flow.v1",
        "market.trade_flow_feature.v1",
        "market.futures_spot_basis.v1",
        "market.derivative_state.v1",
        "market.market_response.v1",
    }
)


def _collect_typed_archive_refs(
    session,
    *,
    records: Sequence[TypedFeatureRecord],
) -> dict[str, dict[str, str]]:
    """Resolve typed derived lineage back to acknowledged raw objects."""

    references: dict[str, dict[str, str]] = {}
    queue = [(record.series_id, record.fact.material_hash) for record in records]
    visited: set[tuple[int, str]] = set()

    def add_manifest_rows(rows: Sequence[Mapping[str, Any]]) -> None:
        for row in rows:
            references[str(row["manifest_id"])] = {
                "object_sha256": str(row["object_sha256"]),
                "content_fingerprint": str(row["content_fingerprint"]),
                "object_key": str(row["object_key"]),
                "object_uri": str(row["object_uri"]),
            }

    def add_book_position(position: Mapping[str, Any]) -> None:
        definition_id = str(position.get("definition_id") or "")
        session_id = str(position.get("session_id") or "")
        receive_ordinal = int(position.get("receive_ordinal") or 0)
        if not definition_id or not session_id or receive_ordinal <= 0:
            raise RuntimeError(
                "market_dataset_archive_incomplete: derived book position is malformed"
            )
        rows = session.execute(
            text(
                """
                SELECT id AS manifest_id, object_sha256,
                       content_fingerprint, object_key, object_uri
                FROM market.raw_archive_manifests
                WHERE definition_id = :definition_id
                  AND session_id = :session_id
                  AND first_receive_ordinal <= :receive_ordinal
                  AND last_receive_ordinal >= :receive_ordinal
                ORDER BY first_receive_ordinal, id
                """
            ),
            {
                "definition_id": definition_id,
                "session_id": session_id,
                "receive_ordinal": receive_ordinal,
            },
        ).mappings().all()
        if not rows:
            raise RuntimeError(
                "market_dataset_archive_incomplete: book source position has no acknowledged archive"
            )
        add_manifest_rows(rows)

    def add_coverage(coverage_interval_id: str) -> None:
        rows = session.execute(
            text(
                """
                SELECT DISTINCT manifests.id AS manifest_id,
                       manifests.object_sha256,
                       manifests.content_fingerprint,
                       manifests.object_key, manifests.object_uri
                FROM market.stream_coverage_interval_versions AS coverage
                JOIN market.raw_archive_manifests AS manifests
                  ON manifests.definition_id = coverage.definition_id
                 AND manifests.session_id = coverage.session_id
                WHERE coverage.interval_id = :coverage_interval_id
                ORDER BY manifests.id
                """
            ),
            {"coverage_interval_id": str(coverage_interval_id)},
        ).mappings().all()
        if not rows:
            raise RuntimeError(
                "market_dataset_archive_incomplete: coverage interval has no acknowledged archive"
            )
        add_manifest_rows(rows)

    def canonical_evidence(
        *,
        series_id: int,
        material_hash: str,
        evidence_key: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        row = session.execute(
            text(
                "SELECT provenance -> :evidence_key AS evidence, payload "
                "FROM market.fact_versions "
                "WHERE series_id = :series_id "
                "  AND provenance -> :evidence_key "
                "      ->> 'legacy_material_hash' = :material_hash "
                "ORDER BY market_commit_seq DESC "
                "LIMIT 1"
            ),
            {
                "series_id": int(series_id),
                "evidence_key": str(evidence_key),
                "material_hash": str(material_hash),
            },
        ).mappings().first()
        if row is None:
            raise RuntimeError(
                "market_dataset_provenance_incomplete: canonical derived "
                f"evidence is missing series_id={series_id}"
            )
        evidence = dict(row["evidence"] or {})
        if not evidence:
            raise RuntimeError(
                "market_dataset_provenance_incomplete: canonical derived "
                f"evidence is malformed series_id={series_id}"
            )
        return evidence, dict(row["payload"] or {})

    while queue:
        series_id, material_hash = queue.pop()
        key = (int(series_id), str(material_hash))
        if key in visited:
            continue
        visited.add(key)
        fact_type = session.execute(
            text("SELECT fact_type FROM market.series WHERE id = :series_id"),
            {"series_id": int(series_id)},
        ).scalar_one_or_none()
        if fact_type is None:
            raise RuntimeError(
                "market_dataset_provenance_incomplete: source series is missing"
            )
        fact_type = str(fact_type)
        if get_fact_contract(fact_type).uses_exact_numeric_storage:
            continue
        if fact_type in {
            CANDLE_FACT_TYPE,
            OPEN_INTEREST_FACT_TYPE,
            FUNDING_RATE_FACT_TYPE,
            "market.derivative_state",
        }:
            continue
        if fact_type == MARKET_TRADE_FACT_TYPE:
            row = session.execute(
                text(
                    """
                    SELECT provenance
                    FROM market.fact_versions
                    WHERE series_id = :series_id AND material_hash = :material_hash
                    LIMIT 1
                    """
                ),
                {"series_id": series_id, "material_hash": material_hash},
            ).mappings().first()
            if row is None:
                raise RuntimeError("market_dataset_provenance_incomplete: trade source missing")
            trade_evidence = dict(row["provenance"] or {}).get("_qt_trade_evidence")
            if not isinstance(trade_evidence, Mapping) or not trade_evidence.get(
                "raw_record_id"
            ):
                raise RuntimeError(
                    "market_dataset_provenance_incomplete: canonical trade raw reference missing"
                )
            manifests = session.execute(
                text(
                    """
                    SELECT manifests.id AS manifest_id, manifests.object_sha256,
                           manifests.content_fingerprint, manifests.object_key,
                           manifests.object_uri
                    FROM market.raw_archive_record_mappings AS mappings
                    JOIN market.raw_archive_manifests AS manifests
                      ON manifests.id = mappings.manifest_id
                    WHERE mappings.raw_record_id = :raw_record_id
                    """
                ),
                {"raw_record_id": str(trade_evidence["raw_record_id"])},
            ).mappings().all()
            if not manifests:
                raise RuntimeError("market_dataset_archive_incomplete: trade raw mapping missing")
            add_manifest_rows(manifests)
            continue
        if fact_type == TRADE_FLOW_FACT_TYPE:
            row = session.execute(
                text(
                    """
                    SELECT provenance, quality
                    FROM market.fact_versions
                    WHERE series_id = :series_id AND material_hash = :material_hash
                    LIMIT 1
                    """
                ),
                {"series_id": series_id, "material_hash": material_hash},
            ).mappings().first()
            flow_evidence = (
                dict(row["provenance"] or {}).get("_qt_trade_flow_evidence")
                if row is not None
                else None
            )
            flow_quality = (
                dict(row["quality"] or {}).get("_qt_trade_flow_quality")
                if row is not None
                else None
            )
            if (
                row is None
                or not isinstance(flow_evidence, Mapping)
                or not isinstance(flow_quality, Mapping)
                or not bool(flow_quality.get("archive_complete"))
                or not bool(flow_quality.get("canonicalization_complete"))
                or not flow_evidence.get("coverage_interval_id")
            ):
                raise RuntimeError(
                    "market_dataset_archive_incomplete: trade-flow source is incomplete"
                )
            add_coverage(str(flow_evidence["coverage_interval_id"]))
            continue
        if fact_type in {"market.bbo", "market.depth_observation"}:
            evidence_key = (
                "_qt_bbo_evidence"
                if fact_type == "market.bbo"
                else "_qt_depth_evidence"
            )
            evidence, _payload = canonical_evidence(
                series_id=series_id,
                material_hash=material_hash,
                evidence_key=evidence_key,
            )
            if not isinstance(evidence.get("source_position"), Mapping):
                raise RuntimeError(
                    "market_dataset_provenance_incomplete: book position missing"
                )
            add_book_position(dict(evidence["source_position"]))
            continue
        if fact_type == "market.trade_flow_feature":
            evidence, payload = canonical_evidence(
                series_id=series_id,
                material_hash=material_hash,
                evidence_key="_qt_trade_flow_feature_evidence",
            )
            queue.append(
                (
                    int(evidence["source_trade_flow_series_id"]),
                    str(payload["aggregate_material_hash"]),
                )
            )
            continue
        if fact_type == "market.futures_spot_relationship":
            evidence, _payload = canonical_evidence(
                series_id=series_id,
                material_hash=material_hash,
                evidence_key="_qt_basis_evidence",
            )
            queue.extend(
                (
                    (
                        int(evidence["futures_series_id"]),
                        str(evidence["futures_bbo_material_hash"]),
                    ),
                    (
                        int(evidence["spot_series_id"]),
                        str(evidence["spot_bbo_material_hash"]),
                    ),
                )
            )
            continue
        if fact_type == "market.market_response":
            evidence, _payload = canonical_evidence(
                series_id=series_id,
                material_hash=material_hash,
                evidence_key="_qt_response_evidence",
            )
            queue.append(
                (
                    int(evidence["source_flow_feature_series_id"]),
                    str(evidence["source_flow_material_hash"]),
                )
            )
            for name in (
                "pre_book_source_position",
                "trough_book_source_position",
                "post_book_source_position",
            ):
                add_book_position(dict(evidence[name]))
            continue
        if fact_type.startswith("market.normalized."):
            # The required frozen source series owns transitive archive lineage;
            # normalized rows retain only bounded witness hashes.
            continue
        raise RuntimeError(
            f"market_dataset_provenance_incomplete: unsupported lineage fact_type={fact_type}"
        )
    return references


def _verify_local_archive_objects(references: Mapping[str, Mapping[str, str]]) -> None:
    """Require physical bytes and checksums for the local object-store adapter."""

    if not references:
        return
    storage_root = Path(
        os.environ.get("MARKET_STRUCTURE_STORAGE_ROOT", "logs/market-structure")
    ).resolve()
    object_root = (storage_root / "objects").resolve()
    for manifest_id, reference in sorted(references.items()):
        uri = str(reference.get("object_uri") or "")
        if not uri.startswith("market-archive://"):
            raise RuntimeError(
                "market_dataset_archive_verification_unsupported: object adapter cannot be verified"
            )
        object_path = (object_root / str(reference["object_key"])).resolve()
        if object_root not in object_path.parents or not object_path.is_file():
            raise RuntimeError(
                f"market_dataset_archive_object_missing: manifest_id={manifest_id}"
            )
        digest = hashlib.sha256()
        with object_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != str(reference["object_sha256"]):
            raise RuntimeError(
                f"market_dataset_archive_object_corrupt: manifest_id={manifest_id}"
            )
class PostgresMarketDataRepository:
    """Single PostgreSQL owner for accepted candle facts and frozen datasets."""

    def current_commit_seq(self) -> int:
        """Return the latest accepted market-fact commit sequence."""

        with db.session() as session:
            return self._current_commit_seq_with_session(session)

    @staticmethod
    def _current_commit_seq_with_session(session) -> int:
        return int(
            session.execute(
                text(
                    """
                    SELECT GREATEST(
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.fact_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.candle_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.open_interest_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.funding_rate_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.numeric_fact_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.market_trade_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.trade_flow_aggregate_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.normalized_feature_versions), 0)
                    )
                    """
                )
            ).scalar_one()
        )

    def list_series(self, *, instrument_id: Optional[str] = None) -> list[dict[str, Any]]:
        """Return canonical logical series and accepted-version counts."""

        predicates: list[str] = []
        params: dict[str, Any] = {}
        if instrument_id is not None:
            normalized = str(instrument_id or "").strip()
            if not normalized:
                raise ValueError("market_data_series_invalid: instrument_id is empty")
            predicates.append("series.instrument_id = :instrument_id")
            params["instrument_id"] = normalized
        where_sql = "WHERE " + " AND ".join(predicates) if predicates else ""
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT series.id, series.identity_key, series.instrument_id,
                           series.fact_type, series.timeframe_seconds,
                           series.contract_version, series.dimensions,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                             THEN COALESCE(canonical.version_count, 0)
                             ELSE COALESCE(candles.version_count, 0)
                             + COALESCE(open_interest.version_count, 0)
                             + COALESCE(funding.version_count, 0)
                             + COALESCE(numeric_facts.version_count, 0)
                             + COALESCE(trades.version_count, 0)
                             + COALESCE(trade_flow.version_count, 0)
                             + COALESCE(typed_features.version_count, 0)
                           END AS version_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                             THEN COALESCE(canonical.fact_count, 0)
                             ELSE COALESCE(candles.fact_count, 0)
                             + COALESCE(open_interest.fact_count, 0)
                             + COALESCE(funding.fact_count, 0)
                             + COALESCE(numeric_facts.fact_count, 0)
                             + COALESCE(trades.fact_count, 0)
                             + COALESCE(trade_flow.fact_count, 0)
                             + COALESCE(typed_features.fact_count, 0)
                           END AS fact_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                                  AND series.fact_type = 'candle.ohlcv'
                             THEN COALESCE(canonical.fact_count, 0)
                             ELSE COALESCE(candles.fact_count, 0)
                           END AS candle_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                                  AND series.fact_type IN (
                                      'derivatives.open_interest',
                                      'derivatives.funding_rate'
                                  )
                             THEN COALESCE(canonical.fact_count, 0)
                             ELSE COALESCE(open_interest.fact_count, 0)
                                + COALESCE(funding.fact_count, 0)
                           END AS observation_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                                  AND series.fact_type = 'derivatives.funding_rate'
                             THEN COALESCE(canonical.fact_count, 0)
                             ELSE COALESCE(funding.fact_count, 0)
                           END AS funding_rate_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                                  AND series.fact_type IN (
                                      'market.reference_price',
                                      'market.reserve_balance'
                                  )
                             THEN COALESCE(canonical.fact_count, 0)
                             ELSE COALESCE(numeric_facts.fact_count, 0)
                           END AS numeric_fact_count,
                           COALESCE(trades.fact_count, 0) AS trade_count,
                           COALESCE(trade_flow.fact_count, 0) AS trade_flow_count,
                           COALESCE(typed_features.fact_count, 0) AS feature_count,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                             THEN canonical.first_fact_time
                             ELSE COALESCE(candles.first_fact_time, open_interest.first_fact_time,
                                    funding.first_fact_time, trades.first_fact_time,
                                    numeric_facts.first_fact_time,
                                    trade_flow.first_fact_time,
                                    typed_features.first_fact_time)
                           END AS first_fact_time,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                             THEN canonical.last_fact_time
                             ELSE COALESCE(candles.last_fact_time, open_interest.last_fact_time,
                                    funding.last_fact_time, trades.last_fact_time,
                                    numeric_facts.last_fact_time,
                                    trade_flow.last_fact_time,
                                    typed_features.last_fact_time)
                           END AS last_fact_time,
                           CASE WHEN registered_schema.schema_id IS NOT NULL
                             THEN COALESCE(canonical.max_commit_seq, 0)
                             ELSE GREATEST(
                             COALESCE(candles.max_commit_seq, 0),
                             COALESCE(open_interest.max_commit_seq, 0),
                             COALESCE(funding.max_commit_seq, 0),
                             COALESCE(numeric_facts.max_commit_seq, 0),
                             COALESCE(trades.max_commit_seq, 0),
                             COALESCE(trade_flow.max_commit_seq, 0),
                             COALESCE(typed_features.max_commit_seq, 0)
                           ) END AS max_commit_seq
                    FROM market.series AS series
                    LEFT JOIN market.fact_schemas AS registered_schema
                      ON registered_schema.schema_id = series.contract_version
                     AND registered_schema.fact_type = series.fact_type
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT observation_key) AS fact_count,
                               min(observation_time) AS first_fact_time,
                               max(observation_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.fact_versions
                        WHERE series_id = series.id
                    ) AS canonical ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT candle_open_time) AS fact_count,
                               min(candle_open_time) AS first_fact_time,
                               max(candle_open_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.candle_versions
                        WHERE series_id = series.id
                    ) AS candles ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT sample_time) AS fact_count,
                               min(sample_time) AS first_fact_time,
                               max(sample_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.open_interest_versions
                        WHERE series_id = series.id
                    ) AS open_interest ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT sample_time) AS fact_count,
                               min(sample_time) AS first_fact_time,
                               max(sample_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.funding_rate_versions
                        WHERE series_id = series.id
                    ) AS funding ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT source_event_key) AS fact_count,
                               min(effective_at) AS first_fact_time,
                               max(effective_at) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.numeric_fact_versions
                        WHERE series_id = series.id
                    ) AS numeric_facts ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT (source_id, provider_product_id,
                                               provider_trade_id)) AS fact_count,
                               min(provider_event_time) AS first_fact_time,
                               max(provider_event_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.market_trade_versions
                        WHERE series_id = series.id
                    ) AS trades ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT (bucket_start, interval_seconds,
                                               aggregation_version)) AS fact_count,
                               min(bucket_start) AS first_fact_time,
                               max(bucket_start) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM market.trade_flow_aggregate_versions
                        WHERE series_id = series.id
                    ) AS trade_flow ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS version_count,
                               count(DISTINCT fact_key) AS fact_count,
                               min(fact_time) AS first_fact_time,
                               max(fact_time) AS last_fact_time,
                               max(market_commit_seq) AS max_commit_seq
                        FROM (
                            SELECT jsonb_build_array(observation_key) AS fact_key,
                                   observation_time AS fact_time,
                                   market_commit_seq
                            FROM market.fact_versions
                            WHERE series_id = series.id
                              AND payload_schema_id IN (
                                  'market.bbo.v1',
                                  'market.depth_band.v1',
                                  'market.trade_flow_feature.v1',
                                  'market.futures_spot_basis.v1',
                                  'market.derivative_state.v1',
                                  'market.market_response.v1'
                              )
                            UNION ALL
                            SELECT jsonb_build_array(effective_at, spec_id),
                                   effective_at, market_commit_seq
                            FROM market.normalized_feature_versions
                            WHERE series_id = series.id
                        ) AS typed_feature_rows
                    ) AS typed_features ON TRUE
                    {where_sql}
                    ORDER BY series.instrument_id, series.fact_type,
                             series.timeframe_seconds NULLS FIRST, series.id
                    """
                ),
                params,
            ).mappings().all()
        return [dict(row) for row in rows]

    def get_dataset(self, dataset_id: str) -> FrozenDataset:
        """Load an immutable dataset manifest by exact ID."""

        normalized = str(dataset_id or "").strip()
        if not normalized:
            raise ValueError("market_dataset_invalid: dataset_id is required")
        with db.session() as session:
            dataset = session.execute(
                text(
                    """
                    SELECT id, dataset_hash, max_commit_seq, name, purpose, metadata,
                           created_at
                    FROM market.datasets
                    WHERE id = :dataset_id
                    """
                ),
                {"dataset_id": normalized},
            ).mappings().first()
            if dataset is None:
                raise ValueError(f"market_dataset_unknown: dataset_id={normalized}")
            rows = session.execute(
                text(
                    """
                    SELECT dataset_series.series_id, dataset_series.range_start,
                           dataset_series.range_end, dataset_series.max_commit_seq,
                           dataset_series.row_count, dataset_series.material_hash,
                           dataset_series.provenance_hash, dataset_series.source_summary,
                           dataset_series.quality_hash, dataset_series.quality_summary,
                           dataset_series.quality_evidence,
                           dataset_series.payload_schemas,
                           series.identity_key, series.instrument_id, series.fact_type,
                           series.timeframe_seconds, series.contract_version,
                           series.dimensions
                    FROM market.dataset_series AS dataset_series
                    JOIN market.series AS series ON series.id = dataset_series.series_id
                    WHERE dataset_id = :dataset_id
                    ORDER BY series_id, range_start, range_end
                    """
                ),
                {"dataset_id": normalized},
            ).mappings().all()
            archive_rows = session.execute(
                text(
                    """
                    SELECT refs.raw_archive_manifest_id, refs.inclusion_role,
                           refs.object_sha256, refs.content_fingerprint,
                           manifests.object_key, manifests.object_uri
                    FROM market.dataset_archive_refs AS refs
                    JOIN market.raw_archive_manifests AS manifests
                      ON manifests.id = refs.raw_archive_manifest_id
                    WHERE refs.dataset_id = :dataset_id
                    ORDER BY raw_archive_manifest_id
                    """
                ),
                {"dataset_id": normalized},
            ).mappings().all()
            normalization_rows = session.execute(
                text(
                    """
                    SELECT spec_id, output_series_id, range_start, range_end,
                           input_range_start, input_range_end, input_count,
                           input_watermark, source_series_ids, input_fingerprint,
                           source_dataset_fingerprints, material_hash,
                           provenance_hash, quality_hash, storage_kind,
                           frozen_object_uri, frozen_object_sha256, row_count
                    FROM market.dataset_normalization_refs
                    WHERE dataset_id = :dataset_id
                    ORDER BY output_series_id, spec_id
                    """
                ),
                {"dataset_id": normalized},
            ).mappings().all()
        _verify_local_archive_objects(
            {
                str(row["raw_archive_manifest_id"]): dict(row)
                for row in archive_rows
            }
        )
        return FrozenDataset(
            dataset_id=str(dataset["id"]),
            dataset_hash=str(dataset["dataset_hash"]),
            max_commit_seq=int(dataset["max_commit_seq"]),
            series=tuple(dict(row) for row in rows),
            contract_version="market_dataset.v1",
            name=str(dataset["name"]) if dataset.get("name") else None,
            purpose=str(dataset["purpose"]),
            metadata={
                **dict(dataset.get("metadata") or {}),
                "archive_refs": [dict(row) for row in archive_rows],
                "normalization_refs": [dict(row) for row in normalization_rows],
            },
            created_at=dataset.get("created_at"),
        )

    def register_source(
        self,
        identity: SourceIdentity,
        *,
        lineage: Optional[Mapping[str, Any]] = None,
    ) -> int:
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.sources (
                        identity_key, provider, venue, source_kind, adapter_version, lineage
                    ) VALUES (
                        :identity_key, :provider, :venue, :source_kind, :adapter_version,
                        CAST(:lineage AS jsonb)
                    )
                    ON CONFLICT (identity_key) DO NOTHING
                    """
                ),
                {
                    "identity_key": identity.identity_key,
                    "provider": identity.provider,
                    "venue": identity.venue,
                    "source_kind": identity.source_kind,
                    "adapter_version": identity.adapter_version,
                    "lineage": _json_text(lineage),
                },
            )
            row = session.execute(
                text(
                    """
                    SELECT id, provider, venue, source_kind, adapter_version
                    FROM market.sources
                    WHERE identity_key = :identity_key
                    """
                ),
                {"identity_key": identity.identity_key},
            ).mappings().one()
        actual = (
            str(row["provider"]),
            str(row["venue"]),
            str(row["source_kind"]),
            str(row["adapter_version"]),
        )
        expected = (
            identity.provider,
            identity.venue,
            identity.source_kind,
            identity.adapter_version,
        )
        if actual != expected:
            raise RuntimeError(
                "market_data_source_conflict: identity hash resolved to different source"
            )
        return int(row["id"])

    def register_series(
        self,
        *,
        instrument_id: str,
        fact_type: str,
        timeframe_seconds: Optional[int],
        contract_version: str,
        dimensions: Optional[Mapping[str, Any]] = None,
    ) -> int:
        instrument_id = str(instrument_id or "").strip()
        fact_type = str(fact_type or "").strip().lower()
        contract_version = str(contract_version or "").strip()
        timeframe = int(timeframe_seconds) if timeframe_seconds is not None else None
        if not instrument_id or not fact_type or not contract_version:
            raise ValueError("market_data_series_invalid: complete series identity is required")
        contract = get_fact_contract(fact_type)
        contract.validate(contract_version=contract_version, timeframe_seconds=timeframe)
        normalized_dimensions = contract.normalize_dimensions(dimensions)

        identity_payload: dict[str, Any] = {
            "schema_version": _SERIES_IDENTITY_VERSION,
            "instrument_id": instrument_id,
            "fact_type": fact_type,
            "timeframe_seconds": timeframe,
            "contract_version": contract_version,
        }
        if normalized_dimensions:
            identity_payload["schema_version"] = _DIMENSIONAL_SERIES_IDENTITY_VERSION
            identity_payload["dimensions"] = normalized_dimensions
        identity_key = _stable_hash(identity_payload)
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.series (
                        identity_key, instrument_id, fact_type,
                        timeframe_seconds, contract_version, dimensions
                    ) VALUES (
                        :identity_key, :instrument_id, :fact_type,
                        :timeframe_seconds, :contract_version,
                        CAST(:dimensions AS jsonb)
                    )
                    ON CONFLICT (identity_key) DO NOTHING
                    """
                ),
                {
                    "identity_key": identity_key,
                    "instrument_id": instrument_id,
                    "fact_type": fact_type,
                    "timeframe_seconds": timeframe,
                    "contract_version": contract_version,
                    "dimensions": _json_text(normalized_dimensions),
                },
            )
            row = session.execute(
                text(
                    """
                    SELECT id, instrument_id, fact_type,
                           timeframe_seconds, contract_version, dimensions
                    FROM market.series
                    WHERE identity_key = :identity_key
                    """
                ),
                {"identity_key": identity_key},
            ).mappings().one()
        actual = (
            str(row["instrument_id"]),
            str(row["fact_type"]),
            row.get("timeframe_seconds"),
            str(row["contract_version"]),
            dict(row.get("dimensions") or {}),
        )
        expected = (
            instrument_id,
            fact_type,
            timeframe,
            contract_version,
            normalized_dimensions,
        )
        if actual != expected:
            raise RuntimeError(
                "market_data_series_conflict: identity hash resolved to different series"
            )
        return int(row["id"])

    def resolve_series_id(
        self,
        *,
        instrument_id: str,
        fact_type: str,
        timeframe_seconds: Optional[int],
        contract_version: str,
        dimensions: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Resolve one canonical logical series or fail without provider fallback."""

        fact_type = str(fact_type or "").strip().lower()
        contract_version = str(contract_version or "").strip()
        timeframe = int(timeframe_seconds) if timeframe_seconds is not None else None
        contract = get_fact_contract(fact_type)
        contract.validate(
            contract_version=contract_version,
            timeframe_seconds=timeframe,
        )
        normalized_dimensions = contract.normalize_dimensions(dimensions)
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT id
                    FROM market.series
                    WHERE instrument_id = :instrument_id
                      AND fact_type = :fact_type
                      AND timeframe_seconds IS NOT DISTINCT FROM :timeframe_seconds
                      AND contract_version = :contract_version
                      AND dimensions = CAST(:dimensions AS jsonb)
                    ORDER BY id
                    """
                ),
                {
                    "instrument_id": str(instrument_id or "").strip(),
                    "fact_type": fact_type,
                    "timeframe_seconds": timeframe,
                    "contract_version": contract_version,
                    "dimensions": _json_text(normalized_dimensions),
                },
            ).scalars().all()
        if not rows:
            raise ValueError(
                "market_data_series_missing: explicit ingestion is required before read"
            )
        if len(rows) != 1:
            raise RuntimeError(
                "market_data_series_ambiguous: canonical series uniqueness is violated"
            )
        return int(rows[0])

    def ingest_facts(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[CanonicalFact],
        request: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        """Persist schema-registered canonical Facts through the one writer."""

        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(
            list(facts),
            key=lambda item: (item.observation_time, item.observation_key),
        )
        if series_id <= 0 or source_id <= 0:
            raise ValueError(
                "market_data_ingest_invalid: series_id and source_id must be positive"
            )
        if not rows:
            raise ValueError(
                "market_data_ingest_invalid: at least one canonical Fact is required"
            )
        observation_keys = [fact.observation_key for fact in rows]
        if len(observation_keys) != len(set(observation_keys)):
            raise ValueError(
                "market_data_ingest_invalid: duplicate canonical observation_key"
            )
        series = self._get_series(series_id)
        source = self._get_source_identity(source_id)
        for fact in rows:
            if (
                fact.fact_type != str(series["fact_type"])
                or fact.payload_schema_id != str(series["contract_version"])
                or fact.source.identity_key != source.identity_key
            ):
                raise ValueError(
                    "market_data_ingest_invalid: canonical Fact disagrees with "
                    f"series/source series_id={series_id} "
                    f"observation_key={fact.observation_key}"
                )
        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")
        self._start_ingestion_run(
            run_id=run_id,
            source_id=source_id,
            request=request,
            source_revision=source_revision,
            requested_start=rows[0].observation_time,
            requested_end=rows[-1].observation_time,
            requested_count=len(rows),
        )
        try:
            return self._ingest_canonical_rows(
                run_id=run_id,
                series_id=series_id,
                rows=rows,
                allow_corrections=bool(allow_corrections),
                collection_fence=collection_fence,
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise

    def ingest_facts_in_session(
        self,
        session,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[CanonicalFact],
        request: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        """Persist canonical Facts inside an existing operational transaction."""

        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(
            list(facts),
            key=lambda item: (item.observation_time, item.observation_key),
        )
        if series_id <= 0 or source_id <= 0:
            raise ValueError(
                "market_data_ingest_invalid: series_id and source_id must be positive"
            )
        if not rows:
            raise ValueError(
                "market_data_ingest_invalid: at least one canonical Fact is required"
            )
        observation_keys = [fact.observation_key for fact in rows]
        if len(observation_keys) != len(set(observation_keys)):
            raise ValueError(
                "market_data_ingest_invalid: duplicate canonical observation_key"
            )
        series = session.execute(
            text(
                """
                SELECT id, identity_key, instrument_id, fact_type,
                       timeframe_seconds, contract_version, dimensions
                FROM market.series
                WHERE id = :series_id
                """
            ),
            {"series_id": series_id},
        ).mappings().first()
        if series is None:
            raise ValueError(f"market_data_series_unknown: series_id={series_id}")
        source_row = session.execute(
            text(
                """
                SELECT id AS source_id,
                       identity_key AS source_identity_key,
                       provider AS source_provider,
                       venue AS source_venue,
                       source_kind,
                       adapter_version AS source_adapter_version
                FROM market.sources
                WHERE id = :source_id
                """
            ),
            {"source_id": source_id},
        ).mappings().first()
        if source_row is None:
            raise ValueError(f"market_data_source_unknown: source_id={source_id}")
        source = _canonical_source(source_row)
        for fact in rows:
            if (
                fact.fact_type != str(series["fact_type"])
                or fact.payload_schema_id != str(series["contract_version"])
                or fact.source.identity_key != source.identity_key
            ):
                raise ValueError(
                    "market_data_ingest_invalid: canonical Fact disagrees with "
                    f"series/source series_id={series_id} "
                    f"observation_key={fact.observation_key}"
                )
        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")
        session.execute(
            text(
                """
                INSERT INTO market.ingestion_runs (
                    id, source_id, status, request, source_revision,
                    requested_start, requested_end, requested_count
                ) VALUES (
                    :id, :source_id, 'running', CAST(:request AS jsonb),
                    :source_revision, :requested_start, :requested_end,
                    :requested_count
                )
                """
            ),
            {
                "id": run_id,
                "source_id": source_id,
                "request": _json_text(request),
                "source_revision": (
                    str(source_revision).strip() if source_revision else None
                ),
                "requested_start": rows[0].observation_time,
                "requested_end": rows[-1].observation_time,
                "requested_count": len(rows),
            },
        )
        return self._ingest_canonical_rows_with_session(
            session,
            run_id=run_id,
            series_id=series_id,
            rows=rows,
            allow_corrections=bool(allow_corrections),
            collection_fence=collection_fence,
        )

    def ingest_candles(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[CandleFact],
        request: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
    ) -> IngestionOutcome:
        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(list(facts), key=lambda item: item.open_time)
        if series_id <= 0:
            raise ValueError("market_data_ingest_invalid: series_id must be positive")
        if source_id <= 0:
            raise ValueError("market_data_ingest_invalid: source_id must be positive")
        if not rows:
            raise ValueError("market_data_ingest_invalid: at least one candle is required")
        duplicate_times = [
            current.open_time
            for previous, current in zip(rows, rows[1:])
            if previous.open_time == current.open_time
        ]
        if duplicate_times:
            raise ValueError(
                "market_data_ingest_invalid: duplicate candle open_time "
                f"{_iso(duplicate_times[0])}"
            )

        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")
        series = self._get_series(series_id)
        if str(series["fact_type"]) != CANDLE_FACT_TYPE:
            raise ValueError(
                f"market_data_ingest_invalid: series_id={series_id} is not a candle series"
            )
        timeframe_seconds = int(series["timeframe_seconds"])
        for fact in rows:
            duration = int((fact.close_time - fact.open_time).total_seconds())
            if duration != timeframe_seconds:
                raise ValueError(
                    "market_data_ingest_invalid: candle duration does not match series "
                    f"open_time={_iso(fact.open_time)} expected_seconds={timeframe_seconds} "
                    f"actual_seconds={duration}"
                )

        self._start_ingestion_run(
            run_id=run_id,
            source_id=source_id,
            request=request,
            source_revision=source_revision,
            requested_start=rows[0].open_time,
            requested_end=rows[-1].close_time,
            requested_count=len(rows),
        )
        try:
            source = self._get_source_identity(source_id)
            outcome = self._ingest_canonical_rows(
                run_id=run_id,
                series_id=series_id,
                rows=[
                    _candle_to_canonical(fact, source=source, provenance={})
                    for fact in rows
                ],
                allow_corrections=bool(allow_corrections),
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise
        return outcome

    def ingest_open_interest(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[OpenInterestFact],
        request: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(list(facts), key=lambda item: item.sample_time)
        if series_id <= 0 or source_id <= 0:
            raise ValueError(
                "market_data_ingest_invalid: series_id and source_id must be positive"
            )
        if not rows:
            raise ValueError(
                "market_data_ingest_invalid: at least one open-interest fact is required"
            )
        duplicate_times = [
            current.sample_time
            for previous, current in zip(rows, rows[1:])
            if previous.sample_time == current.sample_time
        ]
        if duplicate_times:
            raise ValueError(
                "market_data_ingest_invalid: duplicate open-interest sample_time "
                f"{_iso(duplicate_times[0])}"
            )
        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")
        series = self._get_series(series_id)
        if (
            str(series["fact_type"]) != OPEN_INTEREST_FACT_TYPE
            or str(series["contract_version"]) != OPEN_INTEREST_FACT_VERSION
            or series.get("timeframe_seconds") is not None
        ):
            raise ValueError(
                f"market_data_ingest_invalid: series_id={series_id} is not an open-interest v1 series"
            )
        self._start_ingestion_run(
            run_id=run_id,
            source_id=source_id,
            request=request,
            source_revision=source_revision,
            requested_start=rows[0].sample_time,
            requested_end=rows[-1].sample_time,
            requested_count=len(rows),
        )
        try:
            source = self._get_source_identity(source_id)
            return self._ingest_canonical_rows(
                run_id=run_id,
                series_id=series_id,
                rows=[
                    _open_interest_to_canonical(
                        fact,
                        source=source,
                        provenance=dict(provenance or {}),
                    )
                    for fact in rows
                ],
                allow_corrections=bool(allow_corrections),
                collection_fence=collection_fence,
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise

    def ingest_funding_rates(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[FundingRateFact],
        request: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(list(facts), key=lambda item: item.sample_time)
        if series_id <= 0 or source_id <= 0:
            raise ValueError(
                "market_data_ingest_invalid: series_id and source_id must be positive"
            )
        if not rows:
            raise ValueError(
                "market_data_ingest_invalid: at least one funding-rate fact is required"
            )
        duplicate_times = [
            current.sample_time
            for previous, current in zip(rows, rows[1:])
            if previous.sample_time == current.sample_time
        ]
        if duplicate_times:
            raise ValueError(
                "market_data_ingest_invalid: duplicate funding-rate sample_time "
                f"{_iso(duplicate_times[0])}"
            )
        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")
        series = self._get_series(series_id)
        if (
            str(series["fact_type"]) != FUNDING_RATE_FACT_TYPE
            or str(series["contract_version"]) != FUNDING_RATE_FACT_VERSION
            or series.get("timeframe_seconds") is not None
        ):
            raise ValueError(
                f"market_data_ingest_invalid: series_id={series_id} is not a funding-rate v1 series"
            )
        self._start_ingestion_run(
            run_id=run_id,
            source_id=source_id,
            request=request,
            source_revision=source_revision,
            requested_start=rows[0].sample_time,
            requested_end=rows[-1].sample_time,
            requested_count=len(rows),
        )
        try:
            source = self._get_source_identity(source_id)
            return self._ingest_canonical_rows(
                run_id=run_id,
                series_id=series_id,
                rows=[
                    _funding_rate_to_canonical(
                        fact,
                        source=source,
                        provenance=dict(provenance or {}),
                    )
                    for fact in rows
                ],
                allow_corrections=bool(allow_corrections),
                collection_fence=collection_fence,
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise

    def ingest_numeric_facts(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[NumericFact],
        request: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        provenance_by_event: Optional[Mapping[str, Mapping[str, Any]]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
    ) -> IngestionOutcome:
        """Append exact numeric revisions without knowing provider table names."""

        series_id = int(series_id)
        source_id = int(source_id)
        rows = sorted(
            list(facts),
            key=lambda item: (item.effective_at, item.source_event_key),
        )
        if series_id <= 0 or source_id <= 0:
            raise ValueError(
                "market_data_ingest_invalid: series_id and source_id must be positive"
            )
        if not rows:
            raise ValueError(
                "market_data_ingest_invalid: at least one numeric fact is required"
            )
        event_keys = [fact.source_event_key for fact in rows]
        if len(event_keys) != len(set(event_keys)):
            raise ValueError(
                "market_data_ingest_invalid: duplicate numeric source_event_key"
            )
        run_id = str(ingestion_run_id or uuid.uuid4().hex).strip()
        if not run_id or len(run_id) > 64:
            raise ValueError("market_data_ingest_invalid: ingestion_run_id is invalid")

        series = self._get_series(series_id)
        contract = get_fact_contract(str(series["fact_type"]))
        if not contract.uses_exact_numeric_storage:
            raise ValueError(
                f"market_data_ingest_invalid: series_id={series_id} is not exact numeric"
            )
        series_dimensions = contract.normalize_dimensions(series.get("dimensions"))
        for fact in rows:
            if (
                fact.fact_type != str(series["fact_type"])
                or fact.contract_version != str(series["contract_version"])
                or dict(fact.dimensions) != series_dimensions
            ):
                raise ValueError(
                    "market_data_ingest_invalid: numeric fact disagrees with series "
                    f"series_id={series_id} source_event_key={fact.source_event_key}"
                )

        self._start_ingestion_run(
            run_id=run_id,
            source_id=source_id,
            request=request,
            source_revision=source_revision,
            requested_start=rows[0].effective_at,
            requested_end=rows[-1].effective_at,
            requested_count=len(rows),
        )
        try:
            source = self._get_source_identity(source_id)
            common_provenance = dict(provenance or {})
            event_provenance = {
                str(key): dict(value)
                for key, value in dict(provenance_by_event or {}).items()
            }
            return self._ingest_canonical_rows(
                run_id=run_id,
                series_id=series_id,
                rows=[
                    _numeric_to_canonical(
                        fact,
                        source=source,
                        provenance={
                            **common_provenance,
                            **dict(event_provenance.get(fact.source_event_key) or {}),
                        },
                    )
                    for fact in rows
                ],
                allow_corrections=bool(allow_corrections),
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise

    def _ingest_numeric_fact_rows(
        self,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[NumericFact],
        provenance: Mapping[str, Any],
        provenance_by_event: Mapping[str, Mapping[str, Any]],
        allow_corrections: bool,
    ) -> IngestionOutcome:
        inserted_count = 0
        corrected_count = 0
        noop_count = 0
        max_commit_seq = 0
        with db.session() as session:
            session.execute(
                text("SELECT pg_advisory_xact_lock(:series_id)"),
                {"series_id": series_id},
            )
            for fact in rows:
                latest = session.execute(
                    text(
                        """
                        SELECT revision, row_hash
                        FROM market.numeric_fact_versions
                        WHERE series_id = :series_id
                          AND source_event_key = :source_event_key
                        ORDER BY revision DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "series_id": series_id,
                        "source_event_key": fact.source_event_key,
                    },
                ).mappings().first()
                if latest is not None and str(latest["row_hash"]) == fact.row_hash:
                    noop_count += 1
                    continue
                if latest is not None and not allow_corrections:
                    raise RuntimeError(
                        "market_data_correction_rejected: "
                        f"series_id={series_id} "
                        f"source_event_key={fact.source_event_key}"
                    )
                revision = 1 if latest is None else int(latest["revision"]) + 1
                row_provenance = {
                    **dict(provenance),
                    **dict(provenance_by_event.get(fact.source_event_key) or {}),
                }
                commit_seq = int(
                    session.execute(
                        text(
                            """
                            INSERT INTO market.numeric_fact_versions (
                                series_id, source_event_key, revision,
                                ingestion_run_id, fact_type, contract_version,
                                numeric_value, raw_value, unit, dimensions,
                                effective_at, effective_at_method,
                                source_published_at, received_at, accepted_at,
                                known_at, known_at_method,
                                source_event_group_key,
                                source_event_component_key, state,
                                source_event_material_hash,
                                provenance, row_hash
                            ) VALUES (
                                :series_id, :source_event_key, :revision,
                                :ingestion_run_id, :fact_type, :contract_version,
                                CAST(:numeric_value AS numeric), :raw_value, :unit,
                                CAST(:dimensions AS jsonb),
                                :effective_at, :effective_at_method,
                                :source_published_at, :received_at, :accepted_at,
                                :known_at, :known_at_method,
                                :source_event_group_key,
                                :source_event_component_key, :state,
                                :source_event_material_hash,
                                CAST(:provenance AS jsonb), :row_hash
                            )
                            RETURNING market_commit_seq
                            """
                        ),
                        {
                            "series_id": series_id,
                            "source_event_key": fact.source_event_key,
                            "revision": revision,
                            "ingestion_run_id": run_id,
                            "fact_type": fact.fact_type,
                            "contract_version": fact.contract_version,
                            "numeric_value": str(fact.value),
                            "raw_value": fact.raw_value,
                            "unit": fact.unit,
                            "dimensions": _json_text(fact.dimensions),
                            "effective_at": fact.effective_at,
                            "effective_at_method": fact.effective_at_method,
                            "source_published_at": fact.source_published_at,
                            "received_at": fact.received_at,
                            "accepted_at": fact.accepted_at,
                            "known_at": fact.known_at,
                            "known_at_method": fact.known_at_method,
                            "source_event_group_key": fact.source_event_group_key,
                            "source_event_component_key": fact.source_event_component_key,
                            "state": fact.state.value,
                            "source_event_material_hash": fact.source_event_material_hash,
                            "provenance": _json_text(row_provenance),
                            "row_hash": fact.row_hash,
                        },
                    ).scalar_one()
                )
                max_commit_seq = max(max_commit_seq, commit_seq)
                if latest is None:
                    inserted_count += 1
                else:
                    corrected_count += 1
            if max_commit_seq == 0:
                max_commit_seq = self._current_commit_seq_with_session(session)
            session.execute(
                text(
                    """
                    UPDATE market.ingestion_runs
                    SET status = 'completed', finished_at = now(),
                        inserted_count = :inserted_count,
                        corrected_count = :corrected_count,
                        noop_count = :noop_count
                    WHERE id = :run_id AND status = 'running'
                    """
                ),
                {
                    "run_id": run_id,
                    "inserted_count": inserted_count,
                    "corrected_count": corrected_count,
                    "noop_count": noop_count,
                },
            )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=inserted_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

    def _start_ingestion_run(
        self,
        *,
        run_id: str,
        source_id: int,
        request: Optional[Mapping[str, Any]],
        source_revision: Optional[str],
        requested_start: datetime,
        requested_end: datetime,
        requested_count: int,
    ) -> None:
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.ingestion_runs (
                        id, source_id, status, request, source_revision,
                        requested_start, requested_end, requested_count
                    ) VALUES (
                        :id, :source_id, 'running', CAST(:request AS jsonb), :source_revision,
                        :requested_start, :requested_end, :requested_count
                    )
                    """
                ),
                {
                    "id": run_id,
                    "source_id": source_id,
                    "request": _json_text(request),
                    "source_revision": str(source_revision).strip() if source_revision else None,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "requested_count": int(requested_count),
                },
            )

    @staticmethod
    def _assert_collection_fence(
        session,
        *,
        series_id: int,
        collection_fence: Optional[Mapping[str, Any]],
    ) -> None:
        if collection_fence is None:
            return
        fence_kind = str(
            collection_fence.get("fence_kind") or "collection"
        ).strip()
        definition_id = str(
            collection_fence.get("definition_id") or ""
        ).strip()
        owner_id = str(collection_fence.get("owner_id") or "").strip()
        lease_token = str(collection_fence.get("lease_token") or "").strip()
        try:
            fenced_source_id = int(collection_fence.get("source_id"))
            lease_generation = int(collection_fence.get("lease_generation"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "market_collection_fence_invalid: "
                "source and lease generation are required"
            ) from exc
        if not definition_id or not owner_id or not lease_token:
            raise ValueError(
                "market_collection_fence_invalid: complete ownership is required"
            )
        if fence_kind == "stream":
            try:
                definition_generation = int(
                    collection_fence.get("definition_generation")
                )
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    "market_collection_fence_invalid: stream definition "
                    "generation is required"
                ) from exc
            ownership = session.execute(
                text(
                    """
                    SELECT definitions.source_id, definitions.series_id,
                           definitions.generation AS definition_generation,
                           leases.owner_id, leases.token_hash,
                           leases.lease_generation,
                           leases.expires_at > now() AS lease_current
                    FROM market.stream_definitions AS definitions
                    JOIN market.stream_lease_state AS leases
                      ON leases.definition_id = definitions.id
                    WHERE definitions.id = :definition_id
                    FOR UPDATE OF leases
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().first()
            expected_token_hash = hashlib.sha256(
                lease_token.encode("utf-8")
            ).hexdigest()
            if (
                ownership is None
                or int(ownership["source_id"]) != fenced_source_id
                or int(ownership["series_id"]) != series_id
                or int(ownership["definition_generation"])
                != definition_generation
                or str(ownership["owner_id"] or "") != owner_id
                or str(ownership["token_hash"] or "") != expected_token_hash
                or int(ownership["lease_generation"]) != lease_generation
                or not bool(ownership["lease_current"])
            ):
                raise RuntimeError(
                    "market_collection_ownership_lost: rejected stale stream "
                    "fact mutation"
                )
            return
        if fence_kind != "collection":
            raise ValueError(
                "market_collection_fence_invalid: unsupported fence_kind="
                f"{fence_kind}"
            )
        ownership = session.execute(
            text(
                """
                SELECT source_id, series_id, lease_owner, lease_token_hash,
                       lease_generation, lease_expires_at > now() AS lease_current
                FROM market.collection_definitions
                WHERE id = :definition_id
                FOR UPDATE
                """
            ),
            {"definition_id": definition_id},
        ).mappings().first()
        expected_token_hash = hashlib.sha256(
            lease_token.encode("utf-8")
        ).hexdigest()
        if (
            ownership is None
            or int(ownership["source_id"]) != fenced_source_id
            or int(ownership["series_id"]) != series_id
            or str(ownership["lease_owner"] or "") != owner_id
            or str(ownership["lease_token_hash"] or "") != expected_token_hash
            or int(ownership["lease_generation"]) != lease_generation
            or not bool(ownership["lease_current"])
        ):
            raise RuntimeError(
                "market_collection_ownership_lost: rejected stale fact mutation"
            )

    def _ingest_open_interest_rows(
        self,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[OpenInterestFact],
        provenance: Mapping[str, Any],
        allow_corrections: bool,
        collection_fence: Optional[Mapping[str, Any]],
    ) -> IngestionOutcome:
        with db.session() as session:
            session.execute(
                text("SELECT pg_advisory_xact_lock(:series_id)"),
                {"series_id": series_id},
            )
            self._assert_collection_fence(
                session,
                series_id=series_id,
                collection_fence=collection_fence,
            )
            session.execute(
                text(
                    """
                    CREATE TEMP TABLE market_open_interest_ingest_stage (
                        sample_time timestamptz PRIMARY KEY,
                        sample_time_method varchar(64) NOT NULL,
                        open_interest double precision NOT NULL,
                        unit varchar(32) NOT NULL,
                        source_published_at timestamptz,
                        received_at timestamptz,
                        accepted_at timestamptz NOT NULL,
                        known_at timestamptz NOT NULL,
                        known_at_method varchar(64) NOT NULL,
                        row_hash varchar(64) NOT NULL
                    ) ON COMMIT DROP
                    """
                )
            )
            session.execute(
                text(
                    """
                    INSERT INTO market_open_interest_ingest_stage (
                        sample_time, sample_time_method, open_interest, unit,
                        source_published_at, received_at, accepted_at, known_at,
                        known_at_method, row_hash
                    ) VALUES (
                        :sample_time, :sample_time_method, :value, :unit,
                        :source_published_at, :received_at, :accepted_at, :known_at,
                        :known_at_method, :row_hash
                    )
                    """
                ),
                [fact.to_dict() for fact in rows],
            )
            conflicting_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM market_open_interest_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.row_hash
                            FROM market.open_interest_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.sample_time = stage.sample_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            if conflicting_count and not allow_corrections:
                raise RuntimeError(
                    "market_data_correction_rejected: immutable consumer path cannot "
                    f"accept {conflicting_count} changed open-interest sample(s) "
                    f"series_id={series_id}"
                )
            inserted = session.execute(
                text(
                    """
                    INSERT INTO market.open_interest_versions (
                        series_id, sample_time, revision, ingestion_run_id,
                        sample_time_method, open_interest, unit,
                        source_published_at, received_at, accepted_at, known_at,
                        known_at_method, provenance, row_hash
                    )
                    SELECT
                        :series_id,
                        stage.sample_time,
                        COALESCE(latest.revision, 0) + 1,
                        :run_id,
                        stage.sample_time_method,
                        stage.open_interest,
                        stage.unit,
                        stage.source_published_at,
                        stage.received_at,
                        stage.accepted_at,
                        stage.known_at,
                        stage.known_at_method,
                        CAST(:provenance AS jsonb),
                        stage.row_hash
                    FROM market_open_interest_ingest_stage AS stage
                    LEFT JOIN LATERAL (
                        SELECT current.revision, current.row_hash
                        FROM market.open_interest_versions AS current
                        WHERE current.series_id = :series_id
                          AND current.sample_time = stage.sample_time
                        ORDER BY current.revision DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                    WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                    RETURNING revision, market_commit_seq
                    """
                ),
                {
                    "series_id": series_id,
                    "run_id": run_id,
                    "provenance": _json_text(provenance),
                },
            ).mappings().all()
            new_count = sum(1 for row in inserted if int(row["revision"]) == 1)
            corrected_count = sum(
                1 for row in inserted if int(row["revision"]) > 1
            )
            noop_count = len(rows) - len(inserted)
            max_commit_seq = int(
                session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(latest.market_commit_seq), 0)
                        FROM market_open_interest_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.market_commit_seq
                            FROM market.open_interest_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.sample_time = stage.sample_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            session.execute(
                text(
                    """
                    UPDATE market.ingestion_runs
                    SET status = 'completed', finished_at = now(),
                        inserted_count = :inserted_count,
                        corrected_count = :corrected_count,
                        noop_count = :noop_count
                    WHERE id = :run_id AND status = 'running'
                    """
                ),
                {
                    "run_id": run_id,
                    "inserted_count": new_count,
                    "corrected_count": corrected_count,
                    "noop_count": noop_count,
                },
            )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=new_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

    def _ingest_funding_rate_rows(
        self,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[FundingRateFact],
        provenance: Mapping[str, Any],
        allow_corrections: bool,
        collection_fence: Optional[Mapping[str, Any]],
    ) -> IngestionOutcome:
        with db.session() as session:
            session.execute(
                text("SELECT pg_advisory_xact_lock(:series_id)"),
                {"series_id": series_id},
            )
            self._assert_collection_fence(
                session,
                series_id=series_id,
                collection_fence=collection_fence,
            )
            session.execute(
                text(
                    """
                    CREATE TEMP TABLE market_funding_rate_ingest_stage (
                        sample_time timestamptz PRIMARY KEY,
                        sample_time_method varchar(64) NOT NULL,
                        funding_rate double precision NOT NULL,
                        funding_time timestamptz NOT NULL,
                        funding_interval_seconds integer NOT NULL,
                        unit varchar(32) NOT NULL,
                        source_published_at timestamptz,
                        received_at timestamptz,
                        accepted_at timestamptz NOT NULL,
                        known_at timestamptz NOT NULL,
                        known_at_method varchar(64) NOT NULL,
                        row_hash varchar(64) NOT NULL
                    ) ON COMMIT DROP
                    """
                )
            )
            session.execute(
                text(
                    """
                    INSERT INTO market_funding_rate_ingest_stage (
                        sample_time, sample_time_method, funding_rate,
                        funding_time, funding_interval_seconds, unit,
                        source_published_at, received_at, accepted_at, known_at,
                        known_at_method, row_hash
                    ) VALUES (
                        :sample_time, :sample_time_method, :rate,
                        :funding_time, :interval_seconds, :unit,
                        :source_published_at, :received_at, :accepted_at, :known_at,
                        :known_at_method, :row_hash
                    )
                    """
                ),
                [fact.to_dict() for fact in rows],
            )
            conflicting_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM market_funding_rate_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.row_hash
                            FROM market.funding_rate_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.sample_time = stage.sample_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            if conflicting_count and not allow_corrections:
                raise RuntimeError(
                    "market_data_correction_rejected: immutable consumer path cannot "
                    f"accept {conflicting_count} changed funding-rate sample(s) "
                    f"series_id={series_id}"
                )
            inserted = session.execute(
                text(
                    """
                    INSERT INTO market.funding_rate_versions (
                        series_id, sample_time, revision, ingestion_run_id,
                        sample_time_method, funding_rate, funding_time,
                        funding_interval_seconds, unit, source_published_at,
                        received_at, accepted_at, known_at, known_at_method,
                        provenance, row_hash
                    )
                    SELECT
                        :series_id,
                        stage.sample_time,
                        COALESCE(latest.revision, 0) + 1,
                        :run_id,
                        stage.sample_time_method,
                        stage.funding_rate,
                        stage.funding_time,
                        stage.funding_interval_seconds,
                        stage.unit,
                        stage.source_published_at,
                        stage.received_at,
                        stage.accepted_at,
                        stage.known_at,
                        stage.known_at_method,
                        CAST(:provenance AS jsonb),
                        stage.row_hash
                    FROM market_funding_rate_ingest_stage AS stage
                    LEFT JOIN LATERAL (
                        SELECT current.revision, current.row_hash
                        FROM market.funding_rate_versions AS current
                        WHERE current.series_id = :series_id
                          AND current.sample_time = stage.sample_time
                        ORDER BY current.revision DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                    WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                    RETURNING revision, market_commit_seq
                    """
                ),
                {
                    "series_id": series_id,
                    "run_id": run_id,
                    "provenance": _json_text(provenance),
                },
            ).mappings().all()
            new_count = sum(1 for row in inserted if int(row["revision"]) == 1)
            corrected_count = sum(
                1 for row in inserted if int(row["revision"]) > 1
            )
            noop_count = len(rows) - len(inserted)
            max_commit_seq = int(
                session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(latest.market_commit_seq), 0)
                        FROM market_funding_rate_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.market_commit_seq
                            FROM market.funding_rate_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.sample_time = stage.sample_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            session.execute(
                text(
                    """
                    UPDATE market.ingestion_runs
                    SET status = 'completed', finished_at = now(),
                        inserted_count = :inserted_count,
                        corrected_count = :corrected_count,
                        noop_count = :noop_count
                    WHERE id = :run_id AND status = 'running'
                    """
                ),
                {
                    "run_id": run_id,
                    "inserted_count": new_count,
                    "corrected_count": corrected_count,
                    "noop_count": noop_count,
                },
            )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=new_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

    def _ingest_candle_rows(
        self,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[CandleFact],
        allow_corrections: bool,
    ) -> IngestionOutcome:
        with db.session() as session:
            session.execute(
                text("SELECT pg_advisory_xact_lock(:series_id)"),
                {"series_id": series_id},
            )
            session.execute(
                text(
                    """
                    CREATE TEMP TABLE market_candle_ingest_stage (
                        candle_open_time timestamptz PRIMARY KEY,
                        candle_close_time timestamptz NOT NULL,
                        open double precision NOT NULL,
                        high double precision NOT NULL,
                        low double precision NOT NULL,
                        close double precision NOT NULL,
                        volume double precision,
                        trade_count bigint,
                        source_published_at timestamptz,
                        received_at timestamptz,
                        accepted_at timestamptz NOT NULL,
                        known_at timestamptz NOT NULL,
                        known_at_method varchar(64) NOT NULL,
                        row_hash varchar(64) NOT NULL
                    ) ON COMMIT DROP
                    """
                )
            )
            stage_rows = [fact.to_dict() for fact in rows]
            session.execute(
                text(
                    """
                    INSERT INTO market_candle_ingest_stage (
                        candle_open_time, candle_close_time, open, high, low, close,
                        volume, trade_count, source_published_at, received_at,
                        accepted_at, known_at, known_at_method, row_hash
                    ) VALUES (
                        :open_time, :close_time, :open, :high, :low, :close,
                        :volume, :trade_count, :source_published_at, :received_at,
                        :accepted_at, :known_at, :known_at_method, :row_hash
                    )
                    """
                ),
                stage_rows,
            )
            conflicting_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM market_candle_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.row_hash
                            FROM market.candle_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.candle_open_time = stage.candle_open_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            if conflicting_count and not allow_corrections:
                raise RuntimeError(
                    "market_data_correction_rejected: immutable consumer path cannot "
                    f"accept {conflicting_count} changed closed candle(s) "
                    f"series_id={series_id}"
                )
            inserted = session.execute(
                text(
                    """
                    INSERT INTO market.candle_versions (
                        series_id, candle_open_time, revision, ingestion_run_id,
                        candle_close_time, open, high, low, close, volume, trade_count,
                        source_published_at, received_at, accepted_at, known_at,
                        known_at_method, row_hash
                    )
                    SELECT
                        :series_id,
                        stage.candle_open_time,
                        COALESCE(latest.revision, 0) + 1,
                        :run_id,
                        stage.candle_close_time,
                        stage.open,
                        stage.high,
                        stage.low,
                        stage.close,
                        stage.volume,
                        stage.trade_count,
                        stage.source_published_at,
                        stage.received_at,
                        stage.accepted_at,
                        stage.known_at,
                        stage.known_at_method,
                        stage.row_hash
                    FROM market_candle_ingest_stage AS stage
                    LEFT JOIN LATERAL (
                        SELECT current.revision, current.row_hash
                        FROM market.candle_versions AS current
                        WHERE current.series_id = :series_id
                          AND current.candle_open_time = stage.candle_open_time
                        ORDER BY current.revision DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                    WHERE latest.row_hash IS DISTINCT FROM stage.row_hash
                    RETURNING revision, market_commit_seq
                    """
                ),
                {"series_id": series_id, "run_id": run_id},
            ).mappings().all()
            new_count = sum(1 for row in inserted if int(row["revision"]) == 1)
            corrected_count = sum(1 for row in inserted if int(row["revision"]) > 1)
            noop_count = len(rows) - len(inserted)
            max_commit_seq = int(
                session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(latest.market_commit_seq), 0)
                        FROM market_candle_ingest_stage AS stage
                        JOIN LATERAL (
                            SELECT current.market_commit_seq
                            FROM market.candle_versions AS current
                            WHERE current.series_id = :series_id
                              AND current.candle_open_time = stage.candle_open_time
                            ORDER BY current.revision DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        """
                    ),
                    {"series_id": series_id},
                ).scalar_one()
            )
            session.execute(
                text(
                    """
                    UPDATE market.ingestion_runs
                    SET status = 'completed', finished_at = now(),
                        inserted_count = :inserted_count,
                        corrected_count = :corrected_count,
                        noop_count = :noop_count
                    WHERE id = :run_id AND status = 'running'
                    """
                ),
                {
                    "run_id": run_id,
                    "inserted_count": new_count,
                    "corrected_count": corrected_count,
                    "noop_count": noop_count,
                },
            )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=new_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

    def _fail_ingestion_run(self, run_id: str, exc: Exception) -> None:
        with db.session() as session:
            session.execute(
                text(
                    """
                    UPDATE market.ingestion_runs
                    SET status = 'failed', finished_at = now(), error = :error
                    WHERE id = :run_id AND status = 'running'
                    """
                ),
                {"run_id": run_id, "error": str(exc)[:4000]},
            )

    def _get_series(self, series_id: int) -> Mapping[str, Any]:
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT id, identity_key, instrument_id, fact_type,
                           timeframe_seconds, contract_version, dimensions
                    FROM market.series
                    WHERE id = :series_id
                    """
                ),
                {"series_id": int(series_id)},
            ).mappings().first()
        if row is None:
            raise ValueError(f"market_data_series_unknown: series_id={series_id}")
        return dict(row)

    @staticmethod
    def _get_source_identity(source_id: int) -> SourceIdentity:
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT id AS source_id,
                           identity_key AS source_identity_key,
                           provider AS source_provider,
                           venue AS source_venue,
                           source_kind,
                           adapter_version AS source_adapter_version
                    FROM market.sources
                    WHERE id = :source_id
                    """
                ),
                {"source_id": int(source_id)},
            ).mappings().first()
        if row is None:
            raise ValueError(
                f"market_data_source_unknown: source_id={int(source_id)}"
            )
        return _canonical_source(row)

    def get_source_identity(self, source_id: int) -> SourceIdentity:
        """Resolve the canonical provenance identity for an accepted source."""

        return self._get_source_identity(source_id)

    @staticmethod
    def _canonical_source_for_run(session, run_id: str) -> tuple[int, SourceIdentity]:
        row = session.execute(
            text(
                """
                SELECT sources.id AS source_id,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM market.ingestion_runs AS runs
                JOIN market.sources AS sources ON sources.id = runs.source_id
                WHERE runs.id = :run_id
                """
            ),
            {"run_id": run_id},
        ).mappings().one()
        return int(row["source_id"]), _canonical_source(row)

    def _ingest_canonical_rows(
        self,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[CanonicalFact],
        allow_corrections: bool,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        with db.session() as session:
            return self._ingest_canonical_rows_with_session(
                session,
                run_id=run_id,
                series_id=series_id,
                rows=rows,
                allow_corrections=allow_corrections,
                collection_fence=collection_fence,
            )

    def _ingest_canonical_rows_with_session(
        self,
        session,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[CanonicalFact],
        allow_corrections: bool,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        inserted_count = 0
        corrected_count = 0
        noop_count = 0
        max_commit_seq = 0
        session.execute(
            text("SELECT pg_advisory_xact_lock(:series_id)"),
            {"series_id": series_id},
        )
        self._assert_collection_fence(
            session,
            series_id=series_id,
            collection_fence=collection_fence,
        )
        source_id, source = self._canonical_source_for_run(session, run_id)
        for fact in rows:
            if fact.source.identity_key != source.identity_key:
                raise ValueError(
                    "market_data_ingest_invalid: canonical Fact source disagrees "
                    f"with ingestion run run_id={run_id}"
                )
            latest = session.execute(
                text(
                    """
                    SELECT revision, row_hash, market_commit_seq
                    FROM market.fact_versions
                    WHERE series_id = :series_id
                      AND observation_key = :observation_key
                    ORDER BY revision DESC
                    LIMIT 1
                    """
                ),
                {
                    "series_id": series_id,
                    "observation_key": fact.observation_key,
                },
            ).mappings().first()
            if latest is not None:
                max_commit_seq = max(
                    max_commit_seq, int(latest["market_commit_seq"])
                )
                if str(latest["row_hash"]) == fact.row_hash:
                    noop_count += 1
                    continue
                if not allow_corrections:
                    raise RuntimeError(
                        "market_data_correction_rejected: immutable consumer path "
                        "cannot accept changed canonical Fact "
                        f"series_id={series_id} "
                        f"observation_key={fact.observation_key}"
                    )
            revision = 1 if latest is None else int(latest["revision"]) + 1
            version_id = build_fact_version_id(
                series_id=series_id,
                observation_key=fact.observation_key,
                revision=revision,
                row_hash=fact.row_hash,
            )
            commit_seq = int(
                session.execute(
                    text(
                        """
                        INSERT INTO market.fact_versions (
                            id, series_id, observation_key, revision,
                            source_id, ingestion_run_id, fact_type,
                            payload_schema_id, payload_contract_hash,
                            observation_time, observation_time_method,
                            source_published_at, received_at, accepted_at,
                            known_at, known_at_method, transformation_id,
                            external_event_key, external_event_group_key,
                            external_event_component_key, state, payload,
                            payload_hash, material_hash,
                            provenance_schema_id, provenance, provenance_hash,
                            quality_schema_id, quality, quality_hash, row_hash
                        ) VALUES (
                            :id, :series_id, :observation_key, :revision,
                            :source_id, :ingestion_run_id, :fact_type,
                            :payload_schema_id, :payload_contract_hash,
                            :observation_time, :observation_time_method,
                            :source_published_at, :received_at, :accepted_at,
                            :known_at, :known_at_method, :transformation_id,
                            :external_event_key, :external_event_group_key,
                            :external_event_component_key, :state,
                            CAST(:payload AS jsonb), :payload_hash,
                            :material_hash, :provenance_schema_id,
                            CAST(:provenance AS jsonb), :provenance_hash,
                            :quality_schema_id, CAST(:quality AS jsonb),
                            :quality_hash, :row_hash
                        )
                        RETURNING market_commit_seq
                        """
                    ),
                    {
                        "id": version_id,
                        "series_id": series_id,
                        "observation_key": fact.observation_key,
                        "revision": revision,
                        "source_id": source_id,
                        "ingestion_run_id": run_id,
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
                        "payload": _json_text(fact.payload),
                        "payload_hash": fact.payload_hash,
                        "material_hash": fact.material_hash,
                        "provenance_schema_id": fact.provenance_schema_id,
                        "provenance": _json_text(fact.provenance),
                        "provenance_hash": fact.provenance_hash,
                        "quality_schema_id": fact.quality_schema_id,
                        "quality": _json_text(fact.quality),
                        "quality_hash": fact.quality_hash,
                        "row_hash": fact.row_hash,
                    },
                ).scalar_one()
            )
            max_commit_seq = max(max_commit_seq, commit_seq)
            if latest is None:
                inserted_count += 1
            else:
                corrected_count += 1
        if max_commit_seq == 0:
            max_commit_seq = self._current_commit_seq_with_session(session)
        session.execute(
            text(
                """
                UPDATE market.ingestion_runs
                SET status = 'completed', finished_at = now(),
                    inserted_count = :inserted_count,
                    corrected_count = :corrected_count,
                    noop_count = :noop_count
                WHERE id = :run_id AND status = 'running'
                """
            ),
            {
                "run_id": run_id,
                "inserted_count": inserted_count,
                "corrected_count": corrected_count,
                "noop_count": noop_count,
            },
        )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=inserted_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

    @staticmethod
    def _read_canonical_rows_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
        source_identity_keys: Sequence[str] = (),
        latest_only: bool = True,
        include_invalidated: bool = False,
        causal_at_interval_close: bool = False,
    ) -> list[Mapping[str, Any]]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "versions.series_id = :series_id",
            "versions.observation_time >= :start",
            "versions.observation_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("versions.market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("versions.known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        if causal_at_interval_close:
            predicates.append(
                "versions.known_at <= "
                "market.canonical_fact_utc_timestamp(versions.payload->>'close_time')"
            )
        allowed_sources = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if allowed_sources:
            predicates.append("sources.identity_key = ANY(:source_identity_keys)")
            params["source_identity_keys"] = allowed_sources
        select_prefix = (
            "SELECT DISTINCT ON (versions.observation_key)"
            if latest_only
            else "SELECT"
        )
        revision_order = (
            "versions.observation_key, versions.revision DESC"
            if latest_only
            else "versions.observation_key, versions.revision"
        )
        state_predicate = "" if include_invalidated else "WHERE visible.state = 'active'"
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    {select_prefix}
                           versions.*,
                           sources.identity_key AS source_identity_key,
                           sources.provider AS source_provider,
                           sources.venue AS source_venue,
                           sources.source_kind,
                           sources.adapter_version AS source_adapter_version,
                           series.dimensions AS series_dimensions
                    FROM market.fact_versions AS versions
                    JOIN market.sources AS sources ON sources.id = versions.source_id
                    JOIN market.series AS series ON series.id = versions.series_id
                    WHERE {' AND '.join(predicates)}
                    ORDER BY {revision_order}
                )
                SELECT visible.*
                FROM visible
                {state_predicate}
                ORDER BY visible.observation_time,
                         visible.observation_key, visible.revision
                """
            ),
            params,
        ).mappings().all()
        return [dict(row) for row in rows]

    def read_facts(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[CanonicalFactRecord]:
        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                source_identity_keys=source_identity_keys,
            )
        return [_canonical_row_to_record(row) for row in rows]

    @staticmethod
    def _read_candles_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
        source_identity_keys: Sequence[str] = (),
        causal_at_interval_close: bool = False,
    ) -> list[CandleRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "versions.series_id = :series_id",
            "versions.candle_open_time >= :start",
            "versions.candle_open_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("versions.market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("versions.known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        if causal_at_interval_close:
            predicates.append("versions.known_at <= versions.candle_close_time")
        allowed_sources = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if allowed_sources:
            predicates.append("sources.identity_key = ANY(:source_identity_keys)")
            params["source_identity_keys"] = allowed_sources
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (versions.candle_open_time)
                           versions.*,
                           sources.identity_key AS source_identity_key,
                           sources.provider AS source_provider,
                           sources.venue AS source_venue,
                           sources.source_kind,
                           sources.adapter_version AS source_adapter_version
                    FROM market.candle_versions AS versions
                    JOIN market.ingestion_runs AS runs
                      ON runs.id = versions.ingestion_run_id
                    JOIN market.sources AS sources
                      ON sources.id = runs.source_id
                    WHERE {' AND '.join(predicates)}
                    ORDER BY versions.candle_open_time, versions.revision DESC
                )
                SELECT visible.*
                FROM visible
                ORDER BY visible.candle_open_time
                """
            ),
            params,
        ).mappings().all()
        return [_row_to_record(row) for row in rows]

    def read_candles(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[CandleRecord]:
        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                source_identity_keys=source_identity_keys,
            )
        return [_canonical_to_candle_record(row) for row in rows]

    @staticmethod
    def _read_open_interest_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
        source_identity_keys: Sequence[str] = (),
    ) -> list[OpenInterestRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "versions.series_id = :series_id",
            "versions.sample_time >= :start",
            "versions.sample_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("versions.market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("versions.known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        allowed_sources = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if allowed_sources:
            predicates.append("sources.identity_key = ANY(:source_identity_keys)")
            params["source_identity_keys"] = allowed_sources
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (versions.sample_time)
                           versions.*,
                           sources.identity_key AS source_identity_key,
                           sources.provider AS source_provider,
                           sources.venue AS source_venue,
                           sources.source_kind,
                           sources.adapter_version AS source_adapter_version
                    FROM market.open_interest_versions AS versions
                    JOIN market.ingestion_runs AS runs
                      ON runs.id = versions.ingestion_run_id
                    JOIN market.sources AS sources
                      ON sources.id = runs.source_id
                    WHERE {' AND '.join(predicates)}
                    ORDER BY versions.sample_time, versions.revision DESC
                )
                SELECT visible.*
                FROM visible
                ORDER BY visible.sample_time
                """
            ),
            params,
        ).mappings().all()
        return [_row_to_open_interest_record(row) for row in rows]

    def read_open_interest(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[OpenInterestRecord]:
        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                source_identity_keys=source_identity_keys,
            )
        return [_canonical_to_open_interest_record(row) for row in rows]

    @staticmethod
    def _read_funding_rates_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
        source_identity_keys: Sequence[str] = (),
    ) -> list[FundingRateRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "versions.series_id = :series_id",
            "versions.sample_time >= :start",
            "versions.sample_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("versions.market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("versions.known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        allowed_sources = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if allowed_sources:
            predicates.append("sources.identity_key = ANY(:source_identity_keys)")
            params["source_identity_keys"] = allowed_sources
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (versions.sample_time)
                           versions.*,
                           sources.identity_key AS source_identity_key,
                           sources.provider AS source_provider,
                           sources.venue AS source_venue,
                           sources.source_kind,
                           sources.adapter_version AS source_adapter_version
                    FROM market.funding_rate_versions AS versions
                    JOIN market.ingestion_runs AS runs
                      ON runs.id = versions.ingestion_run_id
                    JOIN market.sources AS sources
                      ON sources.id = runs.source_id
                    WHERE {' AND '.join(predicates)}
                    ORDER BY versions.sample_time, versions.revision DESC
                )
                SELECT visible.*
                FROM visible
                ORDER BY visible.sample_time
                """
            ),
            params,
        ).mappings().all()
        return [_row_to_funding_rate_record(row) for row in rows]

    def read_funding_rates(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[FundingRateRecord]:
        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                source_identity_keys=source_identity_keys,
            )
        return [_canonical_to_funding_rate_record(row) for row in rows]

    @staticmethod
    def _read_numeric_facts_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
        latest_only: bool,
        include_invalidated: bool,
        source_identity_keys: Sequence[str] = (),
    ) -> list[NumericFactRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        causal_predicates = ["versions.series_id = :series_id"]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            causal_predicates.append("versions.market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            causal_predicates.append("versions.known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        allowed_sources = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if allowed_sources:
            causal_predicates.append("sources.identity_key = ANY(:source_identity_keys)")
            params["source_identity_keys"] = allowed_sources
        state_predicate = "" if include_invalidated else "AND visible.state = 'active'"
        if latest_only:
            visible_sql = f"""
                SELECT DISTINCT ON (versions.source_event_key)
                       versions.*,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM market.numeric_fact_versions AS versions
                JOIN market.ingestion_runs AS runs
                  ON runs.id = versions.ingestion_run_id
                JOIN market.sources AS sources
                  ON sources.id = runs.source_id
                WHERE {' AND '.join(causal_predicates)}
                ORDER BY versions.source_event_key, versions.revision DESC
            """
        else:
            visible_sql = f"""
                SELECT versions.*,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM market.numeric_fact_versions AS versions
                JOIN market.ingestion_runs AS runs
                  ON runs.id = versions.ingestion_run_id
                JOIN market.sources AS sources
                  ON sources.id = runs.source_id
                WHERE {' AND '.join(causal_predicates)}
            """
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    {visible_sql}
                )
                SELECT visible.*
                FROM visible
                WHERE visible.effective_at >= :start
                  AND visible.effective_at < :end
                  {state_predicate}
                ORDER BY visible.effective_at, visible.source_event_key,
                         visible.revision
                """
            ),
            params,
        ).mappings().all()
        return [_row_to_numeric_fact_record(row) for row in rows]

    def read_numeric_facts(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[NumericFactRecord]:
        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                latest_only=True,
                include_invalidated=False,
                source_identity_keys=source_identity_keys,
            )
        return [_canonical_to_numeric_record(row) for row in rows]

    def read_numeric_fact_revisions(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
    ) -> list[NumericFactRecord]:
        """Return retained active and invalidated revisions for audit/reorg repair."""

        with db.session() as session:
            rows = self._read_canonical_rows_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
                latest_only=False,
                include_invalidated=True,
            )
        return [_canonical_to_numeric_record(row) for row in rows]

    def record_gap_evidence(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        classification: str,
        expected_count: int,
        observed_count: int,
        evidence: Mapping[str, Any],
        source_id: Optional[int] = None,
        ingestion_run_id: Optional[str] = None,
        detected_as_of_commit_seq: Optional[int] = None,
    ) -> str:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        classification = str(classification or "").strip().lower()
        if not classification:
            raise ValueError("market_gap_evidence_invalid: classification is required")
        with db.session() as session:
            resolved_source_id = int(source_id) if source_id is not None else None
            if resolved_source_id is not None and resolved_source_id <= 0:
                raise ValueError("market_gap_evidence_invalid: source_id must be positive")
            if ingestion_run_id:
                run_source_id = session.execute(
                    text("SELECT source_id FROM market.ingestion_runs WHERE id = :run_id"),
                    {"run_id": ingestion_run_id},
                ).scalar_one_or_none()
                if run_source_id is None:
                    raise ValueError(
                        "market_gap_evidence_invalid: ingestion run does not exist"
                    )
                if (
                    resolved_source_id is not None
                    and resolved_source_id != int(run_source_id)
                ):
                    raise ValueError(
                        "market_gap_evidence_invalid: source differs from ingestion run"
                    )
                resolved_source_id = int(run_source_id)
            if resolved_source_id is not None:
                source_exists = session.execute(
                    text("SELECT 1 FROM market.sources WHERE id = :source_id"),
                    {"source_id": resolved_source_id},
                ).scalar_one_or_none()
                if source_exists is None:
                    raise ValueError(
                        "market_gap_evidence_invalid: source does not exist"
                    )
            evidence_source_id = (evidence or {}).get("source_id")
            if (
                evidence_source_id not in (None, "")
                and resolved_source_id != int(evidence_source_id)
            ):
                raise ValueError(
                    "market_gap_evidence_invalid: evidence source identity disagrees"
                )
            payload = {
                "series_id": request.series_id,
                "source_id": resolved_source_id,
                "start": _iso(request.start),
                "end": _iso(request.end),
                "classification": classification,
                "expected_count": int(expected_count),
                "observed_count": int(observed_count),
                "evidence": dict(evidence),
            }
            evidence_hash = build_quality_hash([payload])
            watermark = detected_as_of_commit_seq
            if watermark is None:
                watermark = self._current_commit_seq_with_session(session)
            session.execute(
                text(
                    """
                    INSERT INTO market.gap_evidence (
                        series_id, source_id, ingestion_run_id, start_time, end_time,
                        classification, expected_count, observed_count,
                        detected_as_of_commit_seq, evidence_hash, evidence
                    ) VALUES (
                        :series_id, :source_id, :ingestion_run_id, :start_time, :end_time,
                        :classification, :expected_count, :observed_count,
                        :watermark, :evidence_hash, CAST(:evidence AS jsonb)
                    )
                    ON CONFLICT (
                        series_id, start_time, end_time, evidence_hash
                    ) DO NOTHING
                    """
                ),
                {
                    "series_id": request.series_id,
                    "source_id": resolved_source_id,
                    "ingestion_run_id": ingestion_run_id,
                    "start_time": request.start,
                    "end_time": request.end,
                    "classification": classification,
                    "expected_count": int(expected_count),
                    "observed_count": int(observed_count),
                    "watermark": int(watermark),
                    "evidence_hash": evidence_hash,
                    "evidence": _json_text(evidence),
                },
            )
        return evidence_hash

    @staticmethod
    def _gap_evidence_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: int,
        known_at_lte: Optional[datetime] = None,
        include_source_identity: bool = False,
    ) -> list[dict[str, Any]]:
        rows = session.execute(
            text(
                """
                SELECT gaps.start_time, gaps.end_time, gaps.classification,
                       gaps.expected_count, gaps.observed_count,
                       gaps.detected_as_of_commit_seq, gaps.evidence_hash,
                       gaps.evidence, gaps.created_at, gaps.ingestion_run_id,
                       gaps.source_id AS recorded_source_id,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM market.gap_evidence AS gaps
                LEFT JOIN market.ingestion_runs AS runs
                  ON runs.id = gaps.ingestion_run_id
                LEFT JOIN market.sources AS sources
                  ON sources.id = COALESCE(gaps.source_id, runs.source_id)
                WHERE gaps.series_id = :series_id
                  AND gaps.end_time > :start
                  AND gaps.start_time < :end
                  AND gaps.detected_as_of_commit_seq <= :watermark
                  AND (:known_at_lte IS NULL OR gaps.created_at <= :known_at_lte)
                ORDER BY gaps.start_time, gaps.end_time, gaps.evidence_hash
                """
            ),
            {
                "series_id": series_id,
                "start": start,
                "end": end,
                "watermark": as_of_commit_seq,
                "known_at_lte": known_at_lte,
            },
        ).mappings().all()
        projected = [
            {
                "start": _iso(row["start_time"]),
                "end": _iso(row["end_time"]),
                "classification": str(row["classification"]),
                "expected_count": int(row["expected_count"]),
                "observed_count": int(row["observed_count"]),
                "detected_as_of_commit_seq": int(row["detected_as_of_commit_seq"]),
                "evidence_hash": str(row["evidence_hash"]),
                "evidence": dict(row["evidence"] or {}),
                "detected_at": _iso(row["created_at"]),
            }
            for row in rows
        ]
        if include_source_identity:
            for item, row in zip(projected, rows):
                item["ingestion_run_id"] = row.get("ingestion_run_id")
                item["source_id"] = row.get("recorded_source_id")
                item["source_identity_key"] = row.get("source_identity_key")
                item["source"] = (
                    {
                        "provider": row.get("source_provider"),
                        "venue": row.get("source_venue"),
                        "source_kind": row.get("source_kind"),
                        "adapter_version": row.get("source_adapter_version"),
                    }
                    if row.get("source_identity_key")
                    else None
                )
        return projected

    def list_gap_evidence(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        include_source_identity: bool = False,
    ) -> list[dict[str, Any]]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        with db.session() as session:
            watermark = as_of_commit_seq
            if watermark is None:
                watermark = self._current_commit_seq_with_session(session)
            return self._gap_evidence_with_session(
                session,
                series_id=request.series_id,
                start=request.start,
                end=request.end,
                as_of_commit_seq=int(watermark),
                known_at_lte=known_at_lte,
                include_source_identity=include_source_identity,
            )

    def record_acquisition_coverage(
        self,
        *,
        series_id: int,
        source_id: int,
        binding_id: str,
        manifest_hash: str,
        interface_version: str,
        confirmation_depth: int,
        start: datetime,
        end: datetime,
        source_position_start: str,
        source_position_end: str,
        source_position_head: Optional[str],
        status: str,
        evidence: Mapping[str, Any],
        ingestion_run_id: Optional[str] = None,
    ) -> str:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        binding_id = str(binding_id or "").strip()
        manifest_hash = str(manifest_hash or "").strip().lower()
        interface_version = str(interface_version or "").strip()
        status = str(status or "").strip().lower()
        if not binding_id or not interface_version or len(manifest_hash) != 64:
            raise ValueError("market_acquisition_coverage_invalid: binding identity")
        if status not in {"complete", "partial", "failed"}:
            raise ValueError("market_acquisition_coverage_invalid: status")
        if int(source_id) <= 0 or int(confirmation_depth) < 0:
            raise ValueError("market_acquisition_coverage_invalid: source/finality")
        positions = {
            "start": str(source_position_start or "").strip(),
            "end": str(source_position_end or "").strip(),
            "head": str(source_position_head or "").strip() or None,
        }
        if not positions["start"] or not positions["end"]:
            raise ValueError("market_acquisition_coverage_invalid: source positions")
        identity_key = _stable_hash(
            {
                "schema_version": "market.fact_acquisition_coverage.v1",
                "series_id": request.series_id,
                "source_id": int(source_id),
                "binding_id": binding_id,
                "manifest_hash": manifest_hash,
                "interface_version": interface_version,
                "confirmation_depth": int(confirmation_depth),
                "range_start": _iso(request.start),
                "range_end": _iso(request.end),
                "source_positions": positions,
                "status": status,
                "evidence": dict(evidence),
            }
        )
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.fact_acquisition_coverage (
                        identity_key, series_id, source_id, binding_id,
                        manifest_hash, interface_version, confirmation_depth,
                        range_start, range_end, source_position_start,
                        source_position_end, source_position_head, status,
                        ingestion_run_id, evidence
                    ) VALUES (
                        :identity_key, :series_id, :source_id, :binding_id,
                        :manifest_hash, :interface_version, :confirmation_depth,
                        :range_start, :range_end, :source_position_start,
                        :source_position_end, :source_position_head, :status,
                        :ingestion_run_id, CAST(:evidence AS jsonb)
                    )
                    ON CONFLICT (identity_key) DO NOTHING
                    """
                ),
                {
                    "identity_key": identity_key,
                    "series_id": request.series_id,
                    "source_id": int(source_id),
                    "binding_id": binding_id,
                    "manifest_hash": manifest_hash,
                    "interface_version": interface_version,
                    "confirmation_depth": int(confirmation_depth),
                    "range_start": request.start,
                    "range_end": request.end,
                    "source_position_start": positions["start"],
                    "source_position_end": positions["end"],
                    "source_position_head": positions["head"],
                    "status": status,
                    "ingestion_run_id": ingestion_run_id,
                    "evidence": _json_text(evidence),
                },
            )
        return identity_key

    def list_acquisition_coverage(
        self,
        *,
        series_id: int,
        source_id: int,
        binding_id: str,
        manifest_hash: str,
        interface_version: str,
        confirmation_depth: int,
        start: datetime,
        end: datetime,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "series_id = :series_id",
            "source_id = :source_id",
            "binding_id = :binding_id",
            "manifest_hash = :manifest_hash",
            "interface_version = :interface_version",
            "confirmation_depth = :confirmation_depth",
            "range_end > :start",
            "range_start < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "source_id": int(source_id),
            "binding_id": str(binding_id or "").strip(),
            "manifest_hash": str(manifest_hash or "").strip().lower(),
            "interface_version": str(interface_version or "").strip(),
            "confirmation_depth": int(confirmation_depth),
            "start": request.start,
            "end": request.end,
        }
        if status is not None:
            predicates.append("status = :status")
            params["status"] = str(status).strip().lower()
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT identity_key, series_id, source_id, binding_id,
                           manifest_hash, interface_version, confirmation_depth,
                           range_start, range_end, source_position_start,
                           source_position_end, source_position_head, status,
                           ingestion_run_id, evidence, created_at
                    FROM market.fact_acquisition_coverage
                    WHERE {' AND '.join(predicates)}
                    ORDER BY range_start, range_end, identity_key
                    """
                ),
                params,
            ).mappings().all()
        return [dict(row) for row in rows]

    def list_source_acquisition_coverage(
        self,
        *,
        series_id: int,
        source_identity_keys: Sequence[str],
        start: datetime,
        end: datetime,
        created_at_lte: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        keys = sorted(
            {str(value).strip() for value in source_identity_keys if str(value).strip()}
        )
        if not keys:
            raise ValueError(
                "market_acquisition_coverage_invalid: source identities are required"
            )
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT coverage.identity_key, coverage.series_id,
                           coverage.source_id, sources.identity_key AS source_identity_key,
                           sources.provider, sources.venue, sources.source_kind,
                           sources.adapter_version, coverage.binding_id,
                           coverage.manifest_hash, coverage.interface_version,
                           coverage.confirmation_depth, coverage.range_start,
                           coverage.range_end, coverage.source_position_start,
                           coverage.source_position_end, coverage.source_position_head,
                           coverage.status, coverage.ingestion_run_id,
                           coverage.evidence, coverage.created_at
                    FROM market.fact_acquisition_coverage AS coverage
                    JOIN market.sources AS sources ON sources.id = coverage.source_id
                    WHERE coverage.series_id = :series_id
                      AND sources.identity_key = ANY(CAST(:source_keys AS varchar[]))
                      AND coverage.range_end > :start
                      AND coverage.range_start < :end
                      AND (:created_at_lte IS NULL OR coverage.created_at <= :created_at_lte)
                    ORDER BY coverage.range_start, coverage.range_end,
                             coverage.identity_key
                    """
                ),
                {
                    "series_id": request.series_id,
                    "source_keys": keys,
                    "start": request.start,
                    "end": request.end,
                    "created_at_lte": created_at_lte,
                },
            ).mappings().all()
        return [
            {
                **dict(row),
                "range_start": _iso(row["range_start"]),
                "range_end": _iso(row["range_end"]),
                "created_at": _iso(row["created_at"]),
                "evidence": dict(row.get("evidence") or {}),
            }
            for row in rows
        ]

    def missing_acquisition_ranges(
        self,
        *,
        series_id: int,
        source_id: int,
        binding_id: str,
        manifest_hash: str,
        interface_version: str,
        confirmation_depth: int,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        coverage = self.list_acquisition_coverage(
            series_id=request.series_id,
            source_id=source_id,
            binding_id=binding_id,
            manifest_hash=manifest_hash,
            interface_version=interface_version,
            confirmation_depth=confirmation_depth,
            start=request.start,
            end=request.end,
            status="complete",
        )
        covered = sorted(
            (
                max(request.start, row["range_start"]),
                min(request.end, row["range_end"]),
            )
            for row in coverage
            if row["range_end"] > request.start and row["range_start"] < request.end
        )
        merged: list[tuple[datetime, datetime]] = []
        for range_start, range_end in covered:
            if range_end <= range_start:
                continue
            if not merged or range_start > merged[-1][1]:
                merged.append((range_start, range_end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], range_end))
        missing: list[tuple[datetime, datetime]] = []
        cursor = request.start
        for range_start, range_end in merged:
            if range_start > cursor:
                missing.append((cursor, range_start))
            cursor = max(cursor, range_end)
        if cursor < request.end:
            missing.append((cursor, request.end))
        return missing


    def read_series_records(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
    ) -> list[MarketDataRecord]:
        """Read any registered typed fact without consulting a provider."""

        with db.session() as session:
            identity = session.execute(
                text(
                    """
                    SELECT fact_type, timeframe_seconds, contract_version
                    FROM market.series WHERE id = :series_id
                    """
                ),
                {"series_id": int(series_id)},
            ).mappings().first()
        if identity is None:
            raise ValueError(f"market_data_series_unknown: series_id={series_id}")
        fact_type = str(identity["fact_type"])
        contract_version = str(identity["contract_version"])
        try:
            registered_payload = get_fact_payload_schema(contract_version)
        except ValueError:
            registered_payload = None
        if (
            registered_payload is not None
            and contract_version not in _TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS
        ):
            return list(
                self.read_facts(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                    source_identity_keys=source_identity_keys,
                )
            )
        if get_fact_contract(fact_type).uses_exact_numeric_storage:
            return list(
                self.read_numeric_facts(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                    source_identity_keys=source_identity_keys,
                )
            )
        if fact_type == CANDLE_FACT_TYPE:
            return list(
                self.read_candles(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                    source_identity_keys=source_identity_keys,
                )
            )
        if fact_type == OPEN_INTEREST_FACT_TYPE:
            return list(
                self.read_open_interest(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                    source_identity_keys=source_identity_keys,
                )
            )
        if fact_type == FUNDING_RATE_FACT_TYPE:
            return list(
                self.read_funding_rates(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                    source_identity_keys=source_identity_keys,
                )
            )
        if source_identity_keys:
            raise ValueError(
                "market_data_source_binding_unsupported: exact source filtering "
                f"is not implemented for fact_type={fact_type}"
            )
        if fact_type in {MARKET_TRADE_FACT_TYPE, TRADE_FLOW_FACT_TYPE}:
            from .market_structure import market_structure_repository

            if fact_type == MARKET_TRADE_FACT_TYPE:
                return list(
                    market_structure_repository.read_trades(
                        series_id=int(series_id),
                        start=start,
                        end=end,
                        as_of_commit_seq=as_of_commit_seq,
                        known_at_lte=known_at_lte,
                    )
                )
            timeframe = identity.get("timeframe_seconds")
            if timeframe not in {1, 60}:
                raise RuntimeError(
                    "market_dataset_contract_mismatch: trade-flow interval must be 1s or 60s"
                )
            return list(
                market_structure_repository.read_aggregates(
                    series_id=int(series_id),
                    interval_seconds=int(timeframe),
                    start=start,
                    end=end,
                    as_of_commit_seq=as_of_commit_seq,
                    known_at_lte=known_at_lte,
                )
            )
        if fact_type in {
            "market.bbo",
            "market.depth_observation",
            "market.trade_flow_feature",
            "market.futures_spot_relationship",
            "market.derivative_state",
            "market.market_response",
        }:
            from .market_structure import market_structure_repository

            return list(
                market_structure_repository.read_feature_records(
                    fact_type=fact_type,
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    known_at=known_at_lte,
                    as_of_commit_seq=as_of_commit_seq,
                )
            )
        if fact_type.startswith("market.normalized."):
            from .normalization import normalization_repository

            return list(
                normalization_repository.read_records(
                    series_id=int(series_id),
                    start=start,
                    end=end,
                    known_at_lte=known_at_lte,
                    as_of_commit_seq=as_of_commit_seq,
                )
            )
        raise RuntimeError(
            f"market_dataset_unsupported_fact: series_id={series_id} fact_type={fact_type}"
        )
    def freeze_dataset(
        self,
        requests: Sequence[DatasetSeriesRequest],
        *,
        name: Optional[str] = None,
        purpose: str = "research",
        created_by: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FrozenDataset:
        normalized = sorted(
            [DatasetSeriesRequest(item.series_id, item.start, item.end) for item in requests],
            key=lambda item: (item.series_id, item.start, item.end),
        )
        if not normalized:
            raise ValueError("market_dataset_invalid: at least one series is required")
        keys = [(item.series_id, item.start, item.end) for item in normalized]
        if len(keys) != len(set(keys)):
            raise ValueError("market_dataset_invalid: duplicate series range")
        purpose = str(purpose or "").strip().lower()
        if not purpose:
            raise ValueError("market_dataset_invalid: purpose is required")

        with db.session() as session:
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
            watermark = self._current_commit_seq_with_session(session)
            manifest_series: list[dict[str, Any]] = []
            archive_refs: dict[str, dict[str, str]] = {}
            normalization_refs: list[dict[str, Any]] = []
            for item in normalized:
                identity = session.execute(
                    text(
                        """
                        SELECT identity_key, instrument_id, fact_type,
                               timeframe_seconds, contract_version, dimensions
                        FROM market.series
                        WHERE id = :series_id
                        """
                    ),
                    {"series_id": item.series_id},
                ).mappings().first()
                if identity is None:
                    raise ValueError(
                        f"market_dataset_invalid: unknown series_id={item.series_id}"
                    )
                fact_type = str(identity["fact_type"])
                contract = get_fact_contract(fact_type)
                if not contract.dataset_eligible:
                    raise RuntimeError(
                        f"market_dataset_unsupported_fact: series_id={item.series_id} fact_type={fact_type}"
                    )
                if fact_type in {
                    CANDLE_FACT_TYPE,
                    OPEN_INTEREST_FACT_TYPE,
                    FUNDING_RATE_FACT_TYPE,
                } or contract.uses_exact_numeric_storage:
                    canonical_rows = self._read_canonical_rows_with_session(
                        session,
                        series_id=item.series_id,
                        start=item.start,
                        end=item.end,
                        as_of_commit_seq=watermark,
                        known_at_lte=None,
                        latest_only=not contract.uses_exact_numeric_storage,
                        include_invalidated=contract.uses_exact_numeric_storage,
                    )
                    if str(identity["contract_version"]) in _TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS:
                        records = _decode_core_canonical_rows(
                            fact_type, canonical_rows
                        )
                    else:
                        records = [
                            _canonical_row_to_record(row) for row in canonical_rows
                        ]
                else:
                    records = self.read_series_records(
                        series_id=item.series_id,
                        start=item.start,
                        end=item.end,
                        as_of_commit_seq=watermark,
                        known_at_lte=None,
                    )
                if not records:
                    raise RuntimeError(
                        "market_dataset_incomplete: no facts for "
                        f"series_id={item.series_id} start={_iso(item.start)} "
                        f"end={_iso(item.end)}"
                    )
                series_identity = dict(identity)
                if not dict(series_identity.get("dimensions") or {}):
                    series_identity.pop("dimensions", None)
                quality = self._gap_evidence_with_session(
                    session,
                    series_id=item.series_id,
                    start=item.start,
                    end=item.end,
                    as_of_commit_seq=watermark,
                    include_source_identity=True,
                )
                if fact_type == MARKET_TRADE_FACT_TYPE:
                    raw_record_ids = sorted(
                        {record.fact.raw_record_id for record in records}
                    )
                    archive_rows = session.execute(
                        text(
                            """
                            SELECT mappings.raw_record_id,
                                   manifests.id AS manifest_id,
                                   manifests.object_sha256, manifests.content_fingerprint,
                                   manifests.object_key, manifests.object_uri
                            FROM market.raw_archive_record_mappings AS mappings
                            JOIN market.raw_archive_manifests AS manifests
                              ON manifests.id = mappings.manifest_id
                            WHERE mappings.raw_record_id = ANY(:raw_record_ids)
                            """
                        ),
                        {"raw_record_ids": raw_record_ids},
                    ).mappings().all()
                    mapped_ids = {str(row["raw_record_id"]) for row in archive_rows}
                    if mapped_ids != set(raw_record_ids):
                        raise RuntimeError(
                            "market_dataset_archive_incomplete: one or more trades lack an acknowledged raw mapping"
                        )
                    for row in archive_rows:
                        archive_refs[str(row["manifest_id"])] = {
                            "object_sha256": str(row["object_sha256"]),
                            "content_fingerprint": str(row["content_fingerprint"]),
                            "object_key": str(row["object_key"]),
                            "object_uri": str(row["object_uri"]),
                        }
                    quality = [
                        *quality,
                        *[
                            {
                                "classification": (
                                    "covered_trade"
                                    if record.fact.coverage_interval_id
                                    else "uncovered_snapshot_delivery"
                                ),
                                "provider_product_id": record.fact.provider_product_id,
                                "provider_trade_id": record.fact.provider_trade_id,
                                "raw_record_id": record.fact.raw_record_id,
                                "coverage_interval_id": record.fact.coverage_interval_id,
                            }
                            for record in records
                        ],
                    ]
                elif fact_type == TRADE_FLOW_FACT_TYPE:
                    if any(
                        not row.fact.archive_complete
                        or not row.fact.canonicalization_complete
                        for row in records
                    ):
                        raise RuntimeError(
                            "market_dataset_archive_incomplete: trade-flow evidence is not archive and canonicalization complete"
                        )
                    coverage_ids = sorted(
                        {
                            str(record.fact.coverage_interval_id)
                            for record in records
                            if record.fact.coverage_interval_id
                        }
                    )
                    if not coverage_ids:
                        raise RuntimeError(
                            "market_dataset_archive_incomplete: trade-flow evidence lacks coverage identity"
                        )
                    archive_rows = session.execute(
                        text(
                            """
                            SELECT DISTINCT manifests.id AS manifest_id,
                                   manifests.object_sha256, manifests.content_fingerprint,
                                   manifests.object_key, manifests.object_uri
                            FROM market.stream_coverage_interval_versions AS coverage
                            JOIN market.raw_archive_manifests AS manifests
                              ON manifests.definition_id = coverage.definition_id
                             AND manifests.session_id = coverage.session_id
                            WHERE coverage.interval_id = ANY(:coverage_ids)
                            """
                        ),
                        {"coverage_ids": coverage_ids},
                    ).mappings().all()
                    if not archive_rows:
                        raise RuntimeError(
                            "market_dataset_archive_incomplete: trade-flow coverage has no acknowledged archive"
                        )
                    for row in archive_rows:
                        archive_refs[str(row["manifest_id"])] = {
                            "object_sha256": str(row["object_sha256"]),
                            "content_fingerprint": str(row["content_fingerprint"]),
                            "object_key": str(row["object_key"]),
                            "object_uri": str(row["object_uri"]),
                        }
                if records and all(
                    isinstance(record, TypedFeatureRecord) for record in records
                ):
                    archive_refs.update(_collect_typed_archive_refs(session, records=records))
                if fact_type == MARKET_TRADE_FACT_TYPE:
                    source_ids = sorted({record.source_id for record in records})
                    source_rows = session.execute(
                        text(
                            """
                            SELECT id, identity_key, provider, venue,
                                   source_kind, adapter_version
                            FROM market.sources
                            WHERE id = ANY(:source_ids)
                            """
                        ),
                        {"source_ids": source_ids},
                    ).mappings().all()
                    source_by_id = {int(row["id"]): row for row in source_rows}
                    if set(source_by_id) != set(source_ids):
                        raise RuntimeError(
                            "market_dataset_provenance_incomplete: trade source is missing"
                        )
                    source_counts = Counter(
                        str(source_by_id[record.source_id]["identity_key"])
                        for record in records
                    )
                    source_details = {
                        str(row["identity_key"]): {
                            "provider": str(row["provider"]),
                            "venue": str(row["venue"]),
                            "source_kind": str(row["source_kind"]),
                            "adapter_version": str(row["adapter_version"]),
                        }
                        for row in source_rows
                    }
                elif fact_type == TRADE_FLOW_FACT_TYPE:
                    source_key = "derived:market.trade_flow.v1"
                    source_counts = Counter({source_key: len(records)})
                    source_details = {
                        source_key: {
                            "provider": "QUANT_TRAD",
                            "venue": "",
                            "source_kind": "causal_derivation",
                            "adapter_version": "market.trade_flow.v1",
                        }
                    }
                elif records and all(
                    isinstance(record, TypedFeatureRecord) for record in records
                ):
                    source_key = f"derived:{fact_type}:{identity['contract_version']}"
                    source_counts = Counter({source_key: len(records)})
                    source_details = {
                        source_key: {
                            "provider": "QUANT_TRAD",
                            "venue": "",
                            "source_kind": "causal_derivation",
                            "adapter_version": str(identity["contract_version"]),
                        }
                    }
                else:
                    source_counts = Counter(
                        record.source_identity_key for record in records
                    )
                    source_details = {
                        record.source_identity_key: {
                            "provider": record.source.provider,
                            "venue": record.source.venue,
                            "source_kind": record.source.source_kind,
                            "adapter_version": record.source.adapter_version,
                        }
                        for record in records
                    }
                if fact_type == TRADE_FLOW_FACT_TYPE:
                    structure_quality = [
                        {
                            "classification": (
                                "complete"
                                if record.fact.aggregate_complete
                                else "incomplete_trade_coverage"
                            ),
                            "bucket_start": _iso(record.fact.bucket_start),
                            "archive_complete": record.fact.archive_complete,
                            "canonicalization_complete": record.fact.canonicalization_complete,
                            "coverage_interval_id": record.fact.coverage_interval_id,
                            "coverage_revision": record.fact.coverage_revision,
                        }
                        for record in records
                    ]
                    quality = [*quality, *structure_quality]
                if records and all(
                    isinstance(record, TypedFeatureRecord) for record in records
                ):
                    quality = [
                        *quality,
                        *[
                            {
                                "classification": str(
                                    record.quality.get("classification") or "valid"
                                ),
                                "fact_time": _iso(record_effective_time(record)),
                                "known_at": _iso(record.fact.known_at),
                                "material_hash": record.fact.material_hash,
                                "valid": record.quality.get("valid", True),
                                "reason": record.quality.get("reason"),
                            }
                            for record in records
                        ],
                    ]
                classifications = Counter(
                    str(entry["classification"]) for entry in quality
                )
                manifest_series.append(
                    {
                        "series_id": item.series_id,
                        "range_start": _iso(item.start),
                        "range_end": _iso(item.end),
                        "max_commit_seq": watermark,
                        "row_count": len(records),
                        "material_hash": _build_material_hash(
                            fact_type=fact_type,
                            series_identity=series_identity,
                            records=records,
                        ),
                        "provenance_hash": (
                            _build_canonical_provenance_hash(records)
                            if records
                            and all(
                                isinstance(record, CanonicalFactRecord)
                                for record in records
                            )
                            else build_provenance_hash(records)
                        ),
                        "source_summary": {
                            "counts": dict(sorted(source_counts.items())),
                            "sources": {key: source_details[key] for key in sorted(source_details)},
                        },
                        "quality_hash": build_quality_hash(quality),
                        "quality_summary": {
                            "evidence_count": len(quality),
                            "classifications": dict(sorted(classifications.items())),
                        },
                        "quality_evidence": [dict(row) for row in quality],
                        **(
                            {
                                "payload_schemas": [
                                    {
                                        "schema_id": schema_id,
                                        "contract_hash": next(
                                            record.fact.payload_contract_hash
                                            for record in records
                                            if record.fact.payload_schema_id
                                            == schema_id
                                        ),
                                    }
                                    for schema_id in sorted(
                                        {
                                            record.fact.payload_schema_id
                                            for record in records
                                        }
                                    )
                                ]
                            }
                            if records
                            and all(
                                isinstance(record, CanonicalFactRecord)
                                for record in records
                            )
                            else {}
                        ),
                    }
                )
                if fact_type.startswith("market.normalized."):
                    typed_records = [
                        record
                        for record in records
                        if isinstance(record, TypedFeatureRecord)
                    ]
                    spec_ids = {record.fact.spec_id for record in typed_records}
                    if len(typed_records) != len(records) or len(spec_ids) != 1:
                        raise RuntimeError(
                            "market_dataset_normalization_invalid: one typed spec per output series required"
                        )
                    source_series_ids = sorted(
                        {
                            int(source_series_id)
                            for record in typed_records
                            for source_series_id in record.fact.source_series_ids
                        }
                    )
                    if len(source_series_ids) != 1:
                        raise RuntimeError(
                            "market_dataset_normalization_invalid: v1 requires one frozen source series"
                        )
                    entry = manifest_series[-1]
                    normalization_refs.append(
                        {
                            "spec_id": next(iter(spec_ids)),
                            "output_series_id": item.series_id,
                            "range_start": _iso(item.start),
                            "range_end": _iso(item.end),
                            "input_range_start": _iso(
                                min(record.fact.input_start for record in typed_records)
                            ),
                            "input_range_end": _iso(
                                max(record.fact.input_end for record in typed_records)
                            ),
                            "input_count": 0,
                            "input_watermark": max(
                                record.fact.input_watermark
                                for record in typed_records
                            ),
                            "source_series_ids": source_series_ids,
                            "input_fingerprint": _stable_hash(
                                {
                                    "schema_version": "market.dataset_normalization_input.v1",
                                    "facts": [
                                        {
                                            "effective_at": _iso(record.fact.effective_at),
                                            "input_fingerprint": record.fact.input_fingerprint,
                                        }
                                        for record in typed_records
                                    ],
                                }
                            ),
                            "source_dataset_fingerprints": {},
                            "material_hash": entry["material_hash"],
                            "provenance_hash": entry["provenance_hash"],
                            "quality_hash": entry["quality_hash"],
                            "storage_kind": "database_snapshot",
                            "frozen_object_uri": None,
                            "frozen_object_sha256": None,
                            "row_count": len(typed_records),
                        }
                    )
            for reference in normalization_refs:
                source_fingerprints: dict[str, str] = {}
                source_row_count = 0
                for source_series_id in reference["source_series_ids"]:
                    candidates = [
                        entry
                        for entry in manifest_series
                        if int(entry["series_id"]) == int(source_series_id)
                        and str(entry["range_start"])
                        <= str(reference["input_range_start"])
                        and str(entry["range_end"])
                        > str(reference["input_range_end"])
                    ]
                    if not candidates:
                        raise RuntimeError(
                            "market_dataset_normalization_source_missing: "
                            f"output_series_id={reference['output_series_id']} "
                            f"source_series_id={source_series_id} "
                            f"required_start={reference['input_range_start']} "
                            f"required_end={reference['input_range_end']}"
                        )
                    source_entry = min(
                        candidates,
                        key=lambda entry: (
                            str(entry["range_end"]),
                            str(entry["range_start"]),
                        ),
                    )
                    source_fingerprints[str(source_series_id)] = str(
                        source_entry["material_hash"]
                    )
                    source_row_count += int(source_entry["row_count"])
                reference["source_dataset_fingerprints"] = source_fingerprints
                reference["input_count"] = source_row_count
            _verify_local_archive_objects(archive_refs)
            dataset_hash = build_dataset_identity_hash(
                dataset_series_identity_payload(entry)
                for entry in manifest_series
            )
            dataset_id = f"mds_{dataset_hash[:32]}"
            inserted_dataset_id = session.execute(
                text(
                    """
                    INSERT INTO market.datasets (
                        id, dataset_hash, name, purpose, max_commit_seq,
                        created_by, metadata
                    ) VALUES (
                        :id, :dataset_hash, :name, :purpose, :max_commit_seq,
                        :created_by, CAST(:metadata AS jsonb)
                    )
                    ON CONFLICT (dataset_hash) DO NOTHING
                    RETURNING id
                    """
                ),
                {
                    "id": dataset_id,
                    "dataset_hash": dataset_hash,
                    "name": str(name).strip() if name else None,
                    "purpose": purpose,
                    "max_commit_seq": watermark,
                    "created_by": str(created_by).strip() if created_by else None,
                    "metadata": _json_text(metadata),
                },
            ).scalar_one_or_none()
            reused_existing = inserted_dataset_id is None
            for entry in manifest_series:
                session.execute(
                    text(
                        """
                        INSERT INTO market.dataset_series (
                            dataset_id, series_id, range_start, range_end,
                            max_commit_seq, row_count, material_hash,
                            provenance_hash, source_summary, quality_hash,
                            quality_summary, quality_evidence, payload_schemas
                        ) VALUES (
                            :dataset_id, :series_id, :range_start, :range_end,
                            :max_commit_seq, :row_count, :material_hash,
                            :provenance_hash, CAST(:source_summary AS jsonb),
                            :quality_hash, CAST(:quality_summary AS jsonb),
                            CAST(:quality_evidence AS jsonb),
                            CAST(:payload_schemas AS jsonb)
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        **entry,
                        "dataset_id": dataset_id,
                        "source_summary": _json_text(entry["source_summary"]),
                        "quality_summary": _json_text(entry["quality_summary"]),
                        "quality_evidence": _json_mapping_array_text(
                            entry["quality_evidence"]
                        ),
                        "payload_schemas": _json_mapping_array_text(
                            entry.get("payload_schemas") or ()
                        ),
                    },
                )
            for manifest_id, archive_ref in sorted(archive_refs.items()):
                session.execute(
                    text(
                        """
                        INSERT INTO market.dataset_archive_refs (
                            dataset_id, raw_archive_manifest_id, inclusion_role,
                            object_sha256, content_fingerprint
                        ) VALUES (
                            :dataset_id, :manifest_id, 'source_evidence',
                            :object_sha256, :content_fingerprint
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "dataset_id": dataset_id,
                        "manifest_id": manifest_id,
                        **archive_ref,
                    },
                )
            for reference in normalization_refs:
                session.execute(
                    text(
                        """
                        INSERT INTO market.dataset_normalization_refs (
                            dataset_id, spec_id, output_series_id,
                            range_start, range_end, input_range_start,
                            input_range_end, input_count, input_watermark,
                            source_series_ids, input_fingerprint,
                            source_dataset_fingerprints, material_hash, provenance_hash,
                            quality_hash, storage_kind, frozen_object_uri,
                            frozen_object_sha256, row_count
                        ) VALUES (
                            :dataset_id, :spec_id, :output_series_id,
                            :range_start, :range_end, :input_range_start,
                            :input_range_end, :input_count, :input_watermark,
                            CAST(:source_series_ids AS jsonb), :input_fingerprint,
                            CAST(:source_dataset_fingerprints AS jsonb),
                            :material_hash, :provenance_hash,
                            :quality_hash, :storage_kind, :frozen_object_uri,
                            :frozen_object_sha256, :row_count
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "dataset_id": dataset_id,
                        **reference,
                        "source_series_ids": json.dumps(reference["source_series_ids"]),
                        "source_dataset_fingerprints": json.dumps(
                            reference["source_dataset_fingerprints"],
                            sort_keys=True,
                        ),
                    },
                )
        # Content-identical material may resolve to an already persisted dataset
        # whose immutable read watermark predates unrelated later commits. Always
        # return that canonical stored manifest instead of a transient manifest
        # carrying the current database-global watermark.
        return replace(
            self.get_dataset(dataset_id),
            reused_existing=reused_existing,
        )

    def read_dataset_series(
        self,
        *,
        dataset_id: str,
        series_id: int,
        known_at_lte: Optional[datetime] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        source_identity_keys: Sequence[str] = (),
        causal_at_interval_close: bool = False,
    ) -> list[MarketDataRecord]:
        with db.session() as session:
            entry = session.execute(
                text(
                    """
                    SELECT dataset_series.range_start, dataset_series.range_end,
                           dataset_series.max_commit_seq, series.fact_type,
                           series.timeframe_seconds, series.contract_version
                    FROM market.dataset_series AS dataset_series
                    JOIN market.series AS series ON series.id = dataset_series.series_id
                    WHERE dataset_series.dataset_id = :dataset_id
                      AND dataset_series.series_id = :series_id
                    """
                ),
                {"dataset_id": str(dataset_id), "series_id": int(series_id)},
            ).mappings().first()
            if entry is None:
                raise ValueError(
                    "market_dataset_series_unknown: "
                    f"dataset_id={dataset_id} series_id={series_id}"
                )
            requested = DatasetSeriesRequest(
                series_id=int(series_id),
                start=start or entry["range_start"],
                end=end or entry["range_end"],
            )
            if requested.start < entry["range_start"] or requested.end > entry["range_end"]:
                raise ValueError(
                    "market_dataset_range_expansion_forbidden: requested range is outside "
                    f"dataset_id={dataset_id} series_id={series_id} frozen bounds"
                )
            fact_type = str(entry["fact_type"])
            contract = get_fact_contract(fact_type)
            if fact_type in {
                CANDLE_FACT_TYPE,
                OPEN_INTEREST_FACT_TYPE,
                FUNDING_RATE_FACT_TYPE,
            } or contract.uses_exact_numeric_storage:
                rows = self._read_canonical_rows_with_session(
                    session,
                    series_id=int(series_id),
                    start=requested.start,
                    end=requested.end,
                    as_of_commit_seq=int(entry["max_commit_seq"]),
                    known_at_lte=known_at_lte,
                    latest_only=not contract.uses_exact_numeric_storage,
                    include_invalidated=contract.uses_exact_numeric_storage,
                    source_identity_keys=source_identity_keys,
                    causal_at_interval_close=(
                        causal_at_interval_close and fact_type == CANDLE_FACT_TYPE
                    ),
                )
                if str(entry.get("contract_version") or "") in _TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS:
                    return _decode_core_canonical_rows(fact_type, rows)
                return [_canonical_row_to_record(row) for row in rows]
            if source_identity_keys:
                raise ValueError(
                    "market_dataset_source_binding_unsupported: exact source filtering "
                    f"is not implemented for fact_type={entry['fact_type']}"
                )
            return self.read_series_records(
                series_id=int(series_id),
                start=requested.start,
                end=requested.end,
                as_of_commit_seq=int(entry["max_commit_seq"]),
                known_at_lte=known_at_lte,
            )


market_data_repo = PostgresMarketDataRepository()


__all__ = ["PostgresMarketDataRepository", "market_data_repo"]

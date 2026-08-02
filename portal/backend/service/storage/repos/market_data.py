"""Canonical PostgreSQL repository for market-data facts and datasets."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Optional

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
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
    build_candle_material_hash,
    build_dataset_identity_hash,
    build_funding_rate_material_hash,
    build_open_interest_material_hash,
    build_provenance_hash,
    build_quality_hash,
)
from market_data.store import FrozenDataset, IngestionOutcome
from sqlalchemy import text

from ....db import db


_SERIES_IDENTITY_VERSION = "market_series.v1"


def _json_text(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"), default=str)


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_text(value).encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
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


def _build_material_hash(
    *,
    fact_type: str,
    series_identity: Mapping[str, Any],
    records: Sequence[MarketDataRecord],
) -> str:
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
    raise RuntimeError(
        f"market_dataset_unsupported_fact: fact_type={fact_type}"
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
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.candle_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.open_interest_versions), 0),
                        COALESCE((SELECT MAX(market_commit_seq) FROM market.funding_rate_versions), 0)
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
                           series.contract_version,
                           COALESCE(candles.version_count, 0)
                             + COALESCE(open_interest.version_count, 0)
                             + COALESCE(funding.version_count, 0) AS version_count,
                           COALESCE(candles.fact_count, 0)
                             + COALESCE(open_interest.fact_count, 0)
                             + COALESCE(funding.fact_count, 0) AS fact_count,
                           COALESCE(candles.fact_count, 0) AS candle_count,
                           COALESCE(open_interest.fact_count, 0)
                             + COALESCE(funding.fact_count, 0) AS observation_count,
                           COALESCE(funding.fact_count, 0) AS funding_rate_count,
                           COALESCE(candles.first_fact_time, open_interest.first_fact_time,
                                    funding.first_fact_time)
                             AS first_fact_time,
                           COALESCE(candles.last_fact_time, open_interest.last_fact_time,
                                    funding.last_fact_time)
                             AS last_fact_time,
                           GREATEST(
                             COALESCE(candles.max_commit_seq, 0),
                             COALESCE(open_interest.max_commit_seq, 0),
                             COALESCE(funding.max_commit_seq, 0)
                           ) AS max_commit_seq
                    FROM market.series AS series
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
                    SELECT id, dataset_hash, max_commit_seq, name, purpose, metadata
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
                           series.identity_key, series.instrument_id, series.fact_type,
                           series.timeframe_seconds, series.contract_version
                    FROM market.dataset_series AS dataset_series
                    JOIN market.series AS series ON series.id = dataset_series.series_id
                    WHERE dataset_id = :dataset_id
                    ORDER BY series_id, range_start, range_end
                    """
                ),
                {"dataset_id": normalized},
            ).mappings().all()
        return FrozenDataset(
            dataset_id=str(dataset["id"]),
            dataset_hash=str(dataset["dataset_hash"]),
            max_commit_seq=int(dataset["max_commit_seq"]),
            series=tuple(dict(row) for row in rows),
            contract_version="market_dataset.v1",
            name=str(dataset["name"]) if dataset.get("name") else None,
            purpose=str(dataset["purpose"]),
            metadata=dict(dataset.get("metadata") or {}),
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
    ) -> int:
        instrument_id = str(instrument_id or "").strip()
        fact_type = str(fact_type or "").strip().lower()
        contract_version = str(contract_version or "").strip()
        timeframe = int(timeframe_seconds) if timeframe_seconds is not None else None
        if not instrument_id or not fact_type or not contract_version:
            raise ValueError("market_data_series_invalid: complete series identity is required")
        if fact_type == CANDLE_FACT_TYPE and (timeframe is None or timeframe <= 0):
            raise ValueError("market_data_series_invalid: candle timeframe must be positive")
        if fact_type == OPEN_INTEREST_FACT_TYPE:
            if timeframe is not None:
                raise ValueError(
                    "market_data_series_invalid: open-interest series has no timeframe"
                )
            if contract_version != OPEN_INTEREST_FACT_VERSION:
                raise ValueError(
                    "market_data_series_invalid: unsupported open-interest contract version"
                )
        if fact_type == FUNDING_RATE_FACT_TYPE:
            if timeframe is not None:
                raise ValueError(
                    "market_data_series_invalid: funding-rate series has no timeframe"
                )
            if contract_version != FUNDING_RATE_FACT_VERSION:
                raise ValueError(
                    "market_data_series_invalid: unsupported funding-rate contract version"
                )

        identity_key = _stable_hash(
            {
                "schema_version": _SERIES_IDENTITY_VERSION,
                "instrument_id": instrument_id,
                "fact_type": fact_type,
                "timeframe_seconds": timeframe,
                "contract_version": contract_version,
            }
        )
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.series (
                        identity_key, instrument_id, fact_type,
                        timeframe_seconds, contract_version
                    ) VALUES (
                        :identity_key, :instrument_id, :fact_type,
                        :timeframe_seconds, :contract_version
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
                },
            )
            row = session.execute(
                text(
                    """
                    SELECT id, instrument_id, fact_type,
                           timeframe_seconds, contract_version
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
        )
        expected = (instrument_id, fact_type, timeframe, contract_version)
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
    ) -> int:
        """Resolve one canonical logical series or fail without provider fallback."""

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
                    ORDER BY id
                    """
                ),
                {
                    "instrument_id": str(instrument_id or "").strip(),
                    "fact_type": str(fact_type or "").strip().lower(),
                    "timeframe_seconds": (
                        int(timeframe_seconds) if timeframe_seconds is not None else None
                    ),
                    "contract_version": str(contract_version or "").strip(),
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
            outcome = self._ingest_candle_rows(
                run_id=run_id,
                series_id=series_id,
                rows=rows,
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
            return self._ingest_open_interest_rows(
                run_id=run_id,
                series_id=series_id,
                rows=rows,
                provenance=dict(provenance or {}),
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
            return self._ingest_funding_rate_rows(
                run_id=run_id,
                series_id=series_id,
                rows=rows,
                provenance=dict(provenance or {}),
                allow_corrections=bool(allow_corrections),
                collection_fence=collection_fence,
            )
        except Exception as exc:
            self._fail_ingestion_run(run_id, exc)
            raise

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
                           timeframe_seconds, contract_version
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
    def _read_candles_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
    ) -> list[CandleRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "series_id = :series_id",
            "candle_open_time >= :start",
            "candle_open_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (candle_open_time) *
                    FROM market.candle_versions
                    WHERE {' AND '.join(predicates)}
                    ORDER BY candle_open_time, revision DESC
                )
                SELECT visible.*,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM visible
                JOIN market.ingestion_runs AS runs
                  ON runs.id = visible.ingestion_run_id
                JOIN market.sources AS sources
                  ON sources.id = runs.source_id
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
    ) -> list[CandleRecord]:
        with db.session() as session:
            return self._read_candles_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
            )

    @staticmethod
    def _read_open_interest_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
    ) -> list[OpenInterestRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "series_id = :series_id",
            "sample_time >= :start",
            "sample_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (sample_time) *
                    FROM market.open_interest_versions
                    WHERE {' AND '.join(predicates)}
                    ORDER BY sample_time, revision DESC
                )
                SELECT visible.*,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM visible
                JOIN market.ingestion_runs AS runs
                  ON runs.id = visible.ingestion_run_id
                JOIN market.sources AS sources
                  ON sources.id = runs.source_id
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
    ) -> list[OpenInterestRecord]:
        with db.session() as session:
            return self._read_open_interest_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
            )

    @staticmethod
    def _read_funding_rates_with_session(
        session,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int],
        known_at_lte: Optional[datetime],
    ) -> list[FundingRateRecord]:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        predicates = [
            "series_id = :series_id",
            "sample_time >= :start",
            "sample_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": request.series_id,
            "start": request.start,
            "end": request.end,
        }
        if as_of_commit_seq is not None:
            predicates.append("market_commit_seq <= :as_of_commit_seq")
            params["as_of_commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("known_at <= :known_at_lte")
            params["known_at_lte"] = known_at_lte
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (sample_time) *
                    FROM market.funding_rate_versions
                    WHERE {' AND '.join(predicates)}
                    ORDER BY sample_time, revision DESC
                )
                SELECT visible.*,
                       sources.identity_key AS source_identity_key,
                       sources.provider AS source_provider,
                       sources.venue AS source_venue,
                       sources.source_kind,
                       sources.adapter_version AS source_adapter_version
                FROM visible
                JOIN market.ingestion_runs AS runs
                  ON runs.id = visible.ingestion_run_id
                JOIN market.sources AS sources
                  ON sources.id = runs.source_id
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
    ) -> list[FundingRateRecord]:
        with db.session() as session:
            return self._read_funding_rates_with_session(
                session,
                series_id=series_id,
                start=start,
                end=end,
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
            )

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
        ingestion_run_id: Optional[str] = None,
        detected_as_of_commit_seq: Optional[int] = None,
    ) -> str:
        request = DatasetSeriesRequest(series_id=series_id, start=start, end=end)
        classification = str(classification or "").strip().lower()
        if not classification:
            raise ValueError("market_gap_evidence_invalid: classification is required")
        payload = {
            "series_id": request.series_id,
            "start": _iso(request.start),
            "end": _iso(request.end),
            "classification": classification,
            "expected_count": int(expected_count),
            "observed_count": int(observed_count),
            "evidence": dict(evidence),
        }
        evidence_hash = build_quality_hash([payload])
        with db.session() as session:
            watermark = detected_as_of_commit_seq
            if watermark is None:
                watermark = self._current_commit_seq_with_session(session)
            session.execute(
                text(
                    """
                    INSERT INTO market.gap_evidence (
                        series_id, ingestion_run_id, start_time, end_time,
                        classification, expected_count, observed_count,
                        detected_as_of_commit_seq, evidence_hash, evidence
                    ) VALUES (
                        :series_id, :ingestion_run_id, :start_time, :end_time,
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
    ) -> list[dict[str, Any]]:
        rows = session.execute(
            text(
                """
                SELECT start_time, end_time, classification, expected_count,
                       observed_count, detected_as_of_commit_seq, evidence_hash, evidence
                FROM market.gap_evidence
                WHERE series_id = :series_id
                  AND end_time > :start
                  AND start_time < :end
                  AND detected_as_of_commit_seq <= :watermark
                ORDER BY start_time, end_time, evidence_hash
                """
            ),
            {
                "series_id": series_id,
                "start": start,
                "end": end,
                "watermark": as_of_commit_seq,
            },
        ).mappings().all()
        return [
            {
                "start": _iso(row["start_time"]),
                "end": _iso(row["end_time"]),
                "classification": str(row["classification"]),
                "expected_count": int(row["expected_count"]),
                "observed_count": int(row["observed_count"]),
                "detected_as_of_commit_seq": int(row["detected_as_of_commit_seq"]),
                "evidence_hash": str(row["evidence_hash"]),
                "evidence": dict(row["evidence"] or {}),
            }
            for row in rows
        ]

    def list_gap_evidence(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
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
            watermark = self._current_commit_seq_with_session(session)
            manifest_series: list[dict[str, Any]] = []
            for item in normalized:
                identity = session.execute(
                    text(
                        """
                        SELECT identity_key, instrument_id, fact_type,
                               timeframe_seconds, contract_version
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
                if fact_type == CANDLE_FACT_TYPE:
                    records: list[MarketDataRecord] = self._read_candles_with_session(
                        session,
                        series_id=item.series_id,
                        start=item.start,
                        end=item.end,
                        as_of_commit_seq=watermark,
                        known_at_lte=None,
                    )
                elif fact_type == OPEN_INTEREST_FACT_TYPE:
                    records = self._read_open_interest_with_session(
                        session,
                        series_id=item.series_id,
                        start=item.start,
                        end=item.end,
                        as_of_commit_seq=watermark,
                        known_at_lte=None,
                    )
                elif fact_type == FUNDING_RATE_FACT_TYPE:
                    records = self._read_funding_rates_with_session(
                        session,
                        series_id=item.series_id,
                        start=item.start,
                        end=item.end,
                        as_of_commit_seq=watermark,
                        known_at_lte=None,
                    )
                else:
                    raise RuntimeError(
                        "market_dataset_unsupported_fact: "
                        f"series_id={item.series_id} fact_type={fact_type}"
                    )
                if not records:
                    raise RuntimeError(
                        "market_dataset_incomplete: no facts for "
                        f"series_id={item.series_id} start={_iso(item.start)} "
                        f"end={_iso(item.end)}"
                    )
                series_identity = dict(identity)
                quality = self._gap_evidence_with_session(
                    session,
                    series_id=item.series_id,
                    start=item.start,
                    end=item.end,
                    as_of_commit_seq=watermark,
                )
                source_counts = Counter(record.source_identity_key for record in records)
                source_details = {
                    record.source_identity_key: {
                        "provider": record.source.provider,
                        "venue": record.source.venue,
                        "source_kind": record.source.source_kind,
                        "adapter_version": record.source.adapter_version,
                    }
                    for record in records
                }
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
                        "provenance_hash": build_provenance_hash(records),
                        "source_summary": {
                            "counts": dict(sorted(source_counts.items())),
                            "sources": {key: source_details[key] for key in sorted(source_details)},
                        },
                        "quality_hash": build_quality_hash(quality),
                        "quality_summary": {
                            "evidence_count": len(quality),
                            "classifications": dict(sorted(classifications.items())),
                        },
                    }
                )
            dataset_hash = build_dataset_identity_hash(manifest_series)
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
                            provenance_hash, source_summary, quality_hash, quality_summary
                        ) VALUES (
                            :dataset_id, :series_id, :range_start, :range_end,
                            :max_commit_seq, :row_count, :material_hash,
                            :provenance_hash, CAST(:source_summary AS jsonb),
                            :quality_hash, CAST(:quality_summary AS jsonb)
                        )
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        **entry,
                        "dataset_id": dataset_id,
                        "source_summary": _json_text(entry["source_summary"]),
                        "quality_summary": _json_text(entry["quality_summary"]),
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
    ) -> list[MarketDataRecord]:
        with db.session() as session:
            entry = session.execute(
                text(
                    """
                    SELECT dataset_series.range_start, dataset_series.range_end,
                           dataset_series.max_commit_seq, series.fact_type
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
            if fact_type == CANDLE_FACT_TYPE:
                return self._read_candles_with_session(
                    session,
                    series_id=int(series_id),
                    start=requested.start,
                    end=requested.end,
                    as_of_commit_seq=int(entry["max_commit_seq"]),
                    known_at_lte=known_at_lte,
                )
            if fact_type == OPEN_INTEREST_FACT_TYPE:
                return self._read_open_interest_with_session(
                    session,
                    series_id=int(series_id),
                    start=requested.start,
                    end=requested.end,
                    as_of_commit_seq=int(entry["max_commit_seq"]),
                    known_at_lte=known_at_lte,
                )
            if fact_type == FUNDING_RATE_FACT_TYPE:
                return self._read_funding_rates_with_session(
                    session,
                    series_id=int(series_id),
                    start=requested.start,
                    end=requested.end,
                    as_of_commit_seq=int(entry["max_commit_seq"]),
                    known_at_lte=known_at_lte,
                )
            raise RuntimeError(
                "market_dataset_unsupported_fact: "
                f"series_id={series_id} fact_type={fact_type}"
            )


market_data_repo = PostgresMarketDataRepository()


__all__ = ["PostgresMarketDataRepository", "market_data_repo"]

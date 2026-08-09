"""Causal normalization and deterministic materialization service."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

from market_data.contracts import TypedFeatureRecord, record_effective_time
from market_data.normalization import (
    NormalizationFormula,
    NormalizationInput,
    NormalizationSpec,
    NormalizedStatus,
    evaluate_normalization,
)

from ..storage.repos.market_data import market_data_repo
from ..storage.repos.normalization import (
    PostgresNormalizationRepository,
    normalization_repository,
)


THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60
TWENTY_EIGHT_DAYS_SECONDS = 28 * 24 * 60 * 60


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(
            f"market_normalization_materialize_invalid: {field} must be datetime"
        )
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _source_material_hash(record: Any) -> str:
    material = getattr(record.fact, "material_hash", None)
    if material is None:
        material = getattr(record.fact, "row_hash", None)
    digest = str(material or "").strip().lower()
    if len(digest) != 64:
        raise RuntimeError("market_normalization_source_invalid: material hash missing")
    return digest


def _source_known_at(record: Any) -> datetime:
    value = getattr(record.fact, "known_at", None)
    if not isinstance(value, datetime):
        raise RuntimeError("market_normalization_source_invalid: known_at missing")
    return _utc(value, field="known_at")


def _decimal_attribute(fact: Any, field: str) -> Optional[Decimal]:
    value = getattr(fact, str(field), None)
    if value is None:
        return None
    return Decimal(str(value))


def _gap_contains(gaps: Sequence[Mapping[str, Any]], point: datetime) -> Optional[str]:
    for row in gaps:
        raw_start = row.get("start") or row.get("start_time")
        raw_end = row.get("end") or row.get("end_time")
        if raw_start is None or raw_end is None:
            continue
        start = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(raw_end).replace("Z", "+00:00"))
        if _utc(start, field="gap.start") <= point < _utc(end, field="gap.end"):
            return str(row.get("classification") or "source_gap")
    return None



def _overlapping_gaps(
    gaps: Sequence[Mapping[str, Any]], *, start: datetime, end: datetime
) -> tuple[Mapping[str, Any], ...]:
    overlapping: list[Mapping[str, Any]] = []
    for row in gaps:
        raw_start = row.get("start") or row.get("start_time")
        raw_end = row.get("end") or row.get("end_time")
        if raw_start is None or raw_end is None:
            continue
        gap_start = _utc(
            datetime.fromisoformat(str(raw_start).replace("Z", "+00:00")),
            field="gap.start",
        )
        gap_end = _utc(
            datetime.fromisoformat(str(raw_end).replace("Z", "+00:00")),
            field="gap.end",
        )
        if gap_end > start and gap_start < end:
            overlapping.append(row)
    return tuple(overlapping)


def builtin_normalization_specs() -> tuple[NormalizationSpec, ...]:
    """Return the bounded approved v1 catalog; registration remains explicit."""

    return (
        NormalizationSpec(
            feature_name="funding_rate_bps",
            semantic_version="1.0.1",
            input_fact_type="derivatives.funding_rate",
            output_fact_type="market.normalized.funding_rate_bps",
            formula=NormalizationFormula.BASIS_POINTS,
            units="basis_points",
            window_seconds=None,
            minimum_observations=0,
            warmup_observations=0,
            parameters={"value_field": "rate", "input_step_seconds": 60},
        ),
        NormalizationSpec(
            feature_name="funding_rate_percentile_30d",
            semantic_version="1.0.1",
            input_fact_type="derivatives.funding_rate",
            output_fact_type="market.normalized.funding_rate_percentile_30d",
            formula=NormalizationFormula.CAUSAL_PERCENTILE,
            units="unit_interval",
            window_seconds=THIRTY_DAYS_SECONDS,
            minimum_observations=30,
            warmup_observations=30,
            parameters={
                "value_field": "rate",
                "input_step_seconds": 60,
                "require_full_window": True,
            },
        ),
        NormalizationSpec(
            feature_name="funding_rate_zscore_30d",
            semantic_version="1.0.1",
            input_fact_type="derivatives.funding_rate",
            output_fact_type="market.normalized.funding_rate_zscore_30d",
            formula=NormalizationFormula.CAUSAL_ZSCORE,
            units="standard_deviations",
            window_seconds=THIRTY_DAYS_SECONDS,
            minimum_observations=30,
            warmup_observations=30,
            parameters={
                "value_field": "rate",
                "input_step_seconds": 60,
                "require_full_window": True,
            },
        ),
        NormalizationSpec(
            feature_name="relative_notional_time_of_day_28d",
            semantic_version="1.0.1",
            input_fact_type="market.trade_flow_feature",
            output_fact_type="market.normalized.relative_notional_time_of_day_28d",
            formula=NormalizationFormula.TIME_OF_DAY_MEDIAN_RATIO,
            units="ratio",
            window_seconds=TWENTY_EIGHT_DAYS_SECONDS,
            minimum_observations=10,
            warmup_observations=10,
            partition="series_minute_of_day",
            parameters={
                "value_field": "quote_notional",
                "input_step_seconds": 60,
                "require_full_window": True,
                "required_input_timeframe_seconds": 60,
            },
        ),
        NormalizationSpec(
            feature_name="aggressive_buy_share",
            semantic_version="1.0.1",
            input_fact_type="market.trade_flow_feature",
            output_fact_type="market.normalized.aggressive_buy_share",
            formula=NormalizationFormula.RATIO,
            units="unit_interval",
            window_seconds=None,
            minimum_observations=0,
            warmup_observations=0,
            parameters={
                "value_field": "aggressor_buy_notional",
                "denominator_field": "quote_notional",
                "input_step_seconds": 60,
            },
        ),
        NormalizationSpec(
            feature_name="vol_adjusted_return_60m",
            semantic_version="1.0.1",
            input_fact_type="market.bbo",
            output_fact_type="market.normalized.vol_adjusted_return_60m",
            formula=NormalizationFormula.VOLATILITY_ADJUSTED_RETURN,
            units="standard_deviations",
            window_seconds=3_600,
            minimum_observations=60,
            warmup_observations=60,
            parameters={
                "value_field": "mid_price",
                "input_step_seconds": 60,
                "require_full_window": True,
                "required_input_timeframe_seconds": 1,
                "sample_every_seconds": 60,
                "output_timeframe_seconds": 60,
            },
        ),
    )


class MarketNormalizationService:
    """Build normalized facts only from bounded canonical typed records."""

    def __init__(
        self,
        *,
        repository: PostgresNormalizationRepository = normalization_repository,
        store=market_data_repo,
    ) -> None:
        self.repository = repository
        self.store = store

    def install_builtin_specs(self, *, approved_by: str) -> list[dict[str, Any]]:
        actor = str(approved_by or "").strip()
        if not actor:
            raise ValueError("market_normalization_spec_invalid: approved_by is required")
        installed = [
            self.repository.register_spec(
                spec,
                created_by=actor,
                approved_by=actor,
            )
            for spec in builtin_normalization_specs()
        ]
        return [
            {**spec.material(), "spec_id": spec.spec_id, "spec_hash": spec.spec_hash}
            for spec in installed
        ]

    def list_specs(self) -> list[dict[str, Any]]:
        return [
            {**spec.material(), "spec_id": spec.spec_id, "spec_hash": spec.spec_hash}
            for spec in self.repository.list_specs()
        ]

    def materialize(
        self,
        *,
        spec_id: str,
        source_series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> dict[str, Any]:
        start_at = _utc(start, field="start")
        end_at = _utc(end, field="end")
        decision_at = _utc(known_at, field="known_at")
        if end_at <= start_at or decision_at < end_at:
            raise ValueError("market_normalization_materialize_invalid: causal range")
        spec = self.repository.get_spec(spec_id)
        series_rows = {
            int(row["id"]): row for row in self.store.list_series()
        }
        source = series_rows.get(int(source_series_id))
        if source is None:
            raise ValueError(
                f"market_normalization_source_unknown: series_id={source_series_id}"
            )
        if str(source["fact_type"]) != spec.input_fact_type:
            raise ValueError(
                "market_normalization_source_invalid: source fact type disagrees with spec"
            )
        source_timeframe = (
            int(source["timeframe_seconds"])
            if source.get("timeframe_seconds") is not None
            else None
        )
        required_input_timeframe = spec.parameters.get(
            "required_input_timeframe_seconds"
        )
        if (
            required_input_timeframe is not None
            and source_timeframe != int(required_input_timeframe)
        ):
            raise ValueError(
                "market_normalization_source_invalid: source timeframe disagrees with spec "
                f"expected={required_input_timeframe} actual={source_timeframe}"
            )
        lookback = int(spec.window_seconds or 0)
        selection_watermark = (
            int(as_of_commit_seq)
            if as_of_commit_seq is not None
            else int(self.store.current_commit_seq())
        )
        if selection_watermark <= 0:
            raise RuntimeError(
                "market_normalization_source_incomplete: no commit watermark"
            )
        step = int(spec.parameters.get("input_step_seconds") or 1)
        read_start = start_at - timedelta(seconds=lookback + step)
        source_records = self.store.read_series_records(
            series_id=int(source_series_id),
            start=read_start,
            end=end_at,
            as_of_commit_seq=selection_watermark,
            known_at_lte=decision_at,
        )
        sample_every = int(spec.parameters.get("sample_every_seconds") or 0)
        if sample_every > 0:
            source_records = [
                record
                for record in source_records
                if int(record_effective_time(record).timestamp()) % sample_every == 0
            ]
        if not source_records:
            raise RuntimeError(
                "market_normalization_source_incomplete: no source facts in causal range"
            )
        gaps = self.store.list_gap_evidence(
            series_id=int(source_series_id),
            start=read_start,
            end=end_at,
            as_of_commit_seq=selection_watermark,
            known_at_lte=decision_at,
        )
        inputs: list[NormalizationInput] = []
        value_field = str(spec.parameters.get("value_field") or "value")
        denominator_field = spec.parameters.get("denominator_field")
        for record in source_records:
            effective_at = record_effective_time(record)
            quality_window_start = (
                effective_at - timedelta(seconds=lookback)
                if lookback > 0
                else effective_at
            )
            overlapping_gaps = _overlapping_gaps(
                gaps,
                start=quality_window_start,
                end=effective_at + timedelta(microseconds=1),
            )
            quality = dict(record.quality) if isinstance(record, TypedFeatureRecord) else {}
            value = _decimal_attribute(record.fact, value_field)
            denominator = (
                _decimal_attribute(record.fact, str(denominator_field))
                if denominator_field
                else None
            )
            invalid_reason = _gap_contains(gaps, effective_at)
            if lookback > 0 and overlapping_gaps:
                invalid_reason = "source_window_gap:" + str(
                    overlapping_gaps[0].get("classification") or "unknown"
                )
            if quality.get("valid") is False:
                invalid_reason = str(
                    quality.get("reason")
                    or quality.get("classification")
                    or "source_quality_invalid"
                )
            source_status = getattr(record.fact, "status", None)
            source_status_value = (
                str(getattr(source_status, "value", source_status))
                if source_status
                else None
            )
            if source_status_value is not None and source_status_value != "valid":
                invalid_reason = str(
                    getattr(record.fact, "reason", None) or source_status_value
                )
            if value is None and invalid_reason is None:
                invalid_reason = f"source_field_missing:{value_field}"
            partition_key = "series"
            if spec.partition == "series_minute_of_day":
                partition_key = f"{effective_at.hour:02d}:{effective_at.minute:02d}"
            quality_known_at = _source_known_at(record)
            evidence_watermark = int(record.market_commit_seq)
            if overlapping_gaps:
                quality_known_at = max(
                    [quality_known_at]
                    + [
                        _utc(
                            datetime.fromisoformat(
                                str(gap.get("detected_at")).replace(
                                    "Z", "+00:00"
                                )
                            ),
                            field="gap.detected_at",
                        )
                        for gap in overlapping_gaps
                    ]
                )
                evidence_watermark = max(
                    [evidence_watermark]
                    + [
                        int(gap.get("detected_as_of_commit_seq") or 0)
                        for gap in overlapping_gaps
                    ]
                )
            quality_fingerprint = _stable_hash(
                {
                    "schema_version": "market.normalization_source_quality.v1",
                    "record_quality": quality,
                    "source_status": source_status_value,
                    "invalid_reason": invalid_reason,
                    "gap_evidence": [
                        {
                            "evidence_hash": gap.get("evidence_hash"),
                            "classification": gap.get("classification"),
                            "start": gap.get("start") or gap.get("start_time"),
                            "end": gap.get("end") or gap.get("end_time"),
                            "detected_as_of_commit_seq": gap.get(
                                "detected_as_of_commit_seq"
                            ),
                            "detected_at": gap.get("detected_at"),
                        }
                        for gap in overlapping_gaps
                    ],
                }
            )
            inputs.append(
                NormalizationInput(
                    source_series_id=int(source_series_id),
                    effective_at=effective_at,
                    known_at=quality_known_at,
                    market_commit_seq=evidence_watermark,
                    material_hash=_source_material_hash(record),
                    value=value,
                    denominator=denominator,
                    partition_key=partition_key,
                    valid=invalid_reason is None,
                    invalid_reason=invalid_reason,
                    quality_fingerprint=quality_fingerprint,
                )
            )
        source_watermark = max(row.market_commit_seq for row in inputs)
        output_contract_version = f"market.normalized_feature.v1/{spec.spec_id}"
        output_series_id = self.store.register_series(
            instrument_id=str(source["instrument_id"]),
            fact_type=spec.output_fact_type,
            timeframe_seconds=(
                int(spec.parameters["output_timeframe_seconds"])
                if spec.parameters.get("output_timeframe_seconds") is not None
                else source_timeframe
            ),
            contract_version=output_contract_version,
        )
        evaluated = evaluate_normalization(
            spec,
            inputs,
            output_series_id=output_series_id,
        )
        bounded = tuple(
            fact for fact in evaluated if start_at <= fact.effective_at < end_at
        )
        if not bounded:
            raise RuntimeError(
                "market_normalization_source_incomplete: no output facts in requested range"
            )
        outcome = self.repository.ingest(bounded)
        fingerprint = _stable_hash(
            {
                "schema_version": "market.normalization_materialization.v1",
                "spec_hash": spec.spec_hash,
                "source_series_id": int(source_series_id),
                "output_series_id": output_series_id,
                "source_watermark": source_watermark,
                "output_watermark": outcome.max_commit_seq,
                "range_start": start_at.isoformat(),
                "range_end": end_at.isoformat(),
                "material_hashes": [fact.material_hash for fact in bounded],
            }
        )
        statuses: dict[str, int] = {}
        for fact in bounded:
            statuses[fact.status.value] = statuses.get(fact.status.value, 0) + 1
        return {
            "schema_version": "market.normalization_materialization.v1",
            "spec_id": spec.spec_id,
            "spec_hash": spec.spec_hash,
            "source_series_id": int(source_series_id),
            "output_series_id": output_series_id,
            "source_watermark": source_watermark,
            "selection_watermark": selection_watermark,
            "output_watermark": outcome.max_commit_seq,
            "range_start": start_at.isoformat(),
            "range_end": end_at.isoformat(),
            "known_at": decision_at.isoformat(),
            "row_count": len(bounded),
            "inserted_count": outcome.inserted_count,
            "noop_count": outcome.noop_count,
            "statuses": dict(sorted(statuses.items())),
            "fingerprint": fingerprint,
            "provider_call_performed": False,
        }

    def compare_persisted(
        self,
        *,
        spec_id: str,
        source_series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> dict[str, Any]:
        materialized = self.materialize(
            spec_id=spec_id,
            source_series_id=source_series_id,
            start=start,
            end=end,
            known_at=known_at,
            as_of_commit_seq=as_of_commit_seq,
        )
        stored = self.repository.read_records(
            series_id=int(materialized["output_series_id"]),
            start=_utc(start, field="start"),
            end=_utc(end, field="end"),
            known_at_lte=_utc(known_at, field="known_at"),
            as_of_commit_seq=int(materialized["output_watermark"]),
        )
        stored_hashes = tuple(record.fact.material_hash for record in stored)
        persisted_fingerprint = _stable_hash(
            {
                "schema_version": "market.normalization_materialization.v1",
                "spec_hash": materialized["spec_hash"],
                "source_series_id": int(source_series_id),
                "output_series_id": int(materialized["output_series_id"]),
                "source_watermark": int(materialized["source_watermark"]),
                "output_watermark": int(materialized["output_watermark"]),
                "range_start": _utc(start, field="start").isoformat(),
                "range_end": _utc(end, field="end").isoformat(),
                "material_hashes": list(stored_hashes),
            }
        )
        return {
            **materialized,
            "persisted_row_count": len(stored),
            "persisted_fingerprint": persisted_fingerprint,
            "persisted_equal": persisted_fingerprint == materialized["fingerprint"],
        }


market_normalization_service = MarketNormalizationService()


__all__ = [
    "MarketNormalizationService",
    "builtin_normalization_specs",
    "market_normalization_service",
]

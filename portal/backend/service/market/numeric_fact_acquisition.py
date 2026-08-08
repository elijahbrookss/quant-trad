"""Explicit, bounded acquisition and durable coverage caching for numeric facts."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from data_providers.numeric_facts import (
    NumericAcquisitionBudget,
    NumericFactBinding,
    NumericFactManifest,
    NumericFactProvider,
    NumericFactProviderContractError,
    NumericFactProviderError,
    ProviderNumericBatch,
    load_numeric_fact_manifest,
)
from data_providers.providers.chainlink import (
    CHAINLINK_ADAPTER_ID,
    ChainlinkAggregatorV3Provider,
    HttpJsonRpcTransport,
)
from market_data.contracts import (
    NumericFact,
    NumericFactRecord,
    NumericFactState,
    SourceIdentity,
)

from ..storage.repos.market_data import PostgresMarketDataRepository, market_data_repo


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NumericAcquisitionAuthorization:
    network_allowed: bool = False
    actor: str = ""
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.network_allowed, bool):
            raise ValueError(
                "numeric_fact_acquisition_invalid: network_allowed must be boolean"
            )
        object.__setattr__(self, "actor", str(self.actor or "").strip())
        object.__setattr__(self, "reason", str(self.reason or "").strip())

    def require(self) -> None:
        if not self.network_allowed:
            raise RuntimeError(
                "numeric_fact_acquisition_denied: explicit network authorization is required"
            )
        if not str(self.actor or "").strip() or not str(self.reason or "").strip():
            raise ValueError(
                "numeric_fact_acquisition_invalid: actor and reason are required"
            )


@dataclass(frozen=True)
class NumericAcquisitionResult:
    manifest_id: str
    binding_id: str
    series_id: int
    source_id: int
    requested_ranges: tuple[tuple[datetime, datetime], ...]
    acquired_ranges: tuple[tuple[datetime, datetime], ...]
    cached_ranges: tuple[tuple[datetime, datetime], ...]
    inserted_count: int
    corrected_count: int
    invalidated_count: int
    noop_count: int
    gap_count: int
    requests_used: int
    logs_used: int
    blocks_scanned: int
    complete: bool


ProviderBuilder = Callable[[NumericFactBinding], NumericFactProvider]


class NumericFactAcquisitionService:
    """Coordinates manifests, providers, canonical storage, and scan evidence."""

    def __init__(
        self,
        *,
        store: PostgresMarketDataRepository = market_data_repo,
        provider_builder: Optional[ProviderBuilder] = None,
    ) -> None:
        self.store = store
        self._provider_builder = provider_builder or self._build_provider

    @staticmethod
    def _source_identity(binding: NumericFactBinding) -> SourceIdentity:
        expected = {"provider", "venue", "source_kind", "adapter_version"}
        if set(binding.source) != expected:
            raise ValueError(
                "numeric_fact_manifest_invalid: source fields must match the v1 schema"
            )
        return SourceIdentity(
            provider=str(binding.source["provider"]),
            venue=str(binding.source["venue"]),
            source_kind=str(binding.source["source_kind"]),
            adapter_version=str(binding.source["adapter_version"]),
        )

    @staticmethod
    def _build_provider(binding: NumericFactBinding) -> NumericFactProvider:
        endpoint = str(os.environ.get(binding.endpoint_ref) or "").strip()
        if not endpoint:
            raise RuntimeError(
                "numeric_fact_endpoint_missing: "
                f"binding={binding.id} endpoint_ref={binding.endpoint_ref}"
            )
        if binding.adapter == CHAINLINK_ADAPTER_ID:
            pacing_raw = str(
                os.environ.get(
                    "CHAINLINK_RPC_MIN_INTERVAL_SECONDS", "0.5"
                )
            ).strip()
            try:
                pacing_seconds = float(pacing_raw)
            except ValueError as exc:
                raise RuntimeError(
                    "numeric_fact_provider_config_invalid: "
                    "CHAINLINK_RPC_MIN_INTERVAL_SECONDS must be numeric"
                ) from exc
            return ChainlinkAggregatorV3Provider(
                HttpJsonRpcTransport(
                    endpoint,
                    min_request_interval_seconds=pacing_seconds,
                ),
                endpoint_ref=binding.endpoint_ref,
            )
        raise ValueError(
            f"numeric_fact_provider_unsupported: adapter={binding.adapter}"
        )

    def _register_binding(
        self,
        manifest: NumericFactManifest,
        binding: NumericFactBinding,
    ) -> tuple[int, int, SourceIdentity]:
        source = self._source_identity(binding)
        source_id = self.store.register_source(
            source,
            lineage={
                "schema_version": "market.numeric_fact_source_lineage.v1",
                "manifest_id": manifest.id,
                "manifest_hash": manifest.manifest_hash,
                "binding_id": binding.id,
                "adapter": binding.adapter,
                "endpoint_ref": binding.endpoint_ref,
                "instrument_role": binding.instrument_role,
                "schedule": dict(binding.schedule),
                "quality_policy": dict(binding.quality_policy),
                "risk": dict(binding.risk),
            },
        )
        series_id = self.store.register_series(
            instrument_id=binding.instrument_id,
            fact_type=binding.fact_type,
            timeframe_seconds=None,
            contract_version=binding.contract_version,
            dimensions=binding.dimensions,
        )
        return source_id, series_id, source

    @staticmethod
    def _to_fact(
        binding: NumericFactBinding,
        observation,
        *,
        accepted_at: datetime,
        state: NumericFactState = NumericFactState.ACTIVE,
        known_at: datetime | None = None,
        known_at_method: str | None = None,
    ) -> NumericFact:
        return NumericFact(
            fact_type=binding.fact_type,
            contract_version=binding.contract_version,
            value=observation.value,
            raw_value=observation.raw_value,
            unit=binding.unit,
            dimensions=binding.dimensions,
            effective_at=observation.effective_at,
            effective_at_method=observation.effective_at_method,
            source_published_at=observation.source_published_at,
            received_at=None,
            accepted_at=accepted_at,
            known_at=known_at or observation.known_at,
            known_at_method=known_at_method or observation.known_at_method,
            source_event_key=observation.source_event_key,
            source_event_group_key=observation.source_event_group_key,
            source_event_component_key=observation.source_event_component_key,
            state=state,
            source_event_material_hash=observation.source_event_material_hash,
        )

    @staticmethod
    def _cached_ranges(
        start: datetime,
        end: datetime,
        missing: list[tuple[datetime, datetime]],
    ) -> tuple[tuple[datetime, datetime], ...]:
        cached: list[tuple[datetime, datetime]] = []
        cursor = start
        for range_start, range_end in sorted(missing):
            if range_start > cursor:
                cached.append((cursor, range_start))
            cursor = max(cursor, range_end)
        if cursor < end:
            cached.append((cursor, end))
        return tuple(cached)

    @staticmethod
    def _same_source_event_state(left: NumericFact, right: NumericFact) -> bool:
        return (
            left.fact_type,
            left.contract_version,
            left.value,
            left.raw_value,
            left.unit,
            dict(left.dimensions),
            left.effective_at,
            left.effective_at_method,
            left.source_published_at,
            left.source_event_key,
            left.source_event_group_key,
            left.source_event_component_key,
            left.source_event_material_hash,
            left.state,
        ) == (
            right.fact_type,
            right.contract_version,
            right.value,
            right.raw_value,
            right.unit,
            dict(right.dimensions),
            right.effective_at,
            right.effective_at_method,
            right.source_published_at,
            right.source_event_key,
            right.source_event_group_key,
            right.source_event_component_key,
            right.source_event_material_hash,
            right.state,
        )

    def _persist_batch(
        self,
        *,
        manifest: NumericFactManifest,
        binding: NumericFactBinding,
        source: SourceIdentity,
        source_id: int,
        series_id: int,
        batch: ProviderNumericBatch,
        repair: bool,
        authorization: NumericAcquisitionAuthorization,
        record_historical_coverage: bool = True,
    ) -> tuple[int, int, int, int, int]:
        accepted_at = datetime.now(timezone.utc)
        active_before = self.store.read_numeric_facts(
            series_id=series_id,
            start=batch.range_start,
            end=batch.range_end,
        )
        revisions_before = self.store.read_numeric_fact_revisions(
            series_id=series_id,
            start=batch.range_start,
            end=batch.range_end,
        )
        latest_before: dict[str, NumericFactRecord] = {}
        for record in revisions_before:
            current = latest_before.get(record.fact.source_event_key)
            if current is None or (
                int(record.revision), int(record.market_commit_seq)
            ) > (
                int(current.revision), int(current.market_commit_seq)
            ):
                latest_before[record.fact.source_event_key] = record
        incoming_by_key = {
            item.source_event_key: item for item in batch.observations
        }
        facts: list[NumericFact] = []
        provenance_by_event: dict[str, dict] = {}
        for item in batch.observations:
            candidate = self._to_fact(binding, item, accepted_at=accepted_at)
            prior = latest_before.get(item.source_event_key)
            if prior is not None and candidate.row_hash != prior.fact.row_hash:
                same_manifest = (
                    str(prior.provenance.get("manifest_hash") or "")
                    == manifest.manifest_hash
                )
                if same_manifest and self._same_source_event_state(
                    prior.fact, candidate
                ):
                    candidate = prior.fact
                else:
                    candidate = self._to_fact(
                        binding,
                        item,
                        accepted_at=accepted_at,
                        known_at=max(accepted_at, item.known_at),
                        known_at_method="reconciliation_observed_at",
                    )
            facts.append(candidate)
            event_provenance = dict(item.provenance)
            if prior is not None and candidate.row_hash != prior.fact.row_hash:
                event_provenance["correction"] = {
                    "kind": "source_event_changed",
                    "prior_revision": int(prior.revision),
                    "observed_at": accepted_at.isoformat(),
                    "manifest_hash": manifest.manifest_hash,
                }
            provenance_by_event[item.source_event_key] = event_provenance
        invalidated_count = 0
        if repair and batch.status == "complete":
            for record in active_before:
                if (
                    record.source_identity_key != source.identity_key
                    or record.fact.source_event_key in incoming_by_key
                ):
                    continue
                prior = record.fact
                facts.append(
                    NumericFact(
                        fact_type=prior.fact_type,
                        contract_version=prior.contract_version,
                        value=prior.value,
                        raw_value=prior.raw_value,
                        unit=prior.unit,
                        dimensions=prior.dimensions,
                        effective_at=prior.effective_at,
                        effective_at_method=prior.effective_at_method,
                        source_published_at=prior.source_published_at,
                        received_at=None,
                        accepted_at=accepted_at,
                        known_at=max(accepted_at, prior.known_at),
                        known_at_method="reorg_reconciliation",
                        source_event_key=prior.source_event_key,
                        source_event_group_key=prior.source_event_group_key,
                        source_event_component_key=prior.source_event_component_key,
                        state=NumericFactState.INVALIDATED,
                        source_event_material_hash=prior.source_event_material_hash,
                    )
                )
                provenance_by_event[prior.source_event_key] = {
                    **dict(record.provenance),
                    "correction": {
                        "kind": "source_event_disappeared",
                        "reconciled_at": accepted_at.isoformat(),
                        "manifest_hash": manifest.manifest_hash,
                    },
                }
                invalidated_count += 1

        ingestion_run_id: str | None = None
        inserted = corrected = noops = 0
        if facts:
            outcome = self.store.ingest_numeric_facts(
                series_id=series_id,
                source_id=source_id,
                facts=facts,
                request={
                    **dict(batch.request),
                    "manifest_id": manifest.id,
                    "manifest_hash": manifest.manifest_hash,
                    "binding_id": binding.id,
                    "authorized_by": authorization.actor,
                    "authorization_reason": authorization.reason,
                    "repair": bool(repair),
                },
                provenance={
                    "manifest_id": manifest.id,
                    "manifest_hash": manifest.manifest_hash,
                    "binding_id": binding.id,
                    "endpoint_ref": binding.endpoint_ref,
                    "capabilities": dict(batch.capabilities),
                },
                provenance_by_event=provenance_by_event,
                source_revision=manifest.manifest_hash,
                allow_corrections=True,
            )
            ingestion_run_id = outcome.ingestion_run_id
            inserted = outcome.inserted_count
            corrected = outcome.corrected_count
            noops = outcome.noop_count

        for gap in batch.gaps:
            gap_end = gap.end
            if gap_end <= gap.start:
                gap_end = gap.start + timedelta(microseconds=1)
            self.store.record_gap_evidence(
                series_id=series_id,
                start=gap.start,
                end=gap_end,
                classification=gap.classification,
                expected_count=0,
                observed_count=0,
                evidence=dict(gap.evidence),
                ingestion_run_id=ingestion_run_id,
            )
        if record_historical_coverage:
            self.store.record_acquisition_coverage(
                series_id=series_id,
                source_id=source_id,
                binding_id=binding.id,
                manifest_hash=manifest.manifest_hash,
                interface_version=source.adapter_version,
                confirmation_depth=int(binding.config["confirmations"]),
                start=batch.range_start,
                end=batch.range_end,
                source_position_start=batch.source_position_start,
                source_position_end=batch.source_position_end,
                source_position_head=batch.source_position_head,
                status=batch.status,
                ingestion_run_id=ingestion_run_id,
                evidence={
                    "request": dict(batch.request),
                    "capabilities": dict(batch.capabilities),
                    "observation_count": len(batch.observations),
                    "gap_count": len(batch.gaps),
                    "budget": {
                        "requests_used": batch.budget_requests_used,
                        "logs_used": batch.budget_logs_used,
                        "blocks_scanned": batch.budget_blocks_scanned,
                    },
                    "repair": bool(repair),
                },
            )
        return inserted, corrected, invalidated_count, noops, len(batch.gaps)

    def acquire_history(
        self,
        *,
        manifest_path: str,
        binding_id: str,
        start: datetime,
        end: datetime,
        authorization: NumericAcquisitionAuthorization,
        budget: NumericAcquisitionBudget,
        repair: bool = False,
    ) -> NumericAcquisitionResult:
        manifest = load_numeric_fact_manifest(manifest_path)
        binding = manifest.binding(binding_id, require_enabled=True)
        source_id, series_id, source = self._register_binding(manifest, binding)
        requested = ((start, end),)
        missing = (
            [(start, end)]
            if repair
            else self.store.missing_acquisition_ranges(
                series_id=series_id,
                source_id=source_id,
                binding_id=binding.id,
                manifest_hash=manifest.manifest_hash,
                interface_version=source.adapter_version,
                confirmation_depth=int(binding.config["confirmations"]),
                start=start,
                end=end,
            )
        )
        cached_ranges = self._cached_ranges(start, end, list(missing))
        if not missing:
            return NumericAcquisitionResult(
                manifest_id=manifest.id,
                binding_id=binding.id,
                series_id=series_id,
                source_id=source_id,
                requested_ranges=requested,
                acquired_ranges=(),
                cached_ranges=cached_ranges,
                inserted_count=0,
                corrected_count=0,
                invalidated_count=0,
                noop_count=0,
                gap_count=0,
                requests_used=0,
                logs_used=0,
                blocks_scanned=0,
                complete=True,
            )
        authorization.require()
        provider = self._provider_builder(binding)
        totals = [0, 0, 0, 0, 0]
        usage = [0, 0, 0]
        remaining = [budget.max_requests, budget.max_logs, budget.max_blocks]
        acquired: list[tuple[datetime, datetime]] = []
        all_complete = True
        for range_start, range_end in missing:
            logger.info(
                "numeric_fact_acquisition_start | manifest_id=%s binding_id=%s "
                "series_id=%s provider=%s venue=%s start=%s end=%s repair=%s",
                manifest.id,
                binding.id,
                series_id,
                source.provider,
                source.venue,
                range_start.isoformat(),
                range_end.isoformat(),
                repair,
            )
            try:
                if any(value <= 0 for value in remaining):
                    raise NumericFactProviderError(
                        "numeric_fact_acquisition_budget_exhausted: "
                        "acquisition budget exhausted "
                        f"binding={binding.id} remaining={remaining}"
                    )
                range_budget = NumericAcquisitionBudget(
                    max_requests=remaining[0],
                    max_logs=remaining[1],
                    max_blocks=remaining[2],
                    max_retries=budget.max_retries,
                )
                batch = provider.fetch_history(
                    binding,
                    start=range_start,
                    end=range_end,
                    budget=range_budget,
                )
                if (
                    batch.range_start != range_start
                    or batch.range_end != range_end
                ):
                    raise NumericFactProviderContractError(
                        "numeric_fact_provider_range_disagreement: "
                        f"binding={binding.id} requested="
                        f"{range_start.isoformat()}..{range_end.isoformat()} "
                        f"returned={batch.range_start.isoformat()}.."
                        f"{batch.range_end.isoformat()}"
                    )
                batch_usage = [
                    batch.budget_requests_used,
                    batch.budget_logs_used,
                    batch.budget_blocks_scanned,
                ]
                if any(
                    used > available
                    for used, available in zip(batch_usage, remaining)
                ):
                    raise NumericFactProviderContractError(
                        "numeric_fact_provider_budget_disagreement: "
                        f"binding={binding.id} used={batch_usage} "
                        f"available={remaining}"
                    )
            except NumericFactProviderError as exc:
                evidence = {
                    "schema_version": "market.numeric_acquisition_failure.v1",
                    "manifest_id": manifest.id,
                    "manifest_hash": manifest.manifest_hash,
                    "binding_id": binding.id,
                    "endpoint_ref": binding.endpoint_ref,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                self.store.record_gap_evidence(
                    series_id=series_id,
                    start=range_start,
                    end=range_end,
                    classification="numeric_provider_acquisition_failed",
                    expected_count=0,
                    observed_count=0,
                    evidence=evidence,
                )
                self.store.record_acquisition_coverage(
                    series_id=series_id,
                    source_id=source_id,
                    binding_id=binding.id,
                    manifest_hash=manifest.manifest_hash,
                    interface_version=source.adapter_version,
                    confirmation_depth=int(binding.config["confirmations"]),
                    start=range_start,
                    end=range_end,
                    source_position_start="unknown",
                    source_position_end="unknown",
                    source_position_head=None,
                    status="failed",
                    evidence=evidence,
                )
                logger.error(
                    "numeric_fact_acquisition_failed | manifest_id=%s "
                    "binding_id=%s series_id=%s start=%s end=%s error=%s",
                    manifest.id,
                    binding.id,
                    series_id,
                    range_start.isoformat(),
                    range_end.isoformat(),
                    exc,
                )
                raise
            remaining = [
                available - used
                for used, available in zip(batch_usage, remaining)
            ]
            usage = [left + right for left, right in zip(usage, batch_usage)]
            counts = self._persist_batch(
                manifest=manifest,
                binding=binding,
                source=source,
                source_id=source_id,
                series_id=series_id,
                batch=batch,
                repair=repair,
                authorization=authorization,
            )
            totals = [left + right for left, right in zip(totals, counts)]
            acquired.append((range_start, range_end))
            all_complete = all_complete and batch.status == "complete"
            logger.info(
                "numeric_fact_acquisition_complete | manifest_id=%s binding_id=%s "
                "series_id=%s status=%s observations=%s gaps=%s",
                manifest.id,
                binding.id,
                series_id,
                batch.status,
                len(batch.observations),
                len(batch.gaps),
            )
        return NumericAcquisitionResult(
            manifest_id=manifest.id,
            binding_id=binding.id,
            series_id=series_id,
            source_id=source_id,
            requested_ranges=requested,
            acquired_ranges=tuple(acquired),
            cached_ranges=cached_ranges,
            inserted_count=totals[0],
            corrected_count=totals[1],
            invalidated_count=totals[2],
            noop_count=totals[3],
            gap_count=totals[4],
            requests_used=usage[0],
            logs_used=usage[1],
            blocks_scanned=usage[2],
            complete=all_complete,
        )

    def acquire_current(
        self,
        *,
        manifest_path: str,
        binding_id: str,
        authorization: NumericAcquisitionAuthorization,
        budget: NumericAcquisitionBudget,
    ) -> NumericAcquisitionResult:
        """Fetch the newest confirmed round; never called implicitly by replay."""

        manifest = load_numeric_fact_manifest(manifest_path)
        binding = manifest.binding(binding_id, require_enabled=True)
        source_id, series_id, source = self._register_binding(manifest, binding)
        authorization.require()
        provider = self._provider_builder(binding)
        batch = provider.fetch_current(binding, budget=budget)
        batch_usage = (
            batch.budget_requests_used,
            batch.budget_logs_used,
            batch.budget_blocks_scanned,
        )
        allowed_usage = (
            budget.max_requests,
            budget.max_logs,
            budget.max_blocks,
        )
        if any(used > allowed for used, allowed in zip(batch_usage, allowed_usage)):
            raise NumericFactProviderContractError(
                "numeric_fact_provider_budget_disagreement: "
                f"binding={binding.id} used={list(batch_usage)} "
                f"available={list(allowed_usage)}"
            )
        counts = self._persist_batch(
            manifest=manifest,
            binding=binding,
            source=source,
            source_id=source_id,
            series_id=series_id,
            batch=batch,
            repair=False,
            authorization=authorization,
            record_historical_coverage=False,
        )
        return NumericAcquisitionResult(
            manifest_id=manifest.id,
            binding_id=binding.id,
            series_id=series_id,
            source_id=source_id,
            requested_ranges=((batch.range_start, batch.range_end),),
            acquired_ranges=((batch.range_start, batch.range_end),),
            cached_ranges=(),
            inserted_count=counts[0],
            corrected_count=counts[1],
            invalidated_count=counts[2],
            noop_count=counts[3],
            gap_count=counts[4],
            requests_used=batch.budget_requests_used,
            logs_used=batch.budget_logs_used,
            blocks_scanned=batch.budget_blocks_scanned,
            complete=batch.status == "complete",
        )


numeric_fact_acquisition_service = NumericFactAcquisitionService()


__all__ = [
    "NumericAcquisitionAuthorization",
    "NumericAcquisitionResult",
    "NumericFactAcquisitionService",
    "numeric_fact_acquisition_service",
]

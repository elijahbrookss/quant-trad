"""Provider-neutral collection orchestration with Coinbase OI as fact handler v1."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Optional

from data_providers.facts import (
    ProviderFundingRateSnapshot,
    ProviderOpenInterestSnapshot,
    ProviderReserveStateSnapshot,
)
from data_providers.providers.factory import get_provider
from data_providers.providers.chainlink import (
    CHAINLINK_MVR_ADAPTER_ID,
    ChainlinkMvrReserveProvider,
    HttpJsonRpcTransport,
)
from data_providers.registry import FeatureAuth, feature_contract
from data_providers.structured_facts import (
    StructuredFactBinding,
    load_structured_fact_manifest,
)
from market_data.canonical import CanonicalFact
from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    FundingRateFact,
    MarketDataRequirement,
    OpenInterestFact,
    SourceIdentity,
)
from market_data.requirements import UnavailableMarketData, latest_known_record
from market_data.store import MarketDataStore

from ..storage.repos.market_collection import (
    CollectionClaim,
    MarketCollectionOwnershipError,
    PostgresMarketCollectionRepository,
    market_collection_repo,
)
from ..storage.repos.market_data import market_data_repo
from . import instrument_service


COINBASE_OI_ADAPTER_VERSION = "coinbase_advanced_trade.open_interest.public_poll.v1"
COINBASE_FUNDING_ADAPTER_VERSION = "coinbase_advanced_trade.funding_rate.public_poll.v1"
RESERVE_STATE_FACT_TYPE = "asset.reserve_state"
RESERVE_STATE_FACT_VERSION = "asset.reserve_state.v1"
COLLECTOR_DEFINITION_VERSION = "market_collection_definition.v1"
COLLECTOR_RESULT_VERSION = "market_collection_result.v1"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if key == "lease_token_hash":
            continue
        if isinstance(value, datetime):
            payload[str(key)] = value.astimezone(UTC).isoformat()
        else:
            payload[str(key)] = value
    if "lease_owner" in row:
        payload["lease_active"] = bool(row.get("lease_owner"))
    return payload


class MarketDataCollectorService:
    """Creates collection contracts and executes one fenced scheduled poll."""

    def __init__(
        self,
        *,
        collection_repo: PostgresMarketCollectionRepository = market_collection_repo,
        store: MarketDataStore = market_data_repo,
        provider_factory: Callable[..., Any] = get_provider,
        clock: Callable[[], datetime] = _utcnow,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        structured_provider_builder: Optional[
            Callable[[StructuredFactBinding], Any]
        ] = None,
    ) -> None:
        self.collection_repo = collection_repo
        self.store = store
        self.provider_factory = provider_factory
        self.clock = clock
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.structured_provider_builder = (
            structured_provider_builder or self._build_structured_provider
        )

    @staticmethod
    def _build_structured_provider(binding: StructuredFactBinding) -> Any:
        endpoint = str(os.environ.get(binding.endpoint_ref) or "").strip()
        if not endpoint:
            raise RuntimeError(
                "structured_fact_endpoint_missing: "
                f"binding={binding.id} endpoint_ref={binding.endpoint_ref}"
            )
        pacing_raw = str(
            os.environ.get("CHAINLINK_RPC_MIN_INTERVAL_SECONDS", "0.5")
        ).strip()
        try:
            pacing_seconds = float(pacing_raw)
        except ValueError as exc:
            raise RuntimeError(
                "structured_fact_provider_config_invalid: "
                "CHAINLINK_RPC_MIN_INTERVAL_SECONDS must be numeric"
            ) from exc
        if binding.adapter == CHAINLINK_MVR_ADAPTER_ID:
            return ChainlinkMvrReserveProvider(
                HttpJsonRpcTransport(
                    endpoint,
                    min_request_interval_seconds=pacing_seconds,
                ),
                endpoint_ref=binding.endpoint_ref,
            )
        raise ValueError(
            f"structured_fact_provider_unsupported: adapter={binding.adapter}"
        )

    @staticmethod
    def _structured_binding(config: Mapping[str, Any]) -> StructuredFactBinding:
        raw = config.get("structured_binding")
        if not isinstance(raw, Mapping):
            raise RuntimeError(
                "market_collection_definition_invalid: structured binding is missing"
            )
        binding = StructuredFactBinding(**dict(raw))
        if binding.manifest_hash != str(config.get("manifest_hash") or ""):
            raise RuntimeError(
                "market_collection_definition_invalid: structured manifest hash disagreement"
            )
        return binding

    def create_coinbase_open_interest_definition(
        self,
        *,
        instrument_id: str,
        provider_product_id: str,
        poll_interval_seconds: int = 60,
        max_attempts: int = 3,
        minimum_spacing_seconds: float = 1.0,
        enabled: bool = False,
    ) -> dict[str, Any]:
        return self._create_coinbase_definition(
            instrument_id=instrument_id,
            provider_product_id=provider_product_id,
            fact_type=OPEN_INTEREST_FACT_TYPE,
            contract_version=OPEN_INTEREST_FACT_VERSION,
            feature_id="open_interest_current",
            adapter_version=COINBASE_OI_ADAPTER_VERSION,
            unit="contracts",
            poll_interval_seconds=poll_interval_seconds,
            max_attempts=max_attempts,
            minimum_spacing_seconds=minimum_spacing_seconds,
            enabled=enabled,
            extra_config={},
            lineage={
                "provider_event_time_available": False,
            },
        )

    def create_coinbase_funding_rate_definition(
        self,
        *,
        instrument_id: str,
        provider_product_id: str,
        poll_interval_seconds: int = 60,
        max_attempts: int = 3,
        minimum_spacing_seconds: float = 1.0,
        enabled: bool = False,
    ) -> dict[str, Any]:
        return self._create_coinbase_definition(
            instrument_id=instrument_id,
            provider_product_id=provider_product_id,
            fact_type=FUNDING_RATE_FACT_TYPE,
            contract_version=FUNDING_RATE_FACT_VERSION,
            feature_id="funding_current",
            adapter_version=COINBASE_FUNDING_ADAPTER_VERSION,
            unit="fraction",
            poll_interval_seconds=poll_interval_seconds,
            max_attempts=max_attempts,
            minimum_spacing_seconds=minimum_spacing_seconds,
            enabled=enabled,
            extra_config={
                "funding_time_semantics": "provider_reported_unspecified",
            },
            lineage={
                "provider_funding_time_available": True,
                "provider_funding_time_semantics": "unspecified",
            },
        )

    def create_structured_fact_definition(
        self,
        *,
        manifest_path: str,
        binding_id: str,
        max_attempts: int = 3,
        minimum_spacing_seconds: float = 1.0,
        enabled: bool = False,
    ) -> dict[str, Any]:
        """Register an explicitly reviewed structured-provider polling binding."""

        manifest = load_structured_fact_manifest(manifest_path)
        binding = manifest.binding(binding_id)
        if (
            binding.adapter != CHAINLINK_MVR_ADAPTER_ID
            or binding.fact_type != RESERVE_STATE_FACT_TYPE
            or binding.payload_schema_id != RESERVE_STATE_FACT_VERSION
        ):
            raise ValueError(
                "market_collection_definition_invalid: unsupported structured binding"
            )
        attempts = int(max_attempts)
        spacing = float(minimum_spacing_seconds)
        poll_interval = int(binding.schedule["poll_interval_seconds"])
        if attempts < 1 or attempts > 10:
            raise ValueError(
                "market_collection_definition_invalid: max_attempts must be between 1 and 10"
            )
        if spacing < 0 or spacing > poll_interval:
            raise ValueError(
                "market_collection_definition_invalid: provider spacing is outside poll interval"
            )
        instrument = instrument_service.get_instrument_record(binding.instrument_id)
        provider_id = str(instrument.get("datasource") or "").strip().upper()
        venue_id = str(instrument.get("exchange") or "").strip().upper()
        if (
            provider_id != str(binding.source["provider"]).upper()
            or venue_id != str(binding.source["venue"]).upper()
        ):
            raise ValueError(
                "market_collection_definition_invalid: structured binding source "
                f"disagrees with instrument_id={binding.instrument_id}"
            )
        source = SourceIdentity(
            provider=str(binding.source["provider"]),
            venue=str(binding.source["venue"]),
            source_kind=str(binding.source["source_kind"]),
            adapter_version=str(binding.source["adapter_version"]),
        )
        source_id = self.store.register_source(
            source,
            lineage={
                "schema_version": "market.structured_fact_source_lineage.v1",
                "acquisition": "scheduled_poll",
                "manifest_id": manifest.id,
                "manifest_hash": manifest.manifest_hash,
                "binding_id": binding.id,
                "adapter": binding.adapter,
                "endpoint_ref": binding.endpoint_ref,
                "schedule": dict(binding.schedule),
                "quality_policy": dict(binding.quality_policy),
                "risk": dict(binding.risk),
            },
        )
        series_id = self.store.register_series(
            instrument_id=binding.instrument_id,
            fact_type=binding.fact_type,
            timeframe_seconds=None,
            contract_version=binding.payload_schema_id,
            dimensions=binding.dimensions,
        )
        identity = {
            "schema_version": COLLECTOR_DEFINITION_VERSION,
            "source_identity_key": source.identity_key,
            "instrument_id": binding.instrument_id,
            "fact_type": binding.fact_type,
            "contract_version": binding.payload_schema_id,
            "binding_id": binding.id,
        }
        definition_id = f"mcd_{_stable_hash(identity)[:32]}"
        now = self.clock().astimezone(UTC)
        epoch = int(now.timestamp())
        scheduled = datetime.fromtimestamp(
            epoch - (epoch % poll_interval), tz=UTC
        )
        config = {
            **identity,
            "adapter": binding.adapter,
            "manifest_id": manifest.id,
            "manifest_hash": manifest.manifest_hash,
            "manifest_path": manifest.path,
            "structured_binding": asdict(binding),
            "minimum_spacing_seconds": spacing,
            "retry_base_seconds": 2.0,
            "sample_time_method": "source_report_timestamp",
            "known_at_method": "platform_acceptance",
        }
        row = self.collection_repo.upsert_definition(
            definition_id=definition_id,
            source_id=source_id,
            series_id=series_id,
            poll_interval_seconds=poll_interval,
            max_attempts=attempts,
            enabled=bool(enabled),
            config=config,
            next_scheduled_at=scheduled,
        )
        return _public_row(row)

    def _create_coinbase_definition(
        self,
        *,
        instrument_id: str,
        provider_product_id: str,
        fact_type: str,
        contract_version: str,
        feature_id: str,
        adapter_version: str,
        unit: str,
        poll_interval_seconds: int,
        max_attempts: int,
        minimum_spacing_seconds: float,
        enabled: bool,
        extra_config: Mapping[str, Any],
        lineage: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Register one explicit Coinbase product and implemented fact handler."""

        instrument_id = str(instrument_id or "").strip()
        product_id = str(provider_product_id or "").strip().upper()
        interval = int(poll_interval_seconds)
        spacing = float(minimum_spacing_seconds)
        if not instrument_id or not product_id:
            raise ValueError(
                "market_collection_definition_invalid: instrument and provider product are required"
            )
        if interval < 10:
            raise ValueError(
                "market_collection_definition_invalid: poll interval must be at least 10 seconds"
            )
        if int(max_attempts) < 1 or int(max_attempts) > 10:
            raise ValueError(
                "market_collection_definition_invalid: max_attempts must be between 1 and 10"
            )
        if spacing < 0 or spacing > interval:
            raise ValueError(
                "market_collection_definition_invalid: provider spacing is outside poll interval"
            )
        instrument = instrument_service.get_instrument_record(instrument_id)
        capability = feature_contract("COINBASE", "COINBASE_DIRECT", feature_id)
        if capability.auth != FeatureAuth.PUBLIC:
            raise RuntimeError(
                "provider_feature_contract_invalid: Coinbase collector "
                f"requires declared public access feature={feature_id}"
            )

        provider_id = str(instrument.get("datasource") or "").strip().upper()
        venue_id = str(instrument.get("exchange") or "").strip().upper()
        if provider_id != "COINBASE" or venue_id != "COINBASE_DIRECT":
            raise ValueError(
                "market_collection_definition_invalid: Coinbase market facts require a "
                "COINBASE/COINBASE_DIRECT canonical instrument"
            )
        if fact_type == FUNDING_RATE_FACT_TYPE and not bool(
            instrument.get("has_funding")
        ):
            raise ValueError(
                "market_collection_definition_invalid: funding-rate collection "
                f"requires has_funding instrument_id={instrument_id}"
            )

        source = SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="poll_api",
            adapter_version=adapter_version,
        )
        source_id = self.store.register_source(
            source,
            lineage={
                "schema_version": "market_source_lineage.v1",
                "acquisition": "scheduled_poll",
                "provider_contract": "coinbase_advanced_trade_get_public_product",
                **dict(lineage),
            },
        )
        series_id = self.store.register_series(
            instrument_id=instrument_id,
            fact_type=fact_type,
            timeframe_seconds=None,
            contract_version=contract_version,
        )
        identity = {
            "schema_version": COLLECTOR_DEFINITION_VERSION,
            "source_identity_key": source.identity_key,
            "instrument_id": instrument_id,
            "fact_type": fact_type,
            "contract_version": contract_version,
            "provider_product_id": product_id,
        }
        definition_id = f"mcd_{_stable_hash(identity)[:32]}"
        now = self.clock().astimezone(UTC)
        epoch = int(now.timestamp())
        scheduled = datetime.fromtimestamp(epoch - (epoch % interval), tz=UTC)
        config = {
            **identity,
            "minimum_spacing_seconds": spacing,
            "retry_base_seconds": 2.0,
            "unit": unit,
            "sample_time_method": "collector_schedule",
            "known_at_method": "platform_acceptance",
            **dict(extra_config),
        }
        row = self.collection_repo.upsert_definition(
            definition_id=definition_id,
            source_id=source_id,
            series_id=series_id,
            poll_interval_seconds=interval,
            max_attempts=int(max_attempts),
            enabled=bool(enabled),
            config=config,
            next_scheduled_at=scheduled,
        )
        return _public_row(row)

    def list_definitions(
        self, *, definition_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        return [
            _public_row(row)
            for row in self.collection_repo.list_definitions(
                definition_id=definition_id
            )
        ]

    def list_attempts(
        self, *, definition_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        return [
            _public_row(row)
            for row in self.collection_repo.list_attempts(
                definition_id=definition_id, limit=limit
            )
        ]

    def collector_snapshot(self, *, attempt_limit: int = 5) -> dict[str, Any]:
        definitions = self.list_definitions()
        attempts = [
            _public_row(row)
            for row in self.collection_repo.list_recent_attempts(
                limit_per_definition=attempt_limit
            )
        ]
        grouped_attempts: dict[str, list[dict[str, Any]]] = {
            str(definition["id"]): [] for definition in definitions
        }
        for attempt in attempts:
            grouped_attempts.setdefault(str(attempt["definition_id"]), []).append(attempt)
        workers = [
            _public_row(row)
            for row in self.collection_repo.list_worker_states()
        ]
        active_workers = [worker for worker in workers if bool(worker.get("alive"))]
        observed_at = self.clock().astimezone(UTC).isoformat()
        return {
            "schema_version": "market_collector_snapshot.v1",
            "observed_at": observed_at,
            "worker_health": {
                "status": "alive" if active_workers else "unavailable",
                "active_count": len(active_workers),
                "known_count": len(workers),
                "observed_at": observed_at,
            },
            "workers": workers,
            "collectors": [
                {
                    "definition": definition,
                    "attempts": grouped_attempts.get(str(definition["id"]), []),
                    "attempts_available": True,
                }
                for definition in definitions
            ],
        }

    def register_worker(self, **kwargs: Any) -> dict[str, Any]:
        return _public_row(self.collection_repo.register_worker(**kwargs))

    def heartbeat_worker(self, **kwargs: Any) -> dict[str, Any]:
        return _public_row(self.collection_repo.heartbeat_worker(**kwargs))

    def stop_worker(self, **kwargs: Any) -> None:
        self.collection_repo.stop_worker(**kwargs)

    def set_enabled(self, definition_id: str, *, enabled: bool) -> dict[str, Any]:
        return _public_row(
            self.collection_repo.set_enabled(definition_id, enabled=enabled)
        )

    def claim_due(
        self, *, owner_id: str, lease_seconds: float = 90.0
    ) -> Optional[CollectionClaim]:
        return self.collection_repo.claim_due(
            owner_id=owner_id, lease_seconds=lease_seconds
        )

    def _record_missed_schedule(self, claim: CollectionClaim) -> None:
        if not claim.missed_start or claim.missed_count <= 0:
            return
        self.store.record_gap_evidence(
            series_id=claim.series_id,
            source_id=claim.source_id,
            start=claim.missed_start,
            end=claim.scheduled_for,
            classification="collection_schedule_missed",
            expected_count=claim.missed_count,
            observed_count=0,
            evidence={
                "schema_version": "market_collection_gap.v1",
                "definition_id": claim.definition_id,
                "provider": claim.provider,
                "venue": claim.venue,
                "fact_type": claim.fact_type,
                "reason": "collector_not_available_at_scheduled_times",
            },
        )

    def collect(
        self, claim: CollectionClaim, *, lease_seconds: float = 90.0
    ) -> dict[str, Any]:
        """Execute a claimed poll; all accepted writes carry the claim fence."""

        handlers = {
            (
                "COINBASE",
                "COINBASE_DIRECT",
                OPEN_INTEREST_FACT_TYPE,
                OPEN_INTEREST_FACT_VERSION,
            ): self._collect_open_interest,
            (
                "COINBASE",
                "COINBASE_DIRECT",
                FUNDING_RATE_FACT_TYPE,
                FUNDING_RATE_FACT_VERSION,
            ): self._collect_funding_rate,
            (
                "CHAINLINK",
                "ARBITRUM_MAINNET",
                RESERVE_STATE_FACT_TYPE,
                RESERVE_STATE_FACT_VERSION,
            ): self._collect_reserve_state,
        }
        handler_key = (
            claim.provider.upper(),
            claim.venue.upper(),
            claim.fact_type,
            claim.contract_version,
        )
        handler = handlers.get(handler_key)
        if handler is None:
            raise RuntimeError(
                "market_collection_handler_missing: "
                f"provider={handler_key[0]} venue={handler_key[1]} "
                f"fact_type={handler_key[2]} contract_version={handler_key[3]}"
            )
        self._record_missed_schedule(claim)
        attempt_started = self.monotonic()
        timing: dict[str, Any] = {
            "schema_version": "market_collection_attempt_timing.v1",
            "stage": "pacing_reservation",
            "timings_ms": {
                "schedule_lag": round(
                    max(0.0, (self.clock().astimezone(UTC) - claim.scheduled_for).total_seconds())
                    * 1000.0,
                    3,
                ),
            },
        }
        try:
            stage_started = self.monotonic()
            delay = self.collection_repo.reserve_provider_request(
                provider=claim.provider,
                minimum_spacing_seconds=float(
                    claim.config.get("minimum_spacing_seconds", 1.0)
                ),
            )
            timing["timings_ms"]["pacing_reservation"] = round(
                (self.monotonic() - stage_started) * 1000.0, 3
            )
            timing["timings_ms"]["pacing_wait_requested"] = round(delay * 1000.0, 3)
            if delay > 0:
                self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
                timing["stage"] = "pacing_wait"
                stage_started = self.monotonic()
                self.sleeper(delay)
                timing["timings_ms"]["pacing_wait"] = round(
                    (self.monotonic() - stage_started) * 1000.0, 3
                )
            else:
                timing["timings_ms"]["pacing_wait"] = 0.0
            self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
            timing["stage"] = "provider_factory"
            stage_started = self.monotonic()
            if str(claim.config.get("adapter") or "") == CHAINLINK_MVR_ADAPTER_ID:
                binding = self._structured_binding(claim.config)
                provider = self.structured_provider_builder(binding)
            else:
                provider = self.provider_factory(claim.provider, venue=claim.venue)
            timing["timings_ms"]["provider_factory"] = round(
                (self.monotonic() - stage_started) * 1000.0, 3
            )
            fact, outcome, snapshot = handler(claim, provider, lease_seconds, timing)
            timing["timings_ms"]["precomplete_total"] = round(
                (self.monotonic() - attempt_started) * 1000.0, 3
            )
            timing.pop("stage", None)
            evidence = {
                "schema_version": COLLECTOR_RESULT_VERSION,
                "response_hash": snapshot.response_hash,
                "row_hash": fact.row_hash,
                "known_at": fact.known_at.isoformat(),
                "market_commit_seq": outcome.max_commit_seq,
                "inserted_count": outcome.inserted_count,
                "noop_count": outcome.noop_count,
                "timing": timing,
            }
            self.collection_repo.complete(
                claim,
                ingestion_run_id=outcome.ingestion_run_id,
                evidence=evidence,
            )
            return {
                **claim.public_dict(),
                "schema_version": COLLECTOR_RESULT_VERSION,
                "status": "succeeded",
                "fact": fact.to_dict(),
                "outcome": asdict(outcome),
                "provenance": evidence,
            }
        except MarketCollectionOwnershipError:
            raise
        except Exception as exc:
            timing["timings_ms"]["attempt_total"] = round(
                (self.monotonic() - attempt_started) * 1000.0, 3
            )
            failure_stage = str(timing.pop("stage", "unknown"))
            exhausted = self.collection_repo.fail(
                claim,
                error=exc,
                retry_base_seconds=float(claim.config.get("retry_base_seconds", 2.0)),
                evidence={"failed_stage": failure_stage, "timing": timing},
            )
            if exhausted:
                self.store.record_gap_evidence(
                    series_id=claim.series_id,
                    source_id=claim.source_id,
                    start=claim.scheduled_for,
                    end=claim.scheduled_for
                    + timedelta(seconds=claim.poll_interval_seconds),
                    classification="collection_failed",
                    expected_count=1,
                    observed_count=0,
                    evidence={
                        "schema_version": "market_collection_gap.v1",
                        "definition_id": claim.definition_id,
                        "attempt_id": claim.attempt_id,
                        "attempts": claim.attempt_number,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:1000],
                    },
                )
            raise

    def _collect_open_interest(
        self,
        claim: CollectionClaim,
        provider: Any,
        lease_seconds: float,
        timing: dict[str, Any],
    ) -> tuple[OpenInterestFact, Any, ProviderOpenInterestSnapshot]:
        fetch = getattr(provider, "fetch_open_interest", None)
        if not callable(fetch):
            raise RuntimeError(
                "market_collection_provider_capability_missing: fetch_open_interest"
            )
        timing["stage"] = "provider_request"
        stage_started = self.monotonic()
        snapshot = fetch(str(claim.config["provider_product_id"]))
        timing["timings_ms"]["provider_request"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "provider_contract_validation"
        stage_started = self.monotonic()
        if not isinstance(snapshot, ProviderOpenInterestSnapshot):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: "
                "expected normalized OI snapshot"
            )
        timing["timings_ms"]["provider_contract_validation"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "canonical_normalization"
        stage_started = self.monotonic()
        accepted_at = max(
            self.clock().astimezone(UTC),
            snapshot.received_at,
            claim.scheduled_for,
        )
        source_published_at = snapshot.provider_event_at
        if source_published_at is not None:
            accepted_at = max(accepted_at, source_published_at)
        fact = OpenInterestFact(
            sample_time=claim.scheduled_for,
            sample_time_method="collector_schedule",
            value=snapshot.value,
            unit=snapshot.unit,
            source_published_at=source_published_at,
            received_at=snapshot.received_at,
            accepted_at=accepted_at,
            known_at=accepted_at,
            known_at_method="platform_acceptance",
        )
        timing["timings_ms"]["canonical_normalization"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "lease_heartbeat"
        stage_started = self.monotonic()
        self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
        timing["timings_ms"]["lease_heartbeat"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "persistence"
        stage_started = self.monotonic()
        outcome = self.store.ingest_open_interest(
            series_id=claim.series_id,
            source_id=claim.source_id,
            facts=[fact],
            request=self._ingestion_request(claim, snapshot.provider_product_id),
            provenance={
                "schema_version": "market_fact_provenance.v1",
                "response_hash": snapshot.response_hash,
                "source_path": snapshot.source_path,
                "provider_product_id": snapshot.provider_product_id,
                "provider_event_time_available": snapshot.provider_event_at is not None,
                "provider_metadata": dict(snapshot.metadata),
            },
            source_revision=snapshot.response_hash,
            ingestion_run_id=claim.attempt_id,
            allow_corrections=False,
            collection_fence=claim.fence(),
        )
        timing["timings_ms"]["persistence"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        return fact, outcome, snapshot

    def _collect_funding_rate(
        self,
        claim: CollectionClaim,
        provider: Any,
        lease_seconds: float,
        timing: dict[str, Any],
    ) -> tuple[FundingRateFact, Any, ProviderFundingRateSnapshot]:
        fetch = getattr(provider, "fetch_funding_rate", None)
        if not callable(fetch):
            raise RuntimeError(
                "market_collection_provider_capability_missing: fetch_funding_rate"
            )
        timing["stage"] = "provider_request"
        stage_started = self.monotonic()
        snapshot = fetch(str(claim.config["provider_product_id"]))
        timing["timings_ms"]["provider_request"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "provider_contract_validation"
        stage_started = self.monotonic()
        if not isinstance(snapshot, ProviderFundingRateSnapshot):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: "
                "expected normalized funding-rate snapshot"
            )
        timing["timings_ms"]["provider_contract_validation"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "canonical_normalization"
        stage_started = self.monotonic()
        accepted_at = max(
            self.clock().astimezone(UTC),
            snapshot.received_at,
            claim.scheduled_for,
        )
        fact = FundingRateFact(
            sample_time=claim.scheduled_for,
            sample_time_method="collector_schedule",
            rate=snapshot.rate,
            funding_time=snapshot.funding_time,
            interval_seconds=snapshot.interval_seconds,
            unit=snapshot.unit,
            source_published_at=None,
            received_at=snapshot.received_at,
            accepted_at=accepted_at,
            known_at=accepted_at,
            known_at_method="platform_acceptance",
        )
        timing["timings_ms"]["canonical_normalization"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "lease_heartbeat"
        stage_started = self.monotonic()
        self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
        timing["timings_ms"]["lease_heartbeat"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "persistence"
        stage_started = self.monotonic()
        outcome = self.store.ingest_funding_rates(
            series_id=claim.series_id,
            source_id=claim.source_id,
            facts=[fact],
            request=self._ingestion_request(claim, snapshot.provider_product_id),
            provenance={
                "schema_version": "market_fact_provenance.v1",
                "response_hash": snapshot.response_hash,
                "source_path": snapshot.source_path,
                "provider_product_id": snapshot.provider_product_id,
                "provider_funding_time": snapshot.funding_time.isoformat(),
                "provider_funding_time_semantics": "provider_reported_unspecified",
                "provider_metadata": dict(snapshot.metadata),
            },
            source_revision=snapshot.response_hash,
            ingestion_run_id=claim.attempt_id,
            allow_corrections=False,
            collection_fence=claim.fence(),
        )
        timing["timings_ms"]["persistence"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        return fact, outcome, snapshot

    def _collect_reserve_state(
        self,
        claim: CollectionClaim,
        provider: Any,
        lease_seconds: float,
        timing: dict[str, Any],
    ) -> tuple[CanonicalFact, Any, ProviderReserveStateSnapshot]:
        binding = self._structured_binding(claim.config)
        fetch = getattr(provider, "fetch_reserve_state", None)
        if not callable(fetch):
            raise RuntimeError(
                "market_collection_provider_capability_missing: fetch_reserve_state"
            )
        timing["stage"] = "provider_request"
        stage_started = self.monotonic()
        snapshot = fetch(binding)
        timing["timings_ms"]["provider_request"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "provider_contract_validation"
        stage_started = self.monotonic()
        if not isinstance(snapshot, ProviderReserveStateSnapshot):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: "
                "expected normalized reserve-state snapshot"
            )
        if (
            snapshot.subject_id != str(binding.config["subject_id"])
            or snapshot.reserve_asset != str(binding.dimensions["reserve_asset"])
        ):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: reserve subject disagreement"
            )
        timing["timings_ms"]["provider_contract_validation"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "canonical_normalization"
        stage_started = self.monotonic()
        accepted_at = max(self.clock().astimezone(UTC), snapshot.received_at)
        source = SourceIdentity(
            provider=str(binding.source["provider"]),
            venue=str(binding.source["venue"]),
            source_kind=str(binding.source["source_kind"]),
            adapter_version=str(binding.source["adapter_version"]),
        )
        fact = CanonicalFact(
            fact_type=binding.fact_type,
            payload_schema_id=binding.payload_schema_id,
            observation_key=snapshot.source_event_key,
            observation_time=snapshot.observation_time,
            observation_time_method="source_report_timestamp",
            source_published_at=snapshot.observation_time,
            received_at=snapshot.received_at,
            accepted_at=accepted_at,
            known_at=accepted_at,
            known_at_method="platform_acceptance",
            source=source,
            transformation_id="chainlink_mvr_reserve_state.v1",
            external_event_key=snapshot.source_event_key,
            external_event_group_key=str(binding.config["feed_id"]),
            payload={
                "report_id": snapshot.report_id,
                "reserve_asset": snapshot.reserve_asset,
                "reserve_quantity": snapshot.reserve_quantity,
                "unit": snapshot.reserve_asset,
            },
            provenance={
                "schema_version": "market.structured_fact_provenance.v1",
                "manifest_id": binding.manifest_id,
                "manifest_hash": binding.manifest_hash,
                "binding_id": binding.id,
                "source_path": snapshot.source_path,
                "response_hash": snapshot.response_hash,
                "provider_observation": dict(snapshot.metadata),
            },
            quality={
                "schema_version": "market.structured_fact_quality.v1",
                "expected_update_interval_seconds": int(
                    binding.schedule["expected_update_interval_seconds"]
                ),
                "max_staleness_seconds": int(
                    binding.quality_policy["max_staleness_seconds"]
                ),
                "age_seconds_at_receipt": int(
                    snapshot.metadata["age_seconds_at_receipt"]
                ),
                "gap": False,
            },
        )
        existing = self.store.read_facts(
            series_id=claim.series_id,
            start=snapshot.observation_time,
            end=snapshot.observation_time + timedelta(microseconds=1),
            source_identity_keys=(source.identity_key,),
        )
        matching = [
            record
            for record in existing
            if record.fact.observation_key == fact.observation_key
        ]
        if len(matching) > 1:
            raise RuntimeError(
                "market_collection_existing_fact_ambiguous: "
                f"observation_key={fact.observation_key}"
            )
        if matching:
            if matching[0].fact.material_hash != fact.material_hash:
                raise RuntimeError(
                    "market_collection_existing_fact_conflict: "
                    f"observation_key={fact.observation_key}"
                )
            fact = matching[0].fact
        timing["timings_ms"]["canonical_normalization"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "lease_heartbeat"
        stage_started = self.monotonic()
        self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
        timing["timings_ms"]["lease_heartbeat"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        timing["stage"] = "persistence"
        stage_started = self.monotonic()
        outcome = self.store.ingest_facts(
            series_id=claim.series_id,
            source_id=claim.source_id,
            facts=[fact],
            request=self._ingestion_request(claim, snapshot.subject_id),
            source_revision=snapshot.response_hash,
            ingestion_run_id=claim.attempt_id,
            allow_corrections=False,
            collection_fence=claim.fence(),
        )
        timing["timings_ms"]["persistence"] = round(
            (self.monotonic() - stage_started) * 1000.0, 3
        )
        return fact, outcome, snapshot

    @staticmethod
    def _ingestion_request(
        claim: CollectionClaim, provider_product_id: str
    ) -> dict[str, Any]:
        return {
            "schema_version": "market_ingestion_request.v1",
            "operation": "scheduled_fact_poll",
            "definition_id": claim.definition_id,
            "attempt_id": claim.attempt_id,
            "scheduled_for": claim.scheduled_for.isoformat(),
            "provider_product_id": provider_product_id,
        }

    def fact_history(
        self,
        *,
        definition_id: str,
        hours: int = 24,
        limit: int = 240,
    ) -> dict[str, Any]:
        """Return bounded canonical fact history for one collector definition."""

        definitions = self.list_definitions(definition_id=definition_id)
        if not definitions:
            raise ValueError(
                f"market_collection_definition_unknown: definition_id={definition_id}"
            )
        definition = definitions[0]
        fact_type = str(definition.get("fact_type") or "")
        bounded_hours = max(1, min(int(hours or 24), 24 * 7))
        bounded_limit = max(1, min(int(limit or 240), 1000))
        end = self.clock().astimezone(UTC)
        start = end - timedelta(hours=bounded_hours)
        records = self.store.read_facts(
            series_id=int(definition["series_id"]), start=start, end=end
        )

        samples = []
        for record in records[-bounded_limit:]:
            fact = record.fact.to_dict()
            fact = {
                key: (
                    value.astimezone(UTC).isoformat()
                    if isinstance(value, datetime)
                    else value
                )
                for key, value in fact.items()
            }
            samples.append(
                {
                    "series_id": record.series_id,
                    "revision": record.revision,
                    "market_commit_seq": record.market_commit_seq,
                    "ingestion_run_id": record.ingestion_run_id,
                    "source_identity_key": record.source_identity_key,
                    "source": {
                        "provider": record.source.provider,
                        "venue": record.source.venue,
                        "source_kind": record.source.source_kind,
                        "adapter_version": record.source.adapter_version,
                    },
                    "provenance": dict(record.provenance),
                    "fact": fact,
                }
            )
        return {
            "schema_version": "market_collector_fact_history.v1",
            "definition_id": str(definition_id),
            "series_id": int(definition["series_id"]),
            "fact_type": fact_type,
            "range_start": start.isoformat(),
            "range_end": end.isoformat(),
            "samples": samples,
            "truncated": len(records) > bounded_limit,
            "observed_at": end.isoformat(),
        }

    def latest_open_interest(
        self,
        *,
        instrument_id: str,
        decision_time: datetime,
        max_staleness_seconds: int,
        required: bool = True,
    ) -> Any:
        """Read causally visible OI without provider fallback for paper/runtime use."""

        return self._latest_fact(
            key="open_interest",
            instrument_id=instrument_id,
            fact_type=OPEN_INTEREST_FACT_TYPE,
            contract_version=OPEN_INTEREST_FACT_VERSION,
            read_records=self.store.read_open_interest,
            decision_time=decision_time,
            max_staleness_seconds=max_staleness_seconds,
            required=required,
        )

    def latest_funding_rate(
        self,
        *,
        instrument_id: str,
        decision_time: datetime,
        max_staleness_seconds: int,
        required: bool = True,
    ) -> Any:
        """Read causally visible funding without provider fallback."""

        return self._latest_fact(
            key="funding_rate",
            instrument_id=instrument_id,
            fact_type=FUNDING_RATE_FACT_TYPE,
            contract_version=FUNDING_RATE_FACT_VERSION,
            read_records=self.store.read_funding_rates,
            decision_time=decision_time,
            max_staleness_seconds=max_staleness_seconds,
            required=required,
        )

    def _latest_fact(
        self,
        *,
        key: str,
        instrument_id: str,
        fact_type: str,
        contract_version: str,
        read_records: Callable[..., list[Any]],
        decision_time: datetime,
        max_staleness_seconds: int,
        required: bool,
    ) -> Any:
        requirement = MarketDataRequirement(
            key=key,
            fact_type=fact_type,
            contract_version=contract_version,
            instrument_role="explicit",
            instrument_ref=instrument_id,
            alignment="latest_known",
            max_staleness_seconds=max_staleness_seconds,
            required=required,
        )
        try:
            series_id = self.store.resolve_series_id(
                instrument_id=instrument_id,
                fact_type=fact_type,
                timeframe_seconds=None,
                contract_version=contract_version,
            )
        except ValueError as exc:
            unavailable = UnavailableMarketData(
                key=requirement.key,
                reason="series_missing",
                evaluation_time=decision_time,
                details={
                    "fact_type": requirement.fact_type,
                    "instrument_id": instrument_id,
                    "error": str(exc),
                },
            )
            if required:
                raise RuntimeError(
                    "market_data_required_unavailable: "
                    f"key={requirement.key} reason={unavailable.reason}"
                ) from exc
            return unavailable
        decision = decision_time.astimezone(UTC)
        records = read_records(
            series_id=series_id,
            start=decision - timedelta(seconds=max_staleness_seconds),
            end=decision + timedelta(microseconds=1),
            known_at_lte=decision,
        )
        result = latest_known_record(
            records=records,
            evaluation_time=decision,
            max_staleness_seconds=max_staleness_seconds,
        )
        if isinstance(result, UnavailableMarketData):
            unavailable = UnavailableMarketData(
                key=requirement.key,
                reason=result.reason,
                evaluation_time=result.evaluation_time,
                details={
                    **dict(result.details),
                    "fact_type": requirement.fact_type,
                    "instrument_id": instrument_id,
                },
            )
            if required:
                raise RuntimeError(
                    "market_data_required_unavailable: "
                    f"key={requirement.key} reason={unavailable.reason}"
                )
            return unavailable
        return result


market_data_collector = MarketDataCollectorService()


__all__ = [
    "COINBASE_FUNDING_ADAPTER_VERSION",
    "COINBASE_OI_ADAPTER_VERSION",
    "MarketDataCollectorService",
    "market_data_collector",
]

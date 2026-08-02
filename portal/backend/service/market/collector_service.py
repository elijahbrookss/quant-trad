"""Provider-neutral collection orchestration with Coinbase OI as fact handler v1."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Optional

from data_providers.facts import (
    ProviderFundingRateSnapshot,
    ProviderOpenInterestSnapshot,
)
from data_providers.providers.factory import get_provider
from data_providers.registry import FeatureAuth, feature_contract
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
    ) -> None:
        self.collection_repo = collection_repo
        self.store = store
        self.provider_factory = provider_factory
        self.clock = clock
        self.sleeper = sleeper

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
        try:
            delay = self.collection_repo.reserve_provider_request(
                provider=claim.provider,
                minimum_spacing_seconds=float(
                    claim.config.get("minimum_spacing_seconds", 1.0)
                ),
            )
            if delay > 0:
                self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
                self.sleeper(delay)
            self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
            provider = self.provider_factory(claim.provider, venue=claim.venue)
            fact, outcome, snapshot = handler(claim, provider, lease_seconds)
            evidence = {
                "schema_version": COLLECTOR_RESULT_VERSION,
                "response_hash": snapshot.response_hash,
                "row_hash": fact.row_hash,
                "known_at": fact.known_at.isoformat(),
                "market_commit_seq": outcome.max_commit_seq,
                "inserted_count": outcome.inserted_count,
                "noop_count": outcome.noop_count,
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
            exhausted = self.collection_repo.fail(
                claim,
                error=exc,
                retry_base_seconds=float(claim.config.get("retry_base_seconds", 2.0)),
            )
            if exhausted:
                self.store.record_gap_evidence(
                    series_id=claim.series_id,
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
        self, claim: CollectionClaim, provider: Any, lease_seconds: float
    ) -> tuple[OpenInterestFact, Any, ProviderOpenInterestSnapshot]:
        fetch = getattr(provider, "fetch_open_interest", None)
        if not callable(fetch):
            raise RuntimeError(
                "market_collection_provider_capability_missing: fetch_open_interest"
            )
        snapshot = fetch(str(claim.config["provider_product_id"]))
        if not isinstance(snapshot, ProviderOpenInterestSnapshot):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: "
                "expected normalized OI snapshot"
            )
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
        self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
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
        return fact, outcome, snapshot

    def _collect_funding_rate(
        self, claim: CollectionClaim, provider: Any, lease_seconds: float
    ) -> tuple[FundingRateFact, Any, ProviderFundingRateSnapshot]:
        fetch = getattr(provider, "fetch_funding_rate", None)
        if not callable(fetch):
            raise RuntimeError(
                "market_collection_provider_capability_missing: fetch_funding_rate"
            )
        snapshot = fetch(str(claim.config["provider_product_id"]))
        if not isinstance(snapshot, ProviderFundingRateSnapshot):
            raise RuntimeError(
                "market_collection_provider_contract_invalid: "
                "expected normalized funding-rate snapshot"
            )
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
        self.collection_repo.heartbeat(claim, lease_seconds=lease_seconds)
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

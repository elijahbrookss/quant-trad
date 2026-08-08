from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from data_providers.facts import (
    ProviderFundingRateSnapshot,
    ProviderOpenInterestSnapshot,
)
from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
)
from market_data.store import IngestionOutcome
from portal.backend.service.market.collector_service import MarketDataCollectorService
from portal.backend.service.storage.repos.market_collection import CollectionClaim


UTC = timezone.utc
SCHEDULED = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def _claim(**changes: Any) -> CollectionClaim:
    values = {
        "definition_id": "mcd_test",
        "attempt_id": "mca_test",
        "scheduled_for": SCHEDULED,
        "attempt_number": 1,
        "max_attempts": 3,
        "poll_interval_seconds": 60,
        "source_id": 11,
        "series_id": 22,
        "provider": "COINBASE",
        "venue": "COINBASE_DIRECT",
        "source_kind": "poll_api",
        "adapter_version": "test.v1",
        "instrument_id": "instrument-1",
        "fact_type": "derivatives.open_interest",
        "contract_version": "derivatives.open_interest.v1",
        "config": {
            "provider_product_id": "BTC-PERP-INTX",
            "minimum_spacing_seconds": 1.0,
            "retry_base_seconds": 2.0,
        },
        "owner_id": "worker-1",
        "lease_token": "secret-claim-token",
        "lease_generation": 7,
        "lease_expires_at": SCHEDULED + timedelta(seconds=90),
    }
    values.update(changes)
    return CollectionClaim(**values)


class _CollectionRepo:
    def __init__(self, *, exhausted: bool = False) -> None:
        self.exhausted = exhausted
        self.heartbeats: list[Any] = []
        self.completed: list[Any] = []
        self.failed: list[Any] = []

    def reserve_provider_request(self, **_kwargs: Any) -> float:
        return 0.0

    def heartbeat(self, claim: CollectionClaim, **_kwargs: Any) -> None:
        self.heartbeats.append(claim)

    def complete(self, claim: CollectionClaim, **kwargs: Any) -> None:
        self.completed.append((claim, kwargs))

    def fail(self, claim: CollectionClaim, **kwargs: Any) -> bool:
        self.failed.append((claim, kwargs))
        return self.exhausted


class _Store:
    def __init__(self) -> None:
        self.ingestions: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []

    def ingest_open_interest(self, **kwargs: Any) -> IngestionOutcome:
        self.ingestions.append(dict(kwargs))
        return IngestionOutcome(
            ingestion_run_id=str(kwargs["ingestion_run_id"]),
            requested_count=1,
            inserted_count=1,
            corrected_count=0,
            noop_count=0,
            max_commit_seq=41,
        )

    def ingest_funding_rates(self, **kwargs: Any) -> IngestionOutcome:
        self.ingestions.append(dict(kwargs))
        return IngestionOutcome(
            ingestion_run_id=str(kwargs["ingestion_run_id"]),
            requested_count=1,
            inserted_count=1,
            corrected_count=0,
            noop_count=0,
            max_commit_seq=42,
        )

    def record_gap_evidence(self, **kwargs: Any) -> str:
        self.gaps.append(dict(kwargs))
        return "gap-hash"


def test_collector_snapshot_groups_attempts_and_exposes_worker_liveness() -> None:
    class SnapshotRepo:
        def list_definitions(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [{
                "id": "mcd_test",
                "enabled": True,
                "provider": "COINBASE",
                "instrument_id": "instrument-1",
                "instrument_symbol": "BIP-20DEC30-CDE",
            }]

        def list_recent_attempts(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [{
                "id": "mca_test",
                "definition_id": "mcd_test",
                "status": "succeeded",
                "started_at": SCHEDULED,
            }]

        def list_worker_states(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [{
                "worker_id": "worker-1",
                "state": "idle",
                "heartbeat_at": SCHEDULED,
                "alive": True,
            }]

    service = MarketDataCollectorService(
        collection_repo=SnapshotRepo(),
        store=_Store(),
        clock=lambda: SCHEDULED,
    )

    snapshot = service.collector_snapshot(attempt_limit=3)

    assert snapshot["schema_version"] == "market_collector_snapshot.v1"
    assert snapshot["worker_health"]["status"] == "alive"
    assert snapshot["collectors"][0]["definition"]["instrument_symbol"] == (
        "BIP-20DEC30-CDE"
    )
    assert snapshot["collectors"][0]["attempts"][0]["id"] == "mca_test"


def test_fact_history_is_bounded_and_uses_canonical_typed_store() -> None:
    class HistoryRepo:
        def list_definitions(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [
                {
                    "id": "mcd_test",
                    "series_id": 22,
                    "fact_type": "derivatives.open_interest",
                }
            ]

    class HistoryStore(_Store):
        def __init__(self) -> None:
            super().__init__()
            self.reads = []

        def read_open_interest(self, **kwargs: Any):
            self.reads.append(kwargs)
            return [
                SimpleNamespace(
                    series_id=22,
                    revision=1,
                    market_commit_seq=41,
                    ingestion_run_id="mca_test",
                    source_identity_key="source-test",
                    source=SimpleNamespace(
                        provider="COINBASE",
                        venue="COINBASE_DIRECT",
                        source_kind="poll_api",
                        adapter_version="test.v1",
                    ),
                    provenance={"response_hash": "a" * 64},
                    fact=SimpleNamespace(
                        to_dict=lambda: {
                            "sample_time": SCHEDULED,
                            "known_at": SCHEDULED + timedelta(seconds=1),
                            "value": 42.0,
                            "unit": "contracts",
                        }
                    ),
                )
            ]

    store = HistoryStore()
    service = MarketDataCollectorService(
        collection_repo=HistoryRepo(), store=store, clock=lambda: SCHEDULED
    )

    result = service.fact_history(
        definition_id="mcd_test", hours=999, limit=9999
    )

    assert result["schema_version"] == "market_collector_fact_history.v1"
    assert result["samples"][0]["fact"]["value"] == 42.0
    assert result["samples"][0]["source"]["provider"] == "COINBASE"
    assert store.reads[0]["start"] == SCHEDULED - timedelta(days=7)


def test_collection_accepts_one_fenced_known_at_open_interest_fact() -> None:
    received_at = SCHEDULED + timedelta(seconds=2)
    snapshot = ProviderOpenInterestSnapshot(
        provider_product_id="BTC-PERP-INTX",
        value=12345,
        received_at=received_at,
        response_hash="a" * 64,
        source_path="future_product_details.open_interest",
        metadata={"contract_code": "BIP"},
    )

    class Provider:
        def fetch_open_interest(self, product_id: str) -> ProviderOpenInterestSnapshot:
            assert product_id == "BTC-PERP-INTX"
            return snapshot

    repo = _CollectionRepo()
    store = _Store()
    claim = _claim(
        missed_start=SCHEDULED - timedelta(minutes=2),
        missed_count=2,
    )
    service = MarketDataCollectorService(
        collection_repo=repo,
        store=store,
        provider_factory=lambda provider, **kwargs: Provider(),
        clock=lambda: SCHEDULED + timedelta(seconds=3),
        sleeper=lambda _seconds: None,
    )

    result = service.collect(claim)

    assert result["status"] == "succeeded"
    assert len(store.ingestions) == 1
    ingestion = store.ingestions[0]
    fact = ingestion["facts"][0]
    assert fact.sample_time == SCHEDULED
    assert fact.received_at == received_at
    assert fact.known_at == SCHEDULED + timedelta(seconds=3)
    assert ingestion["allow_corrections"] is False
    assert ingestion["collection_fence"] == claim.fence()
    assert ingestion["provenance"]["response_hash"] == "a" * 64
    assert repo.completed[0][1]["ingestion_run_id"] == "mca_test"
    timing = repo.completed[0][1]["evidence"]["timing"]
    assert timing["schema_version"] == "market_collection_attempt_timing.v1"
    assert timing["timings_ms"]["provider_request"] >= 0
    assert timing["timings_ms"]["canonical_normalization"] >= 0
    assert timing["timings_ms"]["persistence"] >= 0
    assert store.gaps[0]["classification"] == "collection_schedule_missed"
    assert store.gaps[0]["expected_count"] == 2


def test_exhausted_collection_failure_records_explicit_quality_gap() -> None:
    class Provider:
        def fetch_open_interest(self, _product_id: str) -> Any:
            raise ValueError("provider response omitted open interest")

    repo = _CollectionRepo(exhausted=True)
    store = _Store()
    service = MarketDataCollectorService(
        collection_repo=repo,
        store=store,
        provider_factory=lambda provider, **kwargs: Provider(),
        clock=lambda: SCHEDULED + timedelta(seconds=3),
        sleeper=lambda _seconds: None,
    )

    with pytest.raises(ValueError, match="omitted open interest"):
        service.collect(_claim(attempt_number=3, max_attempts=3))

    assert len(repo.failed) == 1
    failure_evidence = repo.failed[0][1]["evidence"]
    assert failure_evidence["failed_stage"] == "provider_request"
    assert failure_evidence["timing"]["timings_ms"]["attempt_total"] >= 0
    assert store.gaps == [
        {
            "series_id": 22,
            "source_id": 11,
            "start": SCHEDULED,
            "end": SCHEDULED + timedelta(seconds=60),
            "classification": "collection_failed",
            "expected_count": 1,
            "observed_count": 0,
            "evidence": {
                "schema_version": "market_collection_gap.v1",
                "definition_id": "mcd_test",
                "attempt_id": "mca_test",
                "attempts": 3,
                "error_type": "ValueError",
                "error": "provider response omitted open interest",
            },
        }
    ]


def test_collection_accepts_signed_funding_without_inventing_event_time() -> None:
    received_at = SCHEDULED + timedelta(seconds=2)
    funding_time = SCHEDULED + timedelta(minutes=58)
    snapshot = ProviderFundingRateSnapshot(
        provider_product_id="ETP-20DEC30-CDE",
        rate=-0.000017,
        funding_time=funding_time,
        interval_seconds=3600,
        received_at=received_at,
        response_hash="b" * 64,
        source_path="future_product_details.funding_rate",
        metadata={"contract_code": "ETP"},
    )

    class Provider:
        def fetch_funding_rate(
            self, product_id: str
        ) -> ProviderFundingRateSnapshot:
            assert product_id == "ETP-20DEC30-CDE"
            return snapshot

    repo = _CollectionRepo()
    store = _Store()
    claim = _claim(
        fact_type=FUNDING_RATE_FACT_TYPE,
        contract_version=FUNDING_RATE_FACT_VERSION,
        config={
            "provider_product_id": "ETP-20DEC30-CDE",
            "minimum_spacing_seconds": 1.0,
            "retry_base_seconds": 2.0,
        },
    )
    service = MarketDataCollectorService(
        collection_repo=repo,
        store=store,
        provider_factory=lambda provider, **kwargs: Provider(),
        clock=lambda: SCHEDULED + timedelta(seconds=3),
        sleeper=lambda _seconds: None,
    )

    result = service.collect(claim)

    fact = store.ingestions[0]["facts"][0]
    provenance = store.ingestions[0]["provenance"]
    assert result["status"] == "succeeded"
    assert fact.rate == -0.000017
    assert fact.funding_time == funding_time
    assert fact.source_published_at is None
    assert fact.known_at == SCHEDULED + timedelta(seconds=3)
    assert provenance["provider_funding_time_semantics"] == (
        "provider_reported_unspecified"
    )
    assert store.ingestions[0]["collection_fence"] == claim.fence()

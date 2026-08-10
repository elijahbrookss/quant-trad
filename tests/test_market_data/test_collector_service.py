from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from data_providers.facts import (
    ProviderFundingRateSnapshot,
    ProviderOpenInterestSnapshot,
    ProviderReserveStateSnapshot,
)
from data_providers.structured_facts import load_structured_fact_manifest
from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
)
from market_data.store import IngestionOutcome
from portal.backend.service.market.collector_service import MarketDataCollectorService
from portal.backend.service.storage.repos.market_collection import CollectionClaim


UTC = timezone.utc
SCHEDULED = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
_ROOT = Path(__file__).resolve().parents[2]
_STRUCTURED_MANIFEST = (
    _ROOT
    / "config"
    / "market-data"
    / "structured-facts"
    / "chainlink-nxtassets-btc-etp-reserves.json"
)


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
        self.existing: list[Any] = []

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

    def read_facts(self, **_kwargs: Any) -> list[Any]:
        return list(self.existing)

    def ingest_facts(self, **kwargs: Any) -> IngestionOutcome:
        self.ingestions.append(dict(kwargs))
        fact = list(kwargs["facts"])[0]
        exists = any(
            record.fact.observation_key == fact.observation_key
            for record in self.existing
        )
        if not exists:
            self.existing.append(SimpleNamespace(fact=fact))
        return IngestionOutcome(
            ingestion_run_id=str(kwargs["ingestion_run_id"]),
            requested_count=1,
            inserted_count=0 if exists else 1,
            corrected_count=0,
            noop_count=1 if exists else 0,
            max_commit_seq=43,
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

        def read_facts(self, **kwargs: Any):
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


def test_structured_collector_appends_updates_and_reuses_identical_report() -> None:
    binding = load_structured_fact_manifest(_STRUCTURED_MANIFEST).binding(
        "nxtassets-btc-direct-etp-reserves"
    )
    observation_time = SCHEDULED - timedelta(hours=1)
    snapshot = ProviderReserveStateSnapshot(
        subject_id="DE000NXTA018",
        report_id="DE000NXTA018",
        reserve_asset="BTC",
        reserve_quantity=Decimal("514.32323119"),
        raw_reserve_quantity="51432323119",
        observation_time=observation_time,
        received_at=SCHEDULED + timedelta(seconds=2),
        response_hash="c" * 64,
        source_path="evm://42161/proxy/latestBundle",
        source_event_key="feed:report:bundle",
        metadata={
            "age_seconds_at_receipt": 3602,
            "bundle": "0x1234",
            "confirmed_head_block": 9980,
        },
    )

    class Provider:
        def fetch_reserve_state(self, actual_binding):
            assert actual_binding == binding
            return snapshot

    config = {
        "adapter": binding.adapter,
        "manifest_hash": binding.manifest_hash,
        "structured_binding": asdict(binding),
        "minimum_spacing_seconds": 1.0,
        "retry_base_seconds": 2.0,
    }
    claim = _claim(
        poll_interval_seconds=3600,
        provider="CHAINLINK",
        venue="ARBITRUM_MAINNET",
        source_kind="public_evm_contract",
        adapter_version=binding.adapter,
        instrument_id=binding.instrument_id,
        fact_type="asset.reserve_state",
        contract_version="asset.reserve_state.v1",
        config=config,
    )
    repo = _CollectionRepo()
    store = _Store()
    service = MarketDataCollectorService(
        collection_repo=repo,
        store=store,
        structured_provider_builder=lambda actual_binding: Provider(),
        clock=lambda: SCHEDULED + timedelta(seconds=3),
        sleeper=lambda _seconds: None,
    )

    first = service.collect(claim)
    second = service.collect(
        _claim(
            attempt_id="mca_test_second",
            scheduled_for=SCHEDULED + timedelta(hours=1),
            lease_expires_at=SCHEDULED + timedelta(hours=1, seconds=90),
            poll_interval_seconds=3600,
            provider="CHAINLINK",
            venue="ARBITRUM_MAINNET",
            source_kind="public_evm_contract",
            adapter_version=binding.adapter,
            instrument_id=binding.instrument_id,
            fact_type="asset.reserve_state",
            contract_version="asset.reserve_state.v1",
            config=config,
        )
    )

    first_fact = store.ingestions[0]["facts"][0]
    second_fact = store.ingestions[1]["facts"][0]
    assert first["outcome"]["inserted_count"] == 1
    assert second["outcome"]["noop_count"] == 1
    assert first_fact is second_fact
    assert first_fact.payload == {
        "report_id": "DE000NXTA018",
        "reserve_asset": "BTC",
        "reserve_quantity": "514.32323119",
        "unit": "BTC",
    }
    assert first_fact.known_at == SCHEDULED + timedelta(seconds=3)
    assert first_fact.observation_time == observation_time
    assert first_fact.provenance["provider_observation"]["bundle"] == "0x1234"
    assert store.ingestions[0]["allow_corrections"] is False
    assert store.ingestions[0]["collection_fence"] == claim.fence()


def test_structured_definition_is_manifest_bound_and_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class DefinitionRepo:
        def upsert_definition(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {
                "id": kwargs["definition_id"],
                "source_id": kwargs["source_id"],
                "series_id": kwargs["series_id"],
                "enabled": kwargs["enabled"],
                "config": dict(kwargs["config"]),
            }

    class DefinitionStore:
        def register_source(self, identity, **kwargs: Any) -> int:
            captured["source"] = identity
            captured["lineage"] = kwargs["lineage"]
            return 71

        def register_series(self, **kwargs: Any) -> int:
            captured["series"] = kwargs
            return 72

    monkeypatch.setattr(
        "portal.backend.service.market.collector_service.instrument_service.get_instrument_record",
        lambda _instrument_id: {
            "datasource": "CHAINLINK",
            "exchange": "ARBITRUM_MAINNET",
        },
    )
    service = MarketDataCollectorService(
        collection_repo=DefinitionRepo(),
        store=DefinitionStore(),
        clock=lambda: SCHEDULED,
    )

    result = service.create_structured_fact_definition(
        manifest_path=str(_STRUCTURED_MANIFEST),
        binding_id="nxtassets-btc-direct-etp-reserves",
    )

    assert result["enabled"] is False
    assert captured["poll_interval_seconds"] == 3600
    assert captured["source"].provider == "CHAINLINK"
    assert captured["series"] == {
        "instrument_id": "nxtassets-de000nxta018",
        "fact_type": "asset.reserve_state",
        "timeframe_seconds": None,
        "contract_version": "asset.reserve_state.v1",
        "dimensions": {"reserve_asset": "BTC"},
    }
    assert captured["config"]["structured_binding"]["endpoint_ref"] == (
        "CHAINLINK_ARBITRUM_RPC_URL"
    )
    assert "endpoint" not in captured["config"]


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

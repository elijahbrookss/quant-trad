from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from data_providers.numeric_facts import (
    NumericAcquisitionBudget,
    NumericFactBinding,
    ProviderNumericBatch,
    ProviderNumericGap,
    ProviderNumericObservation,
)
from market_data.contracts import (
    NumericFact,
    NumericFactRecord,
    NumericFactState,
    SourceIdentity,
)
from market_data.store import IngestionOutcome
from portal.backend.service.market.numeric_fact_acquisition import (
    NumericAcquisitionAuthorization,
    NumericFactAcquisitionService,
)


_START = datetime(2026, 8, 7, 20, tzinfo=UTC)
_END = _START + timedelta(hours=1)
_BUDGET = NumericAcquisitionBudget(
    max_requests=100,
    max_logs=100,
    max_blocks=10_000,
)
_AUTHORIZED = NumericAcquisitionAuthorization(
    network_allowed=True,
    actor="numeric-fact-test",
    reason="bounded offline provider fake",
)


def _write_manifest(tmp_path: Path) -> Path:
    path = tmp_path / "numeric-facts.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "market.numeric_fact_sources.v1",
                "id": "test-chainlink-eth-usd",
                "enabled": True,
                "bindings": [
                    {
                        "id": "eth-usd",
                        "enabled": True,
                        "adapter": "chainlink_aggregator_v3.v1",
                        "instrument_id": "instrument-eth",
                        "instrument_role": "benchmark",
                        "fact_type": "market.reference_price",
                        "contract_version": "market.reference_price.v1",
                        "unit": "USD",
                        "dimensions": {"quote_currency": "USD"},
                        "endpoint_ref": "TEST_CHAINLINK_RPC_URL",
                        "source": {
                            "provider": "CHAINLINK",
                            "venue": "ETHEREUM_MAINNET",
                            "source_kind": "public_evm_contract",
                            "adapter_version": "chainlink_aggregator_v3.v1",
                        },
                        "schedule": {
                            "expected_update_interval_seconds": None,
                            "deviation_threshold_basis_points": 50,
                        },
                        "quality_policy": {
                            "max_staleness_seconds": 7200,
                            "stale_behavior": "gap",
                        },
                        "risk": {
                            "official_catalog_url": "https://data.chain.link/test",
                            "market_risk_tier": "test",
                            "deprecation_status": "not_marked_deprecated",
                            "verified_at": "2026-08-07",
                        },
                        "config": {
                            "chain_id": 1,
                            "network": "ethereum-mainnet",
                            "proxy_address": (
                                "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
                            ),
                            "deployment_block": 0,
                            "history_start": "2026-08-07T00:00:00Z",
                            "confirmations": 12,
                            "max_log_span": 2000,
                            "current_lookback_blocks": 50000,
                            "expected_decimals": 8,
                            "expected_description": "ETH / USD",
                            "expected_version": None,
                        },
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _observation(
    event: str,
    value: str,
    *,
    minute: int,
    block_hash: str,
) -> ProviderNumericObservation:
    effective_at = _START + timedelta(minutes=minute)
    return ProviderNumericObservation(
        value=Decimal(value),
        raw_value=str(Decimal(value) * Decimal(100_000_000)).split(".")[0],
        effective_at=effective_at,
        effective_at_method="chainlink_round_updated_at",
        source_published_at=effective_at,
        known_at=effective_at + timedelta(minutes=1),
        known_at_method="evm_confirmation_block",
        source_event_key=event,
        source_event_group_key=event.removesuffix(":answer"),
        source_event_component_key="answer",
        provenance={
            "block_hash": block_hash,
            "transaction_hash": f"{block_hash}:transaction",
        },
    )


def _batch(
    *observations: ProviderNumericObservation,
    status: str = "complete",
    gaps: tuple[ProviderNumericGap, ...] = (),
    range_start: datetime = _START,
    range_end: datetime = _END,
    requests_used: int = 0,
    logs_used: int = 0,
    blocks_scanned: int = 0,
) -> ProviderNumericBatch:
    return ProviderNumericBatch(
        observations=tuple(observations),
        gaps=gaps,
        range_start=range_start,
        range_end=range_end,
        source_position_start="100",
        source_position_end="200",
        source_position_head="212",
        status=status,
        capabilities={"bounded_logs": True, "archive_block_read": True},
        request={"mode": "historical", "start_block": 100, "end_block": 200},
        budget_requests_used=requests_used,
        budget_logs_used=logs_used,
        budget_blocks_scanned=blocks_scanned,
    )


class _Provider:
    adapter_id = "chainlink_aggregator_v3.v1"

    def __init__(self, batches: Iterable[ProviderNumericBatch]) -> None:
        self.batches = list(batches)
        self.history_calls: list[tuple[datetime, datetime]] = []
        self.history_budgets: list[NumericAcquisitionBudget] = []
        self.current_calls = 0

    def fetch_history(
        self,
        binding: NumericFactBinding,
        *,
        start: datetime,
        end: datetime,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        assert binding.id == "eth-usd"
        self.history_calls.append((start, end))
        self.history_budgets.append(budget)
        batch = self.batches.pop(0)
        assert (batch.range_start, batch.range_end) == (start, end)
        return batch

    def fetch_current(
        self,
        binding: NumericFactBinding,
        *,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        self.current_calls += 1
        return self.batches.pop(0)


class _MemoryStore:
    def __init__(self) -> None:
        self.source_id = 17
        self.series_id = 23
        self.source: SourceIdentity | None = None
        self.source_lineage: list[Mapping[str, Any]] = []
        self.series_registrations: list[Mapping[str, Any]] = []
        self.revisions: dict[str, list[NumericFactRecord]] = {}
        self.coverage: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.ingestions: list[dict[str, Any]] = []
        self._commit = 0
        self._run = 0

    def register_source(
        self,
        identity: SourceIdentity,
        *,
        lineage: Mapping[str, Any] | None = None,
    ) -> int:
        self.source = identity
        self.source_lineage.append(dict(lineage or {}))
        return self.source_id

    def register_series(self, **kwargs: Any) -> int:
        self.series_registrations.append(dict(kwargs))
        return self.series_id

    def read_numeric_facts(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[NumericFactRecord]:
        assert series_id == self.series_id
        visible = [rows[-1] for rows in self.revisions.values() if rows]
        return sorted(
            [
                record
                for record in visible
                if start <= record.fact.effective_at < end
                and record.fact.state is NumericFactState.ACTIVE
            ],
            key=lambda record: (
                record.fact.effective_at,
                record.fact.source_event_key,
            ),
        )

    def read_numeric_fact_revisions(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        **_kwargs: Any,
    ) -> list[NumericFactRecord]:
        assert series_id == self.series_id
        return sorted(
            [
                record
                for rows in self.revisions.values()
                for record in rows
                if start <= record.fact.effective_at < end
            ],
            key=lambda record: (
                record.fact.effective_at,
                record.fact.source_event_key,
                record.revision,
            ),
        )

    def ingest_numeric_facts(
        self,
        *,
        facts: Iterable[NumericFact],
        request: Mapping[str, Any] | None = None,
        provenance: Mapping[str, Any] | None = None,
        provenance_by_event: Mapping[str, Mapping[str, Any]] | None = None,
        **_kwargs: Any,
    ) -> IngestionOutcome:
        assert self.source is not None
        self._run += 1
        run_id = f"numeric-run-{self._run}"
        inserted = corrected = noops = 0
        event_provenance = dict(provenance_by_event or {})
        accepted_facts = list(facts)
        for fact in accepted_facts:
            rows = self.revisions.setdefault(fact.source_event_key, [])
            if rows and rows[-1].fact.row_hash == fact.row_hash:
                noops += 1
                continue
            revision = len(rows) + 1
            self._commit += 1
            rows.append(
                NumericFactRecord(
                    series_id=self.series_id,
                    revision=revision,
                    market_commit_seq=self._commit,
                    ingestion_run_id=run_id,
                    source_identity_key=self.source.identity_key,
                    source=self.source,
                    provenance={
                        **dict(provenance or {}),
                        **dict(event_provenance.get(fact.source_event_key) or {}),
                    },
                    fact=fact,
                )
            )
            if revision == 1:
                inserted += 1
            else:
                corrected += 1
        self.ingestions.append(
            {
                "request": dict(request or {}),
                "facts": tuple(accepted_facts),
                "provenance_by_event": event_provenance,
            }
        )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(accepted_facts),
            inserted_count=inserted,
            corrected_count=corrected,
            noop_count=noops,
            max_commit_seq=self._commit,
        )

    def record_gap_evidence(self, **kwargs: Any) -> str:
        self.gaps.append(dict(kwargs))
        return f"gap-{len(self.gaps)}"

    def record_acquisition_coverage(self, **kwargs: Any) -> str:
        self.coverage.append(dict(kwargs))
        return f"coverage-{len(self.coverage)}"

    def missing_acquisition_ranges(
        self,
        *,
        start: datetime,
        end: datetime,
        **identity: Any,
    ) -> list[tuple[datetime, datetime]]:
        covered = sorted(
            (
                max(start, row["start"]),
                min(end, row["end"]),
            )
            for row in self.coverage
            if row["status"] == "complete"
            and row["series_id"] == identity["series_id"]
            and row["source_id"] == identity["source_id"]
            and row["binding_id"] == identity["binding_id"]
            and row["manifest_hash"] == identity["manifest_hash"]
            and row["interface_version"] == identity["interface_version"]
            and row["confirmation_depth"] == identity["confirmation_depth"]
            and row["end"] > start
            and row["start"] < end
        )
        merged: list[tuple[datetime, datetime]] = []
        for range_start, range_end in covered:
            if not merged or range_start > merged[-1][1]:
                merged.append((range_start, range_end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], range_end))
        missing: list[tuple[datetime, datetime]] = []
        cursor = start
        for range_start, range_end in merged:
            if range_start > cursor:
                missing.append((cursor, range_start))
            cursor = max(cursor, range_end)
        if cursor < end:
            missing.append((cursor, end))
        return missing


def _service(
    store: _MemoryStore,
    provider: _Provider,
) -> tuple[NumericFactAcquisitionService, list[NumericFactBinding]]:
    builds: list[NumericFactBinding] = []

    def builder(binding: NumericFactBinding) -> _Provider:
        builds.append(binding)
        return provider

    return (
        NumericFactAcquisitionService(  # type: ignore[arg-type]
            store=store,
            provider_builder=builder,
        ),
        builds,
    )


def test_network_authorization_defaults_to_deny_before_provider_build(
    tmp_path: Path,
) -> None:
    store = _MemoryStore()
    provider = _Provider([])
    service, builds = _service(store, provider)

    with pytest.raises(RuntimeError, match="explicit network authorization"):
        service.acquire_history(
            manifest_path=str(_write_manifest(tmp_path)),
            binding_id="eth-usd",
            start=_START,
            end=_END,
            authorization=NumericAcquisitionAuthorization(),
            budget=_BUDGET,
        )

    assert builds == []
    assert provider.history_calls == []
    assert store.ingestions == []
    assert store.coverage == []


def test_network_authorization_rejects_truthy_non_boolean_values() -> None:
    with pytest.raises(ValueError, match="network_allowed must be boolean"):
        NumericAcquisitionAuthorization(  # type: ignore[arg-type]
            network_allowed="false"
        )


def test_network_authorization_requires_actor_and_reason_before_provider_build(
    tmp_path: Path,
) -> None:
    store = _MemoryStore()
    provider = _Provider([])
    service, builds = _service(store, provider)

    with pytest.raises(ValueError, match="actor and reason are required"):
        service.acquire_history(
            manifest_path=str(_write_manifest(tmp_path)),
            binding_id="eth-usd",
            start=_START,
            end=_END,
            authorization=NumericAcquisitionAuthorization(network_allowed=True),
            budget=_BUDGET,
        )

    assert builds == []
    assert provider.history_calls == []
    assert store.ingestions == []
    assert store.coverage == []


def test_complete_zero_event_range_is_cached_and_reused_without_network(
    tmp_path: Path,
) -> None:
    store = _MemoryStore()
    provider = _Provider([_batch()])
    service, builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    first = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )
    cached = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=NumericAcquisitionAuthorization(),
        budget=_BUDGET,
    )

    assert first.complete is True
    assert first.inserted_count == 0
    assert first.acquired_ranges == ((_START, _END),)
    assert store.coverage[0]["status"] == "complete"
    assert store.coverage[0]["ingestion_run_id"] is None
    assert cached.complete is True
    assert cached.acquired_ranges == ()
    assert cached.cached_ranges == ((_START, _END),)
    assert len(builds) == 1
    assert provider.history_calls == [(_START, _END)]


def test_current_read_does_not_certify_reusable_historical_coverage(
    tmp_path: Path,
) -> None:
    observation = _observation(
        "evm:1:proxy:current:answer",
        "1914.28523541",
        minute=5,
        block_hash="current-block",
    )
    store = _MemoryStore()
    provider = _Provider([_batch(observation), _batch(observation)])
    service, _builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    current = service.acquire_current(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )
    historical = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )

    assert current.complete is True
    assert provider.current_calls == 1
    assert provider.history_calls == [(_START, _END)]
    assert historical.acquired_ranges == ((_START, _END),)
    assert len(store.coverage) == 1
    assert store.coverage[0]["status"] == "complete"


def test_disjoint_missing_ranges_share_one_budget_and_report_cached_middle(
    tmp_path: Path,
) -> None:
    middle_start = _START + timedelta(minutes=20)
    middle_end = _START + timedelta(minutes=40)
    store = _MemoryStore()
    provider = _Provider(
        [
            _batch(range_start=middle_start, range_end=middle_end),
            _batch(
                range_start=_START,
                range_end=middle_start,
                requests_used=3,
                logs_used=2,
                blocks_scanned=10,
            ),
            _batch(
                range_start=middle_end,
                range_end=_END,
                requests_used=4,
                logs_used=1,
                blocks_scanned=20,
            ),
        ]
    )
    service, _builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=middle_start,
        end=middle_end,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )
    result = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )

    assert result.complete is True
    assert result.acquired_ranges == (
        (_START, middle_start),
        (middle_end, _END),
    )
    assert result.cached_ranges == ((middle_start, middle_end),)
    assert (result.requests_used, result.logs_used, result.blocks_scanned) == (
        7,
        3,
        30,
    )
    assert provider.history_budgets[1] == _BUDGET
    assert provider.history_budgets[2] == NumericAcquisitionBudget(
        max_requests=_BUDGET.max_requests - 3,
        max_logs=_BUDGET.max_logs - 2,
        max_blocks=_BUDGET.max_blocks - 10,
        max_retries=_BUDGET.max_retries,
    )
    assert provider.history_calls == [
        (middle_start, middle_end),
        (_START, middle_start),
        (middle_end, _END),
    ]


def test_partial_batch_records_gap_and_does_not_claim_cached_coverage(
    tmp_path: Path,
) -> None:
    gap = ProviderNumericGap(
        classification="chainlink_log_range_unavailable",
        start=_START + timedelta(minutes=20),
        end=_START + timedelta(minutes=30),
        evidence={"phase_id": 2, "rpc_error": "range denied"},
    )
    store = _MemoryStore()
    provider = _Provider(
        [
            _batch(
                _observation(
                    "evm:1:proxy:round-1:answer",
                    "1900.25",
                    minute=10,
                    block_hash="0xaaa",
                ),
                status="partial",
                gaps=(gap,),
            )
        ]
    )
    service, builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    result = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )

    assert result.complete is False
    assert result.inserted_count == 1
    assert result.gap_count == 1
    assert store.gaps[0]["classification"] == "chainlink_log_range_unavailable"
    assert store.coverage[0]["status"] == "partial"
    assert store.missing_acquisition_ranges(
        series_id=store.series_id,
        source_id=store.source_id,
        binding_id="eth-usd",
        manifest_hash=store.coverage[0]["manifest_hash"],
        interface_version=store.coverage[0]["interface_version"],
        confirmation_depth=12,
        start=_START,
        end=_END,
    ) == [(_START, _END)]
    assert len(builds) == 1


def test_repair_appends_correction_and_invalidates_disappeared_event(
    tmp_path: Path,
) -> None:
    first_event = "evm:1:proxy:round-1:answer"
    disappeared_event = "evm:1:proxy:round-2:answer"
    initial = _batch(
        _observation(first_event, "1900.25", minute=10, block_hash="0xaaa"),
        _observation(disappeared_event, "1901.50", minute=20, block_hash="0xbbb"),
    )
    repaired = _batch(
        _observation(first_event, "1900.75", minute=10, block_hash="0xccc")
    )
    store = _MemoryStore()
    provider = _Provider([initial, repaired])
    service, _builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    inserted = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )
    repair = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
        repair=True,
    )

    assert inserted.inserted_count == 2
    assert repair.corrected_count == 2
    assert repair.invalidated_count == 1
    assert repair.noop_count == 0
    assert [record.fact.source_event_key for record in store.read_numeric_facts(
        series_id=store.series_id,
        start=_START,
        end=_END,
    )] == [first_event]
    assert store.revisions[first_event][-1].fact.value == Decimal("1900.75")
    disappeared = store.revisions[disappeared_event]
    assert [record.fact.state for record in disappeared] == [
        NumericFactState.ACTIVE,
        NumericFactState.INVALIDATED,
    ]
    assert disappeared[-1].fact.known_at_method == "reorg_reconciliation"
    assert disappeared[-1].provenance["correction"]["kind"] == (
        "source_event_disappeared"
    )


def test_provenance_only_reorg_appends_a_correction_revision(tmp_path: Path) -> None:
    event = "evm:1:proxy:round-1:answer"
    original = _batch(
        _observation(event, "1900.25", minute=10, block_hash="0xaaa")
    )
    reorged = _batch(
        _observation(event, "1900.25", minute=10, block_hash="0xbbb")
    )
    store = _MemoryStore()
    provider = _Provider([original, reorged, reorged])
    service, _builds = _service(store, provider)
    manifest = _write_manifest(tmp_path)

    service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
    )
    result = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
        repair=True,
    )
    repeated = service.acquire_history(
        manifest_path=str(manifest),
        binding_id="eth-usd",
        start=_START,
        end=_END,
        authorization=_AUTHORIZED,
        budget=_BUDGET,
        repair=True,
    )

    assert result.corrected_count == 1
    assert result.noop_count == 0
    assert repeated.corrected_count == 0
    assert repeated.noop_count == 1
    assert len(store.revisions[event]) == 2
    assert store.revisions[event][-1].fact.known_at_method == (
        "reconciliation_observed_at"
    )
    assert (
        store.revisions[event][-1].fact.known_at
        >= store.revisions[event][-1].fact.accepted_at
    )
    assert store.revisions[event][-1].provenance["block_hash"] == "0xbbb"
    assert store.revisions[event][-1].provenance["correction"]["kind"] == (
        "source_event_changed"
    )

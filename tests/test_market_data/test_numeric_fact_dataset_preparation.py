from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from data_providers.numeric_facts import NumericAcquisitionBudget
from indicators.manifest import IndicatorManifest, IndicatorMarketInput
from market_data.contracts import NumericFact, NumericFactRecord, SourceIdentity
from portal.backend.service.market.backtest_dataset_service import (
    prepare_backtest_dataset,
)
from portal.backend.service.market.numeric_fact_acquisition import (
    NumericAcquisitionAuthorization,
    NumericAcquisitionResult,
)
from tests.test_market_data.test_backtest_dataset import (
    EVALUATION_END,
    EVALUATION_START,
    _bot,
    _instrument,
    _strategy,
)


_FACT_TYPE = "market.reference_price"
_CONTRACT_VERSION = "market.reference_price.v1"


def _manifest() -> IndicatorManifest:
    return IndicatorManifest(
        type="numeric_preparation_proof",
        version="1.0.0",
        label="Numeric preparation proof",
        description="Offline orchestration fixture.",
        market_inputs=(
            IndicatorMarketInput(
                key="reference_price",
                fact_type=_FACT_TYPE,
                contract_version=_CONTRACT_VERSION,
                dimensions={"quote_currency": "usd"},
                alignment="latest_known",
                max_staleness_seconds=7200,
                required_fields=("effective_at", "value", "unit", "known_at"),
            ),
        ),
    )


def _numeric_record() -> NumericFactRecord:
    source = SourceIdentity(
        provider="CHAINLINK",
        venue="ETHEREUM_MAINNET",
        source_kind="public_evm_contract",
        adapter_version="chainlink_aggregator_v3.v1",
    )
    effective_at = EVALUATION_START - timedelta(minutes=30)
    known_at = EVALUATION_START - timedelta(minutes=20)
    return NumericFactRecord(
        series_id=31,
        revision=1,
        market_commit_seq=101,
        ingestion_run_id="numeric-ingestion-1",
        source_identity_key=source.identity_key,
        source=source,
        provenance={"fixture": "dataset-preparation"},
        fact=NumericFact(
            fact_type=_FACT_TYPE,
            contract_version=_CONTRACT_VERSION,
            value=Decimal("1914.28523541"),
            raw_value="191428523541",
            unit="USD",
            dimensions={"quote_currency": "USD"},
            effective_at=effective_at,
            effective_at_method="chainlink_round_updated_at",
            source_published_at=effective_at + timedelta(minutes=1),
            accepted_at=known_at,
            known_at=known_at,
            known_at_method="evm_confirmation_block",
            source_event_key="evm:1:proxy:round:answer",
        ),
    )


class _PreparationStore:
    def __init__(self) -> None:
        self.numeric_records: list[NumericFactRecord] = []
        self.freeze_calls: list[dict[str, Any]] = []

    def resolve_series_id(self, **kwargs: Any) -> int:
        return 31 if kwargs["fact_type"] == _FACT_TYPE else 7

    def read_numeric_fact_revisions(self, **kwargs: Any) -> list[NumericFactRecord]:
        return [
            record
            for record in self.numeric_records
            if kwargs["start"] <= record.fact.effective_at < kwargs["end"]
        ]

    def freeze_dataset(self, requests, **kwargs: Any):
        request_list = list(requests)
        self.freeze_calls.append({"requests": request_list, "kwargs": dict(kwargs)})
        return SimpleNamespace(
            dataset_id="mds_numeric_prepared",
            dataset_hash="a" * 64,
            max_commit_seq=101,
            contract_version="market_dataset.v1",
            reused_existing=False,
            series=({"row_count": 16}, {"row_count": 1}),
        )


class _NumericAcquirer:
    def __init__(self, store: _PreparationStore) -> None:
        self.store = store
        self.calls: list[dict[str, Any]] = []

    def acquire_history(self, **kwargs: Any) -> NumericAcquisitionResult:
        authorization = kwargs["authorization"]
        budget = kwargs["budget"]
        assert isinstance(authorization, NumericAcquisitionAuthorization)
        authorization.require()
        assert isinstance(budget, NumericAcquisitionBudget)
        self.calls.append(dict(kwargs))
        self.store.numeric_records = [_numeric_record()]
        return NumericAcquisitionResult(
            manifest_id="enabled-chainlink",
            binding_id="eth-usd",
            series_id=31,
            source_id=11,
            requested_ranges=((kwargs["start"], kwargs["end"]),),
            acquired_ranges=((kwargs["start"], kwargs["end"]),),
            cached_ranges=(),
            inserted_count=1,
            corrected_count=0,
            invalidated_count=0,
            noop_count=0,
            gap_count=0,
            requests_used=12,
            logs_used=1,
            blocks_scanned=20,
            complete=True,
        )


def _strategy_with_numeric_input():
    strategy = _strategy()
    strategy.indicator_ids = ["numeric-proof-1"]
    return strategy


def _configure_service(monkeypatch) -> None:
    import portal.backend.service.market.backtest_dataset_service as service

    manifest = _manifest()
    binding = SimpleNamespace(
        instrument_id="instrument-1",
        fact_type=_FACT_TYPE,
        contract_version=_CONTRACT_VERSION,
        dimensions={"quote_currency": "USD"},
    )
    monkeypatch.setattr(service, "get_indicator_manifest", lambda _type: manifest)
    monkeypatch.setattr(
        service,
        "load_numeric_fact_manifest",
        lambda _path: SimpleNamespace(
            binding=lambda binding_id, require_enabled: binding
        ),
    )
    monkeypatch.setattr(
        service,
        "validate_backtest_dataset",
        lambda **_kwargs: {
            "dataset_id": "mds_numeric_prepared",
            "series": ({"row_count": 16}, {"row_count": 1}),
            "quality": {"status": "ready"},
        },
    )


def _prepare(
    *,
    monkeypatch,
    store: _PreparationStore,
    acquirer: _NumericAcquirer,
    acquire_missing: bool,
    numeric_acquisition: dict[str, Any] | None,
):
    _configure_service(monkeypatch)
    return prepare_backtest_dataset(
        bot=_bot(),
        strategy=_strategy_with_numeric_input(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        acquire_missing=acquire_missing,
        numeric_acquisition=numeric_acquisition,
        created_by="operator@example.test",
        store=store,
        coverage_loader=lambda *_args: {"row_count": 16, "missing_ranges": []},
        indicator_meta_loader=lambda _indicator_id: {
            "id": "numeric-proof-1",
            "type": "numeric_preparation_proof",
            "params": {},
            "enabled": True,
        },
        indicator_input_plan_loader=lambda *_args, **_kwargs: {
            "source_timeframe": "1h",
            "start": EVALUATION_START,
        },
        instrument_loader=_instrument,
        numeric_acquirer=acquirer,
    )


def _acquisition_context() -> dict[str, Any]:
    return {
        "bindings": [
            {"manifest_path": "enabled-chainlink.json", "binding_id": "eth-usd"}
        ],
        "authorization": {
            "network_allowed": True,
            "actor": "operator@example.test",
            "reason": "fill required numeric coverage",
        },
        "budget": {
            "max_requests": 25,
            "max_logs": 10,
            "max_blocks": 100,
            "max_retries": 1,
        },
    }


def test_prepare_acquires_required_numeric_gap_then_rechecks_and_freezes(
    monkeypatch,
) -> None:
    store = _PreparationStore()
    acquirer = _NumericAcquirer(store)

    result = _prepare(
        monkeypatch=monkeypatch,
        store=store,
        acquirer=acquirer,
        acquire_missing=True,
        numeric_acquisition=_acquisition_context(),
    )

    assert result["status"] == "ready"
    assert len(acquirer.calls) == 1
    call = acquirer.calls[0]
    assert call["manifest_path"] == "enabled-chainlink.json"
    assert call["binding_id"] == "eth-usd"
    assert call["repair"] is False
    assert call["authorization"] == NumericAcquisitionAuthorization(
        network_allowed=True,
        actor="operator@example.test",
        reason="fill required numeric coverage",
    )
    assert call["budget"] == NumericAcquisitionBudget(
        max_requests=25,
        max_logs=10,
        max_blocks=100,
        max_retries=1,
    )
    assert len(store.freeze_calls) == 1
    assert {request.series_id for request in store.freeze_calls[0]["requests"]} == {
        7,
        31,
    }
    numeric_coverage = next(
        row for row in result["coverage_after"] if row["fact_type"] == _FACT_TYPE
    )
    assert numeric_coverage["coverage"]["missing_ranges"] == []
    assert result["acquisitions"][0]["complete"] is True
    assert result["acquisitions"][0]["cached"] is False


def test_prepare_without_explicit_acquisition_never_calls_numeric_acquirer(
    monkeypatch,
) -> None:
    store = _PreparationStore()
    acquirer = _NumericAcquirer(store)

    with pytest.raises(RuntimeError, match="explicit acquisition enabled"):
        _prepare(
            monkeypatch=monkeypatch,
            store=store,
            acquirer=acquirer,
            acquire_missing=False,
            numeric_acquisition=_acquisition_context(),
        )

    assert acquirer.calls == []
    assert store.freeze_calls == []


def test_prepare_without_numeric_authorization_context_never_calls_acquirer(
    monkeypatch,
) -> None:
    store = _PreparationStore()
    acquirer = _NumericAcquirer(store)

    with pytest.raises(RuntimeError, match="numeric_acquisition configuration"):
        _prepare(
            monkeypatch=monkeypatch,
            store=store,
            acquirer=acquirer,
            acquire_missing=True,
            numeric_acquisition=None,
        )

    assert acquirer.calls == []
    assert store.freeze_calls == []

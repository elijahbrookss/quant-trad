from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping

import pytest

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.reserve_state.manifest import MANIFEST
from indicators.reserve_state.runtime import TypedReserveStateIndicator
from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.contracts import SourceIdentity
from market_data.frozen import semantic_hash
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market import frozen_dataset_service
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver
from portal.backend.service.storage.repos.market_data import market_data_repo
from research_science.check import (
    CHECK_DEFINITION_SCHEMA_VERSION,
    CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_PLAN_SCHEMA_VERSION,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    GAP_POLICY_REJECT,
    CheckDefinition,
    CheckEvidenceBinding,
    CheckRegistry,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
    ScalarAssertionSpec,
    evaluate_scalar_assertions,
    verify_check_replay,
)


pytestmark = pytest.mark.db

_OBSERVED_AT = datetime(2026, 8, 7, 19, 0, tzinfo=UTC)
_KNOWN_AT = _OBSERVED_AT + timedelta(minutes=2)
_DECISION_TIME = _OBSERVED_AT + timedelta(hours=1)


class _ReserveQuantityEvaluator:
    """Provider-free proof evaluator over one Indicator context output."""

    evaluator_id = "canonical_reserve_quantity"
    version = "1"

    def declare_requirements(
        self,
        *,
        definition: CheckDefinition,
        request: CheckRequest,
    ) -> Mapping[str, Any]:
        _ = definition, request
        return {"input_kind": "frozen_market_data"}

    def evaluate(
        self,
        *,
        plan: ResolvedCheckPlan,
        inputs: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        fields = dict(inputs["reserve_state"]["fields"])
        assertions = tuple(
            ScalarAssertionSpec(**dict(raw))
            for raw in plan.execution["assertions"]
        )
        evaluation = evaluate_scalar_assertions(fields, assertions)
        return {
            "schema_version": "canonical_reserve_quantity_check.v1",
            "status": "completed",
            "reserve_quantity": fields["reserve_quantity"],
            "reserve_asset": fields["reserve_asset"],
            **evaluation,
            "promotion_authority": False,
            "execution_authority": False,
        }


def _instrument_snapshot(instrument_id: str, symbol: str) -> dict[str, Any]:
    return {
        "id": instrument_id,
        "symbol": symbol,
        "datasource": "CANONICAL",
        "exchange": "ARBITRUM",
        "instrument_type": "reference_asset",
    }


def _run_indicator(
    *,
    resolver: RuntimeMarketDataResolver,
    declaration: Mapping[str, Any],
    instrument_id: str,
) -> tuple[dict[str, Any], CanonicalFactRecord]:
    inputs = resolver.resolve(
        requirements_by_consumer={"reserve-state-proof": (declaration,)},
        primary_instrument_id=instrument_id,
        evaluation_time=_DECISION_TIME,
    )
    selected = inputs["reserve-state-proof"]["reserve_state"]
    assert isinstance(selected, CanonicalFactRecord)
    indicator = TypedReserveStateIndicator(
        indicator_id="reserve-state-proof",
        version="v1",
    )
    frame = IndicatorExecutionEngine([indicator]).step(
        bar=Candle(
            time=_DECISION_TIME,
            end=_DECISION_TIME + timedelta(hours=1),
            known_at=_DECISION_TIME,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1.0,
        ),
        bar_time=_DECISION_TIME,
        market_data_inputs=inputs,
    )
    return (
        frame.outputs["reserve-state-proof.reserve_state"].value,
        selected,
    )


def test_structured_fact_freezes_replays_and_binds_check_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    token = uuid.uuid4().hex
    instrument_id = f"reserve-proof-{token[:18]}"
    symbol = f"POR-{token[:8].upper()}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="CANONICAL",
                exchange="ARBITRUM",
                symbol=symbol,
                instrument_type="reference_asset",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={"fixture": "structured-fact-research-path"},
            )
        )

    source = SourceIdentity(
        provider="CHAINLINK",
        venue="ARBITRUM_MAINNET",
        source_kind="mvr_proxy",
        adapter_version=f"chainlink.mvr.reserve_state.v1.{token}",
    )
    source_id = market_data_repo.register_source(
        source,
        lineage={"fixture": "structured-fact-research-path"},
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="asset.reserve_state",
        timeframe_seconds=None,
        contract_version="asset.reserve_state.v1",
        dimensions={"reserve_asset": "BTC"},
    )
    fact = CanonicalFact(
        fact_type="asset.reserve_state",
        payload_schema_id="asset.reserve_state.v1",
        observation_key=f"arbitrum:42161:report:{token}",
        observation_time=_OBSERVED_AT,
        observation_time_method="chainlink_latest_bundle_timestamp",
        source_published_at=_OBSERVED_AT,
        received_at=_KNOWN_AT,
        accepted_at=_KNOWN_AT,
        known_at=_KNOWN_AT,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="chainlink_mvr_reserve_state.v1",
        payload={
            "report_id": "DE000NXTA018",
            "reserve_asset": "BTC",
            "reserve_quantity": Decimal("514.32323119"),
            "unit": "BTC",
        },
        external_event_key=f"arbitrum:42161:bundle:{token}",
        provenance={
            "chain_id": 42161,
            "proxy_address": "0xf5eA763bbFc7968A27b28bc612a8B89fCF9E0069",
            "bundle_hash": "a" * 64,
        },
    )
    outcome = market_data_repo.ingest_facts(
        series_id=series_id,
        source_id=source_id,
        facts=(fact,),
        request={"fixture": "structured-fact-research-path"},
    )
    assert outcome.inserted_count == 1

    declaration = MANIFEST.market_inputs[0].to_requirement(
        timeframe_seconds=None
    ).to_dict()
    declaration["instrument_ref"] = instrument_id
    requirement = {
        **declaration,
        "alias": "reserve_state",
        "consumer_id": "reserve-state-proof",
        "instrument_id": instrument_id,
        "required_start": (_OBSERVED_AT - timedelta(minutes=1)).isoformat(),
        "required_end": (_DECISION_TIME + timedelta(minutes=1)).isoformat(),
        "source_policy": {
            "mode": "exact",
            "source_identity_key": source.identity_key,
        },
    }
    prepared = frozen_dataset_service.prepare_frozen_dataset_from_requirements(
        requirements=(requirement,),
        freeze=True,
        name=f"Structured reserve proof {token[:8]}",
        purpose="test",
        created_by="pytest",
        metadata={"fixture": "structured-fact-research-path"},
        store=market_data_repo,
        instrument_loader=lambda value: _instrument_snapshot(value, symbol),
    )
    assert prepared["status"] == "frozen"
    binding = prepared["binding"]
    assert binding["provider_access"] == "disabled"
    assert binding["series"][0]["payload_schemas"] == [
        {
            "schema_id": "asset.reserve_state.v1",
            "contract_hash": fact.payload_contract_hash,
        }
    ]

    class _ProviderCallTrap:
        def __init__(self, *, store: Any) -> None:
            self.store = store

        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"frozen replay attempted provider access: {name}")

    monkeypatch.setattr(
        runtime_market_data,
        "MarketDataCollectorService",
        _ProviderCallTrap,
    )
    first_output, selected = _run_indicator(
        resolver=RuntimeMarketDataResolver(
            store=market_data_repo,
            dataset_binding=binding,
        ),
        declaration=declaration,
        instrument_id=instrument_id,
    )
    assert selected.fact.payload_schema_id == "asset.reserve_state.v1"
    assert selected.fact.payload_contract_hash == fact.payload_contract_hash

    assertions = (
        {
            "metric_path": "reserve_quantity",
            "operator": "gte",
            "threshold": 500,
        },
    )
    definition = CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
        definition_id="canonical_reserve_quantity",
        definition_version="1",
        evaluator_id="canonical_reserve_quantity",
        evaluator_version="1",
        request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"assertions": assertions},
    )
    request = CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=CHECK_MODE_EVIDENCE,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        definition_hash=definition.definition_hash,
        scope={
            "instrument_id": instrument_id,
            "start": requirement["required_start"],
            "end": requirement["required_end"],
        },
        parameters={"assertions": assertions},
        dataset_id=prepared["dataset"]["dataset_id"],
    )
    indicator_graph = (
        {
            "indicator_id": "reserve-state-proof",
            "type": "reserve_state",
            "version": "v1",
        },
    )
    plan = ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash=request.request_hash,
        market_data_requirements=(requirement,),
        indicator_graph=indicator_graph,
        evaluation_range={
            "start": requirement["required_start"],
            "end_exclusive": requirement["required_end"],
        },
        materialization_range={
            "start": requirement["required_start"],
            "end_exclusive": requirement["required_end"],
        },
        warmup={"bars": 0},
        outcome_tail={"bars": 0},
        gap_policy=GAP_POLICY_REJECT,
        execution={"assertions": assertions},
    )
    evaluator = _ReserveQuantityEvaluator()
    registry = CheckRegistry()
    registry.register_evaluator(evaluator)
    registry.register_definition(definition)
    resolved_definition, resolved_evaluator = registry.resolve(
        definition.definition_id,
        definition.definition_version,
    )
    assert resolved_definition is definition
    evaluated = resolved_evaluator.evaluate(
        plan=plan,
        inputs={"reserve_state": first_output},
    )
    assert evaluated["verdict"] == "passed"

    evidence = CheckEvidenceBinding(
        schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        code_revision="structured-fact-research-path.v1",
        evidence_kind="frozen_market_data",
        input_binding=binding,
        indicator_graph_hash=semantic_hash({"indicators": indicator_graph}),
        indicator_output_hash=semantic_hash(
            {"reserve-state-proof.reserve_state": first_output}
        ),
        fact_input_hash=semantic_hash(
            {
                "fact_version_id": selected.fact_version_id,
                "payload_schema_id": selected.fact.payload_schema_id,
                "payload_contract_hash": selected.fact.payload_contract_hash,
            }
        ),
        gap_transition_hash=semantic_hash({"gap_transitions": []}),
        quality_hash=semantic_hash(binding["quality"]),
        gaps_hash=semantic_hash(
            {"recorded_gaps": list(binding["recorded_gaps"])}
        ),
    )
    original = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result=evaluated,
    )

    replay_output, replay_selected = _run_indicator(
        resolver=RuntimeMarketDataResolver(
            store=market_data_repo,
            dataset_binding=binding,
        ),
        declaration=declaration,
        instrument_id=instrument_id,
    )
    assert replay_selected.fact_version_id == selected.fact_version_id
    replayed = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result=evaluator.evaluate(
            plan=plan,
            inputs={"reserve_state": replay_output},
        ),
    )
    assert verify_check_replay(original, replayed) == {
        "schema_version": "research.check_replay.v1",
        "matches": True,
        "original_result_hash": original.result_hash,
        "replayed_result_hash": original.result_hash,
        "mismatches": [],
        "provider_call_performed": False,
    }


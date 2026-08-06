"""Trusted operator runner for the first sealed offline research campaign.

The public research surface cannot call this module.  It resolves the sealed
holdout internally, executes only frozen dataset reads, and never imports a
provider, trading adapter, credential service, or runtime mutation boundary.
"""

from __future__ import annotations

import argparse
import json
import time
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from engines.bot_runtime.core.execution_assumptions import (
    CONSERVATIVE_BAR_MODEL_VERSION,
    resolve_execution_assumptions,
)
from engines.bot_runtime.core.execution_context import resolve_execution_context
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from portal.backend.db.session import db
from portal.backend.service.storage.repos.instruments import get_instrument
from portal.backend.service.storage.repos.market_data import market_data_repo
from research_science import (
    CAMPAIGN_EVALUATOR_VERSION,
    CAMPAIGN_FEATURE_VERSION,
    CAMPAIGN_METRIC_VERSION,
    CampaignCharter,
    CampaignEvaluation,
    CampaignExecutionCosts,
    FrozenCampaignBar,
    build_campaign_features,
    build_campaign_graph_manifest,
    campaign_graph_specs,
    evaluate_campaign_graph,
    full_scoring_indexes,
    rank_evaluations,
    resolve_campaign_charter,
    stable_hash,
    validation_scoring_indexes,
)
from strategies.typed_graph import TypedStrategyGraph

from . import authority, authority_repository, governance
from . import service as research_memory

PROTOCOL_AUTHORITY_ID = "campaign_protocol_authority"
RESEARCH_AGENT_ID = "campaign_research_agent"
RESEARCH_RUNNER_ID = "campaign_research_runner"
OFFLINE_POLICY_ID = "campaign_offline_policy"
HOLDOUT_AUTHORITY_ID = "campaign_holdout_authority"
HOLDOUT_EXECUTOR_ID = "campaign_holdout_executor"
SCIENCE_AUTHORITY_ID = "campaign_science_authority"
HUMAN_OWNER_ID = "campaign_human_research_owner"


def _campaign_family_name(charter: CampaignCharter) -> str:
    """Return the one family name pinned by both protocol and family records."""

    return charter.campaign_id


def _load_public_charter(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("campaign charter must be a JSON object")
    return raw


def _sealed_holdout_binding(campaign_id: str) -> dict[str, Any]:
    with db.session() as session:
        rows = session.execute(
            text(
                """
                SELECT datasets.id, datasets.dataset_hash,
                       MIN(dataset_series.range_start) AS window_start,
                       MAX(dataset_series.range_end) AS window_end
                FROM market.datasets AS datasets
                JOIN market.dataset_series AS dataset_series
                  ON dataset_series.dataset_id = datasets.id
                WHERE datasets.metadata->>'campaign_id' = :campaign_id
                  AND datasets.metadata->>'dataset_role' = 'holdout'
                GROUP BY datasets.id, datasets.dataset_hash
                """
            ),
            {"campaign_id": campaign_id},
        ).mappings().all()
    if len(rows) != 1:
        raise ValueError("campaign sealed holdout binding is not unique")
    row = rows[0]
    return {
        "dataset_id": str(row["id"]),
        "dataset_hash": str(row["dataset_hash"]),
        "window_start": row["window_start"].isoformat().replace("+00:00", "Z"),
        "window_end": row["window_end"].isoformat().replace("+00:00", "Z"),
    }


def load_private_charter(path: str | Path) -> CampaignCharter:
    public = _load_public_charter(path)
    campaign_id = str(public.get("campaign_id") or "").strip()
    if not campaign_id:
        raise ValueError("campaign_id is required")
    return resolve_campaign_charter(
        public,
        sealed_holdout_binding=_sealed_holdout_binding(campaign_id),
    )


def _dataset_manifest(charter: CampaignCharter, role: str) -> dict[str, Any]:
    binding = charter.dataset(role)
    dataset = market_data_repo.get_dataset(binding.dataset_id)
    if dataset.dataset_hash != binding.dataset_hash:
        raise ValueError(f"campaign {role} dataset hash mismatch")
    series = [dict(row) for row in dataset.series]
    if {str(row["instrument_id"]) for row in series} != {charter.instrument_id}:
        raise ValueError(f"campaign {role} dataset instrument mismatch")
    if {str(row["fact_type"]) for row in series} != set(charter.eligible_fact_types):
        raise ValueError(f"campaign {role} dataset fact set mismatch")
    trade_flow = [row for row in series if row["fact_type"] == "market.trade_flow"]
    if len(trade_flow) != 1 or int(trade_flow[0].get("timeframe_seconds") or 0) != charter.primary_timeframe_seconds:
        raise ValueError(f"campaign {role} primary timeframe mismatch")
    for row in series:
        start = row["range_start"]
        end = row["range_end"]
        if start.isoformat().replace("+00:00", "Z") != binding.window_start:
            raise ValueError(f"campaign {role} range start mismatch")
        if end.isoformat().replace("+00:00", "Z") != binding.window_end:
            raise ValueError(f"campaign {role} range end mismatch")
        if int(row.get("row_count") or 0) <= 0:
            raise ValueError(f"campaign {role} dataset contains an empty series")
    metadata = dict(dataset.metadata or {})
    if metadata.get("provider_fetch_allowed") is not False:
        raise ValueError(f"campaign {role} dataset lacks provider-free declaration")
    if str(metadata.get("instrument_economics_class") or "").lower() != "incomplete":
        raise ValueError(f"campaign {role} derivative economics declaration mismatch")
    return {
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "max_commit_seq": dataset.max_commit_seq,
        "series": series,
        "dataset_manifest_hash": stable_hash(
            {
                "dataset_id": dataset.dataset_id,
                "dataset_hash": dataset.dataset_hash,
                "max_commit_seq": dataset.max_commit_seq,
                "series": series,
            }
        ),
    }


def _latest_asof(records: Sequence[Any], known_at: datetime) -> Any | None:
    if not records:
        return None
    keys = [row.fact.known_at for row in records]
    index = bisect_right(keys, known_at) - 1
    return records[index] if index >= 0 else None


def _load_bars(charter: CampaignCharter, role: str) -> tuple[FrozenCampaignBar, ...]:
    manifest = _dataset_manifest(charter, role)
    by_fact: dict[str, list[Any]] = {}
    for entry in manifest["series"]:
        by_fact[str(entry["fact_type"])] = market_data_repo.read_dataset_series(
            dataset_id=manifest["dataset_id"],
            series_id=int(entry["series_id"]),
        )
    trade_records = sorted(
        by_fact["market.trade_flow"],
        key=lambda row: (row.fact.bucket_start, row.fact.known_at),
    )
    oi_records = sorted(
        by_fact["derivatives.open_interest"], key=lambda row: row.fact.known_at
    )
    funding_records = sorted(
        by_fact["derivatives.funding_rate"], key=lambda row: row.fact.known_at
    )
    bars: list[FrozenCampaignBar] = []
    for record in trade_records:
        fact = record.fact
        if not fact.aggregate_complete or not fact.archive_complete or not fact.canonicalization_complete:
            raise ValueError("campaign trade-flow row is not dataset-eligible complete evidence")
        oi = _latest_asof(oi_records, fact.known_at)
        funding = _latest_asof(funding_records, fact.known_at)
        source_hashes = [
            str(record.provenance_hash),
            str(fact.material_hash),
            str(fact.input_fingerprint),
        ]
        if oi is not None:
            source_hashes.append(str(oi.fact.row_hash))
        if funding is not None:
            source_hashes.append(str(funding.fact.row_hash))
        bars.append(
            FrozenCampaignBar(
                bucket_start=fact.bucket_start,
                bucket_end=fact.bucket_end,
                known_at=fact.known_at,
                open_price=float(fact.open_price) if fact.open_price is not None else None,
                high_price=float(fact.high_price) if fact.high_price is not None else None,
                low_price=float(fact.low_price) if fact.low_price is not None else None,
                close_price=float(fact.close_price) if fact.close_price is not None else None,
                trade_count=int(fact.trade_count),
                base_volume=float(fact.base_volume or 0.0),
                quote_notional=float(fact.quote_notional or 0.0),
                cvd_delta=float(fact.cvd_delta or 0.0),
                open_interest=float(oi.fact.value) if oi is not None else None,
                funding_rate=float(funding.fact.rate) if funding is not None else None,
                source_hashes=tuple(sorted(set(source_hashes))),
            )
        )
    return tuple(bars)


def _execution_bundle(charter: CampaignCharter) -> tuple[CampaignExecutionCosts, dict[str, Any]]:
    assumptions = resolve_execution_assumptions(
        charter.economic_claim_intent,
        {
            "model_version": CONSERVATIVE_BAR_MODEL_VERSION,
            "market_slippage_bps": charter.market_slippage_bps,
            "stop_slippage_bps": charter.stop_slippage_bps,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "explicit_zero_cost_override": False,
            "cost_stress_scenarios": list(charter.cost_stress_scenarios),
        },
        source="sealed_autonomous_campaign_charter",
    )
    instrument = get_instrument(charter.instrument_id)
    if instrument is None:
        raise ValueError("campaign instrument is not persisted")
    if str(instrument.get("symbol") or "") != charter.instrument_symbol:
        raise ValueError("campaign instrument symbol mismatch")
    payload = deepcopy(instrument)
    payload["fee_schedule"] = {
        "maker_rate": charter.maker_fee_rate,
        "taker_rate": charter.taker_fee_rate,
        "source": "campaign_declared_conservative_not_venue_calibrated",
        "version": charter.fee_schedule_version,
        "schedule_id": f"{charter.campaign_id}:conservative_fees",
        "tier": "campaign_conservative",
        "configured": True,
        "verified_zero": False,
    }
    profile = compile_series_execution_profile(
        payload,
        risk_config={
            "base_risk_per_trade": 0.01,
            "global_risk_multiplier": 1.0,
            "instrument_risk_multiplier": 1.0,
        },
        require_margin_accounting=True,
        allowed_source_instrument_types=("future", "perp"),
        execution_semantics="derivative",
        research_market_role="execution_instrument",
    )
    context = resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=payload,
        source="sealed_autonomous_campaign_resolution",
    )
    if context.model.execution_quality_ceiling != "X2":
        raise ValueError("campaign execution context did not resolve to X2")
    costs = CampaignExecutionCosts(
        market_slippage_bps=charter.market_slippage_bps,
        taker_fee_rate=float(context.fee_schedule.taker_rate),
        execution_quality_class="X2",
        execution_model_hash=context.context_hash,
        fee_schedule_hash=context.fee_schedule.schedule_hash,
        stress_scenarios=charter.cost_stress_scenarios,
    )
    return costs, context.to_dict()


def _protocol_manifest(
    charter: CampaignCharter,
    *,
    code_revision: str,
) -> dict[str, Any]:
    contamination = max(charter.feature_lookback_bars, charter.label_horizon_bars, 1)
    return {
        "schema_version": "scientific_protocol.v1",
        "protocol_id": f"protocol:{charter.campaign_id}",
        "family_name": _campaign_family_name(charter),
        "economic_claim_intent": charter.economic_claim_intent,
        "datasets": [row.to_dict() for row in charter.datasets],
        "blindness": "PLATFORM_CONTROLLED_HISTORICAL",
        "budget": {
            "max_attempts": charter.max_attempts,
            "max_runtime_seconds": 3600.0,
            "max_compute_units": float(charter.max_attempts),
            "max_validation_feedback_uses": charter.max_validation_feedback_uses,
        },
        "leakage": {
            "max_feature_lookback_bars": charter.feature_lookback_bars,
            "label_horizon_bars": charter.label_horizon_bars,
            "max_holding_period_bars": charter.label_horizon_bars,
            "order_expiry_bars": 1,
            "purge_bars": contamination,
            "embargo_bars": contamination,
        },
        "walk_forward": {
            "train_bars": charter.walk_forward_train_bars,
            "validation_bars": charter.walk_forward_validation_bars,
            "step_bars": charter.walk_forward_step_bars,
            "fold_count": charter.walk_forward_fold_count,
        },
        "instrument_ids": [charter.instrument_id],
        "allowed_mutation_dimensions": [
            "initial_graph",
            "expressions",
            "actions",
            "parameter_values",
        ],
        "benchmark_ids": list(charter.benchmark_ids),
        "primary_metric": charter.primary_metric,
        "primary_metric_direction": charter.primary_metric_direction,
        "minimum_effect_size": charter.minimum_effect_size,
        "secondary_metrics": list(charter.secondary_metrics),
        "safety_metrics": list(charter.safety_metrics),
        "alpha": charter.alpha,
        "minimum_sample_count": charter.minimum_sample_count,
        "minimum_trade_count": charter.minimum_trade_count,
        "minimum_calendar_days": charter.minimum_calendar_days,
        "minimum_exposure": charter.minimum_exposure,
        "minimum_execution_quality_class": "X2",
        "execution_stress_ids": list(charter.execution_stress_ids),
        "multiple_testing_method": charter.multiple_testing_method,
        "robustness_requirements": list(charter.robustness_requirements),
        "statistical_method_versions": {
            "effect_test": "one_sided_normal_approx.v1",
            "confidence_interval": "moving_block_bootstrap_512.v1",
            "multiplicity": f"{charter.multiple_testing_method}.v1",
        },
        "policy_versions": {
            "campaign_charter": charter.charter_hash,
            "campaign_evaluator": CAMPAIGN_EVALUATOR_VERSION,
            "campaign_features": CAMPAIGN_FEATURE_VERSION,
            "campaign_metrics": CAMPAIGN_METRIC_VERSION,
            "code_revision": code_revision,
            "execution_progression": "fixed_x2_search_x3_x5_feasibility.v1",
            "instrument_economics": "incomplete_no_promotion.v1",
        },
    }


def _transition(
    case_id: str,
    target_state: str,
    *,
    evidence_hashes: Sequence[str],
    binding_updates: Mapping[str, Any] | None = None,
    proposer: str = RESEARCH_AGENT_ID,
    authorizer: str = OFFLINE_POLICY_ID,
    authorizer_role: str = "offline_policy_engine",
) -> dict[str, Any]:
    trail = governance.case_trail(case_id)
    case = trail["case"]
    request_base = f"{case_id}:{case['state_version']}:{target_state.lower()}"
    proposal = governance.propose_transition(
        {
            "actor_id": proposer,
            "actor_role": "research_agent",
            "request_id": f"{request_base}:proposal",
            "case_id": case_id,
            "expected_state_version": case["state_version"],
            "target_state": target_state,
            "binding_updates": dict(binding_updates or {}),
            "evidence_hashes": list(evidence_hashes),
            "rationale": f"Campaign evidence permits transition to {target_state}.",
        }
    )
    return governance.decide_transition(
        {
            "actor_id": authorizer,
            "actor_role": authorizer_role,
            "request_id": f"{request_base}:decision",
            "proposal_id": proposal["id"],
            "disposition": "approve",
        }
    )


def _gate_failures(
    charter: CampaignCharter,
    evaluation: CampaignEvaluation,
) -> tuple[str, ...]:
    failures: list[str] = []
    if evaluation.sample_count < charter.minimum_sample_count:
        failures.append("sample_count_below_minimum")
    if evaluation.trade_count < charter.minimum_trade_count:
        failures.append("trade_count_below_minimum")
    if evaluation.calendar_days < charter.minimum_calendar_days:
        failures.append("calendar_days_below_minimum")
    if evaluation.exposure < charter.minimum_exposure:
        failures.append("exposure_below_minimum")
    if set(evaluation.execution_stress_ids_passed) != set(charter.execution_stress_ids):
        failures.append("cost_stress_survival_failed")
    primary = float(evaluation.metric_results[charter.primary_metric])
    benchmark = max(
        float(row[charter.primary_metric])
        for row in evaluation.benchmark_metric_results.values()
    )
    if primary - benchmark < charter.minimum_effect_size:
        failures.append("benchmark_effect_below_minimum")
    required_metrics = {
        charter.primary_metric,
        *charter.secondary_metrics,
        *charter.safety_metrics,
    }
    if not required_metrics <= set(evaluation.metric_results):
        failures.append("required_metrics_missing")
    return tuple(sorted(set(failures)))


def _create_research_memory(charter: CampaignCharter) -> tuple[dict[str, Any], dict[str, Any]]:
    validation = charter.dataset("validation")
    observation = research_memory.create_research_item(
        {
            "kind": "observation",
            "status": "active",
            "title": "BTC PERP causal trade-flow evidence is frozen but session-limited",
            "body": (
                "The canonical store contains provider-free BTC PERP trade-flow, "
                "open-interest, and funding facts with explicit known-at delays."
            ),
            "instrument_id": charter.instrument_id,
            "symbol": charter.instrument_symbol,
            "timeframe": "1m",
            "datasource": "COINBASE",
            "exchange": "COINBASE_DIRECT",
            "window_start": charter.dataset("train").window_start,
            "window_end": validation.window_end,
            "tags": ["autonomous-campaign", "perpetual", "market-structure"],
            "payload": {
                "campaign_id": charter.campaign_id,
                "campaign_charter_hash": charter.charter_hash,
                "holdout_binding_disclosed": False,
                "instrument_economics_class": "incomplete",
                "promotion_eligible": False,
            },
        }
    )
    hypothesis = research_memory.create_research_item(
        {
            "kind": "hypothesis",
            "status": "active",
            "title": "Causal BTC PERP trade flow adds short-horizon value beyond price",
            "body": charter.economic_claim,
            "instrument_id": charter.instrument_id,
            "symbol": charter.instrument_symbol,
            "timeframe": "1m",
            "datasource": "COINBASE",
            "exchange": "COINBASE_DIRECT",
            "window_start": charter.dataset("train").window_start,
            "window_end": validation.window_end,
            "tags": ["autonomous-campaign", "typed-hypothesis", "selection"],
            "payload": {
                "campaign_id": charter.campaign_id,
                "campaign_charter_hash": charter.charter_hash,
                "economic_claim_intent": charter.economic_claim_intent,
                "external_trading_authority": False,
            },
        }
    )
    return observation, hypothesis


def _execution_progression_evidence(charter: CampaignCharter) -> dict[str, Any]:
    material = {
        "schema_version": "campaign_execution_progression.v1",
        "campaign_id": charter.campaign_id,
        "X2": {"status": "evaluated", "search_surface": True},
        "X3": {
            "status": "unavailable",
            "reason": "no protocol-frozen spread series overlaps validation and holdout",
            "search_surface": False,
        },
        "X4": {
            "status": "unavailable",
            "reason": "no replay-certified L2 tape is pinned to the protocol windows",
            "search_surface": False,
        },
        "X5": {
            "status": "unavailable",
            "reason": "X4 tape and fixed passive queue policy are absent",
            "search_surface": False,
        },
    }
    return {**material, "evidence_hash": stable_hash(material)}


def preflight_campaign(path: str | Path, *, code_revision: str) -> dict[str, Any]:
    if len(str(code_revision).strip()) < 7:
        raise ValueError("campaign code revision is required")
    charter = load_private_charter(path)
    manifests = {role: _dataset_manifest(charter, role) for role in ("train", "validation", "holdout")}
    train_bars = _load_bars(charter, "train")
    validation_bars = _load_bars(charter, "validation")
    train_features = build_campaign_features(
        train_bars, lookback_bars=charter.feature_lookback_bars
    )
    validation_features = build_campaign_features(
        validation_bars, lookback_bars=charter.feature_lookback_bars
    )
    train_indexes = full_scoring_indexes(charter, len(train_features))
    validation_indexes = validation_scoring_indexes(charter, len(validation_features))
    costs, context = _execution_bundle(charter)
    specs = campaign_graph_specs()
    if len(specs) != charter.graph_budget:
        raise ValueError("campaign graph generator disagrees with charter budget")
    return {
        "schema_version": "autonomous_campaign_preflight.v1",
        "campaign_id": charter.campaign_id,
        "campaign_charter_hash": charter.charter_hash,
        "code_revision": code_revision,
        "instrument": {
            "instrument_id": charter.instrument_id,
            "symbol": charter.instrument_symbol,
            "instrument_class": charter.instrument_class,
            "instrument_economics_class": "incomplete",
        },
        "datasets": {
            "train": {
                "dataset_id": manifests["train"]["dataset_id"],
                "dataset_hash": manifests["train"]["dataset_hash"],
                "rows": len(train_bars),
                "scoring_indexes": len(train_indexes),
            },
            "validation": {
                "dataset_id": manifests["validation"]["dataset_id"],
                "dataset_hash": manifests["validation"]["dataset_hash"],
                "rows": len(validation_bars),
                "scoring_indexes": len(validation_indexes),
            },
            "holdout": {
                "sealed": True,
                "blind_alias": charter.dataset("holdout").blind_alias,
                "manifest_valid": True,
            },
        },
        "provider_fetch_allowed": False,
        "external_trading_authority": False,
        "promotion_eligible": False,
        "graph_budget": len(specs),
        "execution_context_hash": context["context_hash"],
        "fee_schedule_hash": costs.fee_schedule_hash,
        "execution_progression": _execution_progression_evidence(charter),
        "preflight_hash": stable_hash(
            {
                "charter_hash": charter.charter_hash,
                "code_revision": code_revision,
                "dataset_hashes": [manifests[role]["dataset_hash"] for role in ("train", "validation", "holdout")],
                "execution_context_hash": context["context_hash"],
                "graph_specs": list(specs),
            }
        ),
    }


def execute_campaign(path: str | Path, *, code_revision: str) -> dict[str, Any]:
    started = time.perf_counter()
    preflight = preflight_campaign(path, code_revision=code_revision)
    charter = load_private_charter(path)
    costs, execution_context = _execution_bundle(charter)
    train_features = build_campaign_features(
        _load_bars(charter, "train"), lookback_bars=charter.feature_lookback_bars
    )
    validation_features = build_campaign_features(
        _load_bars(charter, "validation"), lookback_bars=charter.feature_lookback_bars
    )
    train_indexes = full_scoring_indexes(charter, len(train_features))
    validation_indexes = validation_scoring_indexes(charter, len(validation_features))
    observation, hypothesis = _create_research_memory(charter)
    case = governance.create_case(
        {
            "actor_id": RESEARCH_AGENT_ID,
            "actor_role": "research_agent",
            "request_id": f"{charter.campaign_id}:governance-case",
            "case_id": f"governance:{charter.campaign_id}",
            "observation_id": observation["id"],
        }
    )
    _transition(
        case["id"],
        "HYPOTHESIS",
        evidence_hashes=(charter.charter_hash,),
        binding_updates={"hypothesis_id": hypothesis["id"]},
    )
    protocol = authority.create_protocol(
        {
            "actor_id": PROTOCOL_AUTHORITY_ID,
            "actor_role": "research_authority",
            "request_id": f"{charter.campaign_id}:protocol-authorize",
            "protocol": _protocol_manifest(charter, code_revision=code_revision),
        }
    )
    family = authority.create_family(
        {
            "actor_id": RESEARCH_AGENT_ID,
            "actor_role": "research_agent",
            "request_id": f"{charter.campaign_id}:family-create",
            "protocol_id": protocol["id"],
            "family_id": f"family:{charter.campaign_id}",
            "name": _campaign_family_name(charter),
        }
    )
    _transition(
        case["id"],
        "PROTOCOL_PROPOSED",
        evidence_hashes=(protocol["protocol_hash"], family["family_hash"]),
        binding_updates={"protocol_id": protocol["id"], "family_id": family["id"]},
    )
    _transition(
        case["id"],
        "PROTOCOL_APPROVED",
        evidence_hashes=(protocol["protocol_hash"],),
    )
    _transition(
        case["id"],
        "TRIALS_RUNNING",
        evidence_hashes=(family["family_hash"], preflight["preflight_hash"]),
    )

    train_rows: list[tuple[CampaignEvaluation, dict[str, Any], dict[str, Any]]] = []
    all_attempt_ids: list[str] = []
    for spec in campaign_graph_specs():
        admission = authority.create_typed_strategy_graph(
            {
                "actor_id": RESEARCH_AGENT_ID,
                "actor_role": "research_agent",
                "request_id": f"{charter.campaign_id}:graph:{int(spec['ordinal']):02d}",
                "family_id": family["id"],
                "graph": build_campaign_graph_manifest(
                    campaign_id=charter.campaign_id,
                    family_id=family["id"],
                    protocol_hash=protocol["protocol_hash"],
                    spec=spec,
                ),
                "mutation_dimensions": [
                    "initial_graph",
                    "expressions",
                    "actions",
                    "parameter_values",
                ],
                "estimated_runtime_seconds": 1.0,
                "estimated_compute_units": 0.5,
            }
        )
        graph_record = admission["strategy_graph"]
        attempt = admission["search_attempt"]
        graph = TypedStrategyGraph.from_dict(graph_record["manifest"])
        evaluation = evaluate_campaign_graph(
            charter=charter,
            graph=graph,
            rows=train_features,
            execution=costs,
            scoring_indexes=train_indexes,
        )
        train_failures = []
        if evaluation.sample_count <= 0:
            train_failures.append("no_causal_scoring_opportunities")
        if evaluation.trade_count <= 0:
            train_failures.append("no_signals")
        evidence = evaluation.to_attempt_evidence(charter=charter, validation=False)
        evidence["campaign_graph_spec"] = dict(spec)
        evidence["gate_failures"] = train_failures
        terminal = authority.complete_attempt(
            {
                "actor_id": RESEARCH_RUNNER_ID,
                "actor_role": "experiment_runner",
                "request_id": f"{charter.campaign_id}:train-complete:{int(spec['ordinal']):02d}",
                "attempt_id": attempt["id"],
                "status": "invalid" if train_failures else "completed",
                "result_evidence": evidence,
                "error": ",".join(train_failures) if train_failures else None,
                "actual_runtime_seconds": 0.01,
                "actual_compute_units": 0.1,
            }
        )
        all_attempt_ids.append(terminal["id"])
        if terminal["status"] == "completed":
            train_rows.append((evaluation, graph_record, terminal))

    ranked_train = rank_evaluations(
        (row[0] for row in train_rows), primary_metric=charter.primary_metric
    )
    by_hash = {row[0].graph_hash: row for row in train_rows}
    validation_rows: list[tuple[CampaignEvaluation, dict[str, Any], dict[str, Any]]] = []
    for rank, train_evaluation in enumerate(
        ranked_train[: charter.validation_survivor_limit], start=1
    ):
        _, graph_record, train_attempt = by_hash[train_evaluation.graph_hash]
        attempt = authority.register_attempt(
            {
                "actor_id": RESEARCH_AGENT_ID,
                "actor_role": "research_agent",
                "request_id": f"{charter.campaign_id}:validation:{rank:02d}",
                "family_id": family["id"],
                "dataset_role": "validation",
                "trial_inputs": {
                    "strategy_graph_id": graph_record["id"],
                    "strategy_graph_hash": graph_record["graph_hash"],
                    "parent_attempt_ids": [train_attempt["id"]],
                    "selection_rank": rank,
                },
                "estimated_runtime_seconds": 1.0,
                "estimated_compute_units": 0.5,
            }
        )
        graph = TypedStrategyGraph.from_dict(graph_record["manifest"])
        evaluation = evaluate_campaign_graph(
            charter=charter,
            graph=graph,
            rows=validation_features,
            execution=costs,
            scoring_indexes=validation_indexes,
        )
        failures = _gate_failures(charter, evaluation)
        evidence = evaluation.to_attempt_evidence(charter=charter, validation=True)
        evidence["gate_failures"] = list(failures)
        terminal = authority.complete_attempt(
            {
                "actor_id": RESEARCH_RUNNER_ID,
                "actor_role": "experiment_runner",
                "request_id": f"{charter.campaign_id}:validation-complete:{rank:02d}",
                "attempt_id": attempt["id"],
                "status": "invalid" if failures else "completed",
                "result_evidence": evidence,
                "error": ",".join(failures) if failures else None,
                "actual_runtime_seconds": 0.01,
                "actual_compute_units": 0.1,
            }
        )
        all_attempt_ids.append(terminal["id"])
        if terminal["status"] == "completed":
            validation_rows.append((evaluation, graph_record, terminal))

    attempt_evidence_hash = stable_hash(all_attempt_ids)
    _transition(
        case["id"],
        "EVIDENCE_PRODUCED",
        evidence_hashes=(attempt_evidence_hash, charter.charter_hash),
    )
    progression = _execution_progression_evidence(charter)
    if not validation_rows:
        _transition(
            case["id"],
            "REJECTED",
            evidence_hashes=(attempt_evidence_hash, progression["evidence_hash"]),
        )
        authority.archive_rejected_family(
            {
                "actor_id": PROTOCOL_AUTHORITY_ID,
                "actor_role": "research_authority",
                "request_id": f"{charter.campaign_id}:family-archive-no-candidate",
                "family_id": family["id"],
                "reason": "no_validation_qualified_candidate",
            }
        )
        _transition(
            case["id"],
            "ARCHIVED",
            evidence_hashes=(attempt_evidence_hash,),
        )
        return {
            "schema_version": "autonomous_campaign_result.v1",
            "campaign_id": charter.campaign_id,
            "outcome": "rejected_before_holdout",
            "reason": "no_validation_qualified_candidate",
            "protocol_id": protocol["id"],
            "protocol_hash": protocol["protocol_hash"],
            "family_id": family["id"],
            "governance_case_id": case["id"],
            "attempt_count": len(all_attempt_ids),
            "validation_qualified_count": 0,
            "holdout_opened": False,
            "execution_progression": progression,
            "instrument_economics_class": "incomplete",
            "promotion_eligible": False,
            "external_trading_authority": False,
            "elapsed_seconds": time.perf_counter() - started,
        }

    ranked_validation = rank_evaluations(
        (row[0] for row in validation_rows), primary_metric=charter.primary_metric
    )
    winner = ranked_validation[0]
    _, winner_graph, winner_attempt = next(
        row for row in validation_rows if row[0].graph_hash == winner.graph_hash
    )
    candidate = authority.freeze_candidate(
        {
            "actor_id": RESEARCH_AGENT_ID,
            "actor_role": "research_agent",
            "request_id": f"{charter.campaign_id}:candidate-freeze",
            "candidate": {
                "family_id": family["id"],
                "protocol_hash": protocol["protocol_hash"],
                "source_attempt_id": winner_attempt["id"],
                "strategy_artifact_hash": winner.graph_hash,
                "parameter_artifact_hash": winner.parameter_artifact_hash,
                "execution_model_hash": winner.execution_model_hash,
                "metric_contract_hash": winner.metric_contract_hash,
                "research_dataset_hashes": [
                    charter.dataset("train").dataset_hash,
                    charter.dataset("validation").dataset_hash,
                ],
                "evidence_hashes": [
                    winner.artifact_hash,
                    charter.charter_hash,
                    progression["evidence_hash"],
                ],
            },
        }
    )
    _transition(
        case["id"],
        "CANDIDATE_NOMINATED",
        evidence_hashes=(candidate["candidate_hash"], winner.artifact_hash),
        binding_updates={"candidate_id": candidate["id"]},
    )
    _transition(
        case["id"],
        "VALIDATION_PASSED",
        evidence_hashes=(winner.artifact_hash,),
    )
    authority.close_family(
        {
            "actor_id": PROTOCOL_AUTHORITY_ID,
            "actor_role": "research_authority",
            "request_id": f"{charter.campaign_id}:family-close",
            "family_id": family["id"],
        }
    )
    _transition(
        case["id"],
        "HOLDOUT_ELIGIBLE",
        evidence_hashes=(candidate["candidate_hash"],),
    )
    holdout, token = authority_repository.reserve_holdout(
        family_id=family["id"],
        candidate_id=candidate["id"],
        actor_id=HOLDOUT_AUTHORITY_ID,
        actor_role="research_authority",
        request_id=f"{charter.campaign_id}:holdout-reserve",
    )
    holdout_features = build_campaign_features(
        _load_bars(charter, "holdout"), lookback_bars=charter.feature_lookback_bars
    )
    holdout_graph = TypedStrategyGraph.from_dict(winner_graph["manifest"])
    holdout_evaluation = evaluate_campaign_graph(
        charter=charter,
        graph=holdout_graph,
        rows=holdout_features,
        execution=costs,
        scoring_indexes=full_scoring_indexes(charter, len(holdout_features)),
    )
    holdout_failures = _gate_failures(charter, holdout_evaluation)
    if holdout_failures:
        holdout_evidence = holdout_evaluation.to_attempt_evidence(
            charter=charter, validation=False
        )
        authority.reject_holdout_internal(
            holdout_use_id=holdout["id"],
            reservation_token=token,
            result_evidence=holdout_evidence,
            reason_codes=holdout_failures,
            executor_actor=HOLDOUT_EXECUTOR_ID,
            request_id=f"{charter.campaign_id}:holdout-reject",
        )
        failure_hash = stable_hash(
            {
                "holdout_use_id": holdout["id"],
                "gate_failures": list(holdout_failures),
                "result_artifact_hash": holdout_evaluation.artifact_hash,
            }
        )
        _transition(
            case["id"],
            "REJECTED",
            evidence_hashes=(failure_hash, candidate["candidate_hash"]),
        )
        authority.archive_rejected_family(
            {
                "actor_id": PROTOCOL_AUTHORITY_ID,
                "actor_role": "research_authority",
                "request_id": f"{charter.campaign_id}:family-archive-holdout-rejection",
                "family_id": family["id"],
                "reason": "sealed_holdout_gate_rejection",
            }
        )
        _transition(
            case["id"],
            "ARCHIVED",
            evidence_hashes=(failure_hash,),
        )
        return {
            "schema_version": "autonomous_campaign_result.v1",
            "campaign_id": charter.campaign_id,
            "outcome": "rejected_at_sealed_holdout_gate",
            "reason_codes": list(holdout_failures),
            "protocol_id": protocol["id"],
            "protocol_hash": protocol["protocol_hash"],
            "family_id": family["id"],
            "candidate_id": candidate["id"],
            "candidate_hash": candidate["candidate_hash"],
            "governance_case_id": case["id"],
            "attempt_count": len(all_attempt_ids),
            "holdout_opened": True,
            "holdout_consumed": True,
            "holdout_metrics_released": False,
            "sealed_failure_evidence_hash": failure_hash,
            "execution_progression": progression,
            "instrument_economics_class": "incomplete",
            "promotion_eligible": False,
            "external_trading_authority": False,
            "elapsed_seconds": time.perf_counter() - started,
        }

    holdout_evidence = holdout_evaluation.to_attempt_evidence(
        charter=charter, validation=False
    )
    authority.execute_holdout_internal(
        holdout_use_id=holdout["id"],
        reservation_token=token,
        result_evidence=holdout_evidence,
        executor_actor=HOLDOUT_EXECUTOR_ID,
        request_id=f"{charter.campaign_id}:holdout-execute",
    )
    _transition(
        case["id"],
        "HOLDOUT_EVALUATED",
        evidence_hashes=(holdout_evaluation.artifact_hash,),
    )
    certificate = authority.certify_family(
        {
            "actor_id": SCIENCE_AUTHORITY_ID,
            "actor_role": "research_authority",
            "request_id": f"{charter.campaign_id}:certify",
            "family_id": family["id"],
            "robustness": {
                "passed": list(holdout_evaluation.robustness_passed),
                "cost_stress_passed": holdout_evaluation.cost_stress_passed,
                "latency_stress_passed": False,
            },
        }
    )
    if certificate["status"] == "qualified":
        _transition(
            case["id"],
            "RESEARCH_CERTIFIED",
            evidence_hashes=(certificate["certificate_hash"],),
            binding_updates={"certificate_id": certificate["id"]},
            authorizer=HUMAN_OWNER_ID,
            authorizer_role="human_research_owner",
        )
        outcome = "research_certified"
    else:
        _transition(
            case["id"],
            "REJECTED",
            evidence_hashes=(certificate["certificate_hash"],),
        )
        _transition(
            case["id"],
            "ARCHIVED",
            evidence_hashes=(certificate["certificate_hash"],),
        )
        outcome = "certificate_blocked_and_archived"
    return {
        "schema_version": "autonomous_campaign_result.v1",
        "campaign_id": charter.campaign_id,
        "outcome": outcome,
        "protocol_id": protocol["id"],
        "protocol_hash": protocol["protocol_hash"],
        "family_id": family["id"],
        "candidate_id": candidate["id"],
        "candidate_hash": candidate["candidate_hash"],
        "certificate_id": certificate["id"],
        "certificate_hash": certificate["certificate_hash"],
        "scientific_quality_class": certificate["scientific_quality_class"],
        "certificate_status": certificate["status"],
        "governance_case_id": case["id"],
        "attempt_count": len(all_attempt_ids),
        "holdout_opened": True,
        "holdout_consumed": True,
        "holdout_metrics_released": True,
        "execution_progression": progression,
        "execution_context_hash": execution_context["context_hash"],
        "instrument_economics_class": "incomplete",
        "promotion_eligible": False,
        "external_trading_authority": False,
        "elapsed_seconds": time.perf_counter() - started,
    }


def campaign_evidence(family_id: str, governance_case_id: str) -> dict[str, Any]:
    return {
        "schema_version": "autonomous_campaign_dossier_source.v1",
        "family": authority.family_evidence(family_id),
        "governance": governance.case_trail(governance_case_id),
        "external_trading_authority": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "execute"):
        command = subparsers.add_parser(name)
        command.add_argument("--charter", required=True)
        command.add_argument("--code-revision", required=True)
    evidence = subparsers.add_parser("evidence")
    evidence.add_argument("--family-id", required=True)
    evidence.add_argument("--governance-case-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "preflight":
        result = preflight_campaign(args.charter, code_revision=args.code_revision)
    elif args.command == "execute":
        result = execute_campaign(args.charter, code_revision=args.code_revision)
    else:
        result = campaign_evidence(args.family_id, args.governance_case_id)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "campaign_evidence",
    "execute_campaign",
    "load_private_charter",
    "main",
    "preflight_campaign",
]

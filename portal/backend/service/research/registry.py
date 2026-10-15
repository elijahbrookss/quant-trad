"""Registered Check definitions and evaluator adapters for the research service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from market_data.contracts import SourceIdentity
from market_data.fact_registry import (
    FactPayloadKind,
    get_fact_contract,
    get_fact_payload_schema,
)
from market_data.frozen import semantic_hash
from research_science.check import (
    CHECK_DEFINITION_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_MODE_PREVIEW,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    GAP_POLICY_CONTINUE_DEGRADED,
    GAP_POLICY_RESET_REWARM,
    CheckDefinition,
    CheckRegistry,
    CheckRequest,
    ResolvedCheckPlan,
)

from . import checks
from .liquidity_episode_evaluator import (LiquidityEpisodeEvaluator, SCHEMA as EPISODE_SCHEMA, RULES as EPISODE_RULES, normalize_configuration as normalize_episode_configuration)
from .candle_distribution_evaluator import (CandleDistributionEvaluator, OUTCOME_SCHEMA as DISTRIBUTION_SCHEMA, RULES as DISTRIBUTION_RULES, normalize_configuration as normalize_distribution_configuration)
from .raw_candle_evaluator import RawCandleEvaluator, OUTCOME_SCHEMA, normalize_configuration as normalize_raw_candle_configuration
from .controlled_reversal_evaluator import (ControlledReversalEvaluator, FIXED_RULES,
    OUTCOME_SCHEMA as REVERSAL_SCHEMA, normalize_configuration as normalize_reversal_configuration,
    ConfigurableControlledReversalEvaluator, CONFIGURABLE_RULES, CONFIGURABLE_SCHEMA,
    normalize_configurable_configuration)
from .retrospective_numeric import SCHEMA as RETROSPECTIVE_SCHEMA, normalize_retrospective
from .matched_origin_evaluator import MatchedOriginEvaluator, normalize_matched_origin
from .shared_landmark_evaluator import SharedLandmarkEvaluator, normalize_shared_landmark
from .first_return_evaluator import FirstReturnEvaluator, normalize_first_return
from .forward_risk_evaluator import ForwardRiskEvaluator, normalize_forward_risk
from .event_fact_evaluator import (
    EVENT_FACT_ANALYSIS,
    EVENT_FACT_EVALUATOR_VERSION,
    EVENT_FACT_RESULT_VERSION,
    LEGACY_EVENT_FACT_EVALUATOR_VERSION,
    LEGACY_EVENT_FACT_RESULT_VERSION,
    EventFactEvaluator,
    normalize_event_fact_configuration,
)


LEGACY_CHECK_FAMILIES = (
    checks.RAW_FORWARD_OUTCOME,
    checks.INDICATOR_FORWARD_OUTCOME,
    checks.SIGNAL_AUDIT,
    checks.CANDIDATE_LIFECYCLE,
    checks.RUN_SIGNAL_SUMMARY,
    checks.RUN_DECISION_TRADE_COMPARISON,
)
REGISTERED_CHECK_FAMILIES = frozenset({*LEGACY_CHECK_FAMILIES, EVENT_FACT_ANALYSIS})
# Legacy family-level defaults; all operational admission uses definition_allows_evidence.
DURABLE_EVIDENCE_CHECK_FAMILIES = frozenset(
    {
        EVENT_FACT_ANALYSIS,
        checks.SIGNAL_AUDIT,
        checks.CANDIDATE_LIFECYCLE,
    }
)
PREVIEW_ONLY_CHECK_FAMILIES = frozenset(
    REGISTERED_CHECK_FAMILIES - DURABLE_EVIDENCE_CHECK_FAMILIES
)


@dataclass(frozen=True)
class _LegacyEvaluator:
    evaluator_id: str
    version: str = "2"

    def declare_requirements(
        self,
        *,
        definition: CheckDefinition,
        request: CheckRequest,
    ) -> Mapping[str, Any]:
        del definition
        outcomes = dict(request.parameters.get("outcomes") or {})
        raw_horizons = outcomes.get("forward_bars") or outcomes.get("horizons") or []
        horizons = sorted({int(value) for value in raw_horizons})
        if self.evaluator_id in {
            checks.RUN_SIGNAL_SUMMARY,
            checks.RUN_DECISION_TRADE_COMPARISON,
        }:
            return {
                "input_kind": "immutable_run_evidence",
                "indicator_ids": [],
                "warmup_floor_bars": 0,
                "feature_lookback_bars": 0,
                "feature_windows_seconds_by_alias": {},
                "outcome_horizons": [],
                "required_outcome_horizons": [],
                "horizon_kind": "none",
            }
        indicator_ids = []
        if self.evaluator_id in {
            checks.INDICATOR_FORWARD_OUTCOME,
            checks.SIGNAL_AUDIT,
            checks.CANDIDATE_LIFECYCLE,
        }:
            indicator_id = str(request.scope.get("indicator_id") or "").strip()
            if not indicator_id:
                raise ValueError(
                    "check_requirement_plan_invalid: indicator_id is required"
                )
            indicator_ids.append(indicator_id)
        return {
            "input_kind": "market_data",
            "indicator_ids": indicator_ids,
            "warmup_floor_bars": 14,
            "feature_lookback_bars": 0,
            "feature_windows_seconds_by_alias": {},
            "outcome_horizons": horizons,
            "required_outcome_horizons": horizons,
            "horizon_kind": str(outcomes.get("horizon_kind") or "bars"),
            "entry_lag_bars": int(outcomes.get("entry_lag_bars") or 0),
            "invalidation_max_bars": 0,
        }

    def evaluate(
        self,
        *,
        plan: ResolvedCheckPlan,
        inputs: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        detector = dict(inputs.get("detector") or {})
        outcomes = dict(inputs.get("outcomes") or {})
        data_quality = dict(inputs.get("data_quality") or {})
        gap_rejection = inputs.get("indicator_gap_rejection")
        if isinstance(gap_rejection, Mapping):
            return checks.blocked_check_result(
                reason="Indicator rejected the frozen Dataset gap under Check policy.",
                detector=detector,
                outcomes=outcomes,
                data_quality={
                    **data_quality,
                    "status": "blocked",
                    "gap_rejection": dict(gap_rejection),
                },
                check_family=self.evaluator_id,
            )
        if self.evaluator_id == checks.RAW_FORWARD_OUTCOME:
            return checks.evaluate_raw_event_check(
                inputs["candles"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
                evaluation_range=plan.evaluation_range,
            )
        if self.evaluator_id == checks.INDICATOR_FORWARD_OUTCOME:
            return checks.evaluate_indicator_forward_outcome(
                inputs["indicator_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
                evaluation_range=plan.evaluation_range,
            )
        if self.evaluator_id == checks.SIGNAL_AUDIT:
            return checks.evaluate_signal_audit(
                inputs["indicator_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        if self.evaluator_id == checks.CANDIDATE_LIFECYCLE:
            return checks.evaluate_candidate_lifecycle(
                inputs["indicator_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        if self.evaluator_id == checks.RUN_SIGNAL_SUMMARY:
            readiness = dict(inputs["run_evidence"].get("readiness") or {})
            if not bool(readiness.get("safe_to_compare", False)):
                return checks.blocked_check_result(
                    reason=str(
                        readiness.get("dataset_status")
                        or readiness.get("reason")
                        or "run research evidence is not comparison-ready"
                    ),
                    detector=detector,
                    outcomes=outcomes,
                    data_quality={
                        **data_quality,
                        "status": "blocked",
                        "readiness_status": readiness.get("dataset_status")
                        or readiness.get("reason"),
                        "caveats": list(readiness.get("caveats") or []),
                    },
                    check_family=self.evaluator_id,
                )
            return checks.evaluate_run_signal_summary(
                inputs["run_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        if self.evaluator_id == checks.RUN_DECISION_TRADE_COMPARISON:
            readiness = dict(inputs["run_evidence"].get("readiness") or {})
            if not bool(readiness.get("safe_to_compare", False)):
                return checks.blocked_check_result(
                    reason=str(
                        readiness.get("dataset_status")
                        or readiness.get("reason")
                        or "run research evidence is not comparison-ready"
                    ),
                    detector=detector,
                    outcomes=outcomes,
                    data_quality={
                        **data_quality,
                        "status": "blocked",
                        "readiness_status": readiness.get("dataset_status")
                        or readiness.get("reason"),
                        "caveats": list(readiness.get("caveats") or []),
                    },
                    check_family=self.evaluator_id,
                )
            return checks.evaluate_run_decision_trade_comparison(
                inputs["run_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        raise ValueError(f"check evaluator is not implemented: {self.evaluator_id}")


CHECK_REGISTRY = CheckRegistry()


def _register_legacy_families() -> None:
    for family in LEGACY_CHECK_FAMILIES:
        evaluator = _LegacyEvaluator(family)
        CHECK_REGISTRY.register_evaluator(evaluator)
        CHECK_REGISTRY.register_definition(
            CheckDefinition(
                schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
                definition_id=family,
                definition_version="2",
                evaluator_id=evaluator.evaluator_id,
                evaluator_version=evaluator.version,
                request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
                result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
                material_rules={
                    "family": family,
                    "compatibility_source": "research_check_result.v1",
                    "event_ownership": (
                        "indicator"
                        if family
                        in {
                            checks.INDICATOR_FORWARD_OUTCOME,
                            checks.SIGNAL_AUDIT,
                            checks.CANDIDATE_LIFECYCLE,
                        }
                        else "check"
                    ),
                },
            )
        )


_register_legacy_families()

_raw_candle_evaluator = RawCandleEvaluator()
CHECK_REGISTRY.register_evaluator(_raw_candle_evaluator)
CHECK_REGISTRY.register_definition(CheckDefinition(
    schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=checks.RAW_FORWARD_OUTCOME,
    definition_version="3", evaluator_id=_raw_candle_evaluator.evaluator_id,
    evaluator_version=_raw_candle_evaluator.version, request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
    result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
    material_rules={"family": checks.RAW_FORWARD_OUTCOME, "event_ownership": "check",
        "input_policy": "canonical_candle_clock_path.v1", "outcome_boundary": "evaluation_end_exclusive",
        "outcome_schema": OUTCOME_SCHEMA, "inference": "descriptive_only", "return_unit": "fraction",
        "limits": {"candles": 100000, "horizons": 20, "lag_plus_horizon_bars": 10000,
            "examples": 1000, "predicate_depth": 32, "predicate_nodes": 500}},
))


_reversal_evaluator = ControlledReversalEvaluator()
CHECK_REGISTRY.register_evaluator(_reversal_evaluator)
CHECK_REGISTRY.register_definition(CheckDefinition(
    schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=checks.RAW_FORWARD_OUTCOME,
    definition_version="4", evaluator_id=_reversal_evaluator.evaluator_id,
    evaluator_version=_reversal_evaluator.version, request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
    result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
    material_rules={"family": checks.RAW_FORWARD_OUTCOME, "event_ownership": "check",
        "input_policy": "canonical_candle_clock_path.v1", "outcome_schema": REVERSAL_SCHEMA,
        **FIXED_RULES},
))


_configurable_reversal = ConfigurableControlledReversalEvaluator()
CHECK_REGISTRY.register_evaluator(_configurable_reversal)
CHECK_REGISTRY.register_definition(CheckDefinition(
    schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=checks.RAW_FORWARD_OUTCOME,
    definition_version="5", evaluator_id=_configurable_reversal.evaluator_id,
    evaluator_version=_configurable_reversal.version, request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
    result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
    material_rules={"family": checks.RAW_FORWARD_OUTCOME, "event_ownership": "check",
        "input_policy": "canonical_candle_clock_path.v1", "outcome_schema": CONFIGURABLE_SCHEMA,
        **CONFIGURABLE_RULES},
))


_distribution_evaluator = CandleDistributionEvaluator()
CHECK_REGISTRY.register_evaluator(_distribution_evaluator)
CHECK_REGISTRY.register_definition(CheckDefinition(
    schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=checks.RAW_FORWARD_OUTCOME,
    definition_version="6", evaluator_id=_distribution_evaluator.evaluator_id,
    evaluator_version="6", request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
    result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
    material_rules={"family": checks.RAW_FORWARD_OUTCOME, "event_ownership": "check",
        "input_policy": "canonical_candle_clock_path.v1", "outcome_schema": DISTRIBUTION_SCHEMA,
        "outcome_boundary": "evaluation_end_exclusive", "distribution_rules": DISTRIBUTION_RULES},
))


def definition_allows_evidence(definition: CheckDefinition) -> bool:
    """Keep historical raw v2 unqualified; admit only exact registered raw v3/v4/v5/v6 pairs."""
    if definition.definition_id == checks.RAW_FORWARD_OUTCOME:
        return (definition.definition_version.split("+", 1)[0] in {"3", "4", "5", "6"}
                and definition.evaluator_id == checks.RAW_FORWARD_OUTCOME
                and definition.evaluator_version == definition.definition_version.split("+", 1)[0])
    return definition.definition_id in DURABLE_EVIDENCE_CHECK_FAMILIES


def _register_event_fact_family() -> None:
    retrospective_evaluator = EventFactEvaluator(version="13",
        result_schema_version="event_fact_analysis_result.v13", retrospective_enabled=True)
    CHECK_REGISTRY.register_evaluator(retrospective_evaluator)
    CHECK_REGISTRY.register_definition(CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
        definition_version="13", evaluator_id=retrospective_evaluator.evaluator_id,
        evaluator_version="13", request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "check",
            "input_policy": RETROSPECTIVE_SCHEMA, "operator_model": "registered_event_fact_operators.v4",
            "inference": "retrospective_conditional_association", "point_in_time_predictive": False,
            "scientific_certification_eligible": False, "backtest_certified": False,
            "limits": {"evaluation_days":60, "frozen_revisions_per_alias":10000}},
    ))
    legacy_evaluator = EventFactEvaluator(
        version=LEGACY_EVENT_FACT_EVALUATOR_VERSION,
        result_schema_version=LEGACY_EVENT_FACT_RESULT_VERSION,
        fact_snapshot_enabled=False,
    )
    evaluator = EventFactEvaluator()
    CHECK_REGISTRY.register_evaluator(legacy_evaluator)
    CHECK_REGISTRY.register_evaluator(evaluator)
    availability_evaluator = EventFactEvaluator(
        version="4",
        result_schema_version="event_fact_analysis_result.v4",
        availability_trigger_enabled=True,
    )
    CHECK_REGISTRY.register_evaluator(availability_evaluator)
    candle_evaluator = EventFactEvaluator(
        version="5",
        result_schema_version="event_fact_analysis_result.v5",
        descriptive_outcomes_enabled=True,
    )
    CHECK_REGISTRY.register_evaluator(candle_evaluator)
    forward_risk_evaluator = ForwardRiskEvaluator()
    CHECK_REGISTRY.register_evaluator(forward_risk_evaluator)
    CHECK_REGISTRY.register_definition(CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
        definition_version="10", evaluator_id=forward_risk_evaluator.evaluator_id,
        evaluator_version=forward_risk_evaluator.version,
        request_schema_version=CHECK_REQUEST_SCHEMA_VERSION, result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "check_candle_snapshot",
            "input_policy": "candle_only_indicator.v1", "operator_model": "candle_risk_comparison.v1",
            "outcome_boundary": "evaluation_end_exclusive", "inference": "descriptive_only"},
    ))
    paired_state_evaluator = ForwardRiskEvaluator(version="10",
        result_schema_version="event_fact_analysis_result.v10", paired_state_enabled=True)
    CHECK_REGISTRY.register_evaluator(paired_state_evaluator)
    CHECK_REGISTRY.register_definition(CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
        definition_version="11", evaluator_id=paired_state_evaluator.evaluator_id,
        evaluator_version=paired_state_evaluator.version,
        request_schema_version=CHECK_REQUEST_SCHEMA_VERSION, result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "check_candle_snapshot",
            "input_policy": "candle_only_indicator.v1", "operator_model": "crossing_state_matched_pairs.v1",
            "outcome_boundary": "evaluation_end_exclusive", "inference": "descriptive_only"},
    ))
    atr_filtered_evaluator = ForwardRiskEvaluator(version="11",
        result_schema_version="event_fact_analysis_result.v11", paired_state_enabled=True,
        atr_ratio_filter_enabled=True)
    CHECK_REGISTRY.register_evaluator(atr_filtered_evaluator)
    CHECK_REGISTRY.register_definition(CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
        definition_version="12", evaluator_id=atr_filtered_evaluator.evaluator_id,
        evaluator_version=atr_filtered_evaluator.version,
        request_schema_version=CHECK_REQUEST_SCHEMA_VERSION, result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "check_candle_snapshot",
            "input_policy": "candle_only_indicator.v1", "operator_model": "crossing_state_matched_pairs.v2",
            "filter_contract": "crossing_state_atr_ratio_filter.v1",
            "outcome_boundary": "evaluation_end_exclusive", "inference": "descriptive_only"},
    ))
    first_return_evaluator = FirstReturnEvaluator()
    CHECK_REGISTRY.register_evaluator(first_return_evaluator)
    CHECK_REGISTRY.register_definition(CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
        definition_version="9", evaluator_id=first_return_evaluator.evaluator_id,
        evaluator_version=first_return_evaluator.version,
        request_schema_version=CHECK_REQUEST_SCHEMA_VERSION, result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
        material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "indicator",
            "input_policy": "candle_only_indicator.v1", "operator_model": "first_return_comparison.v1",
            "outcome_summary": "eligible_population_descriptive.v1"},
    ))
    landmark_evaluator = SharedLandmarkEvaluator()
    CHECK_REGISTRY.register_evaluator(landmark_evaluator)
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="8",
            evaluator_id=landmark_evaluator.evaluator_id,
            evaluator_version=landmark_evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "indicator",
                "input_policy": "candle_only_indicator.v1", "operator_model": "shared_landmark_comparison.v1",
                "followup_scope": "same_evaluation_window.v1", "outcome_summary": "eligible_population_descriptive.v1"},
        )
    )
    matched_evaluator = MatchedOriginEvaluator()
    CHECK_REGISTRY.register_evaluator(matched_evaluator)
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="7",
            evaluator_id=matched_evaluator.evaluator_id,
            evaluator_version=matched_evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={
                "family": EVENT_FACT_ANALYSIS,
                "event_ownership": "indicator",
                "input_policy": "candle_only_indicator.v1",
                "operator_model": "matched_origin_attribution.v1",
                "followup_scope": "same_evaluation_window.v1",
                "outcome_summary": "eligible_population_descriptive.v1",
            },
        )
    )
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="6",
            evaluator_id=candle_evaluator.evaluator_id,
            evaluator_version=candle_evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={
                "family": EVENT_FACT_ANALYSIS,
                "event_ownership": "indicator",
                "operator_model": "registered_event_fact_operators.v5",
                "input_policy": "candle_only_indicator.v1",
                "outcome_summary": "eligible_population_descriptive.v1",
            },
        )
    )
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="5",
            evaluator_id=availability_evaluator.evaluator_id,
            evaluator_version=availability_evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={
                "family": EVENT_FACT_ANALYSIS,
                "event_ownership": "indicator_or_check",
                "operator_model": "registered_event_fact_operators.v4",
            },
        )
    )
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="3",
            evaluator_id=legacy_evaluator.evaluator_id,
            evaluator_version=legacy_evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={
                "family": EVENT_FACT_ANALYSIS,
                "event_ownership": "indicator",
                "operator_model": "registered_event_fact_operators.v2",
            },
        )
    )
    CHECK_REGISTRY.register_definition(
        CheckDefinition(
            schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
            definition_id=EVENT_FACT_ANALYSIS,
            definition_version="4",
            evaluator_id=evaluator.evaluator_id,
            evaluator_version=evaluator.version,
            request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
            result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
            material_rules={
                "family": EVENT_FACT_ANALYSIS,
                "event_ownership": "indicator_or_check",
                "operator_model": "registered_event_fact_operators.v3",
            },
        )
    )


_episode_evaluator = LiquidityEpisodeEvaluator()
CHECK_REGISTRY.register_evaluator(_episode_evaluator)
CHECK_REGISTRY.register_definition(CheckDefinition(
    schema_version=CHECK_DEFINITION_SCHEMA_VERSION, definition_id=EVENT_FACT_ANALYSIS,
    definition_version="14", evaluator_id=_episode_evaluator.evaluator_id,
    evaluator_version="14", request_schema_version=CHECK_REQUEST_SCHEMA_VERSION,
    result_schema_version=CHECK_RESULT_SCHEMA_VERSION,
    material_rules={"family": EVENT_FACT_ANALYSIS, "event_ownership": "check",
        "input_policy": EPISODE_SCHEMA, "episode_rules": EPISODE_RULES,
        "inference": "retrospective_descriptive_only"},
))


_register_event_fact_family()


def _mapping(value: Any, *, field: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return dict(value)


def _list_of_mappings(value: Any, *, field: str) -> list[dict[str, Any]]:
    if value in (None, ""):
        return []
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise ValueError(f"{field} must be a list of objects")
    return [dict(row) for row in value]


def normalize_fact_inputs(value: Any, *, mode: str) -> list[dict[str, Any]]:
    """Normalize provider-neutral fact aliases and constrained source policies."""

    inputs = _list_of_mappings(value, field="inputs")
    aliases: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for raw in inputs:
        alias = str(raw.get("alias") or raw.get("key") or "").strip()
        if not alias or alias in aliases:
            raise ValueError("check inputs require unique aliases")
        aliases.add(alias)
        fact_type = str(raw.get("fact_type") or "").strip().lower()
        contract_version = str(raw.get("contract_version") or "").strip()
        if not fact_type or not contract_version:
            raise ValueError(
                f"check input {alias} requires fact_type and contract_version"
            )
        contract = get_fact_contract(fact_type)
        timeframe_seconds = raw.get("timeframe_seconds")
        contract.validate(
            contract_version=contract_version,
            timeframe_seconds=(
                int(timeframe_seconds)
                if timeframe_seconds not in (None, "")
                else None
            ),
        )
        source_policy = _mapping(raw.get("source_policy"), field=f"inputs.{alias}.source_policy")
        policy_mode = str(
            source_policy.get("mode")
            or ("exact" if mode == CHECK_MODE_EVIDENCE else "current")
        ).strip().lower()
        if policy_mode not in {"exact", "allowlist", "current"}:
            raise ValueError(f"unsupported source policy: {policy_mode}")
        if mode == CHECK_MODE_EVIDENCE and policy_mode == "current":
            raise ValueError(
                "check_evidence_unconstrained_source_forbidden: use exact or allowlist"
            )
        if mode == CHECK_MODE_EVIDENCE and raw.get("series_required") is False:
            raise ValueError(
                "check_evidence_optional_series_unsupported: v1 evidence inputs must bind a series"
            )
        normalized_policy = {**source_policy, "mode": policy_mode}
        if policy_mode == "exact":
            exact_key = str(source_policy.get("source_identity_key") or "").strip()
            if not exact_key:
                binding = _mapping(
                    source_policy.get("provider_binding"),
                    field=f"inputs.{alias}.source_policy.provider_binding",
                )
                required_binding_fields = (
                    "provider",
                    "venue",
                    "source_kind",
                    "adapter_version",
                )
                missing_binding_fields = [
                    field
                    for field in required_binding_fields
                    if not str(binding.get(field) or "").strip()
                ]
                if missing_binding_fields:
                    raise ValueError(
                        f"check input {alias} exact source policy requires "
                        "source_identity_key or a complete provider_binding"
                    )
                exact_key = SourceIdentity(
                    provider=str(binding["provider"]),
                    venue=str(binding["venue"]),
                    source_kind=str(binding["source_kind"]),
                    adapter_version=str(binding["adapter_version"]),
                ).identity_key
            normalized_policy["source_identity_key"] = exact_key
        if policy_mode == "allowlist":
            allowed = sorted(
                {
                    str(item).strip()
                    for item in source_policy.get("source_identity_keys") or []
                    if str(item).strip()
                }
            )
            if not allowed:
                raise ValueError(
                    f"check input {alias} allowlist requires source_identity_keys"
                )
            normalized_policy["source_identity_keys"] = allowed
        normalized.append(
            {
                **raw,
                "alias": alias,
                "fact_type": fact_type,
                "contract_version": contract_version,
                "source_policy": normalized_policy,
            }
        )
    return normalized


_NUMERIC_PAYLOAD_KINDS = frozenset(
    {
        FactPayloadKind.DECIMAL,
        FactPayloadKind.FLOAT64,
        FactPayloadKind.INTEGER,
    }
)
_L2_RESEARCH_FACT_TYPES = frozenset(
    {"market.bbo", "market.depth_observation"}
)


def _input_by_alias(inputs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["alias"]): row for row in inputs}


def _require_input(
    aliases: Mapping[str, Mapping[str, Any]],
    *,
    alias: str,
    consumer: str,
) -> Mapping[str, Any]:
    row = aliases.get(alias)
    if row is None:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} references undeclared input_alias={alias or '<empty>'}"
        )
    return row


def _structured_payload_schema(
    raw_input: Mapping[str, Any],
    *,
    consumer: str,
):
    fact_type = str(raw_input["fact_type"])
    contract_version = str(raw_input["contract_version"])
    contract = get_fact_contract(fact_type)
    try:
        schema = get_fact_payload_schema(contract_version)
    except ValueError as exc:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} requires a registered structured payload schema"
        ) from exc
    if contract.uses_exact_numeric_storage:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} requires a structured canonical Fact input"
        )
    if schema.fact_type != fact_type:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} payload schema does not match fact_type={fact_type}"
        )
    if not contract.dataset_eligible or not schema.dataset_eligible:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} input is not Dataset eligible fact_type={fact_type}"
        )
    if fact_type not in _L2_RESEARCH_FACT_TYPES:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} structured research currently supports only frozen BBO/depth facts; "
            f"fact_type={fact_type}"
        )
    return schema


def _explicit_staleness(raw_input: Mapping[str, Any], *, consumer: str) -> int:
    raw = raw_input.get("max_staleness_seconds")
    if raw in (None, "") or isinstance(raw, bool):
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} requires explicit max_staleness_seconds"
        )
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} max_staleness_seconds must be positive"
        ) from exc
    if value <= 0:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} max_staleness_seconds must be positive"
        )
    return value


def _payload_fields(schema: Any) -> dict[str, Any]:
    return {str(field.name): field for field in schema.fields}


def _normalize_schema_where(
    value: Mapping[str, Any],
    *,
    schema: Any,
    consumer: str,
) -> dict[str, Any]:
    fields = _payload_fields(schema)
    normalized: dict[str, Any] = {}
    for path, expected in sorted(value.items()):
        field_name = str(path).split(".", 1)[-1]
        field = fields.get(field_name)
        if field is None or field_name not in schema.query_fields:
            raise ValueError(
                "event_fact_check_invalid: "
                f"{consumer} field={path} is not a declared query field"
            )
        try:
            normalized[str(path)] = field.normalize(expected)
        except ValueError as exc:
            raise ValueError(
                "event_fact_check_invalid: "
                f"{consumer} field={path} predicate is invalid"
            ) from exc
    return normalized


def _normalize_structured_path(
    path: str,
    *,
    schema: Any,
    consumer: str,
) -> str:
    field_name = str(path).split(".", 1)[-1]
    field = _payload_fields(schema).get(field_name)
    if field is None or field_name not in schema.query_fields:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} field={path} is not a declared query field"
        )
    if field.kind not in _NUMERIC_PAYLOAD_KINDS:
        raise ValueError(
            "event_fact_check_invalid: "
            f"{consumer} field={path} must be numeric"
        )
    return str(path)


def _validate_event_fact_bindings(
    *,
    detector: Mapping[str, Any],
    statistics: Mapping[str, Any],
    inputs: list[dict[str, Any]],
    retrospective: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Bind every operator to one declared, schema-constrained Fact input."""

    aliases = _input_by_alias(inputs)
    normalized_detector = dict(detector)
    normalized_statistics = dict(statistics)
    normalized_features = dict(normalized_statistics.get("features") or {})
    normalized_enriched: list[dict[str, Any]] = []

    if str(normalized_detector.get("type") or "") == "fact_snapshot" and not retrospective:
        alias = str(normalized_detector["input_alias"])
        raw_input = _require_input(
            aliases,
            alias=alias,
            consumer="detector",
        )
        schema = _structured_payload_schema(raw_input, consumer="detector")
        _explicit_staleness(raw_input, consumer="detector")
        normalized_detector["where"] = _normalize_schema_where(
            dict(normalized_detector.get("where") or {}),
            schema=schema,
            consumer="detector.where",
        )

    for raw in normalized_features.get("enriched") or []:
        feature = dict(raw)
        name = str(feature["name"])
        alias = str(feature["input_alias"])
        raw_input = _require_input(
            aliases,
            alias=alias,
            consumer=f"feature={name}",
        )
        contract = get_fact_contract(str(raw_input["fact_type"]))
        if str(feature["operator"]) == "latest_payload_number":
            schema = _structured_payload_schema(
                raw_input,
                consumer=f"feature={name}",
            )
            _explicit_staleness(raw_input, consumer=f"feature={name}")
            feature["path"] = _normalize_structured_path(
                str(feature["path"]),
                schema=schema,
                consumer=f"feature={name}",
            )
            feature["where"] = _normalize_schema_where(
                dict(feature.get("where") or {}),
                schema=schema,
                consumer=f"feature={name}.where",
            )
        elif not contract.uses_exact_numeric_storage:
            raise ValueError(
                "event_fact_check_invalid: "
                f"feature={name} operator={feature['operator']} requires an exact numeric Fact input"
            )
        normalized_enriched.append(feature)

    if normalized_detector.get("evaluation_trigger") == "required_facts_available":
        for alias, raw_input in aliases.items():
            _explicit_staleness(raw_input, consumer=f"evaluation_trigger.{alias}")

    normalized_features["enriched"] = normalized_enriched
    normalized_statistics["features"] = normalized_features
    return normalized_detector, normalized_statistics, inputs


def _resolve_definition_version(payload, *, family, base_version):
    """Select existing historical procedures; no taxonomy for future methods."""
    availability_trigger = (
        _mapping(payload.get("detector"), field="detector").get("evaluation_trigger")
        == "required_facts_available"
    )
    candle_only = (
        family == EVENT_FACT_ANALYSIS
        and _mapping(payload.get("detector"), field="detector").get("type")
        == "indicator_event"
        and not payload.get("inputs")
    )
    matched_origin = _mapping(payload.get("outcomes"), field="outcomes").get("matched_origin")
    shared_landmark = _mapping(payload.get("outcomes"), field="outcomes").get("shared_landmark")
    first_return = _mapping(payload.get("outcomes"), field="outcomes").get("first_return")
    forward_risk = _mapping(payload.get("outcomes"), field="outcomes").get("forward_risk")
    paired_state = isinstance(forward_risk, Mapping) and forward_risk.get("schema_version") == "candle_risk_matched_state.v1"
    atr_filter = isinstance(forward_risk, Mapping) and forward_risk.get("schema_version") == "candle_risk_matched_state.v2"
    if atr_filter:
        risk_version = "12"
    elif paired_state:
        risk_version = "11"
    else:
        risk_version = "10"
    retrospective = _mapping(payload.get("outcomes"), field="outcomes").get("retrospective")
    if family == EVENT_FACT_ANALYSIS and _mapping(payload.get("outcomes"), field="outcomes").get("schema_version") == EPISODE_SCHEMA:
        default_version = "14"
    elif retrospective is not None:
        default_version = "13"
    elif forward_risk is not None:
        default_version = risk_version
    elif first_return is not None:
        default_version = "9"
    elif shared_landmark is not None:
        default_version = "8"
    elif matched_origin is not None:
        default_version = "7"
    elif availability_trigger:
        default_version = "5"
    elif candle_only:
        default_version = "6"
    else:
        default_version = "4"
    resolved_base_version = str(
        base_version or ("6" if family == checks.RAW_FORWARD_OUTCOME and
            _mapping(payload.get("outcomes"), field="outcomes").get("schema_version") == DISTRIBUTION_SCHEMA
            else "5" if family == checks.RAW_FORWARD_OUTCOME and
            _mapping(payload.get("outcomes"), field="outcomes").get("schema_version") == CONFIGURABLE_SCHEMA
            else "4" if family == checks.RAW_FORWARD_OUTCOME and
            _mapping(payload.get("outcomes"), field="outcomes").get("schema_version") == REVERSAL_SCHEMA
            else "3" if family == checks.RAW_FORWARD_OUTCOME and
            _mapping(payload.get("outcomes"), field="outcomes").get("schema_version") == OUTCOME_SCHEMA
            else default_version if family == EVENT_FACT_ANALYSIS else "2")
    )
    if retrospective is not None and (family != EVENT_FACT_ANALYSIS or resolved_base_version != "13"
            or any(v is not None for v in (matched_origin, shared_landmark, first_return, forward_risk))):
        raise ValueError("retrospective_invalid: requires standalone event_fact_analysis version 13")
    if resolved_base_version == "13" and retrospective is None:
        raise ValueError("retrospective_invalid: explicit analysis contract required")
    if availability_trigger and resolved_base_version != "5":
        raise ValueError(
            "event_fact_check_invalid: required_facts_available requires definition version 5"
        )
    if matched_origin is not None and (family != EVENT_FACT_ANALYSIS or resolved_base_version not in {"7", "8"}):
        raise ValueError("matched_origin_invalid: requires event_fact_analysis definition version 7 or 8")
    if shared_landmark is not None and (family != EVENT_FACT_ANALYSIS or resolved_base_version != "8" or matched_origin is None):
        raise ValueError("shared_landmark_invalid: requires matched_origin and definition version 8")
    if first_return is not None and (family != EVENT_FACT_ANALYSIS or resolved_base_version != "9" or matched_origin is not None or shared_landmark is not None):
        raise ValueError("first_return_invalid: requires standalone definition version 9")
    if forward_risk is not None and (family != EVENT_FACT_ANALYSIS or resolved_base_version != risk_version
            or any(v is not None for v in (matched_origin, shared_landmark, first_return))):
        raise ValueError(f"forward_risk_invalid: requires standalone definition version {risk_version}")
    return resolved_base_version, matched_origin, shared_landmark, first_return, forward_risk


def materialize_check_definition(
    payload: Mapping[str, Any],
    *,
    mode: str,
    base_version: str | None = None,
) -> CheckDefinition:
    family = str(payload.get("check_family") or checks.SUPPORTED_CHECK_FAMILY).strip()
    (resolved_base_version, matched_origin, shared_landmark,
     first_return, forward_risk) = _resolve_definition_version(
        payload, family=family, base_version=base_version,
    )
    base = CHECK_REGISTRY.resolve_definition(family, resolved_base_version)
    detector = _mapping(payload.get("detector"), field="detector")
    outcomes = _mapping(payload.get("outcomes"), field="outcomes")
    statistics = _mapping(payload.get("statistics"), field="statistics")
    if family == EVENT_FACT_ANALYSIS and resolved_base_version != "14":
        detector, outcomes, statistics = normalize_event_fact_configuration(
            detector=detector,
            outcomes=outcomes,
            statistics=statistics,
        )
    elif family != EVENT_FACT_ANALYSIS and not (family == checks.RAW_FORWARD_OUTCOME and resolved_base_version in {"3", "4", "5", "6"}):
        checks.validate_check_detector(check_family=family, detector=detector)
    scope = _mapping(payload.get("scope"), field="scope")
    inputs = normalize_fact_inputs(payload.get("inputs"), mode=mode)
    if family == EVENT_FACT_ANALYSIS and resolved_base_version in {"6", "7", "8", "9", "10", "11", "12"} and (
        inputs or detector.get("type") != "indicator_event"
    ):
        raise ValueError(
            f"event_fact_check_invalid: definition version {resolved_base_version} requires a candle-only indicator_event"
        )
    if family == EVENT_FACT_ANALYSIS and resolved_base_version in {"7", "8"}:
        outcomes["matched_origin"] = normalize_matched_origin(
            matched_origin, detector=detector, outcomes=outcomes, statistics=statistics,
        )
    if family == EVENT_FACT_ANALYSIS and resolved_base_version == "8":
        outcomes["shared_landmark"] = normalize_shared_landmark(
            shared_landmark, outcomes=outcomes, gap_policy=str(payload.get("gap_policy") or ""),
        )
    if family == EVENT_FACT_ANALYSIS and resolved_base_version in {"10", "11", "12"}:
        outcomes["forward_risk"] = normalize_forward_risk(forward_risk, detector=detector,
            outcomes=outcomes, statistics=statistics, gap_policy=str(payload.get("gap_policy") or ""),
            paired_state=resolved_base_version in {"11", "12"}, atr_ratio_filter=resolved_base_version == "12")
    if family == EVENT_FACT_ANALYSIS and resolved_base_version == "9":
        outcomes["first_return"] = normalize_first_return(first_return, outcomes=outcomes,
            detector=detector, statistics=statistics, gap_policy=str(payload.get("gap_policy") or ""))
    if family == EVENT_FACT_ANALYSIS and not inputs and resolved_base_version not in {"6", "7", "8", "9", "10", "11", "12"}:
        raise ValueError("event_fact_check_invalid: at least one typed fact input is required")
    if family == EVENT_FACT_ANALYSIS and resolved_base_version != "14":
        detector, statistics, inputs = _validate_event_fact_bindings(
            detector=detector,
            statistics=statistics,
            inputs=inputs,
            retrospective=resolved_base_version == "13",
        )
    if family == EVENT_FACT_ANALYSIS and resolved_base_version == "13":
        outcomes["retrospective"] = normalize_retrospective(
            _mapping(payload.get("outcomes"), field="outcomes").get("retrospective"),
            detector=detector, outcomes=outcomes, statistics=statistics, scope=scope, inputs=inputs)
    assertions = _list_of_mappings(payload.get("assertions"), field="assertions")
    if mode == CHECK_MODE_EVIDENCE and not str(payload.get("gap_policy") or "").strip():
        raise ValueError("check_evidence_gap_policy_required")
    gap_rewarm_bars = int(payload.get("gap_rewarm_bars") or 0)
    if gap_rewarm_bars < 0:
        raise ValueError("check gap_rewarm_bars must be nonnegative")
    gap_policy = str(
        payload.get("gap_policy") or GAP_POLICY_CONTINUE_DEGRADED
    ).strip().lower()
    if (
        family == EVENT_FACT_ANALYSIS
        and str(detector.get("type") or "") == "fact_snapshot"
        and gap_policy == GAP_POLICY_RESET_REWARM
    ):
        raise ValueError(
            "event_fact_check_invalid: fact_snapshot does not support reset_rewarm"
        )
    if family == checks.RAW_FORWARD_OUTCOME and resolved_base_version == "3":
        outcomes = normalize_raw_candle_configuration(detector=detector, outcomes=outcomes,
            statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
    if family == checks.RAW_FORWARD_OUTCOME and resolved_base_version == "6":
        outcomes = normalize_distribution_configuration(detector=detector, outcomes=outcomes,
            statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
        if gap_rewarm_bars:
            raise ValueError("candle_distribution_invalid: no configurable gap rewarm")
    if family == checks.RAW_FORWARD_OUTCOME and resolved_base_version == "4":
        outcomes = normalize_reversal_configuration(detector=detector, outcomes=outcomes,
            statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
        if gap_rewarm_bars:
            raise ValueError("controlled_reversal_invalid: no configurable gap rewarm")
    if family == checks.RAW_FORWARD_OUTCOME and resolved_base_version == "5":
        outcomes = normalize_configurable_configuration(detector=detector, outcomes=outcomes,
            statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
        if gap_rewarm_bars:
            raise ValueError("controlled_reversal_invalid: no configurable gap rewarm")
    if family == EVENT_FACT_ANALYSIS and resolved_base_version == "13" and (gap_rewarm_bars or gap_policy not in {"reject", "continue_degraded"}):
        raise ValueError("retrospective_invalid: explicit local exclusions or gap rejection; no rewarm")
    if family == EVENT_FACT_ANALYSIS and resolved_base_version == "14":
        detector, outcomes, statistics = normalize_episode_configuration(detector=detector,
            outcomes=outcomes, statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
        if gap_rewarm_bars:
            raise ValueError("liquidity_episode_invalid: no configurable rewarm")
    material_rules = {
        **dict(base.material_rules),
        "registered_definition": {
            "definition_id": base.definition_id,
            "definition_version": base.definition_version,
            "definition_hash": base.definition_hash,
        },
        "inputs": inputs,
        "indicator": {
            key: scope[key]
            for key in ("indicator_id", "indicator_param_overrides")
            if key in scope
        },
        "detector": detector,
        "outcomes": outcomes,
        "statistics": statistics,
        "assertions": assertions,
        "gap_policy": gap_policy,
        "gap_rewarm_bars": gap_rewarm_bars,
    }
    configured_version = (
        f"{base.definition_version}+{semantic_hash(material_rules)[:16]}"
    )
    return CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
        definition_id=base.definition_id,
        definition_version=configured_version,
        evaluator_id=base.evaluator_id,
        evaluator_version=base.evaluator_version,
        request_schema_version=base.request_schema_version,
        result_schema_version=base.result_schema_version,
        material_rules=material_rules,
    )


def normalize_check_request(
    payload: Mapping[str, Any],
    *,
    mode: str | None = None,
) -> tuple[CheckDefinition, CheckRequest]:
    normalized_mode = str(mode or payload.get("mode") or CHECK_MODE_PREVIEW).strip().lower()
    definition = materialize_check_definition(payload, mode=normalized_mode)
    if (
        normalized_mode == CHECK_MODE_EVIDENCE
        and not definition_allows_evidence(definition)
    ):
        raise ValueError(
            "check_evidence_family_preview_only: legacy evaluator cannot create new "
            f"durable evidence family={definition.definition_id}; use "
            f"{EVENT_FACT_ANALYSIS} or preview"
        )
    scope = _mapping(payload.get("scope"), field="scope")
    if (
        normalized_mode == CHECK_MODE_EVIDENCE
        and definition.definition_id
        not in {checks.RUN_SIGNAL_SUMMARY, checks.RUN_DECISION_TRADE_COMPARISON}
        and not str(scope.get("instrument_id") or "").strip()
    ):
        raise ValueError(
            "check_evidence_instrument_identity_required: scope.instrument_id must be pinned"
        )
    parameters = {
        "detector": dict(definition.material_rules["detector"]),
        "outcomes": dict(definition.material_rules["outcomes"]),
        "statistics": dict(definition.material_rules["statistics"]),
        "assertions": list(definition.material_rules["assertions"]),
        "inputs": list(definition.material_rules["inputs"]),
        "gap_policy": definition.material_rules["gap_policy"],
        "gap_rewarm_bars": definition.material_rules["gap_rewarm_bars"],
    }
    immutable_run_evidence = payload.get("immutable_run_evidence")
    return definition, CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=normalized_mode,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        definition_hash=definition.definition_hash,
        scope=scope,
        parameters=parameters,
        dataset_id=str(payload.get("dataset_id") or "").strip() or None,
        immutable_run_evidence=(
            dict(immutable_run_evidence)
            if isinstance(immutable_run_evidence, Mapping)
            else None
        ),
    )


def pin_check_definition_to_plan(
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
) -> tuple[CheckDefinition, CheckRequest, ResolvedCheckPlan]:
    """Make the exact resolved Indicator graph part of Check definition identity."""

    graph = [dict(row) for row in plan.indicator_graph]
    graph_hash = semantic_hash({"indicators": graph})
    material_rules = {
        key: value
        for key, value in dict(definition.material_rules).items()
        if key not in {"resolved_indicator_graph", "resolved_indicator_graph_hash"}
    }
    material_rules.update(
        {
            "resolved_indicator_graph": graph,
            "resolved_indicator_graph_hash": graph_hash,
        }
    )
    registered = dict(material_rules.get("registered_definition") or {})
    registered_version = str(
        registered.get("definition_version") or definition.definition_version
    )
    pinned_definition = CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
        definition_id=definition.definition_id,
        definition_version=(
            f"{registered_version}+{semantic_hash(material_rules)[:16]}"
        ),
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        request_schema_version=definition.request_schema_version,
        result_schema_version=definition.result_schema_version,
        material_rules=material_rules,
    )
    pinned_request = CheckRequest(
        schema_version=request.schema_version,
        mode=request.mode,
        definition_id=pinned_definition.definition_id,
        definition_version=pinned_definition.definition_version,
        definition_hash=pinned_definition.definition_hash,
        scope=request.scope,
        parameters=request.parameters,
        dataset_id=request.dataset_id,
        immutable_run_evidence=request.immutable_run_evidence,
    )
    plan_payload = plan.to_dict()
    plan_payload.pop("plan_hash", None)
    plan_payload["request_hash"] = pinned_request.request_hash
    pinned_plan = ResolvedCheckPlan.from_dict(plan_payload)
    return pinned_definition, pinned_request, pinned_plan


def reconstruct_pinned_check_definition(
    request: CheckRequest,
    plan: ResolvedCheckPlan,
) -> CheckDefinition:
    """Rebuild a pinned definition from only its canonical request and plan."""

    payload = {
        "check_family": request.definition_id,
        "scope": dict(request.scope),
        "detector": dict(request.parameters.get("detector") or {}),
        "outcomes": dict(request.parameters.get("outcomes") or {}),
        "statistics": dict(request.parameters.get("statistics") or {}),
        "assertions": list(request.parameters.get("assertions") or []),
        "inputs": list(request.parameters.get("inputs") or []),
        "gap_policy": request.parameters.get("gap_policy"),
        "gap_rewarm_bars": request.parameters.get("gap_rewarm_bars"),
    }
    registered_base_version = str(request.definition_version).split("+", 1)[0]
    unpinned = materialize_check_definition(
        payload,
        mode=request.mode,
        base_version=registered_base_version,
    )
    reconstructed, _request, _plan = pin_check_definition_to_plan(
        unpinned,
        CheckRequest(
            schema_version=request.schema_version,
            mode=request.mode,
            definition_id=unpinned.definition_id,
            definition_version=unpinned.definition_version,
            definition_hash=unpinned.definition_hash,
            scope=request.scope,
            parameters=request.parameters,
            dataset_id=request.dataset_id,
            immutable_run_evidence=request.immutable_run_evidence,
        ),
        ResolvedCheckPlan.from_dict(
            {
                **plan.to_dict(),
                "request_hash": request.request_hash,
                "plan_hash": "",
            }
        ),
    )
    return reconstructed


def normalize_check_preparation_request(
    payload: Mapping[str, Any],
) -> tuple[CheckDefinition, CheckRequest]:
    """Normalize a pre-freeze plan using durable-evidence validation rules.

    A Dataset does not exist yet, so the returned request is deliberately
    non-executing preview mode. Its definition and source policies are still
    the exact ones required by durable evidence. Preparation returns a real
    evidence request after it freezes the Dataset.
    """

    definition = materialize_check_definition(payload, mode=CHECK_MODE_EVIDENCE)
    if not definition_allows_evidence(definition):
        raise ValueError(
            "check_evidence_family_preview_only: legacy evaluator cannot create new "
            f"durable evidence family={definition.definition_id}; use "
            f"{EVENT_FACT_ANALYSIS} or preview"
        )
    parameters = {
        "detector": dict(definition.material_rules["detector"]),
        "outcomes": dict(definition.material_rules["outcomes"]),
        "statistics": dict(definition.material_rules["statistics"]),
        "assertions": list(definition.material_rules["assertions"]),
        "inputs": list(definition.material_rules["inputs"]),
        "gap_policy": definition.material_rules["gap_policy"],
        "gap_rewarm_bars": definition.material_rules["gap_rewarm_bars"],
    }
    return definition, CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=CHECK_MODE_PREVIEW,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        definition_hash=definition.definition_hash,
        scope=_mapping(payload.get("scope"), field="scope"),
        parameters=parameters,
    )


__all__ = [
    "CHECK_REGISTRY",
    "DURABLE_EVIDENCE_CHECK_FAMILIES",
    "definition_allows_evidence",
    "EVENT_FACT_ANALYSIS",
    "LEGACY_CHECK_FAMILIES",
    "REGISTERED_CHECK_FAMILIES",
    "PREVIEW_ONLY_CHECK_FAMILIES",
    "materialize_check_definition",
    "pin_check_definition_to_plan",
    "reconstruct_pinned_check_definition",
    "normalize_check_preparation_request",
    "normalize_check_request",
    "normalize_fact_inputs",
]

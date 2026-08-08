"""Registered Check definitions and evaluator adapters for the research service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from research_science.check import (
    CHECK_DEFINITION_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_MODE_PREVIEW,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    GAP_POLICY_CONTINUE_DEGRADED,
    CheckDefinition,
    CheckRegistry,
    CheckRequest,
    ResolvedCheckPlan,
)

from . import checks


EVENT_FACT_ANALYSIS = "event_fact_analysis"
LEGACY_CHECK_FAMILIES = (
    checks.RAW_FORWARD_OUTCOME,
    checks.INDICATOR_FORWARD_OUTCOME,
    checks.SIGNAL_AUDIT,
    checks.CANDIDATE_LIFECYCLE,
    checks.RUN_SIGNAL_SUMMARY,
    checks.RUN_DECISION_TRADE_COMPARISON,
)
REGISTERED_CHECK_FAMILIES = frozenset({*LEGACY_CHECK_FAMILIES, EVENT_FACT_ANALYSIS})


@dataclass(frozen=True)
class _LegacyEvaluator:
    evaluator_id: str
    version: str = "2"

    def evaluate(
        self,
        *,
        plan: ResolvedCheckPlan,
        inputs: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        detector = dict(inputs.get("detector") or {})
        outcomes = dict(inputs.get("outcomes") or {})
        data_quality = dict(inputs.get("data_quality") or {})
        if self.evaluator_id == checks.RAW_FORWARD_OUTCOME:
            return checks.evaluate_raw_event_check(
                inputs["candles"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        if self.evaluator_id == checks.INDICATOR_FORWARD_OUTCOME:
            return checks.evaluate_indicator_forward_outcome(
                inputs["indicator_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
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
            return checks.evaluate_run_signal_summary(
                inputs["run_evidence"],
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        if self.evaluator_id == checks.RUN_DECISION_TRADE_COMPARISON:
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
        normalized_policy = {**source_policy, "mode": policy_mode}
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


def materialize_check_definition(
    payload: Mapping[str, Any],
    *,
    mode: str,
) -> CheckDefinition:
    family = str(payload.get("check_family") or checks.SUPPORTED_CHECK_FAMILY).strip()
    base = CHECK_REGISTRY.resolve_definition(family, "2")
    detector = _mapping(payload.get("detector"), field="detector")
    checks.validate_check_detector(check_family=family, detector=detector)
    scope = _mapping(payload.get("scope"), field="scope")
    inputs = normalize_fact_inputs(payload.get("inputs"), mode=mode)
    assertions = _list_of_mappings(payload.get("assertions"), field="assertions")
    material_rules = {
        **dict(base.material_rules),
        "inputs": inputs,
        "indicator": {
            key: scope[key]
            for key in ("indicator_id", "indicator_param_overrides")
            if key in scope
        },
        "detector": detector,
        "outcomes": _mapping(payload.get("outcomes"), field="outcomes"),
        "statistics": _mapping(payload.get("statistics"), field="statistics"),
        "assertions": assertions,
        "gap_policy": str(
            payload.get("gap_policy") or GAP_POLICY_CONTINUE_DEGRADED
        ).strip().lower(),
    }
    return CheckDefinition(
        schema_version=CHECK_DEFINITION_SCHEMA_VERSION,
        definition_id=base.definition_id,
        definition_version=base.definition_version,
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
    parameters = {
        "detector": dict(definition.material_rules["detector"]),
        "outcomes": dict(definition.material_rules["outcomes"]),
        "statistics": dict(definition.material_rules["statistics"]),
        "assertions": list(definition.material_rules["assertions"]),
        "inputs": list(definition.material_rules["inputs"]),
        "gap_policy": definition.material_rules["gap_policy"],
    }
    immutable_run_evidence = payload.get("immutable_run_evidence")
    return definition, CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=normalized_mode,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        definition_hash=definition.definition_hash,
        scope=_mapping(payload.get("scope"), field="scope"),
        parameters=parameters,
        dataset_id=str(payload.get("dataset_id") or "").strip() or None,
        immutable_run_evidence=(
            dict(immutable_run_evidence)
            if isinstance(immutable_run_evidence, Mapping)
            else None
        ),
    )


__all__ = [
    "CHECK_REGISTRY",
    "EVENT_FACT_ANALYSIS",
    "LEGACY_CHECK_FAMILIES",
    "REGISTERED_CHECK_FAMILIES",
    "materialize_check_definition",
    "normalize_check_request",
    "normalize_fact_inputs",
]

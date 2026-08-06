"""Versioned, venue-neutral economic assumptions for deterministic execution.

The run-start boundary resolves this contract once and pins the resulting
manifest in the immutable run snapshot.  Runtime code consumes the manifest;
it must never infer economically material defaults from mutable bot state.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Mapping, Sequence


EXECUTION_ASSUMPTIONS_SCHEMA_VERSION = "execution_assumptions.v1"
LEGACY_BAR_MODEL_VERSION = "legacy_bar_touch.v1"
COSTED_BAR_MODEL_VERSION = "costed_bar.v1"
CONSERVATIVE_BAR_MODEL_VERSION = "conservative_bar.v1"


class EconomicClaimIntent(str, Enum):
    """The strongest economic interpretation authorized for one run."""

    EXPLORATION = "exploration"
    ECONOMIC = "economic"
    SELECTION = "selection"
    PROMOTION = "promotion"


class ExecutionQualityClass(str, Enum):
    """Implemented execution-quality ladder through visible L2 replay."""

    X0 = "X0"
    X1 = "X1"
    X2 = "X2"
    X3 = "X3"
    X4 = "X4"


_QUALITY_RANK = {item.value: index for index, item in enumerate(ExecutionQualityClass)}
_SUPPORTED_MODELS = {
    LEGACY_BAR_MODEL_VERSION,
    COSTED_BAR_MODEL_VERSION,
    CONSERVATIVE_BAR_MODEL_VERSION,
}
_SUPPORTED_PASSIVE_POLICIES = {"touch", "strict_penetration"}
_SUPPORTED_FEE_POLICIES = {"instrument_resolved", "explicit_zero"}


@dataclass(frozen=True)
class CostStressScenario:
    """Deterministic counterfactual cost stress applied in research evidence."""

    scenario_id: str
    additional_slippage_bps: float
    fee_multiplier: float


@dataclass(frozen=True)
class ResolvedExecutionAssumptions:
    """Immutable execution assumptions resolved for a single run."""

    schema_version: str
    economic_claim_intent: str
    model_version: str
    market_slippage_bps: float | None
    stop_slippage_bps: float | None
    passive_fill_policy: str
    fee_policy: str
    full_fill_assumption: bool
    explicit_zero_cost_override: bool
    cost_stress_scenarios: tuple[CostStressScenario, ...]
    execution_quality_ceiling: str
    source: str
    manifest_hash: str

    @property
    def requires_economic_evidence(self) -> bool:
        return self.economic_claim_intent != EconomicClaimIntent.EXPLORATION.value

    @property
    def uses_strict_penetration(self) -> bool:
        return self.passive_fill_policy == "strict_penetration"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_economic_claim_intent(value: Any) -> str:
    """Return a supported claim intent; absence is never silently defaulted."""

    normalized = str(value or "").strip().lower()
    try:
        return EconomicClaimIntent(normalized).value
    except ValueError as exc:
        supported = ", ".join(item.value for item in EconomicClaimIntent)
        raise ValueError(f"economic_claim_intent is required and must be one of: {supported}") from exc


def execution_quality_meets(actual: Any, minimum: Any) -> bool:
    """Return whether an implemented X-class satisfies a required X-class."""

    return _QUALITY_RANK.get(str(actual or "").upper(), -1) >= _QUALITY_RANK.get(
        str(minimum or "").upper(), len(_QUALITY_RANK)
    )


def _number(value: Any, *, field: str, required: bool = False) -> float | None:
    if value in (None, ""):
        if required:
            raise ValueError(f"execution_assumptions.{field} is required")
        return None
    if isinstance(value, bool):
        raise ValueError(f"execution_assumptions.{field} must be a finite non-negative number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"execution_assumptions.{field} must be a finite non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"execution_assumptions.{field} must be a finite non-negative number")
    return parsed


def _boolean(value: Any, *, field: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"execution_assumptions.{field} must be a boolean")
    return value


def _normalize_stress_scenarios(raw: Any) -> tuple[CostStressScenario, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("execution_assumptions.cost_stress_scenarios must be a list")
    scenarios: list[CostStressScenario] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValueError("execution_assumptions.cost_stress_scenarios entries must be objects")
        scenario_id = str(item.get("id") or item.get("scenario_id") or "").strip()
        if not scenario_id:
            raise ValueError(f"execution_assumptions.cost_stress_scenarios[{index}].id is required")
        if scenario_id in seen:
            raise ValueError(f"duplicate cost stress scenario id: {scenario_id}")
        seen.add(scenario_id)
        additional_slippage_bps = float(
            _number(
                item.get("additional_slippage_bps", 0.0),
                field=f"cost_stress_scenarios[{index}].additional_slippage_bps",
                required=True,
            )
            or 0.0
        )
        fee_multiplier = float(
            _number(
                item.get("fee_multiplier", 1.0),
                field=f"cost_stress_scenarios[{index}].fee_multiplier",
                required=True,
            )
            or 0.0
        )
        if fee_multiplier < 1.0:
            raise ValueError(
                f"execution_assumptions.cost_stress_scenarios[{index}].fee_multiplier must be at least 1.0"
            )
        scenarios.append(
            CostStressScenario(
                scenario_id=scenario_id,
                additional_slippage_bps=additional_slippage_bps,
                fee_multiplier=fee_multiplier,
            )
        )
    return tuple(scenarios)


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _quality_ceiling(
    *,
    model_version: str,
    market_slippage_bps: float | None,
    stop_slippage_bps: float | None,
    passive_fill_policy: str,
    fee_policy: str,
    full_fill_assumption: bool,
    explicit_zero_cost_override: bool,
) -> str:
    if explicit_zero_cost_override or fee_policy != "instrument_resolved":
        return ExecutionQualityClass.X0.value
    costed = bool(market_slippage_bps and market_slippage_bps > 0) and bool(
        stop_slippage_bps and stop_slippage_bps > 0
    )
    if not costed:
        return ExecutionQualityClass.X0.value
    if model_version == CONSERVATIVE_BAR_MODEL_VERSION and passive_fill_policy == "strict_penetration" and full_fill_assumption:
        return ExecutionQualityClass.X2.value
    if model_version == COSTED_BAR_MODEL_VERSION:
        return ExecutionQualityClass.X1.value
    return ExecutionQualityClass.X0.value


def resolve_execution_assumptions(
    economic_claim_intent: Any,
    raw: Mapping[str, Any] | None,
    *,
    source: str = "run_start_request",
) -> ResolvedExecutionAssumptions:
    """Validate and resolve a run-scoped execution-assumption manifest."""

    intent = normalize_economic_claim_intent(economic_claim_intent)
    payload = dict(raw or {})
    schema_version = str(payload.get("schema_version") or EXECUTION_ASSUMPTIONS_SCHEMA_VERSION).strip()
    if schema_version != EXECUTION_ASSUMPTIONS_SCHEMA_VERSION:
        raise ValueError(f"unsupported execution_assumptions.schema_version: {schema_version}")

    economic = intent != EconomicClaimIntent.EXPLORATION.value
    default_model = "" if economic else LEGACY_BAR_MODEL_VERSION
    model_version = str(payload.get("model_version") or default_model).strip()
    if not model_version:
        raise ValueError("execution_assumptions.model_version is required for economic claims")
    if model_version not in _SUPPORTED_MODELS:
        raise ValueError(f"unsupported execution_assumptions.model_version: {model_version}")

    default_passive = "strict_penetration" if model_version == CONSERVATIVE_BAR_MODEL_VERSION else "touch"
    passive_fill_policy = str(payload.get("passive_fill_policy") or default_passive).strip().lower()
    if passive_fill_policy not in _SUPPORTED_PASSIVE_POLICIES:
        raise ValueError(f"unsupported execution_assumptions.passive_fill_policy: {passive_fill_policy}")
    if model_version == CONSERVATIVE_BAR_MODEL_VERSION and passive_fill_policy != "strict_penetration":
        raise ValueError("conservative_bar.v1 requires passive_fill_policy=strict_penetration")

    fee_policy = str(payload.get("fee_policy") or ("" if economic else "explicit_zero")).strip().lower()
    if fee_policy not in _SUPPORTED_FEE_POLICIES:
        raise ValueError("execution_assumptions.fee_policy must be instrument_resolved or explicit_zero")

    market_bps = _number(payload.get("market_slippage_bps"), field="market_slippage_bps", required=economic)
    stop_bps = _number(payload.get("stop_slippage_bps"), field="stop_slippage_bps", required=economic)
    full_fill = _boolean(
        payload.get("full_fill_assumption"),
        field="full_fill_assumption",
        default=not economic,
    )
    explicit_zero = _boolean(
        payload.get("explicit_zero_cost_override"),
        field="explicit_zero_cost_override",
        default=False,
    ) or fee_policy == "explicit_zero"
    scenarios = _normalize_stress_scenarios(payload.get("cost_stress_scenarios"))

    if economic:
        if model_version == LEGACY_BAR_MODEL_VERSION:
            raise ValueError("legacy_bar_touch.v1 is exploratory-only")
        if fee_policy != "instrument_resolved":
            raise ValueError("economic claims require fee_policy=instrument_resolved")
        if explicit_zero:
            raise ValueError("explicit zero-cost overrides are exploratory-only without verified venue evidence")
        if not market_bps or not stop_bps:
            raise ValueError("economic claims require non-zero market_slippage_bps and stop_slippage_bps")
        if not full_fill:
            raise ValueError("Phase 1 economic claims must disclose full_fill_assumption=true")
        if not scenarios:
            raise ValueError("economic claims require at least one deterministic cost_stress_scenario")
        if not any(s.additional_slippage_bps > 0 or s.fee_multiplier > 1 for s in scenarios):
            raise ValueError("economic claims require at least one adverse cost stress scenario")

    ceiling = _quality_ceiling(
        model_version=model_version,
        market_slippage_bps=market_bps,
        stop_slippage_bps=stop_bps,
        passive_fill_policy=passive_fill_policy,
        fee_policy=fee_policy,
        full_fill_assumption=full_fill,
        explicit_zero_cost_override=explicit_zero,
    )
    material = {
        "schema_version": schema_version,
        "economic_claim_intent": intent,
        "model_version": model_version,
        "market_slippage_bps": market_bps,
        "stop_slippage_bps": stop_bps,
        "passive_fill_policy": passive_fill_policy,
        "fee_policy": fee_policy,
        "full_fill_assumption": full_fill,
        "explicit_zero_cost_override": explicit_zero,
        "cost_stress_scenarios": [asdict(item) for item in scenarios],
        "execution_quality_ceiling": ceiling,
        "source": str(source),
    }
    resolved_material = {**material, "cost_stress_scenarios": scenarios}
    return ResolvedExecutionAssumptions(
        **resolved_material,
        manifest_hash=_stable_hash(material),
    )


def legacy_execution_assumptions() -> ResolvedExecutionAssumptions:
    """Pinned compatibility contract for direct/internal legacy construction."""

    return resolve_execution_assumptions(EconomicClaimIntent.EXPLORATION.value, None, source="legacy_internal_default")


def apply_adverse_slippage(price: float, side: Any, bps: float | None) -> float:
    """Apply deterministic adverse BPS slippage for a buy or sell."""

    if not bps:
        return float(price)
    direction = 1.0 if str(side).strip().lower() in {"buy", "long"} else -1.0
    return float(price) * (1.0 + direction * (float(bps) / 10000.0))


__all__ = [
    "CONSERVATIVE_BAR_MODEL_VERSION",
    "COSTED_BAR_MODEL_VERSION",
    "EXECUTION_ASSUMPTIONS_SCHEMA_VERSION",
    "EconomicClaimIntent",
    "ExecutionQualityClass",
    "LEGACY_BAR_MODEL_VERSION",
    "ResolvedExecutionAssumptions",
    "apply_adverse_slippage",
    "execution_quality_meets",
    "legacy_execution_assumptions",
    "normalize_economic_claim_intent",
    "resolve_execution_assumptions",
]

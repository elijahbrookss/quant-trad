"""Pure contracts and evaluation logic for bounded offline research campaigns.

This module deliberately has no provider, network, credential, runtime-mutation,
deployment, or order-submission dependency.  It consumes caller-supplied frozen
facts and emits deterministic evidence for the existing scientific authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import pairwise
from statistics import fmean, median
from typing import Any

from .replay_availability import ResearchReplayAvailabilityPolicy
from strategies.typed_graph import (
    CompiledTypedStrategy,
    TypedStrategyGraph,
    compile_typed_strategy_graph,
)

LEGACY_CAMPAIGN_CHARTER_SCHEMA_VERSION = "autonomous_research_campaign.v1"
CAMPAIGN_CHARTER_SCHEMA_VERSION = "autonomous_research_campaign.v2"
CAMPAIGN_EVALUATOR_VERSION = "btc_perp_market_structure_evaluator.v1"
CAMPAIGN_FEATURE_VERSION = "btc_perp_market_structure_features.v1"
CAMPAIGN_METRIC_VERSION = "btc_perp_market_structure_metrics.v1"


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _utc(value: Any, field: str) -> datetime:
    raw = _required(value, field)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _positive_int(value: Any, field: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    parsed = int(value)
    if parsed < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    return parsed


def _finite(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be finite")
    parsed = float(value)
    if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
        raise ValueError(f"{field} must be finite and >= {minimum}")
    return parsed


@dataclass(frozen=True)
class CampaignDatasetBinding:
    role: str
    dataset_id: str
    dataset_hash: str
    window_start: str
    window_end: str
    blind_alias: str | None = None

    def __post_init__(self) -> None:
        role = str(self.role or "").strip().lower()
        if role not in {"train", "validation", "holdout"}:
            raise ValueError("campaign dataset role is invalid")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "dataset_id", _required(self.dataset_id, "dataset_id"))
        object.__setattr__(self, "dataset_hash", _required(self.dataset_hash, "dataset_hash"))
        start = _utc(self.window_start, "window_start")
        end = _utc(self.window_end, "window_end")
        if end <= start:
            raise ValueError("campaign dataset window is invalid")
        object.__setattr__(self, "window_start", start.isoformat().replace("+00:00", "Z"))
        object.__setattr__(self, "window_end", end.isoformat().replace("+00:00", "Z"))
        alias = str(self.blind_alias or "").strip() or None
        if role == "holdout" and alias is None:
            raise ValueError("campaign holdout requires a blind alias")
        object.__setattr__(self, "blind_alias", alias)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CampaignDatasetBinding:
        return cls(
            role=str(raw.get("role") or ""),
            dataset_id=str(raw.get("dataset_id") or ""),
            dataset_hash=str(raw.get("dataset_hash") or ""),
            window_start=str(raw.get("window_start") or ""),
            window_end=str(raw.get("window_end") or ""),
            blind_alias=raw.get("blind_alias"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignCharter:
    schema_version: str
    campaign_id: str
    objective: str
    economic_claim: str
    economic_claim_intent: str
    instrument_id: str
    instrument_symbol: str
    instrument_class: str
    instrument_economics_class: str
    primary_timeframe_seconds: int
    label_horizon_bars: int
    feature_lookback_bars: int
    datasets: tuple[CampaignDatasetBinding, ...]
    eligible_fact_types: tuple[str, ...]
    benchmark_ids: tuple[str, ...]
    graph_budget: int
    max_attempts: int
    max_validation_feedback_uses: int
    validation_survivor_limit: int
    walk_forward_train_bars: int
    walk_forward_validation_bars: int
    walk_forward_step_bars: int
    walk_forward_fold_count: int
    primary_metric: str
    primary_metric_direction: str
    minimum_effect_size: float
    secondary_metrics: tuple[str, ...]
    safety_metrics: tuple[str, ...]
    minimum_sample_count: int
    minimum_trade_count: int
    minimum_calendar_days: int
    minimum_exposure: float
    minimum_execution_quality_class: str
    execution_stress_ids: tuple[str, ...]
    robustness_requirements: tuple[str, ...]
    multiple_testing_method: str
    alpha: float
    market_slippage_bps: float
    stop_slippage_bps: float
    maker_fee_rate: float
    taker_fee_rate: float
    fee_schedule_version: str
    cost_stress_scenarios: tuple[Mapping[str, Any], ...]
    provider_fetch_allowed: bool
    external_trading_allowed: bool
    promotion_eligible: bool
    replay_availability_policy: ResearchReplayAvailabilityPolicy | None = None
    charter_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version not in {
            LEGACY_CAMPAIGN_CHARTER_SCHEMA_VERSION,
            CAMPAIGN_CHARTER_SCHEMA_VERSION,
        }:
            raise ValueError("unsupported campaign charter schema")
        replay_policy = self.replay_availability_policy
        if replay_policy is not None and not isinstance(
            replay_policy, ResearchReplayAvailabilityPolicy
        ):
            replay_policy = ResearchReplayAvailabilityPolicy.from_dict(replay_policy)
        if self.schema_version == LEGACY_CAMPAIGN_CHARTER_SCHEMA_VERSION:
            if replay_policy is not None:
                raise ValueError("legacy campaign charter cannot add replay policy")
        elif replay_policy is None:
            raise ValueError("campaign replay availability policy is required")
        object.__setattr__(self, "replay_availability_policy", replay_policy)
        for name in (
            "campaign_id",
            "objective",
            "economic_claim",
            "instrument_id",
            "instrument_symbol",
            "fee_schedule_version",
            "primary_metric",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        intent = str(self.economic_claim_intent or "").strip().lower()
        if intent != "selection":
            raise ValueError("first campaign must use immutable selection claim intent")
        object.__setattr__(self, "economic_claim_intent", intent)
        instrument_class = str(self.instrument_class or "").strip().lower()
        if instrument_class not in {"perpetual", "perpetual_style_future"}:
            raise ValueError("first campaign requires a perpetual-class instrument")
        object.__setattr__(self, "instrument_class", instrument_class)
        economics = str(self.instrument_economics_class or "").strip().lower()
        if economics != "incomplete":
            raise ValueError("first campaign must retain incomplete derivative economics")
        object.__setattr__(self, "instrument_economics_class", economics)
        for name in (
            "primary_timeframe_seconds",
            "label_horizon_bars",
            "feature_lookback_bars",
            "graph_budget",
            "max_attempts",
            "validation_survivor_limit",
            "walk_forward_train_bars",
            "walk_forward_validation_bars",
            "walk_forward_step_bars",
            "walk_forward_fold_count",
            "minimum_sample_count",
            "minimum_trade_count",
            "minimum_calendar_days",
        ):
            object.__setattr__(self, name, _positive_int(getattr(self, name), name))
        object.__setattr__(
            self,
            "max_validation_feedback_uses",
            _positive_int(
                self.max_validation_feedback_uses,
                "max_validation_feedback_uses",
                minimum=0,
            ),
        )
        if not 24 <= self.graph_budget <= 32:
            raise ValueError("first campaign graph budget must be between 24 and 32")
        if self.max_attempts > 48 or self.max_attempts < self.graph_budget:
            raise ValueError("first campaign attempt budget is invalid")
        if self.max_validation_feedback_uses > 4:
            raise ValueError("first campaign validation feedback budget exceeds four")
        if self.validation_survivor_limit > self.max_attempts - self.graph_budget:
            raise ValueError("validation survivor limit exceeds remaining attempt budget")
        datasets = tuple(self.datasets)
        roles = [row.role for row in datasets]
        if sorted(roles) != ["holdout", "train", "validation"]:
            raise ValueError("campaign requires exactly one train, validation, and holdout")
        ordered = sorted(datasets, key=lambda row: _utc(row.window_start, "window_start"))
        for left, right in pairwise(ordered):
            if _utc(left.window_end, "window_end") > _utc(right.window_start, "window_start"):
                raise ValueError("campaign dataset windows overlap")
        object.__setattr__(self, "datasets", datasets)
        if len({row.dataset_id for row in datasets}) != 3:
            raise ValueError("campaign dataset identities must be unique")
        facts = tuple(sorted({_required(value, "eligible_fact_type") for value in self.eligible_fact_types}))
        required_facts = {
            "market.trade_flow",
            "derivatives.open_interest",
            "derivatives.funding_rate",
        }
        if self.schema_version == CAMPAIGN_CHARTER_SCHEMA_VERSION:
            required_facts.add("market.trade")
        if not required_facts <= set(facts):
            raise ValueError("campaign eligible facts are incomplete")
        object.__setattr__(self, "eligible_fact_types", facts)
        benchmarks = tuple(sorted({_required(value, "benchmark_id") for value in self.benchmark_ids}))
        required_benchmarks = {
            "no_trade",
            "passive_exposure",
            "simple_momentum",
            "simple_mean_reversion",
            "price_only",
            "randomized_control",
            "timing_shifted_control",
        }
        if not required_benchmarks <= set(benchmarks):
            raise ValueError("campaign benchmark set is incomplete")
        object.__setattr__(self, "benchmark_ids", benchmarks)
        direction = str(self.primary_metric_direction or "").strip().lower()
        if direction != "maximize":
            raise ValueError("first campaign primary metric must be maximized")
        object.__setattr__(self, "primary_metric_direction", direction)
        object.__setattr__(self, "minimum_effect_size", _finite(self.minimum_effect_size, "minimum_effect_size", minimum=0.0))
        object.__setattr__(self, "minimum_exposure", _finite(self.minimum_exposure, "minimum_exposure", minimum=0.0))
        if self.minimum_exposure > 1.0:
            raise ValueError("minimum_exposure must be <= 1")
        if str(self.minimum_execution_quality_class or "").strip().upper() != "X2":
            raise ValueError("first campaign search must be fixed at X2")
        object.__setattr__(self, "minimum_execution_quality_class", "X2")
        stresses = tuple(sorted({_required(value, "execution_stress_id") for value in self.execution_stress_ids}))
        if not stresses:
            raise ValueError("campaign requires execution stresses")
        object.__setattr__(self, "execution_stress_ids", stresses)
        object.__setattr__(self, "secondary_metrics", tuple(sorted(set(self.secondary_metrics))))
        object.__setattr__(self, "safety_metrics", tuple(sorted(set(self.safety_metrics))))
        object.__setattr__(self, "robustness_requirements", tuple(sorted(set(self.robustness_requirements))))
        method = str(self.multiple_testing_method or "").strip().lower()
        if method not in {"holm", "bonferroni"}:
            raise ValueError("campaign multiple-testing method is invalid")
        object.__setattr__(self, "multiple_testing_method", method)
        alpha = _finite(self.alpha, "alpha", minimum=0.0)
        if not 0.0 < alpha < 1.0:
            raise ValueError("campaign alpha must be between zero and one")
        object.__setattr__(self, "alpha", alpha)
        for name in ("market_slippage_bps", "stop_slippage_bps", "maker_fee_rate", "taker_fee_rate"):
            object.__setattr__(self, name, _finite(getattr(self, name), name, minimum=0.0))
        if self.market_slippage_bps <= 0.0 or self.stop_slippage_bps <= 0.0:
            raise ValueError("campaign X2 slippage must be non-zero")
        scenarios = tuple(dict(row) for row in self.cost_stress_scenarios)
        scenario_ids = {str(row.get("id") or "").strip() for row in scenarios}
        if scenario_ids != set(stresses):
            raise ValueError("campaign stress scenarios must match pinned stress IDs")
        object.__setattr__(self, "cost_stress_scenarios", scenarios)
        if self.provider_fetch_allowed or self.external_trading_allowed or self.promotion_eligible:
            raise ValueError("offline campaign cannot fetch providers, trade externally, or promote")
        expected = stable_hash(self._material())
        if self.charter_hash and self.charter_hash != expected:
            raise ValueError("campaign charter hash mismatch")
        object.__setattr__(self, "charter_hash", expected)

    def _material(self) -> dict[str, Any]:
        material = {
            key: value
            for key, value in asdict(self).items()
            if key != "charter_hash"
        }
        # Historical v1 manifests retain their exact hash material. They remain
        # readable as terminal evidence but are rejected by the executable
        # campaign preflight because they lack a replay availability contract.
        if self.schema_version == LEGACY_CAMPAIGN_CHARTER_SCHEMA_VERSION:
            material.pop("replay_availability_policy", None)
        return material

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "charter_hash": self.charter_hash}

    def dataset(self, role: str) -> CampaignDatasetBinding:
        normalized = str(role or "").strip().lower()
        return next(row for row in self.datasets if row.role == normalized)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CampaignCharter:
        values = dict(raw)
        values["datasets"] = tuple(
            CampaignDatasetBinding.from_dict(row) for row in raw.get("datasets") or ()
        )
        for name in (
            "eligible_fact_types",
            "benchmark_ids",
            "secondary_metrics",
            "safety_metrics",
            "execution_stress_ids",
            "robustness_requirements",
        ):
            values[name] = tuple(raw.get(name) or ())
        values["cost_stress_scenarios"] = tuple(raw.get("cost_stress_scenarios") or ())
        replay_policy = raw.get("replay_availability_policy")
        values["replay_availability_policy"] = (
            ResearchReplayAvailabilityPolicy.from_dict(replay_policy)
            if replay_policy is not None
            else None
        )
        return cls(**values)


def resolve_campaign_charter(
    public_charter: Mapping[str, Any],
    *,
    sealed_holdout_binding: Mapping[str, Any],
) -> CampaignCharter:
    """Resolve a private charter without placing holdout identity in the repo."""

    raw = deepcopy(dict(public_charter))
    datasets = list(raw.get("datasets") or ())
    holdouts = [
        row
        for row in datasets
        if str(row.get("role") or "").strip().lower() == "holdout"
    ]
    if len(holdouts) != 1:
        raise ValueError("public campaign charter requires one sealed holdout declaration")
    public_holdout = dict(holdouts[0])
    forbidden = {
        "dataset_id",
        "dataset_hash",
        "window_start",
        "window_end",
    } & set(public_holdout)
    if forbidden or public_holdout.get("sealed") is not True:
        raise ValueError("public campaign charter must not disclose holdout binding")
    private = dict(sealed_holdout_binding)
    required = {"dataset_id", "dataset_hash", "window_start", "window_end"}
    if not required <= set(private):
        raise ValueError("sealed holdout binding is incomplete")
    merged = {
        "role": "holdout",
        "blind_alias": public_holdout.get("blind_alias"),
        **{key: private[key] for key in sorted(required)},
    }
    raw["datasets"] = [
        merged
        if str(row.get("role") or "").strip().lower() == "holdout"
        else row
        for row in datasets
    ]
    return CampaignCharter.from_dict(raw)


@dataclass(frozen=True)
class FrozenCampaignBar:
    bucket_start: datetime
    bucket_end: datetime
    known_at: datetime
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    trade_count: int
    base_volume: float
    quote_notional: float
    cvd_delta: float
    open_interest: float | None
    funding_rate: float | None
    source_hashes: tuple[str, ...]
    canonical_known_at: datetime | None = None
    replay_policy_hash: str | None = None
    replay_bucket_hash: str | None = None

    def __post_init__(self) -> None:
        if self.known_at < self.bucket_end:
            raise ValueError("campaign bar known_at precedes bucket end")
        canonical_known_at = self.canonical_known_at or self.known_at
        if canonical_known_at < self.bucket_end:
            raise ValueError("campaign bar canonical known_at precedes bucket end")
        object.__setattr__(self, "canonical_known_at", canonical_known_at)
        if self.trade_count < 0:
            raise ValueError("campaign bar trade_count is negative")
        populated = self.trade_count > 0
        if populated != all(
            value is not None
            for value in (self.open_price, self.high_price, self.low_price, self.close_price)
        ):
            raise ValueError("campaign bar price population conflicts with trade count")


@dataclass(frozen=True)
class CampaignFeatureRow:
    bar: FrozenCampaignBar
    facts: Mapping[str, Any]
    reference_price: float | None
    feature_hash: str


def _rolling_z(values: Sequence[float], index: int, lookback: int) -> float:
    start = max(0, index - lookback + 1)
    window = [float(value) for value in values[start : index + 1]]
    if len(window) < 2:
        return 0.0
    mean = fmean(window)
    variance = fmean([(value - mean) ** 2 for value in window])
    return 0.0 if variance <= 0.0 else (window[-1] - mean) / math.sqrt(variance)


def build_campaign_features(
    bars: Sequence[FrozenCampaignBar],
    *,
    lookback_bars: int,
) -> tuple[CampaignFeatureRow, ...]:
    """Build causal features using only each row's known prefix."""

    lookback = _positive_int(lookback_bars, "lookback_bars")
    ordered = tuple(sorted(bars, key=lambda row: (row.bucket_start, row.known_at)))
    if len({row.bucket_start for row in ordered}) != len(ordered):
        raise ValueError("campaign bars contain duplicate bucket starts")
    rows: list[CampaignFeatureRow] = []
    for index, bar in enumerate(ordered):
        visible = tuple(
            row
            for row in ordered[: index + 1]
            if row.known_at <= bar.known_at
        )
        priced = tuple(
            float(row.close_price)
            for row in visible
            if row.close_price is not None
        )
        price = priced[-1] if priced else None
        prior = priced[-2] if len(priced) >= 2 else None
        prior_three = priced[-4] if len(priced) >= 4 else None
        return_one = (
            0.0
            if price is None or prior in {None, 0.0}
            else (price / prior - 1.0) * 10_000.0
        )
        return_three = (
            0.0
            if price is None or prior_three in {None, 0.0}
            else (price / prior_three - 1.0) * 10_000.0
        )
        visible_prices = priced[-(lookback + 1) :]
        recent_returns = [
            (right / left - 1.0) * 10_000.0
            for left, right in pairwise(visible_prices)
            if left != 0.0
        ]
        volatility = 0.0
        if len(recent_returns) >= 2:
            mean_return = fmean(recent_returns)
            volatility = math.sqrt(fmean([(value - mean_return) ** 2 for value in recent_returns]))
        range_bps = (
            0.0
            if bar.high_price is None or bar.low_price is None or price in {None, 0.0}
            else (float(bar.high_price) - float(bar.low_price)) / float(price) * 10_000.0
        )
        oi_change = 0.0
        visible_oi = tuple(
            float(row.open_interest)
            for row in visible
            if row.open_interest is not None
        )
        if len(visible_oi) >= 6 and visible_oi[-6] != 0.0:
            oi_change = (
                visible_oi[-1] / visible_oi[-6] - 1.0
            ) * 10_000.0
        recent_visible = visible[-lookback:]
        notionals = [float(row.quote_notional) for row in recent_visible]
        trade_counts = [float(row.trade_count) for row in recent_visible]
        cvd_shares = [
            0.0
            if row.base_volume <= 0.0
            else float(row.cvd_delta) / max(float(row.base_volume), 1e-12)
            for row in recent_visible
        ]
        facts = {
            "market.has_trade": bool(bar.trade_count > 0),
            "market.return_1_bps": float(return_one),
            "market.return_3_bps": float(return_three),
            "market.range_bps": float(range_bps),
            "market.cvd_share": float(cvd_shares[-1]),
            "market.volume_zscore": float(_rolling_z(notionals, len(notionals) - 1, lookback)),
            "market.trade_count_zscore": float(_rolling_z(trade_counts, len(trade_counts) - 1, lookback)),
            "market.volatility_bps": float(volatility),
            "market.oi_change_5_bps": float(oi_change),
            "market.funding_rate_bps": float((bar.funding_rate or 0.0) * 10_000.0),
        }
        rows.append(
            CampaignFeatureRow(
                bar=bar,
                facts=facts,
                reference_price=price,
                feature_hash=stable_hash(
                    {
                        "version": CAMPAIGN_FEATURE_VERSION,
                        "bucket_start": bar.bucket_start.isoformat(),
                        "known_at": bar.known_at.isoformat(),
                        "facts": facts,
                        "source_hashes": list(bar.source_hashes),
                        "visible_prefix_hashes": [
                            source_hash
                            for row in visible
                            for source_hash in row.source_hashes
                        ],
                    }
                ),
            )
        )
    return tuple(rows)


_FACT_DECLARATIONS = (
    {"name": "market.has_trade", "value_type": "boolean"},
    {"name": "market.return_1_bps", "value_type": "number"},
    {"name": "market.return_3_bps", "value_type": "number"},
    {"name": "market.range_bps", "value_type": "number"},
    {"name": "market.cvd_share", "value_type": "number"},
    {"name": "market.volume_zscore", "value_type": "number"},
    {"name": "market.trade_count_zscore", "value_type": "number"},
    {"name": "market.volatility_bps", "value_type": "number"},
    {"name": "market.oi_change_5_bps", "value_type": "number"},
    {"name": "market.funding_rate_bps", "value_type": "number"},
)


def _fact(name: str) -> dict[str, Any]:
    return {"op": "fact", "name": name}


def _number(value: float) -> dict[str, Any]:
    return {"op": "const", "value": float(value), "value_type": "number"}


def _all(*conditions: Mapping[str, Any]) -> dict[str, Any]:
    return {"op": "all", "args": [dict(row) for row in conditions]}


def _comparison(name: str, op: str, value: float) -> dict[str, Any]:
    return {"op": op, "args": [_fact(name), _number(value)]}


def campaign_graph_specs() -> tuple[dict[str, Any], ...]:
    specs: list[dict[str, Any]] = []
    for flow in (0.10, 0.25, 0.40):
        for price in (0.0, 2.0):
            specs.append({"family": "flow_continuation", "flow": flow, "price": price})
    for flow in (0.10, 0.25, 0.40):
        for price in (0.0, 2.0):
            specs.append({"family": "flow_reversal", "flow": flow, "price": price})
    for flow in (0.10, 0.25):
        for activity in (0.5, 1.0):
            specs.append({"family": "volume_flow", "flow": flow, "activity": activity})
    for flow in (0.10, 0.25):
        for activity in (0.5, 1.0):
            specs.append({"family": "trade_activity_flow", "flow": flow, "activity": activity})
    specs.extend(
        [
            {"family": "oi_flow_confirmation", "flow": 0.10, "oi": 0.0},
            {"family": "oi_flow_confirmation", "flow": 0.25, "oi": 0.0},
            {"family": "volatility_flow", "flow": 0.10, "volatility": 2.0},
            {"family": "volatility_flow", "flow": 0.25, "volatility": 4.0},
        ]
    )
    return tuple({**row, "ordinal": index + 1} for index, row in enumerate(specs))


def _spec_conditions(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    family = str(spec["family"])
    flow = float(spec["flow"])
    has_trade = _fact("market.has_trade")
    if family == "flow_continuation":
        price = float(spec["price"])
        return (
            _all(has_trade, _comparison("market.cvd_share", "gt", flow), _comparison("market.return_3_bps", "gt", price)),
            _all(has_trade, _comparison("market.cvd_share", "lt", -flow), _comparison("market.return_3_bps", "lt", -price)),
        )
    if family == "flow_reversal":
        price = float(spec["price"])
        return (
            _all(has_trade, _comparison("market.cvd_share", "gt", flow), _comparison("market.return_3_bps", "lt", -price)),
            _all(has_trade, _comparison("market.cvd_share", "lt", -flow), _comparison("market.return_3_bps", "gt", price)),
        )
    if family == "volume_flow":
        activity = float(spec["activity"])
        return (
            _all(has_trade, _comparison("market.volume_zscore", "gt", activity), _comparison("market.cvd_share", "gt", flow)),
            _all(has_trade, _comparison("market.volume_zscore", "gt", activity), _comparison("market.cvd_share", "lt", -flow)),
        )
    if family == "trade_activity_flow":
        activity = float(spec["activity"])
        return (
            _all(has_trade, _comparison("market.trade_count_zscore", "gt", activity), _comparison("market.cvd_share", "gt", flow)),
            _all(has_trade, _comparison("market.trade_count_zscore", "gt", activity), _comparison("market.cvd_share", "lt", -flow)),
        )
    if family == "oi_flow_confirmation":
        return (
            _all(has_trade, _comparison("market.oi_change_5_bps", "gt", 0.0), _comparison("market.cvd_share", "gt", flow)),
            _all(has_trade, _comparison("market.oi_change_5_bps", "gt", 0.0), _comparison("market.cvd_share", "lt", -flow)),
        )
    if family == "volatility_flow":
        volatility = float(spec["volatility"])
        return (
            _all(has_trade, _comparison("market.volatility_bps", "gt", volatility), _comparison("market.cvd_share", "gt", flow)),
            _all(has_trade, _comparison("market.volatility_bps", "gt", volatility), _comparison("market.cvd_share", "lt", -flow)),
        )
    raise ValueError(f"unsupported campaign graph family: {family}")


def build_campaign_graph_manifest(
    *,
    campaign_id: str,
    family_id: str,
    protocol_hash: str,
    spec: Mapping[str, Any],
    parent_graph_ids: Sequence[str] = (),
) -> dict[str, Any]:
    ordinal = int(spec["ordinal"])
    long_condition, short_condition = _spec_conditions(spec)
    execution = {
        "style": "market",
        "time_in_force": "bar",
        "expiration_bars": 1,
        "price_offset_bps": 0.0,
        "chase_limit": 0,
        "stage_count": 1,
    }
    sizing = {"mode": "fixed_quantity", "value": 1.0}
    return {
        "schema_version": "typed_strategy_graph.v1",
        "graph_id": f"{campaign_id}:graph:{ordinal:02d}",
        "family_id": family_id,
        "protocol_hash": protocol_hash,
        "timeframe": "1m",
        "facts": list(_FACT_DECLARATIONS),
        "rules": [
            {
                "rule_id": "enter_long",
                "priority": 100,
                "condition": long_condition,
                "action": "enter",
                "side": "long",
                "sizing": sizing,
                "execution": execution,
            },
            {
                "rule_id": "enter_short",
                "priority": 100,
                "condition": short_condition,
                "action": "enter",
                "side": "short",
                "sizing": sizing,
                "execution": execution,
            },
        ],
        "risk": {
            "max_position_notional": 100_000.0,
            "max_risk_fraction": 0.01,
            "allow_short": True,
        },
        "parent_graph_ids": list(parent_graph_ids),
        "created_by": "research_agent",
    }


@dataclass(frozen=True)
class CampaignExecutionCosts:
    market_slippage_bps: float
    taker_fee_rate: float
    execution_quality_class: str
    execution_model_hash: str
    fee_schedule_hash: str
    stress_scenarios: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        if self.execution_quality_class != "X2":
            raise ValueError("campaign execution costs must be X2")
        if self.market_slippage_bps <= 0.0 or self.taker_fee_rate < 0.0:
            raise ValueError("campaign execution costs are invalid")


@dataclass(frozen=True)
class CampaignEvaluation:
    graph_id: str
    graph_hash: str
    artifact_hash: str
    parameter_artifact_hash: str
    execution_model_hash: str
    metric_contract_hash: str
    sample_count: int
    trade_count: int
    calendar_days: int
    exposure: float
    metric_results: Mapping[str, float]
    benchmark_metric_results: Mapping[str, Mapping[str, float]]
    p_value: float | None
    confidence_interval_low: float | None
    execution_stress_ids_passed: tuple[str, ...]
    robustness_passed: tuple[str, ...]
    cost_stress_passed: bool
    returns_bps: tuple[float, ...]
    decision_trace_hashes: tuple[str, ...]

    def to_attempt_evidence(
        self,
        *,
        charter: CampaignCharter,
        validation: bool,
    ) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "schema_version": "autonomous_campaign_attempt_evidence.v1",
            "campaign_id": charter.campaign_id,
            "campaign_charter_hash": charter.charter_hash,
            "evaluator_version": CAMPAIGN_EVALUATOR_VERSION,
            "feature_version": CAMPAIGN_FEATURE_VERSION,
            "metric_version": CAMPAIGN_METRIC_VERSION,
            "artifact_hash": self.artifact_hash,
            "strategy_artifact_hash": self.graph_hash,
            "parameter_artifact_hash": self.parameter_artifact_hash,
            "execution_model_hash": self.execution_model_hash,
            "metric_contract_hash": self.metric_contract_hash,
            "reproducible": True,
            "sample_count": self.sample_count,
            "trade_count": self.trade_count,
            "calendar_days": self.calendar_days,
            "exposure": self.exposure,
            "metric_results": dict(self.metric_results),
            "benchmark_metric_results": {
                key: dict(value) for key, value in self.benchmark_metric_results.items()
            },
            "p_value": self.p_value,
            "confidence_interval_low": self.confidence_interval_low,
            "execution_quality_class": "X2",
            "execution_stress_ids_passed": list(self.execution_stress_ids_passed),
            "robustness_passed": list(self.robustness_passed),
            "cost_stress_passed": self.cost_stress_passed,
            "latency_stress_passed": False,
            "decision_trace_bundle_hash": stable_hash(list(self.decision_trace_hashes)),
            "instrument_economics_class": "incomplete",
            "promotion_eligible": False,
            "external_trading_authority": False,
        }
        if validation:
            contamination = max(charter.feature_lookback_bars, charter.label_horizon_bars, 1)
            evidence.update(
                walk_forward_fold_count=charter.walk_forward_fold_count,
                purge_bars=contamination,
                embargo_bars=contamination,
                context_only_warmup=True,
                flat_at_scoring_start=True,
                no_pending_orders_at_scoring_start=True,
                signals_not_before_scoring_start=True,
            )
        return evidence


def validation_scoring_indexes(charter: CampaignCharter, row_count: int) -> tuple[int, ...]:
    contamination = max(charter.feature_lookback_bars, charter.label_horizon_bars, 1)
    indexes: list[int] = []
    for fold in range(charter.walk_forward_fold_count):
        train_start = fold * charter.walk_forward_step_bars
        train_end = train_start + charter.walk_forward_train_bars
        validation_start = train_end + contamination
        validation_end = validation_start + charter.walk_forward_validation_bars
        embargo_end = validation_end + contamination
        if embargo_end > row_count:
            raise ValueError("campaign validation rows do not cover declared walk-forward plan")
        indexes.extend(range(validation_start, validation_end))
    return tuple(indexes)


def full_scoring_indexes(charter: CampaignCharter, row_count: int) -> tuple[int, ...]:
    start = charter.feature_lookback_bars
    end = row_count - charter.label_horizon_bars - 1
    return tuple(range(start, max(start, end + 1)))


def _price_at(rows: Sequence[CampaignFeatureRow], index: int, *, entry: bool) -> float | None:
    row = rows[index]
    direct = row.bar.open_price if entry else row.bar.close_price
    return float(direct) if direct is not None else row.reference_price


def _causal_entry_index(
    rows: Sequence[CampaignFeatureRow],
    signal_index: int,
) -> int | None:
    known_at = rows[signal_index].bar.known_at
    for index in range(signal_index + 1, len(rows)):
        if rows[index].bar.bucket_start >= known_at and _price_at(rows, index, entry=True) not in {None, 0.0}:
            return index
    return None


def causal_opportunity_indexes(
    rows: Sequence[CampaignFeatureRow],
    indexes: Sequence[int],
    *,
    horizon: int,
) -> tuple[int, ...]:
    eligible: list[int] = []
    for index in indexes:
        entry_index = _causal_entry_index(rows, index)
        if entry_index is None:
            continue
        exit_index = entry_index + horizon
        if exit_index >= len(rows):
            continue
        if _price_at(rows, exit_index, entry=False) in {None, 0.0}:
            continue
        eligible.append(index)
    return tuple(eligible)


# Kept private as an implementation alias for pinned evaluator v1 call sites.
_causal_opportunity_indexes = causal_opportunity_indexes


def _net_return_bps(
    *,
    side: int,
    entry_price: float,
    exit_price: float,
    slippage_bps: float,
    taker_fee_rate: float,
) -> float:
    slipped_entry = entry_price * (1.0 + side * slippage_bps / 10_000.0)
    slipped_exit = exit_price * (1.0 - side * slippage_bps / 10_000.0)
    gross = side * (slipped_exit / slipped_entry - 1.0) * 10_000.0
    return gross - (2.0 * taker_fee_rate * 10_000.0)


def _signals_for_graph(
    rows: Sequence[CampaignFeatureRow],
    compiled: CompiledTypedStrategy,
    indexes: Sequence[int],
) -> tuple[dict[int, int], tuple[str, ...]]:
    signals: dict[int, int] = {}
    traces: list[str] = []
    for index in indexes:
        intent, trace = compiled.evaluate(rows[index].facts)
        traces.append(stable_hash(trace))
        if intent is not None and intent.action == "enter":
            signals[index] = 1 if intent.side == "long" else -1
    return signals, tuple(traces)


def _price_only_signals(rows: Sequence[CampaignFeatureRow], indexes: Sequence[int]) -> dict[int, int]:
    result: dict[int, int] = {}
    for index in indexes:
        value = float(rows[index].facts["market.return_3_bps"])
        if value > 0.0:
            result[index] = 1
        elif value < 0.0:
            result[index] = -1
    return result


def _returns_for_signals(
    rows: Sequence[CampaignFeatureRow],
    signals: Mapping[int, int],
    *,
    horizon: int,
    slippage_bps: float,
    taker_fee_rate: float,
) -> tuple[float, ...]:
    values: list[float] = []
    for index, side in sorted(signals.items()):
        entry_index = _causal_entry_index(rows, index)
        if entry_index is None:
            continue
        exit_index = entry_index + horizon
        entry = _price_at(rows, entry_index, entry=True)
        exit_price = _price_at(rows, exit_index, entry=False)
        if entry in {None, 0.0} or exit_price in {None, 0.0}:
            continue
        values.append(
            _net_return_bps(
                side=int(side),
                entry_price=float(entry),
                exit_price=float(exit_price),
                slippage_bps=slippage_bps,
                taker_fee_rate=taker_fee_rate,
            )
        )
    return tuple(values)


def _metrics(returns: Sequence[float], *, sample_count: int) -> dict[str, float]:
    values = tuple(float(value) for value in returns)
    if not values:
        return {
            "mean_net_return_bps": -1_000_000.0,
            "directional_accuracy": 0.0,
            "median_net_return_bps": -1_000_000.0,
            "signal_rate": 0.0,
            "max_drawdown_bps": 1_000_000.0,
            "worst_trade_bps": 1_000_000.0,
            "cost_stress_min_return_bps": -1_000_000.0,
        }
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return {
        "mean_net_return_bps": float(fmean(values)),
        "directional_accuracy": float(sum(value > 0.0 for value in values) / len(values)),
        "median_net_return_bps": float(median(values)),
        "signal_rate": float(len(values) / max(sample_count, 1)),
        "max_drawdown_bps": float(drawdown),
        "worst_trade_bps": float(max(0.0, -min(values))),
        "cost_stress_min_return_bps": float(fmean(values)),
    }


def _one_sided_p_value(values: Sequence[float]) -> float | None:
    rows = tuple(float(value) for value in values)
    if len(rows) < 2:
        return None
    mean = fmean(rows)
    variance = sum((value - mean) ** 2 for value in rows) / (len(rows) - 1)
    if variance <= 0.0:
        return 0.0 if mean > 0.0 else 1.0
    z_score = mean / (math.sqrt(variance) / math.sqrt(len(rows)))
    return float(0.5 * math.erfc(z_score / math.sqrt(2.0)))


def _bootstrap_lower(values: Sequence[float], *, seed: str) -> float | None:
    rows = tuple(float(value) for value in values)
    if len(rows) < 2:
        return None
    rng = random.Random(int(stable_hash(seed), 16))
    block = min(5, len(rows))
    starts = len(rows) - block + 1
    means: list[float] = []
    for _ in range(512):
        sample: list[float] = []
        while len(sample) < len(rows):
            start = rng.randrange(starts)
            sample.extend(rows[start : start + block])
        means.append(fmean(sample[: len(rows)]))
    means.sort()
    return float(means[max(0, int(0.025 * len(means)))])


def evaluate_campaign_graph(
    *,
    charter: CampaignCharter,
    graph: TypedStrategyGraph,
    rows: Sequence[CampaignFeatureRow],
    execution: CampaignExecutionCosts,
    scoring_indexes: Sequence[int],
) -> CampaignEvaluation:
    compiled = compile_typed_strategy_graph(graph)
    requested_indexes = tuple(sorted({int(value) for value in scoring_indexes}))
    indexes = _causal_opportunity_indexes(
        rows,
        requested_indexes,
        horizon=charter.label_horizon_bars,
    )
    signals, trace_hashes = _signals_for_graph(rows, compiled, indexes)
    returns = _returns_for_signals(
        rows,
        signals,
        horizon=charter.label_horizon_bars,
        slippage_bps=execution.market_slippage_bps,
        taker_fee_rate=execution.taker_fee_rate,
    )
    metric_results = _metrics(returns, sample_count=len(indexes))
    benchmark_signals: dict[str, dict[int, int]] = {
        "no_trade": {},
        "passive_exposure": {index: 1 for index in indexes},
        "simple_momentum": _price_only_signals(rows, indexes),
        "simple_mean_reversion": {
            index: -side for index, side in _price_only_signals(rows, indexes).items()
        },
        "price_only": _price_only_signals(rows, indexes),
    }
    shifted = {index + 1: side for index, side in signals.items() if index + 1 in indexes}
    benchmark_signals["timing_shifted_control"] = shifted
    signal_sides = list(signals.values())
    randomized_indexes = list(indexes)
    rng = random.Random(int(stable_hash(f"{charter.charter_hash}:{graph.graph_hash}:random-control"), 16))
    rng.shuffle(randomized_indexes)
    benchmark_signals["randomized_control"] = {
        index: signal_sides[position]
        for position, index in enumerate(randomized_indexes[: len(signal_sides)])
    }
    benchmark_metrics: dict[str, dict[str, float]] = {}
    for benchmark_id in charter.benchmark_ids:
        benchmark_returns = _returns_for_signals(
            rows,
            benchmark_signals[benchmark_id],
            horizon=charter.label_horizon_bars,
            slippage_bps=execution.market_slippage_bps,
            taker_fee_rate=execution.taker_fee_rate,
        )
        benchmark_metrics[benchmark_id] = _metrics(
            benchmark_returns,
            sample_count=len(indexes),
        )
        if benchmark_id == "no_trade":
            benchmark_metrics[benchmark_id][charter.primary_metric] = 0.0
    stress_means: dict[str, float] = {}
    for scenario in execution.stress_scenarios:
        scenario_id = str(scenario["id"])
        stressed = _returns_for_signals(
            rows,
            signals,
            horizon=charter.label_horizon_bars,
            slippage_bps=(
                execution.market_slippage_bps
                + float(scenario.get("additional_slippage_bps") or 0.0)
            ),
            taker_fee_rate=(
                execution.taker_fee_rate
                * float(scenario.get("fee_multiplier") or 1.0)
            ),
        )
        stress_means[scenario_id] = (
            float(fmean(stressed)) if stressed else -1_000_000.0
        )
    stress_passed = tuple(
        sorted(
            scenario_id
            for scenario_id, value in stress_means.items()
            if value >= 0.0
        )
    )
    metric_results["cost_stress_min_return_bps"] = min(
        stress_means.values(), default=-1_000_000.0
    )
    subperiods: list[float] = []
    if returns:
        midpoint = max(1, len(returns) // 2)
        subperiods = [fmean(returns[:midpoint]), fmean(returns[midpoint:] or returns[:midpoint])]
    robustness: list[str] = []
    if subperiods and all(value > 0.0 for value in subperiods):
        robustness.append("subperiod_sign_consistency")
    if set(stress_passed) == set(charter.execution_stress_ids):
        robustness.append("cost_stress_survival")
    shifted_value = benchmark_metrics["timing_shifted_control"][charter.primary_metric]
    if metric_results[charter.primary_metric] > shifted_value:
        robustness.append("timing_shift_control")
    parameter_hash = stable_hash(
        {
            "feature_version": CAMPAIGN_FEATURE_VERSION,
            "feature_lookback_bars": charter.feature_lookback_bars,
            "label_horizon_bars": charter.label_horizon_bars,
            "scoring_indexes": list(indexes),
            "graph_hash": graph.graph_hash,
        }
    )
    metric_hash = stable_hash(
        {
            "metric_version": CAMPAIGN_METRIC_VERSION,
            "primary_metric": charter.primary_metric,
            "secondary_metrics": list(charter.secondary_metrics),
            "safety_metrics": list(charter.safety_metrics),
            "benchmark_ids": list(charter.benchmark_ids),
        }
    )
    material = {
        "campaign_charter_hash": charter.charter_hash,
        "evaluator_version": CAMPAIGN_EVALUATOR_VERSION,
        "graph_hash": graph.graph_hash,
        "parameter_artifact_hash": parameter_hash,
        "execution_model_hash": execution.execution_model_hash,
        "fee_schedule_hash": execution.fee_schedule_hash,
        "metric_contract_hash": metric_hash,
        "feature_hashes": [rows[index].feature_hash for index in indexes],
        "returns_bps": list(returns),
        "metric_results": metric_results,
        "benchmark_metric_results": benchmark_metrics,
        "stress_means": stress_means,
        "decision_trace_hashes": list(trace_hashes),
    }
    calendar_days = len({rows[index].bar.bucket_start.date() for index in indexes})
    exposure = min(
        1.0,
        len(returns) * charter.label_horizon_bars / max(len(indexes), 1),
    )
    return CampaignEvaluation(
        graph_id=graph.graph_id,
        graph_hash=graph.graph_hash,
        artifact_hash=stable_hash(material),
        parameter_artifact_hash=parameter_hash,
        execution_model_hash=execution.execution_model_hash,
        metric_contract_hash=metric_hash,
        sample_count=len(indexes),
        trade_count=len(returns),
        calendar_days=calendar_days,
        exposure=exposure,
        metric_results=metric_results,
        benchmark_metric_results=benchmark_metrics,
        p_value=_one_sided_p_value(returns),
        confidence_interval_low=_bootstrap_lower(
            returns,
            seed=f"{charter.charter_hash}:{graph.graph_hash}",
        ),
        execution_stress_ids_passed=stress_passed,
        robustness_passed=tuple(sorted(robustness)),
        cost_stress_passed=set(stress_passed) == set(charter.execution_stress_ids),
        returns_bps=returns,
        decision_trace_hashes=trace_hashes,
    )


def rank_evaluations(
    evaluations: Iterable[CampaignEvaluation],
    *,
    primary_metric: str,
) -> tuple[CampaignEvaluation, ...]:
    return tuple(
        sorted(
            evaluations,
            key=lambda row: (
                -float(row.metric_results[primary_metric]),
                row.graph_hash,
            ),
        )
    )


__all__ = [
    "CAMPAIGN_CHARTER_SCHEMA_VERSION",
    "CAMPAIGN_EVALUATOR_VERSION",
    "CAMPAIGN_FEATURE_VERSION",
    "CAMPAIGN_METRIC_VERSION",
    "LEGACY_CAMPAIGN_CHARTER_SCHEMA_VERSION",
    "CampaignCharter",
    "CampaignDatasetBinding",
    "CampaignEvaluation",
    "CampaignExecutionCosts",
    "CampaignFeatureRow",
    "FrozenCampaignBar",
    "build_campaign_features",
    "build_campaign_graph_manifest",
    "campaign_graph_specs",
    "causal_opportunity_indexes",
    "evaluate_campaign_graph",
    "full_scoring_indexes",
    "rank_evaluations",
    "resolve_campaign_charter",
    "stable_hash",
    "validation_scoring_indexes",
]

"""Deterministic research-time availability derived from frozen raw evidence.

Canonical market facts keep their actual ``known_at`` timestamps.  This module
defines a separate counterfactual replay contract for research: when could the
same aggregate have been derived by a continuously running, pinned transform
from already-received source events?  The answer is evidence, not a timestamp
rewrite, and is valid only for the exact frozen source/coverage binding.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from market_data.structure import (
    MarketTradeFact,
    MarketTradeRecord,
    TradeCoverageIntervalVersion,
    TradeFlowAggregateRecord,
    aggregate_trade_bucket,
)

TRADE_FLOW_REPLAY_SCHEMA_VERSION = "research.trade_flow_replay.v1"
TRADE_FLOW_REPLAY_TRANSFORM_VERSION = "market.trade_flow.receipt_replay.v1"
TRADE_FLOW_REPLAY_WATERMARK_POLICY = "first_subsequent_covered_trade.v1"


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class TradeFlowReplayPolicy:
    """Immutable rules for deriving one research replay timeline."""

    schema_version: str = TRADE_FLOW_REPLAY_SCHEMA_VERSION
    source_fact_type: str = "market.trade"
    aggregate_fact_type: str = "market.trade_flow"
    interval_seconds: int = 60
    availability_basis: str = "frozen_source_received_at"
    transform_version: str = TRADE_FLOW_REPLAY_TRANSFORM_VERSION
    watermark_policy: str = TRADE_FLOW_REPLAY_WATERMARK_POLICY
    processing_latency_ms: int = 50
    require_complete_coverage: bool = True
    require_archive_complete: bool = True
    require_canonicalization_complete: bool = True
    policy_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != TRADE_FLOW_REPLAY_SCHEMA_VERSION:
            raise ValueError("unsupported research replay availability schema")
        expected_strings = {
            "source_fact_type": "market.trade",
            "aggregate_fact_type": "market.trade_flow",
            "availability_basis": "frozen_source_received_at",
            "transform_version": TRADE_FLOW_REPLAY_TRANSFORM_VERSION,
            "watermark_policy": TRADE_FLOW_REPLAY_WATERMARK_POLICY,
        }
        for name, expected in expected_strings.items():
            if str(getattr(self, name) or "").strip() != expected:
                raise ValueError(f"research replay policy {name} is unsupported")
        interval = int(self.interval_seconds)
        if interval != 60:
            raise ValueError("research replay policy requires 60-second aggregates")
        latency = int(self.processing_latency_ms)
        if latency < 0 or latency > 60_000:
            raise ValueError("research replay processing latency must be 0..60000 ms")
        for name in (
            "require_complete_coverage",
            "require_archive_complete",
            "require_canonicalization_complete",
        ):
            if getattr(self, name) is not True:
                raise ValueError(f"research replay policy {name} must be true")
        object.__setattr__(self, "interval_seconds", interval)
        object.__setattr__(self, "processing_latency_ms", latency)
        expected_hash = _stable_hash(self._material())
        if self.policy_hash and self.policy_hash != expected_hash:
            raise ValueError("research replay policy hash mismatch")
        object.__setattr__(self, "policy_hash", expected_hash)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "policy_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "policy_hash": self.policy_hash}

    @classmethod
    def from_dict(
        cls, raw: Mapping[str, Any]
    ) -> TradeFlowReplayPolicy:
        return cls(**dict(raw))


@dataclass(frozen=True)
class AvailableTradeFlowBucket:
    """One aggregate with canonical and research-availability clocks separated."""

    aggregate: TradeFlowAggregateRecord
    replay_available_at: datetime
    source_receipt_watermark_at: datetime
    source_trade_count: int
    source_receipt_hashes: tuple[str, ...]
    coverage_material_hash: str
    replay_bucket_hash: str

    @property
    def canonical_known_at(self) -> datetime:
        return self.aggregate.fact.known_at


@dataclass(frozen=True)
class TradeFlowReplayArtifact:
    """Pure derivation evidence before dataset identity is bound by the caller."""

    schema_version: str
    policy_hash: str
    bucket_count: int
    eligible_bucket_count: int
    excluded_bucket_count: int
    exclusion_counts: Mapping[str, int]
    coverage_material_hashes: tuple[str, ...]
    replay_bucket_hashes: tuple[str, ...]
    replay_semantic_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _receipt_material(trade: MarketTradeFact) -> dict[str, Any]:
    """Hash delivery evidence while deliberately excluding accepted/known-at."""

    return {
        "provider_product_id": trade.provider_product_id,
        "provider_trade_id": trade.provider_trade_id,
        "material_hash": trade.material_hash,
        "provider_event_time": trade.provider_event_time.isoformat(),
        "received_at": trade.received_at.isoformat(),
        "connection_epoch": trade.connection_epoch,
        "receive_ordinal": trade.receive_ordinal,
        "event_ordinal": trade.event_ordinal,
        "trade_ordinal": trade.trade_ordinal,
        "raw_record_id": trade.raw_record_id,
        "coverage_interval_id": trade.coverage_interval_id,
    }


def _receipt_normalized(trade: MarketTradeFact) -> MarketTradeFact:
    """Make aggregate reconstruction independent of batch acceptance time."""

    return replace(trade, accepted_at=trade.received_at, known_at=trade.received_at)


def derive_trade_flow_replay(
    *,
    policy: TradeFlowReplayPolicy,
    aggregates: Sequence[TradeFlowAggregateRecord],
    source_trades: Sequence[MarketTradeRecord],
    coverage_versions: Mapping[tuple[str, int], TradeCoverageIntervalVersion],
) -> tuple[
    tuple[AvailableTradeFlowBucket, ...],
    TradeFlowReplayArtifact,
]:
    """Derive replay availability and fail closed on any material mismatch.

    A bucket needs both exact aggregate/raw/coverage reconciliation and a later
    covered trade receipt that advances source event time beyond the bucket.
    The latter is the v1 watermark for populated *and* zero-trade buckets.
    """

    ordered_aggregates = tuple(
        sorted(
            aggregates,
            key=lambda row: (row.fact.bucket_start, row.revision, row.version_id),
        )
    )
    if len({row.fact.bucket_start for row in ordered_aggregates}) != len(
        ordered_aggregates
    ):
        raise ValueError("research_replay_invalid: duplicate aggregate buckets")
    expected_step = timedelta(seconds=policy.interval_seconds)
    for previous, current in zip(
        ordered_aggregates,
        ordered_aggregates[1:],
        strict=False,
    ):
        if current.fact.bucket_start - previous.fact.bucket_start != expected_step:
            raise ValueError("research_replay_invalid: aggregate bucket gap")
    trades = tuple(
        sorted(
            (row.fact for row in source_trades),
            key=lambda fact: (
                fact.connection_epoch,
                fact.receive_ordinal,
                fact.event_ordinal,
                fact.trade_ordinal,
                fact.raw_record_id,
            ),
        )
    )
    eligible: list[AvailableTradeFlowBucket] = []
    exclusions: Counter[str] = Counter()
    coverage_hashes: set[str] = set()

    for aggregate_record in ordered_aggregates:
        aggregate = aggregate_record.fact
        if aggregate.interval_seconds != policy.interval_seconds:
            raise ValueError("research_replay_invalid: aggregate interval mismatch")
        if not aggregate.aggregate_complete:
            raise ValueError("research_replay_invalid: aggregate is incomplete")
        if policy.require_archive_complete and not aggregate.archive_complete:
            raise ValueError("research_replay_invalid: aggregate archive is incomplete")
        if (
            policy.require_canonicalization_complete
            and not aggregate.canonicalization_complete
        ):
            raise ValueError(
                "research_replay_invalid: aggregate canonicalization is incomplete"
            )
        if aggregate.coverage_interval_id is None or aggregate.coverage_revision is None:
            raise ValueError("research_replay_invalid: aggregate coverage is missing")
        coverage_key = (
            aggregate.coverage_interval_id,
            aggregate.coverage_revision,
        )
        coverage = coverage_versions.get(coverage_key)
        if coverage is None:
            raise ValueError(
                "research_replay_invalid: exact coverage revision is not bound"
            )
        if policy.require_complete_coverage and not coverage.complete_for_bucket(
            bucket_start=aggregate.bucket_start,
            bucket_end=aggregate.bucket_end,
        ):
            raise ValueError(
                "research_replay_invalid: coverage does not prove aggregate bucket"
            )
        coverage_hashes.add(coverage.material_hash)

        bucket_trades = tuple(
            trade
            for trade in trades
            if trade.coverage_interval_id == coverage.interval_id
            and aggregate.bucket_start
            <= trade.provider_event_time
            < aggregate.bucket_end
        )
        reconstructed = aggregate_trade_bucket(
            (_receipt_normalized(trade) for trade in bucket_trades),
            interval_seconds=policy.interval_seconds,
            bucket_start=aggregate.bucket_start,
            coverage=coverage,
            computed_at=aggregate.bucket_end,
        )
        if (
            reconstructed.material_hash != aggregate.material_hash
            or reconstructed.input_fingerprint != aggregate.input_fingerprint
        ):
            raise ValueError(
                "research_replay_invalid: frozen raw trades do not reconcile to aggregate"
            )

        latest_bucket_ordinal = max(
            (trade.receive_ordinal for trade in bucket_trades),
            default=coverage.opening_receive_ordinal - 1,
        )
        latest_bucket_receipt = max(
            (trade.received_at for trade in bucket_trades),
            default=aggregate.bucket_end,
        )
        watermark_candidates = (
            trade
            for trade in trades
            if trade.coverage_interval_id == coverage.interval_id
            and trade.receive_ordinal > latest_bucket_ordinal
            and trade.provider_event_time >= aggregate.bucket_end
            and trade.received_at >= max(aggregate.bucket_end, latest_bucket_receipt)
        )
        watermark = min(
            watermark_candidates,
            key=lambda trade: (
                trade.received_at,
                trade.receive_ordinal,
                trade.event_ordinal,
                trade.trade_ordinal,
                trade.raw_record_id,
            ),
            default=None,
        )
        if watermark is None:
            exclusions["source_watermark_unavailable"] += 1
            continue

        replay_available_at = max(
            aggregate.bucket_end,
            latest_bucket_receipt,
            watermark.received_at,
        ) + timedelta(milliseconds=policy.processing_latency_ms)
        receipt_material = tuple(
            _receipt_material(trade) for trade in bucket_trades
        )
        source_receipt_hashes = tuple(
            _stable_hash(material) for material in receipt_material
        )
        replay_bucket_hash = _stable_hash(
            {
                "schema_version": TRADE_FLOW_REPLAY_SCHEMA_VERSION,
                "policy_hash": policy.policy_hash,
                "aggregate_material_hash": aggregate.material_hash,
                "aggregate_input_fingerprint": aggregate.input_fingerprint,
                "coverage_material_hash": coverage.material_hash,
                "source_receipts": list(receipt_material),
                "watermark_receipt": _receipt_material(watermark),
                "replay_available_at": replay_available_at.isoformat(),
            }
        )
        eligible.append(
            AvailableTradeFlowBucket(
                aggregate=aggregate_record,
                replay_available_at=replay_available_at,
                source_receipt_watermark_at=watermark.received_at,
                source_trade_count=len(bucket_trades),
                source_receipt_hashes=source_receipt_hashes,
                coverage_material_hash=coverage.material_hash,
                replay_bucket_hash=replay_bucket_hash,
            )
        )

    replay_hashes = tuple(row.replay_bucket_hash for row in eligible)
    artifact = TradeFlowReplayArtifact(
        schema_version=TRADE_FLOW_REPLAY_SCHEMA_VERSION,
        policy_hash=policy.policy_hash,
        bucket_count=len(ordered_aggregates),
        eligible_bucket_count=len(eligible),
        excluded_bucket_count=sum(exclusions.values()),
        exclusion_counts=dict(sorted(exclusions.items())),
        coverage_material_hashes=tuple(sorted(coverage_hashes)),
        replay_bucket_hashes=replay_hashes,
        replay_semantic_hash=_stable_hash(
            {
                "schema_version": TRADE_FLOW_REPLAY_SCHEMA_VERSION,
                "policy_hash": policy.policy_hash,
                "replay_bucket_hashes": list(replay_hashes),
                "exclusion_counts": dict(sorted(exclusions.items())),
            }
        ),
    )
    return tuple(eligible), artifact


__all__ = [
    "TRADE_FLOW_REPLAY_SCHEMA_VERSION",
    "TRADE_FLOW_REPLAY_TRANSFORM_VERSION",
    "TRADE_FLOW_REPLAY_WATERMARK_POLICY",
    "AvailableTradeFlowBucket",
    "TradeFlowReplayArtifact",
    "TradeFlowReplayPolicy",
    "derive_trade_flow_replay",
]

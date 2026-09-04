"""Typed storage-lifecycle policy for continuously collected market data."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence


MARKET_STORAGE_LIFECYCLE_POLICY_VERSION = "market.storage_lifecycle.v1"

# Retired family-table chunk controls must not operate on the generalized
# schema. Canonical retention owns complete daily hot-payload partitions.
DEFAULT_HOT_TABLE_POLICIES: tuple[object, ...] = ()


@dataclass(frozen=True)
class CanonicalFactRetentionPolicy:
    """Placement-day hot windows and explicit, non-overriding pressure limits.

    Budgets are optional planning targets, not permission to discard evidence.
    The filesystem budget includes all used bytes on the archive filesystem;
    the hot-payload budget excludes permanent headers and other PostgreSQL data.
    """

    hot_days: int = 30
    hot_days_by_fact_type: Mapping[str, int] = field(default_factory=dict)
    hot_payload_budget_bytes: int | None = None
    archive_filesystem_budget_bytes: int | None = None
    archive_min_free_bytes: int = 1024**3
    max_candidate_partitions: int = 16
    max_inventory_partitions: int = 4096
    plan_statement_timeout_ms: int = 5000
    max_plan_seconds: int = 15

    def __post_init__(self) -> None:
        bounds = {
            "hot_days": (1, 36500),
            "archive_min_free_bytes": (0, None),
            "max_candidate_partitions": (1, 128),
            "max_inventory_partitions": (1, 10000),
            "plan_statement_timeout_ms": (1, 60000),
            "max_plan_seconds": (1, 300),
        }
        for name, (minimum, maximum) in bounds.items():
            value = getattr(self, name)
            if (type(value) is not int or value < minimum
                    or (maximum is not None and value > maximum)):
                raise ValueError(f"canonical_retention_policy_invalid: field={name}")
        for name in ("hot_payload_budget_bytes", "archive_filesystem_budget_bytes"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError(f"canonical_retention_policy_invalid: field={name}; use null or a positive integer")
        if self.max_candidate_partitions > self.max_inventory_partitions:
            raise ValueError("canonical_retention_policy_invalid: candidate bound exceeds inventory bound")
        if not isinstance(self.hot_days_by_fact_type, Mapping):
            raise ValueError("canonical_retention_policy_invalid: hot_days_by_fact_type must be a mapping")
        overrides = dict(self.hot_days_by_fact_type)
        for name, days in overrides.items():
            if (not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+", name)
                    or type(days) is not int or not 1 <= days <= 36500):
                raise ValueError(f"canonical_retention_policy_invalid: hot_days_by_fact_type={name!r}")
        object.__setattr__(self, "hot_days_by_fact_type", MappingProxyType(overrides))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "CanonicalFactRetentionPolicy":
        if value is not None and not isinstance(value, Mapping):
            raise ValueError("canonical_retention_policy_invalid: expected mapping")
        payload = dict(value or {})
        unknown = sorted(set(payload) - {item.name for item in fields(cls)})
        if unknown:
            raise ValueError("canonical_retention_policy_invalid: unsupported fields=" + ",".join(unknown))
        return cls(**payload)

    def hot_window_days(self, fact_types: Sequence[str]) -> int:
        # A mixed day must satisfy the longest window of every family present.
        return max((self.hot_days_by_fact_type.get(name, self.hot_days) for name in fact_types),
                   default=self.hot_days)

    def to_dict(self) -> dict[str, Any]:
        return {item.name: (dict(self.hot_days_by_fact_type) if item.name == "hot_days_by_fact_type"
                            else getattr(self, item.name)) for item in fields(self)}


def _integer(
    value: object,
    default: int,
    *,
    field_name: str,
    minimum: int,
) -> int:
    try:
        parsed = int(default if value is None else value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"market_storage_lifecycle_policy_invalid: {field_name} must be an integer"
        ) from exc
    if parsed < minimum:
        raise ValueError(
            "market_storage_lifecycle_policy_invalid: "
            f"{field_name} must be >= {minimum}"
        )
    return parsed


def _boolean(value: object, default: bool, *, field_name: str) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(
        f"market_storage_lifecycle_policy_invalid: {field_name} must be boolean"
    )


@dataclass(frozen=True)
class MarketStorageLifecyclePolicy:
    """One bounded, portable policy shared by manual and scheduled lifecycle runs."""

    enabled: bool = True
    execution_enabled: bool = False
    interval_seconds: int = 3600
    archive_compaction_enabled: bool = True
    archive_expiration_enabled: bool = True
    raw_trade_archive_days: int = 400
    raw_l2_archive_days: int = 90
    book_checkpoint_archive_days: int = 90
    compacted_source_grace_hours: int = 24
    compaction_min_age_minutes: int = 15
    compaction_min_objects: int = 2
    compaction_target_bytes: int = 512 * 1024**2
    max_compaction_groups_per_run: int = 8
    max_archive_expirations_per_run: int = 100
    canonical_retention: CanonicalFactRetentionPolicy = field(default_factory=CanonicalFactRetentionPolicy)

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None
    ) -> "MarketStorageLifecyclePolicy":
        payload = dict(value or {})
        supported = {
            "enabled",
            "execution_enabled",
            "interval_seconds",
            "archive_compaction_enabled",
            "archive_expiration_enabled",
            "raw_trade_archive_days",
            "raw_l2_archive_days",
            "book_checkpoint_archive_days",
            "compacted_source_grace_hours",
            "compaction_min_age_minutes",
            "compaction_min_objects",
            "compaction_target_bytes",
            "max_compaction_groups_per_run",
            "max_archive_expirations_per_run",
            "canonical_retention",
        }
        unknown = sorted(set(payload) - supported)
        if unknown:
            raise ValueError(
                "market_storage_lifecycle_policy_invalid: unsupported fields="
                + ",".join(unknown)
            )
        policy = cls(
            canonical_retention=CanonicalFactRetentionPolicy.from_mapping(payload.get("canonical_retention")),
            enabled=_boolean(payload.get("enabled"), True, field_name="enabled"),
            execution_enabled=_boolean(
                payload.get("execution_enabled"),
                False,
                field_name="execution_enabled",
            ),
            interval_seconds=_integer(
                payload.get("interval_seconds"),
                3600,
                field_name="interval_seconds",
                minimum=60,
            ),
            archive_compaction_enabled=_boolean(
                payload.get("archive_compaction_enabled"),
                True,
                field_name="archive_compaction_enabled",
            ),
            archive_expiration_enabled=_boolean(
                payload.get("archive_expiration_enabled"),
                True,
                field_name="archive_expiration_enabled",
            ),
            raw_trade_archive_days=_integer(
                payload.get("raw_trade_archive_days"),
                400,
                field_name="raw_trade_archive_days",
                minimum=1,
            ),
            raw_l2_archive_days=_integer(
                payload.get("raw_l2_archive_days"),
                90,
                field_name="raw_l2_archive_days",
                minimum=1,
            ),
            book_checkpoint_archive_days=_integer(
                payload.get("book_checkpoint_archive_days"),
                90,
                field_name="book_checkpoint_archive_days",
                minimum=1,
            ),
            compacted_source_grace_hours=_integer(
                payload.get("compacted_source_grace_hours"),
                24,
                field_name="compacted_source_grace_hours",
                minimum=1,
            ),
            compaction_min_age_minutes=_integer(
                payload.get("compaction_min_age_minutes"),
                15,
                field_name="compaction_min_age_minutes",
                minimum=1,
            ),
            compaction_min_objects=_integer(
                payload.get("compaction_min_objects"),
                2,
                field_name="compaction_min_objects",
                minimum=2,
            ),
            compaction_target_bytes=_integer(
                payload.get("compaction_target_bytes"),
                512 * 1024**2,
                field_name="compaction_target_bytes",
                minimum=1024**2,
            ),
            max_compaction_groups_per_run=_integer(
                payload.get("max_compaction_groups_per_run"),
                8,
                field_name="max_compaction_groups_per_run",
                minimum=1,
            ),
            max_archive_expirations_per_run=_integer(
                payload.get("max_archive_expirations_per_run"),
                100,
                field_name="max_archive_expirations_per_run",
                minimum=1,
            ),
        )
        return policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
            "enabled": self.enabled,
            "execution_enabled": self.execution_enabled,
            "interval_seconds": self.interval_seconds,
            "archive_compaction_enabled": self.archive_compaction_enabled,
            "archive_expiration_enabled": self.archive_expiration_enabled,
            "raw_trade_archive_days": self.raw_trade_archive_days,
            "raw_l2_archive_days": self.raw_l2_archive_days,
            "book_checkpoint_archive_days": self.book_checkpoint_archive_days,
            "compacted_source_grace_hours": self.compacted_source_grace_hours,
            "compaction_min_age_minutes": self.compaction_min_age_minutes,
            "compaction_min_objects": self.compaction_min_objects,
            "compaction_target_bytes": self.compaction_target_bytes,
            "max_compaction_groups_per_run": self.max_compaction_groups_per_run,
            "max_archive_expirations_per_run": self.max_archive_expirations_per_run,
            "canonical_retention": self.canonical_retention.to_dict(),
        }


__all__ = [
    "CanonicalFactRetentionPolicy",
    "DEFAULT_HOT_TABLE_POLICIES",
    "MARKET_STORAGE_LIFECYCLE_POLICY_VERSION",
    "MarketStorageLifecyclePolicy",
]

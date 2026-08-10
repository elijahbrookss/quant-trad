"""Typed storage-lifecycle policy for continuously collected market data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


MARKET_STORAGE_LIFECYCLE_POLICY_VERSION = "market.storage_lifecycle.v1"

# Canonical facts share one immutable relation. Chunk compression/expiration
# stays disabled until it can be planned per fact contract without deleting
# another family's frozen evidence.
DEFAULT_HOT_TABLE_POLICIES: tuple[object, ...] = ()


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
        }
        unknown = sorted(set(payload) - supported)
        if unknown:
            raise ValueError(
                "market_storage_lifecycle_policy_invalid: unsupported fields="
                + ",".join(unknown)
            )
        policy = cls(
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
        }


__all__ = [
    "DEFAULT_HOT_TABLE_POLICIES",
    "MARKET_STORAGE_LIFECYCLE_POLICY_VERSION",
    "MarketStorageLifecyclePolicy",
]

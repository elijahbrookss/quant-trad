"""Typed storage-lifecycle policy for continuously collected market data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


MARKET_STORAGE_LIFECYCLE_POLICY_VERSION = "market.storage_lifecycle.v1"


@dataclass(frozen=True)
class HotTablePolicy:
    """Compression and optional retention policy for one Timescale hypertable."""

    table_name: str
    time_column: str
    compression_after_days: int
    retention_days: Optional[int]
    dependent_tables: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "time_column": self.time_column,
            "compression_after_days": self.compression_after_days,
            "retention_days": self.retention_days,
            "dependent_tables": [
                {"table_name": table, "time_column": time_column}
                for table, time_column in self.dependent_tables
            ],
        }


def _hot_table_policies(
    *,
    raw_trade_hot_days: int = 180,
    raw_l2_hot_days: int = 7,
    derived_hot_days: int = 400,
) -> tuple[HotTablePolicy, ...]:
    return (
        HotTablePolicy("candle_versions", "candle_open_time", 30, None),
        HotTablePolicy("open_interest_versions", "sample_time", 30, None),
        HotTablePolicy("funding_rate_versions", "sample_time", 30, None),
        HotTablePolicy("market_trade_versions", "provider_event_time", 7, raw_trade_hot_days),
        HotTablePolicy("trade_flow_aggregate_versions", "bucket_start", 7, derived_hot_days),
        HotTablePolicy(
            "l2_snapshot_versions",
            "effective_at",
            1,
            raw_l2_hot_days,
            (("l2_snapshot_levels", "snapshot_effective_at"),),
        ),
        HotTablePolicy(
            "l2_mutation_batches",
            "effective_at",
            1,
            raw_l2_hot_days,
            (("l2_mutations", "batch_effective_at"),),
        ),
        HotTablePolicy("bbo_feature_versions", "bucket_start", 7, derived_hot_days),
        HotTablePolicy("depth_feature_versions", "bucket_start", 7, derived_hot_days),
        HotTablePolicy("trade_flow_feature_versions", "bucket_start", 7, derived_hot_days),
        HotTablePolicy("futures_spot_relationship_versions", "effective_at", 7, derived_hot_days),
        HotTablePolicy("derivative_state_versions", "effective_at", 7, derived_hot_days),
        HotTablePolicy("normalized_feature_versions", "effective_at", 7, derived_hot_days),
        HotTablePolicy("market_response_feature_versions", "bucket_start", 7, derived_hot_days),
    )


DEFAULT_HOT_TABLE_POLICIES = _hot_table_policies()


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
    hot_compression_enabled: bool = True
    hot_expiration_enabled: bool = True
    raw_trade_archive_days: int = 400
    raw_l2_archive_days: int = 90
    book_checkpoint_archive_days: int = 90
    raw_trade_hot_days: int = 180
    raw_l2_hot_days: int = 7
    derived_hot_days: int = 400
    compacted_source_grace_hours: int = 24
    compaction_min_age_minutes: int = 15
    compaction_min_objects: int = 2
    compaction_target_bytes: int = 512 * 1024**2
    max_compaction_groups_per_run: int = 8
    max_archive_expirations_per_run: int = 100
    max_chunk_operations_per_run: int = 16
    hot_tables: tuple[HotTablePolicy, ...] = field(
        default_factory=lambda: DEFAULT_HOT_TABLE_POLICIES
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "hot_tables",
            _hot_table_policies(
                raw_trade_hot_days=self.raw_trade_hot_days,
                raw_l2_hot_days=self.raw_l2_hot_days,
                derived_hot_days=self.derived_hot_days,
            ),
        )

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
            "hot_compression_enabled",
            "hot_expiration_enabled",
            "raw_trade_archive_days",
            "raw_l2_archive_days",
            "book_checkpoint_archive_days",
            "raw_trade_hot_days",
            "raw_l2_hot_days",
            "derived_hot_days",
            "compacted_source_grace_hours",
            "compaction_min_age_minutes",
            "compaction_min_objects",
            "compaction_target_bytes",
            "max_compaction_groups_per_run",
            "max_archive_expirations_per_run",
            "max_chunk_operations_per_run",
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
            hot_compression_enabled=_boolean(
                payload.get("hot_compression_enabled"),
                True,
                field_name="hot_compression_enabled",
            ),
            hot_expiration_enabled=_boolean(
                payload.get("hot_expiration_enabled"),
                True,
                field_name="hot_expiration_enabled",
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
            raw_trade_hot_days=_integer(
                payload.get("raw_trade_hot_days"),
                180,
                field_name="raw_trade_hot_days",
                minimum=8,
            ),
            raw_l2_hot_days=_integer(
                payload.get("raw_l2_hot_days"),
                7,
                field_name="raw_l2_hot_days",
                minimum=2,
            ),
            derived_hot_days=_integer(
                payload.get("derived_hot_days"),
                400,
                field_name="derived_hot_days",
                minimum=8,
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
            max_chunk_operations_per_run=_integer(
                payload.get("max_chunk_operations_per_run"),
                16,
                field_name="max_chunk_operations_per_run",
                minimum=1,
            ),
        )
        for table in policy.hot_tables:
            if table.retention_days is not None and (
                table.retention_days <= table.compression_after_days
            ):
                raise ValueError(
                    "market_storage_lifecycle_policy_invalid: retention must follow "
                    f"compression table={table.table_name}"
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
            "hot_compression_enabled": self.hot_compression_enabled,
            "hot_expiration_enabled": self.hot_expiration_enabled,
            "raw_trade_archive_days": self.raw_trade_archive_days,
            "raw_l2_archive_days": self.raw_l2_archive_days,
            "book_checkpoint_archive_days": self.book_checkpoint_archive_days,
            "raw_trade_hot_days": self.raw_trade_hot_days,
            "raw_l2_hot_days": self.raw_l2_hot_days,
            "derived_hot_days": self.derived_hot_days,
            "compacted_source_grace_hours": self.compacted_source_grace_hours,
            "compaction_min_age_minutes": self.compaction_min_age_minutes,
            "compaction_min_objects": self.compaction_min_objects,
            "compaction_target_bytes": self.compaction_target_bytes,
            "max_compaction_groups_per_run": self.max_compaction_groups_per_run,
            "max_archive_expirations_per_run": self.max_archive_expirations_per_run,
            "max_chunk_operations_per_run": self.max_chunk_operations_per_run,
            "hot_tables": [table.to_dict() for table in self.hot_tables],
        }


__all__ = [
    "DEFAULT_HOT_TABLE_POLICIES",
    "HotTablePolicy",
    "MARKET_STORAGE_LIFECYCLE_POLICY_VERSION",
    "MarketStorageLifecyclePolicy",
]

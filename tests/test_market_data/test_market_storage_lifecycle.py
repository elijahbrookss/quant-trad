from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from core.market_storage_lifecycle import MarketStorageLifecyclePolicy
from portal.backend.service.market.market_storage_lifecycle import (
    MarketStorageLifecycleService,
)


class _LifecycleRepository:
    def __init__(self) -> None:
        self.lock_entered = False

    def list_compaction_manifests(self, **_kwargs):
        base = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
        return [
            {
                "id": f"manifest-{ordinal}",
                "definition_id": "definition-a",
                "session_id": "session-a",
                "connection_epoch": 3,
                "first_receive_ordinal": ordinal,
                "last_receive_ordinal": ordinal,
                "first_received_at": base + timedelta(seconds=ordinal),
                "last_received_at": base + timedelta(seconds=ordinal),
                "object_key": f"raw/{ordinal}.parquet",
                "object_sha256": str(ordinal) * 64,
                "byte_count": 1024,
                "record_count": 1,
            }
            for ordinal in (1, 2)
        ]

    def operation_completed(self, **_kwargs):
        return False

    def list_archive_expiration_candidates(self, **_kwargs):
        effective = datetime(2025, 1, 1, tzinfo=UTC)
        return [
            {
                "target_id": "manifest-unpinned",
                "target_kind": "raw_manifest",
                "object_key": "raw/unpinned.parquet",
                "object_sha256": "a" * 64,
                "byte_count": 4096,
                "effective_at": effective,
                "channels": ["market_trades"],
                "replacement_manifest_id": None,
                "compacted_at": None,
                "explicit_pin_count": 0,
                "dataset_pin_count": 0,
            },
            {
                "target_id": "manifest-pinned",
                "target_kind": "raw_manifest",
                "object_key": "raw/pinned.parquet",
                "object_sha256": "b" * 64,
                "byte_count": 8192,
                "effective_at": effective,
                "channels": ["level2"],
                "replacement_manifest_id": None,
                "compacted_at": None,
                "explicit_pin_count": 1,
                "dataset_pin_count": 2,
            },
        ]

    def list_hot_chunks(self, **_kwargs):
        return []

    @contextmanager
    def lifecycle_lock(self, **_kwargs):
        self.lock_entered = True
        yield


def _service(repository: _LifecycleRepository) -> MarketStorageLifecycleService:
    return MarketStorageLifecycleService(
        lifecycle_repository=repository,
        market_repository=object(),
    )


def test_lifecycle_plan_is_bounded_stable_and_pin_aware() -> None:
    repository = _LifecycleRepository()
    policy = MarketStorageLifecyclePolicy()

    plan = _service(repository).plan(
        policy=policy,
        now=datetime(2026, 8, 5, tzinfo=UTC),
    )

    assert plan["summary"]["archive_compaction_count"] == 1
    compaction = plan["archive_compactions"][0]
    assert compaction["source_manifest_ids"] == ["manifest-1", "manifest-2"]
    assert compaction["target_id"].startswith("rams_")
    assert len(compaction["target_id"]) == 69
    assert "," not in compaction["target_id"]

    unpinned, pinned = plan["archive_expirations"]
    assert unpinned["eligible"] is True
    assert pinned["eligible"] is False
    assert pinned["blockers"] == [
        "explicit_retention_pin",
        "frozen_dataset_pin",
    ]
    assert plan["summary"]["estimated_reclaim_bytes"] == 4096


def test_lifecycle_run_is_dry_by_default_and_never_takes_mutation_lock() -> None:
    repository = _LifecycleRepository()
    policy = MarketStorageLifecyclePolicy()

    result = _service(repository).run(policy=policy)

    assert result["status"] == "dry_run"
    assert result["outcomes"] == []
    assert repository.lock_entered is False


def test_lifecycle_execution_requires_explicit_policy_gate() -> None:
    repository = _LifecycleRepository()
    with pytest.raises(ValueError, match="execution_not_enabled"):
        _service(repository).run(
            policy=MarketStorageLifecyclePolicy(execution_enabled=False),
            execute=True,
        )
    assert repository.lock_entered is False


def test_lifecycle_policy_rejects_unknown_or_unsafe_values() -> None:
    with pytest.raises(ValueError, match="unsupported fields=unknown"):
        MarketStorageLifecyclePolicy.from_mapping({"unknown": True})
    with pytest.raises(ValueError, match="interval_seconds must be >= 60"):
        MarketStorageLifecyclePolicy.from_mapping({"interval_seconds": 10})


def test_lifecycle_hot_windows_are_deployment_configurable() -> None:
    policy = MarketStorageLifecyclePolicy.from_mapping(
        {
            "raw_trade_hot_days": 45,
            "raw_l2_hot_days": 3,
            "derived_hot_days": 120,
        }
    )
    by_name = {table.table_name: table for table in policy.hot_tables}

    assert by_name["market_trade_versions"].retention_days == 45
    assert by_name["l2_snapshot_versions"].retention_days == 3
    assert by_name["l2_mutation_batches"].retention_days == 3
    assert by_name["trade_flow_aggregate_versions"].retention_days == 120
    assert by_name["normalized_feature_versions"].retention_days == 120
    assert by_name["candle_versions"].retention_days is None
    assert policy.to_dict()["raw_trade_hot_days"] == 45

    with pytest.raises(ValueError, match="raw_l2_hot_days must be >= 2"):
        MarketStorageLifecyclePolicy.from_mapping({"raw_l2_hot_days": 1})

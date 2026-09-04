from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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

    @contextmanager
    def lifecycle_lock(self, **_kwargs):
        self.lock_entered = True
        yield


class _PinnedDuringExecutionRepository(_LifecycleRepository):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict] = []

    def archive_target_status(self, **_kwargs):
        return {
            "expired": False,
            "pinned": True,
            "explicit_pin_count": 1,
            "dataset_pin_count": 0,
        }

    def append_event(self, **kwargs):
        self.events.append(dict(kwargs))

    @contextmanager
    def archive_expiration_lock(self, **_kwargs):
        yield


def _service(repository: _LifecycleRepository) -> MarketStorageLifecycleService:
    return MarketStorageLifecycleService(
        lifecycle_repository=repository,
        market_repository=object(),
        canonical_repository=SimpleNamespace(plan=lambda **_: {"actions": [], "execution_available": False}),
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


def test_lifecycle_dry_run_passes_requested_storage_root_to_canonical_plan(tmp_path):
    observed = {}
    service = _service(_LifecycleRepository())

    def plan(**kwargs):
        observed.update(kwargs)
        return {"actions": [], "execution_available": False}

    service.canonical_repository = SimpleNamespace(plan=plan)
    policy = MarketStorageLifecyclePolicy()
    result = service.run(policy=policy, storage_root=tmp_path)
    assert observed["policy"] is policy.canonical_retention
    assert observed["storage_root"] == tmp_path
    assert result["plan"]["canonical_retention"]["execution_available"] is False


def test_canonical_inventory_never_runs_inside_raw_exclusive_fence(tmp_path):
    repository = _LifecycleRepository()
    service = _service(repository)
    repository.list_compaction_manifests = lambda **_: []
    repository.list_archive_expiration_candidates = lambda **_: []

    def plan(**_kwargs):
        assert repository.lock_entered is False
        return {"actions": [], "execution_available": False}

    service.canonical_repository = SimpleNamespace(plan=plan)
    result = service.run(policy=MarketStorageLifecyclePolicy(execution_enabled=True),
                         execute=True, storage_root=tmp_path)
    assert result["status"] == "completed" and result["outcomes"] == []
    assert repository.lock_entered is True


def test_archive_expiration_rechecks_new_retention_pin_before_deletion() -> None:
    repository = _PinnedDuringExecutionRepository()
    item = {
        "operation_id": "operation-a",
        "target_kind": "raw_manifest",
        "target_id": "manifest-a",
        "cutoff_at": "2026-08-01T00:00:00+00:00",
        "reason": "retention elapsed",
        "object_key": "raw/a.parquet",
        "object_sha256": "a" * 64,
        "replacement_manifest_id": None,
    }

    outcome = _service(repository)._execute_archive_expiration(
        item=item,
        store=object(),
    )

    assert outcome == {
        "action": "archive_expire",
        "operation_id": "operation-a",
        "status": "skipped",
        "reason": "pinned",
    }
    assert len(repository.events) == 1
    assert repository.events[0]["event_type"] == "skipped"
    assert repository.events[0]["reason"] == "retention pin appeared after planning"


def test_canonical_archive_hold_blocks_planning_without_user_or_dataset_pins():
    repository = _LifecycleRepository()
    rows = repository.list_archive_expiration_candidates()
    rows[0]["canonical_dependency_count"] = 1
    repository.list_archive_expiration_candidates = lambda **_: rows
    plan = _service(repository).plan(policy=MarketStorageLifecyclePolicy(), now=datetime(2026, 8, 5, tzinfo=UTC))
    held = next(item for item in plan["archive_expirations"] if item["target_id"] == "manifest-unpinned")
    assert held["eligible"] is False
    assert held["blockers"] == ["canonical_archive_dependency"]


def test_hot_canonical_backlog_blocks_raw_expiration_before_any_cold_hold_exists():
    repository = _LifecycleRepository()
    rows = repository.list_archive_expiration_candidates()
    rows[0]["canonical_backlog_present"] = True
    repository.list_archive_expiration_candidates = lambda **_: rows
    plan = _service(repository).plan(policy=MarketStorageLifecyclePolicy(), now=datetime(2026, 8, 5, tzinfo=UTC))
    held = next(item for item in plan["archive_expirations"] if item["target_id"] == "manifest-unpinned")
    assert held["eligible"] is False
    assert held["blockers"] == ["canonical_hot_backlog"]


def test_lifecycle_policy_rejects_unknown_or_unsafe_values() -> None:
    with pytest.raises(ValueError, match="unsupported fields=unknown"):
        MarketStorageLifecyclePolicy.from_mapping({"unknown": True})
    with pytest.raises(ValueError, match="interval_seconds must be >= 60"):
        MarketStorageLifecyclePolicy.from_mapping({"interval_seconds": 10})


def test_lifecycle_rejects_retired_legacy_fact_table_controls() -> None:
    for field_name in (
        "hot_compression_enabled",
        "hot_expiration_enabled",
        "raw_trade_hot_days",
        "raw_l2_hot_days",
        "derived_hot_days",
        "max_chunk_operations_per_run",
    ):
        with pytest.raises(ValueError, match=f"unsupported fields={field_name}"):
            MarketStorageLifecyclePolicy.from_mapping({field_name: 1})

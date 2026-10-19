from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy, MarketStorageLifecyclePolicy
from core.storage_targets import StoragePolicy
from portal.backend.service.storage import retention_observability as module


NOW = datetime(2026, 10, 19, 18, 0, tzinfo=UTC)


class _Result:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def one(self):
        return self._row


class _Session:
    def __init__(self, row):
        self.row = row
        self.statements = []

    def execute(self, statement, values=None):
        self.statements.append((str(statement), values))
        return _Result(self.row)


def _row(**changes):
    values = {
        "sampled_at": NOW, "database_day": date(2026, 10, 19),
        "partitions_truncated": False, "manifests_truncated": False,
        "partition_count": 20, "open_partitions": 4, "sealed_partitions": 6,
        "verified_partitions": 2, "reclaimed_partitions": 8,
        "estimated_hot_rows": 1200, "physical_hot_source_bytes": 9000,
        "eligible_open_partitions": 0, "eligible_sealed_partitions": 3,
        "eligible_verified_partitions": 2, "eligible_source_bytes": 7000,
        "verified_guarded_source_bytes": 2000,
        "oldest_eligible_day": date(2026, 8, 20),
        "last_sealed_at": NOW - timedelta(hours=3), "last_sealed_day": date(2026, 10, 4),
        "last_partition_verified_at": NOW - timedelta(hours=2),
        "last_partition_verified_day": date(2026, 9, 1),
        "last_reclaimed_at": NOW - timedelta(hours=1), "last_reclaimed_day": date(2026, 8, 31),
        "cumulative_reclaimed_bytes": 11000, "compressed_archive_bytes": 4000,
        "archived_rows": 800, "verified_archive_bytes": 3000,
        "last_archived_at": NOW - timedelta(minutes=30), "last_archived_day": date(2026, 9, 2),
        "last_page_verified_at": NOW - timedelta(minutes=20),
        "last_page_verified_day": date(2026, 9, 2),
    }
    for suffix in ("6h", "24h", "7d"):
        values.update({f"intake_{suffix}": 10, f"archive_{suffix}": 8,
                       f"verify_{suffix}": 7, f"reclaim_{suffix}": 6})
    values.update(changes)
    return values


def _settings(*, overrides=None):
    canonical = CanonicalFactRetentionPolicy(hot_days_by_fact_type=overrides or {})
    return SimpleNamespace(
        market_data_lifecycle=MarketStorageLifecyclePolicy(canonical_retention=canonical),
        storage=SimpleNamespace(maintenance_limits_path=None),
    )


def test_projection_is_bounded_read_only_and_keeps_rate_domains_separate(monkeypatch):
    monkeypatch.setattr(module, "get_settings", lambda: _settings())
    monkeypatch.setattr(module, "source_revision", lambda: "a" * 40)
    session = _Session(_row())
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",))
    worker = {"worker_id": "maintenance", "state": "idle", "heartbeat_at": NOW,
              "expires_at": NOW + timedelta(seconds=30), "context": {"storage_lifecycle": {
                  "current_phase": {"name": "canonical_retention", "state": "running",
                                    "started_at": (NOW - timedelta(seconds=4)).isoformat(),
                                    "progress": {"stage": "executing", "storage_day": "2026-09-02"}}}}}
    result = module.RetentionObservability().snapshot(session, policy_payload=policy.to_dict(),
        revision=3, targets=[], workers=[worker])

    assert result["state"] == "ready"
    assert result["policy"]["fingerprint"] == policy.fingerprint
    assert result["lifecycle"]["current"]["progress"]["storage_day"] == "2026-09-02"
    assert result["lifecycle"]["last_durable_advancement"]["stage"] == "archive_page_verified"
    assert result["backlog"]["eligible_partitions"] == 5
    assert result["rates"]["windows"][0]["intake"]["basis"] == "partition_source_bytes_at_seal_time_proxy"
    assert result["rates"]["windows"][0]["reclaim"]["physical_hot_bytes"] == 6
    assert result["rates"]["eta"]["state"] == "unavailable"
    assert result["storage"]["filesystem_delta"]["state"] == "unavailable"
    assert session.statements[0][0].startswith("SET LOCAL statement_timeout")
    assert session.statements[1][0].startswith("SET LOCAL lock_timeout")
    sql = session.statements[2][0].upper()
    assert "LIMIT" in sql and not any(word in sql for word in ("INSERT ", "UPDATE ", "DELETE "))
    assert session.statements[2][1]["partition_page_limit"] == module.PARTITION_SCAN_LIMIT + 1


def test_projection_marks_truncation_and_reports_saved_policy_override_authority(monkeypatch):
    monkeypatch.setattr(module, "get_settings", lambda: _settings(overrides={"trade.tick": 30}))
    monkeypatch.setattr(module, "source_revision", lambda: "b" * 40)
    session = _Session(_row(partitions_truncated=True, eligible_open_partitions=1))
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",))
    result = module.RetentionObservability().snapshot(session, policy_payload=policy.to_dict(),
        revision=1, targets=[], workers=[])
    assert result["state"] == "partial"
    assert result["completeness"]["truncated"] is True
    assert "open_partition_source_bytes_unmeasured" in result["completeness"]["unknowns"]
    assert result["policy"]["effective_fact_type_hot_days"] == {}
    assert result["policy"]["configured_base_fact_type_hot_days"] == {"trade.tick": 30}
    assert result["policy"]["eligibility_basis"].endswith("family_overrides_cleared")
    assert result["lifecycle"]["current"]["state"] == "unknown"

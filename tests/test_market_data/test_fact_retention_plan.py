from datetime import date, timedelta

import pytest

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy, MarketStorageLifecyclePolicy
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository

TODAY = date(2026, 9, 4)


def _row(*, age=31, state="open", fact_types=None, **overrides):
    return {"storage_day": TODAY - timedelta(days=age), "state": state,
            "fact_types": fact_types or ["derivatives.funding_rate"], "hot_payload_bytes": 4096,
            "expected_rows": None if state == "open" else 2,
            "archived_rows": 2 if state == "verified" else 0,
            "page_count": 1 if state == "verified" else 0,
            "verified_page_count": 1 if state == "verified" else 0, **overrides}


def _inventory(rows):
    return {"database_day": TODAY, "database_bytes": 65536, "canonical_header_bytes": 8192,
            "raw_mapping_bytes": 16384, "hot_payload_bytes": sum(row["hot_payload_bytes"] for row in rows),
            "hot_partition_count": len(rows), "partitions": rows, "next_after_storage_day": None}


def _filesystem(**overrides):
    return {"status": "available", "read_only": False, "used_bytes": 1024**3,
            "available_bytes": 8 * 1024**3, **overrides}


def _plan(rows, *, policy=None, filesystem=None):
    return PostgresCanonicalFactRetentionRepository._build_plan(
        policy=policy or CanonicalFactRetentionPolicy(), inventory=_inventory(rows),
        filesystem=filesystem or _filesystem())


@pytest.mark.parametrize("field,value", [
    ("hot_days", 0), ("hot_days", True), ("hot_days", 1.9), ("hot_days", "30"),
    ("hot_payload_budget_bytes", 0), ("hot_payload_budget_bytes", False),
    ("archive_filesystem_budget_bytes", -1), ("archive_filesystem_budget_bytes", 1.0),
    ("archive_min_free_bytes", -1), ("max_candidate_partitions", 129),
    ("max_inventory_partitions", 10001), ("plan_statement_timeout_ms", 60001),
    ("max_plan_seconds", 301),
    ("hot_days_by_fact_type", {"market.trade": True}),
    ("hot_days_by_fact_type", {"market.*": 7}), ("hot_days_by_fact_type", []),
])
def test_canonical_policy_rejects_unsafe_values(field, value):
    with pytest.raises(ValueError, match="canonical_retention_policy_invalid"):
        CanonicalFactRetentionPolicy.from_mapping({field: value})


def test_canonical_policy_is_immutable_and_round_trips():
    overrides = {"market.trade": 7}
    policy = CanonicalFactRetentionPolicy(hot_days_by_fact_type=overrides)
    overrides["market.trade"] = 1
    assert policy.hot_window_days(["market.trade"]) == 7
    assert policy.hot_window_days(["market.trade", "derivatives.funding_rate"]) == 30
    assert CanonicalFactRetentionPolicy.from_mapping(policy.to_dict()) == policy
    with pytest.raises(TypeError):
        policy.hot_days_by_fact_type["market.trade"] = 1
    parent = MarketStorageLifecyclePolicy.from_mapping({"canonical_retention": policy.to_dict()})
    assert parent.canonical_retention == policy and parent.execution_enabled is False
    assert parent.to_dict()["canonical_retention"] == policy.to_dict()
    with pytest.raises(ValueError, match="unsupported fields=other"):
        CanonicalFactRetentionPolicy.from_mapping({"other": 1})
    with pytest.raises(ValueError, match="candidate bound exceeds"):
        CanonicalFactRetentionPolicy(max_inventory_partitions=1)


@pytest.mark.parametrize("age,blocker", [(0, "active_or_future_storage_day"),
                                        (-1, "active_or_future_storage_day"),
                                        (30, "hot_window_not_elapsed"), (31, None)])
def test_hot_retention_uses_complete_placement_days(age, blocker):
    plan = _plan([_row(age=age)])
    action = plan["actions"][0]
    assert action["eligible"] is (blocker is None)
    assert action["blockers"] == ([blocker] if blocker else [])
    assert action["eligible_before"] == (TODAY - timedelta(days=30)).isoformat()
    assert action["requires_execution_recheck"] is True
    assert plan["execution_available"] is False


def test_mixed_day_waits_for_longest_family_window_even_under_pressure():
    policy = CanonicalFactRetentionPolicy(hot_days=7, hot_days_by_fact_type={"market.trade": 45},
                                         hot_payload_budget_bytes=1)
    plan = _plan([_row(fact_types=["market.trade", "derivatives.funding_rate"])], policy=policy)
    assert plan["actions"][0]["hot_window_days"] == 45
    assert plan["actions"][0]["blockers"] == ["hot_window_not_elapsed"]
    assert plan["pressure"]["hot_payload_excess_bytes"] == 4095
    assert plan["pressure"]["may_override_hot_windows_or_evidence_checks"] is False
    assert plan["pressure"]["may_change_ingestion_policy"] is False


@pytest.mark.parametrize("state,progress,action", [
    ("open", {}, "seal_partition"),
    ("sealed", {}, "stage_page"),
    ("sealed", {"archived_rows": 2, "page_count": 1}, "verify_page"),
    ("sealed", {"archived_rows": 2, "page_count": 1, "verified_page_count": 1}, "verify_partition"),
    ("verified", {"archived_rows": 2, "page_count": 1, "verified_page_count": 1}, "reclaim_partition"),
])
def test_plan_next_phase_comes_from_acknowledged_progress(state, progress, action):
    plan = _plan([_row(state=state, **progress)])
    assert plan["actions"][0]["action"] == action
    assert plan["metadata_eligible_reclaim_bytes"] == (4096 if action == "reclaim_partition" else 0)


def test_unproven_family_is_visible_and_blocks_whole_day():
    plan = _plan([_row(fact_types=["market.trade", "market.normalized.funding_rate"])])
    assert plan["actions"][0]["blockers"] == ["canonical_dependency_proof_required"]
    assert plan["actions"][0]["unproven_fact_types"] == ["market.normalized.funding_rate"]


def test_hdd_pressure_blocks_new_publication_not_verified_hot_reclamation():
    policy = CanonicalFactRetentionPolicy(archive_filesystem_budget_bytes=1024**3,
                                         hot_payload_budget_bytes=1)
    plan = _plan([_row(age=34), _row(age=33, state="verified")], policy=policy,
                 filesystem=_filesystem(available_bytes=0))
    reclaim, seal = plan["actions"]
    assert reclaim["action"] == "reclaim_partition" and reclaim["eligible"]
    assert seal["blockers"] == ["archive_free_reserve_reached", "archive_filesystem_budget_reached"]
    assert plan["inventory"]["database_bytes"] != plan["inventory"]["hot_payload_bytes"]
    assert plan["metadata_eligible_reclaim_bytes"] == 4096


@pytest.mark.parametrize("filesystem,blocker", [
    ({"status": "unavailable", "error": "wrong UUID"}, "archive_mount_unavailable"),
    (_filesystem(read_only=True), "archive_mount_read_only"),
])
def test_bad_archive_mount_blocks_every_phase(filesystem, blocker):
    plan = _plan([_row(), _row(state="sealed"), _row(state="verified")], filesystem=filesystem)
    assert all(blocker in action["blockers"] for action in plan["actions"])
    assert plan["metadata_eligible_reclaim_bytes"] == 0


def test_missing_development_root_is_reported_without_creating_it(tmp_path, monkeypatch):
    monkeypatch.delenv("QT_MARKET_DATA_EXPECTED_UUID", raising=False)
    repository = PostgresCanonicalFactRetentionRepository(database=object())
    monkeypatch.setattr(repository, "_inventory", lambda *_args, **_kwargs: _inventory([_row()]))
    root = tmp_path / "absent"
    result = repository.plan(policy=CanonicalFactRetentionPolicy(), storage_root=root)
    assert result["archive_filesystem"]["status"] == "unavailable"
    assert result["actions"][0]["blockers"] == ["archive_mount_unavailable"]
    assert not root.exists() and list(tmp_path.iterdir()) == []


def test_candidate_page_cursor_never_claims_complete_inventory_of_actions():
    inventory = _inventory([_row()])
    inventory["next_after_storage_day"] = TODAY - timedelta(days=31)
    plan = PostgresCanonicalFactRetentionRepository._build_plan(
        policy=CanonicalFactRetentionPolicy(), inventory=inventory, filesystem=_filesystem())
    assert plan["candidate_scan_complete"] is False
    assert plan["next_after_storage_day"] == "2026-08-04"


def test_inconsistent_sealed_progress_fails_loud():
    with pytest.raises(RuntimeError, match="progress_invalid"):
        _plan([_row(state="sealed", archived_rows=3)])


def test_old_verifier_receipts_do_not_become_deletion_authority():
    plan = _plan([_row(state="verified", verified_page_count=0)])
    assert plan["actions"][0]["blockers"] == ["archive_receipts_require_review"]
    assert plan["metadata_eligible_reclaim_bytes"] == 0


def test_hot_budget_pressure_begins_at_the_configured_boundary():
    plan = _plan([_row()], policy=CanonicalFactRetentionPolicy(hot_payload_budget_bytes=4096))
    assert plan["pressure"]["hot_payload_excess_bytes"] == 0
    assert plan["pressure"]["hot_payload_budget_reached"] is True
    assert "prioritize_verified_window_eligible_hot_reclamation" in plan["pressure"]["actions"]

"""Storage health uses one current worker and the current saved policy."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from core.storage_targets import StoragePolicy
from portal.backend.service.storage.maintenance_status import maintenance_status

NOW = datetime(2026, 9, 18, 12, tzinfo=UTC)
POLICY = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",),
                       movement_enabled=True, backup_enabled=True)


def worker(identity="worker", **outcomes):
    common = {"policy_revision": 4, "policy_hash": POLICY.fingerprint}
    defaults = {
        "history_movement": {"state": "completed", "finished_at": (NOW-timedelta(minutes=10)).isoformat()},
        "local_recovery": {"state": "not_due", "last_completed_at": (NOW-timedelta(hours=2)).isoformat(),
                           "next_due_at": (NOW+timedelta(hours=22)).isoformat()},
    }
    return {"worker_id": identity, "state": "idle",
            "heartbeat_at": NOW-timedelta(seconds=5), "expires_at": NOW+timedelta(seconds=25),
            "context": {"storage_lifecycle": {
                "state": "running", "policy": {"interval_seconds": 3600},
                "maintenance": {
                    key: {"configured": True, "state": outcome["state"],
                          "checked_at": (NOW-timedelta(minutes=5)).isoformat(),
                          "outcome": {**common, **outcome}}
                    for key, outcome in (defaults | outcomes).items()
                }}}}


def status(workers, **changes):
    return maintenance_status(workers, **({"policy": POLICY.to_dict(), "revision": 4, "now": NOW} | changes))


def test_current_success_and_disabled_or_missing_configuration():
    result = status([worker()])
    assert result["movement"]["state"] == "completed"
    assert result["backup"]["state"] == "not_due"
    assert result["backup"]["last_completed_at"] == (NOW-timedelta(hours=2)).isoformat()
    assert status([], policy=None)["movement"]["state"] == "unconfigured"
    assert status([], policy=POLICY.to_dict() | {"movement_enabled": False})["movement"]["state"] == "disabled"
    assert status([])["movement"]["state"] == "unavailable"


@pytest.mark.parametrize("state", ["failed", "blocked", "busy", "cancelled"])
def test_failure_or_pending_state_cannot_be_hidden_by_old_completion(state):
    result = status([worker(local_recovery={"state": state, "error": "copy failed",
                                           "last_completed_at": (NOW-timedelta(hours=2)).isoformat()})])
    assert result["backup"]["state"] == state
    assert result["backup"]["last_completed_at"] is None


def test_fresh_worker_cannot_keep_an_old_maintenance_success_healthy():
    data = worker()
    data["context"]["storage_lifecycle"]["maintenance"]["history_movement"]["checked_at"] = (
        NOW-timedelta(hours=2)).isoformat()
    result = status([data])
    assert result["movement"]["state"] == "stale"
    assert result["backup"]["state"] == "not_due"


@pytest.mark.parametrize("change", [
    {"expires_at": NOW},
    {"state": "stopped"},
    {"state": "stopping"},
    {"heartbeat_at": NOW+timedelta(seconds=1)},
])
def test_expired_or_stopped_worker_never_reports_success(change):
    result = status([worker() | change])
    assert result["movement"]["state"] == result["backup"]["state"] == "stale"


def test_no_merging_freshness_with_another_workers_success():
    old = worker("old") | {"expires_at": NOW-timedelta(seconds=1)}
    current = worker("new", local_recovery={"state": "failed", "error": "new failure"})
    assert status([old, current])["backup"]["state"] == "failed"
    assert status([worker("one"), worker("two")])["backup"]["state"] == "unknown"


def test_revision_and_content_must_both_match_current_policy():
    assert status([worker()], revision=5)["movement"]["reason"] == "maintenance_policy_changed"
    result = status([worker()], policy=POLICY.to_dict() | {"recent_days": 31})
    assert result["backup"]["reason"] == "maintenance_policy_changed"


@pytest.mark.parametrize("change", [
    {"checked_at": "not a date"},
    {"checked_at": NOW.replace(tzinfo=None).isoformat()},
    {"checked_at": (NOW+timedelta(seconds=1)).isoformat()},
    {"outcome": {"state": "idle"}},
    {"outcome": {"state": "completed", "policy_revision": 4, "policy_hash": POLICY.fingerprint}},
])
def test_invalid_or_unconfirmed_observations_are_unknown(change):
    data = worker()
    data["context"]["storage_lifecycle"]["maintenance"]["history_movement"].update(change)
    assert status([data])["movement"]["state"] == "unknown"


def test_running_report_never_reuses_previous_success():
    data = worker()
    data["context"]["storage_lifecycle"]["maintenance"]["history_movement"] = {
        "configured": True, "state": "running", "started_at": NOW.isoformat()}
    result = status([data])
    assert result["movement"]["state"] == "running"
    assert result["movement"]["last_completed_at"] is None


def test_backups_become_due_then_overdue_without_fresh_completion():
    data = worker(local_recovery={"state": "not_due",
        "last_completed_at": (NOW-timedelta(hours=24)).isoformat(),
        "next_due_at": (NOW-timedelta(minutes=10)).isoformat()})
    assert status([data])["backup"]["state"] == "due"
    data["context"]["storage_lifecycle"]["maintenance"]["local_recovery"]["outcome"]["next_due_at"] = (
        NOW-timedelta(hours=2)).isoformat()
    assert status([data])["backup"]["state"] == "overdue"


def test_disabled_or_old_runner_and_malformed_loop_are_not_success():
    data = worker()
    data["context"]["storage_lifecycle"].pop("maintenance")
    assert status([data])["backup"]["state"] == "unavailable"
    data = worker()
    data["context"]["storage_lifecycle"]["policy"] = []
    assert status([data])["backup"]["state"] == "unknown"
    assert status([deepcopy(worker(str(i))) for i in range(101)])["movement"]["state"] == "unknown"

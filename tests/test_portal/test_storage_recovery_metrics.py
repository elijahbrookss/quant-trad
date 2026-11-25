from datetime import UTC, datetime, timedelta
import json
import logging
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.recovery_maintenance import _completed_for_due_check


def _snapshot(caplog):
    return next(json.loads(row.message.split(" | ", 1)[1]) for row in caplog.records
                if row.message.startswith("storage_recovery_snapshot | "))


def test_prune_failure_retains_last_good_timestamp_without_claiming_new_backup(caplog):
    last = datetime.now(UTC)-timedelta(hours=2)
    failure = RuntimeError("prune failed")
    calls = []
    class Copies:
        def completed(self):
            calls.append("read")
            return [(last, None, {"storage_layout": {"layout_version": "qualified-layout"}})]
        def _prune(self, keep):
            calls.append(("prune", keep))
            assert _snapshot(caplog)["last_completed_unixtime"] == last.timestamp()
            raise failure
    policy = SimpleNamespace(backup_interval_hours=24, backup_copies=3)
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError) as caught:
        _completed_for_due_check(Copies(), policy, prune=True)
    assert caught.value is failure and calls == ["read", ("prune", 3)]
    snapshot = _snapshot(caplog)
    assert snapshot["state"] == "known" and snapshot["completed_copies"] == 1
    assert snapshot["next_due_unixtime"] == last.timestamp()+86400
    assert snapshot["sampled_at"] != last.isoformat()
    assert snapshot["current_backup_completed"] is False


def test_due_check_uses_fresh_post_prune_inventory(caplog):
    last = datetime.now(UTC)-timedelta(hours=2)
    calls = []
    class Copies:
        def completed(self):
            calls.append("read")
            return [(last, None, {})] if len(calls) == 1 else []
        def _prune(self, keep): calls.append("prune")
    with caplog.at_level(logging.INFO):
        result = _completed_for_due_check(Copies(), SimpleNamespace(backup_interval_hours=24, backup_copies=3), prune=True)
    assert calls == ["read", "prune", "read"] and result == []
    assert _snapshot(caplog)["last_completed_unixtime"] == last.timestamp()


def test_no_retained_copy_is_not_a_fresh_or_zero_age_backup(caplog):
    with caplog.at_level(logging.INFO):
        assert _completed_for_due_check(SimpleNamespace(completed=lambda: []),
            SimpleNamespace(backup_interval_hours=24), prune=False) == []
    snapshot = _snapshot(caplog)
    assert snapshot["state"] == "none" and snapshot["completed_copies"] == 0
    assert snapshot["last_completed_unixtime"] is None and snapshot["next_due_unixtime"] is None


def test_future_completed_timestamp_is_not_published_as_fresh(caplog):
    future = datetime.now(UTC)+timedelta(days=1)
    with caplog.at_level(logging.INFO):
        _completed_for_due_check(SimpleNamespace(completed=lambda: [(future, None, {})]),
            SimpleNamespace(backup_interval_hours=24), prune=False)
    assert _snapshot(caplog)["state"] == "unavailable"
    assert _snapshot(caplog)["last_completed_unixtime"] is None

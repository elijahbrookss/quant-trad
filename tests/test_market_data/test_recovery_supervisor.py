"""Recovery shares the existing loop but remains independent of retention failure."""
from threading import Event
from types import SimpleNamespace

import pytest

from core.market_storage_lifecycle import MarketStorageLifecyclePolicy
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleSupervisor
from portal.backend.service.storage.repos.market_lifecycle import MarketStorageLifecycleBusyError


def _result():
    return {"status":"completed","plan":{"summary":{}},"failure_count":0}


def test_recovery_runs_after_retention_releases_its_context():
    events=[]
    def retention(**kwargs):
        events.extend(["retention","released"])
        return _result()
    def recovery(**kwargs):
        assert events==["retention","released"]
        assert not kwargs["cancelled"]()
        events.append("recovery")
        return {"state":"completed","generation":"saved"}
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=retention),recovery_runner=recovery)
    result=worker.run_once()
    assert events==["retention","released","recovery"]
    assert result["local_recovery"]["generation"]=="saved"
    assert worker.snapshot()["last_run"]["local_recovery"]==result["local_recovery"]


@pytest.mark.parametrize("busy",[False,True])
def test_retention_failure_or_busy_work_does_not_suppress_recovery(busy):
    def retention(**kwargs):
        if busy:
            raise MarketStorageLifecycleBusyError("another worker")
        raise RuntimeError("retention failed")
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=retention),
        recovery_runner=lambda **kwargs:{"state":"completed","generation":"saved"})
    result=worker.run_once()
    assert result["local_recovery"]["state"]=="completed"
    assert result["failure_count"]==(0 if busy else 1)
    assert worker.snapshot()["state"]==("running" if busy else "degraded")
    assert bool(worker.snapshot()["last_error"]) is not busy


@pytest.mark.parametrize("blocked",[False,True])
def test_recovery_problem_is_visible_without_losing_retention_outcome(blocked):
    def recovery(**kwargs):
        if blocked:
            return {"state":"blocked","reason":"space"}
        raise RuntimeError("copy failed")
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=lambda **kwargs:_result()),recovery_runner=recovery)
    result=worker.run_once()
    assert result["status"]=="degraded" and result["failure_count"]==1
    assert result["local_recovery"]["state"]==("blocked" if blocked else "failed")
    assert worker.snapshot()["state"]=="degraded" and "recovery:" in worker.snapshot()["last_error"]


def test_recovery_only_loop_never_executes_disabled_retention_and_stops_promptly():
    entered=Event()
    def retention(**kwargs):
        pytest.fail("disabled retention must not execute")
    def recovery(*,cancelled):
        entered.set()
        while not cancelled():
            Event().wait(0.01)
        raise RuntimeError("recovery_cancelled")
    worker=MarketStorageLifecycleSupervisor(
        policy=MarketStorageLifecyclePolicy(enabled=False,execution_enabled=True,interval_seconds=3600),
        service=SimpleNamespace(run=retention),recovery_runner=recovery)
    worker.start()
    try:
        assert entered.wait(2)
    finally:
        worker.stop(timeout_seconds=2)
    state=worker.snapshot()
    assert state["state"]=="stopped" and state["last_error"] is None
    assert state["last_run"]["local_recovery"]=={"state":"cancelled"}


def test_unconfigured_loop_retains_original_exception_behavior():
    def retention(**kwargs):
        raise RuntimeError("original failure")
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=retention))
    with pytest.raises(RuntimeError,match="original failure"):
        worker.run_once()



def test_history_runs_after_retention_and_failure_does_not_suppress_recovery():
    events=[]
    def retention(**kwargs):
        events.extend(["retention","released"])
        return _result()
    def history(**kwargs):
        assert events==["retention","released"]
        events.append("history")
        raise RuntimeError("history failed")
    def recovery(**kwargs):
        assert events==["retention","released","history"]
        events.append("recovery")
        return {"state":"completed"}
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=retention),history_runner=history,recovery_runner=recovery)
    result=worker.run_once()
    assert events==["retention","released","history","recovery"]
    assert result["history_movement"]["state"]=="failed"
    assert result["local_recovery"]["state"]=="completed"
    assert worker.snapshot()["state"]=="degraded"
    assert "history:" in worker.snapshot()["last_error"]


def test_history_only_loop_cancels_and_does_not_run_disabled_retention():
    entered=Event()
    def history(*,cancelled):
        entered.set()
        while not cancelled():
            Event().wait(.01)
        raise RuntimeError("storage_move_cancelled")
    worker=MarketStorageLifecycleSupervisor(
        policy=MarketStorageLifecyclePolicy(enabled=False,execution_enabled=True,interval_seconds=3600),
        service=SimpleNamespace(run=lambda **kwargs:pytest.fail("disabled retention")),
        history_runner=history)
    worker.start()
    try:
        assert entered.wait(2)
    finally:
        worker.stop(timeout_seconds=2)
    assert worker.snapshot()["state"]=="stopped"
    assert worker.snapshot()["last_error"] is None
    assert worker.snapshot()["last_run"]["history_movement"]=={"state":"cancelled"}


def test_reported_retention_failures_remain_degraded_after_successful_maintenance():
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(),
        service=SimpleNamespace(run=lambda **kwargs:{**_result(),"failure_count":1}),
        history_runner=lambda **kwargs:{"state":"idle"},
        recovery_runner=lambda **kwargs:{"state":"not_due"})
    worker.run_once()
    assert worker.snapshot()["state"]=="degraded"
    assert worker.snapshot()["last_run"]["failure_count"]==1


def test_in_progress_phase_is_visible_before_it_finishes():
    entered, release = Event(), Event()
    def history(*, cancelled):
        entered.set()
        assert release.wait(2)
        return {"state": "idle", "policy_revision": 4, "policy_hash": "saved"}
    worker = MarketStorageLifecycleSupervisor(
        policy=MarketStorageLifecyclePolicy(enabled=False), history_runner=history)
    worker.start()
    try:
        assert entered.wait(2)
        during = worker.snapshot()
        assert during["maintenance"]["history_movement"]["state"] == "running"
        assert during["maintenance"]["history_movement"]["started_at"]
        assert not during["maintenance"]["local_recovery"]["configured"]
    finally:
        release.set()
        worker.stop(timeout_seconds=2)
    final = worker.snapshot()["maintenance"]["history_movement"]
    assert final["state"] == "idle" and final["checked_at"]
    assert final["outcome"]["policy_revision"] == 4
    assert during["maintenance"]["history_movement"]["state"] == "running"

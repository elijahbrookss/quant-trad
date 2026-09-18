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

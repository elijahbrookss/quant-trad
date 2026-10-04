from contextlib import contextmanager
from dataclasses import replace
from threading import Event, Lock, Thread
from time import monotonic

import pytest

from core.execution_control import ExecutionControl, ExecutionCancelledError
from core.settings import get_settings
from portal.backend.service.research import execution_limits as limits


@pytest.fixture
def admission(monkeypatch):
    settings=get_settings()
    monkeypatch.setattr(limits,"get_settings",lambda:replace(settings,
        async_jobs=replace(settings.async_jobs,research_global_serialization=True)))
    mutex=Lock()
    sessions=[]
    class Session:
        def __init__(self):
            self.owned=False
            self.fail=False
            self.closed=False
            self.heartbeat=Event()
        def execute(self,statement):
            assert "statement_timeout" in str(statement)
        def scalar(self,statement,params):
            if "pg_try_advisory" in str(statement):
                self.owned=mutex.acquire(blocking=False)
                return self.owned
            self.heartbeat.set()
            if self.fail: raise RuntimeError("lost admission connection")
            return self.owned
    @contextmanager
    def factory():
        session=Session()
        sessions.append(session)
        try: yield session
        finally:
            if session.owned: mutex.release()
            session.closed=True
    return factory,sessions


def test_global_guard_rejects_another_process_context_and_releases(admission):
    factory,sessions=admission
    failures=[]
    def contender():
        try:
            with limits._global_admission(ExecutionControl(),session_factory=factory):
                failures.append("unexpected admission")
        except limits.ResearchAdmissionError as error:
            failures.append(str(error))
    with limits._global_admission(ExecutionControl(),session_factory=factory):
        thread=Thread(target=contender)
        thread.start()
        thread.join(5)
        assert not thread.is_alive()
        assert failures==["research_execution_busy: global capacity occupied"]
        assert not sessions[0].closed
    assert all(s.closed for s in sessions)
    with limits._global_admission(ExecutionControl(),session_factory=factory):
        pass


def test_nested_work_shares_one_admission(admission):
    factory,sessions=admission
    control=ExecutionControl()
    with limits._global_admission(control,session_factory=factory):
        with limits._global_admission(control,session_factory=factory):
            assert len(sessions)==1
    assert sessions[0].closed


def test_admission_loss_stops_work(admission):
    factory,sessions=admission
    control=ExecutionControl()
    with pytest.raises(limits.ResearchAdmissionError,match="owner_lost"):
        with limits._global_admission(control,session_factory=factory):
            sessions[0].fail=True
            assert sessions[0].heartbeat.wait(3)
            deadline=monotonic()+3
            while not control.stop_requested and monotonic()<deadline:
                Event().wait(.01)
            control.check()
    assert sessions[0].closed


def test_cancellation_releases_only_after_body_unwinds(admission):
    factory,sessions=admission
    control=ExecutionControl()
    with pytest.raises(ExecutionCancelledError):
        with limits._global_admission(control,session_factory=factory):
            control.stop(ExecutionCancelledError("cancel"))
            assert not sessions[0].closed
            control.check()
    assert sessions[0].closed


def test_unconfigured_guard_does_not_open_a_database(monkeypatch):
    settings=get_settings()
    monkeypatch.setattr(limits,"get_settings",lambda:replace(settings,
        async_jobs=replace(settings.async_jobs,research_global_serialization=False)))
    def forbidden(): raise AssertionError("must not connect")
    with limits._global_admission(ExecutionControl(),session_factory=forbidden):
        pass


def test_uncertain_admission_helper_is_not_reported_as_released(admission):
    from core.execution_control import ExecutionStopUncertainError
    factory, sessions = admission
    entered, release = Event(), Event()
    try:
        with pytest.raises(ExecutionStopUncertainError, match="shutdown_uncertain"):
            with limits._global_admission(ExecutionControl(), session_factory=factory):
                original = sessions[0].scalar
                def blocked(statement, params):
                    entered.set()
                    assert release.wait(10)
                    return original(statement, params)
                sessions[0].scalar = blocked
                assert entered.wait(3)
        assert not sessions[0].closed
    finally:
        release.set()
        deadline = monotonic() + 3
        while not sessions[0].closed and monotonic() < deadline:
            Event().wait(.01)
    assert sessions[0].closed


@pytest.mark.parametrize("publication_fails", [False, True])
def test_worker_keeps_global_admission_through_publication(admission, monkeypatch, publication_fails):
    from types import SimpleNamespace
    from core.execution_control import controlled_execution
    from portal.backend.workers import research_worker as worker
    factory, sessions = admission
    control = ExecutionControl()
    stages = []
    @contextmanager
    def heartbeat(*args, **kwargs):
        with controlled_execution(control):
            yield SimpleNamespace(control=control)
        stages.append("heartbeat_stopped")
    def build(request):
        assert not sessions[0].closed
        return {"evidence": True}
    def persist(result, *, session):
        assert stages == ["heartbeat_stopped"]
        assert not sessions[0].closed
        if publication_fails:
            raise RuntimeError("publication failed")
        return result
    monkeypatch.setattr(worker, "maintain_job_heartbeat", heartbeat)
    monkeypatch.setattr(worker, "_global_admission", lambda control, **kw:
                        limits._global_admission(control, session_factory=factory, **kw))
    monkeypatch.setattr(worker.research_service, "build_research_check_evidence", build)
    monkeypatch.setattr(worker.research_service, "persist_built_research_check_evidence", persist)
    monkeypatch.setattr(worker, "complete_job_with_owned_effect", lambda job, effect: effect(object()))
    job = SimpleNamespace(job_type=worker.JOB_TYPE_RESEARCH_CHECK_RUN, payload={"request": {}})
    if publication_fails:
        with pytest.raises(RuntimeError, match="publication failed"):
            worker.execute_claimed_research_job(job)
    else:
        assert worker.execute_claimed_research_job(job)["evidence"]
    assert len(sessions) == 1 and sessions[0].closed


def test_stalled_ownership_probe_stops_work_before_connection_returns(admission):
    from core.execution_control import controlled_execution
    factory, sessions = admission
    control = ExecutionControl()
    entered, release, interrupted = Event(), Event(), Event()
    try:
        with pytest.raises(limits.ResearchAdmissionError, match="ownership_probe_stale"):
            with controlled_execution(control), limits._global_admission(control, session_factory=factory):
                control.register("fixture-io", interrupted.set)
                original = sessions[0].scalar
                def stalled(statement, params):
                    entered.set()
                    assert release.wait(10)
                    return original(statement, params)
                sessions[0].scalar = stalled
                assert entered.wait(3)
                stopped = interrupted.wait(4)
                release.set()
                control.unregister("fixture-io")
                assert stopped, "work continued while ownership observation was stale"
                control.check()
    finally:
        release.set()

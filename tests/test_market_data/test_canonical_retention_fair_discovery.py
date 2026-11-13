"""Fair queued retries and fresh discovery after bounded normal work."""
from datetime import date,timedelta
from types import SimpleNamespace
import json
import pytest
from sqlalchemy.exc import OperationalError
from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from portal.backend.service.market import canonical_retention as module
from tests.test_market_data.test_canonical_retention_executor import _item,_plan


def _trace(tmp_path,monkeypatch,*,preseeded,finite,runs):
    days=[date(2026,9,20)+timedelta(days=n) for n in range(6)]
    clock=[30.0]; phase={day:0 for day in days}; done=set(); successes={day:0 for day in days}
    served=[];trace=[];signatures={};cycle=None
    def action(day):
        if day==days[0]:return "reclaim_partition"
        if day==days[1]:return "reclaim_partition" if finite and successes[day]>=3 else "verify_page"
        return ("stage_page","verify_page","reclaim_partition")[min(phase[day],2)]
    def plan(**kwargs):
        after=kwargs["after_storage_day"]
        return _plan([_item(action(day),day=day) for day in days
                      if day not in done and (after is None or day>after)])
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=plan))
    if preseeded:
        executor._retry_states={day:module._RetryState(day,0,0,10,1,"persisted observation represented as hint") for day in days}
    monkeypatch.setattr(module,"monotonic",lambda:clock[0])
    def step(**kwargs):
        day=date.fromisoformat(kwargs["item"]["storage_day"])
        remaining=kwargs["remaining_seconds"]()
        if remaining<9:
            clock[0]+=remaining
            kwargs["check_budget"]()
            pytest.fail("budget must stop the unfinished operation")
        served.append(day.isoformat());clock[0]+=9
        if day==days[0]:
            failure=RuntimeError("persistent older statement timeout");failure.pgcode="57014"
            raise OperationalError("SELECT qualification",{},failure)
        current=kwargs["item"]["action"]
        successes[day]+=1
        if current=="reclaim_partition":done.add(day);return {"status":"partition_reclaimed"}
        phase[day]+=1
        return {"status":"page_verified" if current=="verify_page" else "page_acknowledged"}
    monkeypatch.setattr(executor,"_execute_step",step)
    policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=16,max_run_seconds=10)
    for n in range(runs):
        signature=(str(executor._cursor),tuple((day.isoformat(),round(state.next_eligible_at-clock[0],6),state.reason)
            for day,state in sorted(executor._retry_states.items())),tuple((day.isoformat(),action(day),day in done,successes[day] if finite else 0) for day in days))
        # Failure count/age metadata does not affect selection. In the perpetual
        # case the same decision state recurs, so this is not merely a64-run
        # patience threshold: the deterministic two-run cycle can repeat.
        if signature in signatures and cycle is None:cycle={"first_run":signatures[signature],"repeat_run":n,"period":n-signatures[signature]}
        signatures.setdefault(signature,n)
        begin=clock[0];before=len(served)
        result=executor.run(policy=policy,storage_root=tmp_path,execute=True)
        trace.append({"run":n,"start":begin,"end":clock[0],"services":served[before:],"stop_reason":result["stop_reason"],"cursor":str(executor._cursor),"retry_days":[day.isoformat() for day in sorted(executor._retry_states)]})
        clock[0]+=1 # Same60/600 inter-run/cooldown ratio as the worker.
    report={"preseeded":preseeded,"finite_older_work":finite,"runs":runs,"distinct_served":sorted(set(served)),"completed_days":sorted(day.isoformat() for day in done),"unserved_days":[day.isoformat() for day in days if day.isoformat() not in served],"decision_state_cycle":cycle,"trace":trace,"scope":"Native executor/planning/clock/cancellation; fake durable operations. No database or production mutation."}
    print("restart_qualification="+json.dumps(report),flush=True)
    return report


def test_six_existing_due_retries_receive_service(tmp_path,monkeypatch):
    result=_trace(tmp_path,monkeypatch,preseeded=True,finite=False,runs=12)
    assert len(result["distinct_served"])==6


def test_fresh_worker_recovers_after_finite_older_work(tmp_path,monkeypatch):
    result=_trace(tmp_path,monkeypatch,preseeded=False,finite=True,runs=64)
    assert len(result["distinct_served"])==6
    assert len(result["completed_days"])==5


def test_fresh_worker_services_later_days_while_older_work_remains_nonterminal(tmp_path,monkeypatch):
    result=_trace(tmp_path,monkeypatch,preseeded=False,finite=False,runs=64)
    assert not result["unserved_days"],("Fresh-worker discovery not fair under repeated bounded older work; "
        f"unserved={result['unserved_days']} repeated_decision_state={result['decision_state_cycle']}")


@pytest.mark.parametrize("committed",[False,True])
def test_automatic_budget_yield_keeps_resume_hint_without_failure_count(tmp_path,monkeypatch,committed):
    day=date(2026,9,21);clock=[30.0];calls=[0]
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=lambda **_:_plan([_item(day=day)])))
    monkeypatch.setattr(module,"monotonic",lambda:clock[0])
    def step(**kwargs):
        calls[0]+=1
        if committed and calls[0]==1:
            clock[0]+=9;return {"status":"page_acknowledged"}
        clock[0]+=kwargs["remaining_seconds"]();kwargs["check_budget"]()
    monkeypatch.setattr(executor,"_execute_step",step)
    result=executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=16,max_run_seconds=10),storage_root=tmp_path,execute=True)
    assert result["stop_reason"]=="time_budget" and result["failure_count"]==0
    assert result["next_after_storage_day"]==day.isoformat()
    state=executor._retry_states[day]
    assert state.attempt_count==0 and state.next_eligible_at==50
    assert state.reason==("partition_followthrough" if committed else "time_budget")
    assert result["resume_authority"]=="committed_partition_pages_and_verifications"


@pytest.mark.parametrize("mode",["manual","stop"])
def test_manual_cursor_and_stop_keep_original_resume_semantics(tmp_path,monkeypatch,mode):
    day=date(2026,9,21);clock=[30.0]
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=lambda **_:_plan([_item(day=day)])))
    monkeypatch.setattr(module,"monotonic",lambda:clock[0])
    def step(**kwargs):
        if mode=="stop":raise module.CanonicalRetentionStopRequested("requested")
        clock[0]+=10;kwargs["check_budget"]()
    monkeypatch.setattr(executor,"_execute_step",step)
    result=executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=16,max_run_seconds=10),storage_root=tmp_path,execute=True,after_storage_day=day-timedelta(days=1) if mode=="manual" else None)
    assert result["next_after_storage_day"]==(day-timedelta(days=1)).isoformat()
    assert result["stop_reason"]==("stop_requested" if mode=="stop" else "time_budget")
    assert not executor._retry_states and result["failure_count"]==0


def test_full_hint_queue_does_not_skip_unrepresented_budget_day(tmp_path,monkeypatch):
    day=date(2026,9,21);older=day-timedelta(days=1);clock=[30.0]
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=lambda **_:_plan([_item(day=day)])))
    executor._cursor=older
    executor._retry_states[older]=module._RetryState(older,0,0,1000,3,"prior failure")
    monkeypatch.setattr(module,"monotonic",lambda:clock[0])
    def step(**kwargs):
        clock[0]+=10;kwargs["check_budget"]()
    monkeypatch.setattr(executor,"_execute_step",step)
    result=executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=16,max_run_seconds=10,max_candidate_partitions=1),storage_root=tmp_path,execute=True)
    assert result["next_after_storage_day"]==older.isoformat()
    assert set(executor._retry_states)=={older}
    assert executor._retry_states[older].attempt_count==3
    assert executor._retry_states[older].next_eligible_at==1000


def test_real_failure_after_progress_keeps_failure_reason_age_and_count(tmp_path,monkeypatch):
    day=date(2026,9,21);clock=[30.0];calls=[0]
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=lambda **_:_plan([_item(day=day)])))
    monkeypatch.setattr(module,"monotonic",lambda:clock[0])
    def step(**kwargs):
        calls[0]+=1
        if calls[0]==1:return {"status":"page_acknowledged"}
        original=RuntimeError("genuine statement timeout");original.pgcode="57014"
        raise OperationalError("SELECT qualification",{},original)
    monkeypatch.setattr(executor,"_execute_step",step)
    result=executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=2,max_run_seconds=10),storage_root=tmp_path,execute=True)
    state=executor._retry_states[day]
    assert result["failure_count"]==1
    assert state.reason=="OperationalError:57014" and state.attempt_count==1
    assert state.first_failure_at==30 and state.next_eligible_at==40


def test_stop_after_committed_progress_keeps_scan_before_day(tmp_path,monkeypatch):
    day=date(2026,9,21);calls=[0]
    executor=module.CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=lambda **_:_plan([_item(day=day)])))
    def step(**kwargs):
        calls[0]+=1
        if calls[0]==1:return {"status":"page_acknowledged"}
        raise module.CanonicalRetentionStopRequested("requested after progress")
    monkeypatch.setattr(executor,"_execute_step",step)
    result=executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,max_steps_per_run=2),storage_root=tmp_path,execute=True)
    assert result["stop_reason"]=="stop_requested" and result["failure_count"]==0
    assert result["next_after_storage_day"]==(day-timedelta(days=1)).isoformat()
    assert not executor._retry_states

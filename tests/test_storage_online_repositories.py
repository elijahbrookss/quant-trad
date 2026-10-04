"""No replay across the committed repository/WAL continuation."""
from copy import deepcopy
from types import SimpleNamespace
import time

import pytest

from scripts.automation import storage_online_final as final
from scripts.automation import storage_online_repositories as repositories
from scripts.automation import storage_recovery_prepare as preparation
from tests.test_storage_online_recovery import transition


def test_repository_entry_requires_same_live_committed_recovery_boundary(transition,monkeypatch):
    path,state,rows,original,mounts=transition
    calls=[]
    monkeypatch.setattr(repositories,"prepare_repositories",lambda *a,**kw:calls.append(kw))
    kwargs=dict(worker_process=object(),max_bytes=100,reserve_bytes=10,recent_free_bytes=10,max_duration_seconds=20)
    with pytest.raises(RuntimeError,match="live_transition_required"):
        final.prepare_online_repositories_locked(path,**kwargs)
    assert not calls
    mounts()
    final.prepare_online_repositories_locked(path,**kwargs)
    assert len(calls)==1 and calls[0]["saved"]["switch"]==original["switch"]
    assert calls[0]["saved"]["deadline"]==original["deadline"]
    token=final._SOURCE_HOLD.set(None)
    try:
        with pytest.raises(RuntimeError,match="live_source_hold_required"):
            final.prepare_online_repositories_locked(path,**kwargs)
    finally:final._SOURCE_HOLD.reset(token)


def repository_receipt():
    now=time.time();deadline=time.monotonic()+30
    return dict(phase="recovery_repository_preparing",deadline=now+30,
        recovery=dict(finished_at=now-1),switch=dict(deadline_monotonic=deadline),
        repositories=dict(recipe_sha256="a"*64,helper_id=None,helper_contract=None,
            helper_retirement=None,started_at=now,deadline_monotonic=deadline,
            completed=[],inflight="logins",finished_at=None,report=None))


@pytest.mark.parametrize("completed,inflight",[([],"create"),(["create"],None),(["logins"],"logins"),(["logins","create"],"settings")])
def test_repository_journal_cannot_skip_or_replay_a_dispatch(completed,inflight):
    saved=repository_receipt();saved["repositories"].update(completed=completed,inflight=inflight)
    with pytest.raises(RuntimeError,match="journal_invalid"):repositories.validate_journal(saved)


def test_repository_completion_requires_whole_ordered_sequence():
    saved=repository_receipt();repositories.validate_journal(saved)
    saved["phase"]="recovery_wal_ready"
    with pytest.raises(RuntimeError,match="completion_invalid"):repositories.validate_journal(saved)
    value=saved["repositories"]
    value.update(completed=list(repositories._ACTIONS),inflight=None,helper_id="b"*64,
        helper_contract="c"*64,helper_retirement="d"*64,report={},finished_at=time.time())
    repositories.validate_journal(saved)
    value["deadline_monotonic"]+=1
    with pytest.raises(RuntimeError,match="journal_invalid"):repositories.validate_journal(saved)


def test_repository_preparer_cannot_renew_original_deadline_before_persistent_work(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr(preparation,"monotonic",lambda:now[0])
    monkeypatch.setattr(preparation,"get_settings",lambda:SimpleNamespace(database=SimpleNamespace(dsn="postgresql+psycopg2://fixture@localhost/owned")))
    monkeypatch.setattr(preparation,"_private_bytes",lambda *a,**kw:b"{}")
    monkeypatch.setattr(preparation.IncrementalRecoveryConfig,"from_dict",lambda value:SimpleNamespace(pg_path=tmp_path))
    targets=[SimpleNamespace(target_id=n,medium=m,roles=[r],inspect=lambda **kw:SimpleNamespace(device_id=1,path=str(tmp_path),available_bytes=1000)) for n,m,r in [("ssd","ssd","recent"),("hdd","hdd","backups")]]
    monkeypatch.setattr(preparation,"read_storage_inventory",lambda path:targets)
    class Owner:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def begin(self):return self
        def execute(self,*args):pass
        def scalar(self,*args):return True
    monkeypatch.setattr(preparation,"create_engine",lambda *a,**kw:SimpleNamespace(connect=lambda:Owner(),dispose=lambda:None))
    monkeypatch.setattr(preparation,"_identity",lambda owner:("owned",None))
    def observe(*a,**kw):
        now[0]=120.0
        return SimpleNamespace(database_identity="owned",bindings=[dict(role="database",directory=str(tmp_path),target_id="ssd")])
    monkeypatch.setattr(preparation,"observe_header_resources",observe)
    def forbidden(**kw):raise AssertionError("persistent repository work started after original deadline")
    monkeypatch.setattr(preparation,"prepare_encrypted_repository",forbidden)
    args=SimpleNamespace(incremental_config=tmp_path,inventory=tmp_path,recent_target="ssd",backup_target="hdd",
        timeout_seconds=60,deadline_monotonic=110.0,recent_free_bytes=0,reserve_bytes=0,max_bytes=100,
        expected_database_identity="owned",pg_controldata=tmp_path,archiver_config=tmp_path)
    with pytest.raises(RuntimeError,match="time_budget_exceeded"):preparation.prepare(args)


def test_preparer_contract_only_normalizes_exact_container_hostname(monkeypatch):
    monkeypatch.setattr(repositories.host, "database_contract", lambda details: details)
    database = {"config": {"Hostname": "database-host"}}
    created = {"id": "a" * 64, "config": {"Hostname": "a" * 12}, "host": {"ReadonlyRootfs": True}}
    started = deepcopy(created)
    started["config"]["Hostname"] = "database-host"
    assert repositories._preparer_contract(created, database) == repositories._preparer_contract(started, database)
    assert created["config"]["Hostname"] == "a" * 12
    started["host"]["ReadonlyRootfs"] = False
    assert repositories._preparer_contract(created, database) != repositories._preparer_contract(started, database)
    started["config"]["Hostname"] = "another-host"
    with pytest.raises(RuntimeError, match="hostname_changed"):
        repositories._preparer_contract(started, database)

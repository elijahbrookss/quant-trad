"""Post-commit preserving spool admission, clocks and private proof binding."""
from copy import deepcopy
import hashlib
import json
import os
import time

import pytest

from scripts.automation import storage_online_runtime as runtime
from scripts.automation import storage_online_final as final
from tests.test_storage_online_recovery import transition


def spool_receipt():
    now=time.time();deadline=time.monotonic()+30
    return dict(phase="recovery_spool_preparing",deadline=now+30,
        repositories=dict(finished_at=now-1),switch=dict(deadline_monotonic=deadline),
        runtime_spool=dict(source="/original",destination="/candidate",source_identity=[1,2],
            destination_identity=[1,3],started_at=now,deadline_monotonic=deadline,
            completed=[],inflight="create",helper_id=None,helper_contract=None,
            helper_retirement=None,report=None,finished_at=None,arguments_sha256="a"*64))


@pytest.mark.parametrize("completed,inflight",[([],"copy"),(["copy"],None),(["create"],"create")])
def test_spool_journal_refuses_skipped_or_replayed_action(completed,inflight):
    value=spool_receipt();value['runtime_spool'].update(completed=completed,inflight=inflight)
    with pytest.raises(RuntimeError,match="journal_invalid"):runtime.validate_spool_journal(value)


def test_spool_completion_cannot_extend_deadline_or_skip_retirement():
    saved=spool_receipt();runtime.validate_spool_journal(saved)
    saved['phase']='recovery_spool_ready'
    with pytest.raises(RuntimeError,match="completion_invalid"):runtime.validate_spool_journal(saved)
    value=saved['runtime_spool']
    value.update(completed=['create','copy'],inflight=None,helper_id='b'*64,
        helper_contract='c'*64,helper_retirement='d'*64,report={},finished_at=time.time())
    runtime.validate_spool_journal(saved)
    value['deadline_monotonic']+=1
    with pytest.raises(RuntimeError,match="journal_invalid"):runtime.validate_spool_journal(saved)


def test_spool_entry_requires_repository_completion_and_live_source_hold(transition,monkeypatch):
    path,state,rows,original,mounts=transition
    calls=[];monkeypatch.setattr(runtime,'prepare_spool',lambda *a,**kw:calls.append(kw))
    kwargs=dict(worker_process=object(),destination=path/'new',max_bytes=1024,max_entries=10,reserve_bytes=0,max_duration_seconds=5)
    with pytest.raises(RuntimeError,match="live_transition_required"):
        final.prepare_online_runtime_spool_locked(path,**kwargs)
    mounts()
    with pytest.raises(RuntimeError,match="live_transition_required"):
        final.prepare_online_runtime_spool_locked(path,**kwargs)
    assert not calls
    token=final._SOURCE_HOLD.set(None)
    try:
        with pytest.raises(RuntimeError,match="live_source_hold_required"):
            final.prepare_online_runtime_spool_locked(path,**kwargs)
    finally:final._SOURCE_HOLD.reset(token)


def test_copy_manifest_is_bound_private_and_resists_substitution(tmp_path,monkeypatch):
    # Native test proves UID1000. Unit input models that owner only when CI differs.
    path=tmp_path/'.qt-recovery-copy.json';raw=json.dumps({'copied_files':[{'path':'spool/a.sealed','sha256':'a'*64}]}).encode()
    path.write_bytes(raw);path.chmod(0o600);digest=hashlib.sha256(raw).hexdigest()
    if os.getuid()!=1000:
        with pytest.raises(RuntimeError,match='manifest_invalid'):
            runtime._copy_report(tmp_path,digest)
        return  # Exact wrong owner refuses; native UID1000 proof covers success.
    assert runtime._copy_report(tmp_path,digest)==json.loads(raw)
    path.write_bytes(raw+b' ')
    with pytest.raises(RuntimeError,match="manifest_changed"):runtime._copy_report(tmp_path,digest)
    path.write_bytes(raw);path.chmod(0o644)
    with pytest.raises(RuntimeError,match="manifest_invalid"):runtime._copy_report(tmp_path,digest)
    path.unlink();path.symlink_to(tmp_path/'other')
    with pytest.raises(OSError):runtime._copy_report(tmp_path,digest)


def test_spool_helper_checks_space_before_opening_source(monkeypatch):
    from types import SimpleNamespace
    from scripts.automation import storage_online_drain
    called=[]
    def copy(*args,check,**kwargs):
        check()
        called.append(True)
        raise AssertionError('copy proceeded below the preserved reserve')
    monkeypatch.setattr(storage_online_drain,'prepare_recovery_spool',copy)
    monkeypatch.setattr(os,'statvfs',lambda _:SimpleNamespace(f_bavail=1,f_frsize=1))
    monkeypatch.setattr(__import__('sys'),'argv',['python',json.dumps(dict(deadline=time.monotonic()+30,
        max_entries=10,max_bytes=100,reserve_bytes=2))])
    with pytest.raises(RuntimeError,match='reserve_exhausted'):
        exec(runtime._COPY,{})
    assert not called

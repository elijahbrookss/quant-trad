"""Key-only host boundary: real private files, controlled SQL/host peers."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta

import pytest

from scripts.automation import storage_online_keys as keys
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_terminal as terminal
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_forward import prepared
from tests.test_storage_online_deadline import attempt, package_attempt, write


@pytest.fixture
def key_attempt(prepared, monkeypatch):
    a = prepared
    monkeypatch.setattr(terminal, "_retire_previous_probe", lambda *args, **kw: None)
    original_probe = terminal._probe
    a.key_proof = None
    a.key_calls = []
    def probe(root, plan, saved, package, **kw):
        if kw["action"] == "reconcile":
            return original_probe(root, plan, saved, package, **kw)
        a.key_calls.append(kw["action"])
        assert kw["expected_capture"] == a.capture
        assert kw["intent_sha256"] == a.canceled["intent_sha256"]
        if kw["action"] == "prepare_keys":
            journal = host.load_receipt(root/keys.STATE)
            assert journal["phase"] == "dispatched"
            assert kw["wall_deadline"] == journal["wall_deadline"]
            stamp = datetime.now(timezone.utc)
            a.key_proof = dict(binding_sha256="b"*64, started_at=stamp.isoformat(),
                expires_at=(stamp+timedelta(seconds=3600)).isoformat(), duration_seconds=3600,
                index_oids={"qt_header_forward_day_pk":10,"qt_header_forward_revision_day":11},complete=True)
        write(root/terminal.KEY_PROBE, dict(retired=True, name="qt-test-storage-key-preparation"))
        return deepcopy(a.key_proof)
    monkeypatch.setattr(terminal, "_probe", probe)
    a.key_probe = probe
    a.key_run = lambda execute=False: keys.prepare_operation(a.path, package_file=a.package_path, execute=execute)
    return a


def preserved(a):
    assert a.path.read_bytes() == a.original_bytes
    assert (a.root/terminal.STATE).read_bytes() == a.terminal_bytes
    assert not (a.root/forward.STATE).exists()
    assert not (a.root/"storage-online-final.json").exists()
    assert not a.path.with_name("forward-operation.json").exists()
    assert host.load_receipt(a.root/launch._STATE) == a.worker


def test_inspection_does_not_prepare_publish_or_select_cutover(key_attempt):
    a = key_attempt
    assert a.key_run()["storage_mutations_performed"] is False
    assert not (a.root/keys.STATE).exists()
    assert a.key_calls == ["inspect_keys"]
    preserved(a)


def test_execution_confirms_sql_after_retirement_and_reuses_completed_clock(key_attempt):
    a = key_attempt
    assert a.key_run(True)["keys_prepared"]
    assert a.key_calls == ["inspect_keys","prepare_keys","inspect_keys"]
    original = host.load_receipt(a.root/keys.STATE)
    assert original["wall_deadline"] == original["started_at"]+3600
    assert original["monotonic_deadline"] == original["started_monotonic"]+3600
    assert a.key_run(True)["keys_prepared"]
    assert host.load_receipt(a.root/keys.STATE) == original
    assert a.key_calls.count("prepare_keys") == 1
    preserved(a)


def test_lost_mutation_reply_reconciles_read_only_after_clock_expiry(key_attempt, monkeypatch):
    a = key_attempt
    def lost(*args, **kw):
        result = a.key_probe(*args, **kw)
        if kw["action"] == "prepare_keys": raise TimeoutError("lost reply")
        return result
    monkeypatch.setattr(terminal,"_probe",lost)
    with pytest.raises(TimeoutError):a.key_run(True)
    before = host.load_receipt(a.root/keys.STATE)
    monkeypatch.setattr(terminal,"_probe",a.key_probe)
    monkeypatch.setattr(keys.time,"time",lambda:before["wall_deadline"]+1)
    assert a.key_run(False)["keys_prepared"]
    after = host.load_receipt(a.root/keys.STATE)
    assert keys._immutable(before) == keys._immutable(after)
    assert a.key_calls.count("prepare_keys") == 1
    preserved(a)


@pytest.mark.parametrize("fault",["wall","monotonic","boot","request"])
def test_interrupted_intent_never_renews_or_accepts_changed_request(key_attempt, monkeypatch, fault):
    a = key_attempt
    real_save = host.save_receipt
    def interrupted(path, value, **kw):
        real_save(path,value,**kw)
        if path.name == keys.STATE and value["phase"] == "prepared":raise TimeoutError()
    with monkeypatch.context() as m:
        m.setattr(host,"save_receipt",interrupted)
        with pytest.raises(TimeoutError):a.key_run(True)
    before = (a.root/keys.STATE).read_bytes()
    journal = host.load_receipt(a.root/keys.STATE)
    if fault == "wall":monkeypatch.setattr(keys.time,"time",lambda:journal["wall_deadline"]+1)
    elif fault == "monotonic":monkeypatch.setattr(keys.time,"monotonic",lambda:journal["monotonic_deadline"]+1)
    elif fault == "boot":monkeypatch.setattr(terminal,"_boot",lambda:"different")
    else:
        a.manifest["end_day"]="2026-10-05"
        write(a.package_path,a.manifest)
    with pytest.raises(RuntimeError):a.key_run(True)
    assert (a.root/keys.STATE).read_bytes() == before
    assert "prepare_keys" not in a.key_calls
    preserved(a)


def test_unowned_complete_keys_never_gain_host_authority(key_attempt):
    a=key_attempt;a.key_proof={"complete":True}
    with pytest.raises(RuntimeError,match="unowned_sql_state"):a.key_run(True)
    assert not (a.root/keys.STATE).exists()
    preserved(a)


def test_publication_refuses_unfinished_key_preparation(key_attempt,monkeypatch):
    a=key_attempt;real_save=host.save_receipt
    def interrupted(path,value,**kw):
        real_save(path,value,**kw)
        if path.name==keys.STATE:raise TimeoutError()
    with monkeypatch.context() as m:
        m.setattr(host,"save_receipt",interrupted)
        with pytest.raises(TimeoutError):a.key_run(True)
    with pytest.raises(RuntimeError,match="reconciliation_required"):a.run()
    preserved(a)


def test_cli_key_preparation_is_explicit_and_read_only_by_default(key_attempt, monkeypatch):
    from cli import main
    from scripts.automation import storage_online_operation as operation
    a = key_attempt
    calls = []
    monkeypatch.setattr(keys, "prepare_operation", lambda path, **kw: calls.append((path, kw)) or {"phase": "inspected"})
    args = main.build_parser().parse_args(["storage", "migrate", "--operation-file", str(a.path),
                                          "--prepare-forward-keys-file", str(a.package_path)])
    assert args.func(args) == 0
    assert calls == [(str(a.path), dict(package_file=str(a.package_path), execute=False))]
    for option in ("forward_package_file", "cancel_attempt_file", "replacement_package_file", "capacity_file", "extend_attempt_seconds"):
        with pytest.raises(ValueError, match="must_be_separate"):
            operation.run_operation_plan(a.path, prepare_forward_keys_file=a.package_path, **{option: 1})
    assert len(calls) == 1


def test_surviving_key_worker_retires_before_cancellation_preflight(key_attempt, monkeypatch):
    a = key_attempt
    original_publish = forward.publish_package
    live = {"running": False}
    calls = []
    def retire(*args, **kw):
        assert kw == {"keys": True}
        calls.append("retire")
        live["running"] = False
    def preflight(*args, **kw):
        assert not live["running"]
        calls.append("preflight")
        return original_publish(*args, **kw)
    def lost(*args, **kw):
        result = a.key_probe(*args, **kw)
        if kw["action"] == "prepare_keys":
            live["running"] = True
            raise TimeoutError("host lost worker")
        return result
    monkeypatch.setattr(terminal, "_retire_previous_probe", retire)
    monkeypatch.setattr(forward, "publish_package", preflight)
    monkeypatch.setattr(terminal, "_probe", lost)
    with pytest.raises(TimeoutError): a.key_run(True)
    assert live["running"]
    monkeypatch.setattr(terminal, "_probe", a.key_probe)
    assert a.key_run(False)["keys_prepared"]
    assert calls == ["retire", "preflight", "retire", "preflight"]
    assert a.key_calls.count("prepare_keys") == 1
    preserved(a)


def test_changed_key_intent_refuses_before_worker_retirement(key_attempt, monkeypatch):
    a = key_attempt
    assert a.key_run(True)["keys_prepared"]
    a.manifest["end_day"] = "2026-10-05"
    write(a.package_path, a.manifest)
    def unexpected(*args, **kw): pytest.fail("changed ownership must refuse before retirement")
    monkeypatch.setattr(terminal, "_retire_previous_probe", unexpected)
    with pytest.raises(RuntimeError, match="binding_changed"): a.key_run(True)

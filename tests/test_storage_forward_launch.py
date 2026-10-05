"""Durable forward startup clocks and interrupted worker-file publication."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import time

import pytest
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_deadline import attempt, package_attempt, write
from tests.test_storage_online_forward import prepared


@pytest.fixture
def owned(prepared, monkeypatch):
    a=prepared
    a.run()
    a.published=forward.inspect_published_operation(a.root)
    a.request=a.published["new_request"]
    a.clock=[float(int(time.time())),100.,200.]
    monkeypatch.setattr(forward.time,"time",lambda:a.clock[0])
    monkeypatch.setattr(forward.time,"monotonic",lambda:a.clock[1])
    monkeypatch.setattr(forward.time,"clock_gettime",lambda _:a.clock[2])
    a.begin=lambda:forward.launch_intent(a.root,a.published,a.published["new_worker"]["binding"])
    a.intent=a.begin()
    return a


def _advance(a, seconds):
    a.clock[:]=[v+seconds for v in a.clock]


def _rows(a, *, initial_offset=3500, complete=False):
    intent=a.request["forward"]
    start=a.intent["started_at"]
    stamp=lambda seconds:datetime.fromtimestamp(seconds,timezone.utc).isoformat()
    keys=dict(binding=dict(capture=intent["original_capture"],cancellation_intent_sha256=intent["cancellation_intent_sha256"]),
        started_at=stamp(start),expires_at=stamp(start+3600),duration_seconds=3600,complete=True,
        index_oids={"qt_header_forward_day_pk":11,"qt_header_forward_revision_day":12})
    initialization=dict(binding=dict(request_sha256=host.digest(a.request),operation_sha256=intent["operation_sha256"],
        cancellation_intent_sha256=intent["cancellation_intent_sha256"],key_seconds=3600,initial_seconds=600,
        attempt_seconds=intent["original_capture"]["attempt_seconds"]),started_at=stamp(start+initial_offset),
        expires_at=stamp(start+initial_offset+600),duration_seconds=600,complete=complete)
    capture=dict(operation_sha256=intent["operation_sha256"],started_at=stamp(start+initial_offset+1),
        expires_at=stamp(start+initial_offset+1+intent["original_capture"]["attempt_seconds"]),
        attempt_seconds=intent["original_capture"]["attempt_seconds"],cancellation=dict(receipt=a.receipt),
        placement={"fixture":"retained"})
    return keys,initialization,capture


def test_forward_launch_intent_keeps_original_clocks_and_refuses_foreign_preimage(owned):
    a=owned
    original=deepcopy(a.intent)
    _advance(a,10)
    assert a.begin()==original
    altered=deepcopy(original["worker"]);altered["deadline"]=123
    write(a.root/launch._STATE,altered)
    with pytest.raises(RuntimeError,match="launch_intent_changed"):a.begin()
    assert host.load_receipt(a.root/launch._STATE)==altered


@pytest.mark.parametrize("boundary",["intent","worker","complete"])
def test_forward_worker_publication_reconciles_exact_pending_preimages(owned,monkeypatch,boundary):
    a=owned;original=deepcopy(a.intent)
    after=deepcopy(original["worker"]);after.update(container_id="e"*64,contract={"fixture":"contract"})
    save=host.save_receipt
    def interrupted(path,value,**kwargs):
        save(path,value,**kwargs)
        if ((boundary=="intent" and path.name==forward.LAUNCH_STATE and value["pending_worker"] is not None)
                or (boundary=="worker" and path.name==launch._STATE)
                or (boundary=="complete" and path.name==forward.LAUNCH_STATE and value["pending_worker"] is None)):
            raise TimeoutError("lost durable worker publication reply")
    with monkeypatch.context() as lost:
        lost.setattr(host,"save_receipt",interrupted)
        with pytest.raises(TimeoutError):forward.save_launched_worker(a.root,a.intent,after)
    recovered=a.begin()
    assert recovered["worker"]==after and recovered["pending_worker"] is None
    assert host.load_receipt(a.root/launch._STATE)==after
    for key in ("started_at","started_monotonic","started_boot","key_deadline","key_deadline_monotonic","key_deadline_boot"):
        assert recovered[key]==original[key]
    changed=deepcopy(after);changed["container_id"]="f"*64
    with pytest.raises(RuntimeError,match="worker_progress_changed"):
        forward.save_launched_worker(a.root,recovered,changed)


@pytest.mark.parametrize("complete", [True, False])
def test_prebuilt_keys_expiry_only_bounds_unfinished_index_work(owned, complete):
    a = owned
    keys, _, _ = _rows(a)
    keys["complete"] = complete
    if not complete:
        keys["index_oids"] = None
    # Index preparation was a separate completed operation hours before launch.
    for name in ("started_at", "expires_at"):
        stamp = datetime.fromisoformat(keys[name]).timestamp() - 3 * 3600
        keys[name] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
    before = (a.root / forward.LAUNCH_STATE).read_bytes()
    if not complete:
        with pytest.raises(RuntimeError, match="original_phase_expired"):
            forward.admit_startup(a.root, a.intent, a.request, keys=keys, initialization=None)
        assert (a.root / forward.LAUNCH_STATE).read_bytes() == before
        return
    clock_fields = ("started_at", "started_monotonic", "started_boot", "key_deadline",
                    "key_deadline_monotonic", "key_deadline_boot")
    original_clocks = {name: a.intent[name] for name in clock_fields}
    assert forward.admit_startup(a.root, a.intent, a.request, keys=keys, initialization=None) == pytest.approx(3600)
    saved = a.begin()
    assert saved["keys"] == keys
    assert {name: saved[name] for name in clock_fields} == original_clocks
    _advance(a, 3601)
    before = (a.root / forward.LAUNCH_STATE).read_bytes()
    with pytest.raises(RuntimeError, match="original_phase_expired"):
        forward.admit_startup(a.root, saved, a.request, keys=keys, initialization=None)
    assert (a.root / forward.LAUNCH_STATE).read_bytes() == before


def test_forward_sql_initialization_owns_its_original_window_after_key_expiry(owned):
    a=owned;keys,initial,capture=_rows(a)
    _advance(a,3501)
    remaining=forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial)
    assert remaining==pytest.approx(599)
    _advance(a,150)
    saved=a.begin()
    remaining=forward.admit_startup(a.root,saved,a.request,keys=keys,initialization=initial)
    assert remaining==pytest.approx(449)
    initial["complete"]=True
    remaining=forward.admit_startup(a.root,saved,a.request,keys=keys,initialization=initial,capture=capture)
    assert saved["forward"]["started_at"]==capture["started_at"]
    assert remaining>600 and saved["keys"]["index_oids"]==keys["index_oids"]
    _advance(a,600)
    retry=a.begin()
    assert forward.admit_startup(a.root,retry,a.request,keys=keys,initialization=initial,capture=capture)>600
    assert retry==saved


@pytest.mark.parametrize("fault",["key_expiry","initial_expiry","late_initial","reboot","wall_back","boot_back","mono_back","key_oid","initial_change","adoption_change"])
def test_forward_startup_refuses_clock_or_proof_changes_without_rewriting_receipts(owned,monkeypatch,fault):
    a=owned;keys,initial,capture=_rows(a)
    _advance(a,3501)
    forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial)
    if fault=="key_expiry":
        a.intent=a.begin();initial=None;_advance(a,200)
    elif fault=="initial_expiry":_advance(a,600)
    elif fault=="late_initial":keys,initial,capture=_rows(a,initial_offset=3601);_advance(a,200)
    elif fault=="reboot":monkeypatch.setattr(forward.terminal,"_boot",lambda:"foreign")
    elif fault=="wall_back":a.clock[0]=a.intent["started_at"]-1
    elif fault=="boot_back":a.clock[2]=a.intent["started_boot"]-1
    elif fault=="mono_back":a.clock[1]=a.intent["started_monotonic"]-1
    elif fault=="key_oid":keys["index_oids"]["qt_header_forward_day_pk"]+=1
    elif fault=="initial_change":initial["binding"]["request_sha256"]="f"*64
    else:initial["complete"]=True;capture["operation_sha256"]="f"*64
    before=(a.root/forward.LAUNCH_STATE).read_bytes()
    with pytest.raises(RuntimeError):
        forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial,capture=capture if fault=="adoption_change" else None)
    assert (a.root/forward.LAUNCH_STATE).read_bytes()==before


def test_ready_forward_admission_is_read_only_and_keeps_pinned_phase_proofs(owned, monkeypatch):
    a=owned;keys,initial,capture=_rows(a,complete=True)
    _advance(a,3501)
    forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial,capture=capture)
    worker=deepcopy(a.intent["worker"])
    worker.update(container_id="e"*64,contract={"fixture":"contract"},deadline=a.intent["deadline"])
    forward.save_launched_worker(a.root,a.intent,worker)
    before=(a.root/forward.LAUNCH_STATE).read_bytes()
    monkeypatch.setattr(host,"save_receipt",lambda *a,**k:pytest.fail("final observation must not write"))
    owner,deadline=forward.admit_launched_adoption(a.root,a.request,worker,initialization=initial,capture=capture)
    assert owner==a.intent["forward"] and deadline==worker["deadline"]
    assert (a.root/forward.LAUNCH_STATE).read_bytes()==before
    with pytest.raises(RuntimeError,match="launched_adoption_changed"):
        forward.admit_launched_adoption(a.root,a.request,worker,initialization=initial,capture={**capture,"placement":{"foreign":True}})


def test_incomplete_initialization_requires_integer_clock_binding(owned):
    a=owned;keys,initial,_=_rows(a)
    _advance(a,3501)
    initial["binding"]["initial_seconds"]=600.0
    with pytest.raises(RuntimeError,match="initialization_clock_changed"):
        forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial)


def test_large_native_metadata_round_trips_without_enlarging_control_reply(owned):
    from scripts.automation.storage_online_forward_worker import capture_binding
    a=owned;keys,initial,capture=_rows(a,complete=True)
    capture["cancellation"]["retained_proof"]={"fixture": "x"*100000}
    _advance(a,3501)
    forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial,capture=capture)
    assert (a.root/forward.LAUNCH_STATE).stat().st_size>65536
    assert a.begin()==a.intent==forward.load_launch(a.root)
    worker=deepcopy(a.intent["worker"])
    worker.update(container_id="e"*64,contract={"fixture":"contract"},deadline=a.intent["deadline"])
    forward.save_launched_worker(a.root,a.intent,worker)
    assert forward.admit_launched_adoption(a.root,a.request,worker,initialization=initial,capture=capture)[1]==worker["deadline"]
    compact=capture_binding(capture)
    assert len(json.dumps(compact))<1024
    changed=deepcopy(capture);changed["cancellation"]["retained_proof"]["fixture"]+="changed"
    assert capture_binding(changed)["proof_sha256"]!=compact["proof_sha256"]
    before=(a.root/forward.LAUNCH_STATE).read_bytes()
    oversized=deepcopy(a.intent);oversized["capture"]["placement"]["fixture"]="x"*forward._MAX_BYTES
    with pytest.raises(RuntimeError,match="receipt_budget_exceeded"):
        forward.save_launch(a.root,oversized)
    assert (a.root/forward.LAUNCH_STATE).read_bytes()==before

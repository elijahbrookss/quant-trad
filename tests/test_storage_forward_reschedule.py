"""Stopped-worker rescheduling preserves operation proof and launch clocks."""
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts.automation import storage_online_reschedule as reschedule
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_forward_worker as worker
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_deadline as publication
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_deadline import attempt as base_attempt, package_attempt, write
from tests.test_storage_forward_operation import attempt as operation_attempt, fixture_day
from tests.test_storage_online_forward import prepared
from tests.test_storage_forward_launch import owned, _rows, _advance
from tests.test_storage_forward_retirement import retiring
from tests.test_storage_forward_successor import successor


@pytest.fixture
def attempt(operation_attempt):
    a = operation_attempt
    a.plan["request"]["max_objects"] = 100
    original = host.load_receipt(a.root/publication.REQUEST)
    original["max_objects"] = 100
    (a.root/publication.REQUEST).write_bytes(publication.request_bytes(original))
    a.worker["binding"].update(request_sha256=publication._sha(publication.request_bytes(original)),
        descriptor_limit=a.plan["descriptor_limit"],memory_bytes=a.plan["memory_bytes"])
    write(a.root/launch._STATE,a.worker); write(a.path,a.plan)
    return a


@pytest.fixture(params=[1, 2])
def rescheduling(successor,monkeypatch,request):
    a = successor
    a.amendment_version = request.param
    # Existing fixtures use a fixed original boundary. Keep a genuinely later
    # boundary within their original 60-hour adoption, without extending it.
    _advance(a, datetime.fromisoformat("2026-10-04T12:00:00+00:00").timestamp()-a.clock[0])
    a.publish_successor()
    a.base = forward.inspect_published_operation(a.root)
    a.request = a.base["new_request"]
    a.intent = forward.launch_intent(a.root,a.base,a.base["new_worker"]["binding"])
    keys,initial,capture = _rows(a,initial_offset=1,complete=True)
    _advance(a,3)
    forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial,capture=capture)
    a.current = deepcopy(a.intent["worker"])
    a.current.update(container_id="7"*64,contract={"fixture":"successor"},deadline=a.intent["deadline"])
    forward.save_launched_worker(a.root,a.intent,a.current)
    a.name = "/"+a.plan["project"]+"-storage-online"
    a.sql = dict(initialization={"id":1,**initial},capture=capture,adoption_sha256="6"*64)
    a.old_sql = deepcopy(a.sql)
    a.reschedule_path = Path(a.successor_manifest["forward_plan_path"])
    a.reschedule_plan = host.load_receipt(a.reschedule_path)
    a.reschedule_package = dict(schema_version="qt.storage_online_forward_reschedule_package.v1",
        plan_sha256=publication._sha(a.reschedule_path.read_bytes()),operation_sha256=a.request["forward"]["operation_sha256"],
        image="sha256:"+"5"*64,source_revision="5"*40,source_tree_hash="6"*64,end_day="2026-10-06",
        max_objects=200,descriptor_limit=a.reschedule_plan["descriptor_limit"]+100,
        forward_plan_path=str(a.path.parent/"rescheduled-operation.json"),capacity_file=str(a.capacity_path))
    a.reschedule_file = a.path.parent/"reschedule-package.json"
    write(a.reschedule_file,a.reschedule_package)
    capacity = host.load_receipt(a.capacity_path)
    capacity.update(plan_sha256=a.reschedule_package["plan_sha256"],attempt_seconds=capture["attempt_seconds"],
        through_epoch=datetime.fromisoformat(capture["expires_at"]).timestamp(),observed_at=a.clock[0])
    write(a.capacity_path,capacity)
    a.actions=[]
    def probe(plan,saved,*,request,action,expected=None):
        assert saved==a.current and plan["image"]==a.reschedule_package["image"]
        a.actions.append(action)
        if action=="apply":
            assert expected==a.sql
            a.sql["initialization"]["binding"]=worker.initialization_binding(request)
            if request["forward_reschedule"]["schema_version"] == "qt.storage_online_forward_reschedule.v2":
                seconds = worker.adoption_seconds(request)
                a.sql["capture"].update(attempt_seconds=seconds, expires_at=(
                    datetime.fromisoformat(a.sql["capture"]["started_at"])+timedelta(seconds=seconds)).isoformat())
        return deepcopy(a.sql)
    a.reschedule_probe=probe
    monkeypatch.setattr(reschedule,"_probe",probe)
    a.change=lambda execute=True:operation.run_operation_plan(a.reschedule_path,
        reschedule_forward_file=a.reschedule_file,execute=execute)
    a.reschedule_state=a.root/reschedule.state_file(a.request["forward"]["operation_sha256"])
    a.protected.update({a.reschedule_path:a.reschedule_path.read_bytes(),
        a.root/forward.operation_file(forward.STATE,request=a.request):
            (a.root/forward.operation_file(forward.STATE,request=a.request)).read_bytes()})
    if a.amendment_version == 2:
        a.change()
        a.protected[a.reschedule_state] = a.reschedule_state.read_bytes()
        a.protected[Path(a.reschedule_package["forward_plan_path"])] = Path(a.reschedule_package["forward_plan_path"]).read_bytes()
        selected = forward.inspect_published_operation(a.root)
        a.request = selected["new_request"]
        a.intent = forward.launch_intent(a.root,selected,selected["new_worker"]["binding"])
        a.current = deepcopy(a.intent["worker"])
        a.current.update(container_id="8"*64, contract={"fixture":"first-amendment"})
        forward.save_launched_worker(a.root,a.intent,a.current)
        a.name = "/"+a.plan["project"]+"-storage-online"
        a.sql["preserved_adoption_sha256"] = a.sql.pop("adoption_sha256")
        a.old_sql = deepcopy(a.sql)
        a.reschedule_path = Path(a.reschedule_package["forward_plan_path"])
        a.reschedule_plan = host.load_receipt(a.reschedule_path)
        a.reschedule_package.update(schema_version="qt.storage_online_forward_reschedule_package.v2",
            plan_sha256=publication._sha(a.reschedule_path.read_bytes()),
            previous_amendment_sha256=publication._sha(a.reschedule_state.read_bytes()),
            attempt_seconds=84*3600, end_day="2026-10-07", image="sha256:"+"a"*64,
            source_revision="a"*40, source_tree_hash="b"*64,
            forward_plan_path=str(a.path.parent/"extended-operation.json"))
        a.reschedule_file = a.path.parent/"extension-package.json"
        write(a.reschedule_file,a.reschedule_package)
        capacity.update(plan_sha256=a.reschedule_package["plan_sha256"],attempt_seconds=84*3600,
            through_epoch=datetime.fromisoformat(a.sql["capture"]["started_at"]).timestamp()+84*3600,
            observed_at=a.clock[0])
        write(a.capacity_path,capacity)
        a.reschedule_state = a.root/reschedule.state_file(a.request["forward"]["operation_sha256"],version=2)
        a.actions.clear()
    return a


def test_reschedule_inspection_and_publication_preserve_proof_and_all_clocks(rescheduling):
    a=rescheduling
    before={p:p.read_bytes() for p in a.root.iterdir() if p.is_file()}
    assert a.change(False)["phase"]=="forward_reschedule_inspected"
    assert a.sql==a.old_sql and not a.reschedule_state.exists()
    assert all(p.read_bytes()==v for p,v in before.items())
    assert a.change()["deadline_renewed"] is (a.amendment_version == 2)
    assert a.actions.count("apply")==1
    selected=forward.inspect_published_operation(a.root,operation_path=a.reschedule_package["forward_plan_path"])
    assert worker.original_request(selected["new_request"])==worker.original_request(a.request)
    assert worker.execution_intent(selected["new_request"])["end_day"]==a.reschedule_package["end_day"]
    started=forward.launch_intent(a.root,selected,selected["new_worker"]["binding"])
    for key in ("started_at","started_monotonic","started_boot","key_deadline","key_deadline_monotonic","key_deadline_boot"):
        assert started[key]==a.intent[key]
    digest_key = "adoption_sha256" if a.amendment_version == 1 else "preserved_adoption_sha256"
    assert a.sql[digest_key]==a.old_sql[digest_key]
    delta = 0 if a.amendment_version == 1 else 24*3600
    assert started["deadline"] == a.intent["deadline"]+delta
    assert started["capture"] == a.sql["capture"]
    assert {k:v for k,v in a.sql["capture"].items() if k not in {"expires_at","attempt_seconds"}} == {
        k:v for k,v in a.old_sql["capture"].items() if k not in {"expires_at","attempt_seconds"}}
    assert a.change()["phase"]=="forward_reschedule_published"
    assert a.actions.count("apply")==1
    assert all(p.read_bytes()==data for p,data in a.protected.items())


@pytest.mark.parametrize("boundary",["intent","sql","rename",publication.REQUEST,launch._STATE,
    "launch",publication.runtime.RUNTIME_RECIPE,"plan","complete"])
def test_reschedule_lost_replies_reconcile_without_replaying_sql(rescheduling,monkeypatch,boundary):
    a=rescheduling
    save,replace,docker,publish=host.save_receipt,publication._replace,host.docker,forward._publish_plan
    def lose_save(path,value,**kwargs):
        save(path,value,**kwargs)
        if path==a.reschedule_state and ((boundary=="intent" and value["phase"]=="prepared")
                or (boundary=="complete" and value["phase"]=="complete")):
            raise TimeoutError("lost reschedule intent reply")
    def lose_probe(*args,**kwargs):
        result=a.reschedule_probe(*args,**kwargs)
        if boundary=="sql" and kwargs["action"]=="apply":raise TimeoutError("lost SQL COMMIT reply")
        return result
    def lose_replace(path,before,after):
        replace(path,before,after)
        if path.name==boundary or boundary=="launch" and path.name.startswith("storage-online-forward-launch-"):
            raise TimeoutError("lost file replacement reply")
    def lose_docker(*args,**kwargs):
        result=docker(*args,**kwargs)
        if args[0]==boundary:raise TimeoutError("lost rename reply")
        return result
    def lose_plan(*args):
        publish(*args)
        if boundary=="plan":raise TimeoutError("lost plan reply")
    with monkeypatch.context() as lost:
        lost.setattr(host,"save_receipt",lose_save);lost.setattr(reschedule,"_probe",lose_probe)
        lost.setattr(publication,"_replace",lose_replace);lost.setattr(host,"docker",lose_docker)
        lost.setattr(forward,"_publish_plan",lose_plan)
        with pytest.raises(TimeoutError):a.change()
    original=host.load_receipt(a.reschedule_state,max_bytes=reschedule.MAX_BYTES)
    assert a.change()["phase"]=="forward_reschedule_published"
    current=host.load_receipt(a.reschedule_state,max_bytes=reschedule.MAX_BYTES)
    assert {k:v for k,v in current.items() if k!="phase"}=={k:v for k,v in original.items() if k!="phase"}
    assert a.actions.count("apply")==1
    assert all(p.read_bytes()==data for p,data in a.protected.items())


@pytest.mark.parametrize("fault",["running_worker","foreign_request","progress","expired","descriptor","capacity","final"])
def test_reschedule_rejects_changed_ownership_scope_and_expired_admission(rescheduling,monkeypatch,fault):
    a=rescheduling
    if fault=="running_worker":monkeypatch.setattr(publication,"_retired",lambda _:(_ for _ in ()).throw(RuntimeError("worker live")))
    elif fault=="foreign_request":(a.root/publication.REQUEST).write_bytes(b'{"foreign":true}\n')
    elif fault=="progress":a.sql["initialization"]["binding"]["request_sha256"]="f"*64
    elif fault=="expired":_advance(a,60*3600)
    elif fault=="descriptor":
        a.reschedule_package["descriptor_limit"]+=1;write(a.reschedule_file,a.reschedule_package)
    elif fault=="capacity":monkeypatch.setattr(publication,"_capacity",lambda *a,**k:(_ for _ in ()).throw(RuntimeError("capacity insufficient")))
    else:write(a.root/"storage-online-final.json",{"already":"held"})
    with pytest.raises((ValueError,RuntimeError)):a.change()
    assert "apply" not in a.actions
    assert not a.reschedule_state.exists()
    assert all(p.read_bytes()==data for p,data in a.protected.items())


def test_uncertain_uncommitted_reschedule_never_replays_apply(rescheduling,monkeypatch):
    a=rescheduling
    def uncertain(*args,**kwargs):
        if kwargs["action"]=="apply":
            a.actions.append("apply");raise TimeoutError("unknown SQL transport")
        return a.reschedule_probe(*args,**kwargs)
    with monkeypatch.context() as lost:
        lost.setattr(reschedule,"_probe",uncertain)
        with pytest.raises(TimeoutError):a.change()
    with pytest.raises(RuntimeError,match="outcome_unresolved_no_replay"):a.change()
    assert a.actions.count("apply")==1 and a.sql==a.old_sql


@pytest.mark.parametrize("field", ["started_at", "started_monotonic", "started_boot",
    "key_deadline", "key_deadline_monotonic", "key_deadline_boot", "deadline", "boot_id"])
def test_completed_reschedule_rejects_changed_launch_clocks(rescheduling,field):
    a=rescheduling
    a.change()
    path=a.root/forward.operation_file(forward.LAUNCH_STATE,request=a.request)
    saved=host.load_receipt(path,max_bytes=reschedule.MAX_BYTES)
    saved[field]=saved[field]+60 if field!="boot_id" else "changed-boot"
    write(path,saved)
    with pytest.raises(RuntimeError,match="reschedule_launch_changed"):
        forward.inspect_published_operation(a.root)


def test_interrupted_reschedule_keeps_original_publication_deadline(rescheduling,monkeypatch):
    a=rescheduling
    save=host.save_receipt
    def lose(path,value,**kwargs):
        save(path,value,**kwargs)
        if path==a.reschedule_state and value["phase"]=="prepared":raise TimeoutError("lost intent")
    with monkeypatch.context() as lost:
        lost.setattr(host,"save_receipt",lose)
        with pytest.raises(TimeoutError):a.change()
    retained=a.reschedule_state.read_bytes()
    _advance(a,301)
    with pytest.raises(RuntimeError):a.change()
    assert a.reschedule_state.read_bytes()==retained and "apply" not in a.actions


@pytest.mark.parametrize("rescheduling", [2], indirect=True)
@pytest.mark.parametrize("fault", ["previous_hash", "same_bound", "unbounded", "same_day", "fewer_objects", "old_forecast", "wrong_predecessor"])
def test_extension_requires_exact_predecessor_and_new_capacity_bound(rescheduling,fault):
    a = rescheduling
    if fault == "previous_hash": a.reschedule_package["previous_amendment_sha256"] = "f"*64
    elif fault == "same_bound": a.reschedule_package["attempt_seconds"] = a.old_sql["capture"]["attempt_seconds"]
    elif fault == "unbounded": a.reschedule_package["attempt_seconds"] = 345601
    elif fault == "same_day": a.reschedule_package["end_day"] = a.request["forward_reschedule"]["end_day"]
    elif fault == "fewer_objects": a.reschedule_package["max_objects"] -= 1
    elif fault == "old_forecast":
        value = host.load_receipt(a.capacity_path)
        value["through_epoch"] -= 86400
        write(a.capacity_path,value)
    else:
        path = a.root/reschedule.state_file(a.request["forward"]["operation_sha256"])
        value = host.load_receipt(path,max_bytes=reschedule.MAX_BYTES)
        value["schema_version"] = "qt.storage_online_forward_reschedule_publication.v2"
        write(path,value)
    write(a.reschedule_file,a.reschedule_package)
    with pytest.raises((ValueError,RuntimeError)):
        a.change()
    assert "apply" not in a.actions and not a.reschedule_state.exists()


@pytest.mark.parametrize("rescheduling", [2], indirect=True)
def test_completed_extension_outlives_old_expiry_without_resetting_starts(rescheduling):
    a = rescheduling
    a.change()
    _advance(a,datetime.fromisoformat(a.old_sql["capture"]["expires_at"]).timestamp()+1-a.clock[0])
    selected = forward.inspect_published_operation(a.root)
    started = forward.launch_intent(a.root,selected,selected["new_worker"]["binding"])
    assert started["started_at"] == a.intent["started_at"]
    assert started["initialization"]["started_at"] == a.intent["initialization"]["started_at"]
    assert forward.admit_startup(a.root,started,selected["new_request"],keys=started["keys"],
        initialization=started["initialization"],capture=started["capture"]) == pytest.approx(86400-1)
    with pytest.raises(RuntimeError,match="request_changed"):
        forward.inspect_published_operation(a.root,request=a.request)
    assert a.actions.count("apply") == 1


@pytest.mark.parametrize("option", ["forward_package_file", "prepare_forward_keys_file", "place_forward_lookups_file",
    "cancel_attempt_file", "replacement_package_file", "extend_attempt_seconds", "capacity_file"])
def test_reschedule_is_separate_from_other_operations(tmp_path,option):
    with pytest.raises(ValueError,match="reschedule_must_be_separate"):
        operation.run_operation_plan(tmp_path/"absent.json",reschedule_forward_file="amendment.json",
            **{option:30 if option=="extend_attempt_seconds" else "other.json"})


def test_reschedule_cli_uses_local_operator_without_http(monkeypatch,tmp_path):
    from cli import main
    monkeypatch.setattr(main,"_client",lambda _:pytest.fail("HTTP client opened"))
    calls=[]
    monkeypatch.setattr(operation,"run_operation_plan",lambda path,**kw:calls.append((path,kw)) or {})
    path,package=tmp_path/"operation.json",tmp_path/"reschedule.json"
    for execute in (False,True):
        args=main.build_parser().parse_args(["storage","migrate","--operation-file",str(path),
            "--reschedule-forward-file",str(package),*(["--execute"] if execute else [])])
        assert args.func(args)==0
    assert calls==[(str(path),dict(execute=value,reschedule_forward_file=str(package))) for value in (False,True)]

"""Host launch admission and interruption tests with disposable inputs."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.automation import storage_online_launch as launch


def _inputs(tmp_path):
    for name in ("pg", "history", "working", "udev", "state"):
        (tmp_path/name).mkdir()
    (tmp_path/"working"/"objects").mkdir()
    inventory = tmp_path/"state"/"inventory.json"
    request = tmp_path/"state"/"storage-online-request.json"
    inventory.write_text("{}")
    request.write_text("{}")
    database = {"mounts": [
        {"Type": "volume", "Name": "owned-pg", "Source": str(tmp_path/"pg"),
         "Destination": "/var/lib/postgresql/data", "RW": True},
        {"Type": "bind", "Source": str(tmp_path/"history"),
         "Destination": "/qt-history", "RW": True},
    ]}
    collector = {"mounts": [
        {"Type": "bind", "Source": str(tmp_path/"working"),
         "Destination": "/app/logs/market-structure", "RW": True}]}
    return database, collector, inventory, request


def _contract_fixture(tmp_path):
    database, collector, inventory, request = _inputs(tmp_path)
    mounts, _ = launch._explicit_mounts(database, collector, inventory, request, tmp_path/"udev")
    identity, peer, image = "a"*64, "b"*64, "sha256:"+"c"*64
    binding = dict(image=image, database_id=peer, database_hostname="original-db",
        request_sha256="d"*64, mounts=mounts, descriptor_limit=2048,
        memory_bytes=2*1024**3, environment_sha256=launch.held._digest(["BOUND=1"]))
    config = dict(User="0:0", Entrypoint=["python"], Cmd=launch._COMMAND,
                  OpenStdin=True, Tty=False, Hostname=identity[:12],
                  Labels={"qt.storage.online": binding["request_sha256"]}, Env=["BOUND=1"])
    host = dict(NetworkMode="container:"+peer, PidMode="container:"+peer,
        ReadonlyRootfs=True, Privileged=False, RestartPolicy={"Name":"no"}, Init=True,
        CapDrop=["ALL"], CapAdd=launch._CAPS, SecurityOpt=["no-new-privileges"],
        Memory=2*1024**3, MemorySwap=2*1024**3, NanoCpus=2*10**9, PidsLimit=128,
        Ulimits=[{"Name":"nofile","Hard":2048,"Soft":2048}], Tmpfs=launch._TMPFS)
    details = dict(id=identity, image=image, config=config, host=host, networks={},
        mounts=[dict(Type=m["type"], Source=m["host_source"], Destination=target,
                     RW=not m["readonly"]) for target,m in mounts.items()])
    return identity, binding, details


def test_explicit_mounts_exclude_keys_and_refuse_writable_source_alias(tmp_path):
    database, collector, inventory, request = _inputs(tmp_path)
    mounts, source = launch._explicit_mounts(database, collector, inventory, request, tmp_path/"udev")
    database["mounts"].append({"Type":"bind", "Source":"/private/NEVER-MOUNT",
        "Destination":"/run/recovery-secrets", "RW":False})
    with pytest.raises(RuntimeError, match="peer_mount_exposure"):
        launch._explicit_mounts(database, collector, inventory, request, tmp_path/"udev")
    database["mounts"].pop()
    assert len(mounts) == 6
    assert "NEVER-MOUNT" not in json.dumps(mounts)
    assert mounts["/app/logs/market-structure"]["readonly"]
    args = launch._arguments("owned", "sha256:"+"c"*64, "b"*64, mounts,
                             {"PG_DSN":"PRIVATE"}, 2048, 2*1024**3, "d"*64)
    assert "--volumes-from" not in args and "PRIVATE" not in " ".join(args)
    assert "--interactive" in args and "nofile=2048:2048" in args
    assert not {"CHOWN","DAC_OVERRIDE"} & set(args)
    collector["mounts"][0]["Source"] = str(tmp_path/"pg")
    with pytest.raises(RuntimeError, match="writable_source_alias"):
        launch._explicit_mounts(database, collector, inventory, request, tmp_path/"udev")


@pytest.mark.parametrize("mutation", [
    lambda d: d["host"].update(VolumesFrom=["peer"]),
    lambda d: d["host"].update(CapAdd=["DAC_OVERRIDE", "SETGID", "SETUID"]),
    lambda d: d["host"].update(MemorySwap=-1),
    lambda d: d["host"].update(Ulimits=[{"Name":"nofile","Hard":4096,"Soft":4096}]),
    lambda d: d["config"].update(Env=["BOUND=changed"]),
    lambda d: d["config"].update(Cmd=["-c","unsafe"]),
    lambda d: d["mounts"].append({"Type":"bind","Source":"/private/keys",
                                  "Destination":"/keys","RW":False}),
    lambda d: d["mounts"][2].update(RW=True),
], ids=["inheritance","capability","swap","fd-limit","environment","command","keys","writable-source"])
def test_live_container_contract_refuses_extra_authority(tmp_path, monkeypatch, mutation):
    identity, binding, details = _contract_fixture(tmp_path)
    monkeypatch.setattr(launch.held, "_database_details", lambda _: details)
    original = launch._admit(identity, binding)
    assert launch._admit(identity, binding, original) == original
    mutation(details)
    with pytest.raises(RuntimeError, match="container_binding_changed"):
        launch._admit(identity, binding, original)


def test_nested_database_mount_and_control_alias_refused(tmp_path):
    database, collector, inventory, request = _inputs(tmp_path)
    database["mounts"].append({"Type":"bind","Source":"/private/key",
        "Destination":"/var/lib/postgresql/data/private","RW":False})
    with pytest.raises(RuntimeError, match="peer_mount_exposure"):
        launch._explicit_mounts(database, collector, inventory, request, tmp_path/"udev")
    database["mounts"].pop()
    with pytest.raises(RuntimeError, match="control_mount_overlap"):
        launch._explicit_mounts(database, collector, tmp_path/"working"/"objects", request, tmp_path/"udev")


def test_interrupted_create_reuses_container_and_original_deadline(tmp_path, monkeypatch):
    database, collector, inventory, request_path = _inputs(tmp_path)
    request_path.unlink()
    state_root = tmp_path/"state"
    revision, image, worker_id = "e"*40, "sha256:"+"c"*64, "a"*64
    (state_root/"release.env").write_text("current_revision="+revision+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(tmp_path/"udev"))
    rows = {service: {"id": str(i+1)*64, "image":"fixed", "restart":"no","running":True}
            for i,service in enumerate((*launch.held.STOP,"tsdb"))}
    database.update(config={"Env":["POSTGRES_USER=fixture","POSTGRES_PASSWORD=disposable",
                                   "POSTGRES_DB=fixture","PGDATA=/var/lib/postgresql/data"],
                            "Hostname":"db"}, host={}, image="db-image", networks={})
    collector.update(config={"Env":["PG_DSN=postgresql+psycopg2://fixture:disposable@tsdb/fixture",
                                    "QT_IMAGE_SOURCE_REVISION="+revision]},
                     host={}, image="collector-image", networks={})
    monkeypatch.setattr(launch.held,"_inventory",lambda *a,**k: deepcopy(rows))
    monkeypatch.setattr(launch.held,"_database_details",
                        lambda identity: database if identity==rows["tsdb"]["id"] else collector)
    started = datetime.now(timezone.utc)-timedelta(seconds=3)
    monkeypatch.setattr(launch.held,"_database_query",lambda *a: json.dumps(
        {"started_at":started.isoformat(),"seconds":60}))
    monkeypatch.setattr(launch, "_admit", lambda *a: "fixed-contract")
    objects=(tmp_path/"working"/"objects").stat()
    request=dict(source_revision=revision,source_tree_hash="f"*64,max_objects=128,
                 source_device=objects.st_dev,source_inode=objects.st_ino,
                 expected_started_at=started.isoformat())
    created=[]
    def docker(*args,**kwargs):
        if args[0]=="ps": return worker_id if created else ""
        if args[0]=="image": return json.dumps({"Id":image,"Config":{"Env":[
            "QT_IMAGE_SOURCE_REVISION="+revision,"QT_IMAGE_SOURCE_TREE_HASH="+"f"*64]}})
        if args[0]=="create":
            created.append(args)
            assert "disposable" not in " ".join(args)
            return worker_id
        if args[0]=="inspect":
            return json.dumps({"Running":False,"Paused":False,"Restarting":False,"OOMKilled":False})
        raise AssertionError(args)
    monkeypatch.setattr(launch.held,"_docker",docker)
    save=launch.held._save
    fail=[True]
    def interrupted(path, receipt, *, initial):
        if not initial and fail[0]:
            fail[0]=False
            raise RuntimeError("lost after create")
        save(path, receipt, initial=initial)
    monkeypatch.setattr(launch.held,"_save",interrupted)
    kwargs=dict(project="fixture",source_revision=revision,image=image,request=request,
                inventory_path=inventory,descriptor_limit=1024,memory_bytes=2*1024**3)
    with pytest.raises(RuntimeError,match="lost after create"):
        with launch.launched_online_worker(state_root,**kwargs): pass
    original=json.loads((state_root/launch._STATE).read_text())
    assert original["container_id"] is None and len(created)==1
    monkeypatch.setattr(launch.subprocess,"Popen",lambda *a,**k:
        SimpleNamespace(stdin=io.BytesIO(),stdout=io.BytesIO(),wait=lambda **kw:0))
    with launch.launched_online_worker(state_root,**kwargs) as (_,receipt):
        assert receipt["deadline"] == original["deadline"]
        assert not receipt["final_switch_authorized"]
    assert len(created)==1
    with pytest.raises(RuntimeError,match="saved_launch_changed"):
        with launch.launched_online_worker(state_root,**(kwargs|{"descriptor_limit":2048})): pass

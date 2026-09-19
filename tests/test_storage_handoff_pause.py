from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys

import pytest

from scripts.automation import storage_handoff_pause as pause

REVISION = "a" * 40
PROJECT = "qt-pause-fixture"


class Docker:
    def __init__(self, state):
        self.state = state
        self.stops = []
        self.fail_after_stop = None
        self.forced = None
        self.rows = {}
        for i, service in enumerate(pause.STOP + pause.PASSIVE, 1):
            self.rows[service] = dict(id=f"{i:064x}", image="sha256:" + f"{i:064x}",
                project=PROJECT, service=service, oneoff="False", restart="unless-stopped",
                running=True, restarting=False, paused=False, pid=i, status="running", exit_code=0, oom=False)
        self.rows["initialize"].update(running=False, pid=0, status="exited", restart="no")

    def __call__(self, action, *args, **kwargs):
        if action == "ps":
            return "\n".join(x["id"] for x in self.rows.values())
        if action == "inspect":
            return "\n".join(json.dumps(x) for x in self.rows.values())
        assert action == "stop"
        assert (self.state / pause.HOLD).exists(), "hold must precede the first stop"
        assert kwargs["timeout"] <= 330
        row = next(x for x in self.rows.values() if x["id"] == args[-1])
        self.stops.append(row["service"])
        row.update(running=False, pid=0, status="exited", exit_code=137 if row["service"] == self.forced else 0)
        if row["service"] == self.fail_after_stop:
            self.fail_after_stop = None
            raise TimeoutError("reply lost after stopping service")
        return row["id"]


@pytest.fixture
def setup(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    (state / "release.env").write_text(f"current_revision={REVISION}\n")
    docker = Docker(state)
    monkeypatch.setattr(pause, "_docker", docker)
    return state, docker


def enter(state):
    return pause.paused_storage_clients(state, project=PROJECT, source_revision=REVISION)


def test_pause_holds_existing_lock_and_keeps_database_and_telemetry_running(setup):
    state, docker = setup
    with enter(state) as receipt:
        assert receipt["phase"] == "paused"
        assert not any(docker.rows[x]["running"] for x in pause.STOP)
        assert all(docker.rows[x]["running"] for x in pause.PASSIVE)
        with (state / "deployment.lock").open("w") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert (state / pause.HOLD).stat().st_mode & 0o777 == 0o600
    prior = docker.stops[:]
    with enter(state):
        pass
    assert docker.stops == prior
    assert (state / pause.HOLD).exists()  # success is not permission to restart


def test_lost_stop_reply_can_resume_without_restarting_already_stopped_services(setup):
    state, docker = setup
    docker.fail_after_stop = "market-data-collector"
    with pytest.raises(TimeoutError):
        with enter(state):
            pytest.fail("pause must not authorize a cutover")
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "pausing"
    with enter(state):
        pass
    assert docker.stops.count("backend") == docker.stops.count("market-data-collector") == 1


def test_caller_failure_keeps_clients_stopped_and_hold_present(setup):
    state, docker = setup
    with pytest.raises(RuntimeError, match="cutover interrupted"):
        with enter(state):
            raise RuntimeError("cutover interrupted")
    assert (state / pause.HOLD).exists()
    assert not any(docker.rows[x]["running"] for x in pause.STOP)


@pytest.mark.parametrize("change", ["unexpected", "restarting", "always", "database_down", "oneoff", "missing", "oom"])
def test_unsafe_initial_state_never_stops_anything(setup, change):
    state, docker = setup
    if change == "unexpected":
        docker.rows["runtime"] = {**docker.rows["backend"], "service": "runtime", "id": "f" * 64}
    elif change == "restarting":
        docker.rows["backend"]["restarting"] = True
    elif change == "always":
        docker.rows["backend"]["restart"] = "always"
    elif change == "database_down":
        docker.rows["tsdb"].update(running=False, pid=0, status="exited")
    elif change == "oneoff":
        docker.rows["backend"]["oneoff"] = "True"
    elif change == "missing":
        del docker.rows["market-data-collector"]
    else:
        docker.rows["backend"]["oom"] = True
    with pytest.raises(RuntimeError):
        with enter(state):
            pytest.fail("unsafe source accepted")
    assert not docker.stops
    assert not (state / pause.HOLD).exists()


def test_forced_kill_is_not_accepted_as_clean_pause(setup):
    state, docker = setup
    docker.forced = "market-data-collector"
    with pytest.raises(RuntimeError, match="unclean_stop"):
        with enter(state):
            pytest.fail("SIGKILL incorrectly qualified")
    assert (state / pause.HOLD).exists()


@pytest.mark.parametrize("change", ["id", "image", "project", "source_revision", "corrupt", "duplicate", "symlink"])
def test_changed_or_corrupt_hold_cannot_resume(setup, change):
    state, docker = setup
    with enter(state):
        pass
    path = state / pause.HOLD
    receipt = json.loads(path.read_text())
    if change in ("id", "image"):
        docker.rows["backend"][change] = "e" * 64
    elif change in ("project", "source_revision"):
        receipt[change] = "other"
        path.write_text(json.dumps(receipt))
    elif change == "corrupt":
        path.write_text("{")
    elif change == "duplicate":
        path.write_text('{"phase":"paused",' + json.dumps(receipt)[1:])
    else:
        path.unlink()
        path.symlink_to(state / "absent")
    prior = docker.stops[:]
    with pytest.raises((RuntimeError, ValueError, OSError)):
        with enter(state):
            pytest.fail("changed hold accepted")
    assert docker.stops == prior
    assert os.path.lexists(path)


def test_container_created_after_backend_stops_blocks_cutover(setup, monkeypatch):
    state, docker = setup
    def boundary(action, *args, **kwargs):
        result = docker(action, *args, **kwargs)
        if action == "stop":
            docker.rows["runtime"] = {**docker.rows["backend"], "service": "runtime", "id": "f" * 64}
        return result
    monkeypatch.setattr(pause, "_docker", boundary)
    with pytest.raises(RuntimeError, match="unexpected_client"):
        with enter(state):
            pytest.fail("late runtime accepted")
    assert docker.stops == ["backend"]
    assert (state / pause.HOLD).exists()


def test_failure_to_persist_hold_never_stops_anything(setup, monkeypatch):
    state, docker = setup
    def fail(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(pause.os, "fsync", fail)
    with pytest.raises(OSError, match="disk full"):
        with enter(state):
            pytest.fail("unpersisted hold accepted")
    assert not docker.stops
    assert (state / pause.HOLD).exists()


def test_process_death_keeps_hold_and_releases_only_transient_lock(setup):
    state, docker = setup
    # Fresh interpreter avoids forking the test suite's existing worker threads.
    code = "\n".join([
        "import importlib.util, signal, sys",
        "from pathlib import Path",
        "from scripts.automation import storage_handoff_pause as pause",
        "spec = importlib.util.spec_from_file_location('pause_fixture', sys.argv[1])",
        "fixture = importlib.util.module_from_spec(spec)",
        "spec.loader.exec_module(fixture)",
        "state = Path(sys.argv[2])",
        "pause._docker = fixture.Docker(state)",
        "with fixture.enter(state):",
        "    print('READY', flush=True)",
        "    signal.pause()",
    ])
    child = subprocess.Popen([sys.executable, "-c", code, __file__, str(state)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        import select
        assert select.select([child.stdout], [], [], 10)[0]
        assert child.stdout.readline() == b"READY\n"
    finally:
        child.kill()
        child.communicate(timeout=10)
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "paused"
    with (state / "deployment.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_existing_deployment_lock_prevents_pause(setup):
    state, docker = setup
    with (state / "deployment.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="lock_busy"):
            with enter(state):
                pytest.fail("overlap accepted")
    assert not docker.stops


def test_recorded_release_and_unfinished_promotions_are_checked_before_docker(setup):
    state, docker = setup
    (state / "release.env").write_text("current_revision=wrong\n")
    with pytest.raises(RuntimeError, match="release_mismatch"):
        with enter(state):
            pass
    (state / "promotion.env").write_text("unfinished")
    with pytest.raises(RuntimeError, match="unfinished_server_operation"):
        with enter(state):
            pass
    assert not docker.stops


class DatabaseDocker(Docker):
    def __init__(self, state, history):
        import copy
        super().__init__(state)
        self.history = history
        self.original = copy.deepcopy(self.rows["tsdb"])
        self.network_id = "d" * 64
        self.operations = []
        self.interrupt = None
        self.tamper = None
        self.details = {self.original["id"]: {
            "image": self.original["image"],
            "config": {"Image": "postgres-fixture", "Env": ["POSTGRES_USER=fixture", "POSTGRES_PASSWORD=fixture-secret"],
                       "Cmd": ["postgres"], "Entrypoint": ["entrypoint"], "Hostname": "tsdb.quanttrad",
                       "Labels": {"com.docker.compose.project": PROJECT, "com.docker.compose.service": "tsdb"},
                       "Healthcheck": {"Test": pause._SOCKET_INSPECT, "Interval": 5000000000}},
            "host": {"Init": True, "NetworkMode": PROJECT+"_quanttrad", "ShmSize": 1073741824,
                     "Binds": None, "Mounts": []},
            "mounts": [{"Type": "volume", "Name": "fixture-postgres", "Source": "/fixture/postgres",
                        "Destination": "/var/lib/postgresql/data", "Driver": "local", "Mode": "rw", "RW": True, "Propagation": ""}],
            "networks": {PROJECT+"_quanttrad": {"NetworkID": self.network_id,
                        "Aliases": ["tsdb", "tsdb.quanttrad"], "IPAMConfig": None}},
        }}
        self.model = {"name": PROJECT, "services": {"tsdb": {
            "image": self.original["image"], "command": ["postgres"], "hostname": "tsdb.quanttrad",
            "init": True, "restart": "unless-stopped", "shm_size": 1073741824, "pull_policy": "never",
            "healthcheck": {"test": pause._TCP_PROBE, "interval": "5s"},
            "environment": {"POSTGRES_USER": "fixture", "POSTGRES_PASSWORD": "fixture-secret"},
            "networks": {"quanttrad": {"aliases": ["tsdb.quanttrad"]}},
            "volumes": [{"type": "volume", "source": "postgres-data", "target": "/var/lib/postgresql/data"},
                        {"type": "bind", "source": str(history), "target": "/qt-history", "bind": {"create_host_path": False}}],
        }}, "volumes": {"postgres-data": {"name": "fixture-postgres", "external": True}},
            "networks": {"quanttrad": {"name": PROJECT+"_quanttrad", "external": True}}}
        path = state / pause.DATABASE_RECIPE
        path.write_text(json.dumps(self.model)); path.chmod(0o600)

    def __call__(self, action, *args, **kwargs):
        import copy
        if action == "image":
            return json.dumps({"Id": self.original["image"], "Config": {"Env": [], "Cmd": ["postgres"], "Entrypoint": ["entrypoint"]}})
        if action == "network":
            return self.network_id
        if action == "inspect" and args[1] != pause.INSPECT:
            return json.dumps(self.details[args[-1]])
        if action == "exec":
            assert args[1:3] == ("readlink", "-f")
            return "/var/lib/postgresql/data/pgdata"
        if action == "rm":
            receipt = json.loads((self.state / pause.HOLD).read_text())
            assert receipt["database_preparation"]["source_stopped"]
            assert not self.rows["tsdb"]["running"] and args == (self.original["id"],)
            self.operations.append("remove")
            self.rows.pop("tsdb")
            if self.interrupt == "removed":
                self.interrupt = None
                raise TimeoutError("reply lost after removing stopped container")
            return self.original["id"]
        if action == "compose":
            receipt = json.loads((self.state / pause.HOLD).read_text())
            assert receipt["phase"] == "preparing_database" and receipt["database_preparation"]["source_stopped"]
            assert not any(self.rows[name]["running"] for name in pause.STOP)
            self.operations.append("create")
            assert "tsdb" not in self.rows
            identity = f"{900:064x}"
            row = {**self.original, "id": identity, "running": False, "pid": 0, "status": "created", "exit_code": 0}
            self.rows["tsdb"] = row
            details = copy.deepcopy(self.details[self.original["id"]])
            details["config"]["Healthcheck"]["Test"] = pause._TCP_INSPECT
            details["mounts"].append({"Type": "bind", "Source": str(self.history), "Destination": "/qt-history",
                                      "Mode": "rw", "RW": True, "Propagation": "rprivate"})
            details["networks"][PROJECT+"_quanttrad"]["NetworkID"] = ""  # created, not attached yet
            self.details[identity] = details
            if self.tamper:
                self.tamper(row, details)
            if self.interrupt == "created":
                self.interrupt = None
                raise TimeoutError("reply lost after create")
            return identity
        if action == "start":
            identity = args[0]
            receipt = json.loads((self.state / pause.HOLD).read_text())
            assert receipt["database_preparation"]["replacement_id"] == identity
            assert not any(self.rows[name]["running"] for name in pause.STOP)
            self.operations.append("start")
            self.rows["tsdb"].update(running=True, pid=900, status="running")
            self.details[identity]["networks"][PROJECT+"_quanttrad"]["NetworkID"] = self.network_id
            if self.interrupt == "started":
                self.interrupt = None
                raise TimeoutError("reply lost after start")
            return identity
        return super().__call__(action, *args, **kwargs)


@pytest.fixture
def database_setup(tmp_path, monkeypatch):
    state = tmp_path / "state"; state.mkdir()
    (state / "release.env").write_text(f"current_revision={REVISION}\n")
    history = tmp_path / "history"; history.mkdir()
    udev = tmp_path / "udev"; udev.mkdir()
    device = history.stat().st_dev
    (udev / f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=fixture-hdd\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    docker = DatabaseDocker(state, history)
    monkeypatch.setattr(pause, "_docker", docker)
    monkeypatch.setattr(pause, "_cluster_identifier", lambda container: "123456789")
    monkeypatch.setattr(pause, "_database_query", lambda container, sql: json.dumps({"directory": "/var/lib/postgresql/data/pgdata", "tablespaces": 0}))
    return state, docker


def enter_database(state):
    return pause.paused_storage_clients(state, project=PROJECT, source_revision=REVISION,
                                        prepare_database=True, history_uuid="fixture-hdd")


def test_database_preparation_preserves_hold_and_never_resumes_clients(database_setup):
    state, docker = database_setup
    with enter_database(state) as receipt:
        assert receipt["phase"] == "database_prepared"
        assert receipt["database_preparation"]["cluster_identifier"] == "123456789"
        assert receipt["containers"]["tsdb"]["id"] != docker.original["id"]
        assert docker.rows["tsdb"]["running"]
        assert not any(docker.rows[name]["running"] for name in pause.STOP)
    assert "fixture-secret" not in (state / pause.HOLD).read_text()
    with enter_database(state):
        pass
    assert docker.operations == ["remove", "create", "start"]
    assert (state / pause.HOLD).exists()
    with pytest.raises(RuntimeError, match="database_binding_mismatch"):
        with enter(state):
            pytest.fail("database transition must not become an ordinary pause")


@pytest.mark.parametrize("interrupt", ["stopped", "removed", "created", "started"])
def test_database_preparation_reconciles_lost_replies_without_duplicate_start(database_setup, interrupt):
    state, docker = database_setup
    if interrupt == "stopped":
        docker.fail_after_stop = "tsdb"
    else:
        docker.interrupt = interrupt
    with pytest.raises(TimeoutError):
        with enter_database(state):
            pytest.fail("injected uncertain action should escape")
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "preparing_database"
    assert not any(docker.rows[name]["running"] for name in pause.STOP)
    with enter_database(state) as receipt:
        assert receipt["phase"] == "database_prepared"
    assert docker.stops.count("tsdb") == 1
    assert docker.operations.count("start") == 1
    assert docker.operations.count("create") == 1
    assert docker.operations.count("remove") == 1


@pytest.mark.parametrize("fault", ["environment", "volume", "image", "network", "command", "oom"])
def test_changed_database_replacement_never_starts(database_setup, fault):
    state, docker = database_setup
    def tamper(row, details):
        if fault == "environment":
            details["config"]["Env"].append("PGDATA=/wrong")
        elif fault == "volume":
            details["mounts"][0]["Name"] = "wrong-volume"
        elif fault == "image":
            details["image"] = "sha256:"+"f"*64
        elif fault == "command":
            details["config"]["Cmd"] = ["other-process"]
        elif fault == "oom":
            details["host"]["OomKillDisable"] = True
        else:
            details["networks"][PROJECT+"_quanttrad"]["NetworkID"] = "f"*64
    docker.tamper = tamper
    with pytest.raises(RuntimeError, match="replacement_not_admitted"):
        with enter_database(state):
            pytest.fail("changed replacement accepted")
    assert "start" not in docker.operations
    assert (state / pause.HOLD).exists()


def test_changed_private_recipe_is_refused_on_reentry(database_setup):
    state, docker = database_setup
    docker.interrupt = "created"
    with pytest.raises(TimeoutError):
        with enter_database(state):
            pass
    path = state / pause.DATABASE_RECIPE
    model = json.loads(path.read_text())
    model["services"]["tsdb"]["environment"]["POSTGRES_PASSWORD"] = "changed"
    path.write_text(json.dumps(model))
    with pytest.raises(RuntimeError, match="recipe_changed"):
        with enter_database(state):
            pytest.fail("changed recipe accepted")
    assert "start" not in docker.operations


def test_expired_preparation_never_starts_created_database(database_setup):
    state, docker = database_setup
    docker.interrupt = "created"
    with pytest.raises(TimeoutError):
        with enter_database(state):
            pass
    path = state / pause.HOLD
    receipt = json.loads(path.read_text()); receipt["database_preparation"]["deadline"] = 1
    path.write_text(json.dumps(receipt))
    with pytest.raises(RuntimeError, match="deadline_expired"):
        with enter_database(state):
            pytest.fail("expired preparation started database")
    assert "start" not in docker.operations


def test_cluster_mismatch_keeps_all_application_clients_paused(database_setup, monkeypatch):
    state, docker = database_setup
    monkeypatch.setattr(pause, "_cluster_identifier", lambda container: "123456789" if container == docker.original["id"] else "999999999")
    with pytest.raises(RuntimeError, match="cluster_identity_changed"):
        with enter_database(state):
            pytest.fail("different database admitted")
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "preparing_database"
    assert not any(docker.rows[name]["running"] for name in pause.STOP)


def test_changed_environment_refuses_before_database_stop(database_setup):
    state, docker = database_setup
    docker.model["services"]["tsdb"]["environment"]["POSTGRES_PASSWORD"] = "different"
    (state / pause.DATABASE_RECIPE).write_text(json.dumps(docker.model))
    with pytest.raises(RuntimeError, match="settings_changed"):
        with enter_database(state):
            pytest.fail("changed environment accepted")
    assert "tsdb" not in docker.stops
    assert not docker.operations


def test_wrong_hdd_uuid_refuses_before_database_stop(database_setup):
    state, docker = database_setup
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        with pause.paused_storage_clients(state, project=PROJECT, source_revision=REVISION,
                                          prepare_database=True, history_uuid="wrong-history"):
            pytest.fail("wrong history filesystem admitted")
    assert "tsdb" not in docker.stops
    assert not docker.operations


@pytest.mark.parametrize("fault", ["directory", "tablespace"])
def test_source_storage_outside_retained_volume_is_refused_before_stop(database_setup, monkeypatch, fault):
    state, docker = database_setup
    monkeypatch.setattr(pause, "_database_query", lambda container, sql: json.dumps({"directory": "/outside", "tablespaces": 1 if fault == "tablespace" else 0}))
    actual = pause._docker
    def boundary(action, *args, **kwargs):
        if action == "exec" and args[1:3] == ("readlink", "-f"):
            return "/outside"
        return actual(action, *args, **kwargs)
    monkeypatch.setattr(pause, "_docker", boundary)
    with pytest.raises(RuntimeError, match="source_tablespaces|data_directory_outside"):
        with enter_database(state):
            pytest.fail("unpreserved database files admitted")
    assert "tsdb" not in docker.stops
    assert not docker.operations


@pytest.fixture
def operator_setup(database_setup, tmp_path, monkeypatch):
    import copy
    state, database = database_setup
    working = tmp_path/"working"; working.mkdir()
    inventory = tmp_path/"inventory.json"
    inventory.write_text(json.dumps({"schema_version":"qt.storage_inventory.v1","targets":[
        dict(target_id="ssd",label="Recent",filesystem_uuid="fixture-ssd",root="/var/lib/postgresql/data",medium="ssd"),
        dict(target_id="hdd",label="History",filesystem_uuid="fixture-hdd",root="/qt-history",medium="hdd")]}))
    database.details[database.original["id"]]["config"]["Env"].append("POSTGRES_DB=fixture")
    database.model["services"]["tsdb"]["environment"]["POSTGRES_DB"]="fixture"
    (state/pause.DATABASE_RECIPE).write_text(json.dumps(database.model))
    collector_id=database.rows["market-data-collector"]["id"]
    database.details[collector_id]={"config":{"Env":["PG_DSN=postgresql+psycopg2://fixture:fixture-secret@tsdb:5432/fixture"]},
        "mounts":[dict(Type="bind",Source=str(working),Destination="/app/logs/market-structure",RW=True)]}
    source_query=pause._database_query
    monkeypatch.setattr(pause,"_database_query",lambda container,sql:
        "123456789/16384" if "system_identifier::text" in sql else source_query(container,sql))
    request=dict(schema_version="qt.storage_database_operator.v1",source_revision="b"*40,
        source_tree_hash="c"*64,database_identity="123456789/16384",
        source_root="/app/logs/market-structure/objects",destination_root="/qt-history/archives/objects",
        inventory_path="/run/quanttrad/storage-inventory.json",max_duration_seconds=600)
    image="sha256:"+"e"*64
    image_env=["PATH=/usr/bin","QT_IMAGE_SOURCE_REVISION="+request["source_revision"],
        "QT_IMAGE_SOURCE_TREE_HASH="+request["source_tree_hash"]]
    class Operator:
        identity="f"*64
        detail=None
        fault=None
        running=False
        creates=0
        starts=0
        def __call__(self, action,*args,**kwargs):
            if action=="ps" and any(str(a).startswith("name=") for a in args):
                return self.identity if self.detail else ""
            if action=="ps":
                result=database(action,*args,**kwargs)
                return result+("\n"+self.identity if self.detail else "")
            if action=="image" and image in args:
                return json.dumps({"Id":image,"Config":{"Env":image_env}})
            if action=="inspect" and args[1]==pause.INSPECT:
                return "\n".join(json.dumps(row) for row in database.rows.values() if row["id"] in args[2:])
            if action=="inspect" and args[-1]==self.identity:
                if args[1]=="{{json .State}}":
                    return json.dumps(dict(Running=self.running,OOMKilled=False,Paused=False,Restarting=False))
                return json.dumps(self.detail)
            if action=="create":
                assert not any(database.rows[name]["running"] for name in pause.STOP)
                saved=json.loads((state/pause._OPERATOR_STATE).read_text())
                assert saved["container_id"] is None
                assert all("fixture-secret" not in a for a in args)
                overrides=[]
                for i,arg in enumerate(args):
                    if arg=="--env":
                        value=args[i+1]
                        overrides.append("PG_DSN="+kwargs["env"]["PG_DSN"] if value=="PG_DSN" else value)
                assert "@127.0.0.1:5432/fixture" in kwargs["env"]["PG_DSN"]
                self.detail=dict(image=image,config=dict(User="70:70",Entrypoint=["python"],Cmd=pause._OPERATOR_COMMAND,
                    Labels={"qt.storage.handoff":pause._digest(request)},Env=image_env+overrides),
                    host=dict(NetworkMode="container:"+database.rows["tsdb"]["id"],PidMode="container:"+database.rows["tsdb"]["id"],
                    ReadonlyRootfs=True,Privileged=False,RestartPolicy={"Name":"no"},Init=True,CapDrop=["ALL"],
                    SecurityOpt=["no-new-privileges"],Memory=2*1024**3,NanoCpus=2*10**9,PidsLimit=128,
                    Tmpfs={"/tmp":"rw,nosuid,nodev,size=67108864,uid=70,gid=70,mode=1770",
                           "/app/logs":"rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0750"}),
                    mounts=[dict(Type=v[0],Source=v[1],RW=v[2],Destination=k) for k,v in saved["binding"]["mounts"].items()]
                        +[dict(Type="tmpfs",Source="",RW=True,Destination=k) for k in ("/tmp","/app/logs")])
                self.creates+=1
                if self.fault=="create":
                    self.fault=None;raise TimeoutError("lost create reply")
                return self.identity
            if action=="start" and args[0]==self.identity:
                self.running=True;self.starts+=1
                if self.fault=="start":
                    self.fault=None;raise TimeoutError("lost start reply")
                return self.identity
            if action=="wait":
                self.running=False
                if self.fault=="wait":
                    self.fault=None;raise TimeoutError("lost completion reply")
                return "0"
            if action=="logs":
                return json.dumps(dict(schema_version="qt.storage_database_operator_result.v1",
                    request_sha256=pause._digest(request),database_identity=request["database_identity"],
                    database_sequence_complete=True,source_preserved=True,policy_current=True,
                    plan_id="fixture",collection_resume_authorized=False,runtime_activation_required=True))
            return database(action,*args,**kwargs)
    operator=Operator()
    monkeypatch.setattr(pause,"_docker",operator)
    options=dict(project=PROJECT,source_revision=REVISION,history_uuid="fixture-hdd",image=image,
                 request=request,inventory_path=inventory)
    return state,database,operator,options


@pytest.mark.parametrize("fault",["create","start","wait"])
def test_held_database_operator_recovers_its_exact_process_without_resuming_clients(operator_setup,fault):
    state,database,operator,options=operator_setup
    operator.fault=fault
    with pytest.raises(TimeoutError):
        pause.run_held_database_handoff(state,**options)
    assert (state/pause.HOLD).exists()
    assert not any(database.rows[name]["running"] for name in pause.STOP)
    assert "fixture-secret" not in (state/pause._OPERATOR_STATE).read_text()
    result=pause.run_held_database_handoff(state,**options)
    assert result["database_sequence_complete"] and not result["collection_resume_authorized"]
    assert operator.creates==1
    assert operator.starts==(2 if fault=="wait" else 1)
    assert (state/pause.HOLD).exists()
    assert not any(database.rows[name]["running"] for name in pause.STOP)


def test_held_database_operator_refuses_changed_mount_before_reentry(operator_setup):
    state,database,operator,options=operator_setup
    operator.fault="create"
    with pytest.raises(TimeoutError):
        pause.run_held_database_handoff(state,**options)
    operator.detail["mounts"][0]["Source"]="/wrong-source"
    with pytest.raises(RuntimeError,match="operator_container_changed"):
        pause.run_held_database_handoff(state,**options)
    assert operator.starts==0 and (state/pause.HOLD).exists()


def test_held_database_operator_refuses_changed_request_after_commit(operator_setup):
    state,database,operator,options=operator_setup
    pause.run_held_database_handoff(state,**options)
    changed={**options,"request":{**options["request"],"max_duration_seconds":601}}
    with pytest.raises(RuntimeError,match="saved_binding_changed"):
        pause.run_held_database_handoff(state,**changed)
    assert operator.starts==1 and (state/pause.HOLD).exists()

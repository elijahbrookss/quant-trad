from copy import deepcopy
from hashlib import sha256
from subprocess import TimeoutExpired

import pytest

from scripts.automation import canonical_cold_runtime as runtime
from tests.test_canonical_cold_host_inventory import _exit, _running, host_scope  # noqa: F401


REVISION = "d" * 40
TREE = "9" * 64
NEW_IMAGE = "sha256:" + "e" * 64


@pytest.fixture
def runtime_scope(host_scope, monkeypatch):
    root, fake, inputs = host_scope
    old = runtime.inventory.inspect_host_inventory(root, **inputs)
    stopped = {name for name, row in old["services"].items()
               if row["running"] and name in runtime.inventory.STOPPABLE_SERVICES}
    for name in stopped:
        _exit(fake["rows"][name], 0)
    wire = {"name": old["project"], "services": {name: {} for name in runtime.REPLACE}}
    admission = {"revision": REVISION, "source_tree_hash": TREE, "protected_mounts": ["/data"], "bind_identity": {},
                 "wire_model_hash": runtime.host.digest(wire)}
    calls, journal = [], []
    inspect_docker = runtime.host.docker
    fault = {"service": None, "lose_reply": None}

    def mutate(*args, **kwargs):
        if args[0] not in ("rm", "start", "compose"):
            return inspect_docker(*args, **kwargs)
        assert journal[-1][0] == "runtime_dispatching"
        action, name = journal[-1][1]["action"].split(":")
        calls.append((action, name))
        if action == "remove":
            assert args == ("rm", old["services"][name]["id"])
            del fake["rows"][name]
        elif action == "create":
            assert "tsdb" not in args and args[-1] == name
            row = deepcopy(fake["original"][name])
            row["id"] = "f" + row["id"][1:]
            row["image"] = NEW_IMAGE
            row["config"]["Env"] = [
                item for item in row["config"]["Env"] if not item.startswith("QT_IMAGE_SOURCE_")
            ] + [f"QT_IMAGE_SOURCE_REVISION={REVISION}", f"QT_IMAGE_SOURCE_TREE_HASH={TREE}"]
            _exit(row)
            row["state"]["Status"] = "created"
            fake["rows"][name] = row
        elif fault["service"] == name:
            _exit(fake["rows"][name], 1)
        elif name == "initialize":
            _exit(fake["rows"][name], 0)
        else:
            fake["rows"][name]["state"] = _running(90)
        if action == "start" and fault["lose_reply"] == name:
            raise TimeoutExpired("fixture-docker-start", 1)
        return ""

    fake["original"] = deepcopy(fake["rows"])
    monkeypatch.setattr(runtime.host, "docker", mutate)
    monkeypatch.setattr(runtime, "_candidate_observation", lambda *args: None)
    arguments = {
        "state_root": root, "source": old, "model": wire, "admission": admission, "stopped_services": stopped,
        "remaining": lambda: 30, "record": lambda phase, evidence: journal.append((phase, evidence)),
        "verify_ready": lambda services: {"ready": services["market-data-collector"]["running"]},
    }
    return arguments, calls, journal, fault


def test_replacement_never_restarts_postgres_and_reconciles_lost_start_reply(runtime_scope):
    arguments, calls, journal, fault = runtime_scope
    fault["lose_reply"] = "market-data-collector"
    assert runtime.restart_runtime(**arguments) == {"ready": True}
    assert calls.count(("start", "market-data-collector")) == 1
    assert all(name != "tsdb" for _, name in calls)
    assert [name for action, name in calls if action == "start"] == [*runtime.REPLACE, "grafana", "pgadmin"]
    assert journal[-1][0] == "runtime_verified"


def test_failed_candidate_start_leaves_journal_and_does_not_start_collectors(runtime_scope):
    arguments, calls, journal, fault = runtime_scope
    fault["service"] = "backend"
    with pytest.raises(RuntimeError, match="canonical_host_service_start_failed"):
        runtime.restart_runtime(**arguments)
    assert journal[-1][0] == "runtime_dispatching"
    assert calls[-1] == ("start", "backend")
    assert ("start", "market-data-collector") not in calls


def test_original_outcome_restarts_only_same_containers(runtime_scope):
    arguments, calls, journal, _ = runtime_scope
    assert runtime.restart_runtime(**{**arguments, "model": None, "admission": None}) == {"ready": True}
    assert {name for _, name in calls} == arguments["stopped_services"]
    assert all(action == "start" for action, _ in calls)
    assert journal[-1][0] == "runtime_verified"


def test_runtime_admission_pins_source_mounts_and_escapes_literal_compose_values(host_scope, monkeypatch, tmp_path):
    root, fake, parameters = host_scope
    for row in fake["rows"].values():
        for mount in row["mounts"]:
            mount["Source"] = str(tmp_path)
        if row["config"].get("Cmd", [""])[-1] != runtime.inventory._APP_COMMANDS["storage-maintenance"]:
            unique = tmp_path / "working"
            unique.mkdir(exist_ok=True)
            for mount in row["mounts"]:
                if mount["Destination"] == "/app/logs/market-structure":
                    mount["Source"] = str(unique)
    fake["rows"]["initialize"]["host"]["RestartPolicy"]["Name"] = "no"
    old = runtime.inventory.inspect_host_inventory(root, **parameters)
    details = {row["id"]: row for row in fake["rows"].values()}
    monkeypatch.setattr(runtime.host, "database_details", details.__getitem__)
    monkeypatch.setattr(runtime.host, "docker", lambda *args, **kwargs: (
        '{"Id":"' + NEW_IMAGE + '","Config":{"Env":["QT_IMAGE_SOURCE_REVISION=' + REVISION
        + '","QT_IMAGE_SOURCE_TREE_HASH=' + TREE + '"]}}'
    ))
    monkeypatch.setattr(runtime, "_compose_hashes", lambda model: {name: "a" * 64 for name in model["services"]})
    services = {}
    for name in runtime.REPLACE:
        prior = fake["rows"][name]
        env = {key: value for key, value in runtime._environment(prior["config"]["Env"]).items()
               if not key.startswith("QT_IMAGE_SOURCE_")}
        services[name] = {
            "image": NEW_IMAGE, "pull_policy": "never", "command": prior["config"].get("Cmd"),
            "restart": prior["host"]["RestartPolicy"]["Name"], "environment": env, "networks": {"quanttrad": None},
            "volumes": [{"type": item["Type"], "source": item["Source"], "target": item["Destination"]}
                        for item in prior["mounts"]],
        }
    services["storage-maintenance"]["pid"] = "service:tsdb"
    services["frontend"]["environment"]["PRIVATE_LITERAL"] = "a$$b"
    model = {"name": old["project"], "services": services, "volumes": {},
             "networks": {"quanttrad": {"name": old["project"] + "_quanttrad", "external": True}}}
    wire, admitted = runtime.prepare_runtime(model, source=old)
    assert wire["services"]["frontend"]["environment"]["PRIVATE_LITERAL"] == "a$$$$b"
    assert admitted["wire_model_hash"] == runtime.host.digest(wire)
    assert wire["services"]["storage-maintenance"]["pid"] == "container:" + old["services"]["tsdb"]["id"]
    services["market-data-collector"]["volumes"][0]["read_only"] = True
    with pytest.raises(RuntimeError, match="mounts_changed"):
        runtime.prepare_runtime(model, source=old)


def test_runtime_release_record_removes_automatic_previous_image_and_preserves_layout(tmp_path):
    path = tmp_path / "release.env"
    old_revision = "1" * 40
    old = f"current_revision={old_revision}\nprevious_revision={'2' * 40}\nstorage_layout=ssd-hdd-v1\n".encode()
    path.write_bytes(old)
    path.chmod(0o600)
    source = {"source_revision": old_revision, "terminal_chain": {"release_sha256": sha256(old).hexdigest()}}
    journal = []

    def record(phase, evidence):
        if phase == "runtime_recording_release":
            assert path.read_bytes() == old
        journal.append((phase, evidence))

    runtime._record_serving_release(
        tmp_path, source=source, admission={"revision": REVISION, "source_tree_hash": TREE},
        observed_at="2026-11-14T19:33:00+00:00", record=record,
    )
    assert [phase for phase, _ in journal] == ["runtime_recording_release", "runtime_release_recorded"]
    fields = runtime._release_values(path.read_bytes())
    assert fields["previous_revision"] == "" and fields["current_revision"] == REVISION
    assert fields["storage_layout"] == "ssd-hdd-v1"
    assert sha256(path.read_bytes()).hexdigest() == journal[-1][1]["new_sha256"]


def test_interrupted_start_can_be_observed_without_reissuing_the_request(host_scope, monkeypatch):
    root, fake, parameters = host_scope
    source = runtime.inventory.inspect_host_inventory(root, **parameters)
    backend = fake["rows"]["backend"]
    journal = {
        "phase": "runtime_dispatching", "inventory_hash": runtime.host.digest(source),
        "runtime_evidence": {"action": "start:backend", "candidate_ids": {}},
    }
    runtime.host.save_receipt(root / "canonical-cold-hold.json", journal, initial=True)
    monkeypatch.setattr(runtime.host, "preserved_client_observations",
                        lambda ids, names: ({ids[0]: backend}, {names[0]: source["network_id"]}))
    assert runtime.observe_interrupted_start(root, source=source, model=None, admission=None, hold=journal)["status"] == "started"
    _exit(backend, 0)
    assert runtime.observe_interrupted_start(root, source=source, model=None, admission=None, hold=journal)["status"] == "unresolved"
    backend["mounts"][0]["RW"] = False
    with pytest.raises(RuntimeError, match="observed_binding_changed"):
        runtime.observe_interrupted_start(root, source=source, model=None, admission=None, hold=journal)

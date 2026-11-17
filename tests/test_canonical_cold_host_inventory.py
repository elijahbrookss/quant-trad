from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts.automation import canonical_cold_host_inventory as inventory


REVISION = "a" * 40
TREE = "b" * 64
NETWORK = inventory.PROJECT + "_quanttrad"
NETWORK_ID = "f" * 64
WORKER_ID = "e" * 64
SECRET = "signed-secret-must-not-appear"


def _running(index):
    return {"Running": True, "Paused": False, "Restarting": False, "OOMKilled": False, "Dead": False,
            "Status": "running", "Pid": 100 + index, "ExitCode": 0, "StartedAt": "test-start",
            "Health": {"Status": "healthy"}}


def _exit(row, code=0):
    row["state"].update(Running=False, Status="exited", Pid=0, ExitCode=code)
    row["state"].pop("Health", None)


def _fixture_service(name, index):
    labels = {"com.docker.compose.project": inventory.PROJECT, "com.docker.compose.service": name,
              "com.docker.compose.oneoff": "False"}
    mounts = []
    env = [f"PG_DSN={SECRET}", f"QT_IMAGE_SOURCE_REVISION={REVISION}", f"QT_IMAGE_SOURCE_TREE_HASH={TREE}"]
    config = {"Labels": labels, "Env": env}
    if name in inventory._APP_COMMANDS:
        config["Cmd"] = ["python", "-m", inventory._APP_COMMANDS[name]]
        mounts.append({"Destination": "/qt-history", "Source": f"/history/{SECRET}", "Type": "bind", "RW": True})
        env.append("MARKET_STRUCTURE_STORAGE_ROOT=/qt-history/archives")
        if name == "storage-maintenance":
            env.append("MARKET_STRUCTURE_WORKING_ROOT=/qt-history/archives")
        else:
            env.append("MARKET_STRUCTURE_WORKING_ROOT=/app/logs/market-structure")
            mounts.append({"Destination": "/app/logs/market-structure", "Source": f"/working/{SECRET}",
                           "Type": "bind", "RW": True})
    row = {"id": f"{index:064x}", "name": f"/{inventory.PROJECT}-{name}-1", "image": "sha256:" + "c" * 64,
           "state": _running(index), "config": config, "mounts": mounts,
           "host": {"RestartPolicy": {"Name": "unless-stopped"}},
           "networks": {NETWORK: {"NetworkID": NETWORK_ID, "Aliases": [name], "IPAMConfig": None}}}
    if name == "initialize":
        _exit(row)
    return row


@pytest.fixture
def host_scope(tmp_path, monkeypatch):
    rows = {name: _fixture_service(name, index) for index, name in enumerate(inventory.SERVICE_ROSTER, 1)}
    bot = _fixture_service("backend", 90)
    bot["name"] = "/quant-trad-bots-opaque-id-run-id"
    bot["config"]["Labels"].pop("com.docker.compose.oneoff")
    bot["config"]["Labels"].update({"quanttrad.runtime": "bot", "quanttrad.bot_id": "opaque-id",
                                      "quanttrad.run_id": "run-id", "loki.service": "bot-runtime"})
    bot["config"]["Cmd"] = ["python", "-m", "portal.backend.service.bots.container_runtime"]
    bot["host"]["RestartPolicy"]["Name"] = "no"
    _exit(bot, 1)
    chain = {"source_tree_hash": TREE, "previous_release_status": "deployed"}
    fake = {"rows": rows, "bot": bot, "chain": chain}

    def docker(*arguments, **_):
        values = [*rows.values(), *(fake[name] for name in ("bot", "unknown") if name in fake)]
        if arguments[0] == "ps":
            return "\n".join(row["id"] for row in values)
        if arguments[0] == "network":
            return NETWORK_ID
        assert arguments[:3] == ("inspect", "--format", inventory._DETAILS)
        assert set(arguments[3:]) == {row["id"] for row in values}
        return "\n".join(json.dumps(row) for row in values)

    monkeypatch.setattr(inventory.host, "docker", docker)
    monkeypatch.setattr(inventory.prior, "_admit_mount_writers", lambda rows, **kwargs: None)
    monkeypatch.setattr(inventory, "_terminal_chain", lambda *args: (deepcopy(chain), WORKER_ID))
    parameters = {"project": inventory.PROJECT, "expected_source_revision": REVISION}
    return tmp_path, fake, parameters


def test_serving_preflight_and_exact_stop_subset_hide_secrets(host_scope):
    root, fake, parameters = host_scope
    observed = inventory.inspect_host_inventory(root, **parameters)
    assert observed["exited_bot_count"] == 1
    assert set(observed["services"]) == set(inventory.SERVICE_ROSTER)
    assert observed["services"]["storage-maintenance"]["health"] == "healthy"
    assert SECRET not in json.dumps(observed)
    _exit(fake["rows"]["market-data-collector"], 143)
    collector = fake["rows"]["market-data-collector"]
    collector["networks"][NETWORK]["NetworkID"] = ""
    with pytest.raises(RuntimeError, match="not_healthy"):
        inventory.assert_host_inventory(root, expected=observed)
    actual = inventory.assert_host_inventory(root, expected=observed, stopped_services=("market-data-collector",))
    assert actual["services"]["market-data-collector"]["running"] is False
    collector["state"] = _running(876)
    collector["networks"][NETWORK]["NetworkID"] = NETWORK_ID
    actual = inventory.assert_host_inventory(root, expected=observed)
    assert actual["services"]["market-data-collector"]["pid"] == 976
    collector["config"]["Env"].append("CHANGED=1")
    with pytest.raises(RuntimeError, match="approved_inventory_changed"):
        inventory.assert_host_inventory(root, expected=observed)


def test_unapproved_writer_and_oneoff_refuse_inventory(host_scope):
    root, fake, parameters = host_scope
    fake["bot"]["state"] = _running(91)
    with pytest.raises(RuntimeError, match="unknown_container_or_writer"):
        inventory.inspect_host_inventory(root, **parameters)
    _exit(fake["bot"])
    fake["unknown"] = _fixture_service("backend", 92)
    fake["unknown"]["config"]["Labels"]["com.docker.compose.oneoff"] = "True"
    with pytest.raises(RuntimeError, match="unknown_container_or_writer"):
        inventory.inspect_host_inventory(root, **parameters)


def test_stopped_passive_service_state_is_preserved(host_scope):
    root, fake, parameters = host_scope
    _exit(fake["rows"]["docker-events"])
    observed = inventory.inspect_host_inventory(root, **parameters)
    assert observed["services"]["docker-events"]["running"] is False
    fake["rows"]["docker-events"]["state"] = _running(90)
    with pytest.raises(RuntimeError, match="approved_process_state_changed"):
        inventory.assert_host_inventory(root, expected=observed)


def test_prior_hold_and_release_must_stay_terminal(tmp_path, monkeypatch):
    preparation = {"phase": "serving", "project": inventory.PROJECT, "source_revision": "1" * 40}
    hold = {"phase": "database_prepared", "project": inventory.PROJECT, "source_revision": "1" * 40}
    worker = {"container_id": WORKER_ID}
    preparation["hold_sha256"] = inventory.host.digest(hold)
    saved = {"phase": "recovery_runtime_ready", "release": {"status": "deployed"}, "binding": {
        "project": inventory.PROJECT, "source_revision": preparation["source_revision"], "worker_id": WORKER_ID,
        "worker_sha256": inventory.host.digest(worker), "preparation_sha256": inventory.host.digest(preparation),
    }}
    receipts = {"storage-online-preparation.json": preparation, inventory.host.HOLD: hold,
                "storage-online-worker.json": worker}
    monkeypatch.setattr(inventory.prior, "_load", lambda *_: saved)
    monkeypatch.setattr(inventory.host, "load_receipt", lambda path: receipts[Path(path).name])
    raw = (f"current_revision={REVISION}\ncurrent_source_tree_hash={TREE}\nprevious_revision=\n"
           "deployed_at=once\nstorage_layout=ssd-hdd-v1\n").encode()
    monkeypatch.setattr(inventory.release, "read_private_environment", lambda *_: raw)
    chain, worker_id = inventory._terminal_chain(tmp_path, inventory.PROJECT, REVISION)
    assert worker_id == WORKER_ID and chain["initial_hold_sha256"] == inventory.host.digest(hold)
    saved["release"]["status"] = "published"
    with pytest.raises(RuntimeError, match="previous_release_not_terminal"):
        inventory._terminal_chain(tmp_path, inventory.PROJECT, REVISION)
    saved["release"]["status"] = "deployed"
    hold["phase"] = "paused"
    with pytest.raises(RuntimeError, match="initial_hold_changed"):
        inventory._terminal_chain(tmp_path, inventory.PROJECT, REVISION)

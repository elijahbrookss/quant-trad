import os
from pathlib import Path
from time import time

import pytest

from scripts.automation import canonical_cold_host_hold as hold
from scripts.automation import storage_host_boundary as host


@pytest.fixture
def authorized_hold(tmp_path, monkeypatch):
    services = {name: {"running": True, "id": f"{index:064x}"}
                for index, name in enumerate(hold.PAUSE_ORDER, 1)}
    services["initialize"] = {"running": False, "id": "f" * 64}
    inventory = {"project": "cold-fixture", "services": services}
    stopped = []

    def observe(state_root, *, expected, stopped_services):
        assert state_root == tmp_path and expected == inventory and stopped_services == tuple(stopped)

    def docker(action, flag, grace, identity, *, timeout):
        assert (action, flag) == ("stop", "--time")
        name = next(name for name, record in services.items() if record["id"] == identity)
        assert host.load_receipt(tmp_path / hold.HOLD)["inflight"] == name
        assert 0 < int(grace) < timeout
        stopped.append(name)

    monkeypatch.setattr(host, "docker", docker)
    monkeypatch.setattr(hold, "assert_host_inventory", observe)
    monkeypatch.setattr(hold, "load_approved_package", lambda *args, **kwargs: {
        "approved_inventory_hash": host.digest(inventory), "max_outage_seconds": 900,
    })
    return {
        "state_root": tmp_path, "inventory": inventory, "package_path": tmp_path / "fixture-package.json",
        "approved_package_hash": "a" * 64, "outage_deadline": time() + 900,
    }, stopped


def test_hold_precedes_stops_and_stays_durable_after_process_owner_exits(authorized_hold):
    arguments, stopped = authorized_hold
    with hold.hold_cold_cutover(**arguments) as (_, check, record, remaining):
        check()
        assert 0 < remaining() < 900
        with pytest.raises(RuntimeError, match="deployment_lock_busy"):
            with host.deployment_lock(arguments["state_root"]):
                pytest.fail("second deployment owner admitted")
        with pytest.raises(RuntimeError, match="phase_out_of_order"):
            record("activation_committing", {})
        record("login_closing", {"backend_pid": 10})
    assert stopped == list(hold.PAUSE_ORDER)
    journal = host.load_receipt(arguments["state_root"] / hold.HOLD)
    assert journal["phase"] == "login_closing" and journal["inflight"] is None
    assert journal["database_evidence"] == {"login_closing": {"backend_pid": 10}}
    with pytest.raises(RuntimeError, match="reconciliation_required"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("replayed an already dispatched pause")


def test_unqualified_package_does_not_interrupt_collection(authorized_hold, monkeypatch):
    arguments, stopped = authorized_hold

    def fail(*args, **kwargs):
        raise RuntimeError("fixture recovery evidence unavailable")

    monkeypatch.setattr(hold, "load_approved_package", fail)
    with pytest.raises(RuntimeError, match="recovery evidence unavailable"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("recovery package refused")
    assert stopped == []
    assert not os.path.lexists(arguments["state_root"] / hold.HOLD)


def test_outage_budget_cannot_exceed_reviewed_limit(authorized_hold):
    arguments, stopped = authorized_hold
    arguments["outage_deadline"] = time() + 1200
    with pytest.raises(ValueError, match="outage_exceeds_approved_budget"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("pause exceeded approved duration")
    assert not stopped
    assert not os.path.lexists(arguments["state_root"] / hold.HOLD)


def test_short_shutdown_window_does_not_stop_the_first_service(authorized_hold):
    arguments, stopped = authorized_hold
    arguments["outage_deadline"] = time() + 60
    with pytest.raises(TimeoutError, match="insufficient_shutdown_budget"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("a partial outage would violate the collector stop budget")
    assert not stopped
    assert not os.path.lexists(arguments["state_root"] / hold.HOLD)


def test_lost_stop_ack_requires_observation_and_cannot_be_replayed(authorized_hold, monkeypatch):
    arguments, stopped = authorized_hold
    original = host.docker

    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        if stopped[-1] == "backend":
            raise TimeoutError("fixture lost response")

    monkeypatch.setattr(host, "docker", interrupted)
    with pytest.raises(TimeoutError, match="lost response"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("pause outcome remains uncertain")
    interrupted_state = host.load_receipt(arguments["state_root"] / hold.HOLD)
    assert interrupted_state["inflight"] == "backend"
    assert stopped == [*interrupted_state["stopped_services"], "backend"]
    with pytest.raises(RuntimeError, match="reconciliation_required"):
        with hold.hold_cold_cutover(**arguments):
            pytest.fail("replayed a potentially stopped client")


def test_deploy_wrapper_checks_cold_hold_before_old_terminal_grant():
    path = Path(__file__).resolve().parents[1] / "scripts" / "automation" / "server_deploy.sh"
    function = path.read_text().split("require_no_storage_handoff() {", 1)[1].split("\n}", 1)[0]
    assert function.index('test -L "$state_root/canonical-cold-hold.json"') < function.index("storage_online_pending")

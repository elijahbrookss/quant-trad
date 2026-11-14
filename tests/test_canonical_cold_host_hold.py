from contextlib import contextmanager, nullcontext
import os
from pathlib import Path
from time import time

import pytest

from market_data.archive_verification import ArchiveVerificationLimits
from scripts.automation import canonical_cold_execute as execute
from scripts.automation import canonical_cold_host_hold as hold
from scripts.automation.canonical_cold_release_guard import compatible_release_floor
from scripts.automation import storage_host_boundary as host


@pytest.fixture
def authorized_hold(tmp_path, monkeypatch):
    services = {name: {"running": True, "id": f"{index:064x}"}
                for index, name in enumerate(hold.PAUSE_ORDER, 1)}
    services["initialize"] = {"running": False, "id": "f" * 64}
    inventory = {"project": "cold-fixture", "source_revision": "a" * 40, "services": services}
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
    monkeypatch.setattr(hold, "read_approved_runtime_model", lambda *args: {})
    monkeypatch.setattr(hold, "prepare_runtime", lambda *args, **kwargs: ({}, {"revision": "b" * 40}))
    monkeypatch.setattr(hold, "load_approved_package", lambda *args, **kwargs: {
        "approved_inventory_hash": host.digest(inventory), "max_outage_seconds": 1200,
    })
    return {
        "state_root": tmp_path, "inventory": inventory, "package_path": tmp_path / "fixture-package.json",
        "approved_package_hash": "a" * 64, "outage_deadline": time() + 1200,
    }, stopped


def test_hold_precedes_stops_and_stays_durable_after_process_owner_exits(authorized_hold):
    arguments, stopped = authorized_hold
    with hold.hold_cold_cutover(**arguments) as (_, check, record, remaining, _):
        check()
        assert 0 < remaining() < 1200
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
    arguments["outage_deadline"] = time() + 1800
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


@pytest.mark.parametrize(("outcome", "floor"), [("original", ""), ("committed", "b" * 40)])
def test_completion_preserves_terminal_evidence_and_sets_correct_promotion_floor(authorized_hold, monkeypatch, outcome, floor):
    arguments, _ = authorized_hold
    root = arguments["state_root"]
    monkeypatch.setattr(hold, "_record_serving_release", lambda *args, **kwargs: None)

    def restart(*args, **kwargs):
        assert isinstance(kwargs["model"], dict) is (outcome == "committed")
        assert kwargs["verify_before_start"]({}) == {"ready": True}
        kwargs["record"]("runtime_verified", {"acceptance": {"ready": True}})
        return kwargs["verify_ready"]({})

    monkeypatch.setattr(hold, "restart_runtime", restart)
    with hold.hold_cold_cutover(**arguments) as (_, _, record, _, resume):
        for phase in ("login_closing", "jobs_stopping", "database_fenced", "activation_committing"):
            record(phase, {})
        record("activation_rolled_back" if outcome == "original" else "activation_verified", {})
        record("login_opening", {})
        record("jobs_restarting", {})
        record("database_reopened", {"outcome": outcome})
        assert resume(lambda _: {"ready": True}, verify_before_start=lambda _: {"ready": True}) == {"ready": True}
    assert not os.path.lexists(root / hold.HOLD)
    assert compatible_release_floor(root / hold.COMPLETION) == floor


@pytest.mark.parametrize("scenario", ("committed", "original", "uncertain", "missing_tail", "wrong_control", "changed_object"))
def test_execute_coordinates_one_approved_host_and_database_transition(authorized_hold, monkeypatch, scenario):
    arguments, stopped = authorized_hold
    arguments = {key: value for key, value in arguments.items() if key != "outage_deadline"}
    root = arguments["state_root"]
    events = []
    journal = root / hold.HOLD

    class Session:
        closed = invalidated = False

        def __init__(self):
            self.transaction = False

        def in_transaction(self):
            return self.transaction

        def get_execution_options(self):
            return {}

        @contextmanager
        def begin(self):
            self.transaction = True
            try:
                yield
            finally:
                self.transaction = False

        def exec_driver_sql(self, command):
            assert self.transaction and command == "SET TRANSACTION READ ONLY" and not stopped

        def scalar(self, query):
            assert str(query) == "SELECT current_database()"
            return "postgres" if scenario != "wrong_control" else "qt"

    conn, control = Session(), Session()
    bound = {"database": "qt"}
    target = {**bound, "allow_connections": True}
    scope = {"target_database": bound, "source_database": {}, "audit_set_hash": "a", "loaded_inventory_hash": "b",
             "identity_registry": {}}
    approved = {"max_outage_seconds": 1200, "source_commit_floor": 20, "scope": scope}
    monkeypatch.setattr(execute, "load_approved_package", lambda *a, **kw: approved)
    monkeypatch.setattr(execute, "read_approved_candidate_witness", lambda *a: b"approved")
    monkeypatch.setattr(execute, "validate_audit_receipts", lambda *a, **kw: scope)
    monkeypatch.setattr(execute, "_bounded_step", lambda *_: nullcontext())
    monkeypatch.setattr(execute, "read_original_watermark", lambda *a, **kw: 20)
    monkeypatch.setattr(execute, "_database", lambda *a: target)
    monkeypatch.setattr(execute, "assert_original_identity_registry", lambda *a, **kw: None)
    monkeypatch.setattr(execute, "raw_index_placement_scope", lambda *a: {})
    monkeypatch.setattr(execute, "assert_raw_mapping_index_placement", lambda *a, **kw: None)
    monkeypatch.setattr(hold, "_record_serving_release", lambda *a, **kw: None)

    class Verified:
        def assert_unchanged(self):
            self.check_budget()
            assert not stopped and not journal.exists()
            events.append("stamps")
            if scenario == "changed_object":
                raise RuntimeError(scenario)

    verified = Verified()

    def preflight(session, *, audit, store, limits, check_budget):
        assert session is conn and session.in_transaction() and not stopped and not journal.exists()
        check_budget()
        events.append("checksums")
        return verified

    monkeypatch.setattr(execute, "verify_mounted_candidate", preflight)

    def capture(remaining):
        assert remaining() > 0 and not stopped and not journal.exists()
        events.append("provider_tail")
        return None if scenario == "missing_tail" else {"spool": 4}

    def drained(session, watermark, tail):
        assert session is conn and watermark == 24 and tail == {"spool": 4}
        events.append("provider_drained")
        return "provider"

    def transition(target_conn, control_conn, *, record, verify_provider_boundary, assert_host_held,
                   verified_objects, candidate_witness, **kwargs):
        assert target_conn is conn and control_conn is control
        assert verified_objects is verified and candidate_witness == b"approved"
        assert_host_held()
        assert stopped == list(hold.PAUSE_ORDER)
        for phase in ("login_closing", "jobs_stopping", "database_fenced"):
            record(phase, {})
        assert verify_provider_boundary(conn, 24) == "provider"
        if scenario == "uncertain":
            raise RuntimeError(scenario)
        if scenario == "committed":
            record("activation_committing", {})
            record("activation_verified", {})
        else:
            record("activation_rolled_back", {})
        for phase in ("login_opening", "jobs_restarting", "database_reopened"):
            record(phase, {"outcome": scenario})
        if scenario == "original":
            raise RuntimeError(scenario)
        return {"source_commit_seq": 24}

    monkeypatch.setattr(execute, "run_fenced_transition", transition)

    def restart(*args, **kwargs):
        assert host.load_receipt(journal)["phase"] == "database_reopened"
        assert (kwargs["model"] is None) is (scenario == "original")
        if scenario == "committed":
            assert kwargs["verify_before_start"]({"before": True}) == {"ready": True}
        else:
            assert kwargs["verify_before_start"] is None
        return kwargs["verify_ready"]({"running": True})

    monkeypatch.setattr(hold, "restart_runtime", restart)

    def application(activation, services):
        assert activation["source_commit_seq"] == 24
        events.append("prestart" if "before" in services else "candidate_ready")
        return {"ready": True}

    def original(services):
        assert services == {"running": True}
        events.append("original_ready")
        return {"ready": True}

    def run():
        return execute.execute_approved_cutover(
            conn, control, **arguments, receipts=object(), store=object(), limits=ArchiveVerificationLimits(),
            preflight_seconds=60, outage_seconds=1200, capture_provider_tail=capture, verify_provider_boundary=drained,
            verify_recovery_boundary=lambda *a: None, verify_candidate=lambda *a: None,
            verify_candidate_prestart=application, verify_candidate_ready=application, verify_original_ready=original,
        )

    if scenario == "committed":
        assert run() == {"activation": {"source_commit_seq": 24}, "runtime": {"ready": True}}
        assert events == ["checksums", "stamps", "provider_tail", "provider_drained", "prestart", "candidate_ready"]
    else:
        with pytest.raises(RuntimeError, match={"missing_tail": "provider_tail_missing",
                                               "wrong_control": "control_endpoint_changed"}.get(scenario, scenario)):
            run()
    if scenario in {"original", "committed"}:
        assert not journal.exists()
        assert host.load_receipt(root / hold.COMPLETION)["runtime_evidence"]["outcome"] == scenario
        if scenario == "original":
            assert events[-1] == "original_ready"
    elif scenario == "uncertain":
        assert host.load_receipt(journal)["phase"] == "database_fenced"
        assert events == ["checksums", "stamps", "provider_tail", "provider_drained"]
    else:
        assert not stopped and not journal.exists()

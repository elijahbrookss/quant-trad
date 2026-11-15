from contextlib import contextmanager, nullcontext
import json
import os
from pathlib import Path
from time import time
from types import SimpleNamespace

import pytest

from market_data.archive_verification import ArchiveVerificationLimits
from scripts.automation import canonical_cold_commit_observer as commit_observer
from scripts.automation import canonical_cold_execute as execute
from scripts.automation import canonical_cold_host_hold as hold
from scripts.automation.canonical_cold_recovery_checks import _ApprovedRecoveryChecks
from scripts.automation.canonical_cold_release_guard import compatible_release_floor
from scripts.automation import storage_host_boundary as host
from scripts.db.canonical_cold_recovery_wal_transfer import check_source_wal_capacity


@pytest.mark.parametrize(("segment", "count", "error"), (
    ("0000000100000000000000FF", 0, None),
    ("00000001000000010000007F", 128, None),
    ("000000010000000100000080", None, "recovery_handoff_wal_range_outside_budget"),
    ("00000002000000010000007F", None, "recovery_handoff_wal_timeline_or_segment_invalid"),
))
def test_pre_stop_wal_admission_checks_exact_suffix_budget(segment, count, error):
    expected = {"database": "qt", "system_identifier": "11", "database_oid": 20, "allow_connections": True}
    row = {**expected, "control_database": "postgres", "archive_mode": "on", "segment_bytes": 16 * 1024**2,
           "segment": segment}
    calls = []

    def query(statement, bindings):
        calls.append(str(statement))
        if "set_config" in calls[-1]:
            assert bindings == {"timeout": 1000}
            return SimpleNamespace(close=lambda: None)
        assert bindings == {"name": "qt"}
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))

    control = SimpleNamespace(begin=nullcontext, exec_driver_sql=calls.append, execute=query)
    options = {"expected_database": expected, "backup_stop": "0000000100000000000000FF", "segment_bytes": 16 * 1024**2,
               "max_segments": 128, "timeout_seconds": 1}
    if error:
        with pytest.raises(ValueError, match=error):
            check_source_wal_capacity(control, **options)
    else:
        assert check_source_wal_capacity(control, **options) == count
    assert len(calls) == 3 and calls[0] == "SET TRANSACTION READ ONLY"
    assert "pg_current_wal_insert_lsn()" in calls[-1] and "pg_switch_wal" not in calls[-1]


@pytest.fixture
def authorized_hold(tmp_path, monkeypatch):
    services = {name: {"running": True, "id": f"{index:064x}"}
                for index, name in enumerate(hold.PAUSE_ORDER, 1)}
    services["initialize"] = {"running": False, "id": "f" * 64}
    services["tsdb"] = {"running": True, "id": "e" * 64}
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


def test_interrupted_activation_observes_only_reviewed_transaction(authorized_hold, monkeypatch):
    arguments, stopped = authorized_hold
    root = arguments["state_root"]
    tx = {"xid": "583", "fsync": "on", "synchronous_commit": "local", "wal_before": "0/402"}
    target = {"system_identifier": "11", "database_oid": 12, "database": "qt"}
    database = {**target, "allow_connections": True, "postmaster_started_at": "1234.5"}
    committing = {"execution_hash": "c" * 64, "source_commit_seq": 45, "provider_boundary": "d" * 64,
                  "independent_recovery": "e" * 64, "transaction": tx}
    precommit = {"execution_hash": "c" * 64, "xid": "583", "witness_hash": "f" * 64,
                 "raw_mapping_index_receipt_hash": "5" * 64, "identity_retirement": {"done": True},
                 "wal_before_commit": "0/456"}
    with hold.hold_cold_cutover(**arguments) as (_, _, record, _, _):
        for phase in ("login_closing", "jobs_stopping", "database_fenced"):
            record(phase, {"database": database, "backend_pid": 9})
        record("activation_committing", committing)
        with pytest.raises(RuntimeError, match="phase_out_of_order"):
            record("activation_verified", precommit)
        record("activation_precommit_verified", precommit)

    package = {"approved_inventory_hash": host.digest(arguments["inventory"])}
    monkeypatch.setattr(commit_observer, "load_approved_package", lambda *a, **k: package)

    def bind(value, **kwargs):
        assert value is package and kwargs["source_commit_seq"] == 45
        assert kwargs["provider_boundary"] == "d" * 64 and kwargs["independent_recovery"] == "e" * 64
        return {"target_database": target, "source_commit_seq": 45}, committing["execution_hash"]

    monkeypatch.setattr(commit_observer, "bind_execution", bind)
    sightings = []

    def host_observe(state_root, *, expected, stopped_services, timeout_seconds):
        assert state_root == root and expected is arguments["inventory"] and stopped_services == stopped
        assert 0 < timeout_seconds <= 30
        sightings.append("host")

    def sql_observe(control, *, database, backend_pid, transaction, precommit, timeout_seconds):
        assert database == {**target, "allow_connections": False, "postmaster_started_at": "1234.5"}
        assert backend_pid == 9 and transaction == tx and precommit == "0/456"
        assert 0 < timeout_seconds <= 30
        sightings.append("sql")
        return {"xid": "583", "transaction_status": "committed", "commit_flushed": True}

    monkeypatch.setattr(commit_observer, "assert_host_inventory", host_observe)
    monkeypatch.setattr(commit_observer, "observe_activation_transaction", sql_observe)
    supplied = {key: value for key, value in arguments.items() if key != "outage_deadline"}
    snapshot = (root / hold.HOLD).read_bytes()
    proof = commit_observer.observe_held_activation(object(), **supplied)
    assert sightings == ["host", "sql", "host"]
    assert proof["transaction_status"] == "committed" and proof["host_phase"] == "activation_precommit_verified"
    assert (root / hold.HOLD).read_bytes() == snapshot
    assert not (root / hold.COMPLETION).exists()

    changed = host.load_receipt(root / hold.HOLD)
    changed["database_evidence"]["activation_precommit_verified"]["xid"] = "999"
    host.save_receipt(root / hold.HOLD, changed, initial=False)
    with pytest.raises(RuntimeError, match="precommit_changed"):
        commit_observer.observe_held_activation(object(), **supplied)


def test_commit_observer_cli_connects_only_to_read_only_postgres(tmp_path, monkeypatch, capsys):
    path = tmp_path / "inventory.json"
    inventory = {"project": "test-fixture"}
    path.write_text(json.dumps(inventory))
    path.chmod(0o600)
    monkeypatch.setenv("PG_DSN", "postgresql://fixture:local@localhost/approved")
    control = object()
    disposed = []

    class FixtureEngine:
        def connect(self):
            return nullcontext(control)

        def dispose(self):
            disposed.append(True)

    def connect(url, **options):
        assert url.database == "postgres" and url.host == "localhost"
        assert options["hide_parameters"] is True
        assert options["connect_args"] == {"connect_timeout": 5, "options": "-c default_transaction_read_only=on"}
        return FixtureEngine()

    def observe(connection, **options):
        assert connection is control and options["inventory"] == inventory
        assert options["approved_package_hash"] == "b" * 64 and options["state_root"] == tmp_path
        return {"transaction_status": "aborted"}

    monkeypatch.setattr(commit_observer, "create_engine", connect)
    monkeypatch.setattr(commit_observer, "observe_held_activation", observe)
    commit_observer.main(["--state-root", str(tmp_path), "--inventory", str(path), "--package", str(tmp_path / "approved.json"),
                         "--approved-package-sha256", "b" * 64])
    assert json.loads(capsys.readouterr().out) == {"transaction_status": "aborted"}
    assert disposed == [True]


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
        if outcome == "committed":
            record("activation_precommit_verified", {})
        record("activation_rolled_back" if outcome == "original" else "activation_verified", {})
        record("login_opening", {})
        record("jobs_restarting", {})
        record("database_reopened", {"outcome": outcome})
        assert resume(lambda _: {"ready": True}, verify_before_start=lambda _: {"ready": True}) == {"ready": True}
    assert not os.path.lexists(root / hold.HOLD)
    assert compatible_release_floor(root / hold.COMPLETION) == floor


@pytest.mark.parametrize("scenario", (
    "committed", "original", "uncertain", "unchanged", "changed_postmaster", "missing_tail", "wrong_control", "changed_object",
    "changed_capacity", "unprepared_receiver", "wal_overflow",
))
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
            self.connection = SimpleNamespace(driver_connection=SimpleNamespace(autocommit=False))

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
            assert self.transaction and command == "SET TRANSACTION READ ONLY"
            assert not stopped or scenario in {"unchanged", "changed_postmaster"}

        def scalar(self, query):
            assert str(query) == "SELECT current_database()"
            return "postgres" if scenario != "wrong_control" else "qt"

    conn, control = Session(), Session()
    bound = {"database": "qt"}
    target = {**bound, "allow_connections": True}
    scope = {"target_database": bound, "source_database": {}, "audit_set_hash": "a", "loaded_inventory_hash": "b",
             "identity_registry": {}}
    approved = {"max_outage_seconds": 1200, "source_commit_floor": 20, "scope": scope,
                "approved_inventory_hash": host.digest(arguments["inventory"])}
    monkeypatch.setattr(execute, "load_approved_package", lambda *a, **kw: approved)
    monkeypatch.setattr(hold, "load_approved_package", lambda *a, **kw: approved)
    monkeypatch.setattr(execute, "read_approved_candidate_witness", lambda *a: b"approved")
    monkeypatch.setattr(execute, "validate_audit_receipts", lambda *a, **kw: scope)
    monkeypatch.setattr(execute, "_bounded_step", lambda *_: nullcontext())
    monkeypatch.setattr(execute, "read_original_watermark", lambda *a, **kw: 20)
    def database(*_):
        return {**target, "postmaster_started_at": "new"} if stopped and scenario == "changed_postmaster" else target

    monkeypatch.setattr(execute, "_database", database)
    monkeypatch.setattr(execute, "assert_original_identity_registry", lambda *a, **kw: None)
    footprint = {"history_filesystem_uuid": "fixture-hdd", "raw_index_bytes": 20}
    monkeypatch.setattr(execute, "_raw_index_headroom", lambda *a: footprint)
    monkeypatch.setattr(hold, "_record_serving_release", lambda *a, **kw: None)

    def capacity(*, timeout_seconds, **physical):
        assert physical == footprint and timeout_seconds > 0
        events.append("capacity")
        if scenario == "changed_capacity" and events.count("capacity") == 2:
            raise RuntimeError(scenario)
        return {"receipt_hash": "e" * 64}

    monkeypatch.setattr(execute, "bind_approved_capacity_checks", lambda **kwargs: capacity)

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

    def bind_provider(connection, *, package_path, package, store, receipt_root):
        assert connection is conn and package_path == arguments["package_path"]
        assert package is approved and receipt_root == root and not stopped

        def restarted(services):
            assert services == {"running": True}
            events.append("provider_restarted")
            return "d" * 64

        return capture, drained, restarted, None

    monkeypatch.setattr(execute, "bind_approved_collector_checks", bind_provider)
    def receiver_ready():
        assert not stopped and not journal.exists()
        events.append("receiver_ready")
        if scenario == "unprepared_receiver":
            raise RuntimeError(scenario)
        return "e" * 64

    def source_wal_capacity(session, *, expected_database, timeout_seconds):
        assert session is control and expected_database == target and timeout_seconds > 0
        assert not stopped and not journal.exists()
        events.append("source_wal_capacity")
        if scenario == "wal_overflow":
            raise ValueError("recovery_handoff_wal_range_outside_budget")

    monkeypatch.setattr(execute, "bind_approved_recovery_checks",
                        lambda **kwargs: _ApprovedRecoveryChecks(receiver_ready, kwargs["verify_site_restore"], source_wal_capacity))

    def transition(target_conn, control_conn, *, record, verify_provider_boundary, assert_host_held,
                   verified_objects, candidate_witness, **kwargs):
        assert target_conn is conn and control_conn is control
        assert verified_objects is verified and candidate_witness == b"approved"
        assert_host_held()
        assert stopped == list(hold.PAUSE_ORDER)
        if scenario in {"unchanged", "changed_postmaster"}:
            raise RuntimeError(scenario)
        for phase in ("login_closing", "jobs_stopping", "database_fenced"):
            record(phase, {})
        assert verify_provider_boundary(conn, 24) == "provider"
        if scenario == "uncertain":
            raise RuntimeError(scenario)
        if scenario == "committed":
            def assert_quiet(session):
                assert session is conn and session.in_transaction()
            fence = SimpleNamespace(connection=target_conn, assert_quiet=assert_quiet)
            assert kwargs["verify_recovery_boundary"](fence, control, 24, "provider", lambda: 60, lambda: None) == "recovered"
            record("activation_committing", {})
            assert host.load_receipt(journal)["database_evidence"]["activation_committing"]["physical_capacity"] == {
                "receipt_hash": "e" * 64}
            record("activation_precommit_verified", {})
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
        assert host.load_receipt(journal)["phase"] == (
            "database_unchanged" if scenario == "unchanged" else "database_reopened"
        )
        assert (kwargs["model"] is None) is (scenario in {"original", "unchanged"})
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
            conn, control, **arguments, receipts=object(), store=SimpleNamespace(root=root), limits=ArchiveVerificationLimits(),
            preflight_seconds=60, outage_seconds=1200, provider_receipt_root=root,
            verify_recovery_boundary=lambda *a: "recovered", verify_candidate=lambda *a: None,
            verify_candidate_prestart=application, verify_candidate_ready=application, verify_original_ready=original,
        )

    if scenario == "committed":
        assert run() == {"activation": {"source_commit_seq": 24},
                         "runtime": {"ready": True, "collector_restart_sha256": "d" * 64}}
        assert events == ["capacity", "checksums", "stamps", "provider_tail", "capacity", "receiver_ready",
                          "source_wal_capacity", "provider_drained", "capacity", "prestart", "candidate_ready", "provider_restarted"]
    elif scenario == "wal_overflow":
        with pytest.raises(ValueError, match="recovery_handoff_wal_range_outside_budget"):
            run()
        assert events[-2:] == ["receiver_ready", "source_wal_capacity"]
    else:
        with pytest.raises(RuntimeError, match={"missing_tail": "provider_tail_missing",
                                               "wrong_control": "control_endpoint_changed"}.get(scenario, scenario)):
            run()
    if scenario in {"original", "unchanged", "committed"}:
        assert not journal.exists()
        expected = "committed" if scenario == "committed" else "original"
        assert host.load_receipt(root / hold.COMPLETION)["runtime_evidence"]["outcome"] == expected
        if scenario != "committed":
            assert events[-1] == "original_ready"
    elif scenario == "uncertain":
        assert host.load_receipt(journal)["phase"] == "database_fenced"
        assert events == ["capacity", "checksums", "stamps", "provider_tail", "capacity", "receiver_ready",
                          "source_wal_capacity", "provider_drained"]
    elif scenario == "changed_postmaster":
        assert host.load_receipt(journal)["phase"] == "writers_stopped"
        assert events == ["capacity", "checksums", "stamps", "provider_tail", "capacity", "receiver_ready", "source_wal_capacity"]
    else:
        assert not stopped and not journal.exists()

"""Bounded pipe framing and response backpressure independent of a database."""
import json
import os
import socket
from time import monotonic
from types import SimpleNamespace

import pytest

from scripts.automation.storage_online_controller import _write, serve


class ChannelPeer:
    # Only wire framing is tested here; actual controller/SQL/proof semantics
    # are covered by the separate disposable SSD/HDD integration suite.
    def __init__(self):
        self.state = "background"
        self.proof = SimpleNamespace(deadline=monotonic()+5)
        self.calls = []

    def check(self):
        pass

    def status(self):
        return {"final_switch_authorized": False}

    def command(self, request):
        self.calls.append(request)
        self.state = "closed"
        return {"accepted": True}


@pytest.mark.parametrize("data,error", [
    (b"{" + b" " * 4096, "budget_exceeded"),
    (b'{"sequence":1,"sequence":2}\n', "duplicate_command_field"),
    (b'{}\n{}\n', "pipelining_refused"),
    (b'{}\nx', "pipelining_refused"),
    (b'{"sequence":', "truncated_command"),
], ids=["oversize", "duplicate", "pipelined", "trailing", "truncated"])
def test_channel_refuses_ambiguous_or_unbounded_frames(data, error):
    peer = ChannelPeer()
    client, server = socket.socketpair()
    with client, server:
        client.sendall(data)
        client.shutdown(socket.SHUT_WR)
        with pytest.raises(ValueError, match=error):
            serve(peer, input_fd=server.fileno(), output_fd=server.fileno(),
                  channel_seconds=0.1)
    assert not peer.calls


def test_channel_partial_frame_timeout_and_eof():
    peer = ChannelPeer()
    client, server = socket.socketpair()
    with client, server:
        client.sendall(b"{")
        with pytest.raises(RuntimeError, match="command_timeout"):
            serve(peer, input_fd=server.fileno(), output_fd=server.fileno(),
                  channel_seconds=0.02)
    assert not peer.calls
    peer = ChannelPeer()
    client, server = socket.socketpair()
    with client, server:
        client.shutdown(socket.SHUT_WR)
        serve(peer, input_fd=server.fileno(), output_fd=server.fileno())
    assert not peer.calls


def test_channel_stalled_reader_cannot_hold_proof_indefinitely():
    client, server = socket.socketpair()
    with client, server:
        server.setblocking(False)
        while True:
            try:
                server.send(b"x"*4096)
            except BlockingIOError:
                break
        server.setblocking(True)
        with pytest.raises(RuntimeError, match="reply_timeout"):
            _write(server.fileno(), {"bounded": True}, 0.02)
        assert os.get_blocking(server.fileno())


def test_channel_high_descriptor_uses_poll_without_raising_limits():
    import fcntl
    import resource
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft != resource.RLIM_INFINITY and soft <= 1100:
        pytest.skip("preexisting FD budget does not permit high-descriptor diagnostic")
    client, server = socket.socketpair()
    with client, server:
        duplicate = fcntl.fcntl(server.fileno(), fcntl.F_DUPFD_CLOEXEC, 1050)
        try:
            _write(duplicate, {"bounded": True}, 0.1)
            assert json.loads(client.recv(4096)) == {"bounded": True}
        finally:
            os.close(duplicate)


def test_spool_observation_requires_fresh_sequence_and_preserves_pending(tmp_path):
    from scripts.automation.storage_online_controller import OnlineController
    root = tmp_path/"objects";root.mkdir()
    spool = tmp_path/"spool";spool.mkdir();(spool/"pending.sealed").write_bytes(b"WAL")
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller.controller_id = "c"*32;controller.state = "background"
    controller.source_root = root;controller._sequence = 0
    controller._final_deadline = None
    controller._last_request = controller._last_reply = None
    controller._reproved = set();controller.limits = {"movement_timeout_seconds": 30}
    controller.proof = SimpleNamespace(deadline=monotonic()+60, hashed_bytes=0, check=lambda: None)
    controller.check = lambda: None
    request = dict(controller_id=controller.controller_id, sequence=1, operation="source_drain",
                   max_entries=100, deadline=monotonic()+5)
    result = controller.command(request)
    assert not result["result"]["spool_empty_at_observation"]
    assert (spool/"pending.sealed").read_bytes() == b"WAL"
    with pytest.raises(RuntimeError, match="fresh_sequence_required"):
        controller.command(request)
    for changed in ({"max_entries": 0}, {"deadline": monotonic()-1}, {"deadline": monotonic()+40},
                    {"source_root": "/arbitrary"}):
        with pytest.raises(ValueError): controller.command(request | changed | {"sequence": 2})
    assert controller.state == "background"


def test_final_tail_command_refuses_unbounded_fields_and_replays_without_work():
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller.controller_id = "c"*32;controller.state = "background"
    controller._sequence = 0;controller._final_deadline = None
    controller._last_request = controller._last_reply = None
    controller._reproved = set()
    controller._admitted_limits = {"movement_timeout_seconds": 60}
    controller.proof = SimpleNamespace(deadline=monotonic()+90, hashed_bytes=1)
    calls = []
    def copy(*, deadline):
        calls.append(deadline);controller._final_deadline = deadline
        return {"migration_ready": False}
    controller.final_delta = copy
    controller.check = lambda: None
    request = dict(controller_id=controller.controller_id, sequence=1,
                   operation="final_delta", deadline=monotonic()+20)
    for changed in ({"deadline": True}, {"deadline": float("nan")},
                    {"deadline": monotonic()-1}, {"deadline": monotonic()+80},
                    {"source_root": "/arbitrary"}, {"max_pages": 1000}):
        with pytest.raises(ValueError):controller.command(request | changed)
    assert not calls
    reply = controller.command(request)
    assert controller.command(request) == reply
    assert calls == [request["deadline"]]
    assert not reply["final_switch_authorized"]
    with pytest.raises(RuntimeError, match="background_work_refused"):
        controller.command(dict(controller_id=controller.controller_id, sequence=2,
                                operation="sql_copy"))


def test_rollback_wire_requires_the_already_bound_absolute_final_window():
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller.controller_id = "c"*32
    controller._final_deadline = monotonic()+20
    controller._admitted_limits = {"movement_timeout_seconds":30}
    request = dict(controller_id=controller.controller_id, sequence=1,
                   operation="rollback_fence_begin",deadline=controller._final_deadline)
    for changed in ({"deadline":True},{"deadline":float("nan")},
                    {"deadline":controller._final_deadline+1},
                    {"deadline":controller._final_deadline-1},{"restart":True}):
        with pytest.raises(ValueError):controller.command(request|changed)
    controller._final_deadline = None
    with pytest.raises(ValueError,match="rollback_command_deadline"):
        controller.command(request)


def test_idle_fenced_channel_obeys_original_deadline_without_new_requests():
    peer = ChannelPeer()
    peer.state = "resume_fenced"
    peer._final_deadline = monotonic()+.05
    original = peer._final_deadline
    client, server = socket.socketpair()
    with client, server:
        with pytest.raises(RuntimeError, match="command_timeout"):
            serve(peer,input_fd=server.fileno(),output_fd=server.fileno(),channel_seconds=5)
    assert monotonic() >= original
    assert peer._final_deadline == original and not peer.calls


def test_aborted_controller_only_allows_fresh_outcome_and_close(monkeypatch):
    from scripts.automation.storage_online_controller import OnlineController
    from copy import deepcopy
    controller=OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller.state="aborted";controller.controller_id="c"*32
    controller._sequence=5;controller._last_request=None
    controller._final_deadline=monotonic()+10
    controller._admitted_limits={"movement_timeout_seconds":30}
    controller.limits={"movement_timeout_seconds":5}
    controller.proof=SimpleNamespace(deadline=monotonic()+30,hashed_bytes=17)
    controller._reproved=set();controller._rollback_context=None;controller._rollback_check=None
    controller._ownership=lambda:None
    controller.check=lambda:None
    controller.inspect_outcome=lambda **kwargs:dict(outcome="uncommitted",database_handoff_committed=False,
        collection_resume_authorized=False,runtime_activation_authorized=False)
    request=dict(controller_id=controller.controller_id,sequence=6,operation="inspect_outcome",deadline=monotonic()+3)
    reply=controller.command(request)
    assert reply["state"]=="aborted" and reply["result"]["outcome"]=="uncommitted"
    with pytest.raises(RuntimeError,match="fresh_sequence_required"):controller.command(deepcopy(request))
    for operation in ("sql_copy","archive_copy","reprove","status","cancel"):
        with pytest.raises(RuntimeError,match="sequence_or_state_invalid"):
            controller.command(dict(controller_id=controller.controller_id,sequence=7,operation=operation))
    with pytest.raises(RuntimeError,match="commit_state_invalid"):
        controller.commit_database(deadline=controller._final_deadline)
    assert controller.command(dict(controller_id=controller.controller_id,sequence=7,operation="close"))["state"]=="closed"


def test_aborted_controller_cannot_outlive_original_final_window(monkeypatch):
    from scripts.automation.storage_online_controller import OnlineController
    controller=OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller.state="aborted";controller._final_deadline=monotonic()-1
    with pytest.raises(RuntimeError,match="terminal_deadline_expired"):controller.check()



def test_sql_drain_expired_owner_deadline_never_touches_database():
    from scripts.automation.storage_online_controller import OnlineController
    class Untouched:
        closed = invalidated = False
        def begin(self):
            pytest.fail("expired SQL ownership check touched database")
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection = None; controller._final_connection_entered = False
    controller._owner = Untouched()
    with pytest.raises(RuntimeError, match="sql_drain_deadline_expired"):
        controller._ownership(deadline=monotonic()-1)


@pytest.mark.parametrize("fault", ["closed", "invalidated", "transaction", "identity", "absent"])
def test_retained_final_session_loss_never_reconnects(fault):
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._final_connection_entered = True
    controller._final_pid = 17
    controller.engine = SimpleNamespace(connect=lambda: pytest.fail("reconnected lost final session"))
    controller._final_connection = SimpleNamespace(
        closed=fault == "closed", invalidated=fault == "invalidated",
        in_transaction=lambda: fault == "transaction",
        connection=SimpleNamespace(driver_connection=SimpleNamespace(
            get_backend_pid=lambda: 18 if fault == "identity" else 17)))
    if fault == "absent":
        controller._final_connection = None
    with pytest.raises(RuntimeError, match="final_session_lost"):
        with controller._database_connection():
            pytest.fail("lost final session admitted")


def test_final_session_admission_keeps_original_deadline_when_capture_shrinks():
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller.state = "background"
    controller._final_connection_entered = False
    controller._final_deadline = None
    controller._admitted_limits = {"movement_timeout_seconds": 30}
    controller.proof = SimpleNamespace(deadline=monotonic()+60)
    controller.engine = SimpleNamespace(connect=lambda: pytest.fail("opened session after capture shrink"))
    deadline = monotonic()+20
    def admission():
        assert controller._final_deadline == deadline
        controller.proof.deadline = deadline-1
    controller._admit_attempt = admission
    with pytest.raises(ValueError, match="final_session_deadline_invalid"):
        with controller.final_database_session(deadline=deadline):
            pytest.fail("capture ceiling exceeded")
    assert controller._final_deadline == deadline
    assert not controller._final_connection_entered
    with pytest.raises(ValueError, match="final_session_deadline_invalid"):
        with controller.final_database_session(deadline=deadline+1):
            pytest.fail("deadline renewed")


def test_final_session_refuses_tail_before_confirmed_job_retirement():
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller.state = "background"
    controller._final_connection_entered = True
    controller._jobs_stopped = False
    with pytest.raises(RuntimeError, match="final_delta_state_invalid"):
        controller.final_delta(deadline=monotonic()+10)


@pytest.mark.parametrize("operation", ["final_session_begin", "final_session_check", "final_session_quiesce"])
def test_final_session_wire_requires_original_deadline_and_fresh_sequence(operation):
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller.controller_id = "c"*32
    controller._final_deadline = monotonic()+20
    controller._admitted_limits = {"movement_timeout_seconds": 30}
    controller.state = "background"
    controller._sequence = 4
    controller.check = lambda: None
    request = dict(controller_id=controller.controller_id,sequence=4,operation=operation,
                   deadline=controller._final_deadline)
    controller._last_request = request.copy()
    for deadline in (True, float("nan"), controller._final_deadline+1, controller._final_deadline-1):
        with pytest.raises(ValueError, match="final_session_command_deadline_invalid"):
            controller.command(request | {"deadline":deadline})
    with pytest.raises(RuntimeError, match="final_session_fresh_sequence_required"):
        controller.command(request)


@pytest.mark.parametrize("fault", [None, "extension", "version", "preload", "gate_or_replication"])
def test_gated_job_stop_refuses_unqualified_database_environment(fault):
    from scripts.automation.storage_online_controller import OnlineController
    extensions = {"plpgsql":"1.0", "timescaledb":"2.14.2", "pgcrypto":"1.3",
                  "pg_buffercache":"1.3", "pg_stat_statements":"1.10"}
    if fault == "extension":extensions["unknown_worker"] = "1.0"
    if fault == "version":extensions["timescaledb"] = "future"
    class Database:
        def execute(self, statement):
            assert str(statement) == "SELECT extname, extversion FROM pg_extension"
            return SimpleNamespace(all=lambda:list(extensions.items()))
        def scalar(self, statement, parameters=None):
            if str(statement).startswith("SHOW"):
                return "timescaledb,unknown_worker" if fault == "preload" else "timescaledb, pg_stat_statements"
            assert parameters == {"allow_connections": False}
            return fault == "gate_or_replication"
    if fault:
        with pytest.raises(RuntimeError, match="unqualified"):
            OnlineController._require_job_environment(Database())
    else:OnlineController._require_job_environment(Database())


def test_job_stop_lost_ack_cannot_be_retried_or_admitted_as_completed():
    from scripts.automation.storage_online_controller import OnlineController
    controller = OnlineController.__new__(OnlineController)
    controller._archive_namespace_check = None
    controller._builtin_catalog = None
    controller._jobs_stop_requested = True
    controller._jobs_stopped = False
    controller.final_session_observation = lambda **kw:{"database":{"allow_connections":False}}
    controller._ownership = lambda **kw:None
    with pytest.raises(RuntimeError,match="closed_gate_required"):
        controller.quiesce_final_database_jobs(deadline=monotonic()+1)
    with pytest.raises(RuntimeError,match="jobs_stop_unconfirmed"):
        controller._require_external_sql_clients_absent(None,deadline=monotonic()+1)


@pytest.mark.parametrize("held", [0, 1, 3])
def test_handoff_catalog_refuses_without_both_owned_locks(monkeypatch, held):
    from types import SimpleNamespace
    from portal.backend.service.storage import header_catalog
    conn = SimpleNamespace(in_transaction=lambda: True,
        get_isolation_level=lambda: "READ COMMITTED", scalar=lambda query: held)
    def forbidden(*args, **kwargs):
        raise AssertionError("unowned catalog observation reached")
    monkeypatch.setattr(header_catalog, "_observe_header_catalog", forbidden)
    with pytest.raises(RuntimeError, match="handoff_locks_required"):
        header_catalog.read_transaction_header_catalog(conn)


def test_catalog_and_discovery_commands_keep_fixed_scope_and_fresh_observation(monkeypatch):
    from scripts.automation import storage_online_controller as module
    controller=module.OnlineController.__new__(module.OnlineController)
    controller._final_connection_entered=False;controller._final_deadline=None
    controller.controller_id="c"*32;controller.state="background";controller._sequence=0
    controller._last_request=controller._last_reply=None;controller._reproved=set()
    controller._admitted_limits={"movement_timeout_seconds":60}
    controller.proof=SimpleNamespace(deadline=monotonic()+90,hashed_bytes=0)
    controller._admit_attempt=lambda:None;controller.check=lambda:None
    calls=[]
    controller._catalog_step=lambda relation,seconds: calls.append((relation,seconds)) or {"committed":True}
    controller._reference_page=lambda after: calls.append(after) or {"references":[],"next_after":None}
    request=dict(controller_id=controller.controller_id,sequence=1,operation="prepare_step",
                 step="catalog_history",relation="market.fact_archive_material_aliases",max_duration_seconds=30)
    for changed in ({"relation":"market.fact_versions"},{"relation":"market.fact_archive_material_aliases;DROP TABLE x"},
                    {"relation":None},{"max_duration_seconds":61},{"source_root":"/elsewhere"}):
        with pytest.raises(ValueError):controller.command(request|changed)
    assert not calls
    reply=controller.command(request)
    assert controller.command(request)==reply and len(calls)==1
    discovery=dict(controller_id=controller.controller_id,sequence=2,operation="inspect_references",after=None)
    result=controller.command(discovery)
    assert result["result"]["references"]==[] and not result["migration_ready"]
    with pytest.raises(RuntimeError,match="fresh_sequence_required"):controller.command(discovery)
    for changed in ({"after":True},{"after":"x"*257},{"limit":999}):
        with pytest.raises(ValueError):controller.command(discovery|changed|{"sequence":3})
    controller._final_deadline=monotonic()+20
    with pytest.raises(RuntimeError,match="background_work_refused"):
        controller.command(discovery|{"sequence":3})


def test_catalog_move_expired_absolute_deadline_never_opens_database():
    from scripts.db.archive_reference_v2_placement import move_reference_catalog
    class NoDatabase:
        def connect(self):pytest.fail("expired catalog operation opened database")
    with pytest.raises(ValueError,match="attempt_binding_invalid"):
        move_reference_catalog(NoDatabase(),relation="market.fact_archive_material_aliases",
            policy=None,resource_limits=None,deadline=monotonic()-1)

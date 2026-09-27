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
    controller.state="aborted";controller._final_deadline=monotonic()-1
    with pytest.raises(RuntimeError,match="terminal_deadline_expired"):controller.check()

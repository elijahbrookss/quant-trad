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

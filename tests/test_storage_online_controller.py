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

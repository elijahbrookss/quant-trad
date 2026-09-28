"""Real pipe framing, retained identity and no-replay host behavior."""
from contextlib import contextmanager
import fcntl
import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

from scripts.automation.storage_host_boundary import OnlineWorkerChannel


def envelope(sequence=0, operation=None, **extra):
    result = dict(schema_version="qt.storage_online_controller.v1", controller_id="a"*32,
        state="background", last_sequence=sequence, bound_final_deadline=None,
        migration_ready=False, final_switch_authorized=False, collection_resume_authorized=False)
    if operation is not None:
        result["operation"] = operation
    return result | extra


def encoded(value):
    return json.dumps(value).encode()+b"\n"


@contextmanager
def peer(respond, *, greeting=None, high_fds=False):
    incoming, host_write = os.pipe()
    host_read, outgoing = os.pipe()
    if high_fds:
        changed = fcntl.fcntl(host_write, fcntl.F_DUPFD_CLOEXEC, 2048)
        os.close(host_write); host_write = changed
        changed = fcntl.fcntl(host_read, fcntl.F_DUPFD_CLOEXEC, 2048)
        os.close(host_read); host_read = changed
    commands = []
    errors = []
    release = threading.Event()
    def worker():
        try:
            with os.fdopen(incoming, "rb", buffering=0) as stream, os.fdopen(outgoing, "wb", buffering=0) as output:
                output.write(encoded(envelope()) if greeting is None else greeting)
                for line in stream:
                    request = json.loads(line)
                    commands.append(request)
                    response = respond(request, release)
                    if response is None:
                        return
                    output.write(response)
        except BrokenPipeError:
            pass  # Host deliberately retires an invalid/expired owned channel.
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=worker)
    thread.start()
    process = SimpleNamespace(stdin=os.fdopen(host_write,"wb",buffering=0),
                              stdout=os.fdopen(host_read,"rb",buffering=0))
    try:
        yield process, commands, release
    finally:
        release.set()
        process.stdin.close();process.stdout.close()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert not errors, errors


def normal(request, release):
    return encoded(envelope(request["sequence"],request["operation"]))


def test_one_retained_worker_above_select_fd_limit():
    with peer(normal,high_fds=True) as (process, commands, _):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        assert channel.greeting["controller_id"]=="a"*32
        first=channel.exchange("status")
        assert first["last_sequence"]==1
        # A caller losing a FULLY RECEIVED response may inspect on the same pipe;
        # it cannot manually reuse the completed sequence.
        second=channel.exchange("inspect_outcome",deadline=time.monotonic()+1)
        assert second["last_sequence"]==2 and channel.sequence==2
        assert [x["sequence"] for x in commands]==[1,2]
        assert os.get_blocking(process.stdin.fileno()) and os.get_blocking(process.stdout.fileno())


@pytest.mark.parametrize("response", [
    b'{"part":',
    b'not-json\n',
    b'{"last_sequence":1,"last_sequence":1}\n',
    encoded(envelope(1,"commit_database"))+encoded(envelope(2,"status")),
    b'x'*16385,
    encoded(envelope(2,"commit_database")),
    encoded(envelope(1,"commit_database",controller_id="b"*32)),
    encoded(envelope(1,"status")),
    encoded(envelope(1,"commit_database",migration_ready=True)),
    encoded(envelope(1,"commit_database"))[:-2]+b',"x":1e999}\n',
])
def test_possible_commit_with_bad_or_partial_reply_is_never_replayed(response):
    def respond(request, release):
        return response
    with peer(respond) as (process, commands, _):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5,command_seconds=.08)
        with pytest.raises((RuntimeError,TimeoutError)):
            channel.exchange("commit_database",deadline=time.monotonic()+1)
        for operation in ("commit_database","inspect_outcome","rollback_resume_begin","status"):
            with pytest.raises(RuntimeError,match="channel_unresolved"):
                channel.exchange(operation)
        assert len(commands)==1 and commands[0]["operation"]=="commit_database"
        assert channel.sequence==1


def test_eof_after_received_command_is_unresolved_without_reconnect():
    with peer(lambda request,release:None) as (process,commands,_):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        with pytest.raises(RuntimeError,match="channel_eof"):
            channel.exchange("commit_database")
        with pytest.raises(RuntimeError,match="channel_unresolved"):
            channel.exchange("status")
        assert len(commands)==1


def test_original_final_deadline_cannot_disappear_or_extend():
    for changed in (None, time.monotonic()+4):
        bound=time.monotonic()+2
        def respond(request,release):
            return encoded(envelope(request["sequence"],request["operation"],
                bound_final_deadline=bound if request["sequence"]==1 else changed))
        with peer(respond) as (process,commands,_):
            channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
            channel.exchange("final_delta",deadline=bound)
            with pytest.raises(RuntimeError,match="final_deadline_changed"):
                channel.exchange("status")
            with pytest.raises(RuntimeError,match="channel_unresolved"):
                channel.exchange("commit_database")
            assert len(commands)==2


def test_expired_caller_budget_sends_nothing():
    with peer(normal) as (process,commands,_):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        with pytest.raises(TimeoutError,match="deadline_expired"):
            channel.exchange("commit_database",deadline=time.monotonic()-1)
        assert not commands


def test_short_writes_complete_one_frame(monkeypatch):
    with peer(normal) as (process,commands,_):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        write=os.write
        def partial(fd,data):
            return write(fd,data[:7] if fd==process.stdin.fileno() else data)
        monkeypatch.setattr(os,"write",partial)
        assert channel.exchange("status")["last_sequence"]==1
        assert len(commands)==1


def test_request_larger_than_worker_bound_sends_nothing():
    with peer(normal) as (process,commands,_):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        with pytest.raises(ValueError,match="command_bound_exceeded"):
            channel.exchange("prepare_step",relation="x"*4096)
        assert not commands


def test_second_caller_cannot_pipeline_while_first_awaits_reply():
    received=threading.Event()
    def respond(request,release):
        received.set()
        assert release.wait(2)
        return normal(request,release)
    with peer(respond) as (process,commands,release):
        channel=OnlineWorkerChannel(process,deadline=time.monotonic()+5)
        results=[]
        thread=threading.Thread(target=lambda:results.append(channel.exchange("status")))
        thread.start()
        assert received.wait(1)
        with pytest.raises(RuntimeError,match="command_inflight"):
            channel.exchange("commit_database")
        release.set();thread.join(timeout=2)
        assert not thread.is_alive() and results[0]["last_sequence"]==1
        assert len(commands)==1

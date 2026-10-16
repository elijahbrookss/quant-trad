"""Exercise real local HTTP disconnect behavior with synthetic service work only."""
import socket
from threading import Event, Thread
from time import monotonic, sleep

from fastapi import FastAPI, HTTPException
import pytest
import uvicorn

from portal.backend.controller import research


def test_client_disconnect_does_not_claim_to_cancel_sync_server_work(monkeypatch):
    entered, release, finished = Event(), Event(), Event()
    def fixture_read():
        entered.set()
        assert release.wait(2)
        finished.set()
        return {"schema_version": "synthetic_disconnect_probe"}
    monkeypatch.setattr(research.research_browse, "overview", fixture_read)
    app = FastAPI()
    app.include_router(research.router, prefix="/api/research")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
    worker = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    worker.start()
    client = socket.create_connection(("127.0.0.1", port), timeout=2)
    try:
        client.sendall(b"GET /api/research/overview HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        assert entered.wait(2)
        disconnected_at = monotonic()
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        sleep(0.05)
        assert not finished.is_set(), "fixture service must still be executing after the client disconnect"
        release.set()
        assert finished.wait(2)
        assert monotonic() - disconnected_at >= 0.05
    finally:
        release.set()
        client.close()
        server.should_exit = True
        worker.join(timeout=3)
        listener.close()
    assert not worker.is_alive()


def test_read_deadline_is_separate_from_scientific_job_cancellation(monkeypatch):
    real = research.ExecutionControl
    class ShortRead(real):
        def limit(self, **kwargs):
            super().limit(seconds=0.02)
    monkeypatch.setattr(research, "ExecutionControl", ShortRead)
    with pytest.raises(HTTPException) as raised:
        research._read_call(lambda: sleep(0.03))
    assert raised.value.status_code == 503

"""Owned Docker start reply fault; never imported by production operation paths.

A private Unix proxy permits only ping and one exact container start. The real
engine completes the start before its HTTP reply is withheld. This tests an
in-flight CLI reply, not a daemon operation completing after ownership loss.
"""
from contextlib import closing, contextmanager
import http.client
from http.server import BaseHTTPRequestHandler
import os
from pathlib import Path
import re
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


@contextmanager
def held_start_reply(container_id, *, deadline, on_started):
    if not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise ValueError("fixture_exact_container_required")
    original_popen = subprocess.Popen
    release = threading.Event()
    outcome = {"requests": [], "start_count": 0, "errors": [], "cli": None}

    class Connection(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect("/var/run/docker.sock")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.forward()

        def do_GET(self):
            self.forward()

        def do_POST(self):
            self.forward()

        def forward(self):
            try:
                start = self.command == "POST" and bool(re.fullmatch(
                    r"/v[0-9]+\.[0-9]+/containers/"+container_id+r"/start", self.path))
                ping = self.command in {"GET", "HEAD"} and self.path == "/_ping"
                if not (start or ping) or self.headers.get("Transfer-Encoding"):
                    raise AssertionError("fixture_proxy_request_refused")
                length = int(self.headers.get("Content-Length", "0"))
                if length != 0:
                    raise AssertionError("fixture_proxy_body_refused")
                outcome["requests"].append(self.command+" "+self.path)
                if start:
                    outcome["start_count"] += 1
                    assert outcome["start_count"] == 1
                remaining = deadline-time.monotonic()
                assert remaining > 0
                with closing(Connection("localhost", timeout=min(5, remaining))) as connection:
                    connection.request(self.command, self.path, headers={"Content-Length": "0"})
                    response = connection.getresponse()
                    status, headers, body = response.status, response.getheaders(), response.read()
                if start:
                    assert status == 204
                    outcome["daemon_start_status"] = status
                    # Popen can still be returning while the HTTP thread runs.
                    while outcome["cli"] is None and time.monotonic() < deadline:
                        time.sleep(.001)
                    assert outcome["cli"] is not None and outcome["cli"].poll() is None
                    outcome["cli_pending_after_daemon_start"] = True
                    on_started()
                    outcome["fault_completed"] = True
                    if not release.wait(max(0, deadline-time.monotonic())):
                        raise AssertionError("fixture_reply_hold_deadline_expired")
                self.send_response(status)
                for key, value in headers:
                    if key.lower() not in {"content-length", "transfer-encoding", "connection"}:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                outcome["reply_peer_closed"] = True
            except BaseException as exc:
                outcome["errors"].append(type(exc).__name__)
                self.close_connection = True

    class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = False

        def get_request(self):
            client, address = super().get_request()
            client.settimeout(max(.1, deadline-time.monotonic()))
            return client, address

    with tempfile.TemporaryDirectory(prefix="qt-start-reply-") as directory:
        address = str(Path(directory)/"engine.sock")
        with Server(address, Handler) as server:
            os.chmod(address, 0o600)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .05})
            thread.start()
            def popen(args, **kwargs):
                if list(args[:2]) == ["docker", "start"]:
                    assert args == ["docker", "start", container_id]
                    assert outcome["cli"] is None
                    kwargs["env"] = dict(os.environ, DOCKER_HOST="unix://"+address)
                    outcome["cli"] = original_popen(args, **kwargs)
                    return outcome["cli"]
                return original_popen(args, **kwargs)
            subprocess.Popen = popen
            try:
                yield outcome
            finally:
                subprocess.Popen = original_popen
                release.set()
                server.shutdown()
                thread.join(timeout=1)
                assert not thread.is_alive()
        if outcome["errors"]:
            raise AssertionError("fixture_proxy_failed: "+",".join(outcome["errors"]))

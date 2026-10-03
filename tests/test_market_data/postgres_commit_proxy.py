"""Disposable Unix-socket protocol proxy that drops a real COMMIT response.

It connects only inside the namespace fixture's owned /tmp root, never TCP or
an ambient DSN. No protocol payload is logged. Frame sizes and waits are bounded.
"""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import os
import socket
import tempfile
import threading


def _read_exact(peer, count):
    chunks = bytearray()
    while len(chunks) < count:
        piece = peer.recv(count - len(chunks))
        if not piece:
            if not chunks:
                return None
            raise RuntimeError("commit_proxy_truncated_frame")
        chunks.extend(piece)
    return bytes(chunks)


def _frame(peer, *, startup=False):
    kind = b"" if startup else _read_exact(peer, 1)
    if kind is None:
        return None
    size = _read_exact(peer, 4)
    if size is None:
        return None
    count = int.from_bytes(size, "big")
    if not 4 <= count <= 1024 * 1024:
        raise RuntimeError("commit_proxy_frame_budget")
    body = _read_exact(peer, count - 4)
    if body is None:
        raise RuntimeError("commit_proxy_missing_frame_body")
    return kind + size + body


@contextmanager
def drop_commit_ack(root, server_directory):
    root, server_directory = root.resolve(strict=True), server_directory.resolve(strict=True)
    if (root.parent != Path("/tmp") or not root.name.startswith("qt-header-ns-")
            or root.stat().st_uid != os.geteuid() or not server_directory.is_relative_to(root)):
        raise RuntimeError("commit_proxy_requires_owned_private_cluster")
    directory = Path(tempfile.mkdtemp(prefix="commit-proxy-", dir=root))
    path = directory / ".s.PGSQL.5432"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.settimeout(30)
    listener.bind(str(path))
    listener.listen(1)
    stopped, sent, committed, armed = (threading.Event() for _ in range(4))
    peers, errors = [], []

    def close_peer(peer):
        try:
            peer.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # Closing an already closed owned socket is expected teardown.
        peer.close()

    def close():
        stopped.set()
        for peer in list(peers):
            close_peer(peer)

    def relay_client(client, server):
        try:
            first = _frame(client, startup=True)
            if first is None:
                raise RuntimeError("commit_proxy_startup_missing")
            # Explicit sslmode/gssencmode=disable must yield protocol3 startup.
            if first[4:8] != bytes((0, 3, 0, 0)):
                raise RuntimeError("commit_proxy_startup_not_plain_private_protocol3")
            server.sendall(first)
            while not stopped.is_set():
                frame = _frame(client)
                if frame is None:
                    break
                if frame[:1] == b"Q" and frame[5:].rstrip(b"\x00").strip().upper() == b"COMMIT":
                    sent.set()
                server.sendall(frame)
        except Exception as exc:
            if not stopped.is_set():
                errors.append(type(exc).__name__ + ":" + str(exc))
        finally:
            close()

    def serve():
        child = None
        try:
            client, _ = listener.accept()
            client.settimeout(30)
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.settimeout(30)
            server.connect(str(server_directory / ".s.PGSQL.5432"))
            peers.extend((client, server))
            child = threading.Thread(target=relay_client, args=(client, server), daemon=True)
            child.start()
            while not stopped.is_set():
                frame = _frame(server)
                if frame is None:
                    break
                if armed.is_set() and sent.is_set() and frame[:1] == b"C" and frame[5:] == b"COMMIT\x00":
                    committed.set()
                    # PostgreSQL has committed. Do not forward CommandComplete
                    # or ReadyForQuery: libpq must see a lost connection instead.
                    break
                client.sendall(frame)
        except Exception as exc:
            if not stopped.is_set():
                errors.append(type(exc).__name__ + ":" + str(exc))
        finally:
            close()
            if child is not None:
                child.join(5)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    body_failed = False
    try:
        yield SimpleNamespace(directory=directory, committed=committed, armed=armed)
    except BaseException:
        body_failed = True
        raise
    finally:
        close()
        listener.close()
        thread.join(5)
        path.unlink(missing_ok=True)
        directory.rmdir()
        if not body_failed and (thread.is_alive() or errors):
            raise RuntimeError("commit_proxy_failed: " + repr(errors))

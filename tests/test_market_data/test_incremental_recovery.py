"""Encrypted recovery key/configuration and bounded subprocess failure paths."""
import os
from pathlib import Path
import signal
import sys
from time import monotonic, sleep

import pytest

from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, _private_bytes, _root_label,
)


def test_private_keys_reject_aliases_permissions_and_unbounded_content(tmp_path):
    path = tmp_path/"key"
    path.write_bytes(b"a"*64)
    path.chmod(0o600)
    assert _private_bytes(path, limit=64) == b"a"*64
    path.chmod(0o644)
    with pytest.raises(RuntimeError, match="permissions"):
        _private_bytes(path, limit=64)
    path.chmod(0o600)
    alias = tmp_path/"alias"
    alias.symlink_to(path)
    with pytest.raises(ValueError, match="unaliased"):
        _private_bytes(alias, limit=64)
    with pytest.raises(RuntimeError, match="size_limit"):
        _private_bytes(path, limit=63)
    os.link(path, tmp_path/"hardlink")
    with pytest.raises(RuntimeError, match="permissions"):
        _private_bytes(path, limit=64)


@pytest.mark.parametrize("label", ["", "../backup", "20260924-130900F_suffix", None, 1])
def test_dependency_retirement_requires_exact_native_labels(label):
    with pytest.raises(RuntimeError, match="label_invalid"):
        _root_label(label)


@pytest.mark.skipif(sys.platform != "linux", reason="POSIX process-group supervision")
def test_deadline_kills_owned_child_even_after_leader_exits(tmp_path):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.env = tmp_path, {"PATH": os.defpath}
    worker.deadline = monotonic()+1
    def check(*args):
        if monotonic() >= worker.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")
    worker.check = check
    marker = tmp_path/"child.pid"
    script = (
        "import os,time,pathlib\n"
        "pid=os.fork()\n"
        "if pid: os._exit(0)\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    with pytest.raises(RuntimeError, match="time_budget"):
        worker._run([sys.executable, "-c", script])
    assert marker.exists()
    child = int(marker.read_text())
    # A reparented child may remain a zombie until the host reaps it, but cannot
    # remain a live writer after the owning operation fails.
    deadline = monotonic()+3
    while True:
        status = Path(f"/proc/{child}/stat")
        if not status.exists() or status.read_text().split()[2] == "Z":
            break
        if monotonic() >= deadline:
            os.kill(child, signal.SIGKILL)
            pytest.fail("owned backup child survived deadline")
        sleep(0.01)

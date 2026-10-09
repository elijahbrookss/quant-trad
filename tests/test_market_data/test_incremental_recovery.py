"""Encrypted recovery key/configuration and bounded subprocess failure paths."""
import json
import os
from pathlib import Path
import shutil
import signal
import sys
from time import monotonic, sleep

import pytest

from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, _private_bytes, _root_label,
)


@pytest.mark.skipif(shutil.which("restic") is None, reason="requires pinned native restic")
def test_native_restic_keeps_summary_without_periodic_progress_and_reports_failure(tmp_path):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root = tmp_path
    worker.restic = Path(shutil.which("restic"))
    worker.env = {"PATH": os.defpath, "HOME": str(tmp_path),
                  "RESTIC_PASSWORD": "disposable-test-key"}
    worker.deadline = monotonic()+30

    def check(*args):
        if monotonic() >= worker.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")

    worker.check = check
    assert worker._run([str(worker.restic), "version"]).startswith(b"restic 0.19.1 ")
    worker._run(worker._rs("init"))
    # With --no-scan, progress starts only after a chunk/item is processed.
    # Exceed the native maximum chunk size before waiting; a small stdin input
    # would finish without exercising periodic progress at all.
    output = worker._run(worker._rs("backup", "--no-scan", "--stdin-from-command",
        "--stdin-filename", "probe", "--", sys.executable, "-c",
        "import sys,time; sys.stdout.buffer.write(b'a'*(16*1024*1024)); "
        "sys.stdout.flush(); time.sleep(1)"))
    messages = [json.loads(line) for line in output.splitlines()]
    assert [item["message_type"] for item in messages] == ["summary"]
    snapshot = messages[0]["snapshot_id"]
    assert len(snapshot) == 64

    with pytest.raises(RuntimeError, match="incremental_tool_failed:restic:exit=") as failure:
        worker._run(worker._rs("backup", "--stdin-from-command", "--",
            sys.executable, "-c",
            "import sys; sys.stderr.write('private diagnostic'); sys.exit(7)"))
    assert "private diagnostic" not in str(failure.value)
    snapshots = json.loads(worker._run(worker._rs("snapshots")))
    assert [item["id"] for item in snapshots] == [snapshot]


@pytest.mark.parametrize("stream", ["out", "err"])
def test_tool_output_limit_names_stream_without_exposing_content(tmp_path, stream):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.env = tmp_path, {"PATH": os.defpath}
    worker.deadline = monotonic()+10
    worker.check = lambda *args: None
    descriptor = 1 if stream == "out" else 2
    script = f"import os; os.write({descriptor}, b'private diagnostic'*500000)"
    with pytest.raises(RuntimeError) as failure:
        worker._run([sys.executable, "-c", script])
    assert str(failure.value) == (
        f"incremental_tool_output_limit:{Path(sys.executable).name}:stream={stream}")


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
        try:
            state = status.read_text().split()[2]
        except FileNotFoundError:
            # Reaping may occur between an existence check and the read.
            # A gone child is the successful termination this test requires.
            break
        if state == "Z":
            break
        if monotonic() >= deadline:
            os.kill(child, signal.SIGKILL)
            pytest.fail("owned backup child survived deadline")
        sleep(0.01)

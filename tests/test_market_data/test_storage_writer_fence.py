"""Actual kernel exclusion and descriptor lifetime; no database or data repair."""
from __future__ import annotations

import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux directory flock")
ROOT = Path(__file__).resolve().parents[2]


def _env(root):
    return dict(os.environ, QT_DISABLE_DOTENV="1", QT_LOGGING_LOKI_URL="",
                QT_STORAGE_SOURCE_FENCE_ROOT=str(root), MARKET_STRUCTURE_STORAGE_ROOT=str(root),
                MARKET_STRUCTURE_WORKING_ROOT=str(root), QT_MARKET_DATA_EXPECTED_UUID="",
                QT_MARKET_DATA_WORKING_EXPECTED_UUID="",
                PG_DSN="postgresql+psycopg2://fixture:fixture@127.0.0.1:1/fixture",
                PYTHONPATH=os.pathsep.join((str(ROOT / "src"), str(ROOT))))


@pytest.fixture
def source(tmp_path):
    root = tmp_path / "source"
    root.mkdir(mode=0o750)
    (root / "private.wal").write_bytes(b"preserved original WAL")
    (root / "private.wal").chmod(0o600)
    return root


def _metadata(root):
    return {str(p.relative_to(root)): (p.stat().st_uid, p.stat().st_gid,
            p.stat().st_mode, p.read_bytes() if p.is_file() else None)
            for p in [root, *root.rglob("*")]}


def _run(source, code, **values):
    return subprocess.run([sys.executable, "-c", code], env=dict(_env(source), **values),
                          cwd=ROOT, text=True, capture_output=True, timeout=15)


def test_optional_fence_does_not_create_source(tmp_path):
    source = tmp_path / "absent"
    env = _env(source)
    del env["QT_STORAGE_SOURCE_FENCE_ROOT"]
    result = subprocess.run([sys.executable, "-c",
        "import sys; sys.modules['fcntl'] = None; "
        "from core.storage_writer_fence import retain_source_writer_fence; "
        "assert retain_source_writer_fence() == ()"], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert not source.exists()


@pytest.mark.parametrize("invalid", ["missing", "relative", "root", "alias", "split"])
def test_invalid_binding_refuses_without_source_changes(source, invalid):
    before = _metadata(source)
    alternate = source.parent / "other"
    alternate.mkdir()
    alias = source.parent / "alias"
    alias.symlink_to(source, target_is_directory=True)
    values = {
        "missing": {"QT_STORAGE_SOURCE_FENCE_ROOT": str(source / "missing")},
        "relative": {"QT_STORAGE_SOURCE_FENCE_ROOT": "source"},
        "root": {"QT_STORAGE_SOURCE_FENCE_ROOT": "/"},
        "alias": {"QT_STORAGE_SOURCE_FENCE_ROOT": str(alias)},
        "split": {"MARKET_STRUCTURE_WORKING_ROOT": str(alternate)},
    }[invalid]
    result = _run(source, "from core.storage_writer_fence import retain_source_writer_fence; retain_source_writer_fence()", **values)
    assert result.returncode != 0
    assert _metadata(source) == before
    assert list(alternate.iterdir()) == []


def test_shared_writers_exclude_operator_until_both_retire(source):
    import fcntl
    before = _metadata(source)
    processes = []
    descriptor = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for _ in range(2):
            process = subprocess.Popen([sys.executable, "-c", "import sys; "
                "from core.storage_writer_fence import retain_source_writer_fence; "
                "fd = retain_source_writer_fence(); assert fd == retain_source_writer_fence(); "
                "print('held', flush=True); sys.stdin.readline()"],
                env=_env(source), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(process)
            assert select.select([process.stdout], [], [], 10)[0]
            assert process.stdout.readline().strip() == "held"
        for process in processes:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            process.communicate("retire\n", timeout=10)
            assert process.returncode == 0
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert _metadata(source) == before
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
        os.close(descriptor)


@pytest.mark.parametrize("module", ["portal.backend.run_backend",
    "portal.backend.workers.market_data_collector", "portal.backend.workers.single_node_initializer"])
def test_actual_entrypoint_refuses_before_work_under_exclusive_hold(source, module):
    import fcntl
    before = _metadata(source)
    descriptor = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run([sys.executable, "-m", module], env=_env(source), cwd=ROOT,
                                capture_output=True, text=True, timeout=15)
        assert result.returncode != 0
        assert "storage_source_writer_excluded_by_handoff" in result.stderr
        assert "Application startup complete" not in result.stderr
        assert _metadata(source) == before
    finally:
        os.close(descriptor)


def test_backend_child_retains_shared_hold_after_supervisor_exit(source, tmp_path):
    import fcntl
    ready = tmp_path / "child.pid"
    child_code = "import os,time; from pathlib import Path; Path(" + repr(str(ready)) + ").write_text(str(os.getpid())); time.sleep(30)"
    parent_code = "from portal.backend.run_backend import _spawn_process; _spawn_process('fixture', " + repr([sys.executable, "-c", child_code]) + ")"
    descriptor = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    child = None
    try:
        # The actual supervisor Popen path must pass the held open-file description.
        parent = subprocess.run([sys.executable, "-c", parent_code], env=_env(source), cwd=ROOT,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        assert parent.returncode == 0
        end = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < end:
            time.sleep(.01)
        assert ready.exists()
        child = int(ready.read_text())
        with pytest.raises(BlockingIOError):
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.kill(child, signal.SIGTERM)
        end = time.monotonic() + 5
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                assert time.monotonic() < end
                time.sleep(.01)
        child = None
    finally:
        if child is not None:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
        os.close(descriptor)


def test_retained_binding_refuses_root_replacement_and_configuration_removal(source):
    result = _run(source, r"""
import os
from pathlib import Path
from core.storage_writer_fence import retain_source_writer_fence
root = Path(os.environ['QT_STORAGE_SOURCE_FENCE_ROOT'])
fd = retain_source_writer_fence()
root.rename(root.with_name('original'))
root.mkdir()
try:
    retain_source_writer_fence()
except RuntimeError as exc:
    assert str(exc) == 'storage_source_fence_binding_changed'
else:
    raise AssertionError('replacement accepted')
del os.environ['QT_STORAGE_SOURCE_FENCE_ROOT']
try:
    retain_source_writer_fence()
except RuntimeError as exc:
    assert str(exc) == 'storage_source_fence_configuration_changed'
else:
    raise AssertionError('configuration removal accepted')
""")
    assert result.returncode == 0, result.stderr
    assert (source.with_name("original") / "private.wal").read_bytes() == b"preserved original WAL"

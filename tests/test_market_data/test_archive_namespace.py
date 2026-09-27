"""Real process-held archive exclusion; mutation failure preserves bytes."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_namespace import archive_namespace


def _peer(root, source, *, blocked):
    script = """
import hashlib, sys
from pathlib import Path
from market_data.archive import FilesystemRawArchiveObjectStore
root, source = map(Path, sys.argv[1:3])
blocked = sys.argv[3] == 'True'
store = FilesystemRawArchiveObjectStore(root)
digest = hashlib.sha256(source.read_bytes()).hexdigest()
for action in (
    lambda: store.put_verified(object_key='late/item', source_path=source, expected_sha256=digest),
    lambda: store.delete_verified(object_key='retained/item', expected_sha256=digest),
):
    try:
        action()
    except RuntimeError as exc:
        assert blocked and str(exc) == 'market_archive_namespace_busy', str(exc)
    else:
        assert not blocked
assert (root/'retained/item').exists() == blocked
"""
    subprocess.run([sys.executable, '-c', script, str(root), str(source), str(blocked)],
                   check=True, capture_output=True, text=True, timeout=10)


def test_exclusive_namespace_blocks_late_process_and_releases_on_exit(tmp_path):
    root, source = tmp_path/'objects', tmp_path/'source'
    source.write_bytes(b'retained bytes')
    store = FilesystemRawArchiveObjectStore(root)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    store.put_verified(object_key='retained/item', source_path=source, expected_sha256=digest)
    before = (root/'retained/item').stat()
    with archive_namespace(root, exclusive=True) as check:
        _peer(root, source, blocked=True)
        assert (root/'retained/item').read_bytes() == source.read_bytes()
        assert not (root/'late').exists()
        assert (root/'retained/item').stat().st_ino == before.st_ino
        check()
    _peer(root, source, blocked=False)
    assert (root/'late/item').read_bytes() == source.read_bytes()


def test_shared_publication_is_concurrent_but_excludes_final_switch(tmp_path):
    root = tmp_path/'objects'; root.mkdir()
    with archive_namespace(root):
        with archive_namespace(root):
            with pytest.raises(RuntimeError, match='namespace_busy'):
                with archive_namespace(root, exclusive=True):
                    pytest.fail('exclusive admission ignored active publication')
    with archive_namespace(root, exclusive=True):
        pass


def test_namespace_owner_death_releases_lock_without_receipt(tmp_path):
    root = tmp_path/'objects'; root.mkdir()
    script = "from market_data.archive_namespace import archive_namespace; import sys; " \
             "c=archive_namespace(sys.argv[1],exclusive=True); c.__enter__(); print('held',flush=True); sys.stdin.read()"
    process = subprocess.Popen([sys.executable, '-c', script, str(root)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        import select
        assert select.select([process.stdout], [], [], 5)[0]
        assert process.stdout.readline().strip() == 'held'
        with pytest.raises(RuntimeError, match='namespace_busy'):
            with archive_namespace(root):pass
        process.kill(); process.wait(timeout=5)
        with archive_namespace(root, exclusive=True):pass
        assert list(root.iterdir()) == []
    finally:
        if process.poll() is None:process.kill(); process.wait(timeout=5)
        process.stdin.close(); process.stdout.close()


def test_namespace_root_replacement_invalidates_live_binding(tmp_path):
    root = tmp_path/'objects'; root.mkdir()
    with pytest.raises(RuntimeError, match='binding_changed'):
        with archive_namespace(root, exclusive=True) as check:
            root.rename(tmp_path/'preserved'); root.mkdir()
            check()

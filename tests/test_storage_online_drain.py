"""Read-only WAL observation: bounds, pending bytes, races and no authority."""
import os
from pathlib import Path
from time import monotonic

import pytest

from scripts.automation import storage_online_drain as drain


def observe(root, **overrides):
    return drain.inspect_spool(root, **({"deadline": monotonic()+5, "max_entries": 100,
                                           "check": lambda: None} | overrides))


def test_absent_and_acknowledged_spool_are_observations_only(tmp_path):
    assert observe(tmp_path)["spool_empty_at_observation"]
    spool = tmp_path/"spool"/"definition"/"session"/"epoch=0"
    spool.mkdir(parents=True)
    ack = spool/"segment.ack.json"
    ack.write_bytes(b"not relied on as a database certificate")
    result = observe(tmp_path)
    assert result["acknowledgement_files"] == 1 and result["spool_empty_at_observation"]
    assert not any(result[k] for k in ("publisher_drain_authorized", "final_switch_authorized",
                                       "collection_resume_authorized"))
    assert ack.read_bytes() == b"not relied on as a database certificate"


@pytest.mark.parametrize("name", ["a.open", "a.sealed", "a.ack.partial", "unknown"])
def test_pending_wal_never_deleted_even_with_acknowledgement(tmp_path, name):
    spool = tmp_path/"spool"; spool.mkdir()
    (spool/name).write_bytes(b"durable WAL")
    (spool/"a.ack.json").write_text("{}")
    before = {p.name: p.read_bytes() for p in spool.iterdir()}
    result = observe(tmp_path)
    assert not result["spool_empty_at_observation"]
    assert result["pending_files"] == 1 and result["pending_bytes"] == 11
    assert {p.name: p.read_bytes() for p in spool.iterdir()} == before


@pytest.mark.parametrize("kind", ["symlink", "fifo", "depth", "entries", "deadline"])
def test_invalid_or_unbounded_walk_refuses_and_closes_fds(tmp_path, kind):
    spool = tmp_path/"spool";spool.mkdir()
    kwargs = {}
    if kind == "symlink": (spool/"link").symlink_to(tmp_path, target_is_directory=True)
    elif kind == "fifo": os.mkfifo(spool/"fifo")
    elif kind == "depth": spool.joinpath(*[str(i) for i in range(10)]).mkdir(parents=True)
    elif kind == "entries":
        (spool/"a.ack.json").write_text("{}");(spool/"b.ack.json").write_text("{}")
        kwargs["max_entries"] = 1
    else: kwargs["deadline"] = monotonic()-1
    before = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(RuntimeError): observe(tmp_path, **kwargs)
    assert len(list(Path("/proc/self/fd").iterdir())) == before


def test_directory_replacement_during_observation_refuses(tmp_path):
    spool = tmp_path/"spool";spool.mkdir();(spool/"a.ack.json").write_text("{}")
    calls = [0]
    def change():
        calls[0] += 1
        if calls[0] == 3:
            spool.rename(tmp_path/"retained")
            spool.mkdir()
    with pytest.raises(RuntimeError, match="spool_changed"):
        observe(tmp_path, check=change)
    assert (tmp_path/"retained"/"a.ack.json").read_text() == "{}"


@pytest.fixture
def recovery_copy(tmp_path, monkeypatch):
    from types import SimpleNamespace
    source, target = tmp_path/"original", tmp_path/"candidate"
    source.mkdir(mode=0o700); target.mkdir(mode=0o700)
    wal = source/"spool"/"definition"/"session"/"epoch=0"/"segment.open"
    wal.parent.mkdir(parents=True)
    wal.write_bytes(b"unaltered complete records\npartial last frame")
    wal.chmod(0o600)
    (wal.parent/"segment.ack.json").write_text("old projection is not authority")
    # A real read-only mount/account transition is qualified by the native
    # fixture; unit tests exercise copying, preservation and fault boundaries.
    # Model the privileged preparer without depending on the CI runner's UID.
    # Keep the real source stats and all I/O; only the target account admission
    # and ownership operation are simulated here (both are native-qualified).
    real_uid = os.geteuid()
    native_fstat = os.fstat
    def preparer_stat(fd):
        info = native_fstat(fd)
        if Path(os.readlink(f"/proc/self/fd/{fd}")) == target:
            values = list(info)
            values[4] = 0
            return os.stat_result(values)
        return info
    proxy = SimpleNamespace(**{name: getattr(os, name) for name in dir(os)})
    proxy.geteuid = lambda: 0
    proxy.fstat = preparer_stat
    proxy.statvfs = lambda _: SimpleNamespace(f_flag=os.ST_RDONLY)
    monkeypatch.setattr(drain, "os", proxy)
    changed = []
    chown = os.fchown
    def destination_chown(fd, uid, gid):
        path = Path(os.readlink(f"/proc/self/fd/{fd}"))
        assert path == target or target in path.parents
        assert (uid, gid) == (1000, 1000)
        changed.append(path)
        if real_uid in (0, 1000):
            chown(fd, uid, gid)
    monkeypatch.setattr(drain.os, "fchown", destination_chown)
    def copy(**overrides):
        return drain.prepare_recovery_spool(source, target, **(dict(
            deadline=monotonic()+5, max_entries=100, max_bytes=1024**2,
            check=lambda: None) | overrides))
    return source, target, wal, changed, copy


def test_recovery_copy_preserves_source_and_leaves_replay_to_runtime(recovery_copy):
    import hashlib
    source, target, wal, changed, copy = recovery_copy
    before = wal.read_bytes(), wal.stat()
    result = copy()
    copied = target/wal.relative_to(source)
    assert copied.read_bytes() == before[0]
    assert copied.stat().st_mode & 0o777 == 0o600
    assert wal.read_bytes() == before[0] and wal.stat() == before[1]
    assert not list(target.rglob("*.ack.json"))
    assert (wal.parent/"segment.ack.json").exists()
    assert result["copied_bytes"] == len(before[0])
    assert result["copied_files"][0]["sha256"] == hashlib.sha256(before[0]).hexdigest()
    assert not result["final_switch_authorized"] and not result["runtime_activation_authorized"]
    assert changed[-1] == target
    with pytest.raises(RuntimeError, match="new_private_ssd_root"):
        copy()
    assert copied.read_bytes() == before[0]


@pytest.mark.parametrize("failure", ["budget", "unknown", "symlink", "deadline", "writable"])
def test_recovery_copy_refuses_without_changing_original(recovery_copy, monkeypatch, failure):
    from types import SimpleNamespace
    source, target, wal, changed, copy = recovery_copy
    before = wal.read_bytes(), wal.stat()
    overrides = {}
    if failure == "budget": overrides["max_bytes"] = 1
    elif failure == "unknown": (wal.parent/"unfinished.partial").write_text("retain")
    elif failure == "symlink": (source/"spool"/"alias").symlink_to(wal)
    elif failure == "deadline": overrides["deadline"] = monotonic()-1
    else: monkeypatch.setattr(drain.os, "statvfs", lambda _: SimpleNamespace(f_flag=0))
    fds = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(RuntimeError): copy(**overrides)
    assert wal.read_bytes() == before[0] and wal.stat() == before[1]
    assert len(list(Path("/proc/self/fd").iterdir())) == fds


def test_recovery_copy_interruption_preserves_partial_and_refuses_reuse(recovery_copy, monkeypatch):
    source, target, wal, changed, copy = recovery_copy
    write = os.write
    def interrupted(fd, data):
        write(fd, data[:5])
        raise RuntimeError("fixture interrupted write")
    monkeypatch.setattr(drain.os, "write", interrupted)
    before = wal.read_bytes(), wal.stat()
    with pytest.raises(RuntimeError, match="interrupted write"): copy()
    assert (target/wal.relative_to(source)).read_bytes() == before[0][:5]
    assert wal.read_bytes() == before[0] and wal.stat() == before[1]
    with pytest.raises(RuntimeError, match="new_private_ssd_root"): copy()


def test_recovery_copy_source_mutation_cannot_complete(recovery_copy, monkeypatch):
    source, target, wal, changed, copy = recovery_copy
    read = os.read
    altered = False
    def concurrent_write(fd, length):
        nonlocal altered
        chunk = read(fd, length)
        if chunk and not altered:
            altered = True
            with wal.open("ab") as handle: handle.write(b"late source record")
        return chunk
    monkeypatch.setattr(drain.os, "read", concurrent_write)
    with pytest.raises(RuntimeError, match="spool_changed"): copy()
    assert wal.read_bytes().endswith(b"late source record")

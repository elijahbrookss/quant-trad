"""Resource paths use disposable fake files and process namespaces only."""
from dataclasses import replace
from pathlib import Path
from time import monotonic
from unittest.mock import Mock

import pytest

from core.storage_mounts import StorageMountError
from portal.backend.service.storage import header_resources as module
from tests.test_market_data.test_header_filesystem import files


@pytest.fixture
def resources(files):
    (files.pgdata / "pg_wal").mkdir(mode=0o700)
    files.context = {"database_oid": 42, "default_oid": 1663, "catalog_version": 202209061}
    files.spaces = ({"oid": 1663, "name": "pg_default", "location": ""},)
    return files


def inspect(files):
    return module._inspect_paths(files.context, files.spaces, (files.pgdata, 77, 1),
                                 (files.target,), monotonic() + 10)


@pytest.mark.parametrize("raw,expected", [
    ("", ("",)), ('""', ('""',)), ("  foo , BAR ", ("foo", "BAR")),
    ('"a,b", "a""b", pg_default', ('"a,b"', '"a""b"', "pg_default")),
])
def test_temporary_identifier_tokenizer_preserves_server_normalization(raw, expected):
    assert module._temp_tokens(raw) == expected


@pytest.mark.parametrize("raw", ['"unclosed', "x"*4097, "a\x00b", ",".join(["a"]*33)])
def test_unbounded_or_malformed_temporary_settings_refuse(raw):
    with pytest.raises(StorageMountError):
        module._temp_tokens(raw)


def test_default_paths_bind_and_absent_spill_directory_is_not_created(resources):
    before = set(resources.pgdata.rglob("*"))
    bindings, capacity = inspect(resources)
    assert set(resources.pgdata.rglob("*")) == before
    assert {item["role"] for item in bindings} == {
        "database", "wal", "temporary_files", "temporary_relations", "database_default"}
    spool = next(item for item in bindings if item["role"] == "temporary_files")
    assert not spool["exists"]
    assert spool["verified_directory"] == str(resources.pgdata / "base")
    assert all(item["target_id"] == "hdd" for item in bindings)
    assert capacity["hdd"] == resources.capacity


@pytest.mark.parametrize("relative", [True, False])
def test_explicit_wal_link_binds_its_real_directory(resources, relative):
    wal = resources.pgdata / "pg_wal"
    wal.rmdir()
    destination = resources.pgdata.parent / "relocated-wal"
    destination.mkdir(mode=0o700)
    wal.symlink_to(Path("../relocated-wal") if relative else destination, target_is_directory=True)
    bindings, _ = inspect(resources)
    observed = next(item for item in bindings if item["role"] == "wal")
    assert observed["directory"] == str(destination)
    assert observed["verified_directory_inode"] == destination.stat().st_ino


def test_wal_outside_registered_roots_is_refused(resources, tmp_path):
    wal = resources.pgdata / "pg_wal"
    wal.rmdir()
    outside = tmp_path / "unregistered"
    outside.mkdir(mode=0o700)
    wal.symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageMountError, match="unique registered"):
        inspect(resources)


def test_wal_link_must_agree_in_server_namespace(resources, monkeypatch):
    wal = resources.pgdata / "pg_wal"
    wal.rmdir()
    destination = resources.pgdata.parent / "relocated-wal"
    destination.mkdir(mode=0o700)
    wal.symlink_to(destination, target_is_directory=True)
    monkeypatch.setattr(module, "_process_readlink", lambda *args: "/different/wal")
    with pytest.raises(StorageMountError, match="server WAL link differs"):
        inspect(resources)


def test_missing_child_is_checked_in_both_namespaces(resources, monkeypatch):
    monkeypatch.setattr(module, "_process_stat", lambda *args: resources.pgdata.stat())
    with pytest.raises(StorageMountError, match="absent child"):
        inspect(resources)


@pytest.mark.parametrize("kind", ["symlink", "file", "unwritable"])
def test_temporary_child_cannot_redirect_or_hide_unwritable_storage(resources, kind):
    child = resources.pgdata / "base" / "pgsql_tmp"
    if kind == "symlink":
        child.symlink_to(resources.pgdata, target_is_directory=True)
    elif kind == "file":
        child.write_bytes(b"not a directory")
    else:
        child.mkdir(mode=0o500)
    with pytest.raises(StorageMountError):
        inspect(resources)


def test_named_temporary_space_keeps_database_default_fallback(resources):
    directory = resources.pgdata.parent / "custom"
    version = directory / "PG_15_202209061"
    version.mkdir(parents=True, mode=0o700)
    links = resources.pgdata / "pg_tblspc"
    links.mkdir()
    (links / "9000").symlink_to(directory, target_is_directory=True)
    resources.spaces += ({"oid": 9000, "name": "custom", "location": str(directory)},)
    bindings, _ = inspect(resources)
    assert {item["tablespace_oid"] for item in bindings if item["role"] == "temporary_files"} == {1663, 9000}
    assert not (version / "42").exists()
    assert not (version / "pgsql_tmp").exists()


def test_resource_directory_in_different_process_root_is_refused(resources, tmp_path):
    server = tmp_path / "server"
    (server / resources.pgdata.relative_to("/")).mkdir(parents=True)
    link = resources.proc / "77" / "root"
    link.unlink()
    link.symlink_to(server, target_is_directory=True)
    with pytest.raises(StorageMountError, match="different storage"):
        inspect(resources)


@pytest.mark.parametrize("isolation", [None, "REPEATABLE READ"])
def test_observer_requires_live_read_committed_caller_before_queries(isolation):
    conn = Mock()
    conn.in_transaction.return_value = isolation is not None
    conn.get_isolation_level.return_value = isolation
    with pytest.raises(RuntimeError, match="read_committed"):
        module.observe_header_resources(conn, (), pg_controldata=Path("/unused"))
    conn.execute.assert_not_called()


@pytest.mark.parametrize("change", ["directory", "setting"])
def test_changed_resource_observation_is_refused_before_return(resources, monkeypatch, change):
    conn = Mock()
    conn.in_transaction.return_value = True
    conn.get_isolation_level.return_value = "READ COMMITTED"
    conn.execute.return_value.mappings.return_value.one.return_value = {"original": "0", "milliseconds": 0}
    context = {**resources.context, "database_identity": "123456789/42",
        "captured_at": resources.inventory.snapshot.captured_at,
        "data_directory": str(resources.pgdata),
        "started_at": resources.inventory.postmaster_started_at,
        "pidfile": (resources.pgdata / "postmaster.pid").read_text(),
        "temporary_setting": "", "backend_pid": 78}
    calls = 0
    def catalog(query):
        nonlocal calls
        calls += 1
        current = dict(context)
        if calls == 2:
            if change == "directory":
                wal = resources.pgdata / "pg_wal"
                wal.rename(resources.pgdata / "retained-wal")
                wal.mkdir(mode=0o700)
            else:
                current["temporary_setting"] = "different"
        return current, resources.spaces
    monkeypatch.setattr(module, "_catalog", catalog)
    with pytest.raises(StorageMountError, match="changed"):
        module.observe_header_resources(conn, (resources.target,), pg_controldata=resources.binary)

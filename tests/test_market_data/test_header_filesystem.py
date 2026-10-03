"""Use temporary fake PostgreSQL files; never invoke a server or real pg_controldata."""
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.storage_header_placement import HeaderPartitionPlacement, HeaderPlacementSnapshot, RelationPlacement
from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageTarget
from portal.backend.service.storage.header_catalog import HeaderCatalogInventory
from portal.backend.service.storage import header_filesystem as module


@pytest.fixture
def files(tmp_path, monkeypatch):
    disk = tmp_path / "disk"
    pgdata = disk / "pgdata"
    relation = pgdata / "base" / "42" / "101"
    relation.parent.mkdir(parents=True)
    relation.write_bytes(b"disposable header")
    (relation.parent / "102").write_bytes(b"disposable index")
    started = datetime(2026, 9, 17, 6, tzinfo=UTC)
    (pgdata / "PG_VERSION").write_text("15\n")
    (pgdata / "global").mkdir()
    (pgdata / "global" / "pg_control").write_bytes(b"disposable control identity")
    (pgdata / "postmaster.pid").write_text(f"77\n{pgdata}\n{int(started.timestamp())}\n")
    binary = tmp_path / "bin" / "pg_controldata"
    binary.parent.mkdir()
    binary.write_text("not an executable")
    (binary.parent / "postgres").write_text("not an executable")
    proc = tmp_path / "proc"
    (proc / "77").mkdir(parents=True)
    (proc / "77" / "exe").symlink_to(binary.parent / "postgres")
    (proc / "77" / "root").symlink_to(Path("/"), target_is_directory=True)
    (proc / "77" / "cmdline").write_bytes(b"postgres\x00-D\x00disposable\x00")
    monkeypatch.setattr(module, "_PROC_ROOT", proc)
    target = StorageTarget("hdd", "History", "uuid-disposable", str(disk), "hdd")
    capacity = FilesystemEvidence(str(disk), module._device_id(disk.stat().st_dev),
                                  target.filesystem_uuid, 1000, 200, 800, False)
    monkeypatch.setattr(StorageTarget, "inspect", lambda self, **kwargs: capacity)
    calls = []
    def run(arguments, **kwargs):
        calls.append((arguments, kwargs))
        assert kwargs["env"] == {"PATH": module.os.defpath, "LC_ALL": "C", "LANG": "C", "PG_COLOR": "never"}
        assert kwargs["check"] is True and kwargs["timeout"] > 0
        assert "shell" not in kwargs
        return SimpleNamespace(stdout=("pg_controldata (PostgreSQL) 15.15\n"
                                       if arguments[-1] == "--version"
                                       else "Database system identifier: 123456789\n"), stderr="")
    monkeypatch.setattr(module.subprocess, "run", run)
    heap = RelationPlacement(10, 101, "market", "header", None, 100)
    index = RelationPlacement(11, 102, "market", "index", None, 50)
    snapshot = HeaderPlacementSnapshot("123456789/42", date(2026, 9, 17), started,
                                      (HeaderPartitionPlacement(date(2026, 9, 1), heap, (index,), True, True),), True)
    locations = tuple(dict(relation_oid=oid, tablespace_oid=1663,
                           tablespace_name="pg_default", tablespace_location="",
                           database_oid=42, database_default=True,
                           relative_path=f"base/42/{filenode}") for oid, filenode in [(10, 101), (11, 102)])
    inventory = HeaderCatalogInventory(snapshot, locations, str(pgdata), started,
        server_postmaster_identity=tuple((pgdata / "postmaster.pid").read_text().splitlines()[:3]))
    return SimpleNamespace(inventory=inventory, target=target, capacity=capacity,
                           binary=binary, pgdata=pgdata, relation=relation, proc=proc, run=run, calls=calls)


def verify(files, inventory=None):
    return module.verify_header_filesystem(inventory or files.inventory, [files.target],
                                           pg_controldata=files.binary)


def test_default_tablespace_files_bind_to_verified_drive_without_mutation(files):
    before = {path: path.read_bytes() for path in files.pgdata.rglob("*") if path.is_file()}
    result = verify(files)
    assert all(item.target_id == "hdd" for item in result.snapshot.partitions[0].relations)
    assert result.capacity == {"hdd": files.capacity}
    assert [item["relation_oid"] for item in result.bindings] == [10, 11]
    assert all(item["filesystem_uuid"] == files.target.filesystem_uuid for item in result.bindings)
    assert result.snapshot.database_identity == files.inventory.snapshot.database_identity
    assert all(item.target_id is None for item in files.inventory.snapshot.partitions[0].relations)
    assert before == {path: path.read_bytes() for path in files.pgdata.rglob("*") if path.is_file()}
    assert len(files.calls) == 3  # binary version and cluster identity before/after


def test_tablespace_directory_symlink_resolves_but_does_not_change_assignment(files):
    tablespace = Path(files.target.root) / "history"
    data = tablespace / "PG_15_202209061" / "42"
    data.mkdir(parents=True)
    (files.pgdata / "pg_tblspc").mkdir()
    (files.pgdata / "pg_tblspc" / "9000").symlink_to(tablespace, target_is_directory=True)
    for number in (101, 102):
        (data / str(number)).write_bytes(b"historical")
    locations = tuple({**item, "tablespace_oid": 9000, "tablespace_name": "history",
                       "tablespace_location": str(tablespace),
                       "relative_path": f"pg_tblspc/9000/PG_15_202209061/42/{101 + n}"}
                      for n, item in enumerate(files.inventory.physical_locations))
    result = verify(files, replace(files.inventory, physical_locations=locations))
    assert all(Path(item["path"]).is_relative_to(tablespace) for item in result.bindings)


@pytest.mark.parametrize("failure", ["version", "cluster", "diagnostics"])
def test_control_binary_results_must_match_clean_cluster_evidence(files, monkeypatch, failure):
    def run(args, **kwargs):
        result = files.run(args, **kwargs)
        if failure == "version" and args[-1] == "--version":
            result.stdout = "pg_controldata (PostgreSQL) 16.1\n"
        if failure == "cluster" and args[-1] != "--version":
            result.stdout = "Database system identifier: 987654321\n"
        if failure == "diagnostics":
            result.stderr = "control CRC mismatch"
        return result
    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(StorageMountError, match="unverified"):
        verify(files)


@pytest.mark.parametrize("failure", ["start", "directory", "process"])
def test_copied_or_stale_postmaster_files_do_not_prove_active_storage(files, failure):
    if failure == "start":
        inventory = replace(files.inventory, postmaster_started_at=datetime(2026, 9, 16, tzinfo=UTC))
    else:
        inventory = files.inventory
    if failure == "directory":
        (files.pgdata / "postmaster.pid").write_text(f"77\n{files.pgdata.parent}\n1789624800\n")
    if failure == "process":
        (files.proc / "77" / "cmdline").write_bytes(b"unrelated-process\x00")
    with pytest.raises(StorageMountError, match="postmaster"):
        verify(files, inventory)


@pytest.mark.parametrize("change", [
    {"relative_path": "../outside"}, {"relative_path": "base/42/999"},
    {"database_oid": 43}, {"tablespace_location": "/wrong"},
])
def test_catalog_path_cannot_substitute_another_file_or_database(files, change):
    locations = ({**files.inventory.physical_locations[0], **change}, files.inventory.physical_locations[1])
    with pytest.raises(StorageMountError):
        verify(files, replace(files.inventory, physical_locations=locations))


@pytest.mark.parametrize("change", [
    {"filesystem_uuid": "wrong"}, {"read_only": True}, {"device_id": "999:999"},
])
def test_uuid_readonly_and_device_mismatch_refuse_binding(files, monkeypatch, change):
    monkeypatch.setattr(StorageTarget, "inspect", lambda self, **kwargs: replace(files.capacity, **change))
    with pytest.raises(StorageMountError, match="filesystem identity"):
        verify(files)


@pytest.mark.parametrize("kind", ["missing", "symlink", "directory"])
def test_relation_must_be_present_regular_and_not_an_individual_symlink(files, kind):
    files.relation.unlink()
    if kind == "symlink":
        files.relation.symlink_to(files.binary)
    if kind == "directory":
        files.relation.mkdir()
    with pytest.raises(StorageMountError):
        verify(files)


def test_files_outside_registered_root_cannot_be_assigned_by_uuid_alone(files, monkeypatch, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    files.target = replace(files.target, root=str(elsewhere))
    changed = replace(files.capacity, path=str(elsewhere))
    monkeypatch.setattr(StorageTarget, "inspect", lambda self, **kwargs: changed)
    with pytest.raises(StorageMountError, match="outside a unique"):
        verify(files)


def test_replaced_file_during_verification_is_detected(files, monkeypatch):
    calls = 0
    def run(args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            files.relation.rename(files.relation.with_name("retained-original"))
            files.relation.write_bytes(b"replacement")
        return files.run(args, **kwargs)
    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(StorageMountError, match="relation changed"):
        verify(files)


def test_mount_identity_change_during_verification_is_detected(files, monkeypatch):
    calls = 0
    def inspect(self, **kwargs):
        nonlocal calls
        calls += 1
        return files.capacity if calls == 1 else replace(files.capacity, filesystem_uuid="changed")
    monkeypatch.setattr(StorageTarget, "inspect", inspect)
    with pytest.raises(StorageMountError, match="target changed"):
        verify(files)


def test_incomplete_or_duplicate_locations_cannot_bind(files):
    with pytest.raises(ValueError, match="relation inventory"):
        verify(files, replace(files.inventory, physical_locations=files.inventory.physical_locations[:1]))
    duplicate = (files.inventory.physical_locations[0],) * 2
    with pytest.raises(StorageMountError, match="catalog relation identity"):
        verify(files, replace(files.inventory, physical_locations=duplicate))



def test_postmaster_pid_one_is_valid_in_its_container_namespace(files):
    (files.proc / "77").rename(files.proc / "1")
    pidfile = files.pgdata / "postmaster.pid"
    pidfile.write_text(pidfile.read_text().replace("77\n", "1\n", 1))
    files.inventory = replace(files.inventory, server_postmaster_identity=tuple(pidfile.read_text().splitlines()[:3]))
    assert verify(files).snapshot.partitions[0].heap.target_id == "hdd"



def test_worker_and_server_binary_paths_can_differ(files, tmp_path):
    server_binary = tmp_path / "server-root" / "usr" / "local" / "bin" / "postgres"
    server_binary.parent.mkdir(parents=True)
    server_binary.write_bytes(b"server binary")
    process_exe = files.proc / "77" / "exe"
    process_exe.unlink()
    process_exe.symlink_to(server_binary)
    assert verify(files).snapshot.partitions[0].heap.target_id == "hdd"


def test_matching_cluster_ids_do_not_substitute_a_different_namespace_volume(files, tmp_path):
    server_root = tmp_path / "different-server-root"
    different_control = server_root / files.pgdata.relative_to("/") / "global" / "pg_control"
    different_control.parent.mkdir(parents=True)
    different_control.write_bytes((files.pgdata / "global" / "pg_control").read_bytes())
    process_root = files.proc / "77" / "root"
    process_root.unlink()
    process_root.symlink_to(server_root, target_is_directory=True)
    with pytest.raises(StorageMountError, match="different database files"):
        verify(files)


def _destination(files, *, oid=9000, can_create=True):
    from portal.backend.service.storage.header_catalog import HeaderTablespaceObservation
    if oid == 1663:
        name, location = "pg_default", ""
        directory = files.pgdata / "base" / "42"
    else:
        name = "prepared_history"
        location = str(Path(files.target.root) / "prepared")
        directory = Path(location) / "PG_15_202209061"
        directory.mkdir(parents=True)
        (files.pgdata / "pg_tblspc").mkdir(exist_ok=True)
        (files.pgdata / "pg_tblspc" / str(oid)).symlink_to(location, target_is_directory=True)
    inventory = replace(files.inventory, catalog_version=202209061,
        destination_tablespaces=(HeaderTablespaceObservation(oid, name, location, can_create),))
    return inventory, directory


def _verify_destination(files, inventory):
    return module.verify_header_filesystem(inventory, [files.target],
        pg_controldata=files.binary,
        destination_assignments={"hdd": inventory.destination_tablespaces[0].oid})


@pytest.mark.parametrize("oid", [1663, 9000])
def test_prepared_default_and_empty_custom_destination_are_verified_without_creating_data(files, oid):
    inventory, directory = _destination(files, oid=oid)
    before = sorted(directory.iterdir())
    result = _verify_destination(files, inventory)
    destination, = result.destinations
    assert destination.target_id == "hdd"
    assert destination.target_root == files.target.root
    assert destination.tablespace_oid == oid
    assert destination.filesystem_uuid == files.target.filesystem_uuid
    assert destination.directory_inode == directory.stat().st_ino
    assert destination.directory == str(directory.resolve())
    assert sorted(directory.iterdir()) == before
    if oid != 1663:
        assert not (directory / "42").exists()


@pytest.mark.parametrize("change", ["privilege", "role", "inactive", "readonly_directory",
                                    "missing_version", "symlink_version", "wrong_location"])
def test_destination_admission_refuses_unusable_or_changed_directory(files, change):
    inventory, directory = _destination(files)
    if change == "privilege":
        inventory = replace(inventory, destination_tablespaces=(
            replace(inventory.destination_tablespaces[0], can_create=False),))
    elif change == "role":
        files.target = replace(files.target, roles=("archives",))
    elif change == "inactive":
        files.target = replace(files.target, state="draining")
    elif change == "readonly_directory":
        directory.chmod(0o500)
    elif change == "missing_version":
        directory.rmdir()
    elif change == "symlink_version":
        directory.rmdir()
        directory.symlink_to(files.pgdata / "base" / "42", target_is_directory=True)
    else:
        inventory = replace(inventory, destination_tablespaces=(
            replace(inventory.destination_tablespaces[0], location=str(directory.parent / "elsewhere")),))
    with pytest.raises(StorageMountError):
        _verify_destination(files, inventory)


@pytest.mark.parametrize("change", ["missing_assignment", "missing_observation", "global", "catalog_version"])
def test_destination_assignment_requires_matching_bounded_catalog(files, change):
    inventory, _ = _destination(files)
    assignments = {"hdd": 9000}
    if change == "missing_assignment":
        assignments = {}
    elif change == "missing_observation":
        inventory = replace(inventory, destination_tablespaces=())
    elif change == "global":
        assignments = {"hdd": 1664}
    else:
        inventory = replace(inventory, catalog_version=None)
    with pytest.raises(ValueError, match="header_filesystem_invalid"):
        module.verify_header_filesystem(inventory, [files.target],
            pg_controldata=files.binary, destination_assignments=assignments)


def _server_copy(files, root):
    # Hard-linked control/data files prove those exact inodes can be shared while
    # a different subdirectory or mount is visible to the two processes.
    server_data = root / files.pgdata.relative_to("/")
    for path in (files.pgdata / "global" / "pg_control",
                 files.pgdata / "base" / "42" / "101",
                 files.pgdata / "base" / "42" / "102"):
        target = root / path.relative_to("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        module.os.link(path, target)
    process_root = files.proc / "77" / "root"
    process_root.unlink()
    process_root.symlink_to(root, target_is_directory=True)
    return server_data


def test_shared_control_file_does_not_prove_shared_relation_mount(files, tmp_path):
    server = _server_copy(files, tmp_path / "server")
    different = server / "base" / "42" / "101"
    different.unlink()
    different.write_bytes(b"different database mount")
    with pytest.raises(StorageMountError, match="different storage files"):
        verify(files)


def test_absolute_symlink_cannot_escape_process_root_to_fake_file_agreement(files, tmp_path):
    server = _server_copy(files, tmp_path / "server")
    base = server / "base"
    (base / "42" / "101").unlink()
    (base / "42" / "102").unlink()
    (base / "42").rmdir()
    base.rmdir()
    base.symlink_to(files.pgdata / "base", target_is_directory=True)
    with pytest.raises(StorageMountError):
        verify(files)


def test_shared_pgdata_does_not_prove_shared_destination_mount(files, tmp_path):
    inventory, directory = _destination(files)
    root = tmp_path / "server"
    server = _server_copy(files, root)
    (server / "pg_tblspc").mkdir()
    (server / "pg_tblspc" / "9000").symlink_to(directory.parent, target_is_directory=True)
    # A separate inode at the same absolute path in the postmaster namespace.
    (root / directory.relative_to("/")).mkdir(parents=True)
    with pytest.raises(StorageMountError, match="different storage files"):
        _verify_destination(files, inventory)


def test_destination_replaced_during_verification_is_refused(files, monkeypatch):
    inventory, directory = _destination(files)
    calls = 0
    def run(args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            directory.rename(directory.with_name("retained-original"))
            directory.mkdir()
        return files.run(args, **kwargs)
    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(StorageMountError, match="destination changed"):
        _verify_destination(files, inventory)


@pytest.mark.parametrize("oids", [(1664,), (True,), (0,), (2**32,), (1663, 1663), tuple(range(2000, 2033))])
def test_invalid_destination_catalog_request_refuses_before_connecting(oids):
    from portal.backend.service.storage.header_catalog import read_header_catalog
    with pytest.raises(ValueError, match="destination tablespace inventory"):
        read_header_catalog(object(), destination_tablespace_oids=oids)


def test_readonly_server_mount_is_refused_even_when_worker_capacity_is_writable(files, monkeypatch):
    inventory, directory = _destination(files)
    original = module.os.fstatvfs
    def statvfs(handle):
        if module.os.fstat(handle).st_ino == directory.stat().st_ino:
            return SimpleNamespace(f_flag=module.os.ST_RDONLY)
        return original(handle)
    monkeypatch.setattr(module.os, "fstatvfs", statvfs)
    with pytest.raises(StorageMountError, match="privately writable"):
        _verify_destination(files, inventory)


def test_destination_permissions_are_rechecked_before_return(files, monkeypatch):
    inventory, directory = _destination(files)
    calls = 0
    def run(args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            directory.chmod(0o500)
        return files.run(args, **kwargs)
    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(StorageMountError, match="privately writable"):
        _verify_destination(files, inventory)


def test_worker_path_alias_cannot_substitute_a_different_server_catalog_path(files, tmp_path):
    base = files.pgdata / "base"
    alias = Path(files.target.root) / "worker-alias"
    base.rename(alias)
    base.symlink_to(alias, target_is_directory=True)
    server_root = tmp_path / "server"
    server = _server_copy(files, server_root)
    for node in ("101", "102"):
        shared_alias = server_root / alias.relative_to("/") / "42" / node
        shared_alias.parent.mkdir(parents=True, exist_ok=True)
        module.os.link(alias / "42" / node, shared_alias)
    # Both processes can see the worker's resolved file, but PostgreSQL actually
    # opens a different file through the relative path reported by its catalog.
    wrong = server / "base" / "42" / "101"
    wrong.unlink()
    wrong.write_bytes(b"database uses this other file")
    with pytest.raises(StorageMountError, match="different storage files"):
        verify(files)


def test_explicit_single_group_can_bind_files_without_becoming_complete_inventory(files):
    partial = replace(files.inventory,
        snapshot=replace(files.inventory.snapshot, inventory_complete=False),
        group_storage_day=files.inventory.snapshot.partitions[0].storage_day)
    verified = verify(files, partial)
    assert not verified.snapshot.inventory_complete
    assert verified.snapshot.partitions[0].heap.target_id == files.target.target_id
    assert len(verified.bindings) == 2


@pytest.mark.parametrize("mode", ["unmarked", "complete", "wrong_day", "no_group", "invalid_day"])
def test_partial_group_scope_cannot_be_disguised_as_complete_inventory(files, mode):
    partial = replace(files.inventory,
        snapshot=replace(files.inventory.snapshot, inventory_complete=False),
        group_storage_day=files.inventory.snapshot.partitions[0].storage_day)
    if mode == "unmarked":
        partial = replace(partial, group_storage_day=None)
    elif mode == "complete":
        partial = replace(partial, snapshot=replace(partial.snapshot, inventory_complete=True))
    elif mode == "wrong_day":
        partial = replace(partial, group_storage_day=date(2026, 8, 1))
    elif mode == "no_group":
        partial = replace(partial, snapshot=replace(partial.snapshot, partitions=()), physical_locations=())
    else:
        partial = replace(partial, group_storage_day="2026-09-01")
    with pytest.raises(ValueError, match="header_filesystem_invalid"):
        verify(files, partial)
    assert not files.calls


def test_separate_postgresql_startup_clocks_can_cross_seconds(files):
    started = files.inventory.postmaster_started_at + timedelta(seconds=3, microseconds=900000)
    inventory = replace(files.inventory, postmaster_started_at=started,
                        snapshot=replace(files.inventory.snapshot, captured_at=started + timedelta(seconds=1)))
    assert verify(files, inventory).snapshot.partitions[0].heap.target_id == "hdd"


@pytest.mark.parametrize("identity", [(), ("78", "wrong", "1"), ("77", "wrong", "1")])
def test_sql_process_identity_must_match_local_pidfile(files, identity):
    with pytest.raises(StorageMountError, match="SQL/file identity"):
        verify(files, replace(files.inventory, server_postmaster_identity=identity))


def test_future_sql_start_time_is_rejected(files):
    with pytest.raises(StorageMountError, match="start time"):
        verify(files, replace(files.inventory,
                             postmaster_started_at=files.inventory.snapshot.captured_at + timedelta(seconds=1)))

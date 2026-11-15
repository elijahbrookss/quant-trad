from dataclasses import replace
from hashlib import sha256
import json
import os
from pathlib import PurePosixPath

import pytest

from core.storage_mounts import FilesystemEvidence
from market_data.fact_archive import archive_evidence_hash
from scripts.automation import canonical_cold_capacity_checks as capacity


@pytest.fixture
def physical_plan(tmp_path, monkeypatch):
    plan = {
        "schema": capacity.VERSION,
        "rehearsal_sha256": "b" * 64,
        "ssd": {"path": "/var/lib/docker", "filesystem_uuid": "recent-ssd", "reserve_bytes": 200, "temporary_bytes": 500},
        "hdd": {"path": "/history", "filesystem_uuid": "archive-hdd", "reserve_bytes": 100, "temporary_bytes": 250},
    }
    evidence = {
        "/var/lib/docker": FilesystemEvidence("/var/lib/docker", "8:1", "recent-ssd", 1000, 100, 800, False),
        "/history": FilesystemEvidence("/history", "8:2", "archive-hdd", 2000, 200, 1700, False),
        "/history/archives": FilesystemEvidence("/history/archives", "8:2", "archive-hdd", 2000, 200, 1700, False),
    }
    pgdata = "/var/lib/docker/volumes/test-db/_data"

    def inspect(path, *, expected_uuid, require_writable=True):
        native_path = PurePosixPath(path.as_posix()).as_posix()
        if native_path not in evidence:
            raise PermissionError("the host account cannot access database directories")
        observed = evidence[native_path]
        assert observed.filesystem_uuid == expected_uuid
        return observed

    docker_info = {"DockerRootDir": "/var/lib/docker", "OperatingSystem": "Linux", "KernelVersion": "6.8.0"}
    mounts = [{"Type": "volume", "Destination": "/pgdata", "Source": pgdata, "RW": True},
              {"Type": "bind", "Destination": "/qt-history", "Source": "/history", "RW": True}]
    docker = []
    native = {path: (path, os.makedev(8, minor))
              for path, minor in (("/pgdata", 1), ("/pgdata/pg_wal", 1), ("/qt-history/catalog", 2))}

    def read_docker(*args, timeout):
        assert 0 < timeout <= 5
        docker.append(args[0])
        if args[0] == "exec":
            assert args[1:7] == ("--user", "postgres", "c" * 64, "sh", "-ec", capacity._NATIVE_DIRECTORIES)
            assert args[7:] == ("qt-capacity", "/pgdata", "/pgdata/pg_wal", "/qt-history/catalog")
            return "\n".join(f"{path}\n{device}" for path, device in (native[path] for path in args[8:])) + "\n"
        if args[0] == "inspect":
            assert args[-1] == "c" * 64
        return json.dumps({"context": "unix:///var/run/docker.sock", "info": docker_info, "inspect": mounts}[args[0]])

    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    monkeypatch.setattr(capacity.host, "docker", read_docker)
    monkeypatch.setattr(capacity, "inspect_filesystem", inspect)

    def bind(archive="/history/archives", *, changed=False, custody_max_copy=None):
        path = tmp_path / "capacity.json"
        content = json.dumps(plan).encode()
        path.write_bytes(content)
        path.chmod(0o600)
        package = {"scope": {"raw_mapping_index": {"max_copy_bytes": 200}},
                   "artifacts": {"physical_capacity": {"filename": path.name, "sha256": sha256(content).hexdigest()}}}
        if custody_max_copy is not None:
            package["scope"]["custody_move"] = {"max_copy_bytes": custody_max_copy}
        if changed:
            path.write_bytes(content + b" ")
        return capacity.bind_approved_capacity_checks(package_path=tmp_path / "package.json",
                                                      package=package, archive_root=archive, database_container="c" * 64)

    def check(binder, **updates):
        return binder(**({"history_filesystem_uuid": "archive-hdd",
                          "raw_index_bytes": 175, "timeout_seconds": 10,
                          "database_directory": "/pgdata", "history_directory": "/qt-history/catalog"} | updates))

    return plan, evidence, docker_info, docker, bind, check, native, mounts


def test_capacity_check_pins_manifest_and_reports_fresh_physical_headroom(physical_plan):
    _, evidence, _, docker, bind, check, *_ = physical_plan
    binder = bind()
    receipt = check(binder)
    assert receipt["devices"]["ssd"]["required_free_bytes"] == 700
    assert receipt["devices"]["hdd"]["required_free_bytes"] == 350
    assert (receipt["devices"]["ssd"]["reserve_bytes"], receipt["devices"]["ssd"]["temporary_bytes"]) == (200, 500)
    assert (receipt["devices"]["hdd"]["reserve_bytes"], receipt["devices"]["hdd"]["temporary_bytes"]) == (100, 250)
    assert receipt["devices"]["hdd"]["device_id"] == "8:2"
    assert receipt["raw_index_bytes"] == 175
    assert receipt["custody_bytes"] == 0 and receipt["total_copy_bytes"] == 175
    assert receipt.pop("receipt_hash") == archive_evidence_hash(receipt)
    assert receipt["postgresql"] == {"database": "/var/lib/docker/volumes/test-db/_data",
                                      "wal": "/var/lib/docker/volumes/test-db/_data/pg_wal",
                                      "history": "/history/catalog"}
    assert docker == ["context", "info", "inspect", "exec"]
    evidence["/var/lib/docker"] = replace(evidence["/var/lib/docker"], available_bytes=699)
    with pytest.raises(RuntimeError, match="insufficient: tier=ssd available=699 required=700"):
        check(binder)


def test_approved_plan_requires_bound_mounts_and_copy_headroom(physical_plan):
    plan, _evidence, _info, _docker, bind, *_ = physical_plan
    with pytest.raises(RuntimeError, match="approved_plan_changed"):
        bind(changed=True)
    with pytest.raises(ValueError, match="plan_scope_changed"):
        bind(archive="/unapproved/archives")
    with pytest.raises(ValueError, match="plan_scope_changed"):
        bind(custody_max_copy=51)
    binder = bind(custody_max_copy=50)
    receipt = physical_plan[5](binder, custody_bytes=49)
    assert receipt["custody_bytes"] == 49 and receipt["total_copy_bytes"] == 224
    with pytest.raises(RuntimeError, match="custody_copy_budget_exceeded"):
        physical_plan[5](binder, custody_bytes=51)
    plan["hdd"]["temporary_bytes"] = 199
    with pytest.raises(ValueError, match="plan_scope_changed"):
        bind()


def test_refuses_growth_and_qualified_device_changes(physical_plan, monkeypatch):
    _, evidence, info, docker, bind, check, native, mounts = physical_plan
    binder = bind()
    with pytest.raises(RuntimeError, match="raw_index_copy_budget_exceeded"):
        check(binder, raw_index_bytes=201)
    with pytest.raises(RuntimeError, match="history_registration_changed"):
        check(binder, history_filesystem_uuid="other-hdd")
    assert not docker
    with monkeypatch.context() as patch:
        patch.setenv("DOCKER_HOST", "tcp://other-machine:2375")
        with pytest.raises(RuntimeError, match="requires_local_docker"):
            check(binder)
    original_root = info["DockerRootDir"]
    info["DockerRootDir"] = "/unapproved"
    with pytest.raises(RuntimeError, match="docker_root_changed"):
        check(binder)
    info["DockerRootDir"] = original_root
    native["/pgdata/pg_wal"] = ("/qt-history/wal", os.makedev(8, 2))
    with pytest.raises(RuntimeError, match="postgresql_device_changed: member=wal"):
        check(binder)
    native["/pgdata/pg_wal"] = ("/unknown/wal", os.makedev(8, 1))
    with pytest.raises(RuntimeError, match="postgresql_path_unmounted"):
        check(binder)
    native["/pgdata/pg_wal"] = ("/pgdata/pg_wal", os.makedev(8, 1))
    mounts[0]["RW"] = False
    with pytest.raises(RuntimeError, match="postgresql_mount_unqualified"):
        check(binder)
    mounts[0]["RW"] = True
    evidence["/history/archives"] = replace(evidence["/history/archives"], device_id="8:3")
    with pytest.raises(RuntimeError, match="requires_distinct_bound_devices"):
        check(binder)

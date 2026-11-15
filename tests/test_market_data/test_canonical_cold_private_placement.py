"""Proof of the private mount rules and the native registry update."""

from io import BytesIO
import json
from pathlib import Path
import os

import pytest
from sqlalchemy import text

from scripts.db import canonical_cold_private_placement as placement
from scripts.db.canonical_cold_private_seal import PrivateClone
from scripts.db.canonical_cold_raw_index import inspect_raw_mapping_index
from tests.test_market_data.test_canonical_cold_raw_index_db import raw_index_database  # noqa: F401


def _mount_file(monkeypatch, data):
    original_open = Path.open

    def open_file(path, *args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            return BytesIO(data[0].encode())
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_file)


def test_private_identity_pins_roots_wal_and_native_tablespace_link(tmp_path, monkeypatch):
    root = tmp_path / "clone"
    archive = tmp_path / "candidate"
    pgdata, history = root / "pgdata", root / "history"
    for path in (root, archive, archive / "objects", pgdata, history, pgdata / "pg_wal", pgdata / "pg_tblspc"):
        path.mkdir(mode=0o700)
    tablespace = pgdata / "pg_tblspc" / "12345"
    tablespace.symlink_to(history, target_is_directory=True)
    device = f"{os.major(root.stat().st_dev)}:{os.minor(root.stat().st_dev)}"
    udev = tmp_path / "udev"
    udev.mkdir()
    (udev / f"b{device}").write_text("E:ID_FS_UUID=private-test-uuid\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    entries = [f"1 0 {device} /disposable/copy {root} rw - ext4 /dev/example rw\n"
               f"2 0 {device} /disposable/candidate {archive} ro - ext4 /dev/example rw\n"
               f"3 1 {device} /disposable/copy/pgdata {pgdata} rw - ext4 /dev/example rw\n"]
    _mount_file(monkeypatch, entries)
    proof = {"device_id": device, "pgdata_inode": pgdata.stat().st_ino, "tablespace_inode": history.stat().st_ino,
             "tablespace_oid": 12345, "archive_inode": 0, "archive_objects_inode": 0}
    clone = PrivateClone(None, proof, root / "control/socket/.s.PGSQL.5432")
    found = placement._paths(clone, archive, "private-test-uuid", require_writable=True)
    assert found["directories"]["wal"]["inode"] == (pgdata / "pg_wal").stat().st_ino
    assert found["backing"] == {"root": "/disposable/copy", "archive": "/disposable/candidate"}
    alias = PrivateClone(None, proof, root / "alias/.s.PGSQL.5432")
    with pytest.raises(ValueError, match="requires_native_clone_socket_path"):
        placement._paths(alias, archive, "private-test-uuid", require_writable=True)

    original = entries[0]
    entries[0] = original.replace("/disposable/copy/pgdata", "/other/pgdata")
    with pytest.raises(ValueError, match="database_child_mount_alias"):
        placement._paths(clone, archive, "private-test-uuid", require_writable=True)
    entries[0] = original + f"4 1 {device} /other/wal {pgdata / 'pg_wal'} rw - ext4 /dev/example rw\n"
    with pytest.raises(ValueError, match="unexpected_nested_mount"):
        placement._paths(clone, archive, "private-test-uuid", require_writable=True)
    entries[0] = original
    tablespace.unlink()
    tablespace.symlink_to(archive, target_is_directory=True)
    with pytest.raises(ValueError, match="tablespace_link_changed"):
        placement._paths(clone, archive, "private-test-uuid", require_writable=True)


@pytest.mark.db
def test_private_rebind_rolls_back_and_preserves_exact_native_tablespace(raw_index_database, monkeypatch):
    engine, _control, _identity, database, oid = raw_index_database
    target = "raw-index-history"
    with engine.begin() as conn:
        native, tablespace, version, index_oid = conn.execute(text("""
            SELECT pg_tablespace_location(s.oid),s.spcname,c.catalog_version_no,
                   'market.pk_market_raw_archive_record_mapping'::regclass::oid::bigint
            FROM pg_tablespace s CROSS JOIN pg_control_system() c WHERE s.oid=:oid
        """), {"oid": oid}).one()
        root = Path(native).parent
        if Path(native) != root / "history":
            pytest.skip("configure the fixture tablespace directory with a history leaf")
        proof = {"database_identity": database, "accepted_commit_seq": 7, "device_id": "8:0", "tablespace_oid": oid,
                 "archive_inode": 113, "archive_objects_inode": 114,
                 "archive": {"inventory_sha256": "c" * 64, "objects": 5, "bytes": 18}}
        clone = PrivateClone(engine.url, proof, root / "control/socket/.s.PGSQL.5432")
        old_root, old_location = Path("/qt-history"), Path("/qt-history/postgres")
        original = placement._stable_binding(clone, {"filesystem_uuid": "native-fixture", "tablespace_oid": oid,
            "tablespace_name": tablespace, "catalog_version": version}, target_id=target,
            root=str(old_root), location=str(old_location))
        conn.exec_driver_sql("UPDATE public.portal_storage_targets SET root='/qt-history'")
        conn.execute(text("UPDATE public.portal_storage_header_tablespaces SET binding=CAST(:binding AS jsonb)"),
                     {"binding": json.dumps(original)})

    # The fixture uses an actual private tablespace and a disposable loopback
    # connection, without a prior physical recovery or its Docker bind map.
    monkeypatch.setattr(placement, "assert_promoted_clone", lambda *_: None)
    monkeypatch.setattr(placement, "_assert_postmaster_file", lambda *_: None)
    archive = Path("/candidate")
    paths = {"directories": {"root": {"path": str(root)}, "archive": {"inode": 211}, "objects": {"inode": 212}}}
    monkeypatch.setattr(placement, "_paths", lambda *_args, **_kwargs: paths)
    seed = {"schema": placement._SEED_VERSION, "clone_proof_sha256": "a" * 64, "database_identity": database,
            "accepted_commit_seq": 7, "device_id": "8:0", "source_archive_inode": 113, "source_objects_inode": 114,
            "archive_inode": 211, "archive_objects_inode": 212, "catalog_inventory_sha256": "c" * 64,
            "catalog_objects": 5, "catalog_bytes": 18}
    arguments = {"clone": clone, "clone_proof_sha256": "a" * 64, "seed": seed, "seed_sha256": "b" * 64,
                 "archive_root": archive, "filesystem_uuid": "native-fixture", "history_target_id": target,
                 "original_root": old_root, "original_location": old_location}
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn, conn.begin():
        with pytest.raises(ValueError, match="requires_read_committed_transaction"):
            placement.bind_private_placement(conn, **arguments)
    with engine.begin() as conn:
        with pytest.raises(ValueError, match="archive_seed_inode_changed"):
            placement.bind_private_placement(conn, **(arguments | {"seed": seed | {"archive_inode": 999}}))
        with pytest.raises(RuntimeError, match="unreviewed_original_registration"):
            placement.bind_private_placement(conn, **(arguments | {"original_root": Path("/changed")}))
    with pytest.raises(RuntimeError, match="rollback control"):
        with engine.begin() as conn:
            placement.bind_private_placement(conn, **arguments)
            raise RuntimeError("rollback control")
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT root FROM public.portal_storage_targets")) == str(old_root)
        with pytest.raises(RuntimeError, match="registered_history_binding_changed"):
            inspect_raw_mapping_index(conn, expected_database=database, target_id=target)
    with engine.begin() as conn:
        result = placement.bind_private_placement(conn, **arguments)
    with engine.begin() as conn:
        assert placement.bind_private_placement(conn, **arguments) == result
        observed = placement.assert_private_placement(conn, clone=clone, placement=result, require_writable=True)
        assert (observed["root"], observed["location"], observed["tablespace_oid"]) == (str(root), native, oid)
        assert inspect_raw_mapping_index(conn, expected_database=database, target_id=target)["member"]["index_oid"] == index_oid

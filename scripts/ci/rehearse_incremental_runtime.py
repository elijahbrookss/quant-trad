"""Exercise QT's encrypted pairing engine in an owned disposable PG15 cluster.

This is a repository/lock/rotation proof, not full frozen-research acceptance or
a production throughput forecast. No caller DSN, data directory or keys accepted.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import pwd
import secrets
import shutil
import subprocess
import tempfile

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from core.storage_targets import StorageTarget
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, IncrementalRecoveryConfig, _PREPARED,
)
from portal.backend.service.storage.recovery_copies import _json_write, _identity
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME


def rehearse(*, pg_bin, pgbackrest, restic):
    if os.geteuid() == 0:
        raise RuntimeError("disposable_runtime_requires_nonroot")
    root = Path(tempfile.mkdtemp(prefix="qt-incremental-runtime-", dir="/tmp"))
    root.chmod(0o700)
    old_udev = os.environ.get("QT_STORAGE_UDEV_ROOT")
    engine = None
    clusters = []
    env = {"PATH": str(pg_bin)+os.pathsep+os.defpath, "HOME": str(root), "LC_ALL": "C"}
    keys = [secrets.token_hex(32), secrets.token_hex(32)]
    def run(command):
        result = subprocess.run([str(a) for a in command], env=env,
                                capture_output=True, text=True, timeout=120)
        if result.returncode:
            message = result.stderr[-3000:] + result.stdout[-3000:]
            for key in keys:
                message = message.replace(key, "[redacted]")
            raise RuntimeError("disposable_command_failed:"+message)
        return result.stdout
    def start(data, socket, *, restored=False):
        clusters.append(data)
        options = f"-c listen_addresses= -c unix_socket_directories={socket} -c shared_buffers=16MB"
        if restored:
            options += " -c archive_mode=off"
        run([pg_bin/"pg_ctl", "-D", data, "-l", root/(data.name+".log"),
             "-w", "-t", "40", "-o", options, "start"])
    try:
        data, socket, hdd = root/"source", root/"socket", root/"hdd"
        for directory in (socket, hdd, root/"udev"):
            directory.mkdir(mode=0o700)
        dev = hdd.stat().st_dev
        (root/"udev"/f"b{os.major(dev)}:{os.minor(dev)}").write_text("E:ID_FS_UUID=uuid-incremental-test\n")
        os.environ["QT_STORAGE_UDEV_ROOT"] = str(root/"udev")
        for name, key in zip(("database-key", "archive-key"), keys):
            (root/name).write_text(key)
            (root/name).chmod(0o600)
        run([pg_bin/"initdb", "-D", data, "--no-locale", "--auth-local=trust",
             "--auth-host=reject", "--data-checksums"])
        # Archiver configuration only applies to this generated cluster.
        with (data/"postgresql.conf").open("a") as out:
            out.write("\narchive_mode=on\nwal_level=replica\nmax_wal_size='128MB'\n")
        start(data, socket)
        username = pwd.getpwuid(os.getuid()).pw_name
        url = URL.create("postgresql+psycopg2", username=username, host=str(socket), database="postgres")
        engine = create_engine(url)
        with engine.begin() as conn:
            conn.exec_driver_sql("""
                CREATE SCHEMA market;
                CREATE TABLE market.fact_storage_state
                  (layout_version text, state text, completed_at timestamptz, evidence jsonb);
                INSERT INTO market.fact_storage_state VALUES
                  ('market.fact_storage_tiers.v2','ready',now(),'{}');
                CREATE TABLE market.storage_lifecycle_events
                  (action text,event_type text,target_kind text,target_id text);
                CREATE TABLE market.raw_archive_manifests
                  (id text PRIMARY KEY,object_key text,object_sha256 text,byte_count bigint);
                CREATE TABLE market.book_checkpoint_manifests
                  (LIKE market.raw_archive_manifests INCLUDING ALL);
                CREATE TABLE market.fact_archive_manifests
                  (LIKE market.raw_archive_manifests INCLUDING ALL);
                CREATE TABLE observations(id int PRIMARY KEY,payload text);
                INSERT INTO observations SELECT n,md5(n::text) FROM generate_series(1,1000) n;
            """)
            identity = _identity(conn)[0]
        config = IncrementalRecoveryConfig(pgbackrest, restic, data, socket,
                    root/"database-key", root/"archive-key", max_chain_backups=2)
        target = StorageTarget("hdd", "Disposable", "uuid-incremental-test", str(hdd), "hdd")
        recovery = hdd/"recovery-incremental"/hashlib.sha256(identity.encode()).hexdigest()[:32]
        recovery.mkdir(mode=0o700, parents=True)
        recovery.parent.chmod(0o700)
        for name in ("database", "archives", "locks", "logs"):
            (recovery/name).mkdir(mode=0o700)
        _json_write(recovery/"prepared.json", {
            "schema_version":_PREPARED, "database_identity":identity,
            "filesystem_uuid":target.filesystem_uuid,
            "database_key_sha256":hashlib.sha256(keys[0].encode()).hexdigest(),
            "archive_key_sha256":hashlib.sha256(keys[1].encode()).hexdigest(),
        })
        archive_config = root/"archiver.conf"
        archive_config.write_text(
            f"[global]\nrepo1-path={recovery/'database'}\nrepo1-cipher-type=aes-256-cbc\n"
            f"repo1-cipher-pass={keys[0]}\nlock-path={recovery/'locks'}\n"
            f"log-path={recovery/'logs'}\nlog-level-file=off\n"
            f"[qt]\npg1-path={data}\npg1-socket-path={socket}\npg1-user={username}\n")
        archive_config.chmod(0o600)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            command = f"{pgbackrest} --config={archive_config} --stanza=qt archive-push %p"
            from psycopg2 import sql
            with conn.connection.driver_connection.cursor() as cursor:
                cursor.execute(sql.SQL("ALTER SYSTEM SET archive_command = {}").format(sql.Literal(command)))
            conn.exec_driver_sql("SELECT pg_reload_conf()")
        def manager(cls=EncryptedRecoveryCopies, **overrides):
            return cls(incremental=config, connection_url=url, target=target,
                       database_identity=identity, max_bytes=256*1024**2,
                       reserve_bytes=1024**2, timeout_seconds=120,
                       max_objects=100, **overrides)
        class SetupDiagnostics(EncryptedRecoveryCopies):
            def _run(self, command):
                try:
                    return super()._run(command)
                except RuntimeError as exc:
                    # Diagnostic re-read only: never replay backup/expiry/restore.
                    raise RuntimeError(str(exc)+": command="+repr(command)) from exc
        initial = manager(SetupDiagnostics)
        initial._run(initial._br("stanza-create"))
        initial._run(initial._rs("init"))
        objects = FilesystemRawArchiveObjectStore(root/"objects")
        def add_object(name):
            payload = ("immutable "+name).encode()
            (objects.root/name).write_bytes(payload)
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO market.raw_archive_manifests VALUES (:n,:n,:h,:s)"),
                             {"n":name,"h":hashlib.sha256(payload).hexdigest(),"s":len(payload)})
        add_object("first")
        @contextmanager
        def snapshot():
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text("SELECT pg_advisory_lock_shared(hashtextextended(:n,0))"),
                             {"n":_LIFECYCLE_LOCK_NAME})
                conn.commit()
                try:
                    conn.execution_options(isolation_level="REPEATABLE READ")
                    with Session(bind=conn) as session:
                        yield session
                        session.commit()
                finally:
                    if conn.in_transaction():
                        conn.rollback()
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    conn.execute(text("SELECT pg_advisory_unlock_shared(hashtextextended(:n,0))"),
                                 {"n":_LIFECYCLE_LOCK_NAME})
                    conn.commit()
        class ConcurrentAdmission(EncryptedRecoveryCopies):
            def _run(self, command):
                if command[-1] == "backup" and str(command[0]) == str(self.pgbackrest):
                    with engine.connect() as contender:
                        assert not contender.scalar(text(
                            "SELECT pg_try_advisory_lock(hashtextextended(:n,0))"),
                            {"n":_LIFECYCLE_LOCK_NAME})
                    # A manifest admitted after the fence but before physical
                    # completion must be present in its matching archive snapshot.
                    add_object("during-backup")
                return super()._run(command)
        with snapshot() as session:
            first = manager(ConcurrentAdmission).create(session, objects=objects, keep_copies=2)
        assert first["archive_objects"] == 2
        with engine.begin() as conn:
            conn.exec_driver_sql("INSERT INTO observations VALUES (1001,'incremental')")
        with snapshot() as session:
            second = manager().create(session, objects=objects, keep_copies=2)
        assert first["database_type"] == "full" and second["database_type"] == "incr"
        # A failed archive half leaves the earlier paired recovery points intact.
        class FailArchives(EncryptedRecoveryCopies):
            def _run(self, command):
                if str(command[0]) == str(self.restic) and "backup" in command:
                    raise RuntimeError("injected_archive_failure")
                return super()._run(command)
        try:
            with snapshot() as session:
                manager(FailArchives).create(session, objects=objects, keep_copies=2)
        except RuntimeError as exc:
            assert str(exc) == "injected_archive_failure"
        else:
            raise AssertionError("failure was not exercised")
        assert [r[2]["name"] for r in manager().completed()] == [first["name"], second["name"]]
        # Renewal/rotation leaves two points and their necessary native chain.
        with snapshot() as session:
            third = manager().create(session, objects=objects, keep_copies=2)
        class InterruptRetirement(EncryptedRecoveryCopies):
            def _run(self, command):
                result = super()._run(command)
                if str(command[0]) == str(self.restic) and "forget" in command:
                    raise RuntimeError("injected_retirement_interruption")
                return result
        try:
            with snapshot() as session:
                manager(InterruptRetirement).create(session, objects=objects, keep_copies=2)
        except RuntimeError as exc:
            assert str(exc) == "injected_retirement_interruption"
        else:
            raise AssertionError("retirement interruption was not exercised")
        current = manager()
        fourth = current.completed()[-1][2]
        assert list(current.root.glob(".copy_*"))
        with current.lock():
            current._prune(2)
        assert [r[2]["name"] for r in current.completed()] == [third["name"], fourth["name"]]
        assert not (current.root/first["name"]).exists()
        assert not list(current.root.glob(".copy_*"))
        labels = {b["label"] for b in current._native_backups()}
        assert first["database_label"] not in labels and second["database_label"] not in labels
        # Restore the selected complete pair; later source writes stay outside.
        with engine.begin() as conn:
            conn.exec_driver_sql("INSERT INTO observations VALUES (999999,'after recovery point')")
        restored, restored_socket = root/"restored", root/"restored-socket"
        restored_socket.mkdir(mode=0o700)
        current._run(current._br("--pg1-path="+str(restored), "--set="+fourth["database_label"],
                                "--type=immediate", "--target-action=promote",
                                "--archive-mode=off", "restore"))
        start(restored, restored_socket, restored=True)
        restored_engine = create_engine(url.set(host=str(restored_socket)))
        try:
            with restored_engine.connect() as conn:
                assert conn.scalar(text("SELECT count(*) FROM observations")) == 1001
                rows = conn.execute(text(
                    "SELECT object_key,object_sha256 FROM market.raw_archive_manifests ORDER BY id")).all()
        finally:
            restored_engine.dispose()
        destination = root/"restored-archives"
        current._run(current._rs("restore", fourth["archive_snapshot"], "--target", str(destination)))
        for name, digest in rows:
            recovered = destination/str(objects.root).lstrip("/")/name
            assert hashlib.sha256(recovered.read_bytes()).hexdigest() == digest
        inventory = destination/fourth["inventory_snapshot_path"].lstrip("/")
        assert hashlib.sha256(inventory.read_bytes()).hexdigest() == fourth["inventory_sha256"]
        assert len(rows) == 2
        with engine.connect() as conn:
            assert conn.scalar(text("SELECT count(*) FROM observations")) == 1002
        print(json.dumps({
            "schema_version":"qt.incremental_runtime_rehearsal.v1",
            "production_touched":False,
            "paired_backup_and_restore":True,
            "post_fence_archive_admission_preserved":True,
            "archive_expiry_excluded":True,
            "failed_archive_half_preserved_completed_points":True,
            "paired_rotation_preserved_dependencies":True,
            "interrupted_retirement_reconciled_without_new_backup":True,
            "later_writes_excluded":True,
            "full_qt_frozen_reader_qualified":False,
            "points":[{"type":r["database_type"],"seconds":r["elapsed_seconds"]}
                      for r in (first,second,third,fourth)],
        },sort_keys=True))
    finally:
        if engine is not None:
            engine.dispose()
        for data in reversed(clusters):
            if (data/"postmaster.pid").exists():
                run([pg_bin/"pg_ctl","-D",data,"-m","fast","-w","-t","40","stop"])
        if old_udev is None:
            os.environ.pop("QT_STORAGE_UDEV_ROOT",None)
        else:
            os.environ["QT_STORAGE_UDEV_ROOT"] = old_udev
        if root.resolve(strict=True) != root or root.parent != Path("/tmp") or not root.name.startswith("qt-incremental-runtime-"):
            raise RuntimeError("disposable_cleanup_path_changed")
        shutil.rmtree(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin",type=Path,required=True)
    parser.add_argument("--pgbackrest",type=Path,required=True)
    parser.add_argument("--restic",type=Path,required=True)
    args = parser.parse_args()
    rehearse(pg_bin=args.pg_bin,pgbackrest=args.pgbackrest,restic=args.restic)

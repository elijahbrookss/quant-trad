"""Cross-container native sidecars for the disposable signed receiver test."""

from contextlib import contextmanager
import os
from pathlib import Path
from types import SimpleNamespace
from time import monotonic, sleep

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.verify_canonical_recovery_archive import _path
from scripts.ci.materialize_paused_archive_suffix import _copy_catalog, _publish_catalog_inventory, _scope_directory
from scripts.ci.verify_paused_recovery_pair import _probe
from scripts.db.canonical_cold_recovery_suffix import prepare_archive_baseline


ATTEMPT = Path("/qt-restore")
CONTROL = ATTEMPT / "rolling-control"
SOCKET = ATTEMPT / "receiver-workspace" / "rolling-socket"
PORT = 55432


def _publish(path, text):
    staged = path.with_name("." + path.name)
    with staged.open("x", encoding="ascii") as stream:
        stream.write(text + "\n")
    assert not path.exists()
    staged.rename(path)


def _await(path, check, *, timeout=65):
    deadline = monotonic() + timeout
    while not path.is_file():
        check()
        if monotonic() >= deadline:
            raise TimeoutError(f"fixture native sidecar did not publish {path.name}")
        sleep(0.1)


class NativeReplayedFixture:
    def __init__(self, *, database, role, pgdata):
        assert pgdata == ATTEMPT / "receiver-workspace" / "pgdata"
        assert database.startswith("qt_migration_") and role == "postgres"
        assert all(ch.isalnum() or ch == "_" for ch in database)
        CONTROL.mkdir(mode=0o700)
        SOCKET.mkdir(mode=0o700)
        _publish(CONTROL / "pg_hba.conf", f"local postgres {role} trust\nlocal {database} {role} trust")
        self.url = URL.create("postgresql+psycopg2", username=role, port=PORT, database=database,
                              query={"host": str(SOCKET)})
        self.pgdata = pgdata
        self.pgdata_identity = self._identity(pgdata)

    @staticmethod
    def _identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino

    @contextmanager
    def boot(self, pgdata, check, *, target=None, expected_replay=None):
        phase = "prefix" if target is not None else "final"
        assert pgdata == self.pgdata and self._identity(pgdata) == self.pgdata_identity
        signal_identity = self._identity(pgdata / "recovery.signal")
        _publish(CONTROL / f"{phase}.start", target or "start")
        _await(CONTROL / f"{phase}.started", check)
        admin = create_engine(self.url.set(database="postgres"), poolclass=NullPool, connect_args={"connect_timeout": 2})
        database = create_engine(self.url, poolclass=NullPool, connect_args={"connect_timeout": 2})
        deadline = monotonic() + 35
        observed = "no SQL connection"
        try:
            while True:
                check()
                if monotonic() >= deadline:
                    raise TimeoutError(f"fixture {phase} native replay did not reach its endpoint: {observed}")
                try:
                    with admin.connect() as conn:
                        state = conn.exec_driver_sql("""
                            SELECT pg_is_in_recovery(),
                                   CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() ELSE 'promoted' END,
                                   pg_last_wal_replay_lsn()::text,
                                   inet_server_addr() IS NULL AND current_setting('listen_addresses')='',
                                   current_setting('data_directory')
                        """).one()
                except OperationalError as error:
                    observed = f"OperationalError {getattr(error.orig, 'pgcode', None)}"
                    sleep(0.1)
                    continue
                observed = repr(tuple(state))
                assert state[3:] == (True, str(pgdata))
                if ((phase == "prefix" and state[:3] == (True, "paused", expected_replay))
                        or (phase == "final" and not state[0])):
                    break
                sleep(0.1)
            yield admin, database
        except BaseException:
            native_log = CONTROL / f"{phase}.postgres.log"
            if native_log.exists():
                print(f"QT_NATIVE_{phase.upper()}_LOG\n" + "\n".join(native_log.read_text().splitlines()[-25:]))
            settings = (pgdata / "postgresql.auto.conf").read_text().splitlines()
            print("QT_NATIVE_RECOVERY_SETTINGS", [line for line in settings if line.startswith("recovery_target")])
            raise
        finally:
            admin.dispose()
            database.dispose()
            _publish(CONTROL / f"{phase}.stop", "stop")
            _await(CONTROL / f"{phase}.stopped", check)
        assert self._identity(pgdata) == self.pgdata_identity
        if phase == "prefix":
            assert self._identity(pgdata / "recovery.signal") == signal_identity
            assert not (pgdata / "backup_label").exists()
            print(f"QT_SIGNED_REPLAYED_PREFIX_STOP pgdata={self.pgdata_identity} recovery_signal={signal_identity}")
        else:
            assert not (pgdata / "recovery.signal").exists()
            print(f"QT_SIGNED_REPLAYED_FINAL_STOP pgdata={self.pgdata_identity}")

    def stopped_control(self):
        assert not (self.pgdata / "postmaster.pid").exists()
        return (CONTROL / "stopped-control").read_text(encoding="ascii"), (CONTROL / "stopped-control.sha256").read_text().strip()


def materialize_fixture_archive(conn, *, pgdata, archive_root, source_objects, base_restore, pair,
                                pair_receipt, pair_receipt_sha256, tablespace, tablespace_oid, role_oid,
                                target, replay, check):
    """Use the exact catalog/codec in the integrated receiver's private workspace."""
    archive_root.mkdir(mode=0o700)
    args = SimpleNamespace(database=conn.engine.url.database, role=conn.engine.url.username, role_oid=role_oid,
                           timeline=1, pgdata=pgdata, socket=SOCKET, minimum_replay_lsn=replay,
                           tablespace_name="qt_history_hdd-01", tablespace=tablespace, tablespace_oid=tablespace_oid,
                           max_objects=100, max_work_bytes=64 * 1024**2, reserve_bytes=0, source_archive=source_objects)

    def snapshot(conn):
        proof = _probe(conn, args, pair, target_lsn=target, replay_lsn=replay)
        return {"database_identity": proof["database_identity"], "storage_layout": proof["storage_layout"],
                "timeline": 1, "target_lsn": target, "replay_lsn": replay,
                "accepted_commit_seq": PostgresMarketDataRepository._current_commit_seq_with_session(conn)}

    baseline = prepare_archive_baseline(pair_receipt, base_restore, expected_receipt_sha256=pair_receipt_sha256,
                                       max_objects=args.max_objects, check=check)
    with archive_namespace(archive_root, exclusive=True) as held:
        paused = snapshot(conn)
        scope = paused | {"schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": pair_receipt_sha256,
                          "materialize_baseline": True, "pgdata": str(pgdata), "pgdata_inode": pgdata.stat().st_ino,
                          "suffix_inode": archive_root.stat().st_ino}
        objects, work = _scope_directory(archive_root, scope)
        with (work / "catalog.jsonl").open("xb") as manifest:
            copied = _copy_catalog(conn, args, baseline, objects, work, check, check, baseline_bytes=pair["archive_bytes"],
                                   baseline_source=_path(base_restore, pair["archive_source_root"], absolute=True),
                                   catalog_inventory=manifest)
            manifest.flush()
            os.fsync(manifest.fileno())
        assert snapshot(conn) == paused
        held()
        inventory = _publish_catalog_inventory(archive_root, work, copied["catalog_inventory_sha256"], check)
    receipt = scope | copied | {"suffix_root": str(objects), "catalog_inventory_path": str(inventory)}
    return paused, receipt | {"verification_sha256": archive_evidence_hash(receipt)}, inventory, objects

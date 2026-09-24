"""Disposable encrypted PostgreSQL + archive recovery experiment.

Creates its own cluster, Unix sockets and test secrets under a fresh /tmp root.
Never reads PG_DSN, dotenv or production configuration. Does not qualify QT's
application recovery, retention scheduler, production performance or deployment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import secrets
import signal
import shutil
import subprocess
import tempfile
import time


def rehearse(*, pg_bin: Path, pgbackrest: Path, restic: Path,
             require_timescale: bool = False) -> dict:
    if os.geteuid() == 0:
        raise RuntimeError("run_disposable_recovery_as_nonroot")
    for binary in (pg_bin / "initdb", pg_bin / "pg_ctl", pg_bin / "psql", pgbackrest, restic):
        if not binary.is_absolute() or not binary.is_file():
            raise ValueError("absolute_recovery_tool_required")
    root = Path(tempfile.mkdtemp(prefix="qt-incremental-proof-", dir="/tmp"))
    os.chmod(root, 0o700)
    env = {"PATH": str(pg_bin) + os.pathsep + os.defpath, "HOME": str(root), "LC_ALL": "C"}
    timings = {}
    live_clusters: list[Path] = []
    report = {"schema_version": "qt.incremental_recovery_experiment.v1",
              "production_touched": False, "application_qualified": False}
    started = time.monotonic()

    def run(args, *, name=None, expected=0, extra_env=None, timeout=120):
        before = time.monotonic()
        result = subprocess.run([str(a) for a in args], env={**env, **(extra_env or {})},
                                cwd=root, input="", text=True, capture_output=True, timeout=timeout)
        if name:
            timings[name] = round(time.monotonic() - before, 3)
        if result.returncode != expected:
            # Disposable credentials only, but keep generated cipher keys out of diagnostics.
            message = result.stderr[-4000:] + result.stdout[-4000:]
            for secret in (db_key, archive_key):
                message = message.replace(secret, "[redacted]")
            raise RuntimeError(f"disposable_command_failed:{args[0]}:{result.returncode}:{message}")
        return result

    db_key, archive_key = secrets.token_hex(32), secrets.token_hex(32)
    source, restored = root / "source", root / "restored"
    socket, restore_socket = root / "socket", root / "restore-socket"
    objects, repo, archive_repo = root / "objects", root / "database-repo", root / "archive-repo"
    history, restored_history = root / "history", root / "restored-history"
    config, password_file = root / "pgbackrest.conf", root / "archive-key"
    for directory in (socket, restore_socket, objects, repo, history, root / "locks", root / "logs"):
        directory.mkdir(mode=0o700)
    password_file.write_text(archive_key)
    password_file.chmod(0o600)
    config.write_text(
        "[global]\n"
        f"repo1-path={repo}\nrepo1-cipher-type=aes-256-cbc\nrepo1-cipher-pass={db_key}\n"
        "repo1-bundle=y\nrepo1-block=y\nrepo1-retention-full=2\n"
        "expire-auto=n\narchive-copy=y\narchive-check=y\n"
        "compress-type=zst\nprocess-max=1\nstart-fast=y\n"
        f"lock-path={root / 'locks'}\nlog-path={root / 'logs'}\nlog-level-console=warn\n"
        f"[qt]\npg1-path={source}\npg1-socket-path={socket}\n"
        f"pg1-user={pwd.getpwuid(os.getuid()).pw_name}\npg1-database=postgres\n"
    )
    config.chmod(0o600)
    br = [pgbackrest, "--config=" + str(config), "--stanza=qt"]
    rs = [restic, "--repo", archive_repo, "--password-file", password_file,
          "--cache-dir", root / "cache", "--json"]

    def sql(query, *, target_socket=socket):
        return run([pg_bin / "psql", "-h", target_socket, "-d", "postgres", "-At",
                    "-v", "ON_ERROR_STOP=1", "-c", query]).stdout.strip()

    def start(cluster, sock, *, restore=False):
        options = f"-c listen_addresses='' -c unix_socket_directories={sock}"
        if restore:
            options += " -c archive_mode=off"
        # Track before startup so timeout/failure cleanup also stops a starting postmaster.
        live_clusters.append(cluster)
        run([pg_bin / "pg_ctl", "-D", cluster, "-l", root / (cluster.name + ".log"),
             "-w", "-t", "40", "-o", options, "start"], timeout=50)

    def capture(backup_type, *, phase=None):
        phase = phase or backup_type
        run([*br, "--type=" + backup_type, "backup"], name=phase + "_database_seconds")
        info = json.loads(run([*br, "--output=json", "info"]).stdout)[0]
        backup = info["backup"][-1]
        assert backup["type"] == backup_type
        archive_result = run([*rs, "backup", "--host", "qt-disposable", "--tag", backup["label"],
                              "objects"], name=phase + "_archives_seconds")
        summary = next(json.loads(line) for line in archive_result.stdout.splitlines()
                       if json.loads(line).get("message_type") == "summary")
        # This pair is only a fixture receipt, not a production recovery certificate.
        return {"database": backup, "archive_snapshot": summary["snapshot_id"],
                "archive_summary": summary}

    try:
        version = run([pg_bin / "postgres", "--version"]).stdout.strip()
        if not version.startswith("postgres (PostgreSQL) 15."):
            raise RuntimeError("experiment_requires_postgresql15")
        report["tools"] = {"postgres": version,
                           "pgbackrest": run([pgbackrest, "version"]).stdout.strip(),
                           "restic": run([restic, "version"]).stdout.strip()}
        run([pg_bin / "initdb", "-D", source, "--auth-local=trust", "--auth-host=reject",
             "--no-locale", "--data-checksums"], name="initialize_seconds")
        with (source / "postgresql.conf").open("a") as out:
            command = f'{pgbackrest} --config={config} --stanza=qt archive-push %p'
            out.write("\nshared_buffers='32MB'\nmax_connections=10\narchive_mode=on\n"
                      "wal_level=replica\nmax_wal_size='128MB'\n"
                      f"archive_command='{command}'\n")
        if require_timescale:
            with (source / "postgresql.conf").open("a") as out:
                out.write("\\nshared_preload_libraries='timescaledb'\\n")
        start(source, socket)
        if require_timescale:
            sql("CREATE EXTENSION timescaledb")
            report["timescale_version"] = sql(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
            sql("CREATE TABLE extension_probe(at timestamptz NOT NULL, value integer);"
                "SELECT create_hypertable('extension_probe','at');"
                "INSERT INTO extension_probe VALUES ('2026-09-01T00:00:00Z',1),"
                "('2026-09-03T00:00:00Z',2);")
        run([*br, "stanza-create"])
        run([*br, "check"])
        run([*rs, "init"])
        first = b"frozen evidence survives independently of current corrections\n"
        second = b"second archive admitted after the baseline\n"
        (objects / "frozen.bin").write_bytes(first)
        first_hash = hashlib.sha256(first).hexdigest()
        sql(f"CREATE TABLESPACE history LOCATION '{history}'")
        sql("CREATE TABLE observations(id bigint PRIMARY KEY, payload text NOT NULL);"
            "INSERT INTO observations SELECT i, repeat(md5(i::text), 32) "
            "FROM generate_series(1,20000) AS i;"
            "CREATE TABLE historical(id bigint PRIMARY KEY, payload text NOT NULL) TABLESPACE history;"
            "ALTER INDEX historical_pkey SET TABLESPACE history;"
            "INSERT INTO historical SELECT * FROM observations WHERE id <= 10000;"
            "CREATE TABLE archive_refs(name text PRIMARY KEY, sha256 text NOT NULL);"
            f"INSERT INTO archive_refs VALUES ('frozen.bin','{first_hash}');"
            "CREATE TABLE frozen AS SELECT * FROM observations WHERE id <= 10;")
        full = capture("full")
        # Retain baseline recovery, add an archive and correct current rows.
        (objects / "new.bin").write_bytes(second)
        second_hash = hashlib.sha256(second).hexdigest()
        sql("UPDATE observations SET payload='corrected' WHERE id=1;"
            "INSERT INTO observations SELECT i, repeat(md5(i::text),32) "
            "FROM generate_series(20001,20100) AS i;"
            f"INSERT INTO archive_refs VALUES ('new.bin','{second_hash}');")
        expected = sql("SELECT count(*)||':'||md5(string_agg(id::text||payload,',' ORDER BY id)) "
                       "FROM observations")
        expected_frozen = sql("SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM frozen")
        incremental = capture("incr")
        assert incremental["database"].get("prior") == full["database"]["label"]
        # Interrupt a deliberately throttled archive backup. Existing completed
        # snapshots must remain intact and no partial snapshot may be published.
        before_snapshots = json.loads(run([*rs, "snapshots"]).stdout)
        interrupted_file = objects / "interrupted.bin"
        interrupted_file.write_bytes(os.urandom(4 * 1024 * 1024))
        interrupted = subprocess.Popen(
            [str(a) for a in [*rs, "--limit-upload", "128", "backup", "--host",
                             "qt-disposable", "--tag", "interrupted-attempt", "objects"]],
            env=env, cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            time.sleep(2)
            if interrupted.poll() is not None:
                raise RuntimeError("interruption_fixture_finished_before_signal")
            os.killpg(interrupted.pid, signal.SIGKILL)
            assert interrupted.wait(timeout=15) != 0
        finally:
            if interrupted.poll() is None:
                os.killpg(interrupted.pid, signal.SIGKILL)
                interrupted.wait(timeout=10)
        interrupted_file.unlink()
        run([*rs, "unlock"])
        after_snapshots = json.loads(run([*rs, "snapshots"]).stdout)
        assert {s["id"] for s in after_snapshots} == {s["id"] for s in before_snapshots}
        report["interrupted_archive_backup_preserved_recovery_points"] = True
        # Kill a real physical backup after PostgreSQL has entered backup mode.
        # A process that completes before the signal is a failed experiment,
        # never evidence that interruption was exercised.
        completed_labels = {b["label"] for b in
                            json.loads(run([*br, "--output=json", "info"]).stdout)[0]["backup"]}
        interrupt_log = root / "database-interruption.log"
        with interrupt_log.open("w") as out:
            physical = subprocess.Popen(
                [str(a) for a in [*br, "--type=incr", "--no-resume",
                                 "--log-level-console=info", "backup"]],
                env=env, cwd=root, stdin=subprocess.DEVNULL, stdout=out,
                stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while "backup start archive =" not in interrupt_log.read_text():
                    if physical.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("database_interruption_boundary_not_reached")
                    time.sleep(0.01)
                if physical.poll() is not None:
                    raise RuntimeError("database_backup_completed_before_interruption")
                os.killpg(physical.pid, signal.SIGKILL)
                assert physical.wait(timeout=15) != 0
            finally:
                if physical.poll() is None:
                    os.killpg(physical.pid, signal.SIGKILL)
                    physical.wait(timeout=10)
        assert {b["label"] for b in
                json.loads(run([*br, "--output=json", "info"]).stdout)[0]["backup"]} == completed_labels
        report["interrupted_database_backup_preserved_recovery_points"] = True
        # Writes after the selected endpoint must NOT leak into its restore.
        sql("INSERT INTO observations VALUES (999999,'after selected backup');")
        report["source_kept_running"] = sql("SELECT count(*) FROM observations") == "20101"
        wrong = subprocess.run(
            [str(a) for a in [*br, "check"]],
            env={**env, "PGBACKREST_REPO1_CIPHER_PASS": secrets.token_hex(32)},
            cwd=root, stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
        assert wrong.returncode != 0
        report["wrong_database_key_refused"] = True
        wrong_file = root / "wrong-key"
        wrong_file.write_text(secrets.token_hex(32))
        wrong_file.chmod(0o600)
        rejected = subprocess.run([str(restic), "--repo", str(archive_repo), "--password-file",
                                   str(wrong_file), "snapshots"], env=env, cwd=root,
                                  capture_output=True, text=True, timeout=30)
        assert rejected.returncode != 0
        report["wrong_archive_key_refused"] = True
        run([*br, "--pg1-path=" + str(restored), "--tablespace-map-all=" + str(restored_history),
             "--set=" + incremental["database"]["label"], "--type=immediate",
             "--target-action=promote", "--archive-mode=off", "restore"],
            name="database_restore_seconds")
        start(restored, restore_socket, restore=True)
        if require_timescale:
            assert sql("SELECT sum(value) FROM extension_probe",
                       target_socket=restore_socket) == "3"
            assert sql("SELECT count(*) FROM timescaledb_information.hypertables "
                       "WHERE hypertable_name='extension_probe'",
                       target_socket=restore_socket) == "1"
            report["timescale_hypertable_restored"] = True
        actual = sql("SELECT count(*)||':'||md5(string_agg(id::text||payload,',' ORDER BY id)) "
                     "FROM observations", target_socket=restore_socket)
        assert actual == expected
        assert sql("SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM frozen",
                   target_socket=restore_socket) == expected_frozen
        assert sql("SELECT count(*) FROM historical", target_socket=restore_socket) == "10000"
        assert sql("SELECT bool_and(indisvalid) FROM pg_index "
                   "WHERE indrelid IN ('observations'::regclass,'historical'::regclass)",
                   target_socket=restore_socket) == "t"
        tablespace = Path(sql("SELECT pg_tablespace_location(oid) FROM pg_tablespace "
                              "WHERE spcname='history'", target_socket=restore_socket))
        assert tablespace.is_relative_to(restored_history)
        assert sql("SELECT c.reltablespace=t.oid FROM pg_class c, pg_tablespace t "
                   "WHERE c.oid='historical_pkey'::regclass AND t.spcname='history'",
                   target_socket=restore_socket) == "t"
        restore_files = root / "restored-archives"
        run([*rs, "restore", incremental["archive_snapshot"], "--target", restore_files],
            name="archive_restore_seconds")
        recovered_objects = restore_files / "objects" 
        for row in sql("SELECT name||':'||sha256 FROM archive_refs ORDER BY name",
                       target_socket=restore_socket).splitlines():
            name, expected_hash = row.split(":")
            assert hashlib.sha256((recovered_objects / name).read_bytes()).hexdigest() == expected_hash
        run([*rs, "check", "--read-data"], name="archive_integrity_seconds")
        # Restore reads immutable source repository; source remains independently writable.
        assert sql("SELECT count(*) FROM observations") == "20101"
        # Rotate only after a complete replacement database+archive pair.
        # Native tools must retain the new baseline and its archive snapshot.
        replacement = capture("full", phase="replacement_full")
        run([*br, "--set=" + full["database"]["label"],
             "--repo1-retention-full=9999999", "--repo1-retention-archive=9999999", "expire"])
        surviving = json.loads(run([*br, "--output=json", "info"]).stdout)[0]["backup"]
        assert [b["label"] for b in surviving] == [replacement["database"]["label"]]
        run([*rs, "forget", full["archive_snapshot"], incremental["archive_snapshot"], "--prune"])
        remaining_snapshots = json.loads(run([*rs, "snapshots"]).stdout)
        assert [s["id"] for s in remaining_snapshots] == [replacement["archive_snapshot"]]
        after_rotation = root / "after-rotation"
        rotation_history = root / "rotation-history"
        rotation_socket = root / "rotation-socket"
        rotation_socket.mkdir(mode=0o700)
        run([*br, "--pg1-path=" + str(after_rotation),
             "--tablespace-map-all=" + str(rotation_history),
             "--set=" + replacement["database"]["label"], "--type=immediate",
             "--target-action=promote", "--archive-mode=off", "restore"],
            name="after_rotation_database_restore_seconds")
        start(after_rotation, rotation_socket, restore=True)
        assert sql("SELECT count(*) FROM observations", target_socket=rotation_socket) == "20101"
        assert sql("SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM frozen",
                   target_socket=rotation_socket) == expected_frozen
        rotation_files = root / "rotation-archives"
        run([*rs, "restore", replacement["archive_snapshot"], "--target", rotation_files],
            name="after_rotation_archive_restore_seconds")
        for row in sql("SELECT name||':'||sha256 FROM archive_refs ORDER BY name",
                       target_socket=rotation_socket).splitlines():
            name, expected_hash = row.split(":")
            assert hashlib.sha256((rotation_files / "objects" / name).read_bytes()).hexdigest() == expected_hash
        run([*rs, "check", "--read-data"])
        report["replacement_recovered_after_old_dependency_chain_expired"] = True
        report.update(
            selected_point_exact=True, frozen_rows_preserved=True,
            historical_table_and_index_restored=True, matching_archives_restored=True,
            later_writes_excluded=True, source_untouched_by_restore=True,
            full=full, incremental=incremental, timings_seconds=timings,
            elapsed_seconds=round(time.monotonic() - started, 3),
            limitations=["synthetic PostgreSQL fixture, not QT application acceptance",
                         *([] if require_timescale else ["no Timescale extension compatibility claim"]),
                         "no production timing/capacity extrapolation",
                         "QT paired-publication/retention orchestration and key escrow not yet qualified"],
        )
        return report
    finally:
        for cluster in reversed(live_clusters):
            if (cluster / "postmaster.pid").exists():
                run([pg_bin / "pg_ctl", "-D", cluster, "-w", "-t", "40", "-m", "fast", "stop"],
                    timeout=50)
        # Never accept a caller-owned data directory as a cleanup target.
        if root.resolve(strict=True) != root or root.parent != Path("/tmp") or not root.name.startswith("qt-incremental-proof-"):
            raise RuntimeError("disposable_cleanup_path_changed")
        shutil.rmtree(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--pgbackrest", type=Path, required=True)
    parser.add_argument("--restic", type=Path, required=True)
    parser.add_argument("--require-timescale", action="store_true")
    args = parser.parse_args()
    print(json.dumps(rehearse(pg_bin=args.pg_bin, pgbackrest=args.pgbackrest,
                             restic=args.restic, require_timescale=args.require_timescale), sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

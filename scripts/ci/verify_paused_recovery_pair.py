"""Verify a paused, private PostgreSQL restore against its paired archive.

Run in a network-disabled verifier with the attempt mounted read-only and the
exact pair receipt mounted separately. The only SQL endpoint is the approved
Unix socket. The approved existing role must be usable with local auth and
able to inspect native PostgreSQL control state. No backup keys are needed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import MAX_RECOVERY_OBJECTS, _archive_rows, _snapshot_layout
from scripts.automation.canonical_cold_named_recovery import _TABLESPACE_NAME
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json
from scripts.db.canonical_cold_recovery_suffix import _encode, prepare_archive_baseline


_PAUSED = text("""
    SELECT control.system_identifier::text AS system_identifier, db.oid::bigint AS database_oid,
           current_database() AS database, session_user AS role, login_role.oid::bigint AS role_oid,
           current_user = session_user AS role_unchanged, checkpoint.timeline_id::bigint AS checkpoint_timeline,
           pg_is_in_recovery() AS in_recovery, pg_get_wal_replay_pause_state() AS pause_state,
           pg_last_wal_replay_lsn()::text AS replay_lsn,
           pg_last_wal_replay_lsn() >= CAST(:minimum AS pg_lsn) AS reached_minimum,
           current_setting('recovery_target') AS recovery_target,
           current_setting('recovery_target_lsn') AS recovery_target_lsn,
           current_setting('recovery_target_inclusive') AS recovery_target_inclusive,
           current_setting('recovery_target_timeline') AS recovery_timeline,
           current_setting('recovery_target_action') AS recovery_action,
           current_setting('archive_mode') AS archive_mode,
           current_setting('primary_conninfo') = '' AND current_setting('primary_slot_name') = '' AS archive_only,
           NOT EXISTS(SELECT FROM pg_stat_wal_receiver) AS no_wal_receiver,
           inet_server_addr() IS NULL AND inet_client_addr() IS NULL
             AND current_setting('listen_addresses') = '' AS socket_only,
           current_setting('data_directory') AS data_directory,
           current_setting('unix_socket_directories') AS socket_directories,
           current_setting('transaction_read_only') AS read_only,
           current_setting('transaction_isolation') AS isolation,
           pg_postmaster_start_time()::text AS postmaster_started_at,
           (SELECT extversion FROM pg_extension WHERE extname='timescaledb') AS timescale_version
    FROM pg_control_system() control CROSS JOIN pg_control_checkpoint() checkpoint
    CROSS JOIN pg_database db CROSS JOIN pg_roles login_role
    WHERE db.datname=current_database() AND login_role.rolname=session_user AND login_role.rolcanlogin
""")


def _mount_paths(args, *, native_root=None):
    attempt, attempt_id = _private_root(args.attempt_root)
    paths = {name: _private_root(getattr(args, name))
             for name in ("pgdata", "tablespace", "socket", "archive_root")}
    device = f"{os.major(attempt_id[0])}:{os.minor(attempt_id[0])}"
    if (device != args.device_id or not os.statvfs(attempt).f_flag & os.ST_RDONLY
            or any(path == attempt or not path.is_relative_to(attempt) or identity[0] != attempt_id[0]
                   for path, identity in paths.values())
            or len({identity for _, identity in paths.values()}) != len(paths)
            or any(left.is_relative_to(right) or right.is_relative_to(left)
                   for index, (left, _) in enumerate(paths.values()) for right, _ in tuple(paths.values())[index + 1:])
            or paths["pgdata"][1][1] != args.pgdata_inode or paths["tablespace"][1][1] != args.tablespace_inode):
        raise ValueError("paused_recovery_private_mount_scope_changed")
    mountinfo = Path("/proc/self/mountinfo")
    with mountinfo.open("rb") as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise RuntimeError("paused_recovery_mountinfo_budget_exceeded")
    observed = []
    for line in os.fsdecode(data).splitlines():
        fields = line.split(" ")
        target = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4]))
        if target == attempt:
            observed.append((fields[2], "ro" in fields[5].split(",")))
        elif target.is_relative_to(attempt):
            raise ValueError("paused_recovery_nested_mount")
    if observed != [(args.device_id, True)]:
        raise ValueError("paused_recovery_private_readonly_attempt_mount_required")

    pgdata, _ = paths["pgdata"]
    link = pgdata / "pg_tblspc" / str(args.tablespace_oid)
    expected = args.tablespace if native_root is None else native_root / args.tablespace.relative_to(attempt)
    if not link.is_symlink() or Path(os.readlink(link)) != expected:
        raise ValueError("paused_recovery_native_tablespace_link_changed")
    if native_root is None and _identity(link.stat()) != paths["tablespace"][1]:
        raise ValueError("paused_recovery_native_tablespace_link_changed")
    sock = args.socket / f".s.PGSQL.{args.port}"
    info = sock.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_dev != attempt_id[0]:
        raise ValueError("paused_recovery_private_socket_changed")
    return {"attempt": attempt_id, "paths": paths, "socket": _identity(info)}


def _lsn(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", value):
        return tuple(int(part, 16) for part in value.split("/"))
    return (0, 0)


def _probe(conn, args, pair, *, target_lsn=None, replay_lsn=None):
    state = dict(conn.execute(_PAUSED, {"minimum": args.minimum_replay_lsn}).mappings().one())
    expected = {
        "database": args.database, "role": args.role, "role_oid": args.role_oid, "role_unchanged": True,
        "checkpoint_timeline": args.timeline, "in_recovery": True,
        "pause_state": "paused", "reached_minimum": True,
        "recovery_target": "immediate" if target_lsn is None else "",
        "recovery_timeline": str(args.timeline), "recovery_action": "pause", "archive_mode": "off",
        "archive_only": True, "no_wal_receiver": True, "socket_only": True,
        "data_directory": str(args.pgdata), "socket_directories": str(args.socket),
        "read_only": "on", "isolation": "repeatable read", "timescale_version": "2.14.2",
    }
    identity = f"{state['system_identifier']}/{state['database_oid']}"
    changed = sorted(key for key, value in expected.items() if state[key] != value)
    if target_lsn is not None and (
        _lsn(state["recovery_target_lsn"]) != _lsn(target_lsn)
        or _lsn(state["replay_lsn"]) != _lsn(replay_lsn)
        or state["recovery_target_inclusive"] != "on"
    ):
        changed.append("recovery_lsn")
    if identity != pair["database_identity"] or changed:
        raise RuntimeError(f"paused_recovery_native_state_changed: {','.join(changed) or 'database_identity'}")
    spaces = conn.execute(text("""
        SELECT oid::bigint, spcname, pg_tablespace_location(oid) FROM pg_tablespace
        WHERE spcname NOT IN ('pg_default','pg_global') LIMIT 2
    """)).all()
    if spaces != [(args.tablespace_oid, args.tablespace_name, str(args.tablespace))]:
        raise RuntimeError("paused_recovery_native_tablespace_scope_changed")
    layout = _snapshot_layout(conn)
    if layout != pair["storage_layout"]:
        raise RuntimeError("paused_recovery_storage_certificate_changed")
    return state | {"database_identity": identity, "storage_layout": layout}


def _catalog_closure(conn, baseline, max_objects, check, check_sql):
    seen = set()
    digest = hashlib.sha256()
    required_bytes = 0
    for row in _archive_rows(conn, max_objects=max_objects, check=check_sql):
        check()
        key = row["object_key"]
        descriptor = row["object_sha256"], row["byte_count"]
        if key in seen or baseline.objects.get(key) != descriptor:
            raise RuntimeError("paused_recovery_archive_duplicate_missing_or_changed")
        seen.add(key)
        required_bytes += descriptor[1]
        digest.update(_encode(key, descriptor))
    return len(seen), required_bytes, digest.hexdigest()


def _check_scope(args):
    if (not 1 <= args.timeout_seconds <= 86400 or not 1 <= args.statement_timeout_seconds <= 600
            or not 1 <= args.max_objects <= MAX_RECOVERY_OBJECTS or not 1 <= args.port <= 65535
            or any(not 0 < oid < 2**32 for oid in (args.timeline, args.tablespace_oid, args.role_oid))
            or min(args.pgdata_inode, args.tablespace_inode) <= 0
            or not _TABLESPACE_NAME.fullmatch(args.tablespace_name)
            or not re.fullmatch(r"[0-9A-F]{1,8}/[0-9A-F]{1,8}", args.minimum_replay_lsn)
            or not any(int(part, 16) for part in args.minimum_replay_lsn.split("/"))
            or not all(0 < len(value) <= 63 and not any(c in value for c in "\x00\r\n")
                       for value in (args.database, args.role))
            or any(os.environ.get(name) for name in ("PGHOSTADDR", "PGSERVICE", "PGPASSWORD"))):
        raise ValueError("paused_recovery_invalid_scope_or_inherited_connection")


def _read_pair(args):
    if not os.statvfs(args.receipt).f_flag & os.ST_RDONLY:
        raise ValueError("paused_recovery_requires_readonly_pair_receipt")
    pair = _bound_json(args.receipt, args.receipt_sha256)
    if (not isinstance(pair, dict) or pair.get("schema_version") != "qt.encrypted_recovery_pair.v1"
            or pair.get("restore_endpoint") != "immediate"
            or not isinstance(pair.get("storage_layout"), dict)
            or pair["storage_layout"].get("layout_version") != "market.fact_storage_tiers.v2"):
        raise ValueError("paused_recovery_pair_outside_original_immediate_scope")
    return pair


def _readonly_engine(args, check):
    url = URL.create("postgresql+psycopg2", username=args.role, database=args.database,
                     query={"host": str(args.socket), "port": str(args.port), "passfile": "/dev/null",
                            "sslmode": "disable", "gssencmode": "disable",
                            "connect_timeout": str(max(2, min(5, int(check())))),
                            "options": "-c default_transaction_read_only=on"})
    return create_engine(url, poolclass=NullPool, isolation_level="REPEATABLE READ", hide_parameters=True)


def _bound_statement(conn, check, seconds):
    milliseconds = max(1, int(1000 * min(check(), seconds)))
    conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"), {"timeout": str(milliseconds)})


def _verify(args):
    _check_scope(args)
    start = monotonic()
    deadline = start + args.timeout_seconds

    def check():
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("paused_recovery_verification_deadline")
        return remaining

    paths = _mount_paths(args)
    pair = _read_pair(args)
    engine = _readonly_engine(args, check)

    def bound(conn):
        _bound_statement(conn, check, args.statement_timeout_seconds)

    def bind(conn):
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        bound(conn)

    try:
        with engine.begin() as conn:
            bind(conn)
            before = _probe(conn, args, pair)
        baseline = prepare_archive_baseline(args.receipt, args.archive_root,
            expected_receipt_sha256=args.receipt_sha256, max_objects=args.max_objects, check=check)
        archive_verified = monotonic()
        if _mount_paths(args) != paths:
            raise RuntimeError("paused_recovery_attempt_mount_changed")
        with engine.begin() as conn:
            bind(conn)
            if _probe(conn, args, pair) != before:
                raise RuntimeError("paused_recovery_endpoint_changed")
            objects, required_bytes, catalog_digest = _catalog_closure(
                conn, baseline, args.max_objects, check, lambda: bound(conn),
            )
            bound(conn)
            if _probe(conn, args, pair) != before:
                raise RuntimeError("paused_recovery_endpoint_changed")
    finally:
        engine.dispose()
    if _mount_paths(args) != paths or _bound_json(args.receipt, args.receipt_sha256) != pair:
        raise RuntimeError("paused_recovery_final_scope_changed")
    check()
    receipt = {
        "schema": "qt.paused_recovery_pair_verification.v1", "pair_receipt_sha256": args.receipt_sha256,
        "database_label": pair["database_label"], "database_identity": baseline.database_identity,
        "database": args.database, "role": args.role, "role_oid": args.role_oid,
        "storage_layout": baseline.layout.copy(), "timeline": args.timeline,
        "minimum_replay_lsn": args.minimum_replay_lsn, "replay_lsn": before["replay_lsn"],
        "postmaster_started_at": before["postmaster_started_at"], "pgdata_inode": args.pgdata_inode,
        "tablespace": {"oid": args.tablespace_oid, "name": args.tablespace_name, "path": str(args.tablespace),
                       "inode": args.tablespace_inode}, "device_id": args.device_id,
        "archive_snapshot": pair["archive_snapshot"], "archive_inventory_sha256": pair["inventory_sha256"],
        "catalog_inventory_sha256": catalog_digest, "catalog_objects": objects, "catalog_bytes": required_bytes,
        "verified_baseline_objects": len(baseline.objects), "verified_baseline_bytes": pair["archive_bytes"],
        "baseline_only_objects": len(baseline.objects) - objects, "baseline_only_bytes": pair["archive_bytes"] - required_bytes,
        "archive_hash_seconds": round(archive_verified - start, 3), "elapsed_seconds": round(monotonic() - start, 3),
    }
    return receipt | {"verification_sha256": archive_evidence_hash(receipt)}


def _add_scope_arguments(parser):
    for name in ("attempt-root", "pgdata", "socket", "tablespace", "archive-root", "receipt"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("database", "role", "device-id", "tablespace-name", "receipt-sha256"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("pgdata-inode", "role-oid", "tablespace-oid", "tablespace-inode", "timeline", "max-objects", "timeout-seconds"):
        parser.add_argument(f"--{name}", type=int, required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--statement-timeout-seconds", type=int, default=15)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "Baseline-only objects are permitted because the paired archive inventory is captured after the physical "
        "backup endpoint. Every object referenced by the paused catalog must match the verified baseline."
    ))
    _add_scope_arguments(parser)
    parser.add_argument("--minimum-replay-lsn", required=True)
    args = parser.parse_args(argv)
    try:
        result = _verify(args)
    except SQLAlchemyError as error:
        parser.exit(1, f"paused_recovery_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

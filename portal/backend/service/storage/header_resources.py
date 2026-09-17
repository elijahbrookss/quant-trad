"""Read-only PostgreSQL resource paths bound to registered filesystems.

This observes PGDATA, WAL and new temporary allocations for the caller's
session, including database fallback for temporary files. It is not a cluster-wide producer inventory,
resource limit, capacity reservation, or authority to activate movement.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import re
import stat
import subprocess
from time import monotonic

from sqlalchemy import text

from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageTarget
from .header_filesystem import (
    PostgresProcessObservation, _check_control_binary, _cluster_identity,
    _device_id, _process_readlink, _process_stat, _same_process_file,
)


@dataclass(frozen=True)
class VerifiedHeaderResources:
    database_identity: str
    backend_pid: int
    observed_at: datetime
    verified_at: datetime
    temporary_setting: str
    capacity: dict[str, FilesystemEvidence]
    bindings: tuple[dict, ...]
    execution_available: bool = False


def _refuse(reason):
    raise StorageMountError("header_resources_unverified: " + reason)


def _temp_tokens(raw):
    # Keep quoted identifiers intact, including doubled quotes and commas.
    # PostgreSQL parse_ident performs identifier normalization, not Python.
    if not isinstance(raw, str) or "\x00" in raw or len(raw.encode("utf-8")) > 4096:
        _refuse("temporary setting exceeds budget")
    tokens, beginning, quoted, index = [], 0, False, 0
    while index < len(raw):
        char = raw[index]
        if char == '"':
            if quoted and index + 1 < len(raw) and raw[index + 1] == '"':
                index += 2
                continue
            quoted = not quoted
        elif char == "," and not quoted:
            tokens.append(raw[beginning:index].strip())
            beginning = index + 1
        index += 1
    if quoted:
        _refuse("unclosed temporary identifier")
    tokens.append(raw[beginning:].strip())
    if len(tokens) > 32:
        _refuse("temporary tablespace budget")
    return tuple(tokens)


def _catalog(query):
    context = dict(query("""
        SELECT clock_timestamp() AS captured_at, pg_backend_pid() AS backend_pid,
               current_setting('data_directory') AS data_directory,
               pg_postmaster_start_time() AS started_at,
               pg_read_file('postmaster.pid',0,4096) AS pidfile,
               current_setting('server_version_num')::integer AS version,
               current_setting('temp_tablespaces') AS temporary_setting,
               current_user::text AS role, pg_is_in_recovery() AS recovery,
               c.system_identifier::text || '/' || d.oid::text AS database_identity,
               c.catalog_version_no::integer AS catalog_version,
               d.oid::bigint AS database_oid, d.dattablespace::bigint AS default_oid
        FROM pg_control_system() c
        CROSS JOIN pg_database d WHERE d.datname=current_database()
    """).mappings().one())
    if context["version"] // 10000 != 15 or context["recovery"]:
        _refuse("writable PostgreSQL 15 required")
    wanted = {context["default_oid"]}
    for token in _temp_tokens(context["temporary_setting"]):
        if token in ("", '""'):
            continue  # Already include database default, also the file fallback.
        identifiers = query("SELECT parse_ident(:identifier, true)", {"identifier": token}).scalar_one()
        if len(identifiers) != 1:
            _refuse("temporary tablespace must be a single identifier")
        row = query("""
            SELECT oid::bigint AS oid, has_tablespace_privilege(oid,'CREATE') AS allowed
            FROM pg_tablespace WHERE spcname=:name
        """, {"name": identifiers[0]}).mappings().one_or_none()
        if row is None or (row["oid"] != context["default_oid"] and not row["allowed"]):
            _refuse("temporary tablespace missing or inaccessible")
        wanted.add(row["oid"])
    if 1664 in wanted or len(wanted) > 33:
        _refuse("unsupported temporary tablespace")
    spaces = tuple(dict(row) for row in query("""
        SELECT oid::bigint AS oid, spcname::text AS name, pg_tablespace_location(oid) AS location
        FROM pg_tablespace WHERE oid=ANY(:oids) ORDER BY oid LIMIT 34
    """, {"oids": sorted(wanted)}).mappings())
    if {row["oid"] for row in spaces} != wanted:
        _refuse("tablespace inventory changed")
    return context, spaces


def _process(context):
    if not re.fullmatch(r"[0-9]{1,20}/[0-9]{1,10}", context["database_identity"]):
        _refuse("database identity")
    return PostgresProcessObservation(context["database_identity"], context["captured_at"],
        context["data_directory"], context["started_at"],
        tuple(context["pidfile"].splitlines()[:3]))


def _inspect_paths(context, spaces, cluster, targets, deadline):
    root, pid, _ = cluster
    roots, capacity, devices = {}, {}, set()
    for target in targets:
        evidence = target.inspect(require_writable=True)
        actual = Path(target.root).resolve(strict=True)
        if (not actual.is_dir() or evidence.read_only
                or Path(evidence.path).resolve(strict=True) != actual
                or evidence.filesystem_uuid != target.filesystem_uuid
                or _device_id(actual.stat().st_dev) != evidence.device_id
                or evidence.device_id in devices):
            _refuse("registered filesystem identity")
        devices.add(evidence.device_id)
        roots[target.target_id], capacity[target.target_id] = actual, evidence
    bindings = []

    def bind(role, path, *, oid=None, allow_missing=False):
        if monotonic() >= deadline:
            _refuse("time budget exceeded")
        if path.is_symlink():
            _refuse("unexpected resource directory link")
        exists = True
        try:
            actual = path.resolve(strict=True)
        except FileNotFoundError:
            if not allow_missing:
                raise
            # Only a final PostgreSQL-created child may be absent; never mkdir.
            actual = path.parent.resolve(strict=True)
            try:
                _process_stat(pid, path)
            except FileNotFoundError:
                pass
            else:
                _refuse("worker and server disagree on absent child")
            exists = False
        info = actual.stat()
        if not stat.S_ISDIR(info.st_mode):
            _refuse("resource is not a directory")
        _same_process_file(pid, path if exists else path.parent, info, require_writable=True)
        matches = [key for key, parent in roots.items()
                   if actual.is_relative_to(parent) and _device_id(info.st_dev) == capacity[key].device_id]
        if len(matches) != 1:
            _refuse("resource outside a unique registered filesystem")
        target_id, = matches
        bindings.append({"role": role, "tablespace_oid": oid, "directory": str(path),
            "exists": exists, "verified_directory": str(actual),
            "verified_directory_inode": info.st_ino, "target_id": target_id,
            "filesystem_uuid": capacity[target_id].filesystem_uuid,
            "device_id": capacity[target_id].device_id})

    bind("database", root)
    wal = root / "pg_wal"
    if wal.is_symlink():
        link = str(wal.readlink())
        if _process_readlink(pid, wal) != link:
            _refuse("server WAL link differs")
        declared = Path(link)
        wal = Path(os.path.normpath(str(declared if declared.is_absolute() else wal.parent / declared)))
    bind("wal", wal)
    for space in spaces:
        oid = space["oid"]
        if oid == 1663:
            if space["name"] != "pg_default" or space["location"] != "":
                _refuse("default tablespace identity")
            directory = root / "base"
        else:
            declared = Path(space["location"])
            if not declared.is_absolute() or ".." in declared.parts:
                _refuse("tablespace location")
            link = root / "pg_tblspc" / str(oid)
            if (not link.is_symlink() or str(link.readlink()) != str(declared)
                    or _process_readlink(pid, link) != str(declared)):
                _refuse("server tablespace link differs")
            _same_process_file(pid, declared, declared.stat())
            directory = declared / ("PG_15_" + str(context["catalog_version"]))
        bind("temporary_files", directory / "pgsql_tmp", oid=oid, allow_missing=True)
        bind("temporary_relations", directory / str(context["database_oid"]), oid=oid, allow_missing=True)
        if oid == context["default_oid"]:
            bind("database_default", directory / str(context["database_oid"]), oid=oid)
    return tuple(bindings), capacity


def observe_header_resources(conn, targets, *, pg_controldata, timeout_seconds=30):
    """Observe caller-session allocation roots without opening another connection.

    Caller must own an active READ COMMITTED transaction and roll it back on
    failure. Success restores its statement timeout. No DDL or filesystem writes
    occur. Before real movement the worker must recheck this evidence under
    storage ownership, qualify/enforce limits and account for other producers.
    """
    if not conn.in_transaction() or conn.get_isolation_level() != "READ COMMITTED":
        raise RuntimeError("header_resources_requires_read_committed_transaction")
    if (not 1 <= len(targets) <= 32 or any(not isinstance(item, StorageTarget) for item in targets)
            or len({item.target_id for item in targets}) != len(targets)
            or len({item.filesystem_uuid for item in targets}) != len(targets)):
        raise ValueError("header_resources_invalid_targets")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("header_resources_invalid_time_budget")
    binary = Path(pg_controldata)
    if not binary.is_absolute():
        raise ValueError("header_resources_absolute_control_binary_required")
    deadline = monotonic() + timeout_seconds
    previous = conn.execute(text("""
        SELECT current_setting('statement_timeout') AS original, setting::bigint AS milliseconds
        FROM pg_settings WHERE name='statement_timeout'
    """)).mappings().one()
    if previous["milliseconds"]:
        deadline = min(deadline, monotonic() + previous["milliseconds"] / 1000)

    def query(sql, parameters=None):
        remaining = int((deadline - monotonic()) * 1000)
        if remaining <= 0:
            _refuse("time budget exceeded")
        conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"), {"timeout": str(remaining)})
        return conn.execute(text(sql), parameters or {})

    try:
        binary = binary.resolve(strict=True)
        _check_control_binary(binary, deadline)
        context, spaces = _catalog(query)
        process = _process(context)
        cluster = _cluster_identity(process, binary, deadline)
        bindings, _ = _inspect_paths(context, spaces, cluster, targets, deadline)
        final_context, final_spaces = _catalog(query)
        if ({key: value for key, value in context.items() if key != "captured_at"}
                != {key: value for key, value in final_context.items() if key != "captured_at"}
                or spaces != final_spaces):
            _refuse("resource catalog changed")
        if _cluster_identity(process, binary, deadline) != cluster:
            _refuse("database process changed")
        final_bindings, capacity = _inspect_paths(context, spaces, cluster, targets, deadline)
        if final_bindings != bindings:
            _refuse("resource directories changed")
        if monotonic() >= deadline:
            _refuse("time budget exceeded")
        conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                     {"timeout": previous["original"]})
        return VerifiedHeaderResources(context["database_identity"], context["backend_pid"],
            context["captured_at"], datetime.now(UTC), context["temporary_setting"],
            capacity, bindings)
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise StorageMountError("header_resources_unavailable: " + type(exc).__name__) from exc

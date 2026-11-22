"""Qualify one existing archived WAL record as a private PostgreSQL replay target."""

from functools import partial
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import struct
from tempfile import TemporaryDirectory
from time import monotonic

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes, _run_native
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _safe_member, _stamp
from scripts.automation.verify_encrypted_recovery_handoff import _WAL, _archive_filenames
from scripts.db.canonical_cold_replayed_checkpoint import _lsn, _lsn_text, _segment_number


_VERSION = "qt.canonical_cold_source_wal_target.v1"
_PG_WALDUMP = "pg_waldump (PostgreSQL) 15.6"
_PGBACKREST = "pgBackRest 2.59.1"
_PAGE_MAGIC = 0xD110
_PAGE_BYTES = 8192
_CONTINUATION = 1
_LONG_HEADER = 2
_OVERWRITE_CONTINUATION = 8
_SHORT = struct.Struct("<HHIQI4x")
_LONG = struct.Struct("<QII")
_RECORD = re.compile(
    rb"rmgr:\s*(?P<manager>\w+)\s+len \(rec/tot\):\s*\d+/\s*(?P<bytes>\d+),"
    rb"\s*tx:\s*\d+,\s*lsn:\s*(?P<lsn>[0-9A-F]+/[0-9A-F]+),\s*prev:?\s*[0-9A-F]+/[0-9A-F]+,"
    rb"\s*desc:\s*(?P<description>[^\r\n]*)"
)
_RECORD_KINDS = {
    "restore_point": (b"XLOG", b"RESTORE_POINT"),
    "checkpoint_online": (b"XLOG", b"CHECKPOINT_ONLINE"),
    "checkpoint_shutdown": (b"XLOG", b"CHECKPOINT_SHUTDOWN"),
    "commit": (b"Transaction", b"COMMIT"),
}


def _read_page(stream, page, first, timeline, *, expected_continuation=None):
    stream.seek(page - first)
    header = stream.read(_SHORT.size)
    if len(header) != _SHORT.size:
        raise ValueError("canonical_source_wal_target_page_header_invalid")
    magic, flags, page_timeline, address, remaining = _SHORT.unpack(header)
    long = bool(flags & _LONG_HEADER)
    if (magic, page_timeline, address, long) != (_PAGE_MAGIC, timeline, page, page == first) or flags & ~0xF:
        raise ValueError("canonical_source_wal_target_page_header_invalid")
    if expected_continuation is not None and (
        not flags & _CONTINUATION or flags & _OVERWRITE_CONTINUATION or remaining != expected_continuation
    ):
        raise ValueError("canonical_source_wal_target_continuation_invalid")
    return _SHORT.size + (_LONG.size if long else 0)


def _record_end(stream, *, first, segment_bytes, timeline, target, record_bytes, check):
    page = target - target % _PAGE_BYTES
    header_size = _read_page(stream, page, first, timeline)
    if target % _PAGE_BYTES < header_size or target % 8:
        raise ValueError("canonical_source_wal_target_record_alignment_invalid")
    position = target
    remaining = record_bytes
    while remaining > page + _PAGE_BYTES - position:
        check()
        remaining -= page + _PAGE_BYTES - position
        page += _PAGE_BYTES
        if page >= first + segment_bytes:
            raise ValueError("canonical_source_wal_target_record_crosses_segment")
        position = page + _read_page(stream, page, first, timeline, expected_continuation=remaining)
    return position + (remaining + 7) // 8 * 8


def _decode_record(output, *, target, max_record_bytes, record_kind, restore_name):
    match = _RECORD.fullmatch(output.strip())
    if match is None or _lsn(match["lsn"].decode("ascii")) != target:
        raise ValueError("canonical_source_wal_target_native_record_start_changed")
    manager, kind = _RECORD_KINDS[record_kind]
    description = match["description"]
    observed_kind, _, details = description.partition(b" ")
    if (match["manager"], observed_kind) != (manager, kind) or (
        restore_name is not None and details != restore_name.encode("ascii")
    ):
        raise ValueError("canonical_source_wal_target_native_record_kind_changed")
    record_bytes = int(match["bytes"])
    if not 24 <= record_bytes <= max_record_bytes:
        raise ValueError("canonical_source_wal_target_native_record_size_invalid")
    return record_bytes


def _private_control_snapshot(pgdata, work):
    """Give archive-get only the control metadata it needs to authenticate the stanza."""
    control = work / "control"
    (control / "global").mkdir(parents=True, mode=0o700)
    for relative, limit in (("global/pg_control", 8192), ("backup_label", 65_536)):
        source = pgdata / relative
        if relative == "backup_label" and not os.path.lexists(source):
            continue
        before = _stamp(source.lstat())
        data = _private_bytes(source, limit=limit)
        if (relative == "global/pg_control" and len(data) != limit) or _stamp(source.lstat()) != before:
            raise RuntimeError("canonical_source_wal_target_control_metadata_changed")
        with os.fdopen(os.open(control / relative, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb") as dest:
            dest.write(data)
    return control


def qualify_source_wal_target(*, pg_waldump, pgbackrest, repository, database_key, workspace, pgdata, archive_id,
                              segment, expected_wal_sha1, wal_segment_bytes, target_lsn, replay_lsn,
                              timeout_seconds, record_kind="restore_point", restore_name=None,
                              expected_system_identifier=None):
    """Check one already archived WAL segment without contacting PostgreSQL.

    The repository is the private pgBackRest database repository. Only the
    supplied segment is fetched into a disposable private workspace. Archive-get
    uses a private copy of native control metadata. A target may cross WAL
    pages inside that segment. The receipt contains only decoded coordinates.
    """
    if (not isinstance(record_kind, str) or record_kind not in _RECORD_KINDS
            or (record_kind == "restore_point") != (restore_name is not None)
            or (restore_name is not None and (not isinstance(restore_name, str)
                or not re.fullmatch(r"[A-Za-z0-9_.-]{1,63}", restore_name)))
            or not isinstance(archive_id, str) or not re.fullmatch(r"15-[1-9][0-9]*", archive_id)
            or not isinstance(segment, str) or not _WAL.fullmatch(segment)
            or not isinstance(expected_wal_sha1, str) or not re.fullmatch(r"[a-f0-9]{40}", expected_wal_sha1)
            or type(wal_segment_bytes) is not int or not 2**20 <= wal_segment_bytes <= 2**30
            or wal_segment_bytes & (wal_segment_bytes - 1)
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 86_400
            or (expected_system_identifier is not None and (not isinstance(expected_system_identifier, str)
                or not re.fullmatch(r"[1-9][0-9]{0,19}", expected_system_identifier)))):
        raise ValueError("canonical_source_wal_target_scope_invalid")
    timeline = int(segment[:8], 16)
    first = _segment_number(segment, timeline, (1 << 32) // wal_segment_bytes) * wal_segment_bytes
    target, replay = _lsn(target_lsn), _lsn(replay_lsn)
    if not timeline or not first <= target < replay <= first + wal_segment_bytes or target == 0:
        raise ValueError("canonical_source_wal_target_segment_mismatch")
    repo, repo_id = _private_root(repository)
    work_root, work_id = _private_root(workspace)
    pgdata, pgdata_id = _private_root(pgdata)
    key_path = Path(database_key)
    roots = (repo, work_root, pgdata)
    if (any(path.is_relative_to(other) or other.is_relative_to(path)
            for index, path in enumerate(roots) for other in roots[index + 1:])
            or any(key_path.is_relative_to(path) for path in roots)):
        raise ValueError("canonical_source_wal_target_private_paths_overlap")
    pg_waldump, pgbackrest = map(_binary, (pg_waldump, pgbackrest))
    if pg_waldump.name != "pg_waldump" or pgbackrest.name != "pgbackrest":
        raise ValueError("canonical_source_wal_target_native_tools_invalid")
    deadline = monotonic() + timeout_seconds
    with TemporaryDirectory(prefix=".qt-wal-target-", dir=work_root) as temporary:
        work = Path(temporary)
        destination = work / segment
        include = work / "config"
        include.mkdir(mode=0o700)

        def check():
            if monotonic() >= deadline:
                raise TimeoutError("canonical_source_wal_target_deadline")
            if any(_identity(path.lstat()) != pin for path, pin in ((repo, repo_id), (work_root, work_id), (pgdata, pgdata_id))):
                raise RuntimeError("canonical_source_wal_target_mount_changed")
            if destination.exists() and destination.stat().st_size > wal_segment_bytes:
                raise RuntimeError("canonical_source_wal_target_native_file_budget")

        if shutil.disk_usage(work).free < wal_segment_bytes:
            raise RuntimeError("canonical_source_wal_target_workspace_insufficient")
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": temporary, "TMPDIR": temporary}
        run = partial(_run_native, cwd=work, deadline=deadline, check=check)
        if (run([str(pg_waldump), "--version"], env=env).strip() != _PG_WALDUMP.encode()
                or run([str(pgbackrest), "version"], env=env).strip() != _PGBACKREST.encode()):
            raise ValueError("canonical_source_wal_target_native_tools_version_changed")
        (_, archive_sha1, archive_path), = _archive_filenames(repo, archive_id, (segment,), check)
        info = _safe_member(archive_path, repo_id[0])
        if (archive_sha1 != expected_wal_sha1 or not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1 or not 0 < info.st_size <= 2 * wal_segment_bytes):
            raise ValueError("canonical_source_wal_target_archive_pin_changed")
        archive_stamp = _stamp(info)
        cipher_key = _private_bytes(key_path, limit=4096).decode("ascii").strip()
        if not re.fullmatch(r"[a-f0-9]{64}", cipher_key):
            raise ValueError("canonical_source_wal_target_database_key_invalid")
        control_snapshot = _private_control_snapshot(pgdata, work)
        run([str(pgbackrest), "--no-config", f"--config-include-path={include}", "--stanza=qt", "--repo=1",
             "--repo1-type=posix", f"--repo1-path={repo}", "--repo1-cipher-type=aes-256-cbc", "--archive-async=n",
             f"--pg1-path={control_snapshot}", f"--lock-path={work}", "--log-level-file=off", "--log-level-console=off",
             "archive-get", segment, str(destination)], env=env | {"PGBACKREST_REPO1_CIPHER_PASS": cipher_key})
        del cipher_key
        plain = _safe_member(destination, work_id[0])
        plain_stamp = _stamp(plain)
        if not stat.S_ISREG(plain.st_mode) or plain.st_nlink != 1 or plain.st_size != wal_segment_bytes:
            raise ValueError("canonical_source_wal_target_native_file_size_changed")
        with os.fdopen(os.open(destination, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
            digest = hashlib.sha1()
            while data := stream.read(1024 * 1024):
                check()
                digest.update(data)
            if digest.hexdigest() != expected_wal_sha1 or _stamp(os.fstat(stream.fileno())) != plain_stamp:
                raise ValueError("canonical_source_wal_target_native_sha1_changed")
            _read_page(stream, first, first, timeline)
            stream.seek(_SHORT.size)
            system_id, segment_size, block_size = _LONG.unpack(stream.read(_LONG.size))
            if (system_id == 0 or segment_size != wal_segment_bytes or block_size != _PAGE_BYTES
                    or (expected_system_identifier is not None and str(system_id) != expected_system_identifier)):
                raise ValueError("canonical_source_wal_target_native_segment_identity_changed")
            decoded = run([str(pg_waldump), f"--path={work}", f"--start={_lsn_text(target)}",
                           f"--end={_lsn_text(replay)}", "--limit=1", f"--timeline={timeline}", segment], env=env)
            record_bytes = _decode_record(decoded, target=target, max_record_bytes=replay - target,
                                          record_kind=record_kind, restore_name=restore_name)
            if _record_end(stream, first=first, segment_bytes=wal_segment_bytes, timeline=timeline, target=target,
                           record_bytes=record_bytes, check=check) != replay:
                raise ValueError("canonical_source_wal_target_native_record_end_changed")
            if _stamp(os.fstat(stream.fileno())) != plain_stamp or _stamp(destination.lstat()) != plain_stamp:
                raise ValueError("canonical_source_wal_target_native_file_changed")
        if (list(_archive_filenames(repo, archive_id, (segment,), check)) != [(segment, expected_wal_sha1, archive_path)]
                or _stamp(archive_path.lstat()) != archive_stamp):
            raise RuntimeError("canonical_source_wal_target_archive_changed")
    report = {"schema": _VERSION, "segment": segment, "wal_sha1": expected_wal_sha1, "archive_id": archive_id,
              "wal_segment_bytes": wal_segment_bytes, "timeline": timeline, "system_identifier": str(system_id),
              "target_lsn": _lsn_text(target), "replay_lsn": _lsn_text(replay), "record_kind": record_kind,
              "restore_name": restore_name, "record_total_bytes": record_bytes,
              "pg_waldump_version": _PG_WALDUMP, "pgbackrest_version": _PGBACKREST}
    return report | {"verification_sha256": archive_evidence_hash(report)}

import hashlib
import io
from pathlib import Path

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import canonical_cold_source_target as source


def _wal_header(*, position, flags, remaining=0):
    return source._SHORT.pack(source._PAGE_MAGIC, flags, 1, position, remaining)


def test_native_record_end_uses_continuation_header_and_alignment():
    first = 1 << 20
    page = first + source._PAGE_BYTES
    data = bytearray(2 * source._PAGE_BYTES)
    data[:source._SHORT.size] = _wal_header(position=first, flags=source._LONG_HEADER)
    data[source._PAGE_BYTES:source._PAGE_BYTES + source._SHORT.size] = _wal_header(
        position=page, flags=source._CONTINUATION, remaining=86,
    )
    options = {"first": first, "segment_bytes": 1 << 20, "timeline": 1,
               "target": page - 8, "record_bytes": 94, "check": lambda: None}
    assert source._record_end(io.BytesIO(data), **options) == page + 24 + 88
    data[source._PAGE_BYTES:source._PAGE_BYTES + source._SHORT.size] = _wal_header(
        position=page, flags=source._CONTINUATION, remaining=85,
    )
    with pytest.raises(ValueError, match="continuation_invalid"):
        source._record_end(io.BytesIO(data), **options)


@pytest.mark.parametrize(("kind", "manager", "description", "name"), (
    ("checkpoint_online", b"XLOG", b"CHECKPOINT_ONLINE redo 0/1000000", None),
    ("checkpoint_shutdown", b"XLOG", b"CHECKPOINT_SHUTDOWN redo 0/1000000", None),
    ("restore_point", b"XLOG", b"RESTORE_POINT fixture", "fixture"),
    ("commit", b"Transaction", b"COMMIT 2026-11-15 06:00:00 UTC", None),
))
def test_decode_requires_exact_record_identity(kind, manager, description, name):
    line = (b"rmgr: " + manager + b"  len (rec/tot):     94/    94, tx: 0, lsn: 0/00100040,"
            b" prev 0/00100028, desc: " + description + b"\n")
    options = {"target": 0x100040, "max_record_bytes": 96, "record_kind": kind, "restore_name": name}
    assert source._decode_record(line, **options) == 94
    with pytest.raises(ValueError, match="record_start_changed"):
        source._decode_record(line, **(options | {"target": 0x100028}))
    if name is not None:
        with pytest.raises(ValueError, match="record_kind_changed"):
            source._decode_record(line, **(options | {"restore_name": "unexpected"}))


def test_qualifier_retrieves_one_segment_and_suppresses_native_details(tmp_path, monkeypatch):
    repo, work, pgdata = (tmp_path / name for name in ("repo", "work", "pgdata"))
    for path in (repo, work, pgdata):
        path.mkdir(mode=0o700)
    (pgdata / "global").mkdir(mode=0o700)
    control = pgdata / "global" / "pg_control"
    control.write_bytes(b"x" * 8192)
    control.chmod(0o600)
    key = tmp_path / "key"
    key.write_bytes(b"a" * 64 + b"\n")
    key.chmod(0o600)
    wal_segment_bytes = 1 << 20
    segment = "000000010000000000000001"
    data = bytearray(wal_segment_bytes)
    data[:40] = _wal_header(position=wal_segment_bytes, flags=source._LONG_HEADER) + source._LONG.pack(100, wal_segment_bytes, 8192)
    wal_hash = hashlib.sha1(data).hexdigest()
    archive = repo / "archive" / "qt" / "15-1" / segment[:16] / (segment + "-" + wal_hash + ".gz")
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"fixture ciphertext")
    executables = []
    for name in ("pg_waldump", "pgbackrest"):
        path = tmp_path / name
        path.touch(mode=0o700)
        executables.append(path)
    calls = []

    def run(command, *, cwd, env, deadline, check):
        check()
        calls.append(command)
        if command[-1] == "--version":
            return b"pg_waldump (PostgreSQL) 15.6\n"
        if command[-1] == "version":
            return b"pgBackRest 2.59.1\n"
        if command[-3] == "archive-get":
            snapshot, = [Path(argument.split("=", 1)[1]) for argument in command if argument.startswith("--pg1-path=")]
            assert snapshot.is_relative_to(cwd) and snapshot != pgdata
            assert (snapshot / "global" / "pg_control").read_bytes() == control.read_bytes()
            assert command[-2] == segment and len(env["PGBACKREST_REPO1_CIPHER_PASS"]) == 64
            Path(command[-1]).write_bytes(data)
            return b""
        assert "PGBACKREST_REPO1_CIPHER_PASS" not in env
        assert "--limit=1" in command and command[-1] == segment
        return (b"rmgr: XLOG len (rec/tot): 94/ 94, tx: 0, lsn: 0/00100040,"
                b" prev 0/00100028, desc: CHECKPOINT_ONLINE private-native-diagnostics\n")

    monkeypatch.setattr(source, "_run_native", run)
    options = {"pg_waldump": executables[0], "pgbackrest": executables[1], "repository": repo, "database_key": key,
               "workspace": work, "pgdata": pgdata, "archive_id": "15-1", "segment": segment, "expected_wal_sha1": wal_hash,
               "wal_segment_bytes": wal_segment_bytes, "target_lsn": "0/100040", "replay_lsn": "0/1000A0",
               "record_kind": "checkpoint_online", "timeout_seconds": 120, "expected_system_identifier": "100"}
    report = source.qualify_source_wal_target(**options)
    assert report["replay_lsn"] == "0/1000A0" and report["record_total_bytes"] == 94
    assert report["system_identifier"] == "100" and report["wal_sha1"] == wal_hash
    assert "private-native-diagnostics" not in str(report)
    assert report["verification_sha256"] == archive_evidence_hash({field: value for field, value in report.items()
                                                                   if field != "verification_sha256"})
    assert len(calls) == 4 and not any(work.iterdir())
    with pytest.raises(ValueError, match="record_end_changed"):
        source.qualify_source_wal_target(**(options | {"replay_lsn": "0/1000A8"}))
    with pytest.raises(ValueError, match="archive_pin_changed"):
        source.qualify_source_wal_target(**(options | {"expected_wal_sha1": "f" * 40}))

"""Verify the exact deployed QT reader on a private named recovery receiver.

The approved replay_equivalence artifact uses qt.named_recovery_runtime_witness.v1.
It pins database_identity, max_rows, max_archive_bytes, a nonempty frozen list,
and a raw list. Frozen cases specify dataset_id, dataset_hash, series_id, mode
(frozen_revisions or frozen_series), rows and sha256. Raw cases specify
manifest_id, rows, content_fingerprint and sha256. The two fingerprint helpers
serialize these ordered witnesses for an artifact producer.

If a sealed segment already has raw SQL mappings at the restore point, include
its whole acknowledged manifest as a raw case. The recovered spool must match
that independently verified archive exactly. Other sealed frames must have no
archive mapping at the restore point.
"""

from __future__ import annotations

import argparse
import hashlib
from itertools import zip_longest
import json
import logging
import os
from pathlib import Path
import re
import stat
import sys
from tempfile import TemporaryDirectory
from time import monotonic


WITNESS_SCHEMA = "qt.named_recovery_runtime_witness.v1"
_RECEIPT_SCHEMA = "qt.named_recovery_runtime_verification.v1"
_MODES = {"frozen_revisions", "frozen_series"}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _match(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _digest(value):
    return _match(value, r"[a-f0-9]{64}")


def _bounded(value, maximum):
    return type(value) is int and 1 <= value <= maximum


def _file_identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_uid, info.st_nlink


def _unique(pairs):
    data = dict(pairs)
    if len(data) != len(pairs):
        raise ValueError("canonical_receiver_runtime_duplicate_json_member")
    return data


def validate_runtime_witness(witness):
    if (not isinstance(witness, dict)
            or set(witness) != {"schema", "database_identity", "max_rows", "max_archive_bytes", "frozen", "raw"}
            or witness["schema"] != WITNESS_SCHEMA
            or not _match(witness["database_identity"], r"[1-9][0-9]{0,19}/[1-9][0-9]{0,9}")
            or not _bounded(witness["max_rows"], 500_000) or not _bounded(witness["max_archive_bytes"], 2**31)
            or not isinstance(witness["frozen"], list) or not 1 <= len(witness["frozen"]) <= 20
            or not isinstance(witness["raw"], list) or len(witness["raw"]) > 20):
        raise ValueError("canonical_receiver_runtime_witness_invalid")
    frozen, raw = set(), set()
    for row in witness["frozen"]:
        if (not isinstance(row, dict) or set(row) != {"dataset_id", "dataset_hash", "series_id", "mode", "rows", "sha256"}
                or not _match(row["dataset_id"], r"[a-zA-Z0-9_-]{1,96}") or not _digest(row["dataset_hash"])
                or not _bounded(row["series_id"], 2**63 - 1) or not isinstance(row["mode"], str) or row["mode"] not in _MODES
                or not _bounded(row["rows"], witness["max_rows"]) or not _digest(row["sha256"])):
            raise ValueError("canonical_receiver_runtime_frozen_witness_invalid")
        scope = row["dataset_id"], row["series_id"], row["mode"]
        if scope in frozen:
            raise ValueError("canonical_receiver_runtime_duplicate_frozen_witness")
        frozen.add(scope)
    for row in witness["raw"]:
        if (not isinstance(row, dict) or set(row) != {"manifest_id", "rows", "content_fingerprint", "sha256"}
                or not _match(row["manifest_id"], r"[a-zA-Z0-9_-]{1,160}") or row["manifest_id"] in raw
                or not _bounded(row["rows"], witness["max_rows"])
                or not _digest(row["content_fingerprint"]) or not _digest(row["sha256"])):
            raise ValueError("canonical_receiver_runtime_raw_witness_invalid")
        raw.add(row["manifest_id"])
    if sum(row["rows"] for row in witness["frozen"] + witness["raw"]) > witness["max_rows"]:
        raise ValueError("canonical_receiver_runtime_witness_row_budget_exceeded")
    return witness


def fingerprint_frozen_records(records, *, mode, series_id, commit_seq, check=lambda: None):
    """Hash canonical revision rows or the exact values returned by a frozen-series read."""
    digest = hashlib.sha256()
    for row in records:
        check()
        if row.series_id != series_id or not 0 < row.market_commit_seq <= commit_seq:
            raise ValueError("canonical_receiver_runtime_frozen_record_outside_scope")
        if mode == "frozen_revisions":
            material = [row.fact_version_id, row.series_id, row.source_id, row.revision,
                        row.market_commit_seq, row.ingestion_run_id, row.row_hash, row.fact.to_dict()]
        elif mode == "frozen_series":
            material = [row.series_id, row.revision, row.market_commit_seq, row.ingestion_run_id,
                        row.source_identity_key, row.canonical_material_hash, row.fact.to_dict()]
        else:
            raise ValueError("canonical_receiver_runtime_frozen_witness_invalid")
        digest.update((_json(material) + "\n").encode())
    return digest.hexdigest()


def _raw_record_bytes(record):
    return (json.dumps([record.receive_ordinal, record.received_at.isoformat(), record.raw_record_id,
                        record.raw_frame_sha256, record.observed_channel], separators=(",", ":")) + "\n").encode()


def fingerprint_raw_records(records):
    digest = hashlib.sha256()
    for record in records:
        digest.update(_raw_record_bytes(record))
    return digest.hexdigest()


def _native_identity(conn):
    return tuple(conn.exec_driver_sql("""
        SELECT system_identifier::text || '/' || oid::text, current_database(), session_user,
               extract(epoch FROM pg_postmaster_start_time())::text, pg_last_wal_replay_lsn()::text
        FROM pg_control_system(), pg_database WHERE datname=current_database()
          AND inet_server_addr() IS NULL AND inet_client_addr() IS NULL AND current_setting('listen_addresses')=''
          AND NOT pg_is_in_recovery() AND current_setting('archive_mode')='off'
    """).one())


def _socket_url(url):
    if (url.get_backend_name() != "postgresql" or url.host is not None or url.password is not None
            or not _match(url.username, r"[a-z_][a-z0-9_]{0,62}") or not _match(url.database, r"[a-z_][a-z0-9_]{0,62}")
            or set(url.query) != {"host"} or not isinstance(url.query["host"], str) or not _bounded(url.port, 65535)):
        raise ValueError("canonical_receiver_runtime_private_socket_required")
    socket = Path(url.query["host"])
    endpoint = socket / f".s.PGSQL.{url.port}"
    if (not socket.is_absolute() or socket.resolve(strict=True) != socket
            or socket.stat().st_uid != os.getuid() or socket.stat().st_mode & 0o077
            or not stat.S_ISSOCK(endpoint.lstat().st_mode) or endpoint.lstat().st_uid != os.getuid()):
        raise ValueError("canonical_receiver_runtime_private_socket_required")
    return endpoint


def _load_private_json(path, *, digest=None, limit=1024**2):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("canonical_receiver_runtime_private_json_invalid")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.getuid())
                or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > limit):
            raise ValueError("canonical_receiver_runtime_private_json_invalid")
        raw = stream.read(limit + 1)
        if len(raw) != info.st_size or len({_file_identity(value) for value in (info, os.fstat(stream.fileno()), path.lstat())}) != 1:
            raise ValueError("canonical_receiver_runtime_private_json_changed")
    if digest is not None and hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("canonical_receiver_runtime_witness_hash_mismatch")
    return json.loads(raw, object_pairs_hook=_unique)


def bind_receiver_runtime(*, witness_path, witness_sha256, source_checkout, expected_revision,
                          expected_source_tree_sha256, receiving, timeout_seconds=60):
    """Bind the approved witness before the source fence, returning the receiver callback."""
    from portal.backend.service.storage.incremental_recovery import _run_native

    if (not _digest(witness_sha256) or not _digest(expected_source_tree_sha256)
            or not _match(expected_revision, r"[a-f0-9]{40}") or not _bounded(timeout_seconds, 600)):
        raise ValueError("canonical_receiver_runtime_pin_invalid")
    witness = validate_runtime_witness(_load_private_json(witness_path, digest=witness_sha256))
    checkout = Path(source_checkout)
    if not checkout.is_absolute() or checkout.resolve(strict=True) != checkout or not (checkout / "src").is_dir():
        raise ValueError("canonical_receiver_runtime_checkout_invalid")
    receiving = Path(receiving)

    def verify_original_runtime(target_engine, base_restore, suffix_restore, pending, check):
        endpoint = _socket_url(target_engine.url)
        endpoint_id = endpoint.stat().st_dev, endpoint.stat().st_ino
        archive = _load_private_json(receiving / "final-archive/complete.json", limit=65536)
        if archive["database_identity"] != witness["database_identity"]:
            raise RuntimeError("canonical_receiver_runtime_archive_scope_changed")
        restored_archive = archive["archive"]
        # The original path is an absolute staging path preserved by Restic.
        suffix = Path(suffix_restore) / restored_archive["archive_source_root"].lstrip("/")
        if not restored_archive["archive_objects"] and not suffix.exists():
            suffix = None
        with target_engine.connect() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            identity = _native_identity(conn)
        if identity[0] != witness["database_identity"]:
            raise RuntimeError("canonical_receiver_runtime_database_identity_changed")

        base = Path(base_restore)
        if "baseline_checkpoint_sha256" not in restored_archive:
            original = _load_private_json(receiving / "complete.json", limit=65536)
            if original["archive_objects"]:
                base /= original["archive_source_root"].lstrip("/")
        pending_root = None if pending is None else str(Path(suffix_restore).parent / "inactive-pending/spool")
        seconds = min(timeout_seconds, int(check()))
        if seconds < 1:
            raise TimeoutError("canonical_receiver_runtime_deadline")
        deadline = monotonic() + seconds

        def bounded():
            check()
            if monotonic() >= deadline:
                raise TimeoutError("canonical_receiver_runtime_deadline")

        with TemporaryDirectory(prefix="qt-receiver-runtime-") as work:
            request = Path(work) / "request.json"
            request.touch(mode=0o600)
            request.write_text(_json({"witness": witness, "witness_sha256": witness_sha256, "identity": identity,
                "commit_seq": archive["source_commit_seq"], "source_revision": expected_revision,
                "source_tree_sha256": expected_source_tree_sha256, "base": str(base), "suffix": str(suffix) if suffix else None,
                "pending": pending, "pending_root": pending_root, "seconds": seconds}), encoding="utf-8")
            env = {"PATH": os.defpath, "LC_ALL": "C.UTF-8", "HOME": work, "TMPDIR": work,
                   "QT_DISABLE_DOTENV": "1", "PYTHONDONTWRITEBYTECODE": "1",
                   "PYTHONPATH": os.pathsep.join((str(checkout / "src"), str(checkout))),
                   "MARKET_STRUCTURE_STORAGE_ROOT": str(base.parent),
                   "PG_DSN": target_engine.url.render_as_string(hide_password=False),
                   "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"}
            output = bytearray()

            def collect(chunk):
                if len(output) + len(chunk) > 128 * 1024:
                    raise RuntimeError("canonical_receiver_runtime_output_limit")
                output.extend(chunk)

            try:
                _run_native([sys.executable, "-P", str(Path(__file__).resolve()), str(request)], cwd=checkout,
                            env=env, deadline=deadline, check=bounded, stdout_consumer=collect)
            except RuntimeError as error:
                try:
                    diagnostic = json.loads(output, object_pairs_hook=_unique)
                    status = diagnostic.get("error")
                    missing = diagnostic.get("missing_module")
                except (AttributeError, ValueError):
                    status = missing = None
                if _match(status, r"canonical_receiver_runtime_[a-z_]{1,92}"):
                    if _match(missing, r"[A-Za-z_][A-Za-z0-9_.]{0,79}"):
                        status += f":module={missing}"
                    raise RuntimeError(status) from error
                raise
        receipt = json.loads(output, object_pairs_hook=_unique)
        if ((endpoint.stat().st_dev, endpoint.stat().st_ino) != endpoint_id or receipt.get("schema") != _RECEIPT_SCHEMA
                or receipt.get("witness_sha256") != witness_sha256 or receipt.get("identity") != list(identity)):
            raise RuntimeError("canonical_receiver_runtime_receipt_changed")
        return hashlib.sha256(_json(receipt).encode()).hexdigest()

    return verify_original_runtime


def _archive_path(root, key):
    from core.storage_targets import StorageLocation

    StorageLocation("archive", key)
    root = Path(root)
    candidate = root / key
    if (not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir()
            or candidate.resolve() != candidate):
        raise RuntimeError("canonical_receiver_runtime_archive_path_changed")
    return candidate


def _file_digest(path, *, expected_bytes, check):
    digest = hashlib.sha256()
    size = 0
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        raise RuntimeError("canonical_receiver_runtime_archive_file_changed")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as handle:
        if _file_identity(os.fstat(handle.fileno())) != _file_identity(before):
            raise RuntimeError("canonical_receiver_runtime_archive_file_changed")
        while chunk := handle.read(1024**2):
            check()
            size += len(chunk)
            if size > expected_bytes:
                raise RuntimeError("canonical_receiver_runtime_archive_file_changed")
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    # Reading can update access time.
    if size != expected_bytes or len({_file_identity(value) for value in (before, after, path.lstat())}) != 1:
        raise RuntimeError("canonical_receiver_runtime_archive_file_changed")
    return digest.hexdigest()


def _verify_frozen_archive_refs(references, *, store, control, max_archive_bytes):
    """Preserve the original dataset's byte checks across two restored roots."""
    for _, reference in sorted(references.items()):
        control.check()
        uri = reference.get("object_uri")
        expected = reference.get("object_sha256")
        if not isinstance(uri, str) or not uri.startswith("market-archive://") or not _digest(expected):
            raise RuntimeError("canonical_receiver_runtime_frozen_archive_reference_invalid")
        path = store.local_path(reference["object_key"])
        try:
            info = path.lstat()
        except FileNotFoundError as error:
            raise RuntimeError("canonical_receiver_runtime_frozen_archive_missing") from error
        if not stat.S_ISREG(info.st_mode) or not _bounded(info.st_size, max_archive_bytes):
            raise RuntimeError("canonical_receiver_runtime_frozen_archive_file_invalid")
        control.consume("archive_read_bytes", info.st_size)
        if _file_digest(path, expected_bytes=info.st_size, check=control.check) != expected:
            raise RuntimeError("canonical_receiver_runtime_frozen_archive_checksum_changed")


def _verify_raw(conn, case, objects, control, max_archive_bytes):
    from market_data.archive import RawArchiveReadLimits, iter_raw_archive_parquet, read_raw_archive_content_fingerprint
    from sqlalchemy import text

    manifest = conn.execute(text("""
        SELECT m.id,m.definition_id,m.session_id,m.connection_epoch,m.object_key,m.object_sha256,m.byte_count,
               m.record_count,m.content_fingerprint,m.first_receive_ordinal,m.last_receive_ordinal
        FROM market.raw_archive_manifests m WHERE m.id=:id AND NOT EXISTS (
            SELECT FROM market.storage_lifecycle_events e
            WHERE e.target_kind='raw_manifest' AND e.target_id=m.id
              AND e.action='archive_expire' AND e.event_type='completed'
        )
    """), {"id": case["manifest_id"]}).mappings().one_or_none()
    if (manifest is None or manifest["record_count"] != case["rows"]
            or manifest["content_fingerprint"] != case["content_fingerprint"]
            or not _bounded(manifest["byte_count"], max_archive_bytes) or not _digest(manifest["object_sha256"])):
        raise RuntimeError("canonical_receiver_runtime_raw_manifest_changed")
    file = objects.local_path(manifest["object_key"])
    control.consume("archive_read_bytes", manifest["byte_count"])
    if _file_digest(file, expected_bytes=manifest["byte_count"], check=control.check) != manifest["object_sha256"]:
        raise RuntimeError("canonical_receiver_runtime_raw_archive_digest_changed")
    limits = RawArchiveReadLimits(max_rows=case["rows"], max_file_bytes=max_archive_bytes,
                                  max_logical_bytes=max_archive_bytes)
    digest = hashlib.sha256()
    count = 0
    record_scope = tuple(manifest[key] for key in ("definition_id", "session_id", "connection_epoch"))
    with conn.execute(text("""
        SELECT raw_record_id,spool_segment_id,session_id,connection_epoch,receive_ordinal,
               raw_frame_sha256,object_row_index,object_row_group
        FROM market.raw_archive_record_mappings WHERE manifest_id=:id
        ORDER BY object_row_index LIMIT :limit
    """).execution_options(yield_per=128), {"id": manifest["id"], "limit": case["rows"] + 1}) as rows:
        records = iter_raw_archive_parquet(file, limits=limits)
        try:
            for ordinal, (row, record) in enumerate(zip_longest(rows.mappings(), records)):
                control.check()
                if (row is None or record is None or row["object_row_index"] != ordinal or row["object_row_group"] != 0
                        or (record.definition_id, record.session_id, record.connection_epoch) != record_scope
                        or any(row[key] != getattr(record, key) for key in (
                            "raw_record_id", "spool_segment_id", "session_id", "connection_epoch",
                            "receive_ordinal", "raw_frame_sha256"))
                        or ordinal == 0 and record.receive_ordinal != manifest["first_receive_ordinal"]
                        or ordinal == case["rows"] - 1 and record.receive_ordinal != manifest["last_receive_ordinal"]):
                    raise RuntimeError("canonical_receiver_runtime_raw_archive_ordinal_changed")
                count += 1
                digest.update(_raw_record_bytes(record))
        finally:
            records.close()
    fingerprint = read_raw_archive_content_fingerprint(file, limits=limits, check_budget=control.check)
    if (count, digest.hexdigest(), fingerprint) != (case["rows"], case["sha256"], case["content_fingerprint"]):
        raise RuntimeError("canonical_receiver_runtime_raw_witness_changed")
    return {"manifest_id": manifest["id"], "rows": count, "sha256": digest.hexdigest()}


def _assert_pending_inactive(conn, pending, root, *, max_rows, identity, commit_seq, verified_raw, check):
    from market_data.archive import DurableRawSpoolSegment
    from market_data.fact_archive import archive_evidence_hash
    from sqlalchemy import text

    if pending is None:
        return None
    expected_hash = pending["verification_sha256"]
    body = {key: value for key, value in pending.items() if key != "verification_sha256"}
    if (archive_evidence_hash(body) != expected_hash or pending["boundary"]["database_identity"] != identity
            or pending["boundary"]["source_commit_seq"] != commit_seq):
        raise RuntimeError("canonical_receiver_runtime_pending_scope_changed")
    count = archived = 0
    for segment in pending["segments"]:
        if not _bounded(segment["records"], max_rows - count) or not _bounded(segment["bytes"], 128 * 1024**2):
            raise RuntimeError("canonical_receiver_runtime_pending_budget_exceeded")
        path = _archive_path(root, segment["path"])
        if path.suffix != ".sealed" or _file_digest(path, expected_bytes=segment["bytes"], check=check) != segment["sha256"]:
            raise RuntimeError("canonical_receiver_runtime_pending_segment_changed")
        ids, digest = [], hashlib.sha256()
        for record in DurableRawSpoolSegment.from_path(path).records():
            check()
            ids.append(record.raw_record_id)
            digest.update(_raw_record_bytes(record))
        if (len(ids), digest.hexdigest()) != (segment["records"], segment["records_sha256"]):
            raise RuntimeError("canonical_receiver_runtime_pending_record_changed")
        manifests = [raw["manifest_id"] for raw in verified_raw if
                     (raw["rows"], raw["sha256"]) == (segment["records"], segment["records_sha256"])]
        if len(manifests) > 1:
            raise RuntimeError("canonical_receiver_runtime_pending_manifest_ambiguous")
        # Raw acknowledgement and book publication commit separately. A sealed
        # copy may still be needed to recover a book even after raw admission.
        for start in range(0, len(ids), 512):
            check()
            batch = ids[start:start + 512]
            mapped = conn.execute(text("""
                SELECT raw_record_id, manifest_id FROM market.raw_archive_record_mappings
                WHERE raw_record_id=ANY(:ids) LIMIT :limit
            """), {"ids": batch, "limit": len(batch) + 1}).all()
            if manifests:
                expected = {(raw_id, manifests[0]) for raw_id in batch}
                if len(mapped) != len(batch) or set(mapped) != expected:
                    raise RuntimeError("canonical_receiver_runtime_pending_archive_mapping_changed")
            elif mapped:
                raise RuntimeError("canonical_receiver_runtime_pending_record_already_accepted")
        if manifests:
            archived += len(ids)
        count += len(ids)
    return {"rows": count, "archive_mapped_rows": archived, "verification_sha256": expected_hash}


def verify_in_deployed_runtime(request, *, phase=lambda _: None):
    phase("native_import")
    from core.execution_control import ExecutionControl, controlled_execution
    from market_data import archive as native_archive
    from portal.backend.db import session
    from portal.backend.service.storage.repos import fact_storage, market_data
    from scripts.provenance.source_tree_hash import working_tree_hash
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool

    phase("source_attestation")
    checkout = Path.cwd()
    witness = validate_runtime_witness(request["witness"])
    if (working_tree_hash(checkout) != request["source_tree_sha256"] or any(
            not Path(module.__file__).resolve().is_relative_to(checkout)
            for module in (session, market_data, native_archive, fact_storage))):
        raise RuntimeError("canonical_receiver_runtime_source_checkout_changed")
    if market_data.db is not session.db:
        raise RuntimeError("canonical_receiver_runtime_native_database_binding_changed")
    url = make_url(os.environ["PG_DSN"])
    _socket_url(url)
    base = Path(request["base"])
    suffix = None if request["suffix"] is None else Path(request["suffix"])

    class ReceiverArchive(native_archive.FilesystemRawArchiveObjectStore):
        def local_path(self, key):
            original = _archive_path(base, key)
            later = None if suffix is None else _archive_path(suffix, key)
            if later is not None and later.exists():
                if original.exists():
                    raise RuntimeError("canonical_receiver_runtime_multiple_archive_members")
                return later
            return original

    phase("archive_setup")
    store = ReceiverArchive(base, writable=False)
    market_data.canonical_fact_storage_repository = fact_storage.PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: store, read_cache_factory=lambda: None)
    control = ExecutionControl()
    control.limit(seconds=request["seconds"], input_rows=witness["max_rows"], archive_read_bytes=witness["max_archive_bytes"])
    # The deployed accessor still owns catalog membership and requires every
    # referenced object's bytes. Supply its verifier with the restored union.
    market_data._verify_local_archive_objects = lambda references: _verify_frozen_archive_refs(
        references, store=store, control=control, max_archive_bytes=witness["max_archive_bytes"])
    timeout = request["seconds"] * 1000
    phase("database_setup")
    engine = create_engine(url, poolclass=NullPool, connect_args={"connect_timeout": 3,
        "options": f"-c default_transaction_read_only=on -c statement_timeout={timeout} -c lock_timeout={timeout}"})
    repo = market_data.PostgresMarketDataRepository()
    frozen, raw = [], []
    try:
        with controlled_execution(control), engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            identity = _native_identity(conn)
            if list(identity) != request["identity"] or identity[0] != witness["database_identity"]:
                raise RuntimeError("canonical_receiver_runtime_database_identity_changed")
            if repo._current_commit_seq_with_session(conn) != request["commit_seq"]:
                raise RuntimeError("canonical_receiver_runtime_market_commit_changed")
            session.db._engine = engine
            session.db._session_factory = sessionmaker(bind=conn, expire_on_commit=False, autoflush=False)
            session.db._available = True
            for case in witness["frozen"]:
                phase("frozen_manifest")
                control.check()
                dataset = repo.get_dataset(case["dataset_id"])
                if dataset.dataset_hash != case["dataset_hash"] or not 0 < dataset.max_commit_seq <= request["commit_seq"]:
                    raise RuntimeError("canonical_receiver_runtime_frozen_dataset_changed")
                method = repo.read_dataset_fact_revisions if case["mode"] == "frozen_revisions" else repo.read_dataset_series
                phase("frozen_read")
                rows = method(dataset_id=case["dataset_id"], series_id=case["series_id"])
                if (len(rows) != case["rows"] or fingerprint_frozen_records(rows, mode=case["mode"],
                        series_id=case["series_id"], commit_seq=dataset.max_commit_seq, check=control.check) != case["sha256"]):
                    raise RuntimeError("canonical_receiver_runtime_frozen_witness_changed")
                frozen.append(case)
            for case in witness["raw"]:
                phase("raw_archive")
                raw.append(_verify_raw(conn, case, store, control, witness["max_archive_bytes"]))
            phase("pending_spool")
            pending = _assert_pending_inactive(conn, request["pending"], request["pending_root"], max_rows=witness["max_rows"],
                                               identity=identity[0], commit_seq=request["commit_seq"],
                                               verified_raw=raw, check=control.check)
            control.check()
    finally:
        engine.dispose()
    return {"schema": _RECEIPT_SCHEMA, "witness_sha256": request["witness_sha256"], "identity": identity,
            "source_revision": request["source_revision"], "source_tree_sha256": request["source_tree_sha256"],
            "commit_seq": request["commit_seq"], "frozen": frozen, "raw": raw, "pending": pending}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request", type=Path)
    args = parser.parse_args(argv)
    logging.disable(logging.CRITICAL)
    stage = "request"

    def phase(new_stage):
        nonlocal stage
        stage = new_stage

    try:
        receipt = verify_in_deployed_runtime(_load_private_json(args.request, limit=8 * 1024**2), phase=phase)
    except Exception as error:
        detail = str(error)
        approved_upstream = {
            "market_archive_requires_pyarrow": "native_arrow_unavailable",
            "market_archive_replay_invalid": "native_archive_invalid",
            "market_archive_read_row_budget_exceeded": "native_archive_row_budget_exceeded",
            "market_archive_read_logical_budget_exceeded": "native_archive_byte_budget_exceeded",
            "market_spool_corrupt": "native_spool_corrupt",
            "market_dataset_revision_history_unpinned": "native_dataset_unpinned",
            "research_execution_budget_exceeded": "native_execution_budget_exceeded",
            "Portal database is not available": "native_database_unavailable",
        }
        safe_reason = approved_upstream.get(detail.partition(":")[0], f"{stage}_failed")
        status = detail if _match(detail, r"canonical_receiver_runtime_[a-z_]{1,92}") else f"canonical_receiver_runtime_{safe_reason}"
        report = {"error": status}
        if isinstance(error, ModuleNotFoundError) and _match(error.name, r"[A-Za-z_][A-Za-z0-9_.]{0,79}"):
            report = {"error": "canonical_receiver_runtime_dependency_unavailable", "missing_module": error.name}
        print(_json(report), flush=True)
        raise SystemExit(1) from None
    print(_json(receipt))


if __name__ == "__main__":
    main()

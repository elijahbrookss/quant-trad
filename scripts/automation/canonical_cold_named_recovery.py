"""Coordinate one approved named restore over an already authorized mounted exchange."""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from time import monotonic, sleep, time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes, _run_native
from portal.backend.service.storage.recovery_copies import _json_write, _sync
from scripts.automation.canonical_cold_collector_checks import RECEIPT_VERSION as COLLECTOR_RECEIPT_VERSION
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _identity, _private_root
from scripts.automation.storage_host_boundary import load_receipt
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json, _selected_backup, verify_handoff
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_recovery_anchor import create_original_recovery_anchor
from scripts.db.canonical_cold_recovery_archive_transport import stage_encrypted_archive_suffix, verify_named_archive_closure
from scripts.db.canonical_cold_recovery_pending_spool import PendingSpoolScope, measure_pending_spool, stage_pending_spool, verify_pending_spool
from scripts.db.canonical_cold_recovery_suffix import PreparedArchiveBaseline, prepare_archive_baseline
from scripts.db.canonical_cold_recovery_wal_transfer import _baseline, verify_final_wal, stage_final_wal
from scripts.db.fact_header_v2_capture import _bounded_step


_REQUEST = "qt.named_recovery_site_request.v1"
_RESPONSE = "qt.named_recovery_site_response.v1"
_MAX_RECEIPT = 1024 * 1024
_MAX_PROVIDER_RECEIPT = 128 * 1024**2


@dataclass(frozen=True)
class NamedRecoverySite:
    """Local policy, provisioned separately on both hosts before the hold."""

    receiving: Path
    approved_package_sha256: str
    base_transfer_sha256: str
    baseline_verification: dict
    source_public_key: str
    receiver_public_key: str
    wal: TransferLimits
    archive: TransferLimits
    pending: TransferLimits
    database: TransferLimits
    max_archive_objects: int
    max_pending_entries: int

    def __post_init__(self):
        pins = (self.approved_package_sha256, self.base_transfer_sha256, self.source_public_key, self.receiver_public_key)
        if (not all(map(_sha256, pins)) or self.source_public_key == self.receiver_public_key
                or not all(isinstance(limit, TransferLimits) for limit in (self.wal, self.archive, self.pending, self.database))
                or type(self.max_archive_objects) is not int or not 1 <= self.max_archive_objects <= 10_000_000
                or type(self.max_pending_entries) is not int or not 1 <= self.max_pending_entries <= 1_000_000):
            raise ValueError("canonical_named_recovery_invalid_site_policy")
        _baseline(self.receiving, self.base_transfer_sha256, self.baseline_verification,
                  self.baseline_verification["verification_sha256"])


def _limit(limit, check):
    return replace(limit, seconds=min(limit.seconds, check()))


def _signed_bytes(body):
    return json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _signer(private_path, public, receiving):
    path = Path(private_path)
    if path.is_relative_to(receiving):
        raise ValueError("canonical_named_recovery_private_key_on_exchange")
    encoded = _private_bytes(path, limit=128).decode("ascii").strip()
    if not _sha256(encoded):
        raise ValueError("canonical_named_recovery_signing_key_invalid")
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(encoded))
    if key.public_key().public_bytes_raw().hex() != public:
        raise ValueError("canonical_named_recovery_signing_key_unapproved")
    return key


def _sign(body, key):
    return {"body": body, "signature": key.sign(_signed_bytes(body)).hex()}


def _opened(path, public):
    payload = load_receipt(path, max_bytes=_MAX_RECEIPT)
    if (not isinstance(payload, dict) or set(payload) != {"body", "signature"} or not isinstance(payload["body"], dict)
            or not isinstance(payload["signature"], str) or not re.fullmatch(r"[0-9a-f]{128}", payload["signature"])):
        raise ValueError("canonical_named_recovery_invalid_envelope")
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(public)).verify(
            bytes.fromhex(payload["signature"]), _signed_bytes(payload["body"]),
        )
    except InvalidSignature:
        raise ValueError("canonical_named_recovery_signature_mismatch") from None
    return payload["body"]


def _publish(path, body):
    with TemporaryDirectory(prefix=".qt-named-exchange-", dir=path.parent) as work:
        staging = Path(work) / path.name
        _json_write(staging, body)
        os.link(staging, path)
    _sync(path.parent)


def _file_hash(path):
    with path.open("rb") as stream:
        return hashlib.sha256(stream.read(65537)).hexdigest()


def _wal_options(site, anchor_path, anchor_sha256, check):
    return {"base_transfer_sha256": site.base_transfer_sha256, "baseline": site.baseline_verification,
            "baseline_verification_sha256": site.baseline_verification["verification_sha256"],
            "anchor_path": anchor_path, "anchor_sha256": anchor_sha256, "limits": _limit(site.wal, check)}


def _private_credential(path, *, receiving, workspace=None):
    path = Path(path)
    if path.is_relative_to(receiving) or (workspace is not None and path.is_relative_to(workspace)):
        raise ValueError("canonical_named_recovery_private_key_on_exchange")
    return path


def bind_named_recovery_source(*, site, signing_key, provider_receipt_root, database_repository, archive_objects,
                               scratch, baseline, restic, archive_key, pending_key=None, pending_spool=None):
    """Make the launcher callback. Actual recovery still requires the approved live fence."""
    root, root_id = _private_root(site.receiving)
    scratch, scratch_id = _private_root(scratch)
    receipts, receipts_id = _private_root(provider_receipt_root)
    if scratch_id[0] == root_id[0] or not isinstance(baseline, PreparedArchiveBaseline) or (
            baseline.receipt_sha256 != site.baseline_verification["pair_receipt_sha256"]):
        raise ValueError("canonical_named_recovery_source_storage_invalid")
    archive_key = _private_credential(archive_key, receiving=root)
    signer = _signer(signing_key, site.source_public_key, root)
    if pending_key is not None:
        pending_key = _private_credential(pending_key, receiving=root)

    def recover(fence, control, source_commit, provider_boundary, remaining, check_archive):
        if (not isinstance(fence, DatabaseFence) or not _sha256(provider_boundary)
                or fence.connection.in_transaction() or control.in_transaction()):
            raise ValueError("canonical_named_recovery_live_fence_required")
        deadline = min(fence.deadline, monotonic() + remaining())

        def check():
            fence.check_clock()
            check_archive()
            if any(_identity(path.lstat()) != pinned for path, pinned in ((root, root_id), (scratch, scratch_id), (receipts, receipts_id))):
                raise RuntimeError("canonical_named_recovery_source_mount_changed")
            seconds = min(int(remaining()), int(deadline - monotonic()))
            if seconds < 1:
                raise TimeoutError("canonical_named_recovery_receiver_deadline")
            return seconds

        check()
        drained = load_receipt(receipts / f"collector-drained-{provider_boundary}.json", max_bytes=_MAX_PROVIDER_RECEIPT)
        evidence = drained["evidence"]
        if (drained.get("schema") != COLLECTOR_RECEIPT_VERSION or drained.get("phase") != "drained"
                or drained.get("sha256") != provider_boundary or archive_evidence_hash({key: value for key, value in drained.items()
                                                                                       if key != "sha256"}) != provider_boundary
                or evidence.get("source_commit_seq") != source_commit):
            raise ValueError("canonical_named_recovery_provider_boundary_changed")
        pending_before = evidence.get("pending_spool")
        if (pending_before is None) != (pending_spool is None and pending_key is None):
            raise ValueError("canonical_named_recovery_pending_configuration_changed")
        if pending_before is not None and (pending_spool is None or pending_key is None):
            raise ValueError("canonical_named_recovery_pending_configuration_changed")
        anchor_path = root / "named-recovery-anchor.json"
        request_path = root / "named-recovery-request.json"
        response_path = root / "named-recovery-response.json"
        if any(path.exists() or path.is_symlink() for path in (anchor_path, request_path, response_path)):
            raise RuntimeError("canonical_named_recovery_exchange_already_used")

        conn = fence.connection
        with TemporaryDirectory(prefix="qt-named-source-", dir=scratch) as work, conn.begin(), _bounded_step(conn, min(600, check())):
            anchor = create_original_recovery_anchor(fence, control, expected_source_commit_seq=source_commit)
            _publish(anchor_path, anchor)
            anchor_hash = _file_hash(anchor_path)
            wal_options = _wal_options(site, anchor_path, anchor_hash, check)
            wal = stage_final_wal(fence=fence, control=control, source=database_repository, receiving=root, **wal_options)
            stage = Path(work) / "accepted"
            stage.mkdir(mode=0o700)
            archived = stage_encrypted_archive_suffix(fence=fence, baseline=baseline, source_root=archive_objects,
                plaintext_stage=stage, receiving=root, wal_options=wal_options, expected_wal_sha256=wal["wal_transfer_sha256"],
                restic=restic, encryption_key=archive_key, max_objects=site.max_archive_objects,
                max_plaintext_bytes=site.archive.bytes, limits=_limit(site.archive, check), check_archive=check)
            pending = source = None
            if pending_before is not None:
                scope = PendingSpoolScope(**pending_before["scope"])
                boundary = {"database_identity": baseline.database_identity, "source_commit_seq": source_commit, "anchor_sha256": anchor_hash}
                source = measure_pending_spool(source=pending_spool, scope=scope, boundary=boundary,
                    timeout_seconds=min(600, check()), max_entries=site.max_pending_entries, check_held=check)
                transported = stage_pending_spool(source=pending_spool, scope=scope, boundary=boundary, receiving=root,
                    private_scratch=Path(work), restic=restic, encryption_key=pending_key, limits=_limit(site.pending, check),
                    max_entries=site.max_pending_entries, check_held=check, pre_anchor=pending_before,
                    expected_pre_anchor_sha256=pending_before["source_sha256"])
                pending = {"transfer_sha256": transported["transfer_sha256"], "pre_anchor_sha256": pending_before["source_sha256"]}
                if source["segments"] != transported["segments"]:
                    raise RuntimeError("canonical_named_recovery_pending_source_changed")
            fence.assert_quiet(conn)
            request = {"schema": _REQUEST, "approved_package_sha256": site.approved_package_sha256,
                       "database_identity": baseline.database_identity, "source_commit_seq": source_commit,
                       "provider_boundary_sha256": provider_boundary, "anchor_sha256": anchor_hash,
                       "base_transfer_sha256": site.base_transfer_sha256,
                       "baseline_verification_sha256": site.baseline_verification["verification_sha256"],
                       "wal_transfer_sha256": wal["wal_transfer_sha256"], "archive_transfer_sha256": archived["archive_transfer_sha256"],
                       "pending": pending, "pending_source": source, "expires_at": int(time()) + check()}
            _publish(request_path, _sign(request, signer))
        expected_request = archive_evidence_hash(request)
        while not response_path.exists():
            check()
            sleep(0.1)
        reply = _opened(response_path, site.receiver_public_key)
        if (set(reply) != {"schema", "request_sha256", "archive_closure", "pending_spool_verified", "runtime_sha256"}
                or reply["schema"] != _RESPONSE or reply["request_sha256"] != expected_request
                or not _sha256(reply["runtime_sha256"])):
            raise RuntimeError("canonical_named_recovery_response_binding_changed")
        closure = reply["archive_closure"]
        if (not isinstance(closure, dict) or any(closure.get(key) != request[key] for key in (
                "database_identity", "source_commit_seq", "anchor_sha256", "wal_transfer_sha256", "archive_transfer_sha256",
                "base_transfer_sha256", "baseline_verification_sha256",))
                or closure.get("receiver_runtime_sha256") != reply["runtime_sha256"]):
            raise RuntimeError("canonical_named_recovery_response_boundary_changed")
        with conn.begin(), _bounded_step(conn, min(5, check())):
            fence.assert_quiet(conn)
        check()
        return {"archive_closure": closure, "pending_spool_source": source, "pending_spool_verified": reply["pending_spool_verified"]}

    return recover


def _read_request(root, site):
    request = _opened(root / "named-recovery-request.json", site.source_public_key)
    baseline = site.baseline_verification
    required = {"schema", "approved_package_sha256", "database_identity", "source_commit_seq", "provider_boundary_sha256",
                "anchor_sha256", "base_transfer_sha256", "baseline_verification_sha256", "wal_transfer_sha256",
                "archive_transfer_sha256", "pending", "pending_source", "expires_at"}
    if (set(request) != required or request["schema"] != _REQUEST
            or request["approved_package_sha256"] != site.approved_package_sha256
            or request["base_transfer_sha256"] != site.base_transfer_sha256
            or request["baseline_verification_sha256"] != baseline["verification_sha256"]
            or request["database_identity"] != baseline["database_identity"]
            or type(request["source_commit_seq"]) is not int or request["source_commit_seq"] < 0
            or not all(_sha256(request[key]) for key in ("provider_boundary_sha256", "anchor_sha256", "wal_transfer_sha256", "archive_transfer_sha256"))
            or type(request["expires_at"]) is not int or request["expires_at"] <= time()
            or (request["pending"] is None) != (request["pending_source"] is None)):
        raise ValueError("canonical_named_recovery_request_outside_approval")
    path = root / "named-recovery-anchor.json"
    anchor = _bound_json(path, request["anchor_sha256"])
    if (anchor["source_commit_seq"] != request["source_commit_seq"]
            or not re.fullmatch(r"qt_original_[a-f0-9]{32}", anchor["restore_name"])
            or type(anchor["timeline"]) is not int or not 1 <= anchor["timeline"] <= 2**32 - 1):
        raise ValueError("canonical_named_recovery_anchor_changed")
    return request, anchor, path


def _native_restore(*, root, workspace, pgdata, tablespaces, pgbackrest, database_key, pair, anchor, budget, deadline, check):
    binary = _binary(pgbackrest)
    key = _private_bytes(database_key, limit=4096).decode("ascii").strip()
    if not _sha256(key):
        raise ValueError("canonical_named_recovery_database_key_invalid")
    settings = workspace / "pgbackrest.conf"
    values = tuple(map(str, (root, workspace, pgdata, binary, *tablespaces.values())))
    if any(not re.fullmatch(r"/[a-zA-Z0-9_./-]+", value) for value in values):
        raise ValueError("canonical_named_recovery_native_configuration_path_invalid")
    with os.fdopen(os.open(settings, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "w") as output:
        output.write(f"[global]\nrepo1-path={root / 'database'}\nrepo1-cipher-type=aes-256-cbc\nrepo1-cipher-pass={key}\n"
                     f"lock-path={workspace}\nlog-level-file=off\nlog-level-console=off\n[qt]\npg1-path={pgdata}\n")
        output.flush()
        os.fsync(output.fileno())
    env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": str(workspace), "TMPDIR": str(workspace)}
    devices = {path.stat().st_dev: path for path in (pgdata, *tablespaces.values())}
    starting = {device: shutil.disk_usage(path).free for device, path in devices.items()}
    restore_deadline = min(deadline, monotonic() + budget.seconds)

    def check_restore():
        check()
        if monotonic() >= restore_deadline:
            raise TimeoutError("canonical_named_recovery_database_restore_deadline")
        free = {device: shutil.disk_usage(path).free for device, path in devices.items()}
        allocated = sum(max(0, starting[device] - size) for device, size in free.items())
        if min(free.values()) < budget.reserve_bytes or allocated > budget.bytes:
            raise RuntimeError("canonical_named_recovery_database_restore_budget_exceeded")

    try:
        version = _run_native([str(binary), "version"], cwd=workspace, env=env, deadline=restore_deadline, check=check_restore)
        if version.strip() != b"pgBackRest 2.59.1":
            raise RuntimeError("canonical_named_recovery_pgbackrest_unqualified")
        selection = [str(binary), f"--config={settings}", "--stanza=qt", f"--set={pair['database_label']}"]
        meta = _run_native([*selection, "--output=json", "info"], cwd=workspace, env=env, deadline=restore_deadline, check=check_restore)
        selected, _, _ = _selected_backup(meta, pair)
        database = anchor["database"]
        stored_spaces = selected.get("tablespace") or []
        if (selected.get("link") or len(stored_spaces) != len(tablespaces)
                or {space["name"] for space in stored_spaces} != set(tablespaces)
                or {"name": database["database"], "oid": database["database_oid"]} not in selected["database-ref"]):
            raise ValueError("canonical_named_recovery_native_manifest_scope_changed")
        physical_bytes = selected["info"]["size"]
        if (type(physical_bytes) is not int or not 0 < physical_bytes <= budget.bytes
                or sum(max(0, free - budget.reserve_bytes) for free in starting.values()) < physical_bytes):
            raise RuntimeError("canonical_named_recovery_database_restore_budget_exceeded")
        command = [*selection, "--type=name",
                   f"--target={anchor['restore_name']}", f"--target-timeline={anchor['timeline']}", "--target-action=promote",
                   "--archive-mode=off", f'--recovery-option=restore_command={binary} --config={settings} --stanza=qt archive-get %f "%p"']
        command.extend(f"--tablespace-map={name}={path}" for name, path in sorted(tablespaces.items()))
        _run_native([*command, "restore"], cwd=workspace, env=env, deadline=restore_deadline, check=check_restore)
    except BaseException:
        settings.unlink()
        _sync(workspace)
        raise
    return settings


def _local_receiver(engine):
    url = engine.url
    query_host = url.query.get("host")
    if (url.get_backend_name() != "postgresql" or url.host not in {None, "127.0.0.1", "::1", "restored"}
            or (query_host is not None and not str(query_host).startswith("/"))):
        raise ValueError("canonical_named_recovery_receiver_endpoint_invalid")


def _open_receive_database(control_engine, target_engine, anchor, pgdata, tablespaces, check):
    for engine in (control_engine, target_engine):
        _local_receiver(engine)
    database = anchor["database"]
    if (control_engine.url.database != "postgres" or target_engine.url.database != database["database"]
            or control_engine.url.set(database=target_engine.url.database) != target_engine.url):
        raise ValueError("canonical_named_recovery_receiver_endpoint_invalid")
    with control_engine.begin() as conn, _bounded_step(conn, min(5, check())):
        state = conn.execute(text("""
            SELECT system_identifier::text, d.oid::bigint, datallowconn, pg_is_in_recovery(),
                   pg_last_wal_replay_lsn() = CAST(:lsn AS pg_lsn), current_setting('recovery_target_name'),
                   current_setting('archive_mode'), extract(epoch FROM pg_postmaster_start_time())::text,
                   current_setting('data_directory')
            FROM pg_control_system() CROSS JOIN pg_database d WHERE d.datname=:database
        """), {"database": database["database"], "lsn": anchor["anchor_lsn"]}).one()
        if (tuple(state[:7]) != (database["system_identifier"], database["database_oid"], False, False, True,
                                 anchor["restore_name"], "off") or state[7] == database["postmaster_started_at"]
                or Path(state[8]).resolve(strict=True) != pgdata):
            raise RuntimeError("canonical_named_recovery_receiver_postmaster_changed")
        observed_spaces = dict(conn.execute(text("""
            SELECT spcname,pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')
        """)).tuples().all())
        if observed_spaces != {name: str(path) for name, (path, _) in tablespaces.items()}:
            raise RuntimeError("canonical_named_recovery_receiver_tablespace_changed")
        quoted = conn.dialect.identifier_preparer.quote_identifier(database["database"])
        conn.exec_driver_sql(f"ALTER DATABASE {quoted} ALLOW_CONNECTIONS true")


def recover_named_site(*, site, signing_key, pgbackrest, restic, database_key, base_archive_key, suffix_key,
                       pending_key, workspace, pgdata, tablespaces, boot_isolated, verify_original_runtime, timeout_seconds):
    """Read a signed request, run native verification, and publish the receiver's signed reply."""
    root, root_id = _private_root(site.receiving)
    workspace, work_id = _private_root(workspace)
    pgdata, pgdata_id = _private_root(pgdata)
    if (root.is_relative_to(workspace) or workspace.is_relative_to(root) or not pgdata.is_relative_to(workspace)
            or pgdata == workspace or any(pgdata.iterdir()) or not all(map(callable, (boot_isolated, verify_original_runtime)))
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600 or not isinstance(tablespaces, dict)
            or not all(isinstance(name, str) and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]{0,62}", name) for name in tablespaces)):
        raise ValueError("canonical_named_recovery_receiver_workspace_invalid")
    destinations = {name: _private_root(path) for name, path in tablespaces.items()}
    if any(any(path.iterdir()) or path.is_relative_to(pgdata) or pgdata.is_relative_to(path)
           or path.is_relative_to(root) or root.is_relative_to(path) for path, _ in destinations.values()):
        raise ValueError("canonical_named_recovery_receiver_tablespace_invalid")
    if len({identity for _, identity in destinations.values()}) != len(destinations):
        raise ValueError("canonical_named_recovery_receiver_tablespace_invalid")
    if any(left != right and (left.is_relative_to(right) or right.is_relative_to(left))
           for left, _ in destinations.values() for right, _ in destinations.values()):
        raise ValueError("canonical_named_recovery_receiver_tablespace_invalid")
    database_key, base_archive_key, suffix_key = (_private_credential(path, receiving=root, workspace=workspace)
                                                  for path in (database_key, base_archive_key, suffix_key))
    signing_key = _private_credential(signing_key, receiving=root, workspace=workspace)
    signer = _signer(signing_key, site.receiver_public_key, root)
    if pending_key is not None:
        pending_key = _private_credential(pending_key, receiving=root, workspace=workspace)
    deadline = monotonic() + timeout_seconds
    roots = ((root, root_id), (workspace, work_id), (pgdata, pgdata_id), *destinations.values())

    def check():
        seconds = int(deadline - monotonic())
        if seconds < 1:
            raise TimeoutError("canonical_named_recovery_receiver_deadline")
        if any(_identity(path.lstat()) != identity for path, identity in roots):
            raise RuntimeError("canonical_named_recovery_receiver_mount_changed")
        return seconds

    request_path = root / "named-recovery-request.json"
    response_path = root / "named-recovery-response.json"
    while not request_path.exists():
        check()
        sleep(0.1)
    request, anchor, anchor_path = _read_request(root, site)
    deadline = min(deadline, monotonic() + request["expires_at"] - time())
    if response_path.exists() or response_path.is_symlink():
        raise RuntimeError("canonical_named_recovery_exchange_already_used")
    wal_options = _wal_options(site, anchor_path, request["anchor_sha256"], check)
    pair = _baseline(root, site.base_transfer_sha256, site.baseline_verification, wal_options["baseline_verification_sha256"])
    verify_final_wal(receiving=root, expected_hash=request["wal_transfer_sha256"], **wal_options)
    base_restore = workspace / "base-archive"
    suffix_restore = workspace / "final-archive"
    for path in (base_restore, suffix_restore):
        path.mkdir(mode=0o700)
    settings = _native_restore(root=root, workspace=workspace, pgdata=pgdata,
        tablespaces={name: path for name, (path, _) in destinations.items()}, pgbackrest=pgbackrest,
        database_key=database_key, pair=pair, anchor=anchor, budget=site.database, deadline=deadline, check=check)
    try:
        native = verify_handoff(repository=root, receipt_sha256=site.baseline_verification["pair_receipt_sha256"],
            archive_restore=base_restore, pgbackrest=pgbackrest, restic=restic, database_key=database_key, archive_key=base_archive_key,
            timeout_seconds=check(), max_archive_bytes=site.archive.bytes, max_archive_objects=site.max_archive_objects,
            wal_segment_bytes=site.baseline_verification["wal_segment_bytes"], max_wal_segments=site.baseline_verification["wal_segments"] + site.wal.files,
            reserve_bytes=site.archive.reserve_bytes, anchor=anchor_path, anchor_sha256=request["anchor_sha256"], isolated_pgdata=pgdata)
        baseline = prepare_archive_baseline(root / "complete.json", base_restore,
            expected_receipt_sha256=native["pair_receipt_sha256"], max_objects=site.max_archive_objects, check=check)
        pending_verified = None
        if request["pending"] is not None:
            pending = request["pending"]
            source = request["pending_source"]
            boundary = {"database_identity": request["database_identity"], "source_commit_seq": request["source_commit_seq"],
                        "anchor_sha256": request["anchor_sha256"]}
            if (pending_key is None or set(pending) != {"transfer_sha256", "pre_anchor_sha256"}
                    or source.get("boundary") != boundary or source.get("schema") != "qt.pending_raw_spool_source.v1"):
                raise ValueError("canonical_named_recovery_pending_request_changed")
            inactive = workspace / "inactive-pending"
            inactive.mkdir(mode=0o700)
            pending_verified = verify_pending_spool(receiving=root, expected_transfer_sha256=pending["transfer_sha256"],
                scope=PendingSpoolScope(**source["scope"]), boundary=boundary, inactive_restore=inactive, restic=restic,
                decryption_key=pending_key, limits=_limit(site.pending, check), max_entries=site.max_pending_entries,
                expected_pre_anchor_sha256=pending["pre_anchor_sha256"])
            if pending_verified["segments"] != source["segments"]:
                raise RuntimeError("canonical_named_recovery_pending_receipts_differ")
        with boot_isolated(pgdata, check) as (control_engine, target_engine):
            _open_receive_database(control_engine, target_engine, anchor, pgdata, destinations, check)
            with target_engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                closure = verify_named_archive_closure(conn, receiving=root, baseline=baseline, wal_options=wal_options,
                    expected_wal_sha256=request["wal_transfer_sha256"], native_named=native, native_named_sha256=native["verification_sha256"],
                    archive_transfer_sha256=request["archive_transfer_sha256"], archive_restore=suffix_restore, restic=restic,
                    decryption_key=suffix_key, max_objects=site.max_archive_objects, limits=_limit(site.archive, check))
            runtime = verify_original_runtime(target_engine, base_restore, suffix_restore, pending_verified, check)
            if not _sha256(runtime):
                raise RuntimeError("canonical_named_recovery_original_runtime_unverified")
    finally:
        settings.unlink()
        _sync(workspace)
    check()
    closure = {key: value for key, value in closure.items() if key != "closure_sha256"}
    closure["receiver_runtime_sha256"] = runtime
    closure["closure_sha256"] = archive_evidence_hash(closure)
    reply = {"schema": _RESPONSE, "request_sha256": archive_evidence_hash(request), "archive_closure": closure,
             "pending_spool_verified": pending_verified, "runtime_sha256": runtime}
    _publish(response_path, _sign(reply, signer))
    return reply

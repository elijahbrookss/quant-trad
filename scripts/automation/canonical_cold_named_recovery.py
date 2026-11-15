"""Coordinate one approved named restore over an already authorized mounted exchange."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import fcntl
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import re
from secrets import token_hex
import shutil
from tempfile import TemporaryDirectory
from time import monotonic, sleep, time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from sqlalchemy import text

from core.storage_mounts import inspect_filesystem
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes, _run_native
from portal.backend.service.storage.recovery_copies import _json_write, _sync
from scripts.automation.canonical_cold_collector_checks import RECEIPT_VERSION as COLLECTOR_RECEIPT_VERSION
from scripts.automation.canonical_cold_receiver_ready import (
    PREPARED_READY_FILENAME, make_prepared_receiver_ready, prepared_receiver_scope, verify_prepared_receiver_ready,
    verify_replayed_checkpoint_baseline,
)
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _identity, _private_root
from scripts.automation.storage_host_boundary import load_receipt
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json, _selected_backup_manifest, verify_handoff
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_recovery_anchor import create_original_recovery_anchor
from scripts.db.canonical_cold_recovery_archive_transport import (
    PREPARED_NATIVE_VERSION, close_named_archive_suffix, restore_named_archive_suffix, stage_encrypted_archive_suffix,
)
from scripts.db.canonical_cold_recovery_pending_spool import PendingSpoolScope, measure_pending_spool, stage_pending_spool, verify_pending_spool
from scripts.db.canonical_cold_recovery_suffix import (
    PreparedArchiveBaseline, prepare_archive_baseline, prepare_replayed_archive_baseline,
)
from scripts.db.canonical_cold_recovery_wal_transfer import (
    _MAX_SEGMENTS, _baseline, _prefix_window, stage_final_wal, verify_wal_prefix,
)
from scripts.db.canonical_cold_prepared_custody import capture_prepared_custody
from scripts.db.canonical_cold_replayed_checkpoint import build_replayed_checkpoint
from scripts.db.canonical_cold_retained_wal import capture_retained_wal
from scripts.db.fact_header_v2_capture import _bounded_step


_REQUEST = "qt.named_recovery_site_request.v3"
_RESPONSE = "qt.named_recovery_site_response.v3"
PREFLIGHT_VERSION = "qt.named_recovery_receiver_preflight.v2"
_WAL_PREFIX_VERIFICATION = "qt.named_recovery_wal_prefix_verification.v1"
_MAX_RECEIPT = 1024 * 1024
_MAX_PROVIDER_RECEIPT = 128 * 1024**2
_TABLESPACE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,62}")


@dataclass(frozen=True)
class RecoverySitePolicy:
    """Local settings and verification inputs available before approval."""

    receiving: Path
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
        pins = (self.base_transfer_sha256, self.source_public_key, self.receiver_public_key)
        if (not all(map(_sha256, pins)) or self.source_public_key == self.receiver_public_key
                or not all(isinstance(limit, TransferLimits) for limit in (self.wal, self.archive, self.pending, self.database))
                or type(self.max_archive_objects) is not int or not 1 <= self.max_archive_objects <= 10_000_000
                or type(self.max_pending_entries) is not int or not 1 <= self.max_pending_entries <= 1_000_000):
            raise ValueError("canonical_named_recovery_invalid_site_policy")
        _baseline(self.receiving, self.base_transfer_sha256, self.baseline_verification,
                  self.baseline_verification["verification_sha256"])

    def preflight_scope(self):
        return {"receiving": str(self.receiving), "source_public_key": self.source_public_key,
                "receiver_public_key": self.receiver_public_key, "base_transfer_sha256": self.base_transfer_sha256,
                **{name: self.baseline_verification[name] for name in ("database_identity", "pair_receipt_sha256")},
                "baseline_verification_sha256": self.baseline_verification["verification_sha256"],
                **{name: asdict(getattr(self, name)) for name in ("wal", "archive", "pending", "database")},
                "max_archive_objects": self.max_archive_objects, "max_pending_entries": self.max_pending_entries}


@dataclass(frozen=True)
class NamedRecoverySite(RecoverySitePolicy):
    """The final approval and signed preflight used by both named recovery owners."""

    approved_package_sha256: str
    receiver_preflight: dict
    replayed_checkpoint_sha256: str | None = None
    max_restart_segments: int | None = None

    def __post_init__(self):
        super().__post_init__()
        if ((self.replayed_checkpoint_sha256 is None) != (self.max_restart_segments is None)
                or self.replayed_checkpoint_sha256 is not None and (
                    not _sha256(self.replayed_checkpoint_sha256) or type(self.max_restart_segments) is not int
                    or not 1 <= self.max_restart_segments <= 2**44)):
            raise ValueError("canonical_named_recovery_checkpoint_pin_invalid")
        self.checked_preflight()

    def checked_preflight(self):
        measured = verify_receiver_preflight(self.receiver_preflight, self.receiver_public_key)
        if not _sha256(self.approved_package_sha256) or measured["site_policy"] != self.preflight_scope():
            raise ValueError("canonical_named_recovery_preflight_policy_changed")
        return measured


def receiver_host_sha256():
    """Fingerprint the receiver's provisioned Linux machine identity."""
    with Path("/etc/machine-id").open("rb") as stream:
        identity = stream.read(129).strip()
    if not re.fullmatch(rb"[0-9a-f]{32}", identity) or identity == b"0" * 32:
        raise ValueError("canonical_recovery_preflight_host_identity_unavailable")
    return hashlib.sha256(b"qt.recovery.receiver.machine-id.v1\0" + identity).hexdigest()


def _assert_receiver_preflight_mounts(measured, root, workspace, pgdata, destinations):
    paths = {"exchange": root, "workspace": workspace, "pgdata": pgdata,
             **{f"tablespace:{name}": path for name, (path, _) in destinations.items()}}
    mounts = measured["receiver_mounts"]
    if receiver_host_sha256() != measured["receiver_host_sha256"] or set(paths) != set(mounts):
        raise ValueError("canonical_named_recovery_preflight_receiver_changed")
    udev = Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
    for role, path in paths.items():
        expected = mounts[role]
        if str(path) != expected["path"] or path.lstat().st_ino != expected["inode"]:
            raise ValueError("canonical_named_recovery_preflight_mount_changed")
        filesystem = inspect_filesystem(path, expected_uuid=expected["filesystem_uuid"], udev_root=udev)
        if filesystem.device_id != expected["device_id"]:
            raise ValueError("canonical_named_recovery_preflight_mount_changed")


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


def verify_signed_recovery(payload, public):
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


def _opened(path, public):
    return verify_signed_recovery(load_receipt(path, max_bytes=_MAX_RECEIPT), public)


def verify_receiver_preflight(payload, receiver_public_key):
    measured = verify_signed_recovery(payload, receiver_public_key)
    policy = measured.get("site_policy")
    if (measured.get("schema") != PREFLIGHT_VERSION or not isinstance(policy, dict)
            or policy.get("receiver_public_key") != receiver_public_key
            or not _sha256(measured.get("receiver_host_sha256")) or not _sha256(measured.get("source_host_sha256"))
            or measured["receiver_host_sha256"] == measured["source_host_sha256"]
            or type(measured.get("pending_expected")) is not bool or not isinstance(measured.get("receiver_mounts"), dict)
            or measured.get("measurement_sha256") != archive_evidence_hash(
                {key: value for key, value in measured.items() if key != "measurement_sha256"})):
        raise ValueError("canonical_named_recovery_invalid_preflight")
    _verify_preflight_capacity(measured)
    return measured


def _preflight_needs(mounts, requirements):
    if (not {"exchange", "workspace", "pgdata"} <= mounts.keys() or not isinstance(requirements, dict)
            or requirements.keys() != mounts.keys()):
        raise ValueError("canonical_named_recovery_invalid_preflight_capacity")
    grouped = {}
    for role, mount in mounts.items():
        need = requirements[role]
        if (not isinstance(mount, dict) or not isinstance(need, dict) or type(mount.get("block_size")) is not int
                or mount["block_size"] < 1 or not isinstance(mount.get("device_id"), str)
                or not re.fullmatch(r"[0-9]+:[0-9]+", mount["device_id"])
                or any(type(need.get(key)) is not int or need[key] < 0 for key in ("checkpoint_bytes", "entries", "reserve_bytes"))):
            raise ValueError("canonical_named_recovery_invalid_preflight_capacity")
        device = grouped.setdefault(mount["device_id"], {"roles": [], "required_bytes": 0, "required_entries": 0, "reserve_bytes": 0})
        device["roles"].append(role)
        device["required_bytes"] += need["checkpoint_bytes"] + mount["block_size"] * need["entries"]
        device["required_entries"] += need["entries"]
        device["reserve_bytes"] = max(device["reserve_bytes"], need["reserve_bytes"])
    return grouped


def _verify_preflight_capacity(measured):
    grouped = _preflight_needs(measured["receiver_mounts"], measured.get("requirements"))
    capacity = measured.get("capacity_by_device")
    if not isinstance(capacity, dict) or grouped.keys() != capacity.keys():
        raise ValueError("canonical_named_recovery_invalid_preflight_capacity")
    for key, needed in grouped.items():
        observed = capacity[key]
        if (not isinstance(observed, dict) or not isinstance(observed.get("roles"), list)
                or not all(isinstance(role, str) for role in observed["roles"]) or sorted(observed["roles"]) != sorted(needed["roles"])
                or any(type(observed.get(name)) is not int or observed[name] != needed[name]
                       for name in ("required_bytes", "required_entries", "reserve_bytes"))
                or type(observed.get("available_bytes")) is not int or type(observed.get("available_entries")) is not int
                or observed["available_bytes"] < needed["required_bytes"] + needed["reserve_bytes"]
                or observed["available_entries"] < needed["required_entries"]):
            raise ValueError("canonical_named_recovery_invalid_preflight_capacity")


def _publish(path, body):
    with TemporaryDirectory(prefix=".qt-named-exchange-", dir=path.parent) as work:
        staging = Path(work) / path.name
        _json_write(staging, body)
        os.link(staging, path)
    _sync(path.parent)


def _file_hash(path):
    with path.open("rb") as stream:
        return hashlib.sha256(stream.read(65537)).hexdigest()


def _ready_scope(site, measured):
    return prepared_receiver_scope(approved_package_sha256=site.approved_package_sha256,
        receiver_preflight_sha256=measured["measurement_sha256"], site_policy=measured["site_policy"])


def _wal_options(site, anchor_path, anchor_sha256, check, *, checkpoint=None):
    options = {"base_transfer_sha256": site.base_transfer_sha256, "baseline": site.baseline_verification,
               "baseline_verification_sha256": site.baseline_verification["verification_sha256"],
               "anchor_path": anchor_path, "anchor_sha256": anchor_sha256, "limits": _limit(site.wal, check)}
    if checkpoint is not None:
        options.update(checkpoint=checkpoint, checkpoint_sha256=site.replayed_checkpoint_sha256,
                       max_restart_segments=site.max_restart_segments)
    return options


def _verify_site_ready(site, body, *, scope):
    digest = verify_prepared_receiver_ready(body, scope=scope, baseline_verification=site.baseline_verification)
    checkpoint = body.get("replayed_checkpoint")
    if (None if checkpoint is None else checkpoint["verification_sha256"]) != site.replayed_checkpoint_sha256:
        raise ValueError("canonical_named_recovery_checkpoint_outside_approval")
    return digest


def _private_credential(path, *, receiving, workspace=None):
    path = Path(path)
    if path.is_relative_to(receiving) or (workspace is not None and path.is_relative_to(workspace)):
        raise ValueError("canonical_named_recovery_private_key_on_exchange")
    return path


def bind_named_recovery_source(*, site, signing_key, provider_receipt_root, database_repository, archive_objects,
                               scratch, baseline, restic, archive_key, pending_key=None, pending_spool=None,
                               replayed_inventory=None):
    """Make the launcher callback. Actual recovery still requires the approved live fence."""
    measured = site.checked_preflight()
    root, root_id = _private_root(site.receiving)
    scratch, scratch_id = _private_root(scratch)
    receipts, receipts_id = _private_root(provider_receipt_root)
    if scratch_id[0] == root_id[0] or not isinstance(baseline, PreparedArchiveBaseline) or (
            baseline.receipt_sha256 != site.baseline_verification["pair_receipt_sha256"] or baseline.checkpoint_sha256 is not None
            or (replayed_inventory is None) != (site.replayed_checkpoint_sha256 is None)):
        raise ValueError("canonical_named_recovery_source_storage_invalid")
    archive_key = _private_credential(archive_key, receiving=root)
    signer = _signer(signing_key, site.source_public_key, root)
    if pending_key is not None:
        pending_key = _private_credential(pending_key, receiving=root)
    ready_scope = _ready_scope(site, measured)
    admitted = None
    admitted_archive = baseline

    def assert_receiver_ready():
        nonlocal admitted, admitted_archive
        if _identity(root.lstat()) != root_id:
            raise RuntimeError("canonical_named_recovery_source_mount_changed")
        try:
            ready = load_receipt(root / PREPARED_READY_FILENAME, max_bytes=_MAX_RECEIPT)
        except FileNotFoundError:
            raise RuntimeError("canonical_named_recovery_prepared_receiver_unavailable") from None
        body = verify_signed_recovery(ready, site.receiver_public_key)
        digest = _verify_site_ready(site, body, scope=ready_scope)
        if admitted is not None and admitted != digest:
            raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
        if admitted is None and replayed_inventory is not None:
            until = monotonic() + min(site.archive.seconds, body["expires_at"] - time())

            def check_inventory():
                if monotonic() >= until:
                    raise TimeoutError("canonical_named_recovery_checkpoint_inventory_deadline")
                if _identity(root.lstat()) != root_id:
                    raise RuntimeError("canonical_named_recovery_source_mount_changed")

            admitted_archive = prepare_replayed_archive_baseline(replayed_inventory,
                checkpoint=body["replayed_checkpoint"], max_objects=site.max_archive_objects, check=check_inventory)
            check_inventory()
        admitted = digest
        return ready

    def recover(fence, control, source_commit, provider_boundary, remaining, check_archive):
        if (admitted is None or site.checked_preflight() != measured or not isinstance(fence, DatabaseFence) or not _sha256(provider_boundary)
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
        ready = assert_receiver_ready()["body"]
        drained = load_receipt(receipts / f"collector-drained-{provider_boundary}.json", max_bytes=_MAX_PROVIDER_RECEIPT)
        evidence = drained["evidence"]
        if (drained.get("schema") != COLLECTOR_RECEIPT_VERSION or drained.get("phase") != "drained"
                or drained.get("sha256") != provider_boundary or archive_evidence_hash({key: value for key, value in drained.items()
                                                                                       if key != "sha256"}) != provider_boundary
                or evidence.get("source_commit_seq") != source_commit):
            raise ValueError("canonical_named_recovery_provider_boundary_changed")
        pending_before = evidence.get("pending_spool")
        if (pending_before is not None) != measured["pending_expected"]:
            raise ValueError("canonical_named_recovery_preflight_pending_changed")
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
            wal_options = _wal_options(site, anchor_path, anchor_hash, check, checkpoint=ready.get("replayed_checkpoint"))
            wal = stage_final_wal(fence=fence, control=control, source=database_repository, receiving=root, **wal_options)
            stage = Path(work) / "accepted"
            stage.mkdir(mode=0o700)
            archived = stage_encrypted_archive_suffix(fence=fence, baseline=admitted_archive, source_root=archive_objects,
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
            assert_receiver_ready()
            request = {"schema": _REQUEST, "approved_package_sha256": site.approved_package_sha256,
                       "receiver_preflight_sha256": measured["measurement_sha256"],
                       "prepared_receiver_sha256": admitted,
                       "database_identity": baseline.database_identity, "source_commit_seq": source_commit,
                       "provider_boundary_sha256": provider_boundary, "anchor_sha256": anchor_hash,
                       "base_transfer_sha256": site.base_transfer_sha256,
                       "baseline_verification_sha256": site.baseline_verification["verification_sha256"],
                       "wal_transfer_sha256": wal["wal_transfer_sha256"], "archive_transfer_sha256": archived["archive_transfer_sha256"],
                       "pending": pending, "pending_source": source, "expires_at": min(ready["expires_at"], int(time()) + check())}
            _publish(request_path, _sign(request, signer))
        expected_request = archive_evidence_hash(request)
        while not response_path.exists():
            check()
            sleep(0.1)
        signed_reply = load_receipt(response_path, max_bytes=_MAX_RECEIPT)
        reply = verify_signed_recovery(signed_reply, site.receiver_public_key)
        if (set(reply) != {"schema", "request_sha256", "archive_closure", "pending_spool_verified", "runtime_sha256",
                           "approved_package_sha256", "provider_boundary_sha256", "receiver_preflight_sha256", "prepared_receiver_sha256"}
                or reply["schema"] != _RESPONSE or reply["request_sha256"] != expected_request
                or any(reply[key] != request[key] for key in ("approved_package_sha256", "provider_boundary_sha256",
                                                            "receiver_preflight_sha256", "prepared_receiver_sha256"))
                or not _sha256(reply["runtime_sha256"])):
            raise RuntimeError("canonical_named_recovery_response_binding_changed")
        closure = reply["archive_closure"]
        if (not isinstance(closure, dict) or any(closure.get(key) != request[key] for key in (
                "database_identity", "source_commit_seq", "anchor_sha256", "wal_transfer_sha256", "archive_transfer_sha256",
                "base_transfer_sha256", "baseline_verification_sha256",))
                or closure.get("receiver_runtime_sha256") != reply["runtime_sha256"]):
            raise RuntimeError("canonical_named_recovery_response_boundary_changed")
        if closure.get("checkpoint_sha256") != site.replayed_checkpoint_sha256:
            raise RuntimeError("canonical_named_recovery_response_checkpoint_changed")
        with conn.begin(), _bounded_step(conn, min(5, check())):
            fence.assert_quiet(conn)
        check()
        return {"archive_closure": closure, "pending_spool_source": source, "pending_spool_verified": reply["pending_spool_verified"],
                "receiver_response": signed_reply}

    recover.assert_receiver_ready = assert_receiver_ready
    recover.wal_baseline = site.baseline_verification
    recover.restart_budget = site.max_restart_segments
    return recover


def _read_request(root, site, *, prepared_sha256):
    request = _opened(root / "named-recovery-request.json", site.source_public_key)
    baseline = site.baseline_verification
    required = {"schema", "approved_package_sha256", "database_identity", "source_commit_seq", "provider_boundary_sha256",
                "anchor_sha256", "base_transfer_sha256", "baseline_verification_sha256", "wal_transfer_sha256",
                "archive_transfer_sha256", "pending", "pending_source", "expires_at", "receiver_preflight_sha256",
                "prepared_receiver_sha256"}
    measured = site.checked_preflight()
    if (set(request) != required or request["schema"] != _REQUEST
            or request["approved_package_sha256"] != site.approved_package_sha256
            or request["receiver_preflight_sha256"] != measured["measurement_sha256"]
            or request["prepared_receiver_sha256"] != prepared_sha256
            or request["base_transfer_sha256"] != site.base_transfer_sha256
            or request["baseline_verification_sha256"] != baseline["verification_sha256"]
            or request["database_identity"] != baseline["database_identity"]
            or type(request["source_commit_seq"]) is not int or request["source_commit_seq"] < 0
            or not all(_sha256(request[key]) for key in ("provider_boundary_sha256", "anchor_sha256", "wal_transfer_sha256", "archive_transfer_sha256"))
            or type(request["expires_at"]) is not int or request["expires_at"] <= time()
            or (request["pending"] is not None) != measured["pending_expected"]
            or (request["pending"] is None) != (request["pending_source"] is None)):
        raise ValueError("canonical_named_recovery_request_outside_approval")
    path = root / "named-recovery-anchor.json"
    anchor = _bound_json(path, request["anchor_sha256"])
    if (anchor["source_commit_seq"] != request["source_commit_seq"]
            or not re.fullmatch(r"qt_original_[a-f0-9]{32}", anchor["restore_name"])
            or type(anchor["timeline"]) is not int or not 1 <= anchor["timeline"] <= 2**32 - 1):
        raise ValueError("canonical_named_recovery_anchor_changed")
    return request, anchor, path


def _native_restore(*, root, workspace, pgdata, tablespaces, pgbackrest, database_key, pair, unarmed, budget, deadline, check):
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
        native = partial(_run_native, cwd=workspace, env=env, deadline=restore_deadline, check=check_restore)
        command_prefix = [str(binary), f"--config={settings}", "--stanza=qt"]
        selected, _, _ = _selected_backup_manifest(native, command_prefix, pair)
        stored_spaces = selected.get("tablespace") or []
        if (selected.get("link") or len(stored_spaces) != len(tablespaces)
                or {space["name"] for space in stored_spaces} != set(tablespaces)
                or int(pair["database_identity"].split("/")[1]) not in {row["oid"] for row in selected["database-ref"]}):
            raise ValueError("canonical_named_recovery_native_manifest_scope_changed")
        physical_bytes = selected["info"]["size"]
        if (type(physical_bytes) is not int or not 0 < physical_bytes <= budget.bytes
                or sum(max(0, free - budget.reserve_bytes) for free in starting.values()) < physical_bytes):
            raise RuntimeError("canonical_named_recovery_database_restore_budget_exceeded")
        command = [*command_prefix, f"--set={pair['database_label']}", "--type=name",
                   f"--target={unarmed}", f"--target-timeline={int(selected['archive']['stop'][:8], 16)}", "--target-action=shutdown",
                   "--archive-mode=off", f'--recovery-option=restore_command={binary} --config={settings} --stanza=qt archive-get %f "%p"']
        command.extend(f"--tablespace-map={name}={path}" for name, path in sorted(tablespaces.items()))
        _run_native([*command, "restore"], cwd=workspace, env=env, deadline=restore_deadline, check=check_restore)
    except BaseException:
        settings.unlink()
        _sync(workspace)
        raise
    return settings, {space["name"]: space["oid"] for space in stored_spaces}


def _local_receiver(engine):
    url = engine.url
    query_host = url.query.get("host")
    if (url.get_backend_name() != "postgresql" or url.host not in {None, "127.0.0.1", "::1", "restored"}
            or (query_host is not None and not str(query_host).startswith("/"))):
        raise ValueError("canonical_named_recovery_receiver_endpoint_invalid")


def _open_receive_database(control_engine, target_engine, anchor, pgdata, tablespaces, tablespace_oids, check):
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
        observed_spaces = {name: (location, oid) for name, location, oid in conn.execute(text("""
            SELECT spcname,pg_tablespace_location(oid),oid::bigint FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')
        """))}
        if observed_spaces != {name: (str(path), tablespace_oids[name]) for name, (path, _) in tablespaces.items()}:
            raise RuntimeError("canonical_named_recovery_receiver_tablespace_changed")
        quoted = conn.dialect.identifier_preparer.quote_identifier(database["database"])
        conn.exec_driver_sql(f"ALTER DATABASE {quoted} ALLOW_CONNECTIONS true")


def _empty_receiver_paths(site, measured, workspace, pgdata, tablespaces):
    root, root_id = _private_root(site.receiving)
    workspace, work_id = _private_root(workspace)
    pgdata, pgdata_id = _private_root(pgdata)
    if (root.is_relative_to(workspace) or workspace.is_relative_to(root) or not pgdata.is_relative_to(workspace)
            or pgdata == workspace or any(pgdata.iterdir()) or not isinstance(tablespaces, dict)
            or not all(isinstance(name, str) and _TABLESPACE_NAME.fullmatch(name) for name in tablespaces)):
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
    _assert_receiver_preflight_mounts(measured, root, workspace, pgdata, destinations)
    return root, workspace, pgdata, destinations, ((root, root_id), (workspace, work_id), (pgdata, pgdata_id), *destinations.values())


@dataclass
class _NativeReceiver:
    site: RecoverySitePolicy
    signer: Ed25519PrivateKey
    root: Path
    roots: tuple
    workspace: Path
    pgdata: Path
    destinations: dict
    tablespace_oids: dict
    pgbackrest: Path
    restic: Path
    settings: Path
    settings_sha256: str
    unarmed: str
    base_restore: Path
    owner_pid: int


@dataclass
class _PreparedReceiver(_NativeReceiver):
    baseline: PreparedArchiveBaseline
    custody: object
    ready: dict
    digest: str
    retained_wal: object = None
    active: bool = True
    armed: bool = False


@dataclass
class _StagedReplayedReceiver:
    native: _NativeReceiver
    measured: dict
    checkpoint: dict
    inventory: Path
    archive_objects: Path
    baseline: PreparedArchiveBaseline
    custody: object
    retained_wal: object
    active: bool = True
    used: bool = False


@contextmanager
def _restored_receiver(*, site, measured, signing_key, pgbackrest, restic, database_key, base_archive_key,
                       workspace, pgdata, tablespaces, timeout_seconds):
    root, workspace, pgdata, destinations, roots = _empty_receiver_paths(site, measured, workspace, pgdata, tablespaces)
    deadline = monotonic() + timeout_seconds

    def check():
        _check_receiver_roots(roots)
        seconds = int(deadline - monotonic())
        if seconds < 1:
            raise TimeoutError("canonical_named_recovery_preparation_deadline")
        return seconds

    database_key, base_archive_key = (_private_credential(path, receiving=root, workspace=workspace)
                                      for path in (database_key, base_archive_key))
    signing_key = _private_credential(signing_key, receiving=root, workspace=workspace)
    signer = _signer(signing_key, site.receiver_public_key, root)
    pgbackrest, restic = _binary(pgbackrest), _binary(restic)
    owner = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("canonical_named_recovery_prepared_workspace_busy") from None
        if _identity(os.fstat(owner)) != dict(roots)[workspace] or os.path.lexists(root / PREPARED_READY_FILENAME):
            raise RuntimeError("canonical_named_recovery_preparation_already_used")
        verifier = site.baseline_verification
        pair = _baseline(root, site.base_transfer_sha256, verifier, verifier["verification_sha256"])
        base_restore = workspace / "base-archive"
        base_restore.mkdir(mode=0o700)
        unarmed = "qt_prepared_" + token_hex(16)
        settings, space_oids = _native_restore(root=root, workspace=workspace, pgdata=pgdata,
            tablespaces={name: path for name, (path, _) in destinations.items()}, pgbackrest=pgbackrest,
            database_key=database_key, pair=pair, unarmed=unarmed, budget=site.database, deadline=deadline, check=check)
        try:
            report = verify_handoff(repository=root, receipt_sha256=verifier["pair_receipt_sha256"],
                archive_restore=base_restore, pgbackrest=pgbackrest, restic=restic, database_key=database_key,
                archive_key=base_archive_key, timeout_seconds=min(check(), 86_400), max_archive_bytes=site.archive.bytes,
                max_archive_objects=site.max_archive_objects, wal_segment_bytes=verifier["wal_segment_bytes"],
                max_wal_segments=verifier["wal_segments"], reserve_bytes=site.archive.reserve_bytes, native_workspace=workspace)
            if report != verifier:
                raise RuntimeError("canonical_named_recovery_prepared_baseline_changed")
            native = _NativeReceiver(site, signer, root, roots, workspace, pgdata, destinations, space_oids,
                pgbackrest, restic, settings, _file_hash(settings), unarmed, base_restore, os.getpid())
            yield native, check
        finally:
            settings.unlink()
            _sync(workspace)
    finally:
        os.close(owner)


@contextmanager
def _publish_prepared_receiver(native, site, measured, baseline, custody, ready_seconds, check, *,
                               checkpoint=None, retained_wal=None, archive_objects=None):
    custody.assert_held(check)
    auto = _prepared_configuration(native.pgdata, native.unarmed, replayed=checkpoint is not None)
    if _file_hash(native.settings) != native.settings_sha256:
        raise RuntimeError("canonical_named_recovery_prepared_pgbackrest_configuration_changed")
    materialized = {"custody_sha256": custody.sha256, "entries": custody.entries,
        "native_verification_sha256": site.baseline_verification["verification_sha256"],
        "tablespace_oids": native.tablespace_oids, "recovery_configuration_sha256": hashlib.sha256(auto).hexdigest()}
    if retained_wal is not None:
        retained_wal.assert_held(check)
        materialized["retained_wal_sha256"] = retained_wal.sha256
    scope = _ready_scope(site, measured)
    body = make_prepared_receiver_ready(scope=scope, materialized_sha256=archive_evidence_hash(materialized),
        expires_at=int(time()) + ready_seconds, replayed_checkpoint=checkpoint, baseline_verification=site.baseline_verification)
    digest = _verify_site_ready(site, body, scope=scope)
    ready = _sign(body, native.signer)
    ready_path = native.root / PREPARED_READY_FILENAME
    check()
    _publish(ready_path, ready)
    fields = vars(native) | {"site": site}
    if archive_objects is not None:
        fields["base_restore"] = archive_objects
    prepared = _PreparedReceiver(**fields, baseline=baseline, custody=custody, ready=ready,
                                 digest=digest, retained_wal=retained_wal)
    try:
        yield prepared
    finally:
        prepared.active = False
        if (_identity(native.root.lstat()) == dict(native.roots)[native.root] and ready_path.exists()
                and load_receipt(ready_path, max_bytes=_MAX_RECEIPT) == ready):
            ready_path.unlink()
            _sync(native.root)


def _custody(native, archive_root, max_custody_entries, check):
    return capture_prepared_custody(pgdata=native.pgdata,
        tablespaces={name: path for name, (path, _) in native.destinations.items()}, archive_root=archive_root,
        max_entries=max_custody_entries, check=check)


@contextmanager
def prepare_named_receiver(*, site, signing_key, pgbackrest, restic, database_key, base_archive_key,
                           workspace, pgdata, tablespaces, timeout_seconds, ready_seconds, max_custody_entries):
    """Prepare the exact native base and hold its private custody until named recovery."""
    if not isinstance(site, NamedRecoverySite) or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 86_400:
        raise ValueError("canonical_named_recovery_invalid_preparation")
    if site.replayed_checkpoint_sha256 is not None:
        raise ValueError("canonical_named_recovery_replayed_custody_required")
    _check_ready_lease(ready_seconds)
    measured = site.checked_preflight()
    with _restored_receiver(site=site, measured=measured, signing_key=signing_key, pgbackrest=pgbackrest, restic=restic,
            database_key=database_key, base_archive_key=base_archive_key, workspace=workspace, pgdata=pgdata,
            tablespaces=tablespaces, timeout_seconds=timeout_seconds) as (native, check):
        with _custody(native, native.base_restore, max_custody_entries, check) as custody:
            baseline = prepare_archive_baseline(native.root / "complete.json", native.base_restore,
                expected_receipt_sha256=site.baseline_verification["pair_receipt_sha256"],
                max_objects=site.max_archive_objects, check=check)
            with _publish_prepared_receiver(native, site, measured, baseline, custody, ready_seconds, check) as prepared:
                yield prepared


def _check_ready_lease(ready_seconds):
    if type(ready_seconds) is not int or not 1 <= ready_seconds <= MAX_MIGRATION_SECONDS:
        raise ValueError("canonical_named_recovery_invalid_readiness_lease")


@contextmanager
def prepare_replayed_receiver(*, site, receiver_preflight, signing_key, pgbackrest, restic, database_key, base_archive_key,
                              workspace, pgdata, tablespaces, timeout_seconds, max_custody_entries, replay_before_approval):
    """Build a later stopped recovery point and keep its custody while it is reviewed."""
    if (not isinstance(site, RecoverySitePolicy) or isinstance(site, NamedRecoverySite)
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS
            or not callable(replay_before_approval)):
        raise ValueError("canonical_named_recovery_invalid_replay_preparation")
    measured = verify_receiver_preflight(receiver_preflight, site.receiver_public_key)
    if measured["site_policy"] != site.preflight_scope():
        raise ValueError("canonical_named_recovery_preflight_policy_changed")
    with _restored_receiver(site=site, measured=measured, signing_key=signing_key, pgbackrest=pgbackrest, restic=restic,
            database_key=database_key, base_archive_key=base_archive_key, workspace=workspace, pgdata=pgdata,
            tablespaces=tablespaces, timeout_seconds=timeout_seconds) as (native, check):
        baseline = site.baseline_verification
        unarmed = _prepared_configuration(native.pgdata, native.unarmed)
        attempt = archive_evidence_hash({"receiver_preflight_sha256": measured["measurement_sha256"],
                                        "preparation": native.unarmed})
        prefixes = []
        authenticating = True

        def authenticate_prefix(*, after, through, prefix_sha256, previous_prefix_sha256=None, limits=None):
            if not authenticating or native.owner_pid != os.getpid():
                raise ValueError("canonical_named_recovery_live_replay_preparation_required")
            head = prefixes[-1]["body"] if prefixes else None
            expected = (baseline["wal_backup_stop"], None) if head is None else (head["through"], head["wal_prefix_sha256"])
            if (after, previous_prefix_sha256) != expected:
                raise ValueError("canonical_named_recovery_prefix_chain_changed")
            limits = _prefix_limits(site, limits)
            expires = monotonic() + min(check(), limits.seconds)

            def check_batch():
                seconds = min(check(), int(expires - monotonic()))
                if seconds < 1:
                    raise TimeoutError("canonical_named_recovery_prepared_wal_deadline")
                return seconds

            envelope = _authenticate_wal_prefix(native, attempt, after=after, through=through, prefix_sha256=prefix_sha256,
                previous_prefix_sha256=previous_prefix_sha256, limits=limits, deadline=expires, check=check_batch)
            prefixes.append(envelope)
            return envelope

        try:
            result = replay_before_approval(pgdata=native.pgdata, base_restore=native.base_restore,
                                            authenticate_prefix=authenticate_prefix, check=check)
        finally:
            authenticating = False
        if not isinstance(result, dict) or not {"paused_receipt", "archive_receipt", "stopped_control",
                "stopped_control_sha256", "archive_inventory", "archive_objects"} <= result.keys():
            raise ValueError("canonical_named_recovery_replay_evidence_required")
        if _prepared_configuration(native.pgdata, native.unarmed, replayed=True) != unarmed:
            raise RuntimeError("canonical_named_recovery_unarmed_configuration_changed")
        wal = verify_wal_prefix_chain(site=site, prepared_sha256=attempt, prefixes=prefixes)
        archive_receipt = result["archive_receipt"]
        archive_objects, objects_id = _private_root(result["archive_objects"])
        archive_root, archive_id = _private_root(archive_objects.parent)
        inventory = Path(result["archive_inventory"])
        protected_roots = (native.root, native.pgdata, native.base_restore, *(path for path, _ in native.destinations.values()))
        if (native.workspace.is_relative_to(archive_root)
                or not (objects_id[0] == archive_id[0] == dict(native.roots)[native.workspace][0])
                or any(archive_root.is_relative_to(path) or path.is_relative_to(archive_root) for path in protected_roots)
                or inventory.parent != archive_root or archive_receipt.get("catalog_inventory_path") != str(inventory)
                or archive_receipt.get("pgdata") != str(native.pgdata)
                or archive_receipt.get("pgdata_inode") != native.pgdata.stat().st_ino
                or archive_receipt.get("suffix_root") != str(archive_objects)
                or archive_receipt.get("suffix_inode") != archive_id[1]):
            raise ValueError("canonical_named_recovery_replayed_archive_scope_changed")
        checkpoint = build_replayed_checkpoint(
            pair_receipt_sha256=baseline["pair_receipt_sha256"], database_identity=baseline["database_identity"],
            timeline=result["paused_receipt"]["timeline"], paused_receipt=result["paused_receipt"],
            archive_receipt=archive_receipt, stopped_control=result["stopped_control"],
            stopped_control_sha256=result["stopped_control_sha256"],
            **{field: wal[field] for field in ("wal_first", "wal_verified_through", "wal_segment_bytes", "wal_verification_sha256")})
        verify_replayed_checkpoint_baseline(checkpoint, baseline_verification=baseline,
                                           expected_checkpoint_sha256=checkpoint["verification_sha256"])
        with _custody(native, archive_root, max_custody_entries, check) as custody:
            _check_stopped_control(native.pgdata, checkpoint)
            archive = prepare_replayed_archive_baseline(inventory, checkpoint=checkpoint, max_objects=site.max_archive_objects,
                                                        archive_objects=archive_objects, check=check)
            retained = capture_retained_wal(receiving=native.root, baseline=baseline,
                base_transfer_sha256=site.base_transfer_sha256, checkpoint=checkpoint,
                checkpoint_sha256=checkpoint["verification_sha256"], wal_verification=wal, prefix_receipts=prefixes,
                max_segments=wal["prefix_segments"] + baseline["wal_segments"],
                native_verify=lambda records: _verify_native_wal_entries(native, records, monotonic() + check(), check),
                check=check)
            custody.assert_held(check)
            staged = _StagedReplayedReceiver(native, measured, checkpoint, inventory, archive_objects, archive, custody, retained)
            try:
                yield staged
            finally:
                staged.active = False


@contextmanager
def seal_replayed_receiver(*, staged, site, ready_seconds):
    """Publish readiness for the exact reviewed, still-held replayed receiver."""
    _check_ready_lease(ready_seconds)
    if (not isinstance(staged, _StagedReplayedReceiver) or not staged.active or staged.used
            or staged.native.owner_pid != os.getpid() or not isinstance(site, NamedRecoverySite)
            or site.checked_preflight() != staged.measured
            or staged.baseline.checkpoint_sha256 != site.replayed_checkpoint_sha256
            or site.replayed_checkpoint_sha256 != staged.checkpoint["verification_sha256"]):
        raise ValueError("canonical_named_recovery_replayed_custody_required")
    if len(staged.retained_wal.files) >= site.max_restart_segments:
        raise ValueError("canonical_named_recovery_replayed_restart_budget_insufficient")
    deadline = monotonic() + ready_seconds

    def check():
        if monotonic() >= deadline:
            raise TimeoutError("canonical_named_recovery_receiver_deadline")
        _check_receiver_roots(staged.native.roots)

    _check_stopped_control(staged.native.pgdata, staged.checkpoint)
    staged.used = True
    with _publish_prepared_receiver(staged.native, site, staged.measured, staged.baseline, staged.custody, ready_seconds, check,
            checkpoint=staged.checkpoint, retained_wal=staged.retained_wal, archive_objects=staged.archive_objects) as prepared:
        yield prepared


def _check_stopped_control(pgdata, checkpoint):
    control = _private_bytes(pgdata / "global" / "pg_control", limit=8192)
    if len(control) != 8192 or hashlib.sha256(control).hexdigest() != checkpoint["stopped_control_sha256"]:
        raise RuntimeError("canonical_named_recovery_replayed_control_changed")


def _check_receiver_roots(roots):
    if any(_identity(path.lstat()) != identity for path, identity in roots):
        raise RuntimeError("canonical_named_recovery_receiver_mount_changed")


def _prepared_configuration(pgdata, unarmed, *, replayed=False):
    config = pgdata / "postgresql.auto.conf"
    backup = pgdata / "backup_label"
    recovery = pgdata / "recovery.signal"
    if (os.path.lexists(pgdata / "postmaster.pid") or os.path.lexists(pgdata / "standby.signal")
            or not recovery.is_file() or recovery.is_symlink()
            or (os.path.lexists(backup) if replayed else not backup.is_file() or backup.is_symlink())):
        raise RuntimeError("canonical_named_recovery_prepared_cluster_used")
    raw = _private_bytes(config, limit=65536)
    if (not re.search(rb"(?m)^recovery_target_name\s*=\s*'" + unarmed.encode() + rb"'\s*$", raw)
            or not re.search(rb"(?m)^recovery_target_action\s*=\s*'shutdown'\s*$", raw)):
        raise RuntimeError("canonical_named_recovery_unarmed_configuration_changed")
    return raw


def _arm_prepared(prepared, anchor):
    raw = _prepared_configuration(prepared.pgdata, prepared.unarmed, replayed=prepared.retained_wal is not None)
    lines = [line for line in raw.splitlines(keepends=True)
             if not re.match(rb"\s*recovery_target(?:_[a-z_]+)?\s*=", line)]
    lines.extend((f"\nrecovery_target_name = '{anchor['restore_name']}'\n".encode(),
                  f"recovery_target_timeline = '{anchor['timeline']}'\n".encode(), b"recovery_target_action = 'promote'\n"))
    with TemporaryDirectory(prefix=".qt-arm-", dir=prepared.pgdata) as work:
        path = Path(work) / "postgresql.auto.conf"
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as handle:
            handle.writelines(lines)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(path, prepared.pgdata / path.name)
    _sync(prepared.pgdata)
    prepared.armed = True


def _verify_native_wal_entries(prepared, wal, deadline, check):
    if _file_hash(prepared.settings) != prepared.settings_sha256:
        raise RuntimeError("canonical_named_recovery_prepared_pgbackrest_configuration_changed")
    entries = []

    def check_wal():
        check()
        if monotonic() >= deadline:
            raise TimeoutError("canonical_named_recovery_prepared_wal_deadline")

    with TemporaryDirectory(prefix="qt-prepared-wal-", dir=prepared.workspace) as work:
        if shutil.disk_usage(work).free < wal["wal_segment_bytes"]:
            raise RuntimeError("canonical_named_recovery_prepared_wal_workspace_insufficient")
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": work, "TMPDIR": work}
        run = partial(_run_native, cwd=work, env=env, deadline=deadline, check=check_wal)
        command = [str(prepared.pgbackrest), f"--config={prepared.settings}", "--stanza=qt", "--archive-async=n"]
        for row in wal["files"]:
            check_wal()
            destination = Path(work) / row["segment"]
            run([*command, "archive-get", row["segment"], str(destination)])
            digest = hashlib.sha1()
            with destination.open("rb") as stream:
                while chunk := stream.read(1024**2):
                    check_wal()
                    digest.update(chunk)
            checksum = digest.hexdigest()
            if destination.stat().st_size != wal["wal_segment_bytes"] or checksum != row["wal_checksum"]:
                raise RuntimeError("canonical_named_recovery_prepared_wal_native_mismatch")
            entries.append([row["segment"], checksum])
            destination.unlink()
    return entries


def _prefix_limits(site, limits):
    limits = site.wal if limits is None else limits
    if (not isinstance(limits, TransferLimits) or any(getattr(limits, name) > getattr(site.wal, name)
            for name in ("seconds", "bytes", "files")) or limits.reserve_bytes < site.wal.reserve_bytes):
        raise ValueError("canonical_named_recovery_prefix_outside_site_budget")
    return limits


def _authenticate_wal_prefix(native, attempt, *, after, through, prefix_sha256, previous_prefix_sha256, limits, deadline, check):
    site = native.site
    verifier = site.baseline_verification
    seconds = int(check())
    if seconds < 1:
        raise TimeoutError("canonical_named_recovery_receiver_deadline")
    wal = verify_wal_prefix(receiving=native.root, base_transfer_sha256=site.base_transfer_sha256,
        baseline=verifier, baseline_verification_sha256=verifier["verification_sha256"], after=after, through=through,
        expected_hash=prefix_sha256, previous_prefix_sha256=previous_prefix_sha256,
        limits=replace(limits, seconds=min(limits.seconds, seconds)))
    entries = _verify_native_wal_entries(native, wal | {"wal_segment_bytes": verifier["wal_segment_bytes"]}, deadline, check)
    return _sign({"schema": _WAL_PREFIX_VERIFICATION, "prepared_receiver_sha256": attempt,
                  "pair_receipt_sha256": verifier["pair_receipt_sha256"],
                  "baseline_verification_sha256": verifier["verification_sha256"], "wal_prefix_sha256": prefix_sha256,
                  "previous_prefix_sha256": previous_prefix_sha256, "after": after, "through": through,
                  "verified_wal_sha256": archive_evidence_hash(entries), "segments": len(entries)}, native.signer)


def verify_prepared_wal_prefix(*, prepared, after, through, prefix_sha256, timeout_seconds,
                               previous_prefix_sha256=None, limits=None):
    """Authenticate one installed archive batch before the receiver starts replay."""
    if (not isinstance(prepared, _PreparedReceiver) or not prepared.active or prepared.armed
            or prepared.retained_wal is not None or prepared.owner_pid != os.getpid() or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS):
        raise ValueError("canonical_named_recovery_live_prepared_receiver_required")
    site = prepared.site
    limits = _prefix_limits(site, limits)
    measured = site.checked_preflight()
    ready = load_receipt(prepared.root / PREPARED_READY_FILENAME, max_bytes=_MAX_RECEIPT)
    if ready != prepared.ready:
        raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
    ready_body = verify_signed_recovery(ready, site.receiver_public_key)
    if verify_prepared_receiver_ready(ready_body, scope=_ready_scope(site, measured)) != prepared.digest:
        raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
    deadline = monotonic() + min(timeout_seconds, limits.seconds, ready_body["expires_at"] - time())

    def check():
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("canonical_named_recovery_receiver_deadline")
        _check_receiver_roots(prepared.roots)
        return remaining

    prepared.custody.assert_held(check)
    envelope = _authenticate_wal_prefix(prepared, prepared.digest, after=after, through=through, prefix_sha256=prefix_sha256,
        previous_prefix_sha256=previous_prefix_sha256, limits=limits, deadline=deadline, check=check)
    prepared.custody.assert_held(check)
    if load_receipt(prepared.root / PREPARED_READY_FILENAME, max_bytes=_MAX_RECEIPT) != ready:
        raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
    check()
    return envelope


def verify_wal_prefix_chain(*, site, prepared_sha256, prefixes):
    """Bind contiguous native verification receipts to one prepared receiver and native backup."""
    if (not isinstance(site, RecoverySitePolicy) or not _sha256(prepared_sha256)
            or not isinstance(prefixes, (list, tuple)) or not prefixes):
        raise ValueError("canonical_named_recovery_prefix_chain_required")
    base = site.baseline_verification
    _baseline(site.receiving, site.base_transfer_sha256, base, base["verification_sha256"])
    binding = {"schema": _WAL_PREFIX_VERIFICATION, "prepared_receiver_sha256": prepared_sha256,
               "pair_receipt_sha256": base["pair_receipt_sha256"],
               "baseline_verification_sha256": base["verification_sha256"]}
    after = base["wal_backup_stop"]
    previous = None
    count = 0
    receipt_hashes = []
    for prefix in prefixes:
        body = verify_signed_recovery(prefix, site.receiver_public_key)
        if (any(body.get(key) != value for key, value in binding.items()) or body.get("after") != after
                or "previous_prefix_sha256" not in body or body["previous_prefix_sha256"] != previous
                or not _sha256(body.get("wal_prefix_sha256")) or not _sha256(body.get("verified_wal_sha256"))):
            raise ValueError("canonical_named_recovery_prefix_chain_changed")
        through = body.get("through")
        segments = _prefix_window(base, after, through, min(site.wal.files, _MAX_SEGMENTS), previous)
        if type(body.get("segments")) is not int or body["segments"] != len(segments):
            raise ValueError("canonical_named_recovery_prefix_chain_segment_count_changed")
        receipt_hashes.append(archive_evidence_hash(prefix))
        count += len(segments)
        after, previous = through, body["wal_prefix_sha256"]
    evidence = {"prepared_receiver_sha256": prepared_sha256, "pair_receipt_sha256": base["pair_receipt_sha256"],
                "baseline_verification_sha256": base["verification_sha256"], "wal_first": base["wal_start"],
                "wal_verified_through": after, "wal_segment_bytes": base["wal_segment_bytes"],
                "prefix_batches": len(receipt_hashes), "prefix_segments": count}
    return evidence | {"wal_verification_sha256": archive_evidence_hash(evidence | {"receipts": receipt_hashes})}


def _verify_prepared_wal(prepared, suffix, deadline, check):
    wal = suffix.wal
    entries = _verify_native_wal_entries(prepared, wal, deadline, check)
    pair = suffix.pair
    report = {"schema": PREPARED_NATIVE_VERSION, "prepared_receiver_sha256": prepared.digest,
              "verified_wal_sha256": archive_evidence_hash(entries), "wal_transfer_sha256": wal["wal_transfer_sha256"],
              "baseline_verification_sha256": wal["baseline_verification_sha256"], "pair_receipt_sha256": wal["pair_receipt_sha256"],
              "anchor_sha256": wal["anchor_sha256"], "wal_verified_through": wal["wal_transferred_through"],
              **{key: pair[key] for key in ("database_identity", "database_label", "archive_snapshot", "storage_layout")}}
    if "checkpoint_sha256" in wal:
        report.update(checkpoint_sha256=wal["checkpoint_sha256"], wal_verification_sha256=wal["wal_verification_sha256"])
    return report | {"verification_sha256": archive_evidence_hash(report)}


def recover_named_site(*, prepared, suffix_key, pending_key, boot_isolated, verify_original_runtime, timeout_seconds):
    """Consume the prepared receiver and replay the signed endpoint without bulk restoration."""
    if (not isinstance(prepared, _PreparedReceiver) or not prepared.active or prepared.armed or prepared.owner_pid != os.getpid()
            or not all(map(callable, (boot_isolated, verify_original_runtime)))
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS):
        raise ValueError("canonical_named_recovery_live_prepared_receiver_required")
    site, root, workspace, pgdata = prepared.site, prepared.root, prepared.workspace, prepared.pgdata
    destinations = prepared.destinations
    measured = site.checked_preflight()
    _assert_receiver_preflight_mounts(measured, root, workspace, pgdata, destinations)
    ready = load_receipt(root / PREPARED_READY_FILENAME, max_bytes=_MAX_RECEIPT)
    if ready != prepared.ready:
        raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
    ready_body = verify_signed_recovery(ready, site.receiver_public_key)
    if _verify_site_ready(site, ready_body, scope=_ready_scope(site, measured)) != prepared.digest:
        raise RuntimeError("canonical_named_recovery_prepared_receiver_changed")
    suffix_key = _private_credential(suffix_key, receiving=root, workspace=workspace)
    if pending_key is not None:
        pending_key = _private_credential(pending_key, receiving=root, workspace=workspace)
    deadline = monotonic() + min(timeout_seconds, ready_body["expires_at"] - time())

    def check():
        seconds = int(deadline - monotonic())
        if seconds < 1:
            raise TimeoutError("canonical_named_recovery_receiver_deadline")
        _check_receiver_roots(prepared.roots)
        return seconds

    request_path = root / "named-recovery-request.json"
    response_path = root / "named-recovery-response.json"
    while not request_path.exists():
        check()
        sleep(0.1)
    request, anchor, anchor_path = _read_request(root, site, prepared_sha256=prepared.digest)
    deadline = min(deadline, monotonic() + request["expires_at"] - time())
    if response_path.exists() or response_path.is_symlink():
        raise RuntimeError("canonical_named_recovery_exchange_already_used")
    wal_options = _wal_options(site, anchor_path, request["anchor_sha256"], check, checkpoint=ready_body.get("replayed_checkpoint"))
    base_restore = prepared.base_restore
    suffix_restore = workspace / "final-archive"
    suffix_restore.mkdir(mode=0o700)
    suffix_id = _identity(suffix_restore.lstat())
    try:
        suffix = restore_named_archive_suffix(receiving=root, wal_options=wal_options,
            expected_wal_sha256=request["wal_transfer_sha256"], archive_transfer_sha256=request["archive_transfer_sha256"],
            archive_restore=suffix_restore, restic=prepared.restic, decryption_key=suffix_key, limits=_limit(site.archive, check),
            native_workspace=workspace)
    except Exception:
        if _identity(suffix_restore.lstat()) == suffix_id and not any(suffix_restore.iterdir()):
            suffix_restore.rmdir()
        raise
    native = _verify_prepared_wal(prepared, suffix, min(deadline, monotonic() + site.wal.seconds), check)
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
            scope=PendingSpoolScope(**source["scope"]), boundary=boundary, inactive_restore=inactive, restic=prepared.restic,
            decryption_key=pending_key, limits=_limit(site.pending, check), max_entries=site.max_pending_entries,
            expected_pre_anchor_sha256=pending["pre_anchor_sha256"])
        if pending_verified["segments"] != source["segments"]:
            raise RuntimeError("canonical_named_recovery_pending_receipts_differ")
    prepared.custody.assert_held(check)
    checkpoint = ready_body.get("replayed_checkpoint")
    if checkpoint is not None:
        if prepared.retained_wal is None or prepared.baseline.checkpoint_sha256 != checkpoint["verification_sha256"]:
            raise RuntimeError("canonical_named_recovery_replayed_custody_required")
        _check_stopped_control(pgdata, checkpoint)
        prepared.retained_wal.assert_held(check)
    _arm_prepared(prepared, anchor)
    with boot_isolated(pgdata, check) as (control_engine, target_engine):
        _open_receive_database(control_engine, target_engine, anchor, pgdata, destinations, prepared.tablespace_oids, check)
        prepared.custody.assert_archive_held(check)
        with target_engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            closure = close_named_archive_suffix(conn, suffix=suffix, baseline=prepared.baseline, native_named=native,
                native_named_sha256=native["verification_sha256"], max_objects=site.max_archive_objects, timeout_seconds=check())
        runtime = verify_original_runtime(target_engine, base_restore, suffix_restore, pending_verified, check)
        if not _sha256(runtime):
            raise RuntimeError("canonical_named_recovery_original_runtime_unverified")
        prepared.custody.assert_archive_held(check)
    check()
    closure = {key: value for key, value in closure.items() if key != "closure_sha256"}
    closure["receiver_runtime_sha256"] = runtime
    closure["closure_sha256"] = archive_evidence_hash(closure)
    assignment = {key: request[key] for key in ("approved_package_sha256", "provider_boundary_sha256", "receiver_preflight_sha256",
                                               "prepared_receiver_sha256")}
    reply = {"schema": _RESPONSE, "request_sha256": archive_evidence_hash(request), "archive_closure": closure, **assignment,
             "pending_spool_verified": pending_verified, "runtime_sha256": runtime}
    _publish(response_path, _sign(reply, prepared.signer))
    return reply

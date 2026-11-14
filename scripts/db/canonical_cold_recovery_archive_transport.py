"""Ship and verify the final encrypted archive at the named recovery boundary."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from time import monotonic

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes, _run_native
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _json_write, _sync
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _safe_member, _stamp
from scripts.automation.verify_canonical_recovery_archive import _sha256, _sha_file
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_recovery_suffix import capture_final_archive_suffix, verify_final_archive_suffix
from scripts.db.canonical_cold_recovery_wal_transfer import _baseline, _receiving_lock, verify_final_wal
from scripts.db.fact_header_v2_capture import _bounded_step


_SCHEMA = "qt.encrypted_recovery_archive_transport.v1"
_CLOSURE = "qt.named_recovery_archive_closure.v1"
_DIR = "final-archive"


def _remaining(deadline):
    remaining = int(deadline - monotonic())
    if remaining <= 0:
        raise TimeoutError("canonical_recovery_archive_transport_time_exceeded")
    return remaining


def _restic_run(binary, key_path, repository, work, deadline, check, *, read_only):
    binary = _binary(binary)
    key_path = Path(key_path)
    if key_path.is_relative_to(repository):
        raise ValueError("canonical_recovery_archive_transport_key_must_be_separate")
    key = _private_bytes(key_path, limit=4096).decode("ascii").strip()
    if not _sha256(key):
        raise ValueError("canonical_recovery_archive_transport_key_invalid")
    env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": work, "TMPDIR": work, "RESTIC_PASSWORD": key}
    command = [str(binary), "--repo", str(repository), "--no-cache", "--json", "--quiet"]
    if read_only:
        command.append("--no-lock")

    def run(*args):
        return _run_native([*command, *args], cwd=work, env=env, deadline=deadline, check=check)

    version = _run_native([str(binary), "version"], cwd=work, env=env, deadline=deadline, check=check)
    if not version.startswith(b"restic 0.19.1 "):
        raise RuntimeError("canonical_recovery_archive_transport_unqualified_restic")
    return run


def _binding(pair, anchor, wal, *, baseline_verification_sha256, archive):
    return {
        "schema": _SCHEMA, "pair_receipt_sha256": wal["pair_receipt_sha256"],
        "database_identity": pair["database_identity"], "storage_layout": anchor["storage_layout"],
        "source_commit_seq": anchor["source_commit_seq"], "restore_name": anchor["restore_name"],
        "anchor_sha256": wal["anchor_sha256"], "anchor_content_sha256": archive_evidence_hash(anchor),
        "base_transfer_sha256": wal["base_transfer_sha256"],
        "baseline_verification_sha256": baseline_verification_sha256,
        "wal_transfer_sha256": wal["wal_transfer_sha256"],
        "wal_segment": wal["wal_transferred_through"], "archive": archive,
    }


def _boundary(receiving, wal_options, expected_wal_sha256, deadline):
    limits = wal_options["limits"]
    wal_options = wal_options | {"limits": replace(limits, seconds=min(limits.seconds, _remaining(deadline)))}
    wal = verify_final_wal(receiving=receiving, expected_hash=expected_wal_sha256, **wal_options)
    baseline = wal_options["baseline"]
    pair = _baseline(receiving, wal_options["base_transfer_sha256"], baseline, wal_options["baseline_verification_sha256"])
    anchor = _bound_json(wal_options["anchor_path"], wal_options["anchor_sha256"])
    return pair, anchor, wal | {"pair_receipt_sha256": baseline["pair_receipt_sha256"]}


def _encrypted_inventory(repository, device, limits, check):
    digest = hashlib.sha256()
    files = total = visited = 0
    for directory, children, names in os.walk(repository, followlinks=False):
        check()
        visited += 1
        if visited > limits.files + 300:
            raise RuntimeError("canonical_recovery_archive_transport_member_budget_exceeded")
        root, _ = _private_root(Path(directory))
        children.sort()
        for child in children:
            _, identity = _private_root(root / child)
            if identity[0] != device:
                raise RuntimeError("canonical_recovery_archive_transport_repository_device_changed")
            check()
        for name in sorted(names):
            check()
            path = root / name
            observed = _safe_member(path, device)
            total += observed.st_size
            files += 1
            if observed.st_nlink != 1 or files > limits.files or total > limits.bytes:
                raise RuntimeError("canonical_recovery_archive_transport_ciphertext_budget_exceeded")
            sha256, size = _sha_file(path, check=check)
            if _stamp(path.lstat()) != _stamp(observed) or size != observed.st_size:
                raise RuntimeError("canonical_recovery_archive_transport_repository_changed")
            entry = [path.relative_to(repository).as_posix(), size, sha256]
            digest.update((json.dumps(entry, separators=(",", ":")) + "\n").encode())
    if not files:
        raise RuntimeError("canonical_recovery_archive_transport_repository_empty")
    return {"encrypted_inventory_sha256": digest.hexdigest(), "encrypted_files": files, "encrypted_bytes": total}


def stage_encrypted_archive_suffix(*, fence, baseline, source_root, plaintext_stage, receiving, wal_options,
                                   expected_wal_sha256, restic, encryption_key, max_objects, max_plaintext_bytes,
                                   limits, check_archive):
    """Write a new Restic suffix directly onto the mounted receiving filesystem."""
    if not isinstance(fence, DatabaseFence) or not callable(check_archive):
        raise ValueError("canonical_recovery_archive_transport_fence_required")
    receiving, receiver_id = _private_root(receiving)
    plaintext_stage, stage_id = _private_root(plaintext_stage)
    source_root = Path(source_root)
    if (stage_id[0] == receiver_id[0] or source_root.stat().st_dev == receiver_id[0]
            or Path(encryption_key).is_relative_to(receiving)):
        raise ValueError("canonical_recovery_archive_transport_requires_separate_receiving_mount")
    deadline = min(monotonic() + limits.seconds, fence.deadline)
    free = shutil.disk_usage(receiving).free
    private = receiving / ("." + _DIR)
    published = receiving / _DIR
    if free < limits.reserve_bytes:
        raise RuntimeError("canonical_recovery_archive_transport_reserve_exhausted")

    def check():
        _remaining(deadline)
        fence.check_clock()
        check_archive()
        if _identity(receiving.lstat()) != receiver_id or _identity(plaintext_stage.lstat()) != stage_id:
            raise RuntimeError("canonical_recovery_archive_transport_mount_changed")
        now = shutil.disk_usage(receiving).free
        if now < limits.reserve_bytes or free - now > limits.bytes:
            raise RuntimeError("canonical_recovery_archive_transport_ciphertext_budget_exceeded")

    with _receiving_lock(receiving), _bounded_step(fence.connection, min(600, _remaining(deadline))) as bound:
        bound(deadline - monotonic())
        fence.assert_quiet(fence.connection)
        check()
        if published.exists() or published.is_symlink() or private.exists() or private.is_symlink():
            raise ValueError("canonical_recovery_archive_transport_requires_unused_destination")
        pair, anchor, wal = _boundary(receiving, wal_options, expected_wal_sha256, deadline)
        if anchor["database"] != fence.database or baseline.receipt_sha256 != wal["pair_receipt_sha256"]:
            raise RuntimeError("canonical_recovery_archive_transport_live_boundary_changed")
        archive = capture_final_archive_suffix(fence, anchor, baseline, source_root=source_root,
            stage=plaintext_stage, max_objects=max_objects, max_suffix_bytes=max_plaintext_bytes, check_archive=check)
        private.mkdir(mode=0o700)
        repository = private / "restic"
        repository.mkdir(mode=0o700)
        with TemporaryDirectory(prefix="qt-archive-suffix-") as work:
            run = _restic_run(restic, encryption_key, repository, work, deadline, check, read_only=False)
            run("init")
            output = run("backup", "--host", "qt-" + hashlib.sha256(pair["database_identity"].encode()).hexdigest()[:32],
                "--tag", anchor["restore_name"], "--no-scan", "--read-concurrency", "1",
                "--files-from-raw", str(plaintext_stage / "files.list"))
        summaries = [item for line in output.splitlines() if (item := json.loads(line)).get("message_type") == "summary"]
        if len(summaries) != 1 or not _sha256(summaries[0].get("snapshot_id")):
            raise RuntimeError("canonical_recovery_archive_transport_snapshot_invalid")
        archive["archive_snapshot"] = summaries[0]["snapshot_id"]
        archive.update(_encrypted_inventory(repository, receiver_id[0], limits, check))
        _sync(repository)
        report = _binding(pair, anchor, wal, baseline_verification_sha256=wal_options["baseline_verification_sha256"], archive=archive)
        receipt = private / "complete.json"
        _json_write(receipt, report)
        with receipt.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        fence.assert_quiet(fence.connection)
        check()
        _sync(private)
        private.rename(published)
        _sync(receiving)
    return report | {"archive_transfer_sha256": digest}


def verify_named_archive_closure(conn, *, receiving, baseline, wal_options, expected_wal_sha256,
                                 native_named, native_named_sha256, archive_transfer_sha256,
                                 archive_restore, restic, decryption_key, max_objects, limits):
    """Restore the exact suffix, then close it against the named database in a stable read transaction."""
    receiving, receiver_id = _private_root(receiving)
    archive_restore, restore_id = _private_root(archive_restore)
    if (receiving.is_relative_to(archive_restore) or archive_restore.is_relative_to(receiving)
            or any(archive_restore.iterdir()) or Path(decryption_key).is_relative_to(receiving)):
        raise ValueError("canonical_recovery_archive_transport_isolated_restore_required")
    endpoint = conn.engine.url
    query_host = endpoint.query.get("host")
    if (endpoint.host not in {None, "127.0.0.1", "::1", "restored"}
            or (query_host is not None and not str(query_host).startswith("/"))
            or not conn.in_transaction() or conn.get_isolation_level() != "REPEATABLE READ"
            or conn.scalar(text("SHOW transaction_read_only")) != "on"):
        raise ValueError("canonical_recovery_archive_transport_readonly_snapshot_required")
    deadline = monotonic() + limits.seconds
    pair, anchor, wal = _boundary(receiving, wal_options, expected_wal_sha256, deadline)
    recovery = conn.execute(text("""
        SELECT pg_is_in_recovery() AS recovering, current_setting('archive_mode') AS archive_mode,
               current_setting('recovery_target_name') AS target_name,
               pg_last_wal_replay_lsn() = CAST(:lsn AS pg_lsn) AS anchor_replayed
    """), {"lsn": anchor["anchor_lsn"]}).one()
    if tuple(recovery) != (False, "off", anchor["restore_name"], True):
        raise RuntimeError("canonical_recovery_archive_transport_named_replay_changed")
    archive_dir = receiving / _DIR
    report = _bound_json(archive_dir / "complete.json", archive_transfer_sha256)
    archive = report["archive"]
    if report != _binding(pair, anchor, wal, baseline_verification_sha256=wal_options["baseline_verification_sha256"], archive=archive):
        raise RuntimeError("canonical_recovery_archive_transport_receipt_mismatch")
    named = {key: value for key, value in native_named.items() if key != "verification_sha256"}
    if (native_named.get("schema") != "qt.encrypted_recovery_repository_verification.v1"
            or native_named.get("verification_sha256") != native_named_sha256 or archive_evidence_hash(named) != native_named_sha256
            or named.get("pair_receipt_sha256") != baseline.receipt_sha256
            or named.get("anchor_sha256") != report["anchor_sha256"] or named.get("wal_verified_through") != report["wal_segment"]
            or any(named.get(key) != pair[key] for key in ("database_identity", "database_label", "archive_snapshot", "storage_layout"))):
        raise RuntimeError("canonical_recovery_archive_transport_native_named_mismatch")
    free = shutil.disk_usage(archive_restore).free

    def check():
        _remaining(deadline)
        if _identity(receiving.lstat()) != receiver_id or _identity(archive_restore.lstat()) != restore_id:
            raise RuntimeError("canonical_recovery_archive_transport_mount_changed")
        now = shutil.disk_usage(archive_restore).free
        if now < limits.reserve_bytes or free - now > limits.bytes:
            raise RuntimeError("canonical_recovery_archive_transport_plaintext_budget_exceeded")

    with TemporaryDirectory(prefix="qt-archive-closure-") as work:
        repository, repository_id = _private_root(archive_dir / "restic")
        if repository_id[0] != receiver_id[0]:
            raise ValueError("canonical_recovery_archive_transport_repository_device_changed")
        run = _restic_run(restic, decryption_key, repository, work, deadline, check, read_only=True)
        snapshot = archive["archive_snapshot"]
        if not _sha256(snapshot):
            raise RuntimeError("canonical_recovery_archive_transport_snapshot_invalid")
        encrypted = _encrypted_inventory(repository, receiver_id[0], limits, check)
        if any(archive.get(key) != value for key, value in encrypted.items()):
            raise RuntimeError("canonical_recovery_archive_transport_ciphertext_changed")
        metadata = json.loads(run("cat", "snapshot", snapshot), object_pairs_hook=_unique_fields)
        owner = "qt-" + hashlib.sha256(pair["database_identity"].encode()).hexdigest()[:32]
        if metadata.get("hostname") != owner or metadata.get("tags") != [anchor["restore_name"]]:
            raise RuntimeError("canonical_recovery_archive_transport_snapshot_owner_mismatch")
        size = json.loads(run("stats", snapshot, "--mode=restore-size"), object_pairs_hook=_unique_fields)["total_size"]
        if type(size) is not int or not archive["archive_bytes"] <= size <= limits.bytes or free < limits.reserve_bytes + size:
            raise RuntimeError("canonical_recovery_archive_transport_restore_budget_exceeded")
        run("restore", snapshot, "--target", str(archive_restore), "--verify")
    check()
    conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"), {"timeout": str(_remaining(deadline) * 1000)})
    coverage = verify_final_archive_suffix(conn, anchor, baseline, archive, archive_restore,
                                           max_objects=max_objects, suffix_snapshot_id=snapshot, check=check)
    result = {"schema": _CLOSURE, "archive_transfer_sha256": archive_transfer_sha256,
              "native_named_sha256": native_named_sha256, "wal_transfer_sha256": expected_wal_sha256,
              "base_transfer_sha256": report["base_transfer_sha256"], "anchor_sha256": report["anchor_sha256"],
              "baseline_verification_sha256": report["baseline_verification_sha256"],
              "storage_layout": report["storage_layout"], "wal_segment": report["wal_segment"],
              "pair_receipt_sha256": baseline.receipt_sha256, "database_identity": pair["database_identity"],
              "source_commit_seq": anchor["source_commit_seq"], "restore_name": anchor["restore_name"],
              "replay_lsn": anchor["anchor_lsn"], "coverage": coverage}
    return result | {"closure_sha256": archive_evidence_hash(result)}

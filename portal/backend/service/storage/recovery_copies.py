"""Bounded local recovery generations for the existing single-node layout.

The caller owns the exported snapshot and archive-expiry fence, and must reserve
capacity before calling. This primitive is not wired to policy activation or a
scheduler. It never deletes source records or source archive objects.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
from time import monotonic
from uuid import uuid4

from sqlalchemy import text

from core.storage_targets import StorageLocation, StorageTarget

logger = logging.getLogger(__name__)
_VERSION = "qt.local_recovery_copy.v1"
_NAME = re.compile(r"copy_[0-9a-f]{32}")
_HASH = re.compile(r"[0-9a-f]{64}")
_FAMILIES = (
    ("raw_archive_manifests", "raw_manifest"),
    ("book_checkpoint_manifests", "book_checkpoint"),
    ("fact_archive_manifests", None),
)
_CHUNK = 1024 * 1024
MAX_RECOVERY_OBJECTS = 10_000_000


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _json_write(path, value):
    with path.open("x", encoding="utf-8") as output:
        os.chmod(path, 0o600)
        output.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def _private_directory(path, device, *, create=False):
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077 or info.st_dev != device):
        raise RuntimeError("recovery_directory_not_private_or_wrong_filesystem")
    return info


def _identity(session):
    value = session.execute(text("""
        SELECT c.system_identifier::text || '/' || d.oid::text
        FROM pg_control_system() c CROSS JOIN pg_database d
        WHERE d.datname=current_database()
    """)).scalar_one()
    return value, hashlib.sha256(value.encode()).hexdigest()[:32]



def _snapshot_layout(session):
    """Identify the storage certificate visible to this exact database snapshot."""
    rows = session.execute(text("""
        SELECT layout_version,state,completed_at,evidence
        FROM market.fact_storage_state ORDER BY layout_version LIMIT 2
    """)).mappings().all()
    if (len(rows) != 1 or rows[0]["state"] != "ready"
            or rows[0]["layout_version"] not in (
                "market.fact_storage_tiers.v1", "market.fact_storage_tiers.v2")
            or rows[0]["completed_at"] is None or rows[0]["completed_at"].tzinfo is None
            or not isinstance(rows[0]["evidence"], dict)):
        raise RuntimeError("recovery_storage_layout_not_ready")
    row = rows[0]
    certificate = {"layout_version": row["layout_version"],
        "completed_at": row["completed_at"].astimezone(UTC).isoformat(),
        "evidence": row["evidence"]}
    digest = hashlib.sha256(json.dumps(certificate, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return {"layout_version": row["layout_version"], "certificate_sha256": digest}


def _archive_rows(session, *, max_objects, check):
    count = 0
    for table, kind in _FAMILIES:
        after = ""
        predicate = "" if kind is None else """
            AND NOT EXISTS (
                SELECT 1 FROM market.storage_lifecycle_events e
                WHERE e.action='archive_expire' AND e.event_type='completed'
                  AND e.target_kind=:kind AND e.target_id=m.id)
        """
        while True:
            check()
            rows = session.execute(text(f"""
                SELECT m.id,m.object_key,m.object_sha256,m.byte_count
                FROM market.{table} m WHERE m.id>:after {predicate}
                ORDER BY m.id LIMIT 256
            """), {"after": after, "kind": kind}).mappings().all()
            if not rows:
                break
            for row in rows:
                count += 1
                if count > max_objects:
                    raise RuntimeError("recovery_object_budget_exceeded")
                yield dict(row)
            after = rows[-1]["id"]


class LocalRecoveryCopies:
    """One private, identity-bound recovery directory on the configured HDD."""

    schema_version = _VERSION
    namespace = "recovery"

    def __init__(self, *, target: StorageTarget, database_identity: str,
                 max_bytes: int, reserve_bytes: int, timeout_seconds: int,
                 max_objects: int = 1000000, cancelled=None, check_resources=None):
        if (target.medium != "hdd" or target.state != "active" or "backups" not in target.roles
                or not re.fullmatch(r"[0-9]+/[0-9]+", database_identity)):
            raise ValueError("recovery_target_or_database_invalid")
        for value, lower, upper in (
            (max_bytes, 1, 2**63-1), (reserve_bytes, 0, 2**63-1),
            (timeout_seconds, 1, 86400), (max_objects, 1, MAX_RECOVERY_OBJECTS),
        ):
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError("recovery_budget_invalid")
        if any(value is not None and not callable(value) for value in (cancelled, check_resources)):
            raise ValueError("recovery_control_callback_invalid")
        self.cancelled, self.check_resources = cancelled, check_resources
        observed = target.inspect(require_writable=True)
        self.target, self.identity = target, database_identity
        self.device = Path(observed.path).stat().st_dev
        self.target_root = Path(observed.path)
        self.max_bytes, self.reserve_bytes = max_bytes, reserve_bytes
        self.deadline, self.max_objects = monotonic()+timeout_seconds, max_objects
        self.written = 0
        parent = self.target_root/self.namespace
        _private_directory(parent, self.device, create=True)
        self.root = parent/hashlib.sha256(database_identity.encode()).hexdigest()[:32]
        _private_directory(self.root, self.device, create=True)
        self.root_inode = self.root.stat().st_ino
        _sync(parent)
        _sync(self.target_root)

    def check(self, additional=0):
        if self.cancelled is not None and self.cancelled():
            raise RuntimeError("recovery_cancelled")
        if monotonic() >= self.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")
        if self.check_resources is not None:
            self.check_resources()
        evidence = self.target.inspect(require_writable=True)
        if (Path(evidence.path) != self.target_root
                or self.target_root.stat().st_dev != self.device
                or self.root.stat().st_ino != self.root_inode):
            raise RuntimeError("recovery_filesystem_changed")
        _private_directory(self.root, self.device)
        if self.written + additional > self.max_bytes:
            raise RuntimeError("recovery_byte_budget_exceeded")
        if additional and evidence.available_bytes < self.reserve_bytes + additional:
            raise RuntimeError("recovery_free_space_reserve_reached")

    @contextmanager
    def lock(self):
        import fcntl
        self.check()
        fd = os.open(self.root/"writer.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise RuntimeError("recovery_lock_not_private")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("recovery_copy_already_running") from exc
            yield
        finally:
            os.close(fd)

    def _write(self, output, data, digest):
        self.check(len(data))
        output.write(data)
        digest.update(data)
        self.written += len(data)

    def _dump(self, session, destination, binary):
        binary = Path(binary)
        if (not binary.is_absolute() or not binary.is_file()
                or binary.stat().st_uid not in (0, os.getuid())
                or binary.stat().st_mode & 0o022 or not os.access(binary, os.X_OK)):
            raise ValueError("recovery_absolute_pg_dump_required")
        url = session.bind.engine.url
        if url.get_backend_name() != "postgresql" or url.query:
            raise ValueError("recovery_requires_plain_postgresql_connection")
        snapshot = session.execute(text("SELECT pg_export_snapshot()")).scalar_one()
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": "/tmp",
               "PGHOST": url.host or "", "PGPORT": str(url.port or 5432),
               "PGDATABASE": url.database or "", "PGUSER": url.username or "",
               "PGPASSWORD": url.password or "", "PGCONNECT_TIMEOUT": "10"}
        if not all(env[key] for key in ("PGHOST", "PGDATABASE", "PGUSER")):
            raise ValueError("recovery_connection_incomplete")
        command = [str(binary), "--format=custom", "--compress=6", "--no-owner",
                   "--no-privileges", "--snapshot="+snapshot]
        digest = hashlib.sha256()
        with destination.open("xb") as output:
            os.chmod(destination, 0o600)
            with subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
                errors = bytearray()
                try:
                    with selectors.DefaultSelector() as streams:
                        streams.register(process.stdout, selectors.EVENT_READ, "data")
                        streams.register(process.stderr, selectors.EVENT_READ, "error")
                        while streams.get_map():
                            self.check()
                            for key, _ in streams.select(timeout=min(0.25, max(0, self.deadline-monotonic()))):
                                data = os.read(key.fileobj.fileno(), _CHUNK)
                                if not data:
                                    streams.unregister(key.fileobj)
                                elif key.data == "data":
                                    self._write(output, data, digest)
                                else:
                                    errors.extend(data)
                                    if len(errors) > 65536:
                                        raise RuntimeError("recovery_dump_error_output_limit")
                    code = process.wait(timeout=max(0.01, self.deadline-monotonic()))
                    if code:
                        # PostgreSQL error output can contain record contents.
                        raise RuntimeError(f"recovery_pg_dump_failed: exit_code={code}")
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=10)
                output.flush()
                os.fsync(output.fileno())
        self.check()
        if destination.stat().st_size == 0:
            raise RuntimeError("recovery_dump_empty")
        return {"bytes": destination.stat().st_size, "sha256": digest.hexdigest()}

    def _object(self, objects, row, generation):
        key, expected, size = row["object_key"], row["object_sha256"], row["byte_count"]
        StorageLocation(self.target.target_id, key)
        if len(key)>2048 or len(Path(key).parts)>64:
            raise RuntimeError("recovery_archive_key_budget_exceeded")
        if not _HASH.fullmatch(expected) or type(size) is not int or size <= 0:
            raise RuntimeError("recovery_archive_descriptor_invalid")
        source = objects.local_path(key)
        if source.is_symlink():
            raise RuntimeError("recovery_archive_symlink")
        destination = generation/"objects"/key
        ancestor = generation
        for part in destination.relative_to(generation).parts[:-1]:
            ancestor = ancestor/part
            _private_directory(ancestor, self.device, create=True)
        digest = hashlib.sha256()
        total = 0
        with os.fdopen(os.open(source, os.O_RDONLY | os.O_NOFOLLOW), "rb") as incoming:
            before = os.fstat(incoming.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise RuntimeError("recovery_archive_size_mismatch")
            try:
                output = destination.open("xb")
            except FileExistsError as exc:
                raise RuntimeError("recovery_duplicate_archive_key") from exc
            with output:
                os.chmod(destination, 0o600)
                while data := incoming.read(_CHUNK):
                    self._write(output, data, digest)
                    total += len(data)
                output.flush()
                os.fsync(output.fileno())
            after = os.fstat(incoming.fileno())
        stable = lambda value: (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
        if total != size or digest.hexdigest() != expected or stable(before) != stable(after):
            raise RuntimeError("recovery_archive_changed_or_corrupt")
        return {"object_key": key, "bytes": total, "sha256": expected}

    def _owned(self, directory):
        self.check()
        if directory.parent != self.root or not _NAME.fullmatch(directory.name.removeprefix(".")):
            raise RuntimeError("recovery_generation_name_invalid")
        _private_directory(directory, self.device)
        marker = directory/"owner.json"
        if marker.is_symlink() or marker.stat().st_size > 4096:
            raise RuntimeError("recovery_generation_owner_invalid")
        owner = json.loads(marker.read_text())
        if owner != {"schema_version": self.schema_version, "database_identity": self.identity,
                     "filesystem_uuid": self.target.filesystem_uuid, "name": directory.name.removeprefix(".")}:
            raise RuntimeError("recovery_generation_owner_mismatch")
        return owner

    def _remove(self, directory):
        self.check()
        if directory.parent!=self.root or not _NAME.fullmatch(directory.name.removeprefix(".")):
            raise RuntimeError("recovery_generation_name_invalid")
        _private_directory(directory,self.device)
        # A crash between removing the final owner marker and rmdir leaves only
        # an empty private tombstone. Never treat an unmarked nonempty tree as ours.
        if directory.name.startswith(".") and not any(directory.iterdir()):
            directory.rmdir()
            _sync(self.root)
            return
        self._owned(directory)
        for root, directories, files in os.walk(directory, followlinks=False):
            self.check()
            _private_directory(Path(root), self.device)
            for name in (*directories, *files):
                info = (Path(root)/name).lstat()
                if (info.st_dev != self.device or info.st_uid != os.getuid()
                        or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))):
                    raise RuntimeError("recovery_generation_unsafe_member")
        if not directory.name.startswith("."):
            tombstone=self.root/("."+directory.name)
            if tombstone.exists():
                raise RuntimeError("recovery_retirement_name_conflict")
            directory.rename(tombstone)
            directory=tombstone
            _sync(self.root)
        # Delete the owner marker last so an interrupted retirement is resumable.
        for root, directories, files in os.walk(directory, topdown=False):
            self.check()
            for name in files:
                path=Path(root)/name
                if path!=directory/"owner.json":
                    path.unlink()
            for name in directories:
                (Path(root)/name).rmdir()
        (directory/"owner.json").unlink()
        directory.rmdir()
        _sync(self.root)

    def completed(self):
        results = []
        for directory in sorted(self.root.iterdir()):
            if not _NAME.fullmatch(directory.name):
                continue
            self._owned(directory)
            path = directory/"complete.json"
            if path.is_symlink() or path.stat().st_size > 16384:
                raise RuntimeError("recovery_completion_invalid")
            receipt = json.loads(path.read_text())
            if (receipt.get("schema_version") != self.schema_version or receipt.get("name") != directory.name
                    or receipt.get("database_identity") != self.identity
                    or receipt.get("filesystem_uuid") != self.target.filesystem_uuid):
                raise RuntimeError("recovery_completion_identity_mismatch")
            # Older copies remain recoverable, but cannot prove coverage of the
            # current layout. Scheduling will create a newly certified copy.
            if "storage_layout" in receipt:
                layout = receipt["storage_layout"]
                if (not isinstance(layout, dict)
                        or set(layout) != {"layout_version", "certificate_sha256"}
                        or layout["layout_version"] not in (
                            "market.fact_storage_tiers.v1", "market.fact_storage_tiers.v2")
                        or not isinstance(layout["certificate_sha256"], str)
                        or not _HASH.fullmatch(layout["certificate_sha256"])):
                    raise RuntimeError("recovery_completion_layout_invalid")
            stamp = datetime.fromisoformat(receipt["completed_at"])
            if stamp.tzinfo is None:
                raise RuntimeError("recovery_completion_time_invalid")
            results.append((stamp, directory, receipt))
        return sorted(results, key=lambda row: (row[0], row[1].name))

    def create(self, session, *, objects, pg_dump, keep_copies):
        """Caller holds its consistent snapshot and shared archive-expiry fence.

        A failed attempt never rotates completed copies. No source file is
        removed. Caller must own capacity against competing storage jobs.
        """
        if type(keep_copies) is not int or not 1 <= keep_copies <= 30:
            raise ValueError("recovery_keep_copies_invalid")
        original_timeout=session.scalar(text("SHOW statement_timeout"))
        original_ms=session.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
        if original_ms:
            self.deadline=min(self.deadline,monotonic()+original_ms/1000)
        def phase():
            self.check()
            remaining=max(1,int((self.deadline-monotonic())*1000))
            session.execute(text("SELECT set_config('statement_timeout',:value,true)"),
                            {"value":str(remaining)})
        phase()
        identity, _ = _identity(session)
        if identity != self.identity or session.connection().get_isolation_level() != "REPEATABLE READ":
            raise RuntimeError("recovery_snapshot_identity_or_isolation_invalid")
        snapshot_layout = _snapshot_layout(session)
        # Verify the actual shared session-level expiry fence, not a caller flag.
        from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
        held=session.scalar(text("""
            WITH key AS (SELECT hashtextextended(:name,0) AS value)
            SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
              AND pid=pg_backend_pid() AND mode='ShareLock' AND granted AND objsubid=1
              AND classid::bigint=((key.value>>32)&4294967295)
              AND objid::bigint=(key.value&4294967295))
        """),{"name":_LIFECYCLE_LOCK_NAME})
        if not held:
            raise RuntimeError("recovery_archive_expiry_fence_required")
        with self.lock():
            previous = self.completed()
            # An interrupted private generation never counts as a usable copy.
            for partial in sorted(self.root.glob(".copy_*")):
                self._remove(partial)
            name = "copy_"+uuid4().hex
            partial = self.root/("."+name)
            partial.mkdir(mode=0o700)
            _json_write(partial/"owner.json", {"schema_version": self.schema_version,
                "database_identity": self.identity, "filesystem_uuid": self.target.filesystem_uuid, "name": name})
            _sync(partial)
            _sync(self.root)
            try:
                dump = self._dump(session, partial/"database.dump", pg_dump)
                inventory_digest = hashlib.sha256()
                count = archive_bytes = 0
                with (partial/"objects.jsonl").open("xb") as inventory:
                    os.chmod(partial/"objects.jsonl", 0o600)
                    for row in _archive_rows(session, max_objects=self.max_objects, check=phase):
                        item = self._object(objects, row, partial)
                        encoded = (json.dumps(item, sort_keys=True, separators=(",", ":"))+"\n").encode()
                        self._write(inventory, encoded, inventory_digest)
                        count += 1
                        archive_bytes += item["bytes"]
                    inventory.flush()
                    os.fsync(inventory.fileno())
                receipt = {"schema_version": self.schema_version, "name": name, "database_identity": self.identity,
                    "filesystem_uuid": self.target.filesystem_uuid,
                    "storage_layout": snapshot_layout,
                    "completed_at": datetime.now(UTC).isoformat(), "database": dump,
                    "archive_objects": count, "archive_bytes": archive_bytes,
                    "inventory_sha256": inventory_digest.hexdigest(), "written_bytes": self.written}
                _json_write(partial/"complete.json", receipt)
                for root, directories, files in os.walk(partial, topdown=False):
                    self.check()
                    _sync(Path(root))
                self.check()
                completed = self.root/name
                partial.rename(completed)
                _sync(self.root)
            except BaseException:
                logger.exception("local_recovery_copy_failed | generation=%s", name)
                # Keep the marked partial for the next bounded retry to remove.
                raise
            # Publication is durable before the oldest completed copies retire.
            for _, directory, _ in previous[:max(0, len(previous)+1-keep_copies)]:
                self._remove(directory)
            logger.info("local_recovery_copy_completed | generation=%s archive_objects=%s bytes=%s",
                        name, count, self.written)
            session.execute(text("SELECT set_config('statement_timeout',:value,true)"),
                            {"value":original_timeout})
            return receipt

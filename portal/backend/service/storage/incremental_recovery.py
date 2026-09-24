"""Encrypted paired physical/archive recovery on the registered local HDD.

Repositories and independent keys are provisioned explicitly by the operator.
Runtime never initializes a repository, enables WAL archiving or migrates data.
The caller holds storage management ownership and an unqueried snapshot session
whose shared archive-expiry fence predates the physical backup.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
from time import monotonic
from uuid import uuid4

from sqlalchemy import text

from core.storage_targets import StorageLocation
from .recovery_copies import (
    LocalRecoveryCopies, _archive_rows, _identity, _snapshot_layout,
    _json_write, _private_directory, _sync, _HASH, _NAME, _CHUNK,
)

_LABEL = re.compile(r"[0-9]{8}-[0-9]{6}F(?:_[0-9]{8}-[0-9]{6}[DI])?")
_VERSION = "qt.encrypted_recovery_pair.v1"
_PREPARED = "qt.encrypted_recovery_repository.v1"


def _private_bytes(path, *, limit):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("incremental_private_file_absolute_unaliased_path_required")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.getuid())
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise RuntimeError("incremental_private_file_permissions_invalid")
        result = handle.read(limit + 1)
        if len(result) > limit:
            raise RuntimeError("incremental_private_file_size_limit")
        return result


def _binary(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("incremental_absolute_tool_required")
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.getuid())
            or info.st_mode & 0o022 or not os.access(path, os.X_OK)):
        raise ValueError("incremental_tool_permissions_invalid")
    return path


def _root_label(label):
    if not isinstance(label, str) or not _LABEL.fullmatch(label):
        raise RuntimeError("incremental_native_backup_label_invalid")
    return label[:16]


@dataclass(frozen=True)
class IncrementalRecoveryConfig:
    pgbackrest: Path
    restic: Path
    pg_path: Path
    pg_socket_path: Path
    database_key_path: Path
    archive_key_path: Path
    max_chain_backups: int

    @classmethod
    def from_dict(cls, value):
        fields = set(cls.__dataclass_fields__)
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("incremental_configuration_fields_invalid")
        count = value["max_chain_backups"]
        if type(count) is not int or not 1 <= count <= 365:
            raise ValueError("incremental_chain_limit_invalid")
        paths = {}
        for key in fields - {"max_chain_backups"}:
            raw = value[key]
            if (not isinstance(raw, str) or not raw or len(raw) > 4096
                    or any(c in raw for c in "\x00\r\n") or not Path(raw).is_absolute()
                    or ".." in Path(raw).parts):
                raise ValueError("incremental_configuration_path_invalid")
            paths[key] = Path(raw)
        return cls(**paths, max_chain_backups=count)


def _keys(config, target_root):
    # Keys on the archive drive cannot establish independent recovery.
    for key in (config.database_key_path, config.archive_key_path):
        if key.resolve(strict=True).is_relative_to(target_root.resolve(strict=True)):
            raise RuntimeError("incremental_key_must_not_live_on_backup_target")
    database_key = _private_bytes(config.database_key_path, limit=4096).decode("ascii").strip()
    archive_key = _private_bytes(config.archive_key_path, limit=4096).decode("ascii").strip()
    if not all(re.fullmatch(r"[0-9a-f]{64}", key) for key in (database_key, archive_key)):
        raise ValueError("incremental_requires_independent_256bit_keys")
    if database_key == archive_key:
        raise ValueError("incremental_keys_must_differ")
    return database_key, archive_key


class EncryptedRecoveryCopies(LocalRecoveryCopies):
    """Reuse the existing target, ownership, deadline and publication guards."""

    schema_version = _VERSION
    namespace = "recovery-incremental"

    def __init__(self, *, incremental, connection_url, **kwargs):
        super().__init__(**kwargs)
        self.config = incremental
        self.pgbackrest = _binary(incremental.pgbackrest)
        self.restic = _binary(incremental.restic)
        if (connection_url.get_backend_name() != "postgresql" or connection_url.query
                or not connection_url.username or not connection_url.database):
            raise ValueError("incremental_requires_canonical_postgresql_connection")
        self.url = connection_url
        self.free_at_start = self.target.inspect(require_writable=True).available_bytes
        self.peak_allocated_bytes = 0
        database_key, archive_key = _keys(incremental, self.target_root)
        self.env = {
            "PATH": os.defpath, "LC_ALL": "C", "HOME": str(self.root),
            "PGBACKREST_REPO1_CIPHER_PASS": database_key,
            "RESTIC_PASSWORD": archive_key,
            # Authentication derives only from the already-open PG_DSN.
            "PGPASSWORD": connection_url.password or "",
            "PGPASSFILE": "/dev/null", "PGSERVICEFILE": "/dev/null",
            "PGCONNECT_TIMEOUT": "10",
        }
        prepared = json.loads(_private_bytes(self.root/"prepared.json", limit=16384))
        expected = {
            "schema_version": _PREPARED, "database_identity": self.identity,
            "filesystem_uuid": self.target.filesystem_uuid,
            "database_key_sha256": hashlib.sha256(database_key.encode()).hexdigest(),
            "archive_key_sha256": hashlib.sha256(archive_key.encode()).hexdigest(),
        }
        if prepared != expected:
            raise RuntimeError("incremental_prepared_repository_identity_mismatch")
        for name in ("database", "archives", "locks", "logs"):
            _private_directory(self.root/name, self.device)

    def check(self, additional=0):
        super().check(additional)
        if hasattr(self, "free_at_start"):
            free = self.target.inspect(require_writable=True).available_bytes
            self.peak_allocated_bytes = max(self.peak_allocated_bytes, self.free_at_start-free)
            if free < self.reserve_bytes + additional:
                raise RuntimeError("recovery_free_space_reserve_reached")
            # Conservative peak allocation includes concurrent writers. A native
            # child cannot silently consume its admitted reserve between phases.
            if self.peak_allocated_bytes + additional > self.max_bytes:
                raise RuntimeError("recovery_byte_budget_exceeded")

    def _br(self, *args):
        command = args[-1]
        options = {
            "stanza": "qt", "repo1-path": str(self.root/"database"),
            "repo1-cipher-type": "aes-256-cbc",
            "lock-path": str(self.root/"locks"), "log-path": str(self.root/"logs"),
            "log-level-console": "warn", "log-level-file": "off",
        }
        if command in ("backup", "stanza-create", "check", "restore"):
            options["pg1-path"] = str(self.config.pg_path)
        if command in ("backup", "stanza-create", "check"):
            options.update({
                "pg1-socket-path": str(self.config.pg_socket_path),
                "pg1-port": str(self.url.port or 5432),
                "pg1-user": self.url.username, "pg1-database": self.url.database,
            })
        if command == "backup":
            options.update({
                "repo1-bundle": "y", "repo1-block": "y", "expire-auto": "n",
                "archive-copy": "y", "archive-check": "y", "archive-timeout": "60",
                "compress-type": "zst", "process-max": "1", "start-fast": "y",
            })
        for option in args[:-1]:
            key, value = option.removeprefix("--").split("=", 1)
            options[key] = value
        return [str(self.pgbackrest), "--no-config",
                *["--"+key+"="+value for key, value in options.items()], command]

    def _rs(self, *args):
        return [str(self.restic), "--repo", str(self.root/"archives"),
                "--no-cache", "--json", *args]

    def _run(self, command):
        """Bounded subprocess, no secret-bearing diagnostics or ambient config."""
        self.check()
        captured = {"out": bytearray(), "err": bytearray()}
        with subprocess.Popen(command, cwd=self.root, env=self.env,
                              stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, start_new_session=True) as process:
            try:
                with selectors.DefaultSelector() as streams:
                    streams.register(process.stdout, selectors.EVENT_READ, "out")
                    streams.register(process.stderr, selectors.EVENT_READ, "err")
                    while streams.get_map():
                        self.check()
                        for key, _ in streams.select(timeout=0.25):
                            chunk = os.read(key.fileobj.fileno(), 65536)
                            if not chunk:
                                streams.unregister(key.fileobj)
                                continue
                            captured[key.data].extend(chunk)
                            if len(captured[key.data]) > 8*1024*1024:
                                raise RuntimeError("incremental_tool_output_limit")
                code = process.wait(timeout=max(0.01, self.deadline-monotonic()))
                if code:
                    # Even native error output may include file contents or keys.
                    raise RuntimeError(f"incremental_tool_failed:{Path(command[0]).name}:exit={code}")
            finally:
                # Kill the owned group even if its leader exited while a child
                # kept pipes open. Never leave a writer beyond our deadline.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                if process.poll() is None:
                    process.wait(timeout=10)
        self.check()
        return bytes(captured["out"])

    def _native_backups(self):
        info = json.loads(self._run(self._br("--output=json", "info")))
        if (not isinstance(info, list) or len(info) != 1 or info[0].get("name") != "qt"
                or info[0].get("cipher") != "aes-256-cbc"):
            raise RuntimeError("incremental_native_repository_invalid")
        stanza = info[0]
        databases = stanza.get("db", [])
        if (len(databases) != 1 or str(databases[0].get("system-id")) != self.identity.split("/")[0]
                or str(databases[0].get("version")) != "15"):
            raise RuntimeError("incremental_native_database_identity_mismatch")
        backups = stanza.get("backup", [])
        for backup in backups:
            _root_label(backup["label"])
            if (backup.get("error") is not False
                    or backup.get("database", {}).get("id") != databases[0]["id"]
                    or not _NAME.fullmatch(backup.get("annotation", {}).get("qt-generation", ""))):
                raise RuntimeError("incremental_native_backup_not_owned_or_valid")
        labels = {backup["label"] for backup in backups}
        for backup in backups:
            references = backup.get("reference") or []
            if (not set(references) <= labels
                    or any(_root_label(ref) != _root_label(backup["label"]) for ref in references)):
                raise RuntimeError("incremental_native_dependency_missing")
        return backups

    def completed(self):
        results = super().completed()
        for _, _, receipt in results:
            _root_label(receipt.get("database_label"))
            if (not isinstance(receipt.get("archive_snapshot"), str)
                    or not _HASH.fullmatch(receipt["archive_snapshot"])
                    or not _HASH.fullmatch(receipt.get("inventory_sha256", ""))):
                raise RuntimeError("incremental_pair_receipt_invalid")
        return results

    def _assert_fence(self, session):
        from .repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
        remaining = max(1, int((self.deadline-monotonic())*1000))
        session.execute(text("SELECT set_config('statement_timeout',:limit,true)"),
                        {"limit": str(min(remaining, 1000))})
        held = session.scalar(text("""
            WITH key AS (SELECT hashtextextended(:name,0) AS value)
            SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
              AND pid=pg_backend_pid() AND mode='ShareLock' AND granted AND objsubid=1
              AND classid::bigint=((key.value>>32)&4294967295)
              AND objid::bigint=(key.value&4294967295))
        """), {"name": _LIFECYCLE_LOCK_NAME})
        if not held or session.connection().get_isolation_level() != "REPEATABLE READ":
            raise RuntimeError("recovery_archive_expiry_fence_required")
        if _identity(session)[0] != self.identity:
            raise RuntimeError("recovery_snapshot_identity_or_isolation_invalid")
        self.check()

    def _inventory(self, session, objects, partial):
        digest, count, total = hashlib.sha256(), 0, 0
        # File list is NUL-delimited and consumed verbatim: no glob expansion,
        # comments, newline ambiguity or shell interpretation.
        inventory_path, list_path = partial/"objects.jsonl", partial/"files.list"
        seen = set()
        with inventory_path.open("xb") as inventory, list_path.open("xb") as listing:
            inventory_path.chmod(0o600)
            list_path.chmod(0o600)
            for row in _archive_rows(session, max_objects=self.max_objects,
                                     check=lambda: self._assert_fence(session)):
                key, expected, size = row["object_key"], row["object_sha256"], row["byte_count"]
                StorageLocation(self.target.target_id, key)
                if (len(key) > 2048 or len(Path(key).parts) > 64
                        or not _HASH.fullmatch(expected) or type(size) is not int or size <= 0):
                    raise RuntimeError("recovery_archive_descriptor_invalid")
                if key in seen:
                    raise RuntimeError("recovery_duplicate_archive_key")
                seen.add(key)
                source = objects.local_path(key)
                if source.resolve(strict=True) != source:
                    raise RuntimeError("recovery_archive_symlink")
                actual, length = hashlib.sha256(), 0
                with os.fdopen(os.open(source, os.O_RDONLY | os.O_NOFOLLOW), "rb") as incoming:
                    before = os.fstat(incoming.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                        raise RuntimeError("recovery_archive_size_mismatch")
                    while chunk := incoming.read(_CHUNK):
                        self.check()
                        actual.update(chunk)
                        length += len(chunk)
                    after = os.fstat(incoming.fileno())
                stable = lambda s: (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
                if length != size or actual.hexdigest() != expected or stable(before) != stable(after):
                    raise RuntimeError("recovery_archive_changed_or_corrupt")
                encoded = (json.dumps({"object_key":key, "bytes":size, "sha256":expected},
                                      sort_keys=True, separators=(",", ":"))+"\n").encode()
                self._write(inventory, encoded, digest)
                listing.write(os.fsencode(source)+b"\0")
                count += 1
                total += size
            listing.write(os.fsencode(inventory_path)+b"\0")
            for handle in (inventory, listing):
                handle.flush()
                os.fsync(handle.fileno())
        return digest.hexdigest(), count, total

    def _prune(self, keep_copies):
        points = self.completed()
        if not points:
            return
        # Native unlock removes stale locks only. Never use --remove-all; active
        # restore/check locks must continue to exclude pruning.
        self._run(self._rs("unlock"))
        # Retire the public receipt BEFORE deleting any dependency. If interrupted,
        # hidden owner-marked generations form the retry journal.
        for _, directory, _ in points[:-keep_copies]:
            tombstone = self.root/("."+directory.name)
            if tombstone.exists():
                raise RuntimeError("recovery_retirement_name_conflict")
            directory.rename(tombstone)
            _sync(self.root)
        survivors = self.completed()
        labels = {r["database_label"] for _, _, r in survivors}
        roots = {_root_label(label) for label in labels}
        snapshots = {r["archive_snapshot"] for _, _, r in survivors}
        backups = self._native_backups()
        if not labels <= {b["label"] for b in backups}:
            raise RuntimeError("incremental_published_database_backup_missing")
        # Explicit whole-chain expiration only. Internal dependencies remain even
        # when their old recovery point has aged out of the user-facing count.
        for backup in backups:
            label = backup["label"]
            if label == _root_label(label) and label not in roots:
                self._run(self._br("--set="+label, "--repo1-retention-full=9999999",
                                   "--repo1-retention-archive=9999999", "expire"))
        native_snapshots = json.loads(self._run(self._rs("snapshots")))
        if not snapshots <= {s["id"] for s in native_snapshots}:
            raise RuntimeError("incremental_published_archive_snapshot_missing")
        retired = []
        for snapshot in native_snapshots:
            tags = snapshot.get("tags", [])
            if (snapshot.get("hostname") != "qt-"+hashlib.sha256(self.identity.encode()).hexdigest()[:32]
                    or len(tags) != 1 or not _NAME.fullmatch(tags[0])):
                raise RuntimeError("incremental_archive_snapshot_not_owned")
            if snapshot["id"] not in snapshots:
                retired.append(snapshot["id"])
        # Bounded batches avoid argument-size dependence on retention history.
        for offset in range(0, len(retired), 64):
            self._run(self._rs("forget", *retired[offset:offset+64]))
        if retired or any(self.root.glob(".copy_*")):
            # Hidden generations survive forget-before-prune crashes, so the next
            # retry still reclaims unreferenced packs even if snapshot IDs vanished.
            self._run(self._rs("prune"))
        for partial in sorted(self.root.glob(".copy_*")):
            self._remove(partial)

    def create(self, session, *, objects, keep_copies, pg_dump=None):
        """Do not query session before native backup end; its fence is already held."""
        if type(keep_copies) is not int or not 1 <= keep_copies <= 30:
            raise ValueError("recovery_keep_copies_invalid")
        if session.in_transaction():
            raise RuntimeError("incremental_requires_unqueried_snapshot_session")
        with self.lock():
            self._prune(keep_copies)
            backups = self._native_backups()
            self._run(self._rs("unlock"))
            self._run(self._rs("snapshots"))
            versions = {
                "pgbackrest": self._run([str(self.pgbackrest), "version"]).decode().strip(),
                "restic": self._run([str(self.restic), "version"]).decode().strip(),
            }
            if (versions["pgbackrest"] != "pgBackRest 2.59.1"
                    or not versions["restic"].startswith("restic 0.19.1 ")):
                raise RuntimeError("incremental_unqualified_tool_version")
            chain = _root_label(backups[-1]["label"]) if backups else None
            chain_size = sum(_root_label(b["label"]) == chain for b in backups)
            kind = "full" if not backups or chain_size >= self.config.max_chain_backups else "incr"
            name = "copy_"+uuid4().hex
            partial = self.root/("."+name)
            partial.mkdir(mode=0o700)
            _json_write(partial/"owner.json", {
                "schema_version": self.schema_version, "database_identity": self.identity,
                "filesystem_uuid": self.target.filesystem_uuid, "name": name,
            })
            _sync(partial)
            _sync(self.root)
            start = monotonic()
            self._run(self._br("--type="+kind, "--annotation=qt-generation="+name, "backup"))
            selected = [b for b in self._native_backups()
                        if b.get("annotation", {}).get("qt-generation") == name]
            if len(selected) != 1:
                raise RuntimeError("incremental_native_backup_selection_invalid")
            database_seconds = monotonic()-start
            # This first snapshot query happens AFTER the physical endpoint.
            # Expiry remained fenced for the entire operation.
            self._assert_fence(session)
            layout = _snapshot_layout(session)
            inventory, count, archive_bytes = self._inventory(session, objects, partial)
            database_seconds = monotonic()-start
            output = self._run(self._rs(
                "backup", "--host", "qt-"+hashlib.sha256(self.identity.encode()).hexdigest()[:32],
                "--tag", name, "--read-concurrency", "1", "--no-scan",
                "--files-from-raw", str(partial/"files.list")))
            summaries = [item for line in output.splitlines()
                         if (item := json.loads(line)).get("message_type") == "summary"]
            if len(summaries) != 1 or not _HASH.fullmatch(summaries[0].get("snapshot_id", "")):
                raise RuntimeError("incremental_archive_snapshot_selection_invalid")
            self._assert_fence(session)
            receipt = {
                "schema_version": self.schema_version, "name": name,
                "database_identity": self.identity, "filesystem_uuid": self.target.filesystem_uuid,
                "storage_layout": layout, "completed_at": datetime.now(UTC).isoformat(),
                "database_label": selected[0]["label"], "database_type": selected[0]["type"],
                "archive_snapshot": summaries[0]["snapshot_id"], "inventory_sha256": inventory,
                "archive_objects": count, "archive_bytes": archive_bytes,
                "inventory_snapshot_path": str(partial/"objects.jsonl"),
                "archive_source_root": str(objects.root),
                "database_seconds": round(database_seconds, 3),
                "elapsed_seconds": round(monotonic()-start, 3),
                "peak_allocated_bytes": self.peak_allocated_bytes,
                "restore_endpoint": "immediate", "tools": versions,
            }
            # Plaintext inventory exists only while preparing the encrypted snapshot.
            (partial/"objects.jsonl").unlink()
            (partial/"files.list").unlink()
            _json_write(partial/"complete.json", receipt)
            _sync(partial)
            self.check()
            partial.rename(self.root/name)
            _sync(self.root)
            self._prune(keep_copies)
            return receipt


class _RepositoryPreparationGuard(LocalRecoveryCopies):
    namespace = EncryptedRecoveryCopies.namespace
    schema_version = EncryptedRecoveryCopies.schema_version


def prepare_encrypted_repository(*, incremental, connection_url, archiver_config, **limits):
    """Explicit operator-only repository initialization; never a runtime fallback.

    Requires already-created independent keys and an identity-attested target.
    Does not create keys, change PostgreSQL settings, restart services, enable
    saved policy or create a recovery point. A failed attempt is resumable only
    with the exact same target/database/key identity.
    """
    guard = _RepositoryPreparationGuard(**limits)
    database_key, archive_key = _keys(incremental, guard.target_root)
    archiver_config = Path(archiver_config)
    if (not archiver_config.is_absolute()
            or archiver_config.parent.resolve(strict=True) != archiver_config.parent
            or archiver_config.parent != incremental.database_key_path.parent
            or archiver_config.parent != incremental.archive_key_path.parent
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", archiver_config.name)):
        raise ValueError("incremental_archiver_config_must_share_private_key_directory")
    secret_directory = archiver_config.parent.stat()
    if (secret_directory.st_uid not in (0, os.getuid()) or secret_directory.st_mode & 0o077):
        raise RuntimeError("incremental_key_directory_not_private")
    for value in (str(incremental.pg_path), str(incremental.pg_socket_path),
                  str(guard.root), connection_url.username, connection_url.database):
        if not value or not re.fullmatch(r"[A-Za-z0-9_./-]+", value):
            raise ValueError("incremental_archiver_configuration_invalid")
    expected = {
        "schema_version": _PREPARED, "database_identity": guard.identity,
        "filesystem_uuid": guard.target.filesystem_uuid,
        "database_key_sha256": hashlib.sha256(database_key.encode()).hexdigest(),
        "archive_key_sha256": hashlib.sha256(archive_key.encode()).hexdigest(),
    }
    with guard.lock():
        marker = guard.root/"prepared.json"
        if marker.exists():
            if json.loads(_private_bytes(marker, limit=16384)) != expected:
                raise RuntimeError("incremental_prepared_repository_identity_mismatch")
        else:
            if {p.name for p in guard.root.iterdir()} != {"writer.lock"}:
                raise RuntimeError("incremental_preparation_requires_empty_owned_directory")
            # Marker binds ownership, not successful initialization/recoverability.
            # Runtime still requires native repository identity and a complete pair.
            _json_write(marker, expected)
            _sync(guard.root)
        for name in ("database", "archives", "locks", "logs"):
            _private_directory(guard.root/name, guard.device, create=True)
        _sync(guard.root)
        config_text = (
            "[global]\n"
            f"repo1-path={guard.root/'database'}\nrepo1-cipher-type=aes-256-cbc\n"
            f"repo1-cipher-pass={database_key}\ncompress-type=zst\n"
            f"lock-path={guard.root/'locks'}\nlog-path={guard.root/'logs'}\n"
            "log-level-file=off\nlog-level-console=warn\narchive-async=n\n"
            f"[qt]\npg1-path={incremental.pg_path}\n"
            f"pg1-socket-path={incremental.pg_socket_path}\n"
            f"pg1-port={connection_url.port or 5432}\npg1-user={connection_url.username}\n"
            f"pg1-database={connection_url.database}\n"
        ).encode()
        if archiver_config.exists():
            if _private_bytes(archiver_config, limit=16384) != config_text:
                raise RuntimeError("incremental_archiver_config_differs")
        else:
            fd = os.open(archiver_config, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as output:
                output.write(config_text)
                output.flush()
                os.fsync(output.fileno())
            _sync(archiver_config.parent)
        remaining = max(1, int(guard.deadline-monotonic()))
        options = {**limits, "timeout_seconds": remaining}
        worker = EncryptedRecoveryCopies(incremental=incremental,
                                         connection_url=connection_url, **options)
        worker.deadline = min(worker.deadline, guard.deadline)
        worker._run(worker._br("stanza-create"))
        worker._native_backups()
        if (guard.root/"archives"/"config").exists():
            worker._run(worker._rs("cat", "config"))
        else:
            worker._run(worker._rs("init"))
        guard.check()
        return {
            "schema_version": "qt.encrypted_recovery_preparation.v1",
            "database_identity": guard.identity, "filesystem_uuid": guard.target.filesystem_uuid,
            "repository_root": str(guard.root),
            "archiver_config_sha256": hashlib.sha256(config_text).hexdigest(),
            "repositories_initialized": True, "backup_created": False, "policy_enabled": False,
        }

"""Materialize the canonical archive catalog of a private paused receiver."""

import os
from pathlib import Path
import re
import stat
from time import monotonic
from types import SimpleNamespace

from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import MAX_RECOVERY_OBJECTS, _snapshot_layout
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.canonical_cold_receiver_replay import _NATIVE_PAUSE
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root
from scripts.automation.verify_canonical_recovery_archive import _path, _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _bound_json, _directory
from scripts.db.canonical_cold_recovery_suffix import prepare_archive_baseline
from scripts.db.canonical_cold_replayed_checkpoint import _lsn, _lsn_text


def _overlap(first, second):
    return first.is_relative_to(second) or second.is_relative_to(first)


def _archive_mounts(source, output, protected):
    """Verify the actual source bind and reject hidden aliases into the output."""
    with Path("/proc/self/mountinfo").open("rb") as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise RuntimeError("canonical_receiver_archive_mount_budget_exceeded")
    mounts = []
    for line in os.fsdecode(data).splitlines():
        fields = line.split(" ")
        root, mount = (Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), field))
                       for field in fields[3:5])
        if (mount != source and mount.is_relative_to(source)) or (mount != output and mount.is_relative_to(output)):
            raise ValueError("canonical_receiver_archive_nested_mount")
        mounts.append((fields[0], fields[2], root, mount, "ro" in fields[5].split(",")))

    def backing(path):
        candidates = [entry for entry in mounts if path.is_relative_to(entry[3])]
        depth = max((len(entry[3].parts) for entry in candidates), default=0)
        active = [entry for entry in candidates if len(entry[3].parts) == depth]
        if len(active) != 1:
            raise ValueError("canonical_receiver_archive_ambiguous_mount")
        mount_id, device, root, mount, readonly = active[0]
        actual = path.stat().st_dev
        if device != f"{os.major(actual)}:{os.minor(actual)}":
            raise ValueError("canonical_receiver_archive_mount_device_changed")
        return mount_id, device, root / path.relative_to(mount), mount, readonly

    source_mount, output_mount, *protected_mounts = (backing(path) for path in (source, output, *protected))
    if source_mount[3] != source or not source_mount[4] or output_mount[4]:
        raise ValueError("canonical_receiver_archive_readonly_source_bind_required")
    for first, second in ((source_mount, output_mount),
                          *((item, other) for item in protected_mounts for other in (source_mount, output_mount))):
        if first[1] == second[1] and _overlap(first[2], second[2]):
            raise ValueError("canonical_receiver_archive_backing_paths_overlap")
    return source_mount, output_mount, *protected_mounts


def _check_materialized_objects(objects, *, device, count, check):
    """All catalog names are verified by the copier. Refuse extra or aliased files."""
    pending = [objects]
    entries = files = 0
    while pending:
        with os.scandir(pending.pop()) as children:
            for child in children:
                check()
                entries += 1
                info = child.stat(follow_symlinks=False)
                if (entries > 64 * count or info.st_dev != device or info.st_uid != os.getuid()
                        or info.st_mode & 0o077):
                    raise RuntimeError("canonical_receiver_archive_materialized_namespace_changed")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(child.path))
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size > 0:
                    files += 1
                    if files > count:
                        raise RuntimeError("canonical_receiver_archive_materialized_namespace_changed")
                else:
                    raise RuntimeError("canonical_receiver_archive_materialized_namespace_changed")
    if files != count:
        raise RuntimeError("canonical_receiver_archive_materialized_namespace_changed")


def bind_replayed_archive_materializer(*, pgdata, pair_receipt, pair_receipt_sha256, base_restore,
                                       source_archive, source_identity, output, max_objects, max_work_bytes,
                                       reserve_bytes, statement_timeout_seconds=5):
    """Return the read-only SQL archive callback used by private replay preparation.

    The source is the explicit read-only bound application object directory.
    Output is a private directory on the receiver workspace filesystem. Retries
    reuse only the same database, pause, source and destination.
    """
    from scripts.ci.materialize_paused_archive_suffix import (
        _CATALOG_INVENTORY, _copy_catalog, _publish_catalog_inventory, _scope_directory,
    )
    from scripts.ci.verify_paused_recovery_pair import _bound_statement

    if (not _sha256(pair_receipt_sha256) or not isinstance(source_identity, tuple) or len(source_identity) != 2
            or any(type(value) is not int or value < 0 for value in source_identity)
            or any(type(value) is not int or not lower <= value <= upper for value, lower, upper in (
                (max_objects, 1, MAX_RECOVERY_OBJECTS), (max_work_bytes, 1, 2**63 - 1),
                (reserve_bytes, 0, 2**63 - 1), (statement_timeout_seconds, 1, 600),
            ))):
        raise ValueError("canonical_receiver_archive_scope_or_limits_invalid")
    pgdata, pgdata_id = _private_root(pgdata)
    output, output_id = _private_root(output)
    base_restore, base_id = _private_root(base_restore)
    source = _directory(source_archive)
    pair_receipt = Path(pair_receipt)
    roots = (pgdata, base_restore, source, output)
    if (output_id[0] != base_id[0] or _identity(source.lstat()) != source_identity
            or any(_overlap(path, other) for index, path in enumerate(roots) for other in roots[index + 1:])
            or any(pair_receipt.is_relative_to(path) for path in roots)
            or pair_receipt.resolve(strict=True) != pair_receipt):
        raise ValueError("canonical_receiver_archive_paths_changed")
    pinned_mounts = _archive_mounts(source, output, (pgdata, base_restore, pair_receipt))
    pinned = dict(zip(roots, (pgdata_id, base_id, source_identity, output_id), strict=True))
    pair = _bound_json(pair_receipt, pair_receipt_sha256)
    if not isinstance(pair, dict) or pair.get("schema_version") != "qt.encrypted_recovery_pair.v1":
        raise ValueError("canonical_receiver_archive_original_pair_invalid")
    baseline_bytes = pair.get("archive_bytes")
    if type(baseline_bytes) is not int or not 0 <= baseline_bytes <= max_work_bytes:
        raise ValueError("canonical_receiver_archive_original_base_budget")
    args = SimpleNamespace(source_archive=source, max_objects=max_objects, max_work_bytes=max_work_bytes,
                           reserve_bytes=reserve_bytes)

    def verify_archive(conn, check_owner):
        if not conn.in_transaction() or not callable(check_owner):
            raise ValueError("canonical_receiver_archive_readonly_transaction_required")
        started = monotonic()
        next_check = started

        def check():
            nonlocal next_check
            remaining = check_owner()
            if remaining <= 0:
                raise TimeoutError("canonical_receiver_archive_deadline")
            now = monotonic()
            if now >= next_check:
                next_check = now + 0.25
                free = os.statvfs(output)
                if (any(_identity(path.lstat()) != identity for path, identity in pinned.items())
                        or not os.statvfs(source).f_flag & os.ST_RDONLY or free.f_flag & os.ST_RDONLY):
                    raise RuntimeError("canonical_receiver_archive_mount_changed")
                if free.f_bavail * free.f_frsize < reserve_bytes:
                    raise RuntimeError("canonical_receiver_archive_reserve_exhausted")
            return remaining

        def bound():
            _bound_statement(conn, check, statement_timeout_seconds)

        def endpoint():
            bound()
            database = conn.engine.url.database
            native = conn.execute(_NATIVE_PAUSE, {"target": database}).mappings().one()
            target, replay = _lsn(native["target_lsn"]), _lsn(native["replay_lsn"])
            if (native["database"] != database or native["database_identity"] != pair["database_identity"]
                    or native["pgdata"] != str(pgdata) or native["read_only"] != "on" or native["isolation"] != "repeatable read"
                    or not all(native[field] is True for field in ("paused", "archive_only", "isolated_socket", "exact_target"))
                    or not 150000 <= native["version"] < 160000 or not 0 < target < replay
                    or native["target_timeline"] != str(native["timeline"])):
                raise RuntimeError("canonical_receiver_archive_native_endpoint_changed")
            layout = _snapshot_layout(conn)
            if layout != pair["storage_layout"]:
                raise RuntimeError("canonical_receiver_archive_original_layout_changed")
            return {"database_identity": native["database_identity"], "timeline": native["timeline"],
                    "target_lsn": _lsn_text(target), "replay_lsn": _lsn_text(replay), "storage_layout": layout,
                    "accepted_commit_seq": PostgresMarketDataRepository._current_commit_seq_with_session(conn),
                    "postmaster_started": native["postmaster_started"]}

        def check_mounts():
            if _archive_mounts(source, output, (pgdata, base_restore, pair_receipt)) != pinned_mounts:
                raise RuntimeError("canonical_receiver_archive_mount_changed")

        before = endpoint()
        with archive_namespace(output, exclusive=True) as held:
            def check_locked():
                held()
                return check()

            check_mounts()
            baseline = prepare_archive_baseline(pair_receipt, base_restore, expected_receipt_sha256=pair_receipt_sha256,
                                                max_objects=max_objects, check=check_locked)
            baseline_source = _path(base_restore, pair["archive_source_root"], absolute=True) if baseline.objects else base_restore
            scope = {"schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": pair_receipt_sha256,
                     **{key: value for key, value in before.items() if key != "postmaster_started"},
                     "pgdata": str(pgdata), "pgdata_inode": pgdata_id[1], "source_archive": str(source),
                     "source_inode": source_identity[1], "source_device_id": pinned_mounts[0][1],
                     "suffix_inode": output_id[1], "device_id": pinned_mounts[1][1], "materialize_baseline": True}
            objects, work = _scope_directory(output, scope)
            allowed = {"scope.json", work.name, objects.name, _CATALOG_INVENTORY}
            if any(path.name not in allowed for path in output.iterdir()):
                raise RuntimeError("canonical_receiver_archive_materialized_namespace_changed")
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
            with os.fdopen(os.open(work / _CATALOG_INVENTORY, flags, 0o600), "wb") as inventory:
                copied = _copy_catalog(conn, args, baseline, objects, work, check_locked, bound, baseline_bytes=baseline_bytes,
                                       baseline_source=baseline_source, catalog_inventory=inventory)
                inventory.flush()
                os.fsync(inventory.fileno())
            _check_materialized_objects(objects, device=output_id[0], count=copied["catalog_objects"], check=check_locked)
            if (not conn.in_transaction() or endpoint() != before
                    or _bound_json(pair_receipt, pair_receipt_sha256) != pair):
                raise RuntimeError("canonical_receiver_archive_endpoint_changed")
            check_mounts()
            inventory_path = _publish_catalog_inventory(output, work, copied["catalog_inventory_sha256"], check_locked)
            check_locked()
        receipt = scope | copied | {"archive_snapshot": pair["archive_snapshot"],
            "baseline_inventory_sha256": pair["inventory_sha256"], "catalog_inventory_path": str(inventory_path),
            "suffix_root": str(objects), "work_logical_bytes": baseline_bytes + copied["suffix_bytes"] + copied["catalog_inventory_bytes"],
            "elapsed_seconds": round(monotonic() - started, 3)}
        return receipt | {"verification_sha256": archive_evidence_hash(receipt)}

    return verify_archive

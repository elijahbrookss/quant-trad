"""Bounded preserving archive pages for the fixed SSD/HDD migration.

Copies immutable catalog-referenced objects without deleting source files or
switching configuration. A page cursor is progress, never inventory readiness:
concurrent publication requires a final fenced reconciliation before cutover.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
import re
import stat
from time import monotonic

from sqlalchemy import text

from core.storage_targets import StorageLocation
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage.header_movement import _MoveWatch
from portal.backend.service.storage.header_resource_claims import _limits
from portal.backend.service.storage.header_resources import observe_header_resources
from portal.backend.service.storage.recovery_copies import _FAMILIES
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db import archive_reference_v2_placement as reference_move
from scripts.db import fact_header_v2_copy as headers, fact_header_v2_placement as physical
from scripts.db.fact_header_v2_capture import SCHEMA, migration_step

logger = logging.getLogger(__name__)
FAMILIES = dict(_FAMILIES)


def _root(path, device):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise RuntimeError("archive_copy_absolute_unsymlinked_root_required")
    info = path.lstat()
    observed = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
    if not stat.S_ISDIR(info.st_mode) or observed != device:
        raise RuntimeError("archive_copy_root_wrong_filesystem")
    return path, (info.st_dev, info.st_ino)


def _path(root, key, device, *, missing=False):
    StorageLocation("fixed-archive", key)
    if len(key) > 2048 or len(Path(key).parts) > 64:
        raise RuntimeError("archive_copy_key_budget_exceeded")
    path = root
    parts = Path(key).parts
    for index, part in enumerate(parts):
        path = path / part
        try:
            info = path.lstat()
        except FileNotFoundError:
            if missing:
                return root / key
            raise
        kind = stat.S_ISREG if index == len(parts)-1 else stat.S_ISDIR
        if info.st_dev != device or not kind(info.st_mode):
            raise RuntimeError("archive_copy_path_not_regular_or_wrong_filesystem")
    return path


def copy_archive_page(engine, *, family, source_root, destination_root, after_id="",
                      page_rows=128, max_page_bytes, policy, resource_limits, cancelled=None):
    """Copy one known catalog page; retry re-verifies and reuses completed files.

    Owns the existing storage lock, expiry fence, clock and capacity watcher.
    The caller advances its cursor only after success. Files published before
    interruption remain valid; SQL rollback does not delete them. No source or
    manifest changes, persistent job, activation or readiness token are created.
    """
    if family not in FAMILIES:
        raise ValueError("archive_copy_known_family_required")
    if type(page_rows) is not int or not 1 <= page_rows <= 256:
        raise ValueError("archive_copy_page_rows_invalid")
    if type(max_page_bytes) is not int or max_page_bytes <= 0:
        raise ValueError("archive_copy_byte_budget_invalid")
    if not isinstance(after_id, str) or len(after_id) > 128:
        raise ValueError("archive_copy_cursor_invalid")
    if cancelled is not None and not callable(cancelled):
        raise ValueError("archive_copy_cancellation_callback_invalid")
    limits = _limits(resource_limits)
    started = monotonic()
    deadline = started + limits["movement_timeout_seconds"]
    watch = None
    with engine.connect() as conn:
        try:
            with conn.begin():
                previous = conn.scalar(text(
                    "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, started + previous/1000)
                with migration_step(conn, limits["movement_timeout_seconds"]):
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                                            "hashtextextended('qt.storage.management.v1',0))")):
                        raise RuntimeError("archive_copy_storage_busy")
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock_shared("
                                            "hashtextextended(:name,0))"),
                                       {"name": _LIFECYCLE_LOCK_NAME}):
                        raise RuntimeError("archive_copy_expiry_busy")
                    conn.exec_driver_sql(f"LOCK TABLE {headers.SOURCE} IN ACCESS SHARE MODE NOWAIT")
                    state = headers._inspect_progress(conn)
                    saved = state["placement"]
                    if saved is None:
                        raise RuntimeError("archive_copy_fixed_placement_required")
                    plan = physical._restore(saved["plan"])
                    targets = (plan.recent, plan.history)
                    reference_move._fixed_inputs(policy, limits, targets)
                    if policy.archives != (plan.history.target_id,):
                        raise ValueError("archive_copy_history_archive_policy_required")
                    source, source_identity = _root(source_root, saved["recent_device"])
                    destination, destination_identity = _root(destination_root, saved["history_device"])
                    if not destination.is_relative_to(Path(plan.history.root)):
                        raise RuntimeError("archive_copy_destination_outside_history_target")
                    seconds = conn.scalar(text(f"""
                        SELECT EXTRACT(EPOCH FROM prepared_at+interval '24 hours'-clock_timestamp())
                        FROM {SCHEMA}.capture WHERE id=1
                    """))
                    deadline = min(deadline, monotonic()+float(seconds))
                    kind = FAMILIES[family]
                    predicate = "" if kind is None else """
                        AND NOT EXISTS (
                            SELECT 1 FROM market.storage_lifecycle_events e
                            WHERE e.action='archive_expire' AND e.event_type='completed'
                              AND e.target_kind=:kind AND e.target_id=m.id)
                    """
                    rows = conn.execute(text(f"""
                        SELECT m.id,m.object_key,m.object_sha256,m.byte_count
                        FROM market.{family} m WHERE m.id>:after {predicate}
                        ORDER BY m.id LIMIT :limit
                    """), {"after": after_id, "kind": kind, "limit": page_rows}).mappings().all()
                    for row in rows:
                        if (not re.fullmatch(r"[0-9a-f]{64}", row["object_sha256"])
                                or type(row["byte_count"]) is not int or row["byte_count"] <= 0):
                            raise RuntimeError("archive_copy_descriptor_invalid")
                    byte_count = sum(row["byte_count"] for row in rows)
                    if byte_count > max_page_bytes:
                        raise RuntimeError("archive_copy_page_byte_budget_exceeded")
                    resources = observe_header_resources(conn, targets,
                        pg_controldata=plan.pg_controldata,
                        timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                    budget, floors = reference_move._budget(conn,
                        observed={"bytes": byte_count, "_binding": saved}, policy=policy,
                        limits=limits, targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                    watch.start()
                    objects = FilesystemRawArchiveObjectStore(destination)
                    copied = reused = 0
                    for row in rows:
                        key = row["object_key"]

                        def check():
                            watch.check()
                            if (_root(source, saved["recent_device"])[1] != source_identity
                                    or _root(destination, saved["history_device"])[1] != destination_identity):
                                raise RuntimeError("archive_copy_root_changed")
                            _path(source, key, source_identity[0])
                            _path(destination, key, destination_identity[0], missing=True)

                        check()
                        incoming = _path(source, key, source_identity[0])
                        before = incoming.stat()
                        if before.st_size != row["byte_count"]:
                            raise RuntimeError("archive_copy_source_size_mismatch")
                        acknowledgement = objects.put_verified(object_key=key, source_path=incoming,
                            expected_sha256=row["object_sha256"], check_budget=check)
                        after = incoming.stat()
                        stable = lambda value: (value.st_dev, value.st_ino, value.st_size,
                                                value.st_mtime_ns, value.st_ctime_ns)
                        if (stable(before) != stable(after)
                                or acknowledgement.byte_count != row["byte_count"]):
                            raise RuntimeError("archive_copy_source_changed")
                        reused += int(acknowledgement.reused_existing)
                        copied += int(not acknowledgement.reused_existing)
                    watch.check()
                    report = {"family": family, "next_after_id": rows[-1]["id"] if rows else after_id,
                        "page_objects": len(rows), "copied_objects": copied, "reused_objects": reused,
                        "verified_bytes": byte_count, "resource_budget": budget,
                        "source_preserved": True, "migration_ready": False,
                        "final_fenced_inventory_verification_required": True}
                # Keep watcher and filesystem claims until this transaction ends.
            watch.check()
            logger.info("archive_migration_page_verified | family=%s copied=%s reused=%s duration_seconds=%s",
                        family, copied, reused, monotonic()-started)
            return report
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(conn)

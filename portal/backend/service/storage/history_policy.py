"""Bind canonical payload archival to the existing saved history policy."""
from dataclasses import replace
from pathlib import Path

from core.storage_targets import StoragePolicy
from portal.backend.db.storage_target_models import StoragePolicyRecord
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import lock_header_storage, registered_header_targets


def _read(session):
    lock_header_storage(session)
    row = session.get(StoragePolicyRecord, 1, populate_existing=True)
    if row is None or row.policy is None:
        return None, None
    saved = StoragePolicy.from_dict(row.policy)
    return saved, {"policy_revision": row.revision, "policy_hash": saved.fingerprint}


def _archive_policy(session, saved, base, storage_root):
    records = registered_header_targets(session)
    targets = tuple(_target(record) for record in records)
    saved.validate_targets(targets)
    by_id = {target.target_id: target for target in targets}
    if (len(saved.archives) != 1 or saved.archives[0] not in saved.history
            or by_id[saved.recent[0]].medium != "ssd"
            or any(by_id[key].medium != "hdd" for key in saved.history)):
        raise StorageConflict("history_policy_requires_recent_ssd_and_history_hdd")
    target = by_id[saved.archives[0]]
    evidence = target.inspect(require_writable=True)
    root = Path(storage_root).resolve(strict=True)
    target_root = Path(evidence.path).resolve(strict=True)
    if (not root.is_relative_to(target_root)
            or root.stat().st_dev != target_root.stat().st_dev):
        raise StorageConflict("history_archive_root_outside_saved_target")
    record = next(record for record in records if record.id == target.target_id)
    reserve = ((evidence.total_bytes * saved.reserve_percent + 99) // 100
               + record.reserved_bytes + record.auxiliary_reserved_bytes)
    return replace(base, hot_days=saved.recent_days, hot_days_by_fact_type={},
                   archive_min_free_bytes=max(base.archive_min_free_bytes, reserve))


def saved_canonical_policy(database, *, policy, storage_root):
    """Observe the saved window without enabling a disabled deployment gate."""
    with database.session() as session:
        saved, witness = _read(session)
        if saved is None:
            return replace(policy, execution_enabled=False), None
        effective = replace(policy, hot_days=saved.recent_days, hot_days_by_fact_type={},
                            execution_enabled=policy.execution_enabled and saved.movement_enabled)
        if saved.movement_enabled:
            effective = _archive_policy(session, saved, effective, storage_root)
        return effective, witness


def require_saved_history_policy(session, *, witness, policy, storage_root):
    """Fence each archive/reclaim transaction against changed or paused policy."""
    saved, current = _read(session)
    if saved is None or not saved.movement_enabled or current != witness:
        raise StorageConflict("history_policy_changed: replan before archival")
    return _archive_policy(session, saved, policy, storage_root)


def configured_history_read_cache():
    """Compose an optional copy cache using the same policy, targets and claims."""
    from core.settings import get_settings
    from core.storage_mounts import configured_working_root, require_configured_working_mount, StorageMountError
    from market_data.history_read_cache import CanonicalArchiveReadCache, CacheBypass, HistoryCacheLimits
    import os

    settings = get_settings().storage
    if not settings.history_cache_bytes:
        return None
    if not os.environ.get("MARKET_STRUCTURE_WORKING_ROOT", "").strip():
        raise RuntimeError("history_cache_requires_explicit_working_root")
    root = configured_working_root() / "history-read-cache"

    def check_mount(path):
        try:
            evidence = require_configured_working_mount(path)
        except StorageMountError as error:
            raise CacheBypass(str(error)) from error
        if evidence is None:
            raise CacheBypass("working_filesystem_identity_required")
        return evidence

    from contextlib import contextmanager
    from core.execution_control import execution_checkpoint
    from sqlalchemy import text

    @contextmanager
    def capacity_scope():
        # A fresh, separate READ COMMITTED transaction avoids stale reservations
        # in the research selector's repeatable snapshot. No second DSN or row.
        from portal.backend.db.session import db
        with db.session() as owner:
            if owner.connection().get_isolation_level() != "READ COMMITTED":
                raise CacheBypass("admission_requires_read_committed")
            owner.execute(text("SET LOCAL statement_timeout='1000ms'"))
            try:
                saved, witness = _read(owner)
            except StorageConflict as error:
                raise CacheBypass(str(error)) from error
            if saved is None:
                raise CacheBypass("saved_policy_required")
            records = registered_header_targets(owner)
            targets = tuple(_target(record) for record in records)
            saved.validate_targets(targets)
            by_id = {target.target_id: target for target in targets}
            target = by_id[saved.recent[0]]
            evidence = target.inspect(require_writable=True)
            working = check_mount(root)
            if (target.medium != "ssd" or evidence.filesystem_uuid != working.filesystem_uuid
                    or evidence.device_id != working.device_id):
                raise CacheBypass("cache_must_use_saved_recent_ssd")
            if root.stat().st_dev != Path(evidence.path).stat().st_dev:
                raise CacheBypass("cache_filesystem_changed")
            record = next(row for row in records if row.id == target.target_id)
            floor = max(settings.history_cache_min_free_bytes,
                        (evidence.total_bytes * saved.reserve_percent + 99) // 100
                        + record.reserved_bytes + record.auxiliary_reserved_bytes)

            def capacity(additional):
                execution_checkpoint()
                # A failed connection/transaction loses the exclusion. Stop the
                # fill before it can compete with a newly admitted maintenance job.
                if not owner.scalar(text("""
                    WITH key AS (SELECT hashtextextended('qt.storage.management.v1',0) AS value)
                    SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
                      AND pid=pg_backend_pid() AND mode='ExclusiveLock' AND granted AND objsubid=1
                      AND classid::bigint=((key.value>>32)&4294967295)
                      AND objid::bigint=(key.value&4294967295))
                """)):
                    raise RuntimeError("history_cache_admission_owner_lost")
                current = check_mount(root)
                if current.filesystem_uuid != evidence.filesystem_uuid:
                    raise RuntimeError("history_cache_admission_filesystem_changed")
                available = current.available_bytes - floor
                if additional and additional > available:
                    raise CacheBypass("collection_and_maintenance_headroom")
                return available

            yield capacity

    return CanonicalArchiveReadCache(root, limits=HistoryCacheLimits(settings.history_cache_bytes),
                                     capacity_scope=capacity_scope, check_mount=check_mount)

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

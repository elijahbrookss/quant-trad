"""One due local recovery operation under existing storage-management ownership.

No additional scheduler, placement policy or automatic activation. The existing
maintenance supervisor can invoke this after its retention transaction ends.
"""
from datetime import datetime, timedelta
import logging
from pathlib import Path
from time import monotonic

from sqlalchemy import text

from core.storage_targets import StoragePolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db.storage_target_models import StoragePolicyRecord
from portal.backend.db.session import DatabaseSnapshotBusyError
from portal.backend.service.storage_management import _target
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from .header_admission import registered_header_targets
from .header_resources import observe_header_resources
from .recovery_copies import LocalRecoveryCopies, _identity, _snapshot_layout

logger = logging.getLogger(__name__)


def validate_recovery_maintenance_limits(*, max_bytes, timeout_seconds, headroom_bytes,
                                        max_objects=1000000):
    """Validate operating budgets before configuration or execution."""
    if (type(max_bytes) is not int or not 1<=max_bytes<=2**63-1
            or type(timeout_seconds) is not int or not 1<=timeout_seconds<=86400
            or type(max_objects) is not int or not 1<=max_objects<=1000000
            or not isinstance(headroom_bytes,dict) or not 1<=len(headroom_bytes)<=32
            or any(not isinstance(key,str) or not 1<=len(key)<=48
                   or type(value) is not int or not 0<=value<=2**63-1
                   for key,value in headroom_bytes.items())):
        raise ValueError("recovery_maintenance_limits_invalid")


def run_due_local_recovery(database, *, storage_root, pg_dump, pg_controldata,
                           max_bytes, timeout_seconds, headroom_bytes,
                           max_objects=1000000, cancelled=None):
    """Use the saved backup interval/count and explicit measured capacity limits.

    Holds the existing management lock for the copy, excluding movement and new
    reservations. Existing reservations plus caller-declared growth/allocation
    overhead remain unavailable. Collection continues; actual free space on both
    drives is rechecked during copying. No limits are inferred from toy fixtures.
    """
    validate_recovery_maintenance_limits(max_bytes=max_bytes, timeout_seconds=timeout_seconds,
        headroom_bytes=headroom_bytes, max_objects=max_objects)
    deadline=monotonic()+timeout_seconds
    with database.session() as owner:
        if owner.connection().get_isolation_level()!="READ COMMITTED":
            raise RuntimeError("recovery_maintenance_requires_read_committed")
        original_timeout=owner.scalar(text("SHOW statement_timeout"))
        original_ms=owner.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
        if not owner.scalar(text(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"
        )):
            return {"state":"busy","reason":"storage_operation_running"}
        config=owner.get(StoragePolicyRecord,1)
        if config is None or config.policy is None:
            return {"state":"unconfigured"}
        policy=StoragePolicy.from_dict(config.policy)
        if not policy.backup_enabled:
            return {"state":"disabled"}
        records=registered_header_targets(owner)
        targets=[_target(record) for record in records]
        policy.validate_targets(targets)
        by_id={target.target_id:target for target in targets}
        if len(policy.backups)!=1:
            raise RuntimeError("recovery_requires_one_backup_target")
        recent=by_id[policy.recent[0]]
        history=by_id[policy.backups[0]]
        if (recent.medium!="ssd" or history.medium!="hdd"
                or history.target_id not in policy.history
                or set(headroom_bytes)!={recent.target_id,history.target_id}):
            raise RuntimeError("recovery_requires_fixed_recent_and_history_budget")
        identity,_=_identity(owner)
        observed={target.target_id:target.inspect(require_writable=True)
                  for target in (recent,history)}
        if observed[recent.target_id].device_id==observed[history.target_id].device_id:
            raise RuntimeError("recovery_requires_distinct_filesystems")
        registered={record.id:record for record in records}
        floors={key:((value.total_bytes*policy.reserve_percent+99)//100
                     +registered[key].reserved_bytes+registered[key].auxiliary_reserved_bytes
                     +headroom_bytes[key]) for key,value in observed.items()}

        def resources():
            if cancelled is not None and cancelled():
                raise RuntimeError("recovery_cancelled")
            remaining=int((deadline-monotonic())*1000)
            if remaining<=0:
                raise RuntimeError("recovery_time_budget_exceeded")
            # A lost owner connection/transaction must stop the copy before a
            # second worker can use its space. This query also bounds lock loss.
            owner.execute(text("SELECT set_config('statement_timeout',:limit,true)"),
                          {"limit":str(min(remaining,1000,original_ms or 1000))})
            if not owner.scalar(text("""
                WITH key AS (SELECT hashtextextended('qt.storage.management.v1',0) AS value)
                SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
                  AND pid=pg_backend_pid() AND mode='ExclusiveLock' AND granted AND objsubid=1
                  AND classid::bigint=((key.value>>32)&4294967295)
                  AND objid::bigint=(key.value&4294967295))
            """)):
                raise RuntimeError("recovery_storage_ownership_lost")
            owner.execute(text("SELECT set_config('statement_timeout',:limit,true)"),
                          {"limit":original_timeout})
            for target in (recent,history):
                current=target.inspect(require_writable=True)
                original=observed[target.target_id]
                if (current.device_id!=original.device_id or current.path!=original.path
                        or current.available_bytes<floors[target.target_id]):
                    raise RuntimeError("recovery_capacity_or_filesystem_changed")

        copies=LocalRecoveryCopies(target=history,database_identity=identity,
            max_bytes=max_bytes,reserve_bytes=floors[history.target_id],
            timeout_seconds=max(1,int(deadline-monotonic())),max_objects=max_objects,
            cancelled=cancelled,check_resources=resources)
        with copies.lock():
            completed=copies.completed()
        current_layout=_snapshot_layout(owner)
        now=owner.scalar(text("SELECT clock_timestamp()"))
        if completed:
            last=completed[-1][0]
            if last>now:
                raise RuntimeError("recovery_completion_clock_ahead")
            next_due=last+timedelta(hours=policy.backup_interval_hours)
            if now<next_due and completed[-1][2].get("storage_layout")==current_layout:
                return {"state":"not_due","policy_revision":config.revision,
                        "generation":completed[-1][2]["name"],"storage_layout":current_layout,
                        "policy_hash":policy.fingerprint,"last_completed_at":last.isoformat(),
                        "next_due_at":next_due.isoformat(),"completed_copies":len(completed)}
        resources()
        if history.inspect(require_writable=True).available_bytes<floors[history.target_id]+max_bytes:
            return {"state":"blocked","reason":"recovery_creation_capacity"}
        # Reuse the serving-PostgreSQL namespace and WAL/temp verifier.
        proof=observe_header_resources(owner.connection(),(recent,history),
            pg_controldata=Path(pg_controldata),timeout_seconds=min(30,max(1,int(deadline-monotonic()))))
        if (proof.database_identity!=identity or any(binding["target_id"]!=recent.target_id
                for binding in proof.bindings if binding["role"] in ("database","database_default","wal"))):
            raise RuntimeError("recovery_recent_database_binding_mismatch")
        objects=FilesystemRawArchiveObjectStore(Path(storage_root)/"objects",writable=False)
        archive_root=objects.root.resolve(strict=True)
        archive_targets=[by_id[key] for key in policy.archives]
        if not any(archive_root.is_relative_to(Path(target.root).resolve(strict=True))
                   and archive_root.stat().st_dev==Path(target.root).stat().st_dev
                   for target in archive_targets):
            raise RuntimeError("recovery_archive_root_outside_registered_targets")
        try:
            with database.locked_snapshot_session(shared_lock_name=_LIFECYCLE_LOCK_NAME,
                                                  wait_for_lock=False) as snapshot:
                receipt=copies.create(snapshot,objects=objects,pg_dump=Path(pg_dump),
                                      keep_copies=policy.backup_copies)
        except DatabaseSnapshotBusyError:
            return {"state":"busy","reason":"archive_lifecycle_running"}
        resources()
        if receipt["storage_layout"]!=current_layout:
            raise RuntimeError("recovery_storage_layout_changed_during_copy")
        logger.info("local_recovery_maintenance_completed | generation=%s policy_revision=%s",
                    receipt["name"],config.revision)
        return {"state":"completed","last_completed_at":receipt["completed_at"],
                "generation":receipt["name"],"policy_revision":config.revision,
                "storage_layout":receipt["storage_layout"],
                "policy_hash":policy.fingerprint,
                "next_due_at":(datetime.fromisoformat(receipt["completed_at"])
                               +timedelta(hours=policy.backup_interval_hours)).isoformat()}

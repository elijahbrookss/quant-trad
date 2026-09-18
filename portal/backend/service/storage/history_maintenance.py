"""One automatic historical group using the saved policy and existing journal.

No scheduler, tablespace creation, policy activation or rebalancing. Prepared
destinations and measured resource limits are required from deployment.
"""
import logging
from uuid import uuid4

from sqlalchemy import select, text

from core.storage_targets import StoragePolicy
from portal.backend.db.storage_target_models import (
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
    StoragePlanRecord, StoragePolicyRecord,
)
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import lock_header_storage, registered_header_targets
from .header_catalog import read_header_catalog
from .header_destinations import review_header_moves
from .header_filesystem import verify_header_filesystem
from .header_journal import reserve_header_batch, cancel_unstarted_header_batch, _rows
from .header_movement import execute_reserved_header_move
from .header_resource_claims import reserve_header_resources, _limits, _hash

logger = logging.getLogger(__name__)
_OPERATION = "qt.automatic_header_history.v1"
_ACTIVE = ("queued","running","blocked")


def _cancelled(callback):
    if callback is not None and callback():
        raise RuntimeError("storage_move_cancelled")


def _owned_move(session,plan):
    batch=session.get(StorageHeaderBatchRecord,plan.id,populate_existing=True)
    if (batch is None or batch.group_count!=1 or batch.base_revision!=plan.base_revision
            or batch.policy_hash!=plan.policy_hash or batch.cancelled_at is not None):
        raise StorageConflict("history_maintenance_batch_changed")
    moves=_rows(session,batch)
    if len(moves)!=1 or moves[0].state not in ("reserved","completed"):
        raise StorageConflict("history_maintenance_reconciliation_required")
    return batch,moves[0]


def _finish(session,plan,batch,move):
    if move.state!="completed":
        raise StorageConflict("history_maintenance_completion_unconfirmed")
    plan.state="completed"
    plan.updated_at=move.updated_at
    plan.progress={"operation":_OPERATION,"state":"completed",
                   "move_id":move.id,"review_hash":batch.review_hash,
                   "storage_day":move.storage_day.isoformat(),
                   "finished_at":move.updated_at.isoformat()}
    session.flush()
    return {"state":"completed","plan_id":plan.id,"move_id":move.id,
            "storage_day":move.storage_day.isoformat(),"finished_at":move.updated_at.isoformat()}


def _prepare(database,*,pg_controldata,limits,cancelled):
    with database.session() as session:
        lock_header_storage(session)
        _cancelled(cancelled)
        config=session.get(StoragePolicyRecord,1,populate_existing=True)
        policy=StoragePolicy.from_dict(config.policy) if config is not None and config.policy is not None else None
        records=registered_header_targets(session)
        targets=tuple(_target(record) for record in records)
        if policy is not None and policy.movement_enabled:
            policy.validate_targets(targets)
            by_id={target.target_id:target for target in targets}
            if (by_id[policy.recent[0]].medium!="ssd"
                    or any(by_id[key].medium!="hdd" for key in policy.history)):
                return {"state":"blocked","reason":"history_requires_recent_ssd_and_history_hdd"}
        active=list(session.scalars(select(StoragePlanRecord).where(
            StoragePlanRecord.state.in_(_ACTIVE)).limit(2).execution_options(populate_existing=True)))
        if len(active)>1:
            raise StorageConflict("history_maintenance_multiple_active_plans")
        if active:
            plan=active[0]
            if plan.progress.get("operation")!=_OPERATION:
                return {"state":"busy","reason":"storage_change_in_progress"}
            batch,move=_owned_move(session,plan)
            if move.state=="completed":
                # Stage completion is already atomic. Re-observe its files
                # through the same execution boundary before finalizing the plan.
                return {"plan_id":plan.id,"move_id":move.id,"review_hash":batch.review_hash}
            recorded_limits=move.resource_claim.get("limits_hash") if move.resource_claim else None
            changed=(policy is None or not policy.movement_enabled
                     or config.revision!=plan.base_revision or policy.fingerprint!=plan.policy_hash
                     or recorded_limits!=_hash(limits)
                     or set(limits["temporary_bytes"])!={record.id for record in records})
            if changed:
                # One-group batches are wholly unstarted or completed; there
                # is no partly completed cancellation/rebalancing workflow.
                cancel_unstarted_header_batch(session,plan_id=plan.id,review_hash=batch.review_hash)
                plan.state="cancelled"
                plan.updated_at=session.scalar(text("SELECT clock_timestamp()"))
                plan.progress={"operation":_OPERATION,"state":"cancelled",
                               "reason":"policy_or_resource_configuration_changed"}
                session.flush()
                return {"state":"cancelled","plan_id":plan.id,
                        "reason":"policy_or_resource_configuration_changed"}
            plan.state="running"
            plan.progress={**plan.progress,"state":"running","error":None}
            session.flush()
            return {"plan_id":plan.id,"move_id":move.id,"review_hash":batch.review_hash}

        if policy is None:
            return {"state":"unconfigured"}
        if not policy.movement_enabled:
            return {"state":"disabled"}
        if set(limits["temporary_bytes"])!={target.target_id for target in targets}:
            raise StorageConflict("history_maintenance_complete_resource_limits_required")
        identity=session.scalar(text("""
            SELECT c.system_identifier::text||'/'||d.oid::text
            FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()
        """))
        registrations=list(session.scalars(select(StorageHeaderTablespaceRecord).where(
            StorageHeaderTablespaceRecord.database_identity==identity).limit(33)))
        destinations={row.target_id:row.tablespace_oid for row in registrations
                      if row.target_id in policy.history}
        if len(registrations)>32 or set(destinations)!=set(policy.history):
            return {"state":"blocked","reason":"history_tablespace_unconfigured"}
        catalog=read_header_catalog(database._engine,
            destination_tablespace_oids=tuple(destinations.values()))
        _cancelled(cancelled)
        verified=verify_header_filesystem(catalog,targets,pg_controldata=pg_controldata,
                                          destination_assignments=destinations)
        review=review_header_moves(verified=verified,policy=policy,targets=targets,
            reserved_bytes={row.id:row.reserved_bytes+row.auxiliary_reserved_bytes for row in records},
            max_moves=1)
        if not review["planning_complete"]:
            return {"state":"blocked","reason":"history_placement_not_admitted",
                    "blockers":review["blockers"]}
        if not review["moves"]:
            return {"state":"idle"}
        _cancelled(cancelled)
        now=session.scalar(text("SELECT clock_timestamp()"))
        identity="history_auto_"+uuid4().hex
        plan=StoragePlanRecord(id=identity,request_id=identity,base_revision=config.revision,
            policy=policy.to_dict(),policy_hash=policy.fingerprint,state="running",
            impact={"operation":_OPERATION,"max_moves":1,
                    "deferred_storage_days":review["deferred_storage_days"]},
            progress={"operation":_OPERATION,"state":"running"},
            created_at=now,updated_at=now)
        session.add(plan)
        session.flush()
        reserved=reserve_header_batch(session,plan_id=plan.id,review_hash=review["plan_hash"],
                                       verified=verified,max_moves=1)
        if len(reserved["moves"])!=1:
            raise StorageConflict("history_maintenance_single_group_required")
        move_id=reserved["moves"][0]["id"]
        reserve_header_resources(session,move_id=move_id,review_hash=review["plan_hash"],
                                 pg_controldata=pg_controldata,**limits)
        _cancelled(cancelled)
        plan.progress={**plan.progress,"move_id":move_id,"review_hash":review["plan_hash"]}
        session.flush()
        return {"plan_id":plan.id,"move_id":move_id,"review_hash":review["plan_hash"]}


def _failure(database,intent,error):
    with database.session() as session:
        lock_header_storage(session)
        plan=session.get(StoragePlanRecord,intent["plan_id"],populate_existing=True)
        if plan is None or plan.progress.get("operation")!=_OPERATION:
            raise StorageConflict("history_maintenance_plan_changed")
        batch,move=_owned_move(session,plan)
        if batch.review_hash!=intent["review_hash"] or move.id!=intent["move_id"]:
            raise StorageConflict("history_maintenance_intent_changed")
        if move.state=="completed":
            # A lost commit response must not overwrite durable success with
            # failure. The next pass will reconcile physical completion.
            return
        plan.state="blocked"
        plan.updated_at=session.scalar(text("SELECT clock_timestamp()"))
        plan.progress={**plan.progress,"state":"blocked","error":error[:1024],
                       "checked_at":plan.updated_at.isoformat()}
        session.flush()


def run_history_maintenance(database,*,pg_controldata,resource_limits,cancelled=None):
    """Resume existing work or move at most one eligible day under saved policy."""
    if cancelled is not None and not callable(cancelled):
        raise ValueError("history_maintenance_cancellation_callback_invalid")
    limits=_limits(resource_limits)
    intent=None
    try:
        intent=_prepare(database,pg_controldata=pg_controldata,limits=limits,cancelled=cancelled)
        if "state" in intent:
            return intent
        _cancelled(cancelled)
        receipt=execute_reserved_header_move(database,move_id=intent["move_id"],
            review_hash=intent["review_hash"],pg_controldata=pg_controldata,cancelled=cancelled)
        with database.session() as session:
            lock_header_storage(session)
            plan=session.get(StoragePlanRecord,intent["plan_id"],populate_existing=True)
            if plan is None or plan.progress.get("operation")!=_OPERATION:
                raise StorageConflict("history_maintenance_plan_changed")
            batch,move=_owned_move(session,plan)
            if batch.review_hash!=intent["review_hash"] or move.id!=intent["move_id"]:
                raise StorageConflict("history_maintenance_intent_changed")
            result=_finish(session,plan,batch,move)
        logger.info("storage_history_maintenance_completed | plan_id=%s move_id=%s",
                    intent["plan_id"],intent["move_id"])
        return {**result,"reused":receipt["reused"]}
    except Exception as exc:
        if isinstance(exc,StorageConflict) and str(exc)=="storage_journal_busy":
            return {"state":"busy","reason":"storage_operation_running"}
        if intent is not None and "move_id" in intent:
            try:
                _failure(database,intent,f"{type(exc).__name__}: {exc}")
            except Exception:
                # Preserve the original failure; reporting ownership may itself
                # be busy. This is logged, never described as a successful move.
                logger.exception("storage_history_failure_record_unavailable")
        raise

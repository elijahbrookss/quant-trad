"""Read existing worker heartbeats without confusing liveness with storage health."""
from datetime import datetime, timedelta

from core.storage_targets import StoragePolicy


def _time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _result(state, reason=None, **values):
    return {"state": state, "last_completed_at": None,
            **({"reason": reason} if reason else {}), **values}


def phase_status(workers, *, phase, policy, revision, now):
    """Project one phase; never combine freshness and success from different workers."""
    if policy is None:
        return _result("unconfigured")
    enabled = policy.movement_enabled if phase == "history_movement" else policy.backup_enabled
    if not enabled:
        return _result("disabled")
    if len(workers) > 100:
        return _result("unknown", "maintenance_worker_inventory_limit")
    candidates = []
    for worker in workers:
        context = worker.get("context")
        lifecycle = context.get("storage_lifecycle") if isinstance(context, dict) else None
        maintenance = lifecycle.get("maintenance") if isinstance(lifecycle, dict) else None
        report = maintenance.get(phase) if isinstance(maintenance, dict) else None
        if isinstance(report, dict) and report.get("configured") is True:
            candidates.append((worker, lifecycle, report))
    live = [(worker, lifecycle, report) for worker, lifecycle, report in candidates
            if worker["expires_at"] > now and worker["heartbeat_at"] <= now
            and worker["state"] not in {"stopping", "stopped"}]
    if not live:
        return _result("stale" if candidates else "unavailable",
                       "maintenance_worker_expired" if candidates else "maintenance_worker_not_configured")
    if len(live) != 1:
        return _result("unknown", "multiple_maintenance_workers")
    worker, lifecycle, report = live[0]
    evidence = {"worker_id": worker["worker_id"], "heartbeat_at": worker["heartbeat_at"].isoformat(),
                "lifecycle_state": lifecycle.get("state")}
    if lifecycle.get("state") not in {"running", "degraded"}:
        return _result("unavailable", "maintenance_loop_not_running", **evidence)
    if report.get("state") == "running":
        started = _time(report.get("started_at"))
        if started is None or started > now:
            return _result("unknown", "maintenance_clock_invalid", **evidence)
        return _result("running", started_at=started.isoformat(), **evidence)
    checked = _time(report.get("checked_at"))
    loop_policy = lifecycle.get("policy")
    cadence = loop_policy.get("interval_seconds") if isinstance(loop_policy, dict) else None
    if checked is None or checked > now or type(cadence) is not int or cadence < 1:
        return _result("unknown", "maintenance_observation_unavailable", **evidence)
    ttl = worker["expires_at"] - worker["heartbeat_at"]
    if ttl <= timedelta(0) or now > checked + timedelta(seconds=cadence) + ttl:
        return _result("stale", "maintenance_observation_expired", checked_at=checked.isoformat(), **evidence)
    outcome = report.get("outcome")
    if not isinstance(outcome, dict) or outcome.get("state") != report.get("state"):
        return _result("unknown", "maintenance_observation_invalid", **evidence)
    state = outcome["state"]
    if state in {"failed", "blocked", "busy", "cancelled"}:
        return _result(state, outcome.get("reason") or outcome.get("error"),
                       checked_at=checked.isoformat(), **evidence)
    allowed = {"idle", "completed"} if phase == "history_movement" else {"not_due", "completed"}
    if state not in allowed:
        return _result("unknown", "maintenance_policy_not_confirmed", **evidence)
    if outcome.get("policy_revision") != revision or outcome.get("policy_hash") != policy.fingerprint:
        return _result("unknown", "maintenance_policy_changed", **evidence)
    last = _time(outcome.get("last_completed_at") or outcome.get("finished_at"))
    if (state in {"completed", "not_due"} and last is None) or (last is not None and last > checked):
        return _result("unknown", "maintenance_completion_invalid", **evidence)
    values = {"checked_at": checked.isoformat(),
              "last_completed_at": last.isoformat() if last is not None else None, **evidence}
    if phase == "local_recovery":
        due = _time(outcome.get("next_due_at"))
        if due is None or due <= last:
            return _result("unknown", "maintenance_due_time_invalid", **evidence)
        values["next_due_at"] = due.isoformat()
        if now >= due:
            state = "overdue" if now > due + timedelta(seconds=cadence) + ttl else "due"
    return _result(state, **values)


def maintenance_status(workers, *, policy, revision, now):
    configured = StoragePolicy.from_dict(policy) if policy is not None else None
    return {
        key: phase_status(workers, phase=phase, policy=configured, revision=revision, now=now)
        for key, phase in (("movement", "history_movement"), ("backup", "local_recovery"))
    }

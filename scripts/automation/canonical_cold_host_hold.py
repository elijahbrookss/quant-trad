"""Journal the host-side writer pause for one approved cold cutover."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from math import isfinite
import os
from pathlib import Path
import re
from time import monotonic, time

from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_host_inventory import assert_host_inventory
from scripts.automation.canonical_cold_package import load_approved_package, read_approved_runtime_model
from scripts.automation.canonical_cold_runtime import _record_serving_release, prepare_runtime, restart_runtime


HOLD = "canonical-cold-hold.json"
COMPLETION = "canonical-cold-completion.json"
VERSION = "qt.canonical_cold_host_hold.v1"

# Shut entry points and storage maintenance down before gracefully draining
# the raw receiver. The completed initializer is inventoried but not rerun.
PAUSE_ORDER = ("frontend", "frontend-v2", "pgadmin", "grafana", "docker-stats", "backend",
               "storage-maintenance", "market-data-collector")
_DATABASE_PREVIOUS = {
    "database_unchanged": {"writers_stopped"},
    "login_closing": {"writers_stopped"}, "jobs_stopping": {"login_closing"},
    "database_fenced": {"jobs_stopping"}, "activation_committing": {"database_fenced"},
    "activation_verified": {"activation_committing"},
    "activation_rolled_back": {"database_fenced", "activation_committing"},
    "login_opening": {"activation_verified", "activation_rolled_back"},
    "jobs_restarting": {"login_opening"}, "database_reopened": {"jobs_restarting"},
}


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


@contextmanager
def hold_cold_cutover(state_root, *, inventory, package_path, approved_package_hash, outage_deadline, before_pause=None):
    """Hold existing host deployment exclusion for an already approved exact package.

    The caller verifies archive and spool custody, recovery and phase-specific
    application acceptance. The returned restart owner retains the marker
    until the target runtime and release are verified. An interrupted
    invocation must reconcile first.
    """
    state_root = Path(state_root)
    remaining = outage_deadline - time() if type(outage_deadline) in (int, float) else 0
    if (not _digest(approved_package_hash) or not isfinite(remaining) or remaining <= 0
            or (before_pause is not None and not callable(before_pause))):
        raise ValueError("canonical_cold_host_hold_invalid_package")
    stop_at = monotonic() + remaining

    def check_deadline():
        remaining = min(outage_deadline - time(), stop_at - monotonic())
        if remaining <= 0:
            raise TimeoutError("canonical_cold_host_hold_deadline")
        return remaining

    services = inventory["services"]
    writers = {"backend", "storage-maintenance", "market-data-collector"}
    if any(not services[name]["running"] for name in writers) or services["initialize"]["running"]:
        raise RuntimeError("canonical_cold_host_hold_requires_source_services")
    pause = [name for name in PAUSE_ORDER if services[name]["running"]]
    path = state_root / HOLD
    with host.deployment_lock(state_root), host.docker_deadline(stop_at):
        if os.path.lexists(path) or os.path.lexists(state_root / COMPLETION):
            raise RuntimeError("canonical_cold_host_hold_reconciliation_required")
        package = load_approved_package(package_path, approved_package_hash=approved_package_hash, inventory=inventory)
        if remaining > package["max_outage_seconds"]:
            raise ValueError("canonical_cold_host_hold_outage_exceeds_approved_budget")
        raw = read_approved_runtime_model(package_path, package)
        runtime_model, runtime_admission = prepare_runtime(raw, source=inventory)
        state = {
            "schema": VERSION, "phase": "pausing", "package_hash": approved_package_hash,
            "inventory_hash": package["approved_inventory_hash"], "project": inventory["project"],
            "source_revision": inventory["source_revision"], "candidate_revision": runtime_admission["revision"],
            "deadline": outage_deadline, "stopped_services": [], "inflight": None,
        }

        def observe(stopped):
            check_deadline()
            assert_host_inventory(state_root, expected=inventory, stopped_services=tuple(stopped))
            check_deadline()

        observe(())
        min_shutdown_time = (300 + 30) + (len(pause) - 1) * (60 + 30)
        if before_pause is not None:
            before_pause(check_deadline)
            observe(())
        if check_deadline() < min_shutdown_time + 5:
            raise TimeoutError("canonical_cold_host_hold_insufficient_shutdown_budget")
        host.save_receipt(path, state, initial=True)
        for name in pause:
            observe(state["stopped_services"])
            state["inflight"] = name
            host.save_receipt(path, state, initial=False)
            remaining = check_deadline()
            grace = 300 if name == "market-data-collector" else 60
            # Keep the collector's qualified shutdown budget intact.
            if remaining < grace + 5:
                raise TimeoutError(f"canonical_cold_host_hold_insufficient_stop_budget: {name}")
            host.docker("stop", "--time", str(grace), services[name]["id"], timeout=min(grace + 30, remaining))
            stopped = [*state["stopped_services"], name]
            observe(stopped)
            state.update(stopped_services=stopped, inflight=None)
            host.save_receipt(path, state, initial=False)
        state["phase"] = "writers_stopped"
        host.save_receipt(path, state, initial=False)

        def assert_held():
            if host.load_receipt(path) != state:
                raise RuntimeError("canonical_cold_host_hold_changed")
            if state["phase"].startswith("runtime"):
                raise RuntimeError("canonical_cold_host_hold_already_restarting")
            observe(state["stopped_services"])

        def record_database_phase(phase, evidence):
            assert_held()
            if state["phase"] not in _DATABASE_PREVIOUS.get(phase, ()) or not isinstance(evidence, dict):
                raise RuntimeError("canonical_cold_host_hold_database_phase_out_of_order")
            state["phase"] = phase
            state["database_evidence"] = state.get("database_evidence", {}) | {phase: evidence}
            host.save_receipt(path, state, initial=False)

        def resume(verify_ready, *, verify_before_start=None):
            if not callable(verify_ready):
                raise ValueError("canonical_cold_host_hold_acceptance_required")
            assert_held()
            phase = state["phase"]
            outcome = state.get("database_evidence", {}).get(phase, {}).get("outcome")
            allowed = {"database_unchanged": {"original"}, "database_reopened": {"original", "committed"}}
            if outcome not in allowed.get(phase, ()):
                raise RuntimeError("canonical_cold_host_hold_database_outcome_unverified")

            def record_runtime(phase, evidence):
                if host.load_receipt(path) != state:
                    raise RuntimeError("canonical_cold_host_hold_changed")
                check_deadline()
                if phase == "runtime_action_confirmed":
                    state["runtime_progress"] = evidence
                if phase == "runtime_prestart_verified":
                    state["runtime_prestart"] = evidence
                state["phase"] = phase
                state["runtime_evidence"] = evidence
                host.save_receipt(path, state, initial=False)

            evidence = restart_runtime(
                state_root, source=inventory, model=runtime_model if outcome == "committed" else None,
                admission=runtime_admission if outcome == "committed" else None,
                stopped_services=state["stopped_services"], remaining=check_deadline,
                record=record_runtime, verify_ready=verify_ready, verify_before_start=verify_before_start,
            )
            if outcome == "committed":
                _record_serving_release(
                    state_root, source=inventory, admission=runtime_admission,
                    observed_at=datetime.now(UTC).isoformat(), record=record_runtime,
                )
            record_runtime("runtime_complete", {"outcome": outcome, "acceptance": evidence})
            host.save_receipt(state_root / COMPLETION, state, initial=True)
            if host.load_receipt(path) != state:
                raise RuntimeError("canonical_cold_host_hold_changed")
            path.unlink()
            host.sync_directory(state_root)
            return evidence

        assert_held()
        yield package, assert_held, record_database_phase, check_deadline, resume
        if state["phase"] != "runtime_complete":
            assert_held()

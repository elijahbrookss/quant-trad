"""Explicit key-only preparation before forward publication or UTC selection.

Uses the existing confined terminal transport and supervised concurrent-index
owner. No capture, adoption, archive preparation, source stop or runtime change.
"""
from __future__ import annotations

from datetime import timedelta
import logging
import os
from pathlib import Path
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_terminal as terminal

STATE = "storage-online-key-preparation.json"
LOG = logging.getLogger(__name__)


def _immutable(value):
    return {k:v for k,v in value.items() if k not in {"intent_sha256", "phase", "proof"}}


def _journal(root):
    from scripts.automation import storage_online_forward as forward
    value = host.load_receipt(root/STATE, max_bytes=forward._MAX_BYTES)
    if (host.digest(_immutable(value)) != value.get("intent_sha256")
            or value.get("phase") not in {"prepared", "dispatched", "complete"}
            or value.get("duration_seconds") != 3600
            or value["wall_deadline"] != value["started_at"]+3600
            or value["monotonic_deadline"] != value["started_monotonic"]+3600):
        raise RuntimeError("storage_key_preparation_intent_changed")
    return value


def require_finished(root):
    """Publication cannot overtake an unresolved preparation or retained reader."""
    if not os.path.lexists(root/STATE):
        return
    value = _journal(root)
    if value["phase"] != "complete" or not value.get("proof", {}).get("complete"):
        raise RuntimeError("storage_key_preparation_reconciliation_required")
    saved = host.load_receipt(root/terminal.KEY_PROBE)
    if (saved.get("retired") is not True
            or host.docker("ps", "-aq", "--no-trunc", "--filter", "name=^/"+saved["name"]+"$").split()):
        raise RuntimeError("storage_key_preparation_reader_retirement_required")


def prepare_operation(path, *, package_file, execute=False):
    """Inspect by default; only the fixed key phase may mutate on execute.

    The ordinary forward proposal supplies the attested candidate and complete
    runtime preflights, but its date and destination plan are not published.
    Interrupted execution reuses this original host clock and the SQL key clock.
    """
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_deadline as publication
    from scripts.automation import storage_online_forward as forward
    if type(execute) is not bool:
        raise ValueError("storage_key_preparation_execute_invalid")
    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    package_path = launch._canonical(package_file)
    package_bytes = package_path.read_bytes()
    names = (publication.REQUEST, launch._STATE, terminal.STATE, forward.runtime.RUNTIME_RECIPE)
    preimages = {name: publication._sha((root/name).read_bytes()) for name in names}
    original = path.read_bytes()
    package = host.load_receipt(package_path)
    canceled = forward._terminal(root, path)
    saved = canceled["worker"]
    binding = dict(operation_path=str(path), plan_sha256=publication._sha(original),
        preimages=preimages, package=package, cancellation_intent_sha256=canceled["intent_sha256"],
        operator_sha256=publication._sha(Path(__file__).read_bytes()+Path(terminal.__file__).read_bytes()))
    with host.deployment_lock(root):
        journal = _journal(root) if os.path.lexists(root/STATE) else None
        if journal and journal["binding"] != binding:
            raise RuntimeError("storage_key_preparation_binding_changed")
        # A surviving key worker owns the controller lock needed by the
        # cancellation preflight. Retire only its exact durable container
        # contract before opening another SQL inspection; never redispatch it.
        terminal._retire_previous_probe(root, saved, package, keys=True)
    inspected = forward.publish_package(path, package_file=package_path, execute=False)
    if inspected["phase"] != "forward_package_inspected":
        raise RuntimeError("storage_key_preparation_before_publication_required")
    with host.deployment_lock(root):
        def admit():
            if (path.read_bytes() != original or package_path.read_bytes() != package_bytes
                    or any(publication._sha((root/name).read_bytes()) != digest for name,digest in preimages.items())
                    or any(os.path.lexists(root/name) for name in (forward.STATE, "storage-online-final.json", "promotion.env", "alert-preview.env"))):
                raise RuntimeError("storage_key_preparation_preimages_changed")
            if publication._sha(launch._canonical(plan["inventory_path"]).read_bytes()) != saved["binding"]["inventory_sha256"]:
                raise RuntimeError("storage_key_preparation_inventory_changed")
            publication._retired(saved)
            rows = host.inventory(plan["project"], operator_id=saved["container_id"])
            if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
                raise RuntimeError("storage_key_preparation_source_changed")
        journal = _journal(root) if os.path.lexists(root/STATE) else None
        if journal and journal["binding"] != binding:
            raise RuntimeError("storage_key_preparation_binding_changed")
        args = dict(expected_capture=canceled["original_capture"], intent_sha256=canceled["intent_sha256"])
        with host.docker_deadline(time.monotonic()+300):
            admit()
            # Retires only a previously owned key probe before read-only SQL
            # reconciliation. No index creation is replayed by inspection.
            proof = terminal._probe(root, plan, saved, package, action="inspect_keys",
                wall_deadline=time.time()+300, **args)
            if proof is not None and journal is None:
                raise RuntimeError("storage_key_preparation_unowned_sql_state")
            if journal and journal["phase"] == "complete" and proof != journal.get("proof"):
                raise RuntimeError("storage_key_preparation_completed_proof_changed")
            if proof is not None and proof["complete"]:
                journal.update(phase="complete", proof=proof)
                host.save_receipt(root/STATE, journal, initial=False)
                admit()
                return dict(phase="forward_keys_prepared", keys_prepared=True,
                    migration_started=False, forward_worker_authorized=False, final_switch_authorized=False)
            if not execute:
                return dict(phase="forward_keys_inspected", keys_prepared=False,
                    storage_mutations_performed=False, migration_started=False, final_switch_authorized=False)
        if journal is None:
            wall, mono = time.time(), time.monotonic()
            journal = dict(binding=binding, duration_seconds=3600, started_at=wall,
                started_monotonic=mono, wall_deadline=wall+3600, monotonic_deadline=mono+3600,
                boot_id=terminal._boot())
            journal["intent_sha256"] = host.digest(journal)
            journal["phase"] = "prepared"
            host.save_receipt(root/STATE, journal, initial=True)
        forward._clock(journal)
        with host.docker_deadline(journal["monotonic_deadline"]):
            admit()
            journal["phase"] = "dispatched"
            host.save_receipt(root/STATE, journal, initial=False)
            LOG.info("storage_key_preparation_start project=%s intent_sha256=%s", plan["project"], journal["intent_sha256"])
            terminal._probe(root, plan, saved, package, action="prepare_keys",
                wall_deadline=journal["wall_deadline"], **args)
            # The mutation reply is not completion authority. Inspect the actual
            # SQL receipt after independent transport-owned worker retirement.
        with host.docker_deadline(time.monotonic()+300):
            proof = terminal._probe(root, plan, saved, package, action="inspect_keys",
                wall_deadline=time.time()+300, **args)
            if proof is None or not proof["complete"]:
                raise RuntimeError("storage_key_preparation_incomplete")
            admit()
            journal.update(phase="complete", proof=proof)
            host.save_receipt(root/STATE, journal, initial=False)
        LOG.info("storage_key_preparation_complete project=%s intent_sha256=%s", plan["project"], journal["intent_sha256"])
        return dict(phase="forward_keys_prepared", keys_prepared=True,
            migration_started=False, forward_worker_authorized=False, final_switch_authorized=False)


def worker_keys(engine, payload):
    """Fixed-image internal action; host transport owns source-read confinement."""
    from sqlalchemy import text
    from scripts.automation.storage_online_worker import request_configuration
    from scripts.automation.storage_online_controller import OnlineController
    from scripts.db import fact_header_forward_keys as keys
    from scripts.db import fact_header_v2_capture as capture
    from scripts.db import fact_header_v2_copy as headers
    from scripts.db import fact_header_v2_placement as physical
    request = payload["request"]
    if "forward" in request or payload["action"] not in {"inspect_keys", "prepare_keys"}:
        raise ValueError("storage_key_preparation_worker_input_invalid")
    policy, limits, targets = request_configuration(request, Path("/run/qt-online/inventory.json"))
    expected, intent = payload["expected_capture"], payload["intent_sha256"]
    def inspect(conn):
        identity = conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
        if identity != request["database_identity"]:
            raise RuntimeError("storage_key_preparation_database_changed")
        OnlineController._require_job_environment(conn, allow_connections=True)
        OnlineController._supported_builtin_catalog(conn)
        if any(conn.scalar(text("SELECT to_regclass(:name)"), {"name":name}) is not None for name in
                ("qt_fact_header_forward_v2.initialization", "qt_fact_header_forward_v2.adoption")):
            raise RuntimeError("storage_key_preparation_adoption_already_initialized")
        binding = keys._source_binding(conn, expected, intent)
        state = keys._read_state(conn)
        actual = keys.inspect_keys(conn)
        if state is None:
            if any(v is not None for v in actual.values()):
                raise RuntimeError("storage_key_preparation_unowned_index")
            return None
        if (state["binding"] != binding or state["duration_seconds"] != 3600
                or state["expires_at"] != state["started_at"]+timedelta(seconds=3600)
                or any(actual.get(k) != oid for k,oid in state["index_oids"].items())
                or (state["complete"] and (state["index_oids"] != actual or set(actual) != set(keys.KEYS)))):
            raise RuntimeError("storage_key_preparation_sql_binding_changed")
        return dict(binding_sha256=host.digest(binding), started_at=state["started_at"].isoformat(),
            expires_at=state["expires_at"].isoformat(), duration_seconds=3600,
            index_oids=state["index_oids"], complete=state["complete"])
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        with capture._bounded_step(conn, 10):
            proof = inspect(conn)
            placement = physical._restore(conn.scalar(text("SELECT placement FROM "+headers.STATE+" WHERE id=1"))["plan"])
            if {t.target_id:t for t in targets} != {t.target_id:t for t in (placement.recent, placement.history)}:
                raise RuntimeError("storage_key_preparation_placement_changed")
    if payload["action"] == "inspect_keys":
        return proof
    remaining = payload["wall_deadline"]-time.time()-70
    if remaining <= 0:
        raise RuntimeError("storage_key_preparation_host_deadline_expired")
    keys.prepare_keys_supervised(engine, expected_capture=expected, intent_sha256=intent,
        placement=placement, policy=policy, resource_limits=limits, max_duration_seconds=3600,
        deadline=time.monotonic()+remaining)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        with capture._bounded_step(conn, 10):
            return inspect(conn)

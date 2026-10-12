"""Fixed key and initialization phases for the confined forward worker.

No host launch, source stop or capture renewal. The published request binds the
canceled attempt; actual database receipts own the separate preparation clocks.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from copy import deepcopy
import hashlib
import json
import logging
import math
import re
from time import monotonic

LOG = logging.getLogger(__name__)
INITIAL = "qt_fact_header_forward_v2.initialization"


def request_binding(request):
    if "forward_reschedule" in request:
        original = original_request(request)
        value = request_binding(original)
        if value is None or value["schema_version"] != "qt.storage_online_forward_intent.v2":
            raise ValueError("storage_forward_reschedule_successor_required")
        return value
    if "forward" not in request:
        return None
    value = request["forward"]
    fields = {"schema_version", "original_plan_sha256", "cancellation_intent_sha256",
        "original_capture", "end_day", "candidate_revision", "candidate_source_hash", "operation_sha256"}
    version = value.get("schema_version") if isinstance(value, dict) else None
    successor_fields = {"predecessor_operation_sha256", "predecessor_terminal_sha256", "lookup_operation_sha256"}
    if version == "qt.storage_online_forward_intent.v2":
        fields |= successor_fields
    if (not isinstance(value, dict) or set(value) != fields
            or version not in ("qt.storage_online_forward_intent.v1", "qt.storage_online_forward_intent.v2")
            or "capture_preparation" in request
            or not isinstance(value["original_capture"], dict) or not value["original_capture"]
            or not isinstance(value["end_day"], str)
            or any(not isinstance(value[k], str) or not re.fullmatch(pattern, value[k]) for k, pattern in (
                ("original_plan_sha256", r"[0-9a-f]{64}"), ("cancellation_intent_sha256", r"[0-9a-f]{64}"),
                ("operation_sha256", r"[0-9a-f]{64}"), ("candidate_revision", r"[0-9a-f]{40}"),
                ("candidate_source_hash", r"[0-9a-f]{64}")))
            or value["candidate_revision"] != request.get("source_revision")
            or value["candidate_source_hash"] != request.get("source_tree_hash")
            or value["operation_sha256"] == value["cancellation_intent_sha256"]
            or value["operation_sha256"] != _digest({k:v for k,v in value.items() if k != "operation_sha256"})):
        raise ValueError("storage_forward_worker_request_invalid")
    if version == "qt.storage_online_forward_intent.v2" and (
            any(not isinstance(value[name], str) or not re.fullmatch(r"[0-9a-f]{64}", value[name]) for name in successor_fields)
            or len({value[name] for name in ("operation_sha256", "predecessor_operation_sha256", "lookup_operation_sha256", "cancellation_intent_sha256")}) != 4):
        raise ValueError("storage_forward_worker_successor_invalid")
    if date.fromisoformat(value["end_day"]).isoformat() != value["end_day"]:
        raise ValueError("storage_forward_worker_end_day_invalid")
    return value


def original_request(request):
    """Recover the immutable request underlying an explicit amendment.

    The operation identity, data proof and clocks remain those of this request.
    V1 keeps its original clocks. V2 names the preceding request and an explicit
    cumulative adoption duration; the host and SQL initializer independently admit
    that explicit extension. Neither version rewrites the canceled capture.
    """
    update = request.get("forward_reschedule")
    if update is None and "forward_reschedule" not in request:
        return deepcopy(request)
    fields = {"schema_version", "original_request_sha256", "original_max_objects", "end_day"}
    version = update.get("schema_version") if isinstance(update, dict) else None
    if version == "qt.storage_online_forward_reschedule.v2":
        fields |= {"previous_request_sha256", "attempt_seconds"}
        if "continue_guarded_proof" in update:
            fields.add("continue_guarded_proof")
            if update["continue_guarded_proof"] is not True or update.get("raw_mapping_mode") != "retain_source":
                raise ValueError("storage_forward_guarded_continuation_invalid")
        if "raw_mapping_mode" in update:
            fields.add("raw_mapping_mode")
            if update["raw_mapping_mode"] != "retain_source":
                raise ValueError("storage_forward_reschedule_raw_mapping_mode_invalid")
    if (not isinstance(update, dict) or set(update) != fields
            or version not in {"qt.storage_online_forward_reschedule.v1", "qt.storage_online_forward_reschedule.v2"}
            or not isinstance(update["original_request_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", update["original_request_sha256"])
            or type(update["original_max_objects"]) is not int
            or type(request.get("max_objects")) is not int
            or not 1 <= update["original_max_objects"] <= request["max_objects"] <= 1000000
            or not isinstance(update["end_day"], str)
            or not isinstance(request.get("forward"), dict)
            or any(not isinstance(request.get(k), str) or not re.fullmatch(pattern, request[k])
                   for k, pattern in (("source_revision", r"[0-9a-f]{40}"),
                                      ("source_tree_hash", r"[0-9a-f]{64}")))):
        raise ValueError("storage_forward_reschedule_request_invalid")
    original = deepcopy(request)
    del original["forward_reschedule"]
    intent = original["forward"]
    original.update(source_revision=intent.get("candidate_revision"),
        source_tree_hash=intent.get("candidate_source_hash"), max_objects=update["original_max_objects"])
    request_binding(original)
    if version == "qt.storage_online_forward_reschedule.v2" and (
            not isinstance(update["previous_request_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", update["previous_request_sha256"])
            or update["previous_request_sha256"] == update["original_request_sha256"]
            or type(update["attempt_seconds"]) is not int
            or type(intent["original_capture"].get("attempt_seconds")) is not int
            or not intent["original_capture"]["attempt_seconds"] < update["attempt_seconds"] <= 345600):
        raise ValueError("storage_forward_reschedule_extension_invalid")
    if (_digest(original) != update["original_request_sha256"]
            or date.fromisoformat(update["end_day"]).isoformat() != update["end_day"]
            or date.fromisoformat(update["end_day"]) <= date.fromisoformat(intent["end_day"])):
        raise ValueError("storage_forward_reschedule_original_request_changed")
    return original


def execution_intent(request):
    """Effective schedule; the original hashed operation intent stays unchanged."""
    value = request_binding(request)
    if value is None:
        return None
    return {**value, "end_day": request.get("forward_reschedule", {}).get("end_day", value["end_day"])}


def adoption_seconds(request):
    """Effective finite bound; the original operation/cancellation stays intact."""
    intent = request_binding(request)
    if intent is None:
        raise ValueError("storage_forward_adoption_intent_required")
    return request.get("forward_reschedule", {}).get("attempt_seconds", intent["original_capture"].get("attempt_seconds"))


def initialization_binding(request, *, key_seconds=3600, initial_seconds=600):
    intent = request_binding(request)
    if intent is None:
        raise ValueError("storage_forward_initialization_intent_required")
    result = dict(request_sha256=_digest(request), operation_sha256=intent["operation_sha256"],
        cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=key_seconds,
        initial_seconds=initial_seconds, attempt_seconds=adoption_seconds(request))
    if "forward_reschedule" in request:
        result["reschedule"] = deepcopy(request["forward_reschedule"])
    return result


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def capture_observation(state):
    """The immutable adoption identity shared by live and terminal observers."""
    from scripts.db import fact_header_forward_adoption as adoption
    return {**{name: state[name] for name in
        ("operation_sha256", "started_at", "expires_at", "attempt_seconds")},
        "cancellation": state["binding"]["terminal"],
        "placement": adoption.placement_binding(state)}


def capture_binding(value):
    """Compact exact proof identity for the bounded host/worker control pipe.

    Full cancellation and placement metadata stay in SQL and the bounded host
    launch journal. Hash the complete observation; never truncate its proof.
    """
    fields = {"operation_sha256", "started_at", "expires_at", "attempt_seconds", "cancellation", "placement"}
    if (not isinstance(value, dict) or set(value) != fields
            or not isinstance(value["operation_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", value["operation_sha256"])
            or type(value["attempt_seconds"]) is not int
            or not 30 <= value["attempt_seconds"] <= 345600
            or any(not isinstance(value[k], dict) or not value[k] for k in ("cancellation", "placement"))):
        raise ValueError("storage_forward_capture_binding_invalid")
    normalized = dict(value)
    for key in ("started_at", "expires_at"):
        stamp = value[key] if isinstance(value[key], datetime) else datetime.fromisoformat(value[key])
        if stamp.utcoffset() != timedelta(0):
            raise ValueError("storage_forward_capture_binding_clock_invalid")
        normalized[key] = stamp.isoformat()
    return dict(schema_version="qt.storage_online_forward_capture.v1",
        operation_sha256=value["operation_sha256"], started_at=normalized["started_at"],
        expires_at=normalized["expires_at"], attempt_seconds=value["attempt_seconds"],
        proof_sha256=_digest(normalized))


def initialization_relation(intent=None):
    """Explicit operation identity, never a newest-attempt lookup."""
    if intent is None or intent.get("schema_version") == "qt.storage_online_forward_intent.v1":
        return INITIAL
    if (intent.get("schema_version") != "qt.storage_online_forward_intent.v2"
            or not isinstance(intent.get("operation_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", intent["operation_sha256"])):
        raise ValueError("storage_forward_initialization_intent_invalid")
    # PostgreSQL names are limited to 63 bytes. Full identity remains in binding.
    return "qt_fact_header_forward_v2.initialization_" + intent["operation_sha256"][:48]


def _initial(conn, intent=None):
    from sqlalchemy import text
    relation = initialization_relation(intent)
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name":relation}) is None:
        return None
    rows = conn.execute(text("SELECT id,binding,started_at,expires_at,duration_seconds,complete FROM "+relation+" LIMIT 2")).mappings().all()
    if (len(rows) != 1 or rows[0]["id"] != 1
            or rows[0]["expires_at"] != rows[0]["started_at"]+timedelta(seconds=rows[0]["duration_seconds"])):
        raise RuntimeError("storage_forward_initialization_receipt_changed")
    return dict(rows[0])


def inspect_reschedule(conn, *, request, timeout_seconds=10):
    """Read the live SQL preimage under controller exclusion; no renewal."""
    from sqlalchemy import text
    from scripts.db import fact_header_forward_adoption as adoption

    intent = request_binding(request)
    if intent is None or intent["schema_version"] != "qt.storage_online_forward_intent.v2":
        raise ValueError("storage_forward_reschedule_successor_required")
    with adoption._step(conn, timeout_seconds) as limit:
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))")):
            raise RuntimeError("storage_forward_reschedule_storage_owner_busy")
        if request.get("forward_reschedule", {}).get("continue_guarded_proof") is True:
            # Expiry stops work, but does not remove these ALWAYS guards. This
            # explicitly selected amendment may inspect preserved proof, never
            # a retired adoption or the original canceled capture.
            adoption.inspect_retirement(conn, operation_sha256=intent["operation_sha256"],
                                        timeout_seconds=timeout_seconds)
            state = adoption._inspect_guarded_state(conn, adoption._state(conn, intent["operation_sha256"]))
            if not all(state["progress"]["identity_"+side]["complete"] for side in ("source", "target")):
                raise RuntimeError("storage_forward_guarded_identity_proof_incomplete")
            expiry = state["started_at"] + timedelta(seconds=adoption_seconds(request))
            remaining = (expiry-conn.scalar(text("SELECT clock_timestamp()"))).total_seconds()
            if remaining <= 0:
                raise RuntimeError("storage_forward_guarded_continuation_expired")
            limit(remaining)
        else:
            state = adoption._inspect(conn, intent["operation_sha256"], limit)
        phase = _initial(conn, intent)
        if phase is None or not phase["complete"]:
            raise RuntimeError("storage_forward_reschedule_completed_initializer_required")
        result = dict(initialization={k:(v.isoformat() if isinstance(v, datetime) else v)
                                      for k,v in phase.items()},
                      capture={k:(v.isoformat() if isinstance(v, datetime) else v)
                               for k,v in capture_observation(state).items()})
        preserved = {k:(v.isoformat() if isinstance(v, datetime) else v) for k,v in state.items()}
        if request.get("forward_reschedule", {}).get("schema_version") == "qt.storage_online_forward_reschedule.v2":
            # Only these two explicit amendment fields may change. Start time,
            # progress, guards, reference state and physical bindings still hash.
            for key in ("expires_at", "attempt_seconds"):
                del preserved[key]
            if request["forward_reschedule"].get("raw_mapping_mode") == "retain_source":
                # Only this named decision and the abandoned raw copy's source
                # mirror may change. Identity guards/progress/files remain pinned.
                preserved = deepcopy(preserved)
                result["raw_mapping_mode"] = preserved["binding"].pop("raw_mapping_mode", "replace")
                preserved["binding"]["functions"] = [entry for entry in preserved["binding"]["functions"]
                    if (entry["relation"], entry["tgname"]) !=
                       (adoption.raw.SOURCE, adoption._trigger_name("raw", "mirror"))]
            result["preserved_adoption_sha256"] = _digest(preserved)
            from scripts.db import archive_root_v2_online as archives
            archive = archives._forward_amendment_state(conn, intent["operation_sha256"])
            del archive["capture"]["binding"]["expires_at"]
            result["preserved_archive_sha256"] = _digest(archive)
        else:
            result["adoption_sha256"] = _digest(preserved)
        return result


def reschedule_initialization(conn, *, request, expected, final_seconds, timeout_seconds=10):
    """Explicit stopped-worker amendment, with no progress or start-time reset.

    The host must have retired the old worker and journaled this exact dispatch.
    The same SQL lock independently excludes a surviving controller. Initializer
    V1 leaves all clocks unchanged. V2 can increase the cumulative adoption bound
    before expiry, or explicitly continue completed guarded proof afterward;
    progress, constraints, mirrors and archive capture stay intact.
    The short initializer and publication clocks are never renewed. Lost COMMIT is observed through
    ``inspect_reschedule``; the host must not replay uncertain dispatch.
    """
    from sqlalchemy import text
    from datetime import timezone
    from scripts.db import fact_header_forward_adoption as adoption

    if "forward_reschedule" not in request or type(final_seconds) is not int or not 1 <= final_seconds <= 600:
        raise ValueError("storage_forward_reschedule_arguments_invalid")
    original = original_request(request)
    intent = request_binding(request)
    extension = request["forward_reschedule"]["schema_version"] == "qt.storage_online_forward_reschedule.v2"
    with adoption._step(conn, timeout_seconds):
        before = inspect_reschedule(conn, request=request, timeout_seconds=timeout_seconds)
        binding_before = before["initialization"]["binding"]
        if before != expected:
            raise RuntimeError("storage_forward_reschedule_preimage_changed")
        if extension:
            previous = binding_before.get("reschedule", {})
            if (binding_before.get("request_sha256") != request["forward_reschedule"]["previous_request_sha256"]
                    or previous.get("schema_version") != "qt.storage_online_forward_reschedule.v1"
                    or {k:v for k,v in binding_before.items() if k not in {"request_sha256", "reschedule"}}
                        != {k:v for k,v in initialization_binding(original).items() if k != "request_sha256"}
                    or before["capture"]["attempt_seconds"] != binding_before["attempt_seconds"]
                    or request["forward_reschedule"]["end_day"] <= previous["end_day"]):
                raise RuntimeError("storage_forward_reschedule_predecessor_changed")
        elif binding_before != initialization_binding(original):
            raise RuntimeError("storage_forward_reschedule_preimage_changed")
        boundary = datetime.fromisoformat(execution_intent(request)["end_day"]).replace(tzinfo=timezone.utc)
        expiry = (datetime.fromisoformat(before["capture"]["started_at"]) + timedelta(seconds=adoption_seconds(request))
                  if extension else datetime.fromisoformat(before["capture"]["expires_at"]))
        now = conn.scalar(text("SELECT clock_timestamp()"))
        if not now < boundary or boundary + timedelta(seconds=final_seconds) > expiry:
            raise RuntimeError("storage_forward_reschedule_outside_original_deadline")
        if extension:
            from scripts.db import archive_root_v2_online as archives
            archives._amend_forward_expiry(conn, intent["operation_sha256"], expiry)
            conn.execute(text("UPDATE " + adoption.state_relation(conn, intent["operation_sha256"]) +
                " SET attempt_seconds=:seconds,expires_at=started_at+:seconds*interval '1 second' WHERE id=1"),
                {"seconds": adoption_seconds(request)})
        if request["forward_reschedule"].get("raw_mapping_mode") == "retain_source":
            adoption._retain_raw_source(conn, adoption._state(conn, intent["operation_sha256"]))
        binding = initialization_binding(request)
        conn.execute(text("UPDATE " + initialization_relation(intent) + " SET binding=CAST(:binding AS jsonb) WHERE id=1"),
                     {"binding": json.dumps(binding)})
        after = inspect_reschedule(conn, request=request, timeout_seconds=timeout_seconds)
        wanted = deepcopy(before)
        wanted["initialization"]["binding"] = binding
        if extension:
            wanted["capture"].update(attempt_seconds=adoption_seconds(request), expires_at=expiry.isoformat())
        if request["forward_reschedule"].get("raw_mapping_mode") == "retain_source":
            wanted["raw_mapping_mode"] = "retain_source"
        if after != wanted:
            raise RuntimeError("storage_forward_reschedule_preservation_failed")
        LOG.info("storage_forward_rescheduled operation_sha256=%s original_request_sha256=%s request_sha256=%s end_day=%s deadline_extended=%s start_unchanged=true",
            intent["operation_sha256"], _digest(original), _digest(request), request["forward_reschedule"]["end_day"], extension)
        return after


def replace_initialization_package(conn, *, request, previous_request, expected,
                                   final_seconds, timeout_seconds=10):
    """Rebind only candidate provenance after exact stopped-worker publication.

    The host journals dispatch once and reconciles a lost reply by observation.
    SQL independently excludes a surviving controller. No capture, adoption,
    reference progress, schedule, data or archive row is rewritten here.
    """
    from sqlalchemy import text
    from datetime import timezone
    from scripts.db import fact_header_forward_adoption as adoption
    from scripts.db import archive_root_v2_online as archives

    intent = request_binding(request)
    previous_intent = request_binding(previous_request)
    provenance = {"source_revision", "source_tree_hash"}
    if (intent is None or intent != previous_intent
            or type(final_seconds) is not int or not 1 <= final_seconds <= 600
            or request.get("forward_reschedule", {}).get("schema_version") != "qt.storage_online_forward_reschedule.v2"
            or request["forward_reschedule"].get("raw_mapping_mode") != "retain_source"
            or {k:v for k,v in request.items() if k not in provenance}
                != {k:v for k,v in previous_request.items() if k not in provenance}):
        raise ValueError("storage_forward_package_replacement_scope_changed")
    with adoption._step(conn, timeout_seconds):
        before = inspect_reschedule(conn, request=request, timeout_seconds=timeout_seconds)
        if (before != expected or before["initialization"]["binding"] != initialization_binding(previous_request)
                or before["raw_mapping_mode"] != "retain_source"
                or before["capture"]["attempt_seconds"] != adoption_seconds(request)):
            raise RuntimeError("storage_forward_package_replacement_preimage_changed")
        boundary = datetime.fromisoformat(execution_intent(request)["end_day"]).replace(tzinfo=timezone.utc)
        expiry = datetime.fromisoformat(before["capture"]["expires_at"])
        if not conn.scalar(text("SELECT clock_timestamp()")) < boundary or boundary+timedelta(seconds=final_seconds) > expiry:
            raise RuntimeError("storage_forward_reschedule_outside_original_deadline")
        archive_before = archives._forward_amendment_state(conn, intent["operation_sha256"])
        binding = initialization_binding(request)
        conn.execute(text("UPDATE " + initialization_relation(intent) + " SET binding=CAST(:binding AS jsonb) WHERE id=1"),
                     {"binding": json.dumps(binding)})
        after = inspect_reschedule(conn, request=request, timeout_seconds=timeout_seconds)
        wanted = deepcopy(before)
        wanted["initialization"]["binding"] = binding
        if (after != wanted
                or archives._forward_amendment_state(conn, intent["operation_sha256"]) != archive_before):
            raise RuntimeError("storage_forward_package_replacement_preservation_failed")
        LOG.info("storage_forward_package_replaced operation_sha256=%s previous_request_sha256=%s request_sha256=%s clocks_unchanged=true",
                 intent["operation_sha256"], _digest(previous_request), _digest(request))
        return after


def prepare_forward(engine, request, *, targets, policy, limits, source, destination,
                    key_seconds=3600, initial_seconds=600):
    """Prepare keys, then persist ONE short initialization clock before adoption.

    A completed initializer only inspects its original adoption/archive state.
    An interrupted initializer uses its original receipt, never a fresh timeout.
    Both initialization writes commit together; lost COMMIT replies reconcile
    the same complete record. The old capture is never activated or amended.
    """
    from sqlalchemy import text
    from scripts.db import fact_header_forward_keys as keys
    from scripts.db import fact_header_forward_adoption as adoption
    from scripts.db import fact_header_v2_capture as capture
    from scripts.db import fact_header_v2_copy as headers
    from scripts.db import fact_header_v2_placement as physical
    from scripts.db import archive_root_v2_online as archives
    from scripts.db import archive_reference_v2_placement as reference_move
    from portal.backend.service.storage.header_resource_claims import _limits
    from portal.backend.service.storage.header_resources import observe_header_resources
    from portal.backend.service.storage.header_movement import _MoveWatch

    intent = request_binding(request)
    successor = intent is not None and intent["schema_version"] == "qt.storage_online_forward_intent.v2"
    initial_relation = initialization_relation(intent)
    if (intent is None or type(key_seconds) is not int or not 30 <= key_seconds <= 3600
            or type(initial_seconds) is not int or not 30 <= initial_seconds <= 600):
        raise ValueError("storage_forward_worker_phase_bounds_invalid")
    seconds = adoption_seconds(request)
    if type(seconds) is not int or not 30 <= seconds <= 345600:
        raise ValueError("storage_forward_worker_adoption_bound_invalid")
    limits = _limits(limits, migration=True)
    binding = initialization_binding(request, key_seconds=key_seconds, initial_seconds=initial_seconds)
    with engine.begin() as conn, capture._bounded_step(conn, 10):
        prior = _initial(conn, intent)
        if "forward_reschedule" in request and (
                prior is None or not prior["complete"] or prior["binding"] != binding):
            raise RuntimeError("storage_forward_reschedule_initializer_required")
        saved = conn.scalar(text("SELECT placement FROM "+headers.STATE+" WHERE id=1"))
        if not isinstance(saved, dict) or not isinstance(saved.get("plan"), dict):
            raise RuntimeError("storage_forward_retained_placement_required")
        if successor:
            from scripts.db import fact_header_forward_placement as lookup
            placed = lookup.completed_receipt(conn, intent["lookup_operation_sha256"])
            if (placed["binding"]["predecessor_operation_sha256"] != intent["predecessor_operation_sha256"]
                    or placed["binding"]["predecessor_terminal_sha256"] != intent["predecessor_terminal_sha256"]):
                raise RuntimeError("storage_forward_lookup_predecessor_changed")
            saved = placed["binding"]["placement"]
        placement = physical._restore(saved["plan"])
        if {t.target_id:t for t in targets} != {t.target_id:t for t in (placement.recent,placement.history)}:
            raise RuntimeError("storage_forward_worker_inventory_changed")
    if prior is None and not successor:
        keys.prepare_keys_supervised(engine, expected_capture=intent["original_capture"],
            intent_sha256=intent["cancellation_intent_sha256"], placement=placement,
            policy=policy, resource_limits=limits, max_duration_seconds=key_seconds)
    with engine.connect() as conn:
        acquired = []
        watch = None
        try:
            with conn.begin(), capture._bounded_step(conn, 10):
                for name in ("qt.storage.management.v1", keys.CONTROLLER_LOCK):
                    if not conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"), {"name":name}):
                        raise RuntimeError("storage_forward_initialization_owner_busy")
                    acquired.append(name)
                phase = _initial(conn, intent)
                prepared = keys._read_state(conn)
                if (prepared is None or not prepared["complete"]
                        or prepared["duration_seconds"] != key_seconds
                        or prepared["index_oids"] != keys.inspect_keys(conn)):
                    raise RuntimeError("storage_forward_initialization_keys_changed")
                if phase is None:
                    if adoption._state(conn, intent["operation_sha256"]) is not None:
                        raise RuntimeError("storage_forward_unowned_adoption")
                    if successor:
                        adoption._predecessor(conn, intent["predecessor_operation_sha256"], intent["predecessor_terminal_sha256"])
                    actual = keys._source_binding(conn, intent["original_capture"], intent["cancellation_intent_sha256"])
                    if actual != prepared["binding"]:
                        raise RuntimeError("storage_forward_initialization_source_changed")
                    conn.exec_driver_sql("CREATE TABLE "+initial_relation+"(id integer PRIMARY KEY CHECK(id=1),"
                        "binding jsonb NOT NULL,started_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,"
                        "duration_seconds integer NOT NULL CHECK(duration_seconds BETWEEN 30 AND 600),"
                        "complete boolean NOT NULL DEFAULT false,"
                        "CHECK(expires_at=started_at+duration_seconds*interval '1 second'))")
                    conn.execute(text("INSERT INTO "+initial_relation+" SELECT 1,CAST(:binding AS jsonb),"
                        "stamp,stamp+:seconds*interval '1 second',:seconds,false FROM (SELECT clock_timestamp() stamp) s"),
                        {"binding":json.dumps(binding),"seconds":initial_seconds})
                    phase = _initial(conn, intent)
                if phase["binding"] != binding or phase["duration_seconds"] != initial_seconds:
                    raise RuntimeError("storage_forward_initialization_binding_changed")
            # Durable initialization intent is committed before adoption/archive
            # preparation. Session locks still belong to this actual connection.
            if not phase["complete"]:
                with conn.begin(), capture._bounded_step(conn, 10):
                    remaining = float(conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM "+initial_relation+" WHERE id=1")))
                    if remaining <= 1:
                        raise RuntimeError("storage_forward_initialization_expired")
                    deadline = monotonic()+min(remaining, limits["movement_timeout_seconds"])
                    actual, _ = physical.observe(conn, placement)
                    if actual != saved:
                        raise RuntimeError("storage_forward_initialization_placement_changed")
                    reference_move._fixed_inputs(policy, limits, targets)
                    resources = observe_header_resources(conn, targets, pg_controldata=placement.pg_controldata, timeout_seconds=10)
                    remaining = deadline-monotonic()
                    if remaining <= 0:
                        raise RuntimeError("storage_forward_initialization_expired")
                    _, floors = reference_move._budget(conn, observed={"bytes":0,"_binding":saved},
                        policy=policy, limits={**limits, "movement_timeout_seconds":math.ceil(remaining)},
                        targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=None, grace=limits["cancellation_grace_seconds"])
                    watch.start()
                with conn.begin():
                    allowance = int(min(30, deadline-monotonic()))
                    if allowance < 1: raise RuntimeError("storage_forward_initialization_expired")
                    adoption.prepare_adoption(conn, expected_capture=intent["original_capture"],
                        cancellation_intent_sha256=intent["cancellation_intent_sha256"],
                        operation_sha256=intent["operation_sha256"], attempt_seconds=seconds, timeout_seconds=allowance,
                        **({name: intent[name] for name in ("predecessor_operation_sha256", "predecessor_terminal_sha256", "lookup_operation_sha256")} if successor else {}))
                    allowance = int(min(30, deadline-monotonic()))
                    if allowance < 1: raise RuntimeError("storage_forward_initialization_expired")
                    archives.prepare(conn, source_root=source, destination_root=destination,
                        forward_operation_sha256=intent["operation_sha256"], timeout_seconds=allowance)
                    watch.check()
                    conn.exec_driver_sql("UPDATE "+initial_relation+" SET complete=true WHERE id=1")
                watch.check()
                LOG.info("storage_forward_initialized operation_sha256=%s", intent["operation_sha256"])
            with conn.begin(), capture._bounded_step(conn, 10) as limit:
                state = adoption._inspect(conn, intent["operation_sha256"], limit)
                if (state["attempt_seconds"] != seconds
                        or state["binding"]["terminal"]["receipt"]["capture"] != intent["original_capture"]
                        or state["binding"]["terminal"]["receipt"]["intent_sha256"] != intent["cancellation_intent_sha256"]):
                    raise RuntimeError("storage_forward_adoption_binding_changed")
                if adoption.retains_raw_source(state) != (request.get("forward_reschedule", {}).get("raw_mapping_mode") == "retain_source"):
                    raise RuntimeError("storage_forward_raw_mapping_mode_changed")
                archives._inspect(conn, source, destination, forward_operation_sha256=intent["operation_sha256"], saved=saved)
                return placement, state["started_at"].isoformat()
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            try:
                if watch is not None: watch.stop(conn)
            finally:
                if not conn.closed and not conn.invalidated:
                    conn.rollback()
                    for name in reversed(acquired):
                        conn.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), {"name":name})
                    conn.commit()

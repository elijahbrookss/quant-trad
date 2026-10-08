"""Preserving identity/raw adoption for the fixed retained-header handoff.

Internal database phase only. A separate qualified host operation must supply
its intent, physical/resource admission and original wall/boot/monotonic bound.
No old capture/progress/queue is resumed, changed or discarded. No attachment,
final switch, runtime activation or migration-ready certificate is produced.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import json
import hashlib
import logging
import re

from sqlalchemy import text

from scripts.db import fact_header_forward_keys as keys
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db import fact_header_v2_references as references
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK

STATE = keys.SCHEMA + ".adoption"
IDENTITY_HEAP_SCAN = "identity_heap_v1"
HEAP_BLOCKS_PER_PAGE = 128
RAW_HEAP_SCAN = "raw_heap_v1"
IDENTITY = capture.SCHEMA + ".fact_identities"
# Native FK AFTER triggers are named RI_ConstraintTrigger_*. This quoted
# uppercase name mirrors identities first, without deferring FK enforcement.
IDENTITY_MIRROR = "AAA_qt_forward_identity_mirror"

FAMILIES = {
    "identity": (headers.SOURCE, IDENTITY, headers.IDENTITY_COLUMNS, ("id",)),
    "raw": (raw.SOURCE, raw.TARGET, raw.COLUMNS, raw.KEYS),
}
logger = logging.getLogger(__name__)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def successor_schema(operation_sha256):
    """A private namespace for this operation; the full intent remains in its row."""
    if not isinstance(operation_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", operation_sha256):
        raise ValueError("fact_header_forward_operation_invalid")
    return "qt_fwd_" + operation_sha256[:56]


def operation_schema(conn, operation_sha256=None):
    """Resolve only an explicitly named operation, never the newest attempt."""
    if operation_sha256 is None:
        return keys.SCHEMA
    candidate = successor_schema(operation_sha256)
    original = None
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is not None:
        rows = conn.execute(text("SELECT id,operation_sha256 FROM " + STATE + " LIMIT 2")).all()
        if len(rows) > 1 or rows and rows[0][0] != 1:
            raise RuntimeError("fact_header_forward_adoption_state_changed")
        original = rows[0][1] if rows else None
    candidate_exists = conn.scalar(text("SELECT to_regnamespace(:name) IS NOT NULL"), {"name": candidate})
    if original == operation_sha256 and candidate_exists:
        raise RuntimeError("fact_header_forward_operation_ambiguous")
    if candidate_exists or original is not None and original != operation_sha256:
        return candidate
    return keys.SCHEMA


def state_relation(conn, operation_sha256=None):
    return operation_schema(conn, operation_sha256) + ".adoption"


def _predecessor(conn, operation_sha256, terminal_sha256):
    terminal = inspect_retirement(conn, operation_sha256=operation_sha256)
    if terminal is None or _digest(terminal) != terminal_sha256:
        raise RuntimeError("fact_header_forward_successor_retirement_required")
    relation = state_relation(conn, operation_sha256)
    return dict(operation_sha256=operation_sha256, terminal_sha256=terminal_sha256,
        state_sha256=_digest(_json_row(conn, relation)),
        relation_oid=conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": relation}))


def _trigger_name(family, suffix):
    if (family, suffix) == ("identity", "mirror"):
        return IDENTITY_MIRROR
    return "trg_qt_forward_" + family + "_" + suffix


def _json_row(conn, relation):
    rows = conn.execute(text("SELECT to_jsonb(t) FROM " + relation + " t LIMIT 2")).scalars().all()
    if len(rows) != 1 or rows[0].get("id") != 1:
        raise RuntimeError("fact_header_forward_adoption_original_state_changed: " + relation)
    return rows[0]


def _require_identity_heap(conn):
    _require_heap(conn, IDENTITY, "identity")


def _require_heap(conn, relation, family):
    # A CTID is local to one physical heap. Ordinary inheritance would make
    # SELECT include other heaps whose CTIDs overlap the saved parent cursor.
    if not conn.scalar(text("SELECT c.relkind='r' AND c.relpersistence='p' "
        "AND a.amname='heap' AND NOT EXISTS(SELECT 1 FROM pg_inherits i "
        "WHERE i.inhparent=c.oid OR i.inhrelid=c.oid) "
        "FROM pg_class c JOIN pg_am a ON a.oid=c.relam "
        "WHERE c.oid=CAST(:relation AS regclass)"), {"relation": relation}):
        raise RuntimeError("fact_header_forward_" + family + "_heap_required")


def _snapshot(conn, operation_sha256=None, *, schema=None, predecessor=None, lookup_operation_sha256=None):
    _require_identity_heap(conn)
    for relation in (raw.SOURCE, raw.TARGET):
        _require_heap(conn, relation, "raw")
    schema = schema or operation_schema(conn, operation_sha256)
    state_name = schema + ".adoption"
    if predecessor is None and schema != keys.SCHEMA:
        predecessor = conn.scalar(text("SELECT binding->'predecessor' FROM " + state_name + " WHERE id=1"))
    if lookup_operation_sha256 is None and conn.scalar(text("SELECT to_regclass(:name)"), {"name": state_name}) is not None:
        lookup_operation_sha256 = conn.scalar(text("SELECT binding->'lookup_placement'->>'operation_sha256' FROM " + state_name + " WHERE id=1"))
    # Fixed objects only. The old journals and queues remain evidence. Target
    # contents may gain missing source rows; their definitions/files may not drift.
    relations = (headers.SOURCE, IDENTITY, raw.SOURCE, raw.TARGET,
                 headers.STATE, raw.STATE, capture.STATE, capture.QUEUE,
                 raw.QUEUE, capture.CANCELLED, keys.STATE, state_name)
    shapes = {}
    for relation in relations:
        schema, name = relation.split(".")
        shapes[relation] = headers._shape(conn, name, schema=schema)
    functions = [dict(row) for row in conn.execute(text("""
        SELECT t.tgrelid::regclass::text AS relation,t.tgname,t.oid::bigint,
               pg_get_triggerdef(t.oid) AS trigger,t.tgenabled,t.tgisinternal,
               t.tgconstraint::bigint AS constraint_oid,p.oid::bigint AS function_oid,
               p.proowner::bigint AS owner,pg_get_functiondef(p.oid) AS body
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        WHERE t.tgrelid IN (SELECT to_regclass(name) FROM unnest(CAST(:names AS text[])) name)
          ORDER BY relation,t.tgname
    """), {"names": [r for family in FAMILIES.values() for r in family[:2]]}).mappings()]
    files = [dict(row) for row in conn.execute(text("""
        SELECT c.oid::bigint,c.relfilenode::bigint,c.relname,c.reltablespace::bigint
        FROM pg_class c WHERE c.oid IN
          (SELECT to_regclass(name) FROM unnest(CAST(:names AS text[])) name)
           OR c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid IN
             (SELECT to_regclass(name) FROM unnest(CAST(:names AS text[])) name))
        ORDER BY c.oid
    """), {"names": list(relations)}).mappings()]
    result = dict(shapes=shapes, functions=functions, files=files,
                  old_headers=_json_row(conn, headers.STATE), old_raw=_json_row(conn, raw.STATE),
                  old_capture=_json_row(conn, capture.STATE), terminal=_json_row(conn, capture.CANCELLED),
                  keys=_json_row(conn, keys.STATE))
    if schema != keys.SCHEMA:
        if not isinstance(predecessor, dict) or set(predecessor) != {
                "operation_sha256", "terminal_sha256", "state_sha256", "relation_oid"}:
            raise RuntimeError("fact_header_forward_successor_binding_changed")
        relation = state_relation(conn, predecessor["operation_sha256"])
        previous = _json_row(conn, relation)
        if (_digest(previous) != predecessor["state_sha256"]
                or previous["terminal"] is None
                or _digest(previous["terminal"]) != predecessor["terminal_sha256"]
                or conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                               {"name": relation}) != predecessor["relation_oid"]):
            raise RuntimeError("fact_header_forward_successor_predecessor_changed")
        result["predecessor"] = predecessor
    if lookup_operation_sha256 is not None:
        from scripts.db import fact_header_forward_placement as lookup
        result["lookup_placement"] = lookup._completed(conn, lookup_operation_sha256)
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": state_name}) is not None:
        mode = conn.scalar(text("SELECT binding->>'raw_mapping_mode' FROM " + state_name + " WHERE id=1"))
        if mode is not None:
            if mode != "retain_source":
                raise RuntimeError("fact_header_forward_raw_mapping_mode_invalid")
            result["raw_mapping_mode"] = mode
    return result


def placement_binding(state):
    """Select the explicitly adopted physical receipt; preserve old copy meaning."""
    placed = state["binding"].get("lookup_placement")
    return placed["binding"]["placement"] if placed is not None else state["binding"]["old_headers"]["placement"]


def _install_family(conn, family, *, schema=keys.SCHEMA):
    source, target, columns, primary = FAMILIES[family]
    source_oid, target_oid = [conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                       {"name": relation}) for relation in (source, target)]
    new = ",".join("NEW." + name for name in columns)
    actual = ",".join("s." + name for name in columns)
    lookup = " AND ".join("s." + name + "=NEW." + name for name in primary)
    # Exact matching source is required even for direct target inserts. Native
    # uniqueness/FKs remain active; disagreement fails the source transaction.
    bodies = {
        "validate": f"""BEGIN
            IF TG_RELID <> {target_oid}::oid OR '{source}'::regclass::oid <> {source_oid}::oid
               OR NOT EXISTS(SELECT 1 FROM {source} s WHERE {lookup} AND
                   ROW({actual}) IS NOT DISTINCT FROM ROW({new})) THEN
                RAISE EXCEPTION 'fact_header_forward_{family}_source_mismatch';
            END IF;
            RETURN NEW;
        END;""",
        "mirror": f"""BEGIN
            IF TG_RELID <> {source_oid}::oid OR '{target}'::regclass::oid <> {target_oid}::oid THEN
                RAISE EXCEPTION 'fact_header_forward_{family}_relation_changed';
            END IF;
            INSERT INTO {target}({','.join(columns)}) VALUES({new}) ON CONFLICT DO NOTHING;
            IF NOT EXISTS(SELECT 1 FROM {target} s WHERE {lookup} AND
                ROW({actual}) IS NOT DISTINCT FROM ROW({new})) THEN
                RAISE EXCEPTION 'fact_header_forward_{family}_target_mismatch';
            END IF;
            RETURN NULL;
        END;""",
        "immutable": "BEGIN RAISE EXCEPTION 'fact_header_forward_adoption_immutable'; END;",
    }
    for action, body in bodies.items():
        function = schema + "." + family + "_" + action
        conn.exec_driver_sql("CREATE FUNCTION " + function + "() RETURNS trigger "
            "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$" + body + "$qt$")
        conn.exec_driver_sql("REVOKE ALL ON FUNCTION " + function + "() FROM PUBLIC")
    for relation, suffix, when, events, scope, action in (
        (target, "validate", "BEFORE", "INSERT", "ROW", "validate"),
        (source, "mirror", "AFTER", "INSERT", "ROW", "mirror"),
        (source, "source_seal", "BEFORE", "UPDATE OR DELETE OR TRUNCATE", "STATEMENT", "immutable"),
        (target, "target_seal", "BEFORE", "UPDATE OR DELETE OR TRUNCATE", "STATEMENT", "immutable"),
    ):
        trigger = conn.dialect.identifier_preparer.quote(_trigger_name(family, suffix))
        conn.exec_driver_sql(f"CREATE TRIGGER {trigger} {when} {events} ON {relation} FOR EACH {scope} "
                            f"EXECUTE FUNCTION {schema}.{family}_{action}()")
        conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}")


def _state(conn, operation_sha256=None):
    relation = state_relation(conn, operation_sha256)
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": relation}) is None:
        return None
    rows = conn.execute(text("SELECT * FROM " + relation + " LIMIT 2")).mappings().all()
    if len(rows) != 1 or rows[0]["id"] != 1 or "reference_progress" not in rows[0]:
        raise RuntimeError("fact_header_forward_adoption_state_changed")
    return dict(rows[0])


@contextmanager
def _step(conn, timeout_seconds):
    with capture._bounded_step(conn, timeout_seconds) as limit:
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                           {"name": CONTROLLER_LOCK}):
            raise RuntimeError("fact_header_forward_controller_active")
        yield limit


def _inspect(conn, operation_sha256, limit):
    state = _state(conn, operation_sha256)
    if state is None or state["operation_sha256"] != operation_sha256:
        raise RuntimeError("fact_header_forward_adoption_intent_changed")
    if state["terminal"] is not None:
        raise RuntimeError("fact_header_forward_adoption_retired")
    remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM " + state_relation(conn, operation_sha256) + " WHERE id=1"))
    if remaining <= 0:
        raise RuntimeError("fact_header_forward_adoption_expired")
    limit(float(remaining))
    return _inspect_guarded_state(conn, state)


def _inspect_guarded_state(conn, state):
    """Content/ownership evidence only; this helper grants no work lifetime."""
    if state["terminal"] is not None:
        raise RuntimeError("fact_header_forward_adoption_retired")
    if state["binding"] != _snapshot(conn, state["operation_sha256"]):
        raise RuntimeError("fact_header_forward_adoption_binding_changed")
    _reference_states(conn, state)
    if state["progress"]["identity_target"].get("scan") != IDENTITY_HEAP_SCAN:
        raise RuntimeError("fact_header_forward_adoption_scan_protocol_changed")
    scans = [state["progress"]["raw_" + direction].get("scan") for direction in ("source", "target")]
    if scans not in ([None, None], [RAW_HEAP_SCAN, RAW_HEAP_SCAN]):
        raise RuntimeError("fact_header_forward_adoption_raw_scan_protocol_changed")
    return state


def prepare_adoption(conn, *, expected_capture, cancellation_intent_sha256,
                     operation_sha256, attempt_seconds, timeout_seconds=30,
                     predecessor_operation_sha256=None, predecessor_terminal_sha256=None,
                     lookup_operation_sha256=None):
    """Fence briefly, mirror new source writes, then admit a separate bounded scan.

    Synchronous target writes need measured HDD/collection admission before any
    production use. The host must retire/reconcile this phase on failure/expiry;
    a stopped controller does not implicitly remove its native guards.
    """
    if (not isinstance(operation_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", operation_sha256)
            or operation_sha256 == cancellation_intent_sha256
            or type(attempt_seconds) is not int or not 30 <= attempt_seconds <= 345600):
        raise ValueError("fact_header_forward_adoption_request_invalid")
    if ((predecessor_operation_sha256 is None) != (predecessor_terminal_sha256 is None)
            or predecessor_operation_sha256 == operation_sha256
            or predecessor_terminal_sha256 is not None and (not isinstance(predecessor_terminal_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", predecessor_terminal_sha256))):
        raise ValueError("fact_header_forward_successor_request_invalid")
    if predecessor_operation_sha256 is not None:
        successor_schema(predecessor_operation_sha256)
    if lookup_operation_sha256 is not None:
        successor_schema(lookup_operation_sha256)
        if predecessor_operation_sha256 is None:
            raise ValueError("fact_header_forward_lookup_successor_required")
    with _step(conn, timeout_seconds) as limit:
        schema = operation_schema(conn, operation_sha256)
        relation = schema + ".adoption"
        if _state(conn, operation_sha256) is not None:
            state = _inspect(conn, operation_sha256, limit)
            bound_lookup = state["binding"].get("lookup_placement", {}).get("operation_sha256")
            if bound_lookup != lookup_operation_sha256:
                raise RuntimeError("fact_header_forward_lookup_intent_changed")
            bound = state["binding"].get("predecessor")
            if ((bound is None) != (predecessor_operation_sha256 is None)
                    or bound is not None and (bound["operation_sha256"] != predecessor_operation_sha256
                        or bound["terminal_sha256"] != predecessor_terminal_sha256)):
                raise RuntimeError("fact_header_forward_successor_intent_changed")
            if (state["attempt_seconds"] != attempt_seconds
                    or state["binding"]["terminal"]["receipt"]["intent_sha256"] != cancellation_intent_sha256
                    or state["binding"]["terminal"]["receipt"]["capture"] != expected_capture):
                raise RuntimeError("fact_header_forward_adoption_intent_changed")
            return _report(state, reused=True)
        predecessor = None
        if predecessor_operation_sha256 is not None:
            predecessor = _predecessor(conn, predecessor_operation_sha256, predecessor_terminal_sha256)
            schema = successor_schema(operation_sha256)
            relation = schema + ".adoption"
            if conn.scalar(text("SELECT to_regnamespace(:name)"), {"name": schema}) is not None:
                raise RuntimeError("fact_header_forward_successor_namespace_unowned")
        elif schema != keys.SCHEMA:
            raise RuntimeError("fact_header_forward_successor_retirement_required")
        from scripts.db import fact_header_forward_placement as lookup
        known_placement = lookup._row(conn)
        if (lookup_operation_sha256 is None and known_placement is not None
                and known_placement["completion"] is not None):
            raise RuntimeError("fact_header_forward_explicit_lookup_placement_required")
        if lookup_operation_sha256 is not None:
            placed = lookup.completed_receipt(conn, lookup_operation_sha256)
            if (placed["binding"]["predecessor_operation_sha256"] != predecessor_operation_sha256
                    or placed["binding"]["predecessor_terminal_sha256"] != predecessor_terminal_sha256):
                raise RuntimeError("fact_header_forward_lookup_predecessor_changed")
        conn.exec_driver_sql("LOCK TABLE " + headers.SOURCE + "," + raw.SOURCE + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        source_binding = keys._source_binding(conn, expected_capture, cancellation_intent_sha256)
        prepared = keys._read_state(conn)
        if (prepared is None or not prepared["complete"] or prepared["binding"] != source_binding
                or prepared["index_oids"] != keys.inspect_keys(conn)):
            raise RuntimeError("fact_header_forward_prepared_keys_required")
        raw._admit_source(conn)
        saved_raw = _json_row(conn, raw.STATE)
        actual_raw = raw._binding(conn)
        if any(saved_raw["binding"][name] != value for name, value in actual_raw.items() if name != "triggers"):
            raise RuntimeError("fact_header_forward_retained_raw_changed")
        conn.exec_driver_sql("LOCK TABLE " + IDENTITY + "," + raw.TARGET + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        if predecessor is not None:
            conn.exec_driver_sql("CREATE SCHEMA " + schema)
            conn.exec_driver_sql("REVOKE ALL ON SCHEMA " + schema + " FROM PUBLIC")
        conn.exec_driver_sql("CREATE TABLE " + relation + "(id integer PRIMARY KEY CHECK(id=1),"
            "operation_sha256 text NOT NULL,started_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,"
            "attempt_seconds integer NOT NULL CHECK(attempt_seconds BETWEEN 30 AND 345600),"
            "binding jsonb NOT NULL,progress jsonb NOT NULL,terminal jsonb,reference_progress jsonb NOT NULL,"
            "CHECK(expires_at=started_at+attempt_seconds*interval '1 second'))")
        if any(references._states(conn, references._native_inventory(conn, forward_header=True)).values()):
            raise RuntimeError("fact_header_forward_unowned_reference")
        progress = {}
        for family, (source, target, _, primary) in FAMILIES.items():
            _install_family(conn, family, schema=schema)
            for direction, relation in (("source", source), ("target", target)):
                if (family, direction) == ("identity", "target"):
                    _require_identity_heap(conn)
                    blocks = conn.scalar(text("SELECT pg_relation_size(CAST(:relation AS regclass)) / "
                        "current_setting('block_size')::bigint"), {"relation": target})
                    progress["identity_target"] = dict(scan=IDENTITY_HEAP_SCAN, high_block=blocks,
                        next_block=0, after_tid=None, complete=blocks == 0, verified=0)
                    continue
                high = conn.execute(text("SELECT " + ",".join(primary) + " FROM " + relation +
                    " ORDER BY " + ",".join(name + " DESC" for name in primary) + " LIMIT 1")).one_or_none()
                progress[family + "_" + direction] = dict(high=list(high) if high else None,
                    after=None, complete=high is None, verified=0)
        conn.execute(text("INSERT INTO " + schema + ".adoption SELECT 1,:operation,stamp,"
            "stamp+:seconds*interval '1 second',:seconds,CAST(:binding AS jsonb),CAST(:progress AS jsonb),NULL,'{}'::jsonb "
            "FROM (SELECT clock_timestamp() stamp) start"),
            {"operation": operation_sha256, "seconds": attempt_seconds,
             "binding": json.dumps(_snapshot(conn, schema=schema, predecessor=predecessor, lookup_operation_sha256=lookup_operation_sha256)), "progress": json.dumps(progress)})
        state = _inspect(conn, operation_sha256, limit)
        logger.info("fact_header_forward_adoption_prepared | operation=%s", operation_sha256)
        return _report(state, reused=False)


def adoption_page(conn, *, operation_sha256, page_rows=2048, timeout_seconds=30):
    """Verify retained identities in physical order, then fill source coverage.

    Source guards and synchronous mirrors cover inserts behind a committed scan
    cursor, including low sequence values. Cursors and target inserts commit in
    one transaction; a lost reply resumes the saved cursor, never a new clock.
    """
    if type(page_rows) is not int or not 1 <= page_rows <= 4096:
        raise ValueError("fact_header_forward_adoption_page_limit")
    with _step(conn, timeout_seconds) as limit:
        state = _inspect(conn, operation_sha256, limit)
        for family, (source, target, columns, primary) in FAMILIES.items():
            if family == "raw":
                if retains_raw_source(state):
                    continue
                if all(state["progress"]["raw_" + direction]["complete"] for direction in ("target", "source")):
                    continue
                _prepare_raw_heap_progress(conn, state)
                direction = "source" if state["progress"]["raw_target"]["complete"] else "target"
                rows = _raw_heap_page(conn, state["progress"]["raw_" + direction], direction, page_rows)
                _save_page(conn, state, family, direction, len(rows))
                return _report(state, reused=True, verified=len(rows))
            # Exact target content is established first. Its native insertion
            # guard and immutable seal preserve that proof while source coverage
            # uses only the target's ID index, not random historical heap reads.
            for direction in ("target", "source"):
                progress = state["progress"][family + "_" + direction]
                if progress["complete"]:
                    continue
                if (family, direction) == ("identity", "target"):
                    rows = _identity_heap_page(conn, progress, page_rows)
                    _save_page(conn, state, family, direction, len(rows))
                    return _report(state, reused=True, verified=len(rows))
                relation, peer = (source, target) if direction == "source" else (target, source)
                names = ",".join(primary)
                params = {"limit": page_rows, **{"high" + str(i): v for i, v in enumerate(progress["high"])}}
                bound = "ROW(" + names + ")<=ROW(" + ",".join(":high" + str(i) for i in range(len(primary))) + ")"
                if progress["after"] is not None:
                    params.update({"after" + str(i): v for i, v in enumerate(progress["after"])})
                    bound += " AND ROW(" + names + ")>ROW(" + ",".join(":after" + str(i) for i in range(len(primary))) + ")"
                rows = [dict(row) for row in conn.execute(text("SELECT " + ",".join(primary) +
                    " FROM " + relation + " WHERE " + bound + " ORDER BY " + names + " LIMIT :limit"), params).mappings()]
                wanted = [tuple(row[name] for name in primary) for row in rows]
                actual = {(value,): None for value in conn.execute(text(
                    "SELECT id FROM " + IDENTITY + " WHERE id=ANY(:ids)"),
                    {"ids": [key[0] for key in wanted]}).scalars()} if wanted else {}
                missing = [key for key in wanted if key not in actual]
                if missing:
                    selection, parameters = "id=ANY(:ids)", {"ids": [key[0] for key in missing]}
                    conn.execute(text("INSERT INTO " + target + "(" + ",".join(columns) + ") SELECT " +
                        ",".join(columns) + " FROM " + source + " WHERE " + selection + " ON CONFLICT DO NOTHING"), parameters)
                    actual.update(_read_rows(conn, family, peer, missing))
                # Retained rows already have an exact, sealed target proof. Read
                # full source metadata only for the identities this page filled;
                # INSERT SELECT and the following point read share source guards.
                compared = list(_read_rows(conn, family, source, missing).values()) if missing else []
                if len(compared) != len(missing):
                    raise RuntimeError("fact_header_forward_adoption_content_mismatch: identity:source")
                if any(actual.get(tuple(row[name] for name in primary)) != row for row in compared):
                    raise RuntimeError("fact_header_forward_adoption_content_mismatch: " + family + ":" + direction)
                if rows:
                    progress["after"] = [rows[-1][name] for name in primary]
                    progress["verified"] += len(rows)
                else:
                    progress["complete"] = True
                _save_page(conn, state, family, direction, len(rows))
                return _report(state, reused=True, verified=len(rows))
        return _report(state, reused=True)


def _save_page(conn, state, family, direction, rows):
    conn.execute(text("UPDATE " + state_relation(conn, state["operation_sha256"]) + " SET progress=CAST(:progress AS jsonb) WHERE id=1"),
                 {"progress": json.dumps(state["progress"])})
    logger.info("fact_header_forward_adoption_page | family=%s direction=%s rows=%s", family, direction, rows)


def _identity_heap_page(conn, progress, page_rows):
    """Read a bounded physical range; exact source lookups stay on its SSD key.

    The original heap extent and row cursor belong to the same atomic journal.
    The outer binding pins the heap OID/file and always-enabled guards before
    every page. Updates/deletes and table rewrites cannot preserve this proof.
    Later inserts, including free slots behind the cursor, already pass the
    exact-source native insertion guard and need no second baseline scan.
    """
    rows, after = _heap_rows(conn, IDENTITY, headers.IDENTITY_COLUMNS, progress, page_rows)
    actual = _read_rows(conn, "identity", headers.SOURCE, [(row["id"],) for row in rows]) if rows else {}
    if any(actual.get((row["id"],)) != row for row in rows):
        raise RuntimeError("fact_header_forward_adoption_content_mismatch: identity:target")
    _advance_heap(progress, rows, after, page_rows)
    return rows


def _heap_rows(conn, relation, columns, progress, page_rows):
    first = progress["next_block"]
    last = min(first + HEAP_BLOCKS_PER_PAGE, progress["high_block"])
    parameters = dict(lower=f"({first},0)", upper=f"({last},0)", limit=page_rows)
    bound = "ctid>=CAST(:lower AS tid) AND ctid<CAST(:upper AS tid)"
    if progress["after_tid"] is not None:
        parameters["after"] = progress["after_tid"]
        bound += " AND ctid>CAST(:after AS tid)"
    rows = [dict(row) for row in conn.execute(text("SELECT ctid::text AS tuple_id," +
        ",".join(columns) + " FROM " + relation + " WHERE " + bound +
        " ORDER BY ctid LIMIT :limit"), parameters).mappings()]
    after = rows[-1]["tuple_id"] if rows else None
    for row in rows:
        row.pop("tuple_id")
    return rows, after


def _advance_heap(progress, rows, after, page_rows):
    last = min(progress["next_block"] + HEAP_BLOCKS_PER_PAGE, progress["high_block"])
    progress["verified"] += len(rows)
    if len(rows) < page_rows:
        progress["next_block"] = last
        progress["after_tid"] = None
        progress["complete"] = last == progress["high_block"]
    else:
        progress["after_tid"] = after


def _prepare_raw_heap_progress(conn, state):
    """Start bounded raw scans inside the explicitly invoked adoption job.

    Guards have remained active since adoption, including across a worker change.
    Keep previous key cursors/counts and completed passes; take no new operation
    clock. Extents and the first verified page commit together. This is never
    invoked by an observer or application reader.
    """
    if state["progress"]["raw_source"].get("scan") == RAW_HEAP_SCAN:
        return
    for direction, relation in (("source", raw.SOURCE), ("target", raw.TARGET)):
        progress = state["progress"]["raw_" + direction]
        blocks = conn.scalar(text("SELECT pg_relation_size(CAST(:relation AS regclass)) / "
            "current_setting('block_size')::bigint"), {"relation": relation})
        progress.update(scan=RAW_HEAP_SCAN, high_block=blocks,
            next_block=blocks if progress["complete"] else 0, after_tid=None, heap_verified=0)
    logger.info("fact_header_forward_raw_heap_started | operation=%s prior_source_rows=%s prior_target_rows=%s",
        state["operation_sha256"], state["progress"]["raw_source"]["verified"],
        state["progress"]["raw_target"]["verified"])


def _raw_heap_page(conn, progress, direction, page_rows):
    """Prove retained content once, then fill missing keys in source heap order.

    Archive publication groups mappings in source heap pages. Keeping that order
    avoids scattering every insert batch across thousands of archive objects and
    HDD secondary-index leaves. Key-only coverage uses the existing target PK.
    Exact guards/comparisons, immutable seals and the original bindings still own
    correctness; locality improves I/O without assuming physical time ordering.
    """
    relation = raw.TARGET if direction == "target" else raw.SOURCE
    columns = raw.COLUMNS if direction == "target" else raw.KEYS
    rows, after = _heap_rows(conn, relation, columns, progress, page_rows)
    wanted = [tuple(row[name] for name in raw.KEYS) for row in rows]
    if direction == "target":
        actual = _read_rows(conn, "raw", raw.SOURCE, wanted) if wanted else {}
        compared = rows
    else:
        actual_keys = set(conn.execute(text("SELECT original.raw_record_id,original.manifest_id FROM " +
            raw.TARGET + " AS original JOIN " + raw._KEY_ROWS +
            " ON original.raw_record_id=wanted.raw_record_id AND original.manifest_id=wanted.manifest_id"),
            raw._key_parameters(wanted)).all()) if wanted else set()
        missing = [key for key in wanted if key not in actual_keys]
        if missing:
            conn.execute(text("INSERT INTO " + raw.TARGET + "(" + ",".join(raw.COLUMNS) + ") SELECT " +
                ",".join("original." + name for name in raw.COLUMNS) + " FROM " + raw.SOURCE +
                " AS original JOIN " + raw._ORDERED_KEY_ROWS +
                " ON original.raw_record_id=wanted.raw_record_id AND original.manifest_id=wanted.manifest_id"
                " ORDER BY wanted.ordinal ON CONFLICT DO NOTHING"), raw._key_parameters(missing))
        actual = _read_rows(conn, "raw", raw.TARGET, missing) if missing else {}
        compared = list(_read_rows(conn, "raw", raw.SOURCE, missing).values()) if missing else []
        if len(compared) != len(missing):
            raise RuntimeError("fact_header_forward_adoption_content_mismatch: raw:source")
    if any(actual.get(tuple(row[name] for name in raw.KEYS)) != row for row in compared):
        raise RuntimeError("fact_header_forward_adoption_content_mismatch: raw:" + direction)
    _advance_heap(progress, rows, after, page_rows)
    progress["heap_verified"] += len(rows)
    return rows


def _reference_states(conn, state):
    slots = references._native_inventory(conn, forward_header=True)
    current = references._states(conn, slots)
    saved = state["reference_progress"]
    if not isinstance(saved, dict):
        raise RuntimeError("fact_header_forward_reference_journal_changed")
    parent = saved.get(references.PARENT)
    for name, previous in saved.items():
        if current.get(name) != previous:
            raise RuntimeError("fact_header_forward_reference_changed: " + name)
    for name, actual in current.items():
        # A normal new payload day inherits the already-admitted parent FK.
        # It is checked natively, never silently adopted as an ordinary root.
        if actual is not None and name not in saved and not (
                parent and name.startswith(references.PARENT + "_")
                and actual["parent_oid"] == parent["oid"] and actual["convalidated"]):
            raise RuntimeError("fact_header_forward_unowned_reference: " + name)
    return slots, current


def _advance_references(conn, state, before, slots):
    after = references._states(conn, slots)
    actual = _snapshot(conn, state["operation_sha256"])
    changed = {item["oid"] for item in (*before.values(), *after.values()) if item is not None}
    expected = deepcopy(state["binding"])
    expected["functions"] = sorted(
        [item for item in expected["functions"] if item["constraint_oid"] not in changed]
        + [item for item in actual["functions"] if item["constraint_oid"] in changed],
        key=lambda item: (item["relation"], item["tgname"]))
    if after.get(headers.SOURCE) is not None:
        # Only the fixed composite FK may change the retained header shape.
        # Its columns, native enforcement and OID were checked by _states.
        shape = expected["shapes"][headers.SOURCE]
        shape[5] = sorted(
            [item for item in shape[5] if item[0] != references.STAGED]
            + [item for item in actual["shapes"][headers.SOURCE][5]
               if item[0] == references.STAGED], key=lambda item: item[0])
    if expected != actual:
        raise RuntimeError("fact_header_forward_reference_publication_changed")
    progress = {name: item for name, item in after.items() if item is not None}
    conn.execute(text("UPDATE " + state_relation(conn, state["operation_sha256"]) + " SET binding=CAST(:binding AS jsonb),"
        "reference_progress=CAST(:progress AS jsonb) WHERE id=1"),
        {"binding": json.dumps(expected), "progress": json.dumps(progress)})


def prepare_reference(conn, *, operation_sha256, relation, timeout_seconds=30):
    """Stage one fixed native FK; source constraints remain authoritative."""
    with _step(conn, timeout_seconds) as limit:
        conn.exec_driver_sql("LOCK TABLE " + headers.SOURCE + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        state = _inspect(conn, operation_sha256, limit)
        if not _report(state, reused=True)["retained_targets_verified"]:
            raise RuntimeError("fact_header_forward_adoption_incomplete")
        slots, before = _reference_states(conn, state)
        references._slot(slots, relation)
        conn.exec_driver_sql("LOCK TABLE " + references._qualified(conn, relation) +
                            " IN ACCESS EXCLUSIVE MODE NOWAIT")
        result = references._prepare_reference(conn, slots, relation)
        _advance_references(conn, state, before, slots)
        return result


def validate_reference(conn, *, operation_sha256, relation, timeout_seconds=30):
    """Validate one staged FK while ordinary source writes remain admitted."""
    with _step(conn, timeout_seconds) as limit:
        state = _inspect(conn, operation_sha256, limit)
        slots, before = _reference_states(conn, state)
        result = references._validate_reference(conn, slots, relation)
        _advance_references(conn, state, before, slots)
        return result


def adopt_payload_references(conn, *, operation_sha256, timeout_seconds=30):
    """Reuse prevalidated leaf FKs under the native payload parent."""
    with _step(conn, timeout_seconds) as limit:
        conn.exec_driver_sql("LOCK TABLE " + headers.SOURCE + ",ONLY " + references.PARENT +
                            " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        state = _inspect(conn, operation_sha256, limit)
        slots, before = _reference_states(conn, state)
        result = references._adopt_payload_references(conn, slots)
        _advance_references(conn, state, before, slots)
        return result


def inspect_references(conn, *, operation_sha256, timeout_seconds=30):
    with _step(conn, timeout_seconds) as limit:
        state = _inspect(conn, operation_sha256, limit)
        _, current = _reference_states(conn, state)
        return dict(references_complete=all(item and item["convalidated"] for item in current.values()),
                    references=current, migration_ready=False, final_switch_authorized=False)


@contextmanager
def verified_adoption(conn, *, operation_sha256, timeout_seconds=30):
    """Hold the exact completed adoption and references through an atomic switch.

    The caller owns host admission, stopped publishers and final phase clocks.
    No historical capture is resumed. An unfinished switch raises inside the
    savepoint, so even a caller catching the error cannot commit partial DDL.
    """
    active = "qt.forward_adoption.final_context"
    metadata = conn.info
    if metadata.get(active) is not None:
        raise RuntimeError("fact_header_forward_nested_switch")
    with _step(conn, timeout_seconds) as limit:
        relations = [relation for family in FAMILIES.values() for relation in family[:2]]
        relations += [references.PARENT, "market.fact_archive_material_aliases",
                      "market.fact_archive_canonical_dependencies", state_relation(conn, operation_sha256)]
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
        state = _inspect(conn, operation_sha256, limit)
        if not _report(state, reused=True)["retained_targets_verified"]:
            raise RuntimeError("fact_header_forward_adoption_incomplete")
        _, current = _reference_states(conn, state)
        if not all(item and item["convalidated"] for item in current.values()):
            raise RuntimeError("fact_header_forward_references_incomplete")
        context = dict(state=state, transaction=conn.scalar(text("SELECT txid_current()")), switched=False)
        metadata[active] = context
        try:
            yield context
            if (not context["switched"] or
                    context["transaction"] != conn.scalar(text("SELECT txid_current()"))):
                raise RuntimeError("fact_header_forward_atomic_switch_required")
        finally:
            metadata.pop(active, None)


def release_for_switch(conn):
    """Remove exactly the admitted mirrors inside the live final transaction."""
    context = conn.info.get("qt.forward_adoption.final_context")
    if (context is None or context["switched"] or
            context["transaction"] != conn.scalar(text("SELECT txid_current()"))):
        raise RuntimeError("fact_header_forward_live_switch_required")
    state = context["state"]
    if state["binding"] != _snapshot(conn, state["operation_sha256"]):
        raise RuntimeError("fact_header_forward_adoption_binding_changed")
    _reference_states(conn, state)
    for family, relation, suffix in _owned_triggers(state):
        trigger = conn.dialect.identifier_preparer.quote(_trigger_name(family, suffix))
        conn.exec_driver_sql("DROP TRIGGER " + trigger + " ON " + relation)
    return context


def inspect_retirement(conn, *, operation_sha256, timeout_seconds=10):
    """Reconcile one committed preserving retirement without replaying it.

    ACCESS SHARE protects the observed relation identities while source writes
    continue. The controller fence also excludes an idle owning worker. Absence
    is uncertainty, never permission to redispatch an earlier terminal intent.
    """
    from scripts.db import archive_root_v2_online as archives
    with _step(conn, timeout_seconds):
        state = _state(conn, operation_sha256)
        if state is None or state["operation_sha256"] != operation_sha256:
            raise RuntimeError("fact_header_forward_adoption_intent_changed")
        relations = [relation for family in FAMILIES.values() for relation in family[:2]]
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) + " IN ACCESS SHARE MODE NOWAIT")
        terminal = state["terminal"]
        if terminal is None:
            if state["binding"] != _snapshot(conn, state["operation_sha256"]):
                raise RuntimeError("fact_header_forward_adoption_binding_changed")
            return None
        from scripts.db import fact_header_forward_placement as lookup
        if (terminal.get("kind") == "switched"
                or terminal.get("operation_sha256") != operation_sha256
                or terminal.get("rows_preserved") is not True
                or lookup.retired_binding(conn, state) != _snapshot(conn, operation_sha256)
                or any(references._states(conn, references._native_inventory(conn, forward_header=True)).values())):
            raise RuntimeError("fact_header_forward_adoption_terminal_changed")
        archives._inspect_forward_cancellation(conn, operation_sha256=operation_sha256,
                                               receipt=terminal.get("archive_capture"))
        return deepcopy(terminal)


def retire_adoption(conn, *, operation_sha256, timeout_seconds=30, read_only_namespace=False):
    """Retire this phase's exact mirrors without deleting any retained rows.

    A separate bounded terminal transaction may run after the work deadline.
    It never renews that deadline or authorizes another adoption. Reconcile a
    lost commit reply against the durable terminal receipt and exact post-state.
    """
    from scripts.db import archive_root_v2_online as archives
    if type(read_only_namespace) is not bool:
        raise ValueError("fact_header_forward_retirement_namespace_mode_invalid")
    with _step(conn, timeout_seconds):
        state = _state(conn, operation_sha256)
        if state is None or state["operation_sha256"] != operation_sha256:
            raise RuntimeError("fact_header_forward_adoption_intent_changed")
        if state["terminal"] is not None and state["terminal"].get("kind") == "switched":
            raise RuntimeError("fact_header_forward_adoption_already_switched")
        relations = [relation for family in FAMILIES.values() for relation in family[:2]]
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        if state["terminal"] is not None:
            inspect_retirement(conn, operation_sha256=operation_sha256, timeout_seconds=timeout_seconds)
            return dict(retired=True, reused=True, source_authoritative=True,
                        migration_ready=False, final_switch_authorized=False)
        if state["binding"] != _snapshot(conn, state["operation_sha256"]):
            raise RuntimeError("fact_header_forward_adoption_binding_changed")
        archive_capture = archives._cancel_forward_capture(conn, state=state,
            **({"read_only_namespace": True} if read_only_namespace else {}))
        slots, staged = _reference_states(conn, state)
        roots = [name for name, item in staged.items() if item and not item["parent_oid"]]
        for name in sorted(roots):
            conn.exec_driver_sql("LOCK TABLE " + references._qualified(conn, name) +
                                " IN ACCESS EXCLUSIVE MODE NOWAIT")
        if _reference_states(conn, state) != (slots, staged):
            raise RuntimeError("fact_header_forward_reference_inventory_changed")
        # Removing the parent first removes only its inherited staged children.
        # Original source FKs survive, and mirrors remain until every staged FK is gone.
        roots.sort(key=lambda name: (name != references.PARENT, name))
        for name in roots:
            conn.exec_driver_sql("ALTER TABLE " + references._qualified(conn, name) +
                                " DROP CONSTRAINT " + references.STAGED)
        if any(references._states(conn, references._native_inventory(conn, forward_header=True)).values()):
            raise RuntimeError("fact_header_forward_reference_retirement_incomplete")
        removed_reference_oids = {item["oid"] for item in staged.values() if item}
        removed = []
        for family, relation, suffix in _owned_triggers(state):
            trigger = _trigger_name(family, suffix)
            conn.exec_driver_sql("DROP TRIGGER " + conn.dialect.identifier_preparer.quote(trigger) + " ON " + relation)
            removed.append([relation, trigger])
        expected = deepcopy(state["binding"])
        admitted_removed = [entry for entry in expected["functions"]
            if [entry["relation"], entry["tgname"]] in removed]
        expected["functions"] = [entry for entry in expected["functions"]
            if [entry["relation"], entry["tgname"]] not in removed
            and entry["constraint_oid"] not in removed_reference_oids]
        if staged.get(headers.SOURCE) is not None:
            shape = expected["shapes"][headers.SOURCE]
            shape[5] = [item for item in shape[5] if item[0] != references.STAGED]
        if len(admitted_removed) != len(removed) or expected != _snapshot(conn, operation_sha256):
            raise RuntimeError("fact_header_forward_adoption_retirement_changed")
        terminal = dict(operation_sha256=operation_sha256, binding=expected,
                        retired_at=conn.scalar(text("SELECT clock_timestamp()" )).isoformat(),
                        removed_triggers=removed, removed_reference_roots=roots, rows_preserved=True,
                        archive_capture=archive_capture)
        conn.execute(text("UPDATE " + state_relation(conn, operation_sha256) + " SET terminal=CAST(:terminal AS jsonb) WHERE id=1"),
                     {"terminal": json.dumps(terminal)})
        logger.info("fact_header_forward_adoption_retired | operation=%s triggers=%s", operation_sha256, len(removed))
        return dict(retired=True, reused=False, source_authoritative=True,
                    migration_ready=False, final_switch_authorized=False)


def _read_rows(conn, family, relation, wanted):
    # One indexed read per page, not one database round trip per retained row.
    if family == "identity":
        rows = conn.execute(text("SELECT " + ",".join(headers.IDENTITY_COLUMNS) + " FROM " + relation +
            " WHERE id=ANY(:ids)"), {"ids": [key[0] for key in wanted]}).mappings()
        return {(row["id"],): dict(row) for row in rows}
    return {tuple(row[name] for name in raw.KEYS): row for row in raw._rows_for_keys(conn, relation, wanted)}


def retains_raw_source(state):
    """The explicit amendment keeps the same canonical raw relation and schema."""
    mode = state["binding"].get("raw_mapping_mode")
    if mode not in (None, "retain_source"):
        raise RuntimeError("fact_header_forward_raw_mapping_mode_invalid")
    return mode == "retain_source"


def _owned_triggers(state):
    """One trigger inventory for both final switch and preserving retirement."""
    for family, (source, target, _, _) in FAMILIES.items():
        for relation, suffix in ((target, "validate"), (source, "mirror"),
                                 (source, "source_seal"), (target, "target_seal")):
            if family == "raw" and suffix == "mirror" and retains_raw_source(state):
                continue
            yield family, relation, suffix


def _retain_raw_source(conn, state):
    """Internal part of the stopped-worker request amendment, never a read repair.

    Preserve every cursor/count and all identity guards. Remove only the raw
    source mirror so collection stops maintaining the abandoned copy. Raw-copy proof
    is no longer required because that copy will never become authoritative.
    Identity mirrors remain continuous; ordinary retirement would break that proof.
    """
    if not all(state["progress"]["identity_" + side]["complete"] for side in ("source", "target")):
        raise RuntimeError("fact_header_forward_completed_identity_required")
    conn.exec_driver_sql("LOCK TABLE " + raw.SOURCE + "," + raw.TARGET + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    if _snapshot(conn, state["operation_sha256"]) != state["binding"]:
        raise RuntimeError("fact_header_forward_raw_retention_binding_changed")
    if retains_raw_source(state):
        raise RuntimeError("fact_header_forward_raw_retention_already_selected")
    mirror = _trigger_name("raw", "mirror")
    functions = [entry for entry in state["binding"]["functions"]
                 if (entry["relation"], entry["tgname"]) != (raw.SOURCE, mirror)]
    if len(functions) != len(state["binding"]["functions"]) - 1:
        raise RuntimeError("fact_header_forward_raw_mirror_changed")
    conn.exec_driver_sql("DROP TRIGGER " + mirror + " ON " + raw.SOURCE)
    binding = {**state["binding"], "functions": functions, "raw_mapping_mode": "retain_source"}
    conn.execute(text("UPDATE " + state_relation(conn, state["operation_sha256"]) +
        " SET binding=CAST(:binding AS jsonb) WHERE id=1"), {"binding": json.dumps(binding)})
    if _snapshot(conn, state["operation_sha256"]) != binding:
        raise RuntimeError("fact_header_forward_raw_retention_binding_changed")


def _report(state, *, reused, verified=0):
    required = ("identity_source", "identity_target") if retains_raw_source(state) else tuple(state["progress"])
    return dict(retained_targets_verified=all(state["progress"][name]["complete"] for name in required),
                verified_page_rows=verified, reused=reused, source_authoritative=True,
                migration_ready=False, final_switch_authorized=False)

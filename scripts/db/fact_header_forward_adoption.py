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
IDENTITY_BLOCKS_PER_PAGE = 128
IDENTITY = capture.SCHEMA + ".fact_identities"
# Native FK AFTER triggers are named RI_ConstraintTrigger_*. This quoted
# uppercase name mirrors identities first, without deferring FK enforcement.
IDENTITY_MIRROR = "AAA_qt_forward_identity_mirror"

FAMILIES = {
    "identity": (headers.SOURCE, IDENTITY, headers.IDENTITY_COLUMNS, ("id",)),
    "raw": (raw.SOURCE, raw.TARGET, raw.COLUMNS, raw.KEYS),
}
logger = logging.getLogger(__name__)


def _trigger_name(family, suffix):
    if (family, suffix) == ("identity", "mirror"):
        return IDENTITY_MIRROR
    return "trg_qt_forward_" + family + "_" + suffix


def _json_row(conn, relation):
    rows = conn.execute(text("SELECT to_jsonb(t) FROM " + relation + " t LIMIT 2")).scalars().all()
    if len(rows) != 1 or rows[0].get("id") != 1:
        raise RuntimeError("fact_header_forward_adoption_original_state_changed: " + relation)
    return rows[0]


def _snapshot(conn):
    # Fixed objects only. The old journals and queues remain evidence. Target
    # contents may gain missing source rows; their definitions/files may not drift.
    relations = (headers.SOURCE, IDENTITY, raw.SOURCE, raw.TARGET,
                 headers.STATE, raw.STATE, capture.STATE, capture.QUEUE,
                 raw.QUEUE, capture.CANCELLED, keys.STATE, STATE)
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
    return dict(shapes=shapes, functions=functions, files=files,
                old_headers=_json_row(conn, headers.STATE), old_raw=_json_row(conn, raw.STATE),
                old_capture=_json_row(conn, capture.STATE), terminal=_json_row(conn, capture.CANCELLED),
                keys=_json_row(conn, keys.STATE))


def _install_family(conn, family):
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
        function = keys.SCHEMA + "." + family + "_" + action
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
                            f"EXECUTE FUNCTION {keys.SCHEMA}.{family}_{action}()")
        conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}")


def _state(conn):
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is None:
        return None
    rows = conn.execute(text("SELECT * FROM " + STATE + " LIMIT 2")).mappings().all()
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
    state = _state(conn)
    if state is None or state["operation_sha256"] != operation_sha256:
        raise RuntimeError("fact_header_forward_adoption_intent_changed")
    if state["terminal"] is not None:
        raise RuntimeError("fact_header_forward_adoption_retired")
    remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM " + STATE + " WHERE id=1"))
    if remaining <= 0:
        raise RuntimeError("fact_header_forward_adoption_expired")
    limit(float(remaining))
    if state["binding"] != _snapshot(conn):
        raise RuntimeError("fact_header_forward_adoption_binding_changed")
    _reference_states(conn, state)
    if state["progress"]["identity_target"].get("scan") != IDENTITY_HEAP_SCAN:
        raise RuntimeError("fact_header_forward_adoption_scan_protocol_changed")
    return state


def prepare_adoption(conn, *, expected_capture, cancellation_intent_sha256,
                     operation_sha256, attempt_seconds, timeout_seconds=30):
    """Fence briefly, mirror new source writes, then admit a separate bounded scan.

    Synchronous target writes need measured HDD/collection admission before any
    production use. The host must retire/reconcile this phase on failure/expiry;
    a stopped controller does not implicitly remove its native guards.
    """
    if (not isinstance(operation_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", operation_sha256)
            or operation_sha256 == cancellation_intent_sha256
            or type(attempt_seconds) is not int or not 30 <= attempt_seconds <= 345600):
        raise ValueError("fact_header_forward_adoption_request_invalid")
    with _step(conn, timeout_seconds) as limit:
        if _state(conn) is not None:
            state = _inspect(conn, operation_sha256, limit)
            if (state["attempt_seconds"] != attempt_seconds
                    or state["binding"]["terminal"]["receipt"]["intent_sha256"] != cancellation_intent_sha256
                    or state["binding"]["terminal"]["receipt"]["capture"] != expected_capture):
                raise RuntimeError("fact_header_forward_adoption_intent_changed")
            return _report(state, reused=True)
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
        conn.exec_driver_sql("CREATE TABLE " + STATE + "(id integer PRIMARY KEY CHECK(id=1),"
            "operation_sha256 text NOT NULL,started_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,"
            "attempt_seconds integer NOT NULL CHECK(attempt_seconds BETWEEN 30 AND 345600),"
            "binding jsonb NOT NULL,progress jsonb NOT NULL,terminal jsonb,reference_progress jsonb NOT NULL,"
            "CHECK(expires_at=started_at+attempt_seconds*interval '1 second'))")
        if any(references._states(conn, references._native_inventory(conn, forward_header=True)).values()):
            raise RuntimeError("fact_header_forward_unowned_reference")
        progress = {}
        for family, (source, target, _, primary) in FAMILIES.items():
            _install_family(conn, family)
            for direction, relation in (("source", source), ("target", target)):
                if (family, direction) == ("identity", "target"):
                    if not conn.scalar(text("SELECT c.relkind='r' AND c.relpersistence='p' "
                        "AND a.amname='heap' FROM pg_class c JOIN pg_am a ON a.oid=c.relam "
                        "WHERE c.oid=CAST(:relation AS regclass)"), {"relation": target}):
                        raise RuntimeError("fact_header_forward_identity_heap_required")
                    blocks = conn.scalar(text("SELECT pg_relation_size(CAST(:relation AS regclass)) / "
                        "current_setting('block_size')::bigint"), {"relation": target})
                    progress["identity_target"] = dict(scan=IDENTITY_HEAP_SCAN, high_block=blocks,
                        next_block=0, after_tid=None, complete=blocks == 0, verified=0)
                    continue
                high = conn.execute(text("SELECT " + ",".join(primary) + " FROM " + relation +
                    " ORDER BY " + ",".join(name + " DESC" for name in primary) + " LIMIT 1")).one_or_none()
                progress[family + "_" + direction] = dict(high=list(high) if high else None,
                    after=None, complete=high is None, verified=0)
        conn.execute(text("INSERT INTO " + STATE + " SELECT 1,:operation,stamp,"
            "stamp+:seconds*interval '1 second',:seconds,CAST(:binding AS jsonb),CAST(:progress AS jsonb),NULL,'{}'::jsonb "
            "FROM (SELECT clock_timestamp() stamp) start"),
            {"operation": operation_sha256, "seconds": attempt_seconds,
             "binding": json.dumps(_snapshot(conn)), "progress": json.dumps(progress)})
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
            # Exact target content is established first. Its native insertion
            # guard and immutable seal preserve that proof while source coverage
            # uses only the target's ID index, not random historical heap reads.
            directions = ("target", "source") if family == "identity" else ("source", "target")
            for direction in directions:
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
                rows = [dict(row) for row in conn.execute(text("SELECT " + ",".join(columns) +
                    " FROM " + relation + " WHERE " + bound + " ORDER BY " + names + " LIMIT :limit"), params).mappings()]
                wanted = [tuple(row[name] for name in primary) for row in rows]
                identity_coverage = family == "identity" and direction == "source"
                if identity_coverage:
                    actual = {(value,): None for value in conn.execute(text(
                        "SELECT id FROM " + IDENTITY + " WHERE id=ANY(:ids)"),
                        {"ids": [key[0] for key in wanted]}).scalars()} if wanted else {}
                else:
                    actual = _read_rows(conn, family, peer, wanted) if wanted else {}
                missing = [key for key in wanted if key not in actual]
                if missing and direction == "source":
                    if family == "identity":
                        selection, parameters = "id=ANY(:ids)", {"ids": [key[0] for key in missing]}
                    else:
                        selection = "(raw_record_id,manifest_id) IN (SELECT raw_record_id,manifest_id FROM " + raw._KEY_ROWS + ")"
                        parameters = raw._key_parameters(missing)
                    conn.execute(text("INSERT INTO " + target + "(" + ",".join(columns) + ") SELECT " +
                        ",".join(columns) + " FROM " + source + " WHERE " + selection + " ON CONFLICT DO NOTHING"), parameters)
                    actual.update(_read_rows(conn, family, peer, missing))
                missing_keys = set(missing)
                compared = [row for row in rows if (row["id"],) in missing_keys] if identity_coverage else rows
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
    conn.execute(text("UPDATE " + STATE + " SET progress=CAST(:progress AS jsonb) WHERE id=1"),
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
    first = progress["next_block"]
    last = min(first + IDENTITY_BLOCKS_PER_PAGE, progress["high_block"])
    parameters = dict(lower=f"({first},0)", upper=f"({last},0)", limit=page_rows)
    bound = "ctid>=CAST(:lower AS tid) AND ctid<CAST(:upper AS tid)"
    if progress["after_tid"] is not None:
        parameters["after"] = progress["after_tid"]
        bound += " AND ctid>CAST(:after AS tid)"
    rows = [dict(row) for row in conn.execute(text("SELECT ctid::text AS tuple_id," +
        ",".join(headers.IDENTITY_COLUMNS) + " FROM " + IDENTITY + " WHERE " + bound +
        " ORDER BY ctid LIMIT :limit"), parameters).mappings()]
    after = rows[-1]["tuple_id"] if rows else None
    for row in rows:
        row.pop("tuple_id")
    actual = _read_rows(conn, "identity", headers.SOURCE, [(row["id"],) for row in rows]) if rows else {}
    if any(actual.get((row["id"],)) != row for row in rows):
        raise RuntimeError("fact_header_forward_adoption_content_mismatch: identity:target")
    progress["verified"] += len(rows)
    if len(rows) < page_rows:
        progress["next_block"] = last
        progress["after_tid"] = None
        progress["complete"] = last == progress["high_block"]
    else:
        progress["after_tid"] = after
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
    actual = _snapshot(conn)
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
    conn.execute(text("UPDATE " + STATE + " SET binding=CAST(:binding AS jsonb),"
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


def retire_adoption(conn, *, operation_sha256, timeout_seconds=30):
    """Retire this phase's exact mirrors without deleting any retained rows.

    A separate bounded terminal transaction may run after the work deadline.
    It never renews that deadline or authorizes another adoption. Reconcile a
    lost commit reply against the durable terminal receipt and exact post-state.
    """
    with _step(conn, timeout_seconds):
        state = _state(conn)
        if state is None or state["operation_sha256"] != operation_sha256:
            raise RuntimeError("fact_header_forward_adoption_intent_changed")
        relations = [relation for family in FAMILIES.values() for relation in family[:2]]
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        if state["terminal"] is not None:
            terminal = state["terminal"]
            if (terminal["operation_sha256"] != operation_sha256 or terminal["binding"] != _snapshot(conn)
                    or any(references._states(conn, references._native_inventory(conn, forward_header=True)).values())):
                raise RuntimeError("fact_header_forward_adoption_terminal_changed")
            return dict(retired=True, reused=True, source_authoritative=True,
                        migration_ready=False, final_switch_authorized=False)
        if state["binding"] != _snapshot(conn):
            raise RuntimeError("fact_header_forward_adoption_binding_changed")
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
        for family, (source, target, _, _) in FAMILIES.items():
            for relation, suffix in ((target, "validate"), (source, "mirror"),
                                     (source, "source_seal"), (target, "target_seal")):
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
        if len(admitted_removed) != len(removed) or expected != _snapshot(conn):
            raise RuntimeError("fact_header_forward_adoption_retirement_changed")
        terminal = dict(operation_sha256=operation_sha256, binding=expected,
                        retired_at=conn.scalar(text("SELECT clock_timestamp()" )).isoformat(),
                        removed_triggers=removed, removed_reference_roots=roots, rows_preserved=True)
        conn.execute(text("UPDATE " + STATE + " SET terminal=CAST(:terminal AS jsonb) WHERE id=1"),
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


def _report(state, *, reused, verified=0):
    return dict(retained_targets_verified=all(p["complete"] for p in state["progress"].values()),
                verified_page_rows=verified, reused=reused, source_authoritative=True,
                migration_ready=False, final_switch_authorized=False)

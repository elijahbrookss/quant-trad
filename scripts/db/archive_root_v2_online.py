"""Committed archive change capture and bounded preserving background pages.

The catalog source stays authoritative. Queue closure is not filesystem proof,
publisher drain, root activation or switch authority. Original deadlines apply.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
import logging

from sqlalchemy import text

from scripts.db import archive_root_v2_copy as archives, fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import SCHEMA

logger = logging.getLogger(__name__)
STATE = SCHEMA + ".archive_online_capture"
PROGRESS = SCHEMA + ".archive_online_progress"
QUEUE = SCHEMA + ".pending_archive_objects"
CLOSED = SCHEMA + ".archive_online_closed"
TRIGGER = "trg_qt_archive_v2_capture"
GUARD = "trg_qt_archive_v2_reject_change"
_FUNCTION = SCHEMA + ".capture_archive_insert"


@dataclass(frozen=True)
class _Capture:
    schema: str
    trigger: str
    guard: str

    @property
    def state(self):
        return self.schema + ".archive_online_capture"

    @property
    def progress(self):
        return self.schema + ".archive_online_progress"

    @property
    def queue(self):
        return self.schema + ".pending_archive_objects"

    @property
    def closed(self):
        return self.schema + ".archive_online_closed"

    @property
    def function(self):
        return self.schema + ".capture_archive_insert"

    @property
    def reject_function(self):
        return self.schema + ".reject_fact_source_change"


def _capture(forward_operation_sha256=None, *, conn=None):
    if forward_operation_sha256 is None:
        return _Capture(SCHEMA, TRIGGER, GUARD)
    if (not isinstance(forward_operation_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", forward_operation_sha256)):
        raise ValueError("archive_forward_operation_invalid")
    from scripts.db import fact_header_forward_adoption as adoption
    if conn is None:
        raise ValueError("archive_forward_operation_connection_required")
    forward_schema = adoption.operation_schema(conn, forward_operation_sha256)
    return _Capture(forward_schema, "trg_qt_forward_archive_capture", "trg_qt_forward_archive_reject_change")


def _roots(conn, source_root, destination_root, *, read_only_namespace=False, saved=None):
    if saved is None:
        saved = headers._inspect_progress(conn, **({"read_only_namespace":True} if read_only_namespace else {}))["placement"]
    if saved is None:
        raise RuntimeError("archive_online_fixed_placement_required")
    source, source_id = archives._root(source_root, saved["recent_device"])
    destination, destination_id = archives._root(destination_root, saved["history_device"])
    return {"source": [str(source), *source_id], "destination": [str(destination), *destination_id]}


def _binding(conn, forward_operation_sha256=None):
    owner = _capture(forward_operation_sha256, conn=conn)
    if forward_operation_sha256 is None:
        result = {"capture_oid": conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                             {"name": SCHEMA + ".capture"}),
                  "prepared_at": str(conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture WHERE id=1")))}
    else:
        from scripts.db import fact_header_forward_adoption as adoption
        state = adoption._state(conn, forward_operation_sha256)
        result = {"operation_sha256": state["operation_sha256"],
                  "adoption_oid": conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                               {"name": adoption.state_relation(conn, forward_operation_sha256)}),
                  "started_at": state["started_at"].isoformat(),
                  "expires_at": state["expires_at"].isoformat()}
    for family in archives.FAMILIES:
        relation = "market."+family
        properties = raw._relation(conn, relation)
        if (properties[1:3] != ["r", "p"] or properties[5:7] != [False, False]
                or conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE "
                    "inhrelid=to_regclass(:name) OR inhparent=to_regclass(:name)) OR "
                    "EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=to_regclass(:name))"),
                    {"name": relation})):
            raise RuntimeError("archive_online_catalog_layout_unsupported: "+family)
        triggers = conn.execute(text("""
            SELECT to_jsonb(t),pg_get_triggerdef(t.oid),to_jsonb(p),pg_get_functiondef(p.oid)
            FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
            WHERE t.tgrelid=to_regclass(:name) ORDER BY t.tgname
        """), {"name": relation}).all()
        result[family] = {"shape": headers._shape(conn, family, schema="market"),
                          "properties": properties, "triggers": [list(row) for row in triggers]}
    result["private_shapes"] = {
        name: headers._shape(conn, name.split(".")[1], schema=owner.schema)
        for name in (owner.state, owner.progress, owner.queue)}
    return json.loads(json.dumps(result))


def _require_open(conn, owner):
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": owner.closed}) is not None:
        raise RuntimeError("archive_online_capture_closed")


def _inspect(conn, source_root, destination_root, *, read_only_namespace=False,
             forward_operation_sha256=None, saved=None):
    owner = _capture(forward_operation_sha256, conn=conn)
    if forward_operation_sha256 is not None and saved is None:
        raise RuntimeError("archive_forward_admitted_placement_required")
    _require_open(conn, owner)
    row = conn.execute(text(f"SELECT * FROM {owner.state} WHERE id=1")).mappings().one()
    if row["roots"] != _roots(conn, source_root, destination_root, read_only_namespace=read_only_namespace, saved=saved) or row["binding"] != _binding(conn, forward_operation_sha256):
        raise RuntimeError("archive_online_capture_binding_changed")
    if set(conn.execute(text(f"SELECT family FROM {owner.progress}")).scalars()) != set(archives.FAMILIES):
        raise RuntimeError("archive_online_progress_families_changed")
    return row


def prepare(conn, *, source_root, destination_root, timeout_seconds=30, forward_operation_sha256=None):
    """Install capture and a finite baseline at one short catalog writer fence."""
    owner = _capture(forward_operation_sha256, conn=conn)
    with archives._operation_step(conn, timeout_seconds,
            forward_operation_sha256=forward_operation_sha256) as (saved, _):
        _require_open(conn, owner)
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": owner.state}) is not None:
            _inspect(conn, source_root, destination_root,
                     forward_operation_sha256=forward_operation_sha256, saved=saved)
            return {"reused": True, "migration_ready": False}
        # The raw shadow's FK installs internal triggers on its manifest
        # catalog. Prepare that dependency before sealing the exact trigger set.
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": raw.STATE}) is None:
            raise RuntimeError("archive_online_raw_shadow_preparation_required")
        if forward_operation_sha256 is None:
            raw._inspect(conn)
        relations = ",".join("market."+name for name in archives.FAMILIES)
        conn.exec_driver_sql("LOCK TABLE "+relations+" IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        roots = _roots(conn, source_root, destination_root, saved=saved)
        if any(conn.scalar(text("SELECT to_regclass(:name)"), {"name": name}) is not None
               for name in (owner.progress, owner.queue)):
            raise RuntimeError("archive_online_unregistered_capture")
        conn.exec_driver_sql(f"CREATE TABLE {owner.state}(id integer PRIMARY KEY CHECK(id=1),"
                             "roots jsonb NOT NULL,binding jsonb NOT NULL)")
        conn.exec_driver_sql(f"CREATE TABLE {owner.progress}(family text PRIMARY KEY,high_id text,"
            "after_id text NOT NULL DEFAULT '',baseline_complete boolean NOT NULL,"
            "verified_objects bigint NOT NULL DEFAULT 0 CHECK(verified_objects>=0))")
        conn.exec_driver_sql(f"CREATE TABLE {owner.queue}(family text NOT NULL,id text NOT NULL,"
                             "PRIMARY KEY(family,id))")
        cases = []
        for family in archives.FAMILIES:
            oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                              {"name": "market."+family})
            cases.append(f"WHEN {oid}::oid THEN '{family}'")
        conn.exec_driver_sql(f"""
            CREATE FUNCTION {owner.function}() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$
            DECLARE family text;
            BEGIN
                family := CASE TG_RELID {" ".join(cases)} ELSE NULL END;
                IF family IS NULL OR TG_OP <> 'INSERT' THEN
                    RAISE EXCEPTION 'archive_online_unexpected_source';
                END IF;
                INSERT INTO {owner.queue}(family,id) VALUES(family,NEW.id)
                    ON CONFLICT DO NOTHING;
                RETURN NULL;
            END;
            $qt$
        """)
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {owner.function}() FROM PUBLIC")
        if forward_operation_sha256 is not None:
            from scripts.db.fact_header_v2_capture import _REJECT_BODY
            conn.exec_driver_sql(f"CREATE FUNCTION {owner.reject_function}() RETURNS trigger "
                "LANGUAGE plpgsql SET search_path=pg_catalog AS $qt$" + _REJECT_BODY + "$qt$")
            conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {owner.reject_function}() FROM PUBLIC")
        for family in archives.FAMILIES:
            relation = "market."+family
            conn.exec_driver_sql(f"CREATE TRIGGER {owner.trigger} AFTER INSERT ON {relation} "
                                f"FOR EACH ROW EXECUTE FUNCTION {owner.function}()")
            conn.exec_driver_sql(f"CREATE TRIGGER {owner.guard} BEFORE UPDATE OR DELETE OR TRUNCATE ON {relation} "
                                f"FOR EACH STATEMENT EXECUTE FUNCTION {owner.reject_function}()")
            for trigger in (owner.trigger, owner.guard):
                conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}")
            high = conn.scalar(text(f"SELECT id FROM {relation} ORDER BY id DESC LIMIT 1"))
            conn.execute(text(f"INSERT INTO {owner.progress}(family,high_id,baseline_complete) "
                              "VALUES(:family,:high,:empty)"),
                         {"family": family, "high": high, "empty": high is None})
        conn.execute(text(f"INSERT INTO {owner.state} VALUES(1,CAST(:roots AS jsonb),CAST(:binding AS jsonb))"),
                     {"roots": json.dumps(roots), "binding": json.dumps(_binding(conn, forward_operation_sha256))})
        _inspect(conn, source_root, destination_root,
                     forward_operation_sha256=forward_operation_sha256, saved=saved)
        logger.info("archive_online_capture_prepared | families=%s", len(archives.FAMILIES))
        return {"reused": False, "migration_ready": False}


def copy_page(engine, *, family, source_root, destination_root, page_rows=128, tail_only=False,
              forward_operation_sha256=None, **kwargs):
    """Copy one finite baseline/tail page and commit queue retirement with it.

    Uses the existing capacity/WAL/expiry/deadline/cancellation guards and exact
    file publication. A lost reply reuses durable files; failed SQL leaves the
    cursor/queue intact. Retention expiry can skip a catalog object legitimately
    absent on the source. It does not delete any destination object.
    """
    if family not in archives.FAMILIES:
        raise ValueError("archive_copy_known_family_required")
    if type(tail_only) is not bool:
        raise ValueError("archive_online_tail_mode_invalid")
    selection = {}

    def select(conn, selected_family, unused_after, limit):
        owner = _capture(forward_operation_sha256, conn=conn)
        saved = None
        if forward_operation_sha256 is not None:
            from scripts.db import fact_header_forward_adoption as adoption
            saved = adoption._state(conn, forward_operation_sha256)["binding"]["old_headers"]["placement"]
        _inspect(conn, source_root, destination_root,
                 forward_operation_sha256=forward_operation_sha256, saved=saved)
        state = dict(conn.execute(text(f"SELECT * FROM {owner.progress} WHERE family=:family"),
                                  {"family": family}).mappings().one())
        if tail_only and not state["baseline_complete"]:
            raise RuntimeError("archive_online_tail_requires_completed_baseline")
        if state["baseline_complete"]:
            ids = conn.execute(text(f"SELECT id FROM {owner.queue} WHERE family=:family "
                                    "ORDER BY id LIMIT :limit FOR UPDATE"),
                               {"family": family, "limit": limit}).scalars().all()
        else:
            ids = conn.execute(text(f"SELECT id FROM market.{family} "
                                    "WHERE id>:after AND id<=:high ORDER BY id LIMIT :limit"),
                               {"after": state["after_id"], "high": state["high_id"],
                                "limit": limit}).scalars().all()
        all_rows = conn.execute(text(f"SELECT id,object_key,object_sha256,byte_count "
                                    f"FROM market.{family} WHERE id=ANY(:ids) ORDER BY id"),
                                {"ids": ids}).mappings().all() if ids else []
        if {row["id"] for row in all_rows} != set(ids):
            raise RuntimeError("archive_online_captured_source_missing")
        kind = archives.FAMILIES[family]
        expired = set(conn.execute(text("""
            SELECT DISTINCT target_id FROM market.storage_lifecycle_events
            WHERE action='archive_expire' AND event_type='completed'
              AND target_kind=:kind AND target_id=ANY(:ids)
        """), {"kind": kind, "ids": ids}).scalars()) if ids and kind is not None else set()
        rows = [row for row in all_rows if row["id"] not in expired]
        archives._validate_descriptors(rows)
        selection.update(owner=owner, state=state, ids=ids, expired=len(expired))
        return rows

    def record(conn, rows):
        owner = _capture(forward_operation_sha256, conn=conn)
        if owner != selection["owner"]:
            raise RuntimeError("archive_forward_operation_owner_changed")
        state, ids = selection["state"], selection["ids"]
        conn.execute(text(f"DELETE FROM {owner.queue} WHERE family=:family AND id=ANY(:ids)"),
                     {"family": family, "ids": ids})
        if not state["baseline_complete"]:
            conn.execute(text(f"UPDATE {owner.progress} SET after_id=:after,baseline_complete=:complete "
                              "WHERE family=:family"),
                         {"after": ids[-1] if ids else state["after_id"],
                          "complete": not ids, "family": family})
        conn.execute(text(f"UPDATE {owner.progress} SET verified_objects=verified_objects+:count "
                          "WHERE family=:family"), {"count": len(rows), "family": family})
        current = conn.execute(text(f"SELECT * FROM {owner.progress} WHERE family=:family"),
                               {"family": family}).mappings().one()
        empty = not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {owner.queue} WHERE family=:family)"),
                                {"family": family})
        return {"selected_catalog_rows": len(ids), "expired_catalog_rows": selection["expired"],
                "baseline_complete": current["baseline_complete"],
                "next_after_id": current["after_id"], "captured_tail_empty_at_observation": empty,
                "verified_objects_so_far": current["verified_objects"],
                "final_switch_authorized": False}

    return archives._copy_archive_page(engine, select_page=select, record_page=record,
        family=family, source_root=source_root, destination_root=destination_root,
        page_rows=page_rows, forward_operation_sha256=forward_operation_sha256, **kwargs)


def retire_capture(conn, *, source_root, destination_root, timeout_seconds=30, forward_operation_sha256=None):
    """Remove temporary triggers inside the live verified inventory transaction.

    The caller must include this in the final switch transaction. Any abort
    restores capture; successful commit retains state/progress/queue and a
    terminal receipt, refusing preparation/copy retries of the closed attempt.
    This is neither publisher drain, root activation nor collection permission.
    """
    owner = _capture(forward_operation_sha256, conn=conn)
    context = conn.info.get("qt.archive_inventory_context.v2")
    if (context is None or context["transaction"] is not conn.get_transaction()
            or not context["transaction"].is_active
            or context.get("forward_operation_sha256") != forward_operation_sha256
            or str(source_root) != context["source_root"]
            or str(destination_root) != context["destination_root"]):
        raise RuntimeError("archive_online_live_inventory_context_required")
    with archives._operation_step(conn, timeout_seconds,
            forward_operation_sha256=forward_operation_sha256) as (saved, _):
        state = dict(_inspect(conn, source_root, destination_root,
                     forward_operation_sha256=forward_operation_sha256, saved=saved))
        progress = [dict(row) for row in conn.execute(
            text(f"SELECT * FROM {owner.progress} ORDER BY family")).mappings()]
        if (any(not row["baseline_complete"] for row in progress)
                or conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {owner.queue})"))):
            raise RuntimeError("archive_online_capture_not_closed")
        # Exact inventory holds all catalog publication locks until caller
        # commit. No queued late commit can race trigger removal.
        receipt = {"capture": state, "progress": progress,
                   "inventory": dict(context["report"]),
                   "source_retained": True, "root_activation_authorized": False}
        _close(conn, owner, receipt)
        logger.info("archive_online_capture_retired | families=%s source_retained=true",
                    len(archives.FAMILIES))
        return receipt


def _close(conn, owner, receipt):
    conn.exec_driver_sql(f"CREATE TABLE {owner.closed}(id integer PRIMARY KEY CHECK(id=1),"
                         "closed_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
                         "receipt jsonb NOT NULL)")
    for family in archives.FAMILIES:
        for trigger in (owner.trigger, owner.guard):
            conn.exec_driver_sql(f"DROP TRIGGER {trigger} ON market.{family}")
    conn.exec_driver_sql(f"DROP FUNCTION {owner.function}()")
    conn.execute(text(f"INSERT INTO {owner.closed}(id,receipt) VALUES(1,CAST(:receipt AS jsonb))"),
                 {"receipt": json.dumps(receipt)})


def _cancel_forward_capture(conn, *, state, read_only_namespace=False):
    """Part of the adoption owner's bounded, preserving terminal transaction.

    No inventory or empty queue is required for cancellation. Every queued row,
    copied file and journal survives; only this owner's admitted triggers stop.
    """
    if type(read_only_namespace) is not bool:
        raise ValueError("archive_forward_terminal_namespace_mode_invalid")
    placement_options = {"read_only_namespace": True} if read_only_namespace else {}
    operation = state["operation_sha256"]
    owner = _capture(operation, conn=conn)
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": owner.state}) is None:
        return None
    conn.exec_driver_sql("LOCK TABLE " + ",".join("market." + name for name in archives.FAMILIES)
                         + " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    saved = state["binding"]["old_headers"]["placement"]
    archives.physical.verify(conn, saved, **placement_options)
    row = conn.execute(text(f"SELECT * FROM {owner.state} WHERE id=1")).mappings().one()
    source, destination = row["roots"]["source"][0], row["roots"]["destination"][0]
    _inspect(conn, source, destination, forward_operation_sha256=operation, saved=saved, **placement_options)
    receipt = dict(kind="cancelled", operation_sha256=operation, capture=dict(row),
                   source_retained=True, queued_rows_retained=True, root_activation_authorized=False)
    _close(conn, owner, receipt)
    # Bind only changes made under the exact catalog writer fence above.
    receipt["post_binding"] = _binding(conn, operation)
    conn.execute(text(f"UPDATE {owner.closed} SET receipt=CAST(:receipt AS jsonb) WHERE id=1"),
                 {"receipt": json.dumps(receipt)})
    return receipt


def _inspect_forward_cancellation(conn, *, operation_sha256, receipt):
    owner = _capture(operation_sha256, conn=conn)
    if receipt is None:
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": owner.state}) is not None:
            raise RuntimeError("archive_forward_terminal_changed")
        return
    actual = conn.execute(text(f"SELECT receipt FROM {owner.closed} WHERE id=1")).scalar_one()
    if actual != receipt or _binding(conn, operation_sha256) != receipt["post_binding"]:
        raise RuntimeError("archive_forward_terminal_changed")

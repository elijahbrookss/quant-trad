"""Committed archive change capture and bounded preserving background pages.

The catalog source stays authoritative. Queue closure is not filesystem proof,
publisher drain, root activation or switch authority. Original deadlines apply.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import text

from scripts.db import archive_root_v2_copy as archives, fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import SCHEMA, migration_step

logger = logging.getLogger(__name__)
STATE = SCHEMA + ".archive_online_capture"
PROGRESS = SCHEMA + ".archive_online_progress"
QUEUE = SCHEMA + ".pending_archive_objects"
TRIGGER = "trg_qt_archive_v2_capture"
GUARD = "trg_qt_archive_v2_reject_change"
_FUNCTION = SCHEMA + ".capture_archive_insert"


def _roots(conn, source_root, destination_root):
    saved = headers._inspect_progress(conn)["placement"]
    if saved is None:
        raise RuntimeError("archive_online_fixed_placement_required")
    source, source_id = archives._root(source_root, saved["recent_device"])
    destination, destination_id = archives._root(destination_root, saved["history_device"])
    return {"source": [str(source), *source_id], "destination": [str(destination), *destination_id]}


def _binding(conn):
    result = {"capture_oid": conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                         {"name": SCHEMA+".capture"}),
              "prepared_at": str(conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture WHERE id=1")))}
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
        name: headers._shape(conn, name.split(".")[1]) for name in (STATE, PROGRESS, QUEUE)}
    return json.loads(json.dumps(result))


def _inspect(conn, source_root, destination_root):
    row = conn.execute(text(f"SELECT * FROM {STATE} WHERE id=1")).mappings().one()
    if row["roots"] != _roots(conn, source_root, destination_root) or row["binding"] != _binding(conn):
        raise RuntimeError("archive_online_capture_binding_changed")
    if set(conn.execute(text(f"SELECT family FROM {PROGRESS}")).scalars()) != set(archives.FAMILIES):
        raise RuntimeError("archive_online_progress_families_changed")
    return row


def prepare(conn, *, source_root, destination_root, timeout_seconds=30):
    """Install capture and a finite baseline at one short catalog writer fence."""
    with migration_step(conn, timeout_seconds):
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is not None:
            _inspect(conn, source_root, destination_root)
            return {"reused": True, "migration_ready": False}
        relations = ",".join("market."+name for name in archives.FAMILIES)
        conn.exec_driver_sql("LOCK TABLE "+relations+" IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        roots = _roots(conn, source_root, destination_root)
        if any(conn.scalar(text("SELECT to_regclass(:name)"), {"name": name}) is not None
               for name in (PROGRESS, QUEUE)):
            raise RuntimeError("archive_online_unregistered_capture")
        conn.exec_driver_sql(f"CREATE TABLE {STATE}(id integer PRIMARY KEY CHECK(id=1),"
                             "roots jsonb NOT NULL,binding jsonb NOT NULL)")
        conn.exec_driver_sql(f"CREATE TABLE {PROGRESS}(family text PRIMARY KEY,high_id text,"
            "after_id text NOT NULL DEFAULT '',baseline_complete boolean NOT NULL,"
            "verified_objects bigint NOT NULL DEFAULT 0 CHECK(verified_objects>=0))")
        conn.exec_driver_sql(f"CREATE TABLE {QUEUE}(family text NOT NULL,id text NOT NULL,"
                             "PRIMARY KEY(family,id))")
        cases = []
        for family in archives.FAMILIES:
            oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                              {"name": "market."+family})
            cases.append(f"WHEN {oid}::oid THEN '{family}'")
        conn.exec_driver_sql(f"""
            CREATE FUNCTION {_FUNCTION}() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$
            DECLARE family text;
            BEGIN
                family := CASE TG_RELID {" ".join(cases)} ELSE NULL END;
                IF family IS NULL OR TG_OP <> 'INSERT' THEN
                    RAISE EXCEPTION 'archive_online_unexpected_source';
                END IF;
                INSERT INTO {QUEUE}(family,id) VALUES(family,NEW.id)
                    ON CONFLICT DO NOTHING;
                RETURN NULL;
            END;
            $qt$
        """)
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {_FUNCTION}() FROM PUBLIC")
        for family in archives.FAMILIES:
            relation = "market."+family
            conn.exec_driver_sql(f"CREATE TRIGGER {TRIGGER} AFTER INSERT ON {relation} "
                                f"FOR EACH ROW EXECUTE FUNCTION {_FUNCTION}()")
            conn.exec_driver_sql(f"CREATE TRIGGER {GUARD} BEFORE UPDATE OR DELETE OR TRUNCATE ON {relation} "
                                f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_fact_source_change()")
            for trigger in (TRIGGER, GUARD):
                conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}")
            high = conn.scalar(text(f"SELECT id FROM {relation} ORDER BY id DESC LIMIT 1"))
            conn.execute(text(f"INSERT INTO {PROGRESS}(family,high_id,baseline_complete) "
                              "VALUES(:family,:high,:empty)"),
                         {"family": family, "high": high, "empty": high is None})
        conn.execute(text(f"INSERT INTO {STATE} VALUES(1,CAST(:roots AS jsonb),CAST(:binding AS jsonb))"),
                     {"roots": json.dumps(roots), "binding": json.dumps(_binding(conn))})
        _inspect(conn, source_root, destination_root)
        logger.info("archive_online_capture_prepared | families=%s", len(archives.FAMILIES))
        return {"reused": False, "migration_ready": False}


def copy_page(engine, *, family, source_root, destination_root, page_rows=128, **kwargs):
    """Copy one finite baseline/tail page and commit queue retirement with it.

    Uses the existing capacity/WAL/expiry/deadline/cancellation guards and exact
    file publication. A lost reply reuses durable files; failed SQL leaves the
    cursor/queue intact. Retention expiry can skip a catalog object legitimately
    absent on the source. It does not delete any destination object.
    """
    if family not in archives.FAMILIES:
        raise ValueError("archive_copy_known_family_required")
    selection = {}

    def select(conn, selected_family, unused_after, limit):
        _inspect(conn, source_root, destination_root)
        state = dict(conn.execute(text(f"SELECT * FROM {PROGRESS} WHERE family=:family"),
                                  {"family": family}).mappings().one())
        if state["baseline_complete"]:
            ids = conn.execute(text(f"SELECT id FROM {QUEUE} WHERE family=:family "
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
        selection.update(state=state, ids=ids, expired=len(expired))
        return rows

    def record(conn, rows):
        state, ids = selection["state"], selection["ids"]
        conn.execute(text(f"DELETE FROM {QUEUE} WHERE family=:family AND id=ANY(:ids)"),
                     {"family": family, "ids": ids})
        if not state["baseline_complete"]:
            conn.execute(text(f"UPDATE {PROGRESS} SET after_id=:after,baseline_complete=:complete "
                              "WHERE family=:family"),
                         {"after": ids[-1] if ids else state["after_id"],
                          "complete": not ids, "family": family})
        conn.execute(text(f"UPDATE {PROGRESS} SET verified_objects=verified_objects+:count "
                          "WHERE family=:family"), {"count": len(rows), "family": family})
        current = conn.execute(text(f"SELECT * FROM {PROGRESS} WHERE family=:family"),
                               {"family": family}).mappings().one()
        empty = not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE} WHERE family=:family)"),
                                {"family": family})
        return {"selected_catalog_rows": len(ids), "expired_catalog_rows": selection["expired"],
                "baseline_complete": current["baseline_complete"],
                "next_after_id": current["after_id"], "captured_tail_empty_at_observation": empty,
                "verified_objects_so_far": current["verified_objects"],
                "final_switch_authorized": False}

    return archives._copy_archive_page(engine, select_page=select, record_page=record,
        family=family, source_root=source_root, destination_root=destination_root,
        page_rows=page_rows, **kwargs)

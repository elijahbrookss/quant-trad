"""Bounded, resumable shadow copy for the fixed tiered-v1 header upgrade.

Internal operator primitive; no CLI, cutover, readiness certificate or runtime
wiring. The source remains authoritative. Final source re-admission, verified disk
placement, capacity limits and the final dependency handoff remain prerequisites
for a production orchestrator. This stage never deletes source records.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import date, timedelta
from functools import lru_cache
import json
import logging
from time import monotonic

from sqlalchemy import MetaData, bindparam, inspect, text
from sqlalchemy.dialects.postgresql import insert

from portal.backend.db import Base, MarketFactVersionRecord
from scripts.db.fact_header_v2_capture import (
    SCHEMA, SOURCE, QUEUE, install_capture, inspect_capture,
    install_identity_capture, inspect_identity_capture, migration_step,
)
from scripts.db.fact_header_v2_admission import assert_v1_source_admission
from scripts.db import fact_header_v2_placement as physical

STATE = SCHEMA + ".copy_progress"
TABLE_NAMES = ("fact_identities", "fact_header_partitions", "fact_header_series_days", "fact_versions")
HEADER_COLUMNS = tuple(column.name for column in MarketFactVersionRecord.__table__.columns)
IDENTITY_COLUMNS = ("id", "storage_day", "series_id", "observation_key", "revision")
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _tables():
    # These are fixed code-defined SQLAlchemy declarations, never observed database
    # state. Rebuilding the entire model graph for each small page dominated the
    # physical rehearsal. Catalog/content/placement admission still runs per page.
    metadata = MetaData()
    selected = {"market."+name for name in TABLE_NAMES}
    for key, table in Base.metadata.tables.items():
        if key not in selected:
            table.to_metadata(metadata)
    def referred(table, schema, constraint, referred_schema):
        return SCHEMA if constraint.elements[0].target_fullname.rsplit(".", 1)[0] in selected else referred_schema
    return {name: Base.metadata.tables["market."+name].to_metadata(
        metadata, schema=SCHEMA, referred_schema_fn=referred) for name in TABLE_NAMES}


def _source_columns(conn):
    columns = inspect(conn).get_columns("fact_versions", schema="market")
    actual = {c["name"]:(str(c["type"].compile(dialect=conn.dialect)),c["nullable"]) for c in columns}
    expected = {c.name:(str(c.type.compile(dialect=conn.dialect)),c.nullable)
                for c in MarketFactVersionRecord.__table__.columns}
    if actual != expected:
        raise RuntimeError("fact_header_copy_source_columns_changed")
    index = conn.scalar(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_am am ON am.oid=c.relam
            WHERE i.indrelid='market.fact_versions'::regclass
              AND c.relname='ix_market_fact_storage_page'
              AND i.indisvalid AND i.indisready AND NOT i.indisunique
              AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname='btree'
              AND i.indnkeyatts=3 AND i.indoption::text='0 0 0'
              AND pg_get_indexdef(i.indexrelid,1,true)='storage_day'
              AND pg_get_indexdef(i.indexrelid,2,true)='market_commit_seq'
              AND pg_get_indexdef(i.indexrelid,3,true)='id')
    """))
    if not index:
        raise RuntimeError("fact_header_copy_bounded_source_index_required")


def _shape(conn, name, *, schema=SCHEMA):
    relation = schema+"."+name
    oid, kind, persistence, partkey = conn.execute(text("""
        SELECT oid::bigint,relkind,relpersistence,pg_get_partkeydef(oid)
        FROM pg_class WHERE oid=to_regclass(:name)
    """), {"name":relation}).one()
    columns = [tuple(row) for row in conn.execute(text("""
        SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
               pg_get_expr(d.adbin,d.adrelid)
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum
    """), {"oid":oid})]
    constraints = [tuple(row) for row in conn.execute(text("""
        SELECT conname,contype,convalidated,condeferrable,pg_get_constraintdef(oid)
        FROM pg_constraint WHERE conrelid=:oid ORDER BY conname
    """), {"oid":oid})]
    indexes = [tuple(row) for row in conn.execute(text("""
        SELECT pg_get_indexdef(indexrelid),indisvalid,indisready
        FROM pg_index WHERE indrelid=:oid ORDER BY indexrelid
    """), {"oid":oid})]
    return json.loads(json.dumps([oid,kind,persistence,partkey,columns,constraints,indexes]))


def _inspect_progress(conn):
    inspect_capture(conn)
    _source_columns(conn)
    state = dict(conn.execute(text(f"SELECT * FROM {STATE} WHERE id=1")).mappings().one())
    if state["identity_capture"]:
        inspect_identity_capture(conn)
    elif conn.scalar(text("""
        SELECT to_regprocedure(:function) IS NOT NULL OR EXISTS(
            SELECT 1 FROM pg_trigger WHERE tgrelid='market.fact_versions'::regclass
            AND tgname='trg_qt_header_v2_capture_identity')
    """),{"function":SCHEMA+".capture_fact_identity()"}):
        raise RuntimeError("fact_header_copy_unrecorded_identity_capture")
    if state["targets"] != {name:_shape(conn,name) for name in TABLE_NAMES}:
        raise RuntimeError("fact_header_copy_shadow_definition_changed")
    state["_placement_pid"]=None
    if state["placement"] is not None:
        state["_placement_pid"]=physical.verify(conn,state["placement"])
        for name in (*TABLE_NAMES,"pending_fact_ids","capture","copy_progress"):
            physical.verify_group(conn,SCHEMA+"."+name,history=name=="fact_identities" and state["identity_history_ready"],
                                  saved=state["placement"],pid=state["_placement_pid"])
    return state


def prepare_copy(conn, *, placement=None, timeout_seconds=30):
    """Create a private target atomically; retry never resets copied progress."""
    with migration_step(conn, timeout_seconds), (physical.tablespace(conn,"") if placement is not None else nullcontext()):
        binding=physical.observe(conn,placement)[0] if placement is not None else None
        install_capture(conn)
        if binding is not None:
            physical.verify_group(conn,SOURCE,history=False,saved=binding,pid=physical.verify(conn,binding))
        _source_columns(conn)
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name":STATE}) is not None:
            state = _inspect_progress(conn)
            if state["placement"]!=binding:
                raise RuntimeError("fact_header_copy_placement_cannot_change")
            assert_v1_source_admission(conn,identity_capture=state["identity_capture"])
            return _report(conn, state, verified=0, reused=True)
        tables = _tables()
        for name in TABLE_NAMES:
            if conn.scalar(text("SELECT to_regclass(:name)"), {"name":SCHEMA+"."+name}) is not None:
                raise RuntimeError("fact_header_copy_unregistered_shadow")
            # Build the private identity indexes on SSD before bulk relocation.
            tables[name].create(conn)
        assert_v1_source_admission(conn)
        conn.exec_driver_sql(f"""
            CREATE TABLE {STATE}(
                id integer PRIMARY KEY CHECK(id=1),
                high_day date,high_seq bigint,high_id text,
                after_day date,after_seq bigint,after_id text,
                baseline_complete boolean NOT NULL,
                identity_capture boolean NOT NULL DEFAULT false,
                identity_history_ready boolean NOT NULL DEFAULT false,
                verified_rows bigint NOT NULL DEFAULT 0 CHECK(verified_rows>=0),
                targets jsonb NOT NULL,
                placement jsonb,
                CHECK((high_day IS NULL)=(high_seq IS NULL) AND (high_day IS NULL)=(high_id IS NULL)),
                CHECK((after_day IS NULL)=(after_seq IS NULL) AND (after_day IS NULL)=(after_id IS NULL)))
        """)
        high = conn.execute(text(f"""
            SELECT storage_day,market_commit_seq,id FROM {SOURCE}
            ORDER BY storage_day DESC,market_commit_seq DESC,id DESC LIMIT 1
        """)).one_or_none()
        conn.execute(text(f"""
            INSERT INTO {STATE}(id,high_day,high_seq,high_id,baseline_complete,targets,placement)
            VALUES(1,:day,:seq,:identity,:empty,CAST(:targets AS jsonb),CAST(:placement AS jsonb))
        """), {"day":high[0] if high else None,"seq":high[1] if high else None,
               "identity":high[2] if high else None,"empty":high is None,
               "targets":json.dumps({name:_shape(conn,name) for name in TABLE_NAMES}),
               "placement":json.dumps(binding) if binding is not None else None})
        logger.info("fact_header_v2_shadow_prepared | source=%s", SOURCE)
        return _report(conn, _inspect_progress(conn), verified=0, reused=False)


def _partition(conn, day, *, placement=None, pid=None):
    name = "fact_versions_"+day.strftime("%Y%m%d")
    relation = SCHEMA+"."+name
    bound = f"FOR VALUES FROM ('{day.isoformat()}') TO ('{(day+timedelta(days=1)).isoformat()}')"
    recorded = conn.scalar(text(f"SELECT 1 FROM {SCHEMA}.fact_header_partitions WHERE storage_day=:day"),{"day":day})
    existing = conn.execute(text("""
        SELECT c.relkind,c.relpersistence,pg_get_expr(c.relpartbound,c.oid),i.inhparent::regclass::text
        FROM pg_class c LEFT JOIN pg_inherits i ON i.inhrelid=c.oid
        WHERE c.oid=to_regclass(:name)
    """),{"name":relation}).one_or_none()
    history=bool(placement and day<date.fromisoformat(placement["plan"]["history_before"]))
    if recorded:
        if existing is None or tuple(existing)!=("r","p",bound,SCHEMA+".fact_versions"):
            raise RuntimeError("fact_header_copy_shadow_partition_changed")
        if placement:
            physical.verify_group(conn,relation,history=history,saved=placement,pid=pid)
        return
    if existing is not None:
        raise RuntimeError("fact_header_copy_shadow_partition_unregistered")
    space=placement["history_name"] if history else "pg_default"
    with (physical.tablespace(conn,space) if placement else nullcontext()):
        clause=" TABLESPACE "+conn.dialect.identifier_preparer.quote(space) if placement else ""
        conn.exec_driver_sql(f"CREATE TABLE {relation} PARTITION OF {SCHEMA}.fact_versions {bound}{clause}")
    if placement:
        physical.verify_group(conn,relation,history=history,saved=placement,pid=pid)
    conn.execute(text(f"INSERT INTO {SCHEMA}.fact_header_partitions(storage_day) VALUES(:day)"),{"day":day})


def _copy_rows(conn, rows, *, placement=None, pid=None):
    if not rows:
        return
    for day in sorted({row["storage_day"] for row in rows}):
        _partition(conn,day,placement=placement,pid=pid)
    tables = _tables()
    conn.execute(insert(tables["fact_identities"]).on_conflict_do_nothing(),
                 [{name:row[name] for name in IDENTITY_COLUMNS} for row in rows])
    ids = [row["id"] for row in rows]
    identities = {row["id"]:dict(row) for row in conn.execute(text(f"""
        SELECT {",".join(IDENTITY_COLUMNS)} FROM {SCHEMA}.fact_identities WHERE id=ANY(:ids)
    """),{"ids":ids}).mappings()}
    if any(identities.get(row["id"]) != {name:row[name] for name in IDENTITY_COLUMNS} for row in rows):
        raise RuntimeError("fact_header_copy_identity_mismatch")
    conn.execute(insert(tables["fact_versions"]).on_conflict_do_nothing(),rows)
    copied = {row["id"]:dict(row) for row in conn.execute(text(f"""
        SELECT {",".join(HEADER_COLUMNS)} FROM {SCHEMA}.fact_versions
        WHERE storage_day=ANY(:days) AND id=ANY(:ids)
    """),{"days":list({row["storage_day"] for row in rows}),"ids":ids}).mappings()}
    if any(copied.get(row["id"]) != row for row in rows):
        raise RuntimeError("fact_header_copy_full_row_mismatch")
    ranges = {}
    for row in rows:
        key = row["series_id"],row["storage_day"]
        stamp = row["observation_time"]
        lower,upper = ranges.get(key,(stamp,stamp))
        ranges[key] = min(lower,stamp),max(upper,stamp)
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.fact_header_series_days AS bounds
            (series_id,storage_day,min_observation_time,max_observation_time)
        VALUES(:series,:day,:lower,:upper)
        ON CONFLICT(series_id,storage_day) DO UPDATE SET
            min_observation_time=LEAST(bounds.min_observation_time,EXCLUDED.min_observation_time),
            max_observation_time=GREATEST(bounds.max_observation_time,EXCLUDED.max_observation_time)
    """), [{"series":key[0],"day":key[1],"lower":value[0],"upper":value[1]} for key,value in ranges.items()])
    # A queue entry retires only in the transaction that verified its full row.
    conn.execute(text(f"DELETE FROM {QUEUE} WHERE id=ANY(:ids)"),{"ids":ids})


def copy_page(conn, *, page_rows=128, timeout_seconds=30):
    """Copy a bounded baseline or captured page, including its durable cursor."""
    if type(page_rows) is not int or not 1 <= page_rows <= 4096:
        raise ValueError("fact_header_copy_page_rows_out_of_bounds")
    with migration_step(conn, timeout_seconds):
        state = _inspect_progress(conn)
        if not state["baseline_complete"]:
            predicate = "(storage_day,market_commit_seq,id) <= (:high_day,:high_seq,:high_id)"
            if state["after_day"] is not None:
                predicate += " AND (storage_day,market_commit_seq,id) > (:after_day,:after_seq,:after_id)"
            rows = [dict(row) for row in conn.execute(text(f"""
                SELECT {",".join(HEADER_COLUMNS)} FROM {SOURCE} WHERE {predicate}
                ORDER BY storage_day,market_commit_seq,id LIMIT :limit
            """), {**state,"limit":page_rows}).mappings()]
        else:
            # Keep the query bounded using the queue PK and source global ID PK.
            queued = conn.execute(text(f"SELECT id FROM {QUEUE} ORDER BY id LIMIT :limit FOR UPDATE"),
                                  {"limit":page_rows}).scalars().all()
            rows = [dict(row) for row in conn.execute(text(f"""
                SELECT {",".join(HEADER_COLUMNS)} FROM {SOURCE} WHERE id=ANY(:ids)
            """),{"ids":queued}).mappings()] if queued else []
            if {row["id"] for row in rows} != set(queued):
                raise RuntimeError("fact_header_copy_captured_source_missing")
        _copy_rows(conn,rows,placement=state["placement"],pid=state["_placement_pid"])
        if state["placement"]:
            pid=physical.verify(conn,state["placement"])
            for day in sorted({row["storage_day"] for row in rows}):
                physical.verify_group(conn,SCHEMA+".fact_versions_"+day.strftime("%Y%m%d"),
                    history=day<date.fromisoformat(state["placement"]["plan"]["history_before"]),
                    saved=state["placement"],pid=pid)
        if not state["baseline_complete"]:
            if rows:
                last = rows[-1]
                conn.execute(text(f"""
                    UPDATE {STATE} SET after_day=:day,after_seq=:seq,after_id=:identity WHERE id=1
                """),{"day":last["storage_day"],"seq":last["market_commit_seq"],"identity":last["id"]})
            else:
                conn.exec_driver_sql(f"UPDATE {STATE} SET baseline_complete=true WHERE id=1")
        conn.execute(text(f"UPDATE {STATE} SET verified_rows=verified_rows+:count WHERE id=1"),{"count":len(rows)})
        state = dict(conn.execute(text(f"SELECT * FROM {STATE} WHERE id=1")).mappings().one())
        logger.info("fact_header_v2_shadow_page_verified | rows=%s baseline_complete=%s",
                    len(rows),state["baseline_complete"])
        return _report(conn,state,verified=len(rows),reused=True)


def place_identity_on_history(conn, *, timeout_seconds=30):
    """Relocate the completed private identity/index copy before source mirroring.

    Source writers remain independent until identity capture is enabled. The
    operator budgets the temporary SSD copy and the original attempt deadline.
    Physical relocation and its progress flag commit or roll back together.
    """
    with migration_step(conn, timeout_seconds):
        state = _inspect_progress(conn)
        if state["placement"] is None or state["identity_history_ready"]:
            return {"identity_history_ready":state["identity_history_ready"],"reused":True}
        if not state["baseline_complete"]:
            raise RuntimeError("fact_header_identity_history_baseline_incomplete")
        if state["identity_capture"]:
            raise RuntimeError("fact_header_identity_history_capture_already_active")
        target = SCHEMA+".fact_identities"
        conn.exec_driver_sql(f"LOCK TABLE {target} IN ACCESS EXCLUSIVE MODE NOWAIT")
        indexes = conn.execute(text("""
            SELECT n.nspname,c.relname FROM pg_index i
            JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE i.indrelid=to_regclass(:target) ORDER BY c.oid
        """),{"target":target}).all()
        quote = conn.dialect.identifier_preparer.quote_identifier
        destination = quote(state["placement"]["history_name"])
        conn.exec_driver_sql(f"ALTER TABLE {target} SET TABLESPACE {destination}")
        for schema,name in indexes:
            conn.exec_driver_sql(f"ALTER INDEX {quote(schema)}.{quote(name)} SET TABLESPACE {destination}")
        conn.exec_driver_sql(f"UPDATE {STATE} SET identity_history_ready=true WHERE id=1")
        _inspect_progress(conn)
        return {"identity_history_ready":True,"reused":False}


def enable_identity_capture(conn, *, page_rows=128, timeout_seconds=30):
    """Finish bounded catch-up and mirror new IDs under a short writer fence.

    Baseline partitions must already exist. Mirroring before backfill would let
    long source writers block creation of private header partitions. A busy
    writer or excess backlog requires another ordinary copy pass and retry.
    """
    if type(page_rows) is not int or not 1 <= page_rows <= 4096:
        raise ValueError("fact_header_copy_page_rows_out_of_bounds")
    with migration_step(conn, timeout_seconds):
        state = _inspect_progress(conn)
        if not state["baseline_complete"]:
            raise RuntimeError("fact_header_identity_capture_baseline_required")
        # Autovacuum does not maintain partition-parent statistics. Without
        # them the ordered verification can repeatedly sort the remaining table.
        conn.exec_driver_sql(f"ANALYZE {SCHEMA}.fact_versions, {SCHEMA}.fact_identities")
        # Relocate before the source-writer fence, never while mirroring writes.
        place_identity_on_history(conn,timeout_seconds=timeout_seconds)
        conn.exec_driver_sql("LOCK TABLE market.fact_versions IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        state=_inspect_progress(conn)
        if not state["baseline_complete"]:
            raise RuntimeError("fact_header_identity_capture_baseline_required")
        assert_v1_source_admission(conn,identity_capture=state["identity_capture"])
        if state["identity_capture"]:
            return {**_report(conn,state,verified=0,reused=True),"identity_capture_active":True}
        pending=conn.execute(text(f"SELECT id FROM {QUEUE} ORDER BY id LIMIT :limit"),
                             {"limit":page_rows+1}).scalars().all()
        if len(pending)>page_rows:
            raise RuntimeError("fact_header_identity_capture_backlog_exceeds_fence_budget")
        report=copy_page(conn,page_rows=page_rows)
        if not report["caught_up_at_observation"]:
            raise RuntimeError("fact_header_identity_capture_catchup_incomplete")
        install_identity_capture(conn)
        conn.exec_driver_sql(f"UPDATE {STATE} SET identity_capture=true WHERE id=1")
        state=_inspect_progress(conn)
        assert_v1_source_admission(conn,identity_capture=True)
        logger.info("fact_header_v2_identity_capture_enabled | source=%s", SOURCE)
        return {**_report(conn,state,verified=report["verified_page_rows"],reused=False),
                "identity_capture_active":True}


def _report(conn, state, *, verified, reused):
    pending = conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE})"))
    return {"schema_version":"qt.fact_header_shadow_copy.v1","verified_page_rows":verified,
            "baseline_complete":state["baseline_complete"],"capture_pending":pending,
            "caught_up_at_observation":state["baseline_complete"] and not pending,
            "migration_ready":False,"source_authoritative":True,"reused":reused,
            "physical_placement_configured":state["placement"] is not None}


def _verified_pages(conn, *, source, target, columns, keys, page_rows, label):
    """Compare two fixed internal relations exactly, with bounded client memory.

    Callers own both relation locks and the cumulative migration deadline.
    Names come only from this module/the fixed raw-lookup module, never UI input.
    Equality is field-by-field; counts or hashes alone cannot certify a copy.
    """
    after = None
    while True:
        params = {"limit": page_rows}
        predicate = ""
        if after is not None:
            params.update({f"after_{i}": value for i, value in enumerate(after)})
            predicate = f"WHERE ({','.join(keys)}) > ({','.join(':after_'+str(i) for i in range(len(keys)))})"
        pages = [conn.execute(text(
            f"SELECT {','.join(columns)} FROM {relation} {predicate} "
            f"ORDER BY {','.join(keys)} LIMIT :limit"), params).mappings().all()
            for relation in (source, target)]
        if pages[0] != pages[1]:
            raise RuntimeError("fact_header_handoff_content_mismatch: " + label)
        if not pages[0]:
            return
        yield pages[0]
        after = tuple(pages[0][-1][key] for key in keys)


def _verify_routing(conn, rows):
    pairs = sorted({(row["series_id"], row["storage_day"]) for row in rows})
    bounds = {(row["series_id"], row["storage_day"]): row for row in conn.execute(
        text(f"SELECT * FROM {SCHEMA}.fact_header_series_days "
             "WHERE (series_id,storage_day) IN :pairs").bindparams(bindparam("pairs", expanding=True)),
        {"pairs": pairs}).mappings()}
    for row in rows:
        bound = bounds.get((row["series_id"], row["storage_day"]))
        if (bound is None or not
                bound["min_observation_time"] <= row["observation_time"] <= bound["max_observation_time"]):
            raise RuntimeError("fact_header_handoff_routing_incomplete")


@contextmanager
def verified_copy(conn, *, page_rows=128, timeout_seconds=30):
    """Fence and verify the fixed header copy in the caller's transaction.

    Yield only after full header/identity equality, conservative routing coverage
    and partition/placement checks. The caller may perform its admitted handoff
    inside this context, under the same cumulative deadline. Ordinary reads
    remain allowed; the actual rename needs a separate exclusive fence. Success retains
    locks until caller commit/rollback; failure releases this savepoint's locks.
    This is NOT an operator command, durable readiness or commit supervision.
    """
    if type(page_rows) is not int or not 1 <= page_rows <= 4096:
        raise ValueError("fact_header_copy_page_rows_out_of_bounds")
    started = monotonic()
    with migration_step(conn, timeout_seconds):
        # Close source writers before inspecting progress or taking a snapshot.
        # Lock all copied data and routing tables too: the shadow is not yet
        # protected by the runtime immutable guards.
        owners = ("market.fact_hot_payloads", "market.fact_archive_material_aliases",
                  "market.fact_archive_canonical_dependencies")
        relations = (SOURCE, *owners, *(SCHEMA+"."+name for name in TABLE_NAMES),
                     STATE, QUEUE, SCHEMA+".capture")
        conn.exec_driver_sql("LOCK TABLE "+",".join(relations)+" IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        state = _inspect_progress(conn)
        if state["placement"] is not None and not state["identity_history_ready"]:
            raise RuntimeError("fact_header_handoff_identity_history_required")
        if (not state["baseline_complete"] or not state["identity_capture"]
                or conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE})"))):
            raise RuntimeError("fact_header_handoff_copy_incomplete")
        assert_v1_source_admission(conn, identity_capture=True)
        days = conn.execute(text(f"SELECT storage_day FROM {SCHEMA}.fact_header_partitions "
                                 "ORDER BY storage_day LIMIT 4097")).scalars().all()
        children = conn.execute(text("""
            SELECT n.nspname,c.relname FROM pg_inherits i
            JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE i.inhparent=to_regclass(:parent) ORDER BY c.relname LIMIT 4097
        """), {"parent": SCHEMA+".fact_versions"}).all()
        expected = {(SCHEMA, "fact_versions_"+day.strftime("%Y%m%d")) for day in days}
        if len(days)>4096 or len(children)>4096 or set(children)!=expected:
            raise RuntimeError("fact_header_handoff_partition_catalog_mismatch")
        for day in days:
            _partition(conn, day, placement=state["placement"], pid=state["_placement_pid"])
        header_rows = 0
        for rows in _verified_pages(conn, source=SOURCE, target=SCHEMA+".fact_versions",
                                    columns=HEADER_COLUMNS, keys=("storage_day","market_commit_seq","id"),
                                    page_rows=page_rows, label="headers"):
            _verify_routing(conn, rows)
            header_rows += len(rows)
        identity_rows = sum(len(rows) for rows in _verified_pages(
            conn, source=SOURCE, target=SCHEMA+".fact_identities",
            columns=IDENTITY_COLUMNS, keys=("id",), page_rows=page_rows, label="identities"))
        report = {"verified_header_rows": header_rows, "verified_identity_rows": identity_rows,
                  "verification_seconds": monotonic()-started, "migration_ready": False,
                  "source_authoritative": True,
                  "physical_placement_configured": state["placement"] is not None}
        logger.info("fact_header_v2_handoff_copy_verified | rows=%s duration_seconds=%s",
                    header_rows, report["verification_seconds"])
        yield report

"""Stage and atomically replace the hot Fact identity registry within cutover.

This is an operator primitive, not a production entrypoint. The caller owns
independent recovery, device admission, writer fencing and transaction commit.
"""

from __future__ import annotations

from datetime import date
import re

from sqlalchemy import text

from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db import fact_header_v2_online_proof as online


STAGE_SCHEMA = "qt_cold_identity_rebuild"
STAGED = f"{STAGE_SCHEMA}.fact_identities"
PREVIOUS = f"{STAGE_SCHEMA}.fact_identities_previous"
SOURCE = "market.fact_identities"
_FIELDS = "id,storage_day,series_id,observation_key,revision"
_DAY_INDEX = "ix_cold_hot_identity_day_id"
_SWAP_RECEIPT = f"{STAGE_SCHEMA}.identity_swap_receipt"
_KEYS = {
    "fact_identities_pkey": ("PRIMARY KEY", "id"),
    "uq_market_fact_identity_day": ("UNIQUE", "id, storage_day"),
    "uq_market_fact_identity_revision": ("UNIQUE", "series_id, observation_key, revision"),
}
_REFERENCES = {
    "market.fact_versions": ("fk_market_fact_identity_day", "id, storage_day"),
    "market.fact_hot_payloads": ("fact_hot_payloads_id_fkey", "id"),
    "qt_fact_header_cutover_v2.fact_versions": ("fk_market_fact_identity_day", "id, storage_day"),
}
_RETAINED_PARENT = "qt_fact_header_cutover_v2.fact_versions"
_MAX_OID = 4_294_967_295
_GUARDS = {
    "trg_guard_fact_identities": ("market.reject_fact_identity_mutation()", 27, "A", False),
    "trg_reject_mutation_fact_identities": ("market.reject_immutable_mutation()", 27, "O", False),
    "trg_require_fact_identity_header": ("market.require_fact_identity_header()", 5, "A", True),
    "trg_qt_online_no_truncate": ("qt_fact_header_cutover_v2.online_guard_truncate()", 34, "A", False),
}


def _name(conn, value):
    return ".".join(conn.dialect.identifier_preparer.quote_identifier(part) for part in value.split("."))


def _oid(conn, relation):
    return conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": relation})


def _requires_database(conn, expected_database, cutoff, timeout):
    if (type(cutoff) is not date or not isinstance(expected_database, str)
            or type(timeout) is not int or not 1 <= timeout <= 3_600_000):
        raise ValueError("cold_identity_invalid_cutover_parameters")
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"), {"timeout": str(timeout)})
    conn.execute(text("SET LOCAL lock_timeout='500ms'"))
    identity = conn.scalar(text("""
        SELECT control.system_identifier::text || '/' || database.oid::text
        FROM pg_control_system() control CROSS JOIN pg_database database WHERE datname=current_database()
    """))
    if identity != expected_database:
        raise RuntimeError("cold_identity_wrong_database")


def _source_registry(conn, oid):
    metadata = conn.execute(text("""
        SELECT c.relkind,c.relpersistence,c.relowner,pg_get_userbyid(c.relowner) AS role,
               c.relacl IS NULL AS default_acl, c.relrowsecurity,c.relforcerowsecurity,
               c.reloptions,c.relreplident::text,sp.spcname AS tablespace,
               NOT EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attacl IS NOT NULL) AS no_column_acl
        FROM pg_class c JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace sp ON sp.oid=COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
        WHERE c.oid=:oid
    """), {"oid": oid}).mappings().one()
    if (metadata["relkind"] != "r" or metadata["relpersistence"] != "p"
            or not metadata["default_acl"] or not metadata["no_column_acl"]
            or metadata["relrowsecurity"] or metadata["relforcerowsecurity"] or metadata["reloptions"]
            or metadata["relreplident"] != "d"):
        raise RuntimeError("cold_identity_source_acl_or_storage_policy_unreviewed")
    columns = conn.execute(text("""
        SELECT attname,format_type(atttypid,atttypmod),attnotnull,atthasdef,attidentity,attgenerated
        FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum
    """), {"oid": oid}).all()
    if columns != [(name, sql_type, True, False, "", "") for name, sql_type in (
        ("id", "character varying(64)"), ("storage_day", "date"), ("series_id", "bigint"),
        ("observation_key", "character varying(512)"), ("revision", "integer"),
    )]:
        raise RuntimeError("cold_identity_source_columns_unreviewed")
    check_rows = conn.execute(text("""
        SELECT conname,pg_get_constraintdef(oid),convalidated,connoinherit
        FROM pg_constraint WHERE conrelid=:oid AND contype='c' ORDER BY conname
    """), {"oid": oid}).all()
    expected = [
        ("ck_market_fact_identity_key", "CHECK (((observation_key)::text <> ''::text))", True, False),
        ("ck_market_fact_identity_revision", "CHECK ((revision > 0))", True, False),
    ]
    if check_rows != expected:
        raise RuntimeError("cold_identity_unreviewed_check_constraints")
    if conn.scalar(text("""
        SELECT EXISTS (SELECT FROM pg_publication_rel WHERE prrelid=:oid)
            OR EXISTS (SELECT FROM pg_publication WHERE puballtables)
    """), {"oid": oid}):
        raise RuntimeError("cold_identity_logical_replication_unreviewed")
    return metadata


def _index_groups(conn, oid, *, staged=False, promoted=False):
    indexes = conn.execute(text("""
        SELECT c.relname,sp.spcname,i.indisvalid,i.indisready,i.indislive,i.indisunique,con.convalidated,
               con.condeferrable,c.reloptions,pg_get_constraintdef(con.oid) AS definition,
               pg_get_indexdef(c.oid,0,false) AS index_definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace sp ON sp.oid=COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
        LEFT JOIN pg_constraint con ON con.conindid=c.oid AND con.contype IN ('p','u')
        WHERE i.indrelid=:oid
    """), {"oid": oid}).mappings().all()
    expected = {("stage_" + name if staged else name): key for name, key in _KEYS.items()}
    extra_names = {_DAY_INDEX} if staged or promoted else set()
    if {row["relname"] for row in indexes} != set(expected) | extra_names:
        raise RuntimeError("cold_identity_unreviewed_index")
    groups = {}
    for row in indexes:
        if row["relname"] == _DAY_INDEX:
            table = STAGED if staged else SOURCE
            if (row["definition"] is not None or row["indisunique"] or row["reloptions"]
                    or row["index_definition"] != f"CREATE INDEX {_DAY_INDEX} ON {table} USING btree (storage_day, id)"
                    or not all(row[name] for name in ("indisvalid", "indisready", "indislive"))):
                raise RuntimeError("cold_identity_unreviewed_day_index")
            groups[_DAY_INDEX] = row["spcname"]
            continue
        kind, fields = expected[row["relname"]]
        if (row["definition"] != f"{kind} ({fields})" or row["condeferrable"] or row["reloptions"]
                or not all(row[name] for name in ("indisvalid", "indisready", "indislive", "indisunique", "convalidated"))):
            raise RuntimeError("cold_identity_unreviewed_index")
        groups[row["relname"].removeprefix("stage_")] = row["spcname"]
    return groups


def _guard_rows(conn, oid):
    return conn.execute(text("""
        SELECT tgname, tgfoid::regprocedure::text AS function, tgtype::integer,
               tgenabled::text,tgdeferrable,tginitdeferred,
               tgqual IS NULL AND tgnargs=0 AS unconditional
        FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal ORDER BY tgname
    """), {"oid": oid}).mappings().all()


def _verify_guards(conn, oid):
    rows = _guard_rows(conn, oid)
    actual = {row["tgname"]: (row["function"], row["tgtype"], row["tgenabled"], row["tgdeferrable"]) for row in rows}
    if actual.keys() - _GUARDS.keys() or not {"trg_guard_fact_identities", "trg_require_fact_identity_header"} <= actual.keys():
        raise RuntimeError(f"cold_identity_unreviewed_trigger: names={sorted(actual)}")
    if any(actual[row["tgname"]] != _GUARDS[row["tgname"]] or not row["unconditional"]
           or row["tginitdeferred"] != row["tgdeferrable"] for row in rows):
        raise RuntimeError(f"cold_identity_unreviewed_trigger: bindings={actual}")
    if conn.scalar(text("""
        SELECT count(*) FROM pg_rewrite WHERE ev_class=:oid
    """), {"oid": oid}):
        raise RuntimeError("cold_identity_unreviewed_rewrite_rule")
    return tuple(actual)


def _prepare_guards(conn, table, guards):
    for name in guards:
        func, _, enabled, deferred = _GUARDS[name]
        constraint = "CONSTRAINT " if deferred else ""
        if deferred:
            event = "AFTER INSERT"
            window = "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW"
        elif name == "trg_qt_online_no_truncate":
            event = "BEFORE TRUNCATE"
            window = "FOR EACH STATEMENT"
        else:
            event = "BEFORE UPDATE OR DELETE"
            window = "FOR EACH ROW"
        conn.exec_driver_sql(f"CREATE {constraint}TRIGGER {name} {event} ON {table} {window} EXECUTE FUNCTION {func}")
        if enabled == "A":
            conn.exec_driver_sql(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {name}")


def _same_identity(left, right):
    return f"""({left}.id,{left}.storage_day,{left}.series_id,{left}.observation_key,{left}.revision)
        IS NOT DISTINCT FROM ({right}.id,{right}.storage_day,{right}.series_id,{right}.observation_key,{right}.revision)"""


def _reconcile_exact_suffix(conn, cutoff):
    identity_match = _same_identity("header", "source")
    staged_match = _same_identity("header", "staged")
    # The left join detects bad or missing counterpart rows without inferring
    # identity from a count or relying on a key digest.
    present = conn.execute(text(f"""
        SELECT header.id
        FROM market.fact_versions AS header
        LEFT JOIN {SOURCE} AS source ON source.id=header.id AND {identity_match}
        WHERE header.storage_day>=:cutoff AND source.id IS NULL LIMIT 1
    """), {"cutoff": cutoff}).scalar_one_or_none()
    if present is not None:
        raise RuntimeError("cold_identity_source_header_mismatch")
    conn.execute(text(f"""
        INSERT INTO {STAGED} ({_FIELDS})
        SELECT source.id,source.storage_day,source.series_id,source.observation_key,source.revision
        FROM market.fact_versions AS header JOIN {SOURCE} AS source ON source.id=header.id
        WHERE header.storage_day>=:cutoff
          AND NOT EXISTS (SELECT FROM {STAGED} AS staged WHERE staged.id=header.id)
    """), {"cutoff": cutoff})
    mismatch = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT FROM {STAGED} AS staged
            WHERE staged.storage_day<:cutoff OR NOT EXISTS (
                SELECT FROM market.fact_versions AS header
                WHERE (header.id,header.storage_day)=(staged.id,staged.storage_day) AND {staged_match}
            )
        ) OR EXISTS (
            SELECT FROM {SOURCE} AS source WHERE source.storage_day>=:cutoff AND NOT EXISTS (
                SELECT FROM {STAGED} AS staged WHERE staged.id=source.id AND {_same_identity("staged","source")}
            )
        ) OR EXISTS (
            SELECT FROM market.fact_versions AS header WHERE header.storage_day>=:cutoff AND NOT EXISTS (
                SELECT FROM {STAGED} AS staged WHERE staged.id=header.id AND {staged_match}
            )
        )
    """), {"cutoff": cutoff}).scalar_one()
    if mismatch:
        raise RuntimeError("cold_identity_retained_set_mismatch")
    return conn.scalar(text(f"SELECT count(*) FROM {STAGED}"))


def prepare_identity_suffix(conn, *, expected_database: str, cutoff: date, statement_timeout_ms=900_000):
    """Build the retained suffix in a separate transaction before the writer fence."""
    # Freeze all staging comparisons in one snapshot. Normal writers continue
    # while this offline candidate is built; final refresh captures their delta.
    if conn.get_isolation_level() != "REPEATABLE READ":
        raise RuntimeError("cold_identity_staging_requires_repeatable_read")
    _requires_database(conn, expected_database, cutoff, statement_timeout_ms)
    conn.exec_driver_sql("LOCK TABLE ONLY market.fact_versions IN ACCESS SHARE MODE NOWAIT")
    conn.exec_driver_sql(f"LOCK TABLE {SOURCE} IN ACCESS SHARE MODE NOWAIT")
    old_oid = _oid(conn, SOURCE)
    metadata = _source_registry(conn, old_oid)
    index_groups = _index_groups(conn, old_oid)
    _verify_guards(conn, old_oid)
    conn.exec_driver_sql(f"CREATE SCHEMA {STAGE_SCHEMA}")
    heap_space = _name(conn, metadata["tablespace"])
    conn.exec_driver_sql(f"CREATE TABLE {STAGED} (LIKE {SOURCE} INCLUDING CONSTRAINTS) TABLESPACE {heap_space}")
    for name, (kind, fields) in _KEYS.items():
        index_space = _name(conn, index_groups[name])
        conn.exec_driver_sql(
            f"ALTER TABLE {STAGED} ADD CONSTRAINT stage_{name} {kind} ({fields}) USING INDEX TABLESPACE {index_space}"
        )
    day_space = _name(conn, index_groups["uq_market_fact_identity_day"])
    conn.exec_driver_sql(f"CREATE INDEX {_DAY_INDEX} ON {STAGED} (storage_day,id) TABLESPACE {day_space}")
    # The fresh candidate is private until promotion. No ordinary role can
    # select, mutate, or insert into a staging schema controlled by the operator.
    new_oid = _oid(conn, STAGED)
    _index_groups(conn, new_oid, staged=True)
    rows = _reconcile_exact_suffix(conn, cutoff)
    conn.exec_driver_sql(f"ANALYZE {STAGED}")
    return {"database_identity": expected_database, "cutoff": cutoff.isoformat(), "old_oid": old_oid,
            "new_oid": new_oid, "retained_rows": rows, "candidate_relation": STAGED}


def _require_fence(conn):
    if not conn.execute(text("""
        WITH key AS (SELECT hashtextextended(:name,0) AS value)
        SELECT EXISTS (
            SELECT FROM pg_locks,key WHERE locktype='advisory' AND pid=pg_backend_pid()
              AND mode='ExclusiveLock' AND granted AND objsubid=1
              AND classid::bigint=((key.value>>32)&4294967295) AND objid::bigint=(key.value&4294967295)
        )
    """), {"name": _LIFECYCLE_LOCK_NAME}).scalar_one():
        raise RuntimeError("cold_identity_writer_fence_not_held")


def _fk_bindings(conn, oid):
    rows = conn.execute(text("""
        SELECT con.oid, con.conname,ns.nspname||'.'||relation.relname AS owner,con.convalidated,
               con.condeferrable,pg_get_constraintdef(con.oid) AS definition
        FROM pg_constraint con JOIN pg_class relation ON relation.oid=con.conrelid
        JOIN pg_namespace ns ON ns.oid=relation.relnamespace
        WHERE con.confrelid=:oid AND con.contype='f' AND con.conparentid=0
    """), {"oid": oid}).mappings().all()
    if len(rows) != len({row["owner"] for row in rows}) or any(row["owner"] not in _REFERENCES for row in rows):
        raise RuntimeError("cold_identity_incoming_foreign_keys_unreviewed")
    bindings = {row["owner"]: row for row in rows}
    if not {"market.fact_versions","market.fact_hot_payloads"} <= bindings.keys():
        raise RuntimeError("cold_identity_hot_foreign_keys_missing")
    for owner, row in bindings.items():
        name, columns = _REFERENCES[owner]
        if (row["definition"] != f"FOREIGN KEY ({columns}) REFERENCES {SOURCE}({columns}) ON DELETE RESTRICT"
                or not row["convalidated"] or row["condeferrable"] or row["conname"] != name):
            raise RuntimeError("cold_identity_foreign_key_shape_mismatch")
    return bindings


def _assert_no_external_dependencies(conn, oid):
    remaining = conn.scalar(text("""
        SELECT pg_describe_object(d.classid,d.objid,d.objsubid)
        FROM pg_depend d
        WHERE d.refclassid='pg_class'::regclass AND d.refobjid=:oid AND d.deptype IN('n','a')
          AND NOT (
              (d.classid='pg_constraint'::regclass AND EXISTS (
                SELECT FROM pg_constraint c WHERE c.oid=d.objid AND c.conrelid=:oid
              ))
              OR (d.classid='pg_trigger'::regclass AND EXISTS (
                SELECT FROM pg_trigger t WHERE t.oid=d.objid AND t.tgrelid=:oid
              ))
              OR (d.classid='pg_class'::regclass AND EXISTS (
                SELECT FROM pg_index i WHERE i.indexrelid=d.objid AND i.indrelid=:oid
              ))
          )
        LIMIT 1
    """), {"oid": oid})
    if remaining is not None:
        raise RuntimeError(f"cold_identity_incoming_dependency_unreviewed: {remaining}")


def _assert_retained_parent(conn, expected_retired_legacy_oid):
    conn.exec_driver_sql(f"LOCK TABLE ONLY {_RETAINED_PARENT} IN ACCESS EXCLUSIVE MODE NOWAIT")
    parent_oid = _oid(conn, _RETAINED_PARENT)
    empty_parent = conn.scalar(text("""
        SELECT relkind='p' AND relfilenode=0
          AND NOT EXISTS (SELECT FROM pg_inherits WHERE inhparent=c.oid OR inhrelid=c.oid)
          AND NOT EXISTS (SELECT FROM pg_rewrite WHERE ev_class=c.oid)
        FROM pg_class c WHERE oid=:oid
    """), {"oid": parent_oid})
    if empty_parent is not True:
        raise RuntimeError("cold_identity_retained_cutover_parent_not_empty")
    if _oid(conn, online.STATE) is None:
        if expected_retired_legacy_oid is None:
            old_closed_guard = conn.execute(text("""
                SELECT tgname='trg_retained_source_closed' AND tgenabled='A' AND tgtype=7
                    AND tgfoid=to_regprocedure('market.reject_immutable_mutation()')
                    AND tgqual IS NULL AND tgnargs=0 AND NOT tgdeferrable AND NOT tginitdeferred
                    AND tgoldtable IS NULL AND tgnewtable IS NULL
                FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal
            """), {"oid": parent_oid}).scalars().all()
            if old_closed_guard == [True]:
                return
        raise RuntimeError("cold_identity_retained_cutover_parent_proof_missing")
    online._inspect_triggers(conn, _RETAINED_PARENT, "header")
    conn.exec_driver_sql(f"LOCK TABLE ONLY {online.STATE} IN SHARE MODE NOWAIT")
    saved = conn.execute(text(f"SELECT id,binding FROM {online.STATE}")).one_or_none()
    try:
        binding = saved.binding if saved is not None and saved.id == 1 else None
        original_oid = binding["source_oids"][online.headers.SOURCE]
        raw_oid = binding["source_oids"][online.raw.SOURCE]
        bound_parent_oid = binding["targets"]["fact_versions"][0]
    except (IndexError, KeyError, TypeError):
        raise RuntimeError("cold_identity_retained_cutover_parent_proof_invalid") from None
    if (any(type(oid) is not int or not 0 < oid <= _MAX_OID
            for oid in (original_oid, raw_oid, bound_parent_oid))
            or bound_parent_oid != parent_oid
            or original_oid == _oid(conn, online.headers.SOURCE)
            or (expected_retired_legacy_oid is not None and original_oid != expected_retired_legacy_oid)):
        raise RuntimeError("cold_identity_retained_cutover_parent_proof_mismatch")

    # Forward adoption keeps the shadow guards but renames their original
    # header source. Use that source's saved OID, even after its audited drop.
    bodies = online._bodies(binding)
    for role in ("header", "truncate"):
        online._inspect_function(conn, role, bodies[role])
        if conn.scalar(text("""
            SELECT EXISTS (
                SELECT FROM pg_proc p, aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl
                WHERE p.oid=to_regprocedure(:name) AND acl.grantee<>p.proowner
            )
        """), {"name": f"{online.SCHEMA}.online_guard_{role}()"}):
            raise RuntimeError(f"cold_identity_retained_cutover_parent_function_acl_changed: {role}")


def swap_identity_suffix(conn, *, expected_database: str, cutoff: date,
                         expected_retired_legacy_oid: int | None = None, statement_timeout_ms=900_000):
    """Replace the registry atomically after the audited hot prefix is removed.

    Requires the caller's exclusive writer fence, the four archive FK
    transitions, and a staged regular table. Leaves the old registry intact
    for a separately verified and approved physical retirement. Pass the
    audited original header OID when retiring a forward-adopted legacy child.
    """
    if (expected_retired_legacy_oid is not None and (
        type(expected_retired_legacy_oid) is not int or not 0 < expected_retired_legacy_oid <= _MAX_OID
    )):
        raise ValueError("cold_identity_invalid_retired_legacy_oid")
    _requires_database(conn, expected_database, cutoff, statement_timeout_ms)
    _require_fence(conn)
    old_oid, new_oid = _oid(conn, SOURCE), _oid(conn, STAGED)
    if not old_oid or not new_oid or _oid(conn, PREVIOUS):
        raise RuntimeError("cold_identity_missing_or_already_promoted_stage")
    for table in ("market.fact_versions", "market.fact_hot_payloads", SOURCE, STAGED):
        conn.exec_driver_sql(f"LOCK TABLE ONLY {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
    bindings = _fk_bindings(conn, old_oid)
    if _RETAINED_PARENT in bindings:
        _assert_retained_parent(conn, expected_retired_legacy_oid)
    if conn.scalar(text("SELECT EXISTS(SELECT FROM market.fact_versions WHERE storage_day<:cutoff)"),
                   {"cutoff": cutoff}):
        raise RuntimeError("cold_identity_source_header_prefix_still_present")
    if conn.scalar(text("SELECT EXISTS(SELECT FROM market.fact_hot_payloads WHERE storage_day<:cutoff)"),
                   {"cutoff": cutoff}):
        raise RuntimeError("cold_identity_source_payload_prefix_still_present")
    metadata = _source_registry(conn, old_oid)
    stage_metadata = _source_registry(conn, new_oid)
    guards = _verify_guards(conn, old_oid)
    expected_indexes = _index_groups(conn, old_oid)
    expected_indexes[_DAY_INDEX] = expected_indexes["uq_market_fact_identity_day"]
    if (metadata["tablespace"] != stage_metadata["tablespace"]
            or expected_indexes != _index_groups(conn, new_oid, staged=True)
            or _guard_rows(conn, new_oid)):
        raise RuntimeError("cold_identity_candidate_placement_or_guards_changed")
    rows = _reconcile_exact_suffix(conn, cutoff)
    for owner, row in bindings.items():
        conn.exec_driver_sql(f"ALTER TABLE {_name(conn, owner)} DROP CONSTRAINT {_name(conn, row['conname'])}")
    _assert_no_external_dependencies(conn, old_oid)

    # Keep the old table/index names out of the canonical schema. Candidate
    # constraint names are restored after moving the whole relation.
    conn.exec_driver_sql(f"ALTER TABLE {SOURCE} RENAME TO fact_identities_previous")
    conn.exec_driver_sql(f"ALTER TABLE market.fact_identities_previous SET SCHEMA {STAGE_SCHEMA}")
    conn.exec_driver_sql(f"ALTER TABLE {STAGED} SET SCHEMA market")
    for name in _KEYS:
        conn.exec_driver_sql(f"ALTER TABLE {SOURCE} RENAME CONSTRAINT stage_{name} TO {name}")
    _prepare_guards(conn, SOURCE, guards)
    conn.exec_driver_sql(
        f"CREATE TRIGGER trg_cold_identity_previous_closed BEFORE INSERT ON {PREVIOUS} "
        "FOR EACH ROW EXECUTE FUNCTION market.reject_fact_identity_mutation()"
    )
    conn.exec_driver_sql(f"ALTER TABLE {PREVIOUS} ENABLE ALWAYS TRIGGER trg_cold_identity_previous_closed")
    for owner, row in bindings.items():
        conn.exec_driver_sql(f"ALTER TABLE {_name(conn, owner)} ADD CONSTRAINT {_name(conn, row['conname'])} {row['definition']}")
    if metadata["relowner"] != stage_metadata["relowner"]:
        conn.exec_driver_sql(f"ALTER TABLE {SOURCE} OWNER TO {_name(conn, metadata['role'])}")
    if _oid(conn, SOURCE) != new_oid or _oid(conn, PREVIOUS) != old_oid:
        raise RuntimeError("cold_identity_promotion_oid_mismatch")
    if (_fk_bindings(conn, new_oid).keys() != bindings.keys() or _verify_guards(conn, new_oid) != guards
            or _index_groups(conn, new_oid, promoted=True) != expected_indexes):
        raise RuntimeError("cold_identity_promoted_contract_mismatch")
    if _RETAINED_PARENT in bindings:
        _assert_retained_parent(conn, expected_retired_legacy_oid)
    _source_registry(conn, new_oid)
    _assert_no_external_dependencies(conn, old_oid)
    conn.exec_driver_sql(f"""
        CREATE TABLE {_SWAP_RECEIPT} (
            singleton boolean PRIMARY KEY CHECK (singleton), database_identity text NOT NULL,
            cutoff date NOT NULL, old_oid bigint NOT NULL, new_oid bigint NOT NULL,
            retained_rows bigint NOT NULL, finalized_at timestamptz, recovery_digest text
        )
    """)
    conn.execute(text(f"""
        INSERT INTO {_SWAP_RECEIPT}
            (singleton,database_identity,cutoff,old_oid,new_oid,retained_rows)
        VALUES(true,:identity,:cutoff,:old_oid,:new_oid,:rows)
    """), {"identity": expected_database, "cutoff": cutoff, "old_oid": old_oid, "new_oid": new_oid, "rows": rows})
    return {"database_identity": expected_database, "cutoff": cutoff.isoformat(), "old_oid": old_oid,
            "new_oid": new_oid, "retained_rows": rows, "old_relation": PREVIOUS, "new_relation": SOURCE,
            "rebound_foreign_keys": {owner: row["conname"] for owner, row in bindings.items()}}


def finalize_identity_suffix(conn, *, expected_database: str, cutoff: date,
                             old_oid: int, new_oid: int, verified_recovery_digest: str,
                             statement_timeout_ms=30_000):
    """Retire old physical files under separate explicit, verified authorization.

    The caller certifies the independently restored recovery digest and its
    approval. A digest string alone is not evidence of either.
    """
    if (type(old_oid) is not int or old_oid <= 0 or type(new_oid) is not int or new_oid <= 0
            or old_oid == new_oid or not isinstance(verified_recovery_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", verified_recovery_digest) is None):
        raise ValueError("cold_identity_invalid_retirement_evidence")
    _requires_database(conn, expected_database, cutoff, statement_timeout_ms)
    _require_fence(conn)
    receipt = conn.execute(text(f"""
        SELECT database_identity,cutoff,old_oid,new_oid,retained_rows,finalized_at,recovery_digest
        FROM {_SWAP_RECEIPT} WHERE singleton FOR UPDATE NOWAIT
    """)).one_or_none()
    if receipt is None or tuple(receipt[:4]) != (expected_database, cutoff, old_oid, new_oid):
        raise RuntimeError("cold_identity_retirement_receipt_mismatch")
    if _oid(conn, SOURCE) != new_oid:
        raise RuntimeError("cold_identity_retirement_active_oid_changed")
    result = {"database_identity": expected_database, "old_oid": old_oid, "new_oid": new_oid,
              "old_relation": PREVIOUS, "verified_recovery_digest": verified_recovery_digest,
              "retained_rows": receipt.retained_rows}
    if receipt.finalized_at is not None:
        if _oid(conn, PREVIOUS) is not None or receipt.recovery_digest != verified_recovery_digest:
            raise RuntimeError("cold_identity_retirement_completion_conflict")
        return result | {"already_finalized": True}
    conn.exec_driver_sql(f"LOCK TABLE ONLY {PREVIOUS} IN ACCESS EXCLUSIVE MODE NOWAIT")
    if _oid(conn, PREVIOUS) != old_oid:
        raise RuntimeError("cold_identity_retirement_old_oid_changed")
    _assert_no_external_dependencies(conn, old_oid)
    conn.exec_driver_sql(f"DROP TABLE {PREVIOUS} RESTRICT")
    conn.execute(text(f"""
        UPDATE {_SWAP_RECEIPT} SET finalized_at=clock_timestamp(),recovery_digest=:digest
        WHERE singleton
    """), {"digest": verified_recovery_digest})
    return result | {"already_finalized": False}

"""Restore one legacy header child from an isolated original-layout donor."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from hashlib import sha256
from itertools import islice
from tempfile import SpooledTemporaryFile
from time import monotonic

from sqlalchemy import text

from core.execution_control import controlled_execution, ExecutionControl
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_header_legacy_schema import assert_fact_header_legacy_contract
from portal.backend.db.fact_identity_schema import assert_fact_identity_contract
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.service.storage.repos.fact_archival import _partition_manifest_set_hash
from portal.backend.service.storage.repos.fact_storage import ensure_payload_contracts
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_audit import _source_rows
from scripts.db.canonical_cold_migration import INITIAL, PostgresMigrationSource
from scripts.db.canonical_cold_receipts import RECEIPT_SET_VERSION, _prefix_days, _validate_common, manifest_inventory_hash
from scripts.db.canonical_cold_restore_references import _require_disposable_fence
from scripts.db.canonical_cold_rollback_cohort import _approved_pages, _certificate, _compare_page, _require_snapshot
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_restore_legacy.v1"
LEGACY = "market.fact_versions_legacy"
STAGE = "qt_cold_legacy_restore.fact_versions_legacy"
_SCHEMA = "qt_cold_legacy_restore"
_COLUMNS = tuple(MarketFactVersionRecord.__table__.columns.keys())
_IDENTITY_FIELDS = "id,storage_day,series_id,observation_key,revision"


def _bound(conn, deadline):
    milliseconds = int((deadline - monotonic()) * 1000)
    if milliseconds <= 0:
        raise TimeoutError("canonical_cold_restore_legacy_deadline")
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                 {"timeout": str(milliseconds)})


def _layout(conn, name):
    relation = conn.execute(text("""
        SELECT c.oid::bigint, pg_get_userbyid(c.relowner) AS owner,c.relacl::text AS acl,c.reloptions AS options,
               t.spcname AS tablespace, c.relrowsecurity,c.relforcerowsecurity,c.relkind,c.relpersistence,
               pg_get_expr(c.relpartbound,c.oid) AS bound
        FROM pg_class c JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace t ON t.oid=coalesce(nullif(c.reltablespace,0),d.dattablespace)
        WHERE c.oid=to_regclass(:relation)
    """), {"relation": name}).mappings().one()
    columns = conn.execute(text("""
        SELECT attname,format_type(atttypid,atttypmod) AS type,attnotnull,attcollation::regcollation::text AS collation,
               attstorage,attcompression
        FROM pg_attribute WHERE attrelid=to_regclass(:relation) AND attnum>0 AND NOT attisdropped ORDER BY attname
    """), {"relation": name}).mappings().all()
    return dict(relation) | {"columns": [dict(row) for row in columns]}


def _indexes(conn, name):
    # Identify the access path and options independently from its qualified name.
    rows = conn.execute(text("""
        SELECT idx.relname AS name, idx.oid::bigint, space.spcname AS tablespace,
               substring(pg_get_indexdef(idx.oid) FROM ' USING .+$') AS definition,
               i.indisunique AS unique, i.indisvalid AND i.indisready AND i.indislive AS valid,
               k.conname AS constraint_name,k.contype AS constraint_type,
               parent.inhparent::regclass::text AS parent
        FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace space ON space.oid=coalesce(nullif(idx.reltablespace,0),d.dattablespace)
        LEFT JOIN pg_constraint k ON k.conrelid=i.indrelid AND k.conindid=idx.oid AND k.contype IN ('p','u')
        LEFT JOIN pg_inherits parent ON parent.inhrelid=idx.oid
        WHERE i.indrelid=to_regclass(:relation) ORDER BY idx.relname LIMIT 129
    """), {"relation": name}).mappings().all()
    if not rows or len(rows) > 128 or any(not row["valid"] or not row["definition"] for row in rows):
        raise RuntimeError("canonical_cold_restore_legacy_index_inventory_invalid")
    return [dict(row) for row in rows]


def _checks(conn, relation):
    rows = conn.execute(text("""
        SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid=to_regclass(:relation) AND contype='c'
          AND convalidated AND NOT condeferrable ORDER BY conname LIMIT 129
    """), {"relation": relation}).all()
    if len(rows) > 128:
        raise RuntimeError("canonical_cold_restore_legacy_check_inventory_invalid")
    return dict(rows)


def _toast_placement(conn, relation):
    return [tuple(row) for row in conn.execute(text("""
        WITH toast AS (
            SELECT reltoastrelid AS oid FROM pg_class WHERE oid=to_regclass(:relation) AND reltoastrelid<>0
        ), forks AS (
            SELECT oid,'heap' AS kind FROM toast
            UNION ALL SELECT indexrelid,'index' FROM pg_index JOIN toast ON toast.oid=indrelid
        )
        SELECT kind, t.spcname, c.reloptions, c.relpersistence
        FROM forks f JOIN pg_class c ON c.oid=f.oid JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace t ON t.oid=coalesce(nullif(c.reltablespace,0),d.dattablespace)
        ORDER BY kind,t.spcname
    """), {"relation": relation}).all()]


def measure_legacy_donor(donor, *, donor_identity, audit, source_commit):
    """Measure the exact original header catalog and authority on a physical clone."""
    _require_snapshot(donor, donor_identity)
    if (type(source_commit) is not int or source_commit < 1 or audit.get("schema") != RECEIPT_SET_VERSION
            or archive_evidence_hash({key: value for key, value in audit.items() if key != "audit_set_hash"})
            != audit.get("audit_set_hash")):
        raise ValueError("canonical_cold_restore_legacy_audit_scope_invalid")
    assert_fact_storage_contract(donor)
    layout, _ = _certificate(donor)
    end = assert_fact_header_legacy_contract(donor)
    physical = _layout(donor, LEGACY)
    entries = _prefix_days(audit["prefix"])
    legacy_days = [item for item in entries.values() if item["child"] == "fact_versions_legacy"]
    if (layout != FACT_STORAGE_LAYOUT_VERSION or end is None or not legacy_days
            or end.isoformat() != audit["prefix"]["legacy_end_day"]
            or {item["child_oid"] for item in legacy_days} != {physical["oid"]}
            or (donor_identity["system_identifier"], donor_identity["header_oid"]) !=
               (audit["source_database"]["system_identifier"], audit["source_database"]["header_oid"])
            or PostgresMarketDataRepository._current_commit_seq_with_session(donor) != source_commit
            or physical["acl"] is not None or physical["relrowsecurity"] or physical["relforcerowsecurity"]
            or physical["relkind"] != "r" or physical["relpersistence"] != "p"
            or {column["attname"] for column in physical["columns"]} != set(_COLUMNS)):
        raise RuntimeError("canonical_cold_restore_legacy_donor_contract_changed")
    native = {"schema": VERSION, "donor_identity": donor_identity, "physical": physical,
              "end_day": end.isoformat(), "audit_set_hash": audit["audit_set_hash"], "source_commit": source_commit,
              "indexes": _indexes(donor, LEGACY), "checks": _checks(donor, LEGACY),
              "toast": [list(item) for item in _toast_placement(donor, LEGACY)]}
    return native | {"scope_sha256": archive_evidence_hash(native)}


class _CopyWindow:
    def __init__(self, handle, *, remaining, deadline):
        self.handle, self.remaining, self.deadline = handle, remaining, deadline
        self.bytes = 0
        self.digest = sha256()

    def write(self, block):
        self.bytes += len(block)
        if self.bytes > self.remaining or monotonic() >= self.deadline:
            raise RuntimeError("canonical_cold_restore_legacy_copy_budget_exceeded")
        self.digest.update(block)
        return self.handle.write(block) if self.handle is not None else len(block)


def _copy_out(conn, sink, *, name, deadline, expected_rows, day=None, after=None, through=None, ids=None):
    _bound(conn, deadline)
    fields = ",".join(_COLUMNS)
    if ids is not None:
        where, params = "id=ANY(%s) AND storage_day=%s", (ids, day)
    else:
        where = "storage_day=%s AND (market_commit_seq,id)>(%s,%s) AND (market_commit_seq,id)<=(%s,%s)"
        params = (day, *after, *through)
    sql = (f"COPY (SELECT {fields} FROM ONLY {name} WHERE {where} "
           "ORDER BY market_commit_seq,id) TO STDOUT (FORMAT binary)")
    with conn.connection.driver_connection.cursor() as cursor:
        cursor.copy_expert(cursor.mogrify(sql, params), sink)
        if cursor.rowcount != expected_rows:
            raise RuntimeError("canonical_cold_restore_legacy_copy_cardinality_changed")


def _copy_page(target, donor, *, relation, day, page, row_ids, remaining, deadline, already_restored):
    scope = {"day": day, "deadline": deadline, "expected_rows": page["row_count"]}
    with SpooledTemporaryFile(max_size=4 * 1024**2) as spool:
        exported = _CopyWindow(spool, remaining=remaining, deadline=deadline)
        _copy_out(donor, exported, name=LEGACY, ids=row_ids, **scope)
        if not already_restored:
            spool.seek(0)
            _bound(target, deadline)
            with target.connection.driver_connection.cursor() as cursor:
                cursor.copy_expert(f"COPY {relation} ({','.join(_COLUMNS)}) FROM STDIN (FORMAT binary)", spool)
        imported = _CopyWindow(None, remaining=exported.bytes, deadline=deadline)
        if already_restored:
            _copy_out(target, imported, name=relation, ids=row_ids, **scope)
        else:
            _copy_out(target, imported, name=relation, after=tuple(page["after"]), through=tuple(page["last_cursor"]), **scope)
        if (imported.bytes, imported.digest.digest()) != (exported.bytes, exported.digest.digest()):
            raise RuntimeError("canonical_cold_restore_legacy_copied_rows_changed")
    return exported.bytes, exported.digest.hexdigest()


def _assert_initial_inventory(conn, audit, source_commit, max_pages, deadline):
    def rows():
        count = 0
        after = ""
        while True:
            _bound(conn, deadline)
            pages = conn.execute(text("""
                SELECT id,manifest_hash,row_count,last_commit_seq FROM market.cold_fact_pages
                WHERE first_commit_seq<=:commit AND id>:after AND staged_storage_day IS NULL
                ORDER BY id LIMIT 512
            """), {"commit": source_commit, "after": after}).all()
            for page in pages:
                count += 1
                if count > max_pages or page.last_commit_seq > source_commit:
                    raise RuntimeError("canonical_cold_restore_legacy_initial_inventory_scope_changed")
                after = page.id
                yield tuple(page[:3])
            if len(pages) < 512:
                break
    if manifest_inventory_hash(rows()) != audit["loaded_inventory_hash"]:
        raise RuntimeError("canonical_cold_restore_legacy_initial_inventory_changed")


def _create_detached(conn, physical, checks, end_day):
    quote = conn.dialect.identifier_preparer.quote_identifier
    conn.exec_driver_sql(f"CREATE SCHEMA {_SCHEMA}")
    conn.exec_driver_sql(f"REVOKE ALL ON SCHEMA {_SCHEMA} FROM PUBLIC")
    conn.exec_driver_sql(f"CREATE TABLE {STAGE} (LIKE market.fact_versions INCLUDING DEFAULTS "
                         f"INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION) TABLESPACE {quote(physical['tablespace'])}")
    present = _checks(conn, STAGE)
    if any(checks.get(key) != value for key, value in present.items()):
        raise RuntimeError("canonical_cold_restore_legacy_parent_checks_changed")
    for name, definition in checks.items():
        if name not in present:
            conn.exec_driver_sql(f"ALTER TABLE {STAGE} ADD CONSTRAINT {quote(name)} {definition}")
    conn.exec_driver_sql(f"ALTER TABLE {STAGE} ADD CONSTRAINT qt_rollback_legacy_bound CHECK (storage_day < DATE '{end_day}')")
    if physical["options"]:
        # Do not silently change the original physical storage policy.
        raise RuntimeError("canonical_cold_restore_legacy_reloptions_require_review")
    if _layout(conn, STAGE)["columns"] != physical["columns"]:
        raise RuntimeError("canonical_cold_restore_legacy_column_contract_changed")


def _build_indexes(conn, indexes):
    quote = conn.dialect.identifier_preparer.quote_identifier
    for index in indexes:
        name = quote(index["name"])
        unique = "UNIQUE " if index["unique"] else ""
        definition = index["definition"]
        # pg_get_indexdef excludes physical tablespace when deparsing a plain
        # index definition. Index membership below additionally checks it.
        if " WHERE " in definition:
            access, predicate = definition.split(" WHERE ", 1)
            definition = f"{access} TABLESPACE {quote(index['tablespace'])} WHERE {predicate}"
        else:
            definition = f"{definition} TABLESPACE {quote(index['tablespace'])}"
        conn.exec_driver_sql(f"CREATE {unique}INDEX {name} ON {STAGE}{definition}")
        if index["constraint_type"]:
            kind = "PRIMARY KEY" if index["constraint_type"] == "p" else "UNIQUE"
            constraint = quote(index["constraint_name"])
            conn.exec_driver_sql(f"ALTER TABLE {STAGE} ADD CONSTRAINT {constraint} {kind} USING INDEX {name}")


def _prepare_identities(conn):
    conn.exec_driver_sql("LOCK TABLE market.fact_identities IN ACCESS EXCLUSIVE MODE NOWAIT")
    # Standalone staged headers do not yet satisfy the deferred parent probe.
    # Avoid queuing a deferred trigger per historical row. The exact identity
    # join and native partition FK validation run before the step can commit.
    conn.exec_driver_sql("ALTER TABLE market.fact_identities DISABLE TRIGGER trg_require_fact_identity_header")
    conn.exec_driver_sql(f"INSERT INTO market.fact_identities({_IDENTITY_FIELDS}) SELECT {_IDENTITY_FIELDS} FROM {STAGE} "
                         "ON CONFLICT(id) DO NOTHING")
    conn.exec_driver_sql("ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_require_fact_identity_header")


def _attach(conn, scope):
    end_day = scope["end_day"]
    quote = conn.dialect.identifier_preparer.quote_identifier
    _prepare_identities(conn)
    conn.exec_driver_sql(f"ALTER TABLE {STAGE} OWNER TO {quote(scope['physical']['owner'])}")
    conn.exec_driver_sql(f"ALTER TABLE {STAGE} SET SCHEMA market")
    conn.exec_driver_sql(f"ALTER TABLE market.fact_versions ATTACH PARTITION {LEGACY} "
                         f"FOR VALUES FROM (MINVALUE) TO ('{end_day}')")
    for suffix, events, granularity in (("", "INSERT OR UPDATE OR DELETE", "ROW"),
                                       ("_truncate", "TRUNCATE", "STATEMENT")):
        trigger = "trg_seal_fact_versions_legacy" + suffix
        conn.exec_driver_sql(f"CREATE TRIGGER {trigger} BEFORE {events} ON {LEGACY} FOR EACH {granularity} "
                             "EXECUTE FUNCTION market.reject_fact_header_legacy_mutation()")
        conn.exec_driver_sql(f"ALTER TABLE {LEGACY} ENABLE ALWAYS TRIGGER {trigger}")
    oid = conn.scalar(text(f"SELECT '{LEGACY}'::regclass::oid::bigint"))
    conn.execute(text("INSERT INTO market.fact_header_legacy(id,end_day,relation_oid) VALUES (1,:end,:oid)"),
                 {"end": end_day, "oid": oid})
    conn.exec_driver_sql(f"DROP SCHEMA {_SCHEMA} RESTRICT")
    return oid


def _assert_attached(conn, scope):
    assert_fact_identity_contract(conn)
    attached = _layout(conn, LEGACY)
    if ({key: value for key, value in attached.items() if key != "oid"} !=
            {key: value for key, value in scope["physical"].items() if key != "oid"}
            or attached["oid"] == scope["physical"]["oid"]):
        raise RuntimeError("canonical_cold_restore_legacy_attached_contract_changed")
    normalize = lambda indexes: [{key: value for key, value in item.items() if key != "oid"} for item in indexes]
    if normalize(_indexes(conn, LEGACY)) != normalize(scope["indexes"]):
        raise RuntimeError("canonical_cold_restore_legacy_attached_indexes_changed")
    if _checks(conn, LEGACY) != scope["checks"]:
        raise RuntimeError("canonical_cold_restore_legacy_attached_checks_changed")
    if [list(item) for item in _toast_placement(conn, LEGACY)] != scope["toast"]:
        raise RuntimeError("canonical_cold_restore_legacy_attached_toast_changed")
    return attached["oid"]


def restore_legacy_headers(target, donor, *, fence, donor_scope, expected_donor_hash, audit, forward_receipts,
                           store, donor_store, max_copy_bytes, max_seconds=900, max_pages=100_000,
                           max_archive_bytes=1024**4,
                           limits=FactArchiveLimits()):
    """COPY the exact old legacy partition. Leave cold authority fenced.

    Distinct archive roots retain their bytes. This step restores headers and
    the matching identities only. Hot payload children, remaining days,
    old archive references and final v2 startup are separate operations.
    """
    _require_disposable_fence(target, fence)
    store_root, donor_root = store.root.resolve(strict=True), donor_store.root.resolve(strict=True)
    if (type(max_copy_bytes) is not int or not 1 <= max_copy_bytes <= 1024**4
            or type(max_archive_bytes) is not int or not 1 <= max_archive_bytes <= 2 * 1024**4
            or type(max_pages) is not int or not 1 <= max_pages <= 100_000
            or type(max_seconds) is not int or not 1 <= max_seconds <= MAX_MIGRATION_SECONDS
            or donor is target or store_root.is_relative_to(donor_root) or donor_root.is_relative_to(store_root)):
        raise ValueError("canonical_cold_restore_legacy_invalid_scope")
    deadline = min(monotonic() + max_seconds, fence.deadline)
    donor_identity = donor_scope["donor_identity"]
    source = _require_snapshot(donor, donor_identity)
    source_commit = donor_scope["source_commit"]
    _bound(donor, deadline)
    if (measure_legacy_donor(donor, donor_identity=donor_identity, audit=audit, source_commit=source_commit) != donor_scope
            or donor_scope["scope_sha256"] != expected_donor_hash):
        raise RuntimeError("canonical_cold_restore_legacy_donor_scope_changed")
    days = {day: entry for day, entry in _prefix_days(audit["prefix"]).items()
            if entry["child"] == "fact_versions_legacy"}
    receipts = defaultdict(list)
    for receipt in islice(forward_receipts, max_pages + len(days) + 1):
        day = receipt.get("day_batch", {}).get("storage_day")
        common = _validate_common(receipt)
        if (day not in days or receipt.get("receipt_hash") not in audit["receipt_hashes"]
                or any(audit[key] != value for key, value in common.items())):
            raise ValueError("canonical_cold_restore_legacy_unbound_day")
        receipts[day].append(receipt)
    if receipts.keys() != days.keys() or sum(map(len, receipts.values())) > max_pages + len(days):
        raise ValueError("canonical_cold_restore_legacy_incomplete_receipts")
    old_reader = ColdHistoryReader(donor_store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(donor, pairs))
    new_reader = ColdHistoryReader(store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(target, pairs))
    io_control = ExecutionControl()
    io_control.limit(seconds=max(0.001, deadline - monotonic()))
    with (archive_namespace(store.root, exclusive=True) as check_archive,
          _bounded_step(target, max_seconds) as clamp, controlled_execution(io_control)):
        clamp(deadline - monotonic())
        fence.assert_quiet(target)
        check_archive()
        if not target.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:id,0))"), {"id": _LIFECYCLE_LOCK_NAME}):
            raise RuntimeError("canonical_cold_restore_legacy_lifecycle_busy")
        target.exec_driver_sql("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT")
        layout, certificate = _certificate(target)
        identity = PostgresMigrationSource(target.engine, expected_system_identifier=donor_identity["system_identifier"]).identity(target)
        if (layout != COLD_HISTORY_LAYOUT_VERSION or not isinstance(certificate, dict)
                or identity == donor_identity
                or any(identity[key] != fence.database[key] for key in ("system_identifier", "database_oid", "database"))
                or identity["header_oid"] != audit["database"]["header_oid"]
                or (certificate.get("audit_set_hash"), certificate.get("loaded_inventory_hash"), certificate.get("source_commit_seq"))
                != (audit["audit_set_hash"], audit["loaded_inventory_hash"], source_commit)):
            raise RuntimeError("canonical_cold_restore_legacy_target_certificate_changed")
        _assert_initial_inventory(target, audit, source_commit, max_pages, deadline)
        existing = target.scalar(text(f"SELECT to_regclass('{LEGACY}') IS NOT NULL"))
        relation = LEGACY if existing else STAGE
        if existing:
            _assert_attached(target, donor_scope)
        else:
            assert_fact_identity_contract(target)
            _create_detached(target, donor_scope["physical"], donor_scope["checks"], donor_scope["end_day"])
        # The transfer query uses this compact temporary access path instead of
        # re-scanning the detached legacy heap for each page.
        if not existing:
            target.exec_driver_sql(f"CREATE INDEX qt_legacy_copy_seek ON {STAGE}(storage_day,market_commit_seq,id)")
        rows = pages = copied = 0
        archive_bytes = 0
        content = archive_evidence_hash({"scope": expected_donor_hash})
        for day_id, entry in days.items():
            day = date.fromisoformat(day_id)
            detail = audit["days"][day_id]
            retired = target.execute(text("""
                SELECT handoff_kind,manifest_set_hash,row_count FROM market.cold_fact_retired_days WHERE storage_day=:day
            """), {"day": day}).one_or_none()
            if retired != ("initial", audit["audit_set_hash"], detail["expected_rows"]):
                raise RuntimeError("canonical_cold_restore_legacy_retirement_changed")
            if not detail["expected_rows"]:
                if (len(receipts[day_id]) != 1 or receipts[day_id][0]["day_batch"]["page_receipts"]
                        or _source_rows(donor, day=day, after=INITIAL, through=None, limit=1)):
                    raise RuntimeError("canonical_cold_restore_legacy_unaccounted_donor_rows")
                continue
            _, detail, day_pages = _approved_pages(audit, receipts[day_id], day, min(max_pages, 4096))
            cohort = source.cohort(donor, day)
            if (cohort.expected_rows, cohort.state, cohort.sealed_at, cohort.reuse_prefix_pages) != (
                detail["expected_rows"], detail["source_retention_state"], detail["source_sealed_at"],
                sum(authority is not None for _, authority in day_pages),
            ):
                raise RuntimeError("canonical_cold_restore_legacy_donor_cohort_changed")
            page_verifications = []
            for page, authority in day_pages:
                pages += 1
                if pages > max_pages:
                    raise RuntimeError("canonical_cold_restore_legacy_page_budget_exceeded")
                _bound(donor, deadline)
                _bound(target, deadline)
                byte_count = target.scalar(text("SELECT byte_count FROM market.cold_fact_pages WHERE id=:id"),
                                           {"id": page["manifest_id"]})
                if byte_count is None or byte_count <= 0:
                    raise RuntimeError("canonical_cold_restore_legacy_cold_page_missing")
                archive_bytes += byte_count * (2 if authority is not None else 1)
                if archive_bytes > max_archive_bytes:
                    raise RuntimeError("canonical_cold_restore_legacy_archive_budget_exceeded")
                page_verifications.append(_compare_page(
                    donor, target, source, old_reader, new_reader, cohort=cohort,
                    header_oid=entry["child_oid"], page=page, authority=authority,
                ))
                row_ids = target.execute(text("""
                    SELECT id FROM market.cold_fact_locators WHERE page_id=:page ORDER BY ordinal LIMIT :limit
                """), {"page": page["manifest_id"], "limit": page["row_count"] + 1}).scalars().all()
                if len(row_ids) != page["row_count"]:
                    raise RuntimeError("canonical_cold_restore_legacy_locator_scope_changed")
                size, digest = _copy_page(target, donor, relation=relation, day=day, page=page, row_ids=row_ids,
                                          remaining=max_copy_bytes - copied, deadline=deadline, already_restored=existing)
                copied += size
                rows += page["row_count"]
                content = archive_evidence_hash({"previous": content, "manifest": page["manifest_hash"], "copy": digest})
            if _source_rows(donor, day=day, after=tuple(detail["last_cursor"]), through=None, limit=1):
                raise RuntimeError("canonical_cold_restore_legacy_unaccounted_donor_rows")
            if cohort.state in {"verified", "reclaimed"}:
                expected = _partition_manifest_set_hash(day, cohort.expected_rows, page_verifications)
                stored = donor.scalar(text("""
                    SELECT manifest_set_hash FROM market.fact_retention_partitions WHERE storage_day=:day
                """), {"day": day})
                if any(item is None for item in page_verifications) or stored != expected:
                    raise RuntimeError("canonical_cold_restore_legacy_donor_day_receipt_changed")
        _bound(donor, deadline)
        _bound(target, deadline)
        if donor.scalar(text(f"SELECT count(*) FROM ONLY {LEGACY}")) != rows or target.scalar(
            text(f"SELECT count(*) FROM ONLY {relation}")
        ) != rows:
            raise RuntimeError("canonical_cold_restore_legacy_inverse_count_changed")
        if not existing:
            target.exec_driver_sql(f"DROP INDEX {_SCHEMA}.qt_legacy_copy_seek")
            _build_indexes(target, donor_scope["indexes"])
            _attach(target, donor_scope)
            target.exec_driver_sql(f"ALTER TABLE {LEGACY} DROP CONSTRAINT qt_rollback_legacy_bound")
        oid = _assert_attached(target, donor_scope)
        if target.scalar(text(f"""
            SELECT EXISTS (SELECT 1 FROM ONLY {LEGACY} h LEFT JOIN market.fact_identities i USING(id)
            WHERE (i.storage_day,i.series_id,i.observation_key,i.revision)
              IS DISTINCT FROM (h.storage_day,h.series_id,h.observation_key,h.revision))
        """)):
            raise RuntimeError("canonical_cold_restore_legacy_identity_mismatch")
        if target.scalar(text("""
            SELECT count(*) FROM market.fact_identities WHERE storage_day<:end
        """), {"end": date.fromisoformat(donor_scope["end_day"])}) != rows:
            raise RuntimeError("canonical_cold_restore_legacy_extra_identity")
        check_archive()
        fence.assert_quiet(target)
    result = {"schema": VERSION, "target_database": identity, "donor_identity": donor_identity,
              "donor_scope_sha256": expected_donor_hash, "original_oid": donor_scope["physical"]["oid"],
              "legacy_oid": oid, "end_day": donor_scope["end_day"], "rows": rows, "pages": pages,
              "copy_bytes": copied, "verified_archive_bytes": archive_bytes,
              "ordered_content_hash": content, "audit_set_hash": audit["audit_set_hash"]}
    return result | {"receipt_sha256": archive_evidence_hash(result)}

"""Recover one initially retired dated header and its original payload placement."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from itertools import islice
from tempfile import SpooledTemporaryFile
from time import monotonic

from sqlalchemy import text

from core.execution_control import ExecutionControl, controlled_execution
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_identity_schema import _partition_bound, assert_fact_identity_contract, fact_header_partition_name
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.db.market_storage_models import MarketFactHotPayloadRecord
from portal.backend.service.storage.repos.fact_archival import _partition_manifest_set_hash
from portal.backend.service.storage.repos.fact_storage import ensure_payload_contracts
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_audit import _source_rows
from scripts.db.canonical_cold_migration import INITIAL, PostgresMigrationSource
from scripts.db.canonical_cold_receipts import RECEIPT_SET_VERSION, _forward_day, _prefix_days, _validate_common
from scripts.db.canonical_cold_restore_legacy import _CopyWindow, _assert_initial_inventory, _checks, _indexes, _layout, _toast_placement
from scripts.db.canonical_cold_restore_legacy_payload import _assert_native_guard, _relation, _retention
from scripts.db.canonical_cold_restore_references import _ready, _require_disposable_fence
from scripts.db.canonical_cold_rollback_cohort import _compare_page, _require_snapshot
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_restore_initial_dated.v1"
_HEADER_PARENT = "market.fact_versions"
_PAYLOAD_PARENT = "market.fact_hot_payloads"
_IDENTITY_FIELDS = "id,storage_day,series_id,observation_key,revision"
_HEADER_COLUMNS = ",".join(MarketFactVersionRecord.__table__.columns.keys())
_PAYLOAD_COLUMNS = ",".join(MarketFactHotPayloadRecord.__table__.columns.keys())
_ROUTING = "market.fact_header_series_days"
_REGISTRY = "market.fact_header_partitions"
_BASE = "canonical_cold_restore_initial_dated"


def _bound(conn, deadline):
    remaining = int((deadline - monotonic()) * 1000)
    if remaining <= 0:
        raise TimeoutError(f"{_BASE}_timeout")
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"), {"timeout": str(remaining)})


def _scope(audit, day):
    if (type(day) is not date or audit.get("schema") != RECEIPT_SET_VERSION
            or archive_evidence_hash({key: value for key, value in audit.items() if key != "audit_set_hash"})
            != audit.get("audit_set_hash")):
        raise ValueError(f"{_BASE}_invalid_audit")
    child = _prefix_days(audit["prefix"]).get(day.isoformat())
    if child is None or child["child"] != fact_header_partition_name(day) or not child["child_oid"]:
        raise ValueError(f"{_BASE}_wrong_day_scope")
    return child, audit["days"][day.isoformat()]


def _placement(conn, relation, *, parent, day):
    attached = _relation(conn, relation)
    if attached is None or attached[1:] != (parent, _partition_bound(day)):
        raise RuntimeError(f"{_BASE}_partition_binding_changed: relation={relation}")
    physical = _layout(conn, relation)
    if physical["acl"] is not None or physical["options"]:
        raise RuntimeError(f"{_BASE}_custom_placement_requires_review: relation={relation}")
    return {"physical": physical, "indexes": _indexes(conn, relation), "checks": _checks(conn, relation),
            "toast": [list(item) for item in _toast_placement(conn, relation)]}


def _routing(conn, day, *, limit):
    bindings = conn.execute(text(f"""
        SELECT series_id,min_observation_time,max_observation_time
        FROM {_ROUTING} WHERE storage_day=:day ORDER BY series_id LIMIT :limit
    """), {"day": day, "limit": limit + 1}).all()
    if len(bindings) > limit:
        raise RuntimeError(f"{_BASE}_routing_budget_exceeded")
    return [[series, start.astimezone(UTC).isoformat(), end.astimezone(UTC).isoformat()]
            for series, start, end in bindings]


def capture_initial_dated_donor(donor, *, donor_identity, day, audit, source_commit, max_routing_rows=100_000):
    """Capture one initially retired day's original native shape and retention state."""
    source = _require_snapshot(donor, donor_identity)
    child, detail = _scope(audit, day)
    if (type(max_routing_rows) is not int or not 1 <= max_routing_rows <= 100_000
            or type(source_commit) is not int or source_commit < audit["retired_prefix_max_commit_seq"]
            or any(donor_identity[key] != audit["source_database"][key] for key in ("system_identifier", "header_oid"))
            or PostgresMarketDataRepository._current_commit_seq_with_session(donor) != source_commit):
        raise RuntimeError(f"{_BASE}_donor_boundary_changed")
    _ready(donor, FACT_STORAGE_LAYOUT_VERSION)
    assert_fact_storage_contract(donor)
    header = _placement(donor, "market." + child["child"], parent=_HEADER_PARENT, day=day)
    created_at = donor.scalar(text(f"SELECT created_at FROM {_REGISTRY} WHERE storage_day=:day"), {"day": day})
    marker = _retention(donor, day)
    cohort = source.cohort(donor, day)
    if (header["physical"]["oid"] != child["child_oid"] or created_at is None
            or (cohort.state, cohort.sealed_at, cohort.expected_rows) != (
                detail["source_retention_state"], detail["source_sealed_at"], detail["expected_rows"],
            )):
        raise RuntimeError(f"{_BASE}_donor_cohort_changed")
    payload_relation = "market." + fact_partition_name(day)
    payload = None
    if cohort.state == "reclaimed":
        if donor.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": payload_relation}):
            raise RuntimeError(f"{_BASE}_reclaimed_payload_present")
    else:
        payload = _placement(donor, payload_relation, parent=_PAYLOAD_PARENT, day=day)
        _assert_native_guard(donor, payload_relation, cloned=True)
    routes = _routing(donor, day, limit=max_routing_rows)
    result = {"schema": VERSION, "donor_identity": donor_identity, "storage_day": day.isoformat(),
              "audit_set_hash": audit["audit_set_hash"], "source_commit": source_commit,
              "header": header, "payload": payload, "retention": marker,
              "registered_at": created_at.astimezone(UTC).isoformat(), "max_routing_rows": max_routing_rows,
              "routing_hash": archive_evidence_hash({"routes": routes}), "routing_count": len(routes)}
    return result | {"receipt_sha256": archive_evidence_hash(result)}


def _create_leaf(conn, relation, parent, placement):
    quote = conn.dialect.identifier_preparer.quote_identifier
    physical, checks = placement["physical"], placement["checks"]
    conn.exec_driver_sql(f"CREATE TABLE {relation} (LIKE {parent} INCLUDING DEFAULTS INCLUDING CONSTRAINTS "
                         f"INCLUDING STORAGE INCLUDING COMPRESSION) TABLESPACE {quote(physical['tablespace'])}")
    copied = _checks(conn, relation)
    if any(checks.get(name) != definition for name, definition in copied.items()):
        raise RuntimeError(f"{_BASE}_parent_checks_changed: relation={relation}")
    for name, definition in checks.items():
        if name not in copied:
            conn.exec_driver_sql(f"ALTER TABLE {relation} ADD CONSTRAINT {quote(name)} {definition}")
    if _layout(conn, relation)["columns"] != physical["columns"]:
        raise RuntimeError(f"{_BASE}_columns_changed: relation={relation}")


def _build_indexes(conn, relation, placement):
    quote = conn.dialect.identifier_preparer.quote_identifier
    for index in placement["indexes"]:
        name = quote(index["name"])
        access, separator, predicate = index["definition"].partition(" WHERE ")
        unique = "UNIQUE " if index["unique"] else ""
        conn.exec_driver_sql(f"CREATE {unique}INDEX {name} ON {relation}{access} "
                             f"TABLESPACE {quote(index['tablespace'])}{separator}{predicate}")
        if index["constraint_type"]:
            kind = "PRIMARY KEY" if index["constraint_type"] == "p" else "UNIQUE"
            constraint = quote(index["constraint_name"])
            conn.exec_driver_sql(f"ALTER TABLE {relation} ADD CONSTRAINT {constraint} {kind} USING INDEX {name}")


def _compare_placement(conn, relation, parent, day, expected):
    actual = _placement(conn, relation, parent=parent, day=day)
    oid = actual["physical"]["oid"]
    def normalize(placement):
        # Native ATTACH proves compatibility against the parent index. Replaying
        # a SQL definition can normalize equivalent implicit array casts, so
        # retain exact SQL comparison for indexes without that native proof.
        indexes = [{key: value for key, value in index.items()
                    if key != "oid" and (key != "definition" or index["parent"] is None)}
                   for index in placement["indexes"]]
        return placement | {"physical": {key: value for key, value in placement["physical"].items() if key != "oid"},
                            "indexes": indexes}
    found, pinned = normalize(actual), normalize(expected)
    if oid == expected["physical"]["oid"] or found != pinned:
        changed = sorted(key for key in pinned if found[key] != pinned[key])
        raise RuntimeError(f"{_BASE}_new_placement_changed: relation={relation} sections={changed}")
    return oid


def _copy_relation(target, donor, *, relation, columns, row_ids, expected, remaining, deadline, write):
    query = f"COPY (SELECT {columns} FROM ONLY {relation} WHERE id=ANY(%s) ORDER BY id) TO STDOUT (FORMAT binary)"
    with SpooledTemporaryFile(max_size=4 * 1024**2) as spool:
        source = _CopyWindow(spool, remaining=remaining, deadline=deadline)
        _bound(donor, deadline)
        with donor.connection.driver_connection.cursor() as cursor:
            cursor.copy_expert(cursor.mogrify(query, (row_ids,)), source)
            if cursor.rowcount != expected:
                raise RuntimeError(f"{_BASE}_donor_copy_row_count_changed")
        _bound(target, deadline)
        with target.connection.driver_connection.cursor() as cursor:
            if write:
                spool.seek(0)
                cursor.copy_expert(f"COPY {relation} ({columns}) FROM STDIN (FORMAT binary)", spool)
                if cursor.rowcount != expected:
                    raise RuntimeError(f"{_BASE}_target_copy_row_count_changed")
            result = _CopyWindow(None, remaining=source.bytes, deadline=deadline)
            cursor.copy_expert(cursor.mogrify(query, (row_ids,)), result)
            if cursor.rowcount != expected or (result.bytes, result.digest.digest()) != (source.bytes, source.digest.digest()):
                raise RuntimeError(f"{_BASE}_copy_bytes_changed")
    return source.bytes, source.digest.hexdigest()


def _attach(target, *, relation, parent, day, placement):
    _build_indexes(target, relation, placement)
    owner = target.dialect.identifier_preparer.quote_identifier(placement["physical"]["owner"])
    target.exec_driver_sql(f"ALTER TABLE {relation} OWNER TO {owner}")
    target.exec_driver_sql(f"ALTER TABLE {parent} ATTACH PARTITION {relation} {_partition_bound(day)}")


def restore_initial_dated_day(target, donor, *, fence, original, expected_original_hash, audit,
                              forward_receipts, store, donor_store, max_copy_bytes,
                              max_archive_bytes=64 * 1024**3, max_pages=4096, max_inventory_pages=100_000,
                              max_seconds=900, limits=FactArchiveLimits()):
    """Restore a bounded original dated placement, leaving cold authority and login fence active."""
    _require_disposable_fence(target, fence)
    root, backup = store.root.resolve(strict=True), donor_store.root.resolve(strict=True)
    if (target is donor or root.is_relative_to(backup) or backup.is_relative_to(root)
            or type(max_seconds) is not int or not 1 <= max_seconds <= MAX_MIGRATION_SECONDS
            or type(max_pages) is not int or not 1 <= max_pages <= 4096
            or type(max_inventory_pages) is not int or not max_pages <= max_inventory_pages <= 100_000
            or any(type(value) is not int or not 1 <= value <= 1024**4 for value in (max_copy_bytes, max_archive_bytes))):
        raise ValueError(f"{_BASE}_invalid_bounds")
    day = date.fromisoformat(original["storage_day"])
    child, detail = _scope(audit, day)
    deadline = min(fence.deadline, monotonic() + max_seconds)
    source = _require_snapshot(donor, original["donor_identity"])
    _bound(donor, deadline)
    if (original.get("receipt_sha256") != expected_original_hash or capture_initial_dated_donor(
        donor, donor_identity=original["donor_identity"], day=day, audit=audit, source_commit=original["source_commit"],
        max_routing_rows=original["max_routing_rows"],
    ) != original):
        raise RuntimeError(f"{_BASE}_donor_receipt_changed")
    cohort = source.cohort(donor, day)
    if detail["expected_pages"] > max_pages:
        raise RuntimeError(f"{_BASE}_page_budget_exceeded")
    batches = []
    for receipt in islice(forward_receipts, max_pages + 2):
        common = _validate_common(receipt)
        batch = receipt.get("day_batch")
        if (receipt["receipt_hash"] not in audit["receipt_hashes"] or any(audit[key] != value for key, value in common.items())
                or batch is None or batch["storage_day"] != day.isoformat()):
            raise RuntimeError(f"{_BASE}_unbound_day_receipt")
        batches.append(batch)
    if (not batches or len(batches) > max(1, max_pages) or _forward_day(day.isoformat(), batches, child["child"], {}) != detail):
        raise RuntimeError(f"{_BASE}_forward_scope_changed")
    old_reader = ColdHistoryReader(donor_store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(donor, pairs))
    new_reader = ColdHistoryReader(store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(target, pairs))
    header = "market." + child["child"]
    payload = "market." + fact_partition_name(day)
    io = ExecutionControl()
    io.limit(seconds=max(0.001, deadline - monotonic()))
    with archive_namespace(store.root, exclusive=True) as archive_check, _bounded_step(target, max_seconds) as clamp, controlled_execution(io):
        clamp(deadline - monotonic())
        fence.assert_quiet(target)
        archive_check()
        if not target.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"), {"name": _LIFECYCLE_LOCK_NAME}):
            raise RuntimeError(f"{_BASE}_lifecycle_busy")
        target.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        identity = PostgresMigrationSource(target.engine, expected_system_identifier=audit["database"]["system_identifier"]).identity(target)
        if (identity == original["donor_identity"] or identity["header_oid"] != audit["database"]["header_oid"]
                or any(identity[key] != fence.database[key] for key in ("system_identifier", "database", "database_oid"))):
            raise RuntimeError(f"{_BASE}_target_identity_changed")
        _ready(target, COLD_HISTORY_LAYOUT_VERSION)
        cert = target.scalar(text("SELECT evidence FROM market.fact_storage_state"))
        if (not isinstance(cert, dict)
                or (cert.get("audit_set_hash"), cert.get("loaded_inventory_hash"), cert.get("source_commit_seq")) != (
                    audit["audit_set_hash"], audit["loaded_inventory_hash"], original["source_commit"],
                )):
            raise RuntimeError(f"{_BASE}_target_certificate_changed")
        retired = target.execute(text("""
            SELECT handoff_kind,manifest_set_hash,row_count FROM market.cold_fact_retired_days WHERE storage_day=:day
        """), {"day": day}).one_or_none()
        if retired != ("initial", audit["audit_set_hash"], cohort.expected_rows) or _retention(target, day) != original["retention"]:
            raise RuntimeError(f"{_BASE}_target_cohort_changed")
        _assert_initial_inventory(target, audit, original["source_commit"], max_inventory_pages, deadline)
        header_exists, payload_exists = target.execute(text("SELECT to_regclass(:h) IS NOT NULL,to_regclass(:p) IS NOT NULL"),
                                                        {"h": header, "p": payload}).one()
        routing_limit = original["max_routing_rows"]
        routes = _routing(donor, day, limit=routing_limit)
        if archive_evidence_hash({"routes": routes}) != original["routing_hash"]:
            raise RuntimeError(f"{_BASE}_donor_routing_changed")
        if not header_exists:
            if payload_exists or _routing(target, day, limit=routing_limit) or target.scalar(text(
                f"SELECT EXISTS(SELECT FROM {_REGISTRY} WHERE storage_day=:day)"), {"day": day},
            ):
                raise RuntimeError(f"{_BASE}_partial_restore_scope")
            _create_leaf(target, header, _HEADER_PARENT, original["header"])
            if original["payload"] is not None:
                _create_leaf(target, payload, _PAYLOAD_PARENT, original["payload"])
            for relation, create in ((header, True), (payload, original["payload"] is not None)):
                if create:
                    target.exec_driver_sql(f"CREATE INDEX {relation.split('.')[1]}_restore_id ON {relation}(id)")
        elif payload_exists != (original["payload"] is not None):
            raise RuntimeError(f"{_BASE}_partial_restore_scope")
        copy_bytes = archive_bytes = 0
        content = archive_evidence_hash({"original": expected_original_hash})
        proofs = []
        for batch in batches:
            authorities = {item["ordinal"]: item["authority_hash"] for item in batch["source_archive_authority_receipts"]}
            for page in batch["page_receipts"]:
                _bound(donor, deadline)
                _bound(target, deadline)
                authority = authorities.get(page["ordinal"])
                size = target.scalar(text("SELECT byte_count FROM market.cold_fact_pages WHERE id=:id"), {"id": page["manifest_id"]})
                archive_bytes += (size or 0) * (2 if authority else 1)
                if archive_bytes > max_archive_bytes:
                    raise RuntimeError(f"{_BASE}_archive_budget_exceeded")
                proofs.append(_compare_page(donor, target, source, old_reader, new_reader, cohort=cohort,
                                           header_oid=child["child_oid"], page=page, authority=authority))
                ids = target.execute(text("""
                    SELECT id FROM market.cold_fact_locators WHERE page_id=:page ORDER BY ordinal LIMIT :limit
                """), {"page": page["manifest_id"], "limit": page["row_count"] + 1}).scalars().all()
                digests = []
                for relation, columns in ((header, _HEADER_COLUMNS), (payload, _PAYLOAD_COLUMNS)):
                    if relation == payload and original["payload"] is None:
                        continue
                    copied, digest = _copy_relation(target, donor, relation=relation, columns=columns,
                        row_ids=ids, expected=page["row_count"], remaining=max_copy_bytes - copy_bytes,
                        deadline=deadline, write=not header_exists)
                    copy_bytes += copied
                    digests.append(digest)
                content = archive_evidence_hash({"previous": content, "manifest": page["manifest_hash"], "copies": digests})
        _bound(donor, deadline)
        if _source_rows(donor, day=day, after=tuple(detail["last_cursor"]), through=None, limit=1):
            raise RuntimeError(f"{_BASE}_extra_donor_headers")
        if cohort.state in {"verified", "reclaimed"} and (
            any(proof is None for proof in proofs) or original["retention"]["manifest_set_hash"] !=
            _partition_manifest_set_hash(day, cohort.expected_rows, proofs)
        ):
            raise RuntimeError(f"{_BASE}_donor_manifest_set_changed")
        for relation, native in ((header, original["header"]), (payload, original["payload"])):
            if native is None:
                continue
            for conn in (donor, target):
                _bound(conn, deadline)
                if conn.scalar(text(f"SELECT count(*) FROM ONLY {relation}")) != cohort.expected_rows:
                    raise RuntimeError(f"{_BASE}_inverse_count_changed: relation={relation}")
        if not header_exists:
            target.exec_driver_sql("LOCK TABLE market.fact_identities IN ACCESS EXCLUSIVE MODE NOWAIT")
            target.exec_driver_sql("ALTER TABLE market.fact_identities DISABLE TRIGGER trg_require_fact_identity_header")
            target.exec_driver_sql(f"INSERT INTO market.fact_identities({_IDENTITY_FIELDS}) "
                                   f"SELECT {_IDENTITY_FIELDS} FROM ONLY {header} ON CONFLICT(id) DO NOTHING")
            target.exec_driver_sql("ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_require_fact_identity_header")
            target.exec_driver_sql(f"DROP INDEX {header}_restore_id")
            _attach(target, relation=header, parent=_HEADER_PARENT, day=day, placement=original["header"])
            target.execute(text(f"INSERT INTO {_REGISTRY}(storage_day,created_at) VALUES(:day,:created)"),
                           {"day": day, "created": datetime.fromisoformat(original["registered_at"])})
            if routes:
                target.execute(text(f"""
                    INSERT INTO {_ROUTING}(series_id,storage_day,min_observation_time,max_observation_time)
                    SELECT ids.series_id,:day,ids.first_seen,ids.last_seen
                    FROM unnest(CAST(:id AS bigint[]),CAST(:start AS timestamptz[]),CAST(:end AS timestamptz[]))
                      AS ids(series_id,first_seen,last_seen)
                """), {"day": day, "id": [series for series, _, _ in routes],
                         "start": [start for _, start, _ in routes], "end": [end for _, _, end in routes]})
            if original["payload"] is not None:
                target.exec_driver_sql(f"DROP INDEX {payload}_restore_id")
                _attach(target, relation=payload, parent=_PAYLOAD_PARENT, day=day, placement=original["payload"])
        assert_fact_identity_contract(target)
        header_oid = _compare_placement(target, header, _HEADER_PARENT, day, original["header"])
        payload_oid = None
        if original["payload"] is not None:
            payload_oid = _compare_placement(target, payload, _PAYLOAD_PARENT, day, original["payload"])
            _assert_native_guard(target, payload, cloned=True)
        registered = target.scalar(text(f"SELECT created_at FROM {_REGISTRY} WHERE storage_day=:day"), {"day": day})
        if (registered is None or registered != datetime.fromisoformat(original["registered_at"])
                or archive_evidence_hash({"routes": _routing(target, day, limit=routing_limit)}) != original["routing_hash"]
                or target.scalar(text(f"""SELECT EXISTS (SELECT FROM ONLY {header} h LEFT JOIN market.fact_identities i USING(id)
                    WHERE (i.storage_day,i.series_id,i.observation_key,i.revision)
                    IS DISTINCT FROM (h.storage_day,h.series_id,h.observation_key,h.revision))"""))
                or target.scalar(text("SELECT count(*) FROM market.fact_identities WHERE storage_day=:day"), {"day": day}) != cohort.expected_rows):
            raise RuntimeError(f"{_BASE}_identity_or_routing_changed")
        archive_check()
        fence.assert_quiet(target)
    result = {"schema": VERSION, "target_database": identity, "donor_identity": original["donor_identity"],
              "storage_day": day.isoformat(), "audit_set_hash": audit["audit_set_hash"], "row_count": cohort.expected_rows,
              "original_header_oid": child["child_oid"], "header_oid": header_oid,
              "original_payload_oid": original["payload"]["physical"]["oid"] if original["payload"] else None,
              "payload_oid": payload_oid, "retention_state": cohort.state, "retention_sha256": archive_evidence_hash(original["retention"]),
              "registered_at": original["registered_at"], "routing_sha256": original["routing_hash"],
              "original_receipt_sha256": expected_original_hash, "copy_bytes": copy_bytes,
              "verified_archive_bytes": archive_bytes, "ordered_content_hash": content}
    return result | {"receipt_sha256": archive_evidence_hash(result)}

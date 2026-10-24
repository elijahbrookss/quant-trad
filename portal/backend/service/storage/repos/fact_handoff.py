"""Ephemeral, bounded authority witnesses for a fresh destructive handoff.

Catalog tuple locations are private routing hints and their proof fields are
read again. Source lineage retains typed first-pass commitments; final authority
comes from locked immutable relations, current manifest material and freshly
verified catalogs. The durable v12 receipts and first-pass byte proof are
unchanged.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import re
import struct
import sys

from sqlalchemy import event, text

from portal.backend.db.fact_identity_schema import assert_fact_identity_contract
from portal.backend.db.fact_storage_schema import (
    HOT_PAYLOAD_VALIDATION_BODY,
    fact_partition_name,
)

# This is a private retained-memory refusal, not a larger verification budget.
_MAX_WITNESS_BYTES = 128 * 1024 * 1024
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
_DATE_EPOCH = date(2000, 1, 1)
_TID = re.compile(r"\((\d+),(\d+)\)\Z")
_CATALOGS = {
    "series": ("fact_archive_series", (
        ("series_id", "int"), ("row_count", "int"),
        *((name, "time") for name in (
            "first_observation_at", "last_observation_at", "first_known_at",
            "last_known_at", "first_accepted_at", "last_accepted_at")),
    )),
    "aliases": ("fact_archive_material_aliases", (
        ("fact_version_id", "text"), ("series_id", "int"),
        ("evidence_key", "text"), ("material_hash", "text"),
    )),
    "dependencies": ("fact_archive_dependencies", tuple((name, "text") for name in (
        "target_kind", "target_id", "object_key", "object_sha256"))),
    "canonical_dependencies": ("fact_archive_canonical_dependencies", (
        ("fact_version_id", "text"), ("row_hash", "text"))),
}
_HEADER = (("id", "text"), ("storage_day", "date"), ("row_hash", "text"),
           ("market_commit_seq", "int"), ("hot_present", "bool"))
_COLD = (("source_id", "text"), ("id", "text"), ("storage_day", "date"),
         ("page_ordinal", "int"), ("object_key", "text"),
         ("object_sha256", "text"), ("manifest_hash", "text"),
         ("row_count", "int"), ("byte_count", "int"),
         ("first_commit_seq", "int"), ("first_id", "text"),
         ("last_commit_seq", "int"), ("last_id", "text"),
         ("_handoff_descriptor", "text"))
_MANIFEST = _COLD[1:]
_SQL_TYPES = {"text": "text", "int": "bigint", "time": "timestamptz",
              "date": "date", "bool": "boolean"}
_OIDS = {"text": 25, "int": 20, "time": 1184, "date": 1082, "bool": 16}
_IMMUTABLE_BODY = """
BEGIN
    RAISE EXCEPTION 'immutable market-data relation %.% rejects %',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
"""


def _record(row, columns):
    """Exact pg_catalog.record_send(ROW(explicitly_cast_columns)) bytes."""
    parts = [struct.pack("!i", len(columns))]
    for name, kind in columns:
        value = row[name]
        if value is None:
            payload = None
        elif kind == "text" and type(value) is str:
            payload = value.encode("utf8")
        elif kind == "int" and type(value) is int:
            payload = struct.pack("!q", value)
        elif kind == "bool" and type(value) is bool:
            payload = bytes([value])
        elif kind == "date" and type(value) is date:
            payload = struct.pack("!i", (value - _DATE_EPOCH).days)
        elif kind == "time" and isinstance(value, datetime) and value.utcoffset() is not None:
            payload = struct.pack("!q", (value.astimezone(timezone.utc) - _EPOCH) // timedelta(microseconds=1))
        else:
            raise RuntimeError(f"canonical_handoff_scalar_invalid: column={name} kind={kind}")
        parts.append(struct.pack("!Ii", _OIDS[kind], -1 if payload is None else len(payload)))
        if payload is not None:
            parts.append(payload)
    return b"".join(parts)


def _digest(rows, columns):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_record(row, columns))
    return digest.hexdigest()


def _tid_key(value):
    found = _TID.fullmatch(value) if type(value) is str else None
    if found is None:
        raise RuntimeError("canonical_handoff_tuple_location_invalid")
    block, slot = map(int, found.groups())
    if block >= 2**32 or not 0 < slot < 2**16:
        raise RuntimeError("canonical_handoff_tuple_location_invalid")
    return block, slot


def _projection(columns, alias="q"):
    return ",".join(f'{alias}."{name}"::{_SQL_TYPES[kind]}' for name, kind in columns)


def _fingerprint_sql(columns, *, order, hashed_rows=False):
    record = "pg_catalog.record_send(ROW(" + _projection(columns) + "))"
    if hashed_rows:
        record = "sha256(" + record + ")"
    return ("encode(sha256(coalesce(string_agg(" + record
            + ",''::bytea ORDER BY " + order + "),''::bytea)),'hex')")


@contextmanager
def _bounded_sql(connection, check_budget):
    # Includes SQL issued inside reused schema reflection/admission helpers.
    checking = False
    def before(*_):
        nonlocal checking
        if not checking:
            checking = True
            try:
                check_budget()
            finally:
                checking = False
    event.listen(connection, "before_cursor_execute", before)
    try:
        before()
        yield
        before()
    finally:
        event.remove(connection, "before_cursor_execute", before)


def _retained_size(value):
    # Overcount shared references rather than omit retained container overhead.
    size = sys.getsizeof(value)
    if is_dataclass(value):
        return size + sum(_retained_size(getattr(value, field.name)) for field in fields(value))
    if isinstance(value, dict):
        return size + sum(_retained_size(k) + _retained_size(v) for k, v in value.items())
    if isinstance(value, (tuple, list, set)):
        return size + sum(_retained_size(item) for item in value)
    return size


@dataclass(frozen=True, slots=True)
class _Rows:
    oid: int
    columns: tuple
    tids: str
    count: int
    digest: str
    days: tuple = ()
    hashed_rows: bool = False
    table: str | None = None
    relation_identity: str | None = None


@dataclass(frozen=True, slots=True)
class _SourceBatch:
    header_count: int
    header_digest: str
    cold_count: int
    cold_digest: str


@dataclass(frozen=True, slots=True)
class _SourceCommitment:
    batches: tuple
    placement_hash: str


def _source_batch(headers, matches):
    headers = tuple(headers)
    matches = tuple(matches)
    header_ids = [row["id"] for row in headers]
    cold_ids = [row["source_id"] for row in matches]
    if header_ids != sorted(set(header_ids)) or cold_ids != sorted(set(cold_ids)):
        raise RuntimeError("canonical_handoff_source_commitment_order_invalid")
    return _SourceBatch(
        header_count=len(headers), header_digest=_digest(headers, _HEADER),
        cold_count=len(matches), cold_digest=_digest(matches, _COLD),
    )


class FactHandoffProof:
    """One invocation only; witnesses come from exact validated first-pass rows."""
    def __init__(self, *, check_budget):
        self.check_budget = check_budget
        self.relations = {}
        self.pages = {}
        self.catalogs = {}
        self.generations = {}
        self.source_batches = {}
        self.source_commitments = {}
        self.relation_kinds = {}
        self.header_days = {}
        self.hot_days = {}
        self.cold_days = set()
        self.cold_manifests = {}
        self.targets = set()
        self.retained_bytes = sum(sys.getsizeof(value) for value in (
            self.relations, self.pages, self.catalogs, self.generations,
            self.source_batches, self.source_commitments, self.relation_kinds,
            self.header_days, self.hot_days, self.cold_days,
            self.cold_manifests, self.targets))
        self.bound = None
        self.authority_prepared = False
        self.admitted = False
        self.headers_current = False
        self.fresh_catalogs = set()
        self.fresh_sources = set()
        self.retained_bytes += sys.getsizeof(self.fresh_catalogs) + sys.getsizeof(self.fresh_sources)

    def _keep(self, value):
        self.retained_bytes += _retained_size(value)
        if self.retained_bytes > _MAX_WITNESS_BYTES:
            raise RuntimeError("canonical_handoff_witness_memory_budget_exceeded")
        return value

    def _assign(self, mapping, key, value):
        before = sys.getsizeof(mapping)
        mapping[key] = value
        self.retained_bytes += sys.getsizeof(mapping) - before
        self._keep(None)

    def record_source_hash(self, manifest_id, value):
        if manifest_id not in self.source_batches or manifest_id in self.source_commitments:
            raise RuntimeError("canonical_handoff_source_scope_invalid")
        batches = tuple(self.source_batches.pop(manifest_id))
        self._assign(
            self.source_commitments, manifest_id,
            self._keep(_SourceCommitment(batches=batches, placement_hash=value)),
        )

    @property
    def source_row_count(self):
        return sum(batch.header_count for commitment in self.source_commitments.values()
                   for batch in commitment.batches)

    @staticmethod
    def _metadata(session, oid):
        row = session.execute(text("""
            SELECT c.oid,n.nspname,c.relname,c.relkind,c.relpersistence,
                   c.relrowsecurity,c.relforcerowsecurity,pg_relation_filenode(c.oid) AS filenode,
                   pg_get_expr(c.relpartbound,c.oid) AS partition_bound,
                   ARRAY(SELECT inhparent FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents,
                   (SELECT jsonb_agg(jsonb_build_array(attnum,attname,atttypid,atttypmod,attnotnull,attisdropped,attcollation)
                                     ORDER BY attnum)
                    FROM pg_attribute WHERE attrelid=c.oid AND attnum>0) AS columns
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=:oid
        """), {"oid": oid}).mappings().one_or_none()
        return dict(row) if row is not None else None

    def _relation(self, session, oid, *, table=None, kind="header"):
        if table is not None:
            kind = "catalog"
        if oid not in self.relations:
            current = self._metadata(session, oid)
            if (current is None or current["nspname"] != "market" or current["relkind"] != "r"
                    or current["relpersistence"] != "p" or current["relrowsecurity"]
                    or current["relforcerowsecurity"] or not current["filenode"]):
                raise RuntimeError("canonical_handoff_relation_scope_invalid")
            if table is not None:
                if current["relname"] != table or current["parents"]:
                    raise RuntimeError("canonical_handoff_catalog_relation_invalid")
            elif kind == "header" and not re.fullmatch(
                    r"fact_versions_(?:legacy|[0-9]{8})", current["relname"]):
                raise RuntimeError("canonical_handoff_header_relation_invalid")
            elif kind == "hot" and not re.fullmatch(
                    r"fact_hot_payloads_[0-9]{8}", current["relname"]):
                raise RuntimeError("canonical_handoff_payload_relation_invalid")
            self._assign(self.relations, oid, self._keep(current))
            self._assign(self.relation_kinds, oid, kind)
        elif table is not None and self.relations[oid]["relname"] != table:
            raise RuntimeError("canonical_handoff_catalog_relation_invalid")
        elif self.relation_kinds[oid] != kind:
            raise RuntimeError("canonical_handoff_relation_kind_changed")

    def _rows(self, session, rows, columns, *, table=None, days=()):
        if not rows:
            # Catalogs can be empty, but still bind their physical table.
            oid = session.execute(text("SELECT to_regclass(:relation)::oid"),
                                  {"relation": "market." + table}).scalar_one()
        else:
            oids = {row["_handoff_oid"] for row in rows}
            if len(oids) != 1:
                raise RuntimeError("canonical_handoff_mixed_physical_relation")
            oid = oids.pop()
        self._relation(session, oid, table=table)
        ordered = sorted(rows, key=lambda row: _tid_key(row["_handoff_tid"]))
        tids = [row["_handoff_tid"] for row in ordered]
        if len(set(map(_tid_key, tids))) != len(tids):
            raise RuntimeError("canonical_handoff_duplicate_tuple_location")
        self.check_budget()
        return self._keep(_Rows(oid, columns,
            "{" + ",".join('"' + value + '"' for value in tids) + "}",
            len(ordered), _digest(ordered, columns), tuple(days)))

    def capture_page(self, page, receipt, catalogs, physical, generation=None):
        identity = page["id"]
        if self.bound is not None or identity in self.pages:
            raise RuntimeError("canonical_handoff_page_scope_invalid")
        self._assign(self.pages, identity, self._keep((dict(page), dict(receipt))))
        self._assign(self.generations, identity,
                     self._keep(dict(generation)) if generation is not None else None)
        self._assign(self.source_batches, identity, [])
        # Physical fields are deliberately excluded from historical receipt hashing.
        self.retained_bytes += sys.getsizeof(physical) + sum(sys.getsizeof(key) for key in physical)
        self._assign(self.catalogs, identity, physical)
        for row in catalogs["dependencies"]:
            pair = (row["target_kind"], row["target_id"])
            if pair not in self.targets:
                before = sys.getsizeof(self.targets)
                self.targets.add(self._keep(pair))
                self.retained_bytes += sys.getsizeof(self.targets) - before
                self._keep(None)

    def catalog_rows(self, session, name, manifest_id, rows, *, table=None, relation_identity=None):
        legacy_table, columns = _CATALOGS[name]
        table = table or legacy_table
        permitted = {legacy_table}
        if name == "aliases":
            permitted.add("fact_archive_prepared_material_aliases")
        elif name == "canonical_dependencies":
            permitted.add("fact_archive_prepared_canonical_dependencies")
        if table not in permitted:
            raise RuntimeError("canonical_handoff_catalog_relation_invalid")
        keys = {"series": ("series_id",), "aliases": ("fact_version_id", "evidence_key"),
                "dependencies": ("target_kind", "target_id"), "canonical_dependencies": ("fact_version_id",)}[name]
        if len({tuple(row[key] for key in keys) for row in rows}) != len(rows):
            raise RuntimeError("canonical_handoff_duplicate_catalog_key")
        physical_identity = relation_identity or manifest_id
        identity_column = "preparation_id" if table.startswith("fact_archive_prepared_") else "manifest_id"
        columns = ((identity_column, "text"),) + columns
        witness = self._rows(
            session, [{**row, identity_column: physical_identity} for row in rows],
            columns, table=table,
        )
        return _Rows(witness.oid, witness.columns, witness.tids, witness.count,
                     witness.digest, witness.days, witness.hashed_rows, table,
                     physical_identity)

    def capture_sources(self, session, manifest_id, headers, matches):
        if (self.bound is not None or manifest_id not in self.pages
                or manifest_id not in self.source_batches
                or manifest_id in self.source_commitments):
            raise RuntimeError("canonical_handoff_source_scope_invalid")
        grouped = {}
        for row in headers:
            grouped.setdefault(row["_handoff_oid"], []).append(row)
        for oid, rows in grouped.items():
            self._relation(session, oid, kind="header")
            if oid not in self.header_days:
                self._assign(self.header_days, oid, set())
                self.retained_bytes += sys.getsizeof(self.header_days[oid])
            added = {row["storage_day"] for row in rows} - self.header_days[oid]
            for day in added:
                self._keep(day)
            before = sys.getsizeof(self.header_days[oid])
            self.header_days[oid].update(added)
            self.retained_bytes += sys.getsizeof(self.header_days[oid]) - before
        hot = {}
        for row in headers:
            oid = row.get("_handoff_hot_oid")
            if row["hot_present"]:
                if type(oid) is not int:
                    raise RuntimeError("canonical_handoff_payload_relation_missing")
                hot.setdefault(oid, []).append(row)
            elif oid is not None:
                raise RuntimeError("canonical_handoff_payload_relation_invalid")
        for oid, rows in hot.items():
            self._relation(session, oid, kind="hot")
            if oid not in self.hot_days:
                self._assign(self.hot_days, oid, set())
                self.retained_bytes += sys.getsizeof(self.hot_days[oid])
            added = {row["storage_day"] for row in rows} - self.hot_days[oid]
            for day in added:
                self._keep(day)
            before = sys.getsizeof(self.hot_days[oid])
            self.hot_days[oid].update(added)
            self.retained_bytes += sys.getsizeof(self.hot_days[oid]) - before
        for row in matches:
            digest = _digest([row], _MANIFEST)
            prior = self.cold_manifests.get(row["id"])
            if prior is not None and prior != digest:
                raise RuntimeError("canonical_handoff_manifest_conflict")
            if prior is None:
                self._assign(self.cold_manifests, self._keep(row["id"]), self._keep(digest))
            day = row["storage_day"]
            if day not in self.cold_days:
                self._keep(day)
                before = sys.getsizeof(self.cold_days)
                self.cold_days.add(day)
                self.retained_bytes += sys.getsizeof(self.cold_days) - before
        batch = self._keep(_source_batch(headers, matches))
        current = self.source_batches[manifest_id]
        before = sys.getsizeof(current)
        current.append(batch)
        self.retained_bytes += sys.getsizeof(current) - before
        self.check_budget()

    def bind(self, day, relation_oid, evidence, limits):
        if (self.bound is not None or len(self.pages) != evidence["page_count"]
                or set(self.generations) != set(self.pages)
                or set(self.source_commitments) != set(self.pages) or self.source_batches):
            raise RuntimeError("canonical_handoff_incomplete_first_proof")
        self.bound = self._keep((day, relation_oid, dict(evidence), limits))

    def admit(self, session, partition, limits, *, check_budget):
        with _bounded_sql(session.connection(), check_budget):
            self._admit(session, partition, limits)
        self.admitted = True

    @staticmethod
    def _assert_immutable_guards(session, relation_oids):
        function = session.execute(text("""
            SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,p.pronargs,
                   p.pronargdefaults,p.prorettype::regtype::text AS returns,
                   p.proconfig,l.lanname
            FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure('market.reject_immutable_mutation()')
        """)).mappings().one_or_none()
        normalize = lambda value: re.sub(r"\s+", "", value or "")
        if (function is None or normalize(function["prosrc"]) != normalize(_IMMUTABLE_BODY)
                or function["provolatile"] != "v" or function["prosecdef"]
                or function["proretset"] or function["pronargs"] != 0
                or function["pronargdefaults"] != 0 or function["returns"] != "trigger"
                or function["proconfig"] is not None or function["lanname"] != "plpgsql"):
            raise RuntimeError("canonical_handoff_immutable_function_changed")
        rows = session.execute(text("""
            SELECT t.tgrelid::bigint AS relation,t.tgparentid::bigint AS parent,
                   t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,t.tgnargs,
                   t.tgqual IS NULL AS unqualified,t.tgoldtable,t.tgnewtable,
                   t.tgfoid=to_regprocedure('market.reject_immutable_mutation()') AS matches
            FROM pg_trigger t
            WHERE t.tgrelid=ANY(CAST(:relations AS oid[]))
              AND t.tgname LIKE 'trg_reject_mutation_%' AND NOT t.tgisinternal
            ORDER BY t.tgrelid,t.tgname
        """), {"relations": sorted(relation_oids)}).mappings().all()
        by_relation = {}
        for row in rows:
            if row["relation"] in by_relation:
                raise RuntimeError("canonical_handoff_immutable_trigger_ambiguous")
            by_relation[row["relation"]] = row
        if set(by_relation) != set(relation_oids):
            raise RuntimeError("canonical_handoff_immutable_trigger_missing")
        for row in by_relation.values():
            if (row["tgtype"] != 27 or row["tgenabled"] not in {"O", "A"}
                    or row["tgdeferrable"] or row["tginitdeferred"]
                    or row["tgnargs"] != 0 or not row["unqualified"]
                    or row["tgoldtable"] is not None or row["tgnewtable"] is not None
                    or not row["matches"]):
                raise RuntimeError("canonical_handoff_immutable_trigger_changed")

    @staticmethod
    def _assert_hot_insert_guards(session, relation_oids):
        function = session.execute(text("""
            SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,p.pronargs,
                   p.pronargdefaults,p.prorettype::regtype::text AS returns,
                   p.proconfig,l.lanname
            FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure('market.assert_fact_hot_payload_valid()')
        """)).mappings().one_or_none()
        if (function is None or function["prosrc"] != HOT_PAYLOAD_VALIDATION_BODY
                or function["provolatile"] != "v" or function["prosecdef"]
                or function["proretset"] or function["pronargs"] != 0
                or function["pronargdefaults"] != 0 or function["returns"] != "trigger"
                or function["proconfig"] is not None or function["lanname"] != "plpgsql"):
            raise RuntimeError("canonical_handoff_payload_insert_function_changed")
        rows = session.execute(text("""
            SELECT t.tgrelid::bigint AS relation,t.tgtype,t.tgenabled,
                   t.tgdeferrable,t.tginitdeferred,t.tgnargs,
                   t.tgqual IS NULL AS unqualified,t.tgoldtable,t.tgnewtable,
                   t.tgfoid=to_regprocedure('market.assert_fact_hot_payload_valid()') AS matches
            FROM pg_trigger t
            WHERE t.tgrelid=ANY(CAST(:relations AS oid[]))
              AND t.tgname='trg_assert_fact_hot_payload_valid' AND NOT t.tgisinternal
            ORDER BY t.tgrelid
        """), {"relations": sorted(relation_oids)}).mappings().all()
        by_relation = {}
        for row in rows:
            if row["relation"] in by_relation:
                raise RuntimeError("canonical_handoff_payload_insert_trigger_ambiguous")
            by_relation[row["relation"]] = row
        if set(by_relation) != set(relation_oids):
            raise RuntimeError("canonical_handoff_payload_insert_trigger_missing")
        for row in by_relation.values():
            if (row["tgtype"] != 7 or row["tgenabled"] != "A"
                    or row["tgdeferrable"] or row["tginitdeferred"]
                    or row["tgnargs"] != 0 or not row["unqualified"]
                    or row["tgoldtable"] is not None or row["tgnewtable"] is not None
                    or not row["matches"]):
                raise RuntimeError("canonical_handoff_payload_insert_trigger_changed")

    def _verify_manifest_authority(self, session):
        identities = sorted(self.cold_manifests)
        seen = set()
        for offset in range(0, len(identities), 1000):
            batch = identities[offset:offset + 1000]
            rows = session.execute(text("""
                SELECT id,storage_day,page_ordinal,object_key,object_sha256,
                       manifest_hash,row_count,byte_count,first_commit_seq,first_id,
                       last_commit_seq,last_id,descriptor::text AS _handoff_descriptor
                FROM market.fact_archive_manifests
                WHERE id=ANY(CAST(:ids AS text[])) ORDER BY id
            """), {"ids": batch}).mappings().all()
            if len(rows) != len(batch) or {row["id"] for row in rows} != set(batch):
                raise RuntimeError("canonical_handoff_manifest_authority_changed")
            for row in rows:
                if _digest([row], _MANIFEST) != self.cold_manifests[row["id"]]:
                    raise RuntimeError("canonical_handoff_manifest_authority_changed")
                seen.add(row["id"])
        if seen != set(identities):
            raise RuntimeError("canonical_handoff_manifest_authority_changed")
        if self.cold_days:
            states = session.execute(text("""
                SELECT storage_day,state FROM market.fact_retention_partitions
                WHERE storage_day=ANY(CAST(:days AS date[]))
            """), {"days": sorted(self.cold_days)}).mappings().all()
            if ({row["storage_day"] for row in states} != self.cold_days
                    or any(row["state"] not in {"verified", "reclaimed"} for row in states)):
                raise RuntimeError("canonical_handoff_manifest_authority_changed")
            overlap = session.execute(text("""
                WITH ordered AS (
                    SELECT storage_day,first_commit_seq,first_id,
                           lag(last_commit_seq) OVER (
                               PARTITION BY storage_day ORDER BY first_commit_seq,first_id
                           ) AS previous_commit,
                           lag(last_id) OVER (
                               PARTITION BY storage_day ORDER BY first_commit_seq,first_id
                           ) AS previous_id
                    FROM market.fact_archive_manifests
                    WHERE storage_day=ANY(CAST(:days AS date[]))
                )
                SELECT storage_day FROM ordered
                WHERE previous_commit IS NOT NULL
                  AND (first_commit_seq,first_id)<=(previous_commit,previous_id)
                LIMIT 1
            """), {"days": sorted(self.cold_days)}).scalar_one_or_none()
            if overlap is not None:
                raise RuntimeError("canonical_handoff_manifest_ranges_overlap")

    def _admit(self, session, partition, limits):
        if self.bound is None or self.admitted or self.bound[0] != partition["storage_day"] or self.bound[3] != limits:
            raise RuntimeError("canonical_handoff_scope_invalid")
        if self.bound[2]["manifest_set_hash"] != partition["manifest_set_hash"]:
            raise RuntimeError("canonical_handoff_evidence_changed")
        # No waiting and no advisory-lock upgrade. These short relation locks
        # freeze DDL/rewrites and prevent uncoordinated catalog/header DML.
        for relation in (
            "market.fact_identities", "market.fact_versions",
            "market.fact_hot_payloads", "market.fact_retention_partitions",
            "market.fact_archive_manifests", "market.fact_archive_active_generations",
        ):
            session.execute(text(f"LOCK TABLE ONLY {relation} IN SHARE MODE NOWAIT"))
        for oid, expected in sorted(self.relations.items()):
            name = expected["relname"]
            session.execute(text(f'LOCK TABLE ONLY market."{name}" IN SHARE MODE NOWAIT'))
            if self._metadata(session, oid) != expected:
                raise RuntimeError("canonical_handoff_relation_changed")
        # Reuse the canonical admission (keys, nondeferrable validated composite
        # FK, legacy seals, inherited indexes/references and partition topology).
        assert_fact_identity_contract(session.connection())
        if session.execute(text("SELECT current_setting('session_replication_role')")).scalar_one() != "origin":
            raise RuntimeError("canonical_handoff_identity_enforcement_invalid")
        header_root = session.execute(text(
            "SELECT 'market.fact_versions'::regclass::oid"
        )).scalar_one()
        hot_root = session.execute(text(
            "SELECT 'market.fact_hot_payloads'::regclass::oid"
        )).scalar_one()
        hot_insert_guards = {hot_root, *self.hot_days}
        if self.cold_days:
            names = ["market." + fact_partition_name(day) for day in sorted(self.cold_days)]
            existing = session.execute(text("""
                SELECT requested,c.oid::bigint AS oid,c.relkind,c.relpersistence,
                       c.relrowsecurity,c.relforcerowsecurity,
                       pg_get_expr(c.relpartbound,c.oid) AS partition_bound,
                       ARRAY(SELECT inhparent FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents
                FROM unnest(CAST(:relations AS text[])) requested
                LEFT JOIN pg_class c ON c.oid=to_regclass(requested)
            """), {"relations": names}).mappings().all()
            if len(existing) != len(names):
                raise RuntimeError("canonical_handoff_payload_day_scope_invalid")
            day_by_name = {"market." + fact_partition_name(day): day for day in self.cold_days}
            for row in existing:
                if row["oid"] is None:
                    continue
                day = day_by_name[row["requested"]]
                until = day + timedelta(days=1)
                expected_bound = (
                    f"FOR VALUES FROM ('{day.isoformat()}') "
                    f"TO ('{until.isoformat()}')"
                )
                if (row["relkind"] != "r" or row["relpersistence"] != "p"
                        or row["relrowsecurity"] or row["relforcerowsecurity"]
                        or row["partition_bound"] != expected_bound
                        or row["parents"] != [hot_root]):
                    raise RuntimeError("canonical_handoff_payload_day_scope_invalid")
                name = fact_partition_name(day)
                session.execute(text(
                    f'LOCK TABLE ONLY market."{name}" IN SHARE MODE NOWAIT'
                ))
                hot_insert_guards.add(row["oid"])
        immutable = {
            header_root, hot_root,
            session.execute(text(
                "SELECT 'market.fact_archive_manifests'::regclass::oid"
            )).scalar_one(),
        }
        for oid, relation in self.relations.items():
            kind = self.relation_kinds[oid]
            if kind == "hot":
                if relation["parents"] != [hot_root]:
                    raise RuntimeError("canonical_handoff_payload_membership_changed")
                days = self.hot_days.get(oid, set())
                if len(days) != 1:
                    raise RuntimeError("canonical_handoff_payload_day_scope_invalid")
                day = next(iter(days))
                if relation["relname"] != "fact_hot_payloads_" + day.strftime("%Y%m%d"):
                    raise RuntimeError("canonical_handoff_payload_day_scope_invalid")
                immutable.add(oid)
                continue
            if kind != "header":
                continue
            if relation["parents"] != [header_root]:
                raise RuntimeError("canonical_handoff_header_membership_changed")
            if relation["relname"] != "fact_versions_legacy":
                days = self.header_days.get(oid, set())
                if len(days) != 1:
                    raise RuntimeError("canonical_handoff_header_day_scope_invalid")
                day = next(iter(days))
                if relation["relname"] != "fact_versions_" + day.strftime("%Y%m%d"):
                    raise RuntimeError("canonical_handoff_header_day_scope_invalid")
                immutable.add(oid)
            checks = session.execute(text("""
                SELECT p.proname,t.tgtype,t.tgenabled,t.tgqual IS NULL AS unqualified
                FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
                JOIN pg_namespace n ON n.oid=p.pronamespace
                JOIN pg_constraint c ON c.oid=t.tgconstraint
                WHERE t.tgrelid=:oid AND t.tgisinternal AND n.nspname='pg_catalog'
                  AND c.contype='f' AND c.convalidated AND NOT c.condeferrable
                  AND pg_get_constraintdef(c.oid)=
                    'FOREIGN KEY (id, storage_day) REFERENCES market.fact_identities(id, storage_day) ON DELETE RESTRICT'
                  AND p.proname IN ('RI_FKey_check_ins','RI_FKey_check_upd')
            """), {"oid": oid}).mappings().all()
            if (len(checks) != 2 or {(row["proname"], row["tgtype"]) for row in checks} != {
                    ("RI_FKey_check_ins", 5), ("RI_FKey_check_upd", 17)}
                    or any(row["tgenabled"] not in {"O", "A"} or not row["unqualified"] for row in checks)):
                raise RuntimeError("canonical_handoff_identity_enforcement_invalid")
        self._assert_immutable_guards(session, immutable)
        self._assert_hot_insert_guards(session, hot_insert_guards)

    def verify_catalogs(self, session, page, receipt, check_budget):
        self.verify_catalog_batch(session, [(page, receipt)], check_budget)

    def verify_catalog_batch(self, session, pages, check_budget):
        # Every hinted field and each manifest's logical count are still read
        # now, under the admitted locks. Batch transport, never proof authority.
        if (not self.admitted or len({page["id"] for page, _ in pages}) != len(pages)
                or any(self.pages.get(page["id"]) != (dict(page), dict(receipt))
                       for page, receipt in pages)):
            raise RuntimeError("canonical_archive_verification_missing_or_stale: handoff page scope")
        generation_rows = session.execute(text("""
            SELECT manifest_id,preparation_id,generation,catalog_commitment
            FROM market.fact_archive_active_generations
            WHERE manifest_id=ANY(CAST(:ids AS text[]))
        """), {"ids": [page["id"] for page, _ in pages]}).mappings().all()
        current_generations = {page["id"]: None for page, _ in pages}
        for generation in generation_rows:
            identity = generation["manifest_id"]
            if identity not in current_generations or current_generations[identity] is not None:
                raise RuntimeError("canonical_archive_generation_pointer_scope_changed")
            current_generations[identity] = dict(generation)
        if any(self.generations[identity] != generation
               for identity, generation in current_generations.items()):
            raise RuntimeError("canonical_archive_generation_pointer_changed")
        for name, (legacy_table, _) in _CATALOGS.items():
            all_requested = [(page["id"], self.catalogs[page["id"]][name]) for page, _ in pages
                             if name in self.catalogs[page["id"]]]
            relations = sorted({witness.table or legacy_table for _, witness in all_requested})
            for table in relations:
                requested = [(identity, witness) for identity, witness in all_requested
                             if (witness.table or legacy_table) == table]
                offset = 0
                while offset < len(requested):
                    check_budget()
                    batch, chars = [], 0
                    # Bound round trips and parameter volume without rejecting a
                    # single previously admissible large page. Its witness already
                    # passed the unchanged retained-memory refusal.
                    while offset < len(requested) and len(batch) < 16:
                        identity, witness = requested[offset]
                        if batch and chars + len(witness.tids) > 256 * 1024:
                            break
                        batch.append((identity, witness))
                        chars += len(witness.tids)
                        offset += 1
                    columns = batch[0][1].columns
                    if any(witness.columns != columns for _, witness in batch):
                        raise RuntimeError("canonical_handoff_catalog_scope_invalid")
                    statements, parameters = [], {}
                    for index, (identity, witness) in enumerate(batch):
                        # Independent bound constants keep the existing direct TID
                        # lookup plan; no correlated outer-variable TID scan.
                        identity_key, lookup_key, tids_key = f"id_{index}", f"lookup_{index}", f"tids_{index}"
                        identity_column = "preparation_id" if table.startswith("fact_archive_prepared_") else "manifest_id"
                        statements.append(
                            f"SELECT CAST(:{identity_key} AS text) AS manifest_id, proof.*, "
                            "(SELECT count(*) FROM market." + table
                            + f" WHERE {identity_column}=:{lookup_key}) AS logical_count FROM "
                            "(SELECT count(*) AS hinted_count,"
                            + _fingerprint_sql(columns, order="q.ctid")
                            + " AS digest FROM market." + table
                            + f" q WHERE q.ctid=ANY(CAST(:{tids_key} AS tid[]))) AS proof")
                        parameters[identity_key] = identity
                        parameters[lookup_key] = witness.relation_identity or identity
                        parameters[tids_key] = witness.tids
                    rows = session.execute(text(" UNION ALL ".join(statements)), parameters).mappings().all()
                    check_budget()
                    current = {row["manifest_id"]: row for row in rows}
                    if len(rows) != len(batch) or set(current) != {identity for identity, _ in batch}:
                        raise RuntimeError("canonical_archive_verification_missing_or_stale: catalog result scope")
                    for identity, witness in batch:
                        proof = current[identity]
                        if (proof["hinted_count"] != witness.count or proof["logical_count"] != witness.count
                                or proof["digest"] != witness.digest):
                            raise RuntimeError(f"canonical_archive_verification_missing_or_stale: manifest_id={identity} catalog={name}")
        check_budget()
        before = sys.getsizeof(self.fresh_catalogs)
        self.fresh_catalogs.update(page["id"] for page, _ in pages)
        self.retained_bytes += sys.getsizeof(self.fresh_catalogs) - before
        self._keep(None)

    def prepare_headers(self, session, check_budget):
        if self.bound is None or self.admitted or self.authority_prepared:
            raise RuntimeError("canonical_handoff_preparation_scope_invalid")
        for manifest_id, commitment in self.source_commitments.items():
            witness = self.catalogs[manifest_id]["canonical_dependencies"]
            if (sum(batch.header_count for batch in commitment.batches) != witness.count
                    or any(batch.cold_count > batch.header_count for batch in commitment.batches)
                    or not re.fullmatch(r"[0-9a-f]{64}", commitment.placement_hash)):
                raise RuntimeError("canonical_handoff_source_commitment_invalid")
        check_budget()
        self.authority_prepared = True

    def verify_headers(self, session, check_budget):
        if not self.admitted or not self.authority_prepared:
            raise RuntimeError("canonical_handoff_not_admitted")
        self._verify_manifest_authority(session)
        check_budget()
        self.headers_current = True

    def verify_sources(self, session, manifest_id, check_budget):
        if not self.admitted or not self.headers_current or manifest_id not in self.fresh_catalogs:
            raise RuntimeError("canonical_handoff_source_proof_not_current")
        before = sys.getsizeof(self.fresh_sources)
        self.fresh_sources.add(manifest_id)
        self.retained_bytes += sys.getsizeof(self.fresh_sources) - before
        self._keep(None)

    def source_hash(self, manifest_id):
        if manifest_id not in self.fresh_sources:
            raise RuntimeError("canonical_handoff_source_proof_not_current")
        return self.source_commitments[manifest_id].placement_hash

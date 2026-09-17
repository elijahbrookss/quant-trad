"""Read-only PostgreSQL catalog observations for historical header placement.

Physical target IDs remain unbound: a tablespace name/path is not filesystem
UUID evidence. This boundary never creates schema, inspects a disk, or moves data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic

from sqlalchemy import text

from core.storage_header_placement import (
    HeaderPartitionPlacement, HeaderPlacementSnapshot, RelationPlacement,
)
from portal.backend.db.fact_identity_schema import fact_header_partition_name


@dataclass(frozen=True)
class HeaderCatalogInventory:
    snapshot: HeaderPlacementSnapshot
    # Paths are in the PostgreSQL server's namespace, never the API host's.
    physical_locations: tuple[dict, ...]
    server_data_directory: str
    postmaster_started_at: datetime
    filesystem_bindings_verified: bool = False


def read_header_catalog(engine, *, max_partitions=4096, timeout_seconds=30):
    """Observe bounded catalog metadata using the caller's PG_DSN engine.

    Access-share locks prevent destructive rewrites while allowing collection.
    Catalog/size observations are not a reservation or execution certificate;
    concurrent growth and index maintenance require a fresh execution check.
    The caller must have SELECT on headers/catalog and pg_control_system access.
    """
    if type(max_partitions) is not int or not 1 <= max_partitions <= 4096:
        raise ValueError("header_catalog_invalid: partition budget")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("header_catalog_invalid: time budget")
    deadline = monotonic() + timeout_seconds
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))

            def query(sql, parameters=None):
                remaining = int((deadline - monotonic()) * 1000)
                if remaining <= 0:
                    raise RuntimeError("header_catalog_time_budget_exceeded")
                conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                             {"timeout": str(remaining)})
                return conn.execute(text(sql), parameters or {})

            # ONLY avoids locking an unbounded descendant inventory before admission.
            query("LOCK TABLE ONLY market.fact_versions, ONLY market.fact_header_partitions IN ACCESS SHARE MODE")
            context = query("""
                SELECT clock_timestamp() AS captured_at,
                       current_setting('data_directory') AS data_directory,
                       pg_postmaster_start_time() AS postmaster_started_at,
                       current_setting('server_version_num')::integer AS version,
                       current_setting('transaction_read_only') AS read_only,
                       c.system_identifier::text || '/' || d.oid::text AS database_identity,
                       d.oid::bigint AS database_oid, d.dattablespace::bigint AS default_tablespace
                FROM pg_control_system() c
                CROSS JOIN pg_database d WHERE d.datname=current_database()
            """).mappings().one()
            if context["version"] // 10000 != 15 or context["read_only"] != "on":
                raise RuntimeError("header_catalog_database_contract_unqualified")
            parent = query("""
                SELECT relkind, relpersistence, pg_get_partkeydef(oid) AS partition_key
                FROM pg_class WHERE oid='market.fact_versions'::regclass
            """).mappings().one()
            if tuple(parent.values()) != ("p", "p", "RANGE (storage_day)"):
                raise RuntimeError("header_catalog_parent_incompatible")
            days = list(query("""
                SELECT storage_day FROM market.fact_header_partitions
                ORDER BY storage_day LIMIT :limit
            """, {"limit": max_partitions + 1}).scalars())
            if len(days) > max_partitions:
                raise RuntimeError("header_catalog_partition_budget_exceeded")
            expected = {fact_header_partition_name(day): day for day in days}
            if len(expected) != len(days):
                raise RuntimeError("header_catalog_duplicate_day")
            children = list(query("""
                SELECT c.oid::bigint AS oid, n.nspname AS schema, c.relname AS name,
                       c.relkind AS kind, c.relpersistence AS persistence,
                       pg_get_expr(c.relpartbound,c.oid) AS bound
                FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE i.inhparent='market.fact_versions'::regclass
                ORDER BY c.oid LIMIT :limit
            """, {"limit": max_partitions + 1}).mappings())
            if len(children) != len(days) or {row["name"] for row in children} != set(expected):
                raise RuntimeError("header_catalog_registry_attachment_mismatch")
            for row in children:
                day = expected[row["name"]]
                bound = f"FOR VALUES FROM ('{day.isoformat()}') TO ('{(day + timedelta(days=1)).isoformat()}')"
                if (row["schema"], row["kind"], row["persistence"], row["bound"]) != ("market", "r", "p", bound):
                    raise RuntimeError("header_catalog_partition_incompatible")
            if children:
                # Every identifier comes from a validated date, never a free-form label.
                names = ", ".join('ONLY market."' + name + '"' for name in sorted(expected))
                query("LOCK TABLE " + names + " IN ACCESS SHARE MODE")
            oids = [row["oid"] for row in children]
            relations = list(query("""
                WITH bounded AS MATERIALIZED (
                    SELECT c.* FROM pg_class c WHERE c.oid=ANY(:oids)
                    ORDER BY c.oid LIMIT :limit
                )
                SELECT c.oid::bigint AS oid, c.relfilenode::bigint AS relfilenode,
                       n.nspname AS schema, c.relname AS name,
                       pg_table_size(c.oid) AS byte_count,
                       COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint AS tablespace_oid,
                       pg_relation_filepath(c.oid) AS relative_path,
                       c.reltoastrelid::bigint AS toast_oid,
                       CASE WHEN c.reltoastrelid=0 THEN true ELSE EXISTS (
                           SELECT 1 FROM pg_class t WHERE t.oid=c.reltoastrelid
                           AND t.relkind='t' AND t.relpersistence='p'
                           AND COALESCE(NULLIF(t.reltablespace,0),d.dattablespace)
                               = COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
                           AND EXISTS (SELECT 1 FROM pg_index ti WHERE ti.indrelid=t.oid)
                           AND NOT EXISTS (
                               SELECT 1 FROM pg_index ti JOIN pg_class ix ON ix.oid=ti.indexrelid
                               WHERE ti.indrelid=t.oid AND (
                                   NOT ti.indisvalid OR NOT ti.indisready OR NOT ti.indislive
                                   OR COALESCE(NULLIF(ix.reltablespace,0),d.dattablespace)
                                      <> COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
                               )
                           )
                       ) END AS toast_colocated
                FROM bounded c JOIN pg_namespace n ON n.oid=c.relnamespace
                CROSS JOIN pg_database d WHERE d.datname=current_database()
                ORDER BY c.oid
            """, {"oids": oids, "limit": max_partitions + 1}).mappings())
            if len(relations) != len(children):
                raise RuntimeError("header_catalog_relations_changed")
            indexes = list(query("""
                WITH bounded AS MATERIALIZED (
                    SELECT i.indrelid, i.indisvalid, i.indisready, i.indislive, c.*
                    FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                    WHERE i.indrelid=ANY(:oids) ORDER BY c.oid LIMIT :limit
                )
                SELECT c.indrelid::bigint AS table_oid,
                       c.oid::bigint AS oid, c.relfilenode::bigint AS relfilenode,
                       n.nspname AS schema, c.relname AS name,
                       c.indisvalid AND c.indisready AND c.indislive
                           AND c.relkind='i' AND c.relpersistence='p' AS admitted,
                       pg_total_relation_size(c.oid) AS byte_count,
                       COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint AS tablespace_oid,
                       pg_relation_filepath(c.oid) AS relative_path
                FROM bounded c JOIN pg_namespace n ON n.oid=c.relnamespace
                CROSS JOIN pg_database d WHERE d.datname=current_database()
                ORDER BY c.oid
            """, {"oids": oids, "limit": len(oids) * 64 + 1}).mappings())
            grouped = {oid: [] for oid in oids}
            for row in indexes:
                grouped[row["table_oid"]].append(row)
            if any(len(items) > 64 for items in grouped.values()) or len(indexes) > len(oids) * 64:
                raise RuntimeError("header_catalog_index_budget_exceeded")
            if any(not row["admitted"] for row in indexes):
                raise RuntimeError("header_catalog_index_unproven")
            tablespace_ids = sorted({row["tablespace_oid"] for row in [*relations, *indexes]})
            tablespaces = {row["oid"]: dict(row) for row in query("""
                SELECT oid::bigint AS oid, spcname AS name, pg_tablespace_location(oid) AS location
                FROM pg_tablespace WHERE oid=ANY(:oids) ORDER BY oid
            """, {"oids": tablespace_ids}).mappings()}
            if set(tablespaces) != set(tablespace_ids):
                raise RuntimeError("header_catalog_tablespace_changed")

            def placement(row):
                return RelationPlacement(row["oid"], row["relfilenode"], row["schema"],
                                         row["name"], None, row["byte_count"])

            partitions = tuple(HeaderPartitionPlacement(
                expected[row["name"]], placement(row),
                tuple(placement(index) for index in grouped[row["oid"]]),
                True, row["toast_colocated"],
            ) for row in relations)
            if monotonic() >= deadline:
                raise RuntimeError("header_catalog_time_budget_exceeded")
            captured = context["captured_at"]
            snapshot = HeaderPlacementSnapshot(context["database_identity"],
                                               captured.astimezone(UTC).date(), captured,
                                               partitions, True)
            locations = tuple({
                "relation_oid": row["oid"], "tablespace_oid": row["tablespace_oid"],
                "tablespace_name": tablespaces[row["tablespace_oid"]]["name"],
                "tablespace_location": tablespaces[row["tablespace_oid"]]["location"],
                "database_oid": context["database_oid"],
                "database_default": row["tablespace_oid"] == context["default_tablespace"],
                "relative_path": row["relative_path"],
            } for row in [*relations, *indexes])
            return HeaderCatalogInventory(snapshot, locations, context["data_directory"],
                                          context["postmaster_started_at"])

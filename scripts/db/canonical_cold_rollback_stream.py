"""Compare one stream's operational state and archive custody on a rollback clone."""

from __future__ import annotations

from time import monotonic

from sqlalchemy import text

from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_restore_references import _database_identity, _require_disposable_fence
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_rollback_stream.v1"
_RAW = "(SELECT id FROM market.raw_archive_manifests WHERE definition_id=:definition)"
_CHECKPOINT = """(SELECT id FROM market.book_checkpoint_manifests WHERE series_id=:series)"""
_TARGETS = f"""((r.target_kind='raw_manifest' AND r.target_id IN {_RAW}) OR
               (r.target_kind='book_checkpoint' AND r.target_id IN {_CHECKPOINT}))"""
_DATASETS = f"""(SELECT dataset_id FROM market.dataset_series WHERE series_id=:series
                UNION SELECT dataset_id FROM market.dataset_archive_refs WHERE raw_archive_manifest_id IN {_RAW})"""
_REQUIRED_RAW = f"""(SELECT raw_archive_manifest_id FROM market.dataset_archive_refs WHERE dataset_id IN {_DATASETS}
                    UNION SELECT jsonb_array_elements_text(source_manifest_ids)
                        FROM market.book_checkpoint_manifests
                        WHERE series_id=:series AND NOT EXISTS (
                            SELECT FROM market.storage_lifecycle_events e
                            WHERE e.action='archive_expire' AND e.event_type='completed'
                              AND e.target_kind='book_checkpoint' AND e.target_id=book_checkpoint_manifests.id
                        )
                    UNION SELECT target_id FROM market.fact_book_prefix_dependencies
                        WHERE chunk_id IN (SELECT id FROM market.fact_book_prefix_chunks WHERE definition_id=:definition))"""

# SQL scope and stable ordering are defined here. No caller-controlled SQL.
_RELATIONS = {
    "market.stream_definitions": ("r.id=:definition", "r.id"),
    "market.sources": ("r.id=:source", "r.id"),
    "market.series": ("r.id=:series", "r.id"),
    "market.product_definition_versions": ("r.source_id=:source AND r.provider_product_id=:product", "r.id"),
    "market.stream_lease_state": ("r.definition_id=:definition", "r.definition_id"),
    "market.stream_session_events": ("r.definition_id=:definition", "r.id"),
    "market.stream_coverage_interval_versions": ("r.definition_id=:definition", "r.id"),
    "market.stream_quality_events": ("r.definition_id=:definition", "r.id"),
    "market.collector_operation_events": ("r.collector_kind='continuous_stream' AND r.collector_id=:definition", "r.id"),
    "market.collector_safety_state": ("r.scope_type IN ('global','fleet') OR "
                                     "(r.scope_type='stream' AND r.scope_id=:definition)", "r.scope_type,r.scope_id"),
    "market.collector_safety_events": ("r.scope_type IN ('global','fleet') OR "
                                      "(r.scope_type='stream' AND r.scope_id=:definition)", "r.id"),
    "market.raw_archive_manifests": ("r.definition_id=:definition", "r.id"),
    "market.raw_archive_ranges": (f"r.manifest_id IN {_RAW}", "r.manifest_id,r.provider_product_id,r.channel"),
    "market.raw_archive_record_mappings": (f"r.manifest_id IN {_RAW}", "r.manifest_id,r.object_row_index"),
    "market.raw_archive_compaction_sources": (f"r.source_manifest_id IN {_RAW} OR r.replacement_manifest_id IN {_RAW}",
                                              "r.replacement_manifest_id,r.source_manifest_id"),
    "market.market_trade_identities": ("r.source_id=:source AND r.provider_product_id=:product", "r.id"),
    "market.book_validity_interval_versions": ("r.series_id=:series", "r.id"),
    "market.book_reconstruction_state": ("r.series_id=:series", "r.series_id"),
    "market.book_operational_rollups": ("r.series_id=:series", "r.series_id"),
    "market.book_checkpoint_manifests": ("r.series_id=:series", "r.id"),
    "market.book_quality_event_links": (
        "r.quality_event_id IN (SELECT id FROM market.stream_quality_events WHERE definition_id=:definition)",
        "r.quality_event_id,r.validity_interval_id",
    ),
    "market.fact_book_prefix_chunks": ("r.definition_id=:definition", "r.id"),
    "market.fact_book_prefix_dependencies": (
        f"""r.target_id IN {_RAW} OR r.chunk_id IN (
              SELECT id FROM market.fact_book_prefix_chunks WHERE definition_id=:definition)""",
        "r.chunk_id,r.target_id",
    ),
    "market.archive_retention_pin_versions": (_TARGETS, "r.id"),
    "market.storage_lifecycle_events": (_TARGETS, "r.id"),
    "market.fact_archive_dependencies": (_TARGETS, "r.manifest_id,r.target_kind,r.target_id"),
    "market.fact_archive_preparation_dependencies": (_TARGETS, "r.preparation_id,r.target_kind,r.target_id"),
    "market.dataset_archive_refs": (f"r.dataset_id IN {_DATASETS}", "r.dataset_id,r.raw_archive_manifest_id"),
    "market.datasets": (f"r.id IN {_DATASETS}", "r.id"),
    "market.dataset_series": (f"r.dataset_id IN {_DATASETS}", "r.dataset_id,r.series_id"),
}


def _check_deadline(deadline):
    if monotonic() >= deadline:
        raise TimeoutError("canonical_cold_rollback_stream_timeout")


def _capture_rows(conn, params, *, max_rows, deadline):
    result = {}
    total = 0
    for name, (predicate, order) in _RELATIONS.items():
        _check_deadline(deadline)
        digest = archive_evidence_hash({"relation": name})
        count = 0
        query = text(f"""SELECT to_jsonb(r) FROM {name} r WHERE {predicate}
                         ORDER BY {order} LIMIT :limit""").execution_options(yield_per=512)
        with conn.execute(query, params | {"limit": max_rows - total + 1}) as rows:
            for row in rows.scalars():
                _check_deadline(deadline)
                total += 1
                count += 1
                if total > max_rows:
                    raise RuntimeError("canonical_cold_rollback_stream_row_budget_exceeded")
                digest = archive_evidence_hash({"previous": digest, "row": row})
        result[name] = {"rows": count, "sha256": digest}
    return result


def _capture_objects(conn, params, *, store, limits, check):
    budget = ArchiveVerificationBatch(store, limits=limits, check_budget=check)
    if conn.scalar(text(f"""
        SELECT EXISTS (
            SELECT FROM {_REQUIRED_RAW} AS hold(id)
            LEFT JOIN market.raw_archive_manifests m USING (id)
            WHERE m.id IS NULL OR EXISTS (
                SELECT FROM market.storage_lifecycle_events e WHERE e.action='archive_expire'
                  AND e.event_type='completed' AND e.target_kind='raw_manifest' AND e.target_id=m.id
            )
        )
    """), params):
        raise RuntimeError("canonical_cold_rollback_stream_required_raw_unavailable")
    rows = conn.execute(text(f"""
        WITH scope AS (
            SELECT 'raw_manifest' AS kind,id,object_key,object_sha256,byte_count
            FROM market.raw_archive_manifests WHERE definition_id=:definition
              OR id IN {_REQUIRED_RAW}
            UNION ALL
            SELECT 'book_checkpoint',id,object_key,object_sha256,byte_count
            FROM market.book_checkpoint_manifests WHERE series_id=:series
        )
        SELECT s.* FROM scope s WHERE NOT EXISTS (
            SELECT FROM market.storage_lifecycle_events e WHERE e.action='archive_expire'
              AND e.event_type='completed' AND e.target_kind=s.kind AND e.target_id=s.id
        )
        ORDER BY s.kind,s.id LIMIT :limit
    """), params | {"limit": limits.max_objects + 1}).mappings().all()
    if len(rows) > limits.max_objects:
        raise RuntimeError("canonical_cold_rollback_stream_archive_budget_exceeded")
    digest = archive_evidence_hash({"archive": "selected_stream"})
    for row in rows:
        budget.verify(row["object_key"], row["object_sha256"], expected_bytes=row["byte_count"])
        digest = archive_evidence_hash({"previous": digest, "descriptor": dict(row)})
    budget.assert_unchanged()
    return {"objects": len(budget.objects), "bytes": budget.byte_count, "sha256": digest}


def capture_rollback_stream(conn, *, fence, definition_id, store, max_rows, object_limits, timeout_seconds=30):
    """Hash exact selected stream SQL and read its live archive dependencies.

    The live database fence proves this isolated snapshot has no other
    publishers. The caller also excludes archive writers and verifies any
    retained spool, provider offsets and external services separately.
    """
    _require_disposable_fence(conn, fence)
    if (not isinstance(definition_id, str) or not 1 <= len(definition_id) <= 64
            or type(max_rows) is not int or not 1 <= max_rows <= 10_000_000
            or not isinstance(object_limits, ArchiveVerificationLimits)
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120):
        raise ValueError("canonical_cold_rollback_stream_scope_invalid")
    deadline = min(monotonic() + timeout_seconds, fence.deadline)

    def check():
        _check_deadline(deadline)

    with _bounded_step(conn, timeout_seconds) as clamp:
        clamp(deadline - monotonic())
        fence.assert_quiet(conn)
        conn.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        scope = conn.execute(text("""
            SELECT source_id AS source,series_id AS series,provider_product_id AS product
            FROM market.stream_definitions WHERE id=:definition
        """), {"definition": definition_id}).mappings().one_or_none()
        if scope is None:
            raise RuntimeError("canonical_cold_rollback_stream_definition_missing")
        params = dict(scope) | {"definition": definition_id}
        relations = _capture_rows(conn, params, max_rows=max_rows, deadline=deadline)
        archive = _capture_objects(conn, params, store=store, limits=object_limits, check=check)
        identity = _database_identity(conn)
        fence.assert_quiet(conn)
    evidence = {"schema": VERSION, "target_database": identity, "definition_id": definition_id,
                "relations": relations, "archive": archive}
    return evidence | {"receipt_sha256": archive_evidence_hash(evidence)}


def verify_rollback_stream(conn, *, fence, evidence, expected_hash, store, max_rows, object_limits,
                           timeout_seconds=30):
    if evidence.get("receipt_sha256") != expected_hash or expected_hash != archive_evidence_hash(
        {key: value for key, value in evidence.items() if key != "receipt_sha256"},
    ):
        raise ValueError("canonical_cold_rollback_stream_receipt_changed")
    observed = capture_rollback_stream(
        conn, fence=fence, definition_id=evidence["definition_id"], store=store, max_rows=max_rows,
        object_limits=object_limits, timeout_seconds=timeout_seconds,
    )
    if observed != evidence:
        differing = sorted(name for name, scope in observed["relations"].items()
                           if scope != evidence["relations"].get(name))
        raise RuntimeError(f"canonical_cold_rollback_stream_state_changed: relations={differing}")
    return observed

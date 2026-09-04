"""Lossless response preservation through named witnesses and complete windows.

The archive preserves canonical outputs and their causal evidence; it does not
recertify a historical calculation or guess v1's omitted processing-chunk scope.
Physical reclamation separately requires verified complete cold placement and
current source, raw/checkpoint, and Dataset dependency checks.
"""
import json

from sqlalchemy import text

from market_data.canonical_adapters import decode_market_trade_record, decode_response_feature_record, decode_trade_flow_feature_record
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import FactArchiveLimits

from .fact_book_admission import resolve_book_position_revisions
from .fact_dependencies import resolve_causal_window_revisions
from .fact_derived_admission import resolve_material_source_revisions
from .fact_flow_feature_admission import resolve_flow_feature_source_revisions
from .fact_storage import PostgresCanonicalFactStorageRepository


def resolve_response_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                      max_file_bytes=128 * 1024**2, check_budget=None):
    roots = {row["id"]: row for row in rows if row["fact_type"] == "market.market_response"}
    if len(roots) > max_rows:
        raise RuntimeError("canonical_response_root_budget_exceeded")
    facts = {identity: decode_response_feature_record(record_from_storage_row(row)).fact for identity, row in roots.items()}
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    requests = [{"root_id": identity, "role": "flow", "series_id": fact.source_flow_feature_series_id,
        "fact_type": "market.trade_flow_feature", "material_hash": fact.source_flow_material_hash,
        "commit_seq": roots[identity]["market_commit_seq"], "known_at": fact.known_at} for identity, fact in facts.items()]
    flows, flow_selections = resolve_material_source_revisions(session, requests=requests, reader=reader,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    scopes = {}
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        batch = [{"root_id": item["root_id"], "series_id": roots[item["root_id"]]["series_id"],
            "flow_series_id": item["series_id"], "book_series_id": facts[item["root_id"]].source_l2_series_id}
            for item in requests[offset:offset + 128]]
        found = session.execute(text("""
            SELECT requested.root_id,root.instrument_id FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,series_id bigint,flow_series_id bigint,book_series_id bigint)
            JOIN market.series AS root ON root.id=requested.series_id AND root.fact_type='market.market_response'
              AND root.timeframe_seconds=1
            JOIN market.series AS flow ON flow.id=requested.flow_series_id AND flow.fact_type='market.trade_flow_feature'
              AND flow.instrument_id=root.instrument_id AND flow.timeframe_seconds=1
            JOIN market.series AS book ON book.id=requested.book_series_id AND book.fact_type='market.l2_book'
              AND book.instrument_id=root.instrument_id
        """), {"requests": json.dumps(batch)}).all()
        if len(found) != len(batch) or {identity for identity, _ in found} != {item["root_id"] for item in batch}:
            raise RuntimeError("canonical_response_series_scope_mismatch")
        scopes.update(found)
    positions = []
    for identity, fact in facts.items():
        for source_id in flow_selections[(identity, "flow")]:
            flow = decode_trade_flow_feature_record(record_from_storage_row(flows[source_id])).fact
            if (flow.bucket_start != fact.bucket_start or flow.bucket_end != fact.bucket_end or flow.interval_seconds != 1
                    or flow.material_hash != fact.source_flow_material_hash):
                raise RuntimeError(f"canonical_response_flow_scope_mismatch: fact_version_id={identity} source_id={source_id}")
        pre = fact.pre_book_source_position.material()
        for role in ("pre", "trough", "post"):
            position = getattr(fact, f"{role}_book_source_position").material()
            if any(position[name] != pre[name] for name in ("definition_id", "session_id", "connection_epoch", "provider_product_id")):
                raise RuntimeError(f"canonical_response_book_scope_mismatch: fact_version_id={identity} role={role}")
            positions.append({"root_id": f"{identity}:{role}", "series_id": fact.source_l2_series_id,
                "position": position, "state_hash": getattr(fact, f"{role}_state_hash"),
                "validity_interval_id": fact.validity_interval_id,
                "commit_seq": roots[identity]["market_commit_seq"], "known_at": fact.known_at})
    books, book_selections = resolve_book_position_revisions(session, requests=positions, object_store=object_store,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, max_file_bytes=max_file_bytes, check_budget=check_budget)
    windows = []
    for identity, fact in facts.items():
        # Keep the complete observation-time envelope of every causal revision
        # at the declared endpoints. Raw prefix proof independently keeps the
        # preceding connection history needed to reconstruct those states.
        times = [books[source_id]["observation_time"] for role in ("pre", "trough", "post")
                 for source_id in book_selections[f"{identity}:{role}"]]
        common = {"instrument_id": scopes[identity], "source_id": None,
                  "root_commit": roots[identity]["market_commit_seq"], "known_at": fact.known_at}
        windows.extend((
            {**common, "root_id": f"{identity}:trades", "series_id": None, "fact_type": "market.trade",
             "range_start": fact.bucket_start, "range_end": fact.bucket_end, "include_end": False},
            {**common, "root_id": f"{identity}:books", "series_id": fact.source_l2_series_id, "fact_type": "market.l2_book",
             "range_start": min(times), "range_end": max(times), "include_end": True},
        ))
    window_rows, window_selections = resolve_causal_window_revisions(session, requests=windows, reader=reader,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    for row in window_rows.values():
        record_from_storage_row(row)
    for identity, fact in facts.items():
        trades = [decode_market_trade_record(record_from_storage_row(window_rows[source_id])).fact
                  for source_id in window_selections.get(f"{identity}:trades", ())]
        for role in ("first", "last"):
            position = getattr(fact, f"{role}_trade_source_position")
            fields = {"connection_epoch", "provider_sequence_num", "receive_ordinal", "event_ordinal", "trade_ordinal", "raw_record_id"}
            if set(position) != fields or not any(
                trade.provider_trade_id == getattr(fact, f"{role}_trade_id") and trade.aggressor_side == fact.direction
                and all(getattr(trade, name) == value for name, value in position.items()) for trade in trades
            ):
                raise RuntimeError(f"canonical_response_trade_endpoint_missing: fact_version_id={identity} role={role}")
    descendants = resolve_flow_feature_source_revisions(session, rows=list(flows.values()), object_store=object_store,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, max_file_bytes=max_file_bytes, check_budget=check_budget)
    sources = {}
    for row in (*flows.values(), *books.values(), *window_rows.values(), *descendants):
        if row["id"] in sources and sources[row["id"]] != row:
            raise RuntimeError(f"canonical_response_source_conflict: source_id={row['id']}")
        sources[row["id"]] = row
    if len(sources) > max_rows:
        raise RuntimeError("canonical_response_source_budget_exceeded: reduce archive page size")
    return [sources[identity] for identity in sorted(sources)]


def collect_response_history_archive_refs(session, *, rows, object_store):
    """Bind every frozen revision's causal raw windows without writing progress."""
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from .fact_book_prefix import resolve_book_prefixes_for_read
    from .fact_flow_admission import collect_trade_history_archive_refs
    sources = resolve_response_source_revisions(session, rows=rows, object_store=object_store,
        max_rows=50_000, max_logical_bytes=64 * 1024**2)
    references = collect_trade_history_archive_refs(session,
        rows=[row for row in sources if row["fact_type"] in {"market.trade", "market.trade_flow"}], object_store=object_store)
    objects = ArchiveVerificationBatch(object_store,
        limits=ArchiveVerificationLimits(max_objects=10_000, max_bytes=4 * 1024**3))
    books = resolve_book_prefixes_for_read(session, rows=[row for row in sources if row["fact_type"] == "market.l2_book"],
        object_store=object_store, byte_verifier=objects, limits=RawArchiveReadLimits(),
        max_mapping_rows=50_000, max_objects=objects.limits.max_objects)
    for identity, reference in books.items():
        if references.setdefault(identity, reference) != reference:
            raise RuntimeError(f"canonical_response_history_dependency_conflict: target_id={identity}")
    if len(references) > objects.limits.max_objects:
        raise RuntimeError("canonical_response_history_object_budget_exceeded: reduce Dataset window")
    return references

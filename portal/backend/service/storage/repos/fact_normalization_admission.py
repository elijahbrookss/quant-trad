"""Bounded, lossless normalization-window and immutable-spec admission.

Three material witnesses are diagnostics, not the complete input set. Preserve
every causal revision in the recorded window, recursively retaining normalized
sources. This does not recompute or recertify a historical normalization value.
"""
import json

from sqlalchemy import text

from market_data.canonical_adapters import decode_normalized_feature_record
from market_data.canonical_storage import legacy_material_alias, record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.fact_registry import NORMALIZED_FACT_PREFIX, NORMALIZED_FACT_VERSION

from .fact_dependencies import resolve_causal_window_revisions
from .fact_derived_admission import resolve_material_source_revisions
from .fact_storage import PostgresCanonicalFactStorageRepository
from .normalization import _spec_from_row


def resolve_normalized_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                         max_file_bytes=128 * 1024**2, check_budget=None):
    from .fact_book_admission import resolve_book_source_revisions
    from .fact_derived_admission import resolve_derived_source_revisions
    frontier = {row["id"]: row for row in rows if row["fact_type"].startswith(NORMALIZED_FACT_PREFIX)}
    if len(frontier) > max_rows:
        raise RuntimeError("canonical_normalization_root_budget_exceeded")
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    sources, visited, specs = {}, set(), {}
    edge_count = 0
    retained_bytes = 0

    def remember(candidates):
        nonlocal retained_bytes
        for row in candidates:
            if check_budget is not None:
                check_budget()
            identity = row["id"]
            if identity in sources:
                if sources[identity] != row:
                    raise RuntimeError(f"canonical_normalization_source_conflict: source_id={identity}")
                continue
            # Nested depth is variable. Per-query hydration limits alone cannot
            # bound the payloads retained across the entire recursive closure.
            retained_bytes += len(json.dumps(row, default=str, separators=(",", ":"),
                                            ensure_ascii=True, allow_nan=False).encode("utf-8"))
            if retained_bytes > max_logical_bytes or len(sources) >= max_rows:
                raise RuntimeError("canonical_normalization_source_budget_exceeded: reduce archive page size")
            sources[identity] = row

    while frontier:
        if check_budget is not None:
            check_budget()
        facts = {identity: decode_normalized_feature_record(record_from_storage_row(row)).fact
                 for identity, row in frontier.items()}
        missing_specs = sorted({fact.spec_id for fact in facts.values()} - set(specs))
        for offset in range(0, len(missing_specs), 128):
            if check_budget is not None:
                check_budget()
            found = session.execute(text("SELECT * FROM market.normalization_specs WHERE id=ANY(:ids)"),
                {"ids": missing_specs[offset:offset + 128]}).mappings().all()
            for row in found:
                specs[row["id"]] = _spec_from_row(row)
        requests = []
        for identity, fact in facts.items():
            root = frontier[identity]
            spec = specs.get(fact.spec_id)
            if (spec is None or spec.spec_hash != fact.spec_hash or spec.output_fact_type != root["fact_type"]
                    or root["payload_schema_id"] != f"{NORMALIZED_FACT_VERSION}/{fact.spec_id}"
                    or fact.input_watermark >= root["market_commit_seq"] or len(fact.source_series_ids) != 1):
                raise RuntimeError(f"canonical_normalization_spec_or_clock_mismatch: fact_version_id={identity}")
            requests.append({"root_id": identity, "series_id": root["series_id"],
                "source_series_id": fact.source_series_ids[0], "source_type": spec.input_fact_type,
                "contract_version": root["payload_schema_id"], "fact_type": root["fact_type"],
                "required_timeframe": spec.parameters.get("required_input_timeframe_seconds")})
        scopes = {}
        for offset in range(0, len(requests), 128):
            if check_budget is not None:
                check_budget()
            batch = requests[offset:offset + 128]
            found = session.execute(text("""
                SELECT requested.root_id,root.instrument_id FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                    root_id text,series_id bigint,source_series_id bigint,source_type text,
                    contract_version text,fact_type text,required_timeframe integer)
                JOIN market.series AS root ON root.id=requested.series_id AND root.fact_type=requested.fact_type
                  AND root.contract_version=requested.contract_version
                JOIN market.series AS source ON source.id=requested.source_series_id AND source.fact_type=requested.source_type
                  AND source.instrument_id=root.instrument_id
                  AND (requested.required_timeframe IS NULL OR source.timeframe_seconds=requested.required_timeframe)
            """), {"requests": json.dumps(batch)}).all()
            if len(found) != len(batch) or {identity for identity, _ in found} != {item["root_id"] for item in batch}:
                raise RuntimeError("canonical_normalization_series_scope_mismatch")
            scopes.update(found)
        windows, witnesses = [], []
        for identity, fact in facts.items():
            source_type = specs[fact.spec_id].input_fact_type
            # Typed response inputs use bucket_start, while their canonical
            # observation time is the later post-book instant. Source known-at
            # bounds that instant without guessing an undocumented delay cap.
            end = fact.known_at if source_type == "market.market_response" else fact.input_end
            windows.append({"root_id": identity, "instrument_id": scopes[identity], "fact_type": source_type,
                "series_id": fact.source_series_ids[0], "source_id": None, "root_commit": fact.input_watermark,
                "known_at": fact.known_at, "range_start": fact.input_start, "range_end": end, "include_end": True})
            witnesses.extend({"root_id": identity, "role": str(index), "series_id": fact.source_series_ids[0],
                "fact_type": source_type, "material_hash": digest, "commit_seq": fact.input_watermark,
                "known_at": fact.known_at} for index, digest in enumerate(fact.source_material_hashes))
        matched, witness_selections = resolve_material_source_revisions(session, requests=witnesses, reader=reader,
            max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
        selected, window_selections = resolve_causal_window_revisions(session, requests=windows, reader=reader,
            max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
        edge_count += sum(len(ids) for ids in window_selections.values())
        if edge_count > max_rows:
            raise RuntimeError("canonical_normalization_edge_budget_exceeded: reduce archive page size")
        for row in selected.values():
            if check_budget is not None:
                check_budget()
            record_from_storage_row(row)
        for identity, fact in facts.items():
            ids = set(window_selections.get(identity, ()))
            material_ids = set()
            for source_id in ids:
                row = selected[source_id]
                alias = legacy_material_alias(row)
                material_ids.add((row["series_id"], alias["material_hash"] if alias else row["material_hash"]))
            if len(material_ids) < fact.input_count:
                raise RuntimeError(f"canonical_normalization_window_incomplete: fact_version_id={identity}")
            for index in range(len(fact.source_material_hashes)):
                for source_id in witness_selections[(identity, str(index))]:
                    if source_id not in ids or matched[source_id] != selected[source_id]:
                        raise RuntimeError(f"canonical_normalization_witness_outside_window: fact_version_id={identity} source_id={source_id}")
        remember(selected.values())
        visited.update(frontier)
        if len(visited) > max_rows:
            raise RuntimeError("canonical_normalization_source_budget_exceeded: reduce archive page size")
        # Every edge decreases the canonical commit watermark, so a valid
        # nested graph cannot cycle. The global row/edge budgets bound its work.
        frontier = {identity: row for identity, row in selected.items()
                    if identity not in visited and row["fact_type"].startswith(NORMALIZED_FACT_PREFIX)}
    non_normalized = [row for row in sources.values() if not row["fact_type"].startswith(NORMALIZED_FACT_PREFIX)]
    descendants = resolve_derived_source_revisions(session, rows=non_normalized, object_store=object_store,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, max_file_bytes=max_file_bytes, check_budget=check_budget)
    books = resolve_book_source_revisions(session, rows=[*non_normalized, *descendants], object_store=object_store,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, max_file_bytes=max_file_bytes, check_budget=check_budget)
    remember((*descendants, *books))
    return [sources[identity] for identity in sorted(sources)]


def collect_normalized_history_archive_refs(session, *, rows, object_store):
    from .fact_dependencies import collect_source_history_archive_refs
    sources = resolve_normalized_source_revisions(session, rows=rows, object_store=object_store,
        max_rows=50_000, max_logical_bytes=64 * 1024**2)
    return collect_source_history_archive_refs(session, rows=sources, object_store=object_store)

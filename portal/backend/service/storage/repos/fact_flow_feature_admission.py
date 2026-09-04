"""Exact aggregate and bounded causal trade closure for historical flow features."""
from collections import defaultdict
import json

from sqlalchemy import text

from market_data.canonical_adapters import (
    canonicalize_trade_flow_feature, decode_market_trade_record, decode_trade_flow_feature_record,
)
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.market_state import derive_trade_flow_feature

from .fact_derived_admission import resolve_material_source_revisions
from .fact_flow_admission import load_trade_flow_source_closure
from .fact_storage import PostgresCanonicalFactStorageRepository


def _matches_feature_inputs(fact, aggregate, rows, *, check_budget=None):
    """Check historical producer selections, never search arbitrary subsets.

    Trade material hashes exclude delivery identity. A bounded producer can
    therefore use an update deduplicated against an older canonical snapshot.
    Retain that real snapshot and the aggregate's raw prefix, not a synthesized
    canonical delivery. Continuous collection uses latest active covered rows.
    """
    groups = defaultdict(list)
    for row in rows:
        if check_budget is not None:
            check_budget()
        trade = decode_market_trade_record(record_from_storage_row(row)).fact
        groups[row["series_id"]].append((row, trade))
    for candidates in groups.values():
        latest = {}
        for row, trade in candidates:
            if row["observation_key"] not in latest or row["revision"] > latest[row["observation_key"]][0]["revision"]:
                latest[row["observation_key"]] = (row, trade)
        active = [trade for row, trade in latest.values() if row["state"] == "active"]
        selections = (
            [trade for trade in active if trade.coverage_interval_id == aggregate.coverage_interval_id],
            active,
            [trade for row, trade in candidates if row["state"] == "active"],
        )
        for trades in selections:
            if check_budget is not None:
                check_budget()
            try:
                expected = derive_trade_flow_feature(series_id=fact.series_id,
                    source_trade_flow_series_id=fact.source_trade_flow_series_id,
                    aggregate=aggregate, trades=trades, computed_at=fact.known_at)
            except ValueError as exc:
                # Nonmatching historical producer candidates are expected.
                # Any other owner failure remains an actionable exception.
                if str(exc) not in {"market_flow_feature_invalid: conflicting trade identity",
                                    "market_flow_feature_invalid: trade count does not reconcile",
                                    "market_flow_feature_invalid: quote notional does not reconcile"}:
                    raise
                continue
            if (expected is not None and expected.material_hash == fact.material_hash
                    and expected.known_at <= fact.known_at
                    and canonicalize_trade_flow_feature(expected).payload == canonicalize_trade_flow_feature(fact).payload):
                return True
    return False


def resolve_flow_feature_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                           max_file_bytes=128 * 1024**2, check_budget=None):
    """Keep every causal material-matched aggregate and its complete input window.

    v1 records an aggregate hash and combined trade fingerprint, not individual
    trade revision IDs. Exact hash selection plus conservative window retention
    preserves this evidence without claiming an invented original revision set.
    Reconcile the feature through its owner before granting deletion admission.
    """
    roots = {row["id"]: row for row in rows if row["fact_type"] == "market.trade_flow_feature"}
    if len(roots) > max_rows:
        raise RuntimeError("canonical_flow_feature_root_budget_exceeded")
    facts = {identity: decode_trade_flow_feature_record(record_from_storage_row(row)).fact
             for identity, row in roots.items()}
    requests = [{"root_id": identity, "role": "aggregate", "series_id": fact.source_trade_flow_series_id,
        "fact_type": "market.trade_flow", "material_hash": fact.aggregate_material_hash,
        "commit_seq": roots[identity]["market_commit_seq"], "known_at": fact.known_at} for identity, fact in facts.items()]
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    aggregates, selected = resolve_material_source_revisions(session, requests=requests, reader=reader,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        batch = [{"root_id": item["root_id"], "series_id": roots[item["root_id"]]["series_id"],
                  "source_series_id": item["series_id"], "interval_seconds": facts[item["root_id"]].interval_seconds}
                 for item in requests[offset:offset + 128]]
        found = session.execute(text("""
            SELECT requested.root_id FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,series_id bigint,source_series_id bigint,interval_seconds integer)
            JOIN market.series AS root ON root.id=requested.series_id AND root.fact_type='market.trade_flow_feature'
              AND root.timeframe_seconds=requested.interval_seconds
            JOIN market.series AS source ON source.id=requested.source_series_id AND source.fact_type='market.trade_flow'
              AND source.instrument_id=root.instrument_id AND source.timeframe_seconds=requested.interval_seconds
        """), {"requests": json.dumps(batch)}).scalars().all()
        if len(found) != len(batch) or set(found) != {item["root_id"] for item in batch}:
            raise RuntimeError("canonical_flow_feature_series_scope_mismatch")
    flow_roots, trades, trade_selections = load_trade_flow_source_closure(session, rows=list(aggregates.values()),
        object_store=object_store, max_rows=max_rows, max_logical_bytes=max_logical_bytes,
        max_file_bytes=max_file_bytes, check_budget=check_budget)
    by_id = {root.row["id"]: root for root in flow_roots}
    if set(by_id) != set(aggregates):
        raise RuntimeError("canonical_flow_feature_aggregate_missing")
    sources = {**aggregates, **trades}
    if len(sources) > max_rows:
        raise RuntimeError("canonical_flow_feature_source_budget_exceeded: reduce archive page size")
    attempted_inputs = 0
    for identity, fact in facts.items():
        matched = False
        for source_id in selected[(identity, "aggregate")]:
            if check_budget is not None:
                check_budget()
            aggregate = by_id[source_id].fact
            if (aggregate.bucket_start != fact.bucket_start or aggregate.bucket_end != fact.bucket_end
                    or aggregate.interval_seconds != fact.interval_seconds
                    or aggregate.input_fingerprint != fact.aggregate_input_fingerprint
                    or aggregate.material_hash != fact.aggregate_material_hash):
                raise RuntimeError(f"canonical_flow_feature_aggregate_mismatch: fact_version_id={identity} source_id={source_id}")
            candidate_ids = trade_selections.get(source_id, ())
            attempted_inputs += len(candidate_ids)
            if attempted_inputs > max_rows:
                raise RuntimeError("canonical_flow_feature_derivation_budget_exceeded: reduce archive page size")
            candidates = [trades[trade_id] for trade_id in candidate_ids]
            if _matches_feature_inputs(fact, aggregate, candidates, check_budget=check_budget):
                matched = True
        if not matched:
            raise RuntimeError(f"canonical_flow_feature_derivation_unproven: fact_version_id={identity}")
    return [sources[identity] for identity in sorted(sources)]


def collect_flow_feature_history_archive_refs(session, *, rows, object_store):
    from .fact_flow_admission import collect_trade_history_archive_refs
    sources = resolve_flow_feature_source_revisions(session, rows=rows, object_store=object_store,
        max_rows=50_000, max_logical_bytes=64 * 1024**2)
    return collect_trade_history_archive_refs(session, rows=sources, object_store=object_store)

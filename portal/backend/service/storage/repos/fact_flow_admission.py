"""Bounded historical trade-input and coverage admission for flow buckets."""
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any

from sqlalchemy import text

from market_data.canonical_adapters import decode_market_trade_record, decode_trade_flow_record
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.structure import TradeCoverageIntervalVersion, TradeFlowAggregateFact, aggregate_trade_bucket

from .fact_dependencies import read_canonical_dependency_rows
from .fact_storage import PostgresCanonicalFactStorageRepository


@dataclass(frozen=True)
class TradeFlowRoot:
    row: Mapping[str, Any]
    fact: TradeFlowAggregateFact
    instrument_id: str
    coverage: TradeCoverageIntervalVersion | None


def load_trade_flow_roots(session, *, rows, max_rows, max_logical_bytes, check_budget=None):
    """Read exact immutable coverage versions, sized before transferring JSON.

    Coverage has no source-series field. Product definitions establish its
    historical instrument/source scope without consulting mutable stream config.
    Their unit values are not substituted for a trade's own versioned contract.
    """
    from .market_structure import _coverage_version
    roots = {row["id"]: dict(row) for row in rows if row["fact_type"] == "market.trade_flow"}
    if len(roots) > max_rows:
        raise RuntimeError("canonical_flow_root_budget_exceeded")
    facts = {identity: decode_trade_flow_record(record_from_storage_row(row)).fact for identity, row in roots.items()}
    requests = [{"root_id": identity, "series_id": row["series_id"], "source_id": row["source_id"],
        "venue": row["source_venue"], "interval_id": facts[identity].coverage_interval_id,
        "revision": facts[identity].coverage_revision} for identity, row in roots.items()]
    locators, coverage_bytes = {}, {}
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        batch = requests[offset:offset + 128]
        found = session.execute(text("""
            SELECT requested.root_id,series.instrument_id,coverage.id AS coverage_version_id,
                   coverage.known_at AS coverage_known_at,
                   CASE WHEN coverage.id IS NULL THEN 0 ELSE 6::bigint * octet_length(to_jsonb(coverage)::text) END AS logical_bytes,
                   EXISTS (SELECT 1 FROM market.product_definition_versions AS product
                     WHERE product.source_id=requested.source_id AND product.instrument_id=series.instrument_id
                       AND product.venue=requested.venue AND product.provider_product_id=coverage.provider_product_id
                       AND product.known_at<=coverage.known_at AND product.effective_at<=coverage.opening_effective_at) AS product_scope_valid
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,series_id bigint,source_id bigint,venue text,interval_id text,revision integer)
            JOIN market.series AS series ON series.id=requested.series_id AND series.fact_type='market.trade_flow'
            LEFT JOIN market.stream_coverage_interval_versions AS coverage
              ON coverage.interval_id=requested.interval_id AND coverage.revision=requested.revision
            ORDER BY requested.root_id LIMIT :limit
        """), {"requests": json.dumps(batch), "limit": len(batch) + 1}).mappings().all()
        if len(found) != len(batch) or {item["root_id"] for item in found} != {item["root_id"] for item in batch}:
            raise RuntimeError("canonical_flow_root_scope_missing_or_ambiguous")
        for item in found:
            identity = item["root_id"]
            if facts[identity].coverage_interval_id is not None:
                if (item["coverage_version_id"] is None or not item["product_scope_valid"]
                        or item["coverage_known_at"] > roots[identity]["known_at"]):
                    raise RuntimeError(f"canonical_flow_coverage_scope_unproven: fact_version_id={identity}")
                coverage_bytes[item["coverage_version_id"]] = item["logical_bytes"]
            locators[identity] = dict(item)
        if sum(coverage_bytes.values()) > max_logical_bytes:
            raise RuntimeError("canonical_flow_coverage_byte_budget_exceeded")
    coverages = {}
    identities = sorted(coverage_bytes)
    for offset in range(0, len(identities), 1000):
        if check_budget is not None:
            check_budget()
        batch = identities[offset:offset + 1000]
        found = session.execute(text("SELECT * FROM market.stream_coverage_interval_versions WHERE id=ANY(:ids) ORDER BY id"),
                                {"ids": batch}).mappings().all()
        if {item["id"] for item in found} != set(batch):
            raise RuntimeError("canonical_flow_coverage_missing")
        coverages.update({item["id"]: _coverage_version(item) for item in found})
    result = []
    for identity, row in roots.items():
        fact = facts[identity]
        locator = locators[identity]
        coverage = coverages.get(locator["coverage_version_id"])
        if coverage is not None and (coverage.interval_id != fact.coverage_interval_id or coverage.revision != fact.coverage_revision
                or coverage.channel != "market_trades" or coverage.known_at > fact.known_at):
            raise RuntimeError(f"canonical_flow_coverage_mismatch: fact_version_id={identity}")
        result.append(TradeFlowRoot(row, fact, locator["instrument_id"], coverage))
    return result


def trade_flow_prefix_requirements(roots):
    """Keep the declared full coverage prefix and exact named endpoints."""
    prefixes, witnesses = {}, []
    for root in roots:
        coverage = root.coverage
        if coverage is None:
            continue
        closing = (coverage.closing_raw_record_id, coverage.closing_receive_ordinal, coverage.closing_effective_at)
        if any(value is not None for value in closing) and not all(value is not None for value in closing):
            raise RuntimeError(f"canonical_flow_coverage_closing_identity_invalid: fact_version_id={root.row['id']}")
        scope = {"definition_id": coverage.definition_id, "session_id": coverage.session_id,
            "connection_epoch": coverage.connection_epoch, "provider_product_id": coverage.provider_product_id,
            "provider": root.row["source_provider"], "venue": root.row["source_venue"]}
        prefix = {**scope, "first_receive_ordinal": 1, "root_fact_version_id": root.row["id"],
            "receive_ordinal": max(coverage.last_receive_ordinal, coverage.closing_receive_ordinal or 0,
                coverage.archive_complete_through_ordinal, coverage.canonicalization_watermark_ordinal)}
        key = (coverage.definition_id, coverage.session_id, coverage.connection_epoch)
        previous = prefixes.get(key)
        if previous is not None and any(previous[name] != scope[name] for name in ("provider_product_id", "provider", "venue")):
            raise RuntimeError("canonical_flow_coverage_prefix_scope_conflict")
        if previous is None or previous["receive_ordinal"] < prefix["receive_ordinal"]:
            prefixes[key] = prefix
        endpoints = [("opening", coverage.opening_raw_record_id, coverage.opening_receive_ordinal),
                     ("last", coverage.last_raw_record_id, coverage.last_receive_ordinal)]
        if coverage.closing_raw_record_id is not None:
            endpoints.append(("closing", coverage.closing_raw_record_id, coverage.closing_receive_ordinal))
        witnesses.extend({**scope, "root_fact_version_id": f"{root.row['id']}:{role}", "raw_record_id": raw_id,
            "receive_ordinal": ordinal, "first_receive_ordinal": ordinal, "requested_channel": "market_trades"}
            for role, raw_id, ordinal in endpoints)
    return list(prefixes.values()), witnesses


def select_trade_flow_inputs(root, rows, *, check_budget=None):
    """Preserve the complete causal input window, not guessed delivery revisions.

    A covered v1 bucket names no canonical input IDs or source trade series.
    In particular, bounded captures aggregate raw update deliveries even when
    ingest_trades deduplicates them against an older snapshot/session. Keep all
    causal canonical candidates plus the separately verified full raw coverage
    prefix. This conservative closure preserves both producer paths, including
    invalidations and repeated deliveries, without claiming a reconstructed
    delivery was a persisted revision or certifying historical market quality.

    Without coverage there is no independent raw-session witness. Such roots
    additionally require reconciliation through the original aggregation owner.
    """
    context = f"fact_version_id={root.row['id']}"
    by_series = defaultdict(list)
    for row in rows:
        if check_budget is not None:
            check_budget()
        canonical = record_from_storage_row(row)
        trade = decode_market_trade_record(canonical).fact
        if (row["source_id"] != root.row["source_id"] or row["market_commit_seq"] > root.row["market_commit_seq"]
                or row["known_at"] > root.fact.known_at or not root.fact.bucket_start <= row["observation_time"] < root.fact.bucket_end):
            raise RuntimeError(f"canonical_flow_source_scope_mismatch: {context} source_id={row['id']}")
        if root.coverage is not None and trade.provider_product_id != root.coverage.provider_product_id:
            raise RuntimeError(f"canonical_flow_source_product_mismatch: {context} source_id={row['id']}")
        by_series[row["series_id"]].append((row, trade))
    if root.coverage is not None:
        return sorted(rows, key=lambda row: row["id"])
    def eligible(item):
        row, trade = item
        return row["state"] == "active" and trade.coverage_interval_id is None
    matched, rejected = False, []
    groups = list(by_series.values()) or [[]]
    for candidates in groups:
        latest = {}
        for item in candidates:
            row = item[0]
            key = row["observation_key"]
            if key not in latest or row["revision"] > latest[key][0]["revision"]:
                latest[key] = item
        for mode, selection in (("continuous_latest", list(latest.values())), ("bounded_deliveries", candidates)):
            if check_budget is not None:
                check_budget()
            selected = [item for item in selection if eligible(item)]
            try:
                derived = aggregate_trade_bucket((item[1] for item in selected), interval_seconds=root.fact.interval_seconds,
                    bucket_start=root.fact.bucket_start, coverage=root.coverage, computed_at=root.fact.known_at)
            except ValueError as exc:
                # These two expected candidate-set rejections are evidence of
                # a nonmatching historical selection, not a swallowed failure.
                if str(exc) not in {"trade_flow_invalid: conflicting provider trade identity in aggregate",
                                    "trade_flow_incomplete_zero_forbidden: zero rows require proven complete coverage"}:
                    raise
                rejected.append(f"{mode}:{exc}")
                continue
            if derived != root.fact:
                rejected.append(f"{mode}:derived bucket differs")
                continue
            matched = True
    if not matched:
        # Keep diagnostics bounded even when several immutable series are
        # candidates. Never convert a failed reconciliation into empty lineage.
        raise RuntimeError(f"canonical_flow_source_derivation_unproven: {context} reasons={rejected[:4]}")
    return sorted(rows, key=lambda row: row["id"])


def load_trade_flow_source_closure(session, *, rows, object_store, max_rows, max_logical_bytes,
                                         max_file_bytes=128 * 1024**2, check_budget=None):
    roots = load_trade_flow_roots(session, rows=rows, max_rows=max_rows, max_logical_bytes=max_logical_bytes,
                                 check_budget=check_budget)
    requests = [{"root_id": root.row["id"], "instrument_id": root.instrument_id, "source_id": root.row["source_id"],
        "root_commit": root.row["market_commit_seq"], "known_at": root.fact.known_at.isoformat(),
        "range_start": root.fact.bucket_start.isoformat(), "range_end": root.fact.bucket_end.isoformat()} for root in roots]
    selections = defaultdict(list)
    count = 0
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        found = session.execute(text("""
            SELECT requested.root_id,source.id
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,instrument_id text,source_id bigint,root_commit bigint,known_at timestamptz,range_start timestamptz,range_end timestamptz)
            JOIN market.series AS series ON series.instrument_id=requested.instrument_id AND series.fact_type='market.trade'
            JOIN market.fact_versions AS source ON source.series_id=series.id AND source.fact_type='market.trade'
              AND source.source_id=requested.source_id AND source.market_commit_seq<=requested.root_commit
              AND source.known_at<=requested.known_at AND source.observation_time>=requested.range_start AND source.observation_time<requested.range_end
            ORDER BY requested.root_id,source.series_id,source.observation_key,source.revision LIMIT :limit
        """), {"requests": json.dumps(requests[offset:offset + 128]), "limit": max_rows - count + 1}).all()
        count += len(found)
        if count > max_rows:
            raise RuntimeError("canonical_flow_source_budget_exceeded: reduce archive page size")
        for root_id, source_id in found:
            selections[root_id].append(source_id)
    ids = sorted({identity for group in selections.values() for identity in group})
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes, max_file_bytes=max_file_bytes))
    sources = read_canonical_dependency_rows(session, ids, reader=reader,
        max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    if set(sources) != set(ids):
        raise RuntimeError("canonical_flow_source_missing")
    retained = {}
    for root in roots:
        if check_budget is not None:
            check_budget()
        for row in select_trade_flow_inputs(root, [sources[identity] for identity in selections[root.row["id"]]], check_budget=check_budget):
            retained[row["id"]] = row
    # Sources keep their own exact raw witnesses. An older canonical delivery
    # may legitimately belong to a different session than the derived bucket.
    return roots, retained, dict(selections)


def resolve_trade_flow_source_revisions(session, **kwargs):
    _, retained, _ = load_trade_flow_source_closure(session, **kwargs)
    return [retained[identity] for identity in sorted(retained)]


def collect_trade_history_archive_refs(session, *, rows, object_store, max_rows=50_000,
                                       max_logical_bytes=64 * 1024**2):
    """Freeze every trade/flow root's raw evidence without latest-row lookup.

    Historical completeness flags describe their original publication, not
    whether those exact archived bytes are readable today. Preserve the flags
    while requiring complete current physical lineage for every revision.
    This read-only proof writes neither prefix progress nor canonical facts.
    """
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from .fact_book_prefix import resolve_trade_prefixes_for_read
    if any(row["fact_type"] not in {"market.trade", "market.trade_flow"} for row in rows):
        raise ValueError("canonical_trade_history_family_invalid")
    if len(rows) > max_rows:
        raise RuntimeError("canonical_trade_history_root_budget_exceeded: reduce Dataset window")
    roots = load_trade_flow_roots(session, rows=rows, max_rows=max_rows, max_logical_bytes=max_logical_bytes)
    sources = resolve_trade_flow_source_revisions(session, rows=rows, object_store=object_store,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes)
    trades = {row["id"]: row for row in (*rows, *sources) if row["fact_type"] == "market.trade"}
    prefixes, witnesses = trade_flow_prefix_requirements(roots)
    objects = ArchiveVerificationBatch(object_store,
        limits=ArchiveVerificationLimits(max_objects=10_000, max_bytes=4 * 1024**3))
    return resolve_trade_prefixes_for_read(session, prefixes=prefixes, witnesses=witnesses,
        rows=list(trades.values()), object_store=object_store, byte_verifier=objects, limits=RawArchiveReadLimits(),
        max_mapping_rows=max_rows, max_objects=objects.limits.max_objects)

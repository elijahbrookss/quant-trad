"""Exact OI/funding revision admission for the current derivative-state owner."""
from collections import defaultdict
from datetime import timedelta
import json

from sqlalchemy import text

from market_data.canonical_adapters import canonicalize_derivative_state_feature, decode_derivative_state_feature_record
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.market_state import DERIVATIVE_OI_INTERVAL_SECONDS, derivative_state_input_fingerprint, derive_derivative_state_features

from .fact_dependencies import read_canonical_dependency_rows
from .fact_storage import PostgresCanonicalFactStorageRepository


def resolve_derivative_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                        max_file_bytes=128 * 1024**2, check_budget=None):
    """Bind explicit current commits and recover the fingerprint-bound predecessor.

    The production v1 materializer uses legacy OI/funding facts and a 60-second
    OI interval. Do not convert exact-numeric v2 evidence through a float codec.
    An absent predecessor is legitimate (window/gap evidence); never substitute
    today's latest predecessor for the historically declared input.
    """
    from .market_data import _canonical_to_open_interest_record, _canonical_to_funding_rate_record
    roots = {row["id"]: row for row in rows if row["fact_type"] == "market.derivative_state"}
    facts = {identity: decode_derivative_state_feature_record(record_from_storage_row(row)).fact
             for identity, row in roots.items()}
    requests = []
    for identity, fact in facts.items():
        row = roots[identity]
        if check_budget is not None:
            check_budget()
        base = {"root_id": identity, "root_series_id": row["series_id"], "instrument_id": fact.instrument_id,
                "root_commit": row["market_commit_seq"], "known_at": row["known_at"].isoformat()}
        for role, family in (("oi", "derivatives.open_interest"), ("funding", "derivatives.funding_rate")):
            series_id = getattr(fact, f"{role}_series_id")
            commit = getattr(fact, f"{role}_market_commit_seq")
            sample = getattr(fact, f"{role}_sample_time")
            if series_id is None:
                if commit is not None or sample is not None:
                    raise RuntimeError(f"canonical_derivative_source_identity_invalid: fact_version_id={identity} role={role}")
                continue
            if (type(series_id) is not int or series_id <= 0 or type(commit) is not int or commit <= 0
                    or sample is None or sample >= fact.effective_at):
                raise RuntimeError(f"canonical_derivative_source_identity_invalid: fact_version_id={identity} role={role}")
            requests.append({**base, "role": role, "series_id": series_id, "fact_type": family,
                             "commit_seq": commit, "sample_time": sample.isoformat()})
        if fact.oi_series_id is None and fact.funding_series_id is None:
            raise RuntimeError(f"canonical_derivative_sources_missing: fact_version_id={identity}")
        if fact.oi_previous_value is not None:
            if fact.oi_series_id is None or fact.oi_sample_time is None:
                raise RuntimeError(f"canonical_derivative_previous_identity_invalid: fact_version_id={identity}")
            requests.append({**base, "role": "previous_oi", "series_id": fact.oi_series_id,
                "fact_type": "derivatives.open_interest", "commit_seq": None,
                "sample_time": (fact.oi_sample_time - timedelta(seconds=DERIVATIVE_OI_INTERVAL_SECONDS)).isoformat()})
    if len(requests) > max_rows:
        raise RuntimeError("canonical_derivative_source_budget_exceeded: reduce archive page size")
    selections = defaultdict(list)
    found_count = 0
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        found = session.execute(text("""
            SELECT requested.root_id,requested.role,source.id
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,root_series_id bigint,instrument_id text,root_commit bigint,known_at timestamptz,
                role text,series_id bigint,fact_type text,commit_seq bigint,sample_time timestamptz)
            JOIN market.series AS root_series ON root_series.id=requested.root_series_id
              AND root_series.fact_type='market.derivative_state' AND root_series.instrument_id=requested.instrument_id
            JOIN market.series AS source_series ON source_series.id=requested.series_id
              AND source_series.instrument_id=requested.instrument_id AND source_series.fact_type=requested.fact_type
            JOIN market.fact_versions AS source ON source.series_id=requested.series_id AND source.fact_type=requested.fact_type
              AND source.observation_time=requested.sample_time AND source.state='active'
              AND source.market_commit_seq<=requested.root_commit AND source.known_at<=requested.known_at
              AND (requested.role='previous_oi' OR source.market_commit_seq=requested.commit_seq)
            ORDER BY requested.root_id,requested.role,source.market_commit_seq,source.id LIMIT :limit
        """), {"requests": json.dumps(requests[offset:offset + 128]), "limit": max_rows - found_count + 1}).all()
        found_count += len(found)
        if found_count > max_rows:
            raise RuntimeError("canonical_derivative_source_budget_exceeded: reduce archive page size")
        for identity, role, source_id in found:
            selections[(identity, role)].append(source_id)
    identities = sorted({source_id for ids in selections.values() for source_id in ids})
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    sources = read_canonical_dependency_rows(session, identities, reader=reader,
        max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    if set(sources) != set(identities):
        raise RuntimeError("canonical_derivative_source_missing: selected payloads unavailable")
    decoders = {"derivatives.open_interest.v1": _canonical_to_open_interest_record,
                "derivatives.funding_rate.v1": _canonical_to_funding_rate_record}
    decoded = {}
    for identity, row in sources.items():
        if check_budget is not None:
            check_budget()
        record_from_storage_row(row)
        decoder = decoders.get(row["payload_schema_id"])
        if decoder is None:
            raise RuntimeError(f"canonical_derivative_source_schema_unsupported: fact_version_id={identity} schema={row['payload_schema_id']}")
        decoded[identity] = decoder(row)
    for request in requests:
        selected = selections[(request["root_id"], request["role"])]
        if not selected or (request["role"] != "previous_oi" and len(selected) != 1):
            raise RuntimeError(f"canonical_derivative_source_missing_or_ambiguous: fact_version_id={request['root_id']} role={request['role']}")
        for identity in selected:
            row = sources[identity]
            if (row["series_id"] != request["series_id"] or row["fact_type"] != request["fact_type"]
                    or row["state"] != "active" or row["observation_time"].isoformat() != request["sample_time"]
                    or row["known_at"] > roots[request["root_id"]]["known_at"]
                    or row["market_commit_seq"] > request["root_commit"]
                    or (request["commit_seq"] is not None and row["market_commit_seq"] != request["commit_seq"])):
                raise RuntimeError(f"canonical_derivative_source_mismatch: fact_version_id={request['root_id']} source_id={identity}")
    retained = set()
    for identity, fact in facts.items():
        if check_budget is not None:
            check_budget()
        current_ids = {role: selections[(identity, role)][0] if selections[(identity, role)] else None
                       for role in ("oi", "funding")}
        oi = decoded[current_ids["oi"]] if current_ids["oi"] is not None else None
        funding = decoded[current_ids["funding"]] if current_ids["funding"] is not None else None
        candidates = selections[(identity, "previous_oi")] if fact.oi_previous_value is not None else [None]
        matched = []
        for source_id in candidates:
            if check_budget is not None:
                check_budget()
            previous = decoded[source_id] if source_id is not None else None
            if derivative_state_input_fingerprint(instrument_id=fact.instrument_id, effective_at=fact.effective_at,
                oi_record=oi, previous_oi_record=previous, funding_record=funding) == fact.input_fingerprint:
                matched.append(source_id)
        if len(matched) != 1:
            raise RuntimeError(f"canonical_derivative_input_fingerprint_mismatch: fact_version_id={identity}")
        previous_id = matched[0]
        oi_inputs = ([decoded[previous_id]] if previous_id is not None else []) + ([oi] if oi is not None else [])
        expected = [item for item in derive_derivative_state_features(instrument_id=fact.instrument_id,
            oi_records=oi_inputs, funding_records=[funding] if funding is not None else [], oi_gaps=(),
            series_id=fact.series_id, expected_oi_interval_seconds=DERIVATIVE_OI_INTERVAL_SECONDS, computed_at=fact.known_at)
            if item.effective_at == fact.effective_at]
        if (len(expected) != 1 or expected[0].material_hash != fact.material_hash
                or canonicalize_derivative_state_feature(expected[0]).payload != roots[identity]["payload"]):
            raise RuntimeError(f"canonical_derivative_source_derivation_mismatch: fact_version_id={identity}")
        retained.update(source_id for source_id in (*current_ids.values(), previous_id) if source_id is not None)
    return [sources[identity] for identity in sorted(retained)]

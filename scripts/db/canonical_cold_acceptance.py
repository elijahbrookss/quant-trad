"""Compare isolated source/target readbacks and plan selective reacquisition.

Provider planning never fetches data. This compares caller-supplied canonical
outputs at a shared read boundary, separately from byte and row migration.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from itertools import zip_longest
from math import isfinite
from typing import Iterable

from market_data.fact_archive import archive_evidence_hash


class Disposition(StrEnum):
    COPY_EXACT = "copy_exact"
    REACQUIRE_GAP = "reacquire_gap"
    REBUILD_PROJECTION = "rebuild_projection"


PROJECTIONS = {"cold_lookup_index", "typed_candle_projection", "verified_read_cache"}
COINBASE_INTERVALS = {60, 300, 900, 1800, 3600, 7200, 14400, 21600, 86400}
COINBASE_HISTORICAL_OVERLAP_PAGE = 299  # See CoinbaseProvider.fetch_from_api.


def acquisition_disposition(*, kind, provider=None, source_kind=None, timeframe_seconds=None,
                            gap_proven=False, accepted_evidence=True, pinned=True,
                            irreproducible_lineage=True, verified_rebuild_inputs=False):
    """Missing or uncertain evidence leaves exact preservation as the default."""
    if accepted_evidence or pinned or irreproducible_lineage:
        return Disposition.COPY_EXACT
    if (kind == "candle.ohlcv" and provider == "COINBASE" and source_kind == "historical_api"
            and gap_proven and timeframe_seconds in COINBASE_INTERVALS):
        return Disposition.REACQUIRE_GAP
    if kind in PROJECTIONS and verified_rebuild_inputs:
        return Disposition.REBUILD_PROJECTION
    return Disposition.COPY_EXACT


def coinbase_gap_request_budget(*, windows, timeframe_seconds, measured_requests_per_second=None):
    """Return counts per distinct requested window. Elapsed time needs a pilot rate."""
    if type(timeframe_seconds) is not int or timeframe_seconds not in COINBASE_INTERVALS:
        raise ValueError("canonical_cold_reacquisition_unsupported_granularity")
    requests = periods = 0
    for symbol, start, end in windows:
        if (not symbol or not isinstance(start, datetime) or not isinstance(end, datetime)
                or start.utcoffset() is None or end.utcoffset() is None):
            raise ValueError("canonical_cold_reacquisition_invalid_window")
        lower = start.astimezone(UTC).timestamp()
        upper = end.astimezone(UTC).timestamp()
        if upper <= lower or lower % timeframe_seconds or upper % timeframe_seconds:
            raise ValueError("canonical_cold_reacquisition_unaligned_window")
        units = int(upper - lower) // timeframe_seconds
        periods += units
        requests += (units + COINBASE_HISTORICAL_OVERLAP_PAGE - 1) // COINBASE_HISTORICAL_OVERLAP_PAGE
    if measured_requests_per_second is not None and (
            not isfinite(measured_requests_per_second) or measured_requests_per_second <= 0):
        raise ValueError("canonical_cold_reacquisition_invalid_rate")
    return {"expected_periods": periods, "minimum_requests": requests,
            "request_seconds_at_measured_rate": None if measured_requests_per_second is None else
            requests / measured_requests_per_second}


@dataclass(frozen=True)
class ParityCase:
    domain: str
    case_id: str
    source_boundary: str
    target_boundary: str
    source: Iterable[dict]
    target: Iterable[dict]
    allows_empty: bool = False


PARITY_DOMAINS = frozenset({"all_revisions", "as_of", "frozen_dataset", "coverage",
                            "raw_ordinals", "l2_replay", "raw_holds"})
_MISSING = object()


def verify_parity(cases, *, required_domains=PARITY_DOMAINS, max_records=1_000_000):
    """Require ordered, exact readback parity and explicit coverage of named domains.

    The readback harness compares full canonical evidence from separate adapters.
    A matching empty read is admissible only for a declared empty case. Each
    required domain needs at least one observed nonempty witness.
    """
    if type(max_records) is not int or max_records < 1 or not required_domains <= PARITY_DOMAINS:
        raise ValueError("canonical_cold_parity_invalid_limits")
    seen = set()
    verified_domains = set()
    receipts = []
    for case in cases:
        identity = (case.domain, case.case_id)
        if (case.domain not in PARITY_DOMAINS or not case.case_id or identity in seen
                or not case.source_boundary or case.source_boundary != case.target_boundary):
            raise ValueError(f"canonical_cold_parity_invalid_binding: case={identity}")
        seen.add(identity)
        count = 0
        digest = archive_evidence_hash({"case": identity, "boundary": case.source_boundary})
        for source, target in zip_longest(case.source, case.target, fillvalue=_MISSING):
            if count >= max_records:
                raise RuntimeError(f"canonical_cold_parity_budget_exceeded: case={identity}")
            if source is _MISSING or target is _MISSING:
                raise RuntimeError(f"canonical_cold_parity_coverage_mismatch: case={identity} ordinal={count}")
            source_digest = archive_evidence_hash(source)
            if source_digest != archive_evidence_hash(target):
                raise RuntimeError(f"canonical_cold_parity_content_mismatch: case={identity} ordinal={count}")
            digest = archive_evidence_hash({"previous": digest, "row": source_digest})
            count += 1
        if count:
            verified_domains.add(case.domain)
        elif not case.allows_empty:
            raise RuntimeError(f"canonical_cold_parity_unexpected_empty: case={identity}")
        receipts.append({"domain": case.domain, "case_id": case.case_id,
                         "boundary": case.source_boundary, "rows": count, "ordered_digest": digest})
    missing = sorted(required_domains - verified_domains)
    if missing:
        raise RuntimeError(f"canonical_cold_parity_missing_domain_witnesses: domains={missing}")
    return receipts

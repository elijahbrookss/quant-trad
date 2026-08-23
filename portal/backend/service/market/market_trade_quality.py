"""Typed quarantine evidence for provider trades outside canonical semantics."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from data_providers.streams.contracts import CanonicalMarketEvent
from market_data.structure import RawStreamRecord


PROVIDER_TRADE_SIDE_UNKNOWN = "provider_trade_side_unknown"


@dataclass(frozen=True)
class QuarantinedTradeSide:
    """One raw provider trade withheld from the canonical BUY/SELL tape."""

    event: CanonicalMarketEvent
    provider_side: str
    invalidates_coverage: bool


def _stable_hash(value: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_trade_side_quarantine_quality(
    *,
    record: RawStreamRecord,
    observations: Sequence[QuarantinedTradeSide],
    detected_at: datetime,
) -> dict[str, Any]:
    """Fold one raw frame's rejected trades into one durable quality event."""

    if not observations:
        raise ValueError("trade_side_quarantine_invalid: observations are required")
    material = sorted(
        (
            {
                "provider_product_id": str(item.event.product_id or ""),
                "provider_trade_id": str(
                    dict(item.event.payload or {}).get("trade_id") or ""
                ),
                "provider_side": str(item.provider_side or ""),
                "delivery_kind": str(
                    dict(item.event.payload or {}).get("type") or ""
                ).lower(),
                "event_ordinal": int(
                    dict(item.event.payload or {}).get("event_ordinal") or 0
                ),
                "trade_ordinal": int(
                    dict(item.event.payload or {}).get("trade_ordinal") or 0
                ),
                "provider_event_time": (
                    item.event.provider_event_time.isoformat()
                    if isinstance(item.event.provider_event_time, datetime)
                    else str(item.event.provider_event_time or "")
                ),
            }
            for item in observations
        ),
        key=lambda row: (
            row["event_ordinal"],
            row["trade_ordinal"],
            row["provider_trade_id"],
        ),
    )
    identity_hash = _stable_hash(material)
    invalidating = any(item.invalidates_coverage for item in observations)
    provider_sequences = sorted(
        {
            int(item.event.provider_sequence_num)
            for item in observations
            if item.event.provider_sequence_num is not None
        }
    )
    evidence = {
        "schema_version": "market.trade_projection_quarantine.v1",
        "reason_code": "unsupported_provider_maker_side",
        "provider_side_values": sorted(
            {str(item.provider_side or "") for item in observations}
        ),
        "delivery_kinds": sorted({row["delivery_kind"] for row in material}),
        "quarantined_trade_count": len(material),
        "quarantined_trade_identity_hash": identity_hash,
        "first_provider_trade_id": material[0]["provider_trade_id"],
        "last_provider_trade_id": material[-1]["provider_trade_id"],
        "event_ordinals": sorted({row["event_ordinal"] for row in material}),
        "trade_ordinals": sorted({row["trade_ordinal"] for row in material}),
        "coverage_invalidating": invalidating,
        "raw_evidence_retained": True,
        "canonical_action": "quarantined",
    }
    dedupe_hash = _stable_hash(
        {
            "schema_version": "market.trade_projection_quarantine_dedupe.v1",
            "raw_record_id": record.raw_record_id,
            "quarantined_trade_identity_hash": identity_hash,
        }
    )
    return {
        "dedupe_hash": dedupe_hash,
        "connection_epoch": record.connection_epoch,
        "receive_ordinal": record.receive_ordinal,
        "channel": "market_trades",
        "classification": PROVIDER_TRADE_SIDE_UNKNOWN,
        "reason": (
            "provider trade maker side is outside the proven BUY/SELL contract; "
            "exact raw evidence retained and affected canonical trades quarantined"
        ),
        "detected_at": detected_at,
        "raw_record_id": record.raw_record_id,
        "sequence_before": None,
        "sequence_after": provider_sequences[-1] if provider_sequences else None,
        "invalidating": invalidating,
        "evidence": evidence,
    }


__all__ = [
    "PROVIDER_TRADE_SIDE_UNKNOWN",
    "QuarantinedTradeSide",
    "build_trade_side_quarantine_quality",
]

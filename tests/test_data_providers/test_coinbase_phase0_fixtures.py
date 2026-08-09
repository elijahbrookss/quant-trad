from __future__ import annotations

import gzip
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from data_providers.streams import CoinbaseMessageParser


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "providers"
    / "coinbase"
    / "market_structure_phase0"
)


def _load_fixture_bundle() -> tuple[dict, dict[str, bytes], dict[str, dict]]:
    manifest = json.loads((FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))
    payload_path = FIXTURE_DIR / manifest["payload"]["path"]
    compressed = payload_path.read_bytes()
    assert hashlib.sha256(compressed).hexdigest() == manifest["payload"]["sha256"]
    payload_bytes = gzip.decompress(compressed)
    assert (
        hashlib.sha256(payload_bytes).hexdigest()
        == manifest["payload"]["uncompressed_sha256"]
    )
    payload = json.loads(payload_bytes)
    frames = {
        row["fixture_id"]: row["raw_frame"].encode("utf-8")
        for row in payload["frames"]
    }
    entries = {row["fixture_id"]: row for row in manifest["entries"]}
    assert set(frames) == set(entries)
    assert len(frames) == manifest["payload"]["frame_count"]
    for fixture_id, raw_frame in frames.items():
        entry = entries[fixture_id]
        assert len(raw_frame) == entry["raw_frame_bytes"]
        assert hashlib.sha256(raw_frame).hexdigest() == entry["raw_frame_sha256"]
    return manifest, frames, entries


def _parse_exact_frame(
    fixture_id: str,
    frames: dict[str, bytes],
    entries: dict[str, dict],
):
    entry = entries[fixture_id]
    return CoinbaseMessageParser().parse_raw(
        frames[fixture_id],
        received_at=entry["received_at"],
        raw_ref={
            "stream_session_id": entry["stream_session_id"],
            "connection_epoch": entry["connection_epoch"],
            "receive_ordinal": entry["receive_ordinal"],
            "raw_frame_sha256": entry["raw_frame_sha256"],
            "raw_frame_bytes": entry["raw_frame_bytes"],
        },
    )


def test_provider_proof_fixture_bundle_preserves_exact_public_frames_and_checksums() -> None:
    manifest, frames, entries = _load_fixture_bundle()

    assert manifest["source"]["auth_mode"] == "public"
    assert manifest["source"]["archive_complete"] is False
    assert manifest["source"]["dataset_eligible"] is False
    assert manifest["sanitization"] == {
        "mode": "none_required_public_inbound_market_data",
        "raw_frames_changed": False,
        "sensitive_key_scan": "passed",
    }
    assert len(frames["btc_spot_level2_snapshot"]) > 1024 * 1024

    for fixture_id in frames:
        events = _parse_exact_frame(fixture_id, frames, entries)
        assert events
        assert not {
            "provider_malformed_message",
            "provider_unsupported_message",
        } & {event.event_kind for event in events}
        assert all(
            event.raw_ref["raw_frame_sha256"] == entries[fixture_id]["raw_frame_sha256"]
            for event in events
        )


def test_provider_proof_trade_fixtures_preserve_maker_side_and_units() -> None:
    _, frames, entries = _load_fixture_bundle()

    for prefix in ("bip", "btc_spot"):
        for event_type in ("snapshot", "update"):
            fixture_id = f"{prefix}_market_trades_{event_type}"
            events = _parse_exact_frame(fixture_id, frames, entries)
            trades = [event for event in events if event.event_kind == "market_trade"]
            assert trades
            assert {trade.payload["type"] for trade in trades} == {event_type}
            assert {trade.payload["side"] for trade in trades} <= {"BUY", "SELL"}
            assert all(Decimal(str(trade.payload["price"])) > 0 for trade in trades)
            assert all(Decimal(str(trade.payload["size"])) > 0 for trade in trades)
            if prefix == "bip":
                assert all(
                    Decimal(str(trade.payload["size"])).to_integral_value()
                    == Decimal(str(trade.payload["size"]))
                    for trade in trades
                )


def test_provider_proof_level2_fixtures_preserve_snapshot_and_zero_delete() -> None:
    _, frames, entries = _load_fixture_bundle()

    for prefix in ("bip", "btc_spot"):
        snapshot = _parse_exact_frame(f"{prefix}_level2_snapshot", frames, entries)
        snapshot_events = [
            event for event in snapshot if event.event_kind == "market_l2_snapshot"
        ]
        assert len(snapshot_events) == 1
        assert snapshot_events[0].payload["updates"]
        assert {
            update["side"] for update in snapshot_events[0].payload["updates"]
        } <= {"bid", "offer"}

        deletion = _parse_exact_frame(f"{prefix}_level2_zero_delete", frames, entries)
        update_events = [
            event for event in deletion if event.event_kind == "market_l2_update"
        ]
        assert update_events
        assert any(
            Decimal(str(update["new_quantity"])) == 0
            for event in update_events
            for update in event.payload["updates"]
        )

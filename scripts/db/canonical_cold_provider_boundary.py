"""Capture and classify native QT stream boundary evidence in a disposable fixture."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

from market_data.archive import (
    RawArchiveReadLimits, _raw_record_from_spool_row, _read_spool_file, discover_spool_segments, iter_raw_archive_parquet,
)
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash


VERSION = "qt.canonical_cold_provider_boundary_fixture.v1"
GAPS = frozenset({"archive_loss", "backpressure_stop", "book_invalid", "canonicalization_lag", "collector_restart_gap",
                  "decode_error", "disconnect", "divergent_duplicate", "heartbeat_gap", "out_of_order",
                  "provider_trade_conflict", "provider_trade_side_unknown", "sequence_gap", "update_before_snapshot"})


def _json(value):
    def encode(item):
        if isinstance(item, datetime) and item.utcoffset() is not None:
            return item.astimezone(UTC).isoformat(timespec="microseconds")
        raise TypeError(f"unsupported provider boundary evidence: {type(item).__name__}")

    return json.loads(json.dumps(value, default=encode, allow_nan=False))


def capture(session, *, phase, definition_id, spool_root, object_store, max_rows=256):
    """Read endpoint tables and verified files while the scripted channel is paused."""
    from sqlalchemy import text

    from .canonical_cold_custody_audit import _raw_mappings
    from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository

    if (phase not in {"pre_stop", "drained", "resumed"} or type(max_rows) is not int or not 1 <= max_rows <= 4096
            or session.scalar(text("SHOW transaction_read_only")) != "on"
            or session.scalar(text("SHOW transaction_isolation")) != "repeatable read"):
        raise ValueError("provider fixture capture requires a bounded read-only snapshot")
    params = {"id": definition_id, "limit": max_rows + 1}

    def rows(query):
        found = session.execute(text(query + " LIMIT :limit"), params).mappings().all()
        if len(found) > max_rows:
            raise RuntimeError("provider fixture observation exceeded row budget")
        return [dict(row) for row in found]

    now = session.scalar(text("SELECT transaction_timestamp()"))
    watermark = PostgresMarketDataRepository._current_commit_seq_with_session(session)
    definition, = rows("""SELECT id,series_id,provider_product_id,channels
        FROM market.stream_definitions WHERE id=:id""")
    lease = rows("""SELECT owner_id,lease_generation,heartbeat_at,expires_at,
        expires_at>transaction_timestamp() AS live FROM market.stream_lease_state WHERE definition_id=:id""")
    events = rows("""SELECT id,session_id,connection_epoch,event_ordinal,event_type,occurred_at,reason,evidence,evidence_hash
        FROM market.stream_session_events WHERE definition_id=:id ORDER BY session_id,event_ordinal""")
    quality = rows("""SELECT id,session_id,connection_epoch,receive_ordinal,classification,reason,detected_at,known_at,
        raw_record_id,evidence_hash FROM market.stream_quality_events WHERE definition_id=:id ORDER BY known_at,id""")
    manifests = rows("""SELECT * FROM market.raw_archive_manifests WHERE definition_id=:id
        ORDER BY session_id,connection_epoch,first_receive_ordinal,id""")
    limits = RawArchiveReadLimits(max_rows=max_rows, max_file_bytes=32 * 1024**2)
    files = ArchiveVerificationBatch(object_store, limits=ArchiveVerificationLimits(max_objects=max_rows, max_bytes=32 * 1024**2))
    witness_rows, readiness = [], []
    for manifest in manifests:
        if not 0 < manifest["record_count"] <= max_rows:
            raise RuntimeError("provider fixture observation exceeded manifest row budget")
        files.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
        path = object_store.local_path(manifest["object_key"])
        manifest["ordinals"] = _raw_mappings(session, manifest=manifest, path=path,
                                             limits=limits, check=lambda: None)
        raw_rows = session.execute(text("""SELECT raw_record_id,receive_ordinal,raw_frame_sha256
            FROM market.raw_archive_record_mappings WHERE manifest_id=:manifest ORDER BY object_row_index LIMIT :limit"""),
            {"manifest": manifest["id"], "limit": max_rows + 1}).mappings().all()
        witness_rows.extend(dict(row) | {"session_id": manifest["session_id"], "epoch": manifest["connection_epoch"]}
                            for row in raw_rows)
        if len(witness_rows) > max_rows:
            raise RuntimeError("provider fixture observation exceeded raw row budget")
        for item in iter_raw_archive_parquet(path, limits=limits):
            if item.observed_channel not in {"subscriptions", "heartbeats"}:
                continue
            frame = json.loads(item.raw_frame)
            if item.observed_channel == "subscriptions":
                confirmed = any(definition["provider_product_id"] in event.get("subscriptions", {}).get(channel, ())
                                for event in frame["events"] for channel in definition["channels"] if channel != "heartbeats")
                if not confirmed:
                    continue
            readiness.append({"session_id": item.session_id, "epoch": item.connection_epoch,
                              "kind": item.observed_channel, "raw_record_id": item.raw_record_id,
                              "received_at": item.received_at, "sha256": item.raw_frame_sha256})
    if "level2" in definition["channels"]:
        coverage = rows("""SELECT v.id,v.interval_id,v.revision,v.status,v.opening_snapshot_id,v.opening_session_id AS session_id,
            v.opening_connection_epoch AS connection_epoch,v.opening_receive_ordinal,v.last_receive_ordinal,
            v.opening_effective_at,v.last_valid_effective_at AS last_effective_at,v.closing_effective_at,v.known_at,v.reason
            FROM market.book_validity_interval_versions v JOIN market.stream_definitions d ON v.series_id=d.series_id
            WHERE d.id=:id ORDER BY v.opening_effective_at,v.interval_id,v.revision""")
        checkpoints = rows("""SELECT c.id,c.session_id,c.connection_epoch,c.receive_ordinal,c.object_key,c.object_sha256,
            c.byte_count,c.state_hash,c.source_manifest_ids FROM market.book_checkpoint_manifests c
            JOIN market.stream_definitions d ON c.series_id=d.series_id WHERE d.id=:id
            ORDER BY c.session_id,c.connection_epoch,c.receive_ordinal,c.id""")
        for checkpoint in checkpoints:
            files.verify(checkpoint["object_key"], checkpoint["object_sha256"], expected_bytes=checkpoint["byte_count"])
            sources = [manifest for manifest in manifests if manifest["id"] in checkpoint["source_manifest_ids"]]
            if (not sources or {manifest["id"] for manifest in sources} != set(checkpoint["source_manifest_ids"])
                    or not any((manifest["session_id"], manifest["connection_epoch"]) ==
                               (checkpoint["session_id"], checkpoint["connection_epoch"])
                               and manifest["first_receive_ordinal"] <= checkpoint["receive_ordinal"] <= manifest["last_receive_ordinal"]
                               for manifest in sources)):
                raise RuntimeError("provider boundary checkpoint has no verified source manifest at its position")
        book = rows("""SELECT s.session_id,s.connection_epoch,s.receive_ordinal,s.lifecycle,s.state_hash,s.checkpoint_id
            FROM market.book_reconstruction_state s JOIN market.stream_definitions d ON s.series_id=d.series_id
            WHERE d.id=:id""")
    else:
        coverage = rows("""SELECT id,interval_id,revision,status,session_id,connection_epoch,opening_receive_ordinal,
            last_receive_ordinal,canonicalization_watermark_ordinal,archive_complete_through_ordinal,archive_status,
            opening_effective_at,last_effective_at,closing_effective_at,known_at
            FROM market.stream_coverage_interval_versions WHERE definition_id=:id
            ORDER BY opening_effective_at,interval_id,revision""")
        checkpoints, book = [], []
    files.assert_unchanged()
    spool = []
    for path in discover_spool_segments(spool_root, definition_id=definition_id):
        if path.stat().st_size > 1024**2 or len(spool) >= max_rows:
            raise RuntimeError("provider fixture observation exceeded spool budget")
        header, rows_in_segment, truncated = _read_spool_file(path, repair_tail=False)
        if truncated:
            raise RuntimeError("provider boundary read encountered an incomplete local spool tail")
        records = [{"raw_record_id": raw.raw_record_id, "receive_ordinal": raw.receive_ordinal,
                    "raw_frame_sha256": raw.raw_frame_sha256, "session_id": raw.session_id, "epoch": raw.connection_epoch}
                   for raw in map(_raw_record_from_spool_row, rows_in_segment)]
        if len(records) > max_rows:
            raise RuntimeError("provider fixture observation exceeded spool row budget")
        spool.append({"segment_id": header["spool_segment_id"], "kind": path.suffix, "bytes": path.stat().st_size,
                      "records": records})
    evidence = _json({"phase": phase, "captured_at": now, "watermark": watermark, "definition": definition, "lease": lease,
                      "events": events, "quality": quality, "manifests": manifests, "raw": witness_rows,
                      "coverage": coverage, "book": book, "checkpoints": checkpoints, "readiness": readiness, "spool": spool})
    return evidence | {"sha256": archive_evidence_hash(evidence)}


def _latest(rows, session_id):
    versions = {}
    for row in rows:
        if row["session_id"] == session_id and row["revision"] > versions.get(row["interval_id"], {}).get("revision", 0):
            versions[row["interval_id"]] = row
    if not versions:
        raise RuntimeError("provider boundary has no native coverage for the session")
    return max(versions.values(), key=lambda row: row["opening_effective_at"])


def validate(pre_stop, drained, resumed):
    snapshots = (pre_stop, drained, resumed)
    for phase, receipt in zip(("pre_stop", "drained", "resumed"), snapshots, strict=True):
        if (receipt["phase"] != phase or receipt["definition"] != pre_stop["definition"]
                or archive_evidence_hash({key: value for key, value in receipt.items() if key != "sha256"}) != receipt["sha256"]):
            raise RuntimeError("provider boundary snapshot scope or hash differs")
    if not pre_stop["captured_at"] < drained["captured_at"] < resumed["captured_at"]:
        raise RuntimeError("provider boundary clocks are out of order")
    if not pre_stop["watermark"] < drained["watermark"] < resumed["watermark"]:
        raise RuntimeError("provider boundary did not advance accepted canonical commits through drain and resume")
    if (len(pre_stop["lease"]) != 1 or not pre_stop["lease"][0]["live"] or drained["lease"] or resumed["lease"]
            or not pre_stop["spool"] or drained["spool"] or resumed["spool"]):
        raise RuntimeError("provider boundary did not witness the active tail and completed drain")
    tail = [record for segment in pre_stop["spool"] for record in segment["records"]]
    if not tail or len({row["session_id"] for row in tail}) != 1 or any(row not in drained["raw"] for row in tail):
        raise RuntimeError("provider boundary lost or changed a durable pre-stop raw receipt")
    old_session = tail[0]["session_id"]
    starts = [event for event in resumed["events"] if event["event_type"] == "connected"
              and event["occurred_at"] > drained["captured_at"]]
    if len(starts) != 1 or starts[0]["session_id"] == old_session:
        raise RuntimeError("provider boundary did not observe a new connected session")
    new_session = starts[0]["session_id"]
    if not any(event["event_type"] == "subscription_sent" and event["session_id"] == new_session for event in resumed["events"]):
        raise RuntimeError("provider boundary new subscription evidence missing")
    if {row["kind"] for row in resumed["readiness"] if row["session_id"] == new_session
            and row["epoch"] == starts[0]["connection_epoch"]} != {"subscriptions", "heartbeats"}:
        raise RuntimeError("provider boundary resumed archive lacks subscription acknowledgement and heartbeat evidence")
    for stage, identity in ((drained, old_session), (resumed, new_session)):
        terminal = [item for item in stage["events"] if item["session_id"] == identity
                    and item["event_type"] == "continuous_capture_stopped" and item["reason"] == "supervisor_stop_requested"]
        if len(terminal) != 1 or not any(row["session_id"] == identity for row in stage["raw"]):
            raise RuntimeError("provider boundary missing terminal session or acknowledged raw bytes")
    for field in ("events", "raw", "quality", "checkpoints", "manifests", "coverage", "readiness"):
        if any(row not in drained[field] for row in pre_stop[field]) or any(row not in resumed[field] for row in drained[field]):
            raise RuntimeError(f"provider boundary rewrote durable drained evidence: {field}")
    old, new = _latest(drained["coverage"], old_session), _latest(resumed["coverage"], new_session)
    if (old["interval_id"] == new["interval_id"] or old["closing_effective_at"] is None
            or new["opening_effective_at"] <= old["closing_effective_at"]):
        raise RuntimeError("provider boundary lacks independent pre/post coverage intervals")
    if "market_trades" in pre_stop["definition"]["channels"]:
        for item in (old, new):
            if (item["archive_status"] != "complete" or item["archive_complete_through_ordinal"] != item["last_receive_ordinal"]
                    or item["canonicalization_watermark_ordinal"] != item["last_receive_ordinal"]):
                raise RuntimeError("provider boundary trade archive/canonical watermarks did not drain")
    elif (not new["opening_snapshot_id"] or not any(item["session_id"] == new_session for item in resumed["checkpoints"])):
        raise RuntimeError("provider boundary new L2 baseline/checkpoint is missing")
    existing = [row for row in pre_stop["quality"] if row["classification"] in GAPS]
    during_drain = [row for row in drained["quality"] if row["classification"] in GAPS and row not in pre_stop["quality"]]
    later = [row for row in resumed["quality"] if row["classification"] in GAPS and row not in drained["quality"]]
    if any(row["session_id"] != new_session or row["known_at"] <= drained["captured_at"] for row in later):
        raise RuntimeError("provider boundary new gap classification is outside the resumed session")
    if new["status"] in {"invalid", "closed_invalidated"} and not later:
        raise RuntimeError("provider boundary invalid resumed interval lacks typed gap evidence")
    outcome = {"version": VERSION, "definition_id": pre_stop["definition"]["id"],
               "provider_product_id": pre_stop["definition"]["provider_product_id"],
               "pre_stop_session": old_session, "resumed_session": new_session, "pre_stop_tail_records": len(tail),
               "interruption": {"classification": "unproven_provider_interval", "after": old["closing_effective_at"],
                                "before": new["opening_effective_at"], "missing_events": None},
               "pre_existing_events": existing,
               "pre_existing_bip_prefix_evidence": [row for row in existing if
                    pre_stop["definition"]["provider_product_id"].startswith("BIP-")
                    and "canonical_raw_reference_prefix_budget_exceeded" in row["reason"]],
               "drain_events": during_drain, "post_start_events": later, "old_interval": old, "new_interval": new,
               "accepted_commit_watermarks": [receipt["watermark"] for receipt in snapshots],
               "snapshot_sha256": [receipt["sha256"] for receipt in snapshots]}
    return outcome | {"receipt_sha256": archive_evidence_hash(outcome)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for phase in ("pre-stop", "drained", "resumed"):
        parser.add_argument(f"--{phase}", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(validate(*(json.loads(path.read_text()) for path in (args.pre_stop, args.drained, args.resumed))),
                     sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

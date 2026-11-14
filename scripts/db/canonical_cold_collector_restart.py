"""Verify the first usable provider state after the approved collector restart."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
import os
from pathlib import Path

from sqlalchemy import text

from data_providers.streams.coinbase import CoinbaseMessageParser
from market_data.archive_verification import ArchiveVerificationBatch
from market_data.book_archive import read_book_checkpoint_parquet
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_collector_drain import (
    VERSION, CollectorDrainLimits, _CHANNEL_EVENTS, _EVENT_FIELDS, _manifest, _poll_identity, _roster, _sealed, _stream_identity,
)
from scripts.db.canonical_cold_custody_audit import _raw_mappings
from scripts.db.canonical_cold_recovery_pending_spool import pending_spool_record_witness


class CollectorRestartPending(RuntimeError):
    """A native provider receipt can still arrive before the outage deadline."""


def _new_session(conn, definition, previous, *, started_at, now):
    if not definition["owner_id"] or definition["expires_at"] <= now:
        raise CollectorRestartPending(f"canonical_cold_collector_restart_lease_pending: {definition['id']}")
    row = conn.execute(text("""
        SELECT id,session_id,connection_epoch,event_ordinal,occurred_at FROM market.stream_session_events
        WHERE definition_id=:id AND owner_id=:owner AND lease_generation=:generation
          AND occurred_at>=:floor AND event_type IN ('connected','reconnected')
        ORDER BY occurred_at DESC,event_ordinal DESC LIMIT 1
    """), {"id": definition["id"], "owner": definition["owner_id"], "generation": definition["lease_generation"],
           "floor": max(started_at, definition["claimed_at"])}).mappings().one_or_none()
    if row is None:
        raise CollectorRestartPending(f"canonical_cold_collector_restart_connection_pending: {definition['id']}")
    if row["session_id"] == previous["session_id"]:
        raise RuntimeError(f"canonical_cold_collector_restart_reused_session: {definition['id']}")
    return row


def _coverage(conn, definition, session, channel):
    scope = {"id": definition["id"], "series": definition["series_id"], "session": session["session_id"],
             "epoch": session["connection_epoch"], "known": session["occurred_at"], "channel": channel}
    if channel == "market_trades":
        row = conn.execute(text("""
            SELECT id,interval_id,revision,status,opening_receive_ordinal,opening_effective_at,last_receive_ordinal,
                   archive_status,archive_complete_through_ordinal,canonicalization_watermark_ordinal
            FROM market.stream_coverage_interval_versions
            WHERE definition_id=:id AND session_id=:session AND connection_epoch=:epoch AND channel=:channel
              AND known_at>=:known
            ORDER BY known_at DESC,revision DESC,id DESC LIMIT 1
        """), scope).mappings().one_or_none()
        if (row is None or row["status"] != "open_valid" or row["archive_status"] != "complete"
                or row["archive_complete_through_ordinal"] != row["last_receive_ordinal"]
                or row["canonicalization_watermark_ordinal"] != row["last_receive_ordinal"]):
            raise CollectorRestartPending(f"canonical_cold_collector_restart_trade_coverage_pending: {definition['id']}")
        return row
    row = conn.execute(text("""
        SELECT v.id,v.interval_id,v.revision,v.status,v.opening_receive_ordinal,v.opening_effective_at,
               v.opening_snapshot_id,s.checkpoint_id
        FROM market.book_reconstruction_state AS s
        JOIN market.book_validity_interval_versions AS v ON v.interval_id=s.validity_interval_id
        WHERE s.series_id=:series AND s.definition_id=:id AND s.session_id=:session
          AND s.connection_epoch=:epoch AND s.lifecycle='valid' AND v.known_at>=:known
          AND v.opening_session_id=:session AND v.opening_connection_epoch=:epoch
        ORDER BY v.revision DESC LIMIT 1
    """), scope).mappings().one_or_none()
    if row is None or row["status"] != "open_valid" or not row["opening_snapshot_id"] or not row["checkpoint_id"]:
        raise CollectorRestartPending(f"canonical_cold_collector_restart_book_baseline_pending: {definition['id']}")
    return row


def _verify_checkpoint(conn, *, identities, manifest_ids, definition, session, coverage, files, check):
    row = conn.execute(text("""
        SELECT c.* FROM market.book_checkpoint_manifests AS c
        WHERE c.id=ANY(:ids) AND c.series_id=:series AND c.session_id=:session
          AND c.connection_epoch=:epoch AND c.validity_interval_id=:interval
          AND NOT EXISTS (
            SELECT 1 FROM market.storage_lifecycle_events AS e
            WHERE e.action='archive_expire' AND e.event_type='completed'
              AND e.target_kind='book_checkpoint' AND e.target_id=c.id
        )
        ORDER BY c.receive_ordinal,c.event_ordinal,c.id LIMIT 1
    """), {"ids": sorted(identities), "series": definition["series_id"], "session": session["session_id"],
           "epoch": session["connection_epoch"], "interval": coverage["interval_id"]}).mappings().one_or_none()
    if row is None or not row["source_manifest_ids"] or not set(row["source_manifest_ids"]) <= manifest_ids:
        raise RuntimeError(f"canonical_cold_collector_restart_checkpoint_changed: {definition['id']}")
    obj = files.verify(row["object_key"], row["object_sha256"], expected_bytes=row["byte_count"])
    check()
    read_book_checkpoint_parquet(files.object_store.local_path(obj.object_key), expected=row, check_budget=check)
    return {"id": row["id"], "sha256": obj.object_sha256, "state_hash": row["state_hash"]}


def _startup_raw(conn, *, definition, session, channel, coverage, files, limits, budget, check):
    events = conn.execute(text(f"""
        SELECT {_EVENT_FIELDS} FROM market.stream_session_events
        WHERE session_id=:session AND definition_id=:id AND connection_epoch=:epoch
          AND event_ordinal>:first AND event_type=:kind
        ORDER BY event_ordinal LIMIT :limit
    """), {"session": session["session_id"], "id": definition["id"], "epoch": session["connection_epoch"],
           "first": session["event_ordinal"], "kind": _CHANNEL_EVENTS[channel],
           "limit": budget["events"] + 1}).mappings().all()
    parser = CoinbaseMessageParser(provider=definition["provider"], venue=definition["venue"])
    markers, manifests, checkpoint_ids = {}, [], set()
    last = 0

    def observe(record):
        if record.receive_ordinal == coverage["opening_receive_ordinal"]:
            markers["coverage_opened"] = record
        if record.observed_channel == "subscriptions":
            for event in parser.parse_raw(record.raw_frame, received_at=record.received_at.isoformat()):
                products = event.payload.get("subscriptions", {}).get(channel, ())
                if event.event_kind == "provider_subscription_ack" and definition["provider_product_id"] in products:
                    markers.setdefault("subscription", record)
        if record.observed_channel == "heartbeats" and "heartbeat" not in markers:
            if any(event.event_kind == "provider_heartbeat"
                   for event in parser.parse_raw(record.raw_frame, received_at=record.received_at.isoformat())):
                markers["heartbeat"] = record

    for event in events:
        check()
        budget["events"] -= 1
        if budget["events"] < 0:
            raise RuntimeError("canonical_cold_collector_restart_event_budget_exceeded")
        manifest = _manifest(conn, event, definition_id=definition["id"], event_kind=_CHANNEL_EVENTS[channel])
        budget["rows"] -= manifest["record_count"]
        if budget["rows"] < 0 or manifest["record_count"] > limits.raw.max_rows:
            raise RuntimeError("canonical_cold_collector_restart_raw_row_budget_exceeded")
        if manifest["first_receive_ordinal"] != last + 1:
            raise RuntimeError(f"canonical_cold_collector_restart_ordinal_gap: {manifest['id']}")
        obj = files.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
        exact = _raw_mappings(conn, manifest=manifest, path=files.object_store.local_path(obj.object_key),
                              limits=limits.raw, check=check, observe=observe)
        manifests.append({"id": manifest["id"], "sha256": obj.object_sha256, "mappings": exact})
        checkpoint_ids.update(event["evidence"].get("checkpoint_ids", ()))
        last = manifest["last_receive_ordinal"]
        if markers.keys() == {"subscription", "heartbeat", "coverage_opened"} and (channel != "level2" or checkpoint_ids):
            break
    if markers.keys() != {"subscription", "heartbeat", "coverage_opened"} or (channel == "level2" and not checkpoint_ids):
        raise CollectorRestartPending(f"canonical_cold_collector_restart_wire_readiness_pending: {definition['id']}")
    return ({name: {field: getattr(row, field) for field in
                    ("raw_record_id", "raw_frame_sha256", "receive_ordinal", "received_at")}
             for name, row in markers.items()}, manifests, checkpoint_ids)


def _poll_outages(conn, current, previous, *, started_at, budget, check):
    if _poll_identity(current) != _poll_identity(previous):
        raise RuntimeError(f"canonical_cold_collector_restart_poll_changed: {current['id']}")
    first = datetime.fromisoformat(previous["first_due_at_risk"])
    cadence = timedelta(seconds=previous["poll_interval_seconds"])
    count = max(0, (started_at - first + cadence - timedelta(microseconds=1)) // cadence)
    if count > budget["events"]:
        raise RuntimeError("canonical_cold_collector_restart_poll_budget_exceeded")
    if not count:
        return []
    arguments = {"id": current["id"], "series": current["series_id"], "source": current["source_id"],
                 "first": first, "end": started_at, "limit": budget["events"] + 1}
    attempts = conn.execute(text("""
        SELECT id,scheduled_for,status,ingestion_run_id FROM market.collection_attempts
        WHERE definition_id=:id AND scheduled_for>=:first AND scheduled_for<:end
        ORDER BY scheduled_for,attempt_number DESC LIMIT :limit
    """), arguments).mappings().all()
    budget["events"] -= len(attempts) + count
    if budget["events"] < 0:
        raise RuntimeError("canonical_cold_collector_restart_poll_budget_exceeded")
    gaps = conn.execute(text("""
        SELECT id,start_time,end_time,evidence_hash,expected_count,observed_count FROM market.gap_evidence
        WHERE series_id=:series AND source_id=:source AND classification='collection_schedule_missed'
          AND evidence->>'definition_id'=:id AND evidence->>'schema_version'='market_collection_gap.v1'
          AND start_time>=:first AND start_time<:end
        ORDER BY start_time,id LIMIT :limit
    """), {**arguments, "limit": budget["events"] + 1}).mappings().all()
    budget["events"] -= len(gaps)
    if budget["events"] < 0:
        raise RuntimeError("canonical_cold_collector_restart_poll_budget_exceeded")
    if any(row["observed_count"] != 0 or row["expected_count"] <= 0
           or row["end_time"] - row["start_time"] != row["expected_count"] * cadence
           or (row["start_time"] - first) % cadence != timedelta(0) for row in gaps):
        raise RuntimeError(f"canonical_cold_collector_restart_poll_gap_changed: {current['id']}")
    latest = {}
    for attempt in attempts:
        latest.setdefault(attempt["scheduled_for"], attempt)
    outcomes = []
    for index in range(count):
        check()
        due = first + index * cadence
        attempt = latest.get(due)
        if attempt and attempt["status"] in {"succeeded", "failed", "missed"}:
            outcomes.append({"due": due, "status": attempt["status"], "attempt_id": attempt["id"],
                             "ingestion_run_id": attempt["ingestion_run_id"]})
            continue
        outage = next((row for row in gaps if row["start_time"] <= due < row["end_time"]), None)
        if outage is None:
            raise CollectorRestartPending(f"canonical_cold_collector_restart_poll_reconciliation_pending: {current['id']}")
        outcomes.append({"due": due, "status": "missed", "gap_id": outage["id"], "gap_hash": outage["evidence_hash"]})
    return outcomes


def verify_recovered_pending_spool(conn, *, pending, started_at, spool_root, files, limits, budget, check):
    """Match the canonicalized recovery output to each independently preserved sealed segment."""
    scope = pending["scope"]
    segments = pending["segments"]
    root = Path(spool_root)
    for segment in segments:
        check()
        sealed = root / segment["path"]
        if os.path.lexists(sealed) or os.path.lexists(sealed.with_suffix(".open")):
            raise CollectorRestartPending(f"canonical_cold_collector_restart_pending_spool_not_released: {segment['segment_id']}")
    rows = conn.execute(text(f"""
        SELECT {_EVENT_FIELDS} FROM market.stream_session_events
        WHERE definition_id=:id AND session_id=:session AND connection_epoch=:epoch
          AND occurred_at>=:started AND event_type='book_segment_canonicalized'
          AND evidence->>'spool_segment_id'=ANY(:segments)
        ORDER BY event_ordinal DESC LIMIT :limit
    """), {"id": scope["definition_id"], "session": scope["session_id"], "epoch": scope["connection_epoch"],
           "started": started_at, "segments": [segment["segment_id"] for segment in segments],
           "limit": budget["events"] + 1}).mappings().all()
    budget["events"] -= len(rows)
    if budget["events"] < 0:
        raise RuntimeError("canonical_cold_collector_restart_event_budget_exceeded")
    events = {}
    for event in rows:
        events.setdefault(event["evidence"]["spool_segment_id"], event)
    verified = []
    for segment in segments:
        check()
        event = events.get(segment["segment_id"])
        if event is None:
            raise CollectorRestartPending(f"canonical_cold_collector_restart_pending_spool_receipt_missing: {segment['segment_id']}")
        manifest = _manifest(conn, event, definition_id=scope["definition_id"], event_kind="book_segment_canonicalized")
        if (manifest["first_receive_ordinal"], manifest["last_receive_ordinal"], manifest["record_count"]) != (
                segment["first_receive_ordinal"], segment["last_receive_ordinal"], segment["records"]):
            raise RuntimeError(f"canonical_cold_collector_restart_pending_spool_positions_changed: {segment['segment_id']}")
        budget["rows"] -= manifest["record_count"]
        if budget["rows"] < 0 or manifest["record_count"] > limits.raw.max_rows:
            raise RuntimeError("canonical_cold_collector_restart_raw_row_budget_exceeded")
        obj = files.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
        witness = sha256()

        def observe(record):
            if (record.provider, record.venue, record.provider_product_id, record.requested_channel) != (
                    scope["provider"], scope["venue"], scope["provider_product_id"], scope["requested_channel"]):
                raise RuntimeError(f"canonical_cold_collector_restart_pending_spool_product_changed: {segment['segment_id']}")
            witness.update(pending_spool_record_witness(
                receive_ordinal=record.receive_ordinal, received_at=record.received_at, raw_record_id=record.raw_record_id,
                raw_frame_sha256=record.raw_frame_sha256, observed_channel=record.observed_channel,
            ))

        mappings = _raw_mappings(conn, manifest=manifest, path=files.object_store.local_path(obj.object_key), limits=limits.raw,
                                 check=check, observe=observe)
        if witness.hexdigest() != segment["records_sha256"]:
            raise RuntimeError(f"canonical_cold_collector_restart_pending_spool_records_changed: {segment['segment_id']}")
        verified.append({"segment_id": segment["segment_id"], "records_sha256": segment["records_sha256"],
                         "event_id": event["id"], "manifest_id": manifest["id"], "manifest_sha256": obj.object_sha256,
                         "mappings": mappings})
    return {"source_sha256": pending["source_sha256"], "segments": verified}


def verify_collector_restart(conn, *, drained, started_at, store, limits, check, spool_root=None):
    """Bind current provider readiness and the precise paused poll window."""
    if (not isinstance(limits, CollectorDrainLimits) or drained.get("schema") != VERSION or drained.get("phase") != "drained"
            or archive_evidence_hash({key: item for key, item in drained.items() if key != "sha256"}) != drained.get("sha256")
            or conn.scalar(text("SHOW transaction_read_only")) != "on"
            or conn.scalar(text("SHOW transaction_isolation")) != "repeatable read"):
        raise ValueError("canonical_cold_collector_restart_invalid_boundary")
    now = conn.scalar(text("SELECT transaction_timestamp()"))
    if (not isinstance(started_at, datetime) or started_at.utcoffset() is None
            or not datetime.fromisoformat(drained["captured_at"]) < started_at <= now):
        raise RuntimeError("canonical_cold_collector_restart_host_clock_changed")
    watermark = PostgresMarketDataRepository._current_commit_seq_with_session(conn)
    if watermark < drained["source_commit_seq"]:
        raise RuntimeError("canonical_cold_collector_restart_watermark_regressed")
    streams, polls = _roster(conn, stream_ids=tuple(x["definition_id"] for x in drained["streams"]),
                             poll_ids=tuple(x["id"] for x in drained["polls"]), check=check)
    files = ArchiveVerificationBatch(store, limits=limits.archive, check_budget=check)
    budget = {"events": limits.max_events, "rows": limits.max_raw_rows}
    pending = drained.get("pending_spool")
    recovered_pending = None if pending is None else verify_recovered_pending_spool(
        conn, pending=pending, started_at=started_at, spool_root=spool_root, files=files, limits=limits, budget=budget, check=check,
    )
    boundaries = []
    for definition, previous in zip(streams, drained["streams"], strict=True):
        check()
        channel = previous["channel"]
        if _stream_identity(definition) != previous["definition"]:
            raise RuntimeError(f"canonical_cold_collector_restart_stream_changed: {definition['id']}")
        session = _new_session(conn, definition, previous, started_at=started_at, now=now)
        coverage = _coverage(conn, definition, session, channel)
        markers, manifests, checkpoint_ids = _startup_raw(
            conn, definition=definition, session=session, channel=channel, coverage=coverage, files=files,
            limits=limits, budget=budget, check=check,
        )
        checkpoint = None
        if channel == "level2":
            checkpoint = _verify_checkpoint(
                conn, identities=checkpoint_ids, manifest_ids={row["id"] for row in manifests}, definition=definition,
                session=session, coverage=coverage, files=files, check=check,
            )
        before = markers["coverage_opened"]["received_at"]
        after = datetime.fromisoformat(previous["interruption"]["after"])
        if not after < before <= now:
            raise RuntimeError(f"canonical_cold_collector_restart_provider_clock_changed: {definition['id']}")
        boundaries.append({"definition_id": definition["id"], "connection": dict(session), "markers": markers,
                           "manifests": manifests, "coverage": dict(coverage), "checkpoint": checkpoint,
                           "interruption": {**previous["interruption"], "before": before}})
    poll_outcomes = [
        {"id": current["id"], "occurrences": _poll_outages(conn, current, previous, started_at=started_at, budget=budget, check=check),
         "next_scheduled_at": current["next_scheduled_at"]}
        for current, previous in zip(polls, drained["polls"], strict=True)
    ]
    files.assert_unchanged()
    return _sealed({"phase": "restarted", "captured_at": now, "source_commit_seq": watermark,
                    "host_started_at": started_at.astimezone(UTC), "drained_sha256": drained["sha256"],
                    "streams": boundaries, "polls": poll_outcomes, "pending_spool_recovered": recovered_pending})

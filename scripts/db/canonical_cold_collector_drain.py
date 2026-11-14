"""Prove the last accepted collector positions for an approved host shutdown."""

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path

from sqlalchemy import text

from core.storage_mounts import require_configured_working_mount
from market_data.archive import RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_custody_audit import _raw_mappings


VERSION = "qt.canonical_cold_collector_drain.v1"
_CHANNEL_EVENTS = {"level2": "book_segment_canonicalized", "market_trades": "segment_canonicalized"}
_TAIL_EVENTS = (*_CHANNEL_EVENTS.values(), "continuous_capture_stopped", "connected", "reconnected",
                "provider_disconnected", "failed", "interrupted")
_EVENT_FIELDS = "id,session_id,event_ordinal,connection_epoch,event_type,occurred_at,reason,evidence_hash,evidence"


@dataclass(frozen=True)
class CollectorDrainLimits:
    archive: ArchiveVerificationLimits
    raw: RawArchiveReadLimits
    max_events: int
    max_raw_rows: int
    max_spool_entries: int

    def __post_init__(self):
        if (not isinstance(self.archive, ArchiveVerificationLimits) or not isinstance(self.raw, RawArchiveReadLimits)
                or any(type(value) is not int or value <= 0 for value in (
                    self.max_events, self.max_raw_rows, self.max_spool_entries,
                ))):
            raise ValueError("canonical_cold_collector_drain_invalid_limits")


def _encode(value):
    if isinstance(value, datetime) and value.utcoffset() is not None:
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    raise TypeError(f"unsupported collector drain evidence value: {type(value).__name__}")


def _sealed(evidence):
    wire = json.loads(json.dumps({"schema": VERSION, **evidence}, default=_encode, allow_nan=False))
    return {**wire, "sha256": archive_evidence_hash(wire)}


def _timestamp(connection):
    return connection.scalar(text("SELECT transaction_timestamp()"))


def _roster(connection, *, stream_ids, poll_ids, check):
    for identities in (stream_ids, poll_ids):
        if (not isinstance(identities, tuple) or len(identities) > 128
                or any(not isinstance(value, str) or not 0 < len(value) <= 64 for value in identities)
                or tuple(sorted(set(identities))) != identities):
            raise ValueError("canonical_cold_collector_drain_invalid_approved_roster")
    if not stream_ids and not poll_ids:
        raise ValueError("canonical_cold_collector_drain_empty_roster")
    check()
    streams = connection.execute(text("""
        SELECT d.id,d.provider,d.venue,d.provider_product_id,d.series_id,d.channels,d.generation,d.control_generation,
               l.owner_id,l.lease_generation,l.claimed_at,l.expires_at
        FROM market.stream_definitions AS d LEFT JOIN market.stream_lease_state AS l ON l.definition_id=d.id
        WHERE d.enabled AND d.desired_state='running' ORDER BY d.id COLLATE "C" LIMIT :limit
    """), {"limit": len(stream_ids) + 1}).mappings().all()
    check()
    polls = connection.execute(text("""
        SELECT id,source_id,series_id,control_generation,poll_interval_seconds,lease_owner,lease_generation,
               next_scheduled_at,last_attempt_at,last_success_at
        FROM market.collection_definitions WHERE enabled AND desired_state='running'
        ORDER BY id COLLATE "C" LIMIT :limit
    """), {"limit": len(poll_ids) + 1}).mappings().all()
    if tuple(row["id"] for row in streams) != stream_ids or tuple(row["id"] for row in polls) != poll_ids:
        raise RuntimeError("canonical_cold_collector_drain_approved_roster_changed")
    return streams, polls


def _stream_identity(row):
    return {key: row[key] for key in (
        "id", "provider", "venue", "provider_product_id", "series_id", "channels", "generation", "control_generation",
    )}


def _poll_identity(row):
    return {key: row[key] for key in ("id", "source_id", "series_id", "control_generation", "poll_interval_seconds")}


def _manifest(connection, event, *, definition_id, event_kind):
    evidence = event["evidence"]
    identity = evidence.get("manifest_id")
    row = connection.execute(text("""
        SELECT m.* FROM market.raw_archive_manifests AS m
        WHERE m.id=:id AND NOT EXISTS (
            SELECT 1 FROM market.storage_lifecycle_events AS e
            WHERE e.action='archive_expire' AND e.event_type='completed'
            AND e.target_kind='raw_manifest' AND e.target_id=m.id
        )
    """), {"id": identity}).mappings().one_or_none()
    if (event["event_type"] != event_kind or row is None
            or (row["definition_id"], row["session_id"], row["connection_epoch"], row["spool_segment_id"])
            != (definition_id, event["session_id"], event["connection_epoch"], evidence.get("spool_segment_id"))
            or type(evidence.get("record_count")) is not int
            or row["record_count"] != evidence["record_count"]
            or row["record_count"] != row["last_receive_ordinal"] - row["first_receive_ordinal"] + 1):
        raise RuntimeError(f"canonical_cold_collector_drain_segment_receipt_changed: {event['id']}")
    return row


def capture_collector_tail(connection, *, stream_ids, poll_ids, check):
    """Read the live roster and last published position without pausing writers."""
    if (connection.scalar(text("SHOW transaction_read_only")) != "on"
            or connection.scalar(text("SHOW transaction_isolation")) != "repeatable read"):
        raise ValueError("canonical_cold_collector_tail_requires_readonly_snapshot")
    now = _timestamp(connection)
    streams, polls = _roster(connection, stream_ids=stream_ids, poll_ids=poll_ids, check=check)
    tails = []
    for row in streams:
        check()
        primary = [channel for channel in row["channels"] if channel != "heartbeats"]
        if (row["provider"].upper() != "COINBASE" or len(primary) != 1 or primary[0] not in _CHANNEL_EVENTS
                or not row["owner_id"] or row["expires_at"] <= now):
            raise RuntimeError(f"canonical_cold_collector_tail_stream_not_ready: {row['id']}")
        position = connection.execute(text("""
            SELECT id,session_id,event_ordinal,connection_epoch FROM market.stream_session_events
            WHERE definition_id=:id AND owner_id=:owner AND lease_generation=:generation
            AND occurred_at>=:claimed_at
            ORDER BY occurred_at DESC,event_ordinal DESC LIMIT 1
        """), {"id": row["id"], "owner": row["owner_id"], "generation": row["lease_generation"],
               "claimed_at": row["claimed_at"]}).mappings().one_or_none()
        if position is None:
            raise RuntimeError(f"canonical_cold_collector_tail_session_unobserved: {row['id']}")
        kind = _CHANNEL_EVENTS[primary[0]]
        previous = connection.execute(text(f"""
            SELECT {_EVENT_FIELDS} FROM market.stream_session_events
            WHERE session_id=:session AND definition_id=:id AND connection_epoch=:epoch AND event_type=:kind
            ORDER BY event_ordinal DESC LIMIT 1
        """), {"session": position["session_id"], "id": row["id"], "epoch": position["connection_epoch"],
               "kind": kind}).mappings().one_or_none()
        manifest = None if previous is None else _manifest(connection, previous, definition_id=row["id"], event_kind=kind)
        tails.append({"definition": _stream_identity(row), "position": dict(position), "channel": primary[0],
                      "owner_id": row["owner_id"], "lease_generation": row["lease_generation"],
                      "last_receive_ordinal": 0 if manifest is None else manifest["last_receive_ordinal"],
                      "last_manifest_id": None if manifest is None else manifest["id"],
                      "last_segment_event_id": None if previous is None else previous["id"],
                      "last_segment_event_hash": None if previous is None else previous["evidence_hash"]})
    check()
    return _sealed({"phase": "pre_stop", "captured_at": now, "streams": tails, "polls": [dict(row) for row in polls],
                    "source_commit_seq": PostgresMarketDataRepository._current_commit_seq_with_session(connection)})


def _assert_spool_drained(spool_root, *, max_entries, check):
    root = Path(spool_root)
    require_configured_working_mount(root)
    if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("canonical_cold_collector_drain_invalid_spool_root")
    pending = [root]
    count = 0
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                check()
                count += 1
                if count > max_entries:
                    raise RuntimeError("canonical_cold_collector_drain_spool_budget_exceeded")
                # The writer keeps acknowledged JSON sidecars after removing
                # the raw spool. They are disposable and cannot hold frames.
                if entry.is_symlink() or entry.name.endswith((".open", ".sealed")):
                    raise RuntimeError(f"canonical_cold_collector_drain_pending_spool: {entry.name}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
    return count


def verify_collector_drain(connection, *, before, source_commit_seq, spool_root, store, limits, check):
    """Use native terminal events and exact archived positions at the writer fence."""
    if (not isinstance(limits, CollectorDrainLimits) or type(source_commit_seq) is not int
            or before.get("schema") != VERSION or before.get("phase") != "pre_stop"
            or archive_evidence_hash({key: item for key, item in before.items() if key != "sha256"}) != before.get("sha256")):
        raise ValueError("canonical_cold_collector_drain_invalid_tail")
    now = _timestamp(connection)
    captured = datetime.fromisoformat(before["captured_at"])
    if (now <= captured or source_commit_seq < before["source_commit_seq"]
            or PostgresMarketDataRepository._current_commit_seq_with_session(connection) != source_commit_seq):
        raise RuntimeError("canonical_cold_collector_drain_commit_boundary_changed")
    streams, polls = _roster(connection, stream_ids=tuple(item["definition"]["id"] for item in before["streams"]),
                             poll_ids=tuple(item["id"] for item in before["polls"]), check=check)
    if any(row["owner_id"] is not None for row in streams) or any(row["lease_owner"] is not None for row in polls):
        raise RuntimeError("canonical_cold_collector_drain_producer_lease_active")
    spool_entries = _assert_spool_drained(spool_root, max_entries=limits.max_spool_entries, check=check)
    files = ArchiveVerificationBatch(store, limits=limits.archive, check_budget=check)
    positions = []
    remaining_events = limits.max_events
    remaining_rows = limits.max_raw_rows
    for stream, prior in zip(streams, before["streams"], strict=True):
        check()
        if _stream_identity(stream) != prior["definition"]:
            raise RuntimeError(f"canonical_cold_collector_drain_stream_definition_changed: {stream['id']}")
        position = prior["position"]
        scope = {"id": stream["id"], "session": position["session_id"], "previous": position["event_ordinal"],
                 "captured": captured, "kinds": list(_TAIL_EVENTS)}
        # The definition/time index finds any replacement session without
        # scanning the earlier session's event history.
        replaced = connection.scalar(text("""
            SELECT EXISTS(SELECT 1 FROM market.stream_session_events
                WHERE definition_id=:id AND occurred_at>=:captured
                  AND session_id<>:session AND event_type=ANY(:kinds))
        """), scope)
        events = connection.execute(text(f"""
            SELECT {_EVENT_FIELDS},owner_id,lease_generation FROM market.stream_session_events
            WHERE definition_id=:id AND session_id=:session
              AND event_ordinal>:previous AND event_type=ANY(:kinds)
            ORDER BY event_ordinal LIMIT :limit
        """), {**scope, "limit": remaining_events + 1}).mappings().all()
        remaining_events -= len(events)
        if (replaced or any((event["connection_epoch"], event["owner_id"], event["lease_generation"])
                            != (position["connection_epoch"], prior["owner_id"], prior["lease_generation"]) for event in events)):
            raise RuntimeError(f"canonical_cold_collector_drain_session_changed: {stream['id']}")
        if remaining_events < 0:
            raise RuntimeError("canonical_cold_collector_drain_event_budget_exceeded")
        stops = [event for event in events if event["event_type"] == "continuous_capture_stopped"]
        if (len(stops) != 1 or stops[0]["reason"] != "supervisor_stop_requested"
                or not captured <= stops[0]["occurred_at"] <= now
                or any(event["event_type"] not in {"continuous_capture_stopped", _CHANNEL_EVENTS[prior["channel"]]}
                       for event in events)):
            raise RuntimeError(f"canonical_cold_collector_drain_clean_stop_unconfirmed: {stream['id']}")
        stop = stops[0]
        last = prior["last_receive_ordinal"]
        segments = []
        segment_events = [event for event in events if event is not stop]
        count = stop["evidence"].get("raw_record_count")
        baseline_only = not segment_events
        if baseline_only:
            existing = connection.execute(text(f"""
                SELECT {_EVENT_FIELDS} FROM market.stream_session_events WHERE id=:id
            """), {"id": prior["last_segment_event_id"]}).mappings().one_or_none()
            if (existing is None or existing["evidence_hash"] != prior["last_segment_event_hash"]
                    or existing["session_id"] != position["session_id"]
                    or existing["connection_epoch"] != position["connection_epoch"]):
                raise RuntimeError(f"canonical_cold_collector_drain_existing_tail_unconfirmed: {stream['id']}")
            segment_events = [existing]
        for event in segment_events:
            manifest = _manifest(connection, event, definition_id=stream["id"], event_kind=_CHANNEL_EVENTS[prior["channel"]])
            remaining_rows -= manifest["record_count"]
            if remaining_rows < 0 or manifest["record_count"] > limits.raw.max_rows:
                raise RuntimeError("canonical_cold_collector_drain_raw_row_budget_exceeded")
            if baseline_only:
                if manifest["id"] != prior["last_manifest_id"] or manifest["last_receive_ordinal"] != last:
                    raise RuntimeError(f"canonical_cold_collector_drain_existing_tail_changed: {manifest['id']}")
            elif manifest["first_receive_ordinal"] != last + 1:
                raise RuntimeError(f"canonical_cold_collector_drain_ordinal_gap: {manifest['id']}")
            obj = files.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
            exact = _raw_mappings(connection, manifest=manifest, path=store.local_path(obj.object_key),
                                  limits=limits.raw, check=check)
            segments.append({"manifest_id": manifest["id"], "object_sha256": obj.object_sha256,
                             "first_receive_ordinal": manifest["first_receive_ordinal"], "event_id": event["id"],
                             "event_hash": event["evidence_hash"], "mappings": exact,
                             "last_received_at": manifest["last_received_at"]})
            last = manifest["last_receive_ordinal"]
        last_segment = segment_events[-1]
        if (type(count) is not int or count != last or (not baseline_only and (
                last_segment["event_ordinal"] <= stop["event_ordinal"] or last_segment["evidence"].get("terminal") is not True))):
            raise RuntimeError(f"canonical_cold_collector_drain_terminal_watermark_unconfirmed: {stream['id']}")
        positions.append({"definition_id": stream["id"], "session_id": position["session_id"], "epoch": position["connection_epoch"],
                          "channel": prior["channel"], "stop_id": stop["id"], "stop_hash": stop["evidence_hash"],
                          "stopped_at": stop["occurred_at"], "last_receive_ordinal": last, "segments": segments,
                          "tail_was_published_before_stop": baseline_only,
                          "invalid_stream_state": stop["evidence"].get("invalid_stream_state"),
                          "interruption": {"classification": "unproven_provider_interval",
                                           "after": segments[-1]["last_received_at"], "before": None, "missing_events": None}})
    poll_boundaries = []
    for poll, prior in zip(polls, before["polls"], strict=True):
        if _poll_identity(poll) != _poll_identity(prior):
            raise RuntimeError(f"canonical_cold_collector_drain_poll_definition_changed: {poll['id']}")
        poll_boundaries.append({**_poll_identity(poll), "first_due_at_risk": poll["next_scheduled_at"],
                                "last_attempt_at": poll["last_attempt_at"], "last_success_at": poll["last_success_at"]})
    files.assert_unchanged()
    check()
    return _sealed({"phase": "drained", "captured_at": now, "source_commit_seq": source_commit_seq,
                    "pre_stop_sha256": before["sha256"], "streams": positions, "polls": poll_boundaries,
                    "spool_entries_examined": spool_entries})

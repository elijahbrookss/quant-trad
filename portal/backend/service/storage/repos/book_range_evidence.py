"""Book producer's bounded read-only projection of quiet and interrupted ranges.

Generic coverage never interprets book validity, provider frames or receipts.
No facts, gaps or current collector state are mutated by this projection.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import timedelta
import hashlib

from sqlalchemy import text

from data_providers.streams.coinbase import quiet_transport_witness
from market_data.archive import read_raw_archive_parquet, raw_archive_content_fingerprint
from market_data.range_evidence import utc, producer_range_evidence
from .fact_lineage import _witness, BOOK_SCOPE_FIELDS

MAX_OBJECTS = 256
MAX_BYTES = 64 * 1024**2
MAX_RECORDS = 250_000




class BookRangeWitnesses:
    def __init__(self, session, *, object_store, watermark, known_at_lte):
        self.session = session
        self.object_store = object_store
        self.watermark = watermark
        self.cutoff = known_at_lte or session.execute(text("SELECT transaction_timestamp()")).scalar_one()
        self.validities = {}
        self.archives = {}
        self.manifests = {}
        self.processing = {}
        self.bytes_read = 0
        self.records_read = 0

    def validity(self, interval_id):
        if interval_id not in self.validities:
            row = self.session.execute(text("""
                SELECT * FROM market.book_validity_interval_versions
                WHERE interval_id=:id AND known_at<=:cutoff
                ORDER BY revision DESC LIMIT 1
            """), {"id": interval_id, "cutoff": self.cutoff}).mappings().first()
            self.validities[interval_id] = dict(row) if row else None
        return self.validities[interval_id]

    def _manifest(self, identity):
        if identity not in self.manifests:
            if len(self.manifests) >= MAX_OBJECTS:
                raise RuntimeError("book_range_evidence_budget_exceeded: too many lineage manifests")
            row = self.session.execute(text("""
                SELECT * FROM market.raw_archive_manifests WHERE id=:id
            """), {"id": identity}).mappings().first()
            self.manifests[identity] = dict(row) if row else None
        return self.manifests[identity]

    def _processing(self, manifest, path=()):
        """Bind a physical placement to original processing, never mint a receipt."""
        identity = str(manifest["id"])
        if identity in path:
            raise RuntimeError(f"book_range_compaction_cycle: manifest_id={identity}")
        if identity in self.processing:
            return self.processing[identity]
        if manifest["acknowledged_at"] > self.cutoff:
            return None
        edges = [dict(row) for row in self.session.execute(text("""
            SELECT * FROM market.raw_archive_compaction_sources
            WHERE replacement_manifest_id=:id ORDER BY source_ordinal LIMIT :limit
        """), {"id": identity, "limit": MAX_OBJECTS + 1}).mappings()]
        if len(edges) > MAX_OBJECTS:
            raise RuntimeError("book_range_evidence_budget_exceeded: too many lineage edges")
        nodes, receipts, lineage = {identity: manifest}, [], []
        known = [manifest["acknowledged_at"]]
        if edges:
            cursor, count = manifest["first_receive_ordinal"], 0
            for ordinal, edge in enumerate(edges):
                source = self._manifest(str(edge["source_manifest_id"]))
                if source is None:
                    raise RuntimeError(f"book_range_compaction_source_missing: manifest_id={identity}")
                if (edge["source_ordinal"] != ordinal
                        or edge["replacement_content_fingerprint"] != manifest["content_fingerprint"]
                        or any(source[name] != manifest[name] for name in BOOK_SCOPE_FIELDS[:3])
                        or source["first_receive_ordinal"] != cursor
                        or source["last_receive_ordinal"] > manifest["last_receive_ordinal"]):
                    raise RuntimeError(f"book_range_compaction_lineage_conflict: manifest_id={identity}")
                if edge["known_at"] > self.cutoff or edge["compacted_at"] > self.cutoff:
                    return None
                proof = self._processing(source, (*path, identity))
                if proof is None:
                    return None
                if nodes.keys() & proof["nodes"].keys():
                    raise RuntimeError(f"book_range_compaction_lineage_conflict: manifest_id={identity}")
                nodes.update(proof["nodes"])
                receipts.extend(proof["receipts"])
                lineage.extend(proof["lineage"])
                lineage.append(edge)
                known.extend((edge["known_at"], edge["compacted_at"], proof["known_at"]))
                cursor = source["last_receive_ordinal"] + 1
                count += source["record_count"]
            if cursor != manifest["last_receive_ordinal"] + 1 or count != manifest["record_count"]:
                raise RuntimeError(f"book_range_compaction_coverage_conflict: manifest_id={identity}")
        else:
            found = [dict(row) for row in self.session.execute(text("""
                SELECT id,evidence_hash,evidence,known_at FROM market.stream_session_events
                WHERE session_id=:session AND connection_epoch=:epoch
                  AND event_type='book_segment_canonicalized'
                  AND evidence->>'manifest_id'=:manifest AND known_at<=:cutoff
                ORDER BY event_ordinal LIMIT :limit
            """), {"session": manifest["session_id"], "epoch": manifest["connection_epoch"],
                     "manifest": identity, "cutoff": self.cutoff, "limit": MAX_OBJECTS + 1}).mappings()]
            if len(found) > MAX_OBJECTS:
                raise RuntimeError("book_range_evidence_budget_exceeded: too many processing receipts")
            if not found:
                return None
            receipt = found[0]
            if any(item["evidence"] != receipt["evidence"] for item in found[1:]):
                raise RuntimeError(f"book_range_processing_receipt_conflict: manifest_id={identity}")
            evidence = receipt["evidence"]
            commit = evidence.get("book_ingest_commit_seq")
            if (type(commit) is not int or not 0 <= commit <= self.watermark
                    or evidence.get("record_count") != manifest["record_count"]):
                return None
            if evidence.get("spool_segment_id", manifest["spool_segment_id"]) != manifest["spool_segment_id"]:
                raise RuntimeError(f"book_range_processing_receipt_conflict: manifest_id={identity}")
            receipts.append((manifest, receipt))
            known.append(receipt["known_at"])
        proof = {"nodes": nodes, "receipts": receipts, "lineage": lineage, "known_at": max(known)}
        self.processing[identity] = proof
        return proof

    def _read(self, manifest, proof):
        identity = str(manifest["id"])
        if identity in self.archives:
            return self.archives[identity]
        if (len(self.archives) >= MAX_OBJECTS
                or self.bytes_read + int(manifest["byte_count"]) > MAX_BYTES
                or self.records_read + int(manifest["record_count"]) > MAX_RECORDS):
            raise RuntimeError("book_range_evidence_budget_exceeded: narrow the requested range")
        path = self.object_store.local_path(str(manifest["object_key"]))
        if path.stat().st_size != int(manifest["byte_count"]):
            raise RuntimeError(f"book_range_archive_size_mismatch: manifest_id={identity}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["object_sha256"]:
            raise RuntimeError(f"book_range_archive_hash_mismatch: manifest_id={identity}")
        records = read_raw_archive_parquet(path)
        ordinals = [record.receive_ordinal for record in records]
        if any(any(getattr(record, name) != manifest[name] for name in BOOK_SCOPE_FIELDS[:3]) for record in records):
            raise RuntimeError(f"book_range_archive_scope_mismatch: manifest_id={identity}")
        # Verify every original/subtree fingerprint from the retained replacement
        # bytes. Expired originals supply metadata and receipts, never file reads.
        for source in proof["nodes"].values():
            first, last = source["first_receive_ordinal"], source["last_receive_ordinal"]
            subset = records[bisect_left(ordinals, first):bisect_right(ordinals, last)]
            if (len(subset) != source["record_count"] or not subset
                    or (subset[0].receive_ordinal, subset[-1].receive_ordinal) != (first, last)):
                raise RuntimeError(f"book_range_archive_count_mismatch: manifest_id={source['id']}")
            fingerprint = raw_archive_content_fingerprint(
                raw_record_ids=(record.raw_record_id for record in subset),
                raw_frame_sha256=(record.raw_frame_sha256 for record in subset))
            if fingerprint != source["content_fingerprint"]:
                raise RuntimeError(f"book_range_archive_fingerprint_mismatch: manifest_id={source['id']}")
        self.bytes_read += int(manifest["byte_count"])
        self.records_read += len(records)
        self.archives[identity] = records
        return records

    def quiet(self, left, right, start, end):
        if any(left[name] != right[name] for name in BOOK_SCOPE_FIELDS):
            return None
        manifests = self.session.execute(text("""
            SELECT manifests.* FROM market.raw_archive_manifests AS manifests
            WHERE definition_id=:definition_id AND session_id=:session_id
              AND connection_epoch=:connection_epoch
              AND first_receive_ordinal<=:last AND last_receive_ordinal>=:first
              AND acknowledged_at<=:cutoff
              AND NOT EXISTS (
                  SELECT 1 FROM market.storage_lifecycle_events AS expired
                  WHERE expired.action='archive_expire' AND expired.event_type='completed'
                    AND expired.target_kind='raw_manifest' AND expired.target_id=manifests.id)
            ORDER BY byte_count,id LIMIT :limit
        """), {**left, "first": left["receive_ordinal"], "last": right["receive_ordinal"],
                 "cutoff": self.cutoff, "limit": MAX_OBJECTS + 1}).mappings().all()
        if len(manifests) > MAX_OBJECTS:
            raise RuntimeError("book_range_evidence_budget_exceeded: narrow the requested range")
        selected, witnesses, known = {}, [], []
        first, last = left["receive_ordinal"], right["receive_ordinal"]
        if last - first + 1 > MAX_RECORDS:
            raise RuntimeError("book_range_evidence_budget_exceeded: narrow the requested range")
        needed = set(range(first, last + 1))
        for manifest in manifests:
            lower, upper = max(first, manifest["first_receive_ordinal"]), min(last, manifest["last_receive_ordinal"])
            if not any(ordinal in needed for ordinal in range(lower, upper + 1)):
                continue
            proof = self._processing(manifest)
            if proof is None:
                return None
            records = self._read(manifest, proof)
            known.append(proof["known_at"])
            witness = {"manifest_id": manifest["id"], **{name: manifest[name] for name in (
                "object_sha256", "object_key", "object_uri", "content_fingerprint")}}
            if not proof["lineage"]:
                receipt = proof["receipts"][0][1]
                witness.update(completion_receipt_id=receipt["id"], completion_receipt_hash=receipt["evidence_hash"])
            else:
                witness["compaction_sources"] = proof["lineage"]
                witness["source_processing_receipts"] = [
                    {"manifest_id": source["id"], "content_fingerprint": source["content_fingerprint"],
                     "first_receive_ordinal": source["first_receive_ordinal"], "last_receive_ordinal": source["last_receive_ordinal"],
                     "record_count": source["record_count"], "completion_receipt_id": receipt["id"],
                     "completion_receipt_hash": receipt["evidence_hash"], "known_at": receipt["known_at"]}
                    for source, receipt in proof["receipts"]]
            witnesses.append(witness)
            for record in records:
                if first <= record.receive_ordinal <= last:
                    previous = selected.get(record.receive_ordinal)
                    if previous is not None and (previous.raw_record_id, previous.raw_frame_sha256) != (record.raw_record_id, record.raw_frame_sha256):
                        raise RuntimeError("book_range_archive_identity_conflict")
                    selected[record.receive_ordinal] = record
                    needed.discard(record.receive_ordinal)
        if needed:
            return None
        proof = quiet_transport_witness([selected[key] for key in sorted(selected)],
                                        left=left, right=right, start=start, end=end)
        return ({"transport": proof, "archives": witnesses, "known_at": max(known)}
                if proof and known else None)


def project_book_range_evidence(*, rows, start, end, watermark, witnesses):
    """Explain only witnessed holes; absence or uncertainty never proves quiet."""
    groups = defaultdict(dict)
    for row in rows:
        if row["state"] != "active":
            continue
        bucket = utc(row["observation_time"])
        key = (int(row["source_id"]), str(row["source_identity_key"]))
        # All bands share book position; selecting one does not create a sample.
        groups[key].setdefault(bucket, row)
    result = []
    for (source_id, source_key), buckets in sorted(groups.items()):
        ordered = sorted(buckets.items())
        for (prior_time, prior), (next_time, following) in zip(ordered, ordered[1:]):
            lower, upper = max(start, prior_time + timedelta(seconds=1)), min(end, next_time)
            before = witnesses.validity(prior["payload"]["validity_interval_id"])
            after = witnesses.validity(following["payload"]["validity_interval_id"])
            if not before or not after:
                continue
            _, left = _witness(prior)
            _, right = _witness(following)
            evidence_key = "_qt_bbo_evidence" if prior["fact_type"] == "market.bbo" else "_qt_depth_evidence"
            if (before["series_id"] != prior["provenance"][evidence_key]["source_l2_series_id"]
                    or after["series_id"] != following["provenance"][evidence_key]["source_l2_series_id"]
                    or before["opening_session_id"] != left["session_id"]
                    or after["opening_session_id"] != right["session_id"]
                    or before["opening_connection_epoch"] != left["connection_epoch"]
                    or after["opening_connection_epoch"] != right["connection_epoch"]):
                raise RuntimeError("book_range_validity_lineage_disagreement")
            if before["interval_id"] != after["interval_id"]:
                # Only a recorded invalidation followed by a recovery snapshot
                # can explain an interruption. A clean stop is not inferred loss.
                closed = before.get("closing_effective_at")
                if before["status"] != "closed_invalidated" or closed is None:
                    continue
                gap_start = max(start, utc(closed))
                gap_end = min(end, utc(after["opening_effective_at"]))
                if gap_start < gap_end and before["series_id"] == after["series_id"]:
                    result.append(producer_range_evidence(
                        series_id=prior["series_id"], source_id=source_id, source_identity_key=source_key,
                        start=gap_start, end=gap_end, status="interrupted", commit_seq=watermark,
                        known_at=max(before["known_at"], after["opening_known_at"]),
                        witness={"producer": "book_event_buckets.v1", "closing_validity": before,
                                 "recovery_validity": after}))
                continue
            if upper <= lower:
                continue
            if (utc(before["opening_effective_at"]) > lower
                    or before.get("closing_effective_at") is not None
                    and utc(before["closing_effective_at"]) < upper):
                continue
            proof = witnesses.quiet(left, right, lower, upper)
            if proof:
                result.append(producer_range_evidence(
                    series_id=prior["series_id"], source_id=source_id, source_identity_key=source_key,
                    start=lower, end=upper, status="complete", commit_seq=watermark,
                    known_at=max(utc(proof["known_at"]), utc(prior["known_at"]),
                                 utc(following["known_at"]), utc(before["known_at"])),
                    witness={"producer": "book_event_buckets.v1", "validity": before,
                             "left_fact_id": prior["id"], "right_fact_id": following["id"],
                             "left_row_hash": prior["row_hash"], "right_row_hash": following["row_hash"],
                             "transport": proof["transport"], "archives": proof["archives"]}))
    return result


def read_book_range_evidence(session, *, repository, series_id, start, end, watermark, known_at_lte, object_store):
    if end - start > timedelta(hours=6):
        raise ValueError("book_range_evidence_budget_exceeded: request at most six hours")
    # Bound header work before loading payloads or old revisions. A time bound
    # alone does not limit a heavily revised series.
    count = session.execute(text("""
        SELECT count(*) FROM (
            SELECT 1 FROM market.fact_versions
            WHERE series_id=:series_id AND observation_time>=:start AND observation_time<:end
              AND market_commit_seq<=:watermark
              AND (CAST(:cutoff AS timestamptz) IS NULL OR known_at<=:cutoff)
            LIMIT :limit
        ) bounded
    """), {"series_id": series_id, "start": start, "end": end, "watermark": watermark,
             "cutoff": known_at_lte, "limit": MAX_RECORDS + 1}).scalar_one()
    if count > MAX_RECORDS:
        raise ValueError("book_range_evidence_budget_exceeded: narrow the requested range")
    rows = repository._read_canonical_rows_with_session(
        session, series_id=series_id, start=start, end=end, as_of_commit_seq=watermark,
        known_at_lte=known_at_lte, latest_only=False, include_invalidated=True)
    # Revisions are pinned by commit; retain latest per source/observation.
    latest = {}
    for row in rows:
        key = (row["source_id"], row["observation_key"])
        if key not in latest or row["revision"] > latest[key]["revision"]:
            latest[key] = row
    witnesses = BookRangeWitnesses(session,
        object_store=object_store,
        watermark=watermark, known_at_lte=known_at_lte)
    return project_book_range_evidence(rows=list(latest.values()), start=start, end=end,
                                      watermark=watermark, witnesses=witnesses)

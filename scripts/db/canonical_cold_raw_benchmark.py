"""Measure raw archive publication and historical raw mapping reads on a disposable clone."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from math import ceil
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Barrier, Event
from time import monotonic, perf_counter_ns
import json
import os
import platform
import resource
import statistics

from . import canonical_cold_benchmark as common
from .canonical_cold_custody_audit import _raw_mappings


VERSION = "qt.canonical_cold_raw_benchmark.v1"
PK = "market.pk_market_raw_archive_record_mapping"
CHANNELS = {"market_trades": "market.trade", "level2": "market.l2_book"}
MIB = 1024**2


def validate_plan(plan):
    ranges = {"segments": (2, 2048), "rows_per_segment": (2, 1024),
              "backlog_segments": (1, 32), "records_per_second": (0, 100_000),
              "historical_lookup_rows": (1, 32), "historical_lookups": (2, 20_000),
              "retry_every_segments": (1, 128), "timeout_seconds": (5, 7200)}
    if (not isinstance(plan, dict) or set(plan) != {*ranges, "seed_sha256", "historical_record_ids"}
            or not common._matches(plan["seed_sha256"], r"[a-f0-9]{64}")
            or any(not common._bounded_int(plan[key], *bounds) for key, bounds in ranges.items())):
        raise ValueError("raw benchmark plan has invalid scope or seed")
    ids = plan["historical_record_ids"]
    if (not isinstance(ids, list) or not 1 <= len(ids) <= 256
            or any(not common._matches(value, r"raw_[a-f0-9]{64}") for value in ids)
            or len(ids) != len(set(ids)) or len(ids) < plan["historical_lookup_rows"]
            or plan["segments"] * plan["rows_per_segment"] > 100_000
            or plan["records_per_second"] and
            plan["segments"] * plan["rows_per_segment"] >= plan["records_per_second"] * plan["timeout_seconds"]):
        raise ValueError("raw benchmark has inconsistent record bounds")
    return plan


def _latency(values):
    samples = sorted(values)
    if not samples:
        return None
    return {"count": len(samples), "p50_ns": statistics.median(samples),
            "p95_ns": samples[ceil(0.95 * len(samples)) - 1],
            "p99_ns": samples[ceil(0.99 * len(samples)) - 1]}


def _history_read(session, ids):
    from portal.backend.service.storage.repos.fact_lineage import raw_archive_mapping_lookup_statement

    limit = 2048
    rows = session.execute(raw_archive_mapping_lookup_statement(mode="record", bound=False),
                           {"ids": ids, "limit": limit + 1}).mappings().all()
    if len(rows) > limit or {row["raw_record_id"] for row in rows} != set(ids):
        raise RuntimeError("historical raw lookup truncated or missing an authoritative mapping")
    return [dict(row) for row in rows]


def _settled_history(reader, store, ids, check):
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from market_data.fact_archive import archive_evidence_hash

    verifier = ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits(max_objects=256, max_bytes=4 * 1024 * MIB),
                                        check_budget=check)
    checked = {}
    with reader.session() as session:
        rows = _history_read(session, ids)
        for row in rows:
            manifest_id = row["id"]
            if manifest_id in checked:
                continue
            verified = verifier.verify(row["object_key"], row["object_sha256"], expected_bytes=row["byte_count"])
            checked[manifest_id] = _raw_mappings(session, manifest=row, path=store.local_path(verified.object_key),
                                               limits=RawArchiveReadLimits(), check=check)
    return {"mapping_rows": len(rows), "objects": len(checked), "verified_bytes": verifier.byte_count,
            "ordered_sha256": archive_evidence_hash({"rows": rows}), "physical_ordinal_sha256": archive_evidence_hash(checked)}


def _register(writer, repo, scope):
    from market_data.contracts import SourceIdentity
    from portal.backend.db import InstrumentRecord
    from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
    from sqlalchemy import text

    facts = PostgresMarketDataRepository()
    with writer.session() as session:
        if session.scalar(text("SELECT EXISTS (SELECT FROM public.portal_instruments WHERE id=:id)"), {"id": scope}):
            raise RuntimeError("synthetic benchmark scope exists; use a fresh disposable restore")
        session.add(InstrumentRecord(id=scope, datasource="QT_SYNTHETIC", exchange="OFFLINE",
                                     symbol=scope, instrument_type="spot", extra_metadata={"benchmark": VERSION}))
    source_id = facts.register_source(SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT",
                                      source_kind="benchmark", adapter_version=scope))
    definitions = {}
    for channel, fact_type in CHANNELS.items():
        contract = f"{fact_type}.v1"
        series_id = facts.register_series(instrument_id=scope, fact_type=fact_type, timeframe_seconds=None,
                                         contract_version=contract)
        name = f"{scope}-{channel}"
        repo.upsert_stream_definition(definition_id=name, source_id=source_id, series_id=series_id,
            provider="COINBASE", venue="COINBASE_DIRECT", provider_product_id="BTC-USD",
            channels=(channel, "heartbeats"), auth_mode="public", contract_version=contract,
            max_spool_bytes=128 * MIB, max_segment_bytes=2 * MIB)
        definitions[channel] = name
    return definitions


def _append(segment, claim, *, ordinal, channel):
    from data_providers.streams.contracts import ProviderRawMessage
    from market_data.structure import RawStreamRecord

    received_at = datetime.now(UTC)
    # Deterministic material makes paired runs comparable. Actual arrival/ack
    # clocks and lease-generated session IDs are checked within each run.
    material = {"sequence_num": ordinal, "channel": "market_trades" if channel == "market_trades" else "l2_data",
                "product_id": claim.provider_product_id,
                "fixture_payload": sha256(f"{channel}:{ordinal}".encode()).hexdigest()}
    message = ProviderRawMessage.build(provider=claim.provider, venue=claim.venue,
        stream_session_id=claim.session_id, connection_epoch=0, receive_ordinal=ordinal,
        received_at=received_at, raw_frame=json.dumps(material, sort_keys=True))
    record = RawStreamRecord.from_provider_message(message, definition_id=claim.definition_id,
        spool_segment_id=segment.spool_segment_id, provider_product_id=claim.provider_product_id,
        requested_channel=channel, observed_channel=channel)
    segment.append(record)
    return record


def exercise(reader, repo, *, definitions, plan, spool_root, staging, store, check):
    from market_data.archive import DurableRawSpoolSegment, SpoolBacklogTracker, publish_spool_archive, spool_backlog_bytes
    from market_data.fact_archive import archive_evidence_hash

    width = plan["rows_per_segment"]
    rate = plan["records_per_second"]
    history_ids = plan["historical_record_ids"]
    batch_width = plan["historical_lookup_rows"]
    events = Queue(maxsize=plan["backlog_segments"])
    abort, commit_ready = Event(), Event()
    gate = Barrier(3)
    claims = {}
    trackers = {channel: SpoolBacklogTracker(root=spool_root, definition_id=name, initial_bytes=0)
                for channel, name in definitions.items()}
    peak = {"sealed_segments": 0, "spool_bytes": 0, "producer_backpressure_ns": 0}

    def live():
        if abort.is_set():
            raise RuntimeError("raw benchmark peer stopped")
        check()

    def producer():
        target = monotonic()
        channels = tuple(definitions)
        for step in range(plan["segments"]):
            channel = channels[step % len(channels)]
            number = step // len(channels)
            claim = claims[channel]
            segment = DurableRawSpoolSegment(root=spool_root, definition_id=claim.definition_id,
                session_id=claim.session_id, connection_epoch=0, segment_ordinal=number, backlog_tracker=trackers[channel])
            records = []
            try:
                for ordinal in range(number * width + 1, (number + 1) * width + 1):
                    live()
                    if rate:
                        abort.wait(max(0, target - monotonic()))
                        target += 1 / rate
                        live()
                    records.append(_append(segment, claim, ordinal=ordinal, channel=channel))
                    peak["spool_bytes"] = max(peak["spool_bytes"], sum(item.current_bytes for item in trackers.values()))
                segment.seal()
            finally:
                segment.close()
            sealed_ns = perf_counter_ns()
            pending = events.qsize()
            peak["sealed_segments"] = max(peak["sealed_segments"], pending + 1)
            while True:
                live()
                try:
                    events.put((step, channel, segment, records, sealed_ns), timeout=0.1)
                    if pending >= events.maxsize:
                        peak["producer_backpressure_ns"] += perf_counter_ns() - sealed_ns
                    break
                except Full:
                    continue

    def publish():
        samples, manifests, normalized = [], [], []
        for _ in range(plan["segments"]):
            while True:
                live()
                try:
                    step, channel, segment, original, sealed = events.get(timeout=0.1)
                    break
                except Empty:
                    continue
            claim = claims[channel]
            started = perf_counter_ns()
            encoded, ack, rows = publish_spool_archive(segment, object_store=store, temporary_directory=staging)
            if tuple(original) != rows:
                raise RuntimeError("native spool round-trip changed the original frames")
            commit_started = perf_counter_ns()
            commit_ready.set()
            commit = repo.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=rows)
            commit_end = perf_counter_ns()
            if not commit.inserted_manifest or (commit.inserted_mapping_count, commit.mapped_record_count) != (width, width):
                raise RuntimeError("native raw publication did not insert exact fresh mapping count")
            segment.mark_database_acknowledged(manifest_id=commit.manifest_id, object_key=ack.object_key, object_sha256=ack.sha256)
            segment.discard_acknowledged_spool()
            done = perf_counter_ns()
            retry_ns = None
            if (step + 1) % plan["retry_every_segments"] == 0:
                retry_started = perf_counter_ns()
                retry = repo.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=rows)
                retry_ns = perf_counter_ns() - retry_started
                if (retry.manifest_id != commit.manifest_id or retry.inserted_manifest
                        or retry.inserted_mapping_count or retry.mapped_record_count != width):
                    raise RuntimeError("native raw publication retry changed authoritative state")
            samples.append({"channel": channel, "sealed_ns": sealed, "started_ns": started,
                            "commit_started_ns": commit_started, "commit_end_ns": commit_end, "discarded_ns": done,
                            "queued_ns": started - sealed, "verified_upload_ns": commit_started - started,
                            "commit_ns": commit_end - commit_started, "spool_release_ns": done - sealed,
                            "retry_ns": retry_ns, "object_bytes": ack.byte_count})
            manifests.append((commit.manifest_id, tuple(row.raw_record_id for row in rows)))
            normalized.extend((channel, row.receive_ordinal, row.raw_frame_sha256) for row in rows)
        return samples, manifests, archive_evidence_hash({"rows": normalized})

    def read():
        samples = []
        while not commit_ready.wait(0.05):
            live()
        ordinal = 0
        while ordinal < plan["historical_lookups"]:
            live()
            offset = ordinal * batch_width % len(history_ids)
            ids = (history_ids + history_ids)[offset:offset + batch_width]
            started = perf_counter_ns()
            with reader.session() as session:
                rows = _history_read(session, ids)
            end = perf_counter_ns()
            samples.append({"started_ns": started, "end_ns": end, "elapsed_ns": end - started,
                            "ordered_sha256": archive_evidence_hash({"rows": rows})})
            ordinal += 1
        # The mix is measured by actual overlap with database commits below.
        return samples

    def lane(operation):
        try:
            gate.wait(timeout=5)
            return operation()
        except BaseException:
            abort.set()
            gate.abort()
            raise

    with ExitStack() as stack:
        for channel, definition in definitions.items():
            claim = repo.claim_stream(definition_id=definition, owner_id="qt-offline-raw-benchmark",
                                      lease_seconds=plan["timeout_seconds"] + 30, bounded=True)
            claims[channel] = claim
            stack.callback(repo.release, claim)
        began = perf_counter_ns()
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="qt-raw-bench") as workers:
            tasks = [workers.submit(lane, task) for task in (producer, publish, read)]
            tasks[0].result()
            writes, manifests, normalized = tasks[1].result()
            historical = tasks[2].result()
        elapsed_ns = perf_counter_ns() - began
    if spool_backlog_bytes(spool_root):
        raise RuntimeError("raw benchmark final spool backlog did not drain")
    return writes, historical, manifests, normalized, peak | {"terminal_bytes": 0}, elapsed_ns


def _diskstats(devices):
    names = set(devices)
    return {f"{fields[0]}:{fields[1]}": {"kernel_device_name": fields[2], "sectors_read": int(fields[5]),
                                            "sectors_written": int(fields[9]), "io_milliseconds": int(fields[12])}
            for line in Path("/proc/diskstats").read_text().splitlines()
            if len(fields := line.split()) >= 14 and f"{fields[0]}:{fields[1]}" in names}


def _device_path(device):
    entry = Path("/sys/dev/block") / device
    if not entry.exists():
        return None
    resolved = entry.resolve()
    parent = resolved.parent if (resolved / "partition").exists() else resolved
    rotational = parent / "queue" / "rotational"
    return {"kernel_path": str(resolved), "parent_path": str(parent),
            "rotational": rotational.read_text().strip() == "1" if rotational.exists() else None}


def _placement(engine, *, args, archive_root, working):
    from core.storage_mounts import inspect_filesystem
    from sqlalchemy import text

    with engine.connect() as conn:
        durability = dict(conn.execute(text("""
            SELECT name,setting FROM pg_settings
            WHERE name IN ('fsync','synchronous_commit','full_page_writes','block_size','wal_level',
                           'max_wal_size','min_wal_size','checkpoint_timeout','checkpoint_completion_target')
        """)).all())
        catalog = dict(conn.execute(text("""
            SELECT current_setting('data_directory') AS data_directory, pg_relation_filepath(c.oid) AS path,
                   COALESCE(NULLIF(t.spcname, ''), dbspace.spcname) AS tablespace, pg_relation_size(c.oid) AS bytes,
                   i.indisvalid AS valid, i.indisprimary AS primary_key
            FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
            LEFT JOIN pg_tablespace t ON t.oid=c.reltablespace
            JOIN pg_database db ON db.datname=current_database()
            JOIN pg_tablespace dbspace ON dbspace.oid=db.dattablespace
            WHERE c.oid=to_regclass(:index) AND i.indrelid='market.raw_archive_record_mappings'::regclass
        """), {"index": PK}).mappings().one())
        neighbors = conn.execute(text("""
            WITH raw AS (
                SELECT oid,reltoastrelid FROM pg_class WHERE oid='market.raw_archive_record_mappings'::regclass
            ), related AS (
                SELECT oid FROM raw UNION SELECT reltoastrelid FROM raw WHERE reltoastrelid<>0
                UNION SELECT indexrelid FROM pg_index, raw WHERE indrelid IN (raw.oid,raw.reltoastrelid)
            )
            SELECT c.relname, pg_relation_filepath(c.oid) FROM pg_class c JOIN related ON related.oid=c.oid
            WHERE c.oid<>to_regclass(:index) ORDER BY c.relname
        """), {"index": PK}).all()
    if durability["fsync"] != "on" or durability["synchronous_commit"] == "off":
        raise RuntimeError("raw benchmark requires a durable local PostgreSQL commit")
    neighbor_devices = {}
    for name, path in neighbors:
        member = (Path(catalog["data_directory"]) / path).resolve(strict=True)
        info = member.stat()
        neighbor_devices[name] = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
    file_path = (Path(catalog["data_directory"]) / catalog["path"]).resolve(strict=True)
    if (not file_path.is_file() or not catalog["valid"] or not catalog["primary_key"]
            or not file_path.is_relative_to(args.index_mount.resolve(strict=True))
            or catalog["tablespace"] != args.index_tablespace):
        raise RuntimeError("raw benchmark index authority, tablespace or physical path differs")
    index = inspect_filesystem(file_path.parent, expected_uuid=args.index_uuid or "")
    archive = inspect_filesystem(archive_root, expected_uuid=args.archive_uuid or "")
    scratch = inspect_filesystem(working, expected_uuid=args.working_uuid or "")
    index_mount = inspect_filesystem(args.index_mount.resolve(strict=True), expected_uuid=args.index_uuid or "")
    if index.device_id != index_mount.device_id:
        raise RuntimeError("raw benchmark index file is on a different device than its named mount")
    if not args.single_device_smoke and (
        archive.device_id == scratch.device_id
        or index.device_id != {"ssd": scratch.device_id, "hdd": archive.device_id}[args.variant]
    ):
        raise RuntimeError("raw benchmark physical SSD/HDD placement does not match the selected variant")
    paths = {key: _device_path(info.device_id) for key, info in
             (("index", index), ("archive", archive), ("working", scratch))}
    if (not args.single_device_smoke and paths["working"] and paths["archive"]
            and paths["working"]["parent_path"] == paths["archive"]["parent_path"]):
        raise RuntimeError("raw benchmark working and archive filesystems are partitions of the same device")
    return catalog | {"file": str(file_path), "index": asdict(index), "archive": asdict(archive),
                      "working": asdict(scratch), "single_device_smoke": args.single_device_smoke,
                      "neighbor_devices": neighbor_devices, "postgres_durability": durability, "block_devices": paths}


def _pressure(engine, placement):
    from sqlalchemy import text

    with engine.connect() as conn:
        result = dict(conn.execute(text("""
            SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS wal_position,
                   COALESCE((SELECT sum(size) FROM pg_ls_waldir()),0)::bigint AS resident_wal_bytes,
                   pg_relation_size(to_regclass(:index)) AS mapping_primary_bytes,
                   pg_total_relation_size('market.raw_archive_record_mappings') AS mapping_total_bytes,
                   s.idx_blks_read AS mapping_primary_blocks_read, s.idx_blks_hit AS mapping_primary_blocks_hit,
                   t.heap_blks_read AS mapping_heap_blocks_read, t.heap_blks_hit AS mapping_heap_blocks_hit
            FROM pg_statio_user_indexes s CROSS JOIN pg_statio_user_tables t
            WHERE s.indexrelid=to_regclass(:index) AND t.relid='market.raw_archive_record_mappings'::regclass
        """), {"index": PK}).mappings().one())
    return result | {"process_io": common._stat_io(), "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                     "block_device_io": _diskstats(placement[key]["device_id"] for key in ("index", "archive", "working"))}


def _delta(before, after):
    fields = ("wal_position", "resident_wal_bytes", "mapping_primary_bytes", "mapping_total_bytes",
              "mapping_primary_blocks_read", "mapping_primary_blocks_hit", "mapping_heap_blocks_read",
              "mapping_heap_blocks_hit")
    return {field: after[field] - before[field] for field in fields} | {
        "process_io": {key: after["process_io"][key] - value for key, value in before["process_io"].items()
                       if key in after["process_io"]},
        "peak_process_rss_kib_at_end": after["peak_process_rss_kib"],
        "block_device_io": {key: {field: after["block_device_io"][key][field] - old[field]
                                  for field in ("sectors_read", "sectors_written", "io_milliseconds")}
                            for key, old in before["block_device_io"].items() if key in after["block_device_io"]},
    }


def _summary(writes, history, elapsed_ns, plan):
    position, overlaps = 0, 0
    for write in writes:
        while position < len(history) and history[position]["end_ns"] <= write["commit_started_ns"]:
            position += 1
        if position < len(history) and history[position]["started_ns"] < write["commit_end_ns"]:
            overlaps += 1
    if not overlaps:
        raise RuntimeError("historical reads did not overlap native publication commits; increase historical_lookups")
    publish_ns = writes[-1]["discarded_ns"] - writes[0]["sealed_ns"]
    return {"elapsed_ns": elapsed_ns, "records_per_second": len(writes) * plan["rows_per_segment"] * 10**9 / elapsed_ns,
            "publication_window_ns": publish_ns,
            "publication_window_records_per_second": len(writes) * plan["rows_per_segment"] * 10**9 / publish_ns,
            "object_bytes": sum(row["object_bytes"] for row in writes), "commits_overlapping_history": overlaps,
            "history_sql": _latency(row["elapsed_ns"] for row in history),
            **{field: _latency(row[field] for row in writes if row[field] is not None)
               for field in ("queued_ns", "verified_upload_ns", "commit_ns", "spool_release_ns", "retry_ns")}}


def _verify_fresh(reader, store, manifests, width, check):
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from market_data.fact_archive import archive_evidence_hash

    checked = []
    for manifest, ids in manifests:
        check()
        with reader.session() as session:
            rows = _history_read(session, list(ids))
            if len(rows) != width or {row["id"] for row in rows} != {manifest}:
                raise RuntimeError("new raw IDs did not return exactly the published manifest")
            header = rows[0]
            verifier = ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits(max_objects=1, max_bytes=8 * MIB),
                                                check_budget=check)
            object_receipt = verifier.verify(header["object_key"], header["object_sha256"],
                                             expected_bytes=header["byte_count"])
            ordered = _raw_mappings(session, manifest=header, path=store.local_path(object_receipt.object_key),
                                    limits=RawArchiveReadLimits(max_rows=width), check=check)
            if ordered["count"] != width:
                raise RuntimeError("new raw manifest ordinal audit is incomplete")
            checked.append((manifest, object_receipt.object_sha256, tuple(ids), ordered))
    return {"manifests": len(checked), "mappings": len(checked) * width, "exact_run_sha256": archive_evidence_hash({"rows": checked})}


def run(args, plan):
    os.environ["QT_DISABLE_DOTENV"] = "1"
    from core.storage_mounts import configured_archive_root, configured_working_root, require_configured_archive_mount, require_configured_working_mount
    from market_data.archive import FilesystemRawArchiveObjectStore, spool_backlog_bytes
    from market_data.fact_archive import archive_evidence_hash
    from portal.backend.db.session import Database
    from portal.backend.service.storage.repos import market_data, market_structure
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    url = common._db_url(os.environ.get("PG_DSN"))
    validate_plan(plan)
    if (args.disposable_database != url.database or not common._matches(args.runtime_digest, r"sha256:[0-9a-f]{64}")
            or args.seed_receipt.stat().st_size > 64 * MIB
            or sha256(args.seed_receipt.read_bytes()).hexdigest() != plan["seed_sha256"]):
        raise ValueError("raw benchmark requires the exact disposable database, runtime digest and restored seed receipt")
    revision = common._code_revision(market_data, args.revision)
    archive_root = configured_archive_root()
    working = configured_working_root(archive_root)
    if (archive_root != archive_root.resolve(strict=True) or working != working.resolve(strict=True)
            or archive_root != args.archive_root or working != args.working_root
            or not (archive_root / "objects").is_dir() or (archive_root / "objects").is_symlink()
            or (not args.single_device_smoke and not all((args.index_uuid, args.archive_uuid, args.working_uuid)))):
        raise RuntimeError("raw benchmark requires explicit restored archive and working mounts")
    require_configured_archive_mount(archive_root, require_writable=True)
    require_configured_working_mount(working, require_writable=True)
    deadline = monotonic() + plan["timeout_seconds"]

    def check():
        if monotonic() >= deadline:
            raise TimeoutError("raw benchmark exceeded its elapsed budget")

    def database(read_only):
        owner = Database()
        owner._engine = create_engine(url, connect_args={"application_name": "qt_offline_raw_benchmark", "connect_timeout": 5,
            "options": f"-c default_transaction_read_only={read_only} -c statement_timeout=5000 -c lock_timeout=5000"})
        owner._session_factory = sessionmaker(bind=owner._engine, expire_on_commit=False, autoflush=False)
        owner._available = True
        return owner

    reader = database("on")
    writer = None
    scope = "qt-raw-bench-" + common._digest(plan)[:20]
    original = market_structure.db, market_data.db
    try:
        identity = common._database_evidence(reader._engine)
        if ((identity["system_identifier"], identity["oid"], identity["layout"])
                != (args.system_identifier, args.database_oid, args.layout)):
            raise RuntimeError("raw benchmark local database identity or authority differs")
        placement = _placement(reader._engine, args=args, archive_root=archive_root, working=working)
        store = FilesystemRawArchiveObjectStore(archive_root / "objects", writable=False)
        history_ids = plan["historical_record_ids"]
        before = _settled_history(reader, store, history_ids, check)
        width = plan["historical_lookup_rows"]
        expected = []
        with reader.session() as session:
            for offset in range(len(history_ids)):
                ids = (history_ids + history_ids)[offset:offset + width]
                expected.append(archive_evidence_hash({"rows": _history_read(session, ids)}))
        scratch = working / scope
        scratch.mkdir(mode=0o700)
        staging, spool = scratch / "staging", scratch / "spool"
        staging.mkdir(mode=0o700)
        write_store = FilesystemRawArchiveObjectStore(archive_root / "objects")
        writer = database("off")
        market_structure.db = market_data.db = writer
        repository = market_structure.PostgresMarketStructureRepository()
        definitions = _register(writer, repository, scope)
        pressure = _pressure(reader._engine, placement)
        writes, history, manifests, material, backlog, elapsed = exercise(
            reader, repository, definitions=definitions, plan=plan, spool_root=spool, staging=staging, store=write_store, check=check)
        pressure_end = _pressure(reader._engine, placement)
        placement_end = _placement(reader._engine, args=args, archive_root=archive_root, working=working)
        for field in ("path", "file", "tablespace", "block_devices", "neighbor_devices", "postgres_durability"):
            if placement_end[field] != placement[field]:
                raise RuntimeError(f"raw benchmark placement changed during the timed trial: {field}")
        for ordinal, sample in enumerate(history):
            if sample["ordered_sha256"] != expected[ordinal * width % len(history_ids)]:
                raise RuntimeError("concurrent raw lookup differs from its pinned historical catalog")
        after = _settled_history(reader, store, history_ids, check)
        if after != before or spool_backlog_bytes(spool):
            raise RuntimeError("benchmark changed historical raw evidence or left an undrained backlog")
        acceptance = _verify_fresh(reader, store, manifests, plan["rows_per_segment"], check)
        return {"version": VERSION, "plan": plan, "plan_sha256": common._digest(plan), "variant": args.variant,
                "runner_sha256": sha256(Path(__file__).read_bytes()).hexdigest(), "application_revision": revision,
                "machine": platform.uname()._asdict() | {"logical_cpu_count": os.cpu_count()},
                "runtime_digest_declared": args.runtime_digest, "database": identity, "placement": placement,
                "pressure_start": pressure, "pressure_end": pressure_end, "pressure_delta": _delta(pressure, pressure_end),
                "backlog": backlog,
                "history": before, "synthetic": acceptance | {"normalized_material_sha256": material},
                "writes": writes, "historical_sql": history, "summary": _summary(writes, history, elapsed, plan)}
    finally:
        market_structure.db, market_data.db = original
        if writer is not None:
            writer._engine.dispose()
        reader._engine.dispose()


def compare(ssd, hdd):
    for variant, receipt in (("ssd", ssd), ("hdd", hdd)):
        plan = validate_plan(receipt["plan"])
        if (receipt["version"] != VERSION or receipt["variant"] != variant
                or receipt["plan_sha256"] != common._digest(plan)
                or len(receipt["writes"]) != plan["segments"]
                or len(receipt["historical_sql"]) != plan["historical_lookups"]
                or receipt["summary"] != _summary(receipt["writes"], receipt["historical_sql"],
                                                  receipt["summary"]["elapsed_ns"], plan)
                or receipt["pressure_delta"] != _delta(receipt["pressure_start"], receipt["pressure_end"])
                or receipt["backlog"]["terminal_bytes"] or receipt["synthetic"]["manifests"] != plan["segments"]
                or receipt["synthetic"]["mappings"] != plan["segments"] * plan["rows_per_segment"]):
            raise RuntimeError(f"raw benchmark {variant} receipt is inconsistent")
    for field in ("version", "plan_sha256", "application_revision", "runtime_digest_declared", "runner_sha256", "machine", "history"):
        if ssd[field] != hdd[field]:
            raise RuntimeError(f"paired raw benchmark differs in {field}")
    for field in ("postgres", "timescaledb", "settings", "layout", "cold_before_day"):
        if ssd["database"][field] != hdd["database"][field]:
            raise RuntimeError(f"paired raw benchmark database differs in {field}")
    if tuple(ssd["database"][key] for key in ("system_identifier", "oid", "postmaster_start")) == tuple(
            hdd["database"][key] for key in ("system_identifier", "oid", "postmaster_start")):
        raise RuntimeError("paired raw benchmark requires independently restored disposable databases")
    for field in ("archive", "working"):
        if ssd["placement"][field]["device_id"] != hdd["placement"][field]["device_id"]:
            raise RuntimeError(f"paired raw benchmark changed the {field} device")
    for field in ("neighbor_devices", "postgres_durability"):
        if ssd["placement"][field] != hdd["placement"][field]:
            raise RuntimeError(f"paired raw benchmark changed {field}")
    if (ssd["synthetic"]["normalized_material_sha256"] != hdd["synthetic"]["normalized_material_sha256"]
            or ssd["placement"]["single_device_smoke"] != hdd["placement"]["single_device_smoke"]):
        raise RuntimeError("paired raw benchmark changed synthetic material or device scope")
    return {"version": VERSION, "plan_sha256": ssd["plan_sha256"], "single_device_smoke": ssd["placement"]["single_device_smoke"],
            "history": ssd["history"], "ssd": ssd["summary"], "hdd": hdd["summary"],
            "pressure": {"ssd": ssd["pressure_delta"], "hdd": hdd["pressure_delta"]},
            "backlog": {"ssd": ssd["backlog"], "hdd": hdd["backlog"]},
            "ssd_receipt_sha256": common._digest(ssd), "hdd_receipt_sha256": common._digest(hdd)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    trial = subcommands.add_parser("run")
    for field in ("plan", "seed-receipt", "archive-root", "working-root", "index-mount"):
        trial.add_argument(f"--{field}", type=Path, required=True)
    for field in ("disposable-database", "revision", "runtime-digest", "system-identifier", "layout", "index-tablespace"):
        trial.add_argument(f"--{field}", required=True)
    for field in ("archive-uuid", "working-uuid", "index-uuid"):
        trial.add_argument(f"--{field}")
    trial.add_argument("--database-oid", required=True, type=int)
    trial.add_argument("--variant", required=True, choices=("ssd", "hdd"))
    trial.add_argument("--single-device-smoke", action="store_true")
    pair = subcommands.add_parser("compare")
    for field in ("ssd", "hdd"):
        pair.add_argument(f"--{field}", type=Path, required=True)
    args = parser.parse_args()
    report = (run(args, json.loads(args.plan.read_text())) if args.command == "run"
              else compare(json.loads(args.ssd.read_text()), json.loads(args.hdd.read_text())))
    print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

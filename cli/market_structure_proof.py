"""Bounded Coinbase market-structure provider and capacity proof harness.

This module intentionally creates local proof evidence only. Its Parquet files
are not production raw archives, have no archive manifest acknowledgement, and
are never eligible for a frozen dataset.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import math
import os
import platform
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from data_providers.providers.coinbase import CoinbaseProvider
from data_providers.streams import (
    CoinbaseAdvancedTradeStream,
    CoinbaseMessageParser,
    MarketSubscription,
    ProviderRawMessage,
)


PROOF_SCHEMA_VERSION = "coinbase_market_structure_proof.v3"
RAW_PROOF_SCHEMA_VERSION = "coinbase_ws_raw_proof.v1"
ALLOWED_PRODUCTS = frozenset(
    {
        "BIP-20DEC30-CDE",
        "BTC-USD",
        "ETP-20DEC30-CDE",
        "ETH-USD",
        "SLP-20DEC30-CDE",
        "SOL-USD",
    }
)
ALLOWED_CHANNELS = frozenset({"level2", "market_trades", "ticker"})
FUTURES_UNIT_EVIDENCE = {
    "BIP-20DEC30-CDE": {
        "contract_root_unit": "BTC",
        "contract_size": "0.01",
        "base_increment": "1",
    },
    "ETP-20DEC30-CDE": {
        "contract_root_unit": "ETH",
        "contract_size": "0.1",
        "base_increment": "1",
    },
    "SLP-20DEC30-CDE": {
        "contract_root_unit": "SOL",
        "contract_size": "5",
        "base_increment": "1",
    },
}
FUTURES_CONTRACT_SPEC_SOURCE = (
    "https://help.coinbase.com/en/derivatives/perpetual-style-futures/"
    "contract-specifications"
)
CDE_PUBLIC_HISTORY_PAGE = "https://www.coinbase.com/derivatives/historical-data"
CDE_HISTORICAL_FUNDING_URL = (
    "https://api.exchange.fairx.net/rest/funding-rate?symbol=BIPZ30"
)

_SECONDS_PER_YEAR = 31_536_000
_GIB = 1024**3


@dataclass(frozen=True)
class ProofStreamSpec:
    product_id: str
    channel: str
    auth_mode: str

    @property
    def key(self) -> str:
        return f"{self.auth_mode}:{self.product_id}:{self.channel}"


@dataclass(frozen=True)
class ProofCaptureSpec:
    product_id: str
    channels: tuple[str, ...]
    auth_mode: str

    @property
    def channel_label(self) -> str:
        return "+".join(self.channels)

    @property
    def key(self) -> str:
        return f"{self.auth_mode}:{self.product_id}:{self.channel_label}"


class _ProofParquetSink:
    _schema = pa.schema(
        [
            pa.field("schema_version", pa.string(), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("venue", pa.string(), nullable=False),
            pa.field("requested_product_id", pa.string(), nullable=False),
            pa.field("requested_channel", pa.string(), nullable=False),
            pa.field("auth_mode", pa.string(), nullable=False),
            pa.field("stream_session_id", pa.string(), nullable=False),
            pa.field("connection_epoch", pa.int64(), nullable=False),
            pa.field("receive_ordinal", pa.int64(), nullable=False),
            pa.field("received_at", pa.string(), nullable=False),
            pa.field("raw_frame_sha256", pa.string(), nullable=False),
            pa.field("raw_frame", pa.binary(), nullable=False),
        ]
    )

    def __init__(self, output_dir: Path, spec: ProofStreamSpec | ProofCaptureSpec, *, batch_size: int = 1000) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_product = _safe_part(spec.product_id)
        channel_label = spec.channel if isinstance(spec, ProofStreamSpec) else spec.channel_label
        safe_channel = _safe_part(channel_label)
        safe_auth = _safe_part(spec.auth_mode)
        self.partial_path = output_dir / f"{safe_auth}-{safe_product}-{safe_channel}.partial.parquet"
        self.final_path = output_dir / f"{safe_auth}-{safe_product}-{safe_channel}.parquet"
        existing = [path for path in (self.partial_path, self.final_path) if path.exists()]
        if existing:
            raise FileExistsError(
                "Refusing to overwrite existing market-structure proof evidence: "
                + ", ".join(str(path) for path in existing)
            )
        self.spec = spec
        self.channel_label = channel_label
        self.batch_size = max(1, int(batch_size))
        self.rows: list[dict[str, Any]] = []
        self.buffered_raw_bytes = 0
        self.max_buffered_raw_bytes = 0
        self.max_buffered_rows = 0
        self.flush_count = 0
        self.flush_elapsed_seconds = 0.0
        self.max_flush_elapsed_seconds = 0.0
        self.writer = pq.ParquetWriter(
            self.partial_path,
            self._schema,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

    def append(self, message: ProviderRawMessage) -> None:
        raw_frame_bytes = len(message.raw_frame)
        self.rows.append(
            {
                "schema_version": RAW_PROOF_SCHEMA_VERSION,
                "provider": message.provider,
                "venue": message.venue,
                "requested_product_id": self.spec.product_id,
                "requested_channel": self.channel_label,
                "auth_mode": self.spec.auth_mode,
                "stream_session_id": message.stream_session_id,
                "connection_epoch": message.connection_epoch,
                "receive_ordinal": message.receive_ordinal,
                "received_at": message.received_at,
                "raw_frame_sha256": message.raw_frame_sha256,
                "raw_frame": message.raw_frame,
            }
        )
        self.buffered_raw_bytes += raw_frame_bytes
        self.max_buffered_raw_bytes = max(
            self.max_buffered_raw_bytes,
            self.buffered_raw_bytes,
        )
        self.max_buffered_rows = max(self.max_buffered_rows, len(self.rows))
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        started = time.perf_counter()
        self.writer.write_table(pa.Table.from_pylist(self.rows, schema=self._schema))
        elapsed = max(time.perf_counter() - started, 0.0)
        self.flush_count += 1
        self.flush_elapsed_seconds += elapsed
        self.max_flush_elapsed_seconds = max(self.max_flush_elapsed_seconds, elapsed)
        self.rows.clear()
        self.buffered_raw_bytes = 0

    def close(self, *, finalize: bool = True) -> dict[str, Any]:
        self.flush()
        self.writer.close()
        path = self.partial_path
        if finalize:
            self.partial_path.replace(self.final_path)
            path = self.final_path
        return {
            "path": str(path),
            "compressed_bytes": path.stat().st_size,
            "complete": finalize,
            "local_encoder": {
                "batch_size_rows": self.batch_size,
                "flush_count": self.flush_count,
                "flush_elapsed_seconds": round(self.flush_elapsed_seconds, 6),
                "max_flush_elapsed_seconds": round(
                    self.max_flush_elapsed_seconds,
                    6,
                ),
                "max_buffered_rows": self.max_buffered_rows,
                "max_buffered_raw_bytes": self.max_buffered_raw_bytes,
            },
        }


class _ProofBook:
    def __init__(self) -> None:
        self.levels: dict[str, dict[str, str]] = {"bid": {}, "offer": {}}
        self.valid = False
        self.snapshot_count = 0
        self.update_count = 0
        self.update_before_snapshot_count = 0
        self.invalid_mutation_count = 0

    def apply_event(self, event: Mapping[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip().lower()
        updates = event.get("updates")
        if not isinstance(updates, list):
            self.invalid_mutation_count += 1
            return
        if event_type == "snapshot":
            self.levels = {"bid": {}, "offer": {}}
            self.valid = True
            self.snapshot_count += 1
        elif event_type == "update":
            self.update_count += 1
            if not self.valid:
                self.update_before_snapshot_count += 1
                return
        else:
            self.invalid_mutation_count += 1
            return
        for update in updates:
            if not isinstance(update, Mapping):
                self.invalid_mutation_count += 1
                continue
            side = str(update.get("side") or "").strip().lower()
            price = str(update.get("price_level") or "").strip()
            quantity = str(update.get("new_quantity") or "").strip()
            if side == "ask":
                side = "offer"
            if side not in self.levels or not _valid_decimal(price) or not _valid_decimal(quantity):
                self.invalid_mutation_count += 1
                self.valid = False
                continue
            if Decimal(quantity) < 0:
                self.invalid_mutation_count += 1
                self.valid = False
                continue
            if Decimal(quantity) == 0:
                self.levels[side].pop(price, None)
            else:
                self.levels[side][price] = quantity

    def fingerprint(self) -> str | None:
        if not self.valid:
            return None
        payload = {
            side: [
                [price, levels[price]]
                for price in sorted(levels, key=Decimal)
            ]
            for side, levels in sorted(self.levels.items())
        }
        return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()

    def checkpoint_metrics(self) -> dict[str, Any]:
        started = time.perf_counter()
        if not self.valid:
            return {
                "valid": False,
                "level_count": 0,
                "uncompressed_bytes": 0,
                "zstd_bytes": 0,
                "build_elapsed_seconds": round(
                    max(time.perf_counter() - started, 0.0),
                    6,
                ),
            }
        payload = {
            side: [
                [price, levels[price]]
                for price in sorted(levels, key=Decimal)
            ]
            for side, levels in sorted(self.levels.items())
        }
        raw = _canonical_json_bytes(payload)
        compressed = pa.Codec("zstd").compress(raw)
        return {
            "valid": True,
            "level_count": sum(len(levels) for levels in self.levels.values()),
            "uncompressed_bytes": len(raw),
            "zstd_bytes": len(compressed),
            "build_elapsed_seconds": round(
                max(time.perf_counter() - started, 0.0),
                6,
            ),
        }


class _StreamAnalyzer:
    def __init__(self, spec: ProofStreamSpec, *, sample_limit: int) -> None:
        self.spec = spec
        self.sample_limit = max(0, int(sample_limit))
        self.counts: Counter[str] = Counter()
        self.canonical_counts: Counter[str] = Counter()
        self.top_level_schemas: Counter[str] = Counter()
        self.event_schemas: Counter[str] = Counter()
        self.trade_schemas: Counter[str] = Counter()
        self.l2_update_schemas: Counter[str] = Counter()
        self.last_sequence: dict[tuple[int, str], int] = {}
        self.first_sequence: dict[tuple[int, str], int] = {}
        self.last_heartbeat_counter: dict[int, int] = {}
        self.first_requested_event_type: dict[int, str] = {}
        self.samples: list[dict[str, Any]] = []
        self.trade_sizes: list[str] = []
        self.l2_quantities: list[str] = []
        self.trade_sides: set[str] = set()
        self.l2_sides: set[str] = set()
        self.content_hasher = hashlib.sha256()
        self.book_by_epoch: dict[int, _ProofBook] = defaultdict(_ProofBook)
        self.parsers: dict[int, CoinbaseMessageParser] = {}
        self.raw_bytes = 0
        self.max_raw_frame_bytes = 0
        self.trade_count = 0
        self.mutation_count = 0

    def observe(self, message: ProviderRawMessage) -> Counter[str]:
        before_trades = self.trade_count
        before_mutations = self.mutation_count
        self.counts["raw_frames"] += 1
        self.raw_bytes += len(message.raw_frame)
        self.max_raw_frame_bytes = max(self.max_raw_frame_bytes, len(message.raw_frame))
        self.content_hasher.update(bytes.fromhex(message.raw_frame_sha256))
        try:
            payload = json.loads(message.raw_frame)
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.counts["malformed_frames"] += 1
            return Counter()
        if not isinstance(payload, Mapping):
            self.counts["non_object_frames"] += 1
            return Counter()

        self.top_level_schemas[_schema_signature(payload)] += 1
        channel = _normalize_channel(payload.get("channel") or payload.get("type"))
        self.counts[f"channel:{channel}"] += 1
        self._observe_sequence(message.connection_epoch, channel, payload.get("sequence_num"))
        events = payload.get("events")
        event_rows = [event for event in events if isinstance(event, Mapping)] if isinstance(events, list) else []
        for event in event_rows:
            self.event_schemas[_schema_signature(event)] += 1

        parser = self.parsers.setdefault(
            message.connection_epoch,
            CoinbaseMessageParser(
                symbol_by_product_id={self.spec.product_id: self.spec.product_id}
            ),
        )
        parsed = parser.parse_raw(
            message.raw_frame,
            received_at=message.received_at,
            raw_ref=message.evidence_ref(),
        )
        self.canonical_counts.update(event.event_kind for event in parsed)

        if channel == "heartbeats":
            self._observe_heartbeat(message.connection_epoch, event_rows)
        if channel != self.spec.channel:
            return Counter()

        self.counts["requested_channel_frames"] += 1
        if self.sample_limit and len(self.samples) < self.sample_limit:
            self.samples.append(dict(payload))
        for event in event_rows:
            event_type = str(event.get("type") or "").strip().lower()
            if event_type and message.connection_epoch not in self.first_requested_event_type:
                self.first_requested_event_type[message.connection_epoch] = event_type
            if self.spec.channel == "market_trades":
                self._observe_trades(event)
            elif self.spec.channel == "level2":
                self._observe_level2(message.connection_epoch, event)
        return Counter(
            trades=self.trade_count - before_trades,
            mutations=self.mutation_count - before_mutations,
        )

    def _observe_sequence(self, epoch: int, channel: str, value: Any) -> None:
        try:
            sequence = int(value)
        except (TypeError, ValueError):
            self.counts[f"sequence_missing:{channel}"] += 1
            return
        self.counts[f"sequence_observed:{channel}"] += 1
        key = (epoch, "connection")
        prior = self.last_sequence.get(key)
        self.first_sequence.setdefault(key, sequence)
        if prior is not None and sequence > prior + 1:
            self.counts["sequence_gap:connection"] += sequence - prior - 1
        elif prior is not None and sequence == prior:
            self.counts["sequence_duplicate:connection"] += 1
        elif prior is not None and sequence < prior:
            self.counts["sequence_out_of_order:connection"] += 1
        if prior is None or sequence > prior:
            self.last_sequence[key] = sequence

    def _observe_heartbeat(self, epoch: int, events: Sequence[Mapping[str, Any]]) -> None:
        for event in events:
            try:
                counter = int(event.get("heartbeat_counter"))
            except (TypeError, ValueError):
                self.counts["heartbeat_counter_missing"] += 1
                continue
            prior = self.last_heartbeat_counter.get(epoch)
            if prior is not None and counter != prior + 1:
                self.counts["heartbeat_counter_gap"] += max(counter - prior - 1, 1)
            self.last_heartbeat_counter[epoch] = counter

    def _observe_trades(self, event: Mapping[str, Any]) -> None:
        trades = event.get("trades")
        rows = [trade for trade in trades if isinstance(trade, Mapping)] if isinstance(trades, list) else []
        self.counts[f"trade_batch_size:{len(rows)}"] += 1
        self.trade_count += len(rows)
        for trade in rows:
            self.trade_schemas[_schema_signature(trade)] += 1
            side = str(trade.get("side") or "").strip().upper()
            if side:
                self.trade_sides.add(side)
            size = str(trade.get("size") or "").strip()
            if size and len(self.trade_sizes) < 100:
                self.trade_sizes.append(size)

    def _observe_level2(self, epoch: int, event: Mapping[str, Any]) -> None:
        updates = event.get("updates")
        rows = [update for update in updates if isinstance(update, Mapping)] if isinstance(updates, list) else []
        self.counts[f"l2_batch_size:{len(rows)}"] += 1
        self.mutation_count += len(rows)
        for update in rows:
            self.l2_update_schemas[_schema_signature(update)] += 1
            side = str(update.get("side") or "").strip().lower()
            if side:
                self.l2_sides.add(side)
            quantity = str(update.get("new_quantity") or "").strip()
            if quantity and len(self.l2_quantities) < 100:
                self.l2_quantities.append(quantity)
        self.book_by_epoch[epoch].apply_event(event)

    def report(self, *, duplicate_raw_frame_count: int = 0) -> dict[str, Any]:
        requested_sequences = {
            str(epoch): {
                "first": first,
                "last": self.last_sequence.get((epoch, channel)),
            }
            for (epoch, channel), first in sorted(self.first_sequence.items())
            if channel == "connection"
        }
        books = {
            str(epoch): {
                "fingerprint": book.fingerprint(),
                "checkpoint": book.checkpoint_metrics(),
                "snapshot_count": book.snapshot_count,
                "update_count": book.update_count,
                "update_before_snapshot_count": book.update_before_snapshot_count,
                "invalid_mutation_count": book.invalid_mutation_count,
            }
            for epoch, book in sorted(self.book_by_epoch.items())
        }
        return {
            "spec": {
                "product_id": self.spec.product_id,
                "channel": self.spec.channel,
                "auth_mode": self.spec.auth_mode,
            },
            "counts": dict(sorted(self.counts.items())),
            "canonical_counts": dict(sorted(self.canonical_counts.items())),
            "raw_bytes": self.raw_bytes,
            "max_raw_frame_bytes": self.max_raw_frame_bytes,
            "trade_count": self.trade_count,
            "mutation_count": self.mutation_count,
            "duplicate_raw_frame_count": int(duplicate_raw_frame_count),
            "ordered_content_fingerprint": self.content_hasher.hexdigest(),
            "first_requested_event_type_by_epoch": {
                str(key): value for key, value in sorted(self.first_requested_event_type.items())
            },
            "connection_sequence_by_epoch": requested_sequences,
            "schema_signatures": {
                "top_level": dict(sorted(self.top_level_schemas.items())),
                "events": dict(sorted(self.event_schemas.items())),
                "trades": dict(sorted(self.trade_schemas.items())),
                "l2_updates": dict(sorted(self.l2_update_schemas.items())),
            },
            "observed_semantics": {
                "trade_sides": sorted(self.trade_sides),
                "trade_size_samples": self.trade_sizes,
                "l2_sides": sorted(self.l2_sides),
                "l2_quantity_samples": self.l2_quantities,
            },
            "book_by_epoch": books,
            "samples": self.samples,
        }


async def run_coinbase_market_structure_proof(
    *,
    output_dir: Path,
    product_ids: Sequence[str] = ("BIP-20DEC30-CDE", "BTC-USD"),
    channels: Sequence[str] = ("market_trades", "level2", "ticker"),
    auth_mode: str = "public",
    duration_seconds: float = 60.0,
    reconnect_interval_seconds: float | None = None,
    sample_limit: int = 3,
    rest_limit: int = 20,
    max_annual_archive_gib: float | None = None,
) -> dict[str, Any]:
    """Capture one bounded proof run and return its persisted report summary."""

    products = _normalize_products(product_ids)
    normalized_channels = _normalize_channels(channels)
    normalized_auth = _normalize_auth_mode(auth_mode)
    duration = float(duration_seconds)
    if not math.isfinite(duration) or duration < 1 or duration > 86_400:
        raise ValueError("duration_seconds must be between 1 and 86400")
    reconnect_interval = None
    if reconnect_interval_seconds is not None:
        reconnect_interval = float(reconnect_interval_seconds)
        if not math.isfinite(reconnect_interval) or reconnect_interval < 1:
            raise ValueError("reconnect_interval_seconds must be at least 1 when provided")
    annual_archive_budget = None
    if max_annual_archive_gib is not None:
        annual_archive_budget = float(max_annual_archive_gib)
        if not math.isfinite(annual_archive_budget) or annual_archive_budget <= 0:
            raise ValueError("max_annual_archive_gib must be finite and positive when provided")

    output_dir = output_dir.expanduser().resolve()
    raw_output_dir = output_dir / "raw"
    if (output_dir / "proof-report.json").exists():
        raise FileExistsError(
            f"Refusing to overwrite existing proof report: {output_dir / 'proof-report.json'}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    proof_started = time.monotonic()
    started_at = _utc_iso()
    proof_implementation = _proof_implementation()
    aggregate_rates: dict[int, Counter[str]] = defaultdict(Counter)
    capture_specs = [
        ProofCaptureSpec(
            product_id=product_id,
            channels=tuple(normalized_channels),
            auth_mode=normalized_auth,
        )
        for product_id in products
    ]

    rest_results = await asyncio.to_thread(
        _run_rest_proofs,
        products,
        normalized_auth,
        max(1, min(int(rest_limit), 100)),
    )
    captured_products = await asyncio.gather(
        *[
            _capture_product_stream(
                spec,
                output_dir=raw_output_dir,
                duration_seconds=duration,
                reconnect_interval_seconds=reconnect_interval,
                sample_limit=sample_limit,
                proof_started=proof_started,
                aggregate_rates=aggregate_rates,
                start_delay_seconds=index * 0.5,
            )
            for index, spec in enumerate(capture_specs)
        ]
    )
    stream_results = [row for product_rows in captured_products for row in product_rows]
    elapsed = max(time.monotonic() - proof_started, 0.000001)
    capacity = _capacity_summary(
        stream_results,
        aggregate_rates,
        elapsed_seconds=elapsed,
        max_annual_archive_gib=annual_archive_budget,
    )
    quantity_semantics = _quantity_semantics(rest_results, stream_results)
    trade_side_semantics = _trade_side_semantics(stream_results)
    status = "completed" if all(row.get("status") == "completed" for row in stream_results) else "failed"
    implementation_readiness = _phase1_implementation_readiness(
        stream_results,
        rest_results,
        capacity,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
        duration_seconds=duration,
    )
    production_capacity_admission = _phase1_admission(
        stream_results,
        rest_results,
        capacity,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
        duration_seconds=duration,
    )
    report = {
        "schema_version": PROOF_SCHEMA_VERSION,
        "status": status,
        "scope": {
            "provider": "COINBASE",
            "venue": "COINBASE_DIRECT",
            "product_ids": products,
            "channels": normalized_channels,
            "auth_mode": normalized_auth,
            "duration_seconds": duration,
            "reconnect_interval_seconds": reconnect_interval,
            "archive_complete": False,
            "dataset_eligible": False,
            "proof_only": True,
        },
        "started_at": started_at,
        "ended_at": _utc_iso(),
        "elapsed_seconds": round(elapsed, 6),
        "proof_implementation": proof_implementation,
        "rest": rest_results,
        "streams": stream_results,
        "quantity_semantics": quantity_semantics,
        "trade_side_semantics": trade_side_semantics,
        "capacity": capacity,
        "phase1_implementation_readiness": implementation_readiness,
        "production_capacity_admission": production_capacity_admission,
        "explicit_limitations": [
            "raw_frame is the exact WebSocket application-message payload, not TCP/WebSocket framing or compression bytes",
            "proof Parquet files are local evidence, not acknowledged production archives",
            "recent REST trades are bounded reconciliation evidence, not historical completeness",
            "observed contiguity cannot upgrade undocumented recovery semantics",
            "futures quantity decisions require contract metadata plus observed trade and L2 increment reconciliation",
            "spot sizes remain provider base quantities and are not assigned futures contract semantics",
            "implementation readiness does not authorize production enrollment; the deferred 24-hour capacity and explicit budget gates still apply",
        ],
    }
    report_bytes = json.dumps(report, indent=2, sort_keys=True, default=str).encode("utf-8") + b"\n"
    report_path = output_dir / "proof-report.json"
    report_path.write_bytes(report_bytes)
    report_sha = hashlib.sha256(report_bytes).hexdigest()
    checksum_path = output_dir / "proof-report.sha256"
    checksum_path.write_text(f"{report_sha}  {report_path.name}\n", encoding="utf-8")
    return {
        "status": status,
        "report_path": str(report_path),
        "report_sha256": report_sha,
        "checksum_path": str(checksum_path),
        "capacity": capacity,
        "phase1_implementation_readiness": implementation_readiness,
        "production_capacity_admission": production_capacity_admission,
    }


async def _capture_product_stream(
    spec: ProofCaptureSpec,
    *,
    output_dir: Path,
    duration_seconds: float,
    reconnect_interval_seconds: float | None,
    sample_limit: int,
    proof_started: float,
    aggregate_rates: dict[int, Counter[str]],
    start_delay_seconds: float,
) -> list[dict[str, Any]]:
    if start_delay_seconds > 0:
        await asyncio.sleep(start_delay_seconds)
    provider = CoinbaseProvider() if spec.auth_mode == "authenticated" else None
    stream = CoinbaseAdvancedTradeStream(
        jwt_factory=provider.build_websocket_jwt if provider is not None else None
    )
    analyzers = {
        channel: _StreamAnalyzer(
            ProofStreamSpec(spec.product_id, channel, spec.auth_mode),
            sample_limit=sample_limit,
        )
        for channel in spec.channels
    }
    sink = _ProofParquetSink(output_dir, spec)
    deadline = time.monotonic() + duration_seconds
    connection_count = 0
    deliberate_reconnect_count = 0
    unexpected_disconnect_count = 0
    provider_errors: list[dict[str, str]] = []
    observed_raw_hashes: set[tuple[int, bytes]] = set()
    duplicate_raw_frame_count = 0
    error: str | None = None
    status = "completed"
    sink_result: dict[str, Any] = {}
    pending: asyncio.Task[ProviderRawMessage] | None = None
    try:
        while time.monotonic() < deadline:
            connection_count += 1
            await stream.connect()
            await stream.subscribe(
                [
                    MarketSubscription.from_values(
                        provider="COINBASE",
                        venue="COINBASE_DIRECT",
                        symbol=spec.product_id,
                        product_id=spec.product_id,
                        channels=("heartbeats", *spec.channels),
                        auth_mode=spec.auth_mode,
                    )
                ]
            )
            connection_deadline = deadline
            if reconnect_interval_seconds is not None:
                connection_deadline = min(
                    deadline,
                    time.monotonic() + reconnect_interval_seconds,
                )
            iterator = stream.raw_messages().__aiter__()
            ended_early = False
            pending = None
            while time.monotonic() < connection_deadline:
                remaining = min(connection_deadline, deadline) - time.monotonic()
                if remaining <= 0:
                    break
                if pending is None:
                    pending = asyncio.create_task(anext(iterator))
                done, _ = await asyncio.wait({pending}, timeout=min(remaining, 1.0))
                if not done:
                    continue
                try:
                    message = pending.result()
                except StopAsyncIteration:
                    ended_early = True
                    break
                finally:
                    pending = None
                sink.append(message)
                raw_hash = bytes.fromhex(message.raw_frame_sha256)
                raw_identity = (message.connection_epoch, raw_hash)
                if raw_identity in observed_raw_hashes:
                    duplicate_raw_frame_count += 1
                else:
                    observed_raw_hashes.add(raw_identity)
                observed = Counter()
                for analyzer in analyzers.values():
                    observed.update(analyzer.observe(message))
                provider_error = _provider_error_payload(message)
                if provider_error is not None:
                    provider_errors.append(provider_error)
                second = max(0, int(time.monotonic() - proof_started))
                bucket = aggregate_rates[second]
                bucket["frames"] += 1
                bucket["raw_bytes"] += len(message.raw_frame)
                bucket["trades"] += observed["trades"]
                bucket["mutations"] += observed["mutations"]
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pending
            await stream.close()
            if time.monotonic() >= deadline:
                break
            if ended_early:
                unexpected_disconnect_count += 1
                await asyncio.sleep(min(1.0, max(deadline - time.monotonic(), 0.0)))
            elif reconnect_interval_seconds is not None:
                deliberate_reconnect_count += 1
            else:
                break
    except asyncio.CancelledError:
        status = "cancelled"
        raise
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"
    finally:
        if pending is not None:
            pending.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await pending
        with contextlib.suppress(Exception):
            await stream.close()
        sink_result = sink.close(finalize=status == "completed" and not provider_errors)

    if provider_errors:
        status = "failed"
        error = error or "Coinbase returned one or more provider error frames."
    results: list[dict[str, Any]] = []
    for index, (channel, analyzer) in enumerate(analyzers.items()):
        analysis = analyzer.report(
            duplicate_raw_frame_count=duplicate_raw_frame_count
        )
        stream_spec = ProofStreamSpec(spec.product_id, channel, spec.auth_mode)
        replay = await asyncio.to_thread(
            _replay_proof_archive,
            Path(sink_result["path"]),
            stream_spec,
        )
        replay["capture_content_fingerprint"] = analysis["ordered_content_fingerprint"]
        replay["content_fingerprint_equal"] = (
            replay.get("ordered_content_fingerprint")
            == analysis["ordered_content_fingerprint"]
        )
        capture_book_fingerprints = {
            epoch: book.get("fingerprint")
            for epoch, book in (analysis.get("book_by_epoch") or {}).items()
        }
        replay["capture_book_fingerprints"] = capture_book_fingerprints
        replay["book_fingerprints_equal"] = (
            replay.get("book_fingerprints") == capture_book_fingerprints
        )
        result = {
            "status": status,
            "spec": analysis.pop("spec"),
            "connection_count": connection_count,
            "deliberate_reconnect_count": deliberate_reconnect_count,
            "unexpected_disconnect_count": unexpected_disconnect_count,
            "capacity_attribution": index == 0,
            "raw_file": sink_result,
            "analysis": analysis,
            "replay": replay,
        }
        if provider_errors:
            result["provider_errors"] = provider_errors
        if error:
            result["error"] = error
        results.append(result)
    return results


def _run_rest_proofs(
    product_ids: Sequence[str],
    auth_mode: str,
    limit: int,
) -> dict[str, Any]:
    provider = CoinbaseProvider()
    results: dict[str, Any] = {}
    for product_id in product_ids:
        operations: dict[str, Any] = {}
        for name, callback in (
            (
                "product",
                lambda product_id=product_id: provider.fetch_product_proof(
                    product_id,
                    auth_mode=auth_mode,
                ),
            ),
            (
                "product_book",
                lambda product_id=product_id: provider.fetch_product_book_proof(
                    product_id,
                    auth_mode=auth_mode,
                    limit=limit,
                ),
            ),
            (
                "recent_market_trades",
                lambda product_id=product_id: provider.fetch_recent_market_trades_proof(
                    product_id,
                    auth_mode=auth_mode,
                    limit=limit,
                ),
            ),
        ):
            try:
                operations[name] = {"status": "confirmed", "payload": callback()}
            except Exception as exc:  # noqa: BLE001
                operations[name] = {
                    "status": "unsupported_or_failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
        results[product_id] = operations
    return {
        "auth_mode": auth_mode,
        "limit": limit,
        "products": results,
        "historical_completeness": "unsupported",
        "cde_public_history": _run_cde_public_history_proofs(),
    }


def _run_cde_public_history_proofs() -> dict[str, Any]:
    page = _bounded_public_probe(CDE_PUBLIC_HISTORY_PAGE, accept="text/html")
    funding = _bounded_public_probe(
        CDE_HISTORICAL_FUNDING_URL,
        accept="application/json",
    )
    daily_statistics = _classify_cde_daily_statistics(page)
    finalized_funding = _classify_cde_finalized_funding(funding)
    sources = {
        "daily_market_statistics": daily_statistics,
        "finalized_funding": finalized_funding,
    }
    unresolved = [
        name for name, source in sources.items() if source["status"] == "unverified"
    ]
    status = "unverified" if unresolved else "unsupported"
    reasons = [reason for source in sources.values() for reason in source["reasons"]]
    return {
        "status": status,
        "admission": "rejected" if status == "unsupported" else "blocked_pending_proof",
        "reasons": reasons,
        "sources": sources,
        "historical_page": page,
        "historical_funding": funding,
    }


def _classify_cde_daily_statistics(page: Mapping[str, Any]) -> dict[str, Any]:
    if page.get("result") == "network_error":
        return {"status": "unverified", "reasons": ["historical_page_probe_failed"]}
    if page.get("http_status") in {401, 403}:
        return {
            "status": "unsupported",
            "reasons": [
                "historical_page_not_machine_accessible_without_auth_or_challenge"
            ],
        }
    if page.get("http_status") == 200:
        return {
            "status": "unsupported",
            "reasons": [
                "historical_page_has_no_stable_documented_machine_data_contract"
            ],
        }
    return {
        "status": "unverified",
        "reasons": ["historical_page_public_behavior_unresolved"],
    }


def _classify_cde_finalized_funding(funding: Mapping[str, Any]) -> dict[str, Any]:
    if funding.get("result") == "network_error":
        return {
            "status": "unverified",
            "reasons": ["historical_funding_probe_failed"],
        }
    if funding.get("http_status") in {401, 403}:
        return {
            "status": "unsupported",
            "reasons": ["historical_funding_requires_cde_request_credentials"],
        }
    if funding.get("http_status") == 200:
        return {
            "status": "unverified",
            "reasons": ["historical_funding_public_response_requires_semantic_review"],
        }
    return {
        "status": "unverified",
        "reasons": ["historical_funding_public_behavior_unresolved"],
    }


def _bounded_public_probe(url: str, *, accept: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": "quant-trad-market-structure-proof/1.0",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=15.0) as response:
            body = response.read(2 * 1024 * 1024 + 1)
            status = int(response.status)
            content_type = str(response.headers.get("Content-Type") or "")
    except urllib.error.HTTPError as exc:
        body = exc.read(4097)
        status = int(exc.code)
        content_type = str(exc.headers.get("Content-Type") or "")
    except (OSError, urllib.error.URLError) as exc:
        return {
            "url": url,
            "result": "network_error",
            "error": f"{type(exc).__name__}: {exc}",
        }
    truncated = len(body) > 2 * 1024 * 1024 if status < 400 else len(body) > 4096
    bounded_body = body[: 2 * 1024 * 1024] if status < 400 else body[:4096]
    result: dict[str, Any] = {
        "url": url,
        "result": "http_response",
        "http_status": status,
        "content_type": content_type,
        "body_bytes_observed": len(bounded_body),
        "body_truncated": truncated,
        "body_sha256": hashlib.sha256(bounded_body).hexdigest(),
    }
    if "json" in content_type.lower():
        try:
            payload = json.loads(bounded_body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, Mapping) and payload.get("error"):
            result["provider_error"] = str(payload["error"])
    return result


def _replay_proof_archive(path: Path, spec: ProofStreamSpec) -> dict[str, Any]:
    started = time.perf_counter()
    frame_count = 0
    raw_bytes = 0
    trade_count = 0
    mutation_count = 0
    content_hasher = hashlib.sha256()
    last_ordinal: dict[int, int] = {}
    ordering_errors = 0
    books: dict[int, _ProofBook] = defaultdict(_ProofBook)
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(
        columns=[
            "connection_epoch",
            "receive_ordinal",
            "raw_frame_sha256",
            "raw_frame",
        ],
        batch_size=4096,
    ):
        for row in batch.to_pylist():
            epoch = int(row["connection_epoch"])
            ordinal = int(row["receive_ordinal"])
            prior = last_ordinal.get(epoch)
            if prior is not None and ordinal != prior + 1:
                ordering_errors += 1
            last_ordinal[epoch] = ordinal
            frame = bytes(row["raw_frame"])
            frame_hash = hashlib.sha256(frame).hexdigest()
            if frame_hash != row["raw_frame_sha256"]:
                ordering_errors += 1
            content_hasher.update(bytes.fromhex(frame_hash))
            frame_count += 1
            raw_bytes += len(frame)
            try:
                payload = json.loads(frame)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            channel = _normalize_channel(payload.get("channel") or payload.get("type"))
            if channel != spec.channel:
                continue
            events = payload.get("events")
            rows = [event for event in events if isinstance(event, Mapping)] if isinstance(events, list) else []
            if channel == "market_trades":
                for event in rows:
                    trades = event.get("trades")
                    if isinstance(trades, list):
                        trade_count += sum(isinstance(trade, Mapping) for trade in trades)
            elif channel == "level2":
                for event in rows:
                    updates = event.get("updates")
                    if isinstance(updates, list):
                        mutation_count += sum(isinstance(update, Mapping) for update in updates)
                    books[epoch].apply_event(event)
    elapsed = max(time.perf_counter() - started, 0.000001)
    return {
        "elapsed_seconds": round(elapsed, 6),
        "frames": frame_count,
        "raw_bytes": raw_bytes,
        "frames_per_second": round(frame_count / elapsed, 3),
        "mib_per_second": round(raw_bytes / elapsed / (1024**2), 6),
        "trade_count": trade_count,
        "mutation_count": mutation_count,
        "ordering_or_checksum_errors": ordering_errors,
        "ordered_content_fingerprint": content_hasher.hexdigest(),
        "book_fingerprints": {
            str(epoch): book.fingerprint() for epoch, book in sorted(books.items())
        },
    }


def _capacity_summary(
    stream_results: Sequence[Mapping[str, Any]],
    aggregate_rates: Mapping[int, Counter[str]],
    *,
    elapsed_seconds: float,
    max_annual_archive_gib: float | None,
) -> dict[str, Any]:
    raw_bytes = sum(
        int((row.get("analysis") or {}).get("raw_bytes") or 0)
        for row in stream_results
        if row.get("capacity_attribution", True)
    )
    compressed_bytes = sum(
        int((row.get("raw_file") or {}).get("compressed_bytes") or 0)
        for row in stream_results
        if row.get("capacity_attribution", True)
    )
    measured_seconds = max(1, math.ceil(elapsed_seconds))
    buckets = [aggregate_rates.get(second, Counter()) for second in range(measured_seconds)]
    raw_byte_rates = [int(bucket.get("raw_bytes") or 0) for bucket in buckets]
    frame_rates = [int(bucket.get("frames") or 0) for bucket in buckets]
    trade_rates = [int(bucket.get("trades") or 0) for bucket in buckets]
    mutation_rates = [int(bucket.get("mutations") or 0) for bucket in buckets]
    compression_ratio = raw_bytes / compressed_bytes if compressed_bytes else None
    compressed_bps = compressed_bytes / elapsed_seconds
    p99_raw_bps = _percentile(raw_byte_rates, 0.99)
    p99_compressed_bps = (
        p99_raw_bps / compression_ratio if compression_ratio and p99_raw_bps is not None else None
    )
    annual_archive_gib = compressed_bps * _SECONDS_PER_YEAR / _GIB
    required_3x_six_hour_spool_gib = (
        p99_raw_bps * 21_600 * 3 / _GIB if p99_raw_bps is not None else None
    )
    recommended_initial_spool_gib = required_3x_six_hour_spool_gib
    budget_pass = (
        annual_archive_gib <= max_annual_archive_gib
        if max_annual_archive_gib is not None
        else None
    )
    replay_elapsed_seconds = [
        float((row.get("replay") or {}).get("elapsed_seconds") or 0.0)
        for row in stream_results
    ]
    replay_max_seconds = max(replay_elapsed_seconds, default=0.0)
    checkpoint_zstd_bytes = [
        int((book.get("checkpoint") or {}).get("zstd_bytes") or 0)
        for row in stream_results
        if (row.get("spec") or {}).get("channel") == "level2"
        for book in ((row.get("analysis") or {}).get("book_by_epoch") or {}).values()
        if (book.get("checkpoint") or {}).get("valid")
    ]
    checkpoint_uncompressed_bytes = [
        int((book.get("checkpoint") or {}).get("uncompressed_bytes") or 0)
        for row in stream_results
        if (row.get("spec") or {}).get("channel") == "level2"
        for book in ((row.get("analysis") or {}).get("book_by_epoch") or {}).values()
        if (book.get("checkpoint") or {}).get("valid")
    ]
    checkpoint_build_elapsed_seconds = [
        float((book.get("checkpoint") or {}).get("build_elapsed_seconds") or 0.0)
        for row in stream_results
        if (row.get("spec") or {}).get("channel") == "level2"
        for book in ((row.get("analysis") or {}).get("book_by_epoch") or {}).values()
        if (book.get("checkpoint") or {}).get("valid")
    ]
    attributed_raw_files = [
        row.get("raw_file") or {}
        for row in stream_results
        if row.get("capacity_attribution", True)
    ]
    local_encoders = [raw_file.get("local_encoder") or {} for raw_file in attributed_raw_files]
    max_raw_frame_bytes = max(
        (
            int((row.get("analysis") or {}).get("max_raw_frame_bytes") or 0)
            for row in stream_results
            if row.get("capacity_attribution", True)
        ),
        default=0,
    )
    one_day_replay_gate_pass = (
        replay_max_seconds < 3_600 if elapsed_seconds >= 86_400 else False
    )
    return {
        "elapsed_seconds": round(elapsed_seconds, 6),
        "raw_bytes": raw_bytes,
        "parquet_zstd_bytes": compressed_bytes,
        "raw_to_parquet_zstd_ratio": round(compression_ratio, 6) if compression_ratio else None,
        "frames_per_second": _rate_summary(frame_rates),
        "trades_per_second": _rate_summary(trade_rates),
        "l2_mutations_per_second": _rate_summary(mutation_rates),
        "raw_bytes_per_second": _rate_summary(raw_byte_rates),
        "max_raw_frame_bytes": max_raw_frame_bytes,
        "checkpoint_zstd_bytes": _rate_summary(checkpoint_zstd_bytes),
        "checkpoint_uncompressed_bytes": _rate_summary(checkpoint_uncompressed_bytes),
        "checkpoint_build_seconds": _float_rate_summary(
            checkpoint_build_elapsed_seconds
        ),
        "local_encoder": {
            "flush_count": sum(int(row.get("flush_count") or 0) for row in local_encoders),
            "flush_elapsed_seconds": round(
                sum(float(row.get("flush_elapsed_seconds") or 0.0) for row in local_encoders),
                6,
            ),
            "max_flush_elapsed_seconds": max(
                (float(row.get("max_flush_elapsed_seconds") or 0.0) for row in local_encoders),
                default=0.0,
            ),
            "max_buffered_rows": max(
                (int(row.get("max_buffered_rows") or 0) for row in local_encoders),
                default=0,
            ),
            "max_buffered_raw_bytes": max(
                (int(row.get("max_buffered_raw_bytes") or 0) for row in local_encoders),
                default=0,
            ),
            "scope": "local proof Parquet encoder only; not object upload backlog",
        },
        "deferred_measurements": {
            "object_upload_latency_and_backlog": {
                "status": "not_measured",
                "reason": "Phase 1 archive uploader and manifest acknowledgement do not exist yet",
                "required_before": "production collector enrollment",
            },
            "typed_hot_store_bytes_and_index_amplification": {
                "status": "not_measured",
                "reason": "Phase 1 canonical trade tables and indexes do not exist yet",
                "required_before": "production collector enrollment",
            },
        },
        "full_replay": {
            "max_elapsed_seconds": round(replay_max_seconds, 6),
            "one_day_under_one_hour_gate_pass": one_day_replay_gate_pass,
            "channel_replay_count": len(replay_elapsed_seconds),
        },
        "checkpoint_measurement_count": len(checkpoint_zstd_bytes),
        "replay_measurement_count": len(replay_elapsed_seconds),
        "annualized_parquet_zstd_gib": round(annual_archive_gib, 6),
        "p99_estimated_compressed_bytes_per_second": (
            round(p99_compressed_bps, 6) if p99_compressed_bps is not None else None
        ),
        "p99_input_bytes_per_second": p99_raw_bps,
        "recommended_initial_spool_gib": (
            round(recommended_initial_spool_gib, 6)
            if recommended_initial_spool_gib is not None
            else None
        ),
        "required_3x_six_hour_spool_gib": (
            round(required_3x_six_hour_spool_gib, 6)
            if required_3x_six_hour_spool_gib is not None
            else None
        ),
        "max_annual_archive_gib": max_annual_archive_gib,
        "annual_archive_budget_pass": budget_pass,
        "measurement_duration_gate_pass": elapsed_seconds >= 86_400,
    }


def _phase1_admission(
    streams: Sequence[Mapping[str, Any]],
    rest: Mapping[str, Any],
    capacity: Mapping[str, Any],
    *,
    duration_seconds: float,
    quantity_semantics: Mapping[str, Any] | None = None,
    trade_side_semantics: Mapping[str, Any] | None = None,
    require_production_capacity: bool = True,
) -> dict[str, Any]:
    """Evaluate provider correctness plus optional production capacity gates."""

    reasons: list[str] = []
    if require_production_capacity:
        if duration_seconds < 86_400 or not capacity.get("measurement_duration_gate_pass"):
            reasons.append("24_hour_capacity_capture_required")
    elif duration_seconds < 3_600:
        reasons.append("one_hour_provider_capture_required")
    full_replay = capacity.get("full_replay") or {}
    if duration_seconds >= 86_400 and not full_replay.get(
        "one_day_under_one_hour_gate_pass"
    ):
        reasons.append("one_day_replay_under_one_hour_not_proven")
    if any(row.get("status") != "completed" for row in streams):
        reasons.append("stream_capture_failed")
    observed_streams = {
        (
            str((row.get("spec") or {}).get("product_id") or ""),
            str((row.get("spec") or {}).get("channel") or ""),
        )
        for row in streams
    }
    for product_id in ("BIP-20DEC30-CDE", "BTC-USD"):
        for channel in ("market_trades", "level2", "ticker"):
            if (product_id, channel) not in observed_streams:
                reasons.append(f"required_stream_missing:{product_id}:{channel}")
    for row in streams:
        spec = row.get("spec") or {}
        analysis = row.get("analysis") or {}
        replay = row.get("replay") or {}
        counts = analysis.get("counts") or {}
        if not (row.get("raw_file") or {}).get("complete"):
            reasons.append(
                f"raw_proof_evidence_incomplete:{spec.get('product_id')}:{spec.get('channel')}"
            )
        if int(counts.get("channel:heartbeats") or 0) == 0:
            reasons.append(f"no_heartbeat_evidence:{spec.get('product_id')}")
        for evidence_key in (
            "sequence_gap:connection",
            "sequence_out_of_order:connection",
            "heartbeat_counter_gap",
            "heartbeat_counter_missing",
            "malformed_frames",
            "non_object_frames",
        ):
            if int(counts.get(evidence_key) or 0) > 0:
                reasons.append(
                    f"stream_integrity_failed:{spec.get('product_id')}:{evidence_key}"
                )
        if spec.get("channel") in {"market_trades", "level2"}:
            if int((analysis.get("counts") or {}).get("requested_channel_frames") or 0) == 0:
                reasons.append(f"no_{spec.get('channel')}_evidence:{spec.get('product_id')}")
        requested_channel = str(spec.get("channel") or "")
        if int(counts.get(f"sequence_observed:{requested_channel}") or 0) == 0:
            reasons.append(
                f"requested_channel_sequence_not_observed:{spec.get('product_id')}:{requested_channel}"
            )
        if int(counts.get(f"sequence_missing:{requested_channel}") or 0) > 0:
            reasons.append(
                f"requested_channel_sequence_missing:{spec.get('product_id')}:{requested_channel}"
            )
        if replay.get("ordering_or_checksum_errors"):
            reasons.append(f"replay_integrity_failed:{spec.get('product_id')}:{spec.get('channel')}")
        if not replay.get("content_fingerprint_equal"):
            reasons.append(f"replay_fingerprint_failed:{spec.get('product_id')}:{spec.get('channel')}")
        if spec.get("channel") == "level2":
            if int(row.get("deliberate_reconnect_count") or 0) == 0 or int(
                row.get("connection_count") or 0
            ) < 2:
                reasons.append(f"l2_deliberate_reconnect_not_proven:{spec.get('product_id')}")
            event_types = analysis.get("first_requested_event_type_by_epoch") or {}
            if not event_types or any(value != "snapshot" for value in event_types.values()):
                reasons.append(f"l2_resnapshot_not_proven:{spec.get('product_id')}")
            books = analysis.get("book_by_epoch") or {}
            if not books or any(
                not (book.get("checkpoint") or {}).get("valid")
                or int(book.get("update_before_snapshot_count") or 0) > 0
                or int(book.get("invalid_mutation_count") or 0) > 0
                for book in books.values()
            ):
                reasons.append(f"l2_book_validity_failed:{spec.get('product_id')}")
            if not replay.get("book_fingerprints_equal"):
                reasons.append(
                    f"l2_book_replay_fingerprint_failed:{spec.get('product_id')}"
                )
    rest_products = rest.get("products") or {}
    for product_id, operations in rest_products.items():
        for name, result in (operations or {}).items():
            if (result or {}).get("status") != "confirmed":
                reasons.append(f"rest_{name}_failed:{product_id}")
    cde_public_history = rest.get("cde_public_history") or {}
    if cde_public_history.get("status") not in {"unsupported", "admitted"}:
        reasons.append("cde_public_history_contract_unresolved")
    if require_production_capacity:
        if capacity.get("annual_archive_budget_pass") is None:
            reasons.append("operator_annual_archive_budget_required")
        elif capacity.get("annual_archive_budget_pass") is False:
            reasons.append("annual_archive_budget_exceeded")
    for product_id in ("BIP-20DEC30-CDE", "BTC-USD"):
        if product_id not in rest_products:
            reasons.append(f"required_rest_product_missing:{product_id}")
    bip_quantity = (
        ((quantity_semantics or {}).get("products") or {}).get("BIP-20DEC30-CDE") or {}
    )
    if bip_quantity.get("status") != "confirmed_contracts":
        reasons.append("futures_quantity_unit_not_proven:BIP-20DEC30-CDE")
    if (trade_side_semantics or {}).get("status") != "confirmed_maker_side":
        reasons.append("market_trade_maker_side_not_proven")
    unique_reasons = list(dict.fromkeys(reasons))
    return {
        "status": "blocked" if unique_reasons else "admitted",
        "reasons": unique_reasons,
        "scope": "BIP-20DEC30-CDE/BTC-USD only",
    }


def _phase1_implementation_readiness(
    streams: Sequence[Mapping[str, Any]],
    rest: Mapping[str, Any],
    capacity: Mapping[str, Any],
    *,
    duration_seconds: float,
    quantity_semantics: Mapping[str, Any] | None = None,
    trade_side_semantics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Authorize implementation only; this never authorizes collector enrollment."""

    return _phase1_admission(
        streams,
        rest,
        capacity,
        duration_seconds=duration_seconds,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
        require_production_capacity=False,
    )


def _trade_side_semantics(streams: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    observations: dict[str, Any] = {}
    required_products = ("BIP-20DEC30-CDE", "BTC-USD")
    for product_id in required_products:
        row = next(
            (
                candidate
                for candidate in streams
                if (candidate.get("spec") or {}).get("product_id") == product_id
                and (candidate.get("spec") or {}).get("channel") == "market_trades"
            ),
            None,
        )
        analysis = (row or {}).get("analysis") or {}
        sides = list((analysis.get("observed_semantics") or {}).get("trade_sides") or [])
        observations[product_id] = {
            "trade_count": int(analysis.get("trade_count") or 0),
            "observed_provider_side_values": sorted(str(side) for side in sides),
            "trade_schema_signatures": (analysis.get("schema_signatures") or {}).get(
                "trades"
            )
            or {},
        }
    captured = all(
        observation["trade_count"] > 0
        and observation["trade_schema_signatures"]
        and set(observation["observed_provider_side_values"]) <= {"BUY", "SELL"}
        and observation["observed_provider_side_values"]
        for observation in observations.values()
    )
    return {
        "status": "confirmed_maker_side" if captured else "unproven",
        "provider_field": "market_trades.events[].trades[].side",
        "provider_meaning": "maker_side",
        "preservation": "verbatim",
        "aggressor_transform": {
            "version": "coinbase_maker_to_aggressor.v1",
            "BUY": "SELL",
            "SELL": "BUY",
            "unknown": None,
        },
        "documentation": (
            "https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/"
            "websocket/websocket-channels#market-trades-channel"
        ),
        "observations": observations,
    }


def _quantity_semantics(
    rest: Mapping[str, Any],
    streams: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rest_products = rest.get("products") or {}
    decisions: dict[str, Any] = {}
    for product_id, expected in FUTURES_UNIT_EVIDENCE.items():
        if product_id not in rest_products:
            continue
        reasons: list[str] = []
        product_result = (rest_products.get(product_id) or {}).get("product") or {}
        payload = product_result.get("payload") if product_result.get("status") == "confirmed" else None
        if not isinstance(payload, Mapping):
            reasons.append("product_metadata_unavailable")
            payload = {}
        details = payload.get("future_product_details")
        if not isinstance(details, Mapping):
            reasons.append("future_product_details_unavailable")
            details = {}
        for field in ("contract_size", "contract_root_unit"):
            observed = details.get(field)
            wanted = expected[field]
            matches = (
                _decimal_equal(observed, wanted)
                if field == "contract_size"
                else str(observed or "").upper() == wanted
            )
            if not matches:
                reasons.append(f"{field}_mismatch")
        if not _decimal_equal(payload.get("base_increment"), expected["base_increment"]):
            reasons.append("base_increment_mismatch")

        trade_samples: list[str] = []
        l2_samples: list[str] = []
        for row in streams:
            spec = row.get("spec") or {}
            if spec.get("product_id") != product_id:
                continue
            observed = (row.get("analysis") or {}).get("observed_semantics") or {}
            if spec.get("channel") == "market_trades":
                trade_samples.extend(str(value) for value in observed.get("trade_size_samples") or [])
            elif spec.get("channel") == "level2":
                l2_samples.extend(str(value) for value in observed.get("l2_quantity_samples") or [])
        if not trade_samples:
            reasons.append("trade_size_samples_required")
        if not l2_samples:
            reasons.append("l2_quantity_samples_required")
        increment = expected["base_increment"]
        if trade_samples and not _all_decimal_multiples(trade_samples, increment):
            reasons.append("trade_sizes_not_contract_increment_multiples")
        if l2_samples and not _all_decimal_multiples(l2_samples, increment):
            reasons.append("l2_quantities_not_contract_increment_multiples")

        decisions[product_id] = {
            "status": "confirmed_contracts" if not reasons else "blocked",
            "reasons": reasons,
            "provider_size_unit": "contract" if not reasons else None,
            "contract_root_unit": expected["contract_root_unit"],
            "contract_size": expected["contract_size"],
            "contract_quantity_formula": "provider_size",
            "base_quantity_formula": "provider_size * contract_size",
            "quote_notional_formula": "price * provider_size * contract_size",
            "observed_trade_size_sample_count": len(trade_samples),
            "observed_l2_quantity_sample_count": len(l2_samples),
        }
    return {
        "schema_version": "coinbase_futures_quantity_semantics.v1",
        "published_contract_spec_source": FUTURES_CONTRACT_SPEC_SOURCE,
        "products": decisions,
    }


def _all_decimal_multiples(values: Sequence[str], increment: str) -> bool:
    try:
        divisor = Decimal(increment)
        return divisor > 0 and all(
            Decimal(value).is_finite()
            and Decimal(value) >= 0
            and Decimal(value) % divisor == 0
            for value in values
        )
    except (InvalidOperation, ValueError):
        return False


def _decimal_equal(left: Any, right: Any) -> bool:
    try:
        left_decimal = Decimal(str(left))
        right_decimal = Decimal(str(right))
    except (InvalidOperation, ValueError):
        return False
    return left_decimal.is_finite() and right_decimal.is_finite() and left_decimal == right_decimal


def _provider_error_payload(message: ProviderRawMessage) -> dict[str, str] | None:
    try:
        payload = json.loads(message.raw_frame)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    message_type = str(payload.get("type") or payload.get("channel") or "").strip().lower()
    if message_type != "error":
        return None
    return {
        "type": "error",
        "message": str(payload.get("message") or "unspecified provider error"),
    }


def _proof_implementation() -> dict[str, Any]:
    source_paths = {
        "proof_harness": Path(__file__).resolve(),
        "coinbase_stream": Path(inspect.getfile(CoinbaseAdvancedTradeStream)).resolve(),
        "coinbase_provider": Path(inspect.getfile(CoinbaseProvider)).resolve(),
    }
    return {
        "captured_at": _utc_iso(),
        "source_sha256": {
            label: hashlib.sha256(path.read_bytes()).hexdigest()
            for label, path in sorted(source_paths.items())
        },
        "runtime": {
            "python": sys.version.split()[0],
            "pyarrow": pa.__version__,
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
        },
    }


def _normalize_products(values: Sequence[str]) -> list[str]:
    products = list(dict.fromkeys(str(value or "").strip() for value in values if str(value or "").strip()))
    if not products:
        raise ValueError("At least one product_id is required.")
    unsupported = sorted(set(products) - ALLOWED_PRODUCTS)
    if unsupported:
        raise ValueError(
            "Market-structure proof product is outside the bounded allowlist: "
            + ", ".join(unsupported)
        )
    return products


def _normalize_channels(values: Sequence[str]) -> list[str]:
    channels = list(
        dict.fromkeys(_normalize_channel(value) for value in values if str(value or "").strip())
    )
    if not channels:
        raise ValueError("At least one proof channel is required.")
    unsupported = sorted(set(channels) - ALLOWED_CHANNELS)
    if unsupported:
        raise ValueError("Unsupported proof channel: " + ", ".join(unsupported))
    return channels


def _normalize_auth_mode(value: str) -> str:
    auth_mode = str(value or "public").strip().lower()
    if auth_mode not in {"public", "authenticated"}:
        raise ValueError("auth_mode must be 'public' or 'authenticated'")
    return auth_mode


def _normalize_channel(value: Any) -> str:
    channel = str(value or "unknown").strip().lower() or "unknown"
    return "level2" if channel == "l2_data" else channel


def _schema_signature(value: Mapping[str, Any]) -> str:
    return ",".join(sorted(str(key) for key in value))


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _valid_decimal(value: str) -> bool:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return False
    return parsed.is_finite()


def _percentile(values: Sequence[int], quantile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(int(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
    return ordered[index]


def _rate_summary(values: Sequence[int]) -> dict[str, int | None]:
    return {
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def _float_rate_summary(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(float(value) for value in values)

    def select(quantile: float) -> float:
        index = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
        return round(ordered[index], 6)

    return {
        "p50": select(0.50),
        "p95": select(0.95),
        "p99": select(0.99),
        "max": round(max(ordered), 6),
    }


def _safe_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-") or "unknown"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_output_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("logs") / "market-structure-proof" / stamp


__all__ = [
    "ALLOWED_CHANNELS",
    "ALLOWED_PRODUCTS",
    "PROOF_SCHEMA_VERSION",
    "ProofStreamSpec",
    "ProofCaptureSpec",
    "default_output_dir",
    "run_coinbase_market_structure_proof",
]

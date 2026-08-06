"""Bounded Coinbase market-trade acquisition and replay orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import socket
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

from data_providers.providers.factory import get_provider
from data_providers.streams.coinbase import (
    CoinbaseAdvancedTradeStream,
    CoinbaseMessageParser,
)
from data_providers.streams.contracts import (
    CanonicalMarketEvent,
    MarketSubscription,
    ProviderRawMessage,
)
from data_providers.streams.runtime import ContinuousStreamPolicy
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    SpoolBackpressureError,
    publish_compacted_raw_archives,
    publish_spool_archive,
    read_raw_archive_parquet,
    require_spool_capacity,
    spool_backlog_bytes,
)
from market_data.book_archive import (
    publish_book_checkpoint,
    read_book_checkpoint_parquet,
)
from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    SourceIdentity,
)
from market_data.market_state import (
    BASIS_FACT_TYPE,
    BASIS_FACT_VERSION,
    BBO_FACT_TYPE,
    BBO_FACT_VERSION,
    DEPTH_FACT_TYPE,
    DEPTH_FACT_VERSION,
    DERIVATIVE_STATE_FACT_TYPE,
    DERIVATIVE_STATE_FACT_VERSION,
    RESPONSE_FACT_TYPE,
    RESPONSE_FACT_VERSION,
    TRADE_FLOW_FEATURE_FACT_TYPE,
    TRADE_FLOW_FEATURE_FACT_VERSION,
    MarketStateValuationContract,
    derive_basis_features,
    derive_book_features,
    derive_derivative_state_features,
    derive_response_features,
    derive_trade_flow_feature,
)
from market_data.order_book import (
    L2_BOOK_FACT_TYPE,
    L2_BOOK_FACT_VERSION,
    BookLifecycle,
    BookQualityEvidence,
    BookSourcePosition,
    L2ProductContract,
    Level2BookReconstructor,
    checkpoint_canonical_rows,
    translate_coinbase_l2_event,
)
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    MARKET_TRADE_FACT_TYPE,
    MARKET_TRADE_FACT_VERSION,
    OrderingAssurance,
    PHASE1_COINBASE_TRADE_CONTRACTS,
    RawStreamRecord,
    TRADE_FLOW_FACT_TYPE,
    TRADE_FLOW_FACT_VERSION,
    TradeCoverageIntervalVersion,
    aggregate_trade_bucket,
    bucket_start_for,
    translate_coinbase_market_trade,
)
from engines.bot_runtime.core.book_execution import (
    EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
    ExecutionBookSourceReference,
    ExecutionBookTape,
    ExecutionBookTapeBundle,
    ExecutionBookValidityClosure,
)

from ..storage.repos.market_data import market_data_repo
from ..storage.repos.market_structure import (
    MarketTradeConflictError,
    PostgresMarketStructureRepository,
    StreamClaim,
    market_structure_repository,
)
from .instrument_service import get_instrument_record, resolve_or_create_instrument


logger = logging.getLogger(__name__)

PHASE1_PROOF_EFFECTIVE_AT = datetime(2026, 8, 2, tzinfo=UTC)
PHASE1_AUTHENTICATED_PROOF_SHA256 = (
    "81fe668956a794aada0821ee83d202938c5b8ed3c0d1ffb3e83219e83cead032"
)
DEFAULT_SPOOL_BYTES = 8 * 1024**3
DEFAULT_SEGMENT_BYTES = 128 * 1024**2
DEFAULT_STORAGE_ROOT = Path("logs/market-structure")
MAX_ANALYZER_SEQUENCE_HASHES = 8192


@dataclass(frozen=True)
class PairDefinition:
    pair_id: str
    futures_instrument_id: str
    futures_product_id: str
    spot_product_id: str


PHASE1_PAIRS: Mapping[str, PairDefinition] = {
    "bip_btc": PairDefinition(
        pair_id="bip_btc",
        futures_instrument_id="b2deb0a0-f292-408a-876d-3dadd8e3819b",
        futures_product_id="BIP-20DEC30-CDE",
        spot_product_id="BTC-USD",
    ),
    "etp_eth": PairDefinition(
        pair_id="etp_eth",
        futures_instrument_id="44226144-fb38-4566-92c4-580734d76d3c",
        futures_product_id="ETP-20DEC30-CDE",
        spot_product_id="ETH-USD",
    ),
    "slp_sol": PairDefinition(
        pair_id="slp_sol",
        futures_instrument_id="bead556e-22e2-4ac0-8ee0-0d8c5310e9a0",
        futures_product_id="SLP-20DEC30-CDE",
        spot_product_id="SOL-USD",
    ),
}


@dataclass(frozen=True)
class CapturedEvent:
    event: CanonicalMarketEvent
    raw_record: RawStreamRecord


@dataclass(frozen=True)
class CaptureAnalysis:
    raw_record_count: int
    raw_bytes: int
    trade_event_count: int
    snapshot_trade_count: int
    update_trade_count: int
    first_sequence_num: Optional[int]
    last_sequence_num: Optional[int]
    subscription_acknowledged: bool
    heartbeat_healthy: bool
    snapshot_accepted: bool
    coverage_opening_ordinal: Optional[int]
    coverage_opening_effective_at: Optional[datetime]
    coverage_first_sequence_num: Optional[int]
    coverage_last_ordinal: Optional[int]
    coverage_last_effective_at: Optional[datetime]
    coverage_last_sequence_num: Optional[int]
    quality_events: tuple[Mapping[str, Any], ...]


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _build_execution_book_tape_from_replay(
    *,
    states: Sequence[Any],
    closing_validity: Sequence[Any],
    instrument_id: str,
    replay_fingerprint: str,
    trade_records: Sequence[Any] = (),
) -> ExecutionBookTape:
    """Project certified provider-neutral replay facts into an execution tape."""

    normalized_instrument_id = str(instrument_id or "").strip()
    if not normalized_instrument_id:
        raise ValueError("market_book_replay_invalid: execution instrument id is empty")
    if not states:
        raise RuntimeError(
            "market_book_replay_invalid: no causal book states are available for execution"
        )
    closures = tuple(
        ExecutionBookValidityClosure(
            validity_interval_id=row.interval_id,
            status=str(getattr(row.status, "value", row.status)),
            known_at=row.known_at,
            reason=str(row.reason or getattr(row.status, "value", row.status)),
            source_reference=ExecutionBookSourceReference(
                definition_id=row.closing_position.definition_id,
                session_id=row.closing_position.session_id,
                connection_epoch=row.closing_position.connection_epoch,
                source_product_id=row.closing_position.provider_product_id,
                source_sequence=row.closing_position.provider_sequence_num,
                receive_ordinal=row.closing_position.receive_ordinal,
                event_ordinal=row.closing_position.event_ordinal,
            ),
            evidence_hash=str(row.closing_quality_hash or row.version_id),
        )
        for row in closing_validity
    )
    limitations = {
        "aggregated_depth_only",
        "exact_queue_position_unavailable",
    }
    if trade_records:
        limitations.add("passive_queue_requires_explicit_bounded_policy")
    else:
        limitations.add("resting_order_execution_not_modeled")
    return ExecutionBookTape.from_book_states(
        states,
        instrument_id=normalized_instrument_id,
        replay_fingerprint=replay_fingerprint,
        source_capability="l2",
        replay_certified=True,
        limitations=tuple(sorted(limitations)),
        validity_closures=closures,
        trade_records=trade_records,
    )


def _observed_channel(raw_frame: bytes) -> str:
    try:
        payload = json.loads(raw_frame)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "decode_error"
    if not isinstance(payload, Mapping):
        return "decode_error"
    return str(payload.get("channel") or payload.get("type") or "unknown").strip().lower()


def _instrument_decimal(record: Mapping[str, Any], key: str) -> Optional[Decimal]:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    raw = fields.get(key)
    if raw in (None, ""):
        return None
    return Decimal(str(raw))


def _utc_time(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"market_feature_materialization_invalid: {field_name} must be datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class MarketStructureService:
    """Coordinates Phase 1 without creating a second credential or data plane."""

    def __init__(
        self,
        *,
        repository: PostgresMarketStructureRepository = market_structure_repository,
        stream_factory: Callable[..., CoinbaseAdvancedTradeStream] = CoinbaseAdvancedTradeStream,
    ) -> None:
        self.repository = repository
        self.stream_factory = stream_factory

    def configure_pair(
        self,
        *,
        pair_id: str,
        auth_mode: str = "authenticated",
        max_spool_bytes: int = DEFAULT_SPOOL_BYTES,
        max_segment_bytes: int = DEFAULT_SEGMENT_BYTES,
        enable_production: bool = False,
    ) -> dict[str, Any]:
        normalized_pair = str(pair_id or "").strip().lower()
        pair = PHASE1_PAIRS.get(normalized_pair)
        if pair is None:
            raise ValueError(
                f"market_structure_pair_invalid: allowed={','.join(PHASE1_PAIRS)}"
            )
        if enable_production:
            raise ValueError(
                "market_stream_production_not_admitted: the implemented-path 24-hour proof and explicit budget are deferred until after Phase 4"
            )
        futures = get_instrument_record(pair.futures_instrument_id)
        if str(futures.get("symbol")) != pair.futures_product_id:
            raise RuntimeError(
                "market_structure_catalog_conflict: futures instrument/product mismatch"
            )
        spot, error = resolve_or_create_instrument(
            "COINBASE",
            "COINBASE_DIRECT",
            pair.spot_product_id,
            provider_id="COINBASE",
            venue_id="COINBASE_DIRECT",
        )
        if error or not spot:
            raise RuntimeError(
                f"market_structure_spot_registration_failed: product_id={pair.spot_product_id} error={error}"
            )
        trade_source = SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="stream",
            adapter_version="coinbase_advanced_trade.market_trades.v1",
        )
        source_id = market_data_repo.register_source(
            trade_source,
            lineage={
                "schema_version": "market_structure_source_lineage.v1",
                "provider_surface": "Coinbase Advanced Trade WebSocket",
                "channels": ["market_trades", "heartbeats"],
                "phase0_proof_sha256": PHASE1_AUTHENTICATED_PROOF_SHA256,
            },
        )
        l2_source_id = market_data_repo.register_source(
            SourceIdentity(
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                source_kind="stream",
                adapter_version="coinbase_advanced_trade.level2.v1",
            ),
            lineage={
                "schema_version": "market_structure_source_lineage.v1",
                "provider_surface": "Coinbase Advanced Trade WebSocket",
                "channels": ["level2", "heartbeats"],
                "phase0_proof_sha256": PHASE1_AUTHENTICATED_PROOF_SHA256,
                "ordering_scope": "one product per connection epoch",
            },
        )
        instruments = (
            (futures, pair.futures_product_id, "future"),
            (spot, pair.spot_product_id, "spot"),
        )
        definitions: list[dict[str, Any]] = []
        series_catalog: list[dict[str, Any]] = []
        for instrument, product_id, product_type in instruments:
            instrument_id = str(instrument["id"])
            trade_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=MARKET_TRADE_FACT_TYPE,
                timeframe_seconds=None,
                contract_version=MARKET_TRADE_FACT_VERSION,
            )
            aggregate_series_ids = {
                interval: market_data_repo.register_series(
                    instrument_id=instrument_id,
                    fact_type=TRADE_FLOW_FACT_TYPE,
                    timeframe_seconds=interval,
                    contract_version=TRADE_FLOW_FACT_VERSION,
                )
                for interval in (1, 60)
            }
            l2_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=L2_BOOK_FACT_TYPE,
                timeframe_seconds=None,
                contract_version=L2_BOOK_FACT_VERSION,
            )
            bbo_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=BBO_FACT_TYPE,
                timeframe_seconds=1,
                contract_version=BBO_FACT_VERSION,
            )
            depth_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=DEPTH_FACT_TYPE,
                timeframe_seconds=1,
                contract_version=DEPTH_FACT_VERSION,
            )
            flow_feature_series_ids = {
                interval: market_data_repo.register_series(
                    instrument_id=instrument_id,
                    fact_type=TRADE_FLOW_FEATURE_FACT_TYPE,
                    timeframe_seconds=interval,
                    contract_version=TRADE_FLOW_FEATURE_FACT_VERSION,
                )
                for interval in (1, 60)
            }
            response_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=RESPONSE_FACT_TYPE,
                timeframe_seconds=1,
                contract_version=RESPONSE_FACT_VERSION,
            )
            basis_series_id = (
                market_data_repo.register_series(
                    instrument_id=instrument_id,
                    fact_type=BASIS_FACT_TYPE,
                    timeframe_seconds=1,
                    contract_version=BASIS_FACT_VERSION,
                )
                if product_type == "future"
                else None
            )
            derivative_state_series_id = (
                market_data_repo.register_series(
                    instrument_id=instrument_id,
                    fact_type=DERIVATIVE_STATE_FACT_TYPE,
                    timeframe_seconds=None,
                    contract_version=DERIVATIVE_STATE_FACT_VERSION,
                )
                if product_type == "future"
                else None
            )
            contract = PHASE1_COINBASE_TRADE_CONTRACTS[product_id]
            self.repository.register_product_definition(
                definition_version_id=contract.product_definition_version_id,
                source_id=source_id,
                instrument_id=instrument_id,
                provider_product_id=product_id,
                product_type=product_type,
                venue="COINBASE_DIRECT",
                status="online_phase0_proven",
                base_currency=contract.base_currency,
                quote_currency=contract.quote_currency,
                provider_size_unit=contract.provider_size_unit.value,
                contract_size=contract.contract_size,
                price_increment=_instrument_decimal(instrument, "tick_size"),
                base_increment=_instrument_decimal(instrument, "qty_step"),
                effective_at=PHASE1_PROOF_EFFECTIVE_AT,
                received_at=datetime.now(UTC),
                provenance={
                    "phase0_proof_sha256": PHASE1_AUTHENTICATED_PROOF_SHA256,
                    "proof_contract": "market_structure_phase0_proof.v3",
                    "quantity_semantics": "phase0_proven",
                },
            )
            definition_id = f"ms_coinbase_{product_id.lower().replace('-', '_')}"
            definition = self.repository.upsert_stream_definition(
                definition_id=definition_id,
                source_id=source_id,
                series_id=trade_series_id,
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                provider_product_id=product_id,
                channels=("market_trades", "heartbeats"),
                auth_mode=auth_mode,
                contract_version=MARKET_TRADE_FACT_VERSION,
                max_spool_bytes=max_spool_bytes,
                max_segment_bytes=max_segment_bytes,
                enabled=None,
                production_admitted=None,
                config={
                    "schema_version": "market_structure_stream_config.v1",
                    "pair_id": normalized_pair,
                    "aggregate_series_ids": {
                        str(key): value for key, value in aggregate_series_ids.items()
                    },
                    "flow_feature_series_ids": {
                        str(key): value for key, value in flow_feature_series_ids.items()
                    },
                    "response_feature_series_id": response_series_id,
                    "product_definition_version_id": contract.product_definition_version_id,
                    "production_blocker": "post_phase4_24h_capacity_and_budget_gate",
                },
            )
            definitions.append(definition)
            l2_definition_id = (
                f"ms_coinbase_l2_{product_id.lower().replace('-', '_')}"
            )
            l2_definition = self.repository.upsert_stream_definition(
                definition_id=l2_definition_id,
                source_id=l2_source_id,
                series_id=l2_series_id,
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                provider_product_id=product_id,
                channels=("level2", "heartbeats"),
                auth_mode=auth_mode,
                contract_version=L2_BOOK_FACT_VERSION,
                max_spool_bytes=max_spool_bytes,
                max_segment_bytes=max_segment_bytes,
                enabled=None,
                production_admitted=None,
                config={
                    "schema_version": "market_structure_l2_stream_config.v1",
                    "pair_id": normalized_pair,
                    "product_definition_version_id": contract.product_definition_version_id,
                    "provider_size_unit": contract.provider_size_unit.value,
                    "price_increment": (
                        str(_instrument_decimal(instrument, "tick_size"))
                        if _instrument_decimal(instrument, "tick_size") is not None
                        else None
                    ),
                    "bbo_series_id": bbo_series_id,
                    "depth_series_id": depth_series_id,
                    "response_feature_series_id": response_series_id,
                    "trade_series_id": trade_series_id,
                    "flow_feature_series_ids": {
                        str(key): value for key, value in flow_feature_series_ids.items()
                    },
                    "base_currency": contract.base_currency,
                    "quote_currency": contract.quote_currency,
                    "contract_size": (
                        str(contract.contract_size)
                        if contract.contract_size is not None
                        else None
                    ),
                    "quantity_increment": (
                        "1"
                        if contract.provider_size_unit.value == "contracts"
                        else (
                            str(_instrument_decimal(instrument, "qty_step"))
                            if _instrument_decimal(instrument, "qty_step") is not None
                            else None
                        )
                    ),
                    "checkpoint_max_seconds": 300,
                    "checkpoint_max_mutations": 100000,
                    "production_blocker": "post_phase4_24h_capacity_and_budget_gate",
                },
            )
            definitions.append(l2_definition)
            series_catalog.append(
                {
                    "instrument_id": instrument_id,
                    "product_id": product_id,
                    "trade_series_id": trade_series_id,
                    "aggregate_series_ids": aggregate_series_ids,
                    "l2_series_id": l2_series_id,
                    "bbo_series_id": bbo_series_id,
                    "depth_series_id": depth_series_id,
                    "flow_feature_series_ids": flow_feature_series_ids,
                    "response_feature_series_id": response_series_id,
                    "basis_series_id": basis_series_id,
                    "derivative_state_series_id": derivative_state_series_id,
                    "valuation_contract_hash": MarketStateValuationContract(
                        product_definition_version_id=contract.product_definition_version_id,
                        provider_size_unit=contract.provider_size_unit,
                        base_currency=contract.base_currency,
                        quote_currency=contract.quote_currency,
                        contract_size=contract.contract_size,
                    ).material_hash,
                }
            )
        mapping_id = self.repository.register_instrument_mapping(
            primary_instrument_id=str(futures["id"]),
            related_instrument_id=str(spot["id"]),
            role="spot_reference",
            effective_from=PHASE1_PROOF_EFFECTIVE_AT,
            mapping_reason="operator-approved Phase 1 futures/spot pair",
            mapping_source="market_structure_phase0_proof.v3",
        )
        return {
            "schema_version": "market_structure_pair_configuration.v1",
            "pair_id": normalized_pair,
            "mapping_id": mapping_id,
            "definitions": definitions,
            "series": series_catalog,
            "production_admitted": False,
            "production_blockers": [
                "post_phase4_24h_implemented_path_capture",
                "explicit_storage_and_cost_budget",
            ],
        }

    def materialize_pair_features(
        self,
        *,
        pair_id: str,
        start: datetime,
        end: datetime,
        known_at: datetime,
    ) -> dict[str, Any]:
        """Materialize cross-stream Phase 3 facts at one causal commit watermark."""

        start_at = _utc_time(start, field_name="start")
        end_at = _utc_time(end, field_name="end")
        decision_time = _utc_time(known_at, field_name="known_at")
        if end_at <= start_at:
            raise ValueError(
                "market_feature_materialization_invalid: end must follow start"
            )
        configured = self.configure_pair(pair_id=pair_id)
        pair = PHASE1_PAIRS[str(configured["pair_id"])]
        series_by_product = {
            str(row["product_id"]): dict(row) for row in configured["series"]
        }
        futures = series_by_product[pair.futures_product_id]
        spot = series_by_product[pair.spot_product_id]

        source_series = market_data_repo.list_series(
            instrument_id=pair.futures_instrument_id
        )
        oi_series = [
            row
            for row in source_series
            if row["fact_type"] == OPEN_INTEREST_FACT_TYPE
            and row["contract_version"] == OPEN_INTEREST_FACT_VERSION
        ]
        funding_series = [
            row
            for row in source_series
            if row["fact_type"] == FUNDING_RATE_FACT_TYPE
            and row["contract_version"] == FUNDING_RATE_FACT_VERSION
        ]
        if len(oi_series) > 1 or len(funding_series) > 1:
            raise RuntimeError(
                "market_feature_materialization_ambiguous: multiple canonical OI/funding series"
            )
        lookback_start = start_at - timedelta(seconds=60)
        source_commit_seq = self.repository.cross_stream_input_commit_seq(
            futures_bbo_series_id=int(futures["bbo_series_id"]),
            spot_bbo_series_id=int(spot["bbo_series_id"]),
            oi_series_id=int(oi_series[0]["id"]) if oi_series else None,
            funding_series_id=(
                int(funding_series[0]["id"]) if funding_series else None
            ),
            start=lookback_start,
            end=end_at,
            known_at=decision_time,
        )

        futures_bbo = self.repository.read_bbo_features(
            series_id=int(futures["bbo_series_id"]),
            start=start_at,
            end=end_at,
            known_at=decision_time,
            as_of_commit_seq=source_commit_seq,
        )
        spot_bbo = self.repository.read_bbo_features(
            series_id=int(spot["bbo_series_id"]),
            start=start_at - timedelta(seconds=2),
            end=end_at,
            known_at=decision_time,
            as_of_commit_seq=source_commit_seq,
        )
        basis_facts = derive_basis_features(
            futures_bbo,
            spot_bbo,
            mapping_id=str(configured["mapping_id"]),
            computed_at=decision_time,
            series_id=int(futures["basis_series_id"]),
        )

        oi_records = (
            market_data_repo.read_open_interest(
                series_id=int(oi_series[0]["id"]),
                start=lookback_start,
                end=end_at,
                as_of_commit_seq=source_commit_seq,
                known_at_lte=decision_time,
            )
            if oi_series
            else []
        )
        funding_records = (
            market_data_repo.read_funding_rates(
                series_id=int(funding_series[0]["id"]),
                start=lookback_start,
                end=end_at,
                as_of_commit_seq=source_commit_seq,
                known_at_lte=decision_time,
            )
            if funding_series
            else []
        )
        oi_gaps = (
            market_data_repo.list_gap_evidence(
                series_id=int(oi_series[0]["id"]),
                start=lookback_start,
                end=end_at,
                as_of_commit_seq=source_commit_seq,
            )
            if oi_series
            else []
        )
        derivative_facts = tuple(
            fact
            for fact in derive_derivative_state_features(
                instrument_id=pair.futures_instrument_id,
                oi_records=oi_records,
                funding_records=funding_records,
                oi_gaps=oi_gaps,
                series_id=int(futures["derivative_state_series_id"]),
                expected_oi_interval_seconds=60,
                computed_at=decision_time,
            )
            if start_at <= fact.effective_at < end_at
        )
        outcome = self.repository.ingest_market_state_features(
            basis_facts=basis_facts,
            derivative_facts=derivative_facts,
        )
        execution_trade_records = ()
        if replay_states and config.get("trade_series_id") is not None:
            execution_trade_records = tuple(
                self.repository.read_trades(
                    series_id=int(config["trade_series_id"]),
                    start=min(row.effective_at for row in replay_states) - timedelta(seconds=2),
                    end=max(row.effective_at for row in replay_states) + timedelta(seconds=2),
                    known_at_lte=max(row.known_at for row in replay_states),
                )
            )
        fingerprint = _stable_hash(
            {
                "schema_version": "market.cross_stream_materialization.v1",
                "pair_id": pair.pair_id,
                "start": start_at.isoformat(),
                "end": end_at.isoformat(),
                "known_at": decision_time.isoformat(),
                "source_commit_seq": source_commit_seq,
                "basis_material_hashes": [
                    row.material_hash for row in basis_facts
                ],
                "derivative_material_hashes": [
                    row.material_hash for row in derivative_facts
                ],
            }
        )
        return {
            "schema_version": "market.cross_stream_materialization.v1",
            "pair_id": pair.pair_id,
            "start": start_at.isoformat(),
            "end": end_at.isoformat(),
            "known_at": decision_time.isoformat(),
            "source_commit_seq": source_commit_seq,
            "basis_count": len(basis_facts),
            "derivative_state_count": len(derivative_facts),
            "oi_series_id": int(oi_series[0]["id"]) if oi_series else None,
            "funding_series_id": (
                int(funding_series[0]["id"]) if funding_series else None
            ),
            "inserted": outcome.inserted_count,
            "noop": outcome.noop_count,
            "max_commit_seq": outcome.max_commit_seq,
            "materialization_fingerprint": fingerprint,
        }

    def start_continuous_validation(
        self,
        *,
        definition_id: str,
        duration_seconds: float,
        requested_by: str,
        policy: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        duration = float(duration_seconds)
        if not 60 <= duration <= 7 * 24 * 3600:
            raise ValueError(
                "market_stream_validation_invalid: duration must be 60..604800 seconds"
            )
        runtime_policy = ContinuousStreamPolicy.from_mapping(policy)
        stop_at = datetime.now(UTC) + timedelta(seconds=duration)
        row = self.repository.configure_continuous_runtime(
            definition_id=definition_id,
            enabled=True,
            mode="validation",
            requested_by=requested_by,
            policy=runtime_policy.to_dict(),
            stop_at=stop_at,
        )
        return {
            "schema_version": "market.continuous_collector_control.v1",
            "definition_id": str(row["id"]),
            "enabled": bool(row["enabled"]),
            "production_admitted": bool(row["production_admitted"]),
            "mode": "validation",
            "stop_at": stop_at.isoformat(),
            "policy": runtime_policy.to_dict(),
        }

    def start_continuous_production(
        self,
        *,
        definition_id: str,
        requested_by: str,
        policy: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        runtime_policy = ContinuousStreamPolicy.from_mapping(policy)
        row = self.repository.configure_continuous_runtime(
            definition_id=definition_id,
            enabled=True,
            mode="production",
            requested_by=requested_by,
            policy=runtime_policy.to_dict(),
        )
        return {
            "schema_version": "market.continuous_collector_control.v1",
            "definition_id": str(row["id"]),
            "enabled": bool(row["enabled"]),
            "production_admitted": bool(row["production_admitted"]),
            "mode": "production",
            "stop_at": None,
            "policy": runtime_policy.to_dict(),
        }

    def stop_continuous(
        self,
        *,
        definition_id: str,
        requested_by: str,
    ) -> dict[str, Any]:
        row = self.repository.configure_continuous_runtime(
            definition_id=definition_id,
            enabled=False,
            mode="stopped",
            requested_by=requested_by,
            policy={},
        )
        return {
            "schema_version": "market.continuous_collector_control.v1",
            "definition_id": str(row["id"]),
            "enabled": bool(row["enabled"]),
            "production_admitted": bool(row["production_admitted"]),
            "mode": "stopped",
        }

    def set_production_admission(
        self,
        *,
        definition_id: str,
        admitted: bool,
        approved_by: str,
        evidence: Mapping[str, Any],
        storage_budget: Mapping[str, Any],
    ) -> dict[str, Any]:
        evidence_payload = dict(evidence or {})
        budget_payload = dict(storage_budget or {})
        if admitted:
            validation_session_id = str(
                evidence_payload.get("validation_session_id") or ""
            ).strip()
            if not validation_session_id:
                raise ValueError(
                    "market_stream_admission_invalid: validation_session_id is required"
                )
            evidence_payload = self.repository.continuous_validation_evidence(
                definition_id=definition_id,
                session_id=validation_session_id,
            )
            if not evidence_payload["continuous_capture_completed"]:
                raise ValueError(
                    "market_stream_admission_invalid: canonical validation blockers="
                    + ",".join(evidence_payload["blockers"])
                )
            required_budget_fields = {
                "capacity_resource_id",
                "capacity_scope",
                "capacity_authority",
                "physical_host_visible",
                "capacity_observed_at",
                "observed_available_bytes",
                "observed_growth_bytes_per_day",
                "growth_budget_bytes_per_day",
                "minimum_headroom_bytes",
            }
            missing = sorted(required_budget_fields - set(budget_payload))
            if missing:
                raise ValueError(
                    "market_stream_admission_invalid: storage budget fields missing="
                    + ",".join(missing)
                )
            authority = str(budget_payload["capacity_authority"] or "").strip()
            if authority not in {
                "physical_host_filesystem",
                "engine_storage_filesystem",
                "cloud_volume",
            } or not bool(budget_payload["physical_host_visible"]):
                raise ValueError(
                    "market_stream_admission_invalid: authoritative physical/cloud storage capacity is required; virtual guest capacity is insufficient"
                )
            observed_available = int(budget_payload["observed_available_bytes"])
            observed_growth = float(
                budget_payload["observed_growth_bytes_per_day"]
            )
            growth_budget = float(budget_payload["growth_budget_bytes_per_day"])
            minimum_headroom = int(budget_payload["minimum_headroom_bytes"])
            if (
                observed_available < minimum_headroom
                or observed_growth < 0
                or growth_budget <= 0
                or observed_growth > growth_budget
                or minimum_headroom <= 0
            ):
                raise ValueError(
                    "market_stream_admission_invalid: observed capacity/growth exceeds the explicit storage budget"
                )
        row = self.repository.set_production_admission(
            definition_id=definition_id,
            admitted=admitted,
            approved_by=approved_by,
            evidence=evidence_payload,
            storage_budget=budget_payload,
        )
        return {
            "schema_version": "market.stream_production_admission.v1",
            "definition_id": str(row["id"]),
            "enabled": bool(row["enabled"]),
            "production_admitted": bool(row["production_admitted"]),
            "admission": dict(row["config"] or {}).get("production_admission"),
        }

    async def capture_bounded(
        self,
        *,
        definition_id: str,
        duration_seconds: float,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        owner_id: Optional[str] = None,
    ) -> dict[str, Any]:
        duration = float(duration_seconds)
        if not 1 <= duration <= 3600:
            raise ValueError("market_structure_capture_invalid: duration must be 1..3600 seconds")
        owner = str(owner_id or f"qt:{socket.gethostname()}:{os.getpid()}")
        lease_seconds = max(90.0, duration + 300.0)
        claim = self.repository.claim_stream(
            definition_id=definition_id,
            owner_id=owner,
            lease_seconds=lease_seconds,
            bounded=True,
        )
        if claim.provider != "COINBASE" or claim.venue != "COINBASE_DIRECT":
            with contextlib.suppress(Exception):
                self.repository.release(claim)
            raise ValueError("market_structure_capture_invalid: unsupported provider/venue")
        storage = Path(storage_root).expanduser().resolve()
        spool_root = storage / "spool"
        object_store = FilesystemRawArchiveObjectStore(storage / "objects")
        temporary_root = storage / "tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        started_monotonic = time.monotonic()
        started_at = datetime.now(UTC)
        session_event_ordinal = 0
        stream: CoinbaseAdvancedTradeStream | None = None
        pending: asyncio.Task[ProviderRawMessage] | None = None
        segments: list[DurableRawSpoolSegment] = []
        current_segment: DurableRawSpoolSegment | None = None
        captured: list[CapturedEvent] = []
        raw_records: list[RawStreamRecord] = []
        primary_channel = "level2" if "level2" in claim.channels else "market_trades"
        analysis_state = _CaptureAnalyzer(claim, primary_channel=primary_channel)
        heartbeat_deadline = time.monotonic() + min(30.0, lease_seconds / 3.0)
        try:
            provider = get_provider("COINBASE", venue="COINBASE_DIRECT")
            jwt_factory = (
                provider.build_websocket_jwt
                if claim.auth_mode == "authenticated"
                else None
            )
            stream = self.stream_factory(
                jwt_factory=jwt_factory,
                stream_session_id=claim.session_id,
            )
            await stream.connect()
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="connected",
                occurred_at=datetime.now(UTC),
                evidence={"bounded": True, "duration_seconds": duration},
            )
            session_event_ordinal += 1
            await stream.subscribe(
                [
                    MarketSubscription.from_values(
                        provider="COINBASE",
                        venue="COINBASE_DIRECT",
                        symbol=claim.provider_product_id,
                        product_id=claim.provider_product_id,
                        channels=claim.channels,
                        auth_mode=claim.auth_mode,
                    )
                ]
            )
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="subscription_sent",
                occurred_at=datetime.now(UTC),
                evidence={
                    "product_id": claim.provider_product_id,
                    "channels": list(claim.channels),
                    "auth_mode": claim.auth_mode,
                },
            )
            session_event_ordinal += 1
            parser = CoinbaseMessageParser(
                symbol_by_product_id={claim.provider_product_id: claim.provider_product_id}
            )
            iterator = stream.raw_messages().__aiter__()
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if pending is None:
                    pending = asyncio.create_task(anext(iterator))
                done, _ = await asyncio.wait({pending}, timeout=min(remaining, 1.0))
                if not done:
                    if time.monotonic() >= heartbeat_deadline:
                        self.repository.heartbeat(claim, lease_seconds=lease_seconds)
                        heartbeat_deadline = time.monotonic() + min(30.0, lease_seconds / 3.0)
                    analysis_state.observe_idle(datetime.now(UTC))
                    continue
                try:
                    message = pending.result()
                except StopAsyncIteration as exc:
                    analysis_state.unexpected_disconnect(datetime.now(UTC))
                    raise RuntimeError("market_stream_disconnected_before_bounded_deadline") from exc
                finally:
                    pending = None
                observed_channel = _observed_channel(message.raw_frame)
                estimated_spool_bytes = len(message.raw_frame) * 2 + 4096
                require_spool_capacity(
                    root=spool_root,
                    max_backlog_bytes=claim.max_spool_bytes,
                    next_frame_bytes=estimated_spool_bytes,
                    definition_id=claim.definition_id,
                )
                if (
                    current_segment is None
                    or (
                        current_segment.record_count > 0
                        and current_segment.current_bytes + estimated_spool_bytes
                        > claim.max_segment_bytes
                    )
                ):
                    if current_segment is not None:
                        current_segment.seal()
                    current_segment = DurableRawSpoolSegment(
                        root=spool_root,
                        definition_id=claim.definition_id,
                        session_id=claim.session_id,
                        connection_epoch=message.connection_epoch,
                        segment_ordinal=len(segments),
                    )
                    segments.append(current_segment)
                record = RawStreamRecord.from_provider_message(
                    message,
                    definition_id=claim.definition_id,
                    spool_segment_id=current_segment.spool_segment_id,
                    provider_product_id=claim.provider_product_id,
                    requested_channel=primary_channel,
                    observed_channel=observed_channel,
                )
                current_segment.append(record)
                raw_records.append(record)
                events = parser.parse_raw(
                    message.raw_frame,
                    received_at=message.received_at,
                    raw_ref={
                        **message.evidence_ref(),
                        "raw_record_id": record.raw_record_id,
                        "spool_segment_id": record.spool_segment_id,
                    },
                )
                analysis_state.observe(record, events)
                captured.extend(CapturedEvent(event=event, raw_record=record) for event in events)
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await pending
                pending = None
            await stream.close()
            stream = None
            stopped_at = datetime.now(UTC)
            analysis_state.observe_idle(stopped_at)
            bounded_stop_event_id = self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="bounded_capture_stopped",
                occurred_at=stopped_at,
                reason="requested_duration_elapsed",
                evidence={"raw_record_count": len(raw_records)},
            )
            session_event_ordinal += 1
            if not raw_records:
                raise RuntimeError("market_structure_capture_empty: no provider frames received")
            if current_segment is not None and current_segment.record_count:
                current_segment.seal()
            self.repository.heartbeat(claim, lease_seconds=lease_seconds)
            manifest_ids: list[str] = []
            for segment in segments:
                if segment.record_count <= 0:
                    continue
                encoded, acknowledgement, segment_records = publish_spool_archive(
                    segment,
                    object_store=object_store,
                    temporary_directory=temporary_root,
                )
                commit = self.repository.commit_archive(
                    claim,
                    encoded=encoded,
                    acknowledgement=acknowledgement,
                    records=segment_records,
                )
                segment.mark_database_acknowledged(
                    manifest_id=commit.manifest_id,
                    object_key=acknowledgement.object_key,
                    object_sha256=acknowledgement.sha256,
                )
                segment.discard_acknowledged_spool()
                manifest_ids.append(commit.manifest_id)
                self.repository.heartbeat(claim, lease_seconds=lease_seconds)

            analysis = analysis_state.finalize()
            if primary_channel == "level2":
                return self._finalize_level2_capture(
                    claim=claim,
                    captured=captured,
                    raw_records=raw_records,
                    analysis=analysis,
                    manifest_ids=manifest_ids,
                    object_store=object_store,
                    temporary_root=temporary_root,
                    storage_root=storage,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    bounded_stop_event_id=bounded_stop_event_id,
                    session_event_ordinal=session_event_ordinal,
                    segment_count=len(segments),
                )
            coverage_interval_id = _stable_hash(
                {
                    "schema_version": "market.trade_coverage_interval_id.v1",
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                    "connection_epoch": 0,
                    "product_id": claim.provider_product_id,
                    "channel": "market_trades",
                }
            )
            quality_event_ids: list[str] = []
            invalidating_quality_ids: list[str] = []
            for quality in analysis.quality_events:
                quality_id = self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=int(quality["receive_ordinal"]),
                    channel=str(quality["channel"]),
                    classification=str(quality["classification"]),
                    reason=str(quality["reason"]),
                    detected_at=quality["detected_at"],
                    raw_record_id=quality.get("raw_record_id"),
                    coverage_interval_id=coverage_interval_id,
                    sequence_before=quality.get("sequence_before"),
                    sequence_after=quality.get("sequence_after"),
                    evidence=quality.get("evidence"),
                )
                quality_event_ids.append(quality_id)
                if bool(quality.get("invalidating")):
                    invalidating_quality_ids.append(quality_id)

            coverage_opened = (
                analysis.subscription_acknowledged
                and analysis.heartbeat_healthy
                and analysis.snapshot_accepted
                and analysis.coverage_opening_ordinal is not None
                and analysis.coverage_opening_effective_at is not None
                and analysis.coverage_last_ordinal is not None
                and analysis.coverage_last_effective_at is not None
            )
            opening_event_id: Optional[str] = None
            coverage: Optional[TradeCoverageIntervalVersion] = None
            if coverage_opened:
                opening_record = next(
                    record
                    for record in raw_records
                    if record.receive_ordinal == analysis.coverage_opening_ordinal
                )
                last_record = next(
                    record
                    for record in reversed(raw_records)
                    if record.receive_ordinal == analysis.coverage_last_ordinal
                )
                opening_event_id = self.repository.append_session_event(
                    claim,
                    event_ordinal=session_event_ordinal,
                    connection_epoch=0,
                    event_type="trade_coverage_opened",
                    occurred_at=analysis.coverage_opening_effective_at,
                    received_at=datetime.now(UTC),
                    evidence={
                        "raw_record_id": opening_record.raw_record_id,
                        "receive_ordinal": opening_record.receive_ordinal,
                        "ordering_assurance": OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS.value,
                    },
                )
                session_event_ordinal += 1
                status = (
                    CoverageStatus.INVALID
                    if invalidating_quality_ids
                    else CoverageStatus.CLOSED_VALID
                )
                known_at = datetime.now(UTC)
                coverage = TradeCoverageIntervalVersion(
                    interval_id=coverage_interval_id,
                    revision=1,
                    definition_id=claim.definition_id,
                    session_id=claim.session_id,
                    connection_epoch=0,
                    provider_product_id=claim.provider_product_id,
                    channel=primary_channel,
                    status=status,
                    ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
                    archive_status=ArchiveStatus.COMPLETE,
                    opening_raw_record_id=opening_record.raw_record_id,
                    opening_receive_ordinal=opening_record.receive_ordinal,
                    opening_effective_at=analysis.coverage_opening_effective_at,
                    last_raw_record_id=last_record.raw_record_id,
                    last_receive_ordinal=last_record.receive_ordinal,
                    last_effective_at=analysis.coverage_last_effective_at,
                    closing_raw_record_id=last_record.raw_record_id,
                    closing_receive_ordinal=last_record.receive_ordinal,
                    closing_effective_at=analysis.coverage_last_effective_at,
                    canonicalization_watermark_ordinal=raw_records[-1].receive_ordinal,
                    archive_complete_through_ordinal=raw_records[-1].receive_ordinal,
                    known_at=known_at,
                    first_provider_sequence_num=analysis.coverage_first_sequence_num,
                    last_provider_sequence_num=analysis.coverage_last_sequence_num,
                    gap_quality_event_ids=tuple(invalidating_quality_ids),
                    opening_evidence={
                        "subscription_acknowledged": True,
                        "heartbeat_healthy": True,
                        "snapshot_accepted": True,
                    },
                    closing_evidence={
                        "bounded_stop_session_event_id": bounded_stop_event_id,
                        "all_raw_records_mapped": True,
                    },
                )

            translated = []
            for item in captured:
                if item.event.event_kind != "market_trade":
                    continue
                delivery_kind = str(item.event.payload.get("type") or "").lower()
                coverage_ref = (
                    coverage_interval_id
                    if coverage is not None
                    and delivery_kind == "update"
                    and item.raw_record.receive_ordinal >= coverage.opening_receive_ordinal
                    else None
                )
                translated.append(
                    translate_coinbase_market_trade(
                        item.event,
                        contract=PHASE1_COINBASE_TRADE_CONTRACTS[claim.provider_product_id],
                        raw_record_id=item.raw_record.raw_record_id,
                        connection_epoch=item.raw_record.connection_epoch,
                        receive_ordinal=item.raw_record.receive_ordinal,
                        accepted_at=datetime.now(UTC),
                        coverage_interval_id=coverage_ref,
                    )
                )
            try:
                trade_outcome = self.repository.ingest_trades(
                    claim,
                    facts=translated,
                    require_archive_mapping=True,
                )
            except MarketTradeConflictError as exc:
                conflict = translated[0] if translated else None
                self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=conflict.receive_ordinal if conflict else raw_records[-1].receive_ordinal,
                    channel=primary_channel,
                    classification="provider_trade_conflict",
                    reason=str(exc),
                    detected_at=datetime.now(UTC),
                    raw_record_id=conflict.raw_record_id if conflict else raw_records[-1].raw_record_id,
                )
                raise
            if coverage is not None and opening_event_id is not None:
                self.repository.append_coverage_version(
                    claim,
                    coverage=coverage,
                    opening_session_event_id=opening_event_id,
                    closing_session_event_id=bounded_stop_event_id,
                )

            aggregate_counts: dict[str, dict[str, int]] = {}
            feature_counts: dict[str, dict[str, int]] = {}
            all_trade_facts = []
            if coverage is not None:
                # Initial snapshot trades are valuable canonical evidence, but
                # they predate proven live delivery and must never be projected
                # as complete flow. Keep current-session update deliveries so a
                # trade first seen in a snapshot and then redelivered live is
                # still represented; aggregate_trade_bucket deduplicates its
                # provider identity deterministically. Membership is based on
                # the delivery's typed coverage reference rather than comparing
                # provider event time to receipt time at the interval edge.
                all_trade_facts = [
                    fact
                    for fact in translated
                    if fact.coverage_interval_id == coverage.interval_id
                ]
            aggregate_series_ids = {
                int(key): int(value)
                for key, value in dict(claim.config.get("aggregate_series_ids") or {}).items()
            }
            flow_feature_series_ids = {
                int(key): int(value)
                for key, value in dict(claim.config.get("flow_feature_series_ids") or {}).items()
            }
            for interval in (1, 60):
                bucket_starts = {
                    bucket_start_for(fact.provider_event_time, interval_seconds=interval)
                    for fact in all_trade_facts
                }
                if coverage is not None:
                    cursor = bucket_start_for(
                        coverage.opening_effective_at, interval_seconds=interval
                    )
                    last = bucket_start_for(
                        coverage.closing_effective_at or coverage.last_effective_at,
                        interval_seconds=interval,
                    )
                    while cursor <= last:
                        if coverage.complete_for_bucket(
                            bucket_start=cursor,
                            bucket_end=cursor + timedelta(seconds=interval),
                        ):
                            bucket_starts.add(cursor)
                        cursor += timedelta(seconds=interval)
                aggregate_facts = []
                for bucket_start in sorted(bucket_starts):
                    bucket_rows = [
                        fact
                        for fact in all_trade_facts
                        if bucket_start
                        <= fact.provider_event_time
                        < bucket_start + timedelta(seconds=interval)
                    ]
                    try:
                        aggregate_facts.append(
                            aggregate_trade_bucket(
                                bucket_rows,
                                interval_seconds=interval,
                                bucket_start=bucket_start,
                                coverage=coverage,
                                computed_at=max(
                                    datetime.now(UTC),
                                    bucket_start + timedelta(seconds=interval),
                                ),
                            )
                        )
                    except ValueError as exc:
                        if "incomplete_zero_forbidden" not in str(exc):
                            raise
                aggregate_outcome = self.repository.ingest_aggregates(
                    series_id=aggregate_series_ids[interval],
                    facts=aggregate_facts,
                )
                aggregate_counts[str(interval)] = {
                    "requested": len(aggregate_facts),
                    "inserted": aggregate_outcome.inserted_count,
                    "noop": aggregate_outcome.noop_count,
                }
                if interval not in flow_feature_series_ids:
                    raise RuntimeError(
                        "market_flow_feature_config_missing: re-run pair configuration"
                    )
                flow_features = []
                for aggregate in aggregate_facts:
                    feature = derive_trade_flow_feature(
                        series_id=flow_feature_series_ids[interval],
                        source_trade_flow_series_id=aggregate_series_ids[interval],
                        aggregate=aggregate,
                        trades=all_trade_facts,
                        computed_at=max(datetime.now(UTC), aggregate.bucket_end),
                    )
                    if feature is not None:
                        flow_features.append(feature)
                feature_outcome = self.repository.ingest_market_state_features(
                    flow_facts=flow_features
                )
                feature_counts[str(interval)] = {
                    "requested": len(flow_features),
                    "inserted": feature_outcome.inserted_count,
                    "noop": feature_outcome.noop_count,
                }
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="canonicalization_completed",
                occurred_at=datetime.now(UTC),
                evidence={
                    "trade_requested": trade_outcome.requested_count,
                    "trade_inserted": trade_outcome.inserted_count,
                    "trade_noop": trade_outcome.noop_count,
                    "aggregate_counts": aggregate_counts,
                    "flow_feature_counts": feature_counts,
                    "manifest_ids": manifest_ids,
                },
            )
            status = self.repository.archive_status(definition_id=claim.definition_id)
            result = {
                "schema_version": "market_structure_bounded_capture.v1",
                "status": "completed",
                "definition_id": claim.definition_id,
                "session_id": claim.session_id,
                "product_id": claim.provider_product_id,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "elapsed_seconds": time.monotonic() - started_monotonic,
                "raw_record_count": analysis.raw_record_count,
                "raw_bytes": analysis.raw_bytes,
                "spool_segment_count": len(segments),
                "spool_backlog_bytes": spool_backlog_bytes(
                    spool_root, definition_id=claim.definition_id
                ),
                "manifest_ids": manifest_ids,
                "trade_events": analysis.trade_event_count,
                "snapshot_trades": analysis.snapshot_trade_count,
                "update_trades": analysis.update_trade_count,
                "trade_ingestion": {
                    "requested": trade_outcome.requested_count,
                    "inserted": trade_outcome.inserted_count,
                    "noop": trade_outcome.noop_count,
                    "max_commit_seq": trade_outcome.max_commit_seq,
                },
                "aggregates": aggregate_counts,
                "flow_features": feature_counts,
                "coverage": {
                    "opened": coverage is not None,
                    "status": coverage.status.value if coverage else "unproven",
                    "interval_id": coverage.interval_id if coverage else None,
                    "complete_bucket_eligible": bool(
                        coverage and coverage.status is CoverageStatus.CLOSED_VALID
                    ),
                },
                "quality_event_ids": quality_event_ids,
                "archive_status": status,
                "production_admitted": False,
                "production_blockers": [
                    "post_phase4_24h_implemented_path_capture",
                    "explicit_storage_and_cost_budget",
                ],
            }
            logger.info(
                "market_structure_capture_completed | definition_id=%s session_id=%s product_id=%s raw_records=%s trades=%s manifests=%s elapsed_seconds=%.3f",
                claim.definition_id,
                claim.session_id,
                claim.provider_product_id,
                analysis.raw_record_count,
                analysis.trade_event_count,
                len(manifest_ids),
                result["elapsed_seconds"],
            )
            return result
        except SpoolBackpressureError as exc:
            with contextlib.suppress(Exception):
                self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=raw_records[-1].receive_ordinal if raw_records else 0,
                    channel=primary_channel,
                    classification="backpressure_stop",
                    reason=str(exc),
                    detected_at=datetime.now(UTC),
                    raw_record_id=raw_records[-1].raw_record_id if raw_records else None,
                )
            raise
        except Exception as exc:
            with contextlib.suppress(Exception):
                self.repository.append_session_event(
                    claim,
                    event_ordinal=session_event_ordinal,
                    connection_epoch=0,
                    event_type="failed",
                    occurred_at=datetime.now(UTC),
                    reason=f"{type(exc).__name__}: {exc}",
                    evidence={"raw_record_count": len(raw_records)},
                )
            logger.exception(
                "market_structure_capture_failed | definition_id=%s session_id=%s product_id=%s",
                claim.definition_id,
                claim.session_id,
                claim.provider_product_id,
            )
            raise
        finally:
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await pending
            if stream is not None:
                with contextlib.suppress(Exception):
                    await stream.close()
            if current_segment is not None:
                with contextlib.suppress(Exception):
                    current_segment.close()
            with contextlib.suppress(Exception):
                self.repository.release(claim)

    def _finalize_level2_capture(
        self,
        *,
        claim: StreamClaim,
        captured: Sequence[CapturedEvent],
        raw_records: Sequence[RawStreamRecord],
        analysis: CaptureAnalysis,
        manifest_ids: Sequence[str],
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        storage_root: Path,
        started_at: datetime,
        started_monotonic: float,
        bounded_stop_event_id: str,
        session_event_ordinal: int,
        segment_count: int,
    ) -> dict[str, Any]:
        """Reduce acknowledged raw L2 evidence and persist one bounded session."""

        config = dict(claim.config or {})
        contract = L2ProductContract(
            provider_product_id=claim.provider_product_id,
            product_definition_version_id=str(
                config.get("product_definition_version_id") or ""
            ),
            provider_size_unit=str(config.get("provider_size_unit") or ""),
            price_increment=config.get("price_increment"),
            quantity_increment=config.get("quantity_increment"),
        )
        reducer = Level2BookReconstructor(
            series_id=claim.series_id,
            contract=contract,
            ordering_assurance=OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
        )
        events_by_raw: dict[str, list[CanonicalMarketEvent]] = {}
        for item in captured:
            events_by_raw.setdefault(item.raw_record.raw_record_id, []).append(item.event)
        analyzer_quality_by_ordinal: dict[int, list[Mapping[str, Any]]] = {}
        for item in analysis.quality_events:
            analyzer_quality_by_ordinal.setdefault(
                int(item["receive_ordinal"]), []
            ).append(item)

        snapshots = []
        batches = []
        validity_versions = []
        checkpoints = []
        book_quality: list[BookQualityEvidence] = []
        valid_states = []
        direct_quality: list[Mapping[str, Any]] = []
        interval_by_position: dict[tuple[int, int], str] = {}
        last_l2_event = None
        last_source_position: Optional[BookSourcePosition] = None
        final_interval_id: Optional[str] = None

        def collect(result) -> None:
            nonlocal final_interval_id
            if result.snapshot is not None:
                snapshots.append(result.snapshot)
                final_interval_id = result.snapshot.validity_interval_id
                interval_by_position[
                    (
                        result.snapshot.event.position.receive_ordinal,
                        result.snapshot.event.position.event_ordinal,
                    )
                ] = result.snapshot.validity_interval_id
            if result.batch is not None:
                batches.append(result.batch)
                final_interval_id = result.batch.validity_interval_id
                interval_by_position[
                    (
                        result.batch.event.position.receive_ordinal,
                        result.batch.event.position.event_ordinal,
                    )
                ] = result.batch.validity_interval_id
            validity_versions.extend(result.validity_versions)
            checkpoints.extend(result.checkpoints)
            book_quality.extend(result.quality)
            if result.state is not None:
                valid_states.append(result.state)

        for raw in raw_records:
            raw_events = events_by_raw.get(raw.raw_record_id, [])
            sequence_num = next(
                (
                    event.provider_sequence_num
                    for event in raw_events
                    if event.provider_sequence_num is not None
                ),
                None,
            )
            for quality in analyzer_quality_by_ordinal.get(raw.receive_ordinal, []):
                if not bool(quality.get("invalidating")):
                    direct_quality.append(quality)
                    continue
                position = BookSourcePosition(
                    definition_id=claim.definition_id,
                    session_id=claim.session_id,
                    connection_epoch=raw.connection_epoch,
                    provider_product_id=claim.provider_product_id,
                    provider_sequence_num=(
                        int(quality["sequence_after"])
                        if quality.get("sequence_after") is not None
                        else sequence_num
                    ),
                    receive_ordinal=raw.receive_ordinal,
                    event_ordinal=0,
                )
                result = reducer.invalidate_transport(
                    position=position,
                    effective_at=quality["detected_at"],
                    known_at=quality["detected_at"],
                    raw_record_id=raw.raw_record_id,
                    classification=str(quality["classification"]),
                    reason=str(quality["reason"]),
                    evidence=dict(quality.get("evidence") or {}),
                )
                collect(result)
                last_source_position = position

            for event in raw_events:
                if event.event_kind not in {"market_l2_snapshot", "market_l2_update"}:
                    continue
                fact = translate_coinbase_l2_event(
                    event,
                    raw_record=raw,
                    contract=contract,
                    accepted_at=datetime.now(UTC),
                )
                prior_lifecycle = reducer.lifecycle
                result = reducer.process(fact)
                collect(result)
                last_l2_event = fact
                last_source_position = fact.position
                if (
                    result.snapshot is not None
                    and prior_lifecycle is BookLifecycle.INVALID
                ):
                    book_quality.append(
                        BookQualityEvidence(
                            classification="resync_snapshot_accepted",
                            reason="fresh complete snapshot restored book validity",
                            position=fact.position,
                            known_at=fact.known_at,
                            raw_record_id=fact.raw_record_id,
                            invalidating=False,
                            evidence={
                                "state_hash": result.snapshot.state_hash,
                                "validity_interval_id": result.snapshot.validity_interval_id,
                            },
                        )
                    )

        if not snapshots or last_l2_event is None or last_source_position is None:
            raise RuntimeError(
                "market_l2_capture_unproven: no valid complete snapshot was accepted"
            )
        terminal_lifecycle = reducer.lifecycle
        final_state_hash = reducer.current_state_hash
        final_interval_id = (
            reducer.current_interval.interval_id
            if reducer.current_interval is not None
            else final_interval_id
        )
        final_position = last_source_position
        if terminal_lifecycle is BookLifecycle.VALID:
            validity_versions.extend(
                reducer.close_bounded(at_event=last_l2_event)
            )

        checkpoint_ids: list[str] = []
        for checkpoint in checkpoints:
            encoded, acknowledgement = publish_book_checkpoint(
                checkpoint,
                object_store=object_store,
                temporary_directory=temporary_root,
            )
            self.repository.commit_book_checkpoint(
                claim,
                checkpoint=checkpoint,
                encoded=encoded,
                acknowledgement=acknowledgement,
                source_manifest_ids=manifest_ids,
            )
            checkpoint_ids.append(checkpoint.checkpoint_id)

        ingest = self.repository.ingest_book_facts(
            claim,
            snapshots=snapshots,
            batches=batches,
            validity_versions=validity_versions,
            lifecycle=(
                BookLifecycle.AWAITING_SNAPSHOT
                if terminal_lifecycle is BookLifecycle.VALID
                else terminal_lifecycle
            ),
            final_validity_interval_id=final_interval_id,
            checkpoint_id=checkpoint_ids[-1] if checkpoint_ids else None,
            final_state_hash=final_state_hash,
            final_connection_epoch=final_position.connection_epoch,
            final_receive_ordinal=final_position.receive_ordinal,
            final_event_ordinal=final_position.event_ordinal,
            final_sequence_num=final_position.provider_sequence_num,
        )
        if "bbo_series_id" not in config or "depth_series_id" not in config:
            raise RuntimeError(
                "market_book_feature_config_missing: re-run pair configuration"
            )
        valuation = MarketStateValuationContract(
            product_definition_version_id=str(
                config.get("product_definition_version_id") or ""
            ),
            provider_size_unit=str(config.get("provider_size_unit") or ""),
            base_currency=str(config.get("base_currency") or ""),
            quote_currency=str(config.get("quote_currency") or ""),
            contract_size=config.get("contract_size"),
        )
        bbo_facts, depth_facts = derive_book_features(
            valid_states,
            contract=valuation,
            bbo_series_id=int(config["bbo_series_id"]),
            depth_series_id=int(config["depth_series_id"]),
            computed_at=datetime.now(UTC),
        )
        feature_ingest = self.repository.ingest_market_state_features(
            bbo_facts=bbo_facts,
            depth_facts=depth_facts,
        )
        response_facts = ()
        response_inserted = 0
        response_noop = 0
        if valid_states:
            required_response_config = (
                "trade_series_id",
                "flow_feature_series_ids",
                "response_feature_series_id",
            )
            if not all(name in config for name in required_response_config):
                raise RuntimeError(
                    "market_response_feature_config_missing: re-run pair configuration"
                )
            response_start = min(row.effective_at for row in valid_states) - timedelta(
                seconds=2
            )
            response_end = max(row.effective_at for row in valid_states) + timedelta(
                seconds=2
            )
            response_known_at = datetime.now(UTC)
            flow_series_id = int(
                dict(config["flow_feature_series_ids"])["1"]
            )
            flow_rows = self.repository.read_trade_flow_features(
                series_id=flow_series_id,
                start=response_start,
                end=response_end,
                known_at=response_known_at,
            )
            trade_rows = self.repository.read_trades(
                series_id=int(config["trade_series_id"]),
                start=response_start,
                end=response_end,
                known_at_lte=response_known_at,
            )
            response_facts = derive_response_features(
                valid_states,
                tuple(row.fact for row in trade_rows),
                flow_rows,
                contract=valuation,
                series_id=int(config["response_feature_series_id"]),
                computed_at=response_known_at,
            )
            response_ingest = self.repository.ingest_market_state_features(
                response_facts=response_facts
            )
            response_inserted = response_ingest.inserted_count
            response_noop = response_ingest.noop_count

        quality_event_ids: list[str] = []
        closing_interval_by_quality_hash = {
            row.closing_quality_hash: row.interval_id
            for row in validity_versions
            if row.closing_quality_hash
        }
        for quality in book_quality:
            quality_id = self.repository.record_quality_event(
                claim,
                connection_epoch=quality.position.connection_epoch,
                receive_ordinal=quality.position.receive_ordinal,
                channel="level2",
                classification=quality.classification,
                reason=quality.reason,
                detected_at=quality.known_at,
                raw_record_id=quality.raw_record_id,
                sequence_after=quality.position.provider_sequence_num,
                evidence={
                    **dict(quality.evidence),
                    "book_quality_evidence_hash": quality.evidence_hash,
                    "event_ordinal": quality.position.event_ordinal,
                },
            )
            quality_event_ids.append(quality_id)
            interval_id = closing_interval_by_quality_hash.get(
                quality.evidence_hash
            ) or interval_by_position.get(
                (
                    quality.position.receive_ordinal,
                    quality.position.event_ordinal,
                )
            )
            if interval_id:
                self.repository.link_book_quality_event(
                    claim,
                    quality_event_id=quality_id,
                    validity_interval_id=interval_id,
                    link_role=(
                        "invalidated" if quality.invalidating else "observed_within"
                    ),
                    known_at=quality.known_at,
                )
        for quality in direct_quality:
            quality_event_ids.append(
                self.repository.record_quality_event(
                    claim,
                    connection_epoch=int(quality["connection_epoch"]),
                    receive_ordinal=int(quality["receive_ordinal"]),
                    channel="level2",
                    classification=str(quality["classification"]),
                    reason=str(quality["reason"]),
                    detected_at=quality["detected_at"],
                    raw_record_id=quality.get("raw_record_id"),
                    sequence_before=quality.get("sequence_before"),
                    sequence_after=quality.get("sequence_after"),
                    evidence=quality.get("evidence"),
                )
            )

        self.repository.append_session_event(
            claim,
            event_ordinal=session_event_ordinal,
            connection_epoch=0,
            event_type="book_canonicalization_completed",
            occurred_at=datetime.now(UTC),
            evidence={
                "bounded_stop_session_event_id": bounded_stop_event_id,
                "snapshot_count": len(snapshots),
                "batch_count": len(batches),
                "checkpoint_ids": checkpoint_ids,
                "final_state_hash": final_state_hash,
                "archive_manifest_ids": list(manifest_ids),
                "feature_counts": {
                    "bbo": len(bbo_facts),
                    "depth": len(depth_facts),
                    "response": len(response_facts),
                    "response_inserted": response_inserted,
                    "response_noop": response_noop,
                    "inserted": feature_ingest.inserted_count,
                    "noop": feature_ingest.noop_count,
                },
            },
        )
        status = self.repository.archive_status(definition_id=claim.definition_id)
        result = {
            "schema_version": "market_structure_l2_bounded_capture.v1",
            "status": "completed",
            "definition_id": claim.definition_id,
            "session_id": claim.session_id,
            "product_id": claim.provider_product_id,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": time.monotonic() - started_monotonic,
            "raw_record_count": analysis.raw_record_count,
            "raw_bytes": analysis.raw_bytes,
            "spool_segment_count": segment_count,
            "spool_backlog_bytes": spool_backlog_bytes(
                storage_root / "spool", definition_id=claim.definition_id
            ),
            "manifest_ids": list(manifest_ids),
            "snapshot_count": len(snapshots),
            "mutation_batch_count": len(batches),
            "mutation_count": sum(len(row.event.mutations) for row in batches),
            "checkpoint_ids": checkpoint_ids,
            "final_state_hash": final_state_hash,
            "validity_interval_id": final_interval_id,
            "validity_closed_cleanly": terminal_lifecycle is BookLifecycle.VALID,
            "ingestion": {
                "inserted_snapshots": ingest.inserted_snapshot_count,
                "noop_snapshots": ingest.noop_snapshot_count,
                "inserted_batches": ingest.inserted_batch_count,
                "noop_batches": ingest.noop_batch_count,
                "inserted_validity_versions": ingest.inserted_validity_count,
                "max_commit_seq": ingest.max_commit_seq,
            },
            "features": {
                "bbo_count": len(bbo_facts),
                "depth_count": len(depth_facts),
                "response_count": len(response_facts),
                "response_inserted": response_inserted,
                "response_noop": response_noop,
                "inserted": feature_ingest.inserted_count,
                "noop": feature_ingest.noop_count,
            },
            "quality_event_ids": quality_event_ids,
            "archive_status": status,
            "production_admitted": False,
            "production_blockers": [
                "post_phase4_24h_implemented_path_capture",
                "explicit_storage_and_cost_budget",
            ],
        }
        logger.info(
            "market_structure_l2_capture_completed | definition_id=%s session_id=%s product_id=%s snapshots=%s batches=%s mutations=%s checkpoints=%s final_state_hash=%s",
            claim.definition_id,
            claim.session_id,
            claim.provider_product_id,
            len(snapshots),
            len(batches),
            result["mutation_count"],
            len(checkpoint_ids),
            final_state_hash,
        )
        return result

    def replay_manifest(
        self, *, manifest_id: str, storage_root: Path = DEFAULT_STORAGE_ROOT
    ) -> dict[str, Any]:
        retention = self.repository.archive_retention_status(
            target_kind="raw_manifest",
            target_id=manifest_id,
        )
        if retention["object_retention_state"] == "expired":
            raise RuntimeError(
                "market_archive_object_expired: "
                f"manifest_id={manifest_id} expiration={retention['expiration']}"
            )
        manifest = self.repository.get_manifest(manifest_id)
        store = FilesystemRawArchiveObjectStore(
            Path(storage_root).expanduser().resolve() / "objects"
        )
        path = store.local_path(str(manifest["object_key"]))
        if not path.exists():
            raise RuntimeError(
                f"market_archive_object_missing: manifest_id={manifest_id} object_key={manifest['object_key']}"
            )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != manifest["object_sha256"]:
            raise RuntimeError("market_archive_replay_invalid: object checksum mismatch")
        records = read_raw_archive_parquet(path)
        parser = CoinbaseMessageParser()
        trade_ids: list[str] = []
        for record in records:
            for event in parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={"raw_record_id": record.raw_record_id},
            ):
                if event.event_kind == "market_trade":
                    trade_ids.append(str(event.payload.get("trade_id")))
        fingerprint = _stable_hash(
            {
                "schema_version": "market_structure_trade_replay.v1",
                "raw_record_ids": [record.raw_record_id for record in records],
                "trade_ids": trade_ids,
            }
        )
        reconciliation = self.repository.reconcile_manifest_trade_ids(
            manifest_id=manifest_id,
            provider_product_id=(
                records[0].provider_product_id if records else ""
            ),
            provider_trade_ids=trade_ids,
        )
        return {
            "schema_version": "market_structure_manifest_replay.v1",
            "manifest_id": manifest_id,
            "object_sha256": digest,
            "raw_record_count": len(records),
            "trade_count": len(trade_ids),
            "first_receive_ordinal": records[0].receive_ordinal if records else None,
            "last_receive_ordinal": records[-1].receive_ordinal if records else None,
            "replay_fingerprint": fingerprint,
            "reconciliation": reconciliation,
        }

    def compact_session_archives(
        self,
        *,
        definition_id: str,
        source_session_id: str,
        source_manifest_ids: Sequence[str],
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        owner_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Compact an explicit active source set without provider access."""

        requested_ids = tuple(dict.fromkeys(str(value) for value in source_manifest_ids))
        if len(requested_ids) < 2:
            raise ValueError(
                "market_archive_compaction_invalid: at least two source manifests required"
            )
        active = self.repository.list_session_manifests(
            definition_id=definition_id,
            session_id=source_session_id,
        )
        active_by_id = {str(row["id"]): row for row in active}
        if any(manifest_id not in active_by_id for manifest_id in requested_ids):
            raise ValueError(
                "market_archive_compaction_invalid: source is missing or already replaced"
            )
        selected = sorted(
            (active_by_id[manifest_id] for manifest_id in requested_ids),
            key=lambda row: (int(row["connection_epoch"]), int(row["first_receive_ordinal"])),
        )
        if len({int(row["connection_epoch"]) for row in selected}) != 1:
            raise ValueError(
                "market_archive_compaction_invalid: source objects cross connection epochs"
            )
        store = FilesystemRawArchiveObjectStore(
            Path(storage_root).expanduser().resolve() / "objects"
        )
        source_paths: list[Path] = []
        for manifest in selected:
            path = store.local_path(str(manifest["object_key"]))
            if not path.exists():
                raise RuntimeError(
                    f"market_archive_object_missing: manifest_id={manifest['id']}"
                )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != str(manifest["object_sha256"]):
                raise RuntimeError(
                    "market_archive_compaction_invalid: source checksum mismatch"
                )
            source_paths.append(path)

        owner = str(owner_id or f"qt-compaction:{socket.gethostname()}:{os.getpid()}")
        claim = self.repository.claim_stream(
            definition_id=definition_id,
            owner_id=owner,
            lease_seconds=600,
            bounded=True,
        )
        event_ordinal = 0
        try:
            self.repository.append_session_event(
                claim,
                event_ordinal=event_ordinal,
                connection_epoch=int(selected[0]["connection_epoch"]),
                event_type="archive_compaction_started",
                occurred_at=datetime.now(UTC),
                evidence={
                    "source_session_id": source_session_id,
                    "source_manifest_ids": [str(row["id"]) for row in selected],
                },
            )
            event_ordinal += 1
            encoded, acknowledgement, records = publish_compacted_raw_archives(
                source_paths,
                object_store=store,
                temporary_directory=(
                    Path(storage_root).expanduser().resolve() / "tmp"
                ),
            )
            commit = self.repository.commit_archive(
                claim,
                encoded=encoded,
                acknowledgement=acknowledgement,
                records=records,
                compaction_source_manifest_ids=[str(row["id"]) for row in selected],
            )
            self.repository.append_session_event(
                claim,
                event_ordinal=event_ordinal,
                connection_epoch=int(selected[0]["connection_epoch"]),
                event_type="archive_compaction_completed",
                occurred_at=datetime.now(UTC),
                evidence={
                    "source_session_id": source_session_id,
                    "source_manifest_ids": [str(row["id"]) for row in selected],
                    "replacement_manifest_id": commit.manifest_id,
                    "content_fingerprint": encoded.content_fingerprint,
                },
            )
            return {
                "schema_version": "market.raw_archive_compaction.v1",
                "definition_id": definition_id,
                "source_session_id": source_session_id,
                "compaction_session_id": claim.session_id,
                "source_manifest_ids": [str(row["id"]) for row in selected],
                "replacement_manifest_id": commit.manifest_id,
                "record_count": len(records),
                "first_receive_ordinal": records[0].receive_ordinal,
                "last_receive_ordinal": records[-1].receive_ordinal,
                "object_sha256": acknowledgement.sha256,
                "content_fingerprint": encoded.content_fingerprint,
                "reused_existing": acknowledgement.reused_existing,
                "source_objects_deleted": False,
            }
        except Exception as exc:
            with contextlib.suppress(Exception):
                self.repository.append_session_event(
                    claim,
                    event_ordinal=event_ordinal,
                    connection_epoch=int(selected[0]["connection_epoch"]),
                    event_type="archive_compaction_failed",
                    occurred_at=datetime.now(UTC),
                    reason=str(exc),
                    evidence={
                        "source_session_id": source_session_id,
                        "source_manifest_ids": [str(row["id"]) for row in selected],
                    },
                )
            raise
        finally:
            with contextlib.suppress(Exception):
                self.repository.release(claim)

    def replay_book_session(
        self,
        *,
        definition_id: str,
        session_id: str,
        execution_instrument_id: Optional[str] = None,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
    ) -> dict[str, Any]:
        """Replay acknowledged raw objects without provider access."""

        definitions = self.repository.list_stream_definitions(
            definition_id=definition_id
        )
        if len(definitions) != 1 or "level2" not in tuple(definitions[0]["channels"]):
            raise ValueError("market_book_replay_invalid: Level 2 definition required")
        definition = definitions[0]
        config = dict(definition.get("config") or {})
        contract = L2ProductContract(
            provider_product_id=str(definition["provider_product_id"]),
            product_definition_version_id=str(
                config.get("product_definition_version_id") or ""
            ),
            provider_size_unit=str(config.get("provider_size_unit") or ""),
            price_increment=config.get("price_increment"),
            quantity_increment=config.get("quantity_increment"),
        )
        manifests = self.repository.list_session_manifests(
            definition_id=definition_id, session_id=session_id
        )
        if not manifests:
            raise ValueError("market_book_replay_invalid: session has no archive manifests")
        store = FilesystemRawArchiveObjectStore(
            Path(storage_root).expanduser().resolve() / "objects"
        )
        records_by_id: dict[str, RawStreamRecord] = {}
        for manifest in manifests:
            path = store.local_path(str(manifest["object_key"]))
            if not path.exists():
                raise RuntimeError(
                    f"market_archive_object_missing: manifest_id={manifest['id']}"
                )
            if hashlib.sha256(path.read_bytes()).hexdigest() != str(
                manifest["object_sha256"]
            ):
                raise RuntimeError("market_book_replay_invalid: raw object checksum mismatch")
            for record in read_raw_archive_parquet(path):
                prior = records_by_id.get(record.raw_record_id)
                if prior is not None and prior.raw_frame_sha256 != record.raw_frame_sha256:
                    raise RuntimeError("market_book_replay_invalid: raw identity conflict")
                records_by_id[record.raw_record_id] = record
        records = sorted(
            records_by_id.values(),
            key=lambda row: (row.connection_epoch, row.receive_ordinal),
        )
        parser = CoinbaseMessageParser(
            symbol_by_product_id={contract.provider_product_id: contract.provider_product_id}
        )
        operations: list[tuple[str, BookSourcePosition, Any]] = []
        invalidating_classes = {
            "sequence_gap",
            "out_of_order",
            "heartbeat_gap",
            "disconnect",
            "decode_error",
        }
        quality_rows = self.repository.list_session_quality_events(
            definition_id=definition_id,
            session_id=session_id,
        )
        quality_by_position: dict[tuple[int, int], list[Mapping[str, Any]]] = {}
        for quality in quality_rows:
            if str(quality["classification"]) not in invalidating_classes:
                continue
            key = (
                int(quality["connection_epoch"]),
                int(quality["receive_ordinal"]),
            )
            quality_by_position.setdefault(key, []).append(quality)
        for record in records:
            parsed = parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={"raw_record_id": record.raw_record_id},
            )
            for quality in quality_by_position.get(
                (record.connection_epoch, record.receive_ordinal), ()
            ):
                evidence = dict(quality.get("evidence") or {})
                event_ordinal = int(evidence.pop("event_ordinal", 0))
                evidence.pop("book_quality_evidence_hash", None)
                position = BookSourcePosition(
                    definition_id=definition_id,
                    session_id=session_id,
                    connection_epoch=record.connection_epoch,
                    provider_product_id=contract.provider_product_id,
                    provider_sequence_num=(
                        int(quality["sequence_after"])
                        if quality.get("sequence_after") is not None
                        else None
                    ),
                    receive_ordinal=record.receive_ordinal,
                    event_ordinal=event_ordinal,
                )
                operations.append(
                    (
                        "invalidate",
                        position,
                        {
                            "effective_at": quality["detected_at"],
                            "known_at": quality["known_at"],
                            "raw_record_id": str(
                                quality.get("raw_record_id") or record.raw_record_id
                            ),
                            "classification": str(quality["classification"]),
                            "reason": str(quality["reason"]),
                            "evidence": evidence,
                        },
                    )
                )
            for event in parsed:
                if event.event_kind not in {"market_l2_snapshot", "market_l2_update"}:
                    continue
                fact = translate_coinbase_l2_event(
                    event,
                    raw_record=record,
                    contract=contract,
                    accepted_at=record.received_at,
                )
                operations.append(("event", fact.position, fact))

        def reduce_operations(
            reducer: Level2BookReconstructor,
            selected: Sequence[tuple[str, BookSourcePosition, Any]],
        ) -> tuple[
            list[str],
            list[str],
            dict[str, Any],
            dict[str, Any],
            list[Any],
            list[Any],
        ]:
            snapshot_ids: list[str] = []
            batch_ids: list[str] = []
            checkpoints: dict[str, Any] = {}
            opening_validity: dict[str, Any] = {}
            states: list[Any] = []
            closing_validity: list[Any] = []
            for kind, position, payload in selected:
                if kind == "invalidate":
                    result = reducer.invalidate_transport(
                        position=position,
                        **payload,
                    )
                else:
                    result = reducer.process(payload)
                if result.snapshot is not None:
                    snapshot_ids.append(result.snapshot.snapshot_id)
                if result.batch is not None:
                    batch_ids.append(result.batch.batch_id)
                if result.state is not None:
                    states.append(result.state)
                for checkpoint in result.checkpoints:
                    checkpoints[checkpoint.checkpoint_id] = checkpoint
                for validity in result.validity_versions:
                    if validity.revision == 1:
                        opening_validity[validity.interval_id] = validity
                    elif validity.closing_position is not None:
                        closing_validity.append(validity)
            return (
                snapshot_ids,
                batch_ids,
                checkpoints,
                opening_validity,
                states,
                closing_validity,
            )

        full = Level2BookReconstructor(
            series_id=int(definition["series_id"]),
            contract=contract,
            ordering_assurance=OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
        )
        (
            snapshot_ids,
            batch_ids,
            replay_checkpoints,
            opening_validity,
            replay_states,
            closing_validity,
        ) = reduce_operations(full, operations)
        final_state_hash = full.current_state_hash
        reconciliation = self.repository.reconcile_book_replay(
            definition_id=definition_id,
            session_id=session_id,
            snapshot_ids=snapshot_ids,
            batch_ids=batch_ids,
            final_state_hash=final_state_hash,
        )

        persisted_checkpoints = self.repository.list_book_checkpoints(
            definition_id=definition_id, session_id=session_id
        )
        checkpoint_checks: list[dict[str, Any]] = []
        latest_checkpoint = None
        for row in persisted_checkpoints:
            checkpoint = replay_checkpoints.get(str(row["id"]))
            if checkpoint is None:
                raise RuntimeError(
                    "market_book_replay_invalid: persisted checkpoint was not reproduced"
                )
            path = store.local_path(str(row["object_key"]))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != str(row["object_sha256"]):
                raise RuntimeError(
                    "market_book_replay_invalid: checkpoint object checksum mismatch"
                )
            rows = read_book_checkpoint_parquet(path)
            if rows != checkpoint_canonical_rows(checkpoint):
                raise RuntimeError(
                    "market_book_replay_invalid: checkpoint typed levels differ"
                )
            checkpoint_checks.append(
                {
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "object_sha256": digest,
                    "state_hash": checkpoint.state_hash,
                    "level_count": len(rows),
                }
            )
            latest_checkpoint = checkpoint

        checkpoint_delta_equal: Optional[bool] = None
        if latest_checkpoint is not None:
            validity = opening_validity.get(latest_checkpoint.validity_interval_id)
            if validity is None:
                raise RuntimeError(
                    "market_book_replay_invalid: checkpoint validity opening is missing"
                )
            resumed = Level2BookReconstructor.from_checkpoint(
                latest_checkpoint,
                contract=contract,
                validity=validity,
            )
            checkpoint_position = (
                latest_checkpoint.source_position.connection_epoch,
                latest_checkpoint.source_position.receive_ordinal,
                latest_checkpoint.source_position.event_ordinal,
            )
            deltas = [
                operation
                for operation in operations
                if (
                    operation[1].connection_epoch,
                    operation[1].receive_ordinal,
                    operation[1].event_ordinal,
                )
                > checkpoint_position
            ]
            reduce_operations(resumed, deltas)
            checkpoint_delta_equal = resumed.current_state_hash == final_state_hash
            if not checkpoint_delta_equal:
                raise RuntimeError(
                    "market_book_replay_invalid: checkpoint-plus-delta differs from full replay"
                )

        replay_bbo = ()
        replay_depth = ()
        persisted_bbo = ()
        persisted_depth = ()
        feature_equal: Optional[bool] = None
        if replay_states:
            valuation = MarketStateValuationContract(
                product_definition_version_id=str(
                    config.get("product_definition_version_id") or ""
                ),
                provider_size_unit=str(config.get("provider_size_unit") or ""),
                base_currency=str(config.get("base_currency") or ""),
                quote_currency=str(config.get("quote_currency") or ""),
                contract_size=config.get("contract_size"),
            )
            replay_bbo, replay_depth = derive_book_features(
                replay_states,
                contract=valuation,
                bbo_series_id=int(config["bbo_series_id"]),
                depth_series_id=int(config["depth_series_id"]),
                computed_at=datetime.now(UTC),
            )
            if replay_bbo:
                feature_start = replay_bbo[0].bucket_start
                feature_end = replay_bbo[-1].bucket_end
                read_known_at = datetime.now(UTC)
                persisted_bbo = self.repository.read_bbo_features(
                    series_id=int(config["bbo_series_id"]),
                    start=feature_start,
                    end=feature_end,
                    known_at=read_known_at,
                )
                persisted_depth = self.repository.read_depth_features(
                    series_id=int(config["depth_series_id"]),
                    start=feature_start,
                    end=feature_end,
                    known_at=read_known_at,
                )
                replay_bbo_hashes = sorted(row.material_hash for row in replay_bbo)
                persisted_bbo_hashes = sorted(
                    row.material_hash for row in persisted_bbo
                )
                replay_depth_hashes = sorted(
                    row.material_hash for row in replay_depth
                )
                persisted_depth_hashes = sorted(
                    row.material_hash for row in persisted_depth
                )
                feature_equal = (
                    replay_bbo_hashes == persisted_bbo_hashes
                    and replay_depth_hashes == persisted_depth_hashes
                )
                if not feature_equal:
                    raise RuntimeError(
                        "market_book_replay_invalid: persisted features differ from raw replay"
                    )
        fingerprint = _stable_hash(
            {
                "schema_version": "market.book_session_replay.v1",
                "raw_record_ids": [row.raw_record_id for row in records],
                "snapshot_ids": snapshot_ids,
                "batch_ids": batch_ids,
                "final_state_hash": final_state_hash,
                "checkpoint_checks": checkpoint_checks,
                "bbo_feature_hashes": sorted(row.material_hash for row in replay_bbo),
                "depth_feature_hashes": sorted(row.material_hash for row in replay_depth),
                "execution_trade_version_ids": [
                    row.version_id for row in execution_trade_records
                ],
                "execution_trade_material_hashes": [
                    row.fact.material_hash for row in execution_trade_records
                ],
                "transport_quality_hashes": sorted(
                    str(row["evidence_hash"])
                    for row in quality_rows
                    if str(row["classification"]) in invalidating_classes
                ),
            }
        )
        execution_book_tape = None
        if execution_instrument_id is not None:
            execution_book_tape = _build_execution_book_tape_from_replay(
                states=replay_states,
                closing_validity=closing_validity,
                instrument_id=execution_instrument_id,
                replay_fingerprint=fingerprint,
                trade_records=execution_trade_records,
            ).to_dict()
        result = {
            "schema_version": "market.book_session_replay.v1",
            "definition_id": definition_id,
            "session_id": session_id,
            "raw_manifest_ids": [str(row["id"]) for row in manifests],
            "raw_record_count": len(records),
            "snapshot_count": len(snapshot_ids),
            "mutation_batch_count": len(batch_ids),
            "final_state_hash": final_state_hash,
            "checkpoint_count": len(checkpoint_checks),
            "checkpoint_delta_equal": checkpoint_delta_equal,
            "transport_invalidation_count": sum(
                1
                for row in quality_rows
                if str(row["classification"]) in invalidating_classes
            ),
            "replay_fingerprint": fingerprint,
            "reconciliation": reconciliation,
            "features": {
                "bbo_count": len(replay_bbo),
                "depth_count": len(replay_depth),
                "persisted_equal": feature_equal,
            },
        }
        if execution_book_tape is not None:
            result["execution_book_tape"] = execution_book_tape
            result["execution_book_tape_bundle"] = ExecutionBookTapeBundle(
                schema_version=EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
                tapes=(ExecutionBookTape.from_dict(execution_book_tape),),
            ).to_dict()
        return result

    def reconcile_recent_trades(
        self, *, definition_id: str, limit: int = 100
    ) -> dict[str, Any]:
        """Compare one bounded REST window with stored IDs; never claim backfill."""

        bounded_limit = max(1, min(int(limit), 1000))
        definitions = self.repository.list_stream_definitions(
            definition_id=definition_id
        )
        if len(definitions) != 1:
            raise ValueError(
                f"market_stream_definition_unknown: definition_id={definition_id}"
            )
        definition = definitions[0]
        provider = get_provider("COINBASE", venue="COINBASE_DIRECT")
        payload = provider.fetch_recent_market_trades_proof(
            str(definition["provider_product_id"]),
            auth_mode=str(definition["auth_mode"]),
            limit=bounded_limit,
        )
        raw_trades = payload.get("trades")
        if not isinstance(raw_trades, Sequence) or isinstance(
            raw_trades, (str, bytes)
        ):
            raise RuntimeError(
                "market_recent_trade_reconciliation_invalid: Coinbase response lacks trades"
            )
        trade_ids = [
            str(row.get("trade_id") or "").strip()
            for row in raw_trades
            if isinstance(row, Mapping) and str(row.get("trade_id") or "").strip()
        ]
        overlap = self.repository.recent_trade_id_overlap(
            definition_id=definition_id,
            provider_trade_ids=trade_ids,
        )
        return {
            "schema_version": "market.recent_trade_reconciliation.v1",
            "definition_id": definition_id,
            "provider_product_id": overlap["provider_product_id"],
            "auth_mode": str(definition["auth_mode"]),
            "rest_limit": bounded_limit,
            "rest_trade_count": len(trade_ids),
            "rest_unique_trade_count": len(set(trade_ids)),
            "canonical_overlap_count": len(
                overlap["canonical_overlap_trade_ids"]
            ),
            "rest_only_trade_ids": overlap["rest_only_trade_ids"],
            "historical_completeness_claim": "none",
            "interpretation": (
                "bounded diagnostic only; REST-only IDs may have occurred after the latest capture"
            ),
        }


class _CaptureAnalyzer:
    def __init__(self, claim: StreamClaim, *, primary_channel: str = "market_trades") -> None:
        self.claim = claim
        self.primary_channel = str(primary_channel)
        self.raw_count = 0
        self.raw_bytes = 0
        self.trade_events = 0
        self.snapshot_trades = 0
        self.update_trades = 0
        self.first_sequence: Optional[int] = None
        self.last_sequence: Optional[int] = None
        self.subscription_ack = False
        self.first_heartbeat_at: Optional[datetime] = None
        self.last_heartbeat_at: Optional[datetime] = None
        self.snapshot = False
        self.baseline_ready_ordinal: Optional[int] = None
        self.coverage_opening_ordinal: Optional[int] = None
        self.coverage_opening_effective_at: Optional[datetime] = None
        self.coverage_first_sequence_num: Optional[int] = None
        self.coverage_last_ordinal: Optional[int] = None
        self.coverage_last_effective_at: Optional[datetime] = None
        self.coverage_last_sequence_num: Optional[int] = None
        self.sequence_hashes: dict[int, str] = {}
        self.quality: list[dict[str, Any]] = []
        self.last_record: Optional[RawStreamRecord] = None

    def observe(
        self, record: RawStreamRecord, events: Sequence[CanonicalMarketEvent]
    ) -> None:
        self.raw_count += 1
        self.raw_bytes += len(record.raw_frame)
        self.last_record = record
        message_sequence: Optional[int] = None
        try:
            payload = json.loads(record.raw_frame)
            if isinstance(payload, Mapping) and isinstance(payload.get("sequence_num"), int):
                message_sequence = int(payload["sequence_num"])
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        if message_sequence is not None:
            self.first_sequence = (
                message_sequence if self.first_sequence is None else self.first_sequence
            )
            self.last_sequence = max(message_sequence, self.last_sequence or message_sequence)
            prior_hash = self.sequence_hashes.get(message_sequence)
            if prior_hash is None:
                self.sequence_hashes[message_sequence] = record.raw_frame_sha256
                if len(self.sequence_hashes) > MAX_ANALYZER_SEQUENCE_HASHES:
                    self.sequence_hashes.pop(next(iter(self.sequence_hashes)))
            elif prior_hash != record.raw_frame_sha256:
                self._quality(
                    record,
                    classification="divergent_duplicate",
                    reason="same connection sequence carried different raw bytes",
                    invalidating=True,
                    sequence_before=message_sequence,
                    sequence_after=message_sequence,
                )
        for event in events:
            if event.event_kind == "provider_subscription_ack":
                subscriptions = json.dumps(dict(event.payload or {})).lower()
                if self.primary_channel in subscriptions:
                    self.subscription_ack = True
            elif event.event_kind == "provider_heartbeat":
                now = record.received_at
                self.first_heartbeat_at = self.first_heartbeat_at or now
                self.last_heartbeat_at = now
            elif event.event_kind == "provider_sequence_gap":
                status = str(event.payload.get("status") or "")
                classification = {
                    "gap": "sequence_gap",
                    "out_of_order": "out_of_order",
                    "duplicate": "duplicate",
                }.get(status, "sequence_gap")
                # A divergent duplicate is already classified from exact bytes.
                invalidating = classification in {"sequence_gap", "out_of_order"}
                self._quality(
                    record,
                    classification=classification,
                    reason=f"provider connection sequence status={status}",
                    invalidating=invalidating,
                    sequence_before=event.payload.get("previous_sequence_num"),
                    sequence_after=event.payload.get("current_sequence_num"),
                )
            elif event.event_kind == "provider_malformed_message":
                self._quality(
                    record,
                    classification="decode_error",
                    reason=str(event.payload.get("error") or "provider frame decode failed"),
                    invalidating=True,
                )
            elif event.event_kind == "market_trade":
                self.trade_events += 1
                delivery = str(event.payload.get("type") or "").lower()
                if delivery == "snapshot":
                    self.snapshot = True
                    self.snapshot_trades += 1
                elif delivery == "update":
                    self.update_trades += 1
            elif event.event_kind == "market_l2_snapshot":
                self.snapshot = True
        if self._baseline_ready(record.receive_ordinal) and self.baseline_ready_ordinal is None:
            self.baseline_ready_ordinal = record.receive_ordinal
        # Trade coverage is a connection-delivery assertion, not an assertion
        # that a trade occurred. Once subscription, heartbeat, and initial
        # snapshot evidence are all present, every subsequently received frame
        # advances the healthy interval. This is what makes an explicitly empty
        # bucket distinguishable from a missed or unhealthy stream.
        if self.baseline_ready_ordinal is not None:
            if self.coverage_opening_ordinal is None:
                self.coverage_opening_ordinal = record.receive_ordinal
                self.coverage_opening_effective_at = record.received_at
                self.coverage_first_sequence_num = message_sequence
            self.coverage_last_ordinal = record.receive_ordinal
            self.coverage_last_effective_at = record.received_at
            self.coverage_last_sequence_num = message_sequence

    def _baseline_ready(self, current_ordinal: int) -> bool:
        return bool(
            self.subscription_ack
            and self.first_heartbeat_at is not None
            and self.snapshot
            and (self.baseline_ready_ordinal is None or current_ordinal >= self.baseline_ready_ordinal)
        )

    def observe_idle(self, now: datetime) -> None:
        if (
            self.last_heartbeat_at is not None
            and (now - self.last_heartbeat_at).total_seconds() > 5
            and self.last_record is not None
            and not any(item["classification"] == "heartbeat_gap" for item in self.quality)
        ):
            self._quality(
                self.last_record,
                classification="heartbeat_gap",
                reason="no heartbeat received for more than five seconds",
                invalidating=True,
            )

    def unexpected_disconnect(self, now: datetime) -> None:
        if self.last_record is None:
            return
        self._quality(
            self.last_record,
            classification="disconnect",
            reason=f"stream ended before bounded deadline at {now.isoformat()}",
            invalidating=True,
        )

    def _quality(
        self,
        record: RawStreamRecord,
        *,
        classification: str,
        reason: str,
        invalidating: bool,
        sequence_before: Optional[int] = None,
        sequence_after: Optional[int] = None,
    ) -> None:
        material = {
            "classification": classification,
            "connection_epoch": record.connection_epoch,
            "receive_ordinal": record.receive_ordinal,
            "raw_record_id": record.raw_record_id,
            "sequence_before": sequence_before,
            "sequence_after": sequence_after,
        }
        if any(
            item.get("dedupe_hash") == _stable_hash(material) for item in self.quality
        ):
            return
        self.quality.append(
            {
                "dedupe_hash": _stable_hash(material),
                "connection_epoch": record.connection_epoch,
                "receive_ordinal": record.receive_ordinal,
                "channel": record.observed_channel,
                "classification": classification,
                "reason": reason,
                "detected_at": record.received_at,
                "raw_record_id": record.raw_record_id,
                "sequence_before": sequence_before,
                "sequence_after": sequence_after,
                "invalidating": invalidating,
                "evidence": {"raw_frame_sha256": record.raw_frame_sha256},
            }
        )

    def finalize(self) -> CaptureAnalysis:
        heartbeat_healthy = bool(
            self.first_heartbeat_at is not None
            and not any(item["classification"] == "heartbeat_gap" for item in self.quality)
        )
        return CaptureAnalysis(
            raw_record_count=self.raw_count,
            raw_bytes=self.raw_bytes,
            trade_event_count=self.trade_events,
            snapshot_trade_count=self.snapshot_trades,
            update_trade_count=self.update_trades,
            first_sequence_num=self.first_sequence,
            last_sequence_num=self.last_sequence,
            subscription_acknowledged=self.subscription_ack,
            heartbeat_healthy=heartbeat_healthy,
            snapshot_accepted=self.snapshot,
            coverage_opening_ordinal=self.coverage_opening_ordinal,
            coverage_opening_effective_at=self.coverage_opening_effective_at,
            coverage_first_sequence_num=self.coverage_first_sequence_num,
            coverage_last_ordinal=self.coverage_last_ordinal,
            coverage_last_effective_at=self.coverage_last_effective_at,
            coverage_last_sequence_num=self.coverage_last_sequence_num,
            quality_events=tuple(self.quality),
        )


market_structure_service = MarketStructureService()


__all__ = [
    "DEFAULT_SEGMENT_BYTES",
    "DEFAULT_SPOOL_BYTES",
    "DEFAULT_STORAGE_ROOT",
    "MarketStructureService",
    "PHASE1_PAIRS",
    "market_structure_service",
]

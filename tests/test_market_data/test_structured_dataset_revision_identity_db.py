from __future__ import annotations

import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from types import SimpleNamespace

import pytest

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    publish_spool_archive,
)
from market_data.canonical import (
    CanonicalFact,
    FactState,
    build_canonical_fact_provenance_hash,
    build_canonical_fact_series_material_hash,
)
from market_data.canonical_adapters import (
    canonicalize_bbo_feature,
    canonicalize_depth_feature,
)
from market_data.contracts import DatasetSeriesRequest, SourceIdentity
from market_data.market_state import (
    BBO_FACT_TYPE,
    BBO_FACT_VERSION,
    DEPTH_FACT_TYPE,
    DEPTH_FACT_VERSION,
    BboFeatureFact,
    DepthFeatureFact,
)
from market_data.order_book import BookSourcePosition
from market_data.structure import ProviderSizeUnit, RawStreamRecord
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market import frozen_dataset_service
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver
from portal.backend.service.research.event_fact_evaluator import _causal_fact_records
from portal.backend.service.storage.repos.market_data import market_data_repo
from portal.backend.service.storage.repos.market_structure import (
    market_structure_repository,
)


pytestmark = pytest.mark.db

_OBSERVED_AT = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
_RANGE_START = _OBSERVED_AT - timedelta(minutes=1)
_RANGE_END = _OBSERVED_AT + timedelta(hours=1)


def _instrument_snapshot(instrument_id: str, symbol: str) -> dict[str, Any]:
    return {
        "id": instrument_id,
        "symbol": symbol,
        "datasource": "CANONICAL",
        "exchange": "ARBITRUM",
        "instrument_type": "reference_asset",
    }


def _reserve_fact(
    *,
    source: SourceIdentity,
    observation_key: str,
    quantity: str,
    known_at: datetime,
    state: FactState = FactState.ACTIVE,
) -> CanonicalFact:
    return CanonicalFact(
        fact_type="asset.reserve_state",
        payload_schema_id="asset.reserve_state.v1",
        observation_key=observation_key,
        observation_time=_OBSERVED_AT,
        observation_time_method="chainlink_latest_bundle_timestamp",
        source_published_at=_OBSERVED_AT,
        received_at=known_at,
        accepted_at=known_at,
        known_at=known_at,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="structured_dataset_revision_fixture.v1",
        payload={
            "report_id": "DE000NXTA018",
            "reserve_asset": "BTC",
            "reserve_quantity": Decimal(quantity),
            "unit": "BTC",
        },
        external_event_key=observation_key,
        state=state,
        provenance={"fixture_revision": quantity, "fixture_state": state.value},
    )


def _series_identity(entry: dict[str, Any]) -> dict[str, Any]:
    identity = {
        "identity_key": str(entry["identity_key"]),
        "instrument_id": str(entry["instrument_id"]),
        "fact_type": str(entry["fact_type"]),
        "timeframe_seconds": entry.get("timeframe_seconds"),
        "contract_version": str(entry["contract_version"]),
    }
    dimensions = dict(entry.get("dimensions") or {})
    if dimensions:
        identity["dimensions"] = dimensions
    return identity


def _fixture_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _book_source_position(
    *,
    definition_id: str,
    session_id: str,
    receive_ordinal: int,
    connection_epoch: int = 0,
) -> BookSourcePosition:
    return BookSourcePosition(
        definition_id=definition_id,
        session_id=session_id,
        connection_epoch=connection_epoch,
        provider_product_id="BTC-USD",
        provider_sequence_num=receive_ordinal,
        receive_ordinal=receive_ordinal,
        event_ordinal=0,
    )


def _book_feature_revisions(
    *,
    source: SourceIdentity,
    l2_series_id: int,
    bbo_series_id: int,
    depth_series_id: int,
    definition_id: str,
    session_id: str,
    receive_ordinal: int,
    connection_epoch: int = 0,
    known_at_offset_seconds: int | None = None,
    state: FactState = FactState.ACTIVE,
) -> tuple[CanonicalFact, CanonicalFact]:
    position = _book_source_position(
        definition_id=definition_id,
        session_id=session_id,
        receive_ordinal=receive_ordinal,
        connection_epoch=connection_epoch,
    )
    bucket_end = _OBSERVED_AT + timedelta(seconds=1)
    source_effective_at = _OBSERVED_AT + timedelta(
        milliseconds=receive_ordinal * 100
    )
    known_at_offset = (
        receive_ordinal
        if known_at_offset_seconds is None
        else known_at_offset_seconds
    )
    known_at = bucket_end + timedelta(seconds=known_at_offset)
    bid_price = Decimal("100") + receive_ordinal
    ask_price = bid_price + Decimal("2")
    mid_price = (bid_price + ask_price) / Decimal("2")
    spread = ask_price - bid_price
    bbo_input_fingerprint = _fixture_hash(
        f"bbo-input:{session_id}:{connection_epoch}:{receive_ordinal}"
    )
    bbo = BboFeatureFact(
        series_id=bbo_series_id,
        source_l2_series_id=l2_series_id,
        bucket_start=_OBSERVED_AT,
        bucket_end=bucket_end,
        source_effective_at=source_effective_at,
        known_at=known_at,
        source_position=position,
        validity_interval_id=f"validity-{session_id}",
        product_definition_version_id=f"product-{session_id}",
        provider_size_unit=ProviderSizeUnit.BASE,
        source_state_hash=_fixture_hash(
            f"book-state:{session_id}:{connection_epoch}:{receive_ordinal}"
        ),
        bid_price=bid_price,
        bid_quantity=Decimal("3"),
        bid_base_quantity=Decimal("3"),
        ask_price=ask_price,
        ask_quantity=Decimal("1"),
        ask_base_quantity=Decimal("1"),
        mid_price=mid_price,
        spread=spread,
        spread_bps=Decimal("10000") * spread / mid_price,
        input_fingerprint=bbo_input_fingerprint,
    )
    depth = DepthFeatureFact(
        series_id=depth_series_id,
        source_l2_series_id=l2_series_id,
        bucket_start=_OBSERVED_AT,
        bucket_end=bucket_end,
        source_effective_at=source_effective_at,
        known_at=known_at,
        source_position=position,
        validity_interval_id=f"validity-{session_id}",
        source_state_hash=bbo.source_state_hash,
        bbo_input_fingerprint=bbo_input_fingerprint,
        provider_size_unit=ProviderSizeUnit.BASE,
        band_bps=10,
        mid_price=mid_price,
        bid_quantity=Decimal("3"),
        ask_quantity=Decimal("1"),
        bid_base_quantity=Decimal("3"),
        ask_base_quantity=Decimal("1"),
        bid_notional=Decimal("3") * mid_price,
        ask_notional=mid_price,
        imbalance=Decimal("0.5"),
        input_fingerprint=_fixture_hash(
            f"depth-input:{session_id}:{connection_epoch}:{receive_ordinal}"
        ),
    )
    return (
        replace(canonicalize_bbo_feature(bbo, source=source), state=state),
        replace(canonicalize_depth_feature(depth, source=source), state=state),
    )


def _commit_book_source_archives(
    *,
    tmp_path: Path,
    claim: Any,
    receive_ordinals: tuple[int, ...],
) -> tuple[str, ...]:
    object_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest_ids: list[str] = []
    for receive_ordinal in receive_ordinals:
        spool = DurableRawSpoolSegment(
            root=tmp_path / "spool",
            definition_id=claim.definition_id,
            session_id=claim.session_id,
            connection_epoch=0,
            segment_ordinal=receive_ordinal - 1,
        )
        received_at = _OBSERVED_AT + timedelta(milliseconds=receive_ordinal)
        message = ProviderRawMessage.build(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            stream_session_id=claim.session_id,
            connection_epoch=0,
            receive_ordinal=receive_ordinal,
            received_at=received_at.isoformat(),
            raw_frame=(
                '{"channel":"l2_data","fixture_receive_ordinal":'
                f"{receive_ordinal}}}"
            ),
        )
        raw_record = RawStreamRecord.from_provider_message(
            message,
            definition_id=claim.definition_id,
            spool_segment_id=spool.spool_segment_id,
            provider_product_id="BTC-USD",
            requested_channel="level2",
            observed_channel="level2",
        )
        spool.append(raw_record)
        spool.seal()
        encoded, acknowledgement, records = publish_spool_archive(
            spool,
            object_store=object_store,
            temporary_directory=tmp_path / "tmp",
        )
        committed = market_structure_repository.commit_archive(
            claim,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=records,
        )
        manifest_ids.append(committed.manifest_id)
    return tuple(manifest_ids)


def test_structured_dataset_pins_all_causal_revisions_and_excludes_post_freeze_change() -> None:
    token = uuid.uuid4().hex
    instrument_id = f"revision-proof-{token[:17]}"
    symbol = f"RVP-{token[:8].upper()}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="CANONICAL",
                exchange="ARBITRUM",
                symbol=symbol,
                instrument_type="reference_asset",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={"fixture": "structured-dataset-revision-identity"},
            )
        )

    source = SourceIdentity(
        provider="CHAINLINK",
        venue="ARBITRUM_MAINNET",
        source_kind="mvr_proxy",
        adapter_version=f"structured.dataset.revisions.{token}",
    )
    source_id = market_data_repo.register_source(
        source,
        lineage={"fixture": "structured-dataset-revision-identity"},
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="asset.reserve_state",
        timeframe_seconds=None,
        contract_version="asset.reserve_state.v1",
        dimensions={"reserve_asset": "BTC"},
    )
    observation_key = f"arbitrum:42161:report:{token}"
    revision_facts = (
        _reserve_fact(
            source=source,
            observation_key=observation_key,
            quantity="100",
            known_at=_OBSERVED_AT + timedelta(seconds=10),
        ),
        _reserve_fact(
            source=source,
            observation_key=observation_key,
            quantity="125",
            known_at=_OBSERVED_AT + timedelta(seconds=20),
        ),
        _reserve_fact(
            source=source,
            observation_key=observation_key,
            quantity="125",
            known_at=_OBSERVED_AT + timedelta(seconds=30),
            state=FactState.INVALIDATED,
        ),
    )
    for expected_revision, fact in enumerate(revision_facts, start=1):
        outcome = market_data_repo.ingest_facts(
            series_id=series_id,
            source_id=source_id,
            facts=(fact,),
            request={
                "fixture": "structured-dataset-revision-identity",
                "revision": expected_revision,
            },
        )
        assert outcome.inserted_count == (1 if expected_revision == 1 else 0)
        assert outcome.corrected_count == (0 if expected_revision == 1 else 1)

    requirement = {
        "key": "reserve_state",
        "alias": "reserve_state",
        "consumer_id": "check",
        "instrument_id": instrument_id,
        "fact_type": "asset.reserve_state",
        "contract_version": "asset.reserve_state.v1",
        "timeframe_seconds": None,
        "dimensions": {"reserve_asset": "BTC"},
        "alignment": "latest_known",
        "max_staleness_seconds": 3600,
        "required": True,
        "known_at_required": True,
        "required_fields": ["reserve_quantity", "known_at"],
        "required_start": _RANGE_START.isoformat(),
        "required_end": _RANGE_END.isoformat(),
        "source_policy": {
            "mode": "exact",
            "source_identity_key": source.identity_key,
        },
    }
    frozen = market_data_repo.freeze_dataset(
        (
            DatasetSeriesRequest(
                series_id=series_id,
                start=_RANGE_START,
                end=_RANGE_END,
            ),
        ),
        name=f"Structured revision proof {token[:8]}",
        purpose="test",
        created_by="pytest",
        metadata={"fixture": "structured-dataset-revision-identity"},
    )
    binding = frozen_dataset_service.resolve_frozen_dataset_read_binding(
        dataset_id=frozen.dataset_id,
        requirements=(requirement,),
        store=market_data_repo,
        instrument_loader=lambda value: _instrument_snapshot(value, symbol),
    )
    dataset_id = frozen.dataset_id
    entry = dict(market_data_repo.get_dataset(dataset_id).series[0])
    assert entry["row_count"] == 3

    frozen_revisions = market_data_repo.read_dataset_fact_revisions(
        dataset_id=dataset_id,
        series_id=series_id,
    )
    assert [record.revision for record in frozen_revisions] == [1, 2, 3]
    assert [record.fact.state for record in frozen_revisions] == [
        FactState.ACTIVE,
        FactState.ACTIVE,
        FactState.INVALIDATED,
    ]
    assert entry["material_hash"] == build_canonical_fact_series_material_hash(
        series_identity=_series_identity(entry),
        records=frozen_revisions,
    )
    assert entry["provenance_hash"] == build_canonical_fact_provenance_hash(
        frozen_revisions
    )

    post_freeze = _reserve_fact(
        source=source,
        observation_key=observation_key,
        quantity="150",
        known_at=_OBSERVED_AT + timedelta(seconds=40),
    )
    post_freeze_outcome = market_data_repo.ingest_facts(
        series_id=series_id,
        source_id=source_id,
        facts=(post_freeze,),
        request={
            "fixture": "structured-dataset-revision-identity",
            "revision": 4,
        },
    )
    assert post_freeze_outcome.corrected_count == 1
    assert [
        record.revision
        for record in market_data_repo.read_fact_revisions(
            series_id=series_id,
            start=_RANGE_START,
            end=_RANGE_END,
        )
    ] == [1, 2, 3, 4]

    resolver = RuntimeMarketDataResolver(
        store=market_data_repo,
        dataset_binding=binding,
    )

    def causal_history_at(offset_seconds: int):
        return resolver.causal_history(
            consumer_id="check",
            requirement=requirement,
            primary_instrument_id=instrument_id,
            start=_RANGE_START,
            end=_RANGE_END,
            evaluation_time=_OBSERVED_AT + timedelta(seconds=offset_seconds),
        )

    before_correction = causal_history_at(15)
    assert [record.revision for record in before_correction] == [1]
    assert _causal_fact_records(
        before_correction,
        evaluation_time=_OBSERVED_AT + timedelta(seconds=15),
    )[0].fact.payload["reserve_quantity"] == "100"

    after_correction = causal_history_at(25)
    assert [record.revision for record in after_correction] == [1, 2]
    assert _causal_fact_records(
        after_correction,
        evaluation_time=_OBSERVED_AT + timedelta(seconds=25),
    )[0].fact.payload["reserve_quantity"] == "125"

    after_tombstone = causal_history_at(35)
    assert [record.revision for record in after_tombstone] == [1, 2, 3]
    assert _causal_fact_records(
        after_tombstone,
        evaluation_time=_OBSERVED_AT + timedelta(seconds=35),
    ) == ()

    after_post_freeze_change = causal_history_at(45)
    assert [record.revision for record in after_post_freeze_change] == [1, 2, 3]
    assert _causal_fact_records(
        after_post_freeze_change,
        evaluation_time=_OBSERVED_AT + timedelta(seconds=45),
    ) == ()
    assert market_data_repo.get_dataset(dataset_id).dataset_hash == frozen.dataset_hash


@pytest.mark.parametrize("cooled", [False, True])
def test_book_feature_revision_history_pins_every_source_archive_and_fails_loud(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cooled: bool,
) -> None:
    token = uuid.uuid4().hex
    instrument_id = f"book-lineage-{token[:18]}"
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    if cooled:
        # A private placement day prevents this destructive fixture from
        # touching any other test's canonical payload partition.
        from tests.test_market_data.test_fact_storage_tiers_db import _placement, _verified_cold_fixture
        _placement(monkeypatch, date(1900, 1, 1))
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol=f"BKL-{token[:8].upper()}",
                instrument_type="spot",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={"fixture": "book-feature-revision-lineage"},
            )
        )

    source = SourceIdentity(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        source_kind="stream",
        adapter_version=f"book.feature.revision.lineage.{token}",
    )
    source_id = market_data_repo.register_source(
        source,
        lineage={"fixture": "book-feature-revision-lineage"},
    )
    l2_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.l2_book",
        timeframe_seconds=None,
        contract_version="market.l2_book.v1",
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
    definition_id = f"book_lineage_{token}"
    market_structure_repository.upsert_stream_definition(
        definition_id=definition_id,
        source_id=source_id,
        series_id=l2_series_id,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("level2",),
        auth_mode="public",
        contract_version="market.l2_book.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=128 * 1024**2,
        config={"fixture": "book-feature-revision-lineage"},
    )
    claim = market_structure_repository.claim_stream(
        definition_id=definition_id,
        owner_id=f"book-lineage-db-test-{token}",
        lease_seconds=600,
        bounded=True,
    )
    manifest_ids = _commit_book_source_archives(
        tmp_path=tmp_path,
        claim=claim,
        receive_ordinals=(1, 2, 3),
    )

    for revision in (1, 2, 3):
        state = FactState.INVALIDATED if revision == 3 else FactState.ACTIVE
        bbo_fact, depth_fact = _book_feature_revisions(
            source=source,
            l2_series_id=l2_series_id,
            bbo_series_id=bbo_series_id,
            depth_series_id=depth_series_id,
            definition_id=definition_id,
            session_id=claim.session_id,
            receive_ordinal=revision,
            state=state,
        )
        for series_id, fact in (
            (bbo_series_id, bbo_fact),
            (depth_series_id, depth_fact),
        ):
            outcome = market_data_repo.ingest_facts(
                series_id=series_id,
                source_id=source_id,
                facts=(fact,),
                request={
                    "fixture": "book-feature-revision-lineage",
                    "revision": revision,
                },
            )
            assert outcome.inserted_count == (1 if revision == 1 else 0)
            assert outcome.corrected_count == (0 if revision == 1 else 1)

    frozen = market_data_repo.freeze_dataset(
        (
            DatasetSeriesRequest(
                series_id=bbo_series_id,
                start=_RANGE_START,
                end=_RANGE_END,
            ),
            DatasetSeriesRequest(
                series_id=depth_series_id,
                start=_RANGE_START,
                end=_RANGE_END,
            ),
        ),
        name=f"Book revision lineage {token[:8]}",
        purpose="test",
        created_by="pytest",
        metadata={"fixture": "book-feature-revision-lineage"},
    )
    series_entries = {int(entry["series_id"]): entry for entry in frozen.series}
    additional_pin = 0
    if cooled:
        _verified_cold_fixture(SimpleNamespace(database=db, today=date(1900, 1, 1)), tmp_path, monkeypatch)
        refrozen = market_data_repo.freeze_dataset(
            (DatasetSeriesRequest(bbo_series_id, _RANGE_START, _RANGE_END),
             DatasetSeriesRequest(depth_series_id, _RANGE_START, _RANGE_END)),
            name=f"Book revision lineage {token[:8]}", purpose="test", created_by="pytest",
            metadata={"fixture": "book-feature-revision-lineage"},
        )
        assert refrozen.dataset_hash == frozen.dataset_hash
        additional_pin = int(refrozen.dataset_id != frozen.dataset_id)
        _placement(monkeypatch, date(1900, 1, 2))
    assert series_entries[bbo_series_id]["row_count"] == 3
    assert series_entries[depth_series_id]["row_count"] == 3

    for series_id, evidence_key in (
        (bbo_series_id, "_qt_bbo_evidence"),
        (depth_series_id, "_qt_depth_evidence"),
    ):
        revisions = market_data_repo.read_dataset_fact_revisions(
            dataset_id=frozen.dataset_id,
            series_id=series_id,
        )
        assert [record.revision for record in revisions] == [1, 2, 3]
        from portal.backend.service.storage.repos.market_data import _load_lineage_material_rows, _collect_typed_archive_refs
        material_hashes = [record.fact.provenance[evidence_key]["legacy_material_hash"] for record in revisions]
        with db.session() as session:
            witnesses = _load_lineage_material_rows(
                session, series_id=series_id, fact_type=revisions[0].fact.fact_type,
                material_hashes=material_hashes, evidence_key=evidence_key,
            )
            for record, material_hash in zip(revisions, material_hashes, strict=True):
                assert witnesses[material_hash]["fact_version_id"] == record.fact_version_id
                assert witnesses[material_hash]["provenance"] == record.fact.provenance
            references = _collect_typed_archive_refs(session, records=[
                SimpleNamespace(series_id=series_id, fact=SimpleNamespace(material_hash=value))
                for value in material_hashes
            ])
            assert set(references) == set(manifest_ids)
        assert [record.fact.state for record in revisions] == [
            FactState.ACTIVE,
            FactState.ACTIVE,
            FactState.INVALIDATED,
        ]
        assert [
            int(
                record.fact.provenance[evidence_key]["source_position"][
                    "connection_epoch"
                ]
            )
            for record in revisions
        ] == [0, 0, 0]
        assert [
            int(
                record.fact.provenance[evidence_key]["source_position"][
                    "receive_ordinal"
                ]
            )
            for record in revisions
        ] == [1, 2, 3]

    pinned_manifest_ids = {
        str(reference["raw_archive_manifest_id"])
        for reference in frozen.metadata["archive_refs"]
    }
    assert pinned_manifest_ids == set(manifest_ids)
    for manifest_id in manifest_ids:
        retention = market_structure_repository.archive_retention_status(
            target_kind="raw_manifest",
            target_id=manifest_id,
        )
        assert retention["dataset_pin_count"] == 1 + additional_pin
        assert retention["pinned"] is True

    reconnect_without_archive_bbo, _reconnect_without_archive_depth = (
        _book_feature_revisions(
            source=source,
            l2_series_id=l2_series_id,
            bbo_series_id=bbo_series_id,
            depth_series_id=depth_series_id,
            definition_id=definition_id,
            session_id=claim.session_id,
            receive_ordinal=1,
            connection_epoch=1,
            known_at_offset_seconds=4,
        )
    )
    reconnect_position = reconnect_without_archive_bbo.provenance[
        "_qt_bbo_evidence"
    ]["source_position"]
    assert reconnect_position["connection_epoch"] == 1
    assert reconnect_position["receive_ordinal"] == 1
    missing_outcome = market_data_repo.ingest_facts(
        series_id=bbo_series_id,
        source_id=source_id,
        facts=(reconnect_without_archive_bbo,),
        request={
            "fixture": "book-feature-revision-lineage-unarchived-reconnect",
            "revision": 4,
        },
    )
    assert missing_outcome.corrected_count == 1
    with pytest.raises(
        RuntimeError,
        match=(
            "market_dataset_archive_incomplete: canonical book source position "
            "has no acknowledged archive"
        ),
    ):
        market_data_repo.freeze_dataset(
            (
                DatasetSeriesRequest(
                    series_id=bbo_series_id,
                    start=_RANGE_START,
                    end=_RANGE_END,
                ),
            ),
            purpose="test",
            created_by="pytest",
        )
    market_structure_repository.release(claim)

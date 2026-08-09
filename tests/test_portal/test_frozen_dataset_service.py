from __future__ import annotations

from types import SimpleNamespace

import pytest

from market_data.contracts import build_dataset_identity_hash
from market_data.frozen import semantic_hash
from market_data.store import FrozenDataset
from portal.backend.service.market import frozen_dataset_service


def _entry() -> dict:
    return {
        "series_id": 9,
        "identity_key": "series-9",
        "instrument_id": "instrument-1",
        "fact_type": "market.reference_price",
        "contract_version": "market.reference_price.v1",
        "timeframe_seconds": None,
        "dimensions": {"quote_currency": "USD"},
        "range_start": "2026-01-01T00:00:00Z",
        "range_end": "2026-02-01T00:00:00Z",
        "max_commit_seq": 77,
        "row_count": 20,
        "material_hash": "material",
        "provenance_hash": "provenance",
        "quality_hash": "quality",
        "source_summary": {
            "counts": {"source-a": 10, "source-b": 10},
            "sources": {
                "source-a": {"provider": "provider-a", "venue": "venue"},
                "source-b": {"provider": "provider-b", "venue": "venue"},
            },
        },
        "quality_summary": {
            "evidence_count": 1,
            "classifications": {"provider_missing_data": 1},
        },
    }


class _Store:
    def __init__(self):
        entry = _entry()
        material = frozen_dataset_service.dataset_manifest_hash_payload(entry)
        dataset_hash = build_dataset_identity_hash([material])
        self.dataset = FrozenDataset(
            dataset_id="mds_" + dataset_hash[:32],
            dataset_hash=dataset_hash,
            max_commit_seq=77,
            series=(entry,),
        )

    def get_dataset(self, dataset_id):
        assert dataset_id == self.dataset.dataset_id
        return self.dataset

    def list_gap_evidence(self, **_kwargs):
        return [
            {
                "classification": "provider_missing_data",
                "start": "2026-01-10T00:00:00Z",
                "end": "2026-01-10T01:00:00Z",
                "source_identity_key": "source-a",
            }
        ]

    def list_source_acquisition_coverage(self, **kwargs):
        rows = []
        for index, source_key in enumerate(kwargs["source_identity_keys"], start=1):
            material = {
                "schema_version": "market.fact_acquisition_coverage.v1",
                "series_id": int(kwargs["series_id"]),
                "source_id": index,
                "binding_id": f"binding-{index}",
                "manifest_hash": "a" * 64,
                "interface_version": "test.v1",
                "confirmation_depth": 12,
                "range_start": "2026-01-01T00:00:00.000000Z",
                "range_end": "2026-02-01T00:00:00.000000Z",
                "source_positions": {
                    "start": "1",
                    "end": "2",
                    "head": "2",
                },
                "status": "complete",
                "evidence": {"response_count": 0},
            }
            rows.append(
                {
                    **material,
                    "identity_key": semantic_hash(material),
                    "source_identity_key": source_key,
                    "source_position_start": "1",
                    "source_position_end": "2",
                    "source_position_head": "2",
                    "created_at": "2026-02-01T00:00:00.000000Z",
                }
            )
        return rows


def _requirement(alias: str, source: str | None) -> dict:
    policy = {"mode": "exact"}
    if source:
        policy["source_identity_key"] = source
    return {
        "alias": alias,
        "instrument_id": "instrument-1",
        "fact_type": "market.reference_price",
        "contract_version": "market.reference_price.v1",
        "timeframe_seconds": None,
        "dimensions": {"quote_currency": "USD"},
        "required_start": "2026-01-02T00:00:00Z",
        "required_end": "2026-01-31T00:00:00Z",
        "source_policy": policy,
    }


def test_frozen_dataset_can_bind_same_fact_under_two_exact_provider_aliases(
    monkeypatch,
) -> None:
    store = _Store()

    def fake_validate(*, entry, **_kwargs):
        return (
            {
                **entry,
                "quality_evidence": [
                    {
                        "classification": "provider_missing_data",
                        "start": "2026-01-10T00:00:00Z",
                        "end": "2026-01-10T01:00:00Z",
                        "source_identity_key": "source-a",
                    }
                ],
            },
            [],
            [object()],
        )

    monkeypatch.setattr(
        frozen_dataset_service, "validate_frozen_dataset_series", fake_validate
    )
    binding = frozen_dataset_service.resolve_frozen_dataset_read_binding(
        dataset_id=store.dataset.dataset_id,
        requirements=(
            _requirement("reference_a", "source-a"),
            _requirement("reference_b", "source-b"),
        ),
        store=store,
        instrument_loader=lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH-USD",
            "datasource": "canonical",
            "exchange": "canonical",
        },
    )

    assert [row["alias"] for row in binding["series"]] == [
        "reference_a",
        "reference_b",
    ]
    assert binding["series"][0]["source_binding"][
        "resolved_source_identity_keys"
    ] == ["source-a"]
    assert binding["series"][1]["source_binding"][
        "resolved_source_identity_keys"
    ] == ["source-b"]
    assert binding["provider_access"] == "disabled"
    assert binding["recorded_gaps"]


def test_exact_policy_rejects_ambiguous_provider_binding(monkeypatch) -> None:
    store = _Store()
    monkeypatch.setattr(
        frozen_dataset_service,
        "validate_frozen_dataset_series",
        lambda **kwargs: (kwargs["entry"], [], [object()]),
    )

    with pytest.raises(ValueError, match="did not resolve one provider binding"):
        frozen_dataset_service.resolve_frozen_dataset_read_binding(
            dataset_id=store.dataset.dataset_id,
            requirements=(_requirement("reference", None),),
            store=store,
            instrument_loader=lambda instrument_id: {"id": instrument_id},
        )


def test_preparation_accepts_repository_series_id_projection() -> None:
    source = SimpleNamespace(
        identity_key="source-a",
        provider="provider-a",
        venue="venue",
        source_kind="test",
        adapter_version="test.v1",
    )

    class PreparationStore:
        def current_commit_seq(self):
            return 77

        def list_series(self, *, instrument_id=None):
            return [
                {
                    "id": 9,
                    "series_id": None,
                    "identity_key": "series-9",
                    "instrument_id": instrument_id,
                    "fact_type": "market.reference_price",
                    "contract_version": "market.reference_price.v1",
                    "timeframe_seconds": None,
                    "dimensions": {"quote_currency": "USD"},
                }
            ]

        def read_series_records(self, **_kwargs):
            return [
                SimpleNamespace(
                    source_identity_key="source-a", source=source
                )
            ]

    prepared = frozen_dataset_service.prepare_frozen_dataset_from_requirements(
        requirements=(_requirement("reference", "source-a"),),
        freeze=False,
        store=PreparationStore(),
    )

    assert prepared["status"] == "ready_to_freeze"
    assert prepared["resolved_requirements"][0]["series_id"] == 9

from __future__ import annotations

from datetime import UTC, datetime
import json
from types import SimpleNamespace
from typing import Any

import pytest

from market_data.canonical_storage import LEGACY_MATERIAL_EVIDENCE_KEYS
from market_data.fact_registry import get_fact_payload_schema

from portal.backend.service.storage.repos.market_data import (
    _LINEAGE_QUERY_BATCH_SIZE,
    _collect_canonical_book_archive_refs,
    _collect_typed_archive_refs,
    _load_lineage_material_rows,
    _lineage_values_context,
)


class _RowsResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = []
        for raw in rows:
            row = dict(raw)
            if "lineage_material_hash" in row:
                # Model the canonical hot row selected by the real SQL; these
                # tests exercise traversal batching, not archive file decoding.
                schema = get_fact_payload_schema(row["fact_type"] + ".v1")
                row.update(id=row["fact_version_id"], material_hash=row["lineage_material_hash"],
                           payload_schema_id=schema.schema_id, payload_contract_hash=schema.contract_hash)
                key = LEGACY_MATERIAL_EVIDENCE_KEYS.get(row["fact_type"])
                if key is not None:
                    row["provenance"] = {**row["provenance"], key: {
                        **row.get("evidence", {}), "legacy_material_hash": row["lineage_material_hash"],
                    }}
            self._rows.append(row)

    def mappings(self) -> _RowsResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows]

    def one(self) -> dict[str, Any]:
        assert len(self._rows) == 1
        return dict(self._rows[0])


class _StaticSession:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any],
    ) -> _RowsResult:
        sql = " ".join(str(statement).split()).lower()
        self.calls.append((sql, dict(params)))
        return _RowsResult(self.rows)


def _lineage_row(
    material_hash: str,
    *,
    series_id: int = 17,
    fact_type: str = "market.trade_flow",
    observation_key: str = "flow:1",
    revision: int = 1,
    market_commit_seq: int = 1,
) -> dict[str, Any]:
    return {
        "series_id": series_id,
        "fact_type": fact_type,
        "lineage_material_hash": material_hash,
        "observation_key": observation_key,
        "revision": revision,
        "provenance": {},
        "quality": {},
        "payload": {},
        "market_commit_seq": market_commit_seq,
        "fact_version_id": f"fact-{material_hash[:12]}-{revision}",
    }


def test_exact_material_lookup_batches_one_series_into_one_indexable_query() -> None:
    material_hashes = [f"{index:064x}" for index in range(1, 65)]
    session = _StaticSession(
        [_lineage_row(material_hash) for material_hash in material_hashes]
    )

    loaded = _load_lineage_material_rows(
        session,
        series_id=17,
        fact_type="market.trade_flow",
        material_hashes=list(reversed(material_hashes)),
    )

    assert list(loaded) == material_hashes
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "series_id = :series_id" in sql
    assert "material_hash = any(:material_hashes)" in sql
    assert "legacy_material_hash" not in sql
    assert params == {
        "series_id": 17,
        "material_hashes": material_hashes,
    }


def test_lineage_error_context_is_deterministic_and_bounded() -> None:
    values = ["value-5", "value-2", "value-4", "value-1", "value-3"]

    assert _lineage_values_context(values, field="raw_record_ids") == (
        "count=5 raw_record_ids=[value-1,value-2,value-3,...]"
    )


def _book_records(count, *, series_id=52, fact_type="market.depth_observation"):
    key = "_qt_depth_evidence" if fact_type == "market.depth_observation" else "_qt_bbo_evidence"
    return [SimpleNamespace(
        series_id=series_id, market_commit_seq=99,
        fact=SimpleNamespace(fact_type=fact_type, observation_time=datetime(2026, 8, 21, tzinfo=UTC),
            provenance={key: {"source_position": {"definition_id": "definition", "session_id": "session",
                                                "connection_epoch": 1, "receive_ordinal": ordinal + 1}}}),
    ) for ordinal in range(count)]


def test_canonical_book_archive_lineage_uses_selected_revision_positions() -> None:
    reference = {
        "manifest_id": "manifest-1",
        "object_sha256": "1" * 64,
        "content_fingerprint": "2" * 64,
        "object_key": "archive/manifest-1.parquet.zst",
        "object_uri": "market-archive://manifest-1",
    }
    session = _StaticSession(
        [
            {
                "fact_count": 4,
                "malformed_count": 0,
                "position_count": 4,
                "missing_count": 0,
                "archive_refs": [reference],
            }
        ]
    )
    start = datetime(2026, 8, 21, tzinfo=UTC)
    end = datetime(2026, 9, 1, tzinfo=UTC)

    references = _collect_canonical_book_archive_refs(
        session,
        series_id=52,
        fact_type="market.depth_observation",
        start=start,
        end=end,
        as_of_commit_seq=99,
        expected_record_count=4,
        records=_book_records(4),
    )

    assert references == {
        "manifest-1": {
            key: value for key, value in reference.items() if key != "manifest_id"
        }
    }
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "with raw_positions as materialized" in sql
    assert "select distinct definition_id, session_id," in sql
    assert "left join market.raw_archive_manifests" in sql
    assert "manifests.connection_epoch = positions.connection_epoch" in sql
    assert "jsonb_agg" in sql
    assert "market.fact_rows" not in sql
    assert "market.fact_versions" not in sql
    assert [position["receive_ordinal"] for position in json.loads(params["source_positions"])] == [1, 2, 3, 4]

def test_large_canonical_book_lineage_bounds_queries_and_preserves_every_position():
    class PositionSession(_StaticSession):
        def execute(self, statement, params):
            positions = json.loads(params["source_positions"])
            self.rows = [{
                "fact_count": len(positions), "malformed_count": 0,
                "position_count": len(positions), "missing_count": 0,
                "archive_refs": [{"manifest_id": "manifest", "object_sha256": "1" * 64,
                                  "content_fingerprint": "2" * 64, "object_key": "raw/manifest",
                                  "object_uri": "market-archive://raw/manifest"}],
            }]
            return super().execute(statement, params)
    session = PositionSession([])
    records = _book_records(10_001)
    result = _collect_canonical_book_archive_refs(
        session, series_id=52, fact_type="market.depth_observation",
        start=datetime(2026, 8, 21, tzinfo=UTC), end=datetime(2026, 9, 1, tzinfo=UTC),
        as_of_commit_seq=99, expected_record_count=len(records), records=records,
    )
    assert set(result) == {"manifest"}
    assert len(session.calls) == 2
    batches = [json.loads(params["source_positions"]) for _, params in session.calls]
    assert [len(batch) for batch in batches] == [10_000, 1]
    assert [position["receive_ordinal"] for batch in batches for position in batch] == list(range(1, 10_002))


@pytest.mark.parametrize(
    ("row", "match"),
    (
        (
            {
                "fact_count": 2,
                "malformed_count": 0,
                "position_count": 2,
                "missing_count": 0,
                "archive_refs": [],
            },
            "provenance_mismatch",
        ),
        (
            {
                "fact_count": 3,
                "malformed_count": 1,
                "position_count": 2,
                "missing_count": 0,
                "archive_refs": [],
            },
            "source position is malformed count=1",
        ),
        (
            {
                "fact_count": 3,
                "malformed_count": 0,
                "position_count": 3,
                "missing_count": 1,
                "archive_refs": [],
            },
            "has no acknowledged archive count=1",
        ),
    ),
)
def test_canonical_book_archive_lineage_fails_loud(
    row: dict[str, Any],
    match: str,
) -> None:
    session = _StaticSession([row])

    with pytest.raises(RuntimeError, match=match):
        _collect_canonical_book_archive_refs(
            session,
            series_id=51,
            fact_type="market.bbo",
            start=datetime(2026, 8, 21, tzinfo=UTC),
            end=datetime(2026, 9, 1, tzinfo=UTC),
            as_of_commit_seq=99,
            expected_record_count=3,
            records=_book_records(3, series_id=51, fact_type="market.bbo"),
        )


@pytest.mark.parametrize(
    ("rows", "match"),
    (
        ([], "provenance_incomplete"),
        (
            [
                _lineage_row("a" * 64, observation_key="flow:1"),
                _lineage_row("a" * 64, observation_key="flow:2"),
            ],
            "provenance_ambiguous",
        ),
        (
            [_lineage_row("b" * 64)],
            "provenance_mismatch",
        ),
    ),
)
def test_exact_material_lookup_fails_loud_for_unproven_batch_rows(
    rows: list[dict[str, Any]],
    match: str,
) -> None:
    session = _StaticSession(rows)

    with pytest.raises(RuntimeError, match=match):
        _load_lineage_material_rows(
            session,
            series_id=17,
            fact_type="market.trade_flow",
            material_hashes=["a" * 64],
        )


def test_repeated_legacy_material_preserves_latest_revision_semantics() -> None:
    material_hash = "a" * 64
    prior = _lineage_row(
        material_hash,
        fact_type="market.trade_flow_feature",
        revision=1,
        market_commit_seq=3,
    )
    latest = _lineage_row(
        material_hash,
        fact_type="market.trade_flow_feature",
        revision=2,
        market_commit_seq=9,
    )
    prior["evidence"] = {"revision": "prior"}
    latest["evidence"] = {"revision": "latest"}
    session = _StaticSession([prior, latest])

    loaded = _load_lineage_material_rows(
        session,
        series_id=17,
        fact_type="market.trade_flow_feature",
        material_hashes=[material_hash],
        evidence_key="_qt_trade_flow_feature_evidence",
    )

    assert loaded[material_hash]["fact_version_id"] == latest["fact_version_id"]
    assert loaded[material_hash]["evidence"] == {"revision": "latest", "legacy_material_hash": material_hash}


@pytest.mark.parametrize("epoch", [0, 1, None, True])
def test_typed_book_lineage_never_resolves_a_reused_ordinal_from_another_epoch(epoch):
    material = "a" * 64
    class BookSession(_StaticSession):
        def execute(self, statement, params):
            sql = " ".join(str(statement).split()).lower()
            self.calls.append((sql, dict(params)))
            if "from market.series" in sql:
                return _RowsResult([{"series_id": 52, "fact_type": "market.bbo"}])
            if "from market.raw_archive_manifests" in sql:
                assert "connection_epoch = :connection_epoch" in sql
                return _RowsResult([{
                    "manifest_id": "epoch-zero", "object_sha256": "b" * 64,
                    "content_fingerprint": "c" * 64, "object_key": "raw/zero.parquet",
                    "object_uri": "market-archive://raw/zero.parquet",
                }] if params["connection_epoch"] == 0 else [])
            row = _lineage_row(material, series_id=52, fact_type="market.bbo")
            row["evidence"] = {"source_position": {
                "definition_id": "definition", "session_id": "session",
                "connection_epoch": epoch, "receive_ordinal": 1,
            }}
            return _RowsResult([row])
    session = BookSession([])
    records = [SimpleNamespace(series_id=52, fact=SimpleNamespace(material_hash=material))]
    if epoch is not None and type(epoch) is int and epoch == 0:
        assert set(_collect_typed_archive_refs(session, records=records)) == {"epoch-zero"}
    else:
        with pytest.raises(RuntimeError, match="market_dataset_archive_incomplete"):
            _collect_typed_archive_refs(session, records=records)


class _LineageSession:
    def __init__(
        self,
        *,
        feature_to_source: dict[str, str],
    ) -> None:
        self.feature_to_source = dict(feature_to_source)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(
        self,
        statement: Any,
        params: dict[str, Any],
    ) -> _RowsResult:
        sql = " ".join(str(statement).split()).lower()
        bound = dict(params)
        self.calls.append((sql, bound))
        if "from market.series" in sql:
            catalog = {
                10: "market.trade_flow_feature",
                20: "market.trade_flow",
                30: "market.trade",
            }
            return _RowsResult(
                [
                    {"series_id": series_id, "fact_type": catalog[series_id]}
                    for series_id in bound["series_ids"]
                ]
            )
        if "legacy_material_hash" in sql:
            return _RowsResult(
                [
                    {
                        "series_id": 10,
                        "fact_type": "market.trade_flow_feature",
                        "lineage_material_hash": feature_hash,
                        "observation_key": f"feature:{index}",
                        "revision": 1,
                        "evidence": {"source_trade_flow_series_id": 20},
                        "provenance": {},
                        "quality": {},
                        "payload": {
                            "aggregate_material_hash": self.feature_to_source[
                                feature_hash
                            ]
                        },
                        "market_commit_seq": index,
                        "fact_version_id": f"feature-{index}",
                    }
                    for index, feature_hash in enumerate(
                        bound["material_hashes"], start=1
                    )
                ]
            )
        if "material_hash = any(:material_hashes)" in sql:
            if bound["series_id"] == 30:
                return _RowsResult(
                    [
                        {
                            "series_id": 30,
                            "fact_type": "market.trade",
                            "lineage_material_hash": material_hash,
                            "observation_key": f"trade:{index}",
                            "revision": 1,
                            "provenance": {
                                "_qt_trade_evidence": {
                                    "raw_record_id": (
                                        f"raw-{material_hash[-12:]}"
                                    )
                                }
                            },
                            "quality": {},
                            "payload": {},
                            "market_commit_seq": index,
                            "fact_version_id": f"trade-{index}",
                        }
                        for index, material_hash in enumerate(
                            bound["material_hashes"], start=1
                        )
                    ]
                )
            return _RowsResult(
                [
                    {
                        "series_id": 20,
                        "fact_type": "market.trade_flow",
                        "lineage_material_hash": material_hash,
                        "observation_key": f"flow:{index}",
                        "revision": 1,
                        "provenance": {
                            "_qt_trade_flow_evidence": {
                                "coverage_interval_id": (
                                    f"coverage-{material_hash[-12:]}"
                                )
                            }
                        },
                        "quality": {
                            "_qt_trade_flow_quality": {
                                "archive_complete": True,
                                "canonicalization_complete": True,
                            }
                        },
                        "payload": {},
                        "market_commit_seq": index,
                        "fact_version_id": f"flow-{index}",
                    }
                    for index, material_hash in enumerate(
                        bound["material_hashes"], start=1
                    )
                ]
            )
        if "from market.raw_archive_record_mappings as mappings" in sql:
            return _RowsResult(
                [
                    {
                        "raw_record_id": raw_record_id,
                        "manifest_id": "manifest-1",
                        "object_sha256": "1" * 64,
                        "content_fingerprint": "2" * 64,
                        "object_key": "archive/manifest-1.parquet.zst",
                        "object_uri": "market-archive://manifest-1",
                    }
                    for raw_record_id in bound["raw_record_ids"]
                ]
            )
        if "from market.stream_coverage_interval_versions as coverage" in sql:
            return _RowsResult(
                [
                    {
                        "coverage_interval_id": coverage_interval_id,
                        "manifest_id": "manifest-1",
                        "object_sha256": "1" * 64,
                        "content_fingerprint": "2" * 64,
                        "object_key": "archive/manifest-1.parquet.zst",
                        "object_uri": "market-archive://manifest-1",
                    }
                    for coverage_interval_id in bound["coverage_interval_ids"]
                ]
            )
        raise AssertionError(f"unexpected lineage SQL: {sql}")


def test_archive_lineage_traversal_batches_each_material_wave_per_series() -> None:
    feature_hashes = [f"{index:064x}" for index in range(1, 65)]
    source_hashes = [f"{index:064x}" for index in range(1001, 1065)]
    feature_to_source = dict(zip(feature_hashes, source_hashes, strict=True))
    session = _LineageSession(feature_to_source=feature_to_source)
    records = [
        SimpleNamespace(
            series_id=10,
            fact=SimpleNamespace(material_hash=feature_hash),
        )
        for feature_hash in feature_hashes
    ]

    references = _collect_typed_archive_refs(session, records=records)

    assert references == {
        "manifest-1": {
            "object_sha256": "1" * 64,
            "content_fingerprint": "2" * 64,
            "object_key": "archive/manifest-1.parquet.zst",
            "object_uri": "market-archive://manifest-1",
        }
    }
    exact_calls = [
        (sql, params)
        for sql, params in session.calls
        if "material_hash = any(:material_hashes)" in sql
        and "legacy_material_hash" not in sql
    ]
    legacy_calls = [
        (sql, params)
        for sql, params in session.calls
        if "legacy_material_hash" in sql
    ]
    coverage_calls = [
        (sql, params)
        for sql, params in session.calls
        if "from market.stream_coverage_interval_versions as coverage" in sql
    ]
    assert len(exact_calls) == 1
    assert exact_calls[0][1]["material_hashes"] == source_hashes
    assert len(legacy_calls) == 1
    assert legacy_calls[0][1]["material_hashes"] == feature_hashes
    assert len(coverage_calls) == 1
    assert len(coverage_calls[0][1]["coverage_interval_ids"]) == 64
    assert len(session.calls) == 5


def test_trade_archive_mappings_are_batched_per_material_wave() -> None:
    material_hashes = [f"{index:064x}" for index in range(1, 65)]
    session = _LineageSession(feature_to_source={})
    records = [
        SimpleNamespace(
            series_id=30,
            fact=SimpleNamespace(material_hash=material_hash),
        )
        for material_hash in material_hashes
    ]

    references = _collect_typed_archive_refs(session, records=records)

    assert list(references) == ["manifest-1"]
    mapping_calls = [
        (sql, params)
        for sql, params in session.calls
        if "from market.raw_archive_record_mappings as mappings" in sql
    ]
    assert len(mapping_calls) == 1
    assert len(mapping_calls[0][1]["raw_record_ids"]) == 64
    assert len(session.calls) == 3


def test_large_flow_lineage_chunks_every_wide_query_deterministically() -> None:
    row_count = (_LINEAGE_QUERY_BATCH_SIZE * 2) + 17
    feature_hashes = [f"{index:064x}" for index in range(1, row_count + 1)]
    source_hashes = [
        f"{index:064x}" for index in range(1001, 1001 + row_count)
    ]
    session = _LineageSession(
        feature_to_source=dict(
            zip(feature_hashes, source_hashes, strict=True)
        )
    )
    records = [
        SimpleNamespace(
            series_id=10,
            fact=SimpleNamespace(material_hash=feature_hash),
        )
        for feature_hash in feature_hashes
    ]

    _collect_typed_archive_refs(session, records=records)

    exact_calls = [
        params
        for sql, params in session.calls
        if "material_hash = any(:material_hashes)" in sql
        and "legacy_material_hash" not in sql
    ]
    legacy_calls = [
        params
        for sql, params in session.calls
        if "legacy_material_hash" in sql
    ]
    coverage_calls = [
        params
        for sql, params in session.calls
        if "from market.stream_coverage_interval_versions as coverage" in sql
    ]
    assert [len(call["material_hashes"]) for call in exact_calls] == [
        _LINEAGE_QUERY_BATCH_SIZE,
        _LINEAGE_QUERY_BATCH_SIZE,
        17,
    ]
    assert [len(call["material_hashes"]) for call in legacy_calls] == [
        _LINEAGE_QUERY_BATCH_SIZE,
        _LINEAGE_QUERY_BATCH_SIZE,
        17,
    ]
    assert [len(call["coverage_interval_ids"]) for call in coverage_calls] == [
        _LINEAGE_QUERY_BATCH_SIZE,
        _LINEAGE_QUERY_BATCH_SIZE,
        17,
    ]
    assert [
        material_hash
        for call in exact_calls
        for material_hash in call["material_hashes"]
    ] == source_hashes
    assert [
        material_hash
        for call in legacy_calls
        for material_hash in call["material_hashes"]
    ] == feature_hashes
    assert len(session.calls) == 11


def test_large_trade_lineage_chunks_exact_and_raw_mapping_queries() -> None:
    row_count = (_LINEAGE_QUERY_BATCH_SIZE * 2) + 17
    material_hashes = [f"{index:064x}" for index in range(1, row_count + 1)]
    session = _LineageSession(feature_to_source={})
    records = [
        SimpleNamespace(
            series_id=30,
            fact=SimpleNamespace(material_hash=material_hash),
        )
        for material_hash in material_hashes
    ]

    _collect_typed_archive_refs(session, records=records)

    exact_calls = [
        params
        for sql, params in session.calls
        if "material_hash = any(:material_hashes)" in sql
        and "legacy_material_hash" not in sql
    ]
    mapping_calls = [
        params
        for sql, params in session.calls
        if "from market.raw_archive_record_mappings as mappings" in sql
    ]
    expected_sizes = [_LINEAGE_QUERY_BATCH_SIZE, _LINEAGE_QUERY_BATCH_SIZE, 17]
    assert [len(call["material_hashes"]) for call in exact_calls] == expected_sizes
    assert [len(call["raw_record_ids"]) for call in mapping_calls] == expected_sizes
    assert len(session.calls) == 7


class _MissingFinalMaterialChunkSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        _statement: Any,
        params: dict[str, Any],
    ) -> _RowsResult:
        bound = dict(params)
        self.calls.append(bound)
        if len(self.calls) == 3:
            return _RowsResult([])
        return _RowsResult(
            [
                _lineage_row(
                    material_hash,
                    observation_key=f"flow:{material_hash}",
                )
                for material_hash in bound["material_hashes"]
            ]
        )


def test_large_missing_material_error_stays_bounded_after_all_chunks() -> None:
    row_count = (_LINEAGE_QUERY_BATCH_SIZE * 2) + 17
    material_hashes = [f"{index:064x}" for index in range(1, row_count + 1)]
    session = _MissingFinalMaterialChunkSession()

    with pytest.raises(RuntimeError) as exc_info:
        _load_lineage_material_rows(
            session,
            series_id=17,
            fact_type="market.trade_flow",
            material_hashes=material_hashes,
        )

    message = str(exc_info.value)
    assert "provenance_incomplete" in message
    assert "count=17 material_hashes=[" in message
    assert ",...]" in message
    assert len(message) < 400
    assert [len(call["material_hashes"]) for call in session.calls] == [
        _LINEAGE_QUERY_BATCH_SIZE,
        _LINEAGE_QUERY_BATCH_SIZE,
        17,
    ]

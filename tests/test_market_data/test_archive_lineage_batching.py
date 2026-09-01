from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from portal.backend.service.storage.repos.market_data import (
    _LINEAGE_QUERY_BATCH_SIZE,
    _collect_typed_archive_refs,
    _load_lineage_material_rows,
    _lineage_values_context,
)


class _RowsResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _RowsResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows]


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
    assert loaded[material_hash]["evidence"] == {"revision": "latest"}


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

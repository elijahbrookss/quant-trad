from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import publish_canonical_fact_archive
from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data import fact_registry
from market_data.normalization import NormalizationFormula, NormalizationSpec
from portal.backend.db.fact_storage_schema import FACT_ROWS_VIEW_SELECT, _view_signature
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository, ensure_payload_contracts
from tests.test_market_data.test_canonical_fact_archive import BASE, _row


class Session:
    def __init__(self, matches=()):
        self.matches = list(matches)
        self.calls = []

    def execute(self, query, parameters):
        self.calls.append((str(query), parameters))
        assert "partition.state IN ('verified', 'reclaimed')" in str(query)
        return self

    def mappings(self):
        return self

    def all(self):
        return self.matches


def _catalog(manifest, identity):
    return {
        "requested_id": identity, "id": manifest.manifest_id, "storage_day": BASE.date(),
        "descriptor": manifest.to_dict(), "manifest_hash": manifest.manifest_hash,
        "object_key": manifest.object_key, "object_sha256": manifest.object_sha256,
        "row_count": manifest.row_count, "byte_count": manifest.byte_count,
        "first_commit_seq": manifest.first_cursor[0], "first_id": manifest.first_cursor[1],
        "last_commit_seq": manifest.last_cursor[0], "last_id": manifest.last_cursor[1],
    }


def _cold(row):
    return {**deepcopy(row), "storage_day": BASE.date(), "payload": None, "provenance": None, "quality": None}


@pytest.fixture
def archive(tmp_path):
    rows = [_row(1), _row(2)]
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest = publish_canonical_fact_archive(rows, object_store=store, temporary_directory=tmp_path / "staging")
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    repo = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    return rows, manifest, reader, repo


def test_hot_only_reads_never_open_or_create_an_archive_root():
    def forbidden():
        raise AssertionError("hot reads must not open the archive tier")
    repo = PostgresCanonicalFactStorageRepository(object_store_factory=forbidden)
    rows = [_row()]
    session = Session()
    assert repo.hydrate_rows(session, rows) == rows
    assert session.calls == []


def test_mixed_hydration_preserves_order_and_every_canonical_field(archive):
    rows, manifest, _, repo = archive
    hot = {**_row(3), "storage_day": BASE.date() + timedelta(days=1)}
    selected = [_cold(rows[1]), hot, _cold(rows[0])]
    original = deepcopy(selected)
    session = Session([_catalog(manifest, row["id"]) for row in rows])
    hydrated = repo.hydrate_rows(session, selected)
    assert hydrated == [{**rows[1], "storage_day": BASE.date()}, hot, {**rows[0], "storage_day": BASE.date()}]
    assert selected == original
    assert len(session.calls) == 1


@pytest.mark.parametrize("count", [0, 2])
def test_missing_or_overlapping_catalog_pages_fail_closed(archive, count):
    rows, manifest, _, repo = archive
    session = Session([_catalog(manifest, rows[0]["id"])] * count)
    with pytest.raises(RuntimeError, match=f"pages={count}"):
        repo.hydrate_rows(session, [_cold(rows[0])])


@pytest.mark.parametrize("field", ["row_hash", "known_at", "source_identity_key", "ingestion_run_id", "series_dimensions"])
def test_cold_copy_must_match_all_selected_envelope_fields(archive, field):
    rows, manifest, _, repo = archive
    selected = _cold(rows[0])
    selected[field] = BASE + timedelta(seconds=9) if field == "known_at" else ({} if field == "series_dimensions" else "tampered")
    with pytest.raises(RuntimeError, match="canonical_archive_envelope_mismatch"):
        repo.hydrate_rows(Session([_catalog(manifest, selected["id"])]), [selected])


@pytest.mark.parametrize("mode", ["missing", "corrupt", "catalog", "partial_hot"])
def test_unavailable_or_untrusted_payloads_are_not_returned(archive, mode):
    rows, manifest, store, repo = archive
    selected = _cold(rows[0])
    catalog = _catalog(manifest, selected["id"])
    path = store.local_path(manifest.object_key)
    if mode == "missing":
        path.unlink()
        expected = FileNotFoundError
    elif mode == "corrupt":
        path.write_bytes(b"not a parquet object")
        expected = RuntimeError
    elif mode == "catalog":
        catalog["row_count"] += 1
        expected = RuntimeError
    else:
        selected["quality"] = {}
        expected = RuntimeError
    with pytest.raises(expected):
        repo.hydrate_rows(Session([catalog]), [selected])


def test_view_signature_accepts_only_the_known_postgres_rendering_changes():
    expected = FACT_ROWS_VIEW_SELECT.replace("versions.*", "versions.id, versions.storage_day")
    rendered = expected.replace(" AS versions", " versions").replace(" AS hot", " hot")
    rendered = rendered.replace("versions.id", "versions.id::text").replace("hot.id =", "hot.id::text =")
    # The SELECT projection itself does not acquire a cast in PostgreSQL.
    rendered = rendered.replace("SELECT versions.id::text", "SELECT versions.id")
    rendered = rendered.replace("'payload'", "'payload'::text").replace("'provenance'", "'provenance'::text").replace("'quality'", "'quality'::text")
    assert _view_signature(rendered) == _view_signature(expected)
    assert _view_signature(rendered.replace("LEFT JOIN", "JOIN")) != _view_signature(expected)
    assert _view_signature(rendered.replace("ELSE hot.payload", "ELSE NULL::jsonb")) != _view_signature(expected)
    assert _view_signature(rendered.replace("hot.storage_day =", "hot.storage_day::text =")) != _view_signature(expected)


def _normalized_schema_row():
    spec = NormalizationSpec(
        feature_name="archive_restart", semantic_version="1.0.0", input_fact_type="market.bbo",
        output_fact_type="market.normalized.archive_restart", formula=NormalizationFormula.BASIS_POINTS,
        units="bps", window_seconds=None, minimum_observations=1, warmup_observations=1,
    )
    schema = fact_registry.build_normalized_fact_payload_schema(
        spec_id=spec.spec_id, fact_type=spec.output_fact_type, units=spec.units,
    )
    row = {**spec.material(), "id": spec.spec_id, "spec_hash": spec.spec_hash,
           "stored_schema_id": schema.schema_id, "stored_contract_hash": schema.contract_hash,
           "stored_contract": schema.contract}
    return spec, schema, row


class SchemaSession(Session):
    def __init__(self, schema_rows, matches=()):
        super().__init__(matches)
        self.schema_rows = schema_rows

    def execute(self, query, parameters):
        if "market.normalization_specs" in str(query):
            self.calls.append((str(query), parameters))
            result = Session()
            result.matches = self.schema_rows
            return result
        return super().execute(query, parameters)


def test_mixed_page_loads_unrequested_normalized_schema_after_process_restart(tmp_path, monkeypatch):
    spec, schema, stored = _normalized_schema_row()
    monkeypatch.setattr(fact_registry, "_DYNAMIC_PAYLOAD_SCHEMAS", {})
    fact_registry.register_fact_payload_schema(schema)
    record = record_from_storage_row(_row(series_id=2, commit=2))
    fact = replace(record.fact, fact_type=spec.output_fact_type, payload_schema_id=schema.schema_id,
                   transformation_id=spec.spec_id, payload={
                       "value": "5", "status": "valid", "reason": None, "units": spec.units,
                       "input_start": BASE - timedelta(minutes=1), "input_end": BASE,
                       "input_count": 1, "input_watermark": 1, "input_fingerprint": "a" * 64,
                   })
    normalized = record_to_storage_row(replace(record, fact=fact, fact_version_id=None, row_hash=None), series_dimensions={})
    rows = [_row(), normalized]
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest = publish_canonical_fact_archive(rows, object_store=store, temporary_directory=tmp_path / "staging")
    fact_registry._DYNAMIC_PAYLOAD_SCHEMAS.clear()  # A fresh process has only static code-defined schemas.
    session = SchemaSession([stored], [_catalog(manifest, rows[0]["id"])])
    repo = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: store)
    assert repo.hydrate_rows(session, [_cold(rows[0])]) == [{**rows[0], "storage_day": BASE.date()}]
    assert fact_registry.get_fact_payload_schema(schema.schema_id) == schema
    assert len(session.calls) == 2
    assert all(query.lstrip().startswith("SELECT") for query, _ in session.calls)


@pytest.mark.parametrize("damage", ["missing", "spec_hash", "stored_contract_hash", "stored_contract"])
def test_dynamic_schemas_require_exact_persisted_evidence_even_when_cached(monkeypatch, damage):
    _, schema, stored = _normalized_schema_row()
    monkeypatch.setattr(fact_registry, "_DYNAMIC_PAYLOAD_SCHEMAS", {})
    fact_registry.register_fact_payload_schema(schema)
    if damage != "missing":
        stored[damage] = {} if damage == "stored_contract" else "b" * 64
    session = SchemaSession([] if damage == "missing" else [stored])
    with pytest.raises(RuntimeError, match="canonical_payload_|market_normalization_spec_storage_corrupt"):
        ensure_payload_contracts(session, [(schema.schema_id, schema.contract_hash)])


def test_schema_contract_conflicts_are_rejected_without_a_database_query():
    row = _row()
    session = SchemaSession([])
    with pytest.raises(RuntimeError, match="canonical_payload_contract_conflict"):
        ensure_payload_contracts(session, [(row["payload_schema_id"], row["payload_contract_hash"]),
                                           (row["payload_schema_id"], "b" * 64)])
    with pytest.raises(RuntimeError, match="canonical_payload_contract_mismatch"):
        ensure_payload_contracts(session, [(row["payload_schema_id"], "b" * 64)])
    assert session.calls == []


def test_selected_identity_reads_deduplicate_overlaps_and_bound_queries():
    class IdSession(Session):
        def execute(self, query, parameters):
            assert "WHERE versions.id = ANY(:fact_ids)" in str(query)
            self.calls.append((str(query), parameters))
            self.matches = [{**_row(), "id": identity} for identity in parameters["fact_ids"]]
            return self

    session = IdSession()
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: pytest.fail("hot-only read"))
    identities = [f"fact-{i:04}" for i in range(1001)]
    assert reader.read_rows_by_ids(session, []) == {}
    actual = reader.read_rows_by_ids(session, [*identities, identities[0]])
    assert set(actual) == set(identities)
    assert [len(params["fact_ids"]) for _, params in session.calls] == [1000, 1]


@pytest.mark.parametrize("returned_count", [0, 2])
def test_selected_identity_coverage_must_be_exact(returned_count):
    class MissingSession(Session):
        def execute(self, query, parameters):
            self.matches = [_row()] * returned_count
            return self

    reader = PostgresCanonicalFactStorageRepository()
    with pytest.raises(RuntimeError, match="canonical_selected_identity_coverage_invalid"):
        reader.read_rows_by_ids(MissingSession(), [_row()["id"]])

"""Physical normalized retention, nested recipes and all-revision freeze parity."""
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_normalized_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.normalization import NormalizationInput, NormalizationSpec, evaluate_normalization
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, market_data, normalization
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement

pytestmark = pytest.mark.db


@pytest.mark.parametrize("source_cold", [False, True])
def test_normalized_nested_history_survives_source_and_root_reclamation(storage, tmp_path, monkeypatch, source_cold):
    source_day, inner_day, outer_day = [storage.today - timedelta(days=days) for days in (3, 2, 1)]
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda:
        FilesystemRawArchiveObjectStore(store.root, writable=False))
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "market_data_repo", storage.repo)
    monkeypatch.setattr(normalization, "db", storage.database)
    recipes = normalization.PostgresNormalizationRepository()
    _placement(monkeypatch, source_day)
    facts = [replace(storage.fact, observation_key=f"normalization-input-{index}",
        observation_time=BASE + timedelta(seconds=index), known_at=BASE + timedelta(seconds=index),
        accepted_at=BASE + timedelta(seconds=index),
        payload={**storage.fact.payload, "rate": str(index + 1), "raw_rate": str(index + 1)}) for index in range(5)]
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=facts[:4])
    # Same material, distinct delivery; then a future-known revision committed
    # before the final causal input. Preserve the first two, exclude the third.
    for seconds in (3, 20):
        storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
            facts=[replace(facts[0], known_at=BASE + timedelta(seconds=seconds),
                accepted_at=BASE + timedelta(seconds=seconds), provenance={"delivery": seconds})])
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=facts[4:])
    window = dict(start=BASE, end=BASE + timedelta(seconds=10))
    inputs = storage.repo.read_facts(series_id=storage.series_id, **window, known_at_lte=BASE + timedelta(seconds=5))
    assert len(inputs) == 5
    spec = recipes.register_spec(NormalizationSpec(feature_name="retention_zscore", semantic_version="1",
        input_fact_type="derivatives.funding_rate", output_fact_type="market.normalized.retention_zscore",
        formula="causal_zscore", units="zscore", window_seconds=10,
        minimum_observations=1, warmup_observations=1, parameters={"require_full_window": False}))
    inner_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=spec.output_fact_type,
        contract_version=f"market.normalized_feature.v1/{spec.spec_id}", timeframe_seconds=None)
    normalized = evaluate_normalization(spec, [NormalizationInput(source_series_id=row.series_id,
        effective_at=row.fact.observation_time, known_at=row.fact.known_at, market_commit_seq=row.market_commit_seq,
        material_hash=row.fact.material_hash, value=Decimal(row.fact.payload["rate"])) for row in inputs],
        output_series_id=inner_series)[-1]
    assert normalized.input_count == 5 > len(normalized.source_material_hashes)
    _placement(monkeypatch, inner_day)
    inner_outcome = recipes.ingest([normalized])
    outer_spec = recipes.register_spec(NormalizationSpec(feature_name="retention_nested", semantic_version="1",
        input_fact_type=spec.output_fact_type, output_fact_type="market.normalized.retention_nested",
        formula="basis_points", units="bps", window_seconds=None, minimum_observations=0, warmup_observations=0))
    outer_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=outer_spec.output_fact_type,
        contract_version=f"market.normalized_feature.v1/{outer_spec.spec_id}", timeframe_seconds=None)
    nested = evaluate_normalization(outer_spec, [NormalizationInput(source_series_id=inner_series,
        effective_at=normalized.effective_at, known_at=normalized.known_at, market_commit_seq=inner_outcome.max_commit_seq,
        material_hash=normalized.material_hash, value=normalized.value)], output_series_id=outer_series)[0]
    _placement(monkeypatch, outer_day)
    recipes.ingest([nested])
    requests = [DatasetSeriesRequest(series_id, **window) for series_id in (storage.series_id, inner_series, outer_series)]
    with monkeypatch.context() as old:
        old.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset(requests)
    with pytest.raises(RuntimeError, match="revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=outer_series)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    for day, series_id, fact, recipe in ((inner_day, inner_series, normalized, spec), (outer_day, outer_series, nested, outer_spec)):
        _placement(monkeypatch, day)
        revised = replace(fact, known_at=BASE + timedelta(seconds=6))
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id,
            facts=[replace(canonicalize_normalized_feature(revised, spec=recipe), state="invalidated")])
    frozen = storage.repo.freeze_dataset(requests)
    assert frozen.dataset_hash != older_dataset.dataset_hash
    history, bindings, causal, latest = {}, {}, {}, {}
    for entry in frozen.series:
        series_id = entry["series_id"]
        history[series_id] = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
        bindings[series_id] = validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id})
        causal[series_id] = storage.repo.read_facts(series_id=series_id, **window, known_at_lte=BASE + timedelta(seconds=5))
        latest[series_id] = storage.repo.read_series_records(series_id=series_id, **window)
    assert len(history[inner_series]) == len(history[outer_series]) == 2
    assert history[outer_series][-1].fact.state.value == "invalidated"
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=store,
        temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        for day in (source_day, inner_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    # Recipe rows stay in PostgreSQL and cannot change between deep verification
    # and the final metadata-only handoff. Keep that database guard enabled.
    with pytest.raises(DBAPIError, match="immutable.*normalization_specs"):
        with storage.database.session() as session:
            session.execute(text("UPDATE market.normalization_specs SET units='corrupt' WHERE id=:id"), {"id": spec.spec_id})
    _archive_day(archive, outer_day)
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT source.* FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id
            JOIN market.fact_archive_manifests AS manifest ON manifest.id=edge.manifest_id
            WHERE manifest.storage_day=:day
        """), {"day": outer_day}).mappings().all()
        assert Counter(row["fact_type"] for row in edges) == {"derivatives.funding_rate": 6, spec.output_fact_type: 1}
        assert all(row["known_at"] <= BASE + timedelta(seconds=4) for row in edges)
    if not source_cold:
        for day in (source_day, inner_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    # A valid root archive is insufficient if any interior canonical input is corrupt.
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"),
            {"day": source_day}).scalar_one()
    path = store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupt full normalization input page")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)
    assert _physical(storage, outer_day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, outer_day)
    assert reclaimer.reclaim_partition(outer_day, eligible_before=storage.today)["status"] == "dry_run"
    assert _physical(storage, outer_day) == physical
    removed = reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)
    assert removed["reclaimed_bytes"] == physical["bytes"] > 0 and removed["protected_dataset_ranges"] >= 2
    assert all(_physical(storage, day)["relation"] is None for day in (source_day, inner_day, outer_day))
    for entry in frozen.series:
        series_id = entry["series_id"]
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == history[series_id]
        assert validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id}) == bindings[series_id]
        assert storage.repo.read_facts(series_id=series_id, **window, known_at_lte=BASE + timedelta(seconds=5)) == causal[series_id]
        assert storage.repo.read_series_records(series_id=series_id, **window) == latest[series_id]
    assert storage.repo.freeze_dataset(requests).dataset_hash == frozen.dataset_hash
    assert reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)["status"] == "already_reclaimed"


@pytest.mark.parametrize("source_cold", [False, True])
def test_source_sized_pages_preserve_mixed_sizes_corrections_and_atomic_resume(
        storage, tmp_path, monkeypatch, source_cold):
    from sqlalchemy import event
    from market_data.archive_verification import ArchiveVerificationLimits
    from market_data.fact_archive import FactArchiveLimits
    from portal.backend.service.market.canonical_retention import (
        CanonicalRetentionBudgetExceeded, CanonicalRetentionStopRequested,
    )
    from portal.backend.service.storage.repos.fact_dependencies import CanonicalSourceByteBudgetExceeded
    from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM

    source_day, day = storage.today - timedelta(days=3), storage.today - timedelta(days=2)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda:
        FilesystemRawArchiveObjectStore(store.root, writable=False))
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "market_data_repo", storage.repo)
    monkeypatch.setattr(normalization, "db", storage.database)
    recipes = normalization.PostgresNormalizationRepository()
    _placement(monkeypatch, source_day)
    facts = [replace(storage.fact, observation_key=f"sized-input-{i}",
        observation_time=BASE + timedelta(seconds=i * 10), known_at=BASE + timedelta(seconds=i * 10),
        accepted_at=BASE + timedelta(seconds=i * 10),
        payload={**storage.fact.payload, "rate": str(i + 1), "raw_rate": str(i + 1)},
        provenance={"provider_evidence": "x" * 20_000}) for i in range(4)]
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=facts)
    for seconds in (1, 100):
        storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
            facts=[replace(facts[0], known_at=BASE + timedelta(seconds=seconds),
                accepted_at=BASE + timedelta(seconds=seconds),
                provenance={**facts[0].provenance, "delivery": seconds})])
    window = dict(start=BASE, end=BASE + timedelta(seconds=40))
    inputs = storage.repo.read_facts(series_id=storage.series_id, **window, known_at_lte=BASE + timedelta(seconds=40))
    assert len(inputs) == 4
    spec = recipes.register_spec(NormalizationSpec(feature_name="sized_bps", semantic_version="1",
        input_fact_type="derivatives.funding_rate", output_fact_type="market.normalized.sized_bps",
        formula="basis_points", units="bps", window_seconds=None, minimum_observations=0, warmup_observations=0))
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=spec.output_fact_type,
        contract_version=f"market.normalized_feature.v1/{spec.spec_id}", timeframe_seconds=None)
    outputs = evaluate_normalization(spec, [NormalizationInput(source_series_id=row.series_id,
        effective_at=row.fact.observation_time, known_at=row.fact.known_at, market_commit_seq=row.market_commit_seq,
        material_hash=row.fact.material_hash, value=Decimal(row.fact.payload["rate"])) for row in inputs],
        output_series_id=series)
    _placement(monkeypatch, day)
    # A full-size, already acknowledged page must remain verifiable alongside
    # later dependency-sized pages without changing the reader's row ceiling.
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
        facts=[replace(storage.fact, observation_key=f"independent-{i}",
            observation_time=BASE - timedelta(seconds=10 + i)) for i in range(4)])
    recipes.ingest(outputs)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    storage.repo.ingest_facts(series_id=series, source_id=source_id,
        facts=[replace(canonicalize_normalized_feature(replace(outputs[-1],
            known_at=BASE + timedelta(seconds=31)), spec=spec), state="invalidated")])
    requests = [DatasetSeriesRequest(item, **window) for item in (storage.series_id, series)]
    frozen = storage.repo.freeze_dataset(requests)
    read = lambda: storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
    before = read()
    causal_before = storage.repo.read_facts(series_id=series, **window, known_at_lte=BASE + timedelta(seconds=30))
    assert len(before) == 5 and before[-1].fact.state.value == "invalidated"
    base_kwargs = dict(database=storage.database, object_store=store, temporary_directory=tmp_path / "staging")
    if source_cold:
        source_archive = fact_archival.PostgresCanonicalFactArchiveRepository(**base_kwargs,
            limits=FactArchiveLimits(max_rows=1, row_group_size=1))
        _archive_day(source_archive, source_day)
        PostgresCanonicalFactReclamationRepository(archive_repository=source_archive, enabled=True).reclaim_partition(
            source_day, eligible_before=storage.today, execute=True)
    with storage.database.session() as session:
        if source_cold:
            byte_limit = 2 * session.execute(text(
                "SELECT max((descriptor->>'logical_byte_count')::bigint) FROM market.fact_archive_manifests "
                "WHERE storage_day=:day"), {"day": source_day}).scalar_one() + 128
        else:
            byte_limit = 2 * session.execute(text(f"""
                SELECT max(6::bigint * octet_length(to_jsonb(candidates)::text)) FROM
                (SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
                 WHERE versions.storage_day=:day) AS candidates
            """), {"day": source_day}).scalar_one() + 128
    limits = FactArchiveLimits(max_rows=4, row_group_size=4, max_logical_bytes=byte_limit)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(**base_kwargs, limits=limits)
    archive.seal_partition(day)
    first = archive.stage_next_page(day)
    assert first["row_count"] == 4 and first["page_ordinal"] == 0
    files_before = {p: p.read_bytes() for p in store.root.rglob("*.parquet")}
    original_resolver = archive._source_revisions
    attempts, refusals = [], []

    def observed(session, rows):
        attempts.append(len(rows))
        try:
            return original_resolver(session, rows)
        except CanonicalSourceByteBudgetExceeded:
            refusals.append(len(rows))
            raise

    monkeypatch.setattr(archive, "_source_revisions", observed)
    # Real preflight refuses one root when even its complete correction set is
    # too large. No objects, metadata or source cursor may advance.
    archive.limits = replace(limits, max_logical_bytes=byte_limit // 3)
    with pytest.raises(CanonicalSourceByteBudgetExceeded):
        archive.stage_next_page(day)
    assert attempts[-1] == 1 and archive.inspect_partition(day)["archived_rows"] == 4
    assert {p: p.read_bytes() for p in store.root.rglob("*.parquet")} == files_before
    archive.limits = limits
    for error in (CanonicalRetentionStopRequested, CanonicalRetentionBudgetExceeded):
        attempts.clear()
        refusals.clear()
        def stop():
            if refusals:
                raise error("stop between source-size attempts")
        archive.check_budget = stop
        with pytest.raises(error, match="stop between"):
            archive.stage_next_page(day)
        archive.check_budget = None
        assert attempts == [4] and archive._prepared_page is None
        assert archive.inspect_partition(day)["archived_rows"] == 4
        assert {p: p.read_bytes() for p in store.root.rglob("*.parquet")} == files_before

    # An ordinary refusal with the same message is not a sizing signal.
    attempts.clear()
    def corrupt(session, rows):
        attempts.append(len(rows))
        raise RuntimeError("canonical_source_dependency_byte_budget_exceeded: injected non-sizing failure")
    with monkeypatch.context() as patch:
        patch.setattr(archive, "_source_revisions", corrupt)
        with pytest.raises(RuntimeError, match="injected non-sizing"):
            archive.stage_next_page(day)
    assert attempts == [4]

    def interrupt(_conn, _cursor, statement, *_):
        if statement.startswith("INSERT INTO market.fact_archive_canonical_dependencies"):
            raise RuntimeError("interrupted reduced-page catalog commit")
    event.listen(storage.database._engine, "after_cursor_execute", interrupt)
    attempts.clear()
    try:
        with pytest.raises(RuntimeError, match="interrupted reduced-page"):
            archive.stage_next_page(day)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", interrupt)
    assert attempts == [4, 2, 1]
    assert archive.inspect_partition(day)["archived_rows"] == 4
    interrupted_files = {p: p.read_bytes() for p in store.root.rglob("*.parquet")}
    assert len(interrupted_files) == len(files_before) + 1
    # New owner resumes after the committed cursor, reusing unacknowledged bytes.
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(**base_kwargs, limits=limits)
    assert archive.stage_next_page(day)["row_count"] == 1
    assert {p: p.read_bytes() for p in store.root.rglob("*.parquet")} == interrupted_files
    for _ in range(6):
        result = archive.stage_next_page(day)
        if result["status"] == "source_exhausted":
            break
    assert result["status"] == "source_exhausted" and result["archived_rows"] == 9
    with storage.database.session() as session:
        pages = session.execute(text("SELECT row_count FROM market.fact_archive_manifests "
            "WHERE storage_day=:day ORDER BY page_ordinal"), {"day": day}).scalars().all()
        edges = session.execute(text("SELECT DISTINCT source.id,source.known_at FROM market.fact_archive_canonical_dependencies edge "
            "JOIN market.fact_versions source ON source.id=edge.fact_version_id "
            "JOIN market.fact_archive_manifests page ON page.id=edge.manifest_id WHERE page.storage_day=:day"),
            {"day": day}).all()
    assert pages[0] == 4 and all(size < 4 for size in pages[1:]) and sum(pages) == 9
    assert len(edges) == 5 and all(known_at <= BASE + timedelta(seconds=30) for _, known_at in edges)
    for _ in range(len(pages)):
        assert archive.verify_next_page(day)["status"] == "page_verified"
    assert archive.verify_next_page(day)["status"] == "no_unverified_pages"
    with pytest.raises(RuntimeError, match="partition_page_budget_exceeded"):
        archive.verify_partition(day, limits=ArchiveVerificationLimits(max_pages=len(pages) - 1))
    archive.verify_partition(day)
    PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True).reclaim_partition(
        day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["relation"] is None
    assert read() == before
    assert storage.repo.read_facts(series_id=series, **window, known_at_lte=BASE + timedelta(seconds=30)) == causal_before
    assert storage.repo.freeze_dataset(requests).dataset_hash == frozen.dataset_hash
    assert all(p.read_bytes() == content for p, content in files_before.items())

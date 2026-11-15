"""Transactional ownership of verified, immutable canonical Fact pages."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from hashlib import sha256
import json
import os
from pathlib import Path

from sqlalchemy import and_, insert, or_, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from core.market_storage_lifecycle import COLD_OPTIONAL_SSD_MAX_USED_PERCENT
from core.storage_mounts import configured_archive_root, require_configured_archive_mount
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdFactLocation, ColdHistoryReader, publish_cold_page
from market_data.fact_archive import FactArchiveLimits, FactArchiveManifest
from portal.backend.db.cold_history_models import (
    COLD_HISTORY_LAYOUT_VERSION, ColdBookSessionFactRecord, ColdFactLocatorRecord, ColdFactMaterialRecord,
    ColdFactPageRecord, ColdFactPageSeriesRecord,
    ColdNormalizationSpecReferenceRecord, cold_page_is_visible, guard_cold_runtime_placement,
)

_pages = ColdFactPageRecord.__table__
_page_series = ColdFactPageSeriesRecord.__table__
_locators = ColdFactLocatorRecord.__table__
_materials = ColdFactMaterialRecord.__table__
_normalization_specs = ColdNormalizationSpecReferenceRecord.__table__
_book_sessions = ColdBookSessionFactRecord.__table__
_BATCH_SIZE = 1000


_PAGE_VISIBILITY = text(cold_page_is_visible("market.cold_fact_pages"))


def _key_digest(key: str) -> bytes:
    return sha256(key.encode("utf-8")).digest()


def _locator_witness(claim):
    return (claim["series_id"], bytes(claim["key_digest"]), claim["revision"],
            claim["market_commit_seq"], bytes(claim["row_hash"]).hex())


def _verify_locator(row, witness, *, key_lookup=False):
    if row["row_hash"] != witness[-1]:
        raise RuntimeError(f"cold_fact_locator_hash_mismatch: fact_version_id={row['id']}")
    if (row["series_id"], _key_digest(row["observation_key"]), row["revision"], row["market_commit_seq"]) != witness[:-1]:
        reason = "cold_fact_key_locator_mismatch" if key_lookup else "cold_fact_locator_mismatch"
        raise RuntimeError(f"{reason}: fact_version_id={row['id']}")


def _material_records(rows):
    material = []
    specs = []
    for row in rows:
        scope = {"series_id": row["series_id"], "fact_id": row["id"]}
        material.append({
            **scope, "canonical": True, "hash_digest": _key_digest(row["material_hash"]),
            "evidence_key_digest": _key_digest(""),
        })
        for key, evidence in row["provenance"].items():
            if not isinstance(evidence, Mapping):
                continue
            legacy_hash = evidence.get("legacy_material_hash")
            if type(legacy_hash) in (str, int):
                material.append({
                    **scope, "canonical": False, "hash_digest": _key_digest(str(legacy_hash)),
                    "evidence_key_digest": _key_digest(key),
                })
            if key == "_qt_normalization_evidence" and isinstance(evidence.get("spec_id"), str):
                specs.append({"spec_digest": _key_digest(evidence["spec_id"]), "fact_id": row["id"]})
    return material, specs


def _book_session_digest(definition_id: str, session_id: str) -> bytes:
    return _key_digest(json.dumps([definition_id, session_id], separators=(",", ":")))


def _book_session_records(rows):
    records = []
    for row in rows:
        if row["payload_schema_id"] != "market.l2_book.v1":
            continue
        witness = row["provenance"].get("_qt_l2_evidence")
        if (not isinstance(witness, Mapping)
                or not isinstance(witness.get("definition_id"), str) or not isinstance(witness.get("session_id"), str)):
            raise RuntimeError(f"cold_book_session_witness_missing: fact_version_id={row['id']}")
        records.append({
            "session_digest": _book_session_digest(witness["definition_id"], witness["session_id"]),
            "market_commit_seq": row["market_commit_seq"], "fact_id": row["id"],
        })
    return records


def _manifest(row: Mapping) -> FactArchiveManifest:
    manifest = FactArchiveManifest.from_dict(row["descriptor"], expected_hash=row["manifest_hash"])
    if (
        row["id"], row["object_key"], row["object_sha256"], row["row_count"],
        row["byte_count"], row["first_commit_seq"], row["last_commit_seq"],
    ) != (
        manifest.manifest_id, manifest.object_key, manifest.object_sha256,
        manifest.row_count, manifest.byte_count, manifest.first_cursor[0],
        manifest.last_cursor[0],
    ):
        raise RuntimeError(f"cold_fact_page_metadata_mismatch: page_id={row['id']}")
    return manifest


def cold_layout_active(session) -> bool:
    """Honor the catalog only after the explicit, approved authority change."""
    if not isinstance(session, (Session, Connection)):
        return False
    return bool(session.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM market.fact_storage_state
            WHERE layout_version=:layout AND state='ready' AND completed_at IS NOT NULL
        )
    """), {"layout": COLD_HISTORY_LAYOUT_VERSION}).scalar_one())


def require_cold_acquisition_headroom(session) -> None:
    """Pause optional historical acquisition before it competes with live writes."""
    from ..history_policy import _read, registered_header_targets, _target

    saved, _ = _read(session)
    if saved is None or not saved.movement_enabled:
        raise RuntimeError("cold_fact_publish_storage_policy_disabled")
    targets = {row.id: _target(row) for row in registered_header_targets(session)}
    recent = targets.get(saved.recent[0])
    if recent is None or recent.medium != "ssd":
        raise RuntimeError("cold_fact_recent_target_missing")
    witness = recent.inspect(require_writable=False)
    usable = witness.used_bytes + witness.available_bytes
    if usable <= 0 or witness.used_bytes * 100 >= usable * COLD_OPTIONAL_SSD_MAX_USED_PERCENT:
        raise RuntimeError("cold_fact_historical_acquisition_ssd_pressure")


def publish_cold_candle_rows(session, rows: Iterable[Mapping]) -> None:
    """Publish bounded pages for the candle writer while its series lock is held."""
    from .fact_storage import ensure_payload_contracts
    from ..history_policy import _archive_policy, _read
    from core.market_storage_lifecycle import CanonicalFactRetentionPolicy

    root = configured_archive_root().resolve(strict=True)
    require_configured_archive_mount(root)
    saved, _ = _read(session)
    if saved is None or not saved.movement_enabled:
        raise RuntimeError("cold_fact_publish_storage_policy_disabled")
    policy = _archive_policy(session, saved, CanonicalFactRetentionPolicy(), root)
    guard_cold_runtime_placement(session, locators=True)
    store = FilesystemRawArchiveObjectStore(root / "objects")
    repository = PostgresColdHistory(ColdHistoryReader(
        store, ensure_contracts=lambda contracts: ensure_payload_contracts(session, contracts),
    ))
    limits = FactArchiveLimits()
    # One page, plus verified staging/readback and filesystem accounting headroom.
    minimum_free = policy.archive_min_free_bytes + 3 * limits.max_file_bytes
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == limits.max_rows:
            _publish_candle_page(session, repository, batch, root, minimum_free)
            batch.clear()
    if batch:
        _publish_candle_page(session, repository, batch, root, minimum_free)


def _publish_candle_page(session, repository, rows, root: Path, minimum_free: int) -> None:
    space = os.statvfs(root)
    if space.f_bavail * space.f_frsize < minimum_free:
        raise RuntimeError("cold_fact_publish_archive_capacity")
    manifest = publish_cold_page(
        rows, object_store=repository.reader.object_store,
        temporary_directory=root / "canonical-staging",
    )
    repository.admit(session, manifest)


class PostgresColdHistory:
    def __init__(self, reader: ColdHistoryReader):
        self.reader = reader

    def admit(self, session, manifest: FactArchiveManifest, *, staged_storage_day=None) -> None:
        """Verify current mounted bytes and publish one page in the caller's transaction."""
        rows = self.reader.read_page(manifest)
        # Roll back all of this page's SQL on a locator collision, even if a
        # migration caller catches the exception and continues its transaction.
        with session.begin_nested():
            inserted = session.execute(
                postgres_insert(_pages).values(
                    id=manifest.manifest_id, object_key=manifest.object_key,
                    object_sha256=manifest.object_sha256, manifest_hash=manifest.manifest_hash,
                    row_count=manifest.row_count, byte_count=manifest.byte_count,
                    first_commit_seq=manifest.first_cursor[0],
                    last_commit_seq=manifest.last_cursor[0], descriptor=manifest.to_dict(),
                    staged_storage_day=staged_storage_day,
                ).on_conflict_do_nothing(index_elements=[_pages.c.id]).returning(_pages.c.id)
            ).scalar_one_or_none()
            if inserted is None:
                existing = session.execute(select(_pages).where(
                    _pages.c.id == manifest.manifest_id
                )).mappings().one()
                if _manifest(existing) != manifest or existing["staged_storage_day"] != staged_storage_day:
                    raise RuntimeError(f"cold_fact_page_publication_conflict: page_id={manifest.manifest_id}")
                return

            series_heads = {}
            active_times = {}
            for row in rows:
                series_id = row["series_id"]
                series_heads[series_id] = max(series_heads.get(series_id, 0), row["market_commit_seq"])
                if row["state"] == "active":
                    accepted_at, observation_at = row["accepted_at"], row["observation_time"]
                    last, observation = active_times.get(series_id, (accepted_at, observation_at))
                    active_times[series_id] = (max(last, accepted_at), max(observation, observation_at))
            catalog_rows = []
            for bounds in manifest.series:
                last, observation = active_times.get(bounds.series_id, (None, None))
                catalog_rows.append({
                    "page_id": manifest.manifest_id, "series_id": bounds.series_id,
                    "first_observation_at": bounds.first_observation_at,
                    "last_observation_at": bounds.last_observation_at,
                    "last_commit_seq": series_heads[bounds.series_id],
                    "last_active_accepted_at": last,
                    "last_active_observation_at": observation,
                })
            session.execute(insert(_page_series), catalog_rows)
            for offset in range(0, len(rows), _BATCH_SIZE):
                batch = rows[offset:offset + _BATCH_SIZE]
                session.execute(insert(_locators), [
                    {
                        "id": row["id"], "page_id": manifest.manifest_id,
                        "ordinal": ordinal, "series_id": row["series_id"],
                        "key_digest": _key_digest(row["observation_key"]),
                        "revision": row["revision"],
                        "row_hash": bytes.fromhex(row["row_hash"]),
                        "market_commit_seq": row["market_commit_seq"],
                    }
                    for ordinal, row in enumerate(batch, start=offset)
                ])
                material, specs = _material_records(batch)
                session.execute(insert(_materials), material)
                if specs:
                    session.execute(insert(_normalization_specs), specs)
                book_sessions = _book_session_records(batch)
                if book_sessions:
                    session.execute(insert(_book_sessions), book_sessions)

    def candidate_pages(self, session, *, series_id, start, end, as_of_commit_seq=None):
        query = select(_pages).join(_page_series, _page_series.c.page_id == _pages.c.id).where(
            _page_series.c.series_id == series_id,
            _page_series.c.first_observation_at < end,
            _page_series.c.last_observation_at >= start,
            _PAGE_VISIBILITY,
        ).order_by(_pages.c.first_commit_seq, _pages.c.id)
        if as_of_commit_seq is not None:
            query = query.where(_pages.c.first_commit_seq <= as_of_commit_seq)
        result = session.execute(query, execution_options={"yield_per": 256}).mappings()
        try:
            for row in result:
                yield _manifest(row)
        finally:
            result.close()

    def iter_range(self, session, *, series_id, start, end, as_of_commit_seq=None,
                   known_at_lte=None, source_identity_keys=()):
        """Read matching revisions from the catalog's visible, admitted pages."""
        scope = dict(series_id=series_id, start=start, end=end, as_of_commit_seq=as_of_commit_seq)
        return self.reader.iter_range(
            self.candidate_pages(session, **scope), **scope, known_at_lte=known_at_lte,
            source_identity_keys=source_identity_keys, admitted=True,
        )

    def causal_window_pages(self, session, requests, *, max_candidates: int):
        """Resolve bounded candidate pages for one batch of declared source scopes."""
        rows = session.execute(text(f"""
            SELECT requested.root_id, catalog.series_id AS requested_series_id, page.*
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text, instrument_id text, fact_type text, series_id bigint,
                root_commit bigint, range_start timestamptz, range_end timestamptz, include_end boolean
            )
            JOIN market.series AS series ON series.instrument_id=requested.instrument_id
              AND series.fact_type=requested.fact_type
              AND (requested.series_id IS NULL OR series.id=requested.series_id)
            JOIN market.cold_fact_page_series AS catalog ON catalog.series_id=series.id
              AND catalog.last_observation_at>=requested.range_start
              AND (catalog.first_observation_at<requested.range_end
                OR (requested.include_end AND catalog.first_observation_at=requested.range_end))
            JOIN market.cold_fact_pages AS page ON page.id=catalog.page_id
              AND page.first_commit_seq<=requested.root_commit
              AND {cold_page_is_visible("page")}
            ORDER BY requested.root_id, catalog.series_id, page.first_commit_seq, page.id
            LIMIT :limit
        """), {"requests": json.dumps(requests), "limit": max_candidates + 1}).mappings().all()
        if len(rows) > max_candidates:
            raise RuntimeError("canonical_window_cold_candidate_budget_exceeded: reduce archive page size")
        for row in rows:
            yield row["root_id"], row["requested_series_id"], _manifest(row)

    def candle_nearest_observation(self, session, *, series_id, start, end, cursor, prefer_latest, commit):
        """Find the next candle clock within its exact directional query and commit boundary."""
        direction = "DESC" if prefer_latest else "ASC"
        edge = "<" if prefer_latest else ">="
        params = dict(series_id=series_id, start=start, end=end, cursor=cursor, commit=commit)
        sql = text(f"""
            SELECT observation_time FROM market.fact_versions
            WHERE series_id=:series_id AND observation_time>=:start AND observation_time<:end
              AND observation_time {edge} :cursor
              AND (CAST(:commit AS bigint) IS NULL OR market_commit_seq<=:commit)
            ORDER BY observation_time {direction} LIMIT 1
        """)
        nearest = session.execute(sql, params).scalar_one_or_none()
        aggregate = "LEAST" if prefer_latest else "GREATEST"
        scope_bound = "last_observation_at" if prefer_latest else "first_observation_at"
        edge_bound = min(cursor, end) if prefer_latest else max(cursor, start)
        params["edge"] = edge_bound
        pages = session.execute(text(f"""
            SELECT page.*, {aggregate}(scope.{scope_bound},:edge) AS seek_bound
            FROM market.cold_fact_page_series AS scope JOIN market.cold_fact_pages AS page
                ON page.id=scope.page_id
            WHERE scope.series_id=:series_id AND scope.last_observation_at>=:start
                AND scope.first_observation_at<:end
                AND scope.{"first" if prefer_latest else "last"}_observation_at {edge} :cursor
                AND (CAST(:commit AS bigint) IS NULL OR page.first_commit_seq<=:commit)
                AND {cold_page_is_visible("page")}
            ORDER BY seek_bound {direction},page.id
        """), params, execution_options={"yield_per": 64}).mappings()
        try:
            for candidate in pages:
                if nearest is not None and (
                    candidate["seek_bound"] <= nearest if prefer_latest else candidate["seek_bound"] >= nearest
                ):
                    break
                rows = self.reader.read_page(_manifest(candidate))
                for row in rows:
                    observed = row["observation_time"]
                    if (row["series_id"] == series_id and start <= observed < end
                            and (observed < cursor if prefer_latest else observed >= cursor)
                            and (commit is None or row["market_commit_seq"] <= commit)
                            and (nearest is None or (observed > nearest if prefer_latest else observed < nearest))):
                        nearest = observed
            return nearest
        finally:
            pages.close()

    def _location_rows(self, session, ids, *, require_all):
        ids = sorted(set(ids))
        for offset in range(0, len(ids), _BATCH_SIZE):
            batch = ids[offset:offset + _BATCH_SIZE]
            found = session.execute(
                select(_pages, _locators.c.id.label("requested_id"), _locators.c.ordinal, _locators.c.row_hash,
                       _locators.c.series_id, _locators.c.key_digest, _locators.c.revision, _locators.c.market_commit_seq)
                .join(_locators, _locators.c.page_id == _pages.c.id)
                .where(_locators.c.id.in_(batch), _PAGE_VISIBILITY)
            ).mappings().all()
            if require_all and {row["requested_id"] for row in found} != set(batch):
                raise RuntimeError("cold_fact_exact_identity_missing")
            yield from found

    def placement_claims(self, session, ids: Sequence[str], *, require_all=True):
        """Resolve manifest, ordinal and certified row hash without loading payloads."""
        return {
            row["requested_id"]: (ColdFactLocation(_manifest(row), row["ordinal"]), bytes(row["row_hash"]).hex())
            for row in self._location_rows(session, ids, require_all=require_all)
        }

    def locations_by_ids(self, session, ids: Sequence[str], *, require_all=True):
        return {identity: location for identity, (location, _) in self.placement_claims(
            session, ids, require_all=require_all,
        ).items()}

    def read_by_ids(self, session, ids: Sequence[str], *, require_all=True):
        locations, witnesses = {}, {}
        for claim in self._location_rows(session, ids, require_all=require_all):
            identity = claim["requested_id"]
            locations[identity] = ColdFactLocation(_manifest(claim), claim["ordinal"])
            witnesses[identity] = _locator_witness(claim)
        rows = self.reader.read_admitted_by_ids(locations)
        for identity, row in rows.items():
            _verify_locator(row, witnesses[identity])
        return rows

    def material_candidates(
        self, session, *, series_ids, material_hashes, evidence_key=None,
        include_canonical=True, include_legacy=True, max_candidates=10_000,
    ):
        """Return bounded (Fact ID, requested hash) hints for exact payload verification."""
        by_digest = {_key_digest(value): value for value in material_hashes}
        series_ids = sorted(set(series_ids))
        if not by_digest or not series_ids or not (include_canonical or include_legacy):
            return ()
        branches = []
        if include_canonical:
            branches.append(_materials.c.canonical.is_(True))
        if include_legacy:
            legacy = _materials.c.canonical.is_(False)
            if evidence_key is not None:
                legacy = and_(legacy, _materials.c.evidence_key_digest == _key_digest(evidence_key))
            branches.append(legacy)
        rows = session.execute(
            select(_materials.c.fact_id, _materials.c.hash_digest).distinct()
            .join(_locators, _locators.c.id == _materials.c.fact_id)
            .join(_pages, _pages.c.id == _locators.c.page_id).where(
                _materials.c.series_id.in_(series_ids),
                _materials.c.hash_digest.in_(by_digest),
                or_(*branches), _PAGE_VISIBILITY,
            ).order_by(_materials.c.fact_id, _materials.c.hash_digest).limit(max_candidates + 1)
        ).all()
        if len(rows) > max_candidates:
            raise RuntimeError("cold_fact_material_candidate_budget_exceeded")
        return tuple((fact_id, by_digest[bytes(digest)]) for fact_id, digest in rows)

    def normalization_spec_ids(self, session, spec_id: str):
        """Stream cold Fact IDs carrying the requested normalization reference."""
        result = session.execute(
            select(_normalization_specs.c.fact_id)
            .join(_locators, _locators.c.id == _normalization_specs.c.fact_id)
            .join(_pages, _pages.c.id == _locators.c.page_id).where(
                _normalization_specs.c.spec_digest == _key_digest(spec_id),
                _PAGE_VISIBILITY,
            ).order_by(_normalization_specs.c.fact_id), execution_options={"yield_per": 128},
        )
        try:
            yield from result.scalars()
        finally:
            result.close()

    def key_candidate_ids(self, session, *, series_id: int, key: str, max_candidates: int):
        """Find every revision ID claimed under a single observation key."""
        result = session.execute(select(_locators.c.id).join(
            _pages, _pages.c.id == _locators.c.page_id,
        ).where(
            _locators.c.series_id == series_id,
            _locators.c.key_digest == _key_digest(key),
            _PAGE_VISIBILITY,
        ).order_by(_locators.c.revision, _locators.c.id).limit(max_candidates + 1)).scalars().all()
        if len(result) > max_candidates:
            raise RuntimeError("cold_fact_key_candidate_budget_exceeded")
        return tuple(result)

    def key_candidates(self, session, *, series_id: int, keys: Sequence[str], max_candidates: int):
        """Resolve a bounded batch of observation keys through the HDD index."""
        requested = {_key_digest(key): key for key in keys}
        if not requested:
            return {}
        rows = session.execute(
            select(_locators.c.id, _locators.c.key_digest).join(
                _pages, _pages.c.id == _locators.c.page_id,
            ).where(
                _locators.c.series_id == series_id, _locators.c.key_digest.in_(requested), _PAGE_VISIBILITY,
            ).limit(max_candidates + 1),
        ).all()
        if len(rows) > max_candidates:
            raise RuntimeError("cold_fact_key_candidate_budget_exceeded")
        return {fact_id: requested[bytes(digest)] for fact_id, digest in rows}

    def commit_candidate_id(self, session, *, series_id: int, commit_seq: int):
        return session.execute(select(_locators.c.id).join(
            _pages, _pages.c.id == _locators.c.page_id,
        ).where(
            _locators.c.series_id == series_id,
            _locators.c.market_commit_seq == commit_seq,
            _PAGE_VISIBILITY,
        ).limit(2)).scalar_one_or_none()

    def trade_source_ids(self, session, *, derived_series_id: int):
        """Distinct admitted cold source IDs for trade series of the same instrument."""
        return session.execute(text(f"""
            SELECT DISTINCT source.value::bigint
            FROM market.series derived JOIN market.series trade
              ON trade.instrument_id=derived.instrument_id AND trade.fact_type='market.trade'
            JOIN market.cold_fact_page_series catalog ON catalog.series_id=trade.id
            JOIN market.cold_fact_pages page ON page.id=catalog.page_id
            CROSS JOIN LATERAL jsonb_array_elements(page.descriptor->'series') bounds(value)
            CROSS JOIN LATERAL jsonb_array_elements_text(bounds.value->'source_ids') source(value)
            WHERE derived.id=:series_id AND (bounds.value->>'series_id')::bigint=trade.id
              AND {cold_page_is_visible("page")}
            ORDER BY 1 LIMIT 2
        """), {"series_id": derived_series_id}).scalars().all()

    def book_session_ids(self, session, *, definition_id: str, session_id: str):
        result = session.execute(select(_book_sessions.c.fact_id)
            .join(_locators, _locators.c.id == _book_sessions.c.fact_id)
            .join(_pages, _pages.c.id == _locators.c.page_id).where(
            _book_sessions.c.session_digest == _book_session_digest(definition_id, session_id),
            _PAGE_VISIBILITY,
        ).order_by(_book_sessions.c.market_commit_seq, _book_sessions.c.fact_id),
            execution_options={"yield_per": 128},
        )
        try:
            yield from result.scalars()
        finally:
            result.close()

    def latest_for_keys(self, session, *, series_id: int, keys: Sequence[str], page_admission=None):
        """Probe HDD only for keys with no hot head and confirm every hash hit."""
        keys_by_digest = {}
        for key in set(keys):
            keys_by_digest.setdefault(_key_digest(key), set()).add(key)
        digests = list(keys_by_digest)
        latest = {}
        for offset in range(0, len(digests), _BATCH_SIZE):
            batch = digests[offset:offset + _BATCH_SIZE]
            found = session.execute(text(f"""
                SELECT page.*, head.fact_id, head.ordinal, head.series_id, head.key_digest,
                       head.revision, head.market_commit_seq, head.row_hash
                FROM (
                    SELECT l.key_digest, l.id AS fact_id, l.page_id, l.ordinal, l.series_id,
                        l.revision, l.market_commit_seq, l.row_hash,
                        rank() OVER (PARTITION BY l.key_digest ORDER BY l.revision DESC) AS head_rank
                    FROM market.cold_fact_locators l
                    JOIN market.cold_fact_pages visible ON visible.id=l.page_id
                    WHERE l.series_id=:series_id AND l.key_digest=ANY(CAST(:digests AS bytea[]))
                      AND {cold_page_is_visible("visible")}
                ) head JOIN market.cold_fact_pages page ON page.id = head.page_id
                WHERE head_rank = 1
            """), {"series_id": series_id, "digests": batch}).mappings().all()
            locations = {
                row["fact_id"]: ColdFactLocation(_manifest(row), row["ordinal"]) for row in found
            }
            if not locations:
                continue
            if page_admission is not None:
                manifests = {location.manifest.manifest_id: location.manifest for location in locations.values()}
                for manifest in manifests.values():
                    page_admission(manifest)
            decoded = self.reader.read_admitted_by_ids(locations)
            for candidate in found:
                row = decoded[candidate["fact_id"]]
                digest = bytes(candidate["key_digest"])
                _verify_locator(row, _locator_witness(candidate), key_lookup=True)
                exact_keys = keys_by_digest[digest]
                if len(exact_keys) != 1 or row["observation_key"] not in exact_keys:
                    # There is no safe absence/no-op decision without proving
                    # collision membership. Fail closed instead of guessing.
                    raise RuntimeError(f"cold_fact_key_digest_collision: series_id={series_id}")
                if row["observation_key"] in latest:
                    raise RuntimeError(f"cold_fact_key_lookup_ambiguous: series_id={series_id}")
                latest[row["observation_key"]] = row
        return latest

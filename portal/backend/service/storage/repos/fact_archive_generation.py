"""Generation-aware archive catalog preparation and read projections.

Large alias and canonical-dependency catalogs are written into a hidden
generation.  One small pointer makes a complete generation authoritative.
Historical pages without a pointer keep reading the legacy catalog tables.
"""
from __future__ import annotations

from hashlib import sha256
import json
import logging
from time import perf_counter

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash


PREPARATION_STATES = frozenset({"building", "ready", "active", "superseded", "abandoned"})
PREPARED_CATALOG_TABLES = {
    "aliases": "fact_archive_prepared_material_aliases",
    "canonical_dependencies": "fact_archive_prepared_canonical_dependencies",
}
LEGACY_CATALOG_TABLES = {
    "aliases": "fact_archive_material_aliases",
    "canonical_dependencies": "fact_archive_canonical_dependencies",
}

logger = logging.getLogger(__name__)


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def archive_preparation_id(manifest_id: str, generation: int) -> str:
    if type(manifest_id) is not str or not manifest_id or type(generation) is not int or generation < 1:
        raise ValueError("canonical_archive_preparation_identity_invalid")
    return "fap_" + sha256(f"{manifest_id}\n{generation}".encode("ascii")).hexdigest()[:48]


def catalog_batch_hash(*, preparation_id: str, catalog_kind: str, ordinal: int, rows) -> str:
    if catalog_kind not in PREPARED_CATALOG_TABLES or type(ordinal) is not int or ordinal < 0:
        raise ValueError("canonical_archive_catalog_batch_invalid")
    payload = {
        "schema_version": "market.fact_archive_catalog_batch.v1",
        "preparation_id": preparation_id,
        "catalog_kind": catalog_kind,
        "ordinal": ordinal,
        "rows": list(rows),
    }
    return sha256(canonical_json(payload).encode("ascii")).hexdigest()


def catalog_commitment(*, manifest_id: str, preparation_id: str, generation: int,
                       alias_count: int, canonical_dependency_count: int,
                       alias_batches, canonical_dependency_batches) -> str:
    payload = {
        "schema_version": "market.fact_archive_catalog_generation.v1",
        "manifest_id": manifest_id,
        "preparation_id": preparation_id,
        "generation": generation,
        "catalogs": {
            "aliases": {"row_count": alias_count, "batches": list(alias_batches)},
            "canonical_dependencies": {
                "row_count": canonical_dependency_count,
                "batches": list(canonical_dependency_batches),
            },
        },
    }
    return sha256(canonical_json(payload).encode("ascii")).hexdigest()


def active_catalog_tables(session, manifest_id: str) -> dict[str, str]:
    """Resolve both large catalogs from one authoritative pointer snapshot."""
    preparation_id = session.execute(text(
        "SELECT preparation_id FROM market.fact_archive_active_generations "
        "WHERE manifest_id=:manifest_id"
    ), {"manifest_id": manifest_id}).scalar_one_or_none()
    if preparation_id is None:
        return dict(LEGACY_CATALOG_TABLES)
    return dict(PREPARED_CATALOG_TABLES)


def material_alias_projection(alias: str = "aliases") -> str:
    """SQL relation exposing one authoritative alias generation per manifest."""
    if not alias.isidentifier():
        raise ValueError("canonical_archive_projection_alias_invalid")
    return f"""(
        SELECT legacy.manifest_id,legacy.fact_version_id,legacy.evidence_key,
               legacy.series_id,legacy.material_hash
        FROM market.fact_archive_material_aliases AS legacy
        WHERE NOT EXISTS (
            SELECT 1 FROM market.fact_archive_active_generations AS active
            WHERE active.manifest_id=legacy.manifest_id)
        UNION ALL
        SELECT active.manifest_id,prepared.fact_version_id,prepared.evidence_key,
               prepared.series_id,prepared.material_hash
        FROM market.fact_archive_active_generations AS active
        JOIN market.fact_archive_prepared_material_aliases AS prepared
          ON prepared.preparation_id=active.preparation_id
    ) AS {alias}"""


def canonical_dependency_projection(alias: str = "dependencies") -> str:
    """SQL relation exposing one authoritative dependency generation per manifest."""
    if not alias.isidentifier():
        raise ValueError("canonical_archive_projection_alias_invalid")
    return f"""(
        SELECT legacy.manifest_id,legacy.fact_version_id,legacy.row_hash
        FROM market.fact_archive_canonical_dependencies AS legacy
        WHERE NOT EXISTS (
            SELECT 1 FROM market.fact_archive_active_generations AS active
            WHERE active.manifest_id=legacy.manifest_id)
        UNION ALL
        SELECT active.manifest_id,prepared.fact_version_id,prepared.row_hash
        FROM market.fact_archive_active_generations AS active
        JOIN market.fact_archive_prepared_canonical_dependencies AS prepared
          ON prepared.preparation_id=active.preparation_id
    ) AS {alias}"""



def catalog_root(rows) -> str:
    return sha256(canonical_json(list(rows)).encode("ascii")).hexdigest()


def json_document(value):
    """Convert datetime/date and nested tuples into a stable JSON document."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_document(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_document(item) for item in value]
    return value


class ArchiveGenerationCoordinator:
    """Private bounded transactions for a hidden archive catalog generation.

    The caller supplies the existing lifecycle/day lock function. Every operation
    follows that same lock order before touching a preparation row.
    """

    def __init__(self, *, database, lock_day, release_day, check_budget, statement_timeout_ms,
                 owner_token_hash: str, verifier_version: str, lease_seconds: int = 120):
        if type(lease_seconds) is not int or lease_seconds < 30:
            raise ValueError("canonical_archive_preparation_lease_invalid")
        self.database = database
        self.lock_day = lock_day
        self.release_day = release_day
        self.check_budget = check_budget
        self.statement_timeout_ms = statement_timeout_ms
        self.owner_token_hash = owner_token_hash
        self.verifier_version = verifier_version
        self.lease_seconds = lease_seconds

    def _budget(self):
        if self.check_budget is not None:
            self.check_budget()

    def _owner_settings(self, session, preparation_id: str, fence: int):
        session.execute(text("SELECT set_config('qt.archive_preparation_owner',:owner,true)"),
                        {"owner": self.owner_token_hash})
        session.execute(text("SELECT set_config('qt.archive_preparation_fence',:fence,true)"),
                        {"fence": str(fence)})
        session.execute(text("SELECT set_config('qt.archive_preparation_id',:identity,true)"),
                        {"identity": preparation_id})

    @staticmethod
    def _require_day(row, day):
        if row["storage_day"] != day:
            raise RuntimeError(
                f"canonical_archive_preparation_day_changed: preparation_id={row['id']}"
            )

    @staticmethod
    def _validate_row(row, *, manifest_id, source_binding_hash):
        if row["manifest_id"] != manifest_id or row["source_binding_hash"] != source_binding_hash:
            raise RuntimeError("canonical_archive_preparation_binding_changed")
        if row["state"] not in {"building", "ready"}:
            raise RuntimeError(
                f"canonical_archive_preparation_not_resumable: preparation_id={row['id']} state={row['state']}"
            )

    def claim(self, *, day, page_ordinal: int, manifest, series_catalog, dependencies,
              aliases, canonical_dependencies):
        aliases = tuple(sorted((dict(row) for row in aliases),
                               key=lambda row: (row["fact_version_id"], row["evidence_key"])))
        canonical_dependencies = tuple(sorted((dict(row) for row in canonical_dependencies),
                                              key=lambda row: row["fact_version_id"]))
        series_catalog = tuple(json_document(dict(row)) for row in series_catalog)
        dependencies = tuple(sorted((dict(row) for row in dependencies),
                                    key=lambda row: (row["target_kind"], row["target_id"])))
        alias_root = catalog_root(aliases)
        dependency_root = catalog_root(canonical_dependencies)
        descriptor = json_document(manifest.to_dict())
        source_binding_hash = sha256(canonical_json({
            "schema_version": "market.fact_archive_preparation_binding.v1",
            "manifest": descriptor,
            "verifier_version": self.verifier_version,
            "series": series_catalog,
            "dependencies": dependencies,
            "alias_root": alias_root,
            "canonical_dependency_root": dependency_root,
        }).encode("ascii")).hexdigest()
        with self.database.session() as session:
            self.lock_day(session, day)
            row = session.execute(text("""
                SELECT * FROM market.fact_archive_preparations
                WHERE storage_day=:day AND page_ordinal=:page_ordinal
                  AND state IN ('building','ready')
                ORDER BY generation DESC LIMIT 1 FOR UPDATE
            """), {"day": day, "page_ordinal": page_ordinal}).mappings().one_or_none()
            if row is not None:
                self._require_day(row, day)
                compatible = (row["manifest_id"] == manifest.manifest_id
                              and row["verifier_version"] == self.verifier_version
                              and row["source_binding_hash"] == source_binding_hash)
                if not compatible:
                    abandoned = session.execute(text("""
                        UPDATE market.fact_archive_preparations
                        SET owner_token_hash=:owner,fence=fence+1,state='abandoned',
                            lease_expires_at=clock_timestamp(),updated_at=clock_timestamp()
                        WHERE id=:id AND lease_expires_at <= clock_timestamp()
                        RETURNING id
                    """), {"id": row["id"], "owner": self.owner_token_hash}).scalar_one_or_none()
                    if abandoned is None:
                        raise RuntimeError(
                            f"canonical_archive_preparation_busy: preparation_id={row['id']}"
                        )
                    row = None
                else:
                    if row["owner_token_hash"] != self.owner_token_hash:
                        claimed = session.execute(text("""
                            UPDATE market.fact_archive_preparations
                            SET owner_token_hash=:owner, fence=fence+1,
                                lease_expires_at=clock_timestamp()+(:lease || ' seconds')::interval,
                                updated_at=clock_timestamp()
                            WHERE id=:id AND lease_expires_at <= clock_timestamp()
                            RETURNING *
                        """), {"id": row["id"], "owner": self.owner_token_hash,
                               "lease": str(self.lease_seconds)}).mappings().one_or_none()
                        if claimed is None:
                            raise RuntimeError(
                                f"canonical_archive_preparation_busy: preparation_id={row['id']}"
                            )
                        row = claimed
                    else:
                        row = session.execute(text("""
                            UPDATE market.fact_archive_preparations
                            SET lease_expires_at=clock_timestamp()+(:lease || ' seconds')::interval,
                                updated_at=clock_timestamp()
                            WHERE id=:id RETURNING *
                        """), {"id": row["id"], "lease": str(self.lease_seconds)}).mappings().one()
                    return {"preparation": dict(row), "aliases": aliases,
                            "canonical_dependencies": canonical_dependencies}
            generation = int(session.execute(text("""
                SELECT coalesce(max(generation),0)+1 FROM market.fact_archive_preparations
                WHERE storage_day=:day AND page_ordinal=:page_ordinal
            """), {"day": day, "page_ordinal": page_ordinal}).scalar_one())
            preparation_id = archive_preparation_id(manifest.manifest_id, generation)
            row = session.execute(text("""
                INSERT INTO market.fact_archive_preparations
                    (id,manifest_id,storage_day,page_ordinal,generation,state,fence,
                     owner_token_hash,lease_expires_at,verifier_version,source_binding_hash,
                     manifest_descriptor,series_catalog,alias_count,canonical_dependency_count,
                     alias_root,canonical_dependency_root)
                VALUES (:id,:manifest_id,:day,:page_ordinal,:generation,'building',1,
                        :owner,clock_timestamp()+(:lease || ' seconds')::interval,:verifier,
                        :binding,CAST(:descriptor AS jsonb),CAST(:series AS jsonb),
                        :alias_count,:dependency_count,:alias_root,:dependency_root)
                RETURNING *
            """), {
                "id": preparation_id, "manifest_id": manifest.manifest_id, "day": day,
                "page_ordinal": page_ordinal, "generation": generation,
                "owner": self.owner_token_hash, "lease": str(self.lease_seconds),
                "verifier": self.verifier_version, "binding": source_binding_hash,
                "descriptor": canonical_json(descriptor), "series": canonical_json(series_catalog),
                "alias_count": len(aliases), "dependency_count": len(canonical_dependencies),
                "alias_root": alias_root, "dependency_root": dependency_root,
            }).mappings().one()
            self._owner_settings(session, preparation_id, 1)
            for dependency in dependencies:
                session.execute(text("""
                    INSERT INTO market.fact_archive_preparation_dependencies
                        (preparation_id,target_kind,target_id,object_key,object_sha256)
                    VALUES (:preparation_id,:target_kind,:target_id,:object_key,:object_sha256)
                """), {"preparation_id": preparation_id, **dependency})
            return {"preparation": dict(row), "aliases": aliases,
                    "canonical_dependencies": canonical_dependencies}

    def _stage_batch(self, *, day, preparation, catalog_kind: str, ordinal: int, rows):
        rows = tuple(rows)
        digest = catalog_batch_hash(preparation_id=preparation["id"],
                                    catalog_kind=catalog_kind, ordinal=ordinal, rows=rows)
        with self.database.session() as session:
            self.lock_day(session, day)
            row = session.execute(text(
                "SELECT * FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation["id"]}).mappings().one()
            self._require_day(row, day)
            now = session.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (row["state"] != "building" or row["fence"] != preparation["fence"]
                    or row["owner_token_hash"] != self.owner_token_hash
                    or row["lease_expires_at"] <= now):
                raise RuntimeError("canonical_archive_preparation_fence_lost")
            existing = session.execute(text("""
                SELECT row_count,first_key,last_key,content_hash
                FROM market.fact_archive_catalog_batches
                WHERE preparation_id=:id AND catalog_kind=:kind AND batch_ordinal=:ordinal
            """), {"id": row["id"], "kind": catalog_kind, "ordinal": ordinal}).mappings().one_or_none()
            keys = ([rows[0]["fact_version_id"], rows[0].get("evidence_key")]
                    if catalog_kind == "aliases" else [rows[0]["fact_version_id"]],
                    [rows[-1]["fact_version_id"], rows[-1].get("evidence_key")]
                    if catalog_kind == "aliases" else [rows[-1]["fact_version_id"]])
            expected = {"row_count": len(rows), "first_key": keys[0],
                        "last_key": keys[1], "content_hash": digest}
            if existing is not None:
                if json_document(dict(existing)) != json_document(expected):
                    raise RuntimeError("canonical_archive_catalog_batch_receipt_changed")
                return False
            self._owner_settings(session, row["id"], row["fence"])
            if catalog_kind == "aliases":
                result = session.execute(text("""
                    INSERT INTO market.fact_archive_prepared_material_aliases
                        (preparation_id,fact_version_id,evidence_key,batch_ordinal,series_id,material_hash)
                    SELECT :id,input.fact_version_id,input.evidence_key,:ordinal,
                           input.series_id,input.material_hash
                    FROM unnest(CAST(:fact_ids AS text[]),CAST(:keys AS text[]),
                                CAST(:series_ids AS bigint[]),CAST(:hashes AS text[]))
                         WITH ORDINALITY input(fact_version_id,evidence_key,series_id,material_hash,position)
                    ORDER BY input.position
                """), {"id": row["id"], "ordinal": ordinal,
                       "fact_ids": [item["fact_version_id"] for item in rows],
                       "keys": [item["evidence_key"] for item in rows],
                       "series_ids": [item["series_id"] for item in rows],
                       "hashes": [item["material_hash"] for item in rows]})
            else:
                result = session.execute(text("""
                    INSERT INTO market.fact_archive_prepared_canonical_dependencies
                        (preparation_id,fact_version_id,batch_ordinal,row_hash)
                    SELECT :id,input.fact_version_id,:ordinal,input.row_hash
                    FROM unnest(CAST(:fact_ids AS text[]),CAST(:hashes AS text[]))
                         WITH ORDINALITY input(fact_version_id,row_hash,position)
                    ORDER BY input.position
                """), {"id": row["id"], "ordinal": ordinal,
                       "fact_ids": [item["fact_version_id"] for item in rows],
                       "hashes": [item["row_hash"] for item in rows]})
            if result.rowcount != len(rows):
                raise RuntimeError("canonical_archive_prepared_insert_count_mismatch")
            session.execute(text("""
                INSERT INTO market.fact_archive_catalog_batches
                    (preparation_id,catalog_kind,batch_ordinal,row_count,first_key,last_key,content_hash)
                VALUES (:id,:kind,:ordinal,:count,CAST(:first_key AS jsonb),CAST(:last_key AS jsonb),:digest)
            """), {"id": row["id"], "kind": catalog_kind, "ordinal": ordinal,
                   "count": len(rows), "first_key": canonical_json(keys[0]),
                   "last_key": canonical_json(keys[1]), "digest": digest})
            session.execute(text("""
                UPDATE market.fact_archive_preparations
                SET lease_expires_at=clock_timestamp()+(:lease || ' seconds')::interval,
                    updated_at=clock_timestamp() WHERE id=:id
            """), {"id": row["id"], "lease": str(self.lease_seconds)})
            return True

    def stage(self, *, day, claim, alias_batch_rows: int, dependency_batch_rows: int):
        preparation = claim["preparation"]
        if preparation["state"] == "ready":
            return preparation
        for kind, rows, size in (
            ("aliases", claim["aliases"], alias_batch_rows),
            ("canonical_dependencies", claim["canonical_dependencies"], dependency_batch_rows),
        ):
            for ordinal, offset in enumerate(range(0, len(rows), size)):
                self._budget()
                batch = rows[offset:offset + size]
                started = perf_counter()
                try:
                    inserted = self._stage_batch(
                        day=day, preparation=preparation, catalog_kind=kind,
                        ordinal=ordinal, rows=batch,
                    )
                except BaseException:
                    logger.info(
                        "canonical_archive_prepared_catalog_batch | preparation_id=%s "
                        "catalog=%s batch_ordinal=%s rows=%s outcome=failed seconds=%.6f",
                        preparation["id"], kind, ordinal, len(batch),
                        perf_counter() - started,
                    )
                    raise
                logger.info(
                    "canonical_archive_prepared_catalog_batch | preparation_id=%s "
                    "catalog=%s batch_ordinal=%s rows=%s outcome=%s seconds=%.6f",
                    preparation["id"], kind, ordinal, len(batch),
                    "committed" if inserted else "resumed", perf_counter() - started,
                )
        return self.mark_ready(day=day, preparation=preparation)

    def mark_ready(self, *, day, preparation):
        with self.database.session() as session:
            self.lock_day(session, day)
            row = session.execute(text(
                "SELECT * FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation["id"]}).mappings().one()
            self._require_day(row, day)
            if row["state"] == "ready":
                return dict(row)
            now = session.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (row["state"] != "building" or row["fence"] != preparation["fence"]
                    or row["owner_token_hash"] != self.owner_token_hash
                    or row["lease_expires_at"] <= now):
                raise RuntimeError("canonical_archive_preparation_fence_lost")
            receipts = session.execute(text("""
                SELECT catalog_kind,batch_ordinal,row_count,first_key,last_key,content_hash
                FROM market.fact_archive_catalog_batches WHERE preparation_id=:id
                ORDER BY catalog_kind,batch_ordinal
            """), {"id": row["id"]}).mappings().all()
            grouped = {kind: [] for kind in PREPARED_CATALOG_TABLES}
            for receipt in receipts:
                grouped[receipt["catalog_kind"]].append(dict(receipt))
            persisted_roots = {}
            for kind, expected_count in (("aliases", row["alias_count"]),
                                         ("canonical_dependencies", row["canonical_dependency_count"])):
                values = grouped[kind]
                if [item["batch_ordinal"] for item in values] != list(range(len(values))):
                    raise RuntimeError("canonical_archive_catalog_batch_gap")
                if kind == "aliases":
                    stored = session.execute(text("""
                        SELECT batch_ordinal,fact_version_id,evidence_key,series_id,material_hash
                        FROM market.fact_archive_prepared_material_aliases
                        WHERE preparation_id=:id ORDER BY fact_version_id,evidence_key
                    """), {"id": row["id"]}).mappings().all()
                    projected = [
                        {"fact_version_id": item["fact_version_id"],
                         "evidence_key": item["evidence_key"], "series_id": item["series_id"],
                         "material_hash": item["material_hash"]}
                        for item in stored
                    ]
                else:
                    stored = session.execute(text("""
                        SELECT batch_ordinal,fact_version_id,row_hash
                        FROM market.fact_archive_prepared_canonical_dependencies
                        WHERE preparation_id=:id ORDER BY fact_version_id
                    """), {"id": row["id"]}).mappings().all()
                    projected = [
                        {"fact_version_id": item["fact_version_id"], "row_hash": item["row_hash"]}
                        for item in stored
                    ]
                if len(stored) != expected_count or sum(item["row_count"] for item in values) != expected_count:
                    raise RuntimeError("canonical_archive_preparation_count_mismatch")
                persisted_roots[kind] = catalog_root(projected)
                by_ordinal = {item["batch_ordinal"]: [] for item in values}
                for item, projection in zip(stored, projected):
                    if item["batch_ordinal"] not in by_ordinal:
                        raise RuntimeError("canonical_archive_catalog_batch_gap")
                    by_ordinal[item["batch_ordinal"]].append(projection)
                previous_last = None
                for receipt in values:
                    batch = by_ordinal[receipt["batch_ordinal"]]
                    if len(batch) != receipt["row_count"] or not batch:
                        raise RuntimeError("canonical_archive_catalog_batch_receipt_changed")
                    first_key = ([batch[0]["fact_version_id"], batch[0].get("evidence_key")]
                                 if kind == "aliases" else [batch[0]["fact_version_id"]])
                    last_key = ([batch[-1]["fact_version_id"], batch[-1].get("evidence_key")]
                                if kind == "aliases" else [batch[-1]["fact_version_id"]])
                    if (json_document(receipt["first_key"]) != first_key
                            or json_document(receipt["last_key"]) != last_key
                            or receipt["content_hash"] != catalog_batch_hash(
                                preparation_id=row["id"], catalog_kind=kind,
                                ordinal=receipt["batch_ordinal"], rows=batch,
                            ) or (previous_last is not None and first_key <= previous_last)):
                        raise RuntimeError("canonical_archive_catalog_batch_receipt_changed")
                    previous_last = last_key
            if (persisted_roots["aliases"] != row["alias_root"]
                    or persisted_roots["canonical_dependencies"] != row["canonical_dependency_root"]):
                raise RuntimeError("canonical_archive_preparation_root_mismatch")
            commitment = catalog_commitment(
                manifest_id=row["manifest_id"], preparation_id=row["id"], generation=row["generation"],
                alias_count=row["alias_count"], canonical_dependency_count=row["canonical_dependency_count"],
                alias_batches=[item["content_hash"] for item in grouped["aliases"]],
                canonical_dependency_batches=[item["content_hash"] for item in grouped["canonical_dependencies"]],
            )
            result = session.execute(text("""
                UPDATE market.fact_archive_preparations
                SET state='ready',catalog_commitment=:commitment,updated_at=clock_timestamp()
                WHERE id=:id RETURNING *
            """), {"id": row["id"], "commitment": commitment}).mappings().one()
            return dict(result)

    def activate(self, *, day, preparation_id: str, fence: int):
        """Atomically publish the small manifest catalogs and generation pointer."""
        with self.database.session() as session:
            partition = self.lock_day(session, day)
            row = session.execute(text(
                "SELECT * FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation_id}).mappings().one()
            self._require_day(row, day)
            if row["state"] == "active":
                return {"manifest_id": row["manifest_id"], "preparation_id": row["id"],
                        "generation": row["generation"], "catalog_commitment": row["catalog_commitment"]}
            if (row["state"] != "ready" or row["fence"] != fence
                    or row["owner_token_hash"] != self.owner_token_hash
                    or row["lease_expires_at"] <= session.execute(
                        text("SELECT clock_timestamp()")
                    ).scalar_one()):
                raise RuntimeError("canonical_archive_preparation_not_activatable")
            if partition["state"] != "sealed":
                raise RuntimeError("canonical_archive_partition_not_sealed")
            descriptor = row["manifest_descriptor"]
            previous = session.execute(text("""
                SELECT page_ordinal,last_commit_seq,last_id FROM market.fact_archive_manifests
                WHERE storage_day=:day ORDER BY page_ordinal DESC LIMIT 1
            """), {"day": day}).mappings().one_or_none()
            expected_ordinal = 0 if previous is None else previous["page_ordinal"] + 1
            if row["page_ordinal"] != expected_ordinal:
                raise RuntimeError("canonical_archive_activation_cursor_changed")
            session.execute(text("""
                INSERT INTO market.fact_archive_manifests
                    (id,storage_day,page_ordinal,object_key,object_sha256,manifest_hash,row_count,byte_count,
                     first_commit_seq,first_id,last_commit_seq,last_id,descriptor)
                VALUES (:id,:day,:ordinal,:object_key,:object_sha256,:manifest_hash,:row_count,:byte_count,
                        :first_commit_seq,:first_id,:last_commit_seq,:last_id,CAST(:descriptor AS jsonb))
            """), {
                "id": row["manifest_id"], "day": row["storage_day"], "ordinal": row["page_ordinal"],
                "object_key": descriptor["object_key"], "object_sha256": descriptor["object_sha256"],
                "manifest_hash": sha256(canonical_json(descriptor).encode("ascii")).hexdigest(), "row_count": descriptor["row_count"],
                "byte_count": descriptor["byte_count"], "first_commit_seq": descriptor["first_cursor"][0],
                "first_id": descriptor["first_cursor"][1], "last_commit_seq": descriptor["last_cursor"][0],
                "last_id": descriptor["last_cursor"][1], "descriptor": canonical_json(descriptor),
            })
            session.execute(text("""
                INSERT INTO market.fact_archive_series
                    (manifest_id,series_id,row_count,first_observation_at,last_observation_at,
                     first_known_at,last_known_at,first_accepted_at,last_accepted_at)
                SELECT :manifest_id,x.series_id,x.row_count,x.first_observation_at,x.last_observation_at,
                       x.first_known_at,x.last_known_at,x.first_accepted_at,x.last_accepted_at
                FROM jsonb_to_recordset(CAST(:series AS jsonb)) AS x(
                    series_id bigint,row_count bigint,first_observation_at timestamptz,last_observation_at timestamptz,
                    first_known_at timestamptz,last_known_at timestamptz,
                    first_accepted_at timestamptz,last_accepted_at timestamptz)
            """), {"manifest_id": row["manifest_id"], "series": canonical_json(row["series_catalog"])})
            session.execute(text("""
                INSERT INTO market.fact_archive_dependencies
                    (manifest_id,target_kind,target_id,object_key,object_sha256)
                SELECT :manifest_id,target_kind,target_id,object_key,object_sha256
                FROM market.fact_archive_preparation_dependencies WHERE preparation_id=:preparation_id
            """), {"manifest_id": row["manifest_id"], "preparation_id": row["id"]})
            session.execute(text("""
                INSERT INTO market.fact_archive_active_generations
                    (manifest_id,preparation_id,generation,catalog_commitment)
                VALUES (:manifest_id,:preparation_id,:generation,:commitment)
            """), {"manifest_id": row["manifest_id"], "preparation_id": row["id"],
                   "generation": row["generation"], "commitment": row["catalog_commitment"]})
            session.execute(text("""
                UPDATE market.fact_archive_preparations SET state='active',updated_at=clock_timestamp()
                WHERE id=:id
            """), {"id": row["id"]})
            return {"manifest_id": row["manifest_id"], "preparation_id": row["id"],
                    "generation": row["generation"], "catalog_commitment": row["catalog_commitment"]}

    def release(self, *, day, preparation_id: str, fence: int) -> bool:
        """Release this worker's resumable preparation after a handled failure.

        A process crash still relies on the bounded lease. A caller that has
        already observed and unwound an exception can relinquish only its exact
        owner/fence so another worker can resume committed hidden batches
        immediately. Active or superseded generations are never changed.
        """
        with self.database.session() as session:
            self.release_day(session, day)
            current = session.execute(text(
                "SELECT id,storage_day FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation_id}).mappings().one()
            self._require_day(current, day)
            changed = session.execute(text("""
                UPDATE market.fact_archive_preparations
                SET lease_expires_at=clock_timestamp(),updated_at=clock_timestamp()
                WHERE id=:id AND fence=:fence AND owner_token_hash=:owner
                  AND state IN ('building','ready')
                RETURNING id
            """), {"id": preparation_id, "fence": fence,
                   "owner": self.owner_token_hash}).scalar_one_or_none()
            return changed is not None


    @staticmethod
    def _catalogs_for_preparation(session, manifest_id: str, preparation_id: str):
        return {
            "series": [dict(row) for row in session.execute(text("""
                SELECT series_id,row_count,first_observation_at,last_observation_at,
                       first_known_at,last_known_at,first_accepted_at,last_accepted_at
                FROM market.fact_archive_series WHERE manifest_id=:manifest_id ORDER BY series_id
            """), {"manifest_id": manifest_id}).mappings().all()],
            "aliases": [dict(row) for row in session.execute(text("""
                SELECT fact_version_id,series_id,evidence_key,material_hash
                FROM market.fact_archive_prepared_material_aliases
                WHERE preparation_id=:preparation_id ORDER BY fact_version_id,evidence_key
            """), {"preparation_id": preparation_id}).mappings().all()],
            "dependencies": [dict(row) for row in session.execute(text("""
                SELECT target_kind,target_id,object_key,object_sha256
                FROM market.fact_archive_preparation_dependencies
                WHERE preparation_id=:preparation_id ORDER BY target_kind,target_id
            """), {"preparation_id": preparation_id}).mappings().all()],
            "canonical_dependencies": [dict(row) for row in session.execute(text("""
                SELECT fact_version_id,row_hash
                FROM market.fact_archive_prepared_canonical_dependencies
                WHERE preparation_id=:preparation_id ORDER BY fact_version_id
            """), {"preparation_id": preparation_id}).mappings().all()],
        }

    @staticmethod
    def _old_large_catalogs(session, manifest_id: str, active):
        if active is None:
            aliases = session.execute(text("""
                SELECT fact_version_id,series_id,evidence_key,material_hash
                FROM market.fact_archive_material_aliases
                WHERE manifest_id=:manifest_id ORDER BY fact_version_id,evidence_key
            """), {"manifest_id": manifest_id}).mappings().all()
            dependencies = session.execute(text("""
                SELECT fact_version_id,row_hash
                FROM market.fact_archive_canonical_dependencies
                WHERE manifest_id=:manifest_id ORDER BY fact_version_id
            """), {"manifest_id": manifest_id}).mappings().all()
        else:
            aliases = session.execute(text("""
                SELECT fact_version_id,series_id,evidence_key,material_hash
                FROM market.fact_archive_prepared_material_aliases
                WHERE preparation_id=:preparation_id ORDER BY fact_version_id,evidence_key
            """), {"preparation_id": active["preparation_id"]}).mappings().all()
            dependencies = session.execute(text("""
                SELECT fact_version_id,row_hash
                FROM market.fact_archive_prepared_canonical_dependencies
                WHERE preparation_id=:preparation_id ORDER BY fact_version_id
            """), {"preparation_id": active["preparation_id"]}).mappings().all()
        return [dict(row) for row in aliases], [dict(row) for row in dependencies]

    @staticmethod
    def _assert_monotone(old_rows, new_rows, keys, *, catalog):
        current = {tuple(row[key] for key in keys): row for row in new_rows}
        for row in old_rows:
            identity = tuple(row[key] for key in keys)
            if current.get(identity) != row:
                raise RuntimeError(
                    f"canonical_archive_supersession_not_monotone: catalog={catalog}"
                )

    def supersede_verified(self, *, day, preparation, verification_receipt: dict) -> dict:
        """Publish a complete monotone replacement and its receipt atomically.

        The old pointer remains authoritative until every membership, hold, and
        receipt check below succeeds. A failed transaction changes no authority.
        """
        with self.database.session() as session:
            self.lock_day(session, day)
            target = session.execute(text(
                "SELECT * FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation["id"]}).mappings().one()
            self._require_day(target, day)
            now = session.execute(text("SELECT clock_timestamp()" )).scalar_one()
            if (target["state"] != "ready" or target["fence"] != preparation["fence"]
                    or target["owner_token_hash"] != self.owner_token_hash
                    or target["lease_expires_at"] <= now
                    or target["verifier_version"] != self.verifier_version):
                raise RuntimeError("canonical_archive_supersession_fence_lost")
            page = session.execute(text(
                "SELECT * FROM market.fact_archive_manifests WHERE id=:id FOR SHARE"
            ), {"id": target["manifest_id"]}).mappings().one()
            if (target["manifest_descriptor"] != page["descriptor"]
                    or target["page_ordinal"] != page["page_ordinal"]):
                raise RuntimeError("canonical_archive_supersession_manifest_changed")
            active = session.execute(text("""
                SELECT pointer.*,preparation.state AS preparation_state
                FROM market.fact_archive_active_generations AS pointer
                JOIN market.fact_archive_preparations AS preparation
                  ON preparation.id=pointer.preparation_id
                WHERE pointer.manifest_id=:manifest_id FOR UPDATE OF pointer,preparation
            """), {"manifest_id": target["manifest_id"]}).mappings().one_or_none()
            if active is not None and (active["preparation_state"] != "active"
                                       or target["generation"] <= active["generation"]):
                raise RuntimeError("canonical_archive_supersession_state_invalid")
            catalogs = self._catalogs_for_preparation(session, target["manifest_id"], target["id"])
            old_aliases, old_dependencies = self._old_large_catalogs(
                session, target["manifest_id"], active,
            )
            self._assert_monotone(
                old_aliases, catalogs["aliases"], ("fact_version_id", "evidence_key"),
                catalog="aliases",
            )
            self._assert_monotone(
                old_dependencies, catalogs["canonical_dependencies"], ("fact_version_id",),
                catalog="canonical_dependencies",
            )
            existing_holds = [dict(row) for row in session.execute(text("""
                SELECT target_kind,target_id,object_key,object_sha256
                FROM market.fact_archive_dependencies
                WHERE manifest_id=:manifest_id ORDER BY target_kind,target_id
            """), {"manifest_id": target["manifest_id"]}).mappings().all()]
            self._assert_monotone(
                existing_holds, catalogs["dependencies"], ("target_kind", "target_id"),
                catalog="dependencies",
            )
            expired = session.execute(text("""
                SELECT pending.target_id
                FROM market.fact_archive_preparation_dependencies AS pending
                WHERE pending.preparation_id=:preparation_id AND EXISTS (
                    SELECT 1 FROM market.storage_lifecycle_events AS lifecycle
                    WHERE lifecycle.action='archive_expire' AND lifecycle.event_type='completed'
                      AND lifecycle.target_kind=pending.target_kind
                      AND lifecycle.target_id=pending.target_id)
                ORDER BY pending.target_kind,pending.target_id LIMIT 1
            """), {"preparation_id": target["id"]}).scalar_one_or_none()
            if expired is not None:
                raise RuntimeError(
                    f"canonical_archive_dependency_expired: target_id={expired}"
                )
            catalog_hash = archive_evidence_hash(catalogs)
            evidence = {
                "manifest_id": page["id"], "manifest_hash": page["manifest_hash"],
                "storage_day": page["storage_day"].isoformat(),
                "page_ordinal": page["page_ordinal"],
                "verifier_version": self.verifier_version, "catalog_hash": catalog_hash,
            }
            expected_receipt = {
                "manifest_id": page["id"], "manifest_hash": page["manifest_hash"],
                "verifier_version": self.verifier_version, "catalog_hash": catalog_hash,
                "verification_hash": archive_evidence_hash(evidence),
            }
            if dict(verification_receipt) != expected_receipt:
                raise RuntimeError("canonical_archive_supersession_receipt_changed")
            session.execute(text("""
                INSERT INTO market.fact_archive_dependencies
                    (manifest_id,target_kind,target_id,object_key,object_sha256)
                SELECT :manifest_id,pending.target_kind,pending.target_id,
                       pending.object_key,pending.object_sha256
                FROM market.fact_archive_preparation_dependencies AS pending
                WHERE pending.preparation_id=:preparation_id AND NOT EXISTS (
                    SELECT 1 FROM market.fact_archive_dependencies AS existing
                    WHERE existing.manifest_id=:manifest_id
                      AND existing.target_kind=pending.target_kind
                      AND existing.target_id=pending.target_id)
            """), {"manifest_id": target["manifest_id"], "preparation_id": target["id"]})
            if active is None:
                session.execute(text("""
                    INSERT INTO market.fact_archive_active_generations
                        (manifest_id,preparation_id,generation,catalog_commitment)
                    VALUES (:manifest_id,:preparation_id,:generation,:commitment)
                """), {"manifest_id": target["manifest_id"], "preparation_id": target["id"],
                       "generation": target["generation"], "commitment": target["catalog_commitment"]})
            else:
                session.execute(text("""
                    UPDATE market.fact_archive_active_generations
                    SET preparation_id=:preparation_id,generation=:generation,
                        catalog_commitment=:commitment,activated_at=clock_timestamp()
                    WHERE manifest_id=:manifest_id
                """), {"manifest_id": target["manifest_id"], "preparation_id": target["id"],
                       "generation": target["generation"], "commitment": target["catalog_commitment"]})
            session.execute(text(
                "UPDATE market.fact_archive_preparations SET state='active',updated_at=clock_timestamp() WHERE id=:id"
            ), {"id": target["id"]})
            if active is not None:
                session.execute(text(
                    "UPDATE market.fact_archive_preparations SET state='superseded',updated_at=clock_timestamp() WHERE id=:id"
                ), {"id": active["preparation_id"]})
            session.execute(text("""
                INSERT INTO market.fact_archive_verifications
                    (manifest_id,verifier_version,manifest_hash,catalog_hash,verification_hash)
                VALUES (:manifest_id,:verifier_version,:manifest_hash,:catalog_hash,:verification_hash)
            """), expected_receipt)
            session.execute(text("""
                INSERT INTO market.fact_archive_verification_generations
                    (manifest_id,verifier_version,preparation_id,catalog_commitment)
                VALUES (:manifest_id,:verifier_version,:preparation_id,:catalog_commitment)
            """), {"manifest_id": target["manifest_id"],
                   "verifier_version": self.verifier_version,
                   "preparation_id": target["id"],
                   "catalog_commitment": target["catalog_commitment"]})
            return {
                "manifest_id": target["manifest_id"], "preparation_id": target["id"],
                "generation": target["generation"], "catalog_commitment": target["catalog_commitment"],
                "prior_preparation_id": active["preparation_id"] if active is not None else None,
                "verification_hash": expected_receipt["verification_hash"],
            }

    def abandon(self, *, day, preparation_id: str, fence: int):
        with self.database.session() as session:
            self.lock_day(session, day)
            current = session.execute(text(
                "SELECT id,storage_day FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation_id}).mappings().one()
            self._require_day(current, day)
            changed = session.execute(text("""
                UPDATE market.fact_archive_preparations
                SET state='abandoned',updated_at=clock_timestamp()
                WHERE id=:id AND fence=:fence AND owner_token_hash=:owner AND state IN ('building','ready')
                RETURNING id
            """), {"id": preparation_id, "fence": fence,
                   "owner": self.owner_token_hash}).scalar_one_or_none()
            if changed is None:
                raise RuntimeError("canonical_archive_preparation_abandon_fence_lost")

    def cleanup_abandoned(self, *, day, preparation_id: str, max_rows: int) -> dict:
        if type(max_rows) is not int or not 1 <= max_rows <= 10_000:
            raise ValueError("canonical_archive_preparation_cleanup_limit_invalid")
        deleted = {}
        with self.database.session() as session:
            self.lock_day(session, day)
            row = session.execute(text(
                "SELECT id,storage_day,state FROM market.fact_archive_preparations WHERE id=:id FOR UPDATE"
            ), {"id": preparation_id}).mappings().one()
            self._require_day(row, day)
            if row["state"] != "abandoned":
                raise RuntimeError("canonical_archive_preparation_cleanup_not_abandoned")
            session.execute(text("SELECT set_config('qt.archive_cleanup_preparation_id',:id,true)"),
                            {"id": preparation_id})
            remaining = max_rows
            for table in ("fact_archive_catalog_batches", "fact_archive_prepared_material_aliases",
                          "fact_archive_prepared_canonical_dependencies", "fact_archive_preparation_dependencies"):
                if remaining <= 0:
                    break
                count = session.execute(text(f"""
                    WITH doomed AS (SELECT ctid FROM market.{table}
                        WHERE preparation_id=:id ORDER BY ctid LIMIT :limit)
                    DELETE FROM market.{table} target USING doomed
                    WHERE target.ctid=doomed.ctid
                """), {"id": preparation_id, "limit": remaining}).rowcount
                deleted[table] = count
                remaining -= count
        return {"preparation_id": preparation_id, "deleted": deleted,
                "deleted_rows": sum(deleted.values()), "tombstone_retained": True}

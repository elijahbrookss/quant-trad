"""Frozen deployed f673cb62 canonical ingestion method for migration fixtures.

The function body is AST-identical to that revision's PostgresMarketDataRepository
method. Bind its code to the repository module globals in the fixture; this keeps
relative imports, collection fencing and database acknowledgement behavior intact.
It is not imported by production code and never provisions v2 identities.
"""
from __future__ import annotations

class SourceV1Ingestion:
    def _ingest_canonical_rows_with_session(
        self,
        session,
        *,
        run_id: str,
        series_id: int,
        rows: Sequence[CanonicalFact],
        allow_corrections: bool,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        inserted_count = 0
        corrected_count = 0
        noop_count = 0
        max_commit_seq = 0
        storage_day = None
        from .fact_references import lock_canonical_raw_references

        session.execute(
            text("SELECT pg_advisory_xact_lock(:series_id)"),
            {"series_id": series_id},
        )
        self._assert_collection_fence(
            session,
            series_id=series_id,
            collection_fence=collection_fence,
        )
        source_id, source = self._canonical_source_for_run(session, run_id)
        latest_by_key = {row["observation_key"]: row for row in session.execute(text("""
            SELECT DISTINCT ON (observation_key) observation_key,revision,row_hash,market_commit_seq
            FROM market.fact_versions WHERE series_id=:series_id AND observation_key=ANY(:keys)
            ORDER BY observation_key,revision DESC
        """), {"series_id": series_id, "keys": [fact.observation_key for fact in rows]}).mappings()}
        # A true no-op creates no reference and must remain envelope-only,
        # including after cold movement. Acquire a deterministic manifest-lock
        # set for the genuine writes before inserting any of their payloads.
        pending_rows = []
        pending_hashes = {key: row["row_hash"] for key, row in latest_by_key.items()}
        for fact in rows:
            if pending_hashes.get(fact.observation_key) != fact.row_hash:
                pending_rows.append(fact)
                pending_hashes[fact.observation_key] = fact.row_hash
        lock_canonical_raw_references(session, pending_rows)
        for fact in rows:
            if fact.source.identity_key != source.identity_key:
                raise ValueError(
                    "market_data_ingest_invalid: canonical Fact source disagrees "
                    f"with ingestion run run_id={run_id}"
                )
            latest = latest_by_key.get(fact.observation_key)
            if latest is not None:
                max_commit_seq = max(
                    max_commit_seq, int(latest["market_commit_seq"])
                )
                if str(latest["row_hash"]) == fact.row_hash:
                    noop_count += 1
                    continue
                if not allow_corrections:
                    raise RuntimeError(
                        "market_data_correction_rejected: immutable consumer path "
                        "cannot accept changed canonical Fact "
                        f"series_id={series_id} "
                        f"observation_key={fact.observation_key}"
                    )
            revision = 1 if latest is None else int(latest["revision"]) + 1
            if storage_day is None:
                # Dedupe is an envelope-only operation, including after archive
                # reclamation. Provision placement only for a genuine new row.
                storage_day = current_fact_storage_day(session)
            version_id = build_fact_version_id(
                series_id=series_id,
                observation_key=fact.observation_key,
                revision=revision,
                row_hash=fact.row_hash,
            )
            commit_seq = int(
                session.execute(
                    text(
                        """
                        INSERT INTO market.fact_versions (
                            id, storage_day, series_id, observation_key, revision,
                            source_id, ingestion_run_id, fact_type,
                            payload_schema_id, payload_contract_hash,
                            observation_time, observation_time_method,
                            source_published_at, received_at, accepted_at,
                            known_at, known_at_method, transformation_id,
                            external_event_key, external_event_group_key,
                            external_event_component_key, state,
                            payload_hash, material_hash,
                            provenance_schema_id, provenance_hash,
                            quality_schema_id, quality_hash, row_hash
                        ) VALUES (
                            :id, :storage_day, :series_id, :observation_key, :revision,
                            :source_id, :ingestion_run_id, :fact_type,
                            :payload_schema_id, :payload_contract_hash,
                            :observation_time, :observation_time_method,
                            :source_published_at, :received_at, :accepted_at,
                            :known_at, :known_at_method, :transformation_id,
                            :external_event_key, :external_event_group_key,
                            :external_event_component_key, :state,
                            :payload_hash,
                            :material_hash, :provenance_schema_id,
                            :provenance_hash,
                            :quality_schema_id,
                            :quality_hash, :row_hash
                        )
                        RETURNING market_commit_seq
                        """
                    ),
                    {
                        "id": version_id,
                        "storage_day": storage_day,
                        "series_id": series_id,
                        "observation_key": fact.observation_key,
                        "revision": revision,
                        "source_id": source_id,
                        "ingestion_run_id": run_id,
                        "fact_type": fact.fact_type,
                        "payload_schema_id": fact.payload_schema_id,
                        "payload_contract_hash": fact.payload_contract_hash,
                        "observation_time": fact.observation_time,
                        "observation_time_method": fact.observation_time_method,
                        "source_published_at": fact.source_published_at,
                        "received_at": fact.received_at,
                        "accepted_at": fact.accepted_at,
                        "known_at": fact.known_at,
                        "known_at_method": fact.known_at_method,
                        "transformation_id": fact.transformation_id,
                        "external_event_key": fact.external_event_key,
                        "external_event_group_key": fact.external_event_group_key,
                        "external_event_component_key": fact.external_event_component_key,
                        "state": fact.state.value,
                        "payload_hash": fact.payload_hash,
                        "material_hash": fact.material_hash,
                        "provenance_schema_id": fact.provenance_schema_id,
                        "provenance_hash": fact.provenance_hash,
                        "quality_schema_id": fact.quality_schema_id,
                        "quality_hash": fact.quality_hash,
                        "row_hash": fact.row_hash,
                    },
                ).scalar_one()
            )
            session.execute(text(
                "INSERT INTO market.fact_hot_payloads "
                "(storage_day, id, series_id, payload_schema_id, observation_time, payload, provenance, quality) "
                "VALUES (:storage_day, :id, :series_id, :schema, :observation_time, "
                "CAST(:payload AS jsonb), CAST(:provenance AS jsonb), CAST(:quality AS jsonb))"
            ), {
                "storage_day": storage_day, "id": version_id, "series_id": series_id,
                "schema": fact.payload_schema_id, "observation_time": fact.observation_time,
                "payload": _json_text(fact.payload), "provenance": _json_text(fact.provenance),
                "quality": _json_text(fact.quality),
            })
            max_commit_seq = max(max_commit_seq, commit_seq)
            latest_by_key[fact.observation_key] = {"revision": revision, "row_hash": fact.row_hash,
                                                  "market_commit_seq": commit_seq}
            if latest is None:
                inserted_count += 1
            else:
                corrected_count += 1
        if max_commit_seq == 0:
            max_commit_seq = self._current_commit_seq_with_session(session)
        session.execute(
            text(
                """
                UPDATE market.ingestion_runs
                SET status = 'completed', finished_at = now(),
                    inserted_count = :inserted_count,
                    corrected_count = :corrected_count,
                    noop_count = :noop_count
                WHERE id = :run_id AND status = 'running'
                """
            ),
            {
                "run_id": run_id,
                "inserted_count": inserted_count,
                "corrected_count": corrected_count,
                "noop_count": noop_count,
            },
        )
        return IngestionOutcome(
            ingestion_run_id=run_id,
            requested_count=len(rows),
            inserted_count=inserted_count,
            corrected_count=corrected_count,
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
        )

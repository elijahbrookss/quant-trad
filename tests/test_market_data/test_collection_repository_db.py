from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid

import pytest

from market_data.contracts import (
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    OpenInterestFact,
    SourceIdentity,
)
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.storage.repos.market_collection import (
    market_collection_repo,
)
from portal.backend.service.storage.repos.market_data import market_data_repo


pytestmark = pytest.mark.db


def test_collection_claim_fences_fact_mutation_and_completion() -> None:
    token = uuid.uuid4().hex
    instrument_id = f"collector-db-{token[:20]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol=f"BTC-{token[:8].upper()}-PERP",
                instrument_type="perp",
                can_short=True,
                short_requires_borrow=False,
                has_funding=True,
                extra_metadata={},
            )
        )
    source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="poll_api",
            adapter_version=f"collector-db-test.{token}",
        )
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=OPEN_INTEREST_FACT_TYPE,
        timeframe_seconds=None,
        contract_version=OPEN_INTEREST_FACT_VERSION,
    )
    scheduled = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=5)
    definition_id = f"mcd_{token}"
    market_collection_repo.upsert_definition(
        definition_id=definition_id,
        source_id=source_id,
        series_id=series_id,
        poll_interval_seconds=60,
        max_attempts=3,
        enabled=True,
        config={"provider_product_id": "BTC-PERP-INTX"},
        next_scheduled_at=scheduled,
    )

    claim = market_collection_repo.claim_due(
        owner_id="db-worker-1", definition_id=definition_id
    )
    assert claim is not None
    assert claim.definition_id == definition_id
    assert market_collection_repo.claim_due(
        owner_id="db-worker-2", definition_id=definition_id
    ) is None
    accepted = max(datetime.now(UTC), claim.scheduled_for)
    fact = OpenInterestFact(
        sample_time=claim.scheduled_for,
        value=1234,
        received_at=accepted,
        accepted_at=accepted,
        known_at=accepted,
        known_at_method="platform_acceptance",
    )
    outcome = market_data_repo.ingest_open_interest(
        series_id=series_id,
        source_id=source_id,
        facts=[fact],
        provenance={"fixture": "collector-fence"},
        ingestion_run_id=claim.attempt_id,
        allow_corrections=False,
        collection_fence=claim.fence(),
    )
    market_collection_repo.complete(
        claim,
        ingestion_run_id=outcome.ingestion_run_id,
        evidence={"market_commit_seq": outcome.max_commit_seq},
    )
    attempts = market_collection_repo.list_attempts(
        definition_id=definition_id
    )
    assert attempts[0]["status"] == "succeeded"
    assert attempts[0]["ingestion_run_id"] == claim.attempt_id

    stale_time = claim.scheduled_for + timedelta(minutes=1)
    stale_fact = OpenInterestFact(
        sample_time=stale_time,
        value=1300,
        received_at=stale_time,
        accepted_at=stale_time,
        known_at=stale_time,
        known_at_method="platform_acceptance",
    )
    with pytest.raises(RuntimeError, match="ownership_lost"):
        market_data_repo.ingest_open_interest(
            series_id=series_id,
            source_id=source_id,
            facts=[stale_fact],
            provenance={"fixture": "stale-fence"},
            ingestion_run_id=f"mca_stale_{token[:24]}",
            allow_corrections=False,
            collection_fence=claim.fence(),
        )
    assert market_data_repo.read_open_interest(
        series_id=series_id,
        start=stale_time,
        end=stale_time + timedelta(seconds=1),
    ) == []

from __future__ import annotations

import pytest

from portal.backend.service.market.collector_definition_enrollment_service import (
    COINBASE_PRODUCT_COLLECTORS,
    CoinbaseFuturesCollectorPack,
    CollectorDefinitionEnrollmentService,
    product_enrollment_confirmation,
)


class _StreamService:
    def __init__(self) -> None:
        self.manifests = []

    def apply_stream_enrollment_manifest(self, *, manifest):
        self.manifests.append(manifest)
        channel = manifest.enrollments[0].channels[0]
        return {
            "fleet_id": manifest.fleet_id,
            "manifest_hash": manifest.manifest_hash,
            "definitions": [{"id": f"stream-{channel}"}],
        }


class _ScheduledCollector:
    def __init__(self) -> None:
        self.open_interest = []
        self.funding_rate = []

    def create_coinbase_open_interest_definition(self, **kwargs):
        self.open_interest.append(kwargs)
        return {"id": "scheduled-open-interest"}

    def create_coinbase_funding_rate_definition(self, **kwargs):
        self.funding_rate.append(kwargs)
        return {"id": "scheduled-funding-rate"}


def _instrument() -> dict:
    return {
        "id": "6f25184d-0fe3-4ec9-85e9-f605bdd3c36d",
        "symbol": "LNP-20DEC30-CDE",
        "datasource": "COINBASE",
        "exchange": "COINBASE_DIRECT",
        "instrument_type": "FUTURE",
        "metadata": {
            "instrument_fields": {
                "tick_size": "0.001",
                "qty_step": "1",
                "contract_size": "50",
                "base_currency": "LINK",
                "quote_currency": "USD",
                "has_funding": True,
            },
            "provider_metadata": {
                "product": {
                    "future_product_details": {"contract_size": "50"}
                }
            },
        },
    }


def test_coinbase_product_pack_enrolls_any_validated_future_without_product_code(
) -> None:
    resolver_calls = []

    def resolve(*args, **kwargs):
        resolver_calls.append((args, kwargs))
        return _instrument(), None

    streams = _StreamService()
    scheduled = _ScheduledCollector()
    pack = CoinbaseFuturesCollectorPack(
        instrument_resolver=resolve,
        stream_service=streams,
        scheduled_collector=scheduled,
    )
    service = CollectorDefinitionEnrollmentService((pack,))

    result = service.enroll_product(
        provider="coinbase",
        venue="coinbase_direct",
        product_id="lnp-20dec30-cde",
        collector_types=COINBASE_PRODUCT_COLLECTORS,
        poll_interval_seconds=60,
        request_id="operator-link-1",
        actor_id="operator:test",
        reason="Add LINK market-data coverage",
        confirmation=product_enrollment_confirmation(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            product_id="LNP-20DEC30-CDE",
        ),
    )

    assert result["status"] == "enrolled"
    assert result["product_id"] == "LNP-20DEC30-CDE"
    assert result["product_contract"] == {
        "provider_size_unit": "contracts",
        "contract_size": "50",
        "base_currency": "LINK",
        "quote_currency": "USD",
        "product_definition_version_id": (
            "coinbase.LNP-20DEC30-CDE.product_contract.v1"
        ),
    }
    assert set(result["definitions"]) == set(COINBASE_PRODUCT_COLLECTORS)
    assert resolver_calls[0][0] == (
        "COINBASE",
        "COINBASE_DIRECT",
        "LNP-20DEC30-CDE",
    )
    assert resolver_calls[0][1]["force_refresh"] is True
    assert [manifest.enrollments[0].channels for manifest in streams.manifests] == [
        ("market_trades", "heartbeats"),
        ("level2", "heartbeats"),
    ]
    for manifest in streams.manifests:
        enrollment = manifest.enrollments[0]
        assert enrollment.instrument_id == _instrument()["id"]
        assert enrollment.product_contract.provider_product_id == (
            "LNP-20DEC30-CDE"
        )
        assert enrollment.continuous is True
    assert scheduled.open_interest[0]["enabled"] is True
    assert scheduled.funding_rate[0]["enabled"] is True
    assert scheduled.open_interest[0]["poll_interval_seconds"] == 60


def test_product_enrollment_requires_exact_confirmation_before_provider_call(
) -> None:
    called = False

    def resolve(*_args, **_kwargs):
        nonlocal called
        called = True
        return _instrument(), None

    service = CollectorDefinitionEnrollmentService(
        (CoinbaseFuturesCollectorPack(instrument_resolver=resolve),)
    )

    with pytest.raises(
        ValueError, match="collector_product_enrollment_confirmation_required"
    ):
        service.enroll_product(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            product_id="LNP-20DEC30-CDE",
            request_id="operator-link-2",
            actor_id="operator:test",
            reason="Add LINK market-data coverage",
            confirmation="yes",
        )

    assert called is False


def test_product_enrollment_rejects_collector_outside_registered_pack() -> None:
    service = CollectorDefinitionEnrollmentService(
        (
            CoinbaseFuturesCollectorPack(
                instrument_resolver=lambda *_args, **_kwargs: (
                    _instrument(),
                    None,
                )
            ),
        )
    )

    with pytest.raises(
        ValueError, match="collector_product_enrollment_collector_unsupported"
    ):
        service.enroll_product(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            product_id="LNP-20DEC30-CDE",
            collector_types=("liquidations",),
            request_id="operator-link-3",
            actor_id="operator:test",
            reason="Attempt unsupported collector",
            confirmation=product_enrollment_confirmation(
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                product_id="LNP-20DEC30-CDE",
            ),
        )

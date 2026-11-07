"""Synthetic fixtures only: these do not assert live Coinalyze market availability."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

import pytest

from data_providers.numeric_facts import NumericAcquisitionBudget, NumericFactBinding
from data_providers.providers import coinalyze
from market_data.fact_registry import get_fact_contract

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
END = START + timedelta(minutes=3)
OBSERVED = END + timedelta(days=1)
STAMP = int(START.timestamp())
BUDGET = NumericAcquisitionBudget(max_requests=2, max_logs=3, max_blocks=1, max_retries=0)
PRODUCT = dict(symbol="BTC_TEST", exchange="TEST_EXCHANGE", symbol_on_exchange="BTC-TEST-PERP",
               base_asset="BTC", quote_asset="USDT", is_perpetual=True, expire_at=None,
               oi_lq_vol_denominated_in="QUOTE_ASSET")


def binding(side="long", product=None):
    product = product or PRODUCT
    unit = {"BASE_ASSET": product["base_asset"], "QUOTE_ASSET": product["quote_asset"],
            "CONTRACTS": "CONTRACTS"}[product["oi_lq_vol_denominated_in"]]
    return NumericFactBinding(
        manifest_id="synthetic", manifest_hash="a" * 64, id=side, enabled=True,
        adapter=coinalyze.ADAPTER_ID, instrument_id="synthetic-btc-instrument", instrument_role="explicit",
        fact_type="derivatives.liquidation_volume", contract_version="derivatives.liquidation_volume.v1",
        unit=unit, dimensions={"liquidated_side": side, "bucket_seconds": 60, "volume_unit": unit,
                              "exchange": product["exchange"], "product": product["symbol_on_exchange"]},
        endpoint_ref=coinalyze.ENDPOINT_REF,
        source={"provider": "COINALYZE", "venue": "COINALYZE_AGGREGATE",
                "source_kind": "rest_history", "adapter_version": coinalyze.ADAPTER_ID},
        schedule={"expected_update_interval_seconds": 60, "deviation_threshold_basis_points": None},
        quality_policy={"max_staleness_seconds": 120, "stale_behavior": "gap"},
        risk={"official_catalog_url": "https://api.coinalyze.net/v1/doc/", "market_risk_tier": "fixture",
              "deprecation_status": "unknown", "verified_at": "2026-10-01"},
        config={"product": dict(product), "interval": "1min", "credential_ref": "synthetic-ref", "confirmations": 0},
    )


class Transport:
    def __init__(self, rows=None, catalogue=None, payload=None):
        self.calls = []
        self.catalogue = [PRODUCT] if catalogue is None else catalogue
        self.payload = payload if payload is not None else [dict(symbol="BTC_TEST", history=rows or [])]

    def __call__(self, path, params, ref):
        self.calls.append((path, params, ref))
        value = self.catalogue if path == "future-markets" else self.payload
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        return raw, OBSERVED


def acquire(transport, *, bound=None, start=START, end=END, budget=BUDGET):
    return coinalyze.CoinalyzeProvider(transport=transport).fetch_history(
        bound or binding(), start=start, end=end, budget=budget,
    )


@pytest.mark.parametrize("side,expected", [("long", "123456789.1234567890123456789"), ("short", "0")])
def test_exact_values_receipt_timing_and_half_open_request(side, expected):
    raw = ('[{"symbol":"BTC_TEST","history":[{"t":%d,"l":123456789.1234567890123456789,"s":0}]}]' % STAMP).encode()
    transport = Transport(payload=raw)
    batch = acquire(transport, bound=binding(side))
    fact = batch.observations[0]
    assert fact.value == Decimal(expected)
    assert fact.effective_at == START
    assert fact.known_at == OBSERVED and fact.known_at_method == "platform_receipt"
    assert fact.source_published_at is None
    assert fact.provenance["bucket_end"] == STAMP + 60
    assert fact.source_event_component_key == side
    assert batch.status == "partial" and batch.budget_requests_used == 2
    assert transport.calls[1][1] == dict(symbols="BTC_TEST", interval="1min", **{"from": STAMP, "to": STAMP + 179, "convert_to_usd": "false"})
    assert [gap.classification for gap in batch.gaps] == ["provider_completeness_unverified", "provider_bucket_missing"]
    assert (batch.gaps[1].start, batch.gaps[1].end) == (START + timedelta(minutes=1), END)


def test_zero_is_retained_missing_is_not_synthesized_and_material_is_stable():
    transport = Transport(rows=[dict(t=STAMP, l=0, s=0), dict(t=STAMP + 120, l=3, s=4)])
    first = acquire(transport)
    second = acquire(transport)
    assert [item.value for item in first.observations] == [Decimal(0), Decimal(3)]
    assert (first.gaps[1].start, first.gaps[1].end) == (START + timedelta(minutes=1), START + timedelta(minutes=2))
    assert first.observations[0].source_event_material_hash == second.observations[0].source_event_material_hash


@pytest.mark.parametrize("change", [{"exchange": "OTHER"}, {"symbol_on_exchange": "OTHER"},
                                      {"oi_lq_vol_denominated_in": "BASE_ASSET"}, {"is_perpetual": False, "expire_at": 2000000000000}])
def test_catalogue_drift_fails_before_history_request(change):
    transport = Transport(catalogue=[{**PRODUCT, **change}])
    with pytest.raises(coinalyze.CoinalyzeError, match="catalogue product"):
        acquire(transport)
    assert len(transport.calls) == 1


@pytest.mark.parametrize("rows", [
    [dict(t=STAMP, l=-1, s=0)], [dict(t=STAMP, l=True, s=0)],
    [dict(t=STAMP, l="1", s=0)], [dict(t=STAMP, l=1)],
    [dict(t=STAMP, l=1, s=0)] * 2,
    [dict(t=STAMP + 1, l=1, s=0)], [dict(t=STAMP + 180, l=1, s=0)],
    [dict(t=True, l=1, s=0)], [dict(t=STAMP, l=1, s=None)],
])
def test_malformed_buckets_fail_loud(rows):
    with pytest.raises(coinalyze.CoinalyzeError):
        acquire(Transport(rows=rows))


@pytest.mark.parametrize("payload", [b"not-json", b'[NaN]', [], [{"symbol": "OTHER", "history": []}]])
def test_malformed_response_is_not_empty_success(payload):
    with pytest.raises(coinalyze.CoinalyzeError):
        acquire(Transport(payload=payload))


@pytest.mark.parametrize("change", [dict(start=START + timedelta(seconds=1)),
    dict(start=START.replace(tzinfo=None)), dict(end=START),
    dict(end=START + timedelta(minutes=1501)),
    dict(budget=replace(BUDGET, max_requests=1)), dict(budget=replace(BUDGET, max_logs=2)),
])
def test_invalid_ranges_and_budgets_make_no_requests(change):
    transport = Transport()
    with pytest.raises(coinalyze.CoinalyzeError):
        acquire(transport, **change)
    assert transport.calls == []


@pytest.mark.parametrize("side,duration", [("buy", "60"), ("long", "0"), ("short", "1.5")])
def test_canonical_contract_rejects_invalid_side_or_bucket(side, duration):
    contract = get_fact_contract("derivatives.liquidation_volume")
    with pytest.raises(ValueError, match="market_fact_dimensions_invalid"):
        contract.validate_numeric_value(value=Decimal(1), unit="USDT",
            dimensions={**binding().dimensions, "liquidated_side": side, "bucket_seconds": duration})


@pytest.mark.parametrize("denomination,unit", [("BASE_ASSET", "BTC"), ("QUOTE_ASSET", "USDT"), ("CONTRACTS", "CONTRACTS")])
def test_units_come_from_catalogue_without_usd_assumption(denomination, unit):
    product = {**PRODUCT, "oi_lq_vol_denominated_in": denomination}
    bound = binding(product=product)
    acquire(Transport(catalogue=[product]), bound=bound)
    assert bound.unit == unit


def test_factory_and_binding_validation_do_not_read_credentials(monkeypatch):
    monkeypatch.setattr(coinalyze, "load_credentials", lambda *a, **k: pytest.fail("credential access"))
    from data_providers.providers.factory import ProviderRegistry
    provider = ProviderRegistry().get_provider("COINALYZE")
    assert isinstance(provider, coinalyze.CoinalyzeProvider)
    provider.validate_binding(binding())
    with pytest.raises(coinalyze.CoinalyzeError, match="current_unsupported"):
        provider.fetch_current(binding(), budget=BUDGET)


@pytest.mark.parametrize("status", [301, 401, 429, 500])
def test_http_uses_header_only_and_sanitizes_failures(monkeypatch, status):
    calls = []
    class Response:
        status_code = status
        headers = {"Retry-After": "12"}
        def __enter__(self): return self
        def __exit__(self, *args): pass
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response()
    monkeypatch.setattr(coinalyze, "load_credentials", lambda *a, **k: {"COINALYZE_API_KEY": "synthetic-secret"})
    monkeypatch.setattr(coinalyze.requests, "get", get)
    monkeypatch.setattr(coinalyze, "_LAST_REQUEST_AT", 0)
    with pytest.raises(coinalyze.CoinalyzeError) as error:
        coinalyze.CoinalyzeProvider._http_get("liquidation-history", {"symbols": "BTC_TEST"}, "synthetic-ref")
    assert "synthetic-secret" not in str(error.value)
    assert error.value.retry_after_seconds == 12
    assert len(calls) == 1
    assert calls[0][1]["headers"] == {"api_key": "synthetic-secret"}
    assert calls[0][1]["params"] == {"symbols": "BTC_TEST"}
    assert calls[0][1]["allow_redirects"] is False


def test_empty_response_preserves_entire_range_as_missing():
    batch = acquire(Transport())
    assert batch.observations == ()
    assert batch.status == "partial"
    assert (batch.gaps[1].start, batch.gaps[1].end) == (START, END)


def test_duplicate_json_keys_are_rejected():
    with pytest.raises(coinalyze.CoinalyzeError, match="finite JSON"):
        acquire(Transport(payload=b'[{"symbol":"BTC_TEST","symbol":"OTHER","history":[]}]'))


def test_perpetual_expiry_absent_and_zero_are_equivalent():
    for expiry in ("absent", 0):
        product = dict(PRODUCT)
        if expiry == "absent":
            product.pop("expire_at")
        else:
            product["expire_at"] = 0
        assert acquire(Transport(catalogue=[product])).status == "partial"


def test_registered_builder_is_lazy_and_rejects_wrong_endpoint(monkeypatch):
    from portal.backend.service.market.numeric_fact_acquisition import NumericFactAcquisitionService
    monkeypatch.setattr(coinalyze, "load_credentials", lambda *a, **k: pytest.fail("credential access"))
    assert isinstance(NumericFactAcquisitionService._build_provider(binding()), coinalyze.CoinalyzeProvider)
    with pytest.raises(ValueError, match="endpoint_invalid"):
        NumericFactAcquisitionService._build_provider(replace(binding(), endpoint_ref="UNKNOWN"))


def test_canonical_receipt_idempotency_corrections_and_causal_visibility():
    from data_providers.numeric_facts import NumericFactManifest
    from market_data.contracts import SourceIdentity
    from market_data.requirements import causal_numeric_fact_records
    from portal.backend.service.market.numeric_fact_acquisition import NumericFactAcquisitionService, NumericAcquisitionAuthorization
    from tests.test_market_data.test_numeric_fact_acquisition import _MemoryStore
    bound = binding()
    manifest = NumericFactManifest("market.numeric_fact_sources.v1", bound.manifest_id, True, bound.manifest_hash, (bound,), "synthetic")
    store = _MemoryStore()
    service = NumericFactAcquisitionService(store=store)
    source = SourceIdentity(**bound.source)
    store.register_source(source)
    kwargs = dict(manifest=manifest, binding=bound, source=source, source_id=store.source_id,
                  series_id=store.series_id, repair=True,
                  authorization=NumericAcquisitionAuthorization(True, "test", "offline fixture"))
    initial = acquire(Transport(rows=[dict(t=STAMP, l=1, s=0)]))
    assert service._persist_batch(batch=initial, **kwargs)[:4] == (1, 0, 0, 0)
    event_key = initial.observations[0].source_event_key
    frozen_first = tuple(store.revisions[event_key])
    first = frozen_first[0].fact
    assert first.received_at == OBSERVED
    assert first.known_at >= first.accepted_at > OBSERVED
    assert causal_numeric_fact_records(frozen_first, evaluation_time=END) == ()
    assert service._persist_batch(batch=initial, **kwargs)[:4] == (0, 0, 0, 1)
    changed = acquire(Transport(rows=[dict(t=STAMP, l=2, s=0)]))
    assert service._persist_batch(batch=changed, **kwargs)[:4] == (0, 1, 0, 0)
    revisions = tuple(store.revisions[event_key])
    assert [item.fact.value for item in revisions] == [Decimal(1), Decimal(2)]
    assert revisions[-1].fact.known_at >= revisions[0].fact.known_at
    assert revisions[-1].fact.known_at_method == "reconciliation_observed_at"
    assert frozen_first[0].fact.value == Decimal(1)
    assert [item.fact.value for item in causal_numeric_fact_records(revisions, evaluation_time=first.known_at)] == [Decimal(1)]
    # An incomplete later response is not evidence that the prior event vanished.
    assert service._persist_batch(batch=acquire(Transport()), **kwargs)[2] == 0
    assert len(store.revisions[event_key]) == 2
    assert all(item["status"] == "partial" for item in store.coverage)


def test_raw_numeric_token_is_preserved_independently_of_exact_value():
    raw = ('[{"symbol":"BTC_TEST","history":[{"t":%d,"l":1.2300e-12,"s":0}]}]' % STAMP).encode()
    fact = acquire(Transport(payload=raw)).observations[0]
    assert fact.raw_value == "1.2300e-12"
    assert fact.value == Decimal("0.00000000000123")


def test_multi_symbol_binding_is_rejected_before_any_request():
    product = {**PRODUCT, "symbol": "BTC_TEST,SECOND"}
    transport = Transport(catalogue=[product])
    with pytest.raises(coinalyze.CoinalyzeError, match="one catalogue symbol"):
        acquire(transport, bound=binding(product=product))
    assert transport.calls == []


def test_typed_numeric_and_canonical_hashes_agree_for_frozen_decoder():
    from market_data.contracts import SourceIdentity
    from portal.backend.service.market.numeric_fact_acquisition import NumericFactAcquisitionService
    from portal.backend.service.storage.repos.market_data import _numeric_to_canonical, _TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS
    bound = binding()
    batch = acquire(Transport(rows=[dict(t=STAMP, l=123, s=0)]))
    fact = NumericFactAcquisitionService._to_fact(bound, batch.observations[0], accepted_at=OBSERVED + timedelta(seconds=1))
    canonical = _numeric_to_canonical(fact, source=SourceIdentity(**bound.source), provenance={})
    assert canonical.row_hash == fact.row_hash
    assert canonical.payload["value"] == "123"
    assert fact.contract_version in _TYPED_RECORD_DECODER_PAYLOAD_SCHEMAS

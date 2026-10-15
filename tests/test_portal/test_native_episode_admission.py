"""Public episode requirements over canonical storage adapters; no Check dispatch."""
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_market_trade, canonicalize_trade_flow
from market_data.contracts import CandleFact, CandleRecord, SourceIdentity
from market_data.structure import (MarketTradeFact, aggregate_trade_bucket, COINBASE_AGGRESSOR_TRANSFORM_VERSION,
    TradeCoverageIntervalVersion, CoverageStatus, OrderingAssurance, ArchiveStatus)
from portal.backend.controller.research import router
from portal.backend.service.research import catalog, planning, service
from portal.backend.service.storage.repos import market_data
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver

START = datetime(2024, 4, 3, tzinfo=UTC)


def at(seconds):
    return START + timedelta(seconds=seconds)


@pytest.fixture
def native_admission(monkeypatch):
    """Replace persistence I/O only; retain native repository dispatch/decoders."""
    source = SourceIdentity(provider='TEST', venue='ISOLATED', source_kind='test', adapter_version='native-admission.v1')
    metadata = [dict(series_id=i, instrument_id='episode-test', fact_type=family,
                     contract_version=contract, timeframe_seconds=seconds, dimensions={})
                for i, family, contract, seconds in (
                    (1, 'candle.ohlcv', 'candle.ohlcv.v1', 60),
                    (2, 'market.bbo', 'market.bbo.v1', 1),
                    (3, 'market.depth_observation', 'market.depth_band.v1', 1),
                    (4, 'market.trade_flow', 'market.trade_flow.v1', 1),
                    (5, 'market.trade', 'market.trade.v1', None))]
    canonical = {i: [] for i in range(2, 6)}
    trade = MarketTradeFact(provider_product_id='TEST-USD', provider_trade_id='trade-1', delivery_kind='update',
        price=Decimal('100'), provider_size=Decimal('1'), provider_size_unit='base', maker_side='SELL',
        aggressor_side='BUY', aggressor_transform_version=COINBASE_AGGRESSOR_TRANSFORM_VERSION, contract_quantity=None,
        base_quantity=Decimal('1'), quote_notional=Decimal('100'), base_currency='TEST', quote_currency='USD',
        product_definition_version_id='synthetic-product', provider_event_time=at(150.25),
        provider_message_time=at(150.3), received_at=at(151), accepted_at=at(152), known_at=at(152),
        provider_sequence_num=1, connection_epoch=0, receive_ordinal=1, event_ordinal=0, trade_ordinal=0,
        raw_record_id='disposable-in-memory-raw', coverage_interval_id='synthetic-coverage')
    canonical[5] = [CanonicalFactRecord(series_id=5, source_id=1, revision=1, market_commit_seq=1,
                                       fact=canonicalize_market_trade(trade, source=source))]
    coverage = TradeCoverageIntervalVersion(interval_id='synthetic-coverage', revision=1,
        definition_id='synthetic-definition', session_id='synthetic-session', connection_epoch=0,
        provider_product_id='TEST-USD', channel='market_trades', status=CoverageStatus.CLOSED_VALID,
        ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS, archive_status=ArchiveStatus.COMPLETE,
        opening_raw_record_id=trade.raw_record_id, opening_receive_ordinal=1, opening_effective_at=at(-1),
        last_raw_record_id=trade.raw_record_id, last_receive_ordinal=1, last_effective_at=at(301),
        closing_raw_record_id=trade.raw_record_id, closing_receive_ordinal=1, closing_effective_at=at(301),
        canonicalization_watermark_ordinal=1, archive_complete_through_ordinal=1, known_at=at(600),
        first_provider_sequence_num=1, last_provider_sequence_num=1)
    for second in range(300):
        flow = aggregate_trade_bucket([trade] if second == 150 else [], interval_seconds=1,
                                     bucket_start=at(second), coverage=coverage, computed_at=at(600))
        canonical[4].append(CanonicalFactRecord(series_id=4, source_id=1, revision=1,
            market_commit_seq=second + 2, fact=canonicalize_trade_flow(flow, source=source)))
        common = dict(bucket_end=at(second+1), validity_interval_id='synthetic-validity',
                      source_state_hash=str(second), provider_size_unit='base', mid_price='100',
                      bid_quantity='10', ask_quantity='10', bid_base_quantity='10', ask_base_quantity='10',
                      input_fingerprint=str(second))
        for sid, family, schema, payload in (
            (2, 'market.bbo', 'market.bbo.v1', {**common, 'product_definition_version_id':'synthetic-product',
                'bid_price':'99', 'ask_price':'101', 'spread':'2', 'spread_bps':'200'}),
            (3, 'market.depth_observation', 'market.depth_band.v1', {**common, 'bbo_input_fingerprint':str(second),
                'band_bps':5, 'bid_notional':'1000', 'ask_notional':'1000', 'imbalance':'0'})):
            fact = CanonicalFact(fact_type=family, payload_schema_id=schema, source=source,
                observation_key=str(second), observation_time=at(second), observation_time_method='fixture_bucket',
                accepted_at=at(600), known_at=at(600), known_at_method='derived_input_watermark',
                transformation_id='disposable-fixture.v1', payload=payload)
            canonical[sid].append(CanonicalFactRecord(series_id=sid, source_id=1, revision=1,
                                                     market_commit_seq=second+2, fact=fact))
    candles = [CandleRecord(series_id=1, revision=1, market_commit_seq=1, ingestion_run_id='disposable',
        source_identity_key=source.identity_key, source=source, provenance={},
        fact=CandleFact(open_time=at(i), close_time=at(i+60), open=100., high=101., low=99., close=100.,
                        volume=1., known_at=at(i+60), known_at_method='interval_close_inferred', accepted_at=at(600)))
        for i in range(0,300,60)]
    calls = []

    def read(**kwargs):
        calls.append(dict(kwargs))
        return [r for r in canonical[kwargs['series_id']]
                if kwargs['start'] <= r.fact.observation_time < kwargs['end']
                and r.market_commit_seq <= kwargs.get('as_of_commit_seq', 10000)
                and (not kwargs.get('source_identity_keys') or r.source_identity_key in kwargs['source_identity_keys'])
                and (kwargs.get('known_at_lte') is None or r.fact.known_at <= kwargs['known_at_lte'])]

    class Result:
        def __init__(self, row): self.row = row
        def mappings(self): return self
        def first(self): return self.row
    class Session:
        def execute(self, sql, parameters):
            assert 'fact_type' in str(sql) and 'contract_version' in str(sql)
            return Result(next(row for row in metadata if row['series_id'] == parameters['series_id']))
    @contextmanager
    def session(): yield Session()

    repo = market_data.market_data_repo
    from portal.backend.service.market import runtime_market_data
    def forbidden(*args, **kwargs):
        raise AssertionError('admission cannot acquire, freeze or execute a Check')
    class CollectorTrap:
        def __init__(self, **kwargs): pass
        def __getattr__(self, name): return forbidden(name)
    monkeypatch.setattr(runtime_market_data, 'MarketDataCollectorService', CollectorTrap)
    monkeypatch.setattr(repo, 'freeze_dataset', forbidden)
    monkeypatch.setattr(service, 'run_research_check', forbidden)
    monkeypatch.setattr(market_data, 'db', SimpleNamespace(session=session))
    monkeypatch.setattr(repo, 'current_commit_seq', lambda: 10000)
    monkeypatch.setattr(repo, 'list_series_metadata', lambda **kw: metadata)
    monkeypatch.setattr(repo, 'list_gap_evidence', lambda **kw: [])
    monkeypatch.setattr(repo, 'inspect_candle_selection', None)
    monkeypatch.setattr(repo, 'read_candles', lambda **kw: candles)
    monkeypatch.setattr(repo, 'read_facts', read)
    monkeypatch.setattr(repo, 'read_fact_revisions', read)
    monkeypatch.setattr(repo, 'resolve_series_id', lambda **kw: next(row['series_id'] for row in metadata
        if row['fact_type'] == kw['fact_type'] and row['contract_version'] == kw['contract_version']))
    real_plan = planning.plan_research_check
    monkeypatch.setattr(service, 'plan_research_check', lambda d,r,**kw: real_plan(d,r,
        instrument_loader=lambda _: {'id':'episode-test'},
        indicator_planner=lambda *a,**k: {'indicators':[], 'requirements':[], 'warmup_bars':0}, **kw))
    payload = deepcopy(catalog._EXAMPLES['event_fact_analysis@14'])
    payload['scope'].update(instrument_id='episode-test', start=at(0).isoformat(), end=at(300).isoformat())
    payload['outcomes']['analysis_as_of'] = at(900).isoformat()
    for value in payload['inputs']:
        value['source_policy'] = {'mode':'exact', 'source_identity_key':source.identity_key}
    app = FastAPI(); app.include_router(router, prefix='/api/research')
    return SimpleNamespace(api=TestClient(app), payload=payload, records=canonical, calls=calls,
                           source=source, repo=repo)


def test_full_episode_http_requirements_admits_native_flow_and_trade_records(native_admission):
    f = native_admission
    response = f.api.post('/api/research/checks/requirements', json=f.payload)
    assert response.status_code == 200, response.text
    value = response.json()
    assert not value['plan']['missing_coverage']
    assert len(value['plan']['market_data_requirements']) == 5
    assert value['plan']['materialization_range']['as_of_commit_seq'] == 10000
    assert value['plan']['outcome_tail']['seconds'] == 0
    assert all(datetime.fromisoformat(r['required_start'].replace('Z','+00:00')) == at(0)
               and datetime.fromisoformat(r['required_end'].replace('Z','+00:00')) == at(300)
               for r in value['plan']['market_data_requirements'])
    assert value['provider_call_performed'] is False and value['preparation_authorized'] is False
    native = [c for c in f.calls if c['series_id'] in (4,5)]
    assert {c['series_id'] for c in native} == {4,5}
    assert all(c['as_of_commit_seq'] == 10000 and c['source_identity_keys'] == (f.source.identity_key,) for c in native)


def test_missing_native_second_and_wrong_source_remain_explicit(native_admission):
    f = native_admission
    f.records[4] = [r for r in f.records[4] if r.fact.observation_time != at(120)]
    response = f.api.post('/api/research/checks/requirements', json=f.payload)
    assert response.status_code == 200, response.text
    missing = response.json()['plan']['missing_coverage']
    assert any(r['alias']=='flow' and r['reason']=='source_bound_interval_unrecorded' for r in missing)
    next(r for r in f.payload['inputs'] if r['alias']=='flow')['source_policy']['source_identity_key'] = 'f'*64
    response = f.api.post('/api/research/checks/requirements', json=f.payload)
    assert response.status_code == 200, response.text
    assert any(r['alias']=='flow' and r['reason']=='no_facts_in_required_range'
               for r in response.json()['plan']['missing_coverage'])


def test_native_canonical_history_preserves_cutoff_watermark_and_exact_sources(native_admission):
    f = native_admission
    response = f.api.post('/api/research/checks/requirements', json=f.payload)
    assert response.status_code == 200, response.text
    requirements = response.json()['plan']['market_data_requirements']
    resolver = RuntimeMarketDataResolver(store=f.repo, as_of_commit_seq=10000)
    for alias in ('flow', 'trades'):
        requirement = next(r for r in requirements if r['alias']==alias)
        late = replace(f.records[requirement['source_policy'].get('series_id', 4 if alias=='flow' else 5)][0],
                       revision=2, market_commit_seq=9999)
        late = replace(late, fact=replace(late.fact, known_at=at(1000)))
        f.records[late.series_id].append(late)
        result = resolver.causal_history(consumer_id='check', requirement=requirement,
            primary_instrument_id='episode-test', start=at(0), end=at(300), evaluation_time=at(900))
        assert result and all(r.fact.known_at <= at(900) and r.revision == 1 for r in result)
        assert f.calls[-1]['known_at_lte'] == at(900)
        assert f.calls[-1]['as_of_commit_seq'] == 10000
        assert f.calls[-1]['source_identity_keys'] == (f.source.identity_key,)


def test_full_episode_preparation_dry_run_retains_native_source_identity(native_admission):
    f = native_admission
    payload = {**f.payload, 'preparation':{'freeze':False, 'acquire_missing':False}}
    response = f.api.post('/api/research/checks/prepare', json=payload)
    assert response.status_code == 200, response.text
    value = response.json()
    assert value['status'] == 'ready_to_freeze', value
    assert not value['preparation']['unresolved_requirements']
    assert len(value['preparation']['resolved_requirements']) == 5
    assert all(r['resolved_source_identity_keys'] == [f.source.identity_key]
               for r in value['preparation']['resolved_requirements'])
    assert value['check_executed'] is False and value['provider_call_performed'] is False
    assert value['preparation']['dataset'] is None


def test_legacy_native_effective_times_still_use_authoritative_clocks(native_admission):
    f = native_admission
    for sid, expected in ((4, at(0)), (5, at(150.25))):
        records = f.repo.read_series_records(series_id=sid, start=at(0), end=at(300),
            as_of_commit_seq=10000, source_identity_keys=(f.source.identity_key,))
        assert records and planning._record_time(records[0]) == expected
        assert records[0].fact.known_at >= expected

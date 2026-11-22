from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from portal.backend.service.storage.repos import market_data as data
from portal.backend.service.storage.repos import market_structure as structure

START = datetime(2026, 9, 22, tzinfo=UTC)


def identity_db(monkeypatch, fact_type, timeframe, contract):
    class Result:
        def mappings(self): return self
        def first(self):
            return {'fact_type': fact_type, 'timeframe_seconds': timeframe,
                    'contract_version': contract}
    @contextmanager
    def session():
        yield NS(execute=lambda *args, **kwargs: Result())
    monkeypatch.setattr(data, 'db', NS(session=session))


@pytest.mark.parametrize('family,interval', [('market.trade', None), ('market.trade_flow', 1), ('market.trade_flow', 60)])
@pytest.mark.parametrize('sources', [(), ('exact-source-a', 'exact-source-b')])
def test_native_series_preserves_exact_sources_cutoffs_and_typed_sorting(monkeypatch, family, interval, sources):
    contract = 'market.trade.v1' if family == 'market.trade' else 'market.trade_flow.v1'
    identity_db(monkeypatch, family, interval, contract)
    def row(second):
        return NS(fact=NS(provider_event_time=START+timedelta(seconds=second),
            provider_sequence_num=second, receive_ordinal=second, event_ordinal=0,
            trade_ordinal=0, provider_trade_id=str(second),
            bucket_start=START+timedelta(seconds=second), interval_seconds=interval))
    later, earlier = row(2), row(1)
    read = Mock(return_value=[later, earlier])
    monkeypatch.setattr(data.market_data_repo, 'read_facts', read)
    monkeypatch.setattr(structure, 'decode_market_trade_record', lambda record: record)
    monkeypatch.setattr(structure, 'decode_trade_flow_record', lambda record: record)
    cutoff = START+timedelta(hours=1)
    result = data.market_data_repo.read_series_records(series_id=17, start=START,
        end=START+timedelta(minutes=3), as_of_commit_seq=42,
        known_at_lte=cutoff, source_identity_keys=sources)
    assert result == [earlier, later]
    read.assert_called_once_with(series_id=17, start=START,
        end=START+timedelta(minutes=3), as_of_commit_seq=42,
        known_at_lte=cutoff, source_identity_keys=sources)


def test_unsupported_typed_family_still_rejects_exact_source(monkeypatch):
    identity_db(monkeypatch, 'market.trade_flow_feature', 1, 'market.trade_flow_feature.v1')
    reader = Mock(side_effect=AssertionError('unsupported read must not run'))
    monkeypatch.setattr(structure.market_structure_repository, 'read_feature_records', reader)
    with pytest.raises(ValueError, match='market_data_source_binding_unsupported'):
        data.market_data_repo.read_series_records(series_id=17, start=START,
            end=START+timedelta(minutes=1), source_identity_keys=('exact-source',))
    reader.assert_not_called()


def test_native_flow_invalid_interval_still_fails_before_read(monkeypatch):
    identity_db(monkeypatch, 'market.trade_flow', 5, 'market.trade_flow.v1')
    reader = Mock(side_effect=AssertionError('invalid interval must not read'))
    monkeypatch.setattr(data.market_data_repo, 'read_facts', reader)
    with pytest.raises(RuntimeError, match='trade-flow interval must be 1s or 60s'):
        data.market_data_repo.read_series_records(series_id=17, start=START,
            end=START+timedelta(minutes=1), source_identity_keys=('exact-source',))
    reader.assert_not_called()

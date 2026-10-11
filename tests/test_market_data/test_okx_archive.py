"""Synthetic behavior of the unqualified archive draft, not source admission."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
import json

import pytest

from data_providers.providers.okx_archive import (
    INFERRED_AVAILABILITY, PRODUCT, iter_archive_lines, translate_okx_archive_record,
)
from market_data.order_book import L2ProductContract, Level2BookReconstructor
from market_data.structure import OrderingAssurance, RawStreamRecord

RECEIVED = datetime(2026, 10, 11, tzinfo=UTC)
SAMPLE = datetime(2026, 10, 5, tzinfo=UTC)
CONTRACT = L2ProductContract(PRODUCT, "fixture-product", "contracts", Decimal("0.1"), Decimal("0.01"))


def record(*, action="snapshot", ordinal=1, bids=None, asks=None, extra=None):
    row = {"instId": PRODUCT, "action": action, "ts": str(1791158400000 + (ordinal-1)*1000),
           "bids": bids if bids is not None else [["99.9", "1.25", "1"]],
           "asks": asks if asks is not None else [["100.1", "2.50", "2"]]}
    row.update(extra or {})
    payload = (json.dumps(row) + "\n").encode()
    import hashlib
    from market_data.structure import build_raw_record_id
    digest = hashlib.sha256(payload).hexdigest()
    return RawStreamRecord(
        build_raw_record_id(definition_id="archive-definition", session_id="archive-session",
                            connection_epoch=0, receive_ordinal=ordinal, raw_frame_sha256=digest),
        "segment", "archive-definition", "archive-session", 0, ordinal,
        RECEIVED, "OKX", "OKX", PRODUCT, "level2", "level2", payload, digest)


def translate(raw, lag=0):
    return translate_okx_archive_record(raw, contract=CONTRACT,
                                       accepted_at=RECEIVED + timedelta(seconds=1),
                                       assumed_delivery_lag_ms=lag)


def test_archive_clock_is_explicit_and_preserves_actual_receipt():
    event = translate(record(), lag=250)
    assert event.known_at_method == INFERRED_AVAILABILITY
    assert event.known_at == SAMPLE + timedelta(milliseconds=250)
    assert event.received_at == RECEIVED
    assert event.accepted_at == RECEIVED + timedelta(seconds=1)
    assert event.provider_message_time is None
    assert event.position.provider_sequence_num is None
    with pytest.raises(ValueError, match="causal times"):
        replace(event, known_at_method="platform_acceptance")
    with pytest.raises(ValueError, match="unsupported availability"):
        replace(event, known_at_method="invented")
    with pytest.raises(ValueError, match="precedes sample"):
        replace(event, known_at=SAMPLE-timedelta(microseconds=1))


def test_exact_absolute_set_delete_reset_and_checkpoint_replay():
    reducer = Level2BookReconstructor(series_id=1, contract=CONTRACT,
                                     ordering_assurance=OrderingAssurance.SAMPLED_ARCHIVE_ORDERED)
    first = translate(record(bids=[["99.9", "1.25", "1"], ["99.8", "2.00", "1"]]))
    opened = reducer.process(first)
    assert opened.accepted and opened.snapshot and opened.checkpoints
    duplicate = reducer.process(first)
    assert not duplicate.accepted and duplicate.quality[0].classification == "duplicate"
    resumed = Level2BookReconstructor.from_checkpoint(opened.checkpoints[0], contract=CONTRACT,
                                                      validity=opened.validity_versions[0])
    update = translate(record(action="update", ordinal=2,
                              bids=[["99.9", "0", "0"], ["99.8", "3.25", "2"]], asks=[]))
    a, b = reducer.process(update), resumed.process(update)
    assert a.state == b.state
    assert a.state.bids == ((Decimal("99.8"), Decimal("3.25")),)
    reset = reducer.process(translate(record(ordinal=3)))
    assert reset.state.bids == ((Decimal("99.9"), Decimal("1.25")),)
    assert len(reset.validity_versions) == 2


def test_fractional_contracts_require_an_explicit_quantity_increment():
    CONTRACT.validate_level(price=Decimal("99.9"), quantity=Decimal("1.25"))
    with pytest.raises(ValueError, match="not integral"):
        replace(CONTRACT, quantity_increment=None).validate_level(price=Decimal("99.9"), quantity=Decimal("1.25"))
    with pytest.raises(ValueError, match="quantity violates"):
        replace(CONTRACT, quantity_increment=Decimal("1")).validate_level(price=Decimal("99.9"), quantity=Decimal("1.25"))


@pytest.mark.parametrize("kwargs, message", [
    ({"extra": {"seqId": "1"}}, "record shape"),
    ({"extra": {"instId": "ETH-USDT-SWAP"}}, "product or action"),
    ({"extra": {"ts": 1791158400000}}, "sample timestamp"),
    ({"bids": [["99.9", "1", "1"], ["99.9", "2", "2"]]}, "duplicate side/price"),
    ({"bids": [["99.9", "0", "1"]]}, "size/order-count"),
    ({"bids": [["99.9", "1.251", "1"]]}, "quantity violates"),
    ({"bids": [["99.9", "1.25", "1", "extra"]]}, "price/size/order-count"),
    ({"action": "update", "bids": [], "asks": []}, "empty sampled update"),
])
def test_ambiguous_or_changed_provider_contract_fails(kwargs, message):
    with pytest.raises(ValueError, match=message):
        translate(record(**kwargs))


def test_line_reader_budgets_and_truncated_record():
    raw = record().raw_frame
    assert list(iter_archive_lines(BytesIO(raw), max_decoded_bytes=len(raw), max_records=1)) == [raw]
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        list(iter_archive_lines(BytesIO(raw), max_decoded_bytes=len(raw)-1, max_records=1))
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        list(iter_archive_lines(BytesIO(raw+raw), max_decoded_bytes=2*len(raw), max_records=1))
    with pytest.raises(ValueError, match="terminator"):
        list(iter_archive_lines(BytesIO(raw[:-1]), max_decoded_bytes=len(raw), max_records=1))

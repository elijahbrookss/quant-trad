"""Finite OKX module-5 archive decoding; no network, storage or research authority.

BLOCKED, UNQUALIFIED draft: this module is not registered or connected to an
importer. Absolute-update, sampled-boundary and historical product assumptions
remain unproven for the source. Synthetic tests do not admit provider data.

A file ordinal identifies retained ordering, never an exchange sequence. File
integrity and reconstruction do not prove completeness between sampled records.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import json
from typing import BinaryIO

from market_data.order_book import BookSourcePosition, L2EventFact, L2Mutation, L2ProductContract
from market_data.structure import RawStreamRecord

ADAPTER_VERSION = "okx.module5.sampled_book.v1"
INFERRED_AVAILABILITY = "historical_sample_time_inferred.v1"
PRODUCT = "BTC-USDT-SWAP"
MAX_LEVELS_PER_SIDE = 5000
MAX_LINE_BYTES = 2 * 1024**2


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"okx_archive_invalid: duplicate JSON field={key}")
        result[key] = value
    return result


def iter_archive_lines(stream: BinaryIO, *, max_decoded_bytes: int,
                       max_records: int, check_budget=lambda: None) -> Iterator[bytes]:
    """Keep exact complete source lines and bound decompression output upstream.

    The caller must also bound the container/decompressor and verify its checksum
    at EOF. A prefix is never qualified as a complete archive by this iterator.
    """
    if type(max_decoded_bytes) is not int or max_decoded_bytes <= 0 or type(max_records) is not int or max_records <= 0:
        raise ValueError("okx_archive_budget_invalid")
    consumed = count = 0
    while True:
        check_budget()
        remaining = max_decoded_bytes - consumed
        raw = stream.readline(min(MAX_LINE_BYTES, remaining) + 1)
        if not raw:
            return
        consumed += len(raw)
        count += 1
        if consumed > max_decoded_bytes or len(raw) > MAX_LINE_BYTES or count > max_records:
            raise RuntimeError("okx_archive_decode_budget_exceeded")
        if not raw.endswith(b"\n"):
            raise ValueError("okx_archive_invalid: incomplete record terminator")
        if not raw.strip():
            raise ValueError("okx_archive_invalid: empty record")
        yield raw


def translate_okx_archive_record(raw: RawStreamRecord, *, contract: L2ProductContract,
                                 accepted_at: datetime, assumed_delivery_lag_ms: int) -> L2EventFact:
    """Translate exact archive values with an explicitly modeled sample clock.

    No provider publication timestamp or live receipt is inferred from `ts`.
    Actual archive receipt and QT acceptance remain distinct from the model.
    """
    if raw.provider.upper() != "OKX" or raw.venue.upper() != "OKX" or raw.provider_product_id != PRODUCT:
        raise ValueError("okx_archive_invalid: source identity mismatch")
    if contract.provider_product_id != PRODUCT or raw.requested_channel != "level2":
        raise ValueError("okx_archive_invalid: product or channel mismatch")
    if type(assumed_delivery_lag_ms) is not int or not 0 <= assumed_delivery_lag_ms <= 60000:
        raise ValueError("okx_archive_invalid: explicit bounded delivery assumption required")
    if not raw.raw_frame or len(raw.raw_frame) > MAX_LINE_BYTES:
        raise ValueError("okx_archive_invalid: record size")
    try:
        row = json.loads(raw.raw_frame, object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("okx_archive_invalid: JSON record") from exc
    if not isinstance(row, dict) or set(row) != {"instId", "action", "ts", "asks", "bids"}:
        raise ValueError("okx_archive_invalid: unsupported record shape")
    if row["instId"] != PRODUCT or row["action"] not in {"snapshot", "update"}:
        raise ValueError("okx_archive_invalid: product or action")
    if not isinstance(row["ts"], str) or not row["ts"].isascii() or not row["ts"].isdigit():
        raise ValueError("okx_archive_invalid: millisecond sample timestamp")
    milliseconds = int(row["ts"])
    if not 0 <= milliseconds <= 253402300799999:
        raise ValueError("okx_archive_invalid: timestamp bounds")
    at = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)
    mutations = []
    for side, field in (("bid", "bids"), ("ask", "asks")):
        levels = row[field]
        if not isinstance(levels, list) or len(levels) > 2 * MAX_LEVELS_PER_SIDE:
            raise ValueError("okx_archive_invalid: level array exceeds bound")
        if row["action"] == "snapshot" and not 0 < len(levels) <= MAX_LEVELS_PER_SIDE:
            raise ValueError("okx_archive_invalid: snapshot side bounds")
        seen = set()
        for values in levels:
            if not isinstance(values, list) or len(values) != 3 or not all(isinstance(v, str) for v in values):
                raise ValueError("okx_archive_invalid: expected price/size/order-count strings")
            price, size, orders = values
            if not orders.isascii() or not orders.isdigit():
                raise ValueError("okx_archive_invalid: order count")
            mutation = L2Mutation(len(mutations), side, price, size, at, contract.provider_size_unit)
            contract.validate_level(price=mutation.price, quantity=mutation.new_quantity)
            if mutation.price in seen:
                raise ValueError("okx_archive_invalid: duplicate side/price")
            seen.add(mutation.price)
            if (mutation.new_quantity == 0) != (int(orders) == 0):
                raise ValueError("okx_archive_invalid: size/order-count disagreement")
            if row["action"] == "snapshot" and mutation.new_quantity == 0:
                raise ValueError("okx_archive_invalid: snapshot zero size")
            mutations.append(mutation)
    if not mutations:
        raise ValueError("okx_archive_unqualified: empty sampled update requires explicit semantics")
    return L2EventFact(
        event_type=row["action"],
        position=BookSourcePosition(raw.definition_id, raw.session_id, raw.connection_epoch,
                                    PRODUCT, None, raw.receive_ordinal, 0),
        product_definition_version_id=contract.product_definition_version_id,
        mutations=tuple(mutations), provider_message_time=None,
        received_at=raw.received_at, accepted_at=accepted_at,
        known_at=at + timedelta(milliseconds=assumed_delivery_lag_ms),
        known_at_method=INFERRED_AVAILABILITY, raw_record_id=raw.raw_record_id,
    )

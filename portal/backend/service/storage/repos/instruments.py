"""Storage repository module."""

from __future__ import annotations

from ._shared import *

def load_instruments() -> List[Dict[str, Any]]:
    """Return all persisted instrument metadata rows."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(InstrumentRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def list_strategy_instrument_symbols(strategy_id: str) -> List[str]:
    """Return symbols for all instruments attached to *strategy_id*.

    This queries the instrument table directly using the strategy->instrument links
    so we always derive authoritative symbol values from the persisted instrument rows.
    """

    if not db.available:
        return []
    if not strategy_id:
        return []
    with db.session() as session:
        # Join StrategyInstrumentLink -> InstrumentRecord and return symbol list
        rows = (
            session.execute(
                select(InstrumentRecord.symbol)
                .join(StrategyInstrumentLink, StrategyInstrumentLink.instrument_id == InstrumentRecord.id)
                .where(StrategyInstrumentLink.strategy_id == strategy_id)
            )
            .scalars()
            .all()
        )
        # Normalise and dedupe while preserving order
        seen: set[str] = set()
        symbols: List[str] = []
        for s in rows:
            if s is None:
                continue
            key = str(s).strip()
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
            symbols.append(key)
    return symbols


def list_strategy_instrument_links(strategy_id: str) -> List[Dict[str, Any]]:
    """Return instrument link rows with resolved symbols for a strategy."""

    if not db.available:
        return []
    try:
        with db.session() as session:
            rows = session.execute(
                select(
                    StrategyInstrumentLink.instrument_id,
                    InstrumentRecord.symbol,
                ).join(
                    InstrumentRecord,
                    StrategyInstrumentLink.instrument_id == InstrumentRecord.id,
                ).where(
                    StrategyInstrumentLink.strategy_id == strategy_id,
                )
            ).all()
            return [
                {"instrument_id": instrument_id, "symbol": symbol}
                for instrument_id, symbol in rows
            ]
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_instrument_list_failed | strategy=%s | error=%s",
            strategy_id,
            exc,
        )
        return []


def get_instrument(instrument_id: str) -> Optional[Dict[str, Any]]:
    """Return a single instrument by identifier."""

    if not db.available:
        return None
    with db.session() as session:
        record = session.get(InstrumentRecord, instrument_id)
        return record.to_dict() if record else None


def find_instrument(datasource: Optional[str], exchange: Optional[str], symbol: str) -> Optional[Dict[str, Any]]:
    """Look up an instrument by datasource/exchange/symbol with fallbacks."""

    if not db.available:
        return None
    symbol_key = (symbol or "").upper()
    if not symbol_key:
        return None
    datasource_key = (datasource or "").lower() or None
    exchange_key = (exchange or "").lower() or None
    with db.session() as session:
        # Require exact symbol match. If datasource and/or exchange are provided,
        # require those fields to match as well so we do not conflate distinct venue symbols.
        query = select(InstrumentRecord).where(InstrumentRecord.symbol == symbol_key)
        if datasource_key:
            query = query.where((InstrumentRecord.datasource or '').ilike(datasource_key))
        if exchange_key:
            query = query.where((InstrumentRecord.exchange or '').ilike(exchange_key))

        record = session.execute(query).scalars().first()
        return record.to_dict() if record else None


def upsert_instrument(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update an instrument record."""

    if not db.available:
        return meta
    # Ignore any caller-provided `id`. Always dedupe by the canonical unique key
    # (datasource, exchange, symbol). This prevents clients from bypassing the
    # uniqueness constraint by inventing IDs.
    symbol = (meta.get("symbol") or "").upper()
    datasource = meta.get("datasource")
    exchange = meta.get("exchange")

    if not symbol:
        raise ValueError("Instrument symbol is required")

    try:
        with db.session() as session:
            now = _utcnow()

            # Look for existing instrument by composite key regardless of any id.
            existing = None
            if datasource and exchange:
                existing = session.execute(
                    select(InstrumentRecord).where(
                        InstrumentRecord.symbol == symbol,
                        InstrumentRecord.datasource == datasource,
                        InstrumentRecord.exchange == exchange,
                    )
                ).scalars().first()

            if existing is not None:
                record = existing
            else:
                # Creating a new instrument requires datasource and exchange.
                if not datasource or not exchange:
                    raise ValueError("Instrument creation requires 'datasource' and 'exchange'")
                instrument_id = str(uuid.uuid4())
                record = InstrumentRecord(id=instrument_id)
                session.add(record)

            # Update fields on the found-or-created record
            record.datasource = datasource
            record.exchange = exchange
            record.symbol = symbol
            record.instrument_type = meta.get("instrument_type")
            # Instrument field values now live in the metadata JSON payload.
            # Merge metadata instead of replacing to preserve existing values
            if "metadata" in meta:
                existing_metadata = dict(record.extra_metadata or {})
                existing_metadata.update(meta.get("metadata") or {})
                record.extra_metadata = existing_metadata
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
            meta = record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("instrument_persist_failed | id=%s | error=%s", instrument_id, exc)
        raise
    return meta




def delete_instrument(instrument_id: str) -> None:
    """Delete an instrument metadata row."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(InstrumentRecord, instrument_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("instrument_delete_failed | id=%s | error=%s", instrument_id, exc)





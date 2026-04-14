"""Storage repository module."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ._shared import SymbolPresetRecord, SQLAlchemyError, _utcnow, db, logger, select

def upsert_symbol_preset(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Store or update a symbol preset."""

    if not db.available:
        return None
    preset_id = payload.get("id")
    try:
        with db.session() as session:
            record = session.get(SymbolPresetRecord, preset_id) if preset_id else None
            now = _utcnow()
            if record is None:
                preset_id = preset_id or payload.get("id") or payload.get("label")
                record = SymbolPresetRecord(
                    id=str(preset_id or f"preset-{now.timestamp():.0f}"),
                    label=payload.get("label") or "Preset",
                    datasource=payload.get("datasource"),
                    exchange=payload.get("exchange"),
                    timeframe=payload.get("timeframe") or "15m",
                    symbol=payload.get("symbol") or "",
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            record.label = payload.get("label") or record.label
            # Bot rows no longer persist datasource/exchange/timeframe; these are
            # owned by strategies. Ignore any payload values for these fields.
            record.symbol = payload.get("symbol") or record.symbol
            record.updated_at = now
            session.flush()
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("symbol_preset_persist_failed | id=%s | error=%s", preset_id, exc)
        return None


def list_symbol_presets() -> List[Dict[str, Any]]:
    """Return all saved symbol presets."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(SymbolPresetRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def delete_symbol_preset(preset_id: str) -> None:
    """Delete a stored symbol preset."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(SymbolPresetRecord, preset_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("symbol_preset_delete_failed | id=%s | error=%s", preset_id, exc)




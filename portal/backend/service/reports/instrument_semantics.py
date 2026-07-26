"""Pure projection of canonical fill semantics into report instrument metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_ALLOWED_VALUES = {
    "accounting_mode": frozenset({"spot", "margin"}),
    "execution_semantics": frozenset({"spot", "derivative", "proxy_derivative"}),
}
_CONFIGURED_FIELDS = (
    "instrument_type",
    "source_instrument_type",
    "execution_semantics",
    "research_market_role",
    "accounting_mode",
    "margin_calc_type",
)


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalized(value: Any, *, field: str, identity: str) -> str | None:
    text = _text(value)
    if not text:
        return None
    normalized = text.lower()
    if normalized not in _ALLOWED_VALUES[field]:
        raise ValueError(
            f"unsupported report instrument {field}={text!r} for {identity}"
        )
    return normalized


def _identity(*, instrument_id: str | None, symbol: str | None) -> str:
    return (
        f"instrument_id={instrument_id or 'unknown'} "
        f"symbol={symbol or 'unknown'}"
    )


def _validate_pair(row: Mapping[str, Any], *, identity: str) -> None:
    accounting_mode = _normalized(
        row.get("accounting_mode"),
        field="accounting_mode",
        identity=identity,
    )
    execution_semantics = _normalized(
        row.get("execution_semantics"),
        field="execution_semantics",
        identity=identity,
    )
    if accounting_mode == "spot" and execution_semantics not in {None, "spot"}:
        raise ValueError(
            f"conflicting report instrument semantics for {identity}: "
            f"accounting_mode=spot execution_semantics={execution_semantics}"
        )
    if accounting_mode == "margin" and execution_semantics == "spot":
        raise ValueError(
            f"conflicting report instrument semantics for {identity}: "
            "accounting_mode=margin execution_semantics=spot"
        )


def _matching_row(
    rows: Sequence[dict[str, Any]],
    *,
    instrument_id: str | None,
    symbol: str | None,
    identity: str,
) -> dict[str, Any] | None:
    id_matches = [
        row
        for row in rows
        if instrument_id and _text(row.get("instrument_id")) == instrument_id
    ]
    if len(id_matches) > 1:
        raise ValueError(f"ambiguous report instrument metadata for {identity}")
    if id_matches:
        return id_matches[0]

    symbol_matches = [
        row
        for row in rows
        if (
            symbol
            and _text(row.get("symbol")) == symbol
            and (not instrument_id or not _text(row.get("instrument_id")))
        )
    ]
    if len(symbol_matches) > 1:
        raise ValueError(f"ambiguous report instrument metadata for {identity}")
    return symbol_matches[0] if symbol_matches else None


def _merge_identity(
    target: dict[str, Any],
    *,
    instrument_id: str | None,
    symbol: str | None,
) -> None:
    target_id = _text(target.get("instrument_id"))
    target_symbol = _text(target.get("symbol"))
    if instrument_id and target_id and instrument_id != target_id:
        raise ValueError(
            f"conflicting report instrument_id for symbol={symbol}: "
            f"{target_id!r} != {instrument_id!r}"
        )
    if symbol and target_symbol and symbol != target_symbol:
        raise ValueError(
            f"conflicting report symbol for instrument_id={instrument_id}: "
            f"{target_symbol!r} != {symbol!r}"
        )
    target["instrument_id"] = target_id or instrument_id
    target["symbol"] = target_symbol or symbol


def _merge_configured_rows(
    configured_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for configured_row in configured_rows:
        source = dict(configured_row)
        instrument_id = _text(source.get("instrument_id"))
        symbol = _text(source.get("symbol"))
        identity = _identity(instrument_id=instrument_id, symbol=symbol)
        if not instrument_id and not symbol:
            raise ValueError(
                "report instrument metadata requires instrument_id or symbol"
            )

        target = _matching_row(
            rows,
            instrument_id=instrument_id,
            symbol=symbol,
            identity=identity,
        )
        if target is None:
            target = source
            target["instrument_id"] = instrument_id
            target["symbol"] = symbol
            rows.append(target)
        else:
            _merge_identity(
                target,
                instrument_id=instrument_id,
                symbol=symbol,
            )
            for field in _CONFIGURED_FIELDS:
                current = _text(target.get(field))
                observed = _text(source.get(field))
                if field in _ALLOWED_VALUES:
                    current = _normalized(
                        current,
                        field=field,
                        identity=identity,
                    )
                    observed = _normalized(
                        observed,
                        field=field,
                        identity=identity,
                    )
                if current and observed and current != observed:
                    raise ValueError(
                        f"conflicting configured report {field} for {identity}: "
                        f"{current!r} != {observed!r}"
                    )
                if not current and observed:
                    target[field] = observed
        _validate_pair(target, identity=identity)
    return rows


def merge_fill_instrument_semantics(
    configured_rows: Sequence[Mapping[str, Any]],
    fill_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Complete missing report semantics from fills and reject contradictions."""

    rows = _merge_configured_rows(configured_rows)
    for fill in fill_rows:
        instrument_id = _text(fill.get("instrument_id"))
        symbol = _text(fill.get("symbol"))
        identity = _identity(instrument_id=instrument_id, symbol=symbol)
        accounting_mode = _normalized(
            fill.get("accounting_mode"),
            field="accounting_mode",
            identity=identity,
        )
        evidence = {
            "accounting_mode": accounting_mode,
            "execution_semantics": "spot" if accounting_mode == "spot" else None,
        }
        _validate_pair(evidence, identity=identity)
        if not instrument_id and not symbol:
            continue

        target = _matching_row(
            rows,
            instrument_id=instrument_id,
            symbol=symbol,
            identity=identity,
        )
        if target is None:
            target = {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "instrument_type": None,
                "source_instrument_type": None,
                "execution_semantics": None,
                "research_market_role": None,
                "accounting_mode": None,
                "margin_calc_type": None,
            }
            rows.append(target)
        else:
            _merge_identity(
                target,
                instrument_id=instrument_id,
                symbol=symbol,
            )

        for field in ("execution_semantics", "accounting_mode"):
            configured = _normalized(
                target.get(field),
                field=field,
                identity=identity,
            )
            observed = evidence[field]
            if configured and observed and configured != observed:
                raise ValueError(
                    f"conflicting report {field} for {identity}: "
                    f"{configured!r} != {observed!r}"
                )
            if not configured and observed:
                target[field] = observed
        _validate_pair(target, identity=identity)
    return sorted(
        rows,
        key=lambda row: (
            _text(row.get("instrument_id")) or "",
            _text(row.get("symbol")) or "",
            _text(row.get("execution_semantics")) or "",
            _text(row.get("accounting_mode")) or "",
        ),
    )

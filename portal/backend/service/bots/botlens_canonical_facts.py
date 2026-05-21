from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple

from ..storage.storage import record_bot_runtime_events_batch
from .botlens_domain_events import build_botlens_domain_events_from_fact_batch
from .botlens_event_retention import retention_summary_for_events, split_events_by_retention
from .botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch


def _build_canonical_fact_rows(
    *,
    bot_id: str,
    run_id: str,
    seq: int,
    batch_kind: str,
    payload: Mapping[str, Any],
) -> Tuple[Any, tuple[Any, ...], List[Dict[str, Any]]]:
    if int(seq) <= 0:
        raise ValueError("canonical BotLens fact append requires seq > 0")

    events = build_botlens_domain_events_from_fact_batch(
        bot_id=str(bot_id),
        run_id=str(run_id),
        payload=payload,
    )
    batch = projection_batch_from_payload(
        batch_kind=batch_kind,
        run_id=str(run_id),
        bot_id=str(bot_id),
        symbol_key=payload.get("series_key"),
        payload=payload,
        events=events,
        seq=int(seq),
    )
    durable_events, _dropped_events = split_events_by_retention(events)
    rows = runtime_event_rows_from_batch(batch=batch, events=durable_events)
    return batch, tuple(events), rows


def append_botlens_canonical_fact_batch(
    *,
    bot_id: str,
    run_id: str,
    seq: int,
    batch_kind: str,
    payload: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    batch, events, rows = _build_canonical_fact_rows(
        bot_id=bot_id,
        run_id=run_id,
        seq=seq,
        batch_kind=batch_kind,
        payload=payload,
    )
    inserted_rows = 0
    if rows:
        inserted_rows = int(
            record_bot_runtime_events_batch(
                rows,
                context={
                    "bot_id": str(bot_id),
                    "run_id": str(run_id),
                    "series_key": batch.symbol_key,
                    "worker_id": payload.get("worker_id"),
                    "message_kind": batch_kind,
                    "pipeline_stage": "botlens_canonical_append",
                    "source_emitter": "bot_runtime",
                    "source_reason": "producer",
                    **dict(context or {}),
                },
            )
        )
    return {
        "seq": int(batch.seq),
        "event_count": len(events),
        "row_count": len(rows),
        "inserted_rows": inserted_rows,
        "event_ids": tuple(event.event_id for event in events),
        "retention_summary": retention_summary_for_events(events),
    }


def append_botlens_canonical_fact_batches(
    items: Sequence[Mapping[str, Any]],
    *,
    context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Persist multiple producer-stamped canonical fact payloads in one DB write."""

    normalized_items = [dict(item) for item in items if isinstance(item, Mapping)]
    if not normalized_items:
        return {
            "batch_count": 0,
            "event_count": 0,
            "row_count": 0,
            "inserted_rows": 0,
            "event_ids": (),
        }

    rows: List[Dict[str, Any]] = []
    event_ids: list[str] = []
    event_count = 0
    retained_count = 0
    dropped_count = 0
    seq_values: list[int] = []
    run_ids: set[str] = set()
    bot_ids: set[str] = set()
    batch_kinds: set[str] = set()
    worker_ids: set[str] = set()
    for item in normalized_items:
        payload = item.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("canonical fact batch item requires payload mapping")
        item_context = item.get("context") if isinstance(item.get("context"), Mapping) else {}
        worker_id = str(item_context.get("worker_id") or payload.get("worker_id") or "").strip()
        if worker_id:
            worker_ids.add(worker_id)
        bot_id = str(item.get("bot_id") or "").strip()
        run_id = str(item.get("run_id") or "").strip()
        batch_kind = str(item.get("batch_kind") or "").strip()
        if not bot_id or not run_id or not batch_kind:
            raise ValueError("canonical fact batch item requires bot_id, run_id, and batch_kind")
        batch, events, item_rows = _build_canonical_fact_rows(
            bot_id=bot_id,
            run_id=run_id,
            seq=int(item.get("seq") or 0),
            batch_kind=batch_kind,
            payload=payload,
        )
        rows.extend(item_rows)
        event_ids.extend(event.event_id for event in events)
        summary = retention_summary_for_events(events)
        event_count += int(summary["event_count"])
        retained_count += int(summary["retained_count"])
        dropped_count += int(summary["dropped_or_summarized_count"])
        seq_values.append(int(batch.seq))
        run_ids.add(str(batch.run_id))
        bot_ids.add(str(batch.bot_id))
        batch_kinds.add(str(batch.batch_kind))

    inserted_rows = 0
    if rows:
        inserted_rows = int(
            record_bot_runtime_events_batch(
                rows,
                context={
                    "bot_id": next(iter(bot_ids)) if len(bot_ids) == 1 else None,
                    "run_id": next(iter(run_ids)) if len(run_ids) == 1 else None,
                    "series_key": None,
                    "worker_id": next(iter(worker_ids)) if len(worker_ids) == 1 else None,
                    "message_kind": next(iter(batch_kinds)) if len(batch_kinds) == 1 else "mixed",
                    "pipeline_stage": "botlens_canonical_append_batch",
                    "source_emitter": "bot_runtime",
                    "source_reason": "producer",
                    "batch_count": len(normalized_items),
                    **dict(context or {}),
                },
            )
        )

    return {
        "batch_count": len(normalized_items),
        "event_count": event_count,
        "row_count": len(rows),
        "inserted_rows": inserted_rows,
        "event_ids": tuple(event_ids),
        "seq_min": min(seq_values) if seq_values else None,
        "seq_max": max(seq_values) if seq_values else None,
        "retention_summary": {
            "event_count": event_count,
            "retained_count": retained_count,
            "dropped_or_summarized_count": dropped_count,
        },
    }


__all__ = ["append_botlens_canonical_fact_batch", "append_botlens_canonical_fact_batches"]

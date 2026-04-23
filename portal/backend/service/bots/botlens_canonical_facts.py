from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from ..storage.storage import record_bot_runtime_events_batch
from .botlens_domain_events import build_botlens_domain_events_from_fact_batch
from .botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch


def append_botlens_canonical_fact_batch(
    *,
    bot_id: str,
    run_id: str,
    seq: int,
    batch_kind: str,
    payload: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
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
    rows = runtime_event_rows_from_batch(batch=batch)
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
    }


__all__ = ["append_botlens_canonical_fact_batch"]

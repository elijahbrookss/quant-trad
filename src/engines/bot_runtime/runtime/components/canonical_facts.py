"""Producer-side canonical BotLens fact append and post-append dispatch."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)

_CANONICAL_SIMPLE_FACT_TYPES = frozenset(
    {
        "candle_upserted",
        "trade_opened",
        "trade_updated",
        "trade_closed",
    }
)
_CANONICAL_DECISION_EVENT_NAMES = frozenset({"SIGNAL_EMITTED", "DECISION_ACCEPTED", "DECISION_REJECTED"})


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def canonical_fact_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    root = dict(payload or {})
    facts = []
    for entry in root.get("facts") if isinstance(root.get("facts"), list) else []:
        if not isinstance(entry, Mapping):
            continue
        fact_type = str(entry.get("fact_type") or "").strip().lower()
        if fact_type in _CANONICAL_SIMPLE_FACT_TYPES:
            facts.append(dict(entry))
            continue
        if fact_type != "decision_emitted":
            continue
        decision = _mapping(entry.get("decision"))
        event_name = str(decision.get("event_name") or "").strip().upper()
        if event_name in _CANONICAL_DECISION_EVENT_NAMES:
            facts.append(dict(entry))
    root["facts"] = facts
    return root


def has_canonical_facts(payload: Mapping[str, Any]) -> bool:
    return bool(canonical_fact_payload(payload).get("facts"))


@dataclass(frozen=True)
class CommittedCanonicalFactBatch:
    batch_kind: str
    bot_id: str
    run_id: str
    seq: int
    symbol_key: Optional[str]
    known_at: Any
    event_time: Any
    canonical_payload: Dict[str, Any]
    live_payload: Dict[str, Any]
    append_result: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PostAppendConsumerResult:
    consumer_name: str
    result: Any = None
    error: Optional[str] = None


@dataclass(frozen=True)
class CanonicalFactAppendOutcome:
    batch: CommittedCanonicalFactBatch
    consumer_results: Tuple[PostAppendConsumerResult, ...] = ()


class CanonicalFactConsumer(Protocol):
    def consume(self, batch: CommittedCanonicalFactBatch) -> Any:
        ...


class LiveFactsBroadcastConsumer:
    def __init__(self, broadcast: Callable[[str, Optional[Dict[str, Any]]], Any]) -> None:
        self._broadcast = broadcast

    def consume(self, batch: CommittedCanonicalFactBatch) -> Any:
        return self._broadcast("facts", batch.live_payload)


class CanonicalFactAppender:
    def __init__(
        self,
        *,
        allocate_seq: Callable[[], int],
        append_batch: Optional[Callable[..., Mapping[str, Any]]] = None,
        consumers: Sequence[CanonicalFactConsumer] = (),
    ) -> None:
        self._allocate_seq = allocate_seq
        self._append_batch = append_batch
        self._consumers = tuple(consumers)

    def append_fact_batch(
        self,
        *,
        bot_id: str,
        run_id: str,
        batch_kind: str,
        payload: Mapping[str, Any],
        context: Mapping[str, Any] | None = None,
        live_payload: Mapping[str, Any] | None = None,
        dispatch: bool = True,
    ) -> CanonicalFactAppendOutcome | None:
        canonical_payload = canonical_fact_payload(payload)
        facts = canonical_payload.get("facts") if isinstance(canonical_payload.get("facts"), list) else []
        if not facts:
            return None
        if self._append_batch is None:
            raise RuntimeError("bot runtime canonical fact appender is not configured")

        seq = int(self._allocate_seq())
        stamped_canonical = dict(canonical_payload)
        stamped_canonical["run_seq"] = seq
        stamped_canonical["seq"] = seq
        stamped_durable = dict(payload or {})
        stamped_durable["run_seq"] = seq
        stamped_durable["seq"] = seq
        stamped_live = dict(live_payload or payload)
        stamped_live["run_seq"] = seq
        stamped_live["seq"] = seq

        append_result = dict(
            self._append_batch(
                bot_id=str(bot_id),
                run_id=str(run_id),
                seq=seq,
                batch_kind=batch_kind,
                payload=stamped_durable,
                context=context,
            )
            or {}
        )
        batch = CommittedCanonicalFactBatch(
            batch_kind=batch_kind,
            bot_id=str(bot_id),
            run_id=str(run_id),
            seq=seq,
            symbol_key=str(stamped_live.get("series_key") or "").strip() or None,
            known_at=stamped_live.get("known_at"),
            event_time=stamped_live.get("event_time"),
            canonical_payload=stamped_canonical,
            live_payload=stamped_live,
            append_result=append_result,
        )

        if not dispatch:
            return CanonicalFactAppendOutcome(batch=batch)
        return CanonicalFactAppendOutcome(batch=batch, consumer_results=self.dispatch(batch))

    def dispatch(self, batch: CommittedCanonicalFactBatch) -> Tuple[PostAppendConsumerResult, ...]:
        if not self._consumers:
            return ()
        consumer_results = []
        for consumer in self._consumers:
            consumer_name = consumer.__class__.__name__
            try:
                result = consumer.consume(batch)
                consumer_results.append(PostAppendConsumerResult(consumer_name=consumer_name, result=result))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "bot_runtime_post_append_consumer_failed | consumer=%s | bot_id=%s | run_id=%s | seq=%s | batch_kind=%s | error=%s",
                    consumer_name,
                    batch.bot_id,
                    batch.run_id,
                    batch.seq,
                    batch.batch_kind,
                    exc,
                )
                consumer_results.append(PostAppendConsumerResult(consumer_name=consumer_name, error=str(exc)))
        return tuple(consumer_results)


__all__ = [
    "CanonicalFactAppendOutcome",
    "CanonicalFactAppender",
    "CanonicalFactConsumer",
    "CommittedCanonicalFactBatch",
    "LiveFactsBroadcastConsumer",
    "PostAppendConsumerResult",
    "canonical_fact_payload",
    "has_canonical_facts",
]

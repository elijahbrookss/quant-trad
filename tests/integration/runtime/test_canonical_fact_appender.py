from __future__ import annotations

from engines.bot_runtime.runtime.components.canonical_facts import CanonicalFactAppender, canonical_fact_payload


class _FailingConsumer:
    def __init__(self, order: list[str]) -> None:
        self._order = order

    def consume(self, batch):  # noqa: ANN001
        self._order.append(f"consume:{batch.seq}")
        raise RuntimeError("transport unavailable")


def test_canonical_fact_appender_persists_before_non_authoritative_consumers() -> None:
    order: list[str] = []

    def _append(**kwargs):
        order.append(f"append:{kwargs['seq']}")
        return {"inserted_rows": 1}

    appender = CanonicalFactAppender(
        allocate_seq=lambda: 5,
        append_batch=_append,
        consumers=(_FailingConsumer(order),),
    )

    outcome = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload={
            "series_key": "instrument-btc|1m",
            "known_at": "2026-04-19T12:00:00Z",
            "event_time": "2026-04-19T12:00:00Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {"status": "running"},
                },
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-19T12:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    },
                },
            ],
        },
    )

    assert order == ["append:5", "consume:5"]
    assert outcome is not None
    assert outcome.batch.seq == 5
    assert outcome.consumer_results[0].error == "transport unavailable"


def test_canonical_fact_payload_filters_warnings_and_logs_off_the_canonical_path() -> None:
    filtered = canonical_fact_payload(
        {
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {"fact_type": "log_emitted", "log": {"message": "debug"}},
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-19T12:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    },
                },
            ],
        }
    )

    assert [fact["fact_type"] for fact in filtered["facts"]] == ["candle_upserted"]

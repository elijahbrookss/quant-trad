from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, select

pytestmark = pytest.mark.db

import portal.backend.service.bots.container_runtime as runtime_mod
from portal.backend.db import db
from portal.backend.db.models import BotRunEventRecord, BotRunEventSeqAllocatorRecord


class _ProxySeqCounter:
    def __init__(self, value: int) -> None:
        self.value = int(value)

    def get(self) -> int:
        return self.value

    def set(self, value: int) -> None:
        self.value = int(value)


class _ProxyLock:
    def acquire(self) -> None:
        return None

    def release(self) -> None:
        return None


def _proxy(initial_seq: int) -> dict[str, object]:
    return {
        "runtime_event_seq": _ProxySeqCounter(initial_seq),
        "lock": _ProxyLock(),
    }


def _delete_test_events(run_id: str) -> None:
    with db.session() as session:
        session.execute(
            delete(BotRunEventRecord).where(BotRunEventRecord.run_id == run_id)
        )
        session.execute(
            delete(BotRunEventSeqAllocatorRecord).where(
                BotRunEventSeqAllocatorRecord.run_id == run_id
            )
        )


def test_wallet_initialization_retry_is_idempotent_and_divergence_stays_strict() -> None:
    run_id = str(uuid.uuid4())
    bot_id = str(uuid.uuid4())
    initialized_at = "2026-07-26T05:00:00Z"
    try:
        first = runtime_mod._append_canonical_wallet_initialized_fact(
            bot_id=bot_id,
            run_id=run_id,
            balances={"USD": 10_000.0},
            shared_wallet_proxy=_proxy(10),
            initialized_at=initialized_at,
        )
        replay = runtime_mod._append_canonical_wallet_initialized_fact(
            bot_id=bot_id,
            run_id=run_id,
            balances={"USD": 10_000.0},
            shared_wallet_proxy=_proxy(40),
            initialized_at=initialized_at,
        )

        assert first["append_result"]["inserted_rows"] == 1
        assert first["idempotent_replay"] is False
        assert replay["append_result"]["inserted_rows"] == 0
        assert replay["idempotent_replay"] is True

        with db.session() as session:
            rows = (
                session.execute(
                    select(BotRunEventRecord).where(
                        BotRunEventRecord.run_id == run_id
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].event_name == "WALLET_INITIALIZED"
        assert rows[0].payload["context"]["known_at"] == initialized_at
        assert rows[0].payload["context"]["source_run_seq"] == 0
        assert (
            rows[0].payload["context"]["source_run_seq_status"]
            == "run_initialization"
        )

        with pytest.raises(
            ValueError,
            match="runtime event_id collision with divergent event material",
        ):
            runtime_mod._append_canonical_wallet_initialized_fact(
                bot_id=bot_id,
                run_id=run_id,
                balances={"USD": 20_000.0},
                shared_wallet_proxy=_proxy(60),
                initialized_at=initialized_at,
            )
    finally:
        _delete_test_events(run_id)

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scripts.db import manual_migration_fact_storage_tiers_v1 as cutover
from tests.test_portal.test_database_snapshot_fence import Connection


class CutoverConnection(Connection):
    @contextmanager
    def begin(self):
        self.events.append("begin")
        try:
            yield self
            self.commit()
        except BaseException:
            self.rollback()
            raise


@pytest.mark.parametrize("failure", [None, "lock", "connection.commit", "unlock", "unlock.false"])
def test_cutover_fence_is_released_or_connection_discarded(monkeypatch, failure):
    events = []
    connection = CutoverConnection(events, failure=failure)
    engine = SimpleNamespace(connect=lambda: connection)
    monkeypatch.setattr(cutover, "_prepare", lambda _: events.append("prepare"))
    monkeypatch.setattr(cutover, "_copy_page", lambda *_: False)
    monkeypatch.setattr(cutover, "inspect_cutover", lambda _: {"status": "ready"})
    if failure is None:
        assert cutover.run_cutover(engine, execute=True, writers_stopped=True) == {"status": "ready"}
        assert not connection.invalidated
        assert events.index("lock") < events.index("prepare") < events.index("unlock")
    else:
        with pytest.raises(RuntimeError):
            cutover.run_cutover(engine, execute=True, writers_stopped=True)
        assert connection.invalidated
        assert events.index("invalidate") < events.index("connection.close")


def test_cutover_interrupt_rolls_back_page_and_unlocks(monkeypatch):
    events = []
    connection = CutoverConnection(events)
    engine = SimpleNamespace(connect=lambda: connection)
    monkeypatch.setattr(cutover, "_prepare", lambda _: None)
    def interrupted(*_):
        raise KeyboardInterrupt()
    monkeypatch.setattr(cutover, "_copy_page", interrupted)
    with pytest.raises(KeyboardInterrupt):
        cutover.run_cutover(engine, execute=True, writers_stopped=True)
    assert events.index("connection.rollback") < events.index("unlock") < events.index("connection.close")
    assert not connection.invalidated

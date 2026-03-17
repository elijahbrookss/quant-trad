from dataclasses import dataclass
from datetime import datetime
import threading

from engines.bot_runtime.runtime.components.series_runner import PoolSeriesRunner, SeriesRunnerContext


@dataclass
class DummyState:
    series: object
    bar_index: int = 0
    total_bars: int = 1
    done: bool = False
    next_step_at: datetime | None = None


def test_pool_series_runner_steps_due_states():
    stop_event = threading.Event()
    pause_event = threading.Event()
    pause_event.set()
    calls = []
    states = [DummyState(series=object()), DummyState(series=object())]
    invoked = {"count": 0}

    def due_series_states(_now):
        invoked["count"] += 1
        return states if invoked["count"] == 1 else []

    ctx = SeriesRunnerContext(
        stop_event=stop_event,
        pause_event=pause_event,
        live_mode=False,
        mode="instant",
        due_series_states=due_series_states,
        next_step_time=lambda: None,
        step_series_state=lambda state: calls.append(state),
        append_live_candles_if_needed=lambda: False,
        append_live_candles_for_state=lambda _state: False,
        pace=lambda _interval, _update: None,
        series_states=lambda: states,
        thread_name=lambda _state, idx: f"thread-{idx}",
        log_debug=lambda *_args, **_kwargs: None,
        log_info=lambda *_args, **_kwargs: None,
    )

    runner = PoolSeriesRunner(ctx, max_workers=2)
    runner.run()

    assert {id(state) for state in calls} == {id(state) for state in states}

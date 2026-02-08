from dataclasses import dataclass
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
import threading
import sys

pandas_stub = ModuleType("pandas")
pandas_stub.DataFrame = object
pandas_stub.Timestamp = object
pandas_stub.Timedelta = object
pandas_stub.DatetimeIndex = object
sys.modules.setdefault("pandas", pandas_stub)
sqlalchemy_stub = ModuleType("sqlalchemy")
sqlalchemy_stub.bindparam = lambda *args, **kwargs: None
sqlalchemy_stub.create_engine = lambda *args, **kwargs: None
sqlalchemy_stub.text = lambda *args, **kwargs: None
sqlalchemy_stub.delete = lambda *args, **kwargs: None
sqlalchemy_stub.select = lambda *args, **kwargs: None
sqlalchemy_stub.func = SimpleNamespace()
sqlalchemy_stub.Engine = object
sys.modules.setdefault("sqlalchemy", sqlalchemy_stub)
engine_stub = ModuleType("sqlalchemy.engine")
engine_stub.Engine = object
sys.modules.setdefault("sqlalchemy.engine", engine_stub)
exc_stub = ModuleType("sqlalchemy.exc")
exc_stub.SQLAlchemyError = Exception
sys.modules.setdefault("sqlalchemy.exc", exc_stub)
sys.modules.setdefault("mplfinance", SimpleNamespace())
sys.modules.setdefault("requests", SimpleNamespace())
matplotlib_stub = ModuleType("matplotlib")
pyplot_stub = SimpleNamespace()
matplotlib_stub.pyplot = pyplot_stub
patches_stub = SimpleNamespace(Rectangle=object)
matplotlib_stub.patches = patches_stub
matplotlib_stub.__path__ = []
dates_stub = SimpleNamespace()
matplotlib_stub.dates = dates_stub
transforms_stub = SimpleNamespace()
matplotlib_stub.transforms = transforms_stub
sys.modules.setdefault("matplotlib", matplotlib_stub)
sys.modules.setdefault("matplotlib.pyplot", pyplot_stub)
sys.modules.setdefault("matplotlib.dates", dates_stub)
sys.modules.setdefault("matplotlib.patches", patches_stub)
sys.modules.setdefault("matplotlib.transforms", transforms_stub)
numpy_stub = ModuleType("numpy")
numpy_stub.ndarray = object
sys.modules.setdefault("numpy", numpy_stub)

from portal.backend.service.bots.bot_runtime.runtime.series_runner import PoolSeriesRunner, SeriesRunnerContext


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
        if invoked["count"] == 1:
            return states
        return []

    def step_series_state(state):
        calls.append(state)

    ctx = SeriesRunnerContext(
        stop_event=stop_event,
        pause_event=pause_event,
        live_mode=False,
        mode="instant",
        due_series_states=due_series_states,
        next_step_time=lambda: None,
        step_series_state=step_series_state,
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

from types import ModuleType, SimpleNamespace
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

storage_stub = SimpleNamespace(record_bot_trade=lambda *_args, **_kwargs: None, record_bot_trade_event=lambda *_args, **_kwargs: None)
storage_module_stub = ModuleType("portal.backend.service.storage")
storage_module_stub.storage = storage_stub
sys.modules.setdefault("portal.backend.service.storage", storage_module_stub)

from portal.backend.service.bots.bot_runtime.runtime.persistence_buffer import TradePersistenceBuffer
from portal.backend.service.storage import storage


def test_persistence_buffer_flushes_by_count(monkeypatch):
    calls = {"entries": 0, "events": 0}

    def record_trade(_payload):
        calls["entries"] += 1

    def record_event(_payload):
        calls["events"] += 1

    monkeypatch.setattr(storage, "record_bot_trade", record_trade)
    monkeypatch.setattr(storage, "record_bot_trade_event", record_event)

    buffer = TradePersistenceBuffer(max_batch_size=2, flush_interval_s=100, time_fn=lambda: 0.0)
    buffer.record_trade_entry({"trade_id": "t1"})
    assert calls == {"entries": 0, "events": 0}

    buffer.record_trade_event({"id": "e1"})
    assert calls == {"entries": 1, "events": 1}


def test_persistence_buffer_flushes_on_close_event(monkeypatch):
    calls = {"entries": 0, "events": 0}

    def record_trade(_payload):
        calls["entries"] += 1

    def record_event(_payload):
        calls["events"] += 1

    monkeypatch.setattr(storage, "record_bot_trade", record_trade)
    monkeypatch.setattr(storage, "record_bot_trade_event", record_event)

    buffer = TradePersistenceBuffer(max_batch_size=10, flush_interval_s=100, time_fn=lambda: 0.0)
    buffer.record_trade_event({"id": "close-event"}, event_type="close")
    assert calls == {"entries": 0, "events": 1}

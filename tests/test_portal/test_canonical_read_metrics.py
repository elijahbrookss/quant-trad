import pytest

from portal.backend.service.storage import canonical_read_metrics as metrics


@pytest.fixture
def observer(monkeypatch):
    records = []
    class Observer:
        def increment(self, name, value=1, **labels):
            records.append((name, value, labels))
        def observe(self, name, value, **labels):
            records.append((name, value, labels))
    monkeypatch.setattr(metrics, "_observer", Observer())
    return records


def _record(records, name):
    return next((value, labels) for metric, value, labels in records if metric == name)


def test_eager_read_preserves_result_and_has_only_bounded_labels(observer):
    result = {"private-fact-id": {"payload": "not a metric"}}
    @metrics.observe_read(tier="cold", operation="identity")
    def read(request_id):
        return result
    assert read("private-request") is result
    value, labels = _record(observer, "canonical_read_calls_total")
    assert value == 1
    assert labels == {"storage_target": "cold", "pipeline_stage": "identity", "outcome": "completed"}
    assert "private" not in str(observer)


def test_producer_time_excludes_consumer_wait_and_closes_once(monkeypatch, observer):
    clock = [0.0]
    monkeypatch.setattr(metrics, "perf_counter", lambda: clock[0])
    closed = []
    @metrics.observe_read_iterator(tier="cold", operation="range")
    def read():
        try:
            clock[0] += 2
            yield "a"
            clock[0] += 3
            yield "b"
        finally:
            clock[0] += 4
            closed.append(True)
    stream = read()
    assert next(stream) == "a"
    clock[0] += 100
    assert next(stream) == "b"
    stream.close()
    stream.close()
    assert closed == [True]
    assert _record(observer, "canonical_read_ms")[0] == 9000
    assert _record(observer, "canonical_read_rows_total")[0] == 2
    assert _record(observer, "canonical_read_calls_total")[1]["outcome"] == "closed"


def test_generator_error_is_the_original_error_and_is_not_success(observer):
    error = ValueError("private input details")
    closed = []
    @metrics.observe_read_iterator(tier="canonical", operation="range")
    def read():
        try:
            yield 1
            raise error
        finally:
            closed.append(True)
    stream = read()
    assert next(stream) == 1
    with pytest.raises(ValueError) as caught:
        next(stream)
    assert caught.value is error and closed == [True]
    assert _record(observer, "canonical_read_errors_total")[0] == 1
    assert _record(observer, "canonical_read_calls_total")[1]["outcome"] == "error"
    assert "private" not in str(observer)


def test_send_throw_and_generator_return_are_preserved(observer):
    @metrics.observe_read_iterator(tier="cold", operation="range")
    def read():
        try:
            received = yield "start"
            yield received
        except ValueError:
            yield "handled"
        return 7
    stream = read()
    assert next(stream) == "start"
    assert stream.send("value") == "value"
    assert stream.throw(ValueError("handled upstream")) == "handled"
    with pytest.raises(StopIteration) as caught:
        next(stream)
    assert caught.value.value == 7
    assert _record(observer, "canonical_read_calls_total")[1]["outcome"] == "completed"


def test_cleanup_failure_is_not_counted_as_success(observer):
    error = RuntimeError("cleanup failed")
    @metrics.observe_read_iterator(tier="cold", operation="range")
    def read():
        try:
            yield 1
        finally:
            raise error
    stream = read()
    next(stream)
    with pytest.raises(RuntimeError) as caught:
        stream.close()
    assert caught.value is error
    assert _record(observer, "canonical_read_calls_total")[1]["outcome"] == "error"


def test_empty_read_completes_with_zero_rows(observer):
    @metrics.observe_read_iterator(tier="canonical", operation="range")
    def read():
        yield from ()
    assert list(read()) == []
    assert _record(observer, "canonical_read_rows_total")[0] == 0
    assert _record(observer, "canonical_read_calls_total")[1]["outcome"] == "completed"


def test_metrics_failure_never_masks_the_storage_error(monkeypatch):
    class Broken:
        def increment(self, *args, **kwargs):
            raise RuntimeError("metric sink failed")
    monkeypatch.setattr(metrics, "_observer", Broken())
    error = ValueError("canonical input failed")
    @metrics.observe_read(tier="cold", operation="identity")
    def read():
        raise error
    with pytest.raises(ValueError) as caught:
        read()
    assert caught.value is error


def test_hydration_fallback_is_not_mislabeled_hot(observer):
    @metrics.observe_read(tier="hot", operation="hydration")
    def hydrate(self, session, rows):
        return rows
    hydrate(None, None, [{"payload": {}, "provenance": {}, "quality": {}}])
    hydrate(None, None, [{"payload": None, "provenance": {}, "quality": {}}])
    tiers = [labels["storage_target"] for name, _, labels in observer if name == "canonical_read_calls_total"]
    assert tiers == ["hot", "canonical"]

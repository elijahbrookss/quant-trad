import logging

import pytest

from utils.perf_log import perf_log, should_sample


def test_perf_log_ok(caplog):
    logger = logging.getLogger("tests.perf")
    with caplog.at_level(logging.DEBUG, logger="tests.perf"):
        with perf_log("test_event", logger=logger, enabled=True):
            pass
    assert any("time_taken_ms" in record.message and "ok=True" in record.message for record in caplog.records)


def test_perf_log_exception(caplog):
    logger = logging.getLogger("tests.perf")
    with caplog.at_level(logging.ERROR, logger="tests.perf"):
        with pytest.raises(ValueError):
            with perf_log("test_event", logger=logger, enabled=True):
                raise ValueError("boom")
    assert any(
        "ok=False" in record.message and "error_type=ValueError" in record.message for record in caplog.records
    )


def test_should_sample_rate_edges():
    assert should_sample(0.0) is False
    assert should_sample(1.0) is True

"""Machine-readable collection and phase evidence for isolated CI proof shards."""
import json
import pytest

_reports = []

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    row = {"nodeid": report.nodeid, "phase": report.when,
           "outcome": report.outcome, "seconds": report.duration}
    if report.skipped:
        row["reason"] = str(report.longrepr[2]).removeprefix("Skipped: ")
    _reports.append(row)

def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    payload = {"nodes": [item.nodeid for item in session.items],
               "exitstatus": int(exitstatus), "reports": _reports}
    reporter.write_line("\nQT_PROOF_RESULT " + json.dumps(payload, sort_keys=True))

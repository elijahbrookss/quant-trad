from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.research import service


def test_research_activity_zero_fills_and_counts_qualifying_check_statuses(
    monkeypatch,
) -> None:
    today = datetime.now(UTC).date()
    captured: dict[str, object] = {}

    def _count_items_by_day(**kwargs):
        captured.update(kwargs)
        return [
            {
                "day": datetime.combine(
                    today,
                    datetime.min.time(),
                    tzinfo=UTC,
                ).replace(tzinfo=None),
                "status": "tested",
                "total": 2,
            },
            {
                "day": datetime.combine(
                    today,
                    datetime.min.time(),
                    tzinfo=UTC,
                ).replace(tzinfo=None),
                "status": "blocked",
                "total": 1,
            },
        ]

    monkeypatch.setattr(service.repository, "count_items_by_day", _count_items_by_day)

    payload = service.get_research_activity(
        activity_type="checks_completed",
        days=3,
    )

    assert payload["timestamp_field"] == "created_at"
    assert payload["timezone"] == "UTC"
    assert payload["qualifying_statuses"] == ["tested", "blocked"]
    assert len(payload["days"]) == 3
    assert payload["days"][-1] == {
        "date": today.isoformat(),
        "total": 3,
        "by_status": {"blocked": 1, "tested": 2},
    }
    assert payload["days"][0]["date"] == (today - timedelta(days=2)).isoformat()
    assert payload["days"][0]["total"] == 0
    assert captured["kind"] == "research_check"
    assert captured["statuses"] == ("tested", "blocked")


@pytest.mark.parametrize(
    ("activity_type", "expected_kind"),
    [
        ("hypotheses_created", "hypothesis"),
        ("observations_recorded", "observation"),
    ],
)
def test_research_activity_uses_created_at_for_supported_memory_records(
    monkeypatch,
    activity_type,
    expected_kind,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        service.repository,
        "count_items_by_day",
        lambda **kwargs: captured.update(kwargs) or [],
    )

    payload = service.get_research_activity(activity_type=activity_type, days=1)

    assert payload["kind"] == expected_kind
    assert payload["timestamp_field"] == "created_at"
    assert captured["statuses"] == ()


def test_research_activity_rejects_unsupported_types() -> None:
    with pytest.raises(ValueError, match="unsupported research activity type"):
        service.get_research_activity(activity_type="agent_goals_completed", days=7)

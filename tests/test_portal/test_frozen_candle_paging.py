from datetime import datetime, timezone

from portal.backend.service.storage.repos.candles import _frozen_dataset_page_flags


def _at(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def test_interior_frozen_window_exposes_both_dataset_directions() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(3),
        effective_end=_at(6),
        has_extra=False,
        prefer_latest=False,
    ) == (True, True)


def test_latest_frozen_page_stops_only_at_dataset_end() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(1),
        effective_end=_at(10),
        has_extra=True,
        prefer_latest=True,
    ) == (True, False)


def test_forward_frozen_page_keeps_prior_boundary_and_reports_extra_rows() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(4),
        effective_end=_at(10),
        has_extra=True,
        prefer_latest=False,
    ) == (True, True)


def test_forward_frozen_page_stops_when_it_reaches_dataset_end() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(4),
        effective_end=_at(10),
        has_extra=False,
        prefer_latest=False,
    ) == (True, False)


def test_empty_windows_retain_the_direction_of_available_dataset_history() -> None:
    before_dataset = _frozen_dataset_page_flags(
        frozen_start=_at(3),
        frozen_end=_at(10),
        effective_start=_at(3),
        effective_end=_at(2),
        has_extra=False,
        prefer_latest=False,
    )
    after_dataset = _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(8),
        effective_start=_at(9),
        effective_end=_at(8),
        has_extra=False,
        prefer_latest=True,
    )
    assert before_dataset == (False, True)
    assert after_dataset == (True, False)

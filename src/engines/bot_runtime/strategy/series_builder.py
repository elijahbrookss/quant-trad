"""Series preparation utilities for bot runtime orchestration."""

from __future__ import annotations

from .series_builder_parts import (
    SeriesBuilderConstructionMixin,
    SeriesBuilderLifecycleMixin,
    SeriesBuilderLiveUpdatesMixin,
    SeriesBuilderOverlaysRegimeMixin,
    StrategySeries,
)


class SeriesBuilder(
    SeriesBuilderLifecycleMixin,
    SeriesBuilderLiveUpdatesMixin,
    SeriesBuilderConstructionMixin,
    SeriesBuilderOverlaysRegimeMixin,
):
    """Prepare strategy series and overlays for the runtime."""


__all__ = ["StrategySeries", "SeriesBuilder"]

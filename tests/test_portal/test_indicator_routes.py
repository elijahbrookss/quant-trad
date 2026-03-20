from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from portal.backend.controller import indicators


def test_indicator_type_routes_are_registered_before_dynamic_instance_route():
    paths = [route.path for route in indicators.router.routes]

    assert "/types" in paths
    assert "/{inst_id}" in paths
    assert paths.index("/types") < paths.index("/{inst_id}")
    assert "/types/{type_id}" in paths
    assert paths.index("/types/{type_id}") < paths.index("/{inst_id}")

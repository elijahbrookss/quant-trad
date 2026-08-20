from __future__ import annotations

import pytest

from data_providers.streams.runtime import (
    ContinuousStreamPolicy,
    StreamReconnectPolicy,
)


def test_continuous_policy_round_trips_provider_neutral_bounds() -> None:
    policy = ContinuousStreamPolicy.from_mapping(
        {
            "segment_max_seconds": 30,
            "max_inflight_segments": 3,
            "lease_seconds": 120,
            "heartbeat_seconds": 10,
            "reconnect_policy": {
                "reconnect_enabled": True,
                "initial_backoff_seconds": 0.5,
                "max_backoff_seconds": 20,
                "continuous_disconnect_budget_seconds": 600,
                "heartbeat_stale_seconds": 25,
            },
        }
    )

    assert policy.max_inflight_segments == 3
    assert policy.reconnect == StreamReconnectPolicy(
        enabled=True,
        initial_backoff_seconds=0.5,
        max_backoff_seconds=20.0,
        continuous_disconnect_budget_seconds=600.0,
        heartbeat_stale_seconds=25.0,
    )
    assert ContinuousStreamPolicy.from_mapping(policy.to_dict()) == policy


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"lease_seconds": 30, "heartbeat_seconds": 30}, "less than"),
        ({"max_inflight_segments": 0}, "must be >= 1"),
        ({"reconnect_policy": []}, "must be an object"),
        ({"segment_max_second": 5}, "unsupported fields"),
        (
            {
                "reconnect_policy": {
                    "initial_backoff_seconds": 2,
                    "max_backoff_seconds": 1,
                }
            },
            "must be >= initial",
        ),
    ],
)
def test_continuous_policy_fails_loud_on_unsafe_or_mistyped_values(
    payload: dict,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        ContinuousStreamPolicy.from_mapping(payload)

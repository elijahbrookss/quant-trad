from copy import deepcopy
from datetime import UTC, datetime, timedelta
import math

import numpy as np
import pytest

from research_science import saved_case_comparison as c
from research_science.hashing import stable_hash


def row(i, *, label="continuation", clock=None, episode="one"):
    at = clock or datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * i)
    return {"event_id": str(i), "decision_at": at.isoformat(), "episode": episode,
            "episode_end": "2025-01-01T00:00:00Z", "recognition_origin": episode,
            "root_weight": 1., "comparison_features": {"sigma96": .01 + i / 10000, "r": .02 + i / 20000,
                                                       "z_pre12": i / 100, "N": i + 1},
            "boundary_geometry": {"E": 100., "L": 98., "upper": 102.},
            "outcome": {"classification": label, "nominal_end": (at + timedelta(hours=3)).isoformat(),
                        "outcome_known_at": (at + timedelta(hours=3)).isoformat()}}


def test_weighted_scalers_four_calendar_bins_and_frozen_numeric_roundtrip():
    rows = [row(i, label=c.CLASSES[i % 3]) for i in range(96)]
    rows[0]["root_weight"] = .25
    model = c.fit_models(rows, training_manifest_hash="fixed")
    c.validate_model(model, training_manifest_hash="fixed")
    expected = np.average([math.log(r["comparison_features"]["sigma96"]) for r in rows],
                          weights=[r["root_weight"] for r in rows])
    assert model["arms"]["base"]["scalers"][0]["mean"] == pytest.approx(expected)
    assert c.raw_features([rows[i] for i in (0, 24, 48, 72)], enriched=False)[:, 3:].tolist() == [
        [0, 0, 0], [1, 0, 0], [0, 1, 0], [0, 0, 1]]
    scores = c.score(rows, model)
    assert scores == c.score(rows, deepcopy(model))
    for r in scores:
        assert sum(r["base_probabilities"]) == pytest.approx(1)
        assert r["base_loss"] == pytest.approx(-math.log(r["base_probabilities"][c.CLASSES.index(r["classification"])]))
    poisoned = deepcopy(rows)
    poisoned[-1]["outcome"]["classification"] = "unresolved"
    assert c.score(poisoned, model) == scores[:-1]
    broken = deepcopy(model)
    broken["arms"]["base"]["coefficients"][0][0] += 1
    with pytest.raises(ValueError, match="identity"):
        c.validate_model(broken, training_manifest_hash="fixed")


def test_zero_variance_and_nonconvergence_are_explicit(monkeypatch):
    rows = [row(i, label=c.CLASSES[i % 3]) for i in range(12)]
    for r in rows:
        r["comparison_features"] = dict(sigma96=.01, r=.02, z_pre12=0., N=3)
    model = c.fit_models(rows, training_manifest_hash="fixed")
    assert all(s["reason"] == "zero_training_variance" for s in model["arms"]["enriched"]["scalers"])
    from types import SimpleNamespace
    monkeypatch.setattr(c, "minimize", lambda *a, **k: SimpleNamespace(success=False, message="fixture"))
    with pytest.raises(ValueError, match="nonconvergence"):
        c.fit_models(rows, training_manifest_hash="fixed")


def test_hand_weighted_three_class_intercept_only_fixture():
    rows = [row(i, label=c.CLASSES[i]) for i in range(3)]
    for i, r in enumerate(rows):
        r["root_weight"] = i + 1
        r["comparison_features"] = dict(sigma96=.01, r=.02, z_pre12=0., N=3)
    model = c.fit_models(rows, training_manifest_hash="fixed")
    scores = c.score(rows, model)
    for r in scores:
        assert r["base_probabilities"] == pytest.approx([1/6, 2/6, 3/6], abs=1e-7)
        assert r["enriched_probabilities"] == pytest.approx(r["base_probabilities"], abs=1e-10)
        assert r["gain"] == pytest.approx(0, abs=1e-10)


def test_predictive_mask_is_nominal_interior_and_training_strict_known_at():
    r = row(0)
    assert c.eligible(r)
    assert not c.eligible(r, training_cutoff=r["outcome"]["outcome_known_at"])
    r["episode_end"] = r["outcome"]["nominal_end"]
    r["outcome"]["outcome_known_at"] = r["decision_at"]
    assert not c.eligible(r)


def test_matching_is_label_and_duration_blind_distinct_clock_and_deterministic():
    cases = [row(2), row(3)]
    alternatives = [row(1), row(4)]
    for r in [*cases, *alternatives]:
        r["comparison_features"].update(sigma96=.01, r=.02, z_pre12=0)
    original = c.match(cases, alternatives)
    assert [p["alternative_id"] for p in original] == ["1", "4"]
    poisoned = deepcopy(alternatives)
    for r in poisoned:
        r["outcome"] = {"poison": object()}
        r["comparison_features"]["N"] = object()
    assert c.match(list(reversed(cases)), list(reversed(poisoned))) == original
    same = deepcopy(alternatives[0]); same["event_id"] = "duplicate-clock"
    assert len(c.match(cases, [alternatives[0], same])) == 1


def test_unknown_and_unmatched_mass_bounds_and_zero_variance_balance():
    cases = [row(2, label="unresolved"), row(3)]
    controls = [row(1, label="failure")]
    for r in [*cases, *controls]:
        r["comparison_features"].update(sigma96=.01, r=.02, z_pre12=0)
    summary = c.matched_summary(cases, controls, c.match(cases, controls))
    assert summary["coverage"] == .5
    assert summary["matched_unknown_bounds"] == [0, 2]
    assert summary["full_population_bounds"] == [-1, 2]
    assert all(v["smd"] == 0 for v in summary["balance"].values())


def test_original_clock_mass_not_reallocated_to_survivors():
    cases = [row(1), row(2)]
    cases[1]["decision_at"] = cases[0]["decision_at"]
    for r in cases:
        r["features"] = dict(sigma96=.01, r=.02, z_pre12=0., recovery_candles=2)
        r["original_weight"] = .5
    converted = c.root_rows(cases, episode="e", end="2025-01-01T00:00:00Z", origin="2024-01-01T00:00:00Z", source_check_id="saved")
    converted[1]["outcome"]["classification"] = "unresolved"
    assert sum(r["root_weight"] for r in converted if c.eligible(r)) == .5
    cases[0]["original_weight"] = 1
    with pytest.raises(ValueError, match="weight"):
        c.root_rows(cases, episode="e", end="2025-01-01T00:00:00Z", origin="2024-01-01T00:00:00Z", source_check_id="saved")


def test_linked_origin_interval_does_not_count_rows_as_independent():
    rows = [row(i) for i in range(20)]
    for r in rows:
        r["gain"] = 1.
    assert c.conditional_interval(rows, value="gain")["status"] == "unsupported"
    rows[-1]["recognition_origin"] = "two"
    assert c.conditional_interval(rows, value="gain")["reason"] == "undefined_or_degenerate_draws"
    rows[-1]["gain"] = -1
    answer = c.conditional_interval(rows, value="gain")
    assert answer["status"] == "completed"
    assert answer["group_count"] == 2
    assert answer["bounds"] == [-1, 1]


def test_alternative_is_upclose_own_trailing_geometry_with_prefix_invariant_features():
    from research_science.recognition import RecognitionCandle, plain
    origin = datetime(2024, 1, 1, tzinfo=UTC)
    source = "synthetic"
    candles = []
    for i in range(130):
        close = 100 + i / 100
        candles.append(RecognitionCandle.from_mapping({
            "open_time": plain(origin + timedelta(minutes=15*i)),
            "close_time": plain(origin + timedelta(minutes=15*(i+1))),
            "known_at": plain(origin + timedelta(minutes=15*(i+1))),
            "open": close, "close": close, "high": close + 1, "low": close - 1}, source_identity_key=source))
    start, end = origin + timedelta(minutes=15*98), origin + timedelta(minutes=15*130)
    result = c.alternative_population(tuple(candles), start=start, end=end, episode="e", source_key=source)
    first = result[0]
    assert first["boundary_geometry"]["E"] == candles[97].close
    assert first["boundary_geometry"]["L"] == min(b.low for b in candles[86:98])
    assert first["outcome"]["classification"] in c.ALL_CLASSES
    poisoned = deepcopy(candles)
    poisoned[-1] = RecognitionCandle.from_mapping({"open_time": plain(candles[-1].open_time),
        "close_time": plain(candles[-1].close_time), "known_at": plain(candles[-1].known_at),
        "open": 500, "close": 500, "high": 501, "low": 499}, source_identity_key=source)
    other = c.alternative_population(tuple(poisoned), start=start, end=end, episode="e", source_key=source)
    assert first == other[0]
    flat = [RecognitionCandle.from_mapping({"open_time": plain(b.open_time), "close_time": plain(b.close_time),
        "known_at": plain(b.known_at), "open": 100, "close": 100, "high": 101, "low": 99}, source_identity_key=source) for b in candles]
    assert c.alternative_population(tuple(flat), start=start, end=end, episode="e", source_key=source) == []


def test_excluded_boundary_support_still_links_uncertainty_groups():
    a, b, excluded = row(0), row(1, episode="two"), row(2)
    a.update(gain=.1, support_start="2024-01-01T00:00:00Z", support_end="2024-01-01T03:00:00Z")
    b.update(gain=-.1, recognition_origin="two", support_start="2024-01-02T00:00:00Z", support_end="2024-01-02T03:00:00Z")
    excluded.update(support_start="2024-01-01T02:00:00Z", support_end="2024-01-02T01:00:00Z")
    result = c.conditional_interval([a, b], value="gain", group_rows=[a, b, excluded])
    assert result["status"] == "unsupported"
    assert result["group_count"] == 1
    assert excluded["event_id"] in result["groups"][0]["event_ids"]


def test_saved_scores_validate_without_predicting_and_fail_on_mask_mass_or_loss():
    rows = [row(i, label=c.CLASSES[i % 3]) for i in range(12)]
    model = c.fit_models(rows, training_manifest_hash="fixed")
    scores = c.score(rows, model)
    c.validate_scores(rows, scores)
    for field, value in (("root_weight", 2), ("gain", .123), ("base_loss", 99), ("base_probabilities", [0, .5, .5])):
        bad = deepcopy(scores)
        bad[0][field] = value
        with pytest.raises(ValueError):
            c.validate_scores(rows, bad)
    with pytest.raises(ValueError, match="mask"):
        c.validate_scores(rows, scores[:-1])

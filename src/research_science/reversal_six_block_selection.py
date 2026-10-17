"""Fixed six-block extension of the admitted comparison-selection procedure."""
from copy import deepcopy
from dataclasses import replace

from .recognition import plain, utc
from .reversal_selection import RULES as V1_RULES, _select_comparisons, stage_availability
from .study import stable_hash

SCHEMA = "reversal_selection_only.v2"
ORIGIN = utc("2025-01-01T00:00:00Z")
START = utc("2025-02-01T00:00:00Z")
SPLIT = utc("2025-04-26T00:00:00Z")
END = utc("2025-07-19T00:00:00Z")
INPUT_BARS = 19104
LEGACY_CHOICES = {"reference_basis": "rising_leg_plus_retreat",
                  "ownership": "retain_all_provisional", "max_bars": 12000, "max_candidates": 10000}
LEGACY_CONFIGURATION_HASH = stable_hash(LEGACY_CHOICES)
RULES = deepcopy(V1_RULES)
RULES.update(version=SCHEMA,
    evaluation="Fixed2025-02-01 through2025-07-19 exclusive;six28day blocks;origin2025-01-01;no reset",
    order="case clock,B2 namespace compatibility order key;distance,absolute calendar distance,control clock,source identity",
    timing_representation="reversal_stage_availability.v3;lossless shared parent clock and inferred duplicate availability fields",
    populations="old first3blocks,new last3blocks,combined;each summary normalizes its own equal completion-moment weights")
RULES["limits"]["candles"] = INPUT_BARS


def compatibility_order_keys(snapshot):
    """Actual event IDs remain untouched; only the deterministic B2 tie key is projected."""
    reference_keys = {}
    for reference in snapshot.references:
        if reference.source_events:
            raise ValueError("reversal_selection_invalid: unsupported reference ancestry for B2 tie projection")
        reference_keys[reference.event_id] = replace(reference,
            configuration_hash=LEGACY_CONFIGURATION_HASH, event_id="").event_id
    keys = {}
    for event in snapshot.candidates:
        try:
            links = tuple(replace(link, event_id=reference_keys[link.event_id]) for link in event.source_events)
        except KeyError as exc:
            raise ValueError("reversal_selection_invalid: missing retained reference for B2 tie projection") from exc
        keys[event.event_id] = replace(event, configuration_hash=LEGACY_CONFIGURATION_HASH,
                                      source_events=links, event_id="").event_id
    if len(set(keys.values())) != len(keys):
        raise ValueError("reversal_selection_invalid: duplicate B2 compatibility order key")
    return keys


def compact_stage_availability(event):
    full = stage_availability(event)
    return {"schema_version": "reversal_stage_availability.v3",
        "parent_known_at": plain(event.known_at),
        "candle_known_at_rule": "observed_at",
        "usable_as_parent_stage_at_rule": "max(role_known_at,parent_known_at)",
        "stages": [{k: stage[k] for k in ("name", "candle_id", "observed_at", "role_known_at")}
                   for stage in full["stages"]]}


def expand_stage_availability(view):
    """Deterministic lossless v3->v2 decoding; no market calculation."""
    if (view.get("schema_version") != "reversal_stage_availability.v3"
            or view.get("candle_known_at_rule") != "observed_at"
            or view.get("usable_as_parent_stage_at_rule") != "max(role_known_at,parent_known_at)"):
        raise ValueError("reversal_selection_invalid: unsupported compact stage view")
    parent = view["parent_known_at"]
    return {"schema_version": "reversal_stage_availability.v2", "stages": [
        {**stage, "candle_known_at": stage["observed_at"], "parent_known_at": parent,
         "usable_as_parent_stage_at": plain(max(utc(stage["role_known_at"]), utc(parent)))}
        for stage in view["stages"]]}


def select_six_blocks(candles, snapshot, *, checkpoint=lambda: None):
    result = _select_comparisons(candles, snapshot.candidates, evaluation_start=START,
        evaluation_end=END, rules=RULES, checkpoint=checkpoint,
        order_keys=compatibility_order_keys(snapshot), timing_view=compact_stage_availability,
        population_ranges=(("old", START, SPLIT), ("new", SPLIT, END), ("combined", START, END)))
    result["compatibility"] = {
        "order_namespace": "B2 original settings projection;not the expanded event identity",
        "legacy_recognition_choices": LEGACY_CHOICES,
        "actual_event_ids_preserved": True,
        "original_result_modified": False,
        "caveat": "Prefix event order can be compared by compatibility keys;weights and connected components normalize over the expanded population."}
    result["selection_manifest_hash"] = stable_hash({k: result[k]
        for k in ("rules", "cases", "pairs", "dependence", "populations", "compatibility")})
    return result

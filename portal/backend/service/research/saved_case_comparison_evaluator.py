"""One native Indicator episode with exact saved development dependencies."""
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
import json
import logging
import re
from time import monotonic
from uuid import UUID

from core.execution_control import execution_checkpoint, consume_execution_json
from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition, CheckRequest, CheckEvidenceBinding, CheckResult
from research_science.event_barriers import population_projection, path_value
from research_science.recognition import RecognitionCandle, plain, utc
from research_science import saved_case_comparison as math
from . import event_barrier_check as barriers, repository

SCHEMA = "reversal_saved_case_comparison.v1"
RESULT_SCHEMA = "reversal_saved_case_comparison_result.v1"
RULES = {"native_episodes": 1, "prior_market_replay": False, "saved_source_limit": 6,
         "scored_source_limit": 3, "saved_case_limit": 60000, "saved_input_bytes": 100663296,
         "result_bytes": 16777216, "wall_seconds": 120,
         "pretty_result_bytes": 7340032,
         "interpretation": "exploratory conditional comparisons;not causality or profitability"}
DETECTOR = {"type": "indicator_event", "output_name": "provisional_reversals",
            "event_keys": [{"key": "provisional_reversal_completed", "direction": "long", "alias": "provisional_reversal_completed"}]}
FEATURE_SPECS = [
    {"name": "recovery_candles", "operator": "event_metadata_number", "path": "metadata.recovery_candles", "units": "native15m bars", "missing_reason_path": "metadata.context_missing_reason"},
    *[{"name": name, "operator": "event_metadata_number", "path": "metadata.context." + path,
       "units": units, "missing_reason_path": "metadata.context_missing_reason"}
      for name, path, units in (("sigma96", "sigma", "RMS log return per native15m bar"),
                                ("r", "r", "fractional distance"), ("q", "q", "three-hour volatility units"),
                                ("z_pre12", "z_pre12", "prior standardized log return"))],
    {"name": "formation_duration_hours", "operator": "window_duration_hours", "units": "hours"},
]
RECIPE = {"model": math.MODEL_SETTINGS, "classes": list(math.CLASSES),
          "matching": {"sigma_ratio": 1.25, "r_ratio": 1.10, "z_absolute": .25,
                       "same_utc6h": True, "distinct_clock": True, "reuse": False},
          "uncertainty": {"draws": 5000, "seed": 20261102, "confidence": .95,
                          "unit": "support_pair_reference_common_origin_linked_groups"},
          "gates": {"training_clocks": 200, "training_each_class": 20, "eligible_mass": .90,
                    "validation_clocks": 100, "validation_target": 20, "validation_failure": 20,
                    "validation_episodes": 4, "matched_mass": .80, "abs_smd": .10,
                    "duration_gain": .005, "alternative_gain": .02},
          "views": ["current_episode", "declared_validation_block"]}
logger = logging.getLogger(__name__)
_SOURCE_CACHE = ContextVar("saved_case_comparison_source_cache", default=None)
_SOURCE_LOADING = ContextVar("saved_case_comparison_source_loading", default=())


@contextmanager
def dependency_scope():
    token = _SOURCE_CACHE.set({}) if _SOURCE_CACHE.get() is None else None
    try:
        yield
    finally:
        if token is not None:
            _SOURCE_CACHE.reset(token)


def invalid(reason):
    math.invalid(reason)


def reference(raw, extra):
    keys = {"check_id", "result_hash", "evidence_hash", extra}
    if not isinstance(raw, dict) or set(raw) != keys:
        invalid("exact typed saved reference required")
    try:
        if str(UUID(raw["check_id"])) != raw["check_id"]:
            invalid("canonical saved Check ID required")
    except (ValueError, TypeError, AttributeError):
        invalid("canonical saved Check ID required")
    if any(not isinstance(raw[k], str) or not re.fullmatch("[0-9a-f]{64}", raw[k]) for k in keys - {"check_id"}):
        invalid("exact saved hashes required")
    return dict(raw)


def closed_recipe(raw, expected):
    if isinstance(expected, dict):
        return isinstance(raw, dict) and set(raw) == set(expected) and all(closed_recipe(raw[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(raw, list) and len(raw) == len(expected) and all(closed_recipe(a, b) for a, b in zip(raw, expected))
    if type(expected) is float:
        return math.finite(raw) and raw == expected
    return type(raw) is type(expected) and raw == expected


def normalize_configuration(*, detector, outcomes, statistics, scope, inputs=(), gap_policy="reject"):
    raw = dict(outcomes)
    raw["schema_version"] = barriers.SCHEMA
    if outcomes.get("schema_version") != SCHEMA:
        invalid("explicit comparison schema and public reversal completion required")
    if not isinstance(statistics, dict) or set(statistics) != {"features", "illustrations", "comparison"}:
        invalid("explicit bounded comparison statistics required")
    d, o, s = barriers.normalize_configuration(detector=detector, outcomes=raw,
        statistics={k: statistics[k] for k in ("features", "illustrations")}, scope=scope, inputs=inputs, gap_policy=gap_policy)
    if (d != DETECTOR or s != {"features": FEATURE_SPECS, "illustrations": []} or o["horizon_bars"] != 12
        or o["support_lookback_bars"] != 97 or o["reference"] != {"operator": "event_close"}
        or o["failure"] != {"operator": "window_low"} or o["target"] != {"operator": "risk_multiple", "multiple": 1.0}
        or o.get("window_start_path") != "metadata.recognition.formation_start"
        or o.get("identity_path") != "metadata.recognition.event_id"
        or o.get("source_path") != "metadata.recognition.context.source_identity_key"
        or o.get("known_at_path") != "metadata.recognition.known_at"
        or utc(scope["start"]) != utc(o["input_origin"]) + timedelta(minutes=15 * 98)):
        invalid("closed reversal geometry, strict context and origin+98 reporting required")
    if not isinstance(statistics["comparison"], dict):
        invalid("typed comparison object required")
    config = deepcopy(statistics["comparison"])
    if set(config) != {"training_sources", "training_cutoff", "frozen_model_source", "prior_scoring_sources", "comparison_recipe", "validation_episodes"} or not closed_recipe(config["comparison_recipe"], RECIPE):
        invalid("exact prospective comparison recipe required")
    config["comparison_recipe"] = deepcopy(RECIPE)
    sources = config["training_sources"]
    previous = config["prior_scoring_sources"]
    if not isinstance(sources, list) or not 1 <= len(sources) <= 6 or not isinstance(previous, list) or len(previous) > 3:
        invalid("finite saved dependency lists required")
    config["training_sources"] = [reference(r, "population_hash") for r in sources]
    config["prior_scoring_sources"] = [reference(r, "scored_evidence_hash") for r in previous]
    if config["frozen_model_source"] is not None:
        config["frozen_model_source"] = reference(config["frozen_model_source"], "model_hash")
    if len({r["check_id"] for r in sources}) != len(sources) or len({r["check_id"] for r in previous}) != len(previous):
        invalid("duplicate saved dependency")
    episodes = config["validation_episodes"]
    if not isinstance(episodes, list) or len(episodes) != 4:
        invalid("four ordered disjoint validation episodes required")
    cutoff = utc(config["training_cutoff"])
    normalized = []
    for episode in episodes:
        if not isinstance(episode, dict) or set(episode) != {"origin", "end_exclusive"}:
            invalid("exact validation episode clocks required")
        a, b = utc(episode["origin"]), utc(episode["end_exclusive"])
        if a < cutoff or not a < b or any(t.timestamp() % 900 for t in (a, b)) or (normalized and a < utc(normalized[-1]["end_exclusive"])):
            invalid("ordered aligned development/validation boundaries required")
        normalized.append({"origin": plain(a), "end_exclusive": plain(b)})
    config["validation_episodes"] = normalized
    config["training_cutoff"] = plain(cutoff)
    episode = {"origin": o["input_origin"], "end_exclusive": plain(utc(scope["end"]))}
    if episode not in normalized:
        invalid("current native episode absent from frozen validation schedule")
    index = normalized.index(episode)
    if (index == 0) != (config["frozen_model_source"] is None) or (previous and (index != 3 or len(previous) != 3)):
        invalid("fit first episode only;explicit final three-source aggregation only")
    o["schema_version"] = SCHEMA
    return d, o, {**s, "comparison": config}


def legacy_parameters(parameters):
    p = deepcopy(parameters)
    p["outcomes"]["schema_version"] = barriers.SCHEMA
    p["statistics"].pop("comparison")
    return p


def check_graph(graph, origin):
    if len(graph) != 1:
        invalid("one public reversal graph required")
    g = graph[0]
    p = g.get("params") or {}
    if (g.get("indicator_type") != "reversal" or (g.get("manifest") or {}).get("version") != "v1"
        or g.get("dependencies") or utc(p.get("computation_origin")) != utc(origin)
        or p.get("ownership") != "retain_all_provisional" or p.get("reference_basis") != "rising_leg_plus_retreat"):
        invalid("reversal graph origin, public version or population differs")


def load_source(ref, *, kind, item_loader=None):
    with dependency_scope():
        key = (kind, *sorted(ref.items()))
        cache = _SOURCE_CACHE.get()
        if key in cache:
            return cache[key]
        if key in _SOURCE_LOADING.get():
            invalid("cyclic saved dependency")
        token = _SOURCE_LOADING.set((*_SOURCE_LOADING.get(), key))
        try:
            loaded = _load_source(ref, kind=kind, item_loader=item_loader)
            cache[key] = loaded
            return loaded
        finally:
            _SOURCE_LOADING.reset(token)


def _load_source(ref, *, kind, item_loader=None):
    from .service import _validate_research_check_evidence_payload
    item = (item_loader or repository.get_item)(ref["check_id"])
    if not isinstance(item, dict) or item.get("id") != ref["check_id"] or item.get("kind") != "research_check" or item.get("status") != "tested":
        invalid("tested saved Check required")
    p = item.get("payload") or {}
    if p.get("observation_eligible") is not True or p.get("replayable") is not True or any(k in item and item[k] is not True for k in ("observation_eligible", "replayable")):
        invalid("persisted eligibility must literally be true;conflicting projection rejected")
    size = len(json.dumps(p, allow_nan=False, separators=(",", ":")).encode())
    consume_execution_json("input_bytes", p)
    if size > RULES["saved_input_bytes"]:
        invalid("saved envelope exceeds existing bounded input budget")
    if len(json.dumps(p.get("result", {}).get("result", {}), allow_nan=False, separators=(",", ":")).encode()) > RULES["result_bytes"]:
        invalid("saved source result exceeds its original16MiB limit")
    declared = (p.get("request", {}).get("parameters", {}).get("statistics", {}).get("comparison") or {})
    if kind == "model" and declared.get("frozen_model_source") is not None:
        invalid("frozen model must come from the original fitting Check")
    if kind == "scores" and declared.get("prior_scoring_sources"):
        invalid("aggregate scored dependencies cannot contain earlier aggregates")
    _validate_research_check_evidence_payload(p, item_id=item["id"])
    definition = CheckDefinition.from_dict(p["definition"])
    request = CheckRequest.from_dict(p["request"])
    evidence = CheckEvidenceBinding.from_dict(p["evidence"])
    contract = CheckResult.from_dict(p["result"])
    result = dict(contract.result)
    version = "15" if kind == "training" else "16"
    if (definition.evaluator_id != "event_fact_analysis" or definition.evaluator_version != version
        or contract.evaluator_version != version or contract.result_hash != ref["result_hash"]
        or evidence.evidence_hash != ref["evidence_hash"] or evidence.evidence_kind != "frozen_market_data"
        or request.mode != "evidence" or result.get("schema_version") != ("indicator_event_barriers_result.v1" if kind == "training" else RESULT_SCHEMA)
        or result.get("status") != "completed"):
        invalid("saved source class, version or immutable identity differs")
    if (semantic_hash(result["indicator_graph"]) != semantic_hash(p["plan"]["indicator_graph"])
        or result["indicator_graph_hash"] != evidence.indicator_graph_hash
        or result["input_origin"] != request.parameters["outcomes"]["input_origin"]
        or result["evaluation_range"] != p["plan"]["evaluation_range"]):
        invalid("saved original graph or episode clocks differ")
    check_graph(result["indicator_graph"], result["input_origin"])
    if kind == "training":
        parameters = deepcopy(request.parameters)
        # Reuse the new closed scientific validation without admitting a new
        # request or confusing the historical version's normalized schema.
        if parameters["detector"] != DETECTOR or parameters["statistics"] != {"features": FEATURE_SPECS, "illustrations": []}:
            invalid("saved development scientific feature contract differs")
        expected = {**parameters["outcomes"], "schema_version": barriers.SCHEMA}
        if result["configuration"] != {"detector": DETECTOR, "outcomes": expected, "statistics": parameters["statistics"]}:
            invalid("saved configuration differs from original request")
        if (expected["horizon_bars"] != 12 or expected["support_lookback_bars"] != 97
            or expected["reference"] != {"operator": "event_close"} or expected["failure"] != {"operator": "window_low"}
            or expected["target"] != {"operator": "risk_multiple", "multiple": 1.0}
            or utc(request.scope["start"]) != utc(expected["input_origin"]) + timedelta(minutes=1470)):
            invalid("saved source geometry or reporting boundary differs")
        paths = {"window_start_path": "metadata.recognition.formation_start", "identity_path": "metadata.recognition.event_id",
                 "source_path": "metadata.recognition.context.source_identity_key", "known_at_path": "metadata.recognition.known_at"}
        if any(expected.get(k) != v for k, v in paths.items()) or request.scope["timeframe"] != "15m":
            invalid("saved source public metadata paths differ")
        if semantic_hash(population_projection(result["cases"], result["dependence"])) != ref["population_hash"] or result["population_hash"] != ref["population_hash"]:
            invalid("saved population hash differs")
    else:
        math.validate_model(result["model"], training_manifest_hash=result["training_manifest"]["manifest_hash"])
        scored = result["scored_evidence"]
        if semantic_hash(scored) != result["scored_evidence_hash"]:
            invalid("saved scored evidence digest differs")
        if kind == "model" and result["model"]["model_hash"] != ref["model_hash"]:
            invalid("frozen model reference differs")
        if kind == "scores" and result["scored_evidence_hash"] != ref["scored_evidence_hash"]:
            invalid("saved scored evidence reference differs")
        original = math.root_rows(result["cases"], episode=result["input_origin"], origin=result["input_origin"],
                                  end=result["evaluation_range"]["end_exclusive"], source_check_id="current")
        math.validate_scores(original, scored["scores"])
        if (original != scored["cases"]
            or math.match(original, scored["alternatives"]) != scored["pairs"]
            or scored["model_hash"] != result["model"]["model_hash"]
            or scored["training_manifest_hash"] != result["training_manifest"]["manifest_hash"]):
            invalid("saved scoring, matching, model or original clock mass differs")
    binding = dict(evidence.input_binding)
    primary, source = barriers.native_candle_series({**binding, "series": [s for s in binding["series"] if s["alias"] == "primary_bars"]}, dataset_id=request.dataset_id)
    if (result["dataset_id"] != binding["dataset_id"] or result["dataset_hash"] != binding["dataset_hash"]
        or result["max_commit_seq"] != binding["max_commit_seq"] or result["frozen_input_binding_hash"] != semantic_hash(binding)
        or result["frozen_primary"] != primary or result["source_identity_key"] != source
        or len(result["cases"]) > 10000):
        invalid("saved native provenance or cardinality differs")
    if result["sample_count"] != len({r["decision_at"] for r in result["cases"]}):
        invalid("saved completion-clock cardinality differs")
    for row in result["cases"]:
        execution_checkpoint()
        at = utc(row["decision_at"])
        event = row["event"]
        recognition = event["metadata"]["recognition"]
        context = recognition["context"]
        outcome = row["outcome"]
        if (not utc(request.scope["start"]) <= at < utc(request.scope["end"])
            or utc(event["known_at"]) != at or utc(recognition["known_at"]) != at
            or row["event_id"] != event["pattern_id"] or row["event_id"] != recognition["event_id"]
            or recognition["definition_id"] != "completed_reversal_candidates" or recognition["definition_version"] != "provisional.1"
            or context["source_identity_key"] != source or context["instrument_id"] != request.scope["instrument_id"]
            or context["timeframe_seconds"] != 900 or utc(context["computation_origin"]) != utc(result["input_origin"])
            or utc(recognition["formation_start"]) != utc(row["reference_window_start"])
            or outcome["classification"] not in math.ALL_CLASSES or utc(outcome["nominal_end"]) != at + timedelta(hours=3)
            or (outcome.get("outcome_known_at") is not None and not at <= utc(outcome["outcome_known_at"]) <= utc(outcome["nominal_end"]))):
            invalid("saved public recognition, known-at, class or episode custody differs")
        if any(utc(ref["known_at"]) > at or utc(ref["consumed_at"]) > at for ref in recognition["source_events"]):
            invalid("future public reference ancestry")
        if outcome["classification"] in math.CLASSES and (outcome.get("outcome_known_at") is None or utc(outcome["outcome_known_at"]) <= at):
            invalid("resolved saved label lacks strictly later known-at")
        for spec in FEATURE_SPECS:
            value = row["reference_window_bars"] / 4 if spec["operator"] == "window_duration_hours" else path_value(event, spec["path"])
            if row["features"].get(spec["name"]) != (value if math.finite(value) else None):
                invalid("saved feature differs from its public projection")
    return {"reference": ref, "request": request, "result": result, "bytes": size,
            "cases": len(result["cases"]), "binding_hash": semantic_hash(binding), "source": source,
            "input_identity": {"dataset_id": binding["dataset_id"], "dataset_hash": binding["dataset_hash"],
                               "max_commit_seq": binding["max_commit_seq"], "binding_hash": semantic_hash(binding),
                               "instrument_id": primary["instrument_id"], "timeframe_seconds": primary["timeframe_seconds"],
                               "material_hash": primary["material_hash"], "provenance_hash": primary["provenance_hash"],
                               "quality_hash": primary["quality_hash"], "indicator_graph_hash": evidence.indicator_graph_hash,
                               "input_origin": result["input_origin"], "evaluation_range": result["evaluation_range"]}}


def dependency_inventory(loaded):
    return [{"reference": r["reference"], "envelope_bytes": r["bytes"], "case_count": r["cases"],
             "original_input_binding_hash": r["binding_hash"], "original_input": r["input_identity"],
             "source_identity_key": r["source"]} for r in loaded]


def dependencies(config, *, item_loader=None):
    with dependency_scope():
        rows = [load_source(r, kind="training", item_loader=item_loader) for r in config["training_sources"]]
        if config["frozen_model_source"]:
            rows.append(load_source(config["frozen_model_source"], kind="model", item_loader=item_loader))
        rows += [load_source(r, kind="scores", item_loader=item_loader) for r in config["prior_scoring_sources"]]
    if sum(r["bytes"] for r in rows) > RULES["saved_input_bytes"] or sum(r["cases"] for r in rows) > RULES["saved_case_limit"]:
        invalid("finite total saved input bound exceeded")
    return rows


@dataclass(frozen=True)
class SavedCaseComparisonEvaluator:
    evaluator_id: str = "event_fact_analysis"
    version: str = "16"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request, inspect_saved=True):
        from dataclasses import replace
        p = legacy_parameters(request.parameters)
        declaration = barriers.requirements(replace(request, parameters=p, request_hash=""))
        config = request.parameters["statistics"]["comparison"]
        if not inspect_saved:
            return {**declaration, "saved_dependency_references": deepcopy(config), "saved_dependency_limits": RULES}
        loaded = dependencies(config)
        inventory = dependency_inventory(loaded)
        return {**declaration, "saved_dependencies": inventory, "saved_dependencies_hash": semantic_hash(inventory),
                "saved_dependency_limits": RULES}

    def evaluate(self, *, plan, inputs):
        return evaluate(plan=plan, inputs=inputs)


def evaluate(*, plan, inputs):
    began = monotonic()
    def checkpoint():
        execution_checkpoint()
        if monotonic() - began >= 120:
            invalid("120second total comparison wall budget exceeded")
    config = inputs["statistics"]["comparison"]
    loaded = dependencies(config)
    inventory = dependency_inventory(loaded)
    if inventory != plan.execution["saved_dependencies"]:
        invalid("saved dependency inventory changed after admission")
    check_graph(plan.indicator_graph, inputs["outcomes"]["input_origin"])
    episode = inputs["outcomes"]["input_origin"]
    binding = inputs["frozen_input_binding"]
    primary, source = barriers.native_candle_series({**binding, "series": [r for r in binding["series"] if r["alias"] == "primary_bars"]}, dataset_id=binding["dataset_id"])
    schedule = config["validation_episodes"]
    index = next(i for i, e in enumerate(schedule) if e["origin"] == episode)
    training = []
    identities, clocks = set(), set()
    for saved in loaded[:len(config["training_sources"])]:
        checkpoint()
        r = saved["result"]
        if saved["request"].scope["instrument_id"] != primary["instrument_id"] or saved["source"] != source:
            invalid("saved instrument or exact source differs")
        if utc(r["evaluation_range"]["end_exclusive"]) > utc(config["training_cutoff"]):
            invalid("training episode after cutoff")
        rows = math.root_rows(r["cases"], episode=r["input_origin"], origin=r["input_origin"],
                             end=r["evaluation_range"]["end_exclusive"], source_check_id=saved["reference"]["check_id"])
        local_clocks = {x["decision_at"] for x in rows}
        if clocks & local_clocks:
            invalid("duplicate actual development decision clocks")
        clocks |= local_clocks
        for row in rows:
            identity = (row["decision_at"], row["event_id"])
            if identity in identities:
                invalid("duplicate saved case")
            identities.add(identity)
        training.extend(rows)
    training.sort(key=lambda r: (utc(r["decision_at"]), r["event_id"], r["source_check_id"]))
    eligible = [r for r in training if math.eligible(r, training_cutoff=config["training_cutoff"])]
    train_support = support(training, eligible)
    if not (train_support["eligible_clocks"] >= 200 and all(train_support["class_clocks"][k] >= 20 for k in math.CLASSES) and train_support["eligible_mass_fraction"] >= .9):
        invalid("unchanged training support gates not met")
    manifest = {"sources": config["training_sources"], "cutoff": config["training_cutoff"],
                "ordered_cases": [[r["source_check_id"], r["decision_at"], r["event_id"], r["root_weight"]] for r in eligible]}
    manifest_hash = semantic_hash(manifest)
    if config["frozen_model_source"] is None:
        model = math.fit_models(eligible, training_manifest_hash=manifest_hash, checkpoint=checkpoint)
    else:
        saved = loaded[len(config["training_sources"])]["result"]
        if saved["comparison_configuration"] != {**config, "frozen_model_source": None, "prior_scoring_sources": []}:
            invalid("frozen training/configuration differs")
        model = deepcopy(saved["model"])
    math.validate_model(model, training_manifest_hash=manifest_hash)
    prior = loaded[len(config["training_sources"]) + (config["frozen_model_source"] is not None):]
    episodes = []
    for saved in prior:
        r = saved["result"]
        other = r["comparison_configuration"]
        if r["model"] != model or any(other[k] != config[k] for k in ("training_sources", "training_cutoff", "comparison_recipe", "validation_episodes")) or other["prior_scoring_sources"]:
            invalid("saved validation model/recipe differs")
        episodes.append(r["scored_evidence"])
    if prior and ([e["episode"] for e in episodes] != [e["origin"] for e in schedule[:3]]
                  or prior[0]["reference"]["check_id"] != config["frozen_model_source"]["check_id"]):
        invalid("aggregate requires the exact first model Check and ordered previous episodes")
    logger.info("saved_case_comparison_model_frozen | model_hash=%s training_manifest_hash=%s episode=%s prior_market_replay=false",
                model["model_hash"], manifest_hash, episode)
    checkpoint()
    current = barriers.evaluate(plan=plan, inputs=legacy_parameters(inputs), external_checkpoint=checkpoint)
    cases = math.root_rows(current["cases"], episode=episode, end=current["evaluation_range"]["end_exclusive"],
                           origin=episode, source_check_id="current")
    candles = tuple(RecognitionCandle.from_mapping(r, source_identity_key=source) for r in inputs["indicator_evidence"]["candles"])
    alternatives = math.alternative_population(candles, start=plan.evaluation_range["start"], end=plan.evaluation_range["end_exclusive"],
                                               episode=episode, source_key=source, checkpoint=checkpoint)
    pairs = math.match(cases, alternatives, checkpoint=checkpoint)
    scores = math.score(cases, model)
    scored = {"episode": episode, "cases": cases, "alternatives": alternatives, "pairs": pairs, "scores": scores,
              "model_hash": model["model_hash"], "training_manifest_hash": manifest_hash}
    episodes.append(scored)
    if prior and [e["episode"] for e in episodes] != [e["origin"] for e in schedule]:
        invalid("explicit aggregate must retain the four original episodes in order")
    result = {**current, "schema_version": RESULT_SCHEMA, "analysis_status": "frozen_saved_case_comparison",
              "comparison_configuration": deepcopy(config), "training_support": train_support,
              "training_manifest": {"sources": config["training_sources"], "cutoff": config["training_cutoff"],
                                    "ordered_cases_hash": semantic_hash(manifest["ordered_cases"]), "manifest_hash": manifest_hash},
              "saved_dependency_inventory": inventory, "model": model, "scored_evidence": scored,
              "scored_evidence_hash": semantic_hash(scored), "comparison": summarize(episodes, schedule, checkpoint),
              "views": {"current_episode": summarize([scored], schedule, checkpoint),
                        "declared_validation_block": summarize([e for e in episodes if e["episode"] in {
                            s["origin"] for s in schedule[(index // 2) * 2:(index // 2) * 2 + 2]}], schedule, checkpoint)},
              "rules": RULES, "economic_verdict": "untested;no executable profitability claim"}
    checkpoint()
    validate_result_size(result)
    return result


def validate_result_size(result):
    if len(json.dumps(result, allow_nan=False, separators=(",", ":")).encode()) > RULES["result_bytes"]:
        invalid("16MiB result bound exceeded;no truncation")
    # The existing native export accepts an8MiB complete pretty-printed record.
    # Reserve1MiB for the bounded request, graph, evidence and record metadata.
    if len(json.dumps(result, allow_nan=False, indent=2, sort_keys=True).encode()) > RULES["pretty_result_bytes"]:
        invalid("existing8MiB direct export envelope allowance exceeded;no truncation")


def support(rows, selected):
    total = sum(r["root_weight"] for r in rows)
    return {"all_case_mass": total, "eligible_mass": sum(r["root_weight"] for r in selected),
            "eligible_mass_fraction": sum(r["root_weight"] for r in selected) / total if total else 0,
            "eligible_clocks": len({r["decision_at"] for r in selected}),
            "class_clocks": {k: len({r["decision_at"] for r in selected if r["outcome"]["classification"] == k}) for k in math.ALL_CLASSES}}


def summarize(episodes, schedule, checkpoint):
    cases = [r for e in episodes for r in e["cases"]]
    controls = [r for e in episodes for r in e["alternatives"]]
    pairs = [r for e in episodes for r in e["pairs"]]
    scores = [r for e in episodes for r in e["scores"]]
    if len({r["event_id"] for r in cases}) != len(cases) or len({r["event_id"] for r in controls}) != len(controls):
        invalid("duplicate actual aggregate case or control identity")
    clock_episode = {}
    for r in cases:
        clock = plain(utc(r["decision_at"]))
        if clock in clock_episode and clock_episode[clock] != r["episode"]:
            invalid("duplicate actual aggregate decision clocks across episodes")
        clock_episode[clock] = r["episode"]
    by_id = {r["event_id"]: r for r in cases}
    enriched = [{**by_id[r["event_id"]], "gain": r["gain"]} for r in scores]
    mass = sum(r["root_weight"] for r in scores)
    duration = sum(r["root_weight"] * r["gain"] for r in scores) / mass if mass else None
    alternative = math.matched_summary(cases, controls, pairs)
    ac = {r["event_id"]: r for r in controls}
    paired = []
    for p in pairs:
        c, a = by_id[p["event_id"]], ac[p["alternative_id"]]
        lc, la = c["outcome"]["classification"], a["outcome"]["classification"]
        paired.append({**c, "support_start": min(c["support_start"], a["support_start"]),
                       "support_end": max(c["support_end"], a["support_end"]),
                       "source_reference_ids": [*c.get("source_reference_ids", []), *a.get("source_reference_ids", [])],
                       "alternative_id": a["event_id"],
                       "contrast": math.direction(lc) - math.direction(la) - (lc == "unresolved") - (la == "unresolved")})
    structural = [*cases, *[ac[p["alternative_id"]] for p in pairs]]
    intervals = {"duration": math.conditional_interval(enriched, value="gain", group_rows=structural, checkpoint=checkpoint),
                 "alternative": math.conditional_interval(paired, value="contrast", group_rows=structural, checkpoint=checkpoint)}
    eligibility = support(cases, [r for r in cases if math.eligible(r)])
    block_rows = []
    for i in range(2):
        names = {e["origin"] for e in schedule[i * 2:i * 2 + 2]}
        ss = [r for r in scores if r["episode"] in names]
        pp = [r for r in paired if r["episode"] in names]
        block_rows.append({"block": i + 1, "present_episodes": sorted({e["episode"] for e in episodes if e["episode"] in names}),
                           "complete": {e["episode"] for e in episodes if e["episode"] in names} == names,
                           "duration_gain": weighted_value(ss, "gain"), "alternative_contrast": weighted_value(pp, "contrast")})
    full = len(episodes) == 4
    valid = full and eligibility["eligible_clocks"] >= 100 and eligibility["class_clocks"]["continuation"] >= 20 and eligibility["class_clocks"]["failure"] >= 20 and eligibility["eligible_mass_fraction"] >= .9
    positive = lambda name: intervals[name]["status"] == "completed" and intervals[name]["bounds"][0] > 0
    balance = all(alternative["balance"][k]["smd"] is not None and abs(alternative["balance"][k]["smd"]) <= .1 for k in ("log_sigma96", "log_r", "z_pre12"))
    return {"episode_count": len(episodes), "validation_support": eligibility, "duration_gain": duration,
            "opportunities": {"cases": opportunity_summary(cases, weighted=True),
                              "alternatives": opportunity_summary(controls, weighted=False),
                              "coincident_alternative_clocks": len({r["decision_at"] for r in cases} & {r["decision_at"] for r in controls})},
            "alternative": alternative, "conditional_intervals": intervals, "chronological_blocks": block_rows,
            "episodes": [{"origin": e["episode"], "duration_gain": weighted_value(e["scores"], "gain"),
                          "scored_mass": sum(r["root_weight"] for r in e["scores"]),
                          "case_opportunities": opportunity_summary(e["cases"], weighted=True),
                          "alternative": math.matched_summary(e["cases"], e["alternatives"], e["pairs"])} for e in episodes],
            "alternative_interval_basis": "worst_allowed_unresolved_contribution_on_original_matched_mass",
            "duration_supported": bool(valid and duration is not None and duration >= .005 and positive("duration") and all(b["duration_gain"] is not None and b["duration_gain"] > 0 for b in block_rows)),
            "alternative_supported": bool(valid and alternative["coverage"] is not None and alternative["coverage"] >= .8 and balance and alternative["matched_unknown_bounds"] and alternative["matched_unknown_bounds"][0] >= .02 and positive("alternative") and all(b["alternative_contrast"] is not None and b["alternative_contrast"] > 0 for b in block_rows)),
            "scientific_verdict": "conditional exploratory support only;four episodes cannot certify generalization",
            "independent_sample_size": None}


def weighted_value(rows, field):
    total = sum(r["root_weight"] for r in rows)
    return sum(r["root_weight"] * r[field] for r in rows) / total if total else None


def opportunity_summary(rows, *, weighted):
    return {"rows": len(rows), "decision_clocks": len({r["decision_at"] for r in rows}),
            "class_counts": {k: sum(r["outcome"]["classification"] == k for r in rows) for k in math.ALL_CLASSES},
            "class_mass": {k: sum((r["root_weight"] if weighted else 1.) for r in rows if r["outcome"]["classification"] == k) for k in math.ALL_CLASSES}}

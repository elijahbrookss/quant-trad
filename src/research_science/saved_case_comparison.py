"""Frozen, weighted case comparison math; no repositories or market replay.

Rows retain original clock units. Matching has a deliberately separate feature
projection so neither labels nor duration can influence control selection.
"""
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import timedelta
import math

import numpy as np
from scipy.optimize import minimize
from scipy.special import logsumexp, softmax

from .hashing import stable_hash
from .recognition import plain, utc

CLASSES = ("continuation", "failure", "neither")
ALL_CLASSES = (*CLASSES, "unresolved")
FEATURES = ("log_sigma96", "log_r", "z_pre12", "utc6h_06", "utc6h_12", "utc6h_18")
MODEL_SETTINGS = {"C": 1.0, "solver": "lbfgs", "fit_intercept": True,
                  "seed": 20261102, "tol": 1e-9, "max_iter": 1000}


def invalid(reason):
    raise ValueError("saved_case_comparison_invalid: " + reason)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def root_rows(cases, *, episode, end, origin, source_check_id):
    """Convert normalized v15 cases without redistributing excluded mass."""
    rows = deepcopy(cases)
    for r in rows:
        r["decision_at"] = plain(utc(r["decision_at"]))
    clocks = len({r["decision_at"] for r in rows})
    counts = Counter(r["decision_at"] for r in rows)
    if len({r["event_id"] for r in rows}) != len(rows):
        invalid("duplicate event identity")
    for r in rows:
        w = r["original_weight"] * clocks
        if not finite(w) or not math.isclose(w, 1 / counts[r["decision_at"]], rel_tol=1e-12):
            invalid("original all-chain clock weight differs")
        r.update(root_weight=w, episode=episode, episode_end=plain(utc(end)),
                 recognition_origin=plain(utc(origin)), source_check_id=source_check_id)
        f = r["features"]
        # recovery_candles is recognizer-owned; never use passage duration.
        r["comparison_features"] = {"sigma96": f.get("sigma96"), "r": f.get("r"),
                                    "z_pre12": f.get("z_pre12"), "N": f.get("recovery_candles")}
        recognition = ((r.get("event") or {}).get("metadata") or {}).get("recognition") or {}
        r["source_reference_ids"] = sorted(set(r.get("source_reference_ids", [])) | {
            ref["event_id"] for ref in recognition.get("source_events", [])})
    return rows


def eligible(row, *, training_cutoff=None):
    f = row["comparison_features"]
    g = row["boundary_geometry"]
    o = row["outcome"]
    return (all(finite(f.get(k)) for k in ("sigma96", "r", "z_pre12", "N"))
            and f["sigma96"] > 0 and f["r"] > 0 and f["N"] >= 1 and float(f["N"]).is_integer()
            and all(finite(g.get(k)) for k in ("L", "E", "upper"))
            and 0 < g["L"] < g["E"] < g["upper"]
            and o["classification"] in CLASSES and o.get("outcome_known_at") is not None
            and utc(o["nominal_end"]) < utc(row["episode_end"])
            and (training_cutoff is None or utc(o["outcome_known_at"]) < utc(training_cutoff)))


def raw_features(rows, *, enriched):
    return np.asarray([[math.log(r["comparison_features"]["sigma96"]),
                        math.log(r["comparison_features"]["r"]),
                        r["comparison_features"]["z_pre12"],
                        *[float(utc(r["decision_at"]).hour // 6 == b) for b in (1, 2, 3)],
                        *([math.log1p(r["comparison_features"]["N"])] if enriched else [])]
                       for r in rows], dtype=float).reshape(len(rows), 7 if enriched else 6)


def weighted_scaler(x, weights, indices):
    result = []
    for i in indices:
        mean = float(np.average(x[:, i], weights=weights))
        variance = 0.0 if np.ptp(x[:, i]) == 0 else float(np.average((x[:, i] - mean) ** 2, weights=weights))
        result.append({"index": i, "mean": mean, "std": math.sqrt(variance),
                       "reason": "zero_training_variance" if variance == 0 else None})
    return result


def transform(x, scaler):
    x = x.copy()
    for s in scaler:
        x[:, s["index"]] = (x[:, s["index"]] - s["mean"]) / s["std"] if s["std"] else 0
    return x


def fit_models(rows, *, training_manifest_hash, checkpoint=lambda: None):
    """Minimize weighted multinomial NLL + ||coef||²/(2C), free intercept.

    Sum-weight objective preserves the declared C at original clock units.
    Numeric artifacts, rather than solver objects, cross the custody boundary.
    """
    if not rows or set(r["outcome"]["classification"] for r in rows) != set(CLASSES):
        invalid("all three training classes required")
    weights = np.asarray([r["root_weight"] for r in rows], dtype=float)
    y = np.asarray([CLASSES.index(r["outcome"]["classification"]) for r in rows])
    base = raw_features(rows, enriched=False)
    scaler = weighted_scaler(base, weights, range(3))
    arms = {}
    for enriched in (False, True):
        x = raw_features(rows, enriched=enriched)
        scales = scaler + (weighted_scaler(x, weights, [6]) if enriched else [])
        x = np.column_stack((transform(x, scales), np.ones(len(rows))))
        p = x.shape[1]

        def objective(flat):
            checkpoint()
            coefficients = flat.reshape(3, p)
            logits = x @ coefficients.T
            loss = float(np.dot(weights, logsumexp(logits, axis=1) - logits[np.arange(len(y)), y]))
            probabilities = softmax(logits, axis=1)
            probabilities[np.arange(len(y)), y] -= 1
            gradient = (probabilities * weights[:, None]).T @ x
            loss += float(np.sum(coefficients[:, :-1] ** 2)) / (2 * MODEL_SETTINGS["C"])
            gradient[:, :-1] += coefficients[:, :-1] / MODEL_SETTINGS["C"]
            return loss, gradient.ravel()

        fitted = minimize(objective, np.zeros(3 * p), jac=True, method="L-BFGS-B",
                          options={"gtol": MODEL_SETTINGS["tol"], "ftol": 1e-15,
                                   "maxiter": MODEL_SETTINGS["max_iter"]})
        if not fitted.success or not np.isfinite(fitted.x).all():
            invalid("unsupported model nonconvergence: " + str(fitted.message))
        coef = fitted.x.reshape(3, p)
        arms["enriched" if enriched else "base"] = {
            "features": [*FEATURES, *(["log1p_N"] if enriched else [])],
            "scalers": scales, "coefficients": coef[:, :-1].tolist(),
            "intercepts": coef[:, -1].tolist(), "iterations": int(fitted.nit)}
    model = {"schema_version": "saved_case_multinomial_model.v1", "classes": list(CLASSES),
             "settings": dict(MODEL_SETTINGS), "training_manifest_hash": training_manifest_hash,
             "arms": arms, "objective": "sum_root_weight_NLL_plus_L2_over_2C;unpenalized_intercept"}
    return {**model, "model_hash": stable_hash(model)}


def validate_model(model, *, training_manifest_hash):
    raw = {k: v for k, v in model.items() if k != "model_hash"}
    if (set(model) != {"schema_version", "classes", "settings", "training_manifest_hash", "arms", "objective", "model_hash"}
            or model.get("schema_version") != "saved_case_multinomial_model.v1"
            or stable_hash(raw) != model.get("model_hash") or model.get("classes") != list(CLASSES)
            or model.get("settings") != MODEL_SETTINGS or model.get("training_manifest_hash") != training_manifest_hash):
        invalid("frozen model identity or configuration differs")
    for enriched, name in ((False, "base"), (True, "enriched")):
        arm = model["arms"][name]
        expected = [*FEATURES, *(["log1p_N"] if enriched else [])]
        if arm["features"] != expected or np.asarray(arm["coefficients"]).shape != (3, len(expected)):
            invalid("frozen feature order or coefficient dimensions differ")
        if np.asarray(arm["intercepts"]).shape != (3,) or not np.isfinite(np.asarray(arm["coefficients"])).all() or not np.isfinite(arm["intercepts"]).all():
            invalid("nonfinite frozen model")
        expected_indices = [0, 1, 2, *([6] if enriched else [])]
        if [s["index"] for s in arm["scalers"]] != expected_indices or any(not finite(s["mean"]) or not finite(s["std"]) or s["std"] < 0 for s in arm["scalers"]):
            invalid("invalid frozen scaler")
    if model["arms"]["base"]["scalers"] != model["arms"]["enriched"]["scalers"][:3]:
        invalid("paired base scalers differ")


def score(rows, model):
    selected = [r for r in rows if eligible(r)]
    result = [{"event_id": r["event_id"], "decision_at": r["decision_at"],
               "episode": r["episode"], "root_weight": r["root_weight"],
               "classification": r["outcome"]["classification"]} for r in selected]
    for enriched, name in ((False, "base"), (True, "enriched")):
        arm = model["arms"][name]
        x = transform(raw_features(selected, enriched=enriched), arm["scalers"])
        probabilities = softmax(x @ np.asarray(arm["coefficients"]).T + arm["intercepts"], axis=1)
        for row, ps in zip(result, probabilities):
            if not np.isfinite(ps).all() or np.any(ps <= 0):
                invalid("unsupported zero or nonfinite probabilities")
            row[name + "_probabilities"] = ps.tolist()
            row[name + "_loss"] = -math.log(float(np.clip(ps[CLASSES.index(row["classification"])], 1e-15, 1 - 1e-15)))
    for row in result:
        row["gain"] = row["base_loss"] - row["enriched_loss"]
    return result


def validate_scores(cases, scores):
    """Validate saved probabilities/loss identities without predicting again."""
    selected = [r for r in cases if eligible(r)]
    if len(selected) != len(scores):
        invalid("saved paired scoring mask differs")
    for case, saved in zip(selected, scores):
        expected = {"event_id": case["event_id"], "decision_at": case["decision_at"], "episode": case["episode"],
                    "root_weight": case["root_weight"], "classification": case["outcome"]["classification"]}
        if any(saved.get(k) != v for k, v in expected.items()):
            invalid("saved score identity, class or original root mass differs")
        for name in ("base", "enriched"):
            ps = saved.get(name + "_probabilities")
            if not isinstance(ps, list) or len(ps) != 3 or any(not finite(p) or not 0 < p <= 1 for p in ps) or not math.isclose(math.fsum(ps), 1., abs_tol=1e-12):
                invalid("saved class-ordered probabilities invalid")
            loss = -math.log(min(1 - 1e-15, max(1e-15, ps[CLASSES.index(saved["classification"])])))
            if not finite(saved.get(name + "_loss")) or not math.isclose(saved[name + "_loss"], loss, rel_tol=1e-12, abs_tol=1e-12):
                invalid("saved paired loss differs")
        if not finite(saved.get("gain")) or not math.isclose(saved["gain"], saved["base_loss"] - saved["enriched_loss"], rel_tol=1e-12, abs_tol=1e-12):
            invalid("saved paired gain differs")


def matching_projection(row):
    f = row["comparison_features"]
    if not all(finite(f.get(k)) for k in ("sigma96", "r", "z_pre12")) or f["sigma96"] <= 0 or f["r"] <= 0:
        return None
    return {"id": row["event_id"], "clock": row["decision_at"], "episode": row["episode"],
            "bin": utc(row["decision_at"]).hour // 6, "sigma": f["sigma96"],
            "r": f["r"], "z": f["z_pre12"]}


def match(cases, alternatives, *, checkpoint=lambda: None):
    controls = [p for r in alternatives if (p := matching_projection(r)) is not None]
    used, pairs = set(), []
    for case in sorted(cases, key=lambda r: (utc(r["decision_at"]), r["event_id"])):
        checkpoint()
        c = matching_projection(case)
        candidates = []
        if c is not None:
            for a in controls:
                clock = (a["episode"], a["clock"])
                if clock in used or a["episode"] != c["episode"] or a["bin"] != c["bin"] or utc(a["clock"]) == utc(c["clock"]):
                    continue
                ds, dr, dz = math.log(c["sigma"] / a["sigma"]), math.log(c["r"] / a["r"]), c["z"] - a["z"]
                if abs(ds) <= math.log(1.25) and abs(dr) <= math.log(1.10) and abs(dz) <= .25:
                    distance = (ds / math.log(1.25)) ** 2 + (dr / math.log(1.10)) ** 2 + (dz / .25) ** 2
                    candidates.append((distance, abs((utc(c["clock"]) - utc(a["clock"])).total_seconds()), utc(a["clock"]), a["id"], a))
        if candidates:
            best = min(candidates)
            used.add((best[-1]["episode"], best[-1]["clock"]))
            pairs.append({"event_id": case["event_id"], "alternative_id": best[-1]["id"],
                          "distance": best[0], "root_weight": case["root_weight"], "episode": case["episode"]})
    return pairs


def direction(label):
    if label not in ALL_CLASSES:
        invalid("unknown class")
    return 1 if label == "continuation" else -1 if label == "failure" else 0


def weighted_moment(values, weights):
    if not values or not sum(weights):
        return None, None
    mean = float(np.average(values, weights=weights))
    return mean, float(np.average((np.asarray(values) - mean) ** 2, weights=weights))


def matched_summary(cases, alternatives, pairs):
    cs = {r["event_id"]: r for r in cases}
    controls = {r["event_id"]: r for r in alternatives}
    total = math.fsum(r["root_weight"] for r in cases)
    mass = math.fsum(p["root_weight"] for p in pairs)
    signed = lower = upper = 0.0
    classes = {k: 0.0 for k in ALL_CLASSES}
    for p in pairs:
        c, a = cs[p["event_id"]], controls[p["alternative_id"]]
        lc, la = c["outcome"]["classification"], a["outcome"]["classification"]
        delta = direction(lc) - direction(la)
        unknown = (lc == "unresolved") + (la == "unresolved")
        signed += p["root_weight"] * delta
        lower += p["root_weight"] * (delta - unknown)
        upper += p["root_weight"] * (delta + unknown)
        for k in ALL_CLASSES:
            classes[k] += p["root_weight"] * ((lc == k) - (la == k))
    balance = {}
    for name in ("sigma96", "r", "z_pre12"):
        for log in ((True, False) if name != "z_pre12" else (False,)):
            left = [cs[p["event_id"]]["comparison_features"][name] for p in pairs]
            right = [controls[p["alternative_id"]]["comparison_features"][name] for p in pairs]
            if log:
                left, right = list(map(math.log, left)), list(map(math.log, right))
            w = [p["root_weight"] for p in pairs]
            ml, vl = weighted_moment(left, w)
            mr, vr = weighted_moment(right, w)
            denominator = math.sqrt((vl + vr) / 2) if vl is not None else None
            smd = ((ml - mr) / denominator if denominator else 0.0 if ml is not None and ml == mr else None)
            balance[("log_" if log else "") + name] = {"case_mean": ml, "alternative_mean": mr, "smd": smd}
    return {"all_case_mass": total, "matched_mass": mass, "unmatched_mass": total - mass,
            "coverage": mass / total if total else None,
            "T_minus_F_algebra": signed / mass if mass else None,
            "matched_unknown_bounds": [lower / mass, upper / mass] if mass else None,
            "full_population_bounds": [(lower - 2 * (total - mass)) / total, (upper + 2 * (total - mass)) / total] if total else None,
            "class_differences": {k: v / mass if mass else None for k, v in classes.items()},
            "balance": balance, "interpretation": "observational conditional contrast;unresolved is unknown;not profit or causality"}


def conditional_interval(rows, *, value, group_rows=None, checkpoint=lambda: None):
    """Common replay origins conservatively join each episode's supports.

    A single origin is one linked group even if direct intervals are disjoint.
    All original groups are sampled; undefined replicates invalidate the interval.
    """
    structural = rows if group_rows is None else group_rows
    origins = sorted({r["recognition_origin"] for r in structural})
    parent = {k: k for k in origins}
    def find(k):
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k
    def union(a, b):
        parent[find(b)] = find(a)
    references = {}
    last, end = None, None
    supported = sorted((r for r in structural if r.get("support_start") and r.get("support_end")),
                       key=lambda r: (utc(r["support_start"]), utc(r["support_end"])))
    for r in supported:
        origin = r["recognition_origin"]
        if end is not None and utc(r["support_start"]) <= end:
            union(last, origin)
        if end is None or utc(r["support_end"]) > end or utc(r["support_start"]) > end:
            last, end = origin, utc(r["support_end"])
    for r in structural:
        for key in [r["event_id"], *r.get("source_reference_ids", [])]:
            if key in references:
                union(references[key], r["recognition_origin"])
            references[key] = r["recognition_origin"]
    groups = defaultdict(list)
    for r in structural:
        groups[find(r["recognition_origin"])].append(r)
    membership = [{"origin": k, "event_ids": sorted(r["event_id"] for r in rs)} for k, rs in sorted(groups.items())]
    result = {"groups": membership, "group_count": len(groups), "replicates": 5000,
              "seed": 20261102, "confidence": .95, "undefined_draws": 0,
              "conditional_on_frozen_models": True, "independent_sample_size": None}
    if len(groups) < 2:
        return {**result, "status": "unsupported", "reason": "one_common_origin_group"}
    values = []
    value_groups = defaultdict(list)
    for r in rows:
        value_groups[find(r["recognition_origin"])].append(r)
    for key in groups:
        rs = value_groups[key]
        mass = math.fsum(r["root_weight"] for r in rs if value in r)
        signed = math.fsum(r["root_weight"] * r[value] for r in rs if value in r)
        values.append((mass, signed))
    rng = np.random.default_rng(20261102)
    samples = []
    for offset in range(0, 5000, 100):
        checkpoint()
        choices = rng.integers(0, len(values), size=(min(100, 5000 - offset), len(values)))
        sums = np.asarray(values)[choices].sum(axis=1)
        result["undefined_draws"] += int(np.count_nonzero(sums[:, 0] == 0))
        samples.extend((sums[:, 1] / np.where(sums[:, 0] == 0, 1, sums[:, 0])).tolist())
    if result["undefined_draws"] or len(set(samples)) < 2:
        return {**result, "status": "unsupported", "reason": "undefined_or_degenerate_draws"}
    return {**result, "status": "completed", "bounds": np.quantile(samples, [.025, .975]).tolist(),
            "caveat": "weak conditional linked-origin diagnostic;not population precision"}


def alternative_population(candles, *, start, end, episode, source_key, checkpoint=lambda: None):
    from .completed_event_context import CompletedEventContext, locked_reference_geometry
    from .reversal_outcomes import first_passage_path

    context = CompletedEventContext(candles, (), evaluation_start=utc(start),
                                    evaluation_end=utc(end), checkpoint=checkpoint)
    path = {c.open_time: c for c in candles}
    result = []
    for i, bar in enumerate(candles):
        checkpoint()
        if i < 97 or not utc(start) <= bar.close_time < utc(end) or bar.close <= candles[i - 1].close:
            continue
        low = context.trailing_lows(12)[i]
        features = context.geometry(i, 12)
        geometry = locked_reference_geometry(bar.close, low, bar.close_time)
        result.append({"event_id": stable_hash({"alternative": "upclose_trailing12.v1", "source": source_key,
                                               "episode": episode, "clock": plain(bar.close_time)}),
                       "decision_at": plain(bar.close_time), "episode": episode,
                       "episode_end": plain(utc(end)), "recognition_origin": plain(candles[0].open_time),
                       "support_start": plain(candles[i - 97].open_time),
                       "support_end": plain(bar.close_time + timedelta(minutes=180)),
                       "source_reference_ids": [], "boundary_geometry": geometry,
                       "comparison_features": {"sigma96": features["sigma"] if features else None,
                                               "r": features["r"] if features else None,
                                               "z_pre12": features["z_pre12"] if features else None},
                       "outcome": first_passage_path(anchor_at=bar.close_time, known_at=bar.known_at,
                                                     entry=bar.close, low=low, target=geometry["upper"],
                                                     source_identity_key=source_key, timeframe_seconds=900,
                                                     by_open=path, evaluation_end=utc(end), checkpoint=checkpoint)})
    return result

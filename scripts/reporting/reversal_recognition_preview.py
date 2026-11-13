"""Render bounded, already-retained candle examples with the domain recognizer.

This offline consumer does not read QT, acquire data, run Checks or calculate
outcomes. Cases name exact reference-confirmation clocks before rendering;
they do not arbitrate which overlapping candidates should own Study events.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from research_science.recognition import RecognitionCandle, RecognitionContext, plain, utc
from research_science.reversal_recognition import ReversalRecognizer, ReversalSettings


def prepare_preview(payload, cases, *, settings):
    if not 1 <= len(cases) <= 4:
        raise ValueError("preview_invalid: one to four explicitly bound cases required")
    manifest = payload["series_manifest"]
    records = payload["records"]
    if len(records) > settings.max_bars:
        raise ValueError("preview_invalid: input exceeds explicit candle bound")
    candles = tuple(RecognitionCandle.from_mapping(r["fact"], source_identity_key=r["source_identity_key"])
                    for r in records)
    context = RecognitionContext(manifest["instrument_id"], int(manifest["timeframe_seconds"]),
                                 utc(payload["start_inclusive"]), candles[0].source_identity_key)
    bindings = []
    for case in cases:
        start, end, role = (utc(case[k]) for k in ("start", "end", "reference_known_at"))
        if not context.computation_origin <= start < role <= end or (end-start).total_seconds() > 86400:
            raise ValueError("preview_invalid: bounded case clocks or reference binding invalid")
        bindings.append((case["label"], start, end, role))
    ends = {x[2] for x in bindings}
    snapshots = {}
    state = ReversalRecognizer(context, settings)
    for candle in candles:
        state.apply_bar(candle)
        if candle.close_time in ends:
            snapshots[candle.close_time] = state.snapshot()
    examples = []
    for label, start, end, role in bindings:
        if end not in snapshots:
            raise ValueError("preview_invalid: missing exact as-of snapshot;no future substitution")
        snapshot = snapshots[end]
        references = [r for r in snapshot.references if r.known_at == role]
        if len(references) != 1:
            raise ValueError("preview_invalid: requested reference is absent or ambiguous")
        reference = references[0]
        selected = [e for e in snapshot.candidates if e.source_events[0].event_id == reference.event_id]
        if len(selected) > 1:
            raise ValueError("preview_invalid: reference has multiple first completions")
        candidate = selected[0] if selected else None
        cut = candidate.known_at if candidate else end
        if cut < start:
            raise ValueError("preview_invalid: selected candidate predates display")
        pending = next((p for p in snapshot.pending if p.reference.event_id == reference.event_id), None)
        stages = candidate.stages if candidate else pending.stages
        levels = candidate.levels if candidate else pending.levels
        visible = [e for e in snapshot.candidates if start <= e.known_at <= cut]
        first = min((e.known_at for e in visible), default=None)
        examples.append({"label": label, "start": plain(start), "as_of": plain(cut),
            "requested_end": plain(end), "reference": reference.to_dict(),
            "candidate": candidate.to_dict() if candidate else None,
            "stages": plain([vars(s) for s in stages]), "levels": plain([vars(x) for x in levels]),
            "candidate_ids_in_display": [e.event_id for e in visible],
            "first_completion_at": plain(first),
            "first_completion_group": [e.to_dict() for e in visible if e.known_at == first],
            "candles": [plain(vars(c)) for c in candles if start <= c.open_time and c.close_time <= cut]})
    return {"schema_version": "reversal_recognition_preview.v1", "context": plain(vars(context)),
            "settings": settings.to_dict(), "examples": examples, "study_events_admitted": 0,
            "outcomes_calculated": False, "population_status": "ownership_unresolved",
            "caveats": ["Bound illustrative cases;not a representative sample or independent event population.",
                        "State starts at recorded origin;no pre-origin state claimed.",
                        "Reference binding preserves the requested chain;other candidates remain visible.",
                        "Close availability is historical input semantics,not measured live receipt."]}


def render_preview(preview, destination):
    context = RecognitionContext(**preview["context"])
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter, HourLocator, MinuteLocator, date2num
    from matplotlib.patches import Rectangle

    examples = preview["examples"]
    fig, axes = plt.subplots(len(examples), 1, figsize=(14, 5*len(examples)), squeeze=False)
    for ax, case in zip(axes[:, 0], examples):
        bars = case["candles"]
        if not bars:
            raise ValueError("preview_invalid: no display candles")
        width = preview["context"]["timeframe_seconds"]/86400*.65
        for bar in bars:
            x = date2num(utc(bar["close_time"]))
            color = "#137e70" if bar["close"] >= bar["open"] else "#bc4944"
            ax.vlines(x, bar["low"], bar["high"], color=color, linewidth=1)
            ax.add_patch(Rectangle((x-width/2, min(bar["open"], bar["close"])), width,
                max(abs(bar["close"]-bar["open"]), .01), facecolor=color, edgecolor=color))
        ref = case["reference"]["levels"][0]
        ax.hlines(ref["price"], date2num(utc(ref["role_known_at"])), date2num(utc(case["as_of"])),
                  color="#6248a0", linewidth=1.6, label=f"R {ref['price']:.2f}; usable after role confirmation")
        ax.scatter([date2num(utc(ref["price_known_at"]))], [ref["price"]], color="#6248a0", s=32, zorder=5)
        for stage, color in (("lower_low_push", "#bd7921"), ("recovery", "#337ca6"), ("completion", "#222222")):
            match = next((s for s in case["stages"] if s["name"] == stage), None)
            if match:
                ax.axvline(date2num(utc(match["known_at"])), color=color, linestyle="--", alpha=.7, label=stage.replace("_", " "))
        if case["first_completion_at"] and (not case["candidate"] or case["first_completion_at"] != case["candidate"]["known_at"]):
            ax.axvline(date2num(utc(case["first_completion_at"])), color="#8a5e9e", linestyle=":",
                       linewidth=2, label="earlier completion of another chain")
        title = "bound candidate completed" if case["candidate"] else "bound candidate incomplete at as-of"
        n = len(case["first_completion_group"])
        first = utc(case["first_completion_at"]).strftime("%H:%M") if case["first_completion_at"] else "none"
        clocks = " | ".join(f"{name} {utc(value).strftime('%H:%M')}" for name, value in
                            (("R price known", ref["price_known_at"]), ("role known", ref["role_known_at"]), ("as-of", case["as_of"])))
        ax.set_title(f"{case['label']} | {title}\n{clocks} UTC | {len(case['candidate_ids_in_display'])} chains in display; first {first}, group {n}",
                     loc="left", fontsize=10, pad=12)
        duration = (utc(case["as_of"])-utc(case["start"])).total_seconds()
        ax.xaxis.set_major_locator(MinuteLocator(byminute=range(0,60,15)) if duration <= 7200 else HourLocator(interval=1))
        ax.xaxis.set_major_formatter(DateFormatter("%H:%M"))
        ax.set_xlim(date2num(utc(bars[0]["close_time"]))-width,
                    date2num(utc(case["as_of"]))+width)
        ax.set_ylabel("Price"); ax.set_xlabel("Candle close, UTC"); ax.grid(alpha=.15)
        ax.legend(loc="lower left", fontsize=8)
    fig.suptitle(f"Provisional reversal recognition — instrument {context.instrument_id}\nFixed origin; retained overlapping chains; no Study population or forward outcomes", fontsize=14)
    fig.tight_layout(pad=2, h_pad=3, rect=(0, 0, 1, .96))
    fig.savefig(destination, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("cases", type=Path, help="JSON list:label,start,end,reference_known_at")
    parser.add_argument("output", type=Path, help="New output directory;existing evidence is never replaced")
    parser.add_argument("--reference-basis", required=True, choices=["rising_leg_only", "rising_leg_plus_retreat"])
    parser.add_argument("--max-bars", type=int, required=True)
    parser.add_argument("--max-candidates", type=int, required=True)
    args = parser.parse_args()
    if args.input.stat().st_size > 32*1024*1024 or args.cases.stat().st_size > 65536:
        raise ValueError("preview_invalid: input byte bound exceeded")
    data = args.input.read_bytes()
    preview = prepare_preview(json.loads(data), json.loads(args.cases.read_text()),
        settings=ReversalSettings(args.reference_basis, "retain_all_provisional", args.max_bars, args.max_candidates))
    preview["input_sha256"] = hashlib.sha256(data).hexdigest()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output/"preview-evidence.json").write_text(json.dumps(preview, indent=2)+"\n")
    render_preview(preview, args.output/"overview.png")


if __name__ == "__main__":
    main()

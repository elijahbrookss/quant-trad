"""Bounded current metadata for explicit referenced Checks, never inferred scope."""
from sqlalchemy import select, func
from portal.backend.db.models import InstrumentRecord, ResearchItemRecord as Item

def instruments(items, session):
    ids = {item.get("instrument_id") for item in items if item.get("instrument_id")}
    if not ids: return
    rows = session.execute(select(InstrumentRecord.id, InstrumentRecord.symbol,
        InstrumentRecord.instrument_type, InstrumentRecord.datasource, InstrumentRecord.exchange)
        .where(InstrumentRecord.id.in_(ids))).all()
    labels = {row.id: dict(row._mapping) for row in rows}
    for item in items:
        if item.get("instrument_id"): item["instrument"] = labels.get(item["instrument_id"])

def check_metadata(references, session):
    from .browse import columns, compact, _assert_item_access
    expected = {}
    for ref in references: expected.setdefault(ref["item_id"], set()).add((ref.get("result_hash"), ref.get("evidence_hash")))
    rows = session.execute(select(*columns(),
        Item.payload["result"]["result_hash"].astext.label("_result_hash"),
        Item.payload["evidence"]["evidence_hash"].astext.label("_evidence_hash"))
        .where(Item.id.in_(expected), Item.kind == "research_check")).all() if expected else []
    evidence = []
    for row in rows:
        item = compact(row)
        _assert_item_access(item["id"], session)
        actual = (item.pop("_result_hash", None), item.pop("_evidence_hash", None))
        if not all(actual) or actual not in expected[item["id"]]: continue
        item["_stored_identity"] = actual
        evidence.append(item)
    instruments(evidence, session)
    return evidence

def context(item, publication, session):
    refs = [ref for ref in (publication or {}).get("references", []) if ref["kind"] == "research_check"]
    if len(refs) > 100: raise ValueError("research_scope_limit: at most 100 direct Checks")
    evidence = check_metadata(refs, session)
    selected = evidence if refs else [item]
    unique = {}
    for row in selected:
        key = (row.get("instrument_id"), row.get("symbol"), row.get("timeframe"), row.get("window_start"), row.get("window_end"))
        unique.setdefault(key, {k: row.get(k) for k in ("id", "instrument_id", "symbol", "instrument", "timeframe", "window_start", "window_end")})
    return {"basis": "referenced_check_metadata" if refs else "record_metadata",
            "complete": all(any(row["id"] == ref["item_id"] and row["_stored_identity"] == (ref.get("result_hash"), ref.get("evidence_hash")) for row in evidence) for ref in refs),
            "items": list(unique.values()), "label_source": "current_instrument_catalog"}

def inventory(items, session):
    """At most 1000 direct latest-publication refs and 64 distinct Checks per page."""
    ids = [row["id"] for row in items if row.get("kind") == "study"]
    if not ids: return
    path = '$.question_contract.publications[last].references[*] ? (@.kind == "research_check")'
    refs = session.execute(select(Item.id.label("study_id"),
        func.jsonb_path_query(Item.payload, path).label("reference")).where(Item.id.in_(ids)).limit(1001)).all()
    by_id = {row.reference["item_id"]: row.reference for row in refs}
    if len(refs) > 1000 or len(by_id) > 64:
        for item in items:
            if item.get("kind") == "study": item["scope_status"] = "Open summary; page scope projection exceeds its bound"
        return
    evidence = {row["id"]: row for row in check_metadata([ref.reference for ref in refs], session)}
    per_study = {}
    for ref in refs:
        row = evidence.get(ref.reference["item_id"])
        per_study.setdefault(ref.study_id, {})
        if row and row["_stored_identity"] == (ref.reference.get("result_hash"), ref.reference.get("evidence_hash")):
            per_study[ref.study_id][row["id"]] = row
    for item in items:
        if item["id"] in per_study:
            item["scope"] = {"basis": "latest_referenced_check_metadata",
                "complete": all(ref.reference["item_id"] in per_study[item["id"]] and per_study[item["id"]][ref.reference["item_id"]]["_stored_identity"] == (ref.reference.get("result_hash"), ref.reference.get("evidence_hash")) for ref in refs if ref.study_id == item["id"]), "items": [
                {key: row.get(key) for key in ("id", "instrument_id", "symbol", "instrument", "timeframe", "window_start", "window_end")}
                for row in per_study[item["id"]].values()]}

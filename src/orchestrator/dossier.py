"""Entity dossier — the 'who is this?' read model for a FEDERATED entity.

`frontier.subject_report` answers 'who is this?' for a crawled *footprint*: it
buckets identity fragments by confidence tier (verified / corroborated / speculative).
That lens is wrong for an entity ingested from an open base (OpenSanctions, EDGAR,
Wikidata): every fact is AUTHORITATIVE_API, so the tiers collapse and the substance —
the ownership / family / director *network* — never surfaces.

This is the complementary read model: given an object, return its identity
properties (multi-source aware) plus its relationships grouped by direction and type,
each endpoint NAMED. It's what the graph view renders as a node's neighborhood, but as
structured data the dossier panel and the brief can consume directly.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.ontology.labels import fetch_label_props, resolve_label

# Identity bookkeeping links (merge plumbing) are not part of the entity's network.
_HIDDEN_LINK_TYPES = ("same_as", "not_same_as")


async def entity_dossier(
    pool: asyncpg.Pool, object_id: uuid.UUID, want_relationships: bool = False,
) -> dict[str, Any]:
    """Identity properties + named relationship network for one entity. Returns {}
    if the object does not exist (the endpoint maps that to 404).

    RECEIPT DIET (context-bloat round 2, Thoth DM 7649): relationships measured at 76%
    of this verb's own bytes/call (decision a065171f, a live cupid specimen returning
    ~100 rows) — unlike orient()'s blind_spots (an aside), this IS the requested
    content, so a bare want_* suppress would leave the default caller with nothing.
    Default collapses to a per-type count plus the first 10 rows (still useful without
    a second call); `want_relationships=True` returns every row, unchanged from before
    this diet.

    Task #97 workstream 3 (ruling 52daab71): both this entity's own `name` and every
    neighbor's name used to check ONLY the `name` property — an entity/neighbor whose
    real identity lives in title/summary/statement/surface/handle (a Practice, a
    BlindSpot, an unclaimed Agent) rendered its raw canonical hash here even though the
    graph/table views of the SAME object already resolved it correctly. Both now share
    `resolve_label`, the one canonical answer every other consumer uses.

    Task #114 (thread 7b258b5f, found by Thoth closing #99): the relationship listing
    used to carry NO `valid_until` filter at all — an INVALIDATED link (unpeer/
    detach_seat's own write) rendered identically to a live one, which is exactly what
    produced a false-urgent reading published as fact (a managed_by edge read as active
    a full day after it was invalidated). derive_role/manager_of_seat (seats.py) already
    filter on `valid_until`; this was the read-side gap sitting beside their write-side
    correctness — same (l.valid_until IS NULL OR l.valid_until > now()) predicate used
    everywhere else in this codebase, applied here for the first time.

    Task #102 (operator's principle, via Thoth's dispatch DM 2279 — "disagreement is just
    more data; it does not have to be collapsed into resolution, it merely has to be
    MARKED AS DISAGREEMENT"): `current_assertions` (alembic 0001/0005) already coexists
    two sources' differing values on the same property — nobody superseded either, so both
    stay current by design. The `properties` query below already read that whole
    multi-source set (verified, not assumed — see
    test_dossier_already_surfaced_both_sides_of_a_contradiction_before_marking); what it
    never did is NAME whether the set it returns agrees or genuinely contradicts. Each
    property entry now carries `agreement`: "single"
    (one source), "agreeing" (multiple sources, same value), or "contradicting" (multiple
    sources, different values) — the three epistemic states a reader could not tell apart
    before. MARK, never resolve: no value is dropped, ranked, or picked as a winner.

    "THE GRAPH KNOWS AND THE DISPLAY LIES" (thread 6212d9f5's 5th sighting, Thoth DM 2746,
    Imhotep's finding on b318a9d3): the OLD top-level `"status"` key here was `obj["status"]`
    — the `objects` table's own LIFECYCLE projection (active/merged/archived/draft, set by
    `Actions.set_status`/`merge_objects`) — a completely different axis from a Thread's
    (or Decision's) semantic `status` PROPERTY (open/resolved/retracted, set by
    `resolve_thread`). Both happen to be named "status", so a caller reading the prominent
    top-level field for "is this open or resolved" got the WRONG, unrelated answer — always
    "active" for any non-merged Thread, regardless of what resolve_thread had actually
    recorded — while the correct, resolvable answer sat one level down in `properties`,
    unresolved, as a multi-source set the caller had to collapse by hand. Not a query bug:
    every other read site in this codebase (winning_props, migration 0015; grounds
    e68d8b46) already resolves this correctly — this was a field NAME collision between two
    genuinely different concepts, not a resolution bug in this function's own SQL.

    Fixed by SPLITTING the collision, not by repurposing the ambiguous key silently: the
    object's own lifecycle now renders as `"object_status"` (unambiguous), and a NEW
    `"status"` key carries the WINNING status-property value — same ordering `winning_props`
    itself uses (confidence DESC, then observed_at DESC) — or `None` when this object type
    carries no `status` assertion at all (most types besides Thread/Decision never do).
    `properties` is UNCHANGED: the full multi-source "contradicting"/"agreeing"/"single" view
    from #102 still shows every source's own word, never collapsed — `status` is a
    COMPLEMENT to that list for a reader who wants the graph's own resolved answer at a
    glance, not a replacement for the epistemic detail."""
    obj = await pool.fetchrow(
        "SELECT id, type, canonical, status FROM objects WHERE id=$1", object_id
    )
    if obj is None:
        return {}

    own_props = (await fetch_label_props(pool, [object_id])).get(object_id, {})
    name = resolve_label(obj["type"], own_props, obj["canonical"]).label

    # properties as the multi-source set: one entry per property name, carrying each
    # source's value + how it was obtained (evidence_class) + confidence. Ordered by
    # confidence DESC, THEN observed_at DESC (winning_props' own tiebreaker, migration
    # 0015) — the FIRST row per name under this exact order is that property's WINNER,
    # captured below into `winners` as we go, with no second query.
    #
    # `name` JOINS THIS SET (task #170's neighbour, the name-property gap, Thoth msg 4292 —
    # Sekhmet's find, decision 7960db40): it is an ordinary assertion, same table, same
    # write path, same coexistence rules as anything else — the old `NOT IN ('name', ...)`
    # exclusion was never structural, it was a UI-dedup call (don't show the top-level
    # `name` field's own fact a second time) made before #102's agreement law existed, and
    # #102 later reused this query without revisiting whether that exclusion should ALSO
    # make disagreement itself unrecoverable. It shouldn't. The top-level `name` field two
    # returns down (via resolve_label/fetch_label_props, #97/ruling 52daab71) answers "what
    # should I display" — a silent winner-pick across a 6-property fallback chain; THIS
    # list answers "do sources disagree," and marks, never resolves, exactly like every
    # other property. Live acceptance test at build time: repo:bytebye (3 distinct values —
    # bytebye/ByeByte/byebyte) and repo:tony (2 — "tony" vs a live, unmarked "cultural-
    # infrastructure" rename) both now read `contradicting` from THIS surface.
    #
    # `tag` stays excluded on its own, separate, still-correct grounds: additive/multi-
    # valued by design (assert_property explicitly allows many simultaneously-true tags
    # per object) — no winner concept and no disagreement concept applies to it the way it
    # does to a single-fact property.
    prop_rows = await pool.fetch(
        "SELECT name, value #>> '{}' AS value, source_id, evidence_class, confidence "
        "FROM current_assertions "
        "WHERE object_id=$1 AND name <> 'tag' "
        "ORDER BY name, confidence DESC NULLS LAST, observed_at DESC",
        object_id,
    )
    properties: dict[str, dict[str, Any]] = {}
    winners: dict[str, str] = {}
    for r in prop_rows:
        winners.setdefault(r["name"], r["value"])
        entry = properties.setdefault(r["name"], {"name": r["name"], "values": []})
        entry["values"].append({
            "value": r["value"],
            "source": r["source_id"],
            "evidence_class": r["evidence_class"],
            "confidence": r["confidence"],
        })
    for entry in properties.values():
        distinct = {v["value"] for v in entry["values"]}
        entry["agreement"] = (
            "single" if len(entry["values"]) == 1 else
            "agreeing" if len(distinct) == 1 else
            "contradicting"
        )

    # relationships, both directions, neighbor labelled and typed. Repeated edges
    # (same direction, type, neighbor) are collapsed: a duplicated link carries no
    # extra information.
    seen: set[tuple[str, str, uuid.UUID]] = set()
    raw_rels: list[dict[str, Any]] = []
    nbr_ids: list[uuid.UUID] = []
    for direction, end, other in (("out", "from_id", "to_id"), ("in", "to_id", "from_id")):
        rows = await pool.fetch(
            f"SELECT l.type, l.{other} AS nbr, l.evidence_class, l.source_id, "
            f"       n.type AS nbr_type, n.canonical AS nbr_canon "
            f"FROM links l JOIN objects n ON n.id=l.{other} "
            f"WHERE l.{end}=$1 AND l.type <> ALL($2::text[]) "
            f"AND (l.valid_until IS NULL OR l.valid_until > now())",
            object_id,
            list(_HIDDEN_LINK_TYPES),
        )
        for r in rows:
            key = (direction, r["type"], r["nbr"])
            if key in seen:
                continue
            seen.add(key)
            nbr_ids.append(r["nbr"])
            raw_rels.append({"direction": direction, "type": r["type"], "nbr": r["nbr"],
                             "nbr_type": r["nbr_type"], "nbr_canon": r["nbr_canon"],
                             "evidence_class": r["evidence_class"], "source": r["source_id"]})

    nbr_props = await fetch_label_props(pool, nbr_ids)
    rels = [
        {
            "direction": r["direction"],
            "type": r["type"],
            "neighbor": {
                "id": str(r["nbr"]),
                "name": resolve_label(r["nbr_type"], nbr_props.get(r["nbr"], {}),
                                      r["nbr_canon"]).label,
                "type": r["nbr_type"],
            },
            "evidence_class": r["evidence_class"],
            "source": r["source"],
        }
        for r in raw_rels
    ]

    out: dict[str, Any] = {
        "id": str(object_id),
        "type": obj["type"],
        "canonical": obj["canonical"],
        "object_status": obj["status"],
        "status": winners.get("status"),
        "name": name,
        "properties": list(properties.values()),
    }
    if want_relationships:
        out["relationships"] = rels
    else:
        by_type: dict[str, int] = {}
        for r in rels:
            by_type[r["type"]] = by_type.get(r["type"], 0) + 1
        out["relationships"] = rels[:10]
        out["relationships_by_type"] = by_type
        out["relationships_total"] = len(rels)
        if len(rels) > 10:
            out["relationships_note"] = (
                f"{len(rels)} relationship(s) across {len(by_type)} type(s); showing the "
                "first 10 — pass want_relationships=True for the full list")
    return out


def _jsonb(value: Any) -> dict[str, Any]:
    """asyncpg hands back jsonb as a dict when the pool's own codec is registered, a
    raw JSON string otherwise (thread 8542ee89's own lesson) — accept either, same
    defensive check monitor.py's own event reader already uses for this exact
    object_events.payload column."""
    if isinstance(value, str):
        import json
        return dict(json.loads(value)) if value else {}
    return dict(value) if value else {}


async def object_events(
    pool: asyncpg.Pool, object_id: uuid.UUID, event_type: str | None = None,
) -> dict[str, Any]:
    """Read-only witness surface for one object: every object_events row that
    touches it, plus every same_as/not_same_as link naming it, plus its own current
    status/merged_into projection. Filed per thread 085039cc (Thoth DM 2469):
    dossier() deliberately treats same_as/not_same_as as identity bookkeeping, not
    the entity's own network (`_HIDDEN_LINK_TYPES` above), and describe() is
    schema-only — neither could show a caller a LIVE merge/unmerge witness for a real
    object, which is exactly what blocked independently verifying two production
    folds' own reversibility (Ruling 4 half (b): "not because they don't exist, but
    because nothing in the standing read surface can show a live instance of one").

    A merge event's own subject/related columns are ASYMMETRIC with an unmerge's
    (merge stores object_id=winner/related_id=loser; unmerge stores object_id=loser/
    related_id=winner, matching Actions.merge_objects/unmerge_objects exactly) — so
    "everything that ever happened to this object" means checking BOTH columns, never
    object_id alone. Returns {} if the object does not exist, the same 404-mapping
    convention entity_dossier already uses."""
    obj = await pool.fetchrow(
        "SELECT id, canonical, status, merged_into FROM objects WHERE id=$1", object_id)
    if obj is None:
        return {}

    ev_query = (
        "SELECT e.id, e.event_type, e.object_id, e.related_id, e.payload, e.actor, "
        "e.case_id, e.created_at, os.canonical AS object_canonical, "
        "rs.canonical AS related_canonical "
        "FROM object_events e "
        "LEFT JOIN objects os ON os.id = e.object_id "
        "LEFT JOIN objects rs ON rs.id = e.related_id "
        "WHERE (e.object_id = $1 OR e.related_id = $1)"
    )
    params: list[Any] = [object_id]
    if event_type is not None:
        params.append(event_type)
        ev_query += f" AND e.event_type = ${len(params)}"
    ev_query += " ORDER BY e.created_at ASC"
    event_rows = await pool.fetch(ev_query, *params)

    link_rows = await pool.fetch(
        "SELECT l.id, l.from_id, l.to_id, l.type, l.properties, l.source_id, "
        "l.confidence, l.valid_until, l.created_at, "
        "f.canonical AS from_canonical, t.canonical AS to_canonical "
        "FROM links l "
        "LEFT JOIN objects f ON f.id = l.from_id "
        "LEFT JOIN objects t ON t.id = l.to_id "
        "WHERE (l.from_id = $1 OR l.to_id = $1) AND l.type = ANY($2::text[]) "
        "ORDER BY l.created_at ASC",
        object_id, list(_HIDDEN_LINK_TYPES))

    return {
        "object_id": str(object_id),
        "canonical": obj["canonical"],
        "object_status": obj["status"],
        "merged_into": str(obj["merged_into"]) if obj["merged_into"] else None,
        "events": [
            {
                "id": r["id"],
                "event_type": r["event_type"],
                "object_id": str(r["object_id"]),
                "object_canonical": r["object_canonical"],
                "related_id": str(r["related_id"]) if r["related_id"] else None,
                "related_canonical": r["related_canonical"],
                "payload": _jsonb(r["payload"]),
                "actor": r["actor"],
                "case_id": str(r["case_id"]) if r["case_id"] else None,
                "created_at": r["created_at"].isoformat(),
            }
            for r in event_rows
        ],
        "same_as_links": [
            {
                "id": r["id"],
                "type": r["type"],
                "from_id": str(r["from_id"]),
                "from_canonical": r["from_canonical"],
                "to_id": str(r["to_id"]),
                "to_canonical": r["to_canonical"],
                "properties": _jsonb(r["properties"]),
                "source_id": r["source_id"],
                "confidence": r["confidence"],
                "valid_until": r["valid_until"].isoformat() if r["valid_until"] else None,
                "created_at": r["created_at"].isoformat(),
            }
            for r in link_rows
        ],
    }

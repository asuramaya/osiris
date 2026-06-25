"""Enrich the graph's Wikidata-keyed stubs from the live Wikidata API (keyless).

When we ingest OpenSanctions (FollowTheMoney), relationship endpoints whose real
entity lives outside the slice are left as **typed stubs** keyed by their Wikidata
id (``Q...``) — an ownership/family/director edge that points at a node with no
name. Wikidata is keyed by those exact ids, so this module *enriches the stubs in
place*: it reads the ids already in the graph, fetches their labels/descriptions/
literal facts (and, optionally, their relationship claims) from ``wbgetentities``,
and writes them back as AUTHORITATIVE_API assertions on the existing object id —
the same find-or-create-or-stub pattern as the FtM loader, so it composes in layers
(each pass names the current frontier of stubs and reveals the next ring of edges).

    uv run python -m src.ingest.wikidata enrich [limit]   # name the existing stubs
    uv run python -m src.ingest.wikidata Q42 Q9061         # ingest specific ids
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "wikidata"
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_EC = EvidenceClass.AUTHORITATIVE_API.value
_API = "https://www.wikidata.org/w/api.php"
# Wikidata asks for a descriptive contact UA (same etiquette as SEC EDGAR).
_UA = {"User-Agent": "osiris-osint operator@example.com"}
_BATCH = 50  # wbgetentities caps ids at 50 per request

_HUMAN = "Q5"  # P31 -> "instance of human"

# Wikidata claim property -> (assertion name) for LITERAL (string/time/url) values.
_LITERAL_PROPS: dict[str, str] = {
    "P569": "birthDate",
    "P570": "deathDate",
    "P856": "website",
    "P968": "email",
    "P1813": "shortName",
}
# Wikidata claim property -> (link type, inferred endpoint type) for ENTITY-valued
# (Q-id) relationship claims. Mirrors the FtM edge map: an absent endpoint becomes a
# typed stub a later same-id pass enriches in place.
_REL_PROPS: dict[str, tuple[str, str]] = {
    "P22": ("family", "Person"),          # father
    "P25": ("family", "Person"),          # mother
    "P40": ("family", "Person"),          # child
    "P3373": ("sibling", "Person"),       # sibling
    "P26": ("spouse", "Person"),          # spouse
    "P1830": ("owns", "Organization"),    # owner of
    "P127": ("owned_by", "Organization"), # owned by
    "P749": ("parent_org", "Organization"),  # parent organization
    "P112": ("founded_by", "Person"),     # founded by
    "P169": ("ceo", "Person"),            # chief executive officer
    "P488": ("chairperson", "Person"),    # chairperson
    "P102": ("member_of", "Organization"),  # member of political party
}


# --- value extraction from the wbgetentities shape ----------------------------

def _label(ent: dict[str, Any]) -> str | None:
    labels = ent.get("labels") or {}
    # prefer en, then 'mul' (Wikidata's language-agnostic label — many transliterated
    # PEP names live ONLY here), then any returned language.
    for key in ("en", "mul"):
        v = labels.get(key)
        if isinstance(v, dict) and v.get("value"):
            return str(v["value"])
    for v in labels.values():
        if isinstance(v, dict) and v.get("value"):
            return str(v["value"])
    return None


def _description(ent: dict[str, Any]) -> str | None:
    en = (ent.get("descriptions") or {}).get("en")
    return str(en["value"]) if isinstance(en, dict) and en.get("value") else None


def _literal_value(mainsnak: dict[str, Any]) -> str | None:
    if mainsnak.get("snaktype") != "value":
        return None
    dv = mainsnak.get("datavalue") or {}
    t, v = dv.get("type"), dv.get("value")
    if t == "string":
        return str(v) if v else None
    if t == "time" and isinstance(v, dict):
        return (str(v.get("time", "")).lstrip("+").split("T")[0]) or None
    if t == "monolingualtext" and isinstance(v, dict):
        return str(v.get("text")) if v.get("text") else None
    return None


def _entity_id(mainsnak: dict[str, Any]) -> str | None:
    if mainsnak.get("snaktype") != "value":
        return None
    dv = mainsnak.get("datavalue") or {}
    if dv.get("type") != "wikibase-entityid":
        return None
    qid = (dv.get("value") or {}).get("id")
    return str(qid) if qid else None


def _entity_type(ent: dict[str, Any]) -> str:
    """Infer Osiris object type from P31 (instance of). Human -> Person; anything
    else with a P31 -> Organization; default Person (only used when creating a NEW
    object — enrichment respects the existing stub's type)."""
    qids = {
        _entity_id(s.get("mainsnak", {}))
        for s in (ent.get("claims") or {}).get("P31", [])
    }
    if _HUMAN in qids:
        return "Person"
    return "Organization" if qids else "Person"


def parse_entities(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Pull the entity map out of a wbgetentities response, dropping ids the API
    reports as missing/redirected (they carry a "missing" marker, no claims)."""
    ents = (data or {}).get("entities") or {}
    return {
        qid: e
        for qid, e in ents.items()
        if isinstance(e, dict) and "missing" not in e
    }


# --- ingest -------------------------------------------------------------------

async def ingest_entities(
    actions: Actions,
    entities: dict[str, dict[str, Any]],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
    relationships: bool = False,
) -> dict[str, int]:
    """Enrich each entity's existing object (or create it) with labels + literal
    facts; optionally form relationship links to other entities (stubbing absent
    endpoints). Idempotent: re-running supersedes the prior wikidata assertion."""
    ts = observed_at or datetime.now(UTC)
    cache: dict[str, uuid.UUID] = {}
    enriched = n_prop = n_link = 0

    async def resolve(qid: str, default_type: str) -> uuid.UUID:
        """Find the object for a Q-id by canonical (any type, so we enrich the
        existing stub in place), else create it with the given type."""
        if qid in cache:
            return cache[qid]
        row = await actions.pool.fetchrow(
            "SELECT id FROM objects WHERE canonical=$1 ORDER BY created_at LIMIT 1", qid
        )
        oid = (
            row["id"]
            if row is not None
            else await actions.create_or_find_object(default_type, qid, _SOURCE, case_id)
        )
        cache[qid] = oid
        return oid

    # pass 1: properties (the headline — bare stubs get a name)
    for qid, ent in entities.items():
        oid = await resolve(qid, _entity_type(ent))
        wrote = False

        async def put(name: str, value: Any, _oid: uuid.UUID = oid) -> None:
            nonlocal n_prop
            await actions.assert_property(
                _oid, name, value, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )
            n_prop += 1

        if (name := _label(ent)) is not None:
            await put("name", name)
            wrote = True
        if (desc := _description(ent)) is not None:
            await put("description", desc)
            wrote = True
        for pid, pname in _LITERAL_PROPS.items():
            for st in (ent.get("claims") or {}).get(pid, []):
                if (val := _literal_value(st.get("mainsnak", {}))) is not None:
                    await put(pname, val)
                    wrote = True
                    break  # scalar: first asserted value wins
        if wrote:
            enriched += 1

    # pass 2: relationship claims -> links (absent endpoints become typed stubs)
    if relationships:
        for qid, ent in entities.items():
            a = cache.get(qid)
            if a is None:
                continue
            for pid, (ltype, rtype) in _REL_PROPS.items():
                for st in (ent.get("claims") or {}).get(pid, []):
                    tq = _entity_id(st.get("mainsnak", {}))
                    if not tq:
                        continue
                    b = await resolve(tq, rtype)
                    if b == a:
                        continue
                    await actions.create_link(
                        a, b, ltype, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                    )
                    n_link += 1

    endpoints = len([q for q in cache if q not in entities])
    return {"enriched": enriched, "properties": n_prop, "links": n_link, "endpoints": endpoints}


# --- fetch + stub selection ---------------------------------------------------

async def fetch_entities(
    ids: list[str],
    *,
    languages: str = "en|mul",
    props: str = "labels|descriptions|claims",
    timeout_s: float = 30.0,
) -> dict[str, dict[str, Any]]:
    """Fetch entities from wbgetentities in batches of 50, merging the results."""
    out: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        for i in range(0, len(ids), _BATCH):
            batch = ids[i : i + _BATCH]
            r = await client.get(
                _API,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": props,
                    "languages": languages,
                    "format": "json",
                },
                headers=_UA,
            )
            r.raise_for_status()
            out.update(parse_entities(r.json()))
    return out


async def select_stub_qids(pool: Any, *, limit: int | None = None) -> list[str]:
    """Q-ids already in the graph with no name — the un-enriched stubs."""
    q = (
        "SELECT o.canonical FROM objects o "
        "WHERE o.canonical ~ '^Q[0-9]+$' "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions a "
        "                  WHERE a.object_id=o.id AND a.name='name') "
        "ORDER BY o.canonical"
    )
    if limit is not None:
        q += f" LIMIT {int(limit)}"
    rows = await pool.fetch(q)
    return [r["canonical"] for r in rows]


async def enrich_stubs(
    actions: Actions,
    *,
    limit: int | None = None,
    relationships: bool = False,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Select the graph's un-named Wikidata stubs and enrich them in place."""
    qids = await select_stub_qids(actions.pool, limit=limit)
    if not qids:
        return {"selected": 0, "enriched": 0, "properties": 0, "links": 0, "endpoints": 0}
    entities = await fetch_entities(qids)
    counts = await ingest_entities(
        actions, entities, relationships=relationships, observed_at=observed_at
    )
    return {"selected": len(qids), **counts}


def _looks_like_qid(s: str) -> bool:
    return len(s) > 1 and s[0] == "Q" and s[1:].isdigit()


def main() -> None:
    argv = sys.argv[1:]
    rel = "--rel" in argv
    argv = [a for a in argv if a != "--rel"]

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        actions = Actions(pool)
        try:
            if argv and argv[0] == "enrich":
                limit = int(argv[1]) if len(argv) > 1 else None
                counts = await enrich_stubs(actions, limit=limit, relationships=rel)
            elif argv and all(_looks_like_qid(a) for a in argv):
                ents = await fetch_entities(argv)
                counts = await ingest_entities(actions, ents, relationships=rel)
            else:
                print("usage: python -m src.ingest.wikidata enrich [limit] [--rel]")
                print("       python -m src.ingest.wikidata Q42 Q9061 [--rel]")
                return
            print(f"ingested: {counts}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()

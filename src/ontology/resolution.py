"""Probabilistic entity resolution (DESIGN §10.2/§10.3, §11 behavioral merge).

Never auto-merges (ruling #3): generates `merge_candidates` for the review tray.
Person pairs are scored on shared strong identifiers / name+DOB / name+employer+
city; IntrusionSet pairs on shared TTPs (the §11 convergence). A confirmed
candidate runs merge_objects; a rejected one writes a permanent `not_same_as`
link — negative memory that suppresses the pair from ever being re-proposed.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import ActionError, Actions
from src.ontology.canonicalize import canonicalize
from src.ontology.entity_type import clean_entity_name, is_organization
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

Pair = tuple[uuid.UUID, uuid.UUID]

# Legal-form suffixes stripped before cross-base org name matching: "Tesla, Inc."
# (EDGAR) and "Tesla" (Wikidata) are the same company — the names differ only by the
# corporate form. Multilingual because the open bases are.
_LEGAL_SUFFIXES = frozenset({
    "inc", "incorporated", "corp", "corporation", "co", "company", "llc", "llp", "lp",
    "ltd", "limited", "plc", "sa", "sas", "sl", "ag", "gmbh", "kg", "srl", "spa", "nv",
    "bv", "ab", "oy", "as", "kk", "pty", "pte", "se", "oao", "ooo", "pao", "zao", "pjsc",
    "ojsc", "jsc", "ao", "rt", "kft", "doo", "dd", "tov", "sro", "the",
})


def normalize_org_name(name: str) -> str:
    """Lowercase, drop punctuation and legal-form suffix tokens — the canonical key
    for matching the same organization across bases."""
    cleaned = re.sub(r"[^\w\s]", " ", name.lower())
    toks = [t for t in cleaned.split() if t and t not in _LEGAL_SUFFIXES]
    return " ".join(toks)


class ResolutionError(Exception):
    pass


async def _suppressed(pool: asyncpg.Pool) -> set[frozenset[uuid.UUID]]:
    """Pairs we must never re-propose: explicit not_same_as, or already rejected."""
    out: set[frozenset[uuid.UUID]] = set()
    for r in await pool.fetch("SELECT from_id, to_id FROM links WHERE type='not_same_as'"):
        out.add(frozenset((r["from_id"], r["to_id"])))
    for r in await pool.fetch("SELECT a_id, b_id FROM merge_candidates WHERE resolved='rejected'"):
        out.add(frozenset((r["a_id"], r["b_id"])))
    return out


async def _insert(
    pool: asyncpg.Pool, a: uuid.UUID, b: uuid.UUID, score: float, reasons: list[str]
) -> bool:
    row = await pool.fetchrow(
        "INSERT INTO merge_candidates (a_id, b_id, score, reasons) VALUES ($1,$2,$3,$4) "
        "ON CONFLICT (a_id, b_id) DO NOTHING RETURNING id",
        a,
        b,
        score,
        {"signals": reasons},
    )
    return row is not None


def _name_variant(a: str, b: str) -> bool:
    """Two names are a likely same-person variant iff they share a surname (last token)
    and one first name is a prefix or single-initial of the other ('Alex'/'Alexander',
    'J.'/'John') — but are NOT identical (exact matches are handled by stronger rules)."""
    ta, tb = a.lower().split(), b.lower().split()
    if len(ta) < 2 or len(tb) < 2 or ta[-1] != tb[-1]:
        return False
    fa, fb = ta[0].rstrip("."), tb[0].rstrip(".")
    if not fa or not fb or fa == fb:
        return False
    return fa.startswith(fb) or fb.startswith(fa)


async def find_person_merge_candidates(pool: asyncpg.Pool) -> int:
    """Queue Person merge candidates from shared identifiers / name+DOB /
    name+employer+city. Returns the number of new candidates queued."""
    scored: dict[Pair, tuple[float, list[str]]] = {}

    def add(a: uuid.UUID, b: uuid.UUID, score: float, reason: str) -> None:
        key = (a, b)
        prev = scored.get(key)
        if prev is None or score > prev[0]:
            reasons = (prev[1] if prev else []) + [reason]
            scored[key] = (max(score, prev[0] if prev else 0.0), reasons)

    # shared strong identifier (email / phone) on two Person objects
    for r in await pool.fetch(
        "SELECT a.object_id AS x, b.object_id AS y, a.name AS sig "
        "FROM current_assertions a "
        "JOIN current_assertions b ON a.name=b.name AND a.value=b.value "
        "  AND a.object_id < b.object_id "
        "JOIN objects oa ON oa.id=a.object_id AND oa.type='Person' "
        "JOIN objects ob ON ob.id=b.object_id AND ob.type='Person' "
        "WHERE a.name IN ('email','phone')"
    ):
        add(r["x"], r["y"], 0.9, f"shared {r['sig']}")

    # exact name + exact DOB
    for r in await pool.fetch(
        "SELECT na.object_id AS x, nb.object_id AS y "
        "FROM current_assertions na "
        "JOIN current_assertions nb ON na.name='name' AND nb.name='name' "
        "  AND na.value=nb.value AND na.object_id < nb.object_id "
        "JOIN current_assertions da ON da.object_id=na.object_id AND da.name='dob' "
        "JOIN current_assertions db ON db.object_id=nb.object_id AND db.name='dob' "
        "  AND db.value=da.value "
        "JOIN objects oa ON oa.id=na.object_id AND oa.type='Person' "
        "JOIN objects ob ON ob.id=nb.object_id AND ob.type='Person'"
    ):
        add(r["x"], r["y"], 0.85, "name+dob")

    # exact name + same employer + same city
    for r in await pool.fetch(
        "SELECT na.object_id AS x, nb.object_id AS y "
        "FROM current_assertions na "
        "JOIN current_assertions nb ON na.name='name' AND nb.name='name' "
        "  AND na.value=nb.value AND na.object_id < nb.object_id "
        "JOIN current_assertions ea ON ea.object_id=na.object_id AND ea.name='employer' "
        "JOIN current_assertions eb ON eb.object_id=nb.object_id AND eb.name='employer' "
        "  AND eb.value=ea.value "
        "JOIN current_assertions ca ON ca.object_id=na.object_id AND ca.name='city' "
        "JOIN current_assertions cb ON cb.object_id=nb.object_id AND cb.name='city' "
        "  AND cb.value=ca.value "
        "JOIN objects oa ON oa.id=na.object_id AND oa.type='Person' "
        "JOIN objects ob ON ob.id=nb.object_id AND ob.type='Person'"
    ):
        add(r["x"], r["y"], 0.8, "name+employer+city")

    # two people behind the SAME company (resolved to its merge winner, so links on a
    # pre-merge company node still group) whose names are identical (0.7) or a compatible
    # variant — a nickname/initial of the other, 'Alex'/'Alexander' Mashinsky (0.55).
    # Exact-name rules elsewhere need a DOB/city; this uses the shared employer link.
    # Weak + review-gated (never auto-merges a Person, #3).
    for r in await pool.fetch(
        "WITH RECURSIVE cwalk AS ("
        "  SELECT DISTINCT l.from_id AS raw, l.from_id AS cur FROM links l "
        "    WHERE l.type IN ('officer','director','founded_by','manager') "
        "  UNION ALL "
        "  SELECT c.raw, o.merged_into FROM cwalk c JOIN objects o ON o.id=c.cur "
        "    WHERE o.merged_into IS NOT NULL"
        "), cwin AS ("
        "  SELECT raw, cur AS company FROM cwalk c "
        "  WHERE NOT EXISTS "
        "    (SELECT 1 FROM objects o WHERE o.id=c.cur AND o.merged_into IS NOT NULL)"
        "), plinks AS ("
        "  SELECT cwin.company, l.to_id AS person, "
        "    (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=l.to_id "
        "     AND a.name='name' ORDER BY confidence DESC LIMIT 1) AS nm "
        "  FROM links l JOIN cwin ON cwin.raw=l.from_id "
        "  JOIN objects p ON p.id=l.to_id AND p.type='Person' AND p.status='active' "
        "  WHERE l.type IN ('officer','director','founded_by','manager')"
        ") "
        "SELECT DISTINCT a.person AS x, b.person AS y, a.nm AS xn, b.nm AS yn "
        "FROM plinks a JOIN plinks b ON a.company=b.company AND a.person < b.person"
    ):
        xn, yn = r["xn"], r["yn"]
        if not xn or not yn:
            continue
        if xn.strip().lower() == yn.strip().lower():
            add(r["x"], r["y"], 0.7, f"same org + same name: {xn}")
        elif _name_variant(xn, yn):
            add(r["x"], r["y"], 0.55, f"same org + name variant: {xn} / {yn}")

    suppressed = await _suppressed(pool)
    queued = 0
    for (a, b), (score, reasons) in scored.items():
        if frozenset((a, b)) in suppressed:
            continue
        if await _insert(pool, a, b, score, reasons):
            queued += 1
    return queued


async def find_behavioral_merge_candidates(
    pool: asyncpg.Pool, *, min_techniques: int = 3, min_tools: int = 2
) -> int:
    """Queue IntrusionSet candidates that share enough TTPs (DESIGN §11) — how
    disparate cases converge on the same actor."""
    rows = await pool.fetch(
        "SELECT la.from_id AS x, lb.from_id AS y, t.type AS ttype, count(*) AS n "
        "FROM links la "
        "JOIN links lb ON la.to_id=lb.to_id AND la.type='uses' AND lb.type='uses' "
        "  AND la.from_id < lb.from_id "
        "JOIN objects oa ON oa.id=la.from_id AND oa.type='IntrusionSet' "
        "JOIN objects ob ON ob.id=lb.from_id AND ob.type='IntrusionSet' "
        "JOIN objects t ON t.id=la.to_id "
        "GROUP BY la.from_id, lb.from_id, t.type"
    )
    shared: dict[Pair, dict[str, int]] = {}
    for r in rows:
        shared.setdefault((r["x"], r["y"]), {})[r["ttype"]] = r["n"]

    suppressed = await _suppressed(pool)
    queued = 0
    for (a, b), counts in shared.items():
        techniques = counts.get("AttackPattern", 0)
        tools = counts.get("Tool", 0) + counts.get("Malware", 0)
        if techniques < min_techniques or tools < min_tools:
            continue
        if frozenset((a, b)) in suppressed:
            continue
        score = min(0.95, 0.5 + 0.1 * techniques + 0.1 * tools)
        reasons = [f"{techniques} shared techniques", f"{tools} shared tools"]
        if await _insert(pool, a, b, score, reasons):
            queued += 1
    return queued


async def find_footprint_merge_candidates(pool: asyncpg.Pool) -> int:
    """Queue Account↔Account candidates from footprint signals (never auto-merges):
      * same handle on different platforms  -> 0.6 (weak; same name, maybe same person)
      * two Accounts that are both rel=me targets of one page -> 0.9 (strong identity)
    Returns the number of new candidates queued."""
    scored: dict[Pair, tuple[float, list[str]]] = {}

    def add(a: uuid.UUID, b: uuid.UUID, score: float, reason: str) -> None:
        key = (a, b) if a < b else (b, a)
        prev = scored.get(key)
        if prev is None or score > prev[0]:
            scored[key] = (score, ((prev[1] if prev else []) + [reason]))

    # same handle, different platform
    for r in await pool.fetch(
        "SELECT a.object_id AS x, b.object_id AS y, a.value #>> '{}' AS handle "
        "FROM current_assertions a "
        "JOIN current_assertions b ON a.name='handle' AND b.name='handle' "
        "  AND a.value=b.value AND a.object_id < b.object_id "
        "JOIN current_assertions pa ON pa.object_id=a.object_id AND pa.name='platform' "
        "JOIN current_assertions pb ON pb.object_id=b.object_id AND pb.name='platform' "
        "  AND pb.value <> pa.value "
        "JOIN objects oa ON oa.id=a.object_id AND oa.type='Account' "
        "JOIN objects ob ON ob.id=b.object_id AND ob.type='Account'"
    ):
        add(r["x"], r["y"], 0.6, f"shared handle '{r['handle']}'")

    # two Accounts both rel=me-linked from the same page -> same identity
    for r in await pool.fetch(
        "SELECT l1.to_id AS x, l2.to_id AS y "
        "FROM links l1 "
        "JOIN links l2 ON l1.from_id=l2.from_id AND l1.type='rel_me' AND l2.type='rel_me' "
        "  AND l1.to_id < l2.to_id "
        "JOIN objects oa ON oa.id=l1.to_id AND oa.type='Account' "
        "JOIN objects ob ON ob.id=l2.to_id AND ob.type='Account'"
    ):
        add(r["x"], r["y"], 0.9, "rel=me identity link")

    suppressed = await _suppressed(pool)
    queued = 0
    for (a, b), (score, reasons) in scored.items():
        if frozenset((a, b)) in suppressed:
            continue
        if await _insert(pool, a, b, score, reasons):
            queued += 1
    return queued


async def ensure_person_hub(
    actions: Actions,
    *,
    key: str,
    account_ids: list[uuid.UUID],
    email_value: str | None = None,
    email_id: uuid.UUID | None = None,
    case_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Idempotently assemble discovered identity-fragments under a Person hub
    (`cluster:<key>`), linking has_account / has_email. Hubs are formed only from
    STRONG signals (bio-email match, rel=me) — never speculatively per account, and
    never by merging two Persons (ruling #3). The hub carries the anchoring email as
    a property so find_person_merge_candidates can later relate it to other Persons."""
    now = datetime.now(UTC)
    person_id = await actions.create_or_find_object(
        "Person", f"cluster:{key}", "convergence", case_id
    )

    async def _link_once(to_id: uuid.UUID, type_: str) -> None:
        exists = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1",
            person_id, to_id, type_,
        )
        if not exists:
            await actions.create_link(person_id, to_id, type_, "convergence", now, 0.8,
                                      case_id=case_id)

    for aid in account_ids:
        await _link_once(aid, "has_account")
    if email_id is not None:
        await _link_once(email_id, "has_email")
    if email_value is not None:
        # within-source supersession keeps this to one current assertion (no spam)
        await actions.assert_property(person_id, "email", email_value, "convergence", now, 0.8,
                                      case_id=case_id)
    return person_id


async def resolve_candidate(
    actions: Actions, candidate_id: int, decision: str, actor: str
) -> None:
    """Apply an analyst's tray decision. 'merged' -> merge_objects (a wins);
    'rejected' -> permanent not_same_as (negative memory)."""
    row = await actions.pool.fetchrow(
        "SELECT a_id, b_id, resolved FROM merge_candidates WHERE id=$1", candidate_id
    )
    if row is None:
        raise ResolutionError(f"candidate {candidate_id} not found")
    if row["resolved"] is not None:
        raise ResolutionError(f"candidate {candidate_id} already {row['resolved']}")
    a_id, b_id = row["a_id"], row["b_id"]

    if decision == "merged":
        await actions.merge_objects(a_id, b_id, "ER candidate confirmed", actor)
    elif decision == "rejected":
        now = datetime.now(UTC)
        await actions.create_link(a_id, b_id, "not_same_as", actor, now, 1.0)
        await actions.create_link(b_id, a_id, "not_same_as", actor, now, 1.0)
    else:
        raise ResolutionError(f"decision must be 'merged' or 'rejected', got {decision!r}")

    await actions.pool.execute(
        "UPDATE merge_candidates SET resolved=$2, resolved_by=$3, resolved_at=now() WHERE id=$1",
        candidate_id,
        decision,
        actor,
    )


def _winner_priority(canonical: str) -> int:
    """Prefer the most authoritatively-keyed canonical as the merge winner: a SEC CIK
    over a Wikidata Q-id over a trials-registry slug over a derived company:<name>."""
    if canonical.startswith("cik:"):
        return 3
    if re.fullmatch(r"Q\d+", canonical):
        return 2
    if canonical.startswith("ctgov-org:"):
        return 1
    return 0


async def resolve_cross_base(actions: Actions, *, min_len: int = 5) -> int:
    """Promote cross-base candidates to actual merges where it is SAFE: the same
    organization is fragmented across bases (company:/cik:/Qxxx) until merged, which
    makes analytics self-reference. We merge a connected component of cross-base
    candidates only when every member shares one distinctive normalized name (>= min_len,
    so 'tesla'/'neuralink' qualify but short namesakes don't) — corroboration across
    bases, not a name guess. Organizations only (#3); event-sourced, so reversible."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT id, a_id, b_id FROM merge_candidates "
        "WHERE reasons->>'signals' LIKE '%cross-base%' AND resolved IS NULL"
    )
    parent: dict[uuid.UUID, uuid.UUID] = {}

    def find(x: uuid.UUID) -> uuid.UUID:
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        parent[x] = root
        return root

    for r in rows:
        parent[find(r["a_id"])] = find(r["b_id"])

    components: dict[uuid.UUID, set[uuid.UUID]] = {}
    cand_ids: dict[uuid.UUID, list[int]] = {}
    for r in rows:
        root = find(r["a_id"])
        components.setdefault(root, set()).update((r["a_id"], r["b_id"]))
        cand_ids.setdefault(root, []).append(r["id"])

    merged = 0
    for root, members in components.items():
        info = await pool.fetch(
            "SELECT o.id, o.canonical, o.status, "
            "  (SELECT value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS nm, "
            "  (SELECT count(*) FROM current_assertions a WHERE a.object_id=o.id) AS na "
            "FROM objects o WHERE o.id = ANY($1::uuid[])",
            list(members),
        )
        active = [i for i in info if i["status"] == "active"]
        norms = {normalize_org_name(i["nm"] or "") for i in active if i["nm"]}
        if len(norms) != 1:
            continue  # mixed names in the component -> not obviously one entity; skip
        norm = next(iter(norms))
        if len(norm) < min_len:
            continue  # too short/common to merge on name corroboration alone
        winner = max(active, key=lambda i: (_winner_priority(i["canonical"]), i["na"]))
        for loser in active:
            if loser["id"] == winner["id"]:
                continue
            try:
                await actions.merge_objects(
                    winner["id"], loser["id"], f"cross-base same-entity: {norm}", "cross-base"
                )
                merged += 1
            except ActionError:
                pass
        await pool.execute(
            "UPDATE merge_candidates SET resolved='merged', resolved_by='cross-base', "
            "resolved_at=now() WHERE id = ANY($1::int[])",
            cand_ids[root],
        )
    return merged


async def consolidate_companies(actions: Actions) -> int:
    """Collapse the noisy 'company:<name>' nodes the SPV-name parser produces. A feeder
    titled 'Crusoe Green Meadow …' and one titled 'Crusoe …' are the same target seen
    through different SPV labels; when one node's name tokens are a strict PREFIX of
    another's, the longer is a label-decorated variant — merge it into the shorter
    (event-sourced, reversible). Conservative: prefix-only, never fuses unrelated names
    that merely share a first word of differing tails."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, a.value #>> '{}' AS nm FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='name' "
        "WHERE o.type='Organization' AND o.status='active' AND o.canonical LIKE 'company:%'"
    )
    nodes = [(r["id"], tuple(normalize_org_name(r["nm"] or "").split())) for r in rows]
    nodes = [(i, toks) for i, toks in nodes if toks]
    by_tokens = {toks: i for i, toks in nodes}  # exact-name -> id (1 per canonical)
    merged = 0
    for loser_id, toks in sorted(nodes, key=lambda n: len(n[1]), reverse=True):
        # find the shortest existing node that is a strict token-prefix of this one
        for k in range(1, len(toks)):
            winner_id = by_tokens.get(toks[:k])
            if winner_id is not None and winner_id != loser_id:
                try:
                    await actions.merge_objects(
                        winner_id, loser_id, "company name prefix-variant", "consolidate"
                    )
                    merged += 1
                except ActionError:  # already merged in a prior step / chain — skip
                    pass
                break
    return merged


async def review_tray(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Unresolved merge candidates, highest score first."""
    return [
        dict(r)
        for r in await pool.fetch(
            "SELECT id, a_id, b_id, score, reasons FROM merge_candidates "
            "WHERE resolved IS NULL ORDER BY score DESC, id"
        )
    ]


async def find_sanctions_candidates(pool: asyncpg.Pool) -> int:
    """Screen the crawled footprint against ingested watchlist entities
    (OpenSanctions et al.) — where the two halves of the kernel meet: the autonomous
    crawl and the federated open base resolve against each other. Two signals, scored
    and merged per pair:

      * a SHARED STRONG IDENTIFIER (email / website / phone) — near-certainly the same
        entity (0.9);
      * a NAME match — now ALIAS-AWARE, over the watchlist entity's {name ∪ alias} set
        — a weak namesake signal (0.5) with a length guard against trivial collisions.

    A 'watchlist entity' is any object carrying an opensanctions assertion (so an
    identifier enriched from another source, e.g. wikidata, on that same object still
    counts). Never auto-flags (ruling #3): the analyst adjudicates same-entity vs
    namesake in the tray."""
    scored: dict[Pair, tuple[float, list[str]]] = {}

    def add(crawled: uuid.UUID, sanctioned: uuid.UUID, score: float, reason: str) -> None:
        key = (min(crawled, sanctioned), max(crawled, sanctioned))
        prev = scored.get(key)
        if prev is None:
            scored[key] = (score, [reason])
        else:
            prev[1].append(reason)
            if score > prev[0]:
                scored[key] = (score, prev[1])

    # (1) shared strong identifier — watchlist identifiers from ANY source
    for r in await pool.fetch(
        "WITH watchlist AS "
        "  (SELECT DISTINCT object_id FROM assertions WHERE source_id='opensanctions') "
        "SELECT c.object_id AS crawled, s.object_id AS sanctioned, s.name AS sig, "
        "       lower(btrim(c.value #>> '{}')) AS val "
        "FROM current_assertions s "
        "JOIN watchlist w ON w.object_id=s.object_id "
        "JOIN current_assertions c "
        "  ON c.name=s.name "
        "  AND lower(btrim(c.value #>> '{}')) = lower(btrim(s.value #>> '{}')) "
        "WHERE s.name IN ('email','website','phone') "
        "  AND c.object_id <> s.object_id "
        "  AND c.object_id NOT IN (SELECT object_id FROM watchlist) "
        "  AND length(btrim(c.value #>> '{}')) >= 4"
    ):
        add(r["crawled"], r["sanctioned"], 0.9, f"shared {r['sig']}: {r['val']}")

    # (2) alias-aware name match over the watchlist entity's {name ∪ alias} set
    for r in await pool.fetch(
        "WITH watchlist AS "
        "  (SELECT DISTINCT object_id FROM assertions WHERE source_id='opensanctions'), "
        "sanc_names AS ("
        "  SELECT s.object_id, lower(btrim(s.value #>> '{}')) AS nm "
        "  FROM current_assertions s JOIN watchlist w ON w.object_id=s.object_id "
        "  WHERE s.name='name' "
        "  UNION "
        "  SELECT s.object_id, lower(btrim(al.v)) "
        "  FROM current_assertions s JOIN watchlist w ON w.object_id=s.object_id "
        "  CROSS JOIN LATERAL jsonb_array_elements_text(s.value) AS al(v) "
        "  WHERE s.name='alias' AND jsonb_typeof(s.value)='array') "
        "SELECT c.object_id AS crawled, sn.object_id AS sanctioned, c.value #>> '{}' AS nm "
        "FROM current_assertions c "
        "JOIN sanc_names sn ON sn.nm = lower(btrim(c.value #>> '{}')) "
        "WHERE c.name='name' "
        "  AND c.object_id <> sn.object_id "
        "  AND c.object_id NOT IN (SELECT object_id FROM watchlist) "
        "  AND length(btrim(c.value #>> '{}')) >= 5"
    ):
        add(r["crawled"], r["sanctioned"], 0.5, f"watchlist name match: {r['nm']}")

    suppressed = await _suppressed(pool)
    queued = 0
    for (a, b), (score, reasons) in scored.items():
        if frozenset((a, b)) in suppressed:
            continue
        if await _insert(pool, a, b, score, reasons):
            queued += 1
    return queued


async def reclassify_mistyped_entities(actions: Actions, *, limit: int | None = None) -> int:
    """Repair pass: a Person object whose NAME is clearly an organization ('Brilliant
    Phoenix GP Inc.', 'LLC Sydecar' — Form D 'related persons' that are GP entities) is
    re-typed by minting the proper Organization and MERGING the Person into it (event-
    sourced, reversible). The mis-typed node's links resolve-on-read to the Org, so
    principals/screening stop seeing fake people and the entity now cross-base-resolves.
    One direction only (Person→Org), gated on the conservative classifier; never the
    reverse, so a real person is never turned into a company."""
    rows = await actions.pool.fetch(
        "SELECT o.id, "
        "  (SELECT value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='name' ORDER BY confidence DESC LIMIT 1) AS nm "
        "FROM objects o WHERE o.type='Person' AND o.status='active'"
    )
    ts = datetime.now(UTC)
    conf = confidence_for(EvidenceClass.AUTHORITATIVE_API)
    n = 0
    for r in rows:
        nm = r["nm"]
        if not nm or not is_organization(nm):
            continue
        cleaned = clean_entity_name(nm) or nm
        org_id = await actions.create_or_find_object(
            "Organization", "sec-org:" + cleaned.lower(), "reclassify"
        )
        await actions.assert_property(
            org_id, "name", cleaned, "reclassify", ts, conf,
            evidence_class=EvidenceClass.AUTHORITATIVE_API.value,
        )
        if org_id == r["id"]:
            continue
        try:
            await actions.merge_objects(
                org_id, r["id"],
                "reclassified Person->Organization (name carries an org signal)", "reclassify",
            )
            n += 1
        except ActionError:
            continue
        if limit is not None and n >= limit:
            break
    return n


async def screen_network(
    pool: asyncpg.Pool, entity_id: uuid.UUID, *, min_len: int = 5
) -> list[dict[str, Any]]:
    """Screen an entity's FINANCING NETWORK against the watchlist base — the un-chased
    follow-the-money question: is anyone in this company's money network (its principals,
    its feeder SPVs, and those SPVs' operators) a sanctioned/PEP entity?

    For each network member, match against any watchlist entity (opensanctions-tagged)
    by a SHARED IDENTIFIER (email/website/phone, 0.9) or an alias-aware NAME match (0.5).
    Returns the hits with the path that connects the member to the entity. A read model
    (does not queue): a null result is a clean network, reported as such. Never asserts
    same-entity (ruling #3) — the analyst adjudicates."""
    cluster = await _network_identity(pool, entity_id)
    members = await pool.fetch(
        "WITH ent AS (SELECT unnest($1::uuid[]) AS id), "
        "feeders AS (SELECT DISTINCT l.from_id AS id FROM links l JOIN ent ON l.to_id=ent.id "
        "            WHERE l.type='raises_for') "
        "SELECT l.to_id AS id, 'principal' AS via, l.type AS role "
        "  FROM links l JOIN ent ON l.from_id=ent.id "
        "  WHERE l.type IN ('officer','director','founded_by','manager','agent','owns','directs') "
        "UNION SELECT f.id, 'feeder_spv', NULL FROM feeders f "
        "UNION SELECT l.to_id, 'spv_operator', l.type "
        "  FROM links l JOIN feeders f ON l.from_id=f.id "
        "  WHERE l.type IN ('officer','director','manager')",
        cluster,
    )
    meta: dict[uuid.UUID, dict[str, Any]] = {}
    for m in members:
        e = meta.setdefault(m["id"], {"via": set(), "roles": set()})
        e["via"].add(m["via"])
        if m["role"]:
            e["roles"].add(m["role"])
    member_ids = [m for m in meta if m not in cluster]
    if not member_ids:
        return []

    hits = await pool.fetch(
        "WITH watchlist AS "
        "  (SELECT DISTINCT object_id FROM assertions WHERE source_id='opensanctions'), "
        "sanc_names AS ("
        "  SELECT s.object_id, lower(btrim(s.value #>> '{}')) AS nm FROM current_assertions s "
        "  JOIN watchlist w ON w.object_id=s.object_id WHERE s.name='name' "
        "  UNION SELECT s.object_id, lower(btrim(al.v)) FROM current_assertions s "
        "  JOIN watchlist w ON w.object_id=s.object_id "
        "  CROSS JOIN LATERAL jsonb_array_elements_text(s.value) AS al(v) "
        "  WHERE s.name='alias' AND jsonb_typeof(s.value)='array'), "
        "members AS (SELECT unnest($1::uuid[]) AS id) "
        # identifier match (strong)
        "SELECT c.object_id AS member, s.object_id AS hit, 0.9 AS score, "
        "  ('shared '||s.name||': '||lower(btrim(c.value #>> '{}'))) AS reason "
        "FROM current_assertions c JOIN members m ON m.id=c.object_id "
        "JOIN current_assertions s ON s.name=c.name "
        "  AND lower(btrim(s.value #>> '{}'))=lower(btrim(c.value #>> '{}')) "
        "JOIN watchlist w ON w.object_id=s.object_id "
        "WHERE c.name IN ('email','website','phone') AND c.object_id<>s.object_id "
        "  AND c.object_id NOT IN (SELECT object_id FROM watchlist) "
        "UNION "
        # name match (alias-aware, weak)
        "SELECT c.object_id, sn.object_id, 0.5, ('watchlist name match: '||(c.value #>> '{}')) "
        "FROM current_assertions c JOIN members m ON m.id=c.object_id "
        "JOIN sanc_names sn ON sn.nm=lower(btrim(c.value #>> '{}')) "
        "WHERE c.name='name' AND c.object_id<>sn.object_id "
        "  AND c.object_id NOT IN (SELECT object_id FROM watchlist) "
        "  AND length(btrim(c.value #>> '{}'))>=$2",
        member_ids, min_len,
    )
    out: list[dict[str, Any]] = []
    for h in hits:
        mid = h["member"]
        out.append({
            "member_id": str(mid),
            "member_name": await _name_of(pool, mid),
            "via": sorted(meta[mid]["via"]),
            "roles": sorted(meta[mid]["roles"]),
            "watchlist_entity": await _name_of(pool, h["hit"]),
            "score": float(h["score"]),
            "reason": h["reason"],
        })
    out.sort(key=lambda x: -x["score"])
    return out


async def _network_identity(pool: asyncpg.Pool, object_id: uuid.UUID) -> list[uuid.UUID]:
    """The entity's full identity set (merge winner + everything merged into it)."""
    winner = object_id
    for _ in range(50):
        nxt = await pool.fetchval("SELECT merged_into FROM objects WHERE id=$1", winner)
        if nxt is None:
            break
        winner = nxt
    rows = await pool.fetch(
        "WITH RECURSIVE m(id) AS (SELECT $1::uuid "
        "  UNION SELECT o.id FROM objects o JOIN m ON o.merged_into=m.id) SELECT id FROM m",
        winner,
    )
    return [r["id"] for r in rows]


async def _name_of(pool: asyncpg.Pool, oid: uuid.UUID) -> str | None:
    val: str | None = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' "
        "ORDER BY confidence DESC LIMIT 1",
        oid,
    )
    return val


async def find_cross_base_candidates(pool: asyncpg.Pool, *, min_len: int = 4) -> int:
    """Resolve the SAME organization across different federated bases. A person bridges
    them — aiming at Musk pulls Tesla (Q478214) from Wikidata, while EDGAR carries
    'Tesla, Inc.' (a cik); they are one company whose names differ only by legal form.
    We bucket Organizations by `normalize_org_name`, and queue a review candidate for
    any two distinct objects in a bucket whose provenance differs (a genuine cross-base
    pair, not a within-base dup the ingest already deduped). Name-normalized only -> a
    weak 0.6 signal; never auto-merges (ruling #3)."""
    rows = await pool.fetch(
        "SELECT a.object_id, a.value #>> '{}' AS nm, "
        "  (SELECT array_agg(DISTINCT source_id) FROM assertions s WHERE s.object_id=a.object_id) "
        "   AS sources "
        "FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Organization' AND o.status='active' "
        "WHERE a.name='name'"
    )
    buckets: dict[str, list[tuple[uuid.UUID, frozenset[str]]]] = {}
    for r in rows:
        norm = normalize_org_name(r["nm"] or "")
        if len(norm) < min_len:
            continue
        buckets.setdefault(norm, []).append(
            (r["object_id"], frozenset(r["sources"] or []))
        )

    suppressed = await _suppressed(pool)
    queued = 0
    for norm, members in buckets.items():
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                (a, sa), (b, sb) = members[i], members[j]
                if a == b or sa == sb:  # require different provenance (cross-base)
                    continue
                lo, hi = sorted((a, b))
                if frozenset((lo, hi)) in suppressed:
                    continue
                bases = "/".join(sorted(sa | sb))
                if await _insert(pool, lo, hi, 0.6, [f"cross-base org match: {norm} ({bases})"]):
                    queued += 1

    # deterministic pass: two objects sharing an LEI ARE the same entity (the LEI is a
    # global primary key) — far stronger than a normalized-name match, so score 0.95.
    lei_rows = await pool.fetch(
        "SELECT lower(btrim(a.value #>> '{}')) AS lei, a.object_id, "
        "  (SELECT array_agg(DISTINCT source_id) FROM assertions s "
        "   WHERE s.object_id=a.object_id) AS sources "
        "FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Organization' AND o.status='active' "
        "WHERE a.name='lei' AND length(btrim(a.value #>> '{}'))=20"
    )
    lei_buckets: dict[str, list[tuple[uuid.UUID, frozenset[str]]]] = {}
    for r in lei_rows:
        lei_buckets.setdefault(r["lei"], []).append(
            (r["object_id"], frozenset(r["sources"] or []))
        )
    for lei, lmembers in lei_buckets.items():
        for i in range(len(lmembers)):
            for j in range(i + 1, len(lmembers)):
                (a, sa), (b, sb) = lmembers[i], lmembers[j]
                if a == b or sa == sb:
                    continue
                lo, hi = sorted((a, b))
                if frozenset((lo, hi)) in suppressed:
                    continue
                if await _insert(pool, lo, hi, 0.95, [f"shared LEI: {lei}"]):
                    queued += 1
    return queued


async def converge_identities(
    actions: Actions, *, case_id: uuid.UUID | None = None
) -> dict[str, int]:
    """Footprint identity convergence (idempotent, safe to re-run): queue merge
    candidates for the review tray, and assemble Person hubs from STRONG signals
    (an Account's listed email matching a known Email; Accounts sharing a rel=me
    source page). Scoped to one case's objects when case_id is given. Never
    auto-merges Persons (ruling #3). Returns {"candidates": n, "hubs": m}."""
    pool = actions.pool
    queued = await find_footprint_merge_candidates(pool)
    queued += await find_person_merge_candidates(pool)
    queued += await find_sanctions_candidates(pool)  # crawl x federated-base screening
    queued += await find_cross_base_candidates(pool)  # base x base org resolution

    case_objs: set[uuid.UUID] | None = None
    if case_id is not None:
        case_objs = {
            r["object_id"]
            for r in await pool.fetch(
                "SELECT object_id FROM case_objects WHERE case_id=$1", case_id
            )
        }

    def in_scope(object_id: uuid.UUID) -> bool:
        return case_objs is None or object_id in case_objs

    hubs = 0
    # hub from bio-email: an Account whose listed email is a known Email object
    for r in await pool.fetch(
        "SELECT a.object_id AS account_id, a.value #>> '{}' AS email "
        "FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Account' "
        "WHERE a.name='email'"
    ):
        if not in_scope(r["account_id"]) or not r["email"]:
            continue
        canon = canonicalize("Email", r["email"])
        email_id = await pool.fetchval(
            "SELECT id FROM objects WHERE type='Email' AND canonical=$1", canon
        )
        if email_id is None:
            continue
        await ensure_person_hub(
            actions, key=canon, account_ids=[r["account_id"]],
            email_value=canon, email_id=email_id, case_id=case_id,
        )
        hubs += 1

    # hub from rel=me: Accounts that are rel=me targets of the same page are one identity
    by_src: dict[tuple[uuid.UUID, str], list[uuid.UUID]] = {}
    for r in await pool.fetch(
        "SELECT l.from_id AS src, s.canonical AS src_canon, l.to_id AS account_id "
        "FROM links l "
        "JOIN objects o ON o.id=l.to_id AND o.type='Account' "
        "JOIN objects s ON s.id=l.from_id "
        "WHERE l.type='rel_me'"
    ):
        if not in_scope(r["account_id"]):
            continue
        by_src.setdefault((r["src"], r["src_canon"]), []).append(r["account_id"])
    for (_src, src_canon), accts in by_src.items():
        await ensure_person_hub(actions, key=src_canon, account_ids=accts, case_id=case_id)
        hubs += 1

    return {"candidates": queued, "hubs": hubs}

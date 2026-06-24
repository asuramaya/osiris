"""Probabilistic entity resolution (DESIGN §10.2/§10.3, §11 behavioral merge).

Never auto-merges (ruling #3): generates `merge_candidates` for the review tray.
Person pairs are scored on shared strong identifiers / name+DOB / name+employer+
city; IntrusionSet pairs on shared TTPs (the §11 convergence). A confirmed
candidate runs merge_objects; a rejected one writes a permanent `not_same_as`
link — negative memory that suppresses the pair from ever being re-proposed.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.ontology.canonicalize import canonicalize

Pair = tuple[uuid.UUID, uuid.UUID]


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


async def review_tray(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Unresolved merge candidates, highest score first."""
    return [
        dict(r)
        for r in await pool.fetch(
            "SELECT id, a_id, b_id, score, reasons FROM merge_candidates "
            "WHERE resolved IS NULL ORDER BY score DESC, id"
        )
    ]


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

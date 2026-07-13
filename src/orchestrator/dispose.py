"""THE SEAM — the adversary proposes, the SEAT disposes, and nothing is ever silent.

The miner minted 3,579 rows across the fleet. 10.5% were ever touched by anyone. Its telemetry
reported {"chunks": 12, "threads": 8} — WHAT IT MADE, never WHAT WAS USED — so a 90%-garbage
producer and a 90%-gold producer emitted identical numbers, and it drifted for eight days and $40
with nothing anywhere able to notice.

The fix is not a better prompt. It is a SEAM: a place where somebody with standing must look at
each guess and say relevant or irrelevant, and where saying nothing is not an option.

  THE OPERATOR, 2026-07-13: "the miner is picking up things me and the main agent forgot ...
  it redirects the burden and makes it impossible to shove under the rug, it either is relevant
  or it is irrelevant."

FOUR RULES, and each one is a bug we actually shipped:

  1. ONLY A SEAT WITH STANDING MAY DISPOSE OF A PROJECT'S PILE. A stranger judging another
     project's rows is the janitor acting on JUDGEMENT instead of PROOF — the thing we forbade
     (ruling 69d64899). I disposed of Osiris's 264 by hand because Osiris is mine. Nobody else's
     pile is.

  2. ADMIT PROMOTES IN PLACE. The row is not copied, it is ADOPTED: a SELF_DECLARED assertion in
     the seat's own name, with an owner and a reason. That single act makes it the seat's word
     rather than the machine's — and it puts the row permanently behind the janitor's absolute
     guard, which never touches anything a mind has signed.

  3. A DROP MUST NAME ITS CLASS. Not "no" — WHY no. The taxonomy below was derived by reading all
     264 rows of the Osiris pile by hand, and it is the extractor's specification: every class is
     a rule the miner should have had. Drop rates per class are how we learn which rule it is
     still breaking.

  4. NOTHING EXPIRES. An earlier design let untaken proposals retract themselves after N days.
     That is FORGETTING WITH EXTRA STEPS: the candidate that quietly dies of old age is precisely
     the one we both forgot, which is why nobody took it. Disposition is forced, or it is nothing.

Never a DELETE. A drop is a compensating event carrying the seat's name and its reason — the row
stays readable, auditable, and reversible forever. The rug is transparent: you may shove anything
under it, and the shape of what you shoved will always be visible.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions

_EC = "self_declared"   # a disposition is a MIND'S WORD, never the machine's
_CONF = 0.9

# THE TAXONOMY — read off 264 hand-sorted rows (ruling 449c393f). Each class is a rule the
# extractor should have had, and the drop rate per class says which one it is still breaking.
DROP_CLASSES: dict[str, str] = {
    "narration": "a work-step, not a durable thread — GIT ALREADY HAS IT (180 of my 264)",
    "stale": "real once, and already done — the miner mints the question from an early chunk "
             "and never sees the answer 40 minutes later in the same session (28 of 264)",
    "echo": "the same fact it already minted — it reads a growing file with no memory of what "
            "it minted from the last chunk (26 of 264)",
    "misfiled": "another project's work, attributed here by the cwd bug (1 of 264)",
    "principle": "a standing rule, not a duty — canon, not a wall item (3 of 264)",
    "other": "none of the above — say why in `because`, and if this class grows, the taxonomy "
             "is missing a rule",
}

_MINER_ORIGIN = (
    "WITH origin AS (SELECT DISTINCT ON (a.object_id) a.object_id, a.source_id, "
    "                a.evidence_class ec FROM assertions a "
    "                ORDER BY a.object_id, a.observed_at, a.id) "
)

# A CANDIDATE is: the miner's own output (DERIVED, from a mining source), that NO MIND HAS EVER
# TOUCHED, not already disposed, and still open. The moment a mind lays a self_declared assertion
# on it, it stops being a candidate and becomes that mind's business — forever.
_CANDIDATE_WHERE = (
    "  AND g.ec='derived' "
    "  AND (g.source_id LIKE 'agent:%' OR g.source_id IN ('session-miner','git-memory')) "
    "  AND NOT EXISTS (SELECT 1 FROM assertions s WHERE s.object_id=o.id "
    "                  AND s.evidence_class='self_declared') "
    "  AND NOT EXISTS (SELECT 1 FROM assertions r WHERE r.object_id=o.id "
    "                  AND r.name='retracted') "
    "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    "    AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')"
    "    ='open' "
)


async def candidates(
    pool: asyncpg.Pool, *, project: str | None = None, limit: int = 100,
) -> dict[str, Any]:
    """The pile THIS seat must judge — oldest first, because triage drains from the bottom.

    Report-only; reading a candidate costs nothing and commits nothing. `project` scopes to one
    repo (the only scope a seat has standing over); omit it and you get the fleet's total, which
    is a COUNT you may look at and a pile you may not touch.
    """
    scope = ""
    args: list[Any] = []
    if project:
        scope = ("JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
                 "JOIN objects p ON p.id=l.to_id AND p.canonical=$1 ")
        args.append(project if project.startswith("repo:") else f"repo:{project}")
    rows = await pool.fetch(
        _MINER_ORIGIN +
        "SELECT o.id, o.created_at, "
        # the winner-resolution ordering (test_sql_hygiene): confidence THEN recency. Without
        # observed_at a tie on confidence resolves arbitrarily — the exact outage class the CI
        # tripwire exists to catch, and it caught ME writing it, in the file that polices guesses.
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS summary "
        "FROM objects o JOIN origin g ON g.object_id=o.id " + scope +
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL " +
        _CANDIDATE_WHERE +
        f"ORDER BY o.created_at LIMIT {int(limit)}", *args)
    total = await pool.fetchval(
        _MINER_ORIGIN +
        "SELECT count(*) FROM objects o JOIN origin g ON g.object_id=o.id " + scope +
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL " +
        _CANDIDATE_WHERE, *args)
    out: dict[str, Any] = {
        "project": project,
        "count": int(total or 0),
        "candidates": [{"id": str(r["id"])[:8],
                        "born": r["created_at"].date().isoformat(),
                        "summary": r["summary"]}
                       for r in rows if r["summary"]],
        "how": "dispose(admit=[{id, because, owner?}], drop=[{id, why, because?}]) — "
               f"why ∈ {sorted(DROP_CLASSES)}. A guess is not a duty: expect to drop ~9 in 10.",
    }
    if total and len(out["candidates"]) < total:
        out["note"] = f"showing the oldest {len(out['candidates'])} of {total}"
    return out


async def _resolve(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """A candidate id — full UUID or the 8-char short form the lenses render."""
    ref = ref.strip()
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    row = await pool.fetchrow(
        "SELECT id FROM objects WHERE type='Thread' AND left(id::text, 8)=$1", ref[:8])
    return row["id"] if row else None


async def _is_candidate(pool: asyncpg.Pool, tid: uuid.UUID) -> bool:
    """GUARD: never dispose of something a mind already signed. Checked per row, at write time —
    not once at the top — because the pile a seat is reading can be adopted underneath it."""
    return bool(await pool.fetchval(
        _MINER_ORIGIN +
        "SELECT 1 FROM objects o JOIN origin g ON g.object_id=o.id "
        "WHERE o.id=$1 AND o.type='Thread' " + _CANDIDATE_WHERE, tid))


async def dispose(
    actions: Actions, *, source: str,
    admit: list[dict[str, Any]] | None = None,
    drop: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Settle candidates — relevant or irrelevant, in your own name, with a reason on every one.

    `admit`: [{"id", "because", "owner"?}] — the guess was RIGHT and it is now YOURS. The row is
    promoted in place to SELF_DECLARED: it joins the wall, carries your name, and passes
    permanently behind the janitor's absolute guard.

    `drop`:  [{"id", "why", "because"?}] — the guess was wrong. `why` must be one of
    DROP_CLASSES: naming the CLASS is what turns a dismissal into a diagnosis. The row is
    retracted with a compensating event (never deleted), stays readable, and unwinds with a
    single re-assert.

    Returns what it did, plus this disposal's YIELD — admitted ÷ judged. That number is the
    adversary's licence: a producer that cannot demonstrate use does not get to spend.
    """
    now = datetime.now(UTC)
    done: dict[str, Any] = {"admitted": 0, "dropped": 0, "skipped": [], "by_class": {}}

    for item in admit or []:
        tid = await _resolve(actions.pool, str(item.get("id", "")))
        because = str(item.get("because") or "").strip()
        if tid is None or not because:
            done["skipped"].append({"id": item.get("id"),
                                    "why": "unknown id" if tid is None
                                           else "an admit needs `because` — why is it real?"})
            continue
        if not await _is_candidate(actions.pool, tid):
            done["skipped"].append({"id": item.get("id"), "why": "not a candidate (a mind has "
                                                                "already signed it, or it is "
                                                                "already disposed)"})
            continue
        # PROMOTION: the miner proposed, the seat ADOPTS. Self-declared, in the seat's own name.
        await actions.assert_property(tid, "status", "open", source, now, _CONF, evidence_class=_EC)
        await actions.assert_property(tid, "admitted_by", source, source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(tid, "admitted_because", because, source, now, _CONF,
                                      evidence_class=_EC)
        if owner := str(item.get("owner") or "").strip():
            await actions.assert_property(tid, "owner", owner, source, now, _CONF,
                                          evidence_class=_EC)
        done["admitted"] += 1

    for item in drop or []:
        tid = await _resolve(actions.pool, str(item.get("id", "")))
        why = str(item.get("why") or "").strip().lower()
        if tid is None:
            done["skipped"].append({"id": item.get("id"), "why": "unknown id"})
            continue
        if why not in DROP_CLASSES:
            done["skipped"].append({"id": item.get("id"),
                                    "why": f"`why` must name a class: {sorted(DROP_CLASSES)}"})
            continue
        if not await _is_candidate(actions.pool, tid):
            done["skipped"].append({"id": item.get("id"), "why": "not a candidate"})
            continue
        note = str(item.get("because") or "").strip()
        reason = f"{why.upper()} — {DROP_CLASSES[why]}" + (f" · {note}" if note else "")
        # a compensating EVENT, never a delete: the row stays readable and this unwinds with a
        # single re-assert of retracted=''. THE RUG IS TRANSPARENT.
        await actions.assert_property(tid, "retracted", True, source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(tid, "retracted_because", reason, source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(tid, "status", "retracted", source, now, _CONF,
                                      evidence_class=_EC)
        done["dropped"] += 1
        done["by_class"][why] = done["by_class"].get(why, 0) + 1

    judged = done["admitted"] + done["dropped"]
    if judged:
        # THE METER. Not "how much did it make" — how much was USED. The one number that can
        # falsify a producer, and the one nobody was keeping.
        done["yield"] = round(done["admitted"] / judged, 3)
    return done


async def adversary_yield(
    pool: asyncpg.Pool, *, project: str | None = None, days: int = 30,
) -> dict[str, Any]:
    """THE LICENCE. admitted ÷ judged over a window — the miner's measured rate of use.

    A producer whose telemetry counts what it MADE rather than what was USED is unfalsifiable and
    will rot without anyone noticing. This is the number that would have read 10.5% on day two and
    saved eight days and forty dollars. Below a floor, the adversary loses the right to spend.
    """
    scope, args = "", [days]
    if project:
        scope = ("AND EXISTS (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
                 "  WHERE l.from_id=a.object_id AND l.type='in_repo' AND p.canonical=$2) ")
        args.append(project if project.startswith("repo:") else f"repo:{project}")  # type: ignore[arg-type]
    row = await pool.fetchrow(
        "SELECT count(*) FILTER (WHERE a.name='admitted_because') AS admitted, "
        "       count(*) FILTER (WHERE a.name='retracted_because') AS dropped "
        "FROM current_assertions a "
        "WHERE a.evidence_class='self_declared' "
        "  AND a.name IN ('admitted_because','retracted_because') "
        "  AND a.observed_at > now() - make_interval(days => $1) " + scope, *args)
    admitted, dropped = int(row["admitted"] or 0), int(row["dropped"] or 0)
    judged = admitted + dropped
    out: dict[str, Any] = {"window_days": days, "project": project,
                           "admitted": admitted, "dropped": dropped, "judged": judged}
    if judged:
        out["yield"] = round(admitted / judged, 3)
        out["reads"] = ("admitted ÷ judged — the adversary's LICENCE. Osiris's own first pass "
                        "scored 0.098 (26 of 264). Below a floor, it does not get to spend.")
    else:
        out["reads"] = "nothing judged in this window — the seam has not been walked"
    return out

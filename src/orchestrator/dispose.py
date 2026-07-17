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
        "how": "dispose(admit=[{id, because, owner?}], drop=[{id, why, because?}], "
               "ask=[{id, because?, owner?}]) — "
               f"why ∈ {sorted(DROP_CLASSES)}. A guess is not a duty: expect to drop ~9 in 10; "
               "a real open QUESTION is asked, never admitted into a promise.",
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
    ask: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Settle candidates — relevant or irrelevant, in your own name, with a reason on every one.

    `admit`: [{"id", "because", "owner"?}] — the guess was RIGHT and it is now YOURS. The row is
    promoted in place to SELF_DECLARED: it joins the wall, carries your name, and passes
    permanently behind the janitor's absolute guard.

    `drop`:  [{"id", "why", "because"?}] — the guess was wrong. `why` must be one of
    DROP_CLASSES: naming the CLASS is what turns a dismissal into a diagnosis. The row is
    retracted with a compensating event (never deleted), stays readable, and unwinds with a
    single re-assert.

    `ask`:   [{"id", "because"?, "owner"?}] — the guess is a real OPEN QUESTION, not a duty
    (Ra V's taxonomy gap, thread 4d01b076: admitting a question makes it read as a promise;
    dropping it as 'other' buries something real). The row is kept open, reclassified
    kind='question' in your name — on the wall AS a question, ranked out of the work lanes,
    exactly as reclassify_thread would do it.

    Returns what it did, plus this disposal's YIELD — (admitted + asked) ÷ judged. That number
    is the adversary's licence: a producer that cannot demonstrate use does not get to spend;
    a question a seat found real enough to keep IS use.
    """
    now = datetime.now(UTC)
    done: dict[str, Any] = {"admitted": 0, "dropped": 0, "asked": 0, "skipped": [],
                            "by_class": {}}

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

    for item in ask or []:
        tid = await _resolve(actions.pool, str(item.get("id", "")))
        if tid is None:
            done["skipped"].append({"id": item.get("id"), "why": "unknown id"})
            continue
        if not await _is_candidate(actions.pool, tid):
            done["skipped"].append({"id": item.get("id"), "why": "not a candidate"})
            continue
        # A QUESTION IS NOT A PROMISE (thread 4d01b076): kept open, reclassified in the
        # seat's name — the same grammar reclassify_thread speaks, so every lens that
        # ranks questions out of the work wall already knows what to do with it.
        await actions.assert_property(tid, "kind", "question", source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(tid, "status", "open", source, now, _CONF,
                                      evidence_class=_EC)
        await actions.assert_property(tid, "asked_by", source, source, now, _CONF,
                                      evidence_class=_EC)
        if note := str(item.get("because") or "").strip():
            await actions.assert_property(tid, "reclassified_because", note, source, now,
                                          _CONF, evidence_class=_EC)
        if owner := str(item.get("owner") or "").strip():
            await actions.assert_property(tid, "owner", owner, source, now, _CONF,
                                          evidence_class=_EC)
        done["asked"] += 1

    judged = done["admitted"] + done["dropped"] + done["asked"]
    if judged:
        # THE METER. Not "how much did it make" — how much was USED. The one number that can
        # falsify a producer, and the one nobody was keeping. A question kept on the wall
        # counts as use: the miner surfaced something a seat judged real.
        done["yield"] = round((done["admitted"] + done["asked"]) / judged, 3)
    return done


# WHICH PRODUCER ARE WE MEASURING? The version marker is already in the data and costs nothing:
# the v2 adversary stamps `about_agent` (it speaks in its OWN name, about the agent), while every
# v1 crawl row was SOURCED to the agent and carries no such field. So "rows the current producer
# made" is a fact we can read, not a config knob someone must remember to turn.
_V2_ONLY = ("AND EXISTS (SELECT 1 FROM current_assertions v WHERE v.object_id=a.object_id "
            "            AND v.name='about_agent') ")


async def adversary_yield(
    pool: asyncpg.Pool, *, project: str | None = None, days: int = 30,
    current_producer_only: bool = False,
) -> dict[str, Any]:
    """THE LICENCE. admitted ÷ judged over a window — the miner's measured rate of use.

    A producer whose telemetry counts what it MADE rather than what was USED is unfalsifiable and
    will rot without anyone noticing. This is the number that would have read 10.5% on day two and
    saved eight days and forty dollars. Below a floor, the adversary loses the right to spend.

    `current_producer_only` measures the ADVERSARY THAT ACTUALLY EXISTS, not its dead predecessor.
    Default False, so the historical record stays readable — that 0.098 is what killed v1 and it
    should not be quietly erased. But the GATE reads the scoped number; see `licence`.
    """
    scope, args = "", [days]
    if project:
        scope = ("AND EXISTS (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
                 "  WHERE l.from_id=a.object_id AND l.type='in_repo' AND p.canonical=$2) ")
        args.append(project if project.startswith("repo:") else f"repo:{project}")  # type: ignore[arg-type]
    if current_producer_only:
        scope += _V2_ONLY
    row = await pool.fetchrow(
        "SELECT count(*) FILTER (WHERE a.name='admitted_because') AS admitted, "
        "       count(*) FILTER (WHERE a.name='retracted_because') AS dropped, "
        "       count(*) FILTER (WHERE a.name='asked_by') AS asked "
        "FROM current_assertions a "
        "WHERE a.evidence_class='self_declared' "
        "  AND a.name IN ('admitted_because','retracted_because','asked_by') "
        "  AND a.observed_at > now() - make_interval(days => $1) " + scope, *args)
    admitted, dropped = int(row["admitted"] or 0), int(row["dropped"] or 0)
    asked = int(row["asked"] or 0)
    judged = admitted + dropped + asked
    out: dict[str, Any] = {"window_days": days, "project": project,
                           "admitted": admitted, "dropped": dropped, "asked": asked,
                           "judged": judged}
    if judged:
        out["yield"] = round((admitted + asked) / judged, 3)
        out["reads"] = ("admitted ÷ judged — the adversary's LICENCE. Osiris's own first pass "
                        "scored 0.098 (26 of 264). Below the floor, it does not get to spend.")
    else:
        out["reads"] = "nothing judged in this window — the seam has not been walked"
    return out


# THE FLOOR. Below this measured rate of use, the adversary is not earning its tokens and loses
# the right to spend them. Chosen, not tuned: the crawl scored 0.098 over its whole life, so a
# floor of 0.15 says "beat what the thing we deleted managed, or stop." It is deliberately LOW —
# this is a circuit breaker for a producer that has gone bad, not a performance target.
YIELD_FLOOR = 0.15

# ...and it cannot fire before there is anything to measure. A producer must be given a real
# sample before it is judged, or the first unlucky session kills it. (The same courtesy the
# WALL now extends to a guess: judged on evidence, never on suspicion.)
LICENCE_MIN_JUDGED = 40


async def licence(pool: asyncpg.Pool, *, days: int = 30) -> dict[str, Any]:
    """MAY THE ADVERSARY SPEND? — the check that runs BEFORE it is allowed to call a model.

    THIS IS THE FIX FOR THE ACTUAL ROOT CAUSE, and it is worth being blunt about why. The miner's
    tick reported {"chunks": 12, "threads": 8} — WHAT IT MADE, never WHAT WAS USED. So a
    90%-garbage producer and a 90%-gold producer emitted IDENTICAL telemetry, and neither the
    operator, nor the miner, nor any mind reading the graph could tell them apart. It drifted to
    garbage for eight days and $40 and NOTHING ANYWHERE COULD NOTICE. A producer that cannot be
    falsified will rot — not might, WILL, because nothing is pushing back.

    So the meter is not a dashboard. It is a GATE. Below the floor, the adversary does not run.

    IT MEASURES THE PRODUCER THAT ACTUALLY EXISTS. I shipped this gate reading the FLEET-WIDE
    yield, ran it, and it refused — on v1's 0.098, over rows v1 made, against a v2 that had not
    yet written a line. And v2 could never have raised that number, because it was not allowed to
    produce. A GATE THAT CAN NEVER OPEN IS A KILL SWITCH WEARING A GATE'S CLOTHES — the same crime
    as everything else killed this week: a mechanism whose stated purpose and actual behaviour
    differ. So it scopes to the current producer (rows carrying `about_agent`), which starts its
    record at zero and EARNS it. The old 0.098 stays visible in adversary_yield(); it is what
    killed v1 and it should not be quietly erased.

    Fail-OPEN on an unmeasurable state (no data yet, or a broken query): a metering bug must never
    silently disable the memory. It fails LOUD instead — `reason` says exactly why it is open.
    """
    m = await adversary_yield(pool, days=days, current_producer_only=True)
    judged, rate = int(m["judged"]), m.get("yield")
    if judged < LICENCE_MIN_JUDGED:
        return {"may_spend": True, "reason": f"only {judged} rows judged in {days}d — a producer "
                f"is given a real sample ({LICENCE_MIN_JUDGED}) before it is judged", **m}
    if rate is not None and rate < YIELD_FLOOR:
        return {"may_spend": False, "reason": f"YIELD {rate} IS BELOW THE FLOOR ({YIELD_FLOOR}) "
                f"over {judged} judged rows in {days}d — the adversary is not earning its tokens "
                "and has lost the right to spend them. Fix its prompt, or leave it dark. Nothing "
                "auto-restarts it (Osiris has no hands over your systems).", **m}
    return {"may_spend": True, "reason": f"yield {rate} clears the floor ({YIELD_FLOOR})", **m}


async def orphans(pool: asyncpg.Pool) -> dict[str, Any]:
    """MACHINE-MINTED ROWS THAT BELONG TO NO PROJECT — and therefore to no SEAT, and therefore to
    NOBODY.

    THE HOLE THIS PLUGS, and it is a hole in the DESIGN, not in a query. The whole remediation
    spec (449c393f) rests on one sentence: EVERY PROJECT JUDGES ITS OWN MACHINE'S GUESSES, at its
    own death rite, by the seat that made the mess. That sentence has a silent precondition — that
    every guess HAS a project. `mine_threads` never filed one (25 of its 26 threads had no repo),
    so those rows were structurally unreachable by the ritual: on nobody's wall, in nobody's queue,
    nobody's problem. FOREVER. Not "hard to reach" — CANNOT BE REACHED.

    A backlog you cannot assign is not a backlog. It is a landfill with a ticketing system.

    So this is the TRIPWIRE, and it must stay at zero. It is not a cleanup tool: it names, loudly,
    any producer that has minted a row it cannot name an owner for — because the next such producer
    will not announce itself either, and the only reason we found THIS one was that the operator
    asked "are orphans even possible with the changes?" and would not accept a sweep as an answer.

        A PRODUCER THAT CANNOT NAME AN OWNER FOR ITS OUTPUT MUST NOT BE ALLOWED TO PRODUCE IT.
    """
    rows = await pool.fetch(
        _MINER_ORIGIN +
        "SELECT g.source_id, count(*) AS n, "
        "       to_char(max(o.created_at),'YYYY-MM-DD HH24:MI') AS newest "
        "FROM objects o JOIN origin g ON g.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo') " +
        _CANDIDATE_WHERE +
        "GROUP BY g.source_id ORDER BY n DESC")
    total = sum(int(r["n"]) for r in rows)
    return {
        "orphans": total,
        "by_producer": [dict(r) for r in rows],
        "verdict": "clean — every machine guess has an owner" if not total else
                   f"{total} row(s) belong to NO project, so NO seat can ever dispose them. "
                   f"The producer must be made to name an owner, not the pile swept.",
    }

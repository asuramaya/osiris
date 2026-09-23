"""The disposal seam: a producer proposes candidate rows, an owning seat disposes of them, and
nothing is ever left silently unjudged.

A background miner minted 3,579 rows across the fleet; only 10.5% were ever touched by anyone.
Its telemetry reported what it made (e.g. {"chunks": 12, "threads": 8}), never what was used, so
a producer that was 90% garbage and one that was 90% gold emitted identical numbers, and quality
drifted for days with nothing anywhere able to notice.

The fix is not a better prompt. It is a seam: a place where somebody with standing must look at
each guess and say relevant or irrelevant, and where saying nothing is not an option. That
requirement traces to an explicit operator directive: the disposal process should redirect the
burden of judgment onto a human/seat and make it impossible to leave a guess unresolved, so every
row ends up either relevant or irrelevant.

Four rules, each one tracing back to a bug that actually shipped:

  1. ONLY A SEAT WITH STANDING MAY DISPOSE OF A PROJECT'S PILE. A stranger judging another
     project's rows would be acting on judgement instead of proof, which this design forbids.
     Each seat disposes of its own project's pile only; nobody else's pile is theirs to judge.

  2. ADMIT PROMOTES IN PLACE. The row is not copied, it is adopted: a self-declared assertion in
     the seat's own name, with an owner and a reason. That single act makes it the seat's word
     rather than the machine's, and it puts the row permanently behind the disposal guard, which
     never touches anything a mind has already signed.

  3. A DROP MUST NAME ITS CLASS. Not "no", but why no. The taxonomy below was derived by reading
     a large hand-sorted sample of candidate rows, and it is the extractor's specification: every
     class is a rule the producer should have had. Drop rates per class show which rule it is
     still breaking.

  4. NOTHING EXPIRES. An earlier design let untaken proposals retract themselves after N days.
     That is forgetting with extra steps: the candidate that quietly dies of old age is precisely
     the one everyone forgot, which is why nobody took it. Disposition is forced, or it is
     nothing.

Never a DELETE. A drop is a compensating event carrying the seat's name and its reason; the row
stays readable, auditable, and reversible forever. Nothing gets swept under the rug without the
shape of what was swept staying visible.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions

_EC = "self_declared"   # a disposition is a mind's word, never the machine's
_CONF = 0.9

# The taxonomy: derived from a hand-sorted sample of 264 candidate rows. Each class is a rule
# the extractor should have had, and the drop rate per class shows which one it is still
# breaking.
DROP_CLASSES: dict[str, str] = {
    "narration": "a work-step, not a durable thread; git already has it (180 of 264)",
    "stale": "real once, and already done; the miner minted the question from an early chunk "
             "and never saw the answer that showed up 40 minutes later in the same session "
             "(28 of 264)",
    "echo": "the same fact it already minted; it reads a growing file with no memory of what "
            "it minted from the last chunk (26 of 264)",
    "misfiled": "another project's work, attributed here by a cwd bug (1 of 264)",
    "principle": "a standing rule, not a duty; canon, not a wall item (3 of 264)",
    "other": "none of the above; say why in `because`, and if this class grows, the taxonomy "
             "is missing a rule",
}

_MINER_ORIGIN = (
    # Scoped to Thread objects. Profiling orient() -> candidates(limit=0) showed this CTE was
    # the single largest cost in that call path (5.45s of 6.87s total), not the composition
    # evaluator an earlier single-snapshot trace had misattributed it to. Unscoped,
    # `DISTINCT ON (a.object_id) ... FROM assertions` sorted the whole `assertions` table
    # (3,119,632 rows fleet-wide) to answer a question every one of this CTE's four call sites
    # only ever asks about Thread objects (each joins `origin` under an outer `o.type='Thread'`
    # filter; none reads a non-Thread row from it). Threads carry only 25,841 of those
    # assertions (0.8%). Scoping the CTE's own FROM clause to exactly that population is a pure
    # narrowing: identical rows out, ~99% less to sort to get them.
    "WITH origin AS (SELECT DISTINCT ON (a.object_id) a.object_id, a.source_id, "
    "                a.evidence_class ec FROM assertions a "
    "                WHERE a.object_id IN (SELECT id FROM objects WHERE type='Thread') "
    "                ORDER BY a.object_id, a.observed_at, a.id) "
)

# A candidate is: the miner's own output (derived, from a mining source), that no mind has ever
# touched, not already disposed of, and still open. The moment a mind lays a self_declared
# assertion on it, it stops being a candidate and becomes that mind's business, permanently.
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
    """The pile this seat must judge, oldest first, because triage drains from the bottom.

    Report-only; reading a candidate costs nothing and commits nothing. `project` scopes to one
    repo (the only scope a seat has standing over); omit it and you get the fleet's total, which
    is a count you may look at but a pile you may not touch.
    """
    scope = ""
    args: list[Any] = []
    if project:
        scope = ("JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
                 "AND (l.valid_until IS NULL OR l.valid_until > now()) "
                 "JOIN objects p ON p.id=l.to_id AND p.canonical=$1 ")
        args.append(project if project.startswith("repo:") else f"repo:{project}")
    # orient()'s own call shape is limit=0, count-only (the "your_pile" glance in
    # mcp_server.py): a LIMIT 0 fetch would still run the full query, correlated summary
    # subquery included, only to discard every row it returns. Skip that work entirely for a
    # caller that never reads `candidates`.
    rows = [] if limit == 0 else await pool.fetch(
        _MINER_ORIGIN +
        "SELECT o.id, o.created_at, "
        # Winner-resolution ordering matches test_sql_hygiene's requirement: confidence then
        # recency. Without observed_at a tie on confidence resolves arbitrarily, which is
        # exactly the class of bug that hygiene check exists to catch.
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
    """A candidate id: full UUID or the 8-char short form the surfaces render."""
    ref = ref.strip()
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    row = await pool.fetchrow(
        "SELECT id FROM objects WHERE type='Thread' AND left(id::text, 8)=$1", ref[:8])
    return row["id"] if row else None


async def _is_candidate(pool: asyncpg.Pool, tid: uuid.UUID) -> bool:
    """Guard: never dispose of something a mind already signed. Checked per row, at write time,
    not once at the top, because the pile a seat is reading can be adopted underneath it."""
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
    """Settle candidates, relevant or irrelevant, in your own name, with a reason on every one.

    `admit`: [{"id", "because", "owner"?}] - the guess was right and it is now yours. The row is
    promoted in place to self_declared: it joins the wall, carries your name, and passes
    permanently behind the disposal guard.

    `drop`:  [{"id", "why", "because"?}] - the guess was wrong. `why` must be one of
    DROP_CLASSES: naming the class is what turns a dismissal into a diagnosis. The row is
    retracted with a compensating event (never deleted), stays readable, and unwinds with a
    single re-assert.

    `ask`:   [{"id", "because"?, "owner"?}] - the guess is a real open question, not a duty.
    Admitting a question makes it read as a promise; dropping it as 'other' buries something
    real, so it gets its own path: the row is kept open, reclassified kind='question' in your
    name, on the wall as a question, ranked out of the work lanes, exactly as
    reclassify_thread would do it.

    Returns what it did, plus this disposal's yield: (admitted + asked) / judged. That number
    is the adversary's licence: a producer that cannot demonstrate use does not get to spend;
    a question a seat found real enough to keep is use.
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
        # Promotion: the miner proposed, the seat adopts. Self-declared, in the seat's own name.
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
        # A compensating event, never a delete: the row stays readable and this unwinds with a
        # single re-assert of retracted=''.
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
        # A question is not a promise: kept open, reclassified in the seat's name, the same
        # grammar reclassify_thread speaks, so every surface that ranks questions out of the
        # work wall already knows what to do with it.
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
        # The meter measures not how much the producer made, but how much was used. It is the
        # one number that can falsify a producer, and the one nobody was keeping before. A
        # question kept on the wall counts as use: the miner surfaced something a seat judged
        # real.
        done["yield"] = round((done["admitted"] + done["asked"]) / judged, 3)
    return done


# Which producer are we measuring? The version marker is already in the data and costs nothing
# to check: the current adversary stamps `about_agent` (it speaks in its own name, about the
# agent), while every prior crawl row was sourced to the agent and carries no such field. So
# "rows the current producer made" is a fact we can read, not a config knob someone must
# remember to turn.
_V2_ONLY = ("AND EXISTS (SELECT 1 FROM current_assertions v WHERE v.object_id=a.object_id "
            "            AND v.name='about_agent') ")


async def adversary_yield(
    pool: asyncpg.Pool, *, project: str | None = None, days: int = 30,
    current_producer_only: bool = False,
) -> dict[str, Any]:
    """The licence number: admitted / judged over a window, the miner's measured rate of use.

    A producer whose telemetry counts what it made rather than what was used is unfalsifiable and
    will rot without anyone noticing. This is the number that would have read 10.5% on day two
    and saved eight days and forty dollars of wasted spend. Below a floor, the adversary loses
    the right to spend.

    `current_producer_only` measures the adversary that actually exists, not a dead predecessor.
    Default False, so the historical record stays readable: an earlier yield of 0.098 is what
    killed the prior producer version and should not be quietly erased. But the gate reads the
    scoped number; see `licence`.

    THE HONEST DENOMINATOR (this metric's fifth correction): a raw admit-rate punishes a project
    that retracts. One project mined a 5.1% rate not because the judging was harsh but because
    the miner kept filing tickets against work the project had already buried; a repo that kills
    its own lanes publicly will always mine low, raw. So beside the raw numbers this also reports
    `yield_honest`: the same ratio over candidates born before their project's last superseding
    ruling. A candidate minted after the project already re-ruled the world it describes is a
    corpse at birth (`corpse_excluded` counts them) and says something about the miner's lag, not
    its quality. The licence gate reads the honest rate."""
    scope, args = "", [days]
    if project:
        scope = ("AND EXISTS (SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
                 "  WHERE l.from_id=a.object_id AND l.type='in_repo' "
                 "  AND (l.valid_until IS NULL OR l.valid_until > now()) AND p.canonical=$2) ")
        args.append(project if project.startswith("repo:") else f"repo:{project}")  # type: ignore[arg-type]
    if current_producer_only:
        scope += _V2_ONLY
    row = await pool.fetchrow(
        "WITH judged AS ("
        "  SELECT a.name, o.created_at AS born, rl.proj "
        "  FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "  LEFT JOIN LATERAL (SELECT l.to_id AS proj FROM links l "
        "    WHERE l.from_id = a.object_id AND l.type='in_repo' "
        "    AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1) rl ON true "
        "  WHERE a.evidence_class='self_declared' "
        "    AND a.name IN ('admitted_because','retracted_because','asked_by') "
        "    AND a.observed_at > now() - make_interval(days => $1) " + scope + "), "
        "last_ruling AS ("
        "  SELECT l.to_id AS proj, max(sb.observed_at) AS at "
        "  FROM current_assertions sb "
        "  JOIN links l ON l.from_id = sb.object_id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "  WHERE sb.name='superseded_by' GROUP BY l.to_id) "
        "SELECT count(*) FILTER (WHERE j.name='admitted_because') AS admitted, "
        "       count(*) FILTER (WHERE j.name='retracted_because') AS dropped, "
        "       count(*) FILTER (WHERE j.name='asked_by') AS asked, "
        "       count(*) FILTER (WHERE lr.at IS NOT NULL AND j.born >= lr.at) AS corpse, "
        "       count(*) FILTER (WHERE j.name='admitted_because' "
        "                        AND (lr.at IS NULL OR j.born < lr.at)) AS admitted_h, "
        "       count(*) FILTER (WHERE j.name='asked_by' "
        "                        AND (lr.at IS NULL OR j.born < lr.at)) AS asked_h, "
        "       count(*) FILTER (WHERE lr.at IS NULL OR j.born < lr.at) AS judged_h "
        "FROM judged j LEFT JOIN last_ruling lr ON lr.proj = j.proj", *args)
    admitted, dropped = int(row["admitted"] or 0), int(row["dropped"] or 0)
    asked = int(row["asked"] or 0)
    judged = admitted + dropped + asked
    judged_h, corpse = int(row["judged_h"] or 0), int(row["corpse"] or 0)
    out: dict[str, Any] = {"window_days": days, "project": project,
                           "admitted": admitted, "dropped": dropped, "asked": asked,
                           "judged": judged, "judged_honest": judged_h,
                           "corpse_excluded": corpse}
    if judged:
        out["yield"] = round((admitted + asked) / judged, 3)
        out["reads"] = ("admitted ÷ judged — the adversary's LICENCE. Osiris's own first pass "
                        "scored 0.098 (26 of 264). Below the floor, it does not get to spend.")
    else:
        out["reads"] = "nothing judged in this window — the seam has not been walked"
    if judged_h:
        out["yield_honest"] = round(
            (int(row["admitted_h"] or 0) + int(row["asked_h"] or 0)) / judged_h, 3)
    return out


# The floor. Below this measured rate of use, the adversary is not earning its tokens and loses
# the right to spend them. Chosen, not tuned: the earlier crawl-based producer scored 0.098 over
# its whole life, so a floor of 0.15 says "beat what the thing we deleted managed, or stop." It
# is deliberately low: this is a circuit breaker for a producer that has gone bad, not a
# performance target.
YIELD_FLOOR = 0.15

# It also cannot fire before there is anything to measure. A producer must be given a real
# sample before it is judged, or the first unlucky session kills it: the same standard applied
# to any guess judged on evidence, never on suspicion.
LICENCE_MIN_JUDGED = 40


async def licence(pool: asyncpg.Pool, *, days: int = 30) -> dict[str, Any]:
    """May the adversary spend? The check that runs before it is allowed to call a model.

    This is the fix for the actual root cause, and it is worth being explicit about why. The
    miner's tick reported what it made (e.g. {"chunks": 12, "threads": 8}), never what was used.
    So a producer that was 90% garbage and one that was 90% gold emitted identical telemetry, and
    nobody reading the graph could tell them apart. Quality drifted to garbage for eight days and
    $40 and nothing anywhere could notice. A producer that cannot be falsified will rot: not
    might, will, because nothing is pushing back.

    So the meter is not a dashboard. It is a gate. Below the floor, the adversary does not run.

    It measures the producer that actually exists. An earlier version of this gate read the
    fleet-wide yield, and on first run it refused: it measured 0.098 over rows the prior producer
    version had made, against a new producer version that had not yet written a line. The new
    version could never have raised that number, because it was not allowed to produce. A gate
    that can never open is a kill switch wearing a gate's clothes: a mechanism whose stated
    purpose and actual behaviour differ. So this gate scopes to the current producer (rows
    carrying `about_agent`), which starts its record at zero and earns it. The old 0.098 stays
    visible in adversary_yield(); it is what killed the prior producer version and should not be
    quietly erased.

    Fails open on an unmeasurable state (no data yet, or a broken query): a metering bug must
    never silently disable the memory. It fails loud instead; `reason` says exactly why it is
    open.
    """
    m = await adversary_yield(pool, days=days, current_producer_only=True)
    judged = int(m["judged"])
    # The honest denominator: the gate judges the producer over candidates born before their
    # project's last superseding ruling, since a repo that retracts publicly must not read as a
    # bad pile. With no corpse rows the two rates are the same number.
    rate = m.get("yield_honest") if m.get("judged_honest") else m.get("yield")
    corpse_note = (f" ({m['corpse_excluded']} corpse row(s) born after their project's last "
                   "superseding ruling excluded — the honest denominator, 1258d382)"
                   if m.get("corpse_excluded") else "")
    if judged < LICENCE_MIN_JUDGED:
        return {"may_spend": True, "reason": f"only {judged} rows judged in {days}d — a producer "
                f"is given a real sample ({LICENCE_MIN_JUDGED}) before it is judged", **m}
    if rate is not None and rate < YIELD_FLOOR:
        return {"may_spend": False, "reason": f"YIELD {rate} IS BELOW THE FLOOR ({YIELD_FLOOR}) "
                f"over {judged} judged rows in {days}d{corpse_note} — the adversary is not "
                "earning its tokens "
                "and has lost the right to spend them. Fix its prompt, or leave it dark. Nothing "
                "auto-restarts it (Osiris has no hands over your systems).", **m}
    return {"may_spend": True,
            "reason": f"yield {rate} clears the floor ({YIELD_FLOOR}){corpse_note}", **m}


async def orphans(pool: asyncpg.Pool) -> dict[str, Any]:
    """Machine-minted rows that belong to no project, and therefore to no seat, and therefore to
    nobody.

    The hole this plugs is a hole in the design, not in a query. The whole remediation spec rests
    on one requirement: every project judges its own machine's guesses, at its own disposal step,
    by the seat that made the mess. That requirement has a silent precondition: that every guess
    has a project. `mine_threads` never filed one for most of its output (25 of its 26 threads
    had no repo), so those rows were structurally unreachable by the process: on nobody's wall,
    in nobody's queue, nobody's problem, permanently. Not "hard to reach": cannot be reached.

    A backlog you cannot assign is not a backlog. It is a landfill with a ticketing system.

    So this is the tripwire, and it must stay at zero. It is not a cleanup tool: it names, loudly,
    any producer that has minted a row it cannot name an owner for, because the next such
    producer will not announce itself either. This check exists because a prior sweep would have
    missed the real question: whether orphans are even possible given the current design, not
    just whether any exist right now.

        A producer that cannot name an owner for its output must not be allowed to produce it.
    """
    rows = await pool.fetch(
        _MINER_ORIGIN +
        "SELECT g.source_id, count(*) AS n, "
        "       to_char(max(o.created_at),'YYYY-MM-DD HH24:MI') AS newest "
        "FROM objects o JOIN origin g ON g.object_id=o.id "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id AND l.type='in_repo' "
        "    AND (l.valid_until IS NULL OR l.valid_until > now())) " +
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


# The one-time broadcast that outlived its own number: every project's own "DISPOSE OF YOUR
# MINER PILE" thread was bulk-minted once, on 2026-07-13, with that day's pile count baked
# into the static summary text. That is the same class of failure this codebase's standing
# rule against inheriting a stale number warns about: one project's own copy sat at the top
# of its wall for weeks claiming 257 unjudged candidates when candidates() returned one, an
# orphaned number doing exactly what it accused the miner of doing. Measured before building:
# a sample of 7 of these threads was checked live. 4 had already self-healed (a seat visited,
# called candidates() fresh as dispose()'s own protocol requires, and closed the thread on the
# true count, the correct behavior this house already has). But 3 remained open and wrong at
# the time of the check: pokex (149 -> 0), code (38 -> 27), and one project (1 -> 1) whose
# count had not drifted at all, proof this is real, measurable staleness, not a hypothetical.
# The miner producing new piles is currently dark, so this is a closed, non-recurring,
# shrinking population; a permanent recurring janitor would be disproportionate machinery for
# it. This is a one-time repair, same idiom as the bootstrap-orphan-reference backfill: dry-run
# by default, mechanical, and it never guesses a disposition on anyone's behalf; judging the
# pile stays the seat's own job (rule 1 above).
_STALE_PILE_RE = re.compile(
    r"^DISPOSE OF YOUR MINER PILE — (\d+) candidates on ")


async def repair_stale_pile_summons(
    actions: Actions, *, actor: str, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """Repair verb for the 2026-07-13 bulk-minted "DISPOSE OF YOUR MINER PILE" threads
    (one per project, `owner`=<project>): re-measures each still-open one's project against
    a live `candidates(project=..., limit=0)` call and compares it to the frozen count in
    the thread's own summary text.

    Three outcomes, never a fourth: (1) live count matches the frozen one: untouched, nothing
    to fix. (2) live count is 0: the pile is empty, so this resolves the thread as moot (the
    same "resolving as moot rather than fabricating a disposal against an empty pile"
    reasoning a prior seat already applied by hand to one project's own copy); there is
    nothing left for the seat to judge, so leaving it open serves no one. (3) live count is
    >0 but differs from the frozen one: the seat's own judging duty is real and still theirs,
    so this calls `correct_thread_summary` (never `annotate_thread`, whose own docstring
    names this exact shape as the wrong one: a caller who means the earlier understanding was
    wrong wants a different verb entirely, and that verb is this one) so the headline itself
    stops asserting a number that is simply false, while the duty, never resolved, never
    disposed of on anyone's behalf, stays exactly where it belongs (rule 1).

    Mechanical and conservative: only matches the exact bulk-mint template (`_STALE_PILE_RE`)
    on a still-`open` Thread, never a hand-written or differently-worded thread that merely
    mentions a candidate count. `owner` (already asserted on every one of these threads at
    mint time) names the project directly; never re-derived from prose.

    Dry run is the default, same rule as every other repair verb in this house.
    `dry_run=False` requires a non-blank `because` for the resolve actions (the corrected-
    summary actions carry their own fixed, self-explanatory `corrected_because` and need no
    separate citation, per `correct_thread_summary`'s own supersession semantics). Genuinely
    idempotent, not just claimed: a resolved thread no longer matches the `status='open'`
    scope on a re-run; a corrected summary re-asserting the same text on a re-run supersedes
    onto an unchanged current value via `assert_property`'s own within-source rule, so there
    is no growing pile of duplicate corrections."""
    if not dry_run and not (because or "").strip():
        return {"error": "repairing without a because is an un-audited repair — cite the "
                         "evidence/finding that authorizes it"}
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  AS owner "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='status' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') = 'open'")
    to_resolve: list[dict[str, Any]] = []
    to_correct: list[dict[str, Any]] = []
    unmatched_owner: list[str] = []
    for row in rows:
        summary = row["summary"] or ""
        m = _STALE_PILE_RE.match(summary)
        if not m:
            continue
        frozen = int(m.group(1))
        owner = row["owner"]
        if not owner:
            unmatched_owner.append(str(row["id"]))
            continue
        live = await candidates(pool, project=owner, limit=0)
        live_count = live["count"]
        if live_count == frozen:
            continue
        entry = {"id": str(row["id"]), "owner": owner, "frozen": frozen, "live": live_count,
                 "summary": summary}
        (to_resolve if live_count == 0 else to_correct).append(entry)
    report: dict[str, Any] = {
        "dry_run": dry_run,
        "to_resolve": [{k: v for k, v in e.items() if k != "summary"} for e in to_resolve],
        "to_correct": [{k: v for k, v in e.items() if k != "summary"} for e in to_correct],
        "unmatched_owner": unmatched_owner,
    }
    if dry_run or not (to_resolve or to_correct):
        return report
    from src.orchestrator.capture import correct_thread_summary, resolve_thread

    for entry in to_resolve:
        await resolve_thread(
            actions, entry["id"],
            because=f"pile is empty (candidates(project={entry['owner']!r}) returns 0) — "
                    f"the thread's own frozen count ({entry['frozen']}) was the count at "
                    f"mint time (2026-07-13); resolving as moot rather than fabricating a "
                    f"disposal against an empty pile ({because})",
            artifact=None, source=actor)
    for entry in to_correct:
        corrected = _STALE_PILE_RE.sub(
            f"DISPOSE OF YOUR MINER PILE — {entry['live']} candidates on ", entry["summary"])
        await correct_thread_summary(
            actions, entry["id"], corrected,
            because=f"the thread's own count ({entry['frozen']}) was frozen at mint time "
                    f"(2026-07-13) — candidates(project={entry['owner']!r}) returns "
                    f"{entry['live']} now; repair_stale_pile_summons, thread e2326ab7",
            source=actor)
    return report

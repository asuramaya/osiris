"""THE SEAM — the adversary proposes, the seat disposes, and nothing is silent.

What may NOT happen is tested harder than what may. A verb that lets one seat retract another
project's work, or lets a mind's signed word be swept by a machine, is worse than the pile it was
cleaning.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import link_repo, open_thread
from src.orchestrator.dispose import adversary_yield, candidates, dispose

NOW = datetime.now(UTC)
SEAT = "agent:thoth-xxviii"


async def _mined(actions: Actions, canon: str, summary: str, *, origin: str = "session-miner",
                 repo: str | None = None, v2: bool = True) -> object:
    """A row the MINER wrote: DERIVED, unsigned by any mind.

    `v2` stamps `about_agent` — the speaker/subject split the current adversary always writes, and
    therefore the marker that says WHICH PRODUCER MADE THIS ROW. v2=False forges v1 sediment: a
    row from the dead crawl, which the licence gate must not hold against its successor."""
    o = await actions.create_or_find_object("Thread", canon, origin)
    await actions.assert_property(o, "summary", summary, origin, NOW, 0.4,
                                  evidence_class="derived")
    await actions.assert_property(o, "status", "open", origin, NOW, 0.4, evidence_class="derived")
    if v2:
        await actions.assert_property(o, "about_agent", "agent:whoever", origin, NOW, 0.4,
                                      evidence_class="derived")
    if repo:
        await link_repo(actions, o, repo, NOW, source=origin, evidence_class="derived")
    return o


async def test_admit_PROMOTES_the_row_into_the_seat_s_own_name(actions: Actions) -> None:
    """The miner proposed; the seat ADOPTS. The row is not copied — it becomes the seat's word,
    which is also what puts it permanently behind the janitor's absolute guard."""
    t = await _mined(actions, "thread:real", "the wake trigger reads the broken liveness field")

    rep = await dispose(actions, source=SEAT, admit=[
        {"id": str(t)[:8], "because": "real, live, and nobody wrote it down", "owner": "osiris"}])
    assert rep["admitted"] == 1 and rep["yield"] == 1.0

    row = await actions.pool.fetchrow(
        "SELECT (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='admitted_by') AS by, "
        "       (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='owner') AS owner, "
        "       (SELECT evidence_class FROM current_assertions WHERE object_id=$1 "
        "        AND name='status' ORDER BY confidence DESC LIMIT 1) AS grade", t)
    assert row["by"] == SEAT and row["owner"] == "osiris"
    assert row["grade"] == "self_declared", "an admitted row must carry the SEAT's authority"

    # ...and it is no longer a candidate: a mind has signed it, so it is nobody's to sweep
    assert (await candidates(actions.pool))["count"] == 0


async def test_a_drop_must_NAME_ITS_CLASS_or_it_is_refused(actions: Actions) -> None:
    """Not "no" — WHY no. Naming the class is what turns a dismissal into a DIAGNOSIS: the drop
    rate per class is how we learn which rule the extractor is still breaking."""
    t = await _mined(actions, "thread:slop", "Execute step 1: re-export the model")

    rep = await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8]}])           # no `why`
    assert rep["dropped"] == 0 and "must name a class" in rep["skipped"][0]["why"]

    rep = await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8], "why": "whatever"}])
    assert rep["dropped"] == 0, "an unknown class is not a class"

    rep = await dispose(actions, source=SEAT, drop=[
        {"id": str(t)[:8], "why": "stale", "because": "done six minutes later, same session"}])
    assert rep["dropped"] == 1 and rep["by_class"] == {"stale": 1}
    why = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='retracted_because'", t)
    assert why.startswith("STALE — ") and "same session" in why


async def test_a_drop_is_a_compensating_event_and_the_rug_is_TRANSPARENT(actions: Actions) -> None:
    """You may shove anything under the rug. The shape of what you shoved stays visible forever —
    the row, your name, and your reason. Never a DELETE (invariant 3)."""
    t = await _mined(actions, "thread:gone", "swept, but not erased")
    await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8], "why": "narration"}])

    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE id=$1", t) == 1
    row = await actions.pool.fetchrow(
        "SELECT (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='summary') AS summary, "
        "       (SELECT source_id FROM current_assertions WHERE object_id=$1 "
        "        AND name='retracted_because') AS who", t)
    assert row["summary"] == "swept, but not erased"     # the record never forgets
    assert row["who"] == SEAT, "every drop carries the name of the seat that signed it"


async def test_a_MIND_S_OWN_WORD_IS_NEVER_A_CANDIDATE(actions: Actions) -> None:
    """THE ABSOLUTE GUARD. A deliberate open_thread is a mind's promise. The seam has no standing
    over it — not at any age, not for any reason, not even from the seat that wrote it."""
    declared = await open_thread(actions, "a duty I declared myself", source=SEAT)

    assert (await candidates(actions.pool))["count"] == 0
    rep = await dispose(actions, source=SEAT, drop=[{"id": str(declared)[:8], "why": "narration"}])
    assert rep["dropped"] == 0 and "not a candidate" in rep["skipped"][0]["why"]

    status = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='status' "
        "ORDER BY confidence DESC LIMIT 1", declared)
    assert status == "open"


async def test_an_ADMIT_without_a_reason_is_refused(actions: Actions) -> None:
    """Admitting is a PROMISE — you are putting your name on a machine's guess. A promise with no
    stated reason is how a guess launders itself into a duty, which is the whole disease."""
    t = await _mined(actions, "thread:x", "something")
    rep = await dispose(actions, source=SEAT, admit=[{"id": str(t)[:8]}])
    assert rep["admitted"] == 0 and "because" in rep["skipped"][0]["why"]


async def test_candidates_scope_to_ONE_project_because_that_is_all_a_seat_has_standing_over(
    actions: Actions,
) -> None:
    """Only a seat with STANDING may judge a project's pile. A stranger disposing of another
    project's rows is the janitor acting on judgement instead of proof (operator, 2026-07-13:
    "this remediation thing should happen for each seat/agent in charge of the project")."""
    await _mined(actions, "thread:mine", "an osiris guess", repo="osiris")
    await _mined(actions, "thread:theirs", "someone else's guess", repo="other")

    mine = await candidates(actions.pool, project="osiris")
    assert mine["count"] == 1
    assert mine["candidates"][0]["summary"] == "an osiris guess"

    # the fleet total is a COUNT you may look at — not a pile you may touch
    assert (await candidates(actions.pool))["count"] == 2


async def test_candidates_limit_zero_skips_the_rows_query_but_keeps_the_true_count(
    actions: Actions,
) -> None:
    """Thread 72e45258 (measured): orient()'s own "your_pile" glance calls
    candidates(limit=0) for the count alone and never reads `candidates` — the rows query
    used to run anyway, correlated summary subquery included, purely to be discarded.
    `count` must still be exact with the rows fetch skipped."""
    await _mined(actions, "thread:mine1", "guess one", repo="osiris")
    await _mined(actions, "thread:mine2", "guess two", repo="osiris")

    out = await candidates(actions.pool, project="osiris", limit=0)

    assert out["count"] == 2
    assert out["candidates"] == []


async def test_the_YIELD_is_the_adversary_s_LICENCE(actions: Actions) -> None:
    """admitted ÷ judged. The one number nobody was keeping — a producer whose telemetry counts
    what it MADE rather than what was USED is unfalsifiable, and will rot unnoticed. Osiris's own
    first pass scored 26 of 264."""
    keep = await _mined(actions, "thread:gem", "the panopticon seam — flagged, then dropped")
    junk = [await _mined(actions, f"thread:junk{i}", f"work-step {i}") for i in range(4)]

    await dispose(
        actions, source=SEAT,
        admit=[{"id": str(keep)[:8], "because": "nobody has ever touched this and it is real"}],
        drop=[{"id": str(j)[:8], "why": "narration"} for j in junk])

    m = await adversary_yield(actions.pool)
    assert m["admitted"] == 1 and m["dropped"] == 4 and m["judged"] == 5
    assert m["yield"] == 0.2


async def test_disposing_twice_is_a_no_op_never_a_double_retraction(actions: Actions) -> None:
    """The seam is idempotent: a seat re-running its own pass, or two seats racing on one pile,
    must not double-write. The guard is checked per ROW at write time, not once at the top."""
    t = await _mined(actions, "thread:once", "drop me once")
    first = await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8], "why": "echo"}])
    again = await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8], "why": "echo"}])
    assert first["dropped"] == 1
    assert again["dropped"] == 0 and "not a candidate" in again["skipped"][0]["why"]


# --- THE LICENCE (B5) — the meter is not a dashboard, it is a GATE ------------------------

async def _judged(actions: Actions, *, admit: int, drop: int, v2: bool = True) -> None:
    """Walk the seam `admit` + `drop` times, so the meter has a real sample to read."""
    for i in range(admit):
        t = await _mined(actions, f"thread:a{i}", f"a real loose end {i}", v2=v2)
        await dispose(actions, source=SEAT,
                      admit=[{"id": str(t)[:8], "because": "real, and nobody wrote it down"}])
    for i in range(drop):
        t = await _mined(actions, f"thread:d{i}", f"work-step {i}", v2=v2)
        await dispose(actions, source=SEAT, drop=[{"id": str(t)[:8], "why": "narration"}])


async def test_a_producer_BELOW_THE_FLOOR_LOSES_THE_RIGHT_TO_SPEND(actions: Actions) -> None:
    """THE FIX FOR THE ACTUAL ROOT CAUSE.

    The miner's tick reported {"chunks": 12, "threads": 8} — WHAT IT MADE, never WHAT WAS USED.
    So a 90%-garbage producer and a 90%-gold producer emitted IDENTICAL telemetry, and nobody —
    not the operator, not the miner, not any mind reading the graph — could tell them apart. It
    drifted to garbage for eight days and $40 and NOTHING ANYWHERE COULD NOTICE.

    A producer that cannot be falsified will rot. Not might: WILL, because nothing pushes back.
    So the meter is a GATE, and this is it closing.
    """
    from src.orchestrator.dispose import YIELD_FLOOR, licence

    await _judged(actions, admit=4, drop=46)          # yield 0.08 — the crawl's own lifetime score
    lic = await licence(actions.pool)
    assert lic["yield"] == 0.08 and lic["yield"] < YIELD_FLOOR
    assert lic["may_spend"] is False
    assert "BELOW THE FLOOR" in lic["reason"]
    assert "no hands" in lic["reason"]                # it refuses; it never restarts itself


async def test_a_producer_that_EARNS_ITS_TOKENS_keeps_spending(actions: Actions) -> None:
    """The gate is a circuit breaker for a producer gone bad, not a performance target. An
    adversary that beats the thing we deleted keeps its licence."""
    from src.orchestrator.dispose import licence

    await _judged(actions, admit=20, drop=30)         # yield 0.4
    lic = await licence(actions.pool)
    assert lic["may_spend"] is True and lic["yield"] == 0.4


async def test_the_gate_cannot_fire_before_there_is_ANYTHING_TO_MEASURE(actions: Actions) -> None:
    """A producer is given a real sample before it is judged, or one unlucky session kills it —
    the same courtesy the wall now extends to a guess: judged on EVIDENCE, never on suspicion.

    And it fails OPEN: a metering bug must never silently disable the memory. It says WHY.
    """
    from src.orchestrator.dispose import LICENCE_MIN_JUDGED, licence

    lic = await licence(actions.pool)                 # nothing judged at all
    assert lic["may_spend"] is True and lic["judged"] == 0
    assert "given a real sample" in lic["reason"]

    await _judged(actions, admit=0, drop=5)           # yield 0.0 — but only 5 rows
    lic = await licence(actions.pool)
    assert lic["judged"] < LICENCE_MIN_JUDGED
    assert lic["may_spend"] is True, "a 0% yield over 5 rows is noise, not a verdict"


async def test_the_gate_JUDGES_THE_PRODUCER_THAT_EXISTS_not_its_dead_predecessor(
    actions: Actions,
) -> None:
    """A GATE THAT CAN NEVER OPEN IS A KILL SWITCH WEARING A GATE'S CLOTHES.

    I shipped the licence reading the FLEET-WIDE yield, ran it live, and it refused — on v1's
    0.098, computed over rows V1 MADE, against a v2 that had not yet written a single line. And
    v2 could never have raised that number, because it was not allowed to produce. The stated
    purpose (a circuit breaker) and the actual behaviour (a permanent lockout) differed, which is
    the same crime as everything else killed this week.

    The version marker is already in the data and costs nothing: the v2 adversary stamps
    `about_agent` (it speaks in its OWN name, ABOUT the agent); every v1 crawl row was sourced to
    the agent and carries no such field. So the gate reads the producer that actually exists.
    """
    from src.orchestrator.dispose import adversary_yield, licence

    # v1's sediment: 4 admits, 46 drops = 0.08, well under the floor, and NO about_agent
    await _judged(actions, admit=4, drop=46, v2=False)

    hist = await adversary_yield(actions.pool)
    assert hist["yield"] == 0.08, "the historical record must stay READABLE — it is what killed v1"

    lic = await licence(actions.pool)
    assert lic["may_spend"] is True, "v2 was condemned for a crime its predecessor committed"
    assert lic["judged"] == 0                       # its own record starts at zero, and it earns it
    assert "given a real sample" in lic["reason"]


async def test_ask_keeps_a_question_a_QUESTION_never_a_promise(actions: Actions) -> None:
    """THE THIRD VERB (Ra V's taxonomy gap, thread 4d01b076): dispose could ADMIT (a duty,
    mine) or DROP (never real) but not say 'this is a QUESTION'. Ra V admitted one question
    (it now reads as a promise it isn't) and dropped another as 'other'. `ask` keeps it
    open, kind='question', in the seat's name — the same grammar reclassify_thread speaks —
    and it counts as USE in the yield: the miner surfaced something a seat judged real."""
    t = await _mined(actions, "thread:xen-mesh",
                     "should the Xen mesh be the north star for the machine broker?")

    rep = await dispose(actions, source=SEAT, ask=[
        {"id": str(t)[:8], "because": "a real open question, not a duty",
         "owner": "operator"}])
    assert rep["asked"] == 1 and rep["yield"] == 1.0

    row = await actions.pool.fetchrow(
        "SELECT (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='kind' ORDER BY confidence DESC LIMIT 1) AS kind, "
        "       (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='status' ORDER BY confidence DESC LIMIT 1) AS status, "
        "       (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='asked_by') AS asked_by, "
        "       (SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "        AND name='owner') AS owner", t)
    assert row["kind"] == "question"        # on the wall AS what it is
    assert row["status"] == "open"          # never resolved, never retracted
    assert row["asked_by"] == SEAT          # the seat's word, countable by the meter
    assert row["owner"] == "operator"       # claimed in the same act

    # signed by a mind -> no longer a candidate; and asking twice is a no-op
    assert (await candidates(actions.pool))["count"] == 0
    again = await dispose(actions, source=SEAT, ask=[{"id": str(t)[:8]}])
    assert again["asked"] == 0 and again["skipped"][0]["why"] == "not a candidate"

    # the licence meter counts the ask as use
    m = await adversary_yield(actions.pool, current_producer_only=True)
    assert m["asked"] == 1 and m["yield"] == 1.0

async def test_the_HONEST_DENOMINATOR_forgives_the_public_retractor(actions: Actions) -> None:
    """THE METRIC'S FIFTH CORRECTION (Anubis XIII, thread 1258d382): raw admit-rate punishes
    a project that RETRACTS — the miner keeps filing tickets against work the project
    already buried, and every one drops as stale, so the honest repos read as the bad piles.
    A candidate born AFTER its project's last superseding ruling is a corpse at birth:
    excluded from the honest denominator the licence reads. Nothing hidden — raw stays
    reported beside it."""
    # a project that retracts publicly: an old decision, buried by a later ruling
    d = await actions.create_or_find_object("Decision", "decision:dead-lane", "agent:h")
    await link_repo(actions, d, "heinrich", NOW, source="agent:h",
                    evidence_class="self_declared")
    # two candidates: one born BEFORE the burial (a fair test), one after (a corpse)
    fair = await _mined(actions, "thread:pre", "the viz lane needs a colorbar",
                        repo="heinrich")
    corpse = await _mined(actions, "thread:post", "the viz lane needs axis labels",
                          repo="heinrich")
    born = {r["canonical"]: r["created_at"] for r in await actions.pool.fetch(
        "SELECT canonical, created_at FROM objects "
        "WHERE canonical IN ('thread:pre','thread:post')")}
    burial = born["thread:pre"] + (born["thread:post"] - born["thread:pre"]) / 2
    await actions.assert_property(d, "superseded_by", "abcd1234", "agent:h", burial, 0.9,
                                  evidence_class="self_declared")
    # the seat admits the fair one and drops the corpse as stale
    await dispose(actions, source=SEAT,
                  admit=[{"id": str(fair)[:8], "because": "real and unrecorded"}],
                  drop=[{"id": str(corpse)[:8], "why": "stale"}])

    m = await adversary_yield(actions.pool)
    assert m["judged"] == 2 and m["yield"] == 0.5          # raw: the punishing number
    assert m["corpse_excluded"] == 1 and m["judged_honest"] == 1
    assert m["yield_honest"] == 1.0                        # judged only where the test was fair

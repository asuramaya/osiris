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
                 repo: str | None = None) -> object:
    """A row the MINER wrote: DERIVED, unsigned by any mind."""
    o = await actions.create_or_find_object("Thread", canon, origin)
    await actions.assert_property(o, "summary", summary, origin, NOW, 0.4,
                                  evidence_class="derived")
    await actions.assert_property(o, "status", "open", origin, NOW, 0.4, evidence_class="derived")
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

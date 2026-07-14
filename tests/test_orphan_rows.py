"""THE ORPHAN ROW — a machine guess that belongs to no project, and therefore to nobody.

    "in the future, are orphans even possible with the changes? smells like it needs fixing
     first"                                                          — the operator, 2026-07-14

He was right, and the answer was yes. The whole remediation spec rests on one sentence — EVERY
PROJECT JUDGES ITS OWN MACHINE'S GUESSES, at its death rite, by the seat that made the mess — and
that sentence has a silent precondition nobody had checked: THAT EVERY GUESS HAS A PROJECT.

`mine_threads` (the git miner, in the PULSE daemon — a producer the miner kill-switch never
touched, still minting, and INVISIBLE TO EVERY GATE WE BUILT BECAUSE IT COSTS NOTHING) filed 25 of
its 26 threads under no repo at all. Those rows were structurally unreachable: on nobody's wall, in
nobody's queue, nobody's problem, forever. Not hard to reach — IMPOSSIBLE to reach.

A backlog you cannot assign is not a backlog. It is a landfill with a ticketing system.

And the owner was always one join away: every Commit already carries `in_repo`. The miner held the
answer in its hand at the moment of minting and never wrote it down.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.threads import mine_threads
from src.orchestrator.dispose import orphans

NOW = datetime(2026, 7, 14, tzinfo=UTC)


async def _commit(actions: Actions, sha: str, body: str, repo: str | None) -> uuid.UUID:
    c = await actions.create_or_find_object("Commit", f"commit:{sha}", "git-memory")
    await actions.assert_property(c, "rationale", body, "git-memory", NOW, 0.9,
                                  evidence_class="derived")
    await actions.assert_property(c, "authored_date", NOW.isoformat(), "git-memory", NOW, 0.9,
                                  evidence_class="derived")
    if repo:
        p = await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", "git-memory")
        await actions.create_link(c, p, "in_repo", "git-memory", NOW, 0.9,
                                  evidence_class="derived")
    return c


async def _repo_of(actions: Actions, canon_like: str) -> list[str]:
    return [r["p"] for r in await actions.pool.fetch(
        "SELECT p.canonical AS p FROM objects t "
        "JOIN links l ON l.from_id=t.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE t.type='Thread' AND t.canonical LIKE $1", canon_like)]


async def test_a_mined_thread_IS_FILED_under_its_commits_repo(actions: Actions) -> None:
    """THE FIX. The owner was always one join away — the miner just never wrote it down."""
    await _commit(actions, "aaa", "TODO: the renderer still needs a key", "osiris")
    out = await mine_threads(actions)
    assert out["threads"] >= 1
    assert await _repo_of(actions, "thread:%") == ["repo:osiris"]


# A body `extract_threads` ACTUALLY mints from. My first cut used prose it silently ignores, so
# the tripwire test passed with ZERO threads in the graph — green, and proving nothing. Anubis XII
# named this an hour earlier, in a letter I quoted approvingly: "A GREEN RESULT FROM A TEST THAT
# CANNOT FAIL CORRECTLY." Every test below now asserts the thread EXISTS before judging it.
_MINES = "TODO: the renderer still needs a key"


async def test_the_ORPHAN_TRIPWIRE_stays_at_zero(actions: Actions) -> None:
    """NOT A CLEANUP TOOL — A TRIPWIRE. It names, loudly, any producer that mints a row it cannot
    name an owner for. The next such producer will not announce itself either, and the only reason
    we caught THIS one is that the operator refused to accept a sweep as an answer."""
    await _commit(actions, "bbb", _MINES, "osiris")
    assert (await mine_threads(actions))["threads"] == 1     # or the check below proves nothing
    got = await orphans(actions.pool)
    assert got["orphans"] == 0
    assert "every machine guess has an owner" in got["verdict"]


async def test_a_row_with_NO_OWNER_is_NAMED_and_its_producer_with_it(actions: Actions) -> None:
    """The regression guard. A commit with no repo (or any future producer that forgets) mints a
    row NO SEAT HAS STANDING OVER — and the disposal ritual can never reach it. That must be
    impossible to do QUIETLY."""
    await _commit(actions, "ccc", _MINES, repo=None)
    assert (await mine_threads(actions))["threads"] == 1
    got = await orphans(actions.pool)
    assert got["orphans"] == 1
    assert got["by_producer"][0]["source_id"] == "git-memory"
    assert "NO seat can ever dispose them" in got["verdict"]
    assert "must be made to name an owner, not the pile swept" in got["verdict"]


async def test_a_thread_a_MIND_touched_is_never_an_orphan_candidate(actions: Actions) -> None:
    """The absolute guard, carried over from the janitor (6177a69): the instant a mind lays a
    self_declared assertion on a row IT IS THAT MIND'S BUSINESS, forever. An orphan sweep that
    could reach a human's own work would be a censor, not a cleaner."""
    await _commit(actions, "ddd", _MINES, repo=None)
    assert (await mine_threads(actions))["threads"] == 1
    assert (await orphans(actions.pool))["orphans"] == 1

    t = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Thread' AND canonical LIKE 'thread:%' LIMIT 1")
    await actions.assert_property(t, "status", "open", "agent:someone", NOW, 0.9,
                                  evidence_class="self_declared")
    assert (await orphans(actions.pool))["orphans"] == 0, "a mind had touched it; it was not ours"

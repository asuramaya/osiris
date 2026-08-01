"""THE CLOSURE MINER (operator ruling, 2026-07-12): a commit may witness that a transcript's
promise was kept — the one sanctioned crossing of the ownership boundary — but SPLIT BY
CONFIDENCE, and never over a mind's own declaration.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.ingest.closure import close_by_commits
from src.orchestrator.capture import open_thread

BORN = datetime(2026, 7, 1, tzinfo=UTC)
LATER = datetime(2026, 7, 5, tzinfo=UTC)


async def _tree(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:cl", "session")
    await actions.assert_property(proj, "name", "cl", "session", BORN, 0.9)

    async def mined_thread(canon: str, summary: str) -> str:
        t = await actions.create_or_find_object("Thread", canon, "session-miner")
        await actions.assert_property(t, "summary", summary, "session-miner", BORN, 0.4,
                                      evidence_class="derived")
        await actions.assert_property(t, "status", "open", "session-miner", BORN, 0.4,
                                      evidence_class="derived")
        await actions.create_link(t, proj, "in_repo", "session-miner", BORN, 0.4,
                                  evidence_class="derived")
        await actions.pool.execute(
            "UPDATE objects SET created_at=$2 WHERE id=$1", t, BORN)
        return str(t)

    async def commit(canon: str, subject: str, rationale: str = "") -> None:
        c = await actions.create_or_find_object("Commit", canon, "git")
        await actions.assert_property(c, "subject", subject, "git", LATER, 0.9)
        if rationale:
            await actions.assert_property(c, "rationale", rationale, "git", LATER, 0.9)
        await actions.assert_property(c, "authored_date", LATER.isoformat(), "git", LATER, 0.9)
        await actions.create_link(c, proj, "in_repo", "git", LATER, 0.9)

    # STRONG: four+ rare words shared with a later commit
    await mined_thread("thread:strong",
                       "wire the precompact hook so compaction triggers an immediate sweep")
    # WEAK: a two-word brush — real enough to ask about, never enough to assert
    await mined_thread("thread:weak", "investigate the compaction sweep latency")
    # NO EVIDENCE at all
    await mined_thread("thread:none", "buy the operator a birthday cake")
    await commit("commit:aaa",
                 "feat(precompact): wire the precompact hook to trigger an immediate sweep",
                 "compaction now triggers the sweep before the context dies")


async def test_only_a_commit_that_NAMES_the_thread_may_close_it(actions: Actions) -> None:
    """WHAT TWO DRY RUNS PROVED, against this module's own first design. Lexical similarity —
    even rarity-weighted — is NOT evidence that work was done: two texts about one codebase share
    vocabulary because they are about one codebase. On real data it wanted to close "The daemon's
    structural assignment" against a census-encoder commit on the strength of `bearing, load`.

    So the automatic lane admits exactly ONE witness: a commit that NAMES THE THREAD BY ID. A mind
    typed that id on purpose. Everything else asks."""
    await _tree(actions)
    tid = str(await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='thread:strong'"))
    # a commit that cites the thread by its short id — the one unambiguous witness
    c = await actions.create_or_find_object("Commit", "commit:cited", "git")
    await actions.assert_property(c, "subject", f"fix: close {tid[:8]} at last", "git", LATER, 0.9)
    await actions.assert_property(c, "authored_date", LATER.isoformat(), "git", LATER, 0.9)
    await actions.create_link(
        c, await actions.create_or_find_object("SoftwareProject", "repo:cl", "session"),
        "in_repo", "git", LATER, 0.9)

    out = await close_by_commits(actions, repo="cl", dry_run=False, strong=2.0, weak=0.4)
    assert out["resolved"] == 1                       # ONLY the id-cited one
    assert out["candidates"] >= 1                     # the lexical matches merely ASK

    tid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='thread:strong'")
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
    because = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='resolved_because' ORDER BY a.observed_at DESC LIMIT 1", tid)
    assert status == "resolved"
    assert "names this thread by id" in because
    # and it is signed by the crossing itself, never smuggled under the session-miner's name
    src = await actions.pool.fetchval(
        "SELECT a.source_id FROM assertions a WHERE a.object_id=$1 AND a.name='status' "
        "AND a.value #>> '{}' = 'resolved' ORDER BY a.created_at DESC LIMIT 1", tid)
    assert src == "closure-miner"
    # THE MISSING EDGE, FIXED (Thoth DM 2581, decision cb38d922/fc5b6c5f): this is the ONLY
    # strong verdict path (a commit literally citing the thread's short id), so it now mints
    # the same traversable resolved_by witness resolve_thread(artifact=...) mints — this
    # closure must no longer be invisible to topology-derived reads.
    edge = await actions.pool.fetchrow(
        "SELECT to_id, source_id FROM links WHERE from_id=$1 AND type='resolved_by'", tid)
    assert edge is not None
    assert str(edge["to_id"]) == str(c)
    assert edge["source_id"] == "closure-miner"


async def test_even_a_word_for_word_match_only_asks(actions: Actions) -> None:
    """thread:strong shares SIX rare terms with a later commit and reads like an obvious match —
    and it still only ASKS. That is the whole ruling: a false "done" in an append-only record
    outlives everyone who could remember it was false, so lexical evidence never asserts."""
    await _tree(actions)
    await close_by_commits(actions, repo="cl", dry_run=False, strong=2.0, weak=0.4)
    tid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='thread:strong'")
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
    rot = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='rot_candidate' ORDER BY a.observed_at DESC LIMIT 1", tid)
    assert status == "open"                       # untouched != resolved (758ded94)
    assert rot and "may have done this" in rot    # it ASKS, with its reason attached
    assert "likely" in rot                        # and hedges honestly about how sure it is
    # a thread with no evidence is left entirely alone — silence is not a verdict
    none_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='thread:none'")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name='rot_candidate'", none_id) == 0


async def test_a_mind_that_touched_it_is_never_overruled_by_a_git_log(actions: Actions) -> None:
    """THE HARD GUARD. A guess may be swept by evidence; a DECLARATION is answered only by its
    owner. Even with a commit that names it word for word, a self_declared thread is untouched —
    this is the line that makes crossing the ownership boundary safe at all."""
    await _tree(actions)
    declared = await open_thread(
        actions, "wire the precompact hook so compaction triggers an immediate sweep",
        repo="cl", kind="obligation", source="agent:someone")
    await actions.pool.execute("UPDATE objects SET created_at=$2 WHERE id=$1", declared, BORN)

    await close_by_commits(actions, repo="cl", dry_run=False, strong=2.0, weak=0.4)
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", declared)
    assert status == "open"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name IN ('rot_candidate','resolved_because')", declared) == 0


async def test_dry_run_is_the_default_and_writes_nothing(actions: Actions) -> None:
    """A sweep that can close hundreds of threads defaults to telling you what it WOULD do."""
    await _tree(actions)
    out = await close_by_commits(actions, repo="cl", strong=2.0, weak=0.4)  # no dry_run=
    assert out["dry_run"] is True and out["candidates"] >= 1
    tid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='thread:strong'")
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)
    assert status == "open"


async def test_only_a_later_commit_can_witness_a_promise(actions: Actions) -> None:
    """A commit that predates the thread cannot have done it — that is a coincidence of
    vocabulary, and reading it as proof would close work that was never started."""
    await _tree(actions)
    # thread:strong shares six rare terms with the commit — but it is now born a MONTH AFTER it,
    # so the commit cannot possibly be its witness. It must vanish from the report entirely, not
    # merely be downgraded: work that had not yet started cannot already be done.
    await actions.pool.execute(
        "UPDATE objects SET created_at=$1 WHERE canonical='thread:strong'",
        LATER + timedelta(days=30))
    out = await close_by_commits(actions, repo="cl", strong=2.0, weak=0.4)
    assert out["resolved"] == 0
    reported = {r["summary"] for r in out["candidate_rows"]}
    assert not any("precompact hook" in s for s in reported)
    # thread:weak still predates the commit, so it is still legitimately asked about
    assert any("compaction sweep latency" in s for s in reported)


def test_rarity_outranks_volume_which_is_the_whole_point() -> None:
    """THE DRY RUN'S OWN VERDICT (2026-07-12). The first matcher counted shared "distinctive"
    tokens and promptly tried to close an unrelated thread on the evidence of four of them:
    `predicts, rather, than, where`. A hand-kept stopword list is a hardcode and it will always
    leak. So the corpus grades its own vocabulary: FIVE words that appear everywhere must never
    outvote TWO that appear nowhere else."""
    from src.ingest.closure import build_idf

    ubiquitous = ("rather", "than", "where", "predicts", "readout")   # the very words that lied
    corpus = [" ".join(ubiquitous) + f" filler{i}" for i in range(40)]
    corpus += ["quokka binding organ", "quokka binding organ again"]
    idf = build_idf(corpus)
    common = sum(idf[t] for t in ubiquitous)
    rare = sum(idf[t] for t in ("quokka", "organ"))
    assert common < 0.3               # five ubiquitous words carry almost no information at all
    assert rare > common * 15         # two fingerprints outweigh all five, by a mile

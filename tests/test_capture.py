"""Capture-at-source — decisions & threads written back DURING a session.

The prosthesis write-back half: what a session decides / leaves open must land in the
graph in the SAME shape the miner produces, so it renders in the real `decision-log` and
`briefing` compositions beside mined items. These tests drive the ACTUAL compositions
(seeded from the defaults), never a hand-rolled query, so a shape mismatch fails here.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision, resolve_thread
from src.orchestrator.compositions import run_composition, seed_default_compositions

_DECISION_LOG = "Decisions — the project's WHY (mined from commit rationale)"
_OPEN = "Open threads — what's unresolved"
_RESOLVED = "Resolved — self-healed by later commits"


async def test_record_decision_renders_in_the_decision_log(actions: Actions) -> None:
    await record_decision(actions, "We event-source merges", kind="ruling",
                          rationale="object_events is the truth; status is a projection")
    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "decision-log")
    rows = next(iter(res["items"].values()))
    row = next(r for r in rows if r["decision"] == "We event-source merges")
    assert row["kind"] == "ruling"
    # a session decision has no decided_in commit → the log's "in"/"when" columns are empty,
    # gracefully (the whole point of linking in_repo, not decided_in).
    assert row["in"] is None and row["when"] is None


async def test_captured_decision_is_self_declared_from_the_session(actions: Actions) -> None:
    d = await record_decision(actions, "Never auto-merge Person")
    row = await actions.pool.fetchrow(
        "SELECT source_id, evidence_class FROM current_assertions "
        "WHERE object_id=$1 AND name='summary'", d,
    )
    # higher-trust than the miner's DERIVED regex inference — the decider's own declaration
    assert row["source_id"] == "session"
    assert row["evidence_class"] == "self_declared"


async def test_record_decision_is_idempotent_on_the_summary(actions: Actions) -> None:
    d1 = await record_decision(actions, "Keyless is a safety feature, not a limitation")
    d2 = await record_decision(actions, "Keyless is a safety feature, not a limitation")
    assert d1 == d2
    assert await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Decision'") == 1


async def test_record_decision_with_repo_links_in_repo_and_still_renders(
    actions: Actions,
) -> None:
    d = await record_decision(actions, "The composer is the front end, not a page",
                              kind="reset", repo="osiris")
    proj = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='in_repo'", d,
    )
    assert proj is not None
    assert await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", proj) == "repo:osiris"
    # re-capture with the same repo must not duplicate the edge (idempotent)
    await record_decision(actions, "The composer is the front end, not a page",
                          kind="reset", repo="osiris")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", d) == 1
    # still renders in the log, in/when empty (linked in_repo, not decided_in)
    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "decision-log")
    rows = next(iter(res["items"].values()))
    row = next(r for r in rows if r["decision"] == "The composer is the front end, not a page")
    assert row["kind"] == "reset" and row["in"] is None and row["when"] is None


async def test_open_thread_surfaces_in_the_briefing_open_section(actions: Actions) -> None:
    await open_thread(actions, "wire the composed watcher into SOURCE_TICKS with a live key")
    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "briefing")
    open_rows = res["items"][_OPEN]
    assert any(
        r["thread"] == "wire the composed watcher into SOURCE_TICKS with a live key"
        for r in open_rows
    )


async def test_resolve_thread_leaves_open_and_joins_resolved(actions: Actions) -> None:
    summary = "prune the internal-URL spread in url_fetch to profile-shaped only"
    t = await open_thread(actions, summary)
    # resolve by summary SUBSTRING (not the whole thing) — the ref is fuzzy
    tid = await resolve_thread(actions, "internal-URL spread", because="tightened url_fetch")
    assert tid == t

    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "briefing")
    open_rows = res["items"][_OPEN]
    resolved_rows = res["items"][_RESOLVED]
    # left the open list (status superseded within-source), joined the resolved section
    assert not any(r["thread"] == summary for r in open_rows)
    resolved = next(r for r in resolved_rows if r["thread"] == summary)
    assert resolved["by"] == "session"        # a session closed it, not a later commit
    assert resolved["because"] == "tightened url_fetch"


async def test_resolve_thread_returns_none_when_nothing_matches(actions: Actions) -> None:
    assert await resolve_thread(actions, "no such thread anywhere") is None

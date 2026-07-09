"""Capture-at-source — decisions & threads written back DURING a session.

The prosthesis write-back half: what a session decides / leaves open must land in the
graph in the SAME shape the miner produces, so it renders in the real `decision-log` and
`briefing` compositions beside mined items. These tests drive the ACTUAL compositions
(seeded from the defaults), never a hand-rolled query, so a shape mismatch fails here.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.mcp_server import _project_briefing
from src.orchestrator.capture import (
    link_repo,
    open_thread,
    record_decision,
    record_tension,
    resolve_thread,
)
from src.orchestrator.compositions import _props, run_composition, seed_default_compositions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

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


async def test_status_resolution_honors_grade_over_recency(actions: Actions) -> None:
    """A thread's status, on every read surface, is the WINNING assertion by evidence GRADE —
    not 'any source says open', not most-recent-wins. The miner opens a thread (DERIVED), a
    session resolves it (SELF_DECLARED), then the miner RE-OPENS it later (DERIVED, the
    freshest status). The session's higher-grade resolution must win. Regression for the 15
    threads that sat open-AND-resolved: orient used EXISTS(status='open'), and the composer's
    _props ordered by recency alone — so a fresh DERIVED re-open would have buried the close."""
    now = datetime.now(UTC)
    d_ec, d_conf = EvidenceClass.DERIVED.value, confidence_for(EvidenceClass.DERIVED)
    summary = "the miner-opened thread a session then closed"

    # the miner opens it — DERIVED, an older observation, filed under osiris
    t = await actions.create_or_find_object("Thread", "thread:graderegress", "session-miner")
    await actions.assert_property(t, "summary", summary, "session-miner",
                                  now - timedelta(days=2), d_conf, evidence_class=d_ec)
    await actions.assert_property(t, "status", "open", "session-miner",
                                  now - timedelta(days=2), d_conf, evidence_class=d_ec)
    await link_repo(actions, t, "osiris", now - timedelta(days=2),
                    source="session-miner", evidence_class=d_ec, confidence=d_conf)

    # a session resolves it — SELF_DECLARED, through the real capture path
    assert await resolve_thread(actions, "miner-opened thread a session", because="handled") == t

    # the miner re-senses and RE-OPENS it — DERIVED, now the most recent status assertion
    await actions.assert_property(t, "status", "open", "session-miner",
                                  now + timedelta(days=365), d_conf, evidence_class=d_ec)

    # 1) the composer's resolver picks the higher grade, not the fresher timestamp
    assert (await _props(actions.pool, t))["status"] == "resolved"

    # 2) the briefing composition — gone from open, present in resolved
    await seed_default_compositions(actions.pool)
    res = await run_composition(actions.pool, "briefing")
    assert not any(r["thread"] == summary for r in res["items"][_OPEN])
    assert any(r["thread"] == summary for r in res["items"][_RESOLVED])

    # 3) the scoped orient briefing (bespoke SQL, a distinct read path) also excludes it
    scoped = await _project_briefing(actions.pool, "osiris")
    assert scoped is not None
    assert not any(r["summary"] == summary for r in scoped["open_threads"])


async def test_record_tension_holds_a_polarity_and_moves_the_lean(actions: Actions) -> None:
    a = "bounded recall — memory is a query"
    b = "complete memory — never lose anything"
    t1 = await record_tension(actions, a, b, lean="bounded, for now", why="cargo problem",
                              repo="osiris")
    props = await _props(actions.pool, t1)
    assert props["pole_a"] == a and props["pole_b"] == b and props["lean"] == "bounded, for now"
    # re-holding the same poles (EITHER order) MOVES the lean — one object, the dance
    t2 = await record_tension(actions, b, a, lean="leaning complete after the leak")
    assert t2 == t1
    assert (await _props(actions.pool, t1))["lean"] == "leaning complete after the leak"
    # it is a Tension, never a Thread/Decision — grade-resolution & consolidation can't reach it
    assert await actions.pool.fetchval("SELECT type FROM objects WHERE id=$1", t1) == "Tension"


async def test_tension_surfaces_in_the_scoped_briefing(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await actions.assert_property(proj, "name", "demo", "session", datetime.now(UTC), 0.9)
    await record_tension(actions, "speed", "safety", lean="safety", repo="demo")
    await seed_default_compositions(actions.pool)
    items = (await run_composition(actions.pool, "project-briefing", proj))["items"]
    assert "tensions" in items
    assert ("speed", "safety") in [(r["pole_a"], r["pole_b"]) for r in items["tensions"]]


async def test_orient_explicit_project_overrides_the_mount(actions: Actions) -> None:
    """Heinrich's verified bug: orient(project=X) silently returned the MOUNT's briefing instead
    of X's — a silent wrong-scope (the confound class the fleet exists to catch). An explicit
    project must OVERRIDE the mount."""
    from src.mcp_server import _agents, _conn_key, orient
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.compositions import seed_default_compositions

    now = datetime.now(UTC)
    dec = await actions.create_or_find_object("SoftwareProject", "repo:decepticons", "session")
    await actions.assert_property(dec, "name", "decepticons", "session", now, 0.9)
    th = await actions.create_or_find_object("Thread", "thread:dec-scope", "session")
    await actions.assert_property(th, "summary", "the decepticons-only thread", "session", now, 0.9)
    await actions.assert_property(th, "status", "open", "session", now, 0.9)
    await actions.create_link(th, dec, "in_repo", "session", now, 0.9)
    await seed_default_compositions(actions.pool)

    class _Ctx:  # minimal fake connection ctx — _conn_key reads id(request_context.session)
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(   # mounted as heinrich...
        agent_id="agent:heinX", session="heinX", project="heinrich", model=None, cwd=None)
    try:
        res = await orient(project="decepticons", ctx=ctx)   # ...but explicitly asks decepticons
    finally:
        _agents.pop(_conn_key(ctx), None)
    assert res["project"] == "decepticons"                   # honored the explicit scope
    assert "the decepticons-only thread" in [r["summary"] for r in res["open_threads"]]


def test_rank_open_threads_orders_obligations_first_and_caps() -> None:
    """The pure ranker (orient's wall → a bounded query): obligations float above ordinary
    threads, the composition's recency order is preserved WITHIN each group (stable sort),
    summary-less rows drop, and the display caps with an accurate remainder count."""
    from src.mcp_server import _ORIENT_OPEN_THREADS, _rank_open_threads

    # input arrives recency-desc from the composition; obligations are interleaved
    rows = [
        {"summary": "ordinary-A", "kind": None},
        {"summary": "obligation-A", "kind": "obligation"},
        {"summary": "ordinary-B", "kind": None},
        {"summary": "obligation-B", "kind": "obligation"},
        {"summary": "", "kind": None},                          # no summary → dropped
        {"summary": "ordinary-C", "kind": None},
    ]
    shown, more = _rank_open_threads(rows)
    assert [r["summary"] for r in shown] == [
        "obligation-A", "obligation-B",              # duties first, input (recency) order kept
        "ordinary-A", "ordinary-B", "ordinary-C",    # then the rest, input order kept
    ]
    assert more == 0

    # capping: more rows than the display limit → the cap is shown, the remainder counted
    many = [{"summary": f"t{i:02d}", "kind": None} for i in range(_ORIENT_OPEN_THREADS + 5)]
    shown, more = _rank_open_threads(many)
    assert len(shown) == _ORIENT_OPEN_THREADS and more == 5
    assert [r["summary"] for r in shown] == [f"t{i:02d}" for i in range(_ORIENT_OPEN_THREADS)]


async def test_orient_briefing_ranks_obligations_first_and_caps(actions: Actions) -> None:
    """End to end through the real composition: orient's open_threads floats a DUTY above
    ordinary threads even when it is the LEAST recent, caps the wall at the display limit, and
    notes the remainder — a bounded, ranked query, not a 500-line scroll. Ranking only."""
    import uuid

    from src.mcp_server import _ORIENT_OPEN_THREADS, _project_briefing

    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:ranktest", "session")
    await actions.assert_property(proj, "name", "ranktest", "session", now, 0.9)

    async def _thread(canon: str, summary: str, *, kind: str | None = None) -> uuid.UUID:
        t = await actions.create_or_find_object("Thread", canon, "session")
        await actions.assert_property(t, "summary", summary, "session", now, 0.9)
        await actions.assert_property(t, "status", "open", "session", now, 0.9)
        if kind:
            await actions.assert_property(t, "kind", kind, "session", now, 0.9)
        await actions.create_link(t, proj, "in_repo", "session", now, 0.9)
        return t

    duty = await _thread("thread:duty", "restart the daemons after the kernel change",
                         kind="obligation")
    # force the obligation to be the LEAST recent — only ranking can float it to the top
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '400 days' WHERE id=$1", duty)
    for i in range(_ORIENT_OPEN_THREADS + 5):  # comfortably over the display cap
        await _thread(f"thread:ord-{i:02d}", f"ordinary next-step number {i:02d}")

    await seed_default_compositions(actions.pool)  # orient runs the project-briefing composition
    scoped = await _project_briefing(actions.pool, "ranktest")
    assert scoped is not None
    open_threads = scoped["open_threads"]

    assert len(open_threads) == _ORIENT_OPEN_THREADS               # display-capped
    assert open_threads[0]["kind"] == "obligation"                # the duty leads despite age
    assert open_threads[0]["summary"] == "restart the daemons after the kernel change"
    assert all(r.get("kind") != "obligation" for r in open_threads[1:])  # only the one obligation
    # 1 duty + (cap + 5) ordinary = cap + 6 total; cap shown → 6 more, surfaced in the note
    assert scoped["open_threads_note"] and "6 more" in scoped["open_threads_note"]


async def test_record_decision_is_atomic_no_orphan_husk(actions: Actions) -> None:
    """The write-integrity fix (rotten-apple audit): record_decision was five sequential
    transactions — a process death between the create and its summary left an orphan Decision
    with no body. Now it is ONE transaction: force a failure mid-sequence and prove NOTHING
    persists — not even the object husk."""
    import pytest
    from src.orchestrator.capture import record_decision

    # a rationale that isn't a string blows up assert_property's JSON path AFTER the object +
    # summary would have been created — the exact mid-sequence crash the husk came from
    class Boom:
        pass

    with pytest.raises(Exception):  # noqa: B017 — any failure; the point is the rollback
        await record_decision(actions, "a decision that must not half-land",
                              rationale=Boom())  # type: ignore[arg-type]
    # the whole transaction rolled back: no Decision object, no husk, no summary
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Decision'") == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions WHERE name='summary'") == 0


async def test_atomic_context_shares_one_transaction(actions: Actions) -> None:
    """atomic() binds one connection: a create + assert inside it are invisible until commit
    and fully present after — the primitive record_decision/open_thread now stand on."""
    async with actions.atomic() as a:
        d = await a.create_or_find_object("Decision", "decision:atomic-probe", "agent:t")
        await a.assert_property(d, "summary", "committed together", "agent:t",
                                __import__("datetime").datetime.now(__import__("datetime").UTC),
                                0.9, evidence_class="self_declared")
    # after the block: both the object and its summary are present
    row = await actions.pool.fetchrow(
        "SELECT o.id, (SELECT value#>>'{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='summary') AS summary "
        "FROM objects o WHERE o.canonical='decision:atomic-probe'")
    assert row is not None and row["summary"] == "committed together"

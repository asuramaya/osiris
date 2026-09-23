"""Wall composition tests: the graded wall as a composition, the console's
triage verbs, and the shelf metadata. The old 919-raw-rows briefing is gone."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator.capture import open_thread
from src.orchestrator.compositions import (
    list_compositions,
    open_thread_wall,
    rank_open_threads,
    run_composition,
    seed_default_compositions,
)

NOW = datetime(2026, 7, 11, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _project_with_threads(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session")
    await actions.assert_property(proj, "name", "walltest", "session", NOW, 0.9)
    await open_thread(actions, "a duty someone owes", repo="walltest",
                      kind="obligation", source="agent:me")
    await open_thread(actions, "an operator blocker", repo="walltest",
                      kind="obligation", owner="operator", source="agent:me")
    # a miner echo: DERIVED-only and old, so it moves off the wall into the pile
    t = await actions.create_or_find_object("Thread", "thread:echo-old", "session-miner")
    await actions.assert_property(t, "summary", "an ancient mined commitment",
                                  "session-miner", NOW, 0.4, evidence_class="derived")
    await actions.assert_property(t, "status", "open", "session-miner", NOW, 0.4,
                                  evidence_class="derived")
    await actions.create_link(
        t, await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session"),
        "in_repo", "session-miner", NOW, 0.4, evidence_class="derived")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1", t)


async def test_the_wall_lens_grades_a_project(actions: Actions) -> None:
    """Project-scoped: obligations ride (operator-owned last); the old echo collapses into
    a counted pile, the same rule orient enforces, now as a composition."""
    await _project_with_threads(actions)
    await seed_default_compositions(actions.pool)
    proj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:walltest'")
    out = (await run_composition(actions.pool, "the-wall", proj))["items"]
    walls = [w["summary"] for w in out["wall"]]
    # the seeded lens is OPERATOR-EYED (args me=['operator']): his blockers are his own
    # moves and ride first; recency breaks the tie with the unowned duty
    assert set(walls) == {"an operator blocker", "a duty someone owes"}
    assert walls[0] == "an operator blocker"
    assert out["echo_pile"]["count"] == 1
    assert "ancient mined commitment" not in walls


async def test_the_fleet_briefing_shows_counts_never_the_scroll(actions: Actions) -> None:
    """Subject-less (the console's default briefing): a per-project rollup plus top
    obligations. The 919-row raw select is gone from the briefing composition."""
    await _project_with_threads(actions)
    await seed_default_compositions(actions.pool)
    out = (await run_composition(actions.pool, "briefing"))["items"]
    wall = next(iter(out.values()))  # first section = the wall
    assert wall["totals"]["open"] == 3 and wall["totals"]["obligations"] == 2
    assert wall["totals"]["pile"] == 1
    proj_row = next(p for p in wall["projects"] if p["project"] == "repo:walltest")
    assert proj_row["pile"] == 1 and proj_row["obligations"] == 2
    tops = [t["summary"] for t in wall["top_of_wall"]]
    assert "a duty someone owes" in tops
    # never a raw thread scroll: the section is a dict of counts, not 900 rows
    assert "wall" not in out or isinstance(wall, dict)


async def test_triage_route_writes_as_the_operator(
        actions: Actions, client: httpx.AsyncClient) -> None:
    """The console's triage verbs (the operator's ruling): resolve closes through the
    Actions waist signed analyst:operator; reclassify adopts an echo as owed work; a
    bogus ref reports a miss instead of failing the batch."""
    await _project_with_threads(actions)
    echo_id = str(await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='thread:echo-old'"))
    r = await client.post("/threads/triage", json={
        "ids": [echo_id, "zzzz-nothing"], "verb": "resolve",
        "because": "operator triage: done weeks ago"})
    body = r.json()
    assert body["acted"] == 1 and body["missed"] == 1
    src = await actions.pool.fetchval(
        "SELECT a.source_id FROM assertions a WHERE a.object_id=$1::uuid "
        "AND a.name='status' AND a.value #>> '{}' = 'resolved' "
        "ORDER BY a.created_at DESC LIMIT 1", echo_id)
    assert src == "analyst:operator"
    # reclassify lane: adopt a thread as an obligation without touching status
    duty = await actions.pool.fetchval(
        "SELECT o.id::text FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE a.name='summary' AND a.value #>> '{}' = 'a duty someone owes'")
    r2 = await client.post("/threads/triage", json={
        "ids": [duty[:8]], "verb": "question", "because": "not work after all"})
    assert r2.json()["acted"] == 1
    # bad verb refused
    r3 = await client.post("/threads/triage", json={"ids": [duty], "verb": "delete"})
    assert "error" in r3.json()


async def test_the_four_doors_off_the_operators_desk(
        actions: Actions, client: httpx.AsyncClient) -> None:
    """Debts used to have only two exits from a project's queue: do it, or let it rot,
    which let the backlog grow without bound. Now there are four:

      done      -> resolved, leaves the record's open set
      NOT MINE  -> owner becomes the project that owes it; STILL OPEN, just no longer assigned
      later     -> deferred_until stamps a date; the lens hides it, the record keeps it open
      (reclassify -> it isn't the kind of thing it claimed to be)

    The two new doors must never lie about status (untouched does not mean resolved); that
    is what separates a hand-back from a quiet delete."""
    from src.orchestrator.mailbox import read_desk
    await _project_with_threads(actions)

    async def owner_of(tid: str) -> str | None:
        return await actions.pool.fetchval(  # type: ignore[no-any-return]
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1::uuid "
            "AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)

    async def status_of(tid: str) -> str | None:
        return await actions.pool.fetchval(  # type: ignore[no-any-return]
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1::uuid "
            "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", tid)

    blocker = str(await actions.pool.fetchval(
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE a.name='summary' AND a.value #>> '{}' = 'an operator blocker'"))
    assert await owner_of(blocker) == "operator"       # it starts on the human

    # not mine: hand it back to the project. Open in the record, off the desk.
    r = await client.post("/threads/triage", json={
        "ids": [blocker[:8]], "verb": "assign", "owner": "walltest",
        "because": "not mine: walltest owns this"})
    assert r.json()["acted"] == 1
    assert await owner_of(blocker) == "walltest"
    assert await status_of(blocker) == "open"          # NOT a resolve, and never pretends to be

    def queued(desk: dict) -> set[str]:
        return {t["id"] for t in (desk.get("your_queue") or {}).get("threads", [])}

    assert blocker[:8] not in queued(await read_desk(actions.pool))   # gone from HIS queue...
    # ...and now on walltest's wall, where orient() will hand it to their next living mind
    wall, _ = await open_thread_wall(actions.pool, await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:walltest'"))
    assert blocker[:8] in {str(w["id"])[:8] for w in wall}

    # later: mine, but not now. Hidden at the lens, untouched in the record.
    duty = str(await actions.pool.fetchval(
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE a.name='summary' AND a.value #>> '{}' = 'a duty someone owes'"))
    await client.post("/threads/triage", json={"ids": [duty], "verb": "assign",
                                               "owner": "operator", "because": "mine"})
    assert duty[:8] in queued(await read_desk(actions.pool))
    r2 = await client.post("/threads/triage", json={
        "ids": [duty], "verb": "defer", "days": 30, "because": "operator: not now"})
    assert r2.json()["acted"] == 1
    assert duty[:8] not in queued(await read_desk(actions.pool))
    assert await status_of(duty) == "open"             # deferral is a LENS act, never a close
    assert await owner_of(duty) == "operator"          # still assigned to operator, just not today

    # assign without an owner is refused, not silently applied
    assert "error" in (await client.post("/threads/triage", json={
        "ids": [duty], "verb": "assign"})).json()


async def test_the_desk_counts_debts_apart_from_letters_and_groups_by_project(
        actions: Actions, client: httpx.AsyncClient) -> None:
    """The old combined "red 11" count was misleading: it summed debts the operator owed
    with condolence letters that owed nothing. `owed` counts only open threads owned by the
    operator; `letters` counts fyi briefs, which clear in bulk through the operator's own
    click, since an agent still cannot settle this desk."""
    from src.orchestrator.mailbox import read_desk, send_message
    await _project_with_threads(actions)
    await send_message(actions.pool, from_agent="agent:ghost", from_project="walltest",
                       to_project="operator", desk_kind="fyi",
                       body="retirement letter: it was a good life")
    await send_message(actions.pool, from_agent="agent:ghost", from_project="walltest",
                       to_project="operator", desk_kind="hands",
                       body="AUTHORIZE the deploy, blocked on you")
    desk = await read_desk(actions.pool)
    assert desk["owed"] == 1 and desk["letters"] == 1      # one operator-owned thread, one letter
    proj = next(p for p in desk["by_project"] if p["project"] == "walltest")
    assert proj["owed"] == 1 and len(proj["asks"]) == 1    # the debt AND who asked, together
    assert all("retirement letter" not in (a.get("body") or "") for a in proj["asks"])

    # the human's own click dismisses; the count falls
    letter = desk["fyi"][0]["id"]
    r = await client.post("/desk/settle", json={"ids": [letter]})
    assert r.json()["settled"] == 1
    assert (await read_desk(actions.pool))["letters"] == 0


async def test_the_shelf_metadata_reaches_the_list(actions: Actions) -> None:
    """Compositions say what they are: section + description ride /compositions, and the
    seeder stamps known names so the sidebar grouping has what it needs."""
    await seed_default_compositions(actions.pool)
    comps = {c["name"]: c for c in await list_compositions(actions.pool)}
    assert comps["briefing"]["section"] == "arrive"
    assert comps["the-wall"]["section"] == "wall"
    assert "GENUINELY unresolved" in comps["the-wall"]["description"]
    assert comps["graph-lint"]["section"] == "engine"
    assert comps["co-investment-ties"]["section"] == "casework"


async def test_object_set_can_exclude_the_agent_hulls(
        actions: Actions, client: httpx.AsyncClient) -> None:
    """A noisy object set: 920 Agent objects, only 10 live, dead session hulls crowding
    the 1500-cap working set. The default object set excludes them via ?exclude_types=Agent;
    a deliberate toggle brings them back."""
    await actions.create_or_find_object("Agent", "agent:hull-1", "session")
    await actions.create_or_find_object("Decision", "decision:real", "session")
    everything = (await client.get("/objects?limit=100")).json()
    assert {o["type"] for o in everything} >= {"Agent", "Decision"}
    slim = (await client.get("/objects?limit=100&exclude_types=Agent")).json()
    assert all(o["type"] != "Agent" for o in slim)
    assert any(o["type"] == "Decision" for o in slim)


async def test_a_guessed_duty_never_rides_the_wall(actions: Actions) -> None:
    """The miner may notice, but it must never oblige: this rule applied to the desk, and now
    applies to the wall too.

    A guessed duty used to get a "loud week" before folding into the pile. That window is what let
    the backlog grow: the miner mints faster than seven days, so the wall stayed permanently full
    of fresh inferences. 908 of the fleet's 1067 open threads (85%) turned out to be untouched
    miner guesses, and one killed project was showing 181 of them.

    So a guess gets no week. A thread no mind has ever touched is a suggestion, and it goes
    straight to the counted pile, one click away. The wall shows only what a mind actually touched.
    Nothing is deleted and nothing is hidden: the record keeps every thread open until testimony
    says otherwise (untouched does not mean resolved). It simply stops being presented as a promise
    nobody made.
    """
    from src.orchestrator.compositions import open_thread_wall

    NOW2 = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:bartest", "session")
    await actions.assert_property(proj, "name", "bartest", "session", NOW2, 0.9)

    async def mined_obligation(canon: str, summary: str) -> None:
        t = await actions.create_or_find_object("Thread", canon, "session-miner")
        for name, val in (("summary", summary), ("status", "open"), ("kind", "obligation")):
            await actions.assert_property(t, name, val, "session-miner", NOW2, 0.4,
                                          evidence_class="derived")
        await actions.create_link(t, proj, "in_repo", "session-miner", NOW2, 0.4,
                                  evidence_class="derived")
        return t

    stale = await mined_obligation("thread:guess-old", "a guessed duty from three weeks ago")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '21 days' WHERE id=$1", stale)
    await mined_obligation("thread:guess-new", "a guessed duty from this morning")
    declared = await open_thread(actions, "a declared duty from three weeks ago",
                                 repo="bartest", kind="obligation", source="agent:me")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '21 days' WHERE id=$1", declared)

    wall, echoes = await open_thread_wall(actions.pool, proj)
    on_wall = {w["summary"] for w in wall}
    in_pile = {e["summary"] for e in echoes}
    assert "a declared duty from three weeks ago" in on_wall      # a MIND said it: never hides
    assert "a guessed duty from this morning" in in_pile          # fresh guess: still a guess
    assert "a guessed duty from three weeks ago" in in_pile       # stale guess: also a guess
    assert len(wall) == 1, "the wall is what minds declared, nothing else"


async def test_the_fleet_totals_actually_add_up(actions: Actions) -> None:
    """The fleet's three headline numbers must partition into consistent parts. They were
    previously reported as 1051 open, 334 obligations, and 951 pile: numbers that never added up.

    They never could have. These were three overlapping cuts of one set, stacked as if they were
    three slices of it: `open` was the whole, `obligations` cut it by kind, `pile` cut it by
    touched-ness. An obligation can sit in the pile, so 381 + 951 = 1332 > 1114. Anyone trying to
    reconcile them was doing arithmetic on a category error.

    And the duty count was inflated threefold: of 381 threads carrying kind='obligation', 259 were
    the miner's guess that somebody owed something, untouched by any mind. The real number was 122.
    The miner may notice, but it must never oblige, here at the level of the headline figure.
    """
    from src.orchestrator.compositions import _fn_wall

    proj = await actions.create_or_find_object("SoftwareProject", "repo:counttest", "session")
    await actions.assert_property(proj, "name", "counttest", "session", datetime.now(UTC), 0.9)
    now = datetime.now(UTC)

    async def guessed(canon: str, kind: str) -> None:
        t = await actions.create_or_find_object("Thread", canon, "session-miner")
        for n, v in (("summary", canon), ("status", "open"), ("kind", kind)):
            await actions.assert_property(t, n, v, "session-miner", now, 0.4,
                                          evidence_class="derived")
        await actions.create_link(t, proj, "in_repo", "session-miner", now, 0.4,
                                  evidence_class="derived")

    await guessed("thread:g1", "obligation")   # the miner guessed a duty, not a debt
    await guessed("thread:g2", "obligation")
    await guessed("thread:g3", "commitment")
    await open_thread(actions, "a duty a mind actually declared", repo="counttest",
                      kind="obligation", source="agent:me")

    out = await _fn_wall(actions.pool, None, {})
    t = out["totals"]
    assert t["open"] == t["wall"] + t["pile"], "open MUST partition into wall + pile"
    assert t["wall"] == 1 and t["pile"] == 3
    # the duty count is what a mind declared; the miner's two guesses are not debt
    assert t["obligations"] == 1
    assert t["guessed_obligations"] == 2
    # and it says so, so no reader ever stacks them again
    assert "open = wall + pile" in t["reads"]


async def test_a_halted_program_is_not_debt(actions: Actions) -> None:
    """Halting a project by name does not discard its history. Two named projects were halted,
    and their 333 open threads are real yield (the miner did its job and the work was genuine),
    so the janitor must never sweep them. But they are not debt either, and they were inflating
    every number in the system long after both programs had stopped.

    A memory that cannot hear "we stopped doing that" will keep billing for it forever.

    This is testimony, not a guess: the human said it, an agent records it, the lens obeys. And
    it is reversible by construction: set the project back to 'active' and every thread returns
    exactly as it was. Nothing is deleted; nothing is swept.
    """
    from src.orchestrator.capture import set_lifecycle
    from src.orchestrator.compositions import _fn_wall

    live = await actions.create_or_find_object("SoftwareProject", "repo:aliveproj", "session")
    await actions.assert_property(live, "name", "aliveproj", "session", datetime.now(UTC), 0.9)
    dead = await actions.create_or_find_object("SoftwareProject", "repo:deadproj", "session")
    await actions.assert_property(dead, "name", "deadproj", "session", datetime.now(UTC), 0.9)

    await open_thread(actions, "a live duty", repo="aliveproj", kind="obligation",
                      source="agent:me")
    await open_thread(actions, "a duty on the program he killed", repo="deadproj",
                      kind="obligation", source="agent:me")

    before = (await _fn_wall(actions.pool, None, {}))["totals"]
    assert before["open"] == 2 and before["obligations"] == 2

    await set_lifecycle(actions, "deadproj", "halted",
                        because="the operator abandoned the program, 2026-07-12")

    after = (await _fn_wall(actions.pool, None, {}))["totals"]
    assert after["open"] == 1 and after["obligations"] == 1   # the dead tree stops billing
    assert after["halted"] == 1                               # ...and is COUNTED, never hidden
    assert after["open"] == after["wall"] + after["pile"]     # still partitions

    # the thread is untouched in the record; this is a lens effect
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a WHERE a.name='status' "
        "AND a.value #>> '{}' = 'open'")
    assert n == 2

    # ...and RESUMING the program brings every thread straight back
    await set_lifecycle(actions, "deadproj", "active", because="he changed his mind")
    resumed = (await _fn_wall(actions.pool, None, {}))["totals"]
    assert resumed["open"] == 2 and resumed["halted"] == 0


async def test_an_unfiled_thread_surfaces_on_its_owners_wall(actions: Actions) -> None:
    """An unfiled thread (opened without repo=) carries no in_repo link, yet it must
    still reach the wall of the project whose move it is, matched by project name or by
    an agent mounted there. A stray owned elsewhere stays off this wall."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session")
    a = await actions.create_or_find_object("Agent", "agent:wa11aaaa", "session")
    await actions.assert_property(a, "project", "walltest", "session", NOW, 0.9,
                                  evidence_class="self_declared")
    await open_thread(actions, "SUCCESSION HANDOFF nobody filed", kind="obligation",
                      owner="walltest", source="agent:wa11aaaa")
    await open_thread(actions, "an agent-owned stray", kind="obligation",
                      owner="agent:wa11aaaa", source="agent:wa11aaaa")
    await open_thread(actions, "someone else's stray", kind="obligation",
                      owner="elsewhere", source="agent:zzzz9999")
    wall, _ = await open_thread_wall(actions.pool, proj)
    summaries = {w["summary"] for w in wall}
    assert "SUCCESSION HANDOFF nobody filed" in summaries
    assert "an agent-owned stray" in summaries
    assert "someone else's stray" not in summaries


async def test_open_thread_tool_files_under_the_mounted_project(actions: Actions) -> None:
    """The tool-layer half of the same fix: open_thread without repo= defaults to the
    caller's mounted project, so an unfiled thread now takes deliberate effort (an
    unmounted caller), not a forgotten kwarg."""
    import src.mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    await actions.create_or_find_object("SoftwareProject", "repo:walltest", "session")
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:wa11aaaa", session="wall0001", project="walltest",
        model=None, cwd=None)
    try:
        out = await srv.open_thread("a duty filed by default", kind="obligation", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    filed = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects t ON t.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE t.id=$1::uuid AND l.type='in_repo' AND p.canonical='repo:walltest'",
        out["id"])
    assert filed == 1


async def test_open_thread_tool_refuses_an_arc_outside_the_locked_taxonomy(
    actions: Actions,
) -> None:
    """The tool layer catches capture.open_thread's ValueError and returns an honest
    {"error": ...} response, never an unhandled exception reaching the MCP transport."""
    import src.mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.open_thread("a duty with a bad arc", arc="Not-A-Real-Arc",
                                    kind="task")
    finally:
        srv._pool = saved_pool
    assert "error" in out and "arc must be one of" in out["error"]


async def test_rank_open_threads_prefers_a_recent_touch_over_a_recent_mint(
    actions: Actions,
) -> None:
    """Relevance is observed, not declared: the same catalog-usage rule (#121) applied
    to threads. The old tie-break was raw SQL input order, i.e. mint
    time. A thread minted a month ago and re-annotated moments ago must outrank one minted
    moments ago and never touched since: annotated recently versus abandoned, answered
    directly by `last_touched`."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:touchrank", "session")
    old_but_touched = await open_thread(actions, "an old thread someone just re-read",
                                        repo="touchrank", source="agent:me")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1",
        old_but_touched)
    fresh_but_ignored = await open_thread(actions, "a brand new thread nobody has revisited",
                                          repo="touchrank", source="agent:me")
    # re-touch the OLD thread just now: a fresh self_declared observation, no content
    # change needed; annotating IS the signal, not what the annotation says
    await actions.assert_property(old_but_touched, "summary", "an old thread someone just re-read",
                                  "agent:me", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    shown, _more = rank_open_threads(wall)
    ids = [t["id"] for t in shown]
    assert ids.index(str(old_but_touched)[:8]) < ids.index(str(fresh_but_ignored)[:8])


async def test_wall_items_carry_arc_only_when_declared(actions: Actions) -> None:
    """`arc` rides the wall item exactly like `kind`/`owner` already do: present only when
    a mind (or the miner) actually named one, never a null-key placeholder."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "session")
    await open_thread(actions, "a thread with a named arc", repo="osiris",
                      arc="Token-Cost", source="agent:me")
    await open_thread(actions, "a thread with no arc at all", repo="osiris",
                      source="agent:me")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    by_summary = {t["summary"]: t for t in wall}
    assert by_summary["a thread with a named arc"]["arc"] == "Token-Cost"
    assert "arc" not in by_summary["a thread with no arc at all"]


async def test_wall_marks_a_thread_whose_note_disputes_its_own_summary(
    actions: Actions,
) -> None:
    """Fix (b): a note that postdates the thread's own last summary touch marks it
    contested. This is the exact shape of the bug: a note proving the headline false
    with no correction to match."""
    from src.orchestrator.capture import annotate_thread

    proj = await actions.create_or_find_object("SoftwareProject", "repo:contested1",
                                                "session")
    t = await open_thread(
        actions, "the master limiter's gain reduction is exposed but nothing renders it",
        repo="contested1", source="agent:me")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    [item] = [w for w in wall if w["id"] == str(t)[:8]]
    assert item.get("contested") is True


async def test_wall_never_marks_a_thread_fixed_in_the_same_annotate_call(
    actions: Actions,
) -> None:
    """Fix (a) closes the gap fix (b) watches for: a note and its own corrected_summary,
    landed in one call, share the identical observed_at, so the note can never postdate a
    correction it arrived beside, and this is never contested."""
    from src.orchestrator.capture import annotate_thread

    proj = await actions.create_or_find_object("SoftwareProject", "repo:contested2",
                                                "session")
    t = await open_thread(
        actions, "the master limiter's gain reduction is exposed but nothing renders it",
        repo="contested2", source="agent:me")
    await annotate_thread(
        actions, str(t), "checked: MasterBand.tsx DOES render it",
        corrected_summary="MasterBand.tsx renders the gain reduction via .mband-gr")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    [item] = [w for w in wall if w["id"] == str(t)[:8]]
    assert "contested" not in item


async def test_wall_uncontests_once_the_summary_is_corrected_after_the_note(
    actions: Actions,
) -> None:
    """Correcting the headline after the disputing note clears the marker: the fix
    resolves it, exactly as fix (d) requires for the no-regrow exclusion to ever lift."""
    from src.orchestrator.capture import annotate_thread, correct_thread_summary

    proj = await actions.create_or_find_object("SoftwareProject", "repo:contested3",
                                                "session")
    t = await open_thread(
        actions, "the master limiter's gain reduction is exposed but nothing renders it",
        repo="contested3", source="agent:me")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")
    await correct_thread_summary(
        actions, str(t), "MasterBand.tsx renders the gain reduction via .mband-gr")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    [item] = [w for w in wall if w["id"] == str(t)[:8]]
    assert "contested" not in item


# --- reader_identity_set: the seat-handle gap in whose_move (#185 leg (a)) ------------------
# orient() and automount()/whisper each hand-rolled `{agent_id, project}` as the ranking
# reader's identity, never the seat's own HANDLE. A charter obligation filed
# owner='<handle>' (the natural way to self-declare "whose move is this") never ranked as
# MINE at either call site: a bare handle matches neither an agent id nor a project name.

async def test_reader_identity_set_folds_in_the_seats_own_handle(actions: Actions) -> None:
    from src.orchestrator.compositions import reader_identity_set
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="kip", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:kip0001")

    me = await reader_identity_set(actions.pool, agent_id="agent:kip0001", project="kast")
    assert me == frozenset({"agent:kip0001", "kast", "kip"})


async def test_reader_identity_set_degrades_to_the_base_pair_when_unseated(
    actions: Actions,
) -> None:
    from src.orchestrator.compositions import reader_identity_set

    me = await reader_identity_set(actions.pool, agent_id="agent:unseated0001", project="kast")
    assert me == frozenset({"agent:unseated0001", "kast"})


async def test_reader_identity_set_with_no_agent_is_just_the_project(actions: Actions) -> None:
    from src.orchestrator.compositions import reader_identity_set

    me = await reader_identity_set(actions.pool, agent_id=None, project="kast")
    assert me == frozenset({"kast"})


async def test_a_charter_obligation_owned_by_a_bare_handle_ranks_as_mine(
    actions: Actions,
) -> None:
    """The end-to-end case this whole leg exists for: an obligation filed
    owner='kip' (a self-declared-charter nudge, #185's own shape) ranks above an
    unrelated operator-owned item once the reader's identity set carries kip's handle,
    exactly what whisper/orient now compute via reader_identity_set."""
    from src.orchestrator.compositions import reader_identity_set
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="kip", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:kip0002")

    proj = await actions.create_or_find_object("SoftwareProject", "repo:kast", "session")
    await open_thread(actions, "an unrelated operator blocker", repo="kast",
                      kind="obligation", owner="operator", source="agent:me")
    await open_thread(actions, "kip's own undeclared charter nudge", repo="kast",
                      kind="obligation", owner="kip", source="agent:me")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    me = await reader_identity_set(actions.pool, agent_id="agent:kip0002", project="kast")
    shown, _more = rank_open_threads(wall, me)
    summaries = [t["summary"] for t in shown]
    assert summaries.index("kip's own undeclared charter nudge") < \
        summaries.index("an unrelated operator blocker")


async def test_whose_move_matches_a_handle_case_insensitively(actions: Actions) -> None:
    """A handle's stored casing follows whatever claim_name/ensure_seat happened to type;
    nobody should have to know a seat's exact capitalization for their own obligation to
    rank as theirs. Exact match stays first (agent:/operator lineage roots need it
    precise); this is the forgiving fallback for a plain name only."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:casetest", "session")
    await open_thread(actions, "Zed's charter nudge, filed with capital-Z casing",
                      repo="casetest", kind="obligation", owner="Zed", source="agent:me")

    wall, _echoes = await open_thread_wall(actions.pool, proj)
    shown, _more = rank_open_threads(wall, frozenset({"zed"}))  # reader's own handle: lowercase
    assert shown[0]["owner"] == "Zed"

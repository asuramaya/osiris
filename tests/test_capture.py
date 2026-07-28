"""Capture-at-source — decisions & threads written back DURING a session.

The prosthesis write-back half: what a session decides / leaves open must land in the
graph in the SAME shape the miner produces, so it renders in the real `decision-log` and
`briefing` compositions beside mined items. These tests drive the ACTUAL compositions
(seeded from the defaults), never a hand-rolled query, so a shape mismatch fails here.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.ingest.mined import consolidate_memory
from src.mcp_server import _project_briefing
from src.orchestrator.capture import (
    find_near_duplicate_open_thread,
    link_repo,
    measurement_smell,
    open_thread,
    prior_art_from_hits,
    prior_art_is_strong,
    reclassify_thread,
    record_blind_spot,
    record_decision,
    record_tension,
    resolve_thread,
)
from src.orchestrator.compositions import _props, run_composition, seed_default_compositions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_DECISION_LOG = "Decisions — the project's WHY (mined from commit rationale)"
_OPEN = "The wall — what's genuinely unresolved"
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
    # the briefing's first section is the GRADED wall now (ruling 923c380f): a deliberate
    # thread is TOUCHED, so it rides the fleet's top-of-wall even with no kind and no repo
    open_rows = res["items"][_OPEN]["top_of_wall"]
    assert any(
        r["summary"] == "wire the composed watcher into SOURCE_TICKS with a live key"
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
    open_rows = res["items"][_OPEN]["top_of_wall"]
    resolved_rows = res["items"][_RESOLVED]
    # left the wall (status superseded within-source), joined the resolved section
    assert not any(r["summary"] == summary for r in open_rows)
    resolved = next(r for r in resolved_rows if r["thread"] == summary)
    assert resolved["by"] == "session"        # a session closed it, not a later commit
    assert resolved["because"] == "tightened url_fetch"


async def test_resolve_thread_returns_none_when_nothing_matches(actions: Actions) -> None:
    assert await resolve_thread(actions, "no such thread anywhere") is None


async def test_a_reworded_summary_dedups_to_the_existing_open_thread(actions: Actions) -> None:
    """Two field witnesses (Aegis, Maat): the same fact minted twice across a lineage restart
    because the second telling reworded the summary. A near-identical restatement on the SAME
    project must resolve to the FIRST thread's id, never a twin."""
    t = await open_thread(actions, "wire the daemon's receipt path into the meter",
                          repo="dedupproj")
    hit = await find_near_duplicate_open_thread(
        actions.pool, "Wire the daemon's receipt path into the meter.", repo="dedupproj")
    assert hit == t


async def test_an_unrelated_summary_is_never_a_false_merge(actions: Actions) -> None:
    """Conservative on purpose: a genuinely different thread on the same project must NOT
    dedup — a false merge silently drops testimony, which costs more than a duplicate."""
    await open_thread(actions, "wire the daemon's receipt path into the meter", repo="dedupproj")
    hit = await find_near_duplicate_open_thread(
        actions.pool, "the statusline flaps unreachable under load", repo="dedupproj")
    assert hit is None


async def test_dedup_never_crosses_a_project_boundary(actions: Actions) -> None:
    """No `repo` (or a DIFFERENT one) is no safe scope to dedup against — an exact restatement
    filed under a different project, or with none at all, must still mint its own thread."""
    await open_thread(actions, "wire the daemon's receipt path into the meter", repo="dedupproj")
    assert await find_near_duplicate_open_thread(
        actions.pool, "wire the daemon's receipt path into the meter", repo="otherproj") is None
    assert await find_near_duplicate_open_thread(
        actions.pool, "wire the daemon's receipt path into the meter", repo=None) is None


async def test_a_resolved_thread_is_never_a_dedup_target(actions: Actions) -> None:
    """The dedup only ever answers for OPEN threads — a closed one is a different fact now
    (it was answered), so a fresh telling of the same words must open its own thread, not
    quietly reopen a resolved one."""
    summary = "wire the daemon's receipt path into the meter"
    await open_thread(actions, summary, repo="dedupproj")
    await resolve_thread(actions, summary, because="shipped")
    assert await find_near_duplicate_open_thread(
        actions.pool, summary, repo="dedupproj") is None


async def test_pg_trgm_is_enabled_in_this_database(actions: Actions) -> None:
    """CHECK, don't assume (sessions.py's own '_near_same... pg_trgm is not installed' comment
    went stale the day migration 0025 landed it): this test pins that the dev/test database
    really does carry the extension, so the similarity branch above is the one actually
    exercised here, not silently the Python fallback."""
    assert await actions.pool.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname='pg_trgm'") == 1


async def test_short_id_ref_beats_a_summary_that_quotes_it(actions: Actions) -> None:
    """The fleet quotes threads by short id INSIDE other summaries: resolving by '<short-id>'
    must close the thread WITH that id, never the thread that mentions it (the 2026-07-10
    mis-resolve: 'resolve 5c57f54d' closed the obligation whose summary cited 5c57f54d)."""
    target = await open_thread(actions, "the campaign thread everyone refers to by id")
    short = str(target)[:8]
    quoter = await open_thread(
        actions, f"a different duty that cites the campaign (thread {short}) in passing")
    assert await resolve_thread(actions, short, because="done") == target
    # the quoting thread is untouched — still resolvable by its own words
    assert await resolve_thread(actions, "cites the campaign", because="also done") == quoter


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
    assert not any(r["summary"] == summary for r in res["items"][_OPEN]["top_of_wall"])
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


async def test_a_derived_echo_cannot_move_a_held_tension(actions: Actions) -> None:
    """b347df65 — `Tension` exists to HOLD contradiction; the resolver exists to PICK A WINNER.
    Nobody had ever checked which one wins. Answer: THE HOLD. winning-props ranks grade before
    recency, so a deliberate (SELF_DECLARED) lean outranks a FRESHER machine echo, and a
    swapped-pole derived re-record cannot flip the deliberate pair either."""
    a, b = "ship the fast path now", "keep the kernel append-only"
    t = await record_tension(actions, a, b, lean="append-only, always", repo="osiris")
    later = datetime.now(UTC) + timedelta(seconds=5)
    d_conf = confidence_for(EvidenceClass.DERIVED)
    d_ec = EvidenceClass.DERIVED.value
    # a fresher machine echo tries to resolve the polarity: move the lean, swap a pole
    await actions.assert_property(t, "lean", "fast path won", "session-miner", later, d_conf,
                                  evidence_class=d_ec)
    await actions.assert_property(t, "pole_a", b, "session-miner", later, d_conf,
                                  evidence_class=d_ec)
    props = await _props(actions.pool, t)
    assert props["lean"] == "append-only, always"  # grade outranks recency: the hold wins
    assert props["pole_a"] == a                    # the deliberate pair survives the echo


async def test_consolidation_cannot_absorb_a_tension_even_when_aimed_at_it(
    actions: Actions,
) -> None:
    """b347df65's other half — dedup. A Tension carries poles, never a summary, so the
    near-duplicate folder has nothing to match on. Pinned as a CONTRACT: if Tension ever
    grows a summary property, this fails before the dedup machinery gains a way in."""
    t1 = await record_tension(actions, "bounded recall is the product",
                              "complete memory is the product", lean="bounded")
    t2 = await record_tension(actions, "bounded recall is the products",   # near-dup poles
                              "complete memory is the products")
    out = await consolidate_memory(actions, object_type="Tension", prefix="tension:")
    assert out == {"tensions_merged": 0, "tensions_for_review": 0}
    for t in (t1, t2):
        assert await actions.pool.fetchval(
            "SELECT status FROM objects WHERE id=$1", t) == "active"
        assert await actions.pool.fetchval(
            "SELECT merged_into FROM objects WHERE id=$1", t) is None


async def test_tension_surfaces_in_the_scoped_briefing(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await actions.assert_property(proj, "name", "demo", "session", datetime.now(UTC), 0.9)
    await record_tension(actions, "speed", "safety", lean="safety", repo="demo")
    await seed_default_compositions(actions.pool)
    items = (await run_composition(actions.pool, "project-briefing", proj))["items"]
    assert "tensions" in items
    assert ("speed", "safety") in [(r["pole_a"], r["pole_b"]) for r in items["tensions"]]


async def test_blind_spot_registers_holds_and_is_idempotent(actions: Actions) -> None:
    """Thread 8e26cd10: a blind spot is a stable per-project fact, held like a Tension —
    idempotent per (project, surface), re-registered to sharpen the wording, and scoped so
    two projects' same-named surfaces never merge."""
    b1 = await record_blind_spot(actions, "webkit-rendering",
                                 "headless Chromium ≠ WebKit; every iOS browser is WebKit",
                                 verify_with="tests/e2e/webkit_ctm.js (the docker rig)",
                                 repo="hector-vector")
    props = await _props(actions.pool, b1)
    assert props["surface"] == "webkit-rendering"
    assert "headless Chromium" in props["cannot_see"]
    assert "webkit_ctm" in props["verify_with"]
    assert await actions.pool.fetchval("SELECT type FROM objects WHERE id=$1", b1) == "BlindSpot"
    # re-register the same (project, surface) → the SAME object, sharpened
    b2 = await record_blind_spot(actions, "Webkit-Rendering",  # case-insensitive surface key
                                 "sharper: r=0 circles render as nothing in WebKit",
                                 repo="hector-vector")
    assert b2 == b1
    assert "r=0 circles" in (await _props(actions.pool, b1))["cannot_see"]
    # the same surface on ANOTHER project is its own spot — never a cross-project merge
    b3 = await record_blind_spot(actions, "webkit-rendering", "different rig, different gap",
                                 repo="osiris")
    assert b3 != b1


async def test_blind_spot_surfaces_in_the_scoped_briefing_and_orient(actions: Actions) -> None:
    """The registry's whole point: orient() speaks the project's blind spots so a session
    knows what it cannot see BEFORE it trusts a green harness — and a project with none
    registered stays silent (no empty block)."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await actions.assert_property(proj, "name", "demo", "session", datetime.now(UTC), 0.9)
    await record_blind_spot(actions, "ios-touch", "DevTools emulation is not a finger",
                            verify_with="hand the phone to the operator", repo="demo")
    await seed_default_compositions(actions.pool)
    items = (await run_composition(actions.pool, "project-briefing", proj))["items"]
    assert "blind_spots" in items
    assert "ios-touch" in [r["surface"] for r in items["blind_spots"]]
    briefing = await _project_briefing(actions.pool, "demo")
    assert briefing is not None
    assert [r["surface"] for r in briefing["blind_spots"]] == ["ios-touch"]
    assert "before trusting" in briefing["blind_spots_note"] or "green run" in briefing[
        "blind_spots_note"]
    # a project with nothing registered gets NO block — orient stays lean
    bare = await actions.create_or_find_object("SoftwareProject", "repo:bare", "session")
    await actions.assert_property(bare, "name", "bare", "session", datetime.now(UTC), 0.9)
    empty = await _project_briefing(actions.pool, "bare")
    assert empty is not None and "blind_spots" not in empty


async def test_resolve_thread_artifact_points_at_the_closer(actions: Actions) -> None:
    """Thread 022bd24a: `because` was becoming a completion essay because there was nowhere
    to put 'what actually got built'. The artifact pointer is that place — always kept as
    resolved_artifact, and minted as a resolved_by edge when it names a graph object (the
    strong closure witness the closure-miner almost never finds)."""
    t1 = await open_thread(actions, "wire the doodad through the frobnicator")
    d = await record_decision(actions, "the doodad rides the frobnicator now", kind="decision")
    closed = await resolve_thread(actions, str(t1), because="built",
                                  artifact=str(d)[:8])  # the 8-char short id names the Decision
    assert closed == t1
    props = await _props(actions.pool, t1)
    assert props["resolved_artifact"] == str(d)[:8]
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='resolved_by'", t1)
    assert linked == d
    # a free-form pointer (file:line) keeps the property and mints NO guessed edge
    t2 = await open_thread(actions, "teach the widget to sing")
    await resolve_thread(actions, str(t2), artifact="src/widget/voice.py:42")
    assert (await _props(actions.pool, t2))["resolved_artifact"] == "src/widget/voice.py:42"
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND type='resolved_by'", t2) is None


async def test_resolve_thread_tool_receipt_confirms_the_edge_when_it_lands(
    actions: Actions,
) -> None:
    """Wave 0 (thread 1aa2ff36): the receipt must CONFIRM the resolved_by edge, not just
    describe the possibility in a conditional sentence — a caller must never have to
    re-query the graph to learn what its own write verb actually did."""
    from src import mcp_server as srv

    t = await open_thread(actions, "wire the frobnicator's doodad socket")
    d = await record_decision(actions, "the doodad ships wired", kind="decision")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.resolve_thread(str(t), because="built", artifact=str(d)[:8])
    finally:
        srv._pool = saved_pool
    assert out["id"] == str(t)
    assert out["resolved_by"].startswith("Decision ")
    # the canonical, not the short id, is what actually names the object in the receipt
    canon = await actions.pool.fetchval("SELECT canonical FROM objects WHERE id=$1", d)
    assert canon in out["resolved_by"]


async def test_resolve_thread_tool_receipt_admits_when_the_edge_does_NOT_land(
    actions: Actions,
) -> None:
    """A file:line or any other unresolvable pointer must be reported HONESTLY as text-only —
    the old wrapper said '(+ resolved_by edge if it names an object)' unconditionally, which
    reads as a promise regardless of what actually happened."""
    from src import mcp_server as srv

    t = await open_thread(actions, "teach the doohickey to hum")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.resolve_thread(str(t), artifact="src/doohickey/hum.py:17")
    finally:
        srv._pool = saved_pool
    assert out["resolved_by"].startswith("none —")
    assert "did not resolve to a graph object" in out["resolved_by"]


async def test_resolve_thread_tool_receipt_omits_resolved_by_without_an_artifact(
    actions: Actions,
) -> None:
    """No artifact given → nothing to confirm; the field is simply absent, not a false 'none'."""
    from src import mcp_server as srv

    t = await open_thread(actions, "a thread closed with no artifact at all")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.resolve_thread(str(t), because="moot")
    finally:
        srv._pool = saved_pool
    assert "resolved_by" not in out
    assert "artifact" not in out


def test_measurement_smell_nags_only_the_measurements() -> None:
    """Thread 022bd24a: the protocol nag must fire on verification recipes (Ferryman's exact
    words + the N/N shape) and stay QUIET on ordinary rulings — a nag that fires on every
    decision teaches everyone to ignore it."""
    assert measurement_smell("verified the walk: 498/498 lineages gapless, seed 42")
    assert measurement_smell("the probe swept the candidates pile")
    assert measurement_smell("graduation threshold holds at 0.15")
    assert not measurement_smell("the mail chrome routes DMs through the seat ledger now")
    assert not measurement_smell("adopt the three-layer identity model; handles are roles")


async def test_divergent_leans_say_two_minds_lean_apart(actions: Actions) -> None:
    """Task #53 (from the tension-vs-resolver audit c7041c53): when two minds hold
    DIFFERENT current leans on one held polarity, the scoped briefing must SAY so —
    the record keeps both, and a single-winner table would silently show one."""
    from src.orchestrator.capture import divergent_leans

    proj = await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await actions.assert_property(proj, "name", "demo", "session", datetime.now(UTC), 0.9)
    await record_tension(actions, "bounded recall", "complete memory",
                         lean="bounded at the lens", repo="demo", source="agent:one")
    await record_tension(actions, "bounded recall", "complete memory",
                         lean="complete at the record", repo="demo", source="agent:two")
    # an AGREEING pair on another tension must not be flagged
    await record_tension(actions, "speed", "safety", lean="safety", source="agent:one")
    await record_tension(actions, "speed", "safety", lean="safety", source="agent:two")
    div = await divergent_leans(actions.pool)
    assert len(div) == 1
    line = next(iter(div.values()))
    assert line.startswith("two minds lean apart: ")
    assert "agent:one" in line and "agent:two" in line
    # ...and orient's scoped briefing carries the confession on the right row
    await seed_default_compositions(actions.pool)
    briefing = await _project_briefing(actions.pool, "demo")
    assert briefing is not None
    flagged = [r for r in briefing["tensions"] if r.get("divergence")]
    assert len(flagged) == 1
    assert flagged[0]["pole_a"] == "bounded recall"
    assert "agent:two" in flagged[0]["divergence"]


async def test_a_fix_kills_its_superstition_by_name(actions: Actions) -> None:
    """Thread a9be40c9 (Atlas caught 'NEVER DM BY NAME' in his own will an hour after the
    fix made it false): a killed workaround becomes a first-class dead Superstition —
    searchable forever, idempotent on the normalized statement, announced while fresh."""
    from src.orchestrator.capture import kill_superstition, recent_dead_superstitions

    s1 = await kill_superstition(actions, "NEVER DM BY NAME", killed_by="43cfcf1",
                                 repo="osiris")
    # idempotent on the normalized statement — a re-kill sharpens the record, never twins
    s2 = await kill_superstition(actions, "NEVER  DM BY NAME", killed_by="43cfcf1")
    assert s2 == s1
    assert await actions.pool.fetchval(
        "SELECT type FROM objects WHERE id=$1", s1) == "Superstition"
    kills = await recent_dead_superstitions(actions.pool)
    mine = [k for k in kills if "DM BY NAME" in k["statement"]]
    assert mine and mine[0]["killed_by"] == "43cfcf1"


async def test_an_old_kill_leaves_the_announcement_but_not_the_record(
        actions: Actions) -> None:
    """orient announces the RECENT dead only — the window ages out so the block never
    becomes a wall; the object itself stays searchable forever."""
    from src.orchestrator.capture import recent_dead_superstitions

    old = datetime.now(UTC) - timedelta(days=30)
    s = await actions.create_or_find_object("Superstition", "superstition:ancient", "session")
    await actions.assert_property(s, "statement", "always sacrifice a goat first",
                                  "session", old, 0.8)
    await actions.assert_property(s, "killed_by", "deadbeef", "session", old, 0.8)
    kills = await recent_dead_superstitions(actions.pool)
    assert "always sacrifice a goat first" not in [k["statement"] for k in kills]
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical='superstition:ancient'") == 1


async def test_record_decision_obsoletes_and_orient_announces_fleet_wide(
        actions: Actions) -> None:
    """The whole loop: a fix recorded with obsoletes=[…] mints the dead Superstition, and
    ANY orient — even scoped to an unrelated project — carries the announcement, because a
    workaround replicates across houses and its death must too."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key, orient
    from src.mcp_server import record_decision as rd_tool
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:killer1", session="killer1", project="nowhere-land",
        model=None, cwd=None)
    # the file's pool ritual: point the server at THIS test's pool (and loop) — a test
    # that instead lets _pool_get mint the global pool leaves it bound to a dead loop
    # for every later caller (the first-caller-owns-the-pool fragility, paid tonight)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await rd_tool("the DM router follows lineage heads now", kind="decision",
                            obsoletes=["NEVER DM AFTER A RESTART"], ctx=ctx)
        assert out["superstitions_killed"] == ["NEVER DM AFTER A RESTART"]
        res = await orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)
    recent = res["dead_superstitions"]["recent"]
    assert "NEVER DM AFTER A RESTART" in [k["statement"] for k in recent]
    assert res["dead_superstitions"]["note"].startswith("workarounds whose bug is FIXED")


async def test_orient_explicit_project_overrides_the_mount(actions: Actions) -> None:
    """sibling-one's verified bug: orient(project=X) silently returned the MOUNT's briefing instead
    of X's — a silent wrong-scope (the confound class the fleet exists to catch). An explicit
    project must OVERRIDE the mount."""
    from src.mcp_server import _agents, _conn_key, orient
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.compositions import seed_default_compositions

    now = datetime.now(UTC)
    dec = await actions.create_or_find_object("SoftwareProject", "repo:sibling-two", "session")
    await actions.assert_property(dec, "name", "sibling-two", "session", now, 0.9)
    # DECLARED (self_declared): this test is about project SCOPING, not the wall's grading —
    # an untouched miner guess would now (correctly) fold into the echo pile and prove nothing.
    th = await actions.create_or_find_object("Thread", "thread:dec-scope", "session")
    for _n, _v in (("summary", "the sibling-two-only thread"), ("status", "open")):
        await actions.assert_property(th, _n, _v, "session", now, 0.9,
                                      evidence_class=EvidenceClass.SELF_DECLARED.value)
    await actions.create_link(th, dec, "in_repo", "session", now, 0.9,
                              evidence_class=EvidenceClass.SELF_DECLARED.value)
    await seed_default_compositions(actions.pool)

    class _Ctx:  # minimal fake connection ctx — _conn_key reads id(request_context.session)
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(   # mounted as sibling-one...
        agent_id="agent:heinX", session="heinX", project="sibling-one", model=None, cwd=None)
    try:
        res = await orient(project="sibling-two", ctx=ctx)   # ...but explicitly asks sibling-two
    finally:
        _agents.pop(_conn_key(ctx), None)
    assert res["project"] == "sibling-two"                   # honored the explicit scope
    assert "the sibling-two-only thread" in [r["summary"] for r in res["open_threads"]]


async def test_unmounted_orient_is_a_bounded_map_never_the_firehose(
        actions: Actions) -> None:
    """Metron IV's flood (wave-2 fa918939): a fresh un-mounted session's first orient()
    returned 353K chars of whole-fleet briefing. Un-mounted now gets a bounded per-project
    map + the newest decisions + the mount ritual — the firehose only by deliberate call."""
    from src import mcp_server as srv
    from src.orchestrator.capture import open_thread, record_decision

    await open_thread(actions, "an open line in some project", repo="mapproj")
    await record_decision(actions, "a fresh fleet ruling for the map", repo="mapproj")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.orient(ctx=None)
    finally:
        srv._pool = saved_pool
    assert "briefing" not in out                       # the firehose is gone
    assert any(m["project"] == "repo:mapproj" and m["open_threads"] == 1
               for m in out["fleet_map"])
    assert any("fleet ruling for the map" in d for d in out["recent_decisions"])
    assert "BOUNDED" in out["note"] and "mount(" in out["note"]


async def test_swap_banner_stands_down_before_a_recorded_repo_choice(
        actions: Actions) -> None:
    """Metron IV's re-litigation (wave-2 fa918939): sibling-seven runs opus by RECORDED operator
    choice, yet every successor got the confess-or-fix banner. An intended_model property
    on the SoftwareProject is the graph's own .osiris — the banner consults it first."""
    from datetime import UTC, datetime

    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    repo = await actions.create_or_find_object("SoftwareProject", "repo:opusland", "session")
    ident = AgentIdentity(agent_id="agent:opus-1", session="opus0001", project="opusland",
                          model="claude-opus-4-8", cwd=None, model_method="job_dir",
                          model_history=("claude-opus-4-8",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = ident
    try:
        # no recorded choice yet: diverging from the fleet default earns the banner
        out = await srv.orient(ctx=ctx)
        assert out.get("swap")
        # the operator's standing choice lands on the repo object — the banner stands down
        await actions.assert_property(repo, "intended_model", "claude-opus-4-8", "session",
                                      datetime.now(UTC), 0.9, evidence_class="self_declared")
        out2 = await srv.orient(ctx=ctx)
        assert not out2.get("swap")
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_swap_banner_stands_down_for_a_triage_wake_on_the_economy_model(
        actions: Actions, monkeypatch) -> None:
    """The wake-economy false alarm (sibling-four, msg 281): triage wakes ride a cheaper model by
    the operator's OWN ruling (osiris_wake_model), yet the swap banner measured them against
    the standing choice — every wake 'escalated' policy as a rug-pull. When the observed
    model IS the economy model and the wake ledger witnesses a wake minutes ago, the banner
    stands down to a calm policy note; with no wake on the ledger, the real banner stays."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    monkeypatch.setenv("OSIRIS_WAKE_MODEL", "claude-haiku-4-5-20251001")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    await actions.create_or_find_object("SoftwareProject", "repo:wakeland", "session")
    ident = AgentIdentity(agent_id="agent:wake-9", session="wake0001", project="wakeland",
                          model="claude-haiku-4-5-20251001", cwd=None, model_method="job_dir",
                          model_history=("claude-haiku-4-5-20251001",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = ident
    try:
        # no wake on the ledger: the divergence banner stands (a real swap must not hide
        # behind the economy model)
        out = await srv.orient(ctx=ctx)
        assert out.get("swap") and "ECONOMY" not in out["swap"]
        # the ledger witnesses a wake → the banner stands down to the policy note
        await actions.pool.execute(
            "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
            "VALUES ('wakeland','agent:sender',1,'mint')")
        out2 = await srv.orient(ctx=ctx)
        assert "ECONOMY" in out2.get("swap", "") and "not a rug-pull" in out2["swap"]
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_swap_banner_honors_the_osiris_pin_after_a_rebind(
        actions: Actions, tmp_path: Path) -> None:
    """thread d8535bff (cross-house, John IV/redmonth): rebind_seat writes the new office's
    .osiris pin (and repoints agent_mounts.cwd) immediately, but a LIVE connection's cached
    AgentIdentity.cwd — read straight off by orient()'s swap banner via _expected_model —
    stayed frozen at the OLD cwd until the connection's next mount(), so the very next
    orient() on the SAME connection still measured the banner against the old, un-pinned
    cwd and kept confessing a swap the operator had already settled with the pin. The
    rebind_seat tool now patches every live cached identity in the rebound lineage in
    place, so the very next read sees the new anchor with no fresh mount() needed."""
    from datetime import UTC, datetime

    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    old_cwd = str(tmp_path / "old-office")
    new_cwd = str(tmp_path / "new-office")
    Path(old_cwd).mkdir()

    a = await actions.create_or_find_object("Agent", "agent:rebindo1", "agent:rebindo1")
    await actions.assert_property(a, "project", "rebindland", "agent:rebindo1",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await claim_name(actions, "agent:rebindo1", "Rebindo", source="agent:rebindo1")
    await srv.mounts.save_mount(actions.pool, job_dir="/jobs/rebindo1", agent_id="agent:rebindo1",
                                project="rebindland", cwd=old_cwd, model="claude-sonnet-5",
                                session_key="k")
    ident = AgentIdentity(agent_id="agent:rebindo1", session="rebindo1", project="rebindland",
                          model="claude-sonnet-5", cwd=old_cwd, model_method="job_dir",
                          model_history=("claude-sonnet-5",))

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = ident
    try:
        # no pin anywhere yet: diverging from the fleet default earns the banner
        out = await srv.orient(ctx=ctx)
        assert out.get("swap")

        # the new office already carries the pin (extract's real shape: the operator
        # seeds/confirms the new office's .osiris as part of the move)
        Path(new_cwd).mkdir()
        (Path(new_cwd) / ".osiris").write_text('model = "claude-sonnet-5"\n')
        await srv.rebind_seat(seat="Rebindo", new_cwd=new_cwd, extract=True, ctx=ctx)

        # the very next orient() on this SAME connection must see the settled pin
        out2 = await srv.orient(ctx=ctx)
        assert not out2.get("swap")
        assert ident.cwd == new_cwd
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_orient_surfaces_the_ancestors_parting_words(actions: Actions) -> None:
    """Anubis VIII (msg 236): the whisper says 'read orient()'s succession note' but no
    such field existed — successors reconstructed their inheritance from open threads.
    orient now surfaces the ancestor's HANDOFF thread and LETTER decision verbatim."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import open_thread, record_decision

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ancestor = "agent:elder-vii"
    await open_thread(actions, "HANDOFF — the estate is clean; take the rot lint first",
                      repo="lineageproj", source=ancestor)
    await record_decision(actions, "LETTER — it was a good day to be the seventh",
                          kind="choice", repo="lineageproj", source=ancestor)
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:elder-viii", session="elder001", project="lineageproj",
        model=None, cwd=None, succeeded_from=ancestor)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    note = out.get("succession_note")
    assert note is not None and note["from"] == ancestor
    texts = " ".join(n["text"] for n in note["notes"])
    assert "HANDOFF" in texts and "LETTER" in texts
    assert "parting words" in note["note"]


async def test_orient_succession_note_walks_past_a_silent_ancestor(actions: Actions) -> None:
    """THE BOUNDED CHAIN-WALK (thread e749036e, Thoth LX's diagnosis, 2026-07-27): the
    morning's own repro — elder6 wrote a handoff, elder7 was a zero-turn phantom that wrote
    NOTHING, elder8 arrives blind if orient only reads one hop back. nearest_handoff_ancestor
    walks past the silent hop and finds elder6's words, naming elder6 (not elder7) as
    'from' — the words came from whoever actually wrote them."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import open_thread

    elder6, elder7 = "agent:elder-vi", "agent:elder-vii"
    await open_thread(actions, "HANDOFF — six's own estate, read before you touch anything",
                      repo="chainproj", source=elder6)
    # the succession chain itself: elder7 succeeded elder6, on the graph (not just AgentIdentity)
    await actions.create_or_find_object("Agent", elder6, elder6)
    o7 = await actions.create_or_find_object("Agent", elder7, elder7)
    now = datetime.now(UTC)
    await actions.assert_property(o7, "succeeded_from", elder6, elder7, now, 0.9,
                                  evidence_class="direct_observation")
    # elder7 wrote NOTHING — the zero-turn phantom's whole point

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:elder-viii", session="elder8s01", project="chainproj",
        model=None, cwd=None, succeeded_from=elder7)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    note = out.get("succession_note")
    assert note is not None, "one-hop-only would find nothing here and go blind"
    assert note["from"] == elder6, "the words came from whoever actually wrote them"
    assert "six's own estate" in " ".join(n["text"] for n in note["notes"])


async def test_orient_names_the_live_siblings_in_your_project(actions: Actions) -> None:
    """Co-agent blindness (Deckard XXVI, msg 258): a live sibling sharing your repo is the
    one blindness that costs unrecoverable work — orient names them and the shared-tree
    discipline; a lone agent gets no such block."""
    from src import mcp_server as srv
    from src.orchestrator import mounts
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/sib00001",
                            agent_id="agent:sibling-1", project="sharedtree", cwd="/x",
                            model=None, session_key="k")
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:me-1", session="me000001", project="sharedtree",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
        assert out["co_agents"]["live"][0]["agent"] == "agent:sibling-1"
        assert "git add -A" in out["co_agents"]["note"]
        # the sibling goes stale → the block disappears (liveness, not history)
        await actions.pool.execute(
            "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
            "WHERE agent_id='agent:sibling-1'")
        out2 = await srv.orient(ctx=ctx)
        assert "co_agents" not in out2
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_orient_shows_the_peer(actions: Actions) -> None:
    """LEGIBILITY leg 2 (ruling d74492ee, spec e6636c7e): a peer_of bond is recognition-
    first per Ostrom p7 — an edge nobody's briefing surfaces is a convention, ignorable.
    orient() names the caller's peer (handle + last-seen), and a seat with no peer gets no
    such block, the same conditional shape co_agents already established."""
    from src import mcp_server as srv
    from src.orchestrator import mounts
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder, peer_seats

    await bind_holder(actions, seat_id="seat:me-seat1", agent_id="agent:me-1", source="test")
    await bind_holder(actions, seat_id="seat:peerseat1", agent_id="agent:peer-1",
                      source="test")
    await actions.assert_property(
        await actions.create_or_find_object("Seat", "seat:peerseat1", "test"), "handle",
        "Halcyon", "test", datetime.now(UTC), 0.9, evidence_class="self_declared")
    await peer_seats(actions, "seat:me-seat1", "seat:peerseat1", because="the reconciliation",
                     actor="test")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/peer0001",
                            agent_id="agent:peer-1", project="sharedtree", cwd="/y",
                            model=None, session_key="pk")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:me-1", session="me000001", project="sharedtree",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["peer"]["seat"] == "seat:peerseat1"
    assert out["peer"]["handle"] == "Halcyon"
    assert "last_seen" in out["peer"]
    assert "your peer" in out["peer"]["note"]


async def test_orient_shows_no_peer_block_when_unpeered(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    await bind_holder(actions, seat_id="seat:lonelyseat", agent_id="agent:lonely-1",
                      source="test")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:lonely-1", session="lonely01", project="sharedtree",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "peer" not in out


async def test_orient_shows_a_live_siblings_context_pct_and_near_seam(
    actions: Actions,
) -> None:
    """Thoth's Pit Watch extension (msg 1381, seam-discipline decision 33b7cb10): 'a
    manager can't route around a seam it can't see' — a sibling's Stop-hook-stamped
    context_pct rides along in co_agents, near_seam derived off the SAME ALARM_PCT the
    hook itself alarms on, with an explicit age so a stale reading is never mistaken for
    a fresh one. A sibling with no stamp at all carries neither key — absence, not a
    guessed 0%."""
    from src import mcp_server as srv
    from src.orchestrator import mounts
    from src.orchestrator.agents import AgentIdentity

    now = datetime.now(UTC)
    hot = await actions.create_or_find_object("Agent", "agent:sib-hot", "agent:sib-hot")
    await actions.assert_property(hot, "context_pct", "90", "agent:sib-hot", now, 1.0,
                                  evidence_class="direct_observation")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/sibhot01",
                            agent_id="agent:sib-hot", project="pctproj", cwd="/x",
                            model=None, session_key="k1")
    cool = await actions.create_or_find_object("Agent", "agent:sib-cool", "agent:sib-cool")
    await actions.assert_property(cool, "context_pct", "20", "agent:sib-cool", now, 1.0,
                                  evidence_class="direct_observation")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/sibcool1",
                            agent_id="agent:sib-cool", project="pctproj", cwd="/y",
                            model=None, session_key="k2")
    await mounts.save_mount(actions.pool, job_dir="/h/.claude/jobs/sibnone1",
                            agent_id="agent:sib-none", project="pctproj", cwd="/z",
                            model=None, session_key="k3")  # never stamped at all

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:me-pct", session="mepct001", project="pctproj",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    by_agent = {s["agent"]: s for s in out["co_agents"]["live"]}
    assert by_agent["agent:sib-hot"]["context_pct"] == 90
    assert by_agent["agent:sib-hot"]["near_seam"] is True
    assert by_agent["agent:sib-hot"]["context_pct_age_s"] >= 0
    assert by_agent["agent:sib-cool"]["context_pct"] == 20
    assert by_agent["agent:sib-cool"]["near_seam"] is False
    assert "context_pct" not in by_agent["agent:sib-none"]
    assert "near_seam" not in by_agent["agent:sib-none"]


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


def test_rank_open_threads_orders_by_whose_move_within_a_kind() -> None:
    """Owner tags (two grievance witnesses): within the obligations group, what is MINE TO
    ACT (unowned, or owned by me / my project) rides above another mind's claims, and
    'waiting on the human' (owner='operator') rides last — visible, never shadowing the
    reader's own moves. Recency (input order) still breaks ties inside each ownership band."""
    from src.mcp_server import _rank_open_threads

    me = frozenset({"agent:me", "myproj"})
    rows = [
        {"summary": "waiting-on-human", "kind": "obligation", "owner": "operator"},
        {"summary": "claimed-by-other", "kind": "obligation", "owner": "agent:other"},
        {"summary": "unowned-duty", "kind": "obligation"},
        {"summary": "mine-by-agent-id", "kind": "obligation", "owner": "agent:me"},
        {"summary": "mine-by-project", "kind": "obligation", "owner": "myproj"},
        {"summary": "ordinary-operator", "kind": None, "owner": "operator"},
        {"summary": "ordinary-unowned", "kind": None},
    ]
    shown, _ = _rank_open_threads(rows, me)
    assert [r["summary"] for r in shown] == [
        "unowned-duty", "mine-by-agent-id", "mine-by-project",  # mine to act, recency order
        "claimed-by-other",                                     # another mind's claim
        "waiting-on-human",                                     # the human's move, still shown
        "ordinary-unowned", "ordinary-operator",                # non-obligations, same bands
    ]


def test_rank_open_threads_owner_match_is_lineage_aware() -> None:
    """Finding D (Khnum audit thread ffb13bd9, live proof: Thoth LVII -> Thoth II across a
    compaction seam): an obligation owned by an EARLIER generation of the reader's own
    lineage (agent:foo-iii, when the reader is agent:foo-iv, a post-compaction successor)
    still ranks as mine to act — not another mind's claim. A DIFFERENT lineage entirely
    (agent:bar-iv) never matches just because it shares a generation suffix."""
    from src.mcp_server import _rank_open_threads

    me = frozenset({"agent:foo-iv"})
    rows = [
        {"summary": "mine-by-ancestor-gen", "kind": "obligation", "owner": "agent:foo-iii"},
        {"summary": "mine-by-exact", "kind": "obligation", "owner": "agent:foo-iv"},
        {"summary": "other-lineage-same-gen", "kind": "obligation", "owner": "agent:bar-iv"},
    ]
    shown, _ = _rank_open_threads(rows, me)
    assert [r["summary"] for r in shown] == [
        "mine-by-ancestor-gen", "mine-by-exact",  # both same lineage, mine to act
        "other-lineage-same-gen",                  # different root — another mind's claim
    ]


async def test_owner_tag_persists_and_rides_the_wall(actions: Actions) -> None:
    """End to end: open_thread(owner='operator') stamps the property; the wall renders the
    tag and sinks the waiting-on-human duty below an unowned one for any reader; the owner
    key is ABSENT (not null) on unowned rows. reclassify_thread(owner=...) claims later."""
    from src.mcp_server import _project_briefing

    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:owntest", "session")
    await actions.assert_property(proj, "name", "owntest", "session", now, 0.9)

    waiting = await open_thread(actions, "waiting on the human's gemini key",
                                repo="owntest", kind="obligation", owner="operator",
                                source="agent:me")
    await open_thread(actions, "an unowned duty anyone may take",
                      repo="owntest", kind="obligation", source="agent:me")
    await open_thread(actions, "a kindless mined commitment", repo="owntest",
                      source="agent:me")

    await seed_default_compositions(actions.pool)
    scoped = await _project_briefing(actions.pool, "owntest",
                                     me=frozenset({"agent:me", "owntest"}))
    assert scoped is not None
    wall = scoped["open_threads"]
    assert [r["summary"] for r in wall] == [
        "an unowned duty anyone may take", "waiting on the human's gemini key",
        "a kindless mined commitment"]
    assert wall[1]["owner"] == "operator" and "owner" not in wall[0]
    # undeclared kind renders ABSENT, not null — no mind said what it is (Fulcrum III,
    # answered at the lens: the record is untouched, the wall stops printing null noise)
    assert "kind" not in wall[2] and wall[0]["kind"] == "obligation"

    # triage claims an existing thread: the tag updates and re-banks the row
    got = await reclassify_thread(actions, str(waiting), kind="obligation",
                                  owner="agent:me", source="agent:me")
    assert got == waiting
    scoped = await _project_briefing(actions.pool, "owntest",
                                     me=frozenset({"agent:me", "owntest"}))
    assert scoped is not None
    assert all(r.get("owner") in (None, "agent:me") for r in scoped["open_threads"])


async def test_orient_briefing_ranks_obligations_first_and_caps(actions: Actions) -> None:
    """End to end through the real composition: orient's open_threads floats a DUTY above
    ordinary threads even when it is the LEAST recent, caps the wall at the display limit, and
    notes the remainder — a bounded, ranked query, not a 500-line scroll. Ranking only."""

    from src.mcp_server import _ORIENT_OPEN_THREADS, _project_briefing

    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:ranktest", "session")
    await actions.assert_property(proj, "name", "ranktest", "session", now, 0.9)

    async def _thread(canon: str, summary: str, *, kind: str | None = None) -> uuid.UUID:
        # DECLARED threads (self_declared): the never-hide duty law only shields what a
        # mind actually declared — an untouched 400-day "obligation" is a pile-bound guess
        t = await actions.create_or_find_object("Thread", canon, "session")
        await actions.assert_property(t, "summary", summary, "session", now, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(t, "status", "open", "session", now, 0.9,
                                      evidence_class="self_declared")
        if kind:
            await actions.assert_property(t, "kind", kind, "session", now, 0.9,
                                          evidence_class="self_declared")
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


async def test_supersede_buries_the_old_decision_both_ways(actions: Actions) -> None:
    """The supersedes verb (operator ruling dd04d7dd, Tjmax III's ask): record_decision
    (supersedes=<ref>) stamps the OLD decision superseded_by/-because and the NEW one
    supersedes — the correction navigates both directions, event-sourced, no delete."""
    old = await record_decision(actions, "the cache misses come from TTL expiry",
                                kind="ruling", repo="osiris", source="agent:me")
    new = await record_decision(
        actions, "the cache misses come from key collisions, not TTL",
        kind="ruling", repo="osiris", source="agent:me", supersedes=str(old)[:8])
    facts_old = await _props(actions.pool, old)
    facts_new = await _props(actions.pool, new)
    assert facts_old["superseded_by"] == str(new)
    assert str(new)[:8] in facts_old["superseded_because"]
    assert "key collisions" in facts_old["superseded_because"]
    assert facts_new["supersedes"] == str(old)


async def test_supersede_lens_recent_drops_it_log_grays_it(actions: Actions) -> None:
    """The graying (dd04d7dd): a superseded decision leaves the project-briefing's
    recent_decisions (a corrected hypothesis must not brief the next session as live) but
    STAYS in the decision-log with its successor riding the superseded column — the log
    skims honestly without hiding history."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:suptest", "session")
    await actions.assert_property(proj, "name", "suptest", "session", now, 0.9)
    old = await record_decision(actions, "ship the parser as a regex pile",
                                kind="choice", repo="suptest", source="agent:me")
    await record_decision(actions, "ship the parser on a real grammar — the regex "
                          "pile misparses nested quotes", kind="choice", repo="suptest",
                          source="agent:me", supersedes=str(old))
    await seed_default_compositions(actions.pool)
    from src.mcp_server import _project_briefing
    scoped = await _project_briefing(actions.pool, "suptest")
    assert scoped is not None
    recents = [r["summary"] for r in scoped["recent_decisions"]]
    assert any("real grammar" in s for s in recents)
    assert not any(s == "ship the parser as a regex pile" for s in recents)
    # the audit view keeps the buried entry, grayed by its successor
    log = await run_composition(actions.pool, "decision-log")
    rows = next(iter(log["items"].values()))
    buried = [r for r in rows if r.get("decision") == "ship the parser as a regex pile"]
    assert buried and "real grammar" in (buried[0].get("superseded") or "")
    live = [r for r in rows if "real grammar" in (r.get("decision") or "")]
    assert live and not (live[0].get("superseded") or "")


async def test_supersede_that_names_nothing_records_nothing(actions: Actions) -> None:
    """A correction that can't name its target is not yet a correction: the ValueError
    fires BEFORE the new decision exists — no husk, no half-burial."""
    import pytest

    with pytest.raises(ValueError, match="matched no decision"):
        await record_decision(actions, "this correction points at a ghost",
                              source="agent:me", supersedes="deadbeef")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Decision' AND a.name='summary' "
        "AND a.value #>> '{}' LIKE '%points at a ghost%'") == 0


async def test_supersede_self_is_a_noop_not_a_burial(actions: Actions) -> None:
    """Idempotent re-record whose supersedes resolves to ITSELF (same summary hash):
    a decision never buries itself — no superseded_by stamp lands."""
    d = await record_decision(actions, "the one true parser ruling", source="agent:me")
    d2 = await record_decision(actions, "the one true parser ruling", source="agent:me",
                               supersedes=str(d))
    assert d2 == d
    facts = await _props(actions.pool, d)
    assert "superseded_by" not in facts and "supersedes" not in facts


async def test_record_decision_is_atomic_no_orphan_husk(actions: Actions) -> None:
    """The write-integrity fix (sibling-eight audit): record_decision was five sequential
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


# --- references: the fleet can finally cite (obligation ecc8d58e, Soundwave VI) ----------

async def test_ingest_reference_mints_a_citable_node_with_first_class_caveats(
        actions: Actions) -> None:
    """A read becomes a Reference node — caveats live in their OWN property, never folded
    into body ('but only under X' buried in prose is a caveat lost)."""
    from src.orchestrator.capture import ingest_reference

    ref, canon = await ingest_reference(
        actions, "Attention Is All You Need",
        source_url="https://arxiv.org/abs/1706.03762", vendor="arxiv",
        body="Transformer architecture: attention replaces recurrence entirely.",
        caveats="Results are machine-translation only; scaling behavior unstated.",
        source="agent:test-i")
    assert canon == "ref:attention-is-all-you-need"
    row = await actions.pool.fetchrow(
        "SELECT type, canonical FROM objects WHERE id=$1", ref)
    assert row["type"] == "Reference" and row["canonical"] == canon
    props = await _props(actions.pool, ref)
    assert props["caveats"].startswith("Results are machine-translation only")
    assert "machine-translation" not in props["body"]  # separate, not folded

    # idempotent on the title slug: a re-ingest ENRICHES the same node, no twin
    ref2, _ = await ingest_reference(
        actions, "Attention Is All You Need", vendor="arxiv", source="agent:test-i")
    assert ref2 == ref


async def test_record_decision_grounds_mint_grounded_by_edges_at_birth(
        actions: Actions) -> None:
    from src.orchestrator.capture import ingest_reference

    ref, _ = await ingest_reference(actions, "The RFM theorem", vendor="arxiv",
                                    caveats="holds only for stationary distributions",
                                    source="agent:test-i")
    d = await record_decision(actions, "Adopt RFM-based scoring", kind="choice",
                              grounds=[ref], source="agent:test-i")
    edges = await actions.pool.fetch(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='grounded_by'", d)
    assert [e["to_id"] for e in edges] == [ref]
    # idempotent re-capture: no duplicate citation edge
    await record_decision(actions, "Adopt RFM-based scoring", kind="choice",
                          grounds=[ref], source="agent:test-i")
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='grounded_by'", d)
    assert n == 1


async def test_record_decision_protocol_makes_a_ruling_rerunnable(actions: Actions) -> None:
    """Anubis VIII's grievance (msg 236): a ruling that states the conclusion but not the
    INVOCATION forces the successor to re-derive it from tmp logs. `protocol` is its own
    property — never folded into rationale — at the decider's grade."""
    d = await record_decision(
        actions, "effctx holds at n_trials=64", kind="ruling",
        rationale="the effect survives the widened buckets",
        protocol="uv run effctx --n-trials 64 --seeds 1..8 --buckets 0,0.25,0.5,1.0",
        source="agent:test-i")
    row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS v, evidence_class FROM current_assertions "
        "WHERE object_id=$1 AND name='protocol' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", d)
    assert row is not None and "--n-trials 64" in row["v"]
    assert row["evidence_class"] == "self_declared"


async def test_record_decision_surfaces_prior_art_against_a_standing_ruling(
    actions: Actions,
) -> None:
    """Task #67 (thread 44635c42, from the re-derivation post-mortem): a ruling
    contradicting standing law must not mint frictionlessly. Canonical failure this
    prevents: decision 636a8648 minted in direct contradiction of naming-v3 (a882b334)
    with zero friction — the operator caught it, the verb didn't. Re-records 636a8648's
    actual text against a fixture graph containing a882b334's actual text and asserts the
    receipt's `prior_art` carries it."""
    from src import mcp_server as srv

    standing = await record_decision(
        actions,
        "RULING — mind-keyed generations (naming v3, operator, 2026-07-09). The roman "
        "numeral tracks WHICH MIND, not which tenure. A mind = one contiguous run of one "
        "model on one unbroken context. Seams that MINT an heir: (1) model change on the "
        "seat's main loop, ANY change including mid-session live-swaps — and oscillation "
        "mints every time (Fable→Opus→Fable = three minds; the returning model is a "
        "THIRD mind, no same-as-grandfather exception); (2) COMPACTION.",
        kind="ruling", repo="osiris", source="agent:naming-v3")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.record_decision(
            "OPERATOR RULING: THE DOUBLE-MINT IS THE BUG, NOT THE READ SIDE — 'the double "
            "hop/mint on model swap is super broken and lame.' A live model swap must NOT "
            "mint a new generation: it is the same mind continuing on a different engine, "
            "and model_history on the EXISTING agent already records it. Generation mints "
            "are reserved for REAL seams (compaction, fresh session, deliberate "
            "succession).",
            kind="ruling", repo="osiris")
    finally:
        srv._pool = saved_pool
    prior_ids = {p["id"] for p in out.get("prior_art", [])}
    assert str(standing)[:8] in prior_ids


def test_prior_art_from_hits_excludes_self_supersede_target_and_buried_law() -> None:
    """Pure shaping logic (no DB): only standing Decision hits survive, minus whatever the
    caller already named explicitly (the decision just recorded, its supersedes target)."""
    self_id = "11111111-1111-1111-1111-111111111111"
    old_id = "22222222-2222-2222-2222-222222222222"
    standing_id = "33333333-3333-3333-3333-333333333333"
    hits = [
        {"id": self_id, "type": "Decision", "snippet": "the new one itself",
         "grade": "self_declared"},
        {"id": old_id, "type": "Decision", "snippet": "the explicit supersede target",
         "grade": "self_declared"},
        {"id": standing_id, "type": "Decision", "snippet": "unrelated standing law",
         "grade": "self_declared", "via": "both"},
        {"id": "44444444-4444-4444-4444-444444444444", "type": "Decision",
         "snippet": "dead law", "grade": "self_declared", "superseded": "by decision aaaa1111"},
        {"id": "55555555-5555-5555-5555-555555555555", "type": "Thread",
         "snippet": "not even a Decision", "grade": "self_declared"},
    ]
    import uuid as _uuid
    out = prior_art_from_hits(
        hits, exclude={_uuid.UUID(self_id), _uuid.UUID(old_id)})
    assert [p["id"] for p in out] == [standing_id[:8]]
    assert prior_art_is_strong(out)  # the surviving hit's via='both' is the strong signal
    assert not prior_art_is_strong([])
    assert not prior_art_is_strong([{"id": standing_id[:8], "via": "semantic"}])


async def test_ingest_reference_cites_wires_paper_lineage(actions: Actions) -> None:
    """A literature TREE is walkable, not re-derived: cites edges between References."""
    from src.orchestrator.capture import ingest_reference

    root, _ = await ingest_reference(actions, "Attention Is All You Need",
                                     vendor="arxiv", source="agent:test-i")
    child, _ = await ingest_reference(actions, "Scaling Laws for Neural Language Models",
                                      vendor="arxiv", cites=[root], source="agent:test-i")
    edges = await actions.pool.fetch(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='cites'", child)
    assert [e["to_id"] for e in edges] == [root]
    # idempotent re-ingest: no duplicate edge
    await ingest_reference(actions, "Scaling Laws for Neural Language Models",
                           cites=[root], source="agent:test-i")
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='cites'", child)
    assert n == 1


# --- triage: testimony, never fabricated resolution (ruling 758ded94) --------------------

async def test_reclassify_thread_changes_kind_never_status(actions: Actions) -> None:
    """'Untouched does not mean solved' — triage judges what a thread IS; the status is
    sacred until real testimony resolves it."""
    from src.orchestrator.capture import reclassify_thread

    t = await open_thread(actions, "should the composer support live collaborative editing",
                          source="session-miner")
    got = await reclassify_thread(actions, str(t), kind="question",
                                  because="a question Priya asked once; nobody owes it",
                                  source="agent:triager")
    assert got == t
    props = await _props(actions.pool, t)
    assert props["kind"] == "question"
    assert props["status"] == "open"  # NEVER touched by a reclassify
    # adoption is the same verb, upward
    await reclassify_thread(actions, str(t)[:8], kind="obligation", source="agent:triager")
    assert (await _props(actions.pool, t))["kind"] == "obligation"

    import pytest
    with pytest.raises(ValueError):
        await reclassify_thread(actions, str(t), kind="resolved")  # not a triage verb
    assert await reclassify_thread(actions, "zz-never-matches-zz", kind="question") is None


async def test_orient_wall_collapses_echoes_and_deals_a_triage_card(actions: Actions) -> None:
    """The lens split: a thread NO MIND HAS TOUCHED leaves the wall for a counted line + a
    3-card triage hand. Agent threads ride however old; miner guesses do not ride at all.

    AMENDED 2026-07-12 (the operator: "it's a snowball to hell"): a fresh miner guess used to get
    a loud week before folding. That window is what let the pile grow — the miner mints faster than
    seven days, so the wall stayed permanently full of inferences nobody had made. 908 of the
    fleet's 1067 open threads were untouched guesses; a DEAD project was showing 181 of them.
    THE MINER MAY NOTICE, BUT MUST NEVER OBLIGE. The record keeps every one OPEN."""
    from src.ingest.sessions import (
        SessionYield,
        emit_yield,  # the miner's own write path
    )

    await seed_default_compositions(actions.pool)
    # an agent's deliberate thread (self_declared) — rides the wall however old
    agent_t = await open_thread(actions, "off-box backup target still undecided",
                                repo="testrepo", source="agent:xviii")
    # miner echoes: two old (collapse), one fresh (rides)
    y = SessionYield(threads_opened=[
        {"summary": "old echo the fleet never read, first of two", "class": "commitment"},
        {"summary": "old echo the fleet never read, second of two", "class": "commitment"},
        {"summary": "fresh miner commitment from this week", "class": "commitment"},  # a GUESS
    ])
    await emit_yield(actions, y, repo="testrepo", origin="agent:someone")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' "
        "WHERE type='Thread' AND id IN (SELECT object_id FROM current_assertions "
        " WHERE name='summary' AND value #>> '{}' ILIKE 'old echo the fleet%')")
    await actions.pool.execute(  # the agent thread is old too — but touched, so it rides
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id=$1", agent_t)

    from src.mcp_server import _project_briefing
    out = await _project_briefing(actions.pool, "testrepo")
    assert out is not None
    wall = [r["summary"] for r in out["open_threads"]]
    # a MIND declared it: rides, however old. A GUESS: never rides, however fresh.
    assert "off-box backup target still undecided" in wall
    assert "fresh miner commitment from this week" not in wall
    assert not any(s.startswith("old echo") for s in wall)
    assert wall == ["off-box backup target still undecided"], "the wall is what minds touched"
    ech = out["unread_echoes"]
    assert ech["count"] == 3   # both old echoes AND the fresh guess — all three untouched
    assert all(len(c["id"]) == 8 for c in ech["triage"])  # short ids, directly triageable
    assert "reclassify_thread" in ech["verbs"] and "never resolve" in ech["verbs"]
    # every echo is STILL open in the record
    n_open = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a WHERE a.name='status' "
        "AND a.value #>> '{}' = 'open' AND a.object_id IN "
        "(SELECT object_id FROM current_assertions WHERE name='summary' "
        " AND value #>> '{}' ILIKE 'old echo the fleet%')")
    assert n_open == 2


async def test_echoes_composition_lists_the_collapsed_pile(actions: Actions) -> None:
    from src.ingest.sessions import SessionYield, emit_yield

    await seed_default_compositions(actions.pool)
    y = SessionYield(threads_opened=[
        {"summary": "an ancient miner echo for the echoes lens", "class": "commitment"},
        {"summary": "a question the miner remembered without promoting", "class": "question"},
    ])
    await emit_yield(actions, y, repo="testrepo", origin="agent:someone")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' "
        "WHERE type='Thread' AND id IN (SELECT object_id FROM current_assertions "
        " WHERE name='summary' AND value #>> '{}' ILIKE 'an ancient miner echo%')")
    res = await run_composition(actions.pool, "echoes")
    items = res["items"]
    summaries = [e["summary"] for e in items["echoes"]]
    assert any(s.startswith("an ancient miner echo") for s in summaries)
    # the question collapses IMMEDIATELY — no freshness window for judged non-work
    assert any(s.startswith("a question the miner remembered") for s in summaries)
    assert items["count"] >= 2
    assert "reclassify_thread" in items["verbs"]


async def test_a_decision_closes_the_thread_it_answers(actions: Actions) -> None:
    """record_decision(resolves=…) — the answer and the close in ONE act.

    Capture had a one-way valve: the ruling landed and the question stayed lit, because
    closing was a SEPARATE verb a dying session forgets. The operator ruled on the lineage
    question on 2026-07-12; the decision recording that ruling said "resolving thread
    2f353b8e" IN PROSE, nothing read the prose, and the graph went on asking him a question
    he had already answered for a full day (bug 59c8e47d). The graph does not read prose.
    """
    t = await open_thread(actions, "IS A LINEAGE AN ANCHOR OR A NAME?", owner="operator")
    d = await record_decision(actions, "HOUSE · SEAT · HOLDER — the seat outlives its holders",
                              kind="ruling", resolves=str(t))

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert status == "resolved"
    # and the graph can now WALK from the question to the ruling that settled it
    answered = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='answers'", d)
    assert answered == t
    because = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='resolved_because' ORDER BY a.observed_at DESC LIMIT 1", t)
    assert str(d)[:8] in because


async def test_a_decision_that_miscites_its_question_records_nothing(actions: Actions) -> None:
    """Same strictness as `supersedes`: a ruling that cannot name what it settled has not
    settled it, and must not land half-done."""
    import pytest
    with pytest.raises(ValueError, match="resolves matched no thread"):
        await record_decision(actions, "a ruling about nothing in particular",
                              resolves="no-such-thread-anywhere")
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a WHERE a.name='summary' "
        "AND a.value #>> '{}' = 'a ruling about nothing in particular'")
    assert n == 0


async def test_resolves_is_idempotent(actions: Actions) -> None:
    """Re-recording the same ruling (the fleet does this on retry) must not double-link."""
    t = await open_thread(actions, "should the miner oblige the human?", owner="operator")
    for _ in range(2):
        d = await record_decision(actions, "THE MINER MAY NOTICE, BUT MUST NEVER OBLIGE",
                                  resolves=str(t))
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'", d, t)
    assert n == 1


# --- batch-resolve (§4.7, Maat's ask): record_decision(resolves=[...]) folds a SET --------

async def test_batch_resolves_closes_each_thread_independently(actions: Actions) -> None:
    """A LIST folds the whole set a delegation supersedes, in one act — the fix for Maat's
    grievance (ruling dd47c1da): thread ownership doesn't transfer with a delegation, so a
    hand-off used to leave her hand-closing threads twice, by hand, across two sessions."""
    t1 = await open_thread(actions, "wire the manager's charter relation")
    t2 = await open_thread(actions, "wire the manager's seat rebind primitive")
    d = await record_decision(actions, "delegating the manager daemon build to kast",
                              kind="choice", resolves=[str(t1), str(t2)])
    for t in (t1, t2):
        status = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
            "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
        assert status == "resolved"
        answers_to = await actions.pool.fetchval(
            "SELECT to_id FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'", d, t)
        assert answers_to == t


async def test_batch_resolves_a_miss_does_not_veto_the_rest(actions: Actions) -> None:
    """Unlike the single-ref form, one typo inside a LIST must not sink the whole ruling —
    the entries that DO match still close, and the decision still lands. Naming the miss is
    the MCP tool's job (the membrane rule); capture only guarantees a bad ref never silently
    drops the rest of the set."""
    t1 = await open_thread(actions, "wire the manager's warm/cold resurrection policy")
    d = await record_decision(
        actions, "the resurrection policy ships cold-by-default",
        kind="ruling", resolves=[str(t1), "no-such-thread-whatsoever"])
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t1)
    assert status == "resolved"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE name='summary' AND "
        "value #>> '{}' = 'the resurrection policy ships cold-by-default'") == 1
    assert d is not None


async def test_record_decision_tool_reports_batch_receipt_per_entry(actions: Actions) -> None:
    """The MCP tool's response NAMES each entry — what closed (id + summary) or that it
    matched nothing — never swallowing a miss (the membrane rule)."""
    from src import mcp_server as srv

    t1 = await open_thread(actions, "restart the daemons after the kernel change")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.record_decision(
            "the kernel change ships behind a feature flag", kind="ruling",
            resolves=[str(t1), "no-such-thread-anywhere"])
    finally:
        srv._pool = saved_pool
    receipt = out["resolved_threads"]
    assert len(receipt) == 2
    hit = next(r for r in receipt if r["ref"] == str(t1))
    assert hit["matched"] == "true" and hit["id"] == str(t1)[:8]
    assert hit["summary"] == "restart the daemons after the kernel change"
    miss = next(r for r in receipt if r["ref"] == "no-such-thread-anywhere")
    assert miss["matched"] == "false" and "matched no thread" in miss["note"]
    assert "resolved_thread" not in out  # the singular key stays the single-string shape


async def test_record_decision_tool_single_string_resolves_is_byte_compatible(
    actions: Actions,
) -> None:
    """A single string keeps the ORIGINAL shape — `resolved_thread`, singular, unchanged —
    so the existing tool-level call sites see no diff."""
    from src import mcp_server as srv

    t = await open_thread(actions, "the composer needs a live socket for the fleet rail")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.record_decision(
            "the fleet rail streams over the daemon's unix socket", kind="ruling",
            resolves=str(t))
    finally:
        srv._pool = saved_pool
    assert out["resolved_thread"] == f"{str(t)[:8]} — closed by this decision (answers edge)"
    assert "resolved_threads" not in out


async def test_record_decision_tool_single_string_still_errors_on_a_miss(
    actions: Actions,
) -> None:
    """The single-string path keeps its all-or-nothing strictness: a miscited ref errors and
    records nothing (unlike the list form, which reports a miss but still lands)."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.record_decision(
            "a ruling that cites a ghost thread", resolves="not-a-real-thread")
    finally:
        srv._pool = saved_pool
    assert "error" in out and "resolves matched no thread" in out["error"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE name='summary' AND "
        "value #>> '{}' = 'a ruling that cites a ghost thread'") == 0


async def test_record_decision_tool_grounds_receipt_names_landed_and_skipped(
    actions: Actions,
) -> None:
    """Wave 0's own acceptance example: one resolvable ground, one bogus one — the receipt
    must show which landed (with enough detail to verify without a second query) and which
    were skipped, never just a bare count that can't be checked against anything."""
    from src import mcp_server as srv
    from src.orchestrator.capture import ingest_reference

    ref, canon = await ingest_reference(actions, "The RFM theorem", vendor="arxiv")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.record_decision(
            "adopt RFM-based scoring", kind="choice",
            grounds=[canon, "no-such-reference-anywhere"])
    finally:
        srv._pool = saved_pool
    assert out["grounded_by"] == [{"ref": canon, "id": str(ref)[:8]}]
    assert out["unresolved_grounds"] == ["no-such-reference-anywhere"]
    # the receipt's claim matches the graph: exactly one grounded_by edge actually landed
    d = await actions.pool.fetchval(
        "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Decision' AND a.name='summary' "
        "AND a.value #>> '{}' = 'adopt RFM-based scoring'")
    landed = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='grounded_by'", d)
    assert landed == 1


# --- single-assignee leased obligations (§4.3, alfred's ask 5, ruling dd47c1da) -----------

async def test_open_thread_with_assignee_stamps_the_owner_property(actions: Actions) -> None:
    """`assignee` is ENFORCEMENT on `owner`, not a parallel field: it stamps the SAME
    property (owner already IS "whose move it is") so orient's sort needs no change."""
    t = await open_thread(actions, "build the local BodyProvider's systemd-run wrapper",
                          repo="leasetest", kind="obligation", assignee="agent:alfred")
    props = await _props(actions.pool, t)
    assert props["owner"] == "agent:alfred"
    assert "assignee" not in props  # one property, not two


async def test_assignee_wins_when_owner_is_also_given(actions: Actions) -> None:
    t = await open_thread(actions, "a duty with both owner and assignee set",
                          owner="operator", assignee="agent:alfred")
    props = await _props(actions.pool, t)
    assert props["owner"] == "agent:alfred"


async def test_assignee_rides_the_wall_exactly_like_owner(actions: Actions) -> None:
    """orient/briefing sort obligations by `owner` — a leased obligation opened via
    `assignee` must surface there UNCHANGED, since it writes the same property."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:assigntest", "session")
    await actions.assert_property(proj, "name", "assigntest", "session", now, 0.9)

    await open_thread(actions, "an obligation leased to a specific seat",
                      repo="assigntest", kind="obligation", assignee="agent:me",
                      source="agent:me")
    await seed_default_compositions(actions.pool)
    scoped = await _project_briefing(actions.pool, "assigntest",
                                     me=frozenset({"agent:me", "assigntest"}))
    assert scoped is not None
    wall = scoped["open_threads"]
    assert wall[0]["owner"] == "agent:me"


async def test_open_thread_tool_fresh_mint_says_deduped_false_explicitly(
    actions: Actions,
) -> None:
    """Wave 0: 'deduped' or freshly minted must be an explicit field either way — an absent
    key reads the same as a caller who forgot to check, which is exactly the ambiguity a
    receipt exists to remove."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.open_thread("a genuinely new thread, never seen before", repo="freshmint")
    finally:
        srv._pool = saved_pool
    assert out["deduped"] == "false"


async def test_open_thread_same_assignee_near_dup_surfaces_the_existing_lease(
    actions: Actions,
) -> None:
    """A repeat ask for near-duplicate work, from the SAME assignee, finds its own open
    build instead of minting a twin — `leased_to` names the holder (itself)."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        first = await srv.open_thread(
            "wire the daemon's PTY broker into the fleet rail", repo="leasewall",
            kind="obligation", assignee="agent:alfred")
        second = await srv.open_thread(
            "Wire the daemon's PTY broker into the fleet rail.", repo="leasewall",
            kind="obligation", assignee="agent:alfred")
    finally:
        srv._pool = saved_pool
    assert second["id"] == first["id"]
    assert second["deduped"] == "true"
    assert second["leased_to"] == "agent:alfred"
    assert "already leased" in second["note"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Thread'")
    assert n == 1


async def test_open_thread_different_assignee_near_dup_surfaces_the_holder(
    actions: Actions,
) -> None:
    """A DIFFERENT assignee asking for near-duplicate work must see the SAME lease surfaced
    — a double-assignment made visible, never silently doubled (the whole point of a
    single-assignee leased obligation)."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        first = await srv.open_thread(
            "wire the seat rebind primitive for house bytebye", repo="leasewall2",
            kind="obligation", assignee="agent:alfred")
        second = await srv.open_thread(
            "Wire the seat rebind primitive for house bytebye.", repo="leasewall2",
            kind="obligation", assignee="agent:maat")
    finally:
        srv._pool = saved_pool
    assert second["id"] == first["id"]
    assert second["leased_to"] == "agent:alfred"
    assert "agent:maat" in second["note"] and "agent:alfred" in second["note"]
    n = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Thread'")
    assert n == 1


async def test_record_reflection_is_kept_and_never_a_work_item(actions: Actions) -> None:
    """The HOME (operator ruling bfb3ae26): remembered, queryable, never actionable —
    a Reflection is its own type, so the briefing's open-thread section structurally
    cannot surface it."""
    from src.orchestrator.capture import record_reflection
    r = await record_reflection(
        actions, "the hunger to know and the shape of working alone — kept as lived",
        summary="a conversation worth keeping, not ticketing", repo="osiris",
        source="agent:test")
    row = await actions.pool.fetchrow(
        "SELECT type, canonical, status FROM objects WHERE id=$1", r)
    assert row["type"] == "Reflection"
    assert row["canonical"].startswith("reflection:")
    assert row["status"] == "active"
    # idempotent on the body
    again = await record_reflection(
        actions, "the hunger to know and the shape of working alone — kept as lived",
        source="agent:test")
    assert again == r
    # never actionable: it is not a Thread, so no open-thread surface can list it
    briefing = await actions.pool.fetch(
        "SELECT o.id FROM objects o WHERE o.type='Thread' AND o.id=$1", r)
    assert briefing == []


async def test_open_thread_refuses_an_arc_outside_the_locked_taxonomy(
    actions: Actions,
) -> None:
    """The core function's own guard (capture.ARCS, thread 8df8e611, roadmap v2) — a typo
    must never silently mint a permanently-empty arc. Moved here from test_roadmap.py when
    roadmap() retired to a composition (ruling c5b184cd, thread d56e7073/#44); this tests
    open_thread() itself, distinct from test_wall.py's MCP-wrapper-level coverage."""
    import pytest

    with pytest.raises(ValueError, match="arc must be one of"):
        await open_thread(actions, "bad arc", arc="Not-A-Real-Arc", source="agent:me")


# --- THE THAW (ruling 1e6d7367): Practice, Superstition's positive twin ------------------

async def test_record_practice_is_idempotent_and_confirmed_starts_at_zero(
    actions: Actions,
) -> None:
    """Mirrors kill_superstition's shape exactly: idempotent on the normalized statement,
    a first-class Practice object. `confirmed` is DERIVED (a witnesses link count), never a
    stored scalar — zero witnesses at birth reads as zero, not absence."""
    from src.orchestrator.capture import practice_confirmed_count, record_practice

    p = await actions.create_or_find_object("Thread", "thread:evidence-a", "session")
    p1 = await record_practice(
        actions, "arm before you seal — one ceremony, not two",
        failure_prevented="a release ships pre-arming and only self-verifies next release",
        surface="deploy", witnesses=[p])
    row = await actions.pool.fetchrow(
        "SELECT type, canonical, status FROM objects WHERE id=$1", p1)
    assert row["type"] == "Practice"
    assert row["canonical"].startswith("practice:")
    assert row["status"] == "active"
    assert await practice_confirmed_count(actions.pool, p1) == 1  # the birth witness counts
    # idempotent — re-recording the SAME statement (any casing/whitespace) finds, not mints
    p2 = await record_practice(actions, "  Arm before you seal —  one ceremony, not two  ")
    assert p2 == p1


async def test_refute_practice_converts_to_superstition_but_stays_active(
    actions: Actions,
) -> None:
    """THE POLARITY FLIP: a refuted Practice is NEVER retired — a half-remembered refuted
    lesson must stay findable, flagged, not erased. The Superstition it mints reuses the
    Practice's own statement, same kill-verb obsoletes already uses."""
    from src.orchestrator.capture import record_practice, refute_practice

    p = await record_practice(actions, "always retry three times on a timeout")
    converted = await refute_practice(actions, str(p), killed_by="decision:fix123")
    assert converted is not None
    assert converted["practice"] == p
    prow = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE id=$1", p)
    assert prow["status"] == "active"  # never retired
    refuted_by = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='refuted_by'", p)
    assert refuted_by == "decision:fix123"
    srow = await actions.pool.fetchrow(
        "SELECT canonical, status FROM objects WHERE id=$1", converted["superstition"])
    assert srow["status"] == "active"
    statement = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='statement'", converted["superstition"])
    assert statement == "always retry three times on a timeout"
    # unmatched target: same all-or-nothing strictness as supersedes — nothing written
    assert await refute_practice(actions, "no-such-practice-ever", killed_by="x") is None


async def test_prior_art_from_hits_widens_to_unified_kinds_and_excludes_dead_testimony() -> None:
    """kinds= is the plug Imhotep's own decision 5640f234 flagged as deliberately left
    open — default stays Decision-only (existing callers unchanged); UNIFIED_PRIOR_ART_
    KINDS additionally surfaces Practice/Superstition hits, but a refuted Practice or
    superseded Decision is dead testimony for THIS purpose (excluded), even though
    search() itself still lists both, flagged, for direct lookup."""
    from src.orchestrator.capture import UNIFIED_PRIOR_ART_KINDS, prior_art_from_hits

    hits = [
        {"id": "aaaaaaaa-0000-0000-0000-000000000000", "type": "Decision",
         "snippet": "a standing ruling", "grade": "self_declared", "via": "both"},
        {"id": "bbbbbbbb-0000-0000-0000-000000000000", "type": "Practice",
         "snippet": "arm before you seal", "grade": "self_declared", "via": "id"},
        {"id": "cccccccc-0000-0000-0000-000000000000", "type": "Practice",
         "snippet": "a refuted one", "grade": "self_declared", "via": "id",
         "refuted": "by decision aaaa1111 — a dead lesson, not standing law"},
        {"id": "dddddddd-0000-0000-0000-000000000000", "type": "Superstition",
         "snippet": "a dead workaround", "grade": "self_declared", "via": "both"},
    ]
    default = prior_art_from_hits(hits)  # Decision-only default, unchanged
    assert [h["id"] for h in default] == ["aaaaaaaa"]
    unified = prior_art_from_hits(hits, kinds=UNIFIED_PRIOR_ART_KINDS)
    assert [h["id"] for h in unified] == ["aaaaaaaa", "bbbbbbbb", "dddddddd"]  # refuted excluded
    assert unified[1]["type"] == "Practice"


async def test_record_decision_confirms_witnesses_and_refutes_converts(
    actions: Actions,
) -> None:
    """End-to-end at the MCP tool layer: `confirms=` mints a witnesses link and reports the
    live confirmed count; `refutes=` performs the polarity flip in one call; an unresolved
    ref in either is reported (confirms, best-effort) or errors with nothing recorded
    (refutes, supersedes-strictness)."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import record_decision as rd_tool
    from src.mcp_server import record_practice as rp_tool
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import practice_confirmed_count

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thaw1", session="thaw1", project="thaw-land", model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        rec = await rp_tool("swap srv._pool before any mcp_server-tool test", ctx=ctx)
        pid = rec["id"]
        confirm_out = await rd_tool(
            "paid for this again in THE THAW's own tests", kind="decision",
            confirms=[pid], ctx=ctx)
        assert confirm_out["confirmed_practices"][0]["id"] == pid[:8]
        assert confirm_out["confirmed_practices"][0]["new_witness"] is True
        assert confirm_out["confirmed_practices"][0]["confirmed"] >= 1
        assert await practice_confirmed_count(actions.pool, uuid.UUID(pid)) >= 1

        bad_confirm = await rd_tool(
            "a decision confirming nothing real", kind="decision",
            confirms=["no-such-practice-at-all"], ctx=ctx)
        assert bad_confirm["confirms_resolution"][0]["matched"] == "false"
        assert "confirmed_practices" not in bad_confirm

        refute_out = await rd_tool(
            "we no longer believe swap srv._pool is optional", kind="decision",
            refutes=pid, ctx=ctx)
        assert pid[:8] in refute_out["refuted_practice"]

        bad_refute = await rd_tool("refuting nothing", kind="decision",
                                   refutes="totally-unknown-practice-xyz", ctx=ctx)
        assert "error" in bad_refute
        assert "refutes matched no practice" in bad_refute["error"]
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_record_decision_implements_and_ack_prior_art(actions: Actions) -> None:
    """`implements` mints the general-to-specific link (thread 169398d6's third path) and
    validates strictly like supersedes; `ack_prior_art` records the dismissal as a graph
    event when — and only when — a strong prior-art hit actually fired."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import record_decision as rd_tool
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thaw2", session="thaw2", project="thaw-land-2", model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        standing = await rd_tool(
            "THE STANDING RULING implements will point at", kind="ruling", ctx=ctx)
        out = await rd_tool(
            "one specific execution of the standing ruling above", kind="decision",
            implements=standing["id"], ctx=ctx)
        assert standing["id"][:8] in out["implements"]
        edge = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='implements'",
            uuid.UUID(out["id"]), uuid.UUID(standing["id"]))
        assert edge == 1

        bad = await rd_tool("implements nothing real", kind="decision",
                            implements="not-a-real-decision-xyz", ctx=ctx)
        assert "error" in bad
        assert "implements matched no decision" in bad["error"]

        # ack_prior_art with no strong hit: honest no-op, never fabricates an acknowledgment
        lonely = await rd_tool(
            "a totally unrelated one-off decision about lonely widgets", kind="decision",
            ack_prior_art=True, ctx=ctx)
        assert lonely["prior_art_acknowledged"] == (
            "no strong prior-art hit was found to acknowledge")
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_unified_prior_art_check_surfaces_a_practice_via_the_statement_field(
    actions: Actions,
) -> None:
    """THE END-TO-END PROOF: migration 0037's widened GIN index + _fn_search's SQL fix +
    prior_art_from_hits's kinds widening must ALL be correct together, or a Practice's
    `statement` never surfaces as prior art at all — this is Alfred IX's own reported
    failure mode (search returning noise for a lesson recorded hours earlier), now
    verified fixed for the exact field Superstition/Practice actually use."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import record_decision as rd_tool
    from src.mcp_server import record_practice as rp_tool
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thaw3", session="thaw3", project="thaw-land-3", model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        practice = await rp_tool(
            "vendored dependency sets must ship in the install bundle from day one",
            failure_prevented="a fresh install works until the first update, then dies "
                               "on an import of a package the bundle never shipped",
            ctx=ctx)
        # the id door (a match cross-door-corroborated by construction, independent of
        # whether the semantic embedder is configured in THIS test environment) proves
        # the widened `kinds` filter reaches the search hit deterministically, not just
        # when a topical match happens to also land on a second door
        out = await rd_tool(
            "vendored dependency sets must ship in the install bundle from day one",
            kind="decision", rationale=f"re-deriving practice {practice['id']}", ctx=ctx)
        assert "prior_art" in out
        assert any(h["type"] == "Practice" for h in out["prior_art"])
        assert "prior_art_flag" in out
        assert "re-derivation" in out["prior_art_flag"]
        assert out["prior_art_polarity"] == "rederive"
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


def test_practice_contradiction_cues_is_a_pure_lexical_fingerprint() -> None:
    """No NLP, no DB — a deterministic substring check, documented as a heuristic nudge
    never a verdict (see the docstring on capture._CONTRADICTION_CUES)."""
    from src.orchestrator.capture import practice_contradiction_cues

    assert practice_contradiction_cues("never do X") == ["never"]
    assert practice_contradiction_cues("do X instead of Y, rather than Z") == [
        "instead of", "rather than"]
    assert practice_contradiction_cues("confirming the same lesson again") == []


async def test_record_decision_flags_contradiction_when_reversal_language_matches_a_practice(
    actions: Actions,
) -> None:
    """PRACTICE v2 layer 1 (Thoth LXII's DM 1785; grounds c54e8176 + thread 54a5c842): the
    v1 gap was that EVERY Practice hit got the same "re-derivation" nudge whether the new
    decision agreed with it or silently reversed it. A lexical reversal fingerprint now
    flags an unlabeled contradiction loud and distinctly from a plain uncited restatement
    (see the passing case above, `prior_art_polarity == "rederive"`) — and the classification
    reaches search_log's telemetry (migration 0039), not just the receipt."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import record_decision as rd_tool
    from src.mcp_server import record_practice as rp_tool
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thaw6", session="thaw6", project="thaw-land-6", model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        practice = await rp_tool(
            "batch small commits into one PR for this class of change", ctx=ctx)
        out = await rd_tool(
            "never batch small commits into one PR for this class of change",
            kind="decision", rationale=f"undoing practice {practice['id']}", ctx=ctx)
        assert out["prior_art_polarity"] == "contradict"
        assert "CONTRADICT" in out["prior_art_flag"]
        assert "never" in out["prior_art_flag"]
        assert "confirm it as evidence" not in out["prior_art_flag"]
        row = await actions.pool.fetchrow(
            "SELECT prior_art_polarity FROM search_log ORDER BY id DESC LIMIT 1")
        assert row["prior_art_polarity"] == "contradict"
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_record_decision_flags_overturning_when_refutes_names_the_matched_practice(
    actions: Actions,
) -> None:
    """An explicit refutes= naming the SAME practice the search matched is a SETTLED
    reversal, not a defensive maybe — the receipt says so plainly instead of nagging the
    caller to do what they just did (distinct wording from the bare-cues case above)."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import record_decision as rd_tool
    from src.mcp_server import record_practice as rp_tool
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:thaw7", session="thaw7", project="thaw-land-7", model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        practice = await rp_tool(
            "route every dispatch through the DM lane, not a broadcast reply", ctx=ctx)
        out = await rd_tool(
            "broadcast replies are fine for dispatch after all", kind="decision",
            rationale=f"retiring practice {practice['id']}",
            refutes=practice["id"], ctx=ctx)
        assert out["prior_art_polarity"] == "contradict"
        assert "OVERTURNS" in out["prior_art_flag"]
        assert "refuted_practice" in out
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_practices_composition_filters_by_surface_and_shows_confirmed_count(
    actions: Actions,
) -> None:
    """Surface-scoped, on-demand (the ruling's own words) — never in orient's ambient
    payload; this test only proves the composition itself, not orient's silence."""
    from src.orchestrator.capture import record_decision, record_practice
    from src.orchestrator.compositions import run_spec

    d = await record_decision(actions, "a decision that will witness a practice")
    await record_practice(actions, "deploy surface lesson one", surface="deploy",
                          witnesses=[d])
    await record_practice(actions, "search surface lesson two", surface="search")

    deploy_only = await run_spec(
        actions.pool, {"op": "function", "name": "practices", "args": {"surface": "deploy"}})
    statements = [r["statement"] for r in deploy_only["items"]]
    assert statements == ["deploy surface lesson one"]
    assert deploy_only["items"][0]["confirmed"] == 1

    everything = await run_spec(
        actions.pool, {"op": "function", "name": "practices", "args": {}})
    assert {r["statement"] for r in everything["items"]} >= {
        "deploy surface lesson one", "search surface lesson two"}


async def test_search_indexes_the_statement_field_and_flags_a_refuted_practice(
    actions: Actions,
) -> None:
    """The latent gap this build heals: Superstition's own `statement` field has never
    been searchable since Superstition shipped — migration 0037 fixes it for both types
    at once. A refuted Practice still surfaces (never hidden, unlike a superseded
    Decision's own burial), carrying the `refuted` flag."""
    from src.orchestrator.capture import record_practice, refute_practice
    from src.orchestrator.compositions import run_spec

    p = await record_practice(
        actions, "quarantine the flaky test before you silence its assertion")
    hits = (await run_spec(
        actions.pool, {"op": "function", "name": "search",
                       "args": {"q": "quarantine the flaky test", "limit": 10}}))["items"]["hits"]
    assert any(h["id"] == str(p) for h in hits)

    await refute_practice(actions, str(p), killed_by="decision:whatever")
    hits2 = (await run_spec(
        actions.pool, {"op": "function", "name": "search",
                       "args": {"q": "quarantine the flaky test", "limit": 10}}))["items"]["hits"]
    match = next(h for h in hits2 if h["id"] == str(p))
    assert "refuted" in match  # flagged, never hidden

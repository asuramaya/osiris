"""The promoted offload-ritual boxes (ruling c5b184cd) — shared by the Stop hook and the
/settle MCP tool, so the two never drift into disagreeing copies."""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision
from src.orchestrator.settle import (
    charter_touched,
    closure_edge_coverage,
    filed_under_check,
    missing_boxes,
    settle_boxes,
    uncommitted_git_work,
)


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def test_charter_touched_absent_file_cannot_be_evaluated(tmp_path: Path) -> None:
    """No charter.md here at all — a repo cwd, not an office — fails open, never punished."""
    assert charter_touched(str(tmp_path), datetime.now(UTC)) is None


def test_charter_touched_checks_mtime_against_session_start(tmp_path: Path) -> None:
    charter = tmp_path / "charter.md"
    charter.write_text("# notes\n")
    now = datetime.now(UTC)
    assert charter_touched(str(tmp_path), now - timedelta(minutes=5)) is True
    assert charter_touched(str(tmp_path), now + timedelta(minutes=5)) is False


def test_charter_touched_none_cwd_cannot_be_evaluated() -> None:
    assert charter_touched(None, datetime.now(UTC)) is None


def test_missing_boxes_only_names_explicit_false() -> None:
    """None (fog-of-war, unevaluable) and True (satisfied) never count as missing —
    only an explicit False does."""
    assert missing_boxes({"a": True, "b": False, "c": None, "d": False}) == ["b", "d"]
    assert missing_boxes({"a": True, "b": None}) == []
    assert missing_boxes({}) == []


async def test_uncommitted_git_work_none_cwd_cannot_be_evaluated() -> None:
    assert await uncommitted_git_work(None) is None


async def test_uncommitted_git_work_a_non_repo_dir_cannot_be_evaluated(tmp_path: Path) -> None:
    """The common, innocent case: a seat-office cwd, or any ordinary non-repo directory —
    fails open, same as charter_touched on a missing file."""
    assert await uncommitted_git_work(str(tmp_path)) is None


async def test_uncommitted_git_work_a_clean_repo_reports_empty(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    (tmp_path / "committed.txt").write_text("hi\n")
    _git(tmp_path, "add", "committed.txt")
    _git(tmp_path, "-c", "user.email=t@t.t", "-c", "user.name=t", "commit", "-m", "seed")
    assert await uncommitted_git_work(str(tmp_path)) == []


async def test_uncommitted_git_work_names_the_dirty_files(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    (tmp_path / "untracked.txt").write_text("new\n")
    out = await uncommitted_git_work(str(tmp_path))
    assert out is not None and any("untracked.txt" in line for line in out)


async def test_settle_boxes_works_against_a_pool_not_just_a_raw_connection(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole point of the promotion: settle_boxes must serve BOTH callers — the hook's
    raw asyncpg.Connection (its ~1s budget can't hold a pool) and the MCP server's
    asyncpg.Pool. Exercised here via the pool directly, unlike test_stophook.py's coverage
    (which only ever calls it through the hook's own connection)."""
    now = datetime.now(UTC)
    mounted_at = now - timedelta(minutes=10)
    agent = "agent:se77le01"
    a = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(a, "minted_because", "compaction", agent, now, 0.9,
                                  evidence_class="self_declared")
    office = tmp_path / "office"
    office.mkdir()

    boxes = await settle_boxes(actions.pool, agent_id=agent, mounted_at=mounted_at,
                               cwd=str(office))
    assert boxes["decisions recorded this session"] is False
    assert boxes["threads trued this session (opened or resolved)"] is False
    assert boxes["charter.md touched this session"] is None  # no such file here
    assert boxes["a live succession/handoff note (this lineage was minted)"] is False

    await record_decision(actions, "a real ruling this session", source=agent)
    await open_thread(actions, "an obligation left for the heir", kind="obligation",
                      source=agent)
    (office / "charter.md").write_text("# notes\n")

    boxes = await settle_boxes(actions.pool, agent_id=agent, mounted_at=mounted_at,
                               cwd=str(office))
    assert boxes["decisions recorded this session"] is True
    assert boxes["threads trued this session (opened or resolved)"] is True
    assert boxes["charter.md touched this session"] is True
    assert boxes["a live succession/handoff note (this lineage was minted)"] is True


async def test_settle_boxes_a_non_minted_agent_carries_no_succession_box(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:se77le02"
    await actions.create_or_find_object("Agent", agent, agent)
    boxes = await settle_boxes(actions.pool, agent_id=agent, mounted_at=datetime.now(UTC),
                               cwd=str(tmp_path))
    assert "a live succession/handoff note (this lineage was minted)" not in boxes


# ═══ filed_under_check — settle verifies WHAT you wrote, never WHO can read it ═══
# (Thoth's Lane 4 finding, 2026-07-31): report-only, never folded into missing_boxes.


async def test_filed_under_check_none_when_project_unknown() -> None:
    assert await filed_under_check(
        None, agent_id="agent:fu01", mounted_at=datetime.now(UTC), project=None) is None


async def test_filed_under_check_none_when_nothing_written_this_session(
    actions: Actions,
) -> None:
    """No signal either way — silence is not evidence of a mismatch."""
    agent = "agent:fu02"
    await actions.create_or_find_object("Agent", agent, agent)
    out = await filed_under_check(actions.pool, agent_id=agent, mounted_at=datetime.now(UTC),
                                  project="someproj")
    assert out is None


async def test_filed_under_check_coherent_when_writes_match_filed_under_project(
    actions: Actions,
) -> None:
    agent = "agent:fu03"
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await record_decision(actions, "fu03's ruling filed under its own project",
                          repo="matchproj", source=agent)
    await open_thread(actions, "fu03's thread filed under its own project",
                      repo="matchproj", source=agent)
    out = await filed_under_check(actions.pool, agent_id=agent, mounted_at=mounted_at,
                                  project="matchproj")
    assert out == {"filed_under": "matchproj", "writes_went_to": ["matchproj"],
                   "coherent": True}


async def test_filed_under_check_flags_a_mismatch_without_erroring(actions: Actions) -> None:
    """John XVI's exact shape: writes land under a DIFFERENT project than the one this
    session is filed under. Report-only — the function itself has no notion of failure,
    just names both sides plainly."""
    agent = "agent:fu04"
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await record_decision(actions, "fu04's ruling, written while mounted elsewhere",
                          repo="ballgem", source=agent)
    out = await filed_under_check(actions.pool, agent_id=agent, mounted_at=mounted_at,
                                  project="redmonth")
    assert out == {"filed_under": "redmonth", "writes_went_to": ["ballgem"],
                   "coherent": False}


async def test_filed_under_check_names_every_distinct_project_written_to(
    actions: Actions,
) -> None:
    agent = "agent:fu05"
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await record_decision(actions, "fu05's first ruling", repo="projA", source=agent)
    await open_thread(actions, "fu05's thread", repo="projB", source=agent)
    out = await filed_under_check(actions.pool, agent_id=agent, mounted_at=mounted_at,
                                  project="projA")
    assert out is not None
    assert out["writes_went_to"] == ["projA", "projB"]
    assert out["coherent"] is False  # projB alone breaks coherence


# ═══ closure_edge_coverage — Phase 1b (decision cb38d922): "78% OF CLOSURES LEAVE NO
# TRAVERSABLE TRACE" — report-only, same discipline as filed_under_check above.


async def test_closure_edge_coverage_none_when_nothing_resolved_this_session(
    actions: Actions,
) -> None:
    """No signal either way — a session that closed zero threads is never punished for
    the silence, the same convention filed_under_check uses."""
    agent = "agent:cec01"
    await actions.create_or_find_object("Agent", agent, agent)
    out = await closure_edge_coverage(actions.pool, agent_id=agent,
                                      mounted_at=datetime.now(UTC))
    assert out is None


async def test_closure_edge_coverage_counts_resolved_threads_and_their_edges(
    actions: Actions,
) -> None:
    """One thread closed WITH a traversable edge (record_decision's own resolves=), one
    resolved WITHOUT any closing verb at all (a raw status property write, the shape a
    pre-Phase-1a/legacy or miner-inferred closure still takes) — coverage counts both as
    resolved this session but only the first as edged. Since Phase 1a (23c5991) + Phase 2b
    (closed_by wired into thread_closure_edges, migration 0044), resolve_thread() itself
    ALWAYS mints some edge, so it can no longer produce the unedged case on its own — the
    asymmetry cb38d922 measured now lives in closures that bypass the sanctioned verbs
    entirely, not in resolve_thread's artifact-less path."""
    agent = "agent:cec02"
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    edged_thread = await open_thread(actions, "cec02's thread that gets an edge", source=agent)
    bare_thread = await open_thread(actions, "cec02's thread resolved by raw property, "
                                    "no closing verb", source=agent)
    await record_decision(actions, "cec02's ruling that answers the first thread",
                          resolves=str(edged_thread)[:8], source=agent)
    await actions.assert_property(bare_thread, "status", "resolved", agent,
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")

    out = await closure_edge_coverage(actions.pool, agent_id=agent, mounted_at=mounted_at)
    assert out == {"resolved_this_session": 2, "with_closure_edge": 1}


# ═══════════ THE settle() MCP TOOL — ruling c5b184cd ═══════════


async def test_settle_tool_refuses_when_unmounted(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.settle()
    finally:
        srv._pool = saved_pool
    assert "error" in out


async def test_settle_tool_accepts_a_decision_and_a_thread_and_verifies_landed(
    actions: Actions,
) -> None:
    """ACCEPT composes the real verbs — never reimplements the write."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:settle01", session="settle01", project="settleproj",
        model=None, cwd=None)
    try:
        out = await srv.settle(
            decisions=[{"summary": "settle wrote this decision itself", "kind": "ruling"}],
            threads_open=[{"summary": "settle opened this obligation itself",
                          "kind": "obligation"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert len(out["accepted"]["decisions"]) == 1
    assert len(out["accepted"]["threads_opened"]) == 1
    d_landed = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Decision' AND a.name='summary' "
        "AND a.value #>> '{}' = 'settle wrote this decision itself'")
    t_landed = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Thread' AND a.name='summary' "
        "AND a.value #>> '{}' = 'settle opened this obligation itself'")
    assert d_landed == 1 and t_landed == 1


async def test_settle_tool_rejects_a_bad_repo_decision_without_sinking_the_dump(
    actions: Actions,
) -> None:
    """Thoth's ruling on #107's fork (DM 2250): settle is the end-of-context ritual — a
    whole-batch abort on one bad item (a path-shaped repo) would lose EVERYTHING else in
    the same call, exactly the failure settle exists to prevent. The good sibling still
    lands; the bad one is NAMED in `rejected`, never silently dropped; `complete` reads
    False because a rejected item is unwritten state, same class as a missing box."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    good_summary = "settle's good decision survives a bad sibling in the same call"
    bad_summary = "settle's decision filed under a path, not a project"

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:settlerj1", session="settlerj1", project="settleproj",
        model=None, cwd=None)
    try:
        out = await srv.settle(
            decisions=[
                {"summary": good_summary},
                {"summary": bad_summary, "repo": "/home/asuramaya/code/ballgem"},
            ],
            threads_open=[
                {"summary": "settle's thread, unaffected by its sibling decision's reject"},
            ],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    # the good decision AND the unrelated thread both still landed — the bad sibling never
    # vetoed either of them
    assert len(out["accepted"]["decisions"]) == 1
    assert len(out["accepted"]["threads_opened"]) == 1
    assert len(out["rejected"]) == 1
    rej = out["rejected"][0]
    assert rej["kind"] == "decision" and rej["summary"] == bad_summary
    assert "never a filesystem path or a placeholder" in rej["error"]
    # a rejected item gates complete, the same as a missing box or an uncommitted file
    assert out["complete"] is False
    assert "1 rejected item(s)" in out["note"]

    # prove it against the DB, not the receipt: the good items are genuinely there...
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Decision' AND a.name='summary' AND a.value #>> '{}' = $1",
        good_summary) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread'") == 1
    # ...and the bad one genuinely never landed — no Decision, no bogus SoftwareProject
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE name='summary' "
        "AND value #>> '{}' = $1", bad_summary) == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject'") == 0


async def test_settle_tool_rejects_a_bad_repo_thread_without_sinking_the_dump(
    actions: Actions,
) -> None:
    """Same per-item degrade, the threads_open loop — a distinct code path from decisions,
    tested independently rather than assumed to behave the same way."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    good_summary = "settle's good thread survives a bad sibling in the same call"
    bad_summary = "settle's thread filed under a path, not a project"

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:settlerj2", session="settlerj2", project="settleproj",
        model=None, cwd=None)
    try:
        out = await srv.settle(
            threads_open=[
                {"summary": good_summary},
                {"summary": bad_summary, "repo": "/home/asuramaya/code/ballgem"},
            ],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert len(out["accepted"]["threads_opened"]) == 1
    assert len(out["rejected"]) == 1
    rej = out["rejected"][0]
    assert rej["kind"] == "thread" and rej["summary"] == bad_summary
    assert "never a filesystem path or a placeholder" in rej["error"]
    assert out["complete"] is False

    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Thread' AND a.name='summary' AND a.value #>> '{}' = $1",
        good_summary) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE name='summary' "
        "AND value #>> '{}' = $1", bad_summary) == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject'") == 0


async def test_settle_tool_resolve_thread_item_reports_a_miss_without_erroring(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:settle02", session="settle02", project="settleproj",
        model=None, cwd=None)
    try:
        out = await srv.settle(
            threads_resolve=[{"ref": "no-such-thread-anywhere-in-this-graph"}], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    (result,) = out["accepted"]["threads_resolved"]
    assert "error" in result and "no-such-thread-anywhere" in result["error"]


# ═══ PHASE 1b — settle wires the closure edge a decision+threads_resolve pair in the SAME
# call already establishes (decision cb38d922, DM 2506) ═══


async def test_settle_tool_wires_the_reverse_edge_for_a_pair_the_batch_establishes(
    actions: Actions,
) -> None:
    """settle already holds BOTH halves in one payload — a decision that resolves= a
    thread, and a threads_resolve item naming the SAME thread, with no artifact= of its
    own. record_decision's resolves= already mints the `answers` edge (Decision->Thread);
    this proves settle ALSO wires the reverse `resolved_by` edge (Thread->Decision), for
    free, straight from what the caller already told it in this one call."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settlewire1"
    thread_id = await open_thread(actions, "settlewire1's thread, resolved two ways at once",
                                  source=agent)
    thread_ref = str(thread_id)[:8]

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlewire1", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(
            decisions=[{"summary": "settlewire1's ruling that answers the thread",
                       "resolves": thread_ref}],
            threads_resolve=[{"ref": thread_ref}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    did = out["accepted"]["decisions"][0]["id"]
    (tres,) = out["accepted"]["threads_resolved"]
    assert out["closure_edges_wired"] == 1
    assert tres["closure_edge_wired_to_decision"] == did

    # prove it against the graph, not just the receipt: BOTH edge directions now exist
    row = await actions.pool.fetchrow(
        "SELECT "
        "  EXISTS(SELECT 1 FROM links WHERE from_id=d.id AND to_id=$1 AND type='answers') "
        "    AS answers, "
        "  EXISTS(SELECT 1 FROM links WHERE from_id=$1 AND to_id=d.id AND type='resolved_by') "
        "    AS resolved_by "
        "FROM objects d WHERE d.type='Decision' AND d.id::text LIKE $2 || '%'",
        thread_id, did)
    assert row is not None
    assert row["answers"] is True
    assert row["resolved_by"] is True


async def test_settle_tool_conservative_join_mints_nothing_when_the_batch_does_not_pair_them(
    actions: Actions,
) -> None:
    """A decision with no resolves= at all, and an UNRELATED thread in threads_resolve —
    the payload never establishes a pair, so settle wires nothing (Thoth's ask: 'a wrong
    edge is worse than a missing one'). Plain resolution still proceeds unaffected."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settlewire2"
    unrelated_thread = await open_thread(actions, "settlewire2's unrelated thread",
                                         source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlewire2", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(
            decisions=[{"summary": "settlewire2's ruling, no resolves= at all"}],
            threads_resolve=[{"ref": str(unrelated_thread)[:8]}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["closure_edges_wired"] == 0
    (tres,) = out["accepted"]["threads_resolved"]
    assert "closure_edge_wired_to_decision" not in tres
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE (from_id=$1 OR to_id=$1) "
        "AND type IN ('answers', 'resolved_by')", unrelated_thread) == 0


async def test_settle_tool_negative_control_decisions_without_resolutions(
    actions: Actions,
) -> None:
    """decisions present, threads_resolve absent — mints nothing, and does not error."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settlewire3"

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlewire3", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(decisions=[{"summary": "settlewire3's lone ruling"}], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["closure_edges_wired"] == 0
    assert out["accepted"]["threads_resolved"] == []


async def test_settle_tool_negative_control_resolutions_without_decisions(
    actions: Actions,
) -> None:
    """threads_resolve present, decisions absent — mints nothing, and does not error."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settlewire4"
    thread_id = await open_thread(actions, "settlewire4's lone thread", source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlewire4", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(threads_resolve=[{"ref": str(thread_id)[:8]}], ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["closure_edges_wired"] == 0
    assert out["accepted"]["decisions"] == []
    (tres,) = out["accepted"]["threads_resolved"]
    assert "closure_edge_wired_to_decision" not in tres


async def test_settle_tool_never_overrides_a_caller_supplied_artifact(actions: Actions) -> None:
    """The caller's own explicit artifact= wins outright, even when a batch decision would
    otherwise match by the conservative join — settle's wiring only fills a GAP, it never
    second-guesses an explicit choice."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settlewire5"
    thread_id = await open_thread(actions, "settlewire5's thread, closed pointing "
                                  "elsewhere on purpose", source=agent)
    other_decision = await record_decision(
        actions, "settlewire5's OTHER decision, cited explicitly as the artifact",
        source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlewire5", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(
            decisions=[{"summary": "settlewire5's NEW ruling that ALSO resolves the thread",
                       "resolves": str(thread_id)[:8]}],
            threads_resolve=[{"ref": str(thread_id)[:8],
                             "artifact": str(other_decision)[:8]}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["closure_edges_wired"] == 0
    (tres,) = out["accepted"]["threads_resolved"]
    assert "closure_edge_wired_to_decision" not in tres
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='resolved_by'",
        thread_id, other_decision) == 1


async def test_settle_tool_surfaces_closure_coverage_without_blocking_complete(
    actions: Actions, tmp_path: Path,
) -> None:
    """Report-only, same discipline as identity_coherence: 'this session resolved N
    threads; M of them now carry a closure edge' — computed AFTER the batch dispatch so it
    reflects edges wired by this very call too. Deliberately PARTIAL coverage here (one
    thread resolved by a raw status write, no closing verb, earlier this same session; one
    freshly edged by this call) proves the field never gates `complete`, however incomplete
    the coverage looks. resolve_thread() itself can no longer produce the unedged case
    since Phase 1a's closed_by fallback + Phase 2b's view wiring (migration 0044) — see
    test_closure_edge_coverage_counts_resolved_threads_and_their_edges above."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settlecc1"
    job_dir = str(tmp_path / "jobs" / "settlecc")  # EXACTLY 8 chars — matches session[:8]
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="settleproj",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)
    (tmp_path / "charter.md").write_text("# notes\n")
    bare_thread = await open_thread(actions, "settlecc1's thread resolved bare, earlier "
                                    "this same session", source=agent)
    await actions.assert_property(bare_thread, "status", "resolved", agent,
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    thread_id = await open_thread(actions, "settlecc1's thread, resolved WITH an edge "
                                  "by this call", source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlecc1", project="settleproj", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(
            decisions=[{"summary": "settlecc1's ruling that answers the second thread",
                       "resolves": str(thread_id)[:8]}],
            threads_open=[{"summary": "settlecc1's own thread, opened by the dump"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["complete"] is True, out  # never gated by coverage, however partial
    assert out["closure_coverage"] == {"resolved_this_session": 2, "with_closure_edge": 1}


async def test_settle_tool_omits_closure_coverage_when_nothing_resolved_this_session(
    actions: Actions, tmp_path: Path,
) -> None:
    """No signal either way — the field stays absent rather than asserting a false zero."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settlecc3"
    job_dir = str(tmp_path / "jobs" / "settlecc")  # EXACTLY 8 chars — matches session[:8]
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="settleproj",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlecc3", project="settleproj", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "closure_coverage" not in out


async def test_settle_tool_handoff_marker_is_found_by_orient_via_structured_property(
    actions: Actions,
) -> None:
    """THE PAYOFF (ruling c5b184cd): a successor's orient() finds the handoff via the
    TYPED property is_handoff='true' — the summary text below contains neither 'handoff'
    nor 'letter', so the OLD ILIKE path could never have found it. Word-matching identity
    is the disease this closes."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _AncestorCtx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    class _HeirCtx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ancestor = "agent:settlehr1"
    actx = _AncestorCtx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(actx)] = AgentIdentity(
        agent_id=ancestor, session="settlehr1", project="handoffproj", model=None, cwd=None)
    try:
        settled = await srv.settle(
            decisions=[{"summary": "the estate is settled structurally, no grep required",
                       "kind": "choice", "is_handoff": True}],
            ctx=actx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(actx), None)
    assert settled["accepted"]["decisions"][0]["is_handoff"] is True

    hctx = _HeirCtx()
    srv._pool = actions.pool
    srv._agents[srv._conn_key(hctx)] = AgentIdentity(
        agent_id="agent:settlehr2", session="settlehr2", project="handoffproj",
        model=None, cwd=None, succeeded_from=ancestor)
    try:
        out = await srv.orient(ctx=hctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(hctx), None)
    note = out.get("succession_note")
    assert note is not None and note["from"] == ancestor
    assert "settled structurally" in " ".join(n["text"] for n in note["notes"])


async def test_settle_tool_confirms_complete_after_a_full_dump(
    actions: Actions, tmp_path: Path,
) -> None:
    """The full arc: before the dump, boxes are explicitly unmet; after, complete."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settlecf1"
    job_dir = str(tmp_path / "jobs" / "settlecf")  # EXACTLY 8 chars — see below
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="settleproj",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    # ident.session's first 8 chars must match job_dir's own trailing 8 chars — the same
    # LIKE '%/jobs/' || sid[:8] contract mounts.find_session_row runs on everywhere
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlecf1", project="settleproj", model=None,
        cwd=str(tmp_path))
    try:
        before = await srv.settle(ctx=ctx)
        assert before["complete"] is False
        assert "decisions recorded this session" in before["missing_boxes"]
        assert "threads trued this session (opened or resolved)" in before["missing_boxes"]

        (tmp_path / "charter.md").write_text("# notes\n")
        out = await srv.settle(
            decisions=[{"summary": "settlecf1's own ruling, written by the dump"}],
            threads_open=[{"summary": "settlecf1's own thread, written by the dump"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["complete"] is True, out
    assert out["missing_boxes"] == []
    assert out["open_obligations"] == []
    assert out["note"] == "compaction-safe by construction"


async def test_settle_tool_uncommitted_git_work_blocks_complete_and_is_named(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NEW BOX (operator, 2026-07-26, watching a live compaction): even with every
    graph box satisfied and no open obligations, dirty git state in the mounted cwd keeps
    `complete` False and names the file — the one check that was never in the graph."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settlegit1"
    job_dir = str(tmp_path / "jobs" / "settlegi")  # EXACTLY 8 chars — the find_session_row contract
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="settleproj",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)
    _git(tmp_path, "init")
    (tmp_path / "charter.md").write_text("# notes\n")
    (tmp_path / "dirty.txt").write_text("uncommitted\n")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlegit1", project="settleproj", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(
            decisions=[{"summary": "settlegit1's own ruling, written by the dump"}],
            threads_open=[{"summary": "settlegit1's own thread, written by the dump"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["missing_boxes"] == []
    assert out["open_obligations"] == []
    assert out["complete"] is False, out
    assert out["uncommitted_git_files"] is not None
    assert any("dirty.txt" in line for line in out["uncommitted_git_files"])
    assert out["git_checked_path"] == str(tmp_path)  # no repo_path given — falls back to cwd
    assert "uncommitted git file" in out["note"]


async def test_settle_tool_repo_path_overrides_the_office_cwd(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE FIX for Thoth's catch (msg 1381): a seat-office agent's mounted cwd is the
    OFFICE, never the repo it governs — checking cwd alone reads None for the entire
    seat-office fleet and never solves the operator's complaint. `repo_path` names the
    real repo explicitly; it must be checked INSTEAD of cwd, not merely in addition."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    office = tmp_path / "office"  # never a git repo — matches ~/.osiris/seats/<handle>
    office.mkdir()
    repo = tmp_path / "repo"  # the code the agent actually governs
    repo.mkdir()
    _git(repo, "init")
    (repo / "dirty.txt").write_text("uncommitted\n")

    agent = "agent:settlerp1"
    job_dir = str(tmp_path / "jobs" / "settlerp")  # EXACTLY 8 chars
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="settleproj",
                     cwd=str(office), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settlerp1", project="settleproj", model=None,
        cwd=str(office))
    try:
        no_repo_path = await srv.settle(ctx=ctx)
        assert no_repo_path["uncommitted_git_files"] is None  # office cwd — can't evaluate
        assert no_repo_path["git_checked_path"] == str(office)

        out = await srv.settle(repo_path=str(repo), ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["git_checked_path"] == str(repo)
    assert out["uncommitted_git_files"] is not None
    assert any("dirty.txt" in line for line in out["uncommitted_git_files"])
    assert out["complete"] is False, out


async def test_settle_tool_carries_open_obligations_without_blocking_complete(
    actions: Actions,
) -> None:
    """thread f0511eed (found on Thoth's first live dogfood): `complete` used to read False
    whenever ANY open obligation named this agent's lineage as owner — even ancient
    backlog this session never touched, so a manager's project (which always has SOME
    open obligation) could never read complete. An open Thread is already durably
    RECORDED — that is what open_thread's write accomplishes — so it is not "unwritten
    state a compaction could lose" the way a missing box or a dirty git tree is.
    Obligations still surface in the receipt (carried forward, informational) but no
    longer gate `complete` — a pure surface call is safe and read-only either way."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    agent = "agent:settleob1"
    await actions.create_or_find_object("Agent", agent, agent)
    await open_thread(actions, "an obligation settleob1 owes and has not closed",
                      kind="obligation", owner=agent, source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settleob1", project="settleproj", model=None, cwd=None)
    try:
        out = await srv.settle(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["complete"] is True
    assert any("obligation settleob1 owes" in o["summary"] for o in out["open_obligations"])
    assert "carried forward" in out["note"]
    assert out["accepted"] == {"decisions": [], "threads_opened": [], "threads_resolved": []}


async def test_settle_tool_surfaces_identity_coherence_without_blocking_complete(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thoth's Lane 4 finding: settle verified WHAT John XVI wrote, never WHETHER his own
    successor could read it from where orient() looks — his writes landed in a different
    project than the one he was filed under. A mismatch surfaces LOUDLY in the receipt and
    its note but never gates `complete` (ruling 577988ed: a fleet-wide single point of
    failure must never refuse-to-serve on a check that can itself false-positive)."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settleic1"
    job_dir = str(tmp_path / "jobs" / "settleic")  # EXACTLY 8 chars
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="redmonth",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)
    (tmp_path / "charter.md").write_text("# notes\n")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settleic1", project="redmonth", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(
            decisions=[{"summary": "settleic1's ruling, filed under a different project",
                       "repo": "ballgem"}],
            threads_open=[{"summary": "settleic1's thread, written by the dump"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["complete"] is True, out          # never gated by the coherence check
    assert out["identity_coherence"] == {
        "filed_under": "redmonth", "writes_went_to": ["ballgem"], "coherent": False}
    assert "John XVI" in out["note"]


async def test_settle_tool_omits_identity_coherence_when_nothing_written(
    actions: Actions, tmp_path: Path,
) -> None:
    """No writes this session — no signal, so the field stays absent rather than asserting
    a false 'coherent'."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settleic2"
    job_dir = str(tmp_path / "jobs" / "settleic")  # EXACTLY 8 chars — matches session[:8]
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="redmonth",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settleic3", project="redmonth", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "identity_coherence" not in out

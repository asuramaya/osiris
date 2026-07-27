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
    assert "uncommitted git file" in out["note"]


async def test_settle_tool_surfaces_my_own_open_obligations_fleet_wide(
    actions: Actions,
) -> None:
    """A pure surface call (no payload) is safe and read-only — and an obligation this
    agent owns, opened in an EARLIER call, keeps it incomplete until resolved."""
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
    assert out["complete"] is False
    assert any("obligation settleob1 owes" in o["summary"] for o in out["open_obligations"])
    assert out["accepted"] == {"decisions": [], "threads_opened": [], "threads_resolved": []}

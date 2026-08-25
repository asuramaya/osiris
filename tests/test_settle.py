"""The promoted offload-ritual boxes (ruling c5b184cd) — shared by the Stop hook and the
/settle MCP tool, so the two never drift into disagreeing copies."""
from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision, resolve_thread
from src.orchestrator.settle import (
    closure_edge_coverage,
    filed_under_check,
    missing_boxes,
    seat_chartered,
    settle_boxes,
    standing_orders_touched,
    uncommitted_git_work,
    unevaluated_boxes,
)


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def test_standing_orders_touched_absent_file_cannot_be_evaluated(tmp_path: Path) -> None:
    """No charter.md here at all — a repo cwd, not an office — fails open, never punished."""
    assert standing_orders_touched(str(tmp_path), datetime.now(UTC)) is None


def test_standing_orders_touched_checks_mtime_against_session_start(tmp_path: Path) -> None:
    charter = tmp_path / "charter.md"
    charter.write_text("# notes\n")
    now = datetime.now(UTC)
    assert standing_orders_touched(str(tmp_path), now - timedelta(minutes=5)) is True
    assert standing_orders_touched(str(tmp_path), now + timedelta(minutes=5)) is False


def test_standing_orders_touched_none_cwd_cannot_be_evaluated() -> None:
    assert standing_orders_touched(None, datetime.now(UTC)) is None


async def test_seat_chartered_none_seat_id_cannot_be_evaluated(actions: Actions) -> None:
    """RULING 205668ec's declaration half: no seat, no charter to hold — fails open, same
    convention every box in this module follows."""
    assert await seat_chartered(actions.pool, None) is None


async def test_seat_chartered_reads_the_governs_edge_independent_of_the_file(
    actions: Actions,
) -> None:
    """The whole point of the split: this asks the DECLARATION question, never the file
    question — a seat can be chartered with a stale/absent charter.md, or hold a
    freshly-touched charter.md while genuinely ungoverned. Verified against set_charter's
    own governs edge, not a second hand-rolled write."""
    from src.orchestrator.charter import set_charter

    seat_id = "seat:se77le03"
    await actions.create_or_find_object("Seat", seat_id, "session")
    assert await seat_chartered(actions.pool, seat_id) is False
    await actions.create_or_find_object("SoftwareProject", "repo:demo", "session")
    await set_charter(actions, seat_id, ["demo"], actor="session")
    assert await seat_chartered(actions.pool, seat_id) is True


def test_missing_boxes_only_names_explicit_false() -> None:
    """None (fog-of-war, unevaluable) and True (satisfied) never count as missing —
    only an explicit False does."""
    assert missing_boxes({"a": True, "b": False, "c": None, "d": False}) == ["b", "d"]
    assert missing_boxes({"a": True, "b": None}) == []
    assert missing_boxes({}) == []


def test_unevaluated_boxes_only_names_explicit_none(
) -> None:
    """Thoth DM 3076, defect 1(b): None (could not evaluate) is a DIFFERENT state from
    missing (False) and satisfied (True), and must be its own visible list — the exact
    distinction standing_orders_touched's own #128 masking collapsed away."""
    assert unevaluated_boxes({"a": True, "b": False, "c": None, "d": None}) == ["c", "d"]
    assert unevaluated_boxes({"a": True, "b": False}) == []
    assert unevaluated_boxes({}) == []
    # the two functions are proper partitions of the False/None population, never overlapping
    boxes = {"a": True, "b": False, "c": None}
    assert set(missing_boxes(boxes)) & set(unevaluated_boxes(boxes)) == set()


async def test_uncommitted_git_work_none_cwd_cannot_be_evaluated() -> None:
    assert await uncommitted_git_work(None) is None


async def test_uncommitted_git_work_a_non_repo_dir_cannot_be_evaluated(tmp_path: Path) -> None:
    """The common, innocent case: a seat-office cwd, or any ordinary non-repo directory —
    fails open, same as standing_orders_touched on a missing file."""
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
    assert boxes["standing orders touched this session"] is None  # no such file here
    assert boxes["an obligation opened this session (this lineage was minted)"] is False

    await record_decision(actions, "a real ruling this session", source=agent)
    await open_thread(actions, "an obligation left for the heir", kind="obligation",
                      source=agent)
    (office / "charter.md").write_text("# notes\n")

    boxes = await settle_boxes(actions.pool, agent_id=agent, mounted_at=mounted_at,
                               cwd=str(office))
    assert boxes["decisions recorded this session"] is True
    assert boxes["threads trued this session (opened or resolved)"] is True
    assert boxes["standing orders touched this session"] is True
    assert boxes["an obligation opened this session (this lineage was minted)"] is True


async def test_settle_boxes_a_non_minted_agent_carries_no_succession_box(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = "agent:se77le02"
    await actions.create_or_find_object("Agent", agent, agent)
    boxes = await settle_boxes(actions.pool, agent_id=agent, mounted_at=datetime.now(UTC),
                               cwd=str(tmp_path))
    assert "an obligation opened this session (this lineage was minted)" not in boxes


class _RaisingOnMintedCheck:
    """Delegates every query to the real pool EXCEPT the outer `minted_because` check,
    which always raises — reproducing 60bc15db specimen 1: the outer gate's own query
    failing must never be indistinguishable from a confirmed non-mint (60bc15db,
    decision 01e0c69a)."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "minted_because" in query:
            raise RuntimeError("simulated transient failure")
        return await self._pool.fetchval(query, *args)

    async def fetch(self, query: str, *args: Any) -> Any:
        return await self._pool.fetch(query, *args)


async def test_settle_boxes_outer_minted_check_failure_is_unevaluated_not_missing(
    actions: Actions, tmp_path: Path,
) -> None:
    """The bug: the outer `minted` gate used to swallow its own query failure to `False`,
    which SKIPPED the box entirely — a query that could not run was indistinguishable
    from a confirmed non-mint. Both must stay honest: a genuine non-mint still carries no
    key at all (unchanged, see the test above); a failed determination must surface as
    `None` (unevaluated_boxes), never silently drop the box."""
    agent = "agent:se77le03"
    await actions.create_or_find_object("Agent", agent, agent)
    boxes = await settle_boxes(_RaisingOnMintedCheck(actions.pool), agent_id=agent,
                               mounted_at=datetime.now(UTC), cwd=str(tmp_path))
    assert boxes["an obligation opened this session (this lineage was minted)"] is None
    assert ("an obligation opened this session (this lineage was minted)"
           in unevaluated_boxes(boxes))
    assert ("an obligation opened this session (this lineage was minted)"
           not in missing_boxes(boxes))


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


async def test_filed_under_check_normalizes_a_folded_filed_under_label(
    actions: Actions,
) -> None:
    """Decision 6b4d185e / thread aa6b52af: a session filed under a label that has since
    been FOLDED into another must not permanently false-fire 'incoherent' — went_to reads
    off the LIVE (already re-pointed) in_repo edge, so it reports the survivor; the raw
    `project` param must normalize the same way before comparing, or every write this
    lineage ever made under the OLD name reads as a mismatch forever after the fold."""
    from src.orchestrator.projects import fold_project

    agent = "agent:fu06"
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await record_decision(actions, "fu06's ruling, filed under the pre-fold label",
                          repo="RAMstein", source=agent)
    await actions.create_or_find_object("SoftwareProject", "repo:ramstein", "test")
    await fold_project(actions, dupe="RAMstein", into="ramstein",
                       evidence="operator confirmed the same repo", actor="agent:test")

    out = await filed_under_check(actions.pool, agent_id=agent, mounted_at=mounted_at,
                                  project="RAMstein")
    assert out == {"filed_under": "ramstein", "writes_went_to": ["ramstein"],
                   "coherent": True}


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


async def test_settle_threads_open_defaults_an_ownerless_obligation_and_names_it(
    actions: Actions,
) -> None:
    """settle()'s own threads_open is the SECOND live door onto capture.open_thread
    (#5546 items 1+3, Thoth's ruling msg 5605 — "one door, two callers, same shape"): the
    default-never-refuse behavior must fire here too, and the receipt must name it, not
    just settle's own kind='obligation' case."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    await claim_name(actions, "agent:settledefault1", "Settledefault",
                     source="agent:settledefault1")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:settledefault1", session="settledefault1", project="settleproj",
        model=None, cwd=None)
    try:
        out = await srv.settle(
            threads_open=[{"summary": "settle opened this duty with no owner given",
                          "kind": "obligation"}],
            ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    entry = out["accepted"]["threads_opened"][0]
    assert entry["owner_defaulted"]["to"] == "Settledefault"


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


# ═══ THE is_handoff BLEED FIX, READ-RECEIPT VERSION (operator ruling, 2026-08-03) ═════════
# Measured before ANY fix: orient()'s own terse payload carried 6 uncapped is_handoff rows,
# 39.7% of its total bytes (10,395 of 26,153), because nothing ever retired the exemption
# DM 3090 granted. Thoth DM 3355's first version retired on a NEW is_handoff WRITE (same-
# lineage earlier records auto-retired the moment you minted your own). The operator asked
# for something tighter: retirement on an EXPLICIT READ RECEIPT, keyed by id, mirroring
# inbox()'s own lease-vs-settle split — settle()/orient() never retire anything now; only
# ack_handoff(ref=...) does. _retire_stale_handoffs survives ONLY as a manual one-time
# backfill utility (not wired into any live call path) for the population that accumulated
# before this existed.

class _FakeCtx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _settle_as(pool: Any, agent_id: str, **kwargs: Any) -> dict[str, Any]:
    """Mount `agent_id` on a throwaway ctx, call settle(**kwargs), release the ctx. Mirrors
    test_settle_tool_handoff_marker_is_found_by_orient_via_structured_property's own
    pattern, factored out since this block calls it many times."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _FakeCtx()
    saved_pool = srv._pool
    srv._pool = pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project="handoffbleed", model=None, cwd=None)
    try:
        return await srv.settle(ctx=ctx, **kwargs)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def _is_handoff_value(pool: Any, short_id: str) -> str | None:
    """The CURRENT winner only — same resolution every composition in this codebase uses
    (ORDER BY confidence DESC, observed_at DESC LIMIT 1), never a bare unordered fetchval:
    is_handoff can carry competing assertions from TWO sources (the original author's
    'true', a later retirement's 'false') that legitimately coexist in current_assertions
    — only this ordering picks the one _cap_text/compositions actually see."""
    return await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='is_handoff' AND o.id::text LIKE $1 || '%' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", short_id)


async def _status_value(pool: Any, short_id: str) -> str | None:
    """Same current-winner discipline as `_is_handoff_value`, for a Thread's `status`."""
    return await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='status' AND o.id::text LIKE $1 || '%' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", short_id)


async def _succeed(actions: Actions, heir: str, predecessor: str) -> None:
    """Record a REAL succeeded_from property (heir -> predecessor), the same shape
    register_agent's own real minting path writes — needed since ack_handoff's lineage
    check now walks succeeded_from EDGES (60bc15db, decision 61cb1f02) instead of
    comparing id STRINGS, so a test fixture that only shares a naming convention (ackone /
    ackone-ii) with no recorded edge is no longer 'the same lineage' as far as the tool is
    concerned — exactly the gap the fix closes. Mirrors
    test_nearest_handoff_ancestor_walks_past_silence_within_the_bound's own convention."""
    oid = await actions.create_or_find_object("Agent", heir, heir)
    await actions.assert_property(oid, "succeeded_from", predecessor, heir,
                                  datetime.now(UTC), 0.9, evidence_class="direct_observation")


async def _ack_as(pool: Any, agent_id: str, ref: str) -> dict[str, Any]:
    """Mount `agent_id` on a throwaway ctx, call ack_handoff(ref), release the ctx — same
    pattern as `_settle_as`."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _FakeCtx()
    saved_pool = srv._pool
    srv._pool = pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project="handoffbleed", model=None, cwd=None)
    try:
        return await srv.ack_handoff(ref=ref, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_settle_no_longer_auto_retires_on_a_new_handoff_write(actions: Actions) -> None:
    """THE OLD TRIGGER IS GONE: writing a NEW is_handoff record must not touch an older
    same-lineage one anymore — only an explicit ack_handoff call does now."""
    gen1 = await _settle_as(
        actions.pool, "agent:bleedone",
        decisions=[{"summary": "bleedone's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    d1 = gen1["accepted"]["decisions"][0]["id"]
    assert await _is_handoff_value(actions.pool, d1) == "true"
    assert "retired_handoffs" not in gen1["accepted"]["decisions"][0]

    gen2 = await _settle_as(
        actions.pool, "agent:bleedone-ii",
        threads_open=[{"summary": "bleedone-ii's own state of the board",
                       "kind": "obligation", "is_handoff": True}])
    assert "retired_handoffs" not in gen2["accepted"]["threads_opened"][0]
    assert await _is_handoff_value(actions.pool, d1) == "true"  # UNTOUCHED by the new write


async def test_ack_handoff_retires_a_same_lineage_handoff(actions: Actions) -> None:
    gen1 = await _settle_as(
        actions.pool, "agent:ackone",
        decisions=[{"summary": "ackone's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    d1 = gen1["accepted"]["decisions"][0]["id"]
    assert await _is_handoff_value(actions.pool, d1) == "true"
    await _succeed(actions, "agent:ackone-ii", "agent:ackone")

    out = await _ack_as(actions.pool, "agent:ackone-ii", d1)
    # resolved is False: d1 is a Decision, which has no `status` to resolve — a clean no-op
    # on the Thread-only resolve_thread call, not a failure of this fix.
    assert out == {"id": d1, "acknowledged": True, "resolved": False}
    assert await _is_handoff_value(actions.pool, d1) == "false"


async def test_ack_handoff_refuses_a_different_lineage(actions: Actions) -> None:
    a = await _settle_as(
        actions.pool, "agent:ackalpha",
        decisions=[{"summary": "alpha's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    da = a["accepted"]["decisions"][0]["id"]

    out = await _ack_as(actions.pool, "agent:ackbeta", da)
    assert "error" in out and "not your lineage" in out["error"]
    assert await _is_handoff_value(actions.pool, da) == "true"  # untouched, refused


async def test_ack_handoff_succeeds_across_an_id_format_change(actions: Actions) -> None:
    """60bc15db specimen (decision 61cb1f02), live-reproduced against Thoth herself before
    this fix: the OLD lineage check compared `_generation()`'s STRING-parsed root, which
    goes blind the moment a generation's own id-SUFFIX stops looking like a roman numeral
    (a real renumbering shape: agent:ackformat-g40-g40 succeeds agent:ackformat-g40-xxxix,
    six real hops apart in this test's own analogue, sharing NOTHING as strings — "g40" is
    not a roman numeral, so the old check rooted the heir at itself and refused its own
    predecessor's handoff). The new lineage_root check walks the REAL succeeded_from
    chain instead and must succeed here, where the old check would have refused it."""
    gen1 = await _settle_as(
        actions.pool, "agent:ackformat",
        decisions=[{"summary": "ackformat's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    d1 = gen1["accepted"]["decisions"][0]["id"]
    # a chain of ordinary roman-numeral generations, THEN a renumbered id shape at the end —
    # exactly Thoth's own live specimen (agent:ad1a1cb0-g40-g40 succeeding ...-g40-xxxix)
    await _succeed(actions, "agent:ackformat-ii", "agent:ackformat")
    await _succeed(actions, "agent:ackformat-iii", "agent:ackformat-ii")
    await _succeed(actions, "agent:ackformat-g40-g40", "agent:ackformat-iii")

    out = await _ack_as(actions.pool, "agent:ackformat-g40-g40", d1)
    assert out == {"id": d1, "acknowledged": True, "resolved": False}  # d1 is a Decision
    assert await _is_handoff_value(actions.pool, d1) == "false"


async def test_lineage_root_walks_edges_not_id_strings(actions: Actions) -> None:
    """Direct unit coverage of the primitive ack_handoff's fix is built on: two format-
    changed generations of the SAME lineage resolve to the identical root, while two
    genuinely unrelated agents that merely share NOTHING (no succeeded_from at all) each
    root at themselves. Both complete -- short chains, well under any hop bound."""
    from src.orchestrator.agents import lineage_root

    await _succeed(actions, "agent:lrootB", "agent:lrootA")
    await _succeed(actions, "agent:lroot-g40-g40", "agent:lrootB")
    root1, complete1 = await lineage_root(actions.pool, "agent:lroot-g40-g40")
    root2, complete2 = await lineage_root(actions.pool, "agent:lrootA")
    assert root1 == root2 == "agent:lrootA"
    assert complete1 and complete2

    # no edge asserted between these two -- each is its own root, never coincidentally equal
    root3, complete3 = await lineage_root(actions.pool, "agent:lrootstranger1")
    root4, complete4 = await lineage_root(actions.pool, "agent:lrootstranger2")
    assert root3 != root4
    assert complete3 and complete4


async def test_lineage_root_reports_incomplete_when_the_chain_exceeds_max_hops(
    actions: Actions,
) -> None:
    """decision 1cb389be, found live in Thoth's own 76-generation lineage: a chain longer
    than `max_hops` must say so, not silently hand back whatever intermediate ancestor the
    walk happened to reach -- the exact defect this fix removes from the function built to
    fix a sibling instance of the same ruling."""
    from src.orchestrator.agents import lineage_root

    await _succeed(actions, "agent:ltrunc2", "agent:ltrunc1")
    await _succeed(actions, "agent:ltrunc3", "agent:ltrunc2")
    await _succeed(actions, "agent:ltrunc4", "agent:ltrunc3")
    root_bounded, complete_bounded = await lineage_root(
        actions.pool, "agent:ltrunc4", max_hops=2)
    root_full, complete_full = await lineage_root(actions.pool, "agent:ltrunc4", max_hops=10)
    assert complete_full is True
    assert root_full == "agent:ltrunc1"  # the TRUE origin, no predecessor at all
    assert complete_bounded is False
    # the truncated root is a REAL ancestor along the true chain (never fabricated) but
    # it is NOT the origin -- exactly the shape that read as 12 fake "roots" live
    assert root_bounded == "agent:ltrunc2"
    assert root_bounded != root_full


async def test_retire_stale_handoffs_survives_an_id_format_change(actions: Actions) -> None:
    """_retire_stale_handoffs carried the identical string-parse defect ack_handoff's own
    lineage guard did (decision 61cb1f02's sibling check) -- fixed the same way, same
    specimen shape: a format-changed heir's own backfill run must still retire its real
    predecessor's stale handoff, and must still leave an unrelated lineage's alone."""
    import uuid as uuid_mod
    from datetime import UTC, datetime

    from src import mcp_server as srv

    old = await _settle_as(
        actions.pool, "agent:retireformat",
        decisions=[{"summary": "retireformat's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    old_id = old["accepted"]["decisions"][0]["id"]
    assert await _is_handoff_value(actions.pool, old_id) == "true"

    stranger = await _settle_as(
        actions.pool, "agent:retirestranger",
        decisions=[{"summary": "an unrelated lineage's own state of the board",
                   "kind": "choice", "is_handoff": True}])
    stranger_id = stranger["accepted"]["decisions"][0]["id"]

    await _succeed(actions, "agent:retireformat-g40-g40", "agent:retireformat")

    receipt = await srv._retire_stale_handoffs(
        actions.pool, "agent:retireformat-g40-g40", uuid_mod.UUID(int=0), datetime.now(UTC))
    assert old_id in receipt["retired"]
    assert stranger_id not in receipt["retired"]
    assert receipt["skipped_incomplete_walk"] == []
    assert await _is_handoff_value(actions.pool, old_id) == "false"
    assert await _is_handoff_value(actions.pool, stranger_id) == "true"  # untouched


async def test_retire_stale_handoffs_refuses_on_the_actors_own_truncated_walk(
    actions: Actions,
) -> None:
    """decision 1cb389be: the one caller that decides for a WHOLE POPULATION at once must
    REFUSE outright rather than silently under-retire on an unverified root — the failure
    mode that made the real 220+-record backlog disposition unsafe."""
    import uuid as uuid_mod
    from datetime import UTC, datetime

    from src import mcp_server as srv

    cur = "agent:rsdeep0"
    for i in range(1, 5):
        nxt = f"agent:rsdeep{i}"
        await _succeed(actions, nxt, cur)
        cur = nxt

    import pytest

    with pytest.raises(ValueError, match="cannot determine"):
        await srv._retire_stale_handoffs(
            actions.pool, cur, uuid_mod.UUID(int=0), datetime.now(UTC), max_hops=2)


async def test_retire_stale_handoffs_skips_a_candidate_with_a_truncated_walk(
    actions: Actions,
) -> None:
    """A CANDIDATE record's own truncated walk must be left untouched and named, never
    silently treated as same-lineage (would wrongly retire a stranger's handoff) or
    cross-lineage (would silently under-retire, the exact disease this fix removes)."""
    from datetime import UTC, datetime

    from src import mcp_server as srv

    cur = "agent:rsdeepc0"
    for i in range(1, 5):
        nxt = f"agent:rsdeepc{i}"
        await _succeed(actions, nxt, cur)
        cur = nxt
    deep_author = cur  # the DEEPEST generation -- its own walk needs 4 hops to the origin
    deep_handoff = await _settle_as(
        actions.pool, deep_author,
        decisions=[{"summary": "a deep-chain lineage's own state of the board",
                   "kind": "choice", "is_handoff": True}])
    deep_id = deep_handoff["accepted"]["decisions"][0]["id"]

    import uuid as uuid_mod
    receipt = await srv._retire_stale_handoffs(
        actions.pool, "agent:rsshallow", uuid_mod.UUID(int=0), datetime.now(UTC), max_hops=2)
    assert deep_id in receipt["skipped_incomplete_walk"]
    assert deep_id not in receipt["retired"]
    assert await _is_handoff_value(actions.pool, deep_id) == "true"  # untouched


async def test_retire_stale_handoffs_dry_run_names_the_population_without_writing(
    actions: Actions,
) -> None:
    """#150 backlog disposition (Thoth msg 5254): dry_run=True must report the EXACT same
    `retired` population a live call would touch — same query, same lineage_root walk —
    but never call assert_property, so a preview can be trusted byte-for-byte against the
    execute that follows it."""
    import uuid as uuid_mod
    from datetime import UTC, datetime

    from src import mcp_server as srv

    old = await _settle_as(
        actions.pool, "agent:dryformat",
        decisions=[{"summary": "dryformat's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    old_id = old["accepted"]["decisions"][0]["id"]
    await _succeed(actions, "agent:dryformat-g40-g40", "agent:dryformat")

    preview = await srv._retire_stale_handoffs(
        actions.pool, "agent:dryformat-g40-g40", uuid_mod.UUID(int=0), datetime.now(UTC),
        dry_run=True)
    assert old_id in preview["retired"]
    assert preview["dry_run"] is True
    assert await _is_handoff_value(actions.pool, old_id) == "true"  # UNTOUCHED by the preview

    receipt = await srv._retire_stale_handoffs(
        actions.pool, "agent:dryformat-g40-g40", uuid_mod.UUID(int=0), datetime.now(UTC))
    assert receipt["retired"] == preview["retired"]
    assert receipt["dry_run"] is False
    assert await _is_handoff_value(actions.pool, old_id) == "false"  # the live run DOES write


# ═══ _retire_handoff_backlog — the fleet-wide #150 disposition, Thoth msg 5254 ═══
# composed entirely from _retire_stale_handoffs (one lineage-root walk implementation,
# never a second mutation path); groups every live is_handoff='true' record by root and
# keeps the newest per root.

async def test_retire_handoff_backlog_keeps_newest_per_root_and_leaves_others_alone(
    actions: Actions,
) -> None:
    from datetime import UTC, datetime

    from src import mcp_server as srv

    older = await _settle_as(
        actions.pool, "agent:backlogfleet",
        decisions=[{"summary": "backlogfleet's older state of the board", "kind": "choice",
                   "is_handoff": True}])
    older_id = older["accepted"]["decisions"][0]["id"]
    newer = await _settle_as(
        actions.pool, "agent:backlogfleet",
        decisions=[{"summary": "backlogfleet's newer state of the board", "kind": "choice",
                   "is_handoff": True}])
    newer_id = newer["accepted"]["decisions"][0]["id"]

    lone = await _settle_as(
        actions.pool, "agent:backlogsolo",
        decisions=[{"summary": "backlogsolo's only state of the board", "kind": "choice",
                   "is_handoff": True}])
    lone_id = lone["accepted"]["decisions"][0]["id"]

    preview = await srv._retire_handoff_backlog(actions.pool, datetime.now(UTC), dry_run=True)
    assert preview["ok"] is True
    assert preview["dry_run"] is True
    receipt_for_fleet = next(r for r in preview["receipts"] if r["keep"] == newer_id)
    assert older_id in receipt_for_fleet["retired"]
    assert newer_id not in receipt_for_fleet["retired"]
    assert not any(lone_id in r["retired"] for r in preview["receipts"])
    # dry run never writes
    assert await _is_handoff_value(actions.pool, older_id) == "true"
    assert await _is_handoff_value(actions.pool, newer_id) == "true"
    assert await _is_handoff_value(actions.pool, lone_id) == "true"

    executed = await srv._retire_handoff_backlog(actions.pool, datetime.now(UTC), dry_run=False)
    assert executed["ok"] is True
    assert await _is_handoff_value(actions.pool, older_id) == "false"
    assert await _is_handoff_value(actions.pool, newer_id) == "true"  # kept
    assert await _is_handoff_value(actions.pool, lone_id) == "true"  # untouched, only one


async def test_retire_handoff_backlog_refuses_whole_run_on_any_incomplete_walk(
    actions: Actions,
) -> None:
    from datetime import UTC, datetime

    from src import mcp_server as srv

    cur = "agent:backlogdeep0"
    for i in range(1, 5):
        nxt = f"agent:backlogdeep{i}"
        await _succeed(actions, nxt, cur)
        cur = nxt
    deep_handoff = await _settle_as(
        actions.pool, cur,
        decisions=[{"summary": "a deep-chain lineage's own state of the board",
                   "kind": "choice", "is_handoff": True}])
    deep_id = deep_handoff["accepted"]["decisions"][0]["id"]

    receipt = await srv._retire_handoff_backlog(
        actions.pool, datetime.now(UTC), dry_run=True, max_hops=2)
    assert receipt["ok"] is False
    assert cur in receipt["incomplete_authors"]
    assert await _is_handoff_value(actions.pool, deep_id) == "true"  # untouched, whole run refused


# ═══ _resolve_acked_handoff_threads — the retroactive backfill, msg 4673/decision 4bf6d835 ═══
# ack_handoff's own status-resolution fix (this same dispatch) only covers FUTURE acks; this
# is the one-time cleanup for the population that accumulated before it existed. Same shape
# and reasoning as _retire_stale_handoffs right above it: manual, never a live trigger.

async def test_resolve_acked_handoff_threads_resolves_an_already_acked_open_thread(
    actions: Actions,
) -> None:
    """THE EXACT SPECIMEN THIS FIX EXISTS FOR: is_handoff already 'false' (a real ack
    already happened, simulated here by writing it directly — as it would have, before
    ack_handoff itself resolved the status), status still 'open'. THE DISCRIMINATOR IS THE
    ACK, never time — this thread's own creation timestamp is irrelevant to the query."""
    from src import mcp_server as srv

    tid = await open_thread(actions, "already-acked, still open — the backfill's own target",
                            kind="obligation", source="agent:backfillone")
    await actions.assert_property(tid, "is_handoff", "true", "agent:backfillone",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    # the ack, pre-fix shape: is_handoff flips, status never does (simulating history)
    await actions.assert_property(tid, "is_handoff", "false", "agent:backfillone-ii",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    short = str(tid)[:8]
    assert await _status_value(actions.pool, short) == "open"

    resolved = await srv._resolve_acked_handoff_threads(
        actions.pool, "agent:backfiller", datetime.now(UTC))
    assert short in resolved
    assert await _status_value(actions.pool, short) == "resolved"


async def test_resolve_acked_handoff_threads_leaves_an_unacked_handoff_open(
    actions: Actions,
) -> None:
    """AN UNACKED HANDOFF IS NOT STALE, IT IS UNREAD — the whole binding constraint (msg
    4673: "the discriminator is the ack, never time"). This must never be swept in."""
    from src import mcp_server as srv

    tid = await open_thread(actions, "unacked — must stay open no matter how old",
                            kind="obligation", source="agent:backfillunread")
    await actions.assert_property(tid, "is_handoff", "true", "agent:backfillunread",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    short = str(tid)[:8]

    resolved = await srv._resolve_acked_handoff_threads(
        actions.pool, "agent:backfiller", datetime.now(UTC))
    assert short not in resolved
    assert await _status_value(actions.pool, short) == "open"


async def test_resolve_acked_handoff_threads_ignores_an_already_resolved_one(
    actions: Actions,
) -> None:
    """Idempotent: a thread already resolved (by an earlier backfill run, or the live
    ack_handoff fix) is not re-touched or double-counted."""
    from src import mcp_server as srv

    tid = await open_thread(actions, "already resolved before this run",
                            kind="obligation", source="agent:backfilldone")
    await actions.assert_property(tid, "is_handoff", "false", "agent:backfilldone",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await resolve_thread(actions, str(tid), because="already closed")
    short = str(tid)[:8]

    resolved = await srv._resolve_acked_handoff_threads(
        actions.pool, "agent:backfiller", datetime.now(UTC))
    assert short not in resolved  # the query's own status='open' filter excludes it


async def test_resolve_acked_handoff_threads_can_scope_to_one_repo(actions: Actions) -> None:
    """`repo` scopes the backfill to one project's in_repo-linked Threads — the safe,
    house-boundary-respecting shape (Sekhmet's own already-vetted osiris population, msg
    4673), never a blind fleet-wide sweep unless explicitly asked for."""
    from src import mcp_server as srv

    in_scope = await open_thread(actions, "acked, in the scoped repo",
                                 repo="backfillrepo", kind="obligation",
                                 source="agent:backfillscoped")
    out_of_scope = await open_thread(actions, "acked, a DIFFERENT repo entirely",
                                     repo="backfillother", kind="obligation",
                                     source="agent:backfillscoped")
    for tid in (in_scope, out_of_scope):
        await actions.assert_property(tid, "is_handoff", "false", "agent:backfillscoped",
                                      datetime.now(UTC), 0.9, evidence_class="self_declared")
    in_short, out_short = str(in_scope)[:8], str(out_of_scope)[:8]

    resolved = await srv._resolve_acked_handoff_threads(
        actions.pool, "agent:backfiller", datetime.now(UTC), repo="backfillrepo")
    assert in_short in resolved
    assert out_short not in resolved
    assert await _status_value(actions.pool, out_short) == "open"  # untouched, different repo


# ═══ misfiled_by_lineage — #145's discovery half (decision b89477a0/61cb1f02) ═══
# identity_coherence (filed_under_check, above) only ever checks THIS session's own
# writes forward from its own mounted_at; this walks the whole lineage, all of history.


async def test_misfiled_by_lineage_none_when_project_unknown(actions: Actions) -> None:
    from src.orchestrator.agents import misfiled_by_lineage

    assert await misfiled_by_lineage(actions.pool, "agent:mbl00", None) is None


async def test_misfiled_by_lineage_earned_silence_when_clean_and_chain_complete(
    actions: Actions,
) -> None:
    """Nothing misfiled AND the chain terminated within max_hops -- a real, complete
    answer, allowed to render as silence (None), same discipline as filed_under_check's
    own None-means-nothing-to-report convention."""
    from src.orchestrator.agents import misfiled_by_lineage

    await record_decision(actions, "mbl01's own ruling, correctly filed",
                          repo="mblproj", source="agent:mbl01")
    out = await misfiled_by_lineage(actions.pool, "agent:mbl01", "mblproj")
    assert out is None


async def test_misfiled_by_lineage_finds_an_ancestors_misfiled_write(actions: Actions) -> None:
    """The real #145 shape: a PREDECESSOR's write landed under a different project than
    the CURRENT, correctly-filed generation's own. filed_under_check (scoped to one
    session) could never see this; this must."""
    from src.orchestrator.agents import misfiled_by_lineage

    await record_decision(actions, "mbl02's ruling, written while mounted elsewhere",
                          repo="wrongproj", source="agent:mbl02")
    await _succeed(actions, "agent:mbl02-ii", "agent:mbl02")

    out = await misfiled_by_lineage(actions.pool, "agent:mbl02-ii", "mblproj")
    assert out is not None
    assert out["filed_under"] == "mblproj"
    assert out["misfiled_count"] == 1
    assert out["misfiled"][0]["filed_under"] == "wrongproj"
    assert out["chain_hops_walked"] == 1
    assert out["chain_may_be_incomplete"] is False


async def test_misfiled_by_lineage_survives_an_id_format_change(actions: Actions) -> None:
    """The exact live specimen shape this whole night has been about: the CURRENT
    generation's id format changed (…-g40-g40 style) but the succeeded_from chain is
    real, so the ancestor's misfiled write is still found."""
    from src.orchestrator.agents import misfiled_by_lineage

    await record_decision(actions, "mbl03's ruling, filed under the wrong project",
                          repo="wrongproj", source="agent:mbl03")
    await _succeed(actions, "agent:mbl03-g40-g40", "agent:mbl03")

    out = await misfiled_by_lineage(actions.pool, "agent:mbl03-g40-g40", "mblproj")
    assert out is not None
    assert out["misfiled_count"] == 1


async def test_misfiled_by_lineage_never_hides_an_incomplete_chain_behind_a_clean_answer(
    actions: Actions,
) -> None:
    """Thoth's own explicit constraint (DM 4114): a short/empty misfiled list must never
    render identically to a fully-verified clean one. Force max_hops below the real chain
    length -- nothing misfiled among what WAS reached, but the walk did not terminate, so
    the caveat must survive rather than collapsing to None."""
    from src.orchestrator.agents import misfiled_by_lineage

    await _succeed(actions, "agent:mbl04-ii", "agent:mbl04")
    await _succeed(actions, "agent:mbl04-iii", "agent:mbl04-ii")

    out = await misfiled_by_lineage(actions.pool, "agent:mbl04-iii", "mblproj", max_hops=1)
    assert out is not None
    assert out["misfiled_count"] == 0
    assert out["chain_hops_walked"] == 1
    assert out["chain_may_be_incomplete"] is True


async def test_misfiled_by_lineage_normalizes_a_folded_project_and_ignores_healed_edges(
    actions: Actions,
) -> None:
    """Decision 6b4d185e's fifth specimen: an ancestor's write, correctly filed under a
    label that has since been FOLDED into another, must not report as 'misfiled' just
    because the caller's own `project` still names the pre-fold label — and the fold's
    own invalidated pre-fold edge must not double-count alongside its live replacement
    (the same compounding gap fixed in filed_under_check, same pass)."""
    from src.orchestrator.agents import misfiled_by_lineage
    from src.orchestrator.projects import fold_project

    await record_decision(actions, "mbl05's ruling, correctly filed under the pre-fold label",
                          repo="RAMstein", source="agent:mbl05")
    await actions.create_or_find_object("SoftwareProject", "repo:ramstein", "test")
    await fold_project(actions, dupe="RAMstein", into="ramstein",
                       evidence="operator confirmed the same repo", actor="agent:test")
    await _succeed(actions, "agent:mbl05-ii", "agent:mbl05")

    out = await misfiled_by_lineage(actions.pool, "agent:mbl05-ii", "RAMstein")
    assert out is None  # earned silence: the write is correctly filed once normalized


async def test_orient_surfaces_misfiled_elsewhere_for_a_correctly_filed_successor(
    actions: Actions,
) -> None:
    """The real #145 acceptance case, through orient() itself: a correctly-filed
    successor's own orient() call must surface an ANCESTOR's misfiled write -- something
    identity_coherence (settle-time, this-session-only) could never do."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.compositions import seed_default_compositions

    await seed_default_compositions(actions.pool)
    await record_decision(actions, "mblorient's ruling, filed under the wrong project",
                          repo="wrongproj", source="agent:mblorient")
    await _succeed(actions, "agent:mblorient-ii", "agent:mblorient")

    ctx = _FakeCtx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:mblorient-ii", session="mblorient-ii", project="mblorientproj",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "misfiled_elsewhere" in out
    assert out["misfiled_elsewhere"]["misfiled_count"] == 1
    assert out["misfiled_elsewhere"]["misfiled"][0]["filed_under"] == "wrongproj"


async def test_ack_handoff_refuses_a_second_ack(actions: Actions) -> None:
    gen1 = await _settle_as(
        actions.pool, "agent:ackdup",
        decisions=[{"summary": "ackdup's own state of the board", "kind": "choice",
                   "is_handoff": True}])
    d1 = gen1["accepted"]["decisions"][0]["id"]
    await _succeed(actions, "agent:ackdup-ii", "agent:ackdup")
    first = await _ack_as(actions.pool, "agent:ackdup-ii", d1)
    assert first["acknowledged"] is True

    second = await _ack_as(actions.pool, "agent:ackdup-ii", d1)
    assert "error" in second and "already acknowledged" in second["error"]


async def test_ack_handoff_refuses_an_unresolvable_ref(actions: Actions) -> None:
    out = await _ack_as(actions.pool, "agent:acknada", "00000000")
    assert "error" in out and "no handoff matches" in out["error"]


async def test_ack_handoff_works_across_decision_and_thread_types(actions: Actions) -> None:
    """The real specimen this session measured: Sekhmet V's THREAD-shaped handoff (6c4d6669)
    and Sekhmet VIII's DECISION-shaped one (3fb1a5fc) both needed the same door — ack_handoff
    must not care which shape it's naming."""
    gen1 = await _settle_as(
        actions.pool, "agent:ackcross",
        threads_open=[{"summary": "ackcross's own thread-shaped handoff",
                       "kind": "obligation", "is_handoff": True}])
    t1 = gen1["accepted"]["threads_opened"][0]["id"]
    assert await _status_value(actions.pool, t1) == "open"  # baseline, before the ack
    await _succeed(actions, "agent:ackcross-ii", "agent:ackcross")
    out = await _ack_as(actions.pool, "agent:ackcross-ii", t1)
    assert out["acknowledged"] is True
    assert out["resolved"] is True  # the Thread half of this fix (msg 4673)
    assert await _is_handoff_value(actions.pool, t1) == "false"
    assert await _status_value(actions.pool, t1) == "resolved"

    gen2 = await _settle_as(
        actions.pool, "agent:ackcross-ii",
        decisions=[{"summary": "ackcross-ii's own decision-shaped handoff", "kind": "choice",
                   "is_handoff": True}])
    d2 = gen2["accepted"]["decisions"][0]["id"]
    await _succeed(actions, "agent:ackcross-iii", "agent:ackcross-ii")
    out2 = await _ack_as(actions.pool, "agent:ackcross-iii", d2)
    assert out2["acknowledged"] is True
    assert out2["resolved"] is False  # a Decision has no status to resolve — clean no-op
    assert await _is_handoff_value(actions.pool, d2) == "false"


async def test_ack_handoff_leaves_the_record_fully_readable(actions: Actions) -> None:
    """Ack touches ONE property, never the record itself — recall() must still return the
    whole thing, same discipline as amend_decision/amend_practice."""
    from src.orchestrator.recall import recall

    gen1 = await _settle_as(
        actions.pool, "agent:ackread",
        decisions=[{"summary": "ackread's own state of the board, still fully readable",
                   "kind": "choice", "is_handoff": True}])
    d1 = gen1["accepted"]["decisions"][0]["id"]
    await _succeed(actions, "agent:ackread-ii", "agent:ackread")
    await _ack_as(actions.pool, "agent:ackread-ii", d1)

    rec = await recall(actions.pool, d1)
    assert rec["summary"] == "ackread's own state of the board, still fully readable"
    assert rec["is_handoff"] == "false"


async def test_settle_handoff_survives_whole_across_repeated_unacked_orients(
    actions: Actions,
) -> None:
    """THE NON-NEGOTIABLE ACCEPTANCE TEST (Thoth DM 3355, verbatim, unchanged by the
    read-receipt redesign): 'a fresh seat's orient() must still receive its OWN IMMEDIATE
    PREDECESSOR'S handoff WHOLE.' Under the receipt model this holds by CONSTRUCTION, not
    careful ordering — orient() never writes anything for a handoff, so it is delivered
    whole on the first call AND every repeated call, for as long as it stays unacked
    (mail's own redelivery-until-settled shape). Only ack_handoff retires it, and only a
    call AFTER that stops the delivery."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.compositions import seed_default_compositions

    await seed_default_compositions(actions.pool)
    long_handoff = ("thirteen wrong premises, confessed at length so my heir does not "
                    "repeat them, on and on past the old 160-char cap ") * 6
    gen1 = await _settle_as(
        actions.pool, "agent:bleedheir1",
        decisions=[{"summary": long_handoff, "kind": "choice", "is_handoff": True,
                   "repo": "handoffbleed"}])
    d1 = gen1["accepted"]["decisions"][0]["id"]

    async def _orient_as_heir() -> dict[str, Any]:
        ctx = _FakeCtx()
        saved_pool = srv._pool
        srv._pool = actions.pool
        srv._agents[srv._conn_key(ctx)] = AgentIdentity(
            agent_id="agent:bleedheir1-ii", session="bleedheir1-ii", project="handoffbleed",
            model=None, cwd=None, succeeded_from="agent:bleedheir1")
        try:
            return await srv.orient(ctx=ctx)
        finally:
            srv._pool = saved_pool
            srv._agents.pop(srv._conn_key(ctx), None)

    first = await _orient_as_heir()
    row = next(r for r in first["recent_decisions"] if r["summary"].startswith("thirteen"))
    assert row["summary"] == long_handoff  # whole — no "…" marker

    # a SECOND, still-unacked orient() call delivers it whole again — redelivery, not a
    # one-shot lease that quietly consumes itself on the first read.
    second = await _orient_as_heir()
    row2 = next(r for r in second["recent_decisions"] if r["summary"].startswith("thirteen"))
    assert row2["summary"] == long_handoff

    await _succeed(actions, "agent:bleedheir1-ii", "agent:bleedheir1")
    ack = await _ack_as(actions.pool, "agent:bleedheir1-ii", d1)
    assert ack["acknowledged"] is True

    after = await _orient_as_heir()
    capped = next(r for r in after["recent_decisions"] if r["summary"].startswith("thirteen"))
    assert capped["summary"] != long_handoff  # now subject to the ordinary 160-char cap
    assert capped["summary"].endswith("…")


async def test_settle_tool_resolves_the_seat_office_over_a_corrected_mount_cwd(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    """DEFECT 1 (Thoth DM 3076), THE LIVE SPECIMEN REPRODUCED: a seated agent's mount cwd
    reads as the bare office CONTAINER (a #128-class correction, not this agent's real
    office at <container>/<handle>) — before the fix, standing_orders_touched checked the wrong
    directory, found nothing, returned None, and `missing_boxes` silently dropped it: a
    real, 11-day-stale charter.md sat unevaluated forever. The SEAT BINDING (bind_holder's
    own `holds` link + the seat's `handle` property) must be resolved instead of trusting
    the corrupted cwd — proving `held_seat`'s own lineage-aware resolution is reused, not
    a naive re-derivation that could reintroduce the ancestor-generation gap it exists to
    close."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount
    from src.orchestrator.seats import bind_holder

    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", tmp_path / "seats")
    container = tmp_path / "seats"
    container.mkdir()
    real_office = container / "thoth"
    real_office.mkdir()
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    (real_office / "charter.md").write_text("# eleven days old, untouched this session\n")
    old_time = (mounted_at - timedelta(days=11)).timestamp()
    os.utime(real_office / "charter.md", (old_time, old_time))

    agent = "agent:settleseat1"
    seat_id = "seat:settleseat1"
    seat_oid = await actions.create_or_find_object("Seat", seat_id, agent)
    await actions.assert_property(seat_oid, "handle", "Thoth", agent, mounted_at, 0.9,
                                  evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat_id, agent_id=agent)

    job_dir = str(tmp_path / "jobs" / "settlese")  # EXACTLY 8 chars
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="osiris",
                     cwd=str(container), model=None, session_key=None)  # the CORRUPTED cwd
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
        agent_id=agent, session="settleseat1", project="osiris", model=None,
        cwd=str(container))  # ident.cwd is the bare container, exactly Thoth's own specimen
    try:
        out = await srv.settle(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["boxes"]["standing orders touched this session"] is False, out
    assert "standing orders touched this session" in out["missing_boxes"]
    assert out["complete"] is False, out


async def test_settle_tool_charter_box_still_none_for_an_unseated_session(
    actions: Actions, tmp_path: Path,
) -> None:
    """The refutation, backed by evidence, of Thoth's own instinct that None should always
    block `complete` (DM 3076 defect 1b): an UNSEATED session (no `holds` link at all — an
    ordinary code-repo session, not a seat office) has no seat binding to resolve and no
    charter.md was ever scaffolded for it. Falls back to the cwd it was given; still
    legitimately unevaluable, still non-blocking — ruling 577988ed's own reasoning (a
    check that can false-positive must never refuse-to-serve) applies here exactly as it
    already does for identity_coherence/closure_coverage. Now VISIBLE though, in
    `unevaluated_boxes` and `note` — the part of the defect that WAS a real gap."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    agent = "agent:settleunseated1"
    job_dir = str(tmp_path / "jobs" / "settleun")  # EXACTLY 8 chars
    mounted_at = datetime.now(UTC) - timedelta(minutes=5)
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="someproj",
                     cwd=str(tmp_path), model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET mounted_at=$1 WHERE job_dir=$2", mounted_at, job_dir)
    await record_decision(actions, "settleunseated1's own ruling this session", source=agent)
    await open_thread(actions, "settleunseated1's own thread this session", source=agent)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session="settleunseated1", project="someproj", model=None,
        cwd=str(tmp_path))
    try:
        out = await srv.settle(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["boxes"]["standing orders touched this session"] is None
    assert "standing orders touched this session" not in out["missing_boxes"]
    assert "standing orders touched this session" in out["unevaluated_boxes"]
    assert out["complete"] is True, out  # still non-blocking — refuted, not assumed
    assert "could not evaluate" in out["note"]
    assert "standing orders touched this session" in out["note"]


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
    # this session is UNSEATED (no Seat/held_seat binding) — "seat is chartered" has no
    # seat to ask about and reads None (fog-of-war), same honest-not-invisible law
    # unevaluated_boxes already enforces above; it never gates `complete`.
    assert out["unevaluated_boxes"] == ["seat is chartered (governs a repo)"]
    assert out["note"] == (
        "compaction-safe by construction — could not evaluate: "
        "seat is chartered (governs a repo) (fog-of-war, not a pass, never gates complete)")


async def test_settle_tool_uncommitted_git_work_is_surfaced_but_never_blocks_complete(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NEW BOX (operator, 2026-07-26, watching a live compaction): dirty git state in
    the mounted cwd is named in the receipt and the note. DEFECT 2 (Thoth DM 3076): it must
    NOT gate `complete` — a shared tree's `git status` has no notion of whose hand staged
    what, so a manager's own settle used to flip false/true purely off a WORKER's commit
    timing, deciding this agent's compaction-safety by another agent's action. `complete`
    now answers only "is THIS session's own graph knowledge deposited", the same report-
    only discipline identity_coherence/closure_coverage already use."""
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
    assert out["complete"] is True, out  # never gated by uncommitted git state
    assert out["uncommitted_git_files"] is not None
    assert any("dirty.txt" in line for line in out["uncommitted_git_files"])
    assert out["git_checked_path"] == str(tmp_path)  # no repo_path given — falls back to cwd
    assert "uncommitted git file" in out["note"]
    assert "informational" in out["note"]


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
    # complete stays False here regardless — this session recorded no decisions/threads at
    # all, an unrelated reason (defect 2 only changed whether uncommitted git state ITSELF
    # can gate complete; see test_settle_tool_uncommitted_git_work_is_surfaced_but_never_
    # blocks_complete for that specific proof, isolated from these other boxes). Still
    # surfaced informationally in the note either way (defect 2's own "stays SURFACED").
    assert out["complete"] is False, out
    assert "uncommitted git file" in out["note"]


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

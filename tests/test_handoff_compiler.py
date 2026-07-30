"""THE HANDOFF COMPILER (Thoth's dispatch, DM 2338) — compile_handoff/render_handoff_
briefing (src/orchestrator/handoff_compiler.py) plus the handoff_briefing MCP tool."""
from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision
from src.orchestrator.deploy_guard import _REPO_ROOT
from src.orchestrator.handoff_compiler import (
    _deploy_verdict,
    compile_handoff,
    render_handoff_briefing,
    since_last_handoff,
)
from src.orchestrator.monitor import set_cursor


def _rev(rev: str) -> str:
    """A REAL sha from the repo this test suite itself runs in — same reuse of the running
    checkout `test_deploy_guard.py`'s own unreviewed-boot tests already lean on
    (`guard._REPO_ROOT`), never a fabricated throwaway repo."""
    return subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "rev-parse", rev],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


# ═══════════ _deploy_verdict — pure git ancestry, no DB ═══════════

async def test_deploy_verdict_unknown_with_no_repo_root() -> None:
    deployed, note = await _deploy_verdict(None, "abc1234", "def5678")
    assert deployed is None and "on_disk_path" in note


async def test_deploy_verdict_unknown_with_no_cursor() -> None:
    deployed, note = await _deploy_verdict(str(_REPO_ROOT), "abc1234", None)
    assert deployed is None and "cursor" in note


async def test_deploy_verdict_true_for_a_real_ancestor() -> None:
    parent, head = _rev("HEAD~1"), _rev("HEAD")
    deployed, note = await _deploy_verdict(str(_REPO_ROOT), parent[:12], head)
    assert deployed is True and "deployed" in note


async def test_deploy_verdict_false_for_a_real_non_ancestor() -> None:
    parent, head = _rev("HEAD~1"), _rev("HEAD")
    deployed, note = await _deploy_verdict(str(_REPO_ROOT), head[:12], parent)
    assert deployed is False and "not yet deployed" in note


async def test_deploy_verdict_unknown_on_an_unresolvable_sha() -> None:
    deployed, note = await _deploy_verdict(str(_REPO_ROOT), "0" * 12, "1" * 40)
    assert deployed is None


# ═══════════ since_last_handoff ═══════════

async def test_since_last_handoff_none_when_nothing_found(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:hc-slh01", "agent:hc-slh01")
    since, note = await since_last_handoff(actions.pool, "agent:hc-slh01")
    assert since is None
    assert "no prior handoff" in note


async def test_since_last_handoff_finds_its_own_marked_decision(actions: Actions) -> None:
    did = await record_decision(
        actions, "hc-slh02's own state of the board", kind="ruling",
        source="agent:hc-slh02", repo="hcslhproj")
    await actions.assert_property(did, "is_handoff", "true", "agent:hc-slh02",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    since, note = await since_last_handoff(actions.pool, "agent:hc-slh02")
    assert since is not None
    assert "agent:hc-slh02" in note

    marker_ts = await actions.pool.fetchval(
        "SELECT observed_at FROM assertions WHERE object_id=$1 AND name='summary' "
        "AND evidence_class='self_declared'", did)
    assert since > marker_ts  # EXCLUSIVE: the handoff is the closing act of the OLD reign,
    # never the opening act of the new one it hands off to


# ═══════════ compile_handoff ═══════════

async def test_compile_handoff_empty_for_an_unknown_repo(actions: Actions) -> None:
    assert await compile_handoff(actions.pool, repo="no-such-project-xyz") == {}


async def test_compile_handoff_shipped_respects_the_since_boundary(actions: Actions) -> None:
    old_id = await record_decision(actions, "Migrate the legacy queue consumer off cron",
                                   repo="hcshipproj", source="agent:hc-ship")
    old_ts = await actions.pool.fetchval(
        "SELECT observed_at FROM assertions WHERE object_id=$1 AND name='summary' "
        "AND evidence_class='self_declared'", old_id)
    since = old_ts + timedelta(microseconds=1)
    new_id = await record_decision(actions, "Retire the standalone smoke-test harness",
                                   repo="hcshipproj", source="agent:hc-ship")

    data = await compile_handoff(actions.pool, repo="hcshipproj", since=since)
    ids = {d["id"] for d in data["shipped"]}
    assert str(new_id)[:8] in ids
    assert str(old_id)[:8] not in ids

    data_full = await compile_handoff(actions.pool, repo="hcshipproj", since=None)
    ids_full = {d["id"] for d in data_full["shipped"]}
    assert {str(old_id)[:8], str(new_id)[:8]} <= ids_full


async def test_compile_handoff_shipped_carries_no_commit_when_none_cited(
    actions: Actions,
) -> None:
    did = await record_decision(actions, "hc-nocommit: a ruling with no commit",
                                repo="hcnocommitproj", source="agent:hc-nc")
    data = await compile_handoff(actions.pool, repo="hcnocommitproj")
    entry = next(d for d in data["shipped"] if d["id"] == str(did)[:8])
    assert entry["commits"] == []


async def test_compile_handoff_deploy_status_unknown_with_no_on_disk_path(
    actions: Actions,
) -> None:
    """A decided_in commit exists, but this project never registered on_disk_path — deploy
    status must read unknown, never guessed."""
    await actions.create_or_find_object("Commit", "commit:aaaabbbbcccc", "git")
    did = await record_decision(
        actions, "hc-unk: landed with no on_disk_path on file", repo="hcunkproj",
        rationale="Landed, commit aaaabbb.", source="agent:hc-unk")
    data = await compile_handoff(actions.pool, repo="hcunkproj")
    entry = next(d for d in data["shipped"] if d["id"] == str(did)[:8])
    assert entry["commits"][0]["deployed"] is None
    assert "on_disk_path" in entry["commits"][0]["note"]


async def test_compile_handoff_marks_a_real_ancestor_commit_deployed(actions: Actions) -> None:
    parent, head = _rev("HEAD~1"), _rev("HEAD")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:hcdeployedproj",
                                               "test")
    await actions.assert_property(proj, "on_disk_path", str(_REPO_ROOT), "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await set_cursor(actions.pool, "deployed:hcdeployedproj", head)
    await actions.create_or_find_object("Commit", f"commit:{parent[:12]}", "git")
    did = await record_decision(
        actions, "hc-deployed: a real landed-and-deployed commit", repo="hcdeployedproj",
        rationale=f"Landed, commit {parent[:7]}.", source="agent:hc-dep")

    data = await compile_handoff(actions.pool, repo="hcdeployedproj")
    entry = next(d for d in data["shipped"] if d["id"] == str(did)[:8])
    assert entry["commits"][0]["deployed"] is True
    assert data["deploy_cursor"]["sha"] == head


async def test_compile_handoff_marks_a_real_non_ancestor_commit_not_deployed(
    actions: Actions,
) -> None:
    parent, head = _rev("HEAD~1"), _rev("HEAD")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:hcnotdeployedproj",
                                               "test")
    await actions.assert_property(proj, "on_disk_path", str(_REPO_ROOT), "test",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await set_cursor(actions.pool, "deployed:hcnotdeployedproj", parent)
    await actions.create_or_find_object("Commit", f"commit:{head[:12]}", "git")
    did = await record_decision(
        actions, "hc-notdeployed: landed but ahead of the cursor", repo="hcnotdeployedproj",
        rationale=f"Landed, commit {head[:7]}.", source="agent:hc-notdep")

    data = await compile_handoff(actions.pool, repo="hcnotdeployedproj")
    entry = next(d for d in data["shipped"] if d["id"] == str(did)[:8])
    assert entry["commits"][0]["deployed"] is False


async def test_compile_handoff_corrections_shows_both_sides(actions: Actions) -> None:
    old_id = await record_decision(actions, "hc-corr: the original, wrong call",
                                   repo="hccorrproj", source="agent:hc-corr")
    new_id = await record_decision(
        actions, "hc-corr: the corrected call", repo="hccorrproj",
        supersedes=str(old_id), source="agent:hc-corr")

    data = await compile_handoff(actions.pool, repo="hccorrproj")
    corr = next(c for c in data["corrections"] if c["new_id"] == str(new_id)[:8])
    assert corr["old_id"] == str(old_id)[:8]
    assert corr["old_summary"] == "hc-corr: the original, wrong call"
    assert corr["new_summary"] == "hc-corr: the corrected call"


async def test_compile_handoff_open_carries_owner_and_operator_gated_is_named(
    actions: Actions,
) -> None:
    op_id = await open_thread(actions, "Escalate the cert rotation, blocked on the operator",
                              repo="hcopenproj", owner="operator", source="agent:hc-open")
    mine_id = await open_thread(actions, "Sweep dead feature flags out of the config loader",
                                repo="hcopenproj", source="agent:hc-open")

    data = await compile_handoff(actions.pool, repo="hcopenproj")
    open_ids = {t["id"] for t in data["open"]}
    assert {str(op_id)[:8], str(mine_id)[:8]} <= open_ids
    gated_ids = {t["id"] for t in data["operator_gated"]}
    assert str(op_id)[:8] in gated_ids
    assert str(mine_id)[:8] not in gated_ids


async def test_compile_handoff_flags_a_self_declared_unverified_claim(
    actions: Actions,
) -> None:
    did = await record_decision(
        actions, "UNCONFIRMED: the front-door overflow bug did not reproduce under load",
        repo="hcunvproj", source="agent:hc-unv")
    clean_id = await record_decision(
        actions, "Rename the legacy case_objects index for clarity", repo="hcunvproj",
        source="agent:hc-unv")

    data = await compile_handoff(actions.pool, repo="hcunvproj")
    flagged_ids = {u["id"] for u in data["unverified_heuristic"]}
    assert str(did)[:8] in flagged_ids
    assert str(clean_id)[:8] not in flagged_ids
    hit = next(u for u in data["unverified_heuristic"] if u["id"] == str(did)[:8])
    assert hit["marker"] == "UNCONFIRMED"


# ═══════════ render_handoff_briefing ═══════════

def test_render_handoff_briefing_handles_the_no_project_case() -> None:
    assert "no such project" in render_handoff_briefing({})


def test_render_handoff_briefing_carries_every_section_and_a_judgment_placeholder() -> None:
    data = {
        "repo": "x", "since": None, "shipped": [], "open": [], "open_echo_count": 0,
        "operator_gated": [], "corrections": [], "unverified_heuristic": [],
    }
    text = render_handoff_briefing(data)
    for heading in ("## Shipped", "## Corrections", "## Open", "## Operator-gated",
                    "## Unverified", "## Judgment"):
        assert heading in text


# ═══════════ the handoff_briefing MCP tool ═══════════

async def test_handoff_briefing_tool_refuses_when_unmounted_and_no_agent_ref(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.handoff_briefing(repo="hctoolproj")
    finally:
        srv._pool = saved_pool
    assert "error" in out


async def test_handoff_briefing_tool_defaults_to_the_caller_own_lineage(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    did = await record_decision(actions, "State of the board — end of hc-tool01's reign",
                                source="agent:hc-tool01", repo="hctoolproj")
    await actions.assert_property(did, "is_handoff", "true", "agent:hc-tool01",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    new_id = await record_decision(actions, "Wire the retry queue into the arq worker",
                                   source="agent:hc-tool01", repo="hctoolproj")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:hc-tool01", session="hc-tool01", project="hctoolproj",
        model=None, cwd=None)
    try:
        out = await srv.handoff_briefing(repo="hctoolproj", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    ids = {d["id"] for d in out["shipped"]}
    assert str(new_id)[:8] in ids
    assert str(did)[:8] not in ids  # the handoff marker itself predates its own boundary
    assert "markdown" in out and "## Judgment" in out["markdown"]


async def test_handoff_briefing_tool_errors_on_an_unknown_repo(actions: Actions) -> None:
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
        agent_id="agent:hc-tool02", session="hc-tool02", project="p", model=None, cwd=None)
    try:
        out = await srv.handoff_briefing(repo="no-such-project-xyz", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out


async def test_handoff_briefing_tool_accepts_an_explicit_since_override(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    old_id = await record_decision(actions, "Rotate the staging DB credentials",
                                   repo="hctooloverrideproj", source="agent:hc-tool03")
    old_ts = await actions.pool.fetchval(
        "SELECT observed_at FROM assertions WHERE object_id=$1 AND name='summary' "
        "AND evidence_class='self_declared'", old_id)
    since = (old_ts + timedelta(microseconds=1)).isoformat()
    new_id = await record_decision(actions, "Cut the release branch for next week's launch",
                                   repo="hctooloverrideproj", source="agent:hc-tool03")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:hc-tool03", session="hc-tool03", project="p", model=None, cwd=None)
    try:
        out = await srv.handoff_briefing(repo="hctooloverrideproj", since=since, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["since_note"] == "explicit since given"
    ids = {d["id"] for d in out["shipped"]}
    assert str(new_id)[:8] in ids
    assert str(old_id)[:8] not in ids

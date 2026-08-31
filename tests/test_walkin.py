"""walk_in — the door for a mind with nothing but this server, walking in cold. Pure
`walk_in_named` core first (name + office, skip-detected, stop-on-refusal), then the MCP
tool layer's own mount half — same split as test_lift.py."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import claim_name
from src.orchestrator.walkin import walk_in_named


async def _mounted(actions: Actions, agent_id: str, *, project: str = "stopslop") -> None:
    """A bare mounted-but-anonymous Agent — walk_in_named's own starting shape (the mount
    half is out of scope for this module; the MCP wrapper owns it)."""
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(a, "project", project, agent_id, now, 0.9,
                                  evidence_class="self_declared")


async def test_walk_in_named_the_whole_ceremony(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _mounted(actions, "agent:ooblek001")

    out = await walk_in_named(
        actions.pool, agent_id="agent:ooblek001", handle="Ooblek", wants_office=True)

    assert "error" not in out
    assert out["agent"] == "agent:ooblek001"
    assert out["handle"] == "Ooblek"
    assert out["claim_name"]["ran"] is True
    assert out["claim_name"]["result"]["claimed"] == "Ooblek"
    assert out["establish_office"]["ran"] is True
    assert out["establish_office"]["result"]["office"] == str(tmp_path / "seats" / "ooblek")
    office = tmp_path / "seats" / "ooblek"
    assert (office / "CLAUDE.md").is_file()


async def test_walk_in_named_refuses_a_blank_handle(actions: Actions) -> None:
    await _mounted(actions, "agent:blank0001")
    out = await walk_in_named(
        actions.pool, agent_id="agent:blank0001", handle="   ", wants_office=True)
    assert "error" in out
    assert "f39a9849" in out["error"]


async def test_walk_in_named_wants_office_false_skips_the_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _mounted(actions, "agent:visitor01")

    out = await walk_in_named(
        actions.pool, agent_id="agent:visitor01", handle="Visitor", wants_office=False)

    assert "error" not in out
    assert out["claim_name"]["ran"] is True                  # named, still
    assert out["establish_office"]["ran"] is False
    assert "visitor" in out["establish_office"]["note"].lower()
    assert not (tmp_path / "seats").exists()                 # no office written at all


async def test_walk_in_named_skips_an_already_claimed_name(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-running (the Ooblek shape: two compactions deep, name already claimed from an
    earlier turn) never re-claims — it reports the skip honestly and still runs the office
    half fresh if asked."""

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _mounted(actions, "agent:already01")
    await claim_name(actions, "agent:already01", "Already", source="test")

    out = await walk_in_named(
        actions.pool, agent_id="agent:already01", handle="Already", wants_office=True)

    assert "error" not in out
    assert out["claim_name"]["ran"] is False
    assert "already claimed" in out["claim_name"]["note"]
    assert out["establish_office"]["ran"] is True             # office half still runs


async def test_walk_in_named_refuses_a_mismatched_re_claim(actions: Actions) -> None:
    """Asking to claim a DIFFERENT name than the one already held refuses rather than
    guessing which one was meant — walk_in never renames."""
    await _mounted(actions, "agent:already02")
    await claim_name(actions, "agent:already02", "FirstName", source="test")

    out = await walk_in_named(
        actions.pool, agent_id="agent:already02", handle="SecondName", wants_office=False)

    assert "error" in out
    assert out["step"] == "claim_name"
    assert "already claimed a different name" in out["error"]
    assert "FirstName" in out["error"]


async def test_walk_in_named_propagates_claim_name_refusals_and_stops(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name already live-held by someone else refuses at claim_name, and walk_in_named
    stops there — never proceeds to establish_office under a name that never landed."""

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _mounted(actions, "agent:holder0001")
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    h = await actions.create_or_find_object("Agent", "agent:holder0001", "agent:holder0001")
    await actions.assert_property(h, "handle", "Taken", "agent:holder0001", now, 0.9,
                                  evidence_class="self_declared")
    from src.orchestrator.mounts import save_mount
    # ONE LIVENESS AUTHORITY, FOURTH DOOR (Thoth msg 5719, 2026-08-26): claim_name's own
    # refusal now cross-checks is_occupied_by_a_live_body — job_dir's basename is exactly
    # 8 chars ("holder01") so a fake harness census can confirm this row (registry_census
    # keys agent_mounts.job_dir's basename against sessionId[:8]).
    await save_mount(actions.pool, job_dir="/j/holder01", agent_id="agent:holder0001",
                     project="stopslop", cwd="/w/holder", model=None, session_key=None)

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "holder01-0000-4000-8000-000000000000", "pid": 222,
                 "cwd": "/w/holder", "name": "[OS] Taken"}]

    await _mounted(actions, "agent:newcomer1")
    out = await walk_in_named(
        actions.pool, agent_id="agent:newcomer1", handle="Taken", wants_office=True,
        agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/w/holder")

    assert "error" in out
    assert out["step"] == "claim_name"
    assert not (tmp_path / "seats").exists()                 # never reached establish_office


# ═══════════ THE MCP TOOL LAYER — THE MOUNT HALF ═══════════
# Same technique test_lift.py already established: fake a mounted connection by injecting
# an AgentIdentity into srv._agents keyed by srv._conn_key(ctx), point srv._pool at the
# test DB, call the tool FUNCTION directly.

class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_mcp_walk_in_refuses_unmounted_with_no_cwd(actions: Actions) -> None:
    import src.mcp_server as srv

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.walk_in(handle="Whoever", wants_office=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" in out
    assert "cwd" in out["error"]


async def test_mcp_walk_in_skips_mount_for_an_already_mounted_caller(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Ooblek shape exactly: already mounted, project already correct, no name yet.
    walk_in must SKIP the mount step honestly rather than re-run or refuse."""
    import src.mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:ooblek002", session="oobleksession", project="stopslop",
        model=None, cwd=None)
    await _mounted(actions, "agent:ooblek002", project="stopslop")
    try:
        out = await srv.walk_in(handle="Ooblek", wants_office=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" not in out
    assert out["mount"]["ran"] is False
    assert "already mounted as agent:ooblek002" in out["mount"]["note"]
    assert out["agent"] == "agent:ooblek002"
    assert out["claim_name"]["ran"] is True
    assert out["establish_office"]["ran"] is True

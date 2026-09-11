"""walk_in — the door for a mind with nothing but this server, walking in cold. Pure
`walk_in_named` core first (name + office, skip-detected, stop-on-refusal), then the MCP
tool layer's own mount half — same split as test_lift.py."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import claim_name
from src.orchestrator.walkin import promote_visitor, walk_in_named


async def _mounted(actions: Actions, agent_id: str, *, project: str = "stopslop") -> None:
    """A bare mounted-but-anonymous Agent — walk_in_named's own starting shape (the mount
    half is out of scope for this module; the MCP wrapper owns it).

    Carries a real `works_in` edge alongside the raw `project` assertion (a real mount/
    register_agent flow always pairs the two) — project_of (agents.py) resolves through
    lineage_works_in, never a raw stamp with nothing behind it, so a fixture missing this
    edge is a fixture lying about how mints actually work (thread 19d6bdcb7fa9/c5a91ea1)."""
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(a, "project", project, agent_id, now, 0.9,
                                  evidence_class="self_declared")
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", agent_id)
    await actions.assert_property(proj, "name", project, agent_id, now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(a, proj, "works_in", agent_id, now, 0.9,
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


# ═══════════ PIECE 2 (thread 879c97b9): promote_visitor, the THIRD-PARTY collapse ═══════════

async def _visitor_seen(
    actions: Actions, agent_id: str, *, project: str = "stopslop", job_dir: str | None = None,
) -> None:
    """A genuine VISITOR's own real shape: an `agent_mounts` row and NOTHING ELSE — no
    `objects` row of type Agent at all (the #48-gate's own third state). Also mints the
    SoftwareProject `charter_for` needs to find real (its own `_resolve_repo` refusal)."""
    from datetime import UTC, datetime

    from src.orchestrator.mounts import save_mount

    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", "test")
    await actions.assert_property(proj, "name", project, "test", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir=job_dir or f"/j/{agent_id}", agent_id=agent_id,
                     project=project, cwd=f"/w/{agent_id}", model=None, session_key=None)


async def _a_manager(actions: Actions, agent_id: str) -> None:
    """Seats `agent_id` as the manager of a real worker seat — `promote_visitor`'s own
    "manager's word" authorization leg (`seats.seats_managed_by` non-empty)."""
    from src.orchestrator.seats import bind_holder, ensure_seat

    mgr = await ensure_seat(actions, house="stopslop", handle=agent_id.split(":")[-1],
                            source="test")
    await bind_holder(actions, seat_id=mgr["seat_id"], agent_id=agent_id, source="test")
    worker = await ensure_seat(actions, house="stopslop", handle=f"{agent_id}-worker",
                               source="test")
    from datetime import UTC, datetime
    await actions.create_link(
        await actions.create_or_find_object("Seat", worker["seat_id"], "test"),
        await actions.create_or_find_object("Seat", mgr["seat_id"], "test"),
        "managed_by", "test", datetime.now(UTC), 0.9, evidence_class="self_declared")


async def test_promote_visitor_the_whole_ceremony(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _visitor_seen(actions, "agent:vis00001")

    out = await promote_visitor(
        actions.pool, target="agent:vis00001", handle="Newbie",
        because="operator ruling: onboarding a recurring visitor", actor="operator")

    assert "error" not in out
    assert out["promoted"] == "agent:vis00001"
    assert out["was_visitor"] is True
    assert out["handle"] == "Newbie"
    assert out["claim_name"]["claimed"] == "Newbie"
    assert out["charter_for"]["charter"] == ["stopslop"]
    assert out["establish_office"]["office"] == str(tmp_path / "seats" / "newbie")
    assert out["authorized_by"]["via"] == "operator"
    assert (tmp_path / "seats" / "newbie" / "CLAUDE.md").is_file()
    # the end-state really did land: a real Agent object now exists where none did before
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", "agent:vis00001")


async def test_promote_visitor_refuses_a_blank_because(actions: Actions) -> None:
    await _visitor_seen(actions, "agent:vis00002")
    out = await promote_visitor(
        actions.pool, target="agent:vis00002", handle="Newbie", because="  ",
        actor="operator")
    assert "error" in out
    assert "because is required" in out["error"]


async def test_promote_visitor_refuses_without_authorization(actions: Actions) -> None:
    await _visitor_seen(actions, "agent:vis00003")
    out = await promote_visitor(
        actions.pool, target="agent:vis00003", handle="Newbie",
        because="just because I felt like it", actor="agent:randomer1")
    assert "error" in out
    assert "not authorized" in out["error"]


async def test_promote_visitor_authorizes_via_a_managers_word(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _visitor_seen(actions, "agent:vis00004")
    await _a_manager(actions, "agent:mgr00001")

    out = await promote_visitor(
        actions.pool, target="agent:vis00004", handle="Newbie2",
        because="a manager's own call — welcoming a recurring visitor", actor="agent:mgr00001")

    assert "error" not in out
    assert out["authorized_by"]["via"] == "manager"


async def test_promote_visitor_authorizes_via_a_ruling_citation(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.capture import record_decision

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _visitor_seen(actions, "agent:vis00005")
    # verify_ruling's own three checks: resolves, kind=='ruling' (record_decision's
    # own default), and the text must literally NAME the write it authorizes.
    ruling_id = await record_decision(
        actions, "operator ruling: promote_visitor is authorized for this recurring "
        "visitor", source="test")

    out = await promote_visitor(
        actions.pool, target="agent:vis00005", handle="Newbie3",
        because="citing the ruling that approved this", actor="agent:randomer2",
        ruling=str(ruling_id))

    assert "error" not in out
    assert out["authorized_by"]["via"] == "ruling"
    assert out["authorized_by"]["ruling"] == str(ruling_id)


async def test_promote_visitor_refuses_a_ruling_that_does_not_resolve(actions: Actions) -> None:
    await _visitor_seen(actions, "agent:vis00006")
    out = await promote_visitor(
        actions.pool, target="agent:vis00006", handle="Newbie4",
        because="citing a ruling", actor="agent:randomer3", ruling="not-a-real-ruling-id")
    assert "error" in out
    # verify_ruling's own specific refusal, propagated verbatim (never re-derived)
    assert "no such decision" in out["error"]


async def test_promote_visitor_refuses_a_ruling_that_does_not_name_the_write(
    actions: Actions,
) -> None:
    """A real ruling, about something else entirely, cannot silently authorize this
    write just because a caller cited it — verify_ruling's own third check."""
    from src.orchestrator.capture import record_decision

    await _visitor_seen(actions, "agent:vis00010")
    ruling_id = await record_decision(
        actions, "Osiris HAS HANDS, admitted and governed", source="test")
    out = await promote_visitor(
        actions.pool, target="agent:vis00010", handle="Newbie10",
        because="citing an unrelated ruling", actor="agent:randomer4",
        ruling=str(ruling_id))
    assert "error" in out
    assert "does not name" in out["error"]


async def test_promote_visitor_refuses_an_already_real_agent(actions: Actions) -> None:
    await _mounted(actions, "agent:real00001")
    out = await promote_visitor(
        actions.pool, target="agent:real00001", handle="Newbie5",
        because="operator ruling", actor="operator")
    assert "error" in out
    assert "already has an Agent object" in out["error"]


async def test_promote_visitor_refuses_a_target_never_seen(actions: Actions) -> None:
    out = await promote_visitor(
        actions.pool, target="agent:ghost0001", handle="Newbie6",
        because="operator ruling", actor="operator")
    assert "error" in out
    assert "never mounted" in out["error"]


async def test_promote_visitor_refuses_when_no_project_is_known_and_none_given(
    actions: Actions,
) -> None:
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/j/vis00007", agent_id="agent:vis00007",
                     project=None, cwd="/w/vis00007", model=None, session_key=None)
    out = await promote_visitor(
        actions.pool, target="agent:vis00007", handle="Newbie7",
        because="operator ruling", actor="operator")
    assert "error" in out
    assert out["step"] == "charter_for"
    assert "never guesses a charter" in out["error"]


async def test_promote_visitor_stops_on_claim_name_refusal(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SEAT-LABEL name (a numeral suffix, e.g. "Newbie VIII") refuses at claim_name —
    the substrate assigns generations, a caller may not claim one — and promote_visitor
    stops there, never proceeding to charter_for/establish_office under a name that never
    landed (same stop-on-refusal law walk_in_named already keeps)."""
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _visitor_seen(actions, "agent:vis00008")

    out = await promote_visitor(
        actions.pool, target="agent:vis00008", handle="Newbie VIII",
        because="operator ruling", actor="operator")

    assert "error" in out
    assert out["step"] == "claim_name"
    assert "steps_so_far" in out
    assert not (tmp_path / "seats").exists()


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


async def test_mcp_seat_promote_visitor_end_to_end(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatcher door itself (`seat(action='promote_visitor')`), mounted as the
    operator, promoting a THIRD-PARTY visitor — not itself."""
    import src.mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    await _visitor_seen(actions, "agent:vis00009")
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="operator", session="operatorsession", project="stopslop",
        model=None, cwd=None)
    try:
        out = await srv.seat(
            action="promote_visitor", target="agent:vis00009", handle="Newbie9",
            because="operator's own hand, via the seat dispatcher", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" not in out
    assert out["promoted"] == "agent:vis00009"
    assert out["authorized_by"]["via"] == "operator"


async def test_mcp_seat_promote_visitor_refuses_unmounted(actions: Actions) -> None:
    import src.mcp_server as srv

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.seat(
            action="promote_visitor", target="agent:whoever1", handle="Whoever",
            because="reason", ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" in out
    assert "mount first" in out["error"]

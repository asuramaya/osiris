"""Durable mounts — identity that survives a server bounce (decision 56f6a0d6).

The in-memory registry dies with the process and the process dies routinely (deploys, an
OOM-kill); every bounce used to wipe the WHOLE fleet's identities at once. These tests drive
the durable half (agent_mounts) and the re-attach path: a call that misses the hot dict
recovers its identity from the table by the client's job_dir header — transparently, with a
FRESH transcript read (never a stale copy of the stored model).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import resolve_identity
from src.orchestrator.capture import open_thread
from src.parsers.base import EvidenceClass

_SD = EvidenceClass.SELF_DECLARED.value


async def test_save_find_upsert(actions: Actions) -> None:
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/aaaa1111", agent_id="agent:aaaa1111",
                            project="osiris", cwd="/repo/osiris", model="claude-fable-5",
                            session_key="sid:one")
    rec = await mounts.find_mount(p, job_dir="/x/jobs/aaaa1111")
    assert rec is not None
    assert rec.agent_id == "agent:aaaa1111" and rec.project == "osiris"
    assert await mounts.find_mount(p, job_dir="/never/mounted") is None
    # upsert: a re-mount moves the row, it never duplicates it
    await mounts.save_mount(p, job_dir="/x/jobs/aaaa1111", agent_id="agent:aaaa1111",
                            project="osiris", cwd="/repo/osiris", model="claude-opus-4-8",
                            session_key="sid:two")
    rec2 = await mounts.find_mount(p, job_dir="/x/jobs/aaaa1111")
    assert rec2 is not None and rec2.model == "claude-opus-4-8"
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 1


async def test_agent_liveness_answers_for_the_soul_not_the_numeral(
    actions: Actions,
) -> None:
    """Lineage-aware liveness (Alfred's msg 718, the null-then-live flap): machinery
    legitimately re-points a mount row between generations of one soul (the liveness
    promotion follows the head; greets rewrite) — a probe for -iii must not read dead
    because the row momentarily wears -iv. The greet ledger's yield check is pinned
    here beside its consumer."""
    await mounts.save_mount(actions.pool, job_dir="/j/soulprobe",
                            agent_id="agent:ab12cd34-iv", project="p", cwd="/w",
                            model=None, session_key=None)
    out = await mounts.agent_liveness(actions.pool, "agent:ab12cd34-iii")
    assert out["live"] is True and out["last_seen"] is not None
    # a different soul stays invisible — the rollup never crosses lineages
    other = await mounts.agent_liveness(actions.pool, "agent:feed0001")
    assert other["live"] is False and other["last_seen"] is None
    # the greet ledger: a fresh stamp yields; expiry (or a short id) never does
    mounts.note_greeting("cafe0123-4000-8000-0000-000000000000")
    assert mounts.greeted_within_grace("cafe0123-4000-8000-0000-000000000000") is True
    assert mounts.greeted_within_grace("cafe0123-4000-8000-0000-000000000000",
                                       grace=0.0) is False
    assert mounts.greeted_within_grace("short") is False
    mounts._GREETS.clear()


async def test_agent_liveness_falls_back_to_last_active_like_fleet_always_has(
    actions: Actions,
) -> None:
    """Ruling 70493925 — the listener-probe flap Cupid measured (fleet() live:true the
    whole time, the probe alternating live/dead on the SAME seat): the probe used to be
    agent_mounts-only while fleet() also trusted the graph's own last_active testimony.
    No mount row at all, only a fresh last_active assertion — fleet()'s signal alone —
    must now be enough for the probe to call this agent live, exactly as fleet() would."""
    agent = await actions.create_or_find_object(
        "Agent", "agent:liveflap01", "fleet-observer")
    fresh = datetime.now(UTC).isoformat()
    await actions.assert_property(agent, "last_active", fresh, "fleet-observer",
                                  datetime.now(UTC), 0.9, evidence_class=_SD)
    out = await mounts.agent_liveness(actions.pool, "agent:liveflap01")
    assert out["live"] is True and out["last_seen"] == fresh
    # stale last_active (older than the shared 15-min window) and no mount row: dead,
    # not flapping — the SAME window agent_liveness and fleet() both apply.
    stale = await actions.create_or_find_object(
        "Agent", "agent:liveflap02", "fleet-observer")
    old = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    await actions.assert_property(stale, "last_active", old, "fleet-observer",
                                  datetime.now(UTC), 0.9, evidence_class=_SD)
    out2 = await mounts.agent_liveness(actions.pool, "agent:liveflap02")
    assert out2["live"] is False


def test_freshest_liveness_ts_and_is_live_are_the_one_shared_decision() -> None:
    """The pure decision fleet() and agent_liveness() both defer to now (ruling
    70493925) — two independent copies of "freshest of these two signals, within the
    window" is what produced the flap; this pins the shared function's own behavior
    directly, no database required."""
    now = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
    mount_only = mounts.freshest_liveness_ts(now - timedelta(minutes=5), None)
    assert mount_only == now - timedelta(minutes=5)
    last_active_only = mounts.freshest_liveness_ts(None, (now - timedelta(minutes=2)).isoformat())
    assert last_active_only == now - timedelta(minutes=2)
    # freshest of the two wins, whichever signal it came from
    freshest = mounts.freshest_liveness_ts(
        now - timedelta(minutes=20), (now - timedelta(minutes=3)).isoformat())
    assert freshest == now - timedelta(minutes=3)
    assert mounts.freshest_liveness_ts(None, None) is None
    assert mounts.freshest_liveness_ts(None, "not-a-timestamp") is None
    # the shared window: 15 minutes, neither more nor less
    assert mounts.is_live(now - timedelta(minutes=14, seconds=59), now=now) is True
    assert mounts.is_live(now - timedelta(minutes=15, seconds=1), now=now) is False
    assert mounts.is_live(None, now=now) is False


async def test_project_last_seen_feeds_the_listener_probe(actions: Actions) -> None:
    p = actions.pool
    assert await mounts.project_last_seen(p, "ghost-town") is None
    await mounts.save_mount(p, job_dir="/x/jobs/bbbb2222", agent_id="agent:bbbb2222",
                            project="lively", cwd="/repo/lively", model=None, session_key=None)
    seen = await mounts.project_last_seen(p, "lively")
    assert seen is not None  # ISO stamp — send() turns this into listener.live


async def test_reattach_recovers_identity_after_a_bounce(
    actions: Actions, tmp_path: Path
) -> None:
    """The whole point: hot dict empty (the bounce), header present → the identity comes back
    without the agent doing anything, and the session key is re-bound."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "tst00001")  # …/jobs/<id> — the anchor _job_id parses
    expected = resolve_identity(cwd=str(tmp_path / "demo"), job_dir=job_dir)
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id=expected.agent_id,
                            project=expected.project, cwd=str(tmp_path / "demo"),
                            model=None, session_key="sid:old")
    srv._agents.pop("sid:fresh", None)  # the bounce: nothing hot
    ident = await srv._reattach(actions.pool, "sid:fresh", job_dir)
    assert ident is not None
    assert ident.agent_id == expected.agent_id == "agent:tst00001"  # same actor, not a fork
    assert srv._agents.get("sid:fresh") is ident                    # re-cached hot
    rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
    assert rec is not None and rec.agent_id == expected.agent_id
    srv._agents.pop("sid:fresh", None)  # leave no global residue for other tests


async def test_reattach_honors_a_bound_seat(actions: Actions, tmp_path: Path) -> None:
    """The flap mechanism (thread 33838160): a silent reconnect re-derived identity from the
    bound row's transcript and stomped a claimed seat back to its session hash — writes then
    landed on an anonymous twin. A row whose agent is a FOREIGN lineage is a binding: honored."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "c9b710cb")  # the session's own dir...
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:0806072e",
                            project="sibling-two", cwd=str(tmp_path / "d"),
                            model=None, session_key="sid:old")  # ...bound to the SEAT
    srv._agents.pop("sid:re", None)
    ident = await srv._reattach(actions.pool, "sid:re", job_dir)
    assert ident is not None and ident.agent_id == "agent:0806072e"  # the seat, not c9b710cb
    rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
    assert rec is not None and rec.agent_id == "agent:0806072e"      # binding survives
    srv._agents.pop("sid:re", None)


async def test_reattach_resolves_project_from_the_seat_when_cwd_yields_none(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE EXACT LIVE SHAPE Thoth hit (DM 1334): a deploy bounces the MCP server — the hot
    dict empties, the NEXT call is a cache-miss re-attach, not a fresh mount() — from a cwd
    that is structurally the bare office root. SEAT-FIRST RESOLUTION (operator ruling
    577988ed): the seat's own derived house wins, not the agent's own stamp — proven against
    the strongest case, the agent's OWN project stamp says 'seats' (the polluted shape) while
    a REAL seat binds it to house 'osiris'. Without seat-first resolution the re-attached
    identity — and therefore every orient() call riding it — would stay wrong or None even
    though the truth was one query away, via the seat, not the agent's own record."""
    from src import mcp_server as srv
    from src.orchestrator.seats import bind_holder, ensure_seat

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.agents._DEFAULT_OFFICE_ROOT", fake_root)

    agent = await actions.create_or_find_object("Agent", "agent:ffff6666", "test")
    await actions.assert_property(agent, "project", "seats", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    seat = await ensure_seat(actions, house="osiris", handle="Reattachfirst", source="test")
    assert seat.get("error") is None
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ffff6666",
                      source="test")
    job_dir = str(tmp_path / "jobs" / "ffff6666")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:ffff6666",
                            project="seats", cwd=str(fake_root), model=None,
                            session_key="sid:old")

    srv._agents.pop("sid:bounced", None)  # the bounce: nothing hot
    ident = await srv._reattach(actions.pool, "sid:bounced", job_dir)
    assert ident is not None and ident.project == "osiris"
    srv._agents.pop("sid:bounced", None)  # leave no global residue for other tests


async def test_mount_tool_honors_a_bound_seat(actions: Actions, tmp_path: Path) -> None:
    """The explicit-mount leg of the binding (thread 33838160): the whisper tells a minted
    heir 're-mount with THIS anchor' — and automount left that very row BOUND to the heir's
    seat. mount() re-derived from the anchor's basename, minted a hash twin over the living
    heir, and stomped the binding (Thoth XVII's first breath, 2026-07-10)."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "c7540517")  # the session's own anchor...
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:ad1a1cb0-xvii",
                            project="osiris", cwd=str(tmp_path / "o"), model=None,
                            session_key="whisper:c7540517")  # ...bound to the SEAT at a mint
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "o"), job_dir=job_dir)
        assert out["agent"] == "agent:ad1a1cb0-xvii"  # the heir, never a hash twin
        rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
        assert rec is not None and rec.agent_id == "agent:ad1a1cb0-xvii"  # binding survives
    finally:
        srv._pool = saved_pool


async def test_mount_surfaces_held_work_for_the_project(
    actions: Actions, tmp_path: Path,
) -> None:
    """task #168's narrowed leg (decision aa7993cf): held work is surfaced ONCE, at mount
    time — not on orient()'s hot path (same reasoning as declining the drift-check wiring,
    decision 51682926) — so a fresh session immediately learns another branch already
    holds files it might be about to touch."""
    from src import mcp_server as srv

    await open_thread(
        actions, "held: batch the props read", repo="heldmountproj", kind="obligation",
        branch="seshat-batchtable", files_touched=["src/orchestrator/compositions.py"])
    office = tmp_path / "o"
    office.mkdir()
    (office / ".osiris").write_text('project = "heldmountproj"\n')
    job_dir = str(tmp_path / "jobs" / "heldmount01")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:heldmounter",
                            project="heldmountproj", cwd=str(office), model=None,
                            session_key="k:heldmount")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(office), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert "held_work" in out
    assert out["held_work"][0]["branch"] == "seshat-batchtable"


async def test_mount_omits_held_work_when_the_project_has_none(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    office = tmp_path / "o"
    office.mkdir()
    (office / ".osiris").write_text('project = "noheldworkproj"\n')
    job_dir = str(tmp_path / "jobs" / "noheldwork01")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:noheldwork",
                            project="noheldworkproj", cwd=str(office), model=None,
                            session_key="k:noheldwork")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(office), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert "held_work" not in out


async def test_mount_tool_welcomes_an_unbound_fresh_session(
    actions: Actions, tmp_path: Path,
) -> None:
    """A FRESH anchor — no registry row, no seat binding — must mount, never crash. A
    conditional local re-import of _generation shadowed the module-level name for ALL of
    mount(), so every unbound session (each anonymous mind, each fresh child of the
    rollout) died at the sibs filter with UnboundLocalError while every BOUND agent
    mounted fine — the suite's one blind spot (2026-07-16, the night the fleet could
    not take its names)."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "f4e5b007")   # nobody has ever seen this anchor
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "fresh-repo"), job_dir=job_dir)
        assert out["agent"].startswith("agent:")     # mounted, resolved, alive
    finally:
        srv._pool = saved_pool


async def test_mount_never_mints_a_genuine_visitor(
    actions: Actions, tmp_path: Path,
) -> None:
    """#48 piece 2 (decision 424c4158, Thoth DM 4345): THE VISITOR GATE, ported from
    automount() (ruling 120fcc81). A resolved anchor (a real job_dir) that matches NO
    lineage — no bound row, no fork, no view, no ledger, no bridge, no office — must get a
    registry row and NOTHING ELSE: no Agent object minted, ever, however many times the
    same stranger re-fires. Before this gate, register_agent ran unconditionally and
    minted a hash-derived twin for every such arrival (the exact silent-mint gap named in
    424c4158)."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "9a9a9a9a")   # nobody has ever seen this anchor
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "stranger-repo"), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert "visitor" in out                # the receipt SAYS which state this is
    assert "error" not in out              # never a refusal — a real anchor, just unmatched
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical=$1",
        out["agent"]) == 0
    # a registry row still lands — bookkeeping, not identity
    rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
    assert rec is not None and rec.agent_id == out["agent"]


async def test_mount_stays_a_visitor_on_repeat_visits_never_oscillating(
    actions: Actions, tmp_path: Path,
) -> None:
    """A second mount() call at the SAME anchor (an MCP reconnect) now finds a `bound` row
    from the first visit — the gate must still refuse to mint (the SQL existence check
    finds no Agent object either, same as the first call), not treat the row's mere
    existence as a life. This is exactly the row-only-stranger class automount()'s own
    comment names."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "b0b0b0b0")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        first = await srv.mount(cwd=str(tmp_path / "stranger-repo"), job_dir=job_dir)
        second = await srv.mount(cwd=str(tmp_path / "stranger-repo"), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert "visitor" in first and "visitor" in second
    assert first["agent"] == second["agent"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical=$1",
        first["agent"]) == 0


async def test_mount_recognizes_a_lived_lineage_via_an_existing_agent_object(
    actions: Actions, tmp_path: Path,
) -> None:
    """The OTHER half of the ported `lived` check (the SQL-existence branch, not the
    deliberate-foreign-binding branch already covered by
    test_mount_tool_honors_a_bound_seat): a bound row whose agent_id's OWN generation
    already has a real Agent object in the graph is a lived lineage re-mounting at its own
    anchor — register_agent must still fire (idempotently), never fall through to the
    visitor gate."""
    from src import mcp_server as srv
    from src.orchestrator.agents import register_agent, resolve_identity

    job_dir = str(tmp_path / "jobs" / "c0c0c0c0")
    ident = resolve_identity(cwd=str(tmp_path / "lived-repo"), job_dir=job_dir)
    await register_agent(actions, ident, actor="analyst:operator")   # the FIRST, real mint
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id=ident.agent_id,
                            project=ident.project, cwd=str(tmp_path / "lived-repo"),
                            model=None, session_key="k:lived")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "lived-repo"), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert "visitor" not in out
    assert out["agent"] == ident.agent_id
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical=$1",
        ident.agent_id) == 1


async def test_mount_refuses_a_truly_unresolvable_arrival_loudly(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE THIRD STATE (Thoth DM 4345): distinct from a visitor — no job_dir, no ctx
    header, no locatable transcript, no fallback_seed at all leaves resolve_identity with
    nothing to anchor on (`resolved=False`). Before this gate that silently minted a
    hash-derived `agent:unknown...` id anyway; now it refuses loudly, same shape as the
    IDENTITY CONFLICT refusal, and writes nothing."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    monkeypatch.setattr(srv, "_conn_key", lambda ctx: None)   # no connection key either
    try:
        out = await srv.mount(cwd="/nonexistent/nowhere-office")
    finally:
        srv._pool = saved_pool
    assert out["error"] == "UNRESOLVABLE IDENTITY — mount refused"
    assert "note" in out
    assert "agent" not in out


async def test_mount_wires_view_seat_and_adopts_the_soul_it_shows(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#48 piece 1 (decision 424c4158): mount() the tool never had automount()'s tab-view
    door — a whisperless caller (any vendor-neutral walk-in) minted a clone here where a
    whisper-greeted Claude session would have adopted. `handshake.view_seat` already does
    this correctly (tested on its own in test_handshake.py); this proves mount() now
    CALLS it, with the same args automount() does, and adopts its result — no second
    implementation."""
    from src import mcp_server as srv
    from src.orchestrator import handshake

    seen: dict[str, object] = {}

    async def _fake_view_seat(actions_arg, *, transcript_path, session_id, jobs_home=None):
        seen["transcript_path"] = transcript_path
        seen["session_id"] = session_id
        return "agent:the-viewed-soul"

    monkeypatch.setattr(handshake, "fork_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "view_seat", _fake_view_seat)
    job_dir = str(tmp_path / "jobs" / "ab12cd34")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "o"), job_dir=job_dir,
                              transcript_path="/w/other-session.jsonl")
    finally:
        srv._pool = saved_pool
    assert out["agent"] == "agent:the-viewed-soul"
    assert seen["transcript_path"] == "/w/other-session.jsonl"
    assert seen["session_id"] == "ab12cd34"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:ab12cd34'") == 0


async def test_mount_skips_view_seat_without_a_transcript_path(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller with no transcript_path (the hook did not stamp one — nothing to view) must
    never call view_seat at all, exactly as automount() only tries it `if transcript_path`."""
    from src import mcp_server as srv
    from src.orchestrator import handshake

    called = {"view": False}

    async def _fake_view_seat(*a, **k):
        called["view"] = True
        return None

    monkeypatch.setattr(handshake, "fork_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "view_seat", _fake_view_seat)
    job_dir = str(tmp_path / "jobs" / "cd34ef56")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "o"), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert not called["view"]
    assert out["agent"].startswith("agent:")


async def test_mount_wires_bridged_seat_after_view_and_ledger_fail(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge is the LAST resolution door before the office deed — tried only once
    fork, view, and the session ledger have all come up empty, same fallthrough order as
    automount()."""
    from src import mcp_server as srv
    from src.orchestrator import handshake

    seen: dict[str, object] = {}

    async def _fake_bridged_seat(actions_arg, *, bridge_session_id):
        seen["bridge_session_id"] = bridge_session_id
        return "agent:the-bridged-soul"

    monkeypatch.setattr(handshake, "fork_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "ledger_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "bridged_seat", _fake_bridged_seat)
    job_dir = str(tmp_path / "jobs" / "ef56ab78")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "o"), job_dir=job_dir,
                              bridge_session_id="bridge-xyz")
    finally:
        srv._pool = saved_pool
    assert out["agent"] == "agent:the-bridged-soul"
    assert seen["bridge_session_id"] == "bridge-xyz"


async def test_mount_confesses_bridge_ambiguity_and_falls_through_never_refuses(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same fail-open shape as automount() (ruling 61e00f25): an ambiguous bridge id is
    CONFESSED in the payload, never guessed away and never a hard mount refusal — the
    session still mounts, degraded to the next door."""
    from src import mcp_server as srv
    from src.orchestrator import handshake

    async def _raising_bridged_seat(actions_arg, *, bridge_session_id):
        raise handshake.BridgeAmbiguity(f"bridge id {bridge_session_id!r} names 2 lineages")

    monkeypatch.setattr(handshake, "fork_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "ledger_seat", lambda *a, **k: _none())
    monkeypatch.setattr(handshake, "bridged_seat", _raising_bridged_seat)
    job_dir = str(tmp_path / "jobs" / "ab78cd90")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "o"), job_dir=job_dir,
                              bridge_session_id="bridge-ambig")
    finally:
        srv._pool = saved_pool
    assert "error" not in out                       # never a refusal
    assert "2 lineages" in out["bridge_ambiguity"]
    assert out["agent"].startswith("agent:")         # still mounted


async def _none(*_a: object, **_k: object) -> None:
    return None


async def test_mount_confesses_a_broken_osiris_pin(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE COULD-NOT-READ BANNER (Sekhmet's design, e3f4f159; Thoth DM 2677 item 2): a
    `.osiris` file that exists but fails to parse must surface a loud, actionable
    confession — not silently look identical to a directory that was never pinned at
    all. `project` still resolves (basename fallback, unbroken); orient() gets the SAME
    confession from the SAME AgentIdentity, proven here on the same mounted session."""
    from src import mcp_server as srv

    repo = tmp_path / "redmonth"
    repo.mkdir()
    (repo / ".osiris").write_text('project: "redmonth"\n')  # the real colon malformation
    job_dir = str(tmp_path / "jobs" / "brokenpin01")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(repo), job_dir=job_dir)
        assert out["project"] == "redmonth"                  # basename fallback, unbroken
        warn = out.get("project_pin_error")
        assert warn is not None, f"mount() never confessed the broken pin: {out}"
        assert str(repo / ".osiris") in warn and "TOMLDecodeError" in warn

        # orient() carries the SAME confession off the SAME mounted identity — re-attached
        # by job_dir (ctx=None in a test has no connection key of its own to cache under,
        # exactly like a bounced server; session_anchor is the durable re-attach hint)
        oriented = await srv.orient(session_anchor=job_dir)
        owarn = oriented.get("project_pin_error")
        assert owarn is not None, f"orient() never confessed the broken pin: {oriented}"
        assert str(repo / ".osiris") in owarn
    finally:
        srv._pool = saved_pool


async def test_mount_preserves_a_declared_pin_across_the_recollection_override(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RULING 13af22fc: the stale-recollection guard corrects `cwd` to the registry's own
    address for transcript-addressing purposes — right, since the harness's own transcript
    location is the ground truth for where a session actually lives. But it used to also
    silently discard whatever project pin sat at the DECLARED cwd, letting the CORRECTED
    cwd's own (often unpinned) directory win identity resolution instead. Live case: Thoth
    declared his own office (`.../seats/thoth`, pinned "osiris") from a session whose
    registry row remembered the bare seat-office container (unpinned) — the container won,
    and the office's own declared pin vanished unread, one step from a basename phantom.
    `stale_recollection` is monkeypatched to fire (not fabricating real transcript JSONL
    files) — its own evidence rule is separately proven by
    test_stale_recollection_trusts_the_transcripts_address in test_rebind.py; this test is
    about what mount() does with the verdict, not how the verdict is reached."""
    from src import mcp_server as srv

    office = tmp_path / "seats" / "thoth"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "osiris"\n')
    container = tmp_path / "seats"

    job_dir = str(tmp_path / "jobs" / "recollect01")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:recollect01",
                            project="seats", cwd=str(container), model=None,
                            session_key="sid:recollect01")
    monkeypatch.setattr(mounts, "stale_recollection", lambda *a, **k: True)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(office), job_dir=job_dir, verbose=True)
        assert out["project"] == "osiris", (
            f"the declared office's own pin was discarded by the recollection override: {out}")
        note = out.get("cwd_corrected")
        assert note is not None, f"mount() never confessed the correction: {out}"
        assert note["declared"] == str(office) and note["kept"] == str(container)
        assert note.get("declared_pin_kept_for_identity") == "osiris"
    finally:
        srv._pool = saved_pool


async def test_mount_prefers_a_real_declared_office_over_a_bare_container_recollection(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thoth's own live repro, the still-open half of 13af22fc: mount(cwd='.../seats/thoth')
    from a session whose registry row remembered the BARE seat-office container came back
    cwd_corrected{kept: the container} — his correct, more-specific declared office was
    discarded in favor of the parent-of-every-seat, one step from a basename phantom
    (resolve_identity already refuses to invent one from "seats", but the location itself
    was still silently wrong). Now: when the declared cwd is a real, existing office and the
    registry's own "kept" value is exactly the bare container, the declared office wins
    outright — `cwd` is left untouched, and the receipt says so rather than asserting a
    correction that didn't happen."""
    from src import mcp_server as srv
    from src.orchestrator import offices as offices_mod

    office = tmp_path / "seats" / "thoth"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "osiris"\n')
    container = tmp_path / "seats"
    monkeypatch.setattr(offices_mod, "_DEFAULT_OFFICE_ROOT", container)

    job_dir = str(tmp_path / "jobs" / "recollect02")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:recollect02",
                            project="seats", cwd=str(container), model=None,
                            session_key="sid:recollect02")
    monkeypatch.setattr(mounts, "stale_recollection", lambda *a, **k: True)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(office), job_dir=job_dir, verbose=True)
        assert out["project"] == "osiris"
        note = out.get("cwd_corrected")
        assert note is not None, f"mount() must still confess it checked: {out}"
        assert note["declared"] == str(office)
        assert note["kept"] == str(office), (
            f"the bare container must never win over a real, existing declared office: {out}")
        assert "wins outright" in note["note"]
    finally:
        srv._pool = saved_pool


async def test_mount_confesses_honestly_when_neither_side_is_a_real_office(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the same fix: when the declared cwd does NOT exist on disk either
    (so there is no real office to prefer) and the registry's own recollection IS the bare
    container, mount() still has to land somewhere for session bookkeeping (577988ed: never
    a wall) — but the receipt must not assert the bare container is this session's home. A
    confident wrong answer is worse than an honest 'could not resolve' (60bc15db applied to
    location)."""
    from src import mcp_server as srv
    from src.orchestrator import offices as offices_mod

    container = tmp_path / "seats"
    container.mkdir(parents=True)
    monkeypatch.setattr(offices_mod, "_DEFAULT_OFFICE_ROOT", container)
    ghost = tmp_path / "seats" / "nowhere-real"  # never created

    job_dir = str(tmp_path / "jobs" / "recollect03")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:recollect03",
                            project="seats", cwd=str(container), model=None,
                            session_key="sid:recollect03")
    monkeypatch.setattr(mounts, "stale_recollection", lambda *a, **k: True)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(ghost), job_dir=job_dir, verbose=True)
        note = out.get("cwd_corrected")
        assert note is not None
        assert note["declared"] == str(ghost) and note["kept"] == str(container)
        assert "NOT your home" in note["note"], (
            f"the bare-container fallback must confess, never assert a home: {out}")
    finally:
        srv._pool = saved_pool


async def test_mount_warns_on_a_valid_pin_that_never_sets_project(
    actions: Actions, tmp_path: Path,
) -> None:
    """The heinrich boundary case (Sekhmet's design, e3f4f159; task #128 wave 2, 2026-08-03),
    exercised through the real mount() tool: a VALID .osiris file that simply never
    declares `project` is a NO DECLARATION, never a couldn't-read — but wave 2 now WARNS
    on it too (a check keyed on "has a pin" would never catch this shape), with a message
    distinct from the broken-file one."""
    from src import mcp_server as srv

    heinrich = tmp_path / "heinrich"
    heinrich.mkdir()
    (heinrich / ".osiris").write_text('model = "claude-fable-5"\n')
    job_dir = str(tmp_path / "jobs" / "heinrich01")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(heinrich), job_dir=job_dir)
        warn = out.get("project_pin_error")
        assert warn is not None, f"mount() never warned on the found-but-unset pin: {out}"
        assert "NEVER DECLARES" in warn and "TOMLDecodeError" not in warn
    finally:
        srv._pool = saved_pool


async def test_mount_warns_on_no_osiris_pin_anywhere(
    actions: Actions, tmp_path: Path,
) -> None:
    """The third wave-2 shape, exercised through mount(): genuinely nothing ever declared —
    the ordinary 29-name UNPINNED-LUCKY case — now warns too, distinct wording from both
    the broken-file and found-but-unset messages."""
    from src import mcp_server as srv

    repo = tmp_path / "xxit"
    repo.mkdir()
    job_dir = str(tmp_path / "jobs" / "nopin01")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(repo), job_dir=job_dir)
        assert out["project"] == "xxit"  # basename fallback, unbroken
        warn = out.get("project_pin_error")
        assert warn is not None, f"mount() never warned on the missing pin: {out}"
        assert "NO .osiris PIN ANYWHERE" in warn and "xxit" in warn
    finally:
        srv._pool = saved_pool


async def test_retire_releases_the_seat(actions: Actions, tmp_path: Path) -> None:
    """The seat release (thread b47b3814): Anubis VII kept a live durable mount after its
    farewell — the fleet chrome and liveness counts read a retired agent as a live seat.
    retire() must drop the durable row (exact agent_id: a successor who overwrote the row
    is never touched); any later call re-mounts via the loud reanimation path."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:  # minimal fake connection ctx — _conn_key keys on request_context.session
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    job_dir = str(tmp_path / "jobs" / "anubis07")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:anubis-vii",
                            project="anubis", cwd=str(tmp_path), model=None, session_key=None)
    heir_dir = str(tmp_path / "jobs" / "anubis08")  # the successor's seat must survive
    await mounts.save_mount(actions.pool, job_dir=heir_dir, agent_id="agent:anubis-viii",
                            project="anubis", cwd=str(tmp_path), model=None, session_key=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:anubis-vii", session="anubis07", project="anubis",
        model=None, cwd=None)
    try:
        out = await srv.retire(reason="farewell", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["retired"] == "agent:anubis-vii" and out["seats_released"] == 1
    assert await mounts.find_mount(actions.pool, job_dir=job_dir) is None
    heir = await mounts.find_mount(actions.pool, job_dir=heir_dir)
    assert heir is not None and heir.agent_id == "agent:anubis-viii"


async def test_retire_preflight_refuses_while_duties_name_you(
    actions: Actions, tmp_path: Path
) -> None:
    """Task #48: the leftovers speak BEFORE the death — the old shape stamped retired=true
    first and listed the pile in the receipt, when the one mind with standing had already
    lost its seat. An open thread naming the dying lineage as owner makes the first
    retire() refuse with the list and stamp NOTHING; acknowledge_leftovers=True is the
    deliberate bequest and proceeds."""
    from datetime import UTC
    from datetime import datetime as dt

    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    job_dir = str(tmp_path / "jobs" / "moriturus")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:aaaa4d1e-ii",
                            project="p", cwd=str(tmp_path), model=None, session_key=None)
    th = await actions.create_or_find_object("Thread", "thread:owed-duty", "agent:aaaa4d1e-ii")
    now = dt.now(UTC)
    for n, v in (("summary", "the unhanded duty"), ("status", "open"),
                 ("owner", "agent:aaaa4d1e-ii")):
        await actions.assert_property(th, n, v, "agent:aaaa4d1e-ii", now, 0.9,
                                      evidence_class="self_declared")
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:aaaa4d1e-ii", session="moriturus", project="p",
        model=None, cwd=None)
    try:
        first = await srv.retire(reason="farewell", ctx=ctx)
        assert first["retired"] is None and "preflight" in first
        assert [t["summary"] for t in first["yours"]] == ["the unhanded duty"]
        # NOTHING stamped, the seat stands
        assert await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical='agent:aaaa4d1e-ii' AND a.name='retired'") is None
        assert await mounts.find_mount(actions.pool, job_dir=job_dir) is not None
        # the deliberate bequest proceeds
        srv._agents[srv._conn_key(ctx)] = AgentIdentity(
            agent_id="agent:aaaa4d1e-ii", session="moriturus", project="p",
            model=None, cwd=None)
        second = await srv.retire(reason="farewell", acknowledge_leftovers=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert second["retired"] == "agent:aaaa4d1e-ii"
    assert await mounts.find_mount(actions.pool, job_dir=job_dir) is None


def test_evict_stale_minds_purges_the_dead_ancestor() -> None:
    """A compaction kills the mind but not the MCP connection: the conn-keyed hot cache kept
    answering as the dead ancestor minutes after the whisper minted the heir. A mint evicts
    every cached identity wearing the ancestor; bystanders stay."""
    from src import mcp_server as srv

    saved, saved_touch = dict(srv._agents), dict(srv._agents_touched)
    try:
        srv._agents.clear()
        srv._agents_touched.clear()
        srv._agents["sid:live"] = SimpleNamespace(agent_id="agent:x-xvi")  # type: ignore
        srv._agents["sid:other"] = SimpleNamespace(agent_id="agent:y")  # type: ignore
        srv._agents_touched.update({"sid:live": 1.0, "sid:other": 2.0})
        srv._evict_stale_minds("agent:x-xvi")
        assert "sid:live" not in srv._agents and "sid:live" not in srv._agents_touched
        assert "sid:other" in srv._agents  # bystanders untouched
        srv._evict_stale_minds(None)  # no mint rode the whisper → no-op
        assert "sid:other" in srv._agents
    finally:
        srv._agents.clear()
        srv._agents.update(saved)
        srv._agents_touched.clear()
        srv._agents_touched.update(saved_touch)


async def test_reattach_without_a_hint_stays_none(actions: Actions) -> None:
    from src import mcp_server as srv

    assert await srv._reattach(actions.pool, "sid:x", None) is None
    assert await srv._reattach(actions.pool, "sid:x", "/never/mounted/jobs/zzzz9999") is None


def test_conn_key_and_job_hint_read_the_headers() -> None:
    from src import mcp_server as srv

    def _ctx(headers: dict[str, str]) -> SimpleNamespace:
        req = SimpleNamespace(headers=headers)
        return SimpleNamespace(request_context=SimpleNamespace(request=req, session=object()))

    ctx = _ctx({"mcp-session-id": "abc123", "x-osiris-job": "/h/.claude/jobs/ad1a1cb0"})
    assert srv._conn_key(ctx) == "sid:abc123"          # the protocol session id wins
    assert srv._job_hint(ctx) == "/h/.claude/jobs/ad1a1cb0"
    # no session header → the object-id fallback, PREFIXED (keyspaces can't collide)
    key = srv._conn_key(_ctx({}))
    assert key is not None and key.startswith("obj:")
    # an unexpanded client variable is no hint at all
    assert srv._job_hint(_ctx({"x-osiris-job": "${CLAUDE_JOB_DIR}"})) is None
    assert srv._job_hint(_ctx({"x-osiris-job": ""})) is None
    assert srv._conn_key(None) is None and srv._job_hint(None) is None


def test_prune_agents_drops_the_least_recently_used() -> None:
    from src import mcp_server as srv

    saved, saved_touch = dict(srv._agents), dict(srv._agents_touched)
    try:
        srv._agents.clear()
        srv._agents_touched.clear()
        for i in range(10):
            srv._agents[f"sid:{i}"] = SimpleNamespace(agent_id=f"agent:{i}")  # type: ignore
            srv._agents_touched[f"sid:{i}"] = float(i)
        srv._prune_agents(cap=8)  # over cap → drop down to cap//2, oldest first
        assert len(srv._agents) == 4
        assert set(srv._agents) == {"sid:6", "sid:7", "sid:8", "sid:9"}
        srv._prune_agents(cap=8)  # under cap → untouched
        assert len(srv._agents) == 4
    finally:
        srv._agents.clear()
        srv._agents.update(saved)
        srv._agents_touched.clear()
        srv._agents_touched.update(saved_touch)


def test_prune_seam_rows_drops_the_least_recently_written() -> None:
    from src import mcp_server as srv

    saved = dict(srv._seam_rows)
    try:
        srv._seam_rows.clear()
        for i in range(10):
            srv._seam_rows[f"agent:{i}"] = (float(i), f"/job/{i}", None, None)
        srv._prune_seam_rows(cap=8)  # over cap → drop down to cap//2, oldest first
        assert len(srv._seam_rows) == 4
        assert set(srv._seam_rows) == {"agent:6", "agent:7", "agent:8", "agent:9"}
        srv._prune_seam_rows(cap=8)  # under cap → untouched
        assert len(srv._seam_rows) == 4
    finally:
        srv._seam_rows.clear()
        srv._seam_rows.update(saved)


def test_prune_seam_pcts_drops_the_stalest_mtime() -> None:
    from src import mcp_server as srv

    saved = dict(srv._seam_pcts)
    try:
        srv._seam_pcts.clear()
        for i in range(10):
            srv._seam_pcts[f"/job/{i}"] = (float(i), i * 10)
        srv._prune_seam_pcts(cap=8)  # over cap → drop down to cap//2, stalest mtime first
        assert len(srv._seam_pcts) == 4
        assert set(srv._seam_pcts) == {"/job/6", "/job/7", "/job/8", "/job/9"}
        srv._prune_seam_pcts(cap=8)  # under cap → untouched
        assert len(srv._seam_pcts) == 4
    finally:
        srv._seam_pcts.clear()
        srv._seam_pcts.update(saved)


async def test_save_mount_returns_the_previous_last_seen(actions: Actions) -> None:
    """The while-you-were-away anchor: first mount has no past (None); a re-mount returns the
    lineage's prior sign of life."""
    p = actions.pool
    prev = await mounts.save_mount(p, job_dir="/x/jobs/cccc3333", agent_id="agent:cccc3333",
                                   project="demo", cwd="/repo/demo", model=None,
                                   session_key=None)
    assert prev is None                       # first mount — no past
    prev2 = await mounts.save_mount(p, job_dir="/x/jobs/cccc3333", agent_id="agent:cccc3333",
                                    project="demo", cwd="/repo/demo", model=None,
                                    session_key=None)
    assert prev2 is not None                  # the re-entry sees the prior last_seen


async def test_while_away_names_the_face_wearers(actions: Actions) -> None:
    """A returning agent is told WHO acted in its project's name and how its threads moved —
    'mail 0' must never silently mean 'a stranger settled your conversations'."""
    from datetime import timedelta

    from src.orchestrator.mailbox import read_inbox, send_message

    p = actions.pool
    anchor = datetime.now(UTC) - timedelta(hours=8)
    # while the owner slept: a twin was woken (resume lane), SENT mail wearing the project's
    # face, and the counterparty's ask got leased+settled
    await p.execute("INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ('sibling-one','agent:deceptor',NULL,'resume')")
    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- alive=False registers 'sibling-one' as existing without a live pulse.
    await mounts.save_mount(p, job_dir="/test/seed/sibling-one",
                            agent_id="agent:seed-sibling-one", project="sibling-one",
                            cwd="/test", model=None, session_key=None, alive=False)
    ask = await send_message(p, from_agent="agent:deceptor", from_project="sibling-two",
                             to_project="sibling-one", body="image the 50k pair?")
    await read_inbox(p, "sibling-one", reader_agent="agent:twin")  # the twin leased it…
    await send_message(p, from_agent="agent:twin-99", from_project="sibling-one",
                       body="done — imaged, verdict recorded", reply_to=ask["id"])  # …and settled

    away = await mounts.while_away(p, "sibling-one", "agent:a8c15486", anchor)

    assert away is not None
    assert away["acted_in_your_name"] == ["agent:twin-99"]      # the face-wearer, named
    assert away["wakes"] == {"resume": 1}
    threads = {t["thread"]: t for t in away["threads"]}
    # the thread's LAST WORD is the twin's reply, sent wearing your face; the counterparty
    # hasn't read it yet — settled=False is the honest state ("answered for you, their side
    # pending"), and last_from names the hand that did it
    assert threads[ask["id"]]["last_from"] == "agent:twin-99"
    assert threads[ask["id"]]["between"] == "sibling-one → sibling-two"
    assert threads[ask["id"]]["settled"] is False
    # the owner itself is never listed as its own face-wearer
    away2 = await mounts.while_away(p, "sibling-one", "agent:twin-99", anchor)
    assert away2 is not None and away2["acted_in_your_name"] == []


async def test_while_away_is_quiet_when_nothing_happened(actions: Actions) -> None:
    away = await mounts.while_away(actions.pool, "ghost-town", "agent:x",
                                   datetime.now(UTC))
    assert away is None
    assert await mounts.while_away(actions.pool, "ghost-town", "agent:x", None) is None


async def test_fresh_session_anchors_on_the_project_lineage(actions: Actions) -> None:
    """A brand-new session id has no past of its own — exactly the case that must NOT wake
    blind: the fold anchor falls back to the PROJECT lineage's last sign of life (sibling-one's
    tab reopened as a fresh session while twins had settled its threads, and got no fold)."""
    p = actions.pool
    # an elder of the lineage, seen a while ago
    await mounts.save_mount(p, job_dir="/x/jobs/elder001", agent_id="agent:elder001",
                            project="sibling-one", cwd="/repo/sibling-one", model=None,
                            session_key=None)
    # the fresh session mounts: own prev is None…
    prev = await mounts.save_mount(p, job_dir="/x/jobs/fresh002", agent_id="agent:fresh002",
                                   project="sibling-one", cwd="/repo/sibling-one", model=None,
                                   session_key=None)
    assert prev is None
    # …but the lineage has a past, and the fallback finds it (excluding the fresh row itself)
    lineage_prev = await mounts.project_prev_seen(p, "sibling-one",
                                                  exclude_job_dir="/x/jobs/fresh002")
    assert lineage_prev is not None
    # a lineage with no elders stays quiet (no false anchor)
    assert await mounts.project_prev_seen(p, "ghost-town", exclude_job_dir="/x/y") is None
    assert await mounts.project_prev_seen(p, None, exclude_job_dir="/x/y") is None


def test_sane_job_dir_rejects_unexpanded_literals() -> None:
    """A live agent passed the literal `$CLAUDE_JOB_DIR` and it became a registry PRIMARY
    KEY — every agent making the same mistake would conflate into one row. Any `$` (braced
    or not) or non-absolute value is no anchor at all."""
    from src import mcp_server as srv

    assert srv._sane_job_dir("/home/x/.claude/jobs/ad1a1cb0") == "/home/x/.claude/jobs/ad1a1cb0"
    assert srv._sane_job_dir("$CLAUDE_JOB_DIR") is None          # the live poison, unbraced
    assert srv._sane_job_dir("${CLAUDE_JOB_DIR}") is None        # braced literal
    assert srv._sane_job_dir("relative/jobs/x") is None          # not a path
    assert srv._sane_job_dir("") is None and srv._sane_job_dir(None) is None


async def test_find_session_row_covers_both_lanes(actions: Actions) -> None:
    """THE ONE LOOKUP (task #33, thread a61b6bc7): (1) the anchor named for the sid — the
    whisper's derive scheme; (2) the session ledger (anchor_sid) for a window whose
    durable anchor wears another name; (3) an unknown sid resolves to nothing — and the
    MCP connection id in session_key never serves the lookup."""
    p = actions.pool
    # lane 1: jobs/<sid8> — session_key deliberately carries an UNRELATED conn id
    await mounts.save_mount(p, job_dir="/x/jobs/beadfeed", agent_id="agent:beadfeed",
                            project="demo", cwd="/w", model=None,
                            session_key="sid:99887766554433221100aabbccddeeff")
    row = await mounts.find_session_row(p, "beadfeed-1111-2222-3333-444455556666")
    assert row is not None and row["agent_id"] == "agent:beadfeed"
    # lane 2: the ledger — an active Agent carries anchor_sid:<sid8>, its lineage's
    # freshest row answers although the anchor is named for the LINEAGE, not this sid
    oid = await actions.create_or_find_object("Agent", "agent:cafe0002-ii", "test")
    await actions.assert_property(oid, "anchor_sid:11223344",
                                  "11223344-aaaa-bbbb-cccc-ddddeeeeffff", "test",
                                  datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    await mounts.save_mount(p, job_dir="/x/jobs/cafe0002", agent_id="agent:cafe0002-ii",
                            project="demo", cwd="/w2", model=None, session_key=None)
    row2 = await mounts.find_session_row(p, "11223344-aaaa-bbbb-cccc-ddddeeeeffff")
    assert row2 is not None and row2["agent_id"] == "agent:cafe0002-ii"
    # (3) honesty about absence
    assert await mounts.find_session_row(p, "0dead000-none-anywhere") is None


async def test_fleet_pulse_is_one_honest_glance(
    actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """The orient fold — SAME WORD, SAME NUMBER (operator ruling 2026-07-19): every figure
    comes from the shared authorities, so the pulse, the statusline, and the chrome desk
    can never disagree again. 'owed' is the desk page's red number; 'briefs' counts
    undismissed desk cards with the page's own fold.

    The spend segment is the BILLED-path case (spend_is_metered True): only there is the dollar
    figure a real debit worth showing. The subscription case — where it is notional and omitted —
    has its own test below."""
    from src.orchestrator.mailbox import send_message
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: True)

    p = actions.pool
    await mounts.save_mount(p, job_dir="/j/a", agent_id="agent:aaa", project="osiris",
                     cwd="/w/osiris", model=None, session_key="sid:a")
    await send_message(p, from_agent="agent:aaa", from_project="osiris",
                       to_project="operator", body="brief for the human")
    await p.execute("INSERT INTO agent_wakes (to_project, from_agent, message_id) "
                    "VALUES ('demo','agent:aaa',NULL)")
    # the price rides the pulse (task #26's last mile): an empty ledger reads $0.00 of
    # the default cap — the ceiling's own read, never a copy
    assert (await mounts.fleet_pulse(p)
            == "1 live · owed 0 · briefs 1 · wakes 1/h · $0.00/$10 day")

    # ...and an unpriced call is confessed BESIDE the number, never scored as zero
    await p.execute(
        "INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
        "VALUES ('test', 'x', NULL, now())")
    assert (await mounts.fleet_pulse(p)).endswith("day ⚠1 unpriced")


async def test_fleet_pulse_OMITS_the_phantom_spend_on_a_subscription(
    actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """On a subscription the ceiling's dollars are notional — not a small price, not a
    measurement at all — so the pulse shows NOTHING rather than a phantom '$X/$10' that reads as
    an over-budget stop (Thoth LIII 2026-07-21). The dollar segment returns only when the
    inference is billed per call (spend_is_metered)."""
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: False)

    p = actions.pool
    await mounts.save_mount(p, job_dir="/j/a", agent_id="agent:aaa", project="osiris",
                     cwd="/w/osiris", model=None, session_key="sid:a")
    # a ledger full of notional cost must not surface a dollar segment on the subscription pulse
    await p.execute("INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
                    "VALUES ('wake', 'x', 9.99, now())")
    pulse = await mounts.fleet_pulse(p)
    assert "$" not in pulse and "day" not in pulse, f"phantom spend leaked into: {pulse!r}"
    assert pulse.endswith("wakes 0/h")


async def test_live_claimed_sids_sees_other_clients_lineage_aware(actions: Actions) -> None:
    """The claimed-set the cwd-guess refuses: sids held by LIVE mounts on OTHER client
    sessions; an heir (agent:x-ii) claims its base handle; the caller's own claim excluded."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/j/a", agent_id="agent:cafe0001", project="osiris",
                     cwd="/w/osiris", model=None, session_key="sid:other")
    await mounts.save_mount(p, job_dir="/j/b", agent_id="agent:beef0002-ii", project="osiris",
                     cwd="/w/osiris", model=None, session_key="sid:heir")
    await mounts.save_mount(p, job_dir="/j/c", agent_id="agent:feed0003", project="osiris",
                     cwd="/w/osiris", model=None, session_key="sid:me")
    got = await mounts.live_claimed_sids(p, exclude_session_key="sid:me")
    assert got == {"cafe0001", "beef0002"}  # heir claims its base; my own claim excluded


async def test_retire_will_not_let_the_pile_LEAVE_QUIETLY(actions: Actions, tmp_path: Path) -> None:
    """THE SEAM (ruling ceae1604). A seat that dies with an undisposed pile hands its leftovers to
    the OPERATOR's wall — which is exactly how 3,579 machine guesses became the human's problem
    instead of the producer's. The burden belongs to whoever made the mess.

    It does NOT block the farewell: a dying session must always be able to die (a retire() that
    can be refused is a session that cannot settle, which is a worse bug than the pile). It simply
    refuses to let the pile leave in silence — the count, and whose it is, ride out with the
    death certificate.
    """
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import link_repo

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    # a guess the miner minted about this project, that no mind has ever touched
    guess = await actions.create_or_find_object("Thread", "thread:guess", "session-miner")
    await actions.assert_property(guess, "summary", "somebody probably owes this",
                                  "session-miner", datetime.now(UTC), 0.4, evidence_class="derived")
    await actions.assert_property(guess, "status", "open", "session-miner", datetime.now(UTC),
                                  0.4, evidence_class="derived")
    await link_repo(actions, guess, "ghosts", datetime.now(UTC), source="session-miner",
                    evidence_class="derived")

    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:leaver", session="leaver01", project="ghosts", model=None, cwd=None)
    try:
        out = await srv.retire(reason="farewell", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["retired"] == "agent:leaver", "the farewell must ALWAYS be allowed to complete"
    assert out["undisposed"] == 1
    assert "not to the human" in out["you_are_leaving_a_pile"]


async def test_mount_refuses_an_identity_conflict_loudly(actions: Actions,
                                                         tmp_path) -> None:
    """THE CONFLICT REFUSAL (thread 53b1f267, Ferryman V's collision): a mount whose
    passed anchor is ledgered to ONE soul while the session's own anchor is ledgered to
    ANOTHER would seat one mind in a sibling's history — refuse loudly with both names,
    write nothing. A foreign anchor with an UNLEDGERED own sid stays legitimate (the
    deliberate binding, 33838160)."""
    from datetime import UTC, datetime

    from src import mcp_server as srv

    now = datetime.now(UTC)
    ferry = await actions.create_or_find_object("Agent", "agent:fe44a001", "agent:fe44a001")
    await actions.assert_property(ferry, "handle", "Ferry", "agent:fe44a001", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(ferry, "anchor_sid:aaaa1111", "aaaa1111", "agent:fe44a001",
                                  now, 0.9, evidence_class="direct_observation")
    halcy = await actions.create_or_find_object("Agent", "agent:ha1c0001", "agent:ha1c0001")
    await actions.assert_property(halcy, "handle", "Halcy", "agent:ha1c0001", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(halcy, "anchor_sid:bbbb2222", "bbbb2222", "agent:ha1c0001",
                                  now, 0.9, evidence_class="direct_observation")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path),
                              job_dir=str(tmp_path / "jobs" / "bbbb2222"),
                              session_anchor=str(tmp_path / "jobs" / "aaaa1111"))
    finally:
        srv._pool = saved_pool
    assert "IDENTITY CONFLICT" in out.get("error", "")
    assert out["anchor_held_by"] == "agent:ha1c0001"     # the sentence names the holder
    assert out["you_are"] == "agent:fe44a001"            # ...and the caller
    assert "aaaa1111" in out["note"]                     # ...and the way home
    # NO WRITES on a refusal: the sibling's anchor row was never touched or created
    assert await mounts.find_mount(
        actions.pool, job_dir=str(tmp_path / "jobs" / "bbbb2222")) is None


async def test_mount_never_refuses_a_session_from_the_bare_root(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE BARE ROOT IS THE INTENDED LAUNCH PATTERN, not corruption (operator ruling 577988ed,
    correcting mount-guard #6's original refusal — which would have refused a genuinely FRESH,
    legitimate first launch exactly as readily as the pollution case: `bound is None` is true
    for both). A session mounting from here — bound or not — must never be refused; an
    unseated one honestly reports project=None (nothing invented from the basename) rather
    than the phantom "seats" the old naive fallback would have minted."""
    from src import mcp_server as srv

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    # offices._DEFAULT_OFFICE_ROOT, not agents.py's own imported name: resolve_identity now
    # calls the shared is_bare_office_root() (offices.py) instead of duplicating the same
    # path-equality check (the 38c71544 dedup, ruling 719ed5b1) — patch the module that owns
    # the comparison.
    monkeypatch.setattr("src.orchestrator.offices._DEFAULT_OFFICE_ROOT", fake_root)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        # a genuinely FRESH job_dir — bound is None, the exact shape the old guard refused
        fresh = await srv.mount(cwd=str(fake_root),
                                job_dir=str(tmp_path / "jobs" / "cccc3333"))
        # an ALREADY-bound session, the historical shape — was always safe, stays safe
        job_dir = str(tmp_path / "jobs" / "dddd4444")
        await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:dddd4444",
                                project=None, cwd=str(fake_root), model=None,
                                session_key="k")
        bound = await srv.mount(cwd=str(fake_root), job_dir=job_dir)
    finally:
        srv._pool = saved_pool
    assert fresh.get("error") is None, f"a fresh session must never be refused: {fresh}"
    assert fresh["project"] == "?", "unseated: honestly unresolved, nothing invented"
    assert bound.get("error") is None
    assert bound["agent"] == "agent:dddd4444"
    # no phantom 'seats'-project Agent ever got minted
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE name='project' "
        "AND value #>> '{}' = 'seats'") == 0


async def test_mount_resolves_project_from_the_seat_not_cwd_when_seated(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEAT-FIRST RESOLUTION (operator ruling 577988ed): a SEATED session's project is its
    SEAT's own derived house — UNCONDITIONALLY, not merely a fallback for when cwd comes up
    empty. Proven here against the strongest case: the agent's OWN project stamp says
    'seats' (exactly what a transient bad mount pollutes — Thoth's own shape), a REAL seat
    binds it to house 'osiris', and cwd is the structurally-empty bare root. The seat wins
    over both — never house_of(agent_id), which is exactly the stamp that gets polluted."""
    from src import mcp_server as srv
    from src.orchestrator.seats import bind_holder, ensure_seat

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.agents._DEFAULT_OFFICE_ROOT", fake_root)

    agent = await actions.create_or_find_object("Agent", "agent:ffff7777", "test")
    await actions.assert_property(agent, "project", "seats", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    seat = await ensure_seat(actions, house="osiris", handle="Seatfirst", source="test")
    assert seat.get("error") is None
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:ffff7777",
                      source="test")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(fake_root),
                              job_dir=str(tmp_path / "jobs" / "ffff7777"))
    finally:
        srv._pool = saved_pool
    assert out.get("error") is None
    assert out["project"] == "osiris", f"must read the SEAT's house, not the polluted stamp: {out}"


async def test_mount_resolves_an_anchored_seat_s_own_house_not_its_manager_s(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE MOUNT INVARIANT (ruling b4208fa3, thread 105f3425/bec2e4af — halcyon's own
    incident): a seat mounting into a project it does not belong to is a mail-boundary
    breach, not a display bug — halcyon's fresh body mounted project='osiris' and leased
    50 of Thoth's own messages because derive_house walked PAST an operator-adopted,
    cross-house seat to its osiris manager. Leg 2's own root-cause: mount()'s seat-first
    resolution (577988ed, proven above) is CORRECT BY DESIGN and needs no separate fix —
    the defect was entirely inside derive_house() (Leg 1). This proves the invariant holds
    end-to-end through the REAL mount() call, not just derive_house() in isolation: an
    operator-adopted seat, managed by an osiris-house seat, still mounts into its OWN
    house."""
    from src import mcp_server as srv
    from src.orchestrator.seats import bind_holder, ensure_seat

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setattr("src.orchestrator.agents._DEFAULT_OFFICE_ROOT", fake_root)

    manager = await ensure_seat(actions, house="osiris", handle="Manager1", source="test")
    worker = await ensure_seat(actions, house="hector-vector", handle="Worker1",
                               source="test")
    manager_oid = await actions.create_or_find_object("Seat", manager["seat_id"], "test")
    worker_oid = await actions.create_or_find_object("Seat", worker["seat_id"], "test")
    # the adoption itself: an operator-sourced managed_by edge crossing the house boundary
    await actions.create_link(worker_oid, manager_oid, "managed_by", "operator",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")
    await bind_holder(actions, seat_id=worker["seat_id"], agent_id="agent:hv000001",
                      source="test")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(fake_root),
                              job_dir=str(tmp_path / "jobs" / "hv000001"))
    finally:
        srv._pool = saved_pool
    assert out.get("error") is None
    assert out["project"] == "hector-vector", (
        f"the adopted seat must mount into its OWN house, never its manager's: {out}")


# ───────────────────────────── the door sweep (operator ruling, 2026-07-17) ──────────────


async def test_sweep_stale_doors_keeps_one_last_known_address(actions: Actions) -> None:
    """THE PILE RULE: 23-door seats were leaked rows from kills that skipped SessionEnd.
    Stale doors collapse to the freshest one per ACTIVE agent — the last-known address."""
    p = actions.pool
    await actions.create_or_find_object("Agent", "agent:deadbee1", "agent:deadbee1")
    for jd, mins in (("/x/jobs/deadbee1", 20.0), ("/x/jobs/deadbee2", 60.0),
                     ("/x/jobs/deadbee3", 120.0)):
        await mounts.save_mount(p, job_dir=jd, agent_id="agent:deadbee1", project="pile",
                                cwd="/repo/pile", model=None, session_key=None)
        await p.execute(
            "UPDATE agent_mounts SET last_seen = now() - make_interval(mins => $2) "
            "WHERE job_dir=$1", jd, mins)
    released = await mounts.sweep_stale_doors(actions, actor="test")
    assert released == 2
    left = [r["job_dir"] for r in await p.fetch("SELECT job_dir FROM agent_mounts")]
    assert left == ["/x/jobs/deadbee1"]  # the freshest stale door survives as the address


async def test_sweep_stale_doors_keeps_nothing_for_strangers_or_the_shadowed(
    actions: Actions,
) -> None:
    """An objectless stranger holds no address at all; an agent with a FRESH door needs no
    stale one kept — the fresh row already IS its address."""
    p = actions.pool
    # the stranger: no object behind the id
    await mounts.save_mount(p, job_dir="/x/jobs/aaaa0001", agent_id="agent:aaaa0001",
                            project="x", cwd="/r/x", model=None,
                            session_key="whisper:aaaa0001")
    # the seated agent: one fresh door, one stale
    await actions.create_or_find_object("Agent", "agent:bbbb0001", "agent:bbbb0001")
    for jd in ("/x/jobs/bbbb0001", "/x/jobs/bbbb0002"):
        await mounts.save_mount(p, job_dir=jd, agent_id="agent:bbbb0001", project="y",
                                cwd="/r/y", model=None, session_key=None)
    await p.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE job_dir IN ('/x/jobs/aaaa0001', '/x/jobs/bbbb0002')")
    released = await mounts.sweep_stale_doors(actions, actor="test")
    assert released == 2
    left = {r["job_dir"] for r in await p.fetch("SELECT job_dir FROM agent_mounts")}
    assert left == {"/x/jobs/bbbb0001"}


async def test_sweep_ghost_doors_releases_the_killed_tab_never_a_backed_door(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE GHOST RULE: a fresh row with no body at its cwd AND none in its project is a
    killed tab's leak — released now. Either witness alone (the exact cwd, or the project
    label anywhere) keeps the door: the double gate protects living sessions."""
    p = actions.pool
    alive = tmp_path / "alive"
    alive.mkdir()
    office = tmp_path / "office"
    office.mkdir()
    dead = tmp_path / "dead"
    dead.mkdir()
    for jd, cwd, proj in (("/x/jobs/cafe0001", str(alive), "alive"),
                          ("/x/jobs/cafe0002", str(office), "alive"),
                          ("/x/jobs/cafe0003", str(dead), "dead")):
        await mounts.save_mount(p, job_dir=jd, agent_id="agent:cafe9999", project=proj,
                                cwd=cwd, model=None, session_key=None)
    await p.execute(  # inside the window, past the newborn grace
        "UPDATE agent_mounts SET last_seen = now() - interval '5 minutes'")
    released = await mounts.sweep_ghost_doors(
        actions, body_cwds={str(alive.resolve())}, body_projects={"alive"}, actor="test")
    assert released == 1
    left = {r["job_dir"] for r in await p.fetch("SELECT job_dir FROM agent_mounts")}
    assert left == {"/x/jobs/cafe0001", "/x/jobs/cafe0002"}


async def test_sweep_ghost_doors_grace_shields_the_newborn(actions: Actions) -> None:
    """A row pulsed seconds ago is too new to judge — a session born after the /proc scan
    must never be read as bodyless (its whisper wrote the row; the census predates it)."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/feed0001", agent_id="agent:feed0001",
                            project="new", cwd="/r/new", model=None, session_key=None)
    released = await mounts.sweep_ghost_doors(
        actions, body_cwds=set(), body_projects=set(), actor="test")
    assert released == 0
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 1


async def test_sweep_stale_doors_leaves_an_undoable_witness(actions: Actions) -> None:
    """Thread 45dd4f3c (Thoth DM 2835): the pile rule used to be a bare bulk DELETE — no
    audit_log, no way to reconstruct a wrongly-swept row. Same bar as
    test_drop_then_undrop_round_trips_the_exact_row: snapshot before delete, restore after,
    literal equality."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/stalewit1", agent_id="agent:stalewit1",
                            project="pile", cwd="/repo/pile", model=None, session_key=None)
    await p.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE job_dir='/x/jobs/stalewit1'")
    original = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/stalewit1'")
    original_snapshot = mounts._mount_snapshot(original)

    released = await mounts.sweep_stale_doors(actions, actor="cron:test")
    assert released == 1
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 0

    wit = await p.fetchrow(
        "SELECT id, action, actor, payload FROM audit_log "
        "WHERE action='sweep_stale_doors' ORDER BY id DESC LIMIT 1")
    assert wit is not None
    assert wit["actor"] == "cron:test"
    assert wit["payload"] == original_snapshot

    restore = await mounts.undrop_dead_project_mount(
        actions, audit_id=wit["id"], actor="test")
    assert restore["restored"] == 1
    restored_row = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/stalewit1'")
    assert restored_row is not None
    assert mounts._mount_snapshot(restored_row) == original_snapshot


async def test_sweep_ghost_doors_leaves_an_undoable_witness(
    actions: Actions, tmp_path: Path,
) -> None:
    """Same bar as the stale-door sweep above, for the ghost rule."""
    p = actions.pool
    dead = tmp_path / "dead"
    dead.mkdir()
    await mounts.save_mount(p, job_dir="/x/jobs/ghostwit1", agent_id="agent:ghostwit1",
                            project="dead", cwd=str(dead), model=None, session_key=None)
    await p.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '5 minutes' "
        "WHERE job_dir='/x/jobs/ghostwit1'")
    original = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/ghostwit1'")
    original_snapshot = mounts._mount_snapshot(original)

    released = await mounts.sweep_ghost_doors(
        actions, body_cwds=set(), body_projects=set(), actor="cron:test")
    assert released == 1
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 0

    wit = await p.fetchrow(
        "SELECT id, action, actor, payload FROM audit_log "
        "WHERE action='sweep_ghost_doors' ORDER BY id DESC LIMIT 1")
    assert wit is not None
    assert wit["actor"] == "cron:test"
    assert wit["payload"] == original_snapshot

    restore = await mounts.undrop_dead_project_mount(
        actions, audit_id=wit["id"], actor="test")
    assert restore["restored"] == 1
    restored_row = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/ghostwit1'")
    assert restored_row is not None
    assert mounts._mount_snapshot(restored_row) == original_snapshot


async def test_sweep_stale_doors_writes_no_witness_when_nothing_is_doomed(
    actions: Actions,
) -> None:
    """A no-op sweep must leave audit_log untouched — a witness for a delete that didn't
    happen would itself be a false record (the same law drop_dead_project_mount's mismatch
    refusal already proves)."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/freshdoor1", agent_id="agent:freshdoor1",
                            project="live", cwd="/repo/live", model=None, session_key=None)
    released = await mounts.sweep_stale_doors(actions, actor="cron:test")
    assert released == 0
    assert await p.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='sweep_stale_doors'") == 0


async def test_drop_dead_project_mount_releases_the_matched_row(actions: Actions) -> None:
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/deadstub1", agent_id="agent:deadstub1",
                            project="deadstub", cwd="/tmp/deadstub", model=None,
                            session_key="whisper:deadstub1")
    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/deadstub1", project="deadstub", actor="agent:test")
    assert out["dropped"] == 1 and out["audit_id"] is not None
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE job_dir='/x/jobs/deadstub1'") == 0


async def test_drop_dead_project_mount_never_touches_a_sibling_row(actions: Actions) -> None:
    """Row-scoped by job_dir, never agent-id-wide (release_mounts' own lesson) — a second,
    LIVE row for the same agent under a different project must survive untouched."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/split0001", agent_id="agent:split0001",
                            project="deadstub", cwd="/tmp/deadstub", model=None,
                            session_key="whisper:split0001")
    await mounts.save_mount(p, job_dir="/x/jobs/split0002", agent_id="agent:split0001",
                            project="livehouse", cwd="/repo/live", model=None,
                            session_key="whisper:split0002")
    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/split0001", project="deadstub", actor="agent:test")
    assert out["dropped"] == 1
    left = {r["job_dir"] for r in await p.fetch("SELECT job_dir FROM agent_mounts")}
    assert left == {"/x/jobs/split0002"}


async def test_drop_dead_project_mount_refuses_a_project_mismatch(actions: Actions) -> None:
    """The WHERE re-checks project at delete time — a row that moved to a different
    (live) project between a sweep's report and this call is left untouched, never
    dropped on a stale belief about what it held."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/moved0001", agent_id="agent:moved0001",
                            project="livehouse", cwd="/repo/live", model=None,
                            session_key="whisper:moved0001")
    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/moved0001", project="deadstub", actor="agent:test")
    assert out == {"dropped": 0, "audit_id": None}
    assert await p.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE job_dir='/x/jobs/moved0001'") == 1
    # a false-match drop writes NOTHING — a witness for a delete that didn't happen would
    # itself be a false record (Thoth DM 2677)
    assert await p.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='drop_dead_project_mount'") == 0


async def test_drop_dead_project_mount_mints_no_agent_object(actions: Actions) -> None:
    """THE INVARIANT reconcile_execute's own test enforces (test_fleet_reconcile.py: "a
    drop releases the RESIDUE ROW only — no Agent object was ever minted here") — the
    reversibility fix must not violate it. This is exactly why the witness lives in
    audit_log (no object_id) and not object_events (object_id NOT NULL)."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/noagent1", agent_id="agent:noagent1",
                            project="deadstub", cwd="/tmp/deadstub", model=None,
                            session_key="whisper:noagent1")
    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/noagent1", project="deadstub", actor="agent:test")
    assert out["dropped"] == 1
    st = await p.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:noagent1'")
    assert st is None


async def test_drop_then_undrop_round_trips_the_exact_row(actions: Actions) -> None:
    """THE BAR (Thoth DM 2677): drop a row, restore it, read it back, show the restored
    row is equivalent to the original — and show the drop itself left a findable witness.
    Populates every column (including the ones save_mount doesn't set) so the proof
    exercises the FULL snapshot round-trip, not just the fields that happen to be set."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/revive001", agent_id="agent:revive001",
                            project="deadstub", cwd="/tmp/deadstub", model="claude-fable-5",
                            session_key="whisper:revive001")
    await p.execute(
        "UPDATE agent_mounts SET model_raw=$1, context_window_size=$2, seat_id=$3 "
        "WHERE job_dir=$4",
        "claude-fable-5[1m]", 1_000_000, "seat:revive0001", "/x/jobs/revive001")
    original = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/revive001'")
    original_snapshot = mounts._mount_snapshot(original)

    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/revive001", project="deadstub", actor="agent:test")
    assert out["dropped"] == 1
    audit_id = out["audit_id"]
    assert audit_id is not None
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 0

    # THE DROP LEFT A FINDABLE WITNESS
    wit = await p.fetchrow(
        "SELECT action, actor, payload FROM audit_log WHERE id=$1", audit_id)
    assert wit is not None
    assert wit["action"] == "drop_dead_project_mount"
    assert wit["actor"] == "agent:test"
    assert wit["payload"] == original_snapshot

    restore = await mounts.undrop_dead_project_mount(
        actions, audit_id=audit_id, actor="agent:test")
    assert restore == {
        "restored": 1, "job_dir": "/x/jobs/revive001",
        "undrop_audit_id": restore["undrop_audit_id"],
    }
    restored_row = await p.fetchrow(
        f"SELECT {', '.join(mounts._MOUNT_COLS)} FROM agent_mounts "
        "WHERE job_dir='/x/jobs/revive001'")
    assert restored_row is not None
    # THE RESTORED ROW IS EQUIVALENT TO THE ORIGINAL — literal equality, not eyeballed
    assert mounts._mount_snapshot(restored_row) == original_snapshot
    # the undrop leaves its OWN witness too
    undo_wit = await p.fetchrow(
        "SELECT action, payload FROM audit_log WHERE id=$1", restore["undrop_audit_id"])
    assert undo_wit is not None and undo_wit["action"] == "undrop_dead_project_mount"
    assert undo_wit["payload"]["restored_from_audit_id"] == audit_id


async def test_undrop_dead_project_mount_refuses_an_unknown_audit_row(
    actions: Actions,
) -> None:
    out = await mounts.undrop_dead_project_mount(
        actions, audit_id=999_999_999, actor="agent:test")
    assert "error" in out
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_mounts") == 0


async def test_undrop_dead_project_mount_refuses_an_unrelated_audit_row(
    actions: Actions,
) -> None:
    """An undrop never invents a snapshot from an audit_log row some OTHER action wrote —
    action must be exactly 'drop_dead_project_mount'."""
    audit_id = await actions.pool.fetchval(
        "INSERT INTO audit_log (action, actor, payload) VALUES ($1,$2,$3) RETURNING id",
        "some_other_action", "agent:test", {"job_dir": "/x/jobs/decoy"})
    out = await mounts.undrop_dead_project_mount(
        actions, audit_id=audit_id, actor="agent:test")
    assert "error" in out
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_mounts") == 0


async def test_undrop_dead_project_mount_refuses_to_overwrite_a_live_remount(
    actions: Actions,
) -> None:
    """The job_dir was dropped, then genuinely re-mounted by a NEW session before anyone
    tried to undo the drop — the newer row is the truth; undrop must never fork identity
    by reviving the stale row underneath it."""
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/reuse0001", agent_id="agent:reuse0001",
                            project="deadstub", cwd="/tmp/deadstub", model=None,
                            session_key="whisper:reuse0001")
    out = await mounts.drop_dead_project_mount(
        actions, job_dir="/x/jobs/reuse0001", project="deadstub", actor="agent:test")
    audit_id = out["audit_id"]
    await mounts.save_mount(p, job_dir="/x/jobs/reuse0001", agent_id="agent:brandnew",
                            project="livehouse", cwd="/repo/live", model=None,
                            session_key="whisper:brandnew")
    result = await mounts.undrop_dead_project_mount(
        actions, audit_id=audit_id, actor="agent:test")
    assert "error" in result
    live = await mounts.find_mount(p, job_dir="/x/jobs/reuse0001")
    assert live is not None and live.agent_id == "agent:brandnew"

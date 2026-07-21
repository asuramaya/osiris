"""MINT_SEAT — the org chart trickles (task #50, ruling cabc28f5). One act: ensure_seat +
office scaffold + intended_model + managed_by (the org chart's first real link type).
Idempotent two ways (fresh mint vs adopt-an-existing-seat); refuses loud on a Person
collision or an unauthorized house crossing.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.greatfold import fold_census
from src.orchestrator.mintseat import _resolve_seat_ref, mint_seat
from src.orchestrator.seats import ensure_seat

NOW = datetime(2026, 7, 20, tzinfo=UTC)


async def _seat(actions: Actions, handle: str, house: str, source: str = "test") -> str:
    r = await ensure_seat(actions, house=house, handle=handle, source=source)
    return str(r["seat_id"])


async def _linked(actions: Actions, worker_seat: str, manager_seat: str) -> bool:
    return bool(await actions.pool.fetchval(
        "SELECT 1 FROM links l "
        "JOIN objects f ON f.id=l.from_id AND f.canonical=$1 "
        "JOIN objects t ON t.id=l.to_id AND t.canonical=$2 "
        "WHERE l.type='managed_by' AND (l.valid_until IS NULL OR l.valid_until > now())",
        worker_seat, manager_seat))


# ═══════════ (a) A FRESH MINT ═══════════

async def test_a_fresh_mint_creates_seat_office_model_and_edge(
    actions: Actions, tmp_path: Path,
) -> None:
    manager = await _seat(actions, "Steward", "osiris")
    offices = tmp_path / "seats"

    out = await mint_seat(actions, manager="Steward", handle="Vajra",
                          office_root=offices, actor="agent:steward01")

    assert out["seat_minted"] is True
    assert out["handle"] == "Vajra" and out["house"] == "osiris"
    assert out["intended_model"] == "claude-sonnet-5"
    assert out["intended_model_stamped"] is True
    assert out["managed_by"] == "linked"
    assert out["manager_seat_id"] == manager

    office = offices / "vajra"
    assert office.is_dir()
    pin = (office / ".osiris").read_text()
    assert 'project = "osiris"' in pin and 'model = "claude-sonnet-5"' in pin
    orders = (office / "CLAUDE.md").read_text()
    assert "Vajra — seat office" in orders and "not yet seated" in orders
    assert "GRADE EVERY DM" in orders  # every minted worker is born knowing the convention
    charter = (office / "charter.md").read_text()
    assert "Vajra's charter" in charter and "OFFLOAD TARGET" in charter

    assert await _linked(actions, out["seat_id"], manager)


async def test_a_intended_model_is_configurable(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, "Steward", "osiris")
    offices = tmp_path / "seats"

    out = await mint_seat(actions, manager="Steward", handle="Vajra",
                          intended_model="claude-opus-4-8", office_root=offices)

    assert out["intended_model"] == "claude-opus-4-8"
    pin = (offices / "vajra" / ".osiris").read_text()
    assert 'model = "claude-opus-4-8"' in pin


# ═══════════ (b) IDEMPOTENT RE-MINT ═══════════

async def test_b_re_minting_adopts_never_twins(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, "Steward", "osiris")
    offices = tmp_path / "seats"

    first = await mint_seat(actions, manager="Steward", handle="Vajra", office_root=offices)
    (offices / "vajra" / "CLAUDE.md").write_text("MY OWN HAND-WRITTEN STANDING ORDERS.\n")

    again = await mint_seat(actions, manager="Steward", handle="Vajra", office_root=offices)

    assert again["seat_id"] == first["seat_id"]
    assert again["seat_minted"] is False
    assert "office" not in again  # never re-scaffolded on a re-mint
    assert again["intended_model_stamped"] is False
    assert again["managed_by"] == "already linked"
    # no twin: exactly one active Seat with this handle, one active managed_by edge
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active' "
        "AND canonical=$1", first["seat_id"]) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='managed_by'") == 1
    # the hand-edit survived — a re-mint never touches an existing office's files
    assert "MY OWN HAND-WRITTEN" in (offices / "vajra" / "CLAUDE.md").read_text()


# ═══════════ (c) TANTRA-SHAPED ADOPT — no new identity minted ═══════════

async def test_c_adopting_an_operator_minted_seat_mints_no_new_identity(
    actions: Actions, tmp_path: Path,
) -> None:
    """Tantra's real shape: a Seat + a named Agent, minted by the operator's own hand
    before mint_seat existed, no managed_by edge yet. mint_seat's first act of record for
    her is asserting managed_by → alfred's seat, nothing else."""
    alfred_seat = await _seat(actions, "Alfred", "bytebye", source="operator")
    tantra_seat = await _seat(actions, "Tantra", "sutrahouse", source="operator")
    agent = await actions.create_or_find_object("Agent", "agent:7a17a000", "operator")
    await actions.assert_property(agent, "handle", "Tantra", "operator", NOW, 0.9,
                                  evidence_class="self_declared")
    before = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")

    out = await mint_seat(actions, manager="Alfred", handle="Tantra",
                          office_root=tmp_path / "unused-seats", actor="operator")

    assert out["seat_id"] == tantra_seat
    assert out["seat_minted"] is False
    assert "office" not in out                      # her real office is untouched
    assert out["intended_model_stamped"] is True     # she had none — the missing piece
    assert out["managed_by"] == "linked"
    after = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")
    assert after == before                           # NO new Seat minted
    assert await _linked(actions, tantra_seat, alfred_seat)


# ═══════════ (d) OUR OWN PAIR — the same adopt path ═══════════

async def test_d_seshat_thoth_pair_gets_its_edge_the_same_way(
    actions: Actions, tmp_path: Path,
) -> None:
    """Seshat and Thoth both already exist (real, live seats) — managed_by is new only
    because the link type is new. Same adopt mechanism as Tantra's case, proving it's
    general, not special-cased for one pair."""
    thoth_seat = await _seat(actions, "Thoth", "osiris", source="operator")
    seshat_seat = await _seat(actions, "Seshat", "osiris", source="operator")

    out = await mint_seat(actions, manager="Thoth", handle="Seshat",
                          office_root=tmp_path / "unused-seats", actor="agent:thoth01")

    assert out["seat_id"] == seshat_seat
    assert out["seat_minted"] is False
    assert out["managed_by"] == "linked"
    assert await _linked(actions, seshat_seat, thoth_seat)

    # idempotent from here too
    again = await mint_seat(actions, manager="Thoth", handle="Seshat",
                            office_root=tmp_path / "unused-seats", actor="agent:thoth01")
    assert again["managed_by"] == "already linked"
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='managed_by'") == 1


# ═══════════ (e) CENSUS COUNTS A MINTED WORKER ═══════════

async def test_e_census_counts_a_fresh_mint_and_keeps_counting_an_adopted_soul(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seat(actions, "Steward", "osiris")
    before = await fold_census(actions.pool)

    # a brand new seat is census-visible from birth (seat_objects), never a doorbell
    await mint_seat(actions, manager="Steward", handle="Vajra", office_root=tmp_path / "seats")
    mid = await fold_census(actions.pool)
    assert mid["seat_objects"] == before["seat_objects"] + 1

    # an ADOPTED worker that's already a named soul (an Agent with a handle) keeps
    # counting as one — mint_seat's edges/stamps never disturb the census
    tantra_seat = await _seat(actions, "Tantra", "sutrahouse", source="operator")
    agent = await actions.create_or_find_object("Agent", "agent:7a17a000", "operator")
    await actions.assert_property(agent, "handle", "Tantra", "operator", NOW, 0.9,
                                  evidence_class="self_declared")
    pre_adopt = await fold_census(actions.pool)
    assert pre_adopt["souls_named"] >= 1
    out = await mint_seat(actions, manager="Steward", handle="Tantra",
                          office_root=tmp_path / "seats")
    post_adopt = await fold_census(actions.pool)
    assert post_adopt["souls_named"] == pre_adopt["souls_named"]
    assert out["seat_id"] == tantra_seat and out["seat_minted"] is False


# ═══════════ (f) THE REFUSALS ═══════════

async def test_f_refuses_a_person_collision_on_the_worker_handle(actions: Actions) -> None:
    await _seat(actions, "Steward", "osiris")
    p = await actions.create_or_find_object("Person", "person:ghost1", "case-work")
    await actions.assert_property(p, "name", "Ghost Handle", "case-work", NOW, 0.9,
                                  evidence_class="self_declared")
    out = await mint_seat(actions, manager="Steward", handle="Ghost Handle")
    assert "error" in out and "Person record" in out["error"]


async def test_f_refuses_a_person_collision_on_the_manager_reference(actions: Actions) -> None:
    p = await actions.create_or_find_object("Person", "person:ghost2", "case-work")
    await actions.assert_property(p, "name", "Ghost Manager", "case-work", NOW, 0.9,
                                  evidence_class="self_declared")
    out = await mint_seat(actions, manager="Ghost Manager", handle="NewWorker")
    assert "error" in out and "Person record" in out["error"]


async def test_f_refuses_an_unknown_manager(actions: Actions) -> None:
    out = await mint_seat(actions, manager="NoSuchSeat", handle="NewWorker")
    assert "error" in out and "no such manager seat" in out["error"]


async def test_f_refuses_an_unauthorized_house_crossing(actions: Actions) -> None:
    await _seat(actions, "Steward", "housea")
    out = await mint_seat(actions, manager="Steward", handle="Reaches",
                          house="houseb", actor="agent:steward01")
    assert "error" in out and "cross-house mint refused" in out["error"]
    assert await _resolve_seat_ref(actions.pool, "Reaches") is None  # nothing was minted


async def test_f_the_operator_may_cross_a_house_boundary(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seat(actions, "Steward", "housea")
    out = await mint_seat(actions, manager="Steward", handle="Reaches", house="houseb",
                          actor="operator", office_root=tmp_path / "seats")
    assert "error" not in out
    assert out["house"] == "houseb" and out["seat_minted"] is True


# ═══════════ THE MCP TOOL LAYER — the calling seat is always the manager ═══════════
# test_wall.py's _Ctx ritual is the precedent: fake a mounted connection by injecting an
# AgentIdentity into srv._agents keyed by srv._conn_key(ctx), point srv._pool at the test
# DB, call the tool FUNCTION directly (never the MCP transport).

class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_mcp_mint_seat_refuses_an_unmounted_caller(actions: Actions) -> None:
    import src.mcp_server as srv

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mint_seat(handle="Vajra", ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" in out and "mount first" in out["error"]


async def test_mcp_mint_seat_refuses_a_caller_holding_no_seat(actions: Actions) -> None:
    import src.mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:un5eated0", session="unseated0", project="osiris",
        model=None, cwd=None)
    try:
        out = await srv.mint_seat(handle="Vajra", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out and "you hold no seat" in out["error"]


async def test_mcp_mint_seat_the_caller_is_always_the_manager(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The happy path through the tool layer: a mounted caller who holds 'Thoth' mints
    'Seshat' with NO manager param at all — the tool resolves the manager from the
    connection's own identity, exactly the semantics Thoth's amendment asked for."""
    import src.mcp_server as srv
    from src.orchestrator import mintseat
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    thoth_seat = await _seat(actions, "Thoth", "osiris", source="operator")
    await bind_holder(actions, seat_id=thoth_seat, agent_id="agent:th0th0001",
                      source="operator")
    # office_root is not a tool param — point the shared default at a scratch dir for
    # the duration of this one test
    monkeypatch.setattr(mintseat, "_DEFAULT_OFFICE_ROOT", tmp_path / "seats")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th0th0001", session="thoth0001", project="osiris",
        model=None, cwd=None)
    try:
        out = await srv.mint_seat(handle="Seshat", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" not in out
    assert out["manager_seat_id"] == thoth_seat
    assert out["handle"] == "Seshat" and out["house"] == "osiris"
    assert out["seat_minted"] is True
    assert await _linked(actions, out["seat_id"], thoth_seat)

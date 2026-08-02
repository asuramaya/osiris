"""MINT_SEAT — the org chart trickles (task #50, ruling cabc28f5). One act: ensure_seat +
office scaffold + intended_model + managed_by (the org chart's first real link type).
Idempotent two ways (fresh mint vs adopt-an-existing-seat); refuses loud on a Person
collision or an unauthorized house crossing.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.greatfold import fold_census
from src.orchestrator.mintseat import _resolve_seat_ref, mint_seat
from src.orchestrator.seats import ensure_seat, seat_facts

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
    grant = json.loads((office / ".claude" / "settings.local.json").read_text())
    assert grant == {"permissions": {"allow": ["mcp__osiris", "mcp__osiris__*"]}}

    assert await _linked(actions, out["seat_id"], manager)


async def test_a_intended_model_is_configurable(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, "Steward", "osiris")
    offices = tmp_path / "seats"

    out = await mint_seat(actions, manager="Steward", handle="Vajra",
                          intended_model="claude-opus-4-8", office_root=offices)

    assert out["intended_model"] == "claude-opus-4-8"
    pin = (offices / "vajra" / ".osiris").read_text()
    assert 'model = "claude-opus-4-8"' in pin


async def test_a_fresh_mint_stamps_anchor_cwd_to_its_own_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """task #68: a fresh mint used to scaffold an office on disk but never told the Seat
    object where it lived — launch() (which reads anchor_cwd) refused every never-launched
    seat with 'no anchor_cwd — establish_office first', a circular ask for a room that
    already existed. anchor_cwd must land in the SAME act as the mint, at the exact path
    the office scaffold uses."""
    await _seat(actions, "Steward", "osiris")

    out = await mint_seat(actions, manager="Steward", handle="Vajra",
                          office_root=tmp_path / "seats", actor="agent:steward01")

    facts = await seat_facts(actions.pool, out["seat_id"])
    assert facts["anchor_cwd"] == str(tmp_path / "seats" / "vajra")


async def test_adopted_hollow_seat_backfills_anchor_cwd(
    actions: Actions, tmp_path: Path,
) -> None:
    """The adopt path (Tantra's shape) never calls ensure_seat, so a pre-existing seat
    minted before this fix (or hand-made by the operator) can carry no anchor_cwd at all —
    fill-missing-only backfills it here, the same law intended_model already gets."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "Tantra", "sutrahouse", source="operator")
    offices = tmp_path / "seats"

    out = await mint_seat(actions, manager="Alfred", handle="Tantra", office_root=offices,
                          actor="operator")

    facts = await seat_facts(actions.pool, out["seat_id"])
    assert facts["anchor_cwd"] == str(offices / "tantra")


# ═══════════ (b) IDEMPOTENT RE-MINT ═══════════

async def test_b_re_minting_adopts_never_twins(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, "Steward", "osiris")
    offices = tmp_path / "seats"

    first = await mint_seat(actions, manager="Steward", handle="Vajra", office_root=offices)
    (offices / "vajra" / "CLAUDE.md").write_text("MY OWN HAND-WRITTEN STANDING ORDERS.\n")

    again = await mint_seat(actions, manager="Steward", handle="Vajra", office_root=offices)

    assert again["seat_id"] == first["seat_id"]
    assert again["seat_minted"] is False
    # the fill-missing scaffold runs again (ruling 7cffda8f) but finds nothing missing —
    # every file this second call sees was already there, so every state reads unchanged
    assert again["office"]["osiris_pin"] == "left in place"
    assert again["office"]["standing_orders"] == "left in place"
    assert again["office"]["charter_file"] == "left in place"
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
    her is asserting managed_by → alfred's seat and filling her hollow office — no new
    IDENTITY, which is this test's core claim (the office-fill mechanics get their own
    dedicated tests below)."""
    alfred_seat = await _seat(actions, "Alfred", "bytebye", source="operator")
    tantra_seat = await _seat(actions, "Tantra", "sutrahouse", source="operator")
    agent = await actions.create_or_find_object("Agent", "agent:7a17a000", "operator")
    await actions.assert_property(agent, "handle", "Tantra", "operator", NOW, 0.9,
                                  evidence_class="self_declared")
    before = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")

    out = await mint_seat(actions, manager="Alfred", handle="Tantra",
                          office_root=tmp_path / "seats", actor="operator")

    assert out["seat_id"] == tantra_seat
    assert out["seat_minted"] is False
    assert out["intended_model_stamped"] is True     # she had none — the missing piece
    assert out["managed_by"] == "linked"
    after = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'")
    assert after == before                           # NO new Seat minted
    assert await _linked(actions, tantra_seat, alfred_seat)


# ═══════════ HOLLOW VS POPULATED ADOPTION (ruling 7cffda8f) ═══════════

async def test_hollow_adoption_fills_the_empty_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """Tantra's exact repro: an operator-made dir with nothing in it. Adoption must not
    leave a seat hollow when there is nothing to clobber — all three files get written."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "Tantra", "sutrahouse", source="operator")
    offices = tmp_path / "seats"
    (offices / "tantra").mkdir(parents=True)  # the operator's bare mkdir, nothing inside

    out = await mint_seat(actions, manager="Alfred", handle="Tantra", office_root=offices,
                          actor="operator")

    assert out["office"]["osiris_pin"] == "written"
    assert out["office"]["standing_orders"] == "written"
    assert out["office"]["charter_file"] == "written"
    assert out["office"]["permission_grant"] == "written"
    office = offices / "tantra"
    assert 'project = "sutrahouse"' in (office / ".osiris").read_text()
    assert "Tantra — seat office" in (office / "CLAUDE.md").read_text()
    assert "Tantra's charter" in (office / "charter.md").read_text()
    grant = json.loads((office / ".claude" / "settings.local.json").read_text())
    assert grant == {"permissions": {"allow": ["mcp__osiris", "mcp__osiris__*"]}}


async def test_adoption_never_touches_a_populated_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """The never-clobber law, proven under adoption too: a seat that already has all
    three files keeps every byte of them — fill-missing-only means exactly that."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "Tantra", "sutrahouse", source="operator")
    offices = tmp_path / "seats"
    office = offices / "tantra"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "sutrahouse"\nmodel = "claude-opus-4-8"\n')
    (office / "CLAUDE.md").write_text("HER OWN HAND-TUNED ORDERS.\n")
    (office / "charter.md").write_text("HER OWN HAND-WRITTEN CHARTER.\n")
    (office / ".claude").mkdir()
    (office / ".claude" / "settings.local.json").write_text('{"permissions": {"allow": []}}')

    out = await mint_seat(actions, manager="Alfred", handle="Tantra", office_root=offices,
                          actor="operator")

    assert out["office"]["osiris_pin"] == "left in place"
    assert out["office"]["standing_orders"] == "left in place"
    assert out["office"]["charter_file"] == "left in place"
    assert out["office"]["permission_grant"] == "left in place"
    assert (office / ".osiris").read_text() == 'project = "sutrahouse"\nmodel = "claude-opus-4-8"\n'
    assert "HER OWN HAND-TUNED ORDERS" in (office / "CLAUDE.md").read_text()
    assert "HER OWN HAND-WRITTEN CHARTER" in (office / "charter.md").read_text()
    assert (office / ".claude" / "settings.local.json").read_text() == \
        '{"permissions": {"allow": []}}'


# ═══════════ THE NEAR-MISS GUARD (ruling 7cffda8f, Alfred's field pilot) ═══════════

async def test_near_miss_refuses_a_normalized_collision(
    actions: Actions, tmp_path: Path,
) -> None:
    """Tantra's real claimed handle is 'tantra 1' — a bare 'Tantra' fresh-mint request
    never exact-matches it and, before this fix, would have silently minted a twin."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "tantra 1", "sutrahouse", source="operator")

    out = await mint_seat(actions, manager="Alfred", handle="Tantra",
                          office_root=tmp_path / "seats")

    assert "error" in out
    assert "tantra 1" in out["error"] and "near" in out["error"].lower()
    # no twin: the near-miss refusal minted nothing
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'"
        ) == 2  # just Alfred + tantra 1, unchanged


async def test_near_miss_covers_generation_suffix_variants(
    actions: Actions, tmp_path: Path,
) -> None:
    """Space/dash/underscore before a roman numeral or plain digit all normalize away —
    none of these exact-match 'Vajra' (that's a plain case-insensitive adopt, tested
    separately below), so each must refuse as a near-miss."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "Vajra", "bytebye", source="operator")
    for variant in ("Vajra II", "vajra-2", "vajra_iv", "vajra 1"):
        out = await mint_seat(actions, manager="Alfred", handle=variant,
                              office_root=tmp_path / "seats")
        assert "error" in out, f"{variant!r} should have refused as a near-miss"


async def test_a_pure_case_variant_is_an_exact_match_not_a_near_miss(
    actions: Actions, tmp_path: Path,
) -> None:
    """'vajra'/'VAJRA' case-insensitively EXACT-match 'Vajra' via _resolve_seat_ref
    itself — that's a normal adopt, never even reaching the near-miss guard."""
    await _seat(actions, "Alfred", "bytebye", source="operator")
    vajra_seat = await _seat(actions, "Vajra", "bytebye", source="operator")
    out = await mint_seat(actions, manager="Alfred", handle="vajra",
                          office_root=tmp_path / "seats")
    assert "error" not in out
    assert out["seat_id"] == vajra_seat and out["seat_minted"] is False


async def test_force_mints_past_a_near_miss_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seat(actions, "Alfred", "bytebye", source="operator")
    await _seat(actions, "tantra 1", "sutrahouse", source="operator")

    out = await mint_seat(actions, manager="Alfred", handle="Tantra", force=True,
                          office_root=tmp_path / "seats")

    assert "error" not in out
    assert out["seat_minted"] is True and out["handle"] == "Tantra"


async def test_adopt_true_refuses_rather_than_falling_through_to_fresh(
    actions: Actions, tmp_path: Path,
) -> None:
    """The caller SAID adopt — minting on a miss would be the lie."""
    await _seat(actions, "Alfred", "bytebye", source="operator")

    out = await mint_seat(actions, manager="Alfred", handle="NobodyYet", adopt=True,
                          office_root=tmp_path / "seats")

    assert "error" in out and "adopt=True" in out["error"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Seat' AND status='active'") == 1  # Alfred only


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


async def test_mcp_mint_seat_resolves_a_succeeded_lineage_via_handle_fallback(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live acceptance (msg 926 — Thoth LI's own first call refused HIM): held_seat()
    needs a `holds` link on the caller's EXACT label, but a succeeded lineage's holds
    link can sit on an ancestor (mint_heir doesn't always re-link it at every mint — a
    separate, deeper gap, not fixed here). The handle ASSERTION, unlike the link, IS
    copied to every new generation (mint_heir's own seat-inheritance step) — the tool
    must fall back to it, the same way mount's own seat display already does."""
    import src.mcp_server as srv
    from src.orchestrator import mintseat
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder

    thoth_seat = await _seat(actions, "Thoth", "osiris", source="operator")
    # the hold sits on an ANCESTOR label — a Great-Fold-deep lineage's real shape
    await bind_holder(actions, seat_id=thoth_seat, agent_id="agent:th0th0001",
                      source="operator")
    # the CALLER is a SUCCEEDED generation carrying only the inherited handle assertion
    # (mint_heir's seat-inheritance copy), never its own fresh holds link
    heir = await actions.create_or_find_object("Agent", "agent:th0th0001-xiv", "operator")
    await actions.assert_property(heir, "handle", "Thoth", "operator", NOW, 0.9,
                                  evidence_class="self_declared")
    monkeypatch.setattr(mintseat, "_DEFAULT_OFFICE_ROOT", tmp_path / "seats")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th0th0001-xiv", session="thoth0014", project="osiris",
        model=None, cwd=None)
    try:
        out = await srv.mint_seat(handle="Seshat", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" not in out
    assert out["manager_seat_id"] == thoth_seat
    assert await _linked(actions, out["seat_id"], thoth_seat)


# ═══════════ (g) THE RECEIPT COMPLETES THE LIFECYCLE — occupancy piece A, 9f566244 ═══════
# mint_seat used to finish HALF the ceremony: a seat, an office, a manager edge, and silence
# about whether a body exists yet. The receipt now states occupancy plainly (piece B's
# machinery) and names whose hand the next step needs.


async def test_g_fresh_mint_receipt_reads_vacant(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, "Steward", "osiris")

    out = await mint_seat(actions, manager="Steward", handle="Vajra",
                          office_root=tmp_path / "seats")

    assert out["occupancy"] == "vacant"
    assert out["holder"] is None
    assert "furniture" in out["next_step"]


async def test_g_adopting_a_live_seat_refuses(
    actions: Actions, tmp_path: Path,
) -> None:
    """Found live 2026-08-02 (decision 2993b4e4): the adopt branch used to write the same
    office+anchor_cwd effect establish_office's own live-seat guard exists to refuse — for
    ANY handle already resolving to a living Seat, including one whose session is running
    right now. It must now refuse, not merely report 'occupied' after the fact — the
    exact scenario this test used to exercise as a success is the bug."""
    from src.orchestrator import mounts
    from src.orchestrator.seats import bind_holder

    await _seat(actions, "Steward", "osiris")
    worker_seat = await _seat(actions, "Tantra", "osiris")
    await actions.create_or_find_object("Agent", "agent:tantra01", "test")
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:tantra01")
    await mounts.save_mount(actions.pool, job_dir="/j/tantra01", agent_id="agent:tantra01",
                            project="osiris", cwd="/x", model="claude-sonnet-5",
                            session_key=None)
    offices = tmp_path / "seats"

    out = await mint_seat(actions, manager="Steward", handle="Tantra", office_root=offices)

    assert "error" in out
    assert "cannot adopt" in out["error"] and "LIVE" in out["error"]
    assert "Tantra" in out["error"] and "agent:tantra01" in out["error"]
    # the vocabulary rule (Khnum's, this reign): this refusal must read differently from
    # mint_seat's OTHER refusals, never reuse "no such seat"
    assert "no such" not in out["error"]
    # nothing was written — the guard fires before any side effect, not after
    facts = await seat_facts(actions.pool, worker_seat)
    assert facts["anchor_cwd"] is None
    assert not (offices / "tantra").exists()
    assert not await _linked(actions, worker_seat, await _resolve_seat_ref(actions.pool, "Steward"))


async def test_g_adopting_a_held_but_quiet_seat_receipt_reads_cold(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import bind_holder

    await _seat(actions, "Steward", "osiris")
    worker_seat = await _seat(actions, "Tantra", "osiris")
    await actions.create_or_find_object("Agent", "agent:tantra02", "test")
    await bind_holder(actions, seat_id=worker_seat, agent_id="agent:tantra02")
    # no mount row at all — held, but nobody's pulse is fresh

    out = await mint_seat(actions, manager="Steward", handle="Tantra",
                          office_root=tmp_path / "seats")

    assert out["occupancy"] == "cold"
    assert out["holder"] == "agent:tantra02"
    assert "resumes on its own" in out["next_step"]

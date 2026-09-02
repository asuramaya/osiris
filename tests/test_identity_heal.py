"""Seat-identity self-healing (fe8ec7ff mechanism 3, operator ruling df646654: self-healing
over manual cleanup). #157's own diagnosed population — henry, alfred, redmonth, khepri
(decision 4fdd419e) — used as fixtures here: the exact shapes that used to need an
operator-authorized retire_assertion call per row now heal through one self-service call.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.identity_heal import (
    detect_possibly_stale_seats,
    heal_contradicting_property,
    heal_seat_anchor,
    heal_seat_anchor_third_party,
    reconcile_seat_identity,
    reconcile_seat_identity_third_party,
)
from src.parsers.base import EvidenceClass

_SD = EvidenceClass.SELF_DECLARED.value
NOW = datetime(2026, 8, 17, tzinfo=UTC)


async def _seat_with_house(
    actions: Actions, seat_id: str, *, founding: str, founding_source: str,
    founding_age_days: int, winning: str, winning_source: str,
):
    seat = await actions.create_or_find_object("Seat", seat_id, "test")
    await actions.assert_property(
        seat, "house", founding, founding_source, NOW - timedelta(days=founding_age_days),
        0.9, evidence_class=_SD)
    await actions.assert_property(
        seat, "house", winning, winning_source, NOW - timedelta(days=1), 0.9,
        evidence_class=_SD)
    return seat


# ═══ heal_contradicting_property — the mechanism itself ═══════════════════════════════

async def test_heal_does_nothing_on_zero_or_one_current_rows(actions: Actions) -> None:
    seat = await actions.create_or_find_object("Seat", "seat:hc1empty", "test")
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test")
    assert out == {"healed": False, "reason": "nothing to reconcile", "current": 0}

    await actions.assert_property(seat, "house", "solo", "test", NOW, 0.9,
                                  evidence_class=_SD)
    out2 = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test")
    assert out2 == {"healed": False, "reason": "nothing to reconcile", "current": 1}


async def test_heal_leaves_genuine_multi_source_corroboration_alone(actions: Actions) -> None:
    """Two DIFFERENT sources agreeing on the SAME value is corroboration, never a
    contradiction — #102's agreement marks, never touched here."""
    seat = await actions.create_or_find_object("Seat", "seat:hc2agree", "test")
    await actions.assert_property(seat, "house", "osiris", "agent:a", NOW - timedelta(days=2),
                                  0.9, evidence_class=_SD)
    await actions.assert_property(seat, "house", "osiris", "agent:b", NOW - timedelta(days=1),
                                  0.9, evidence_class=_SD)
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test")
    assert out == {"healed": False, "reason": "every current row already agrees",
                   "current": 2, "value": "osiris"}


async def test_heal_retires_a_genuinely_contradicting_older_row(actions: Actions) -> None:
    """The henry/alfred/redmonth shape (decision 4fdd419e): one founding value, one
    newer winning value from a different source, never retired before this."""
    seat = await _seat_with_house(
        actions, "seat:hc3henry", founding="henry", founding_source="console",
        founding_age_days=13, winning="shellbiz", winning_source="agent:miner")
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    assert out["healed"] is True
    assert out["winner"] == "shellbiz"
    assert len(out["superseded"]) == 1
    assert out["superseded"][0]["value"] == "henry"
    assert out["superseded"][0]["source"] == "console"
    # verify by re-query: every current row left AGREES (retire_assertion mints its own
    # new row restating the winner rather than deleting anything — event-sourced, never
    # DELETE — so the row COUNT does not collapse to one, but the VALUE set does)
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 AND name='house'",
        seat)
    assert {r["v"] for r in rows} == {"shellbiz"}


async def test_heal_is_idempotent_a_second_call_finds_nothing_left(actions: Actions) -> None:
    seat = await _seat_with_house(
        actions, "seat:hc4idem0", founding="bytebye", founding_source="agent:ad1a1cb0-g40",
        founding_age_days=33, winning="alfred", winning_source="agent:d5c671c1")
    first = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    assert first["healed"] is True
    second = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    assert second["healed"] is False
    # the original winner row and the self-heal's own restating row now both agree —
    # "already agrees", not "nothing to reconcile" (that's the 0-or-1-row case)
    assert second["reason"] == "every current row already agrees"


async def test_heal_the_khepri_shape_a_revert_that_never_retired_the_mistake(
    actions: Actions,
) -> None:
    """Khepri's own real specimen (decision 4fdd419e): THREE live rows — a correct founding
    value, a same-day mistake, and a same-day revert that asserted the correct value again
    but never actually retired the mistake row at the DB level. (The real specimen's mistake
    and revert shared one source; assert_property's own same-source supersession would
    already retire a same-source chain like that, so this fixture uses a distinct corrective
    source instead — the shape this mechanism actually exists to close is the CROSS-source
    residue, same as every other #157 row.) The founding value and the revert AGREE (both
    "tony") — only the mistake row ("cultural-infrastructure") is a genuine contradiction
    and must be the only one healed."""
    seat = await actions.create_or_find_object("Seat", "seat:hc5khepr", "test")
    await actions.assert_property(seat, "house", "tony", "agent:founder",
                                  NOW - timedelta(days=28), 0.9, evidence_class=_SD)
    await actions.assert_property(seat, "house", "cultural-infrastructure", "agent:mistake",
                                  NOW - timedelta(hours=5), 0.9, evidence_class=_SD)
    await actions.assert_property(seat, "house", "tony", "agent:revert",
                                  NOW - timedelta(hours=4), 0.9, evidence_class=_SD)
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    assert out["healed"] is True
    assert out["winner"] == "tony"
    assert len(out["superseded"]) == 1
    assert out["superseded"][0]["value"] == "cultural-infrastructure"
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 AND name='house'",
        seat)
    assert {r["v"] for r in rows} == {"tony"}  # all remaining rows agree


async def test_heal_is_reversible_via_the_retired_assertions_own_id(actions: Actions) -> None:
    """The ruling's own requirement: 'recorded as a reversible event with the loser's
    assertion id, so it unwinds' — the retired id is real and points at a real row."""
    seat = await _seat_with_house(
        actions, "seat:hc6revrs", founding="redmonth", founding_source="agent:x",
        founding_age_days=22, winning="ballgem", winning_source="agent:y")
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    lost_id = out["superseded"][0]["id"]
    row = await actions.pool.fetchrow(
        "SELECT supersedes, value #>> '{}' AS v FROM assertions WHERE id=$1", lost_id)
    assert row["v"] == "redmonth"
    # retire_assertion's own new row (minted by the heal, restating the winner) names
    # `lost_id` as its supersedes target — the unwind path a future un-heal would walk
    superseding_row = await actions.pool.fetchrow(
        "SELECT id, supersedes FROM assertions WHERE supersedes=$1", lost_id)
    assert superseding_row is not None
    assert superseding_row["supersedes"] == lost_id


async def test_heal_reports_healed_false_when_the_only_write_actually_failed(
    actions: Actions, monkeypatch,
) -> None:
    """RECEIPT HONESTY (khepri's own live specimen, #157's fourth row): current_assertions
    can list a row as current while a real `supersedes` FK already excludes it (an is_current
    backfill gap, a DIFFERENT and deeper defect than this mechanism repairs) — retire_
    assertion correctly REFUSES it ("already superseded"), and `healed` must read False over
    a batch where every attempted write actually failed, never True over a receipt that is
    all errors. A success-shaped response inviting a caller to skip the one field that says
    otherwise is exactly the class of bug correct_house's own `was` field exists to prevent."""
    seat = await _seat_with_house(
        actions, "seat:hc7reterr", founding="old", founding_source="agent:a",
        founding_age_days=3, winning="new", winning_source="agent:b")

    async def _always_refuses(actions, **kw):
        return {"error": "assertion is already superseded — nothing to retire"}

    monkeypatch.setattr(
        "src.orchestrator.identity_heal.retire_assertion", _always_refuses)
    out = await heal_contradicting_property(
        actions, object_id=seat, name="house", actor="test-heal")
    assert out["healed"] is False
    assert out["reason"] == "every contradicting row refused retirement"
    assert out["superseded"][0]["error"] is not None


async def test_heal_never_touches_a_non_identity_property(actions: Actions) -> None:
    """#102's agreement marks stay for everything that is not seat identity — this
    mechanism is scoped to 'house'/'project' ONLY, never generalised. Calling it with any
    other property name must not silently start healing that property too; the caller
    (reconcile_seat_identity) simply never invokes it for anything else, and a direct call
    on an unrelated property still runs the SAME mechanical rule — the scoping lives in
    WHO CALLS this, not in a property allowlist inside it, so this pins the boundary from
    the other direction: reconcile_seat_identity never touches a contradiction on a
    property outside its own two names."""
    seat = await actions.create_or_find_object("Seat", "seat:hc7noniden", "test")
    await actions.assert_property(seat, "intended_model", "claude-fable-5", "agent:a",
                                  NOW - timedelta(days=5), 0.9, evidence_class=_SD)
    await actions.assert_property(seat, "intended_model", "claude-sonnet-5", "agent:b",
                                  NOW - timedelta(days=1), 0.9, evidence_class=_SD)
    await actions.assert_property(seat, "house", "osiris", "test", NOW, 0.9,
                                  evidence_class=_SD)

    out = await reconcile_seat_identity(
        actions, seat_id="seat:hc7noniden", agent_id=None, actor="test-heal")
    assert out["healed"]["house"]["healed"] is False  # only one house row, nothing to do
    # the contradicting intended_model rows are UNTOUCHED — still two current rows
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 "
        "AND name='intended_model'", seat)
    assert len(rows) == 2


# ═══ detect_possibly_stale_seats — fold/rename's own detection-only nudge (Thoth dispatch
# 6484/6493, the dtfb specimen f5d5473b) ═══════════════════════════════════════════════

async def test_detect_possibly_stale_seats_finds_a_house_hit(actions: Actions) -> None:
    seat = await actions.create_or_find_object("Seat", "seat:dsdealer1", "test")
    await actions.assert_property(seat, "house", "dealer-to-fb", "test", NOW, 0.9,
                                  evidence_class=_SD)
    out = await detect_possibly_stale_seats(actions.pool, "dealer-to-fb")
    assert out["checked"] is True
    assert {"seat": "seat:dsdealer1", "field": "house", "value": "dealer-to-fb",
           "fix": "reconcile_seat_identity (self) or "
                  "reconcile_seat_identity_third_party (another seat)"} in out["hits"]


async def test_detect_possibly_stale_seats_finds_an_anchor_cwd_basename_hit(
    actions: Actions,
) -> None:
    seat = await actions.create_or_find_object("Seat", "seat:dsdealer2", "test")
    await actions.assert_property(seat, "anchor_cwd", "/home/asuramaya/code/dealer-to-fb",
                                  "test", NOW, 0.9, evidence_class=_SD)
    out = await detect_possibly_stale_seats(actions.pool, "dealer-to-fb")
    hit = next(h for h in out["hits"] if h["field"] == "anchor_cwd")
    assert hit["seat"] == "seat:dsdealer2"
    assert hit["value"] == "/home/asuramaya/code/dealer-to-fb"


async def test_detect_possibly_stale_seats_never_substring_matches_anchor_cwd(
    actions: Actions,
) -> None:
    """A raw substring match would flag a totally unrelated seat whose path merely
    CONTAINS the old name as a prefix segment — this checks the path's own BASENAME,
    never a substring, precisely to avoid that."""
    seat = await actions.create_or_find_object("Seat", "seat:dsdealer3", "test")
    await actions.assert_property(
        seat, "anchor_cwd", "/home/asuramaya/code/dealer-to-fb-unrelated-fork", "test",
        NOW, 0.9, evidence_class=_SD)
    out = await detect_possibly_stale_seats(actions.pool, "dealer-to-fb")
    assert not any(h["seat"] == "seat:dsdealer3" for h in out["hits"])


async def test_detect_possibly_stale_seats_reports_empty_old_name_without_raising(
    actions: Actions,
) -> None:
    out = await detect_possibly_stale_seats(actions.pool, "")
    assert out == {"checked": False, "reason": "empty old_name — nothing to match"}


async def test_detect_possibly_stale_seats_finds_nothing_for_an_unmatched_name(
    actions: Actions,
) -> None:
    out = await detect_possibly_stale_seats(actions.pool, "no-such-project-anywhere")
    assert out["checked"] is True
    assert out["hits"] == []
    assert out["truncated"] is False


async def test_detect_possibly_stale_seats_caps_and_flags_truncation(
    actions: Actions,
) -> None:
    for i in range(3):
        seat = await actions.create_or_find_object("Seat", f"seat:dscap{i}", "test")
        await actions.assert_property(seat, "house", "common", "test", NOW, 0.9,
                                      evidence_class=_SD)
    out = await detect_possibly_stale_seats(actions.pool, "common", cap=2)
    assert len(out["hits"]) == 2
    assert out["truncated"] is True


async def test_detect_possibly_stale_seats_the_note_always_names_the_pin_blind_spot(
    actions: Actions,
) -> None:
    out = await detect_possibly_stale_seats(actions.pool, "anything")
    assert ".osiris pin file is NOT checked" in out["note"]


# ═══ reconcile_seat_identity — the self-service verb ═══════════════════════════════════

async def test_reconcile_seat_identity_heals_house_and_project_in_one_call(
    actions: Actions,
) -> None:
    await _seat_with_house(
        actions, "seat:rs1both0", founding="stalehouse", founding_source="agent:old",
        founding_age_days=9, winning="newhouse", winning_source="agent:new")
    agent = await actions.create_or_find_object("Agent", "agent:rs1holder", "test")
    await actions.assert_property(agent, "project", "staleproject", "agent:old",
                                  NOW - timedelta(days=9), 0.9, evidence_class=_SD)
    await actions.assert_property(agent, "project", "newproject", "agent:new",
                                  NOW - timedelta(days=1), 0.9, evidence_class=_SD)

    out = await reconcile_seat_identity(
        actions, seat_id="seat:rs1both0", agent_id="agent:rs1holder", actor="test-heal")
    assert out["healed"]["house"]["healed"] is True
    assert out["healed"]["house"]["winner"] == "newhouse"
    assert out["healed"]["project"]["healed"] is True
    assert out["healed"]["project"]["winner"] == "newproject"


async def test_reconcile_seat_identity_without_an_agent_id_heals_house_alone(
    actions: Actions,
) -> None:
    await _seat_with_house(
        actions, "seat:rs2solo0", founding="old", founding_source="agent:a",
        founding_age_days=4, winning="new", winning_source="agent:b")
    out = await reconcile_seat_identity(
        actions, seat_id="seat:rs2solo0", agent_id=None, actor="test-heal")
    assert out["healed"]["house"]["healed"] is True
    assert "project" not in out["healed"]


async def test_reconcile_seat_identity_refuses_an_unknown_seat(actions: Actions) -> None:
    out = await reconcile_seat_identity(
        actions, seat_id="seat:rs3ghost", agent_id=None, actor="test-heal")
    assert "no active seat matches" in out["error"]


# ═══ write-time wiring (fe8ec7ff mechanism 3a) — correct_house/resync_seat_house_third_party
# already pinned in test_seats.py; this covers correct_agent_house's own cross-source case,
# which none of that file's existing fixtures exercise (they only overwrite the SAME source).

async def test_correct_agent_house_heals_a_cross_source_contradiction_at_write_time(
    actions: Actions,
) -> None:
    from src.orchestrator.agents import correct_agent_house, house_of

    a = await actions.create_or_find_object("Agent", "agent:cah7cross", "test")
    await actions.assert_property(a, "project", "seats", "some-other-agent",
                                  NOW - timedelta(days=6), 0.9, evidence_class=_SD)

    out = await correct_agent_house(actions, agent_id="agent:cah7cross", project="osiris",
                                    actor="agent:witness")
    assert out["corrected"] == {"project": "osiris"}
    assert await house_of(actions.pool, "agent:cah7cross") == "osiris"
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 "
        "AND name='project'", a)
    assert {r["v"] for r in rows} == {"osiris"}  # the stale cross-source row was healed


# ═══ the MCP tool wrapper — self-scoped, like correct_house ═══════════════════════════

async def test_the_mcp_tool_wrapper_resolves_the_callers_own_seat_and_agent(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity, claim_name

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    claimed = await claim_name(actions, "agent:mcp1self", "SelfHeal1", source="test")
    assert claimed.get("error") is None
    seat_obj = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_obj, "house", "old", "agent:other",
                                  NOW - timedelta(days=2), 0.9, evidence_class=_SD)
    await actions.assert_property(seat_obj, "house", "new", "agent:mcp1self",
                                  NOW - timedelta(days=1), 0.9, evidence_class=_SD)

    ident = AgentIdentity(agent_id="agent:mcp1self", session="mcp1", project="osiris",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        out = await srv.reconcile_seat_identity(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert out["seat_id"] == claimed["seat_id"]
    assert out["healed"]["house"]["healed"] is True
    assert out["healed"]["house"]["winner"] == "new"


async def test_the_mcp_tool_wrapper_refuses_an_unmounted_caller(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.reconcile_seat_identity(ctx=None)
    finally:
        srv._pool = saved_pool
    assert "mount first" in out["error"]


# ═══ reconcile_seat_identity_third_party — decision f78b41c8's own gap: #157's four rows
# belong to OTHER seats, unreachable by the self-service verb. Mirrors resync_seat_house_
# third_party's own precedent exactly (not self-scoped, `because` mandatory).

async def test_third_party_refuses_an_empty_because(actions: Actions) -> None:
    seat = await _seat_with_house(
        actions, "seat:tp1noreas", founding="old", founding_source="agent:a",
        founding_age_days=3, winning="new", winning_source="agent:b")
    out = await reconcile_seat_identity_third_party(
        actions, seat_id="seat:tp1noreas", agent_id=None, because="   ", actor="coordinator")
    assert "silent overwrite" in out["error"]
    # nothing touched — the contradiction survives untouched
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 AND name='house'",
        seat)
    assert {r["v"] for r in rows} == {"old", "new"}


async def test_third_party_heals_a_seat_that_is_not_the_caller(actions: Actions) -> None:
    """The whole point: the caller (a coordinator) is not the seat being healed."""
    await _seat_with_house(
        actions, "seat:tp2other", founding="stale", founding_source="agent:x",
        founding_age_days=7, winning="fresh", winning_source="agent:y")
    out = await reconcile_seat_identity_third_party(
        actions, seat_id="seat:tp2other", agent_id=None, because="#157 batch correction",
        actor="agent:coordinator")
    assert out["healed"]["house"]["healed"] is True
    assert out["healed"]["house"]["winner"] == "fresh"


async def test_third_party_reason_lands_in_the_retired_assertions_own_because_text(
    actions: Actions,
) -> None:
    await _seat_with_house(
        actions, "seat:tp3reasn", founding="old", founding_source="agent:a",
        founding_age_days=3, winning="new", winning_source="agent:b")
    out = await reconcile_seat_identity_third_party(
        actions, seat_id="seat:tp3reasn", agent_id=None,
        because="#157 batch correction, operator-authorized", actor="agent:coordinator")
    lost_id = out["healed"]["house"]["superseded"][0]["id"]
    row = await actions.pool.fetchrow("SELECT * FROM assertions WHERE id=$1", lost_id)
    assert row is not None  # sanity: the loser row itself still exists, event-sourced
    # `because` rides into the audit trail (supersede_assertion's own _audit call), not a
    # column on assertions — the reason text is there, attributed to the coordinator
    audit = await actions.pool.fetchrow(
        "SELECT payload, actor FROM audit_log WHERE action='supersede_assertion' "
        "AND payload->>'superseded_id' = $1 ORDER BY id DESC LIMIT 1", str(lost_id))
    assert "#157 batch correction, operator-authorized" in audit["payload"]["because"]
    assert audit["actor"] == "agent:coordinator"


async def test_third_party_refuses_an_unknown_seat(actions: Actions) -> None:
    out = await reconcile_seat_identity_third_party(
        actions, seat_id="seat:tp4ghost", agent_id=None, because="x", actor="agent:coordinator")
    assert "no active seat matches" in out["error"]


async def test_contract_self_service_and_third_party_produce_identical_graph_writes(
    actions: Actions,
) -> None:
    """The dispatch's own acceptance criteria: self-service and third-party heal the SAME
    row identically — same winner, same superseded set, same resulting current_assertions
    state — the only difference is the `because` text riding into the audit trail."""
    seat_a = await _seat_with_house(
        actions, "seat:cc1self0", founding="old", founding_source="agent:a",
        founding_age_days=5, winning="new", winning_source="agent:b")
    seat_b = await _seat_with_house(
        actions, "seat:cc2third", founding="old", founding_source="agent:a",
        founding_age_days=5, winning="new", winning_source="agent:b")

    self_out = await reconcile_seat_identity(
        actions, seat_id="seat:cc1self0", agent_id=None, actor="agent:self")
    third_out = await reconcile_seat_identity_third_party(
        actions, seat_id="seat:cc2third", agent_id=None, because="parity check",
        actor="agent:coordinator")

    assert self_out["healed"]["house"]["healed"] == third_out["healed"]["house"]["healed"]
    assert self_out["healed"]["house"]["winner"] == third_out["healed"]["house"]["winner"]
    assert (len(self_out["healed"]["house"]["superseded"])
           == len(third_out["healed"]["house"]["superseded"]))

    rows_a = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 AND name='house'",
        seat_a)
    rows_b = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 AND name='house'",
        seat_b)
    assert {r["v"] for r in rows_a} == {r["v"] for r in rows_b} == {"new"}


async def test_the_third_party_mcp_tool_wrapper_heals_any_named_seat(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await _seat_with_house(
        actions, "seat:tp5mcpwr", founding="old", founding_source="agent:a",
        founding_age_days=3, winning="new", winning_source="agent:b")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ident = AgentIdentity(agent_id="agent:coordmcp", session="coord01", project="osiris",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    try:
        out = await srv.reconcile_seat_identity_third_party(
            seat_id="seat:tp5mcpwr", because="#157 batch", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(key, None)
    assert out["seat_id"] == "seat:tp5mcpwr"
    assert out["healed"]["house"]["healed"] is True


async def test_the_third_party_mcp_tool_refuses_an_unmounted_caller(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.reconcile_seat_identity_third_party(
            seat_id="seat:whatever", because="x", ctx=None)
    finally:
        srv._pool = saved_pool
    assert "mount first" in out["error"]


# ═══ heal_seat_anchor — THE ANCHOR INVARIANT (ruling 23771416) ════════════════════════

async def _seat_with_handle_and_anchors(
    actions: Actions, seat_id: str, handle: str, *anchors: tuple[str, str, datetime],
):
    """Each anchors tuple is (value, source_id, observed_at)."""
    seat = await actions.create_or_find_object("Seat", seat_id, "test")
    await actions.assert_property(seat, "handle", handle, "test", NOW - timedelta(days=30),
                                  0.9, evidence_class=_SD)
    for value, source, at in anchors:
        await actions.assert_property(seat, "anchor_cwd", value, source, at, 0.9,
                                      evidence_class=_SD)
    return seat


async def test_heal_seat_anchor_refuses_a_seat_with_no_handle(actions: Actions, tmp_path) -> None:
    await actions.create_or_find_object("Seat", "seat:nohandle", "test")
    out = await heal_seat_anchor(actions, seat_id="seat:nohandle", actor="agent:test",
                                 office_root=tmp_path)
    assert "error" in out and "no handle" in out["error"]


async def test_heal_seat_anchor_refuses_when_office_does_not_exist(
    actions: Actions, tmp_path,
) -> None:
    await _seat_with_handle_and_anchors(
        actions, "seat:noOffice", "NoOffice",
        ("/somewhere/else", "agent:x", NOW - timedelta(days=1)))
    out = await heal_seat_anchor(actions, seat_id="seat:noOffice", actor="agent:test",
                                 office_root=tmp_path)
    assert "error" in out and "does not exist on disk" in out["error"]


async def test_heal_seat_anchor_reports_already_correct(actions: Actions, tmp_path) -> None:
    (tmp_path / "correct").mkdir()
    target = str(tmp_path / "correct")
    await _seat_with_handle_and_anchors(
        actions, "seat:already1", "Correct", (target, "console", NOW))
    out = await heal_seat_anchor(actions, seat_id="seat:already1", actor="agent:test",
                                 office_root=tmp_path)
    assert out["healed"] is False
    assert out["reason"] == "already correct"


async def test_heal_seat_anchor_dry_run_does_not_write(actions: Actions, tmp_path) -> None:
    (tmp_path / "jesus").mkdir()
    target = str(tmp_path / "jesus")
    await _seat_with_handle_and_anchors(
        actions, "seat:dryjesus", "Jesus",
        (target, "console", NOW - timedelta(days=30)),
        ("/home/asuramaya/code/REPOS/Godel", "agent:selfrebind", NOW - timedelta(days=1)))
    out = await heal_seat_anchor(actions, seat_id="seat:dryjesus", actor="agent:test",
                                 office_root=tmp_path, dry_run=True)
    assert out["dry_run"] is True
    assert out["target"] == target
    assert len(out["current_before"]) == 2
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 "
        "AND name='anchor_cwd'", await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical='seat:dryjesus'"))
    assert {r["v"] for r in rows} == {target, "/home/asuramaya/code/REPOS/Godel"}


async def test_heal_seat_anchor_apply_retracts_the_rogue_value(
    actions: Actions, tmp_path,
) -> None:
    (tmp_path / "chad").mkdir()
    target = str(tmp_path / "chad")
    seat = await _seat_with_handle_and_anchors(
        actions, "seat:applychad", "Chad",
        (target, "console", NOW - timedelta(days=30)),
        ("/home/asuramaya/code/cdking", "agent:selfrebind", NOW - timedelta(days=1)))
    out = await heal_seat_anchor(actions, seat_id="seat:applychad", actor="agent:test",
                                 office_root=tmp_path, dry_run=False)
    assert out["healed"] is True
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 "
        "AND name='anchor_cwd'", seat)
    assert [r["v"] for r in rows] == [target]


async def test_heal_seat_anchor_apply_writes_when_no_office_anchor_exists(
    actions: Actions, tmp_path,
) -> None:
    """Marquee's own shape: TWO rogue anchors, no office anchor at all — this call both
    WRITES the office anchor and retracts both strays, in the one assert_singular_property
    call."""
    (tmp_path / "marquee").mkdir()
    target = str(tmp_path / "marquee")
    seat = await _seat_with_handle_and_anchors(
        actions, "seat:applymarquee", "Marquee",
        ("/home/asuramaya/code/dealer-to-fb", "agent:a", NOW - timedelta(days=20)),
        ("/home/asuramaya/code/dtfb", "agent:b", NOW - timedelta(days=1)))
    out = await heal_seat_anchor(actions, seat_id="seat:applymarquee", actor="agent:test",
                                 office_root=tmp_path, dry_run=False)
    assert out["healed"] is True
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id=$1 "
        "AND name='anchor_cwd'", seat)
    assert [r["v"] for r in rows] == [target]


async def test_heal_seat_anchor_third_party_requires_a_reason(
    actions: Actions, tmp_path,
) -> None:
    (tmp_path / "henry").mkdir()
    target = str(tmp_path / "henry")
    await _seat_with_handle_and_anchors(
        actions, "seat:tphenry", "Henry",
        (target, "console", NOW - timedelta(days=30)),
        ("/home/asuramaya/code/shellbiz", "agent:selfrebind", NOW - timedelta(days=1)))
    refused = await heal_seat_anchor_third_party(
        actions, seat_id="seat:tphenry", because="", actor="agent:coordinator",
        office_root=tmp_path)
    assert "error" in refused
    out = await heal_seat_anchor_third_party(
        actions, seat_id="seat:tphenry", because="anchor invariant sweep",
        actor="agent:coordinator", office_root=tmp_path, dry_run=False)
    assert out["healed"] is True

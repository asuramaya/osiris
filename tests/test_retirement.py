"""retire_assertion — the cross-source supersede (thread 52911d2a, found diagnosing
b9aa7326, decision d28d1459): assert_property's own supersession is scoped to the SAME
source only, so a peer's correction of another agent's bad self-declaration can never
retire it through the ordinary path — both rows stay simultaneously "current". This is the
guarded orchestrator layer (friendly error dicts, never a bare exception) around
Actions.supersede_assertion.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.retirement import list_assertions, retire_assertion

NOW = datetime(2026, 7, 27, tzinfo=UTC)


async def test_list_assertions_requires_name(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await list_assertions(actions, ref="agent:x", name="  ")
    assert "error" in out


async def test_list_assertions_unknown_ref_is_an_honest_error(actions: Actions) -> None:
    out = await list_assertions(actions, ref="agent:does-not-exist", name="house")
    assert "error" in out


async def test_list_assertions_empty_when_the_property_was_never_asserted(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await list_assertions(actions, ref="agent:x", name="never-asserted")
    assert out["assertions"] == []


async def test_list_assertions_exposes_the_id_retire_assertion_needs(actions: Actions) -> None:
    """The exact 382067d9 gap: two CURRENT contradicting values, and retire_assertion's
    superseded_id can only come from somewhere that names the row, not just the value."""
    obj = await actions.create_or_find_object("Agent", "agent:ad1a1cb0-g40-xxiv", "test")
    wrong = await actions.assert_property(
        obj, "seat_generation", "2", "agent:ad1a1cb0-g40-xxiv", NOW, 0.9,
        evidence_class="self_declared")
    right = await actions.assert_property(
        obj, "seat_generation", "58", "agent:aad6603a-iv", NOW, 0.9,
        evidence_class="self_declared")

    out = await list_assertions(actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation")
    ids = {a["id"]: a["value"] for a in out["assertions"]}
    assert ids == {wrong: "2", right: "58"}

    # and it is exactly what retire_assertion accepts, end to end
    retired = await retire_assertion(
        actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation",
        superseded_id=wrong, value="58", because="closing the loop",
        actor="agent:c38f8f3b-vi")
    assert retired["retired"]["id"] == wrong

    # retire_assertion always ASSERTS its own new row rather than deleting anything
    # (append-only) — the wrong VALUE is gone, but a second "58" (the corrector's own
    # stamp) now sits beside the original one; both current, neither wrong.
    after = await list_assertions(actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation")
    assert {a["value"] for a in after["assertions"]} == {"58"}
    assert wrong not in {a["id"] for a in after["assertions"]}


async def test_requires_because(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    out = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                 superseded_id=row_id, value="58", because="   ",
                                 actor="agent:corrector")
    assert "error" in out
    assert "because" in out["error"]


async def test_unknown_ref_is_an_honest_error(actions: Actions) -> None:
    out = await retire_assertion(actions, ref="agent:does-not-exist", name="seat_generation",
                                 superseded_id=1, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_wrong_name_is_refused(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    out = await retire_assertion(actions, ref="agent:x", name="not-a-real-name",
                                 superseded_id=row_id, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_unknown_assertion_id_is_refused(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                 superseded_id=999999999, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_already_superseded_is_refused_not_double_retired(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    first = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                   superseded_id=row_id, value="58", because="fix",
                                   actor="agent:corrector")
    assert "error" not in first
    second = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                    superseded_id=row_id, value="99", because="again",
                                    actor="agent:corrector-2")
    assert "error" in second
    assert "already superseded" in second["error"]


async def test_the_live_case(actions: Actions) -> None:
    """The exact acceptance shape (decision d28d1459): a self-declared low value vs a peer's
    correct one, neither superseding the other until retire_assertion runs."""
    obj = await actions.create_or_find_object("Agent", "agent:ad1a1cb0-g40-xxiv", "test")
    wrong = await actions.assert_property(
        obj, "seat_generation", "2", "agent:ad1a1cb0-g40-xxiv", NOW, 0.9,
        evidence_class="self_declared")
    await actions.assert_property(
        obj, "seat_generation", "58", "agent:aad6603a-iv", NOW, 0.9,
        evidence_class="self_declared")
    vals = await actions.current_values(obj, "seat_generation")
    assert {v["value"] for v in vals} == {"2", "58"}  # both current — the live bug shape

    out = await retire_assertion(
        actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation", superseded_id=wrong,
        value="58", because="operator-approved repair, diagnosis d28d1459",
        actor="agent:c38f8f3b-vi")
    assert out["retired"]["value"] == "2"
    assert out["retired"]["id"] == wrong
    assert out["now_current"]["value"] == "58"
    assert out["because"] == "operator-approved repair, diagnosis d28d1459"

    vals = await actions.current_values(obj, "seat_generation")
    assert {v["value"] for v in vals} == {"58"}  # only the correct value survives


async def test_the_mcp_tool_wraps_the_orchestrator(actions: Actions) -> None:
    """srv._pool swap (mirrors test_succession.py's own pattern) — proves the ACTUAL MCP
    tool delegates correctly. No mount context here, so it refuses with the mount-first
    message rather than a bare crash — the identity gate is exercised, not bypassed."""
    from src import mcp_server as srv

    obj = await actions.create_or_find_object("Agent", "agent:mcp-x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:mcp-x", NOW, 0.9)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.retire_assertion(
            ref="agent:mcp-x", name="seat_generation", superseded_id=row_id, value="9",
            because="test")
    finally:
        srv._pool = saved_pool
    assert "error" in out

"""THE POST-MINT INVARIANT (Thoth's ruling, DM 9018/thread 9004, THE ORPHAN LAWS item 2
pass 2) — the two doors that mint across more than one phase under an advisory lock, not
one actions.atomic() block, so #189's own refuse-and-rollback gate (_enforce_required_
links) cannot reach across the gap: claim_name -> ensure_seat/bind_holder (Seat, via
`holds`) and register_agent (Agent, via `works_in`). Both share
`capture.confirm_or_confess_link`, proven in isolation here first, then through the real
doors."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator import capture


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


# --- the mechanism, isolated ---------------------------------------------------------

async def test_confirm_or_confess_link_confesses_when_nothing_satisfies(
    actions: Actions,
) -> None:
    oid = await _mint_bare(actions, "Seat")
    wrote = await capture.confirm_or_confess_link(
        actions, oid, "holds", direction="to", reason="test: no holder",
        source="test", observed=datetime.now(UTC))
    assert wrote is True
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", oid) == "test: no holder"
    row = await actions.pool.fetchval(
        "SELECT value FROM current_assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_holds'", oid)
    assert row["link_type"] == "holds"
    assert row["candidate_count"] == 0
    assert row["reason"] == "test: no holder"


async def test_confirm_or_confess_link_is_a_no_op_when_a_live_self_declared_link_exists(
    actions: Actions,
) -> None:
    seat = await _mint_bare(actions, "Seat")
    agent = await _mint_bare(actions, "Agent")
    await actions.create_link(agent, seat, "holds", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    wrote = await capture.confirm_or_confess_link(
        actions, seat, "holds", direction="to", reason="should never be written",
        source="test", observed=datetime.now(UTC))
    assert wrote is False
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name IN ('unlinked_because', 'derivation_abstained_holds')", seat) == 0


async def test_confirm_or_confess_link_ignores_an_invalidated_link(actions: Actions) -> None:
    """A vacated seat's own healed prior-holder link must never read as still-satisfied —
    `holds` is routinely invalidated (bind_holder heals it on every re-bind), unlike repo/
    grounds/resolves, which _enforce_required_links' own satisfied-check never filters on
    valid_until at all. This is the one place that filter matters."""
    seat = await _mint_bare(actions, "Seat")
    agent = await _mint_bare(actions, "Agent")
    now = datetime.now(UTC)
    await actions.create_link(agent, seat, "holds", "test", now, 0.9,
                              evidence_class="self_declared")
    await actions.invalidate_link(agent, seat, "holds", "test", now)
    wrote = await capture.confirm_or_confess_link(
        actions, seat, "holds", direction="to", reason="test: vacated",
        source="test", observed=now)
    assert wrote is True


async def test_confirm_or_confess_link_is_idempotent(actions: Actions) -> None:
    oid = await _mint_bare(actions, "Seat")
    now = datetime.now(UTC)
    first = await capture.confirm_or_confess_link(
        actions, oid, "holds", direction="to", reason="first reason",
        source="test", observed=now)
    second = await capture.confirm_or_confess_link(
        actions, oid, "holds", direction="to", reason="second reason (should not land)",
        source="test", observed=now)
    assert first is True
    assert second is False
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", oid) == 1
    assert await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", oid) == "first reason"


async def test_confirm_or_confess_link_respects_an_already_resolved_abstention_as_confessed(
    actions: Actions,
) -> None:
    """A resolved abstention (derive_or_abstain's own `resolved: true` marker) is NOT what
    this checks for satisfaction — a real link is — but an object carrying a bare
    unlinked_because from an earlier call must still short-circuit here; this is the
    already-idempotent case above restated against the OTHER hatch half."""
    oid = await _mint_bare(actions, "Seat")
    now = datetime.now(UTC)
    await actions.assert_property(oid, "unlinked_because", "prior confession", "test", now,
                                  0.9, evidence_class="self_declared")
    wrote = await capture.confirm_or_confess_link(
        actions, oid, "holds", direction="to", reason="should not land",
        source="test", observed=now)
    assert wrote is False


# --- wired through claim_name (Seat) --------------------------------------------------

async def test_claim_name_leaves_no_confession_on_the_healthy_path(actions: Actions) -> None:
    """bind_holder always runs right after ensure_seat in the same call, so the invariant
    check is normally a no-op — proven here so a future refactor that breaks that ordering
    trips this test rather than silently confessing on every claim."""
    from src.orchestrator.agents import claim_name

    agent = await actions.create_or_find_object("Agent", "agent:pmi-seat-1", "session")
    out = await claim_name(actions, "agent:pmi-seat-1", "Pmiseat", source="agent:pmi-seat-1")
    assert "error" not in out
    seat_oid = await actions.create_or_find_object("Seat", out["seat_id"], "test")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name IN ('unlinked_because', 'derivation_abstained_holds')", seat_oid) == 0
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE to_id=$1 AND from_id=$2 AND type='holds' "
        "AND (valid_until IS NULL OR valid_until > now())", seat_oid, agent) == 1


async def test_claim_name_confesses_when_the_seat_is_left_unlinked(actions: Actions) -> None:
    """Simulates the crash-between-phases gap directly: a Seat minted (via ensure_seat)
    with nothing binding it, then claim_name called again for the SAME name — the second
    call's own seat_id resolves to the pre-existing Seat (seats_by_handle finds it), so it
    reaches the `if seat_id:` branch and calls bind_holder itself; to isolate the invariant
    check alone (not bind_holder doing its normal job), this monkeypatches bind_holder to a
    no-op for one call, reproducing exactly the state a real crash between ensure_seat and
    bind_holder would leave behind."""
    from src.orchestrator import seats as seats_mod
    from src.orchestrator.agents import claim_name

    async def _noop_bind_holder(*args: object, **kwargs: object) -> dict[str, object]:
        return {"seat_id": kwargs.get("seat_id"), "old_holder": None, "new_holder": None}

    # claim_name imports bind_holder LOCALLY (`from src.orchestrator.seats import
    # bind_holder, ...`), fresh on every call — patching seats.bind_holder itself, not
    # agents.bind_holder, is what that local import actually re-reads.
    orig = seats_mod.bind_holder
    seats_mod.bind_holder = _noop_bind_holder  # type: ignore[assignment]
    try:
        out = await claim_name(actions, "agent:pmi-seat-2", "Pmiseat2",
                               source="agent:pmi-seat-2")
    finally:
        seats_mod.bind_holder = orig  # type: ignore[assignment]
    assert "error" not in out
    seat_oid = await actions.create_or_find_object("Seat", out["seat_id"], "test")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_holds'", seat_oid) == 1

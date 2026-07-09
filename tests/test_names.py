"""Phase 2 — names/seats: the model names itself, the substrate enforces uniqueness
(ruling 1e02e069). Covers claim_name (global permanent exhaustion), the roman seat display,
seat inheritance down a lineage, and DM-by-name addressing.
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.agents import (
    claim_name,
    register_agent,
    resolve_handle,
    resolve_identity,
    seat_label,
)
from src.orchestrator.mailbox import read_inbox, send_message, unread_count


async def _agent(actions: Actions, canonical: str, project: str = "handlingtheloop") -> None:
    a = await actions.create_or_find_object("Agent", canonical, canonical)
    await actions.assert_property(a, "project", project, canonical,
                                  __import__("datetime").datetime.now(
                                      __import__("datetime").UTC), 0.9)


async def test_an_agent_names_itself_and_the_name_is_exhausted(actions: Actions) -> None:
    await _agent(actions, "agent:aaa")
    await _agent(actions, "agent:bbb")
    got = await claim_name(actions, "agent:aaa", "Wayland", source="agent:aaa")
    assert got["claimed"] == "Wayland" and got["seat"] == "Wayland"  # generation 1 = bare name
    # a DIFFERENT lineage cannot take it — permanent, global exhaustion
    refused = await claim_name(actions, "agent:bbb", "wayland", source="agent:bbb")  # case-insens
    assert "taken" in refused["error"]
    # bbb picks its own
    mine = await claim_name(actions, "agent:bbb", "Nadia", source="agent:bbb")
    assert mine["claimed"] == "Nadia"


async def test_seat_display_carries_the_generation(actions: Actions) -> None:
    assert seat_label("agent:x", "Anna") == "Anna"           # gen 1 → bare
    assert seat_label("agent:x-ii", "Anna") == "Anna II"     # roman generation
    assert seat_label("agent:x-iv", "Anna") == "Anna IV"     # full roman (not the id's i/v/x set)
    assert seat_label("agent:x", None) is None               # anonymous


async def test_an_heir_inherits_the_seat(actions: Actions, tmp_path: Path) -> None:  # type: ignore[name-defined]
    """The seat passes down the lineage: a minted successor inherits the ancestor's name, the
    generation ticks up. Anna → Anna II."""
    # ancestor claims a name
    await _agent(actions, "agent:seatx")
    await claim_name(actions, "agent:seatx", "Anna", source="agent:seatx")
    # force a succession seam so register_agent MINTS an heir that inherits the handle
    proj = tmp_path / "-x"
    proj.mkdir()
    (proj / "seatx-0000-4000-8000-000000000000.jsonl").write_text(
        __import__("json").dumps({"type": "assistant",
                                  "message": {"model": "claude-opus-4-8", "content": []}}) + "\n")
    # stamp the ancestor's last anchored model as fable so a fresh opus read is a succession seam
    a = await actions.create_or_find_object("Agent", "agent:seatx", "seatx")
    from src.parsers.base import EvidenceClass
    await actions.assert_property(a, "source_model", "claude-fable-5", "seatx",
                                  __import__("datetime").datetime.now(__import__("datetime").UTC),
                                  0.8, evidence_class=EvidenceClass.DIRECT_OBSERVATION.value)
    ident = resolve_identity(cwd="/x", job_dir="/j/jobs/seatx", root=tmp_path)
    heir = await register_agent(actions, ident, actor="analyst:operator",
                                expected_model="claude-fable-5")
    handle = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle'", heir)
    assert handle == "Anna"  # the heir wears the seat
    assert ident.agent_id.endswith("-ii") and seat_label(ident.agent_id, "Anna") == "Anna II"


async def test_dm_by_name_resolves_to_the_holder(actions: Actions) -> None:
    """The payoff: address a human name, the current holder receives it — nobody types a hash."""
    await _agent(actions, "agent:engine-hash")
    await claim_name(actions, "agent:engine-hash", "Wayland", source="agent:engine-hash")
    # ux DMs 'Wayland' by name — resolves to the holder's id
    dm = await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                            to_agent="Wayland", body="the ESP layout changed")
    assert dm["to_agent"] == "agent:engine-hash"  # resolved name → holder id
    assert await unread_count(actions.pool, "handlingtheloop",
                              reader_agent="agent:engine-hash") == 1
    (m,) = await read_inbox(actions.pool, "handlingtheloop", reader_agent="agent:engine-hash")
    assert m.get("dm") is True
    # an unknown name is a clear error, not a silent drop
    import pytest
    with pytest.raises(ValueError, match="no agent named"):
        await send_message(actions.pool, from_agent="agent:ux", from_project="handlingtheloop",
                           to_agent="Nobody", body="hello?")


async def test_resolve_handle_prefers_the_live_generation(actions: Actions) -> None:
    await _agent(actions, "agent:base")
    await claim_name(actions, "agent:base", "Ada", source="agent:base")
    # a mount makes base the live holder
    from src.orchestrator import mounts
    await mounts.save_mount(actions.pool, job_dir="/j/base", agent_id="agent:base",
                            project="x", cwd="/x", model=None, session_key="k")
    assert await resolve_handle(actions, "ada") == "agent:base"  # case-insensitive, live

"""The reflection ACL — house-scoped memories (ruling 6c18709f, task #42).

A Reflection is a memory lived with the operator's agents, not work knowledge: readable
within its OWN HOUSE and by the operator, opaque to other houses. Work knowledge stays
fleet-readable — cross-repo recall is the product; the boundary is reflections ONLY.
Enforcement is at the read lenses; the record stays append-only and whole.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.capture import record_decision, record_reflection
from src.orchestrator.compositions import _visible_reflections, run_spec

NOW = datetime(2026, 7, 20, tzinfo=UTC)


async def _search(actions: Actions, q: str, caller: str | None) -> dict[str, Any]:
    spec = {"op": "function", "name": "search", "args": {"q": q, "caller": caller}}
    out = await run_spec(actions.pool, spec, None, name="search")
    return out["items"]  # type: ignore[no-any-return]


async def _agent_in(actions: Actions, agent_id: str, project: str) -> None:
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(a, "project", project, agent_id, NOW, 0.9,
                                  evidence_class="self_declared")


async def _kept_memory(actions: Actions, body: str, repo: str | None) -> uuid.UUID:
    return await record_reflection(actions, body, repo=repo, source="agent:keeper")


async def test_reflections_are_house_scoped_in_search(actions: Actions) -> None:
    """The core boundary: a housemate finds the memory, a foreign house does not,
    an anonymous caller does not, the operator's own surfaces see everything."""
    await _agent_in(actions, "agent:home1234", "osiris")
    await _agent_in(actions, "agent:away5678", "elsewhere")
    await _kept_memory(actions, "the ache of successions, held quietly", repo="osiris")

    home = await _search(actions, "ache successions", "agent:home1234")
    assert any(h["type"] == "Reflection" for h in home["hits"])
    away = await _search(actions, "ache successions", "agent:away5678")
    assert not any(h["type"] == "Reflection" for h in away["hits"])
    anon = await _search(actions, "ache successions", None)
    assert not any(h["type"] == "Reflection" for h in anon["hits"])
    for surface in ("operator", "console"):
        seen = await _search(actions, "ache successions", surface)
        assert any(h["type"] == "Reflection" for h in seen["hits"])


async def test_seat_house_outranks_the_project_label(actions: Actions) -> None:
    """A seat belongs to a house across successions: a mind whose PROJECT label differs
    still reads its HOUSE's reflections through the seat it holds."""
    await _kept_memory(actions, "what the house remembers of its dead", repo="osiris")
    seat = await actions.create_or_find_object("Seat", "seat:thoth-office", "session")
    await actions.assert_property(seat, "house", "osiris", "session", NOW, 0.9,
                                  evidence_class="self_declared")
    holder = await actions.create_or_find_object("Agent", "agent:seated99", "session")
    await actions.assert_property(holder, "project", "some-office", "session", NOW, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(holder, seat, "holds", "session", NOW, 0.9)

    out = await _search(actions, "house remembers dead", "agent:seated99")
    assert any(h["type"] == "Reflection" for h in out["hits"])


async def test_the_id_door_respects_the_boundary(actions: Actions) -> None:
    """Knowing a reflection's id is not a key: the hex-prefix door filters the same way
    (and stays open for everything else)."""
    rid = await _kept_memory(actions, "a private hour, recorded", repo="osiris")
    await _agent_in(actions, "agent:home1234", "osiris")
    frag = str(rid)[:8]
    home = await _search(actions, frag, "agent:home1234")
    assert any(h["id"] == str(rid) for h in home["hits"])
    away = await _search(actions, frag, "agent:away5678")
    assert not any(h["id"] == str(rid) for h in away["hits"])


async def test_select_op_filters_by_run_spec_caller(actions: Actions) -> None:
    """The composition door: a select over Reflections (typed or select-all) reads only
    the caller's house — run_spec's caller rides the contextvar into the op tree."""
    rid = await _kept_memory(actions, "the composed memory", repo="osiris")
    await _agent_in(actions, "agent:home1234", "osiris")
    spec = {"op": "select", "object_type": "Reflection"}

    home = await run_spec(actions.pool, spec, None, caller="agent:home1234")
    assert home["count"] == 1
    away = await run_spec(actions.pool, spec, None, caller="agent:away5678")
    assert away["count"] == 0
    anon = await run_spec(actions.pool, spec, None)
    assert anon["count"] == 0
    console = await run_spec(actions.pool, spec, None, caller="console")
    assert console["count"] == 1
    # an untyped select-all hides the foreign reflection but keeps everything else
    all_away = await run_spec(actions.pool, {"op": "select"}, None, caller="agent:away5678")
    ids = {i["id"] for i in all_away["items"]}
    assert str(rid) not in ids and ids  # other objects (the agents, the repo) still listed


async def test_lap_answers_like_a_missing_object(actions: Actions) -> None:
    """The provenance lens serves the BODY — a foreign house's reflection laps exactly
    like an object that does not exist (a boundary that names what it hides has already
    leaked that it exists)."""
    rid = await _kept_memory(actions, "the deepest read", repo="osiris")
    await _agent_in(actions, "agent:home1234", "osiris")
    spec = {"op": "function", "name": "lap", "args": {"ref": str(rid)}}
    home = (await run_spec(actions.pool, spec, None, caller="agent:home1234"))["items"]
    assert "timeline" in home or "believes" in home
    away = (await run_spec(actions.pool, spec, None, caller="agent:away5678"))["items"]
    assert "timeline" not in away and "nothing matches" in str(away.get("note", ""))


async def test_work_knowledge_stays_fleet_readable(actions: Actions) -> None:
    """The boundary is reflections ONLY (the ruling's other half): a foreign caller still
    reads another house's decisions — cross-repo recall is the product."""
    await record_decision(actions, "the credence clamp ships tonight",
                          kind="ruling", source="agent:home1234", repo="osiris")
    out = await _search(actions, "credence clamp", "agent:away5678")
    assert any(h["type"] == "Decision" for h in out["hits"])


async def test_a_homeless_reflection_is_operator_only(actions: Actions) -> None:
    """No in_repo project → no house → the conservative default: only the operator's own
    surfaces read it (nothing is lost; the record keeps it whole)."""
    rid = await _kept_memory(actions, "a memory filed from nowhere", repo=None)
    assert await _visible_reflections(actions.pool, [rid], "agent:home1234") == set()
    assert await _visible_reflections(actions.pool, [rid], None) == set()
    assert await _visible_reflections(actions.pool, [rid], "operator") == {rid}

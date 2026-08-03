"""MERGE / UNMERGE — the collapsed, symmetric pair (ruling 31c02dca) that replaces
fold_agent + fold_seat + fold_project (the three dupe/into/evidence merges) and
unfold_agent (their one, Agent-only reversal). These tests witness the DISPATCH layer
only — self-typing routing by `dupe`'s own form, the one new cross-type refusal this
collapse introduces, and the PARITY acceptance test the operator's own ruling names by
name ("a test that enumerates every type merge() accepts and asserts unmerge() accepts
the same set, so the asymmetry cannot silently return"). Each type's own refusal surface
(thin evidence, dupe==into, unknown/already-folded labels, contradiction gates, the
holder-liveness contradiction, the actor gate) is already fully covered where that type's
own fold_X/unfold_X lives (test_folds.py, the fold_seat/unfold_seat section of
test_seats.py, the fold_project/unfold_project section of test_projects.py) — not
re-proven here.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.merge import _merge_type, merge, unmerge

NOW = datetime.now(UTC)


async def _mk_agent(actions: Actions, label: str) -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", "mergehouse", label, NOW, 0.9,
                                  evidence_class="self_declared")


async def _stub_project(actions: Actions, canon: str, name: str) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", canon, "test")
    await actions.assert_property(pid, "name", name, "test", NOW, 0.9)


def test_merge_type_reads_the_prefix_and_nothing_else() -> None:
    assert _merge_type("agent:foo") == "Agent"
    assert _merge_type("seat:foo") == "Seat"
    assert _merge_type("repo:foo") == "SoftwareProject"
    assert _merge_type("bare-name") == "SoftwareProject"
    assert _merge_type("") == "SoftwareProject"


# ═══ dispatch: each type routes to its own, unmodified fold_X/unfold_X ═══


async def test_merge_routes_an_agent_pair_to_fold_agent(actions: Actions) -> None:
    await _mk_agent(actions, "agent:mg1dupe0")
    await _mk_agent(actions, "agent:mg1into0")
    out = await merge(actions, dupe="agent:mg1dupe0", into="agent:mg1into0",
                      evidence="census: co-timed sessions, same cwd", actor="operator")
    assert out["folded"] == "agent:mg1dupe0" and out["into"] == "agent:mg1into0"
    assert "mail_readdressed" in out  # fold_agent's own estate shape, untouched


async def test_merge_routes_a_seat_pair_to_fold_seat(actions: Actions) -> None:
    await actions.create_or_find_object("Seat", "seat:mg2dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:mg2into0", "test")
    out = await merge(actions, dupe="seat:mg2dupe0", into="seat:mg2into0",
                      evidence="test: the Vajra twin shape", actor="test")
    assert out["folded"] == "seat:mg2dupe0" and out["into"] == "seat:mg2into0"
    assert "holders_moved" in out  # fold_seat's own estate shape, untouched


async def test_merge_routes_a_bare_project_pair_to_fold_project(actions: Actions) -> None:
    await _stub_project(actions, "repo:mg3dupe0", "mg3dupe0")
    await _stub_project(actions, "repo:mg3into0", "mg3into0")
    out = await merge(actions, dupe="mg3dupe0", into="mg3into0",
                      evidence="both mint the same repo", actor="agent:test")
    assert out["folded"] == "repo:mg3dupe0" and out["into"] == "repo:mg3into0"
    assert "edges_moved" in out  # fold_project's own estate shape, untouched


async def test_merge_refuses_a_cross_type_pairing(actions: Actions) -> None:
    """The one refusal this collapse itself introduces — never reachable through any of
    the three original verbs, because none of them could ever be dialed with a foreign
    type's ref (fold_agent only ever queried type='Agent', and so on)."""
    await _mk_agent(actions, "agent:mg4dupe0")
    await actions.create_or_find_object("Seat", "seat:mg4into0", "test")
    out = await merge(actions, dupe="agent:mg4dupe0", into="seat:mg4into0",
                      evidence="x", actor="operator")
    assert "same-type only" in out["error"]
    assert "Agent" in out["error"] and "Seat" in out["error"]
    # nothing written on either side
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:mg4dupe0'")
    assert st == "active"


async def test_unmerge_routes_an_agent_dupe_to_unfold_agent(actions: Actions) -> None:
    await _mk_agent(actions, "agent:um1dupe0")
    await _mk_agent(actions, "agent:um1into0")
    await merge(actions, dupe="agent:um1dupe0", into="agent:um1into0",
               evidence="x", actor="operator")
    out = await unmerge(actions, dupe="agent:um1dupe0", because="wrongly folded",
                        actor="agent:judge")
    assert out["was_merged_into"] == "agent:um1into0"
    assert any(p["op"] == "unmerge_objects" for p in out["plan"])


async def test_unmerge_routes_a_seat_dupe_to_unfold_seat(actions: Actions) -> None:
    await actions.create_or_find_object("Seat", "seat:um2dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:um2into0", "test")
    await merge(actions, dupe="seat:um2dupe0", into="seat:um2into0", evidence="x",
               actor="test")
    out = await unmerge(actions, dupe="seat:um2dupe0", because="wrongly folded",
                        actor="agent:judge")
    assert out["was_merged_into"] == "seat:um2into0"
    assert any(p["op"] == "unmerge_objects" for p in out["plan"])


async def test_unmerge_routes_a_project_dupe_to_unfold_project(actions: Actions) -> None:
    await _stub_project(actions, "repo:um3dupe0", "um3dupe0")
    await _stub_project(actions, "repo:um3into0", "um3into0")
    await merge(actions, dupe="um3dupe0", into="um3into0", evidence="x", actor="agent:test")
    out = await unmerge(actions, dupe="repo:um3dupe0", because="wrongly folded",
                        actor="agent:judge")
    assert out["was_merged_into"] == "repo:um3into0"
    assert any(p["op"] == "unmerge_objects" for p in out["plan"])


# ═══ THE PARITY ACCEPTANCE TEST (the operator's own ruling, 31c02dca, verbatim: "a test
# that enumerates every type merge() accepts and asserts unmerge() accepts the same set,
# so the asymmetry cannot silently return") ═══


async def test_merge_and_unmerge_accept_the_same_set_of_types(actions: Actions) -> None:
    """Before this build, `unfold_agent` was the ONLY reversal — a fold of a Seat or a
    SoftwareProject was permanent (task #127). This proves the parity directly: for every
    type merge() successfully folds, unmerge() on the SAME dupe also succeeds (returns a
    real plan, never an 'unsupported type' refusal) — the asymmetry this ruling exists to
    close cannot silently return."""
    fixtures: dict[str, tuple[str, str, Any]] = {}

    await _mk_agent(actions, "agent:pt1dupe0")
    await _mk_agent(actions, "agent:pt1into0")
    fixtures["Agent"] = ("agent:pt1dupe0", "agent:pt1into0", "operator")

    await actions.create_or_find_object("Seat", "seat:pt2dupe0", "test")
    await actions.create_or_find_object("Seat", "seat:pt2into0", "test")
    fixtures["Seat"] = ("seat:pt2dupe0", "seat:pt2into0", "test")

    await _stub_project(actions, "repo:pt3dupe0", "pt3dupe0")
    await _stub_project(actions, "repo:pt3into0", "pt3into0")
    fixtures["SoftwareProject"] = ("repo:pt3dupe0", "repo:pt3into0", "agent:test")

    # every type _merge_type can name must have a fixture above, or this test itself would
    # silently under-cover the enumeration it claims to run
    assert set(fixtures) == {"Agent", "Seat", "SoftwareProject"}

    merged_types: set[str] = set()
    unmerge_doors: set[str] = set()
    for type_name, (dupe, into, actor) in fixtures.items():
        merge_out = await merge(actions, dupe=dupe, into=into,
                                evidence="parity fixture", actor=actor)
        assert "error" not in merge_out, f"{type_name} merge fixture itself failed: {merge_out}"
        merged_types.add(type_name)

        unmerge_out = await unmerge(actions, dupe=dupe, because="parity check",
                                    actor="agent:judge")
        assert "error" not in unmerge_out, (
            f"{type_name}: merge() accepted this type but unmerge() refused it — "
            f"the exact asymmetry 31c02dca exists to close: {unmerge_out}")
        assert any(p["op"] == "unmerge_objects" for p in unmerge_out["plan"])
        unmerge_doors.add(type_name)

    assert merged_types == unmerge_doors == {"Agent", "Seat", "SoftwareProject"}


# ═══ the MCP tool layer — merge/unmerge must survive exposure through the tool wrapper ═══


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mounted(actions: Actions, agent_id: str, project: str) -> _Ctx:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project=project, model=None, cwd=None)
    return ctx


async def test_merge_tool_refuses_when_unmounted() -> None:
    from src import mcp_server as srv

    out = await srv.merge(dupe="agent:x", into="agent:y", evidence="x", ctx=None)
    assert "mount first" in out["error"]


async def test_unmerge_tool_refuses_when_unmounted() -> None:
    from src import mcp_server as srv

    out = await srv.unmerge(dupe="agent:x", because="x", ctx=None)
    assert "mount first" in out["error"]


async def test_merge_tool_reaches_fold_seat_through_the_wrapper(actions: Actions) -> None:
    from src import mcp_server as srv

    await actions.create_or_find_object("Seat", "seat:mgt1dupe", "test")
    await actions.create_or_find_object("Seat", "seat:mgt1into", "test")

    saved_pool = srv._pool
    ctx = await _mounted(actions, "agent:mergetool1", "mergetoolproj")
    try:
        out = await srv.merge(dupe="seat:mgt1dupe", into="seat:mgt1into",
                              evidence="test: the Vajra twin shape", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["folded"] == "seat:mgt1dupe" and out["into"] == "seat:mgt1into"


async def test_unmerge_tool_reaches_unfold_seat_through_the_wrapper(actions: Actions) -> None:
    from src import mcp_server as srv

    await actions.create_or_find_object("Seat", "seat:umt1dupe", "test")
    await actions.create_or_find_object("Seat", "seat:umt1into", "test")
    await merge(actions, dupe="seat:umt1dupe", into="seat:umt1into", evidence="x",
               actor="test")

    saved_pool = srv._pool
    ctx = await _mounted(actions, "agent:unmergetool1", "mergetoolproj")
    try:
        out = await srv.unmerge(dupe="seat:umt1dupe", because="wrongly folded", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["was_merged_into"] == "seat:umt1into"

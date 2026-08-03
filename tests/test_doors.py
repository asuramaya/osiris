"""doors(ref) — one coherent answer about an agent, a seat, or a cwd (thread 1aa2ff36)."""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.doors import doors
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat


async def test_agent_ref_resolves_project_cwd_model_and_seat(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="Anhur", source="test")
    await actions.create_or_find_object("Agent", "agent:live00001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:live00001")
    await save_mount(actions.pool, job_dir="/jobs/live00001", agent_id="agent:live00001",
                     project="osiris", cwd="/w/osiris", model="claude-sonnet-5",
                     session_key=None)

    out = await doors(actions.pool, "agent:live00001")
    assert out["ref"] == "agent:live00001" and out["resolved"] is True
    assert len(out["matches"]) == 1
    m = out["matches"][0]
    assert m["agent_id"] == "agent:live00001"
    assert m["project"] == "osiris" and m["cwd"] == "/w/osiris" and m["model"] == "claude-sonnet-5"
    assert m["seat"] == {"seat_id": seat["seat_id"], "handle": "Anhur", "house": "osiris"}
    assert m["live"] is True and m["reachable"] == {"mail": True}
    assert m["resolved_via"] == "agent-id"


async def test_agent_ref_with_no_evidence_at_all_is_unresolved(actions: Actions) -> None:
    out = await doors(actions.pool, "agent:ghost0001")
    assert out == {"ref": "agent:ghost0001", "resolved": False, "matches": []}


async def test_seat_binding_reads_the_holds_link_never_the_cache_column(
    actions: Actions,
) -> None:
    """The whole point: agent_mounts.seat_id is a hint, not a fact. Poison it with a WRONG
    seat and confirm doors() still reports the seat the `holds` link actually names."""
    real = await ensure_seat(actions, house="osiris", handle="Real", source="test")
    decoy = await ensure_seat(actions, house="osiris", handle="Decoy", source="test")
    await actions.create_or_find_object("Agent", "agent:cachetrap", "test")
    await bind_holder(actions, seat_id=real["seat_id"], agent_id="agent:cachetrap")
    await save_mount(actions.pool, job_dir="/jobs/cachetrap", agent_id="agent:cachetrap",
                     project="osiris", cwd="/w", model=None, session_key=None)
    # poison the cache column directly — no code path does this; a test simulating staleness
    await actions.pool.execute(
        "UPDATE agent_mounts SET seat_id=$1 WHERE agent_id='agent:cachetrap'", decoy["seat_id"])

    out = await doors(actions.pool, "agent:cachetrap")
    assert out["matches"][0]["seat"]["handle"] == "Real"


async def test_seat_ref_vacant_is_unresolved(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="Ptah", source="test")
    out = await doors(actions.pool, seat["seat_id"])
    assert out == {"ref": seat["seat_id"], "resolved": False, "matches": []}


async def test_seat_ref_cold_resolves_with_live_false(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="Wadjet", source="test")
    await actions.create_or_find_object("Agent", "agent:cold0001", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:cold0001")

    out = await doors(actions.pool, seat["seat_id"])
    assert out["resolved"] is True
    assert out["matches"][0]["live"] is False
    assert out["matches"][0]["resolved_via"] == "seat-link"


async def test_bare_handle_resolves_via_resolve_seat(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="Nekhbet", source="test")
    await actions.create_or_find_object("Agent", "agent:byhandle1", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:byhandle1")
    await save_mount(actions.pool, job_dir="/jobs/byhandle1", agent_id="agent:byhandle1",
                     project="osiris", cwd="/w", model=None, session_key=None)

    out = await doors(actions.pool, "Nekhbet")
    assert out["resolved"] is True
    assert out["matches"][0]["agent_id"] == "agent:byhandle1"
    assert out["matches"][0]["resolved_via"] == "handle"


async def test_unknown_handle_is_unresolved(actions: Actions) -> None:
    out = await doors(actions.pool, "NobodyByThisName")
    assert out == {"ref": "NobodyByThisName", "resolved": False, "matches": []}


async def test_cwd_ref_lists_every_distinct_soul_that_has_mounted_there(
    actions: Actions,
) -> None:
    """An office can be multi-tenant — this is the job nothing else in the corpus answers."""
    await save_mount(actions.pool, job_dir="/jobs/tenantA", agent_id="agent:tenanta01",
                     project="shared", cwd="/w/shared", model=None, session_key=None)
    await save_mount(actions.pool, job_dir="/jobs/tenantB", agent_id="agent:tenantb02",
                     project="shared", cwd="/w/shared", model=None, session_key=None)
    # two generations of the SAME soul — must fold to one match, not two
    await save_mount(actions.pool, job_dir="/jobs/heir1", agent_id="agent:heirsoul-i",
                     project="shared", cwd="/w/shared", model=None, session_key=None)
    await save_mount(actions.pool, job_dir="/jobs/heir2", agent_id="agent:heirsoul-ii",
                     project="shared", cwd="/w/shared", model=None, session_key=None)

    out = await doors(actions.pool, "/w/shared")
    assert out["resolved"] is True
    agent_ids = {m["agent_id"] for m in out["matches"]}
    assert agent_ids == {"agent:tenanta01", "agent:tenantb02", "agent:heirsoul-ii"}
    assert all(m["resolved_via"] == "cwd" for m in out["matches"])


async def test_cwd_ref_with_nobody_ever_mounted_is_unresolved(actions: Actions) -> None:
    out = await doors(actions.pool, "/w/nobody-was-ever-here")
    assert out == {"ref": "/w/nobody-was-ever-here", "resolved": False, "matches": []}


async def test_cwd_ref_expands_a_tilde_path(actions: Actions) -> None:
    home_cwd = f"{Path.home()}/w/tilde-test"
    await save_mount(actions.pool, job_dir="/jobs/tildeagent", agent_id="agent:tildeagnt",
                     project="p", cwd=home_cwd, model=None, session_key=None)
    out = await doors(actions.pool, "~/w/tilde-test")
    assert out["resolved"] is True and out["matches"][0]["agent_id"] == "agent:tildeagnt"


async def test_the_mcp_tool_wrapper_delegates_to_doors(actions: Actions) -> None:
    from src import mcp_server as srv

    await save_mount(actions.pool, job_dir="/jobs/wraptest", agent_id="agent:wraptest1",
                     project="p", cwd="/w", model=None, session_key=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.identify_agent("agent:wraptest1")
    finally:
        srv._pool = saved_pool
    assert out["resolved"] is True and out["matches"][0]["agent_id"] == "agent:wraptest1"

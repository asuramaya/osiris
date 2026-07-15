"""SEAT REBIND — move a seat's anchor cwd, preserving identity, lineage, attribution, and mail
(Phase 1 §4.1, ruling `dd47c1da`: 'path = project = identity' orphaned alfred when the operator
moved his folder — the operator is BLOCKED on this cure). Pilot shape exercised here: house
bytebye, alfred's seat, a pure office with no code in the folder.
"""
from __future__ import annotations

import tomllib
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import (
    claim_name,
    house_of,
    register_agent,
    resolve_handle,
    resolve_identity,
)
from src.orchestrator.mailbox import send_message, unread_count
from src.orchestrator.mounts import rebind_seat


async def test_rebind_moves_the_anchor_preserving_everything(
    actions: Actions, tmp_path: Path
) -> None:
    a_dir = tmp_path / "bytebye-a"
    b_dir = tmp_path / "bytebye-b"
    a_dir.mkdir()

    ident = resolve_identity(cwd=str(a_dir), session="alfred01", project_label="bytebye")
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "alfred", source=ident.agent_id)
    await mounts.save_mount(actions.pool, job_dir="/j/alfred01", agent_id=ident.agent_id,
                            project="bytebye", cwd=str(a_dir), model="claude-fable-5",
                            session_key="k")
    # mail addressed to alfred's PROJECT — unread, waiting
    await send_message(actions.pool, from_agent="agent:ux", from_project="bytebye",
                       to_project="bytebye", body="the office move is happening")
    assert await unread_count(actions.pool, "bytebye", reader_agent=ident.agent_id) == 1

    receipt = await rebind_seat(actions, seat_or_agent="alfred", new_cwd=str(b_dir))
    assert receipt["agent"] == ident.agent_id
    assert receipt["project"] == "bytebye"
    assert receipt["old_cwd"] == str(a_dir)
    assert receipt["new_cwd"] == str(b_dir)
    assert receipt["mount_rows_updated"] == 1
    assert receipt["osiris_written"] == str(b_dir / ".osiris")

    # (a) the SAME agent_id resolves — no mint, no fork
    assert await resolve_handle(actions, "alfred") == ident.agent_id
    # (b) the durable project label is UNCHANGED
    assert await house_of(actions.pool, ident.agent_id) == "bytebye"
    # (c) mail is still readable under the same label
    assert await unread_count(actions.pool, "bytebye", reader_agent=ident.agent_id) == 1
    # (d) the mount row points at B
    row = await actions.pool.fetchrow(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", ident.agent_id)
    assert row is not None and row["cwd"] == str(b_dir)
    # (e) .osiris in B carries the label
    osiris_file = b_dir / ".osiris"
    assert osiris_file.is_file()
    assert tomllib.loads(osiris_file.read_text())["project"] == "bytebye"
    # (f) an assertion records the move
    moved = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='anchor_moved' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", ident.agent_id)
    assert moved is not None and str(a_dir) in moved and str(b_dir) in moved


async def test_rebind_moves_every_generation_of_the_lineage(
    actions: Actions, tmp_path: Path
) -> None:
    """(d): the WHOLE lineage's durable mount rows move, not just the current holder's — or an
    earlier generation's row resurrects the seat at the old path the instant anything reads it
    by job_dir."""
    a_dir = tmp_path / "multi-a"
    b_dir = tmp_path / "multi-b"
    a_dir.mkdir()
    oid = await actions.create_or_find_object("Agent", "agent:multi", "agent:multi")
    await actions.assert_property(oid, "project", "multihouse", "agent:multi",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await claim_name(actions, "agent:multi", "Multi", source="agent:multi")
    await mounts.save_mount(actions.pool, job_dir="/j/multi-1", agent_id="agent:multi",
                            project="multihouse", cwd=str(a_dir), model="claude-fable-5",
                            session_key="k1")
    # a second generation's row (an earlier holder's), same lineage, same old path
    await mounts.save_mount(actions.pool, job_dir="/j/multi-0", agent_id="agent:multi-ii",
                            project="multihouse", cwd=str(a_dir), model="claude-opus-4-8",
                            session_key="k0")

    receipt = await rebind_seat(actions, seat_or_agent="Multi", new_cwd=str(b_dir))
    assert receipt["mount_rows_updated"] == 2
    rows = await actions.pool.fetch(
        "SELECT job_dir, cwd FROM agent_mounts WHERE job_dir IN ('/j/multi-1','/j/multi-0')")
    assert {r["cwd"] for r in rows} == {str(b_dir)}


async def test_rebind_of_a_raw_agent_id_works(actions: Actions, tmp_path: Path) -> None:
    """The grave rule: an explicit id is intent — a seat with NO claimed name can still be
    rebound by its raw agent id."""
    a_dir = tmp_path / "raw-a"
    b_dir = tmp_path / "raw-b"
    a_dir.mkdir()
    oid = await actions.create_or_find_object("Agent", "agent:rawid01", "agent:rawid01")
    await actions.assert_property(oid, "project", "rawhouse", "agent:rawid01",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await mounts.save_mount(actions.pool, job_dir="/j/rawid01", agent_id="agent:rawid01",
                            project="rawhouse", cwd=str(a_dir), model=None, session_key="k")

    receipt = await rebind_seat(actions, seat_or_agent="agent:rawid01", new_cwd=str(b_dir))
    assert receipt["agent"] == "agent:rawid01"
    assert receipt["project"] == "rawhouse"
    row = await actions.pool.fetchrow("SELECT cwd FROM agent_mounts WHERE job_dir='/j/rawid01'")
    assert row is not None and row["cwd"] == str(b_dir)


async def test_rebind_refuses_an_unknown_seat_loudly(actions: Actions, tmp_path: Path) -> None:
    target = tmp_path / "nowhere"
    out = await rebind_seat(actions, seat_or_agent="NobodyHome", new_cwd=str(target))
    assert "error" in out
    assert not target.exists()  # refused — nothing written, not even the directory


async def test_rebind_of_an_unmounted_bare_id_is_also_refused(
    actions: Actions, tmp_path: Path
) -> None:
    """A raw id with no Agent object at all is still 'unknown' — the grave rule is intent
    about a REAL grave, not licence to mint one."""
    out = await rebind_seat(actions, seat_or_agent="agent:never-existed",
                            new_cwd=str(tmp_path / "ghost"))
    assert "error" in out

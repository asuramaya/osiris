"""THE OFFICE CEREMONY (ruling ed5f5ce2) — one act moves a seat into its Osiris-owned home.
alfred's office was hand-assembled; the rollout to his chartered children is one call per
seat, and these witness the call.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.offices import establish_office


async def _seat_fixture(actions: Actions, tmp_path: Path, *, handle: str | None) -> str:
    """A mounted lineage in a shared repo cwd, optionally named — the pre-office shape."""
    agent = "agent:0ff1cee1"
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(a, "project", "butlerhouse", agent, now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "session", "0ff1cee1", agent, now, 0.9,
                                  evidence_class="self_declared")
    if handle:
        await actions.assert_property(a, "handle", handle, agent, now, 0.9,
                                      evidence_class="self_declared")
    shared = str(tmp_path / "shared-repo")
    Path(shared).mkdir(exist_ok=True)
    await save_mount(actions.pool, job_dir="/jobs/0ff1cee1", agent_id=agent,
                     project="butlerhouse", cwd=shared, model=None, session_key=None)
    # the seat is QUIET (the ceremony refuses a live one; the refusal test re-warms it)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' WHERE agent_id=$1",
        agent)
    slug = tmp_path / "projects" / shared.replace("/", "-")
    slug.mkdir(parents=True, exist_ok=True)
    (slug / "0ff1cee1-own-session.jsonl").write_text(f'{{"cwd": "{shared}"}}\n')
    (slug / "cafe0000-co-resident.jsonl").write_text(f'{{"cwd": "{shared}"}}\n')
    return agent


async def test_establish_office_the_whole_ceremony(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = await _seat_fixture(actions, tmp_path, handle="Butler")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    office = tmp_path / "seats" / "butler"
    assert out["office"] == str(office)
    assert out["handle"] == "Butler"
    assert out["house"] == "butlerhouse"
    assert out["standing_orders"] == "written"
    orders = (office / "CLAUDE.md").read_text()
    assert "Butler — seat office" in orders
    assert "house **butlerhouse**" in orders
    assert "never formally declared" in orders          # no governs links yet: instruct
    assert "not yet seated" in orders                   # unbound lineage: the on-ramp note
    assert "GRADE EVERY DM" in orders                    # every seat born knowing this now
    assert (office / ".osiris").read_text().startswith('project = "butlerhouse"')
    # THE CHARTER FILE (d80621a7 piece 3): a fresh office gets a live-state scratchpad
    # beside its standing orders
    assert out["charter_file"] == "written"
    charter = (office / "charter.md").read_text()
    assert "Butler's charter" in charter
    assert "OFFLOAD TARGET when context runs high" in charter
    assert "## Current work" in charter and "## Key ids" in charter
    # the extraction rode along: his transcript moved and re-addressed, co-resident stayed
    assert out["rebind"]["harness"]["transcripts_moved"] == 1
    moved = tmp_path / "projects" / str(office).replace("/", "-")
    assert (moved / "0ff1cee1-own-session.jsonl").is_file()
    shared_slug = tmp_path / "projects" / str(tmp_path / "shared-repo").replace("/", "-")
    assert (shared_slug / "cafe0000-co-resident.jsonl").is_file()
    row = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", agent)
    assert row == str(office)
    # THE DEED (a2d06410): the ceremony records office ownership in the GRAPH, so the
    # fourth door still opens after the seat's death takes its mount rows
    assert out["office_deed"] == "filed"
    deed = await actions.pool.fetchval(
        "SELECT d.value #>> '{}' FROM current_assertions d "
        "JOIN objects o2 ON o2.id=d.object_id "
        "WHERE d.name='office' AND o2.canonical=$1", agent)
    assert deed == str(office)

    # idempotent: the second ceremony converges, never clobbers the standing orders OR a
    # hand-edited charter — alfred's stays his, the whole point of the never-overwrite rule
    (office / "charter.md").write_text("# Butler's charter\n\nMY OWN HAND-WRITTEN NOTES.\n")
    again = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")
    assert again["standing_orders"].startswith("left in place")
    assert again["office_deed"] == "already on the lineage's record"
    assert again["charter_file"].startswith("left in place")
    assert "MY OWN HAND-WRITTEN NOTES" in (office / "charter.md").read_text()


async def test_establish_office_carries_a_declared_charter(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.charter import set_charter

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await set_charter(actions, agent, ["repoA", "repoB"])

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["charter"] == ["repoA", "repoB"]
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "You govern: `repoA`, `repoB`." in orders


async def test_establish_office_refuses_the_anonymous(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = await _seat_fixture(actions, tmp_path, handle=None)

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" in out
    assert "claim_name" in out["error"]
    assert not (tmp_path / "seats").exists()            # a refusal writes NOTHING


async def test_establish_office_refuses_the_unknown(
    actions: Actions, tmp_path: Path,
) -> None:
    out = await establish_office(
        actions, seat_or_agent="agent:00000000", actor="agent:test",
        office_root=tmp_path / "seats")
    assert "error" in out
    assert "never invents" in out["error"]


async def test_establish_office_builds_for_a_never_claimed_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thread 236d3940 (mirroring 3ae57d36's rebind_seat fix): a seat minted by
    mint_seat/ensure_seat but never claim_name'd by any agent (grantprobe's real shape) used
    to refuse outright here — agent resolution found nobody, and the ceremony never checked
    the Seat record directly. Calling by the seat's OWN handle must now build a full office
    off the seat alone, with no Agent object involved anywhere in this test."""
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="anchorhouse", handle="Orphaned", source="test")

    out = await establish_office(
        actions, seat_or_agent="Orphaned", actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" not in out
    assert out["seat"] == seat["seat_id"]
    assert out["handle"] == "Orphaned"
    assert out["house"] == "anchorhouse"
    assert out["office_deed"].startswith("n/a")
    office = tmp_path / "seats" / "orphaned"
    assert out["office"] == str(office)
    orders = (office / "CLAUDE.md").read_text()
    assert f"durable identity `{seat['seat_id']}`" in orders
    assert "never formally declared" in orders          # no governs links: nobody to declare
    assert (office / ".osiris").read_text().startswith('project = "anchorhouse"')
    assert out["charter_file"] == "written"
    anchor = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='anchor_cwd' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat["seat_id"])
    assert anchor == str(office)

    # idempotent, same guarantee as the agent-lineage path
    again = await establish_office(
        actions, seat_or_agent="Orphaned", actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")
    assert again["standing_orders"].startswith("left in place")


async def test_establish_office_renders_peer_addendum_when_peered(
    actions: Actions, tmp_path: Path,
) -> None:
    """LEGIBILITY leg 2 (ruling d74492ee, spec e6636c7e): a peer_of bond is rendered
    into the seat's own standing orders — computed LIVE at establish_office's own call
    time (unlike mintseat.py's fresh-mint scaffold, which never has a peer yet)."""
    from src.orchestrator.seats import bind_holder, peer_seats

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await bind_holder(actions, seat_id="seat:butlerseat", agent_id=agent, source="test")
    await actions.assert_property(
        await actions.create_or_find_object("Seat", "seat:peerseat9", "test"), "handle",
        "Halcyon", "test", datetime.now(UTC), 0.9, evidence_class="self_declared")
    await peer_seats(actions, "seat:butlerseat", "seat:peerseat9", because="the pairing",
                     actor="test")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["standing_orders"] == "written"
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "## Peer" in orders
    assert "peered with **Halcyon** (`seat:peerseat9`)" in orders
    assert "Two-tier decisions" in orders and "Mutual hold" in orders
    # the surrounding sections are untouched — one blank line on either side, same as
    # the unpeered rendering's own spacing
    assert "## How to work from an office" in orders


async def test_establish_office_renders_no_peer_addendum_when_unpeered(
    actions: Actions, tmp_path: Path,
) -> None:
    """A SEATED but unpeered lineage gets none of the addendum — the block is gated on
    an actual peer_of edge, not merely on holding a durable seat."""
    from src.orchestrator.seats import bind_holder

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await bind_holder(actions, seat_id="seat:butlerseat2", agent_id=agent, source="test")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["standing_orders"] == "written"
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "## Peer" not in orders


async def test_establish_office_refuses_a_live_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """The rollout guard: extraction moves the lineage's transcripts, and a running
    harness appends to its own by path — a live seat (any generation) is never moved."""
    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=now() WHERE agent_id=$1", agent)

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" in out
    assert "LIVE right now" in out["error"]
    assert not (tmp_path / "seats").exists()            # a refusal writes NOTHING

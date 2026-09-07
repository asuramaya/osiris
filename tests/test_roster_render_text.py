"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): roster(render='text') -- one
line per seat, grouped by house, with an occupancy glyph."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat


async def test_roster_render_text_groups_by_house_with_occupancy_glyphs(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    await ensure_seat(actions, house="rrthouse", handle="Rrtvacant", source="test")
    cold = await ensure_seat(actions, house="rrthouse", handle="Rrtcold", source="test")
    await actions.create_or_find_object("Agent", "agent:rrtcold001", "test")
    await bind_holder(actions, seat_id=cold["seat_id"], agent_id="agent:rrtcold001")
    occupied = await ensure_seat(actions, house="rrthouse", handle="Rrtoccup", source="test")
    await actions.create_or_find_object("Agent", "agent:rrtlive001", "test")
    await bind_holder(actions, seat_id=occupied["seat_id"], agent_id="agent:rrtlive001")
    await save_mount(actions.pool, job_dir="/jobs/rrtlive001", agent_id="agent:rrtlive001",
                     project="rrthouse", cwd="/w/rrthouse", model="claude-sonnet-5",
                     session_key=None)

    saved = srv._pool
    srv._pool = actions.pool
    try:
        structured = await srv.roster()
        out = await srv.roster(render="text")
    finally:
        srv._pool = saved

    assert set(out.keys()) == {"text"}
    text = out["text"]
    assert "rrthouse:" in text
    assert "· Rrtvacant" in text
    assert "○ Rrtcold" in text
    assert "● Rrtoccup" in text
    assert "agent:rrtlive001" in text  # holder shown for the live seat
    _ = structured


async def test_roster_render_text_with_repo_uses_the_generic_fallback(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.roster(repo="no-such-repo-anywhere", render="text")
    finally:
        srv._pool = saved

    assert set(out.keys()) == {"text"}
    assert "agreement: no-match" in out["text"]
